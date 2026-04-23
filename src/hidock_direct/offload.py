"""Per-file offload pipeline: size-stable check, chunked download, SHA verify,
atomic rename, state update.

See PRD §2.3 for the step-by-step contract. The atomicity invariant is load
bearing: any exception leaves zero partial files in the archive and zero
entries in `offload_state.json`.
"""

from __future__ import annotations

import hashlib
import os
import shutil
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable, List, Optional

from .classify import RecordingKind, classify_recording
from .device import (
    DeviceAdapter,
    DeviceError,
    DeviceFile,
    TransferAborted as DeviceTransferAborted,
    sanitize_device_filename,
)
from .events import (
    DownloadComplete,
    DownloadProgress,
    DownloadStarted,
    Error,
    EventBus,
    FileDiscovered,
    ScanComplete,
    ScanStarted,
    Severity,
    TransferAborted,
    UnknownsDetected,
    WhispersDetected,
)
from .jensen import HTAConverter
from .state import DeviceKey, StateStore, wav_duration_minutes


MAX_DEVICE_FILE_SIZE = 2 * 1024 * 1024 * 1024  # 2 GB hard ceiling (PRD §7)
SIZE_STABLE_DELAY_SECONDS = 1.0
HTA_EXTENSION = ".hda"
WAV_EXTENSION = ".wav"
MP3_EXTENSION = ".mp3"
MP3_SYNC_WORD = b"\xff\xfb"
# Sibling directory under the archive root for operator-offloaded whispers.
# Lazily created on first whisper offload; absence is normal.
WHISPERS_SUBDIR = "whispers"


class OffloadError(RuntimeError):
    pass


@dataclass(frozen=True)
class OffloadResult:
    device_filename: str
    archive_path: Path
    sha256: str
    size_bytes: int
    duration_minutes: float
    converted_from_hda: bool


@dataclass(frozen=True)
class ScanResult:
    """Classified scan output. Meetings auto-offload; whispers/unknowns
    require explicit operator action via the TUI.
    """

    meetings: List[DeviceFile]
    whispers: List[DeviceFile]
    unknowns: List[DeviceFile]


def _chunked_sha256_of_file(path: os.PathLike[str] | str, chunk: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        while True:
            data = fh.read(chunk)
            if not data:
                break
            h.update(data)
    return h.hexdigest()


def _detect_real_extension(staged_path: Path) -> str:
    """Sniff the first 2 bytes of a staged file to detect MP3-in-.hda-clothing.

    HiDock P1 stores recordings as MP3 with a `.hda` extension. Older H1
    models use a genuine proprietary HTA container. We check the sync word
    to tell them apart so the archive filename reflects the real format.
    """
    try:
        with open(staged_path, "rb") as f:
            header = f.read(2)
        if header == MP3_SYNC_WORD:
            return MP3_EXTENSION
    except OSError:
        pass
    return WAV_EXTENSION


def _archive_basename(device_mtime: Optional[datetime], fallback_mtime: Optional[datetime], ext: str = WAV_EXTENSION) -> str:
    """Return the `YYYY-MM-DD_HHMMSS.<ext>` basename.

    `device_mtime` wins; `fallback_mtime` (supplied by the caller, derived
    from the clock) kicks in when the device offered no metadata.
    """
    when = device_mtime or fallback_mtime
    if when is None:
        when = datetime.now()
    return f"{when.strftime('%Y-%m-%d_%H%M%S')}{ext}"


def _archive_subdir(when: datetime) -> Path:
    return Path(f"{when.year:04d}") / f"{when.month:02d}"


def _unique_archive_path(archive_dir: Path, basename: str, when: datetime) -> Path:
    """Return a collision-free archive path.

    Collisions happen when two recordings share a second-precision timestamp
    (rare, but the device does emit them). Append `-1`, `-2`, ... before the
    extension rather than overwriting an existing archive file.
    """
    subdir = archive_dir / _archive_subdir(when)
    subdir.mkdir(parents=True, exist_ok=True)
    candidate = subdir / basename
    if not candidate.exists():
        return candidate
    stem, ext = os.path.splitext(basename)
    i = 1
    while True:
        alt = subdir / f"{stem}-{i}{ext}"
        if not alt.exists():
            return alt
        i += 1


class Offloader:
    """Runs the size-stable -> download -> verify -> rename -> state pipeline.

    `cancel_event` is the signal that detach hand-off uses to abort an
    in-flight download cleanly. If the event is set mid-stream, the vendored
    jensen layer returns a non-"OK" status which we surface as
    `TransferAborted`.
    """

    def __init__(
        self,
        *,
        adapter: DeviceAdapter,
        store: StateStore,
        bus: EventBus,
        archive_dir: Path,
        tmp_dir: Path,
        delete_after_offload: bool,
        transcribe_on_offload: bool = True,
        hta_converter: Optional[HTAConverter] = None,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], datetime] = datetime.now,
    ):
        self._adapter = adapter
        self._store = store
        self._bus = bus
        self._archive_dir = archive_dir
        self._tmp_dir = tmp_dir
        self._delete_after_offload = delete_after_offload
        self._transcribe_on_offload = transcribe_on_offload
        self._hta = hta_converter
        self._sleep = sleep
        self._clock = clock

    def ensure_dirs(self) -> None:
        self._archive_dir.mkdir(parents=True, exist_ok=True)
        self._tmp_dir.mkdir(parents=True, exist_ok=True)

    def clean_stale_partials(self) -> int:
        """Remove any `*.partial` from a prior crashed run. Returns count removed."""
        if not self._tmp_dir.exists():
            return 0
        removed = 0
        for child in self._tmp_dir.iterdir():
            if child.suffix == ".partial" and child.is_file():
                try:
                    child.unlink()
                    removed += 1
                except OSError:
                    pass
        return removed

    def scan_new_files(self, device_key: DeviceKey) -> List[DeviceFile]:
        """DEPRECATED compatibility wrapper. Returns all stable-new files
        regardless of classification. Prefer `scan_pending_files` which
        partitions meetings from whispers/unknowns per PRD §2.3.

        Retained so pipeline-mechanics tests (size-stable, already-processed,
        collision, HTA conversion) remain anchored on their existing fixtures.
        The App uses `scan_pending_files` for production routing.
        """
        self._bus.publish(ScanStarted())
        snapshot_a = self._adapter.list_files()
        self._sleep(SIZE_STABLE_DELAY_SECONDS)
        snapshot_b = self._adapter.list_files()
        by_name_b = {f.name: f.size for f in snapshot_b}
        stable_new: List[DeviceFile] = []
        for file in snapshot_a:
            if self._store.is_processed(device_key, file.name):
                continue
            if file.size <= 0 or file.size > MAX_DEVICE_FILE_SIZE:
                continue
            size_b = by_name_b.get(file.name)
            if size_b is None or size_b != file.size:
                continue
            stable_new.append(file)
            self._bus.publish(FileDiscovered(device_filename=file.name, size_bytes=file.size))
        self._bus.publish(ScanComplete(new_file_count=len(stable_new)))
        return stable_new

    def scan_pending_files(self, device_key: DeviceKey) -> ScanResult:
        """Scan and classify. Meetings get the size-stable check (auto-offload
        candidates must be settled); whispers/unknowns skip it because the TUI
        operator sees them in a modal before any download begins -- a growing
        file that's still being recorded will not be selected in practice.

        Publishes `ScanStarted`, `ScanComplete(new_file_count=len(meetings))`,
        plus `WhispersDetected` / `UnknownsDetected` when either bucket is
        non-empty.
        """
        self._bus.publish(ScanStarted())
        snapshot_a = self._adapter.list_files()
        self._sleep(SIZE_STABLE_DELAY_SECONDS)
        snapshot_b = self._adapter.list_files()
        by_name_b = {f.name: f.size for f in snapshot_b}

        meetings: List[DeviceFile] = []
        whispers: List[DeviceFile] = []
        unknowns: List[DeviceFile] = []
        for file in snapshot_a:
            if self._store.is_processed(device_key, file.name):
                continue
            if file.size <= 0 or file.size > MAX_DEVICE_FILE_SIZE:
                continue
            kind = classify_recording(file.name)
            if kind is RecordingKind.MEETING:
                size_b = by_name_b.get(file.name)
                if size_b is None or size_b != file.size:
                    continue
                meetings.append(file)
                self._bus.publish(
                    FileDiscovered(device_filename=file.name, size_bytes=file.size)
                )
            elif kind is RecordingKind.WHISPER:
                whispers.append(file)
            else:
                unknowns.append(file)

        self._bus.publish(ScanComplete(new_file_count=len(meetings)))
        if whispers:
            self._bus.publish(WhispersDetected(count=len(whispers)))
        if unknowns:
            self._bus.publish(
                UnknownsDetected(
                    count=len(unknowns),
                    filenames=[f.name for f in unknowns],
                )
            )
        return ScanResult(meetings=meetings, whispers=whispers, unknowns=unknowns)

    def _effective_archive_dir(self, kind: RecordingKind) -> Path:
        """Meetings land at the archive root; whispers land under `whispers/`.

        Per PRD §2.4, the whispers sibling is lazily created. Unknowns never
        reach this path directly — the operator routes them as meeting or
        whisper via the TUI (`Offloader.offload_one`), which supplies the
        appropriate `kind`.
        """
        if kind is RecordingKind.WHISPER:
            return self._archive_dir / WHISPERS_SUBDIR
        return self._archive_dir

    def _ensure_whispers_dir(self) -> None:
        """Create `<archive>/whispers/`. Publishes an operator-actionable
        `Error` (PRD §2.7) when the mkdir fails — e.g., archive root on a
        read-only mount. Caller re-raises so the pipeline aborts cleanly.
        """
        target = self._archive_dir / WHISPERS_SUBDIR
        try:
            target.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            message = (
                f"Cannot create whispers archive at {target}: {exc}. "
                "Check that the archive directory is writable (e.g., it may "
                "be on a read-only mount or owned by another user)."
            )
            self._bus.publish(
                Error(message=message, severity=Severity.ERROR, context="archive_setup")
            )
            raise OffloadError(message) from exc

    def offload(
        self,
        *,
        device_key: DeviceKey,
        file: DeviceFile,
        cancel_event: Optional[threading.Event] = None,
        kind: RecordingKind = RecordingKind.MEETING,
    ) -> OffloadResult:
        """Pull one file. Raises `TransferAborted`-derived errors on cancel/detach,
        `OffloadError` on verify/convert failures. State/filesystem stay clean on failure.

        `kind` controls archive routing (meetings → `<archive>/`, whispers →
        `<archive>/whispers/`) and the ledger `kind` field. Defaults to
        MEETING because auto-offload is the common case; explicit-intent
        callers (TUI whisper selector, unknown prompt) supply the kind.
        """
        device_filename = sanitize_device_filename(file.name)
        if file.size > MAX_DEVICE_FILE_SIZE:
            raise OffloadError(f"Device-reported size exceeds {MAX_DEVICE_FILE_SIZE}-byte ceiling: {file.size}")

        self.ensure_dirs()
        if kind is RecordingKind.WHISPER:
            self._ensure_whispers_dir()
        effective_archive_dir = self._effective_archive_dir(kind)
        is_hda = device_filename.lower().endswith(HTA_EXTENSION)

        # Staging: download raw bytes to a .partial under .tmp/.
        staged_name = f"{device_filename}.partial"
        staged = self._tmp_dir / staged_name
        if staged.exists():
            staged.unlink()

        # Archive extension is determined after download by sniffing the
        # header — HiDock P1 writes MP3 with .hda extension; older H1
        # uses a genuine HTA container that needs conversion to .wav.
        # We use .wav as a placeholder here; the real extension is set
        # after the format-detection step below.
        basis_time = file.device_mtime or self._clock()
        target_basename = _archive_basename(file.device_mtime, basis_time)
        target_path = _unique_archive_path(effective_archive_dir, target_basename, basis_time)

        self._bus.publish(DownloadStarted(device_filename=device_filename, target_path=str(target_path)))

        sha = hashlib.sha256()
        bytes_written = 0

        def on_chunk(chunk: bytes) -> None:
            nonlocal bytes_written
            staged_fh.write(chunk)
            sha.update(chunk)
            bytes_written += len(chunk)

        def on_progress(done: int, total: int) -> None:
            self._bus.publish(DownloadProgress(device_filename=device_filename, bytes_done=done, bytes_total=total))

        try:
            with open(staged, "wb") as staged_fh:
                try:
                    self._adapter.download_file(
                        device_filename,
                        file.size,
                        on_chunk=on_chunk,
                        on_progress=on_progress,
                        cancel_event=cancel_event,
                    )
                finally:
                    staged_fh.flush()
                    os.fsync(staged_fh.fileno())
        except (DeviceTransferAborted, DeviceError, OSError) as exc:
            self._safe_unlink(staged)
            reason = getattr(exc, "reason", None) or str(exc) or exc.__class__.__name__
            self._bus.publish(TransferAborted(device_filename=device_filename, reason=reason))
            raise DeviceTransferAborted(reason) from exc

        # Verify size and SHA against what we just streamed.
        actual_size = staged.stat().st_size
        if actual_size != file.size:
            self._safe_unlink(staged)
            reason = f"size mismatch (expected {file.size}, got {actual_size})"
            self._bus.publish(TransferAborted(device_filename=device_filename, reason=reason))
            raise OffloadError(reason)
        streamed_sha = sha.hexdigest()
        disk_sha = _chunked_sha256_of_file(staged)
        if streamed_sha != disk_sha:
            self._safe_unlink(staged)
            reason = "sha mismatch between stream and disk"
            self._bus.publish(TransferAborted(device_filename=device_filename, reason=reason))
            raise OffloadError(reason)

        # .hda handling: sniff the staged bytes to detect the real format.
        # HiDock P1 stores MP3 with .hda extension — archive as .mp3.
        # Older H1 models use a genuine HTA container → convert to .wav.
        converted_from_hda = False
        if is_hda:
            real_ext = _detect_real_extension(staged)
            if real_ext == MP3_EXTENSION:
                # MP3-in-.hda-clothing — just fix the extension, no conversion.
                new_basename = _archive_basename(file.device_mtime, basis_time, ext=MP3_EXTENSION)
                target_path = _unique_archive_path(effective_archive_dir, new_basename, basis_time)
            else:
                # Genuine HTA container — convert to WAV.
                converter = self._hta or HTAConverter()
                try:
                    converted = converter.convert_hta_to_wav(str(staged))
                except Exception as exc:  # noqa: BLE001
                    self._safe_unlink(staged)
                    reason = f"hta conversion failed: {exc}"
                    self._bus.publish(TransferAborted(device_filename=device_filename, reason=reason))
                    raise OffloadError(reason) from exc
                if not converted or not Path(converted).exists():
                    self._safe_unlink(staged)
                    reason = "hta conversion produced no output"
                    self._bus.publish(TransferAborted(device_filename=device_filename, reason=reason))
                    raise OffloadError(reason)
                try:
                    self._safe_unlink(staged)
                    converted_path = Path(converted)
                    new_staged = self._tmp_dir / (device_filename + ".wav.partial")
                    if new_staged.exists():
                        new_staged.unlink()
                    shutil.move(str(converted_path), str(new_staged))
                    staged = new_staged
                except OSError as exc:
                    self._safe_unlink(staged)
                    reason = f"hta conversion rename failed: {exc}"
                    self._bus.publish(TransferAborted(device_filename=device_filename, reason=reason))
                    raise OffloadError(reason) from exc
                disk_sha = _chunked_sha256_of_file(staged)
                converted_from_hda = True

        # Atomic rename into the archive.
        try:
            os.replace(staged, target_path)
        except OSError as exc:
            self._safe_unlink(staged)
            reason = f"archive rename failed: {exc}"
            self._bus.publish(TransferAborted(device_filename=device_filename, reason=reason))
            raise OffloadError(reason) from exc

        # Derive duration from audio header. wav_duration_minutes handles WAV
        # only; MP3s return 0.0 (duration is computed from bitrate, not a
        # simple header read — acceptable for MVP; cumulative minutes will
        # undercount MP3 sources).
        duration_minutes = wav_duration_minutes(target_path)

        self._store.record_processed(
            device_key,
            device_filename=device_filename,
            size_bytes=target_path.stat().st_size,
            device_mtime=file.device_mtime.isoformat() if file.device_mtime else None,
            sha256=disk_sha,
            archive_path=str(target_path.relative_to(self._archive_dir)),
            device_deleted=False,
            duration_minutes=duration_minutes,
            kind=kind,
        )

        # Device-side deletion only applies to meetings. Whispers and unknown-
        # routed files stay on the device even after being pulled — the
        # operator's mental model is "auto-offload is the only destructive
        # side of this tool."
        if self._delete_after_offload and kind is RecordingKind.MEETING:
            try:
                self._adapter.delete_file(device_filename)
                self._mark_device_deleted(device_key, device_filename)
            except DeviceError:
                # Leave archive intact; retries happen naturally on next scan.
                pass

        self._bus.publish(
            DownloadComplete(
                device_filename=device_filename,
                archive_path=str(target_path),
                sha256=disk_sha,
            )
        )

        if self._transcribe_on_offload:
            from .transcribe import transcribe_file as _transcribe
            _transcribe(
                target_path,
                self._archive_dir,
                bus=self._bus,
                device_filename=device_filename,
            )

        return OffloadResult(
            device_filename=device_filename,
            archive_path=target_path,
            sha256=disk_sha,
            size_bytes=target_path.stat().st_size,
            duration_minutes=duration_minutes,
            converted_from_hda=converted_from_hda,
        )

    def offload_one(
        self,
        *,
        device_key: DeviceKey,
        file: DeviceFile,
        kind: RecordingKind,
        cancel_event: Optional[threading.Event] = None,
    ) -> OffloadResult:
        """Explicit-intent entry point used by the TUI whisper selector and
        unknown-router. Identical pipeline to `offload()`; the distinction is
        that the caller declares the classification rather than inheriting
        the MEETING default.
        """
        return self.offload(
            device_key=device_key,
            file=file,
            cancel_event=cancel_event,
            kind=kind,
        )

    def _mark_device_deleted(self, device_key: DeviceKey, device_filename: str) -> None:
        with self._store._mutate() as data:  # type: ignore[attr-defined]
            device = data["devices"].get(device_key.namespaced)
            if device and device_filename in device.get("processed_recordings", {}):
                device["processed_recordings"][device_filename]["device_deleted"] = True

    @staticmethod
    def _safe_unlink(path: os.PathLike[str] | str) -> None:
        try:
            Path(path).unlink()
        except FileNotFoundError:
            pass
        except OSError:
            pass
