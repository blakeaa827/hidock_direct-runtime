#!/usr/bin/env python3
"""Import an audio file into the archive and transcribe it, exactly as an offload would.

Built to be called from Audio Hijack's "Run Shell Script" block, which passes the
recorded file paths as arguments:

    /path/to/hidock_direct-runtime/.venv/bin/python \\
        /path/to/hidock_direct-runtime/scripts/transcribe_file.py "$@"

For each file it:

1. Moves it into the archive as `YYYY/MM/YYYY-MM-DD_HHMMSS.<ext>` (`--keep-source`
   copies instead). The original is deleted only after the archived copy is
   complete and its size matches, or when identical bytes are already archived.
   The name is
   taken from the recording START. That basename is what the ledger and the
   transcript frontmatter read the recording time back out of.
2. Runs the same `transcribe_file` bridge the offload worker calls, so the
   ledger entry, the `.md` and the `.aai.json` land where an offload's would.

Guards, each because the alternative costs money or loses a ledger entry:

* **Same bytes already archived -> reuse that path, and never pay twice.**
  `process_file` does not consult the ledger; it always submits. So a file whose
  ledger entry is `done` is skipped here, and one with a saved `.aai.json` is
  re-rendered from it for free.
* **`.wav` is refused.** The archive is the diarize pipeline's scan root and it
  scans for `.wav`; a WAV there is a duplicate paid transcription waiting to
  happen. Record MP3 or AAC in Audio Hijack instead.
* **The app's single-instance lock is taken first.** The transcription ledger
  has no cross-process locking, so running beside hidock-direct could drop one
  writer's entry. If the app is running the script waits (`--wait`, default 30
  min) and then fails loudly rather than racing it.

Every outcome is written to `<archive>/.state/transcribe_file.log` and shown as
a macOS notification, because Audio Hijack does not surface script output.

Exit status: 0 all files transcribed (or already done), 1 any failure, 2 usage.
"""

from __future__ import annotations

import argparse
import logging
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from hidock_direct.config import load_config, load_env_file_into_environ  # noqa: E402
from hidock_direct.events import (  # noqa: E402
    EventBus,
    TranscribeComplete,
    TranscribeFailed,
    TranscribeSkipped,
)
from hidock_direct.locks import FileLock, LockHeld  # noqa: E402
from hidock_direct.offload import (  # noqa: E402
    ARCHIVE_BASENAME_FORMAT,
    _archive_subdir,
    _chunked_sha256_of_file,
    _unique_archive_path,
)

log = logging.getLogger("transcribe_file")

REFUSED_SUFFIXES = {".wav"}
AUDIO_SUFFIXES = {".mp3", ".m4a", ".aac", ".flac", ".ogg", ".opus"}
LOCK_POLL_SECONDS = 15


class ImportRefused(Exception):
    """The file cannot be imported; the message says why and what to do."""


def recording_start(path: Path) -> datetime:
    """When the recording started. A finished recording's mtime is its END, so
    prefer the birth time (macOS sets it when Audio Hijack creates the file)."""
    st = path.stat()
    born = getattr(st, "st_birthtime", None)
    return datetime.fromtimestamp(born if born else st.st_mtime)


def import_into_archive(src: Path, archive_dir: Path, when: datetime, *, keep_source: bool = False) -> tuple[Path, bool]:
    """Move (or copy) `src` into the archive. Returns (archive_path, already_present)."""
    if src.suffix.lower() in REFUSED_SUFFIXES:
        raise ImportRefused(
            f"{src.name}: .wav is refused -- the archive is the pipeline's scan root "
            "and a WAV there gets transcribed twice. Record MP3 or AAC instead."
        )
    if src.suffix.lower() not in AUDIO_SUFFIXES:
        raise ImportRefused(f"{src.name}: not an audio type this accepts ({', '.join(sorted(AUDIO_SUFFIXES))})")
    if not src.is_file() or src.stat().st_size == 0:
        raise ImportRefused(f"{src.name}: missing or empty")

    ext = src.suffix.lower()
    digest = _chunked_sha256_of_file(src)
    subdir = archive_dir / _archive_subdir(when)
    stem = when.strftime(ARCHIVE_BASENAME_FORMAT)
    if subdir.is_dir():
        for existing in sorted(subdir.glob(f"{stem}*{ext}")):
            if existing.stat().st_size == src.stat().st_size and _chunked_sha256_of_file(existing) == digest:
                if not keep_source:
                    src.unlink()
                return existing, True

    dest = _unique_archive_path(archive_dir, f"{stem}{ext}", when)
    partial = dest.with_name(dest.name + ".partial")
    # Copy-then-delete rather than rename: the archive is usually a cloud-storage
    # mount on another volume, and a half-finished cross-device move must never
    # leave the recording in neither place.
    shutil.copy2(src, partial)
    if partial.stat().st_size != src.stat().st_size:
        partial.unlink()
        raise OSError(f"{src.name}: archived copy is short; original kept")
    os.replace(partial, dest)
    if not keep_source:
        src.unlink()
    return dest, False


def acquire_app_lock(lock: FileLock, wait_seconds: float, sleep=time.sleep, clock=time.monotonic) -> None:
    deadline = clock() + wait_seconds
    while True:
        try:
            lock.acquire()
            return
        except LockHeld:
            if clock() >= deadline:
                raise
            sleep(LOCK_POLL_SECONDS)


def notify(title: str, message: str) -> None:
    try:
        subprocess.run(
            ["osascript", "-e", f"display notification {_as_applescript(message)} with title {_as_applescript(title)}"],
            check=False, capture_output=True, timeout=10,
        )
    except Exception:  # a notification is a convenience; the log is the record
        pass


def _as_applescript(text: str) -> str:
    return '"' + text.replace("\\", "\\\\").replace('"', '\\"') + '"'


def ledger_status(path: Path, archive_dir: Path) -> Optional[str]:
    from hidock_direct.retry import load_diarize_state

    entry = load_diarize_state(archive_dir).get(str(path.relative_to(archive_dir))) or {}
    return entry.get("status")


def transcribe_one(path: Path, archive_dir: Path, *, reuse: bool, transcribe=None, status=ledger_status) -> tuple[bool, str]:
    """Run the offload bridge on one archived file. Returns (ok, detail)."""
    if status(path, archive_dir) == "done":
        return True, "already transcribed (ledger says done); nothing billed"
    reuse = reuse or (path.parent / f"{path.stem}.aai.json").is_file()
    if transcribe is None:
        from hidock_direct.transcribe import transcribe_file as transcribe
    terminal: list = []
    bus = EventBus()
    bus.subscribe(lambda e: terminal.append(e) if isinstance(e, (TranscribeComplete, TranscribeFailed, TranscribeSkipped)) else None)
    transcribe(path, archive_dir, bus=bus, device_filename=path.name, recorded_at=None,
               reuse_existing_transcript=reuse)
    event = terminal[-1] if terminal else None
    if isinstance(event, TranscribeComplete):
        return True, "re-rendered from the saved transcript" if reuse else "transcribed"
    if isinstance(event, TranscribeFailed):
        return False, event.reason
    if isinstance(event, TranscribeSkipped):
        return False, f"skipped: {event.reason}"
    return False, "no terminal event from the pipeline"


def main(argv: Optional[list[str]] = None, *, transcribe=None, status=ledger_status) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("files", nargs="+", type=Path)
    parser.add_argument("--wait", type=float, default=1800,
                        help="seconds to wait for hidock-direct to release its lock (default 1800)")
    parser.add_argument("--reuse", action="store_true",
                        help="re-render from an existing paid transcript instead of paying again")
    parser.add_argument("--dry-run", action="store_true",
                        help="report where each file would go; move and transcribe nothing")
    parser.add_argument("--keep-source", action="store_true",
                        help="copy into the archive and leave the original in place")
    parser.add_argument("--no-notify", action="store_true")
    args = parser.parse_args(argv)

    load_env_file_into_environ()
    config = load_config()
    archive_dir = config.archive_dir.expanduser()
    config.state_dir.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.FileHandler(config.state_dir / "transcribe_file.log"), logging.StreamHandler()],
    )
    say = (lambda t, m: None) if args.no_notify else notify

    if args.dry_run:
        for src in args.files:
            when = recording_start(src)
            if src.suffix.lower() not in AUDIO_SUFFIXES:
                print(f"{src} -> REFUSED (not {', '.join(sorted(AUDIO_SUFFIXES))})")
                continue
            print(f"{src} -> {archive_dir / _archive_subdir(when) / when.strftime(ARCHIVE_BASENAME_FORMAT)}{src.suffix.lower()}")
        return 0

    lock = FileLock(config.lock_path)
    try:
        acquire_app_lock(lock, args.wait)
    except LockHeld:
        msg = f"hidock-direct is running and held its lock for {args.wait:.0f}s; quit it and re-run"
        log.error("%s: %s", msg, ", ".join(str(f) for f in args.files))
        say("HiDock transcription not started", msg)
        return 1

    failures = 0
    try:
        for src in args.files:
            src = src.expanduser().resolve()
            try:
                dest, present = import_into_archive(src, archive_dir, recording_start(src), keep_source=args.keep_source)
            except (ImportRefused, OSError) as exc:
                failures += 1
                log.error("refused %s: %s", src, exc)
                say("HiDock transcription refused", str(exc))
                continue
            log.info("%s %s -> %s", "reusing" if present else "imported", src, dest)
            ok, detail = transcribe_one(dest, archive_dir, reuse=args.reuse, transcribe=transcribe, status=status)
            if ok:
                log.info("done %s: %s", dest.relative_to(archive_dir), detail)
                say("HiDock transcription done", f"{dest.name}: {detail}")
            else:
                failures += 1
                log.error("failed %s: %s", dest.relative_to(archive_dir), detail)
                say("HiDock transcription FAILED", f"{dest.name}: {detail}")
    finally:
        lock.release()
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
