"""Live session archival — the recording we make ourselves, and the transcript
the emitter renders from it.

Phase 1d. PRD: ``projects/hidock_direct/planning/live_archive_prd.md``.

This module closes a data-loss defect that phases 1a–1c introduced. Pressing `l`
puts the device into realtime mode, and the operator verified on hardware
(2026-08-27, power cycle + rescan) that the device **stops its own recording and
never persists the live-session audio**. So a live call used to buy a transcript
on screen at the cost of the recording. Nobody chose that trade.

Four properties shape everything below.

1. **We open the file, so every time in the transcript is arithmetic.**
   `Frame` is already 16 kHz 16-bit stereo and already de-interleaved, so the
   frames go straight into a WAV as they arrive (stdlib `wave`, no new
   dependency). A turn's start time is then *frames written before it arrived,
   divided by the frame rate* — a fact about a file we opened — rather than a
   wall-clock reading correlated against a recording somebody else started, with
   the start-offset skew that correlation would have to correct for.

2. **The transcript format belongs to `diarize_audio.render.render_markdown`.**
   It is vendored into this runtime, so it is importable, so nothing forces a
   re-implementation — and a renderer written here would be a second copy of a
   convention another artifact owns, drifting the first time that artifact
   changed. Live turns are assembled into the AAI-response shape the emitter
   consumes, and names reach the document through the emitter's own
   `speaker_names` parameter. Never by string surgery on rendered markdown:
   that would re-derive the first-appearance numbering the emitter already did
   internally, which is the same second implementation wearing a disguise.

3. **An unnamed speaker must render with a DIGIT.** The consumer is
   `personal_assistant/execution/speaker_id_daemon.py:364`, which skips any
   transcript lacking ``\\*\\*Speaker \\d+\\*\\*`` and logs it "already tagged".
   A document full of real names is *correctly* skipped — those people are
   identified. But the live surface renders an unnamed far label as
   ``Speaker A``, a LETTER: written to the archive verbatim it fails that
   regex, the call is counted "already tagged", and a speaker the operator never
   named is identified by nothing at all, live or later. So the near channel
   renders as the operator's name, a named far label as its name, and an
   unnamed far label is simply **left out of the name map** — the emitter then
   emits its own ``Speaker {num}``, which is a digit by construction. This
   module never mints a speaker label of its own.

4. **The archive's format is MP3, so ours is too when an encoder exists**
   (§2.2b, measured not assumed). The device's own files run 96 kbps / 43 MB per
   hour; our 16 kHz stereo PCM is 512 kbps / 230 MB per hour — 5.3x larger, into
   a Drive-synced folder, so it costs storage on two machines and the bandwidth
   between them. It is also a correctness matter: the archive holds 795 `.mp3`
   and zero `.wav`, and the pipeline hardcodes ``<stem>.mp3`` in five places
   including `auto_speaker_id.py:3869`, so a `.wav` is a live call that
   voice-print matching can never locate — exactly the capability property 3
   exists to preserve. The WAV is therefore an intermediate: recorded
   incrementally as the call runs, transcoded once at stop, and deleted only
   after the MP3 exists and is non-empty. `afconvert` is deliberately not in the
   encoder list — macOS decodes MP3 without encoding it, and `afconvert` LISTS
   `.mp3` among its data formats while failing with ``('cfmt') failed ('fmt?')``
   when asked to write one (PRD §2.2b; do not re-derive this from its `-hf`
   output).

Ownership boundaries this module respects:

* The offload ledger is not ours to touch in either direction. `is_processed`
  gates the DOWNLOAD, so a pre-seeded entry would skip the download and lose the
  device's own recording — trading a double-billing bug for a data-loss bug, in
  a module whose entire premise is that live audio was being lost (FR-3.2).
* Naming and pathing are imported from `offload`, never restated. A second copy
  of the strftime format is a second implementation of the archive's naming
  convention, and the `-1` collision suffix is likewise offload's to mint —
  which is why the sidecars ask `_unique_archive_path` for their names too
  rather than assuming the audio's de-confliction covered them.
* No audio bytes and no transcript text reach a log record or an `Error`
  message (NFR-3). This module handles both halves of a private conversation.
"""

from __future__ import annotations

import errno
import json
import logging
import os
import shutil
import subprocess
import wave
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import RLock
from typing import Callable, Dict, List, Optional, Sequence, Tuple

from diarize_audio.render import render_markdown

from .events import (
    Error,
    EventBus,
    LiveChannel,
    LiveSpeakerRevision,
    LiveTurn,
    Severity,
)
from .offload import (
    MP3_EXTENSION,
    WAV_EXTENSION,
    _archive_basename,
    _unique_archive_path,
)
from .realtime import CHANNELS, SAMPLE_RATE_HZ, Frame

log = logging.getLogger(__name__)

BYTES_PER_SAMPLE = 2
BYTES_PER_WAV_FRAME = CHANNELS * BYTES_PER_SAMPLE

# Errors from this module go to the live surface, which forwards only
# `context == "live"` — anything else never reaches the window the operator is
# looking at during a call.
ERROR_CONTEXT = "live"

TRANSCRIPT_SUFFIX = ".md"
RAW_RESPONSE_SUFFIX = ".aai.json"
_PARTIAL_SUFFIX = ".tmp"

# The archive's own bitrate, read off the device's files (PRD §2.2b): 769
# recordings, 382 hours, 96 kbps / 43 MB per hour. Matching it keeps a live call
# the same size as a batched one rather than making a new size class.
MP3_BITRATE_KBPS = 96
# Floor and slope for the transcode's timeout. Both encoders run tens of times
# faster than real time, so half the recording's duration is a wide margin and
# still bounds `stop()` on a hung child rather than hanging the teardown.
_ENCODE_TIMEOUT_FLOOR_S = 60.0
_ENCODE_TIMEOUT_PER_AUDIO_SECOND = 0.5

# Identifier prefix for the `assemblyai_id` frontmatter field. See
# `_transcript_id` for why the field carries this rather than a provider id.
LIVE_ID_PREFIX = "live-"

# Provider-shaped speaker keys for the assembled response. The two ends are
# distinct speakers by construction — the near channel is the operator and the
# far channel is everyone else — so they must never collapse onto one key just
# because the provider happened to call a far speaker by the same letter.
# These strings are internal: the emitter maps them to display labels and they
# never appear in the document.
NEAR_SPEAKER_KEY = "near"
_FAR_SPEAKER_PREFIX = "far-"
_UNLABELLED_FAR_SPEAKER_KEY = "far-unlabelled"


@dataclass
class _Turn:
    """One live turn, held by its `(channel, turn_order)` identity.

    `start_ms` is captured once, when the turn is FIRST seen. The bridge emits a
    partial as soon as a speaker begins and a final when they stop, so taking
    the final's arrival would time every turn at its end.
    """

    channel: LiveChannel
    turn_order: int
    start_ms: int
    text: str
    label: Optional[str]


def _lame_command(executable: str, source: Path, target: Path) -> List[str]:
    return [
        executable,
        "--quiet",
        "-b",
        str(MP3_BITRATE_KBPS),
        str(source),
        str(target),
    ]


def _ffmpeg_command(executable: str, source: Path, target: Path) -> List[str]:
    # `-f` is given on BOTH sides because the target is a temp name that does
    # not end in `.mp3` (an interrupted encode must never leave something the
    # pipeline would index as a recording), and ffmpeg picks its muxer from the
    # extension unless told otherwise.
    return [
        executable,
        "-nostdin",
        "-loglevel",
        "error",
        "-y",
        "-f",
        "wav",
        "-i",
        str(source),
        "-codec:a",
        "libmp3lame",
        "-b:a",
        f"{MP3_BITRATE_KBPS}k",
        "-f",
        "mp3",
        str(target),
    ]


# In preference order (FR-2.2b). Presence-detected via `shutil.which` and never
# a pip dependency: this is a public clone-and-run app, and an encoder is a
# system tool the operator may or may not have. Absence is a supported
# configuration, not a failure (FR-2.2e).
_ENCODERS: Sequence[Tuple[str, Callable[[str, Path, Path], List[str]]]] = (
    ("lame", _lame_command),
    ("ffmpeg", _ffmpeg_command),
)


def _find_encoder() -> Optional[Tuple[str, str, Callable[[str, Path, Path], List[str]]]]:
    """The first available encoder as `(name, executable, command builder)`."""
    for name, builder in _ENCODERS:
        executable = shutil.which(name)
        if executable:
            return name, executable, builder
    return None


def _interleave(near: bytes, far: bytes) -> bytes:
    """Interleave two mono s16le buffers into one stereo s16le buffer.

    Channel 1 is the near end, channel 2 the far end — the separation the batch
    path never had, and the reason speaker attribution on this path is
    structural rather than inferred. Done by byte-slice assignment rather than
    `struct.unpack`/`pack` so the hot capture loop (~10 chunks/sec) is not
    parsing every sample twice.

    Lengths are normalised to a whole number of samples and zero-padded to the
    longer of the two, so a short buffer costs silence in one ear rather than
    dropping the audio in the other.
    """
    width = max(len(near), len(far))
    width -= width % BYTES_PER_SAMPLE
    if width == 0:
        return b""
    near = near[:width].ljust(width, b"\x00")
    far = far[:width].ljust(width, b"\x00")
    out = bytearray((width // BYTES_PER_SAMPLE) * BYTES_PER_WAV_FRAME)
    out[0::4] = near[0::2]
    out[1::4] = near[1::2]
    out[2::4] = far[0::2]
    out[3::4] = far[1::2]
    return bytes(out)


def _speaker_key(channel: LiveChannel, label: Optional[str]) -> str:
    if channel == LiveChannel.NEAR:
        return NEAR_SPEAKER_KEY
    if not label:
        return _UNLABELLED_FAR_SPEAKER_KEY
    return _FAR_SPEAKER_PREFIX + label


def _safe_unlink(path: Optional[Path]) -> None:
    if path is None:
        return
    try:
        path.unlink()
    except OSError:
        pass


def _atomic_write_text(path: Path, text: str) -> None:
    """Write `text` to `path` via a neighbouring temp file and `os.replace`.

    The temp name deliberately does not end in the target's suffix, so a failed
    write never leaves something the pipeline would index as a transcript.

    `path` is chosen by `_sidecar_target`, which only ever hands back a name
    nothing occupies — this function will happily replace whatever is there, so
    it must never be pointed at a file this session did not choose.
    """
    tmp = path.with_name(path.name + _PARTIAL_SUFFIX)
    try:
        with open(tmp, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(text)
            handle.flush()
        os.replace(tmp, path)
    except OSError:
        _safe_unlink(tmp)
        raise


class LiveArchive:
    """Records a live session to the archive and renders its transcript at stop.

    Driven exactly as PRD §7 drives it::

        with LiveArchive(archive_dir, bus=bus, operator_name=name) as rec:
            for frame in capture.frames():
                rec.write(frame)

    `names` is a zero-argument callable returning the surface's `label -> name`
    map, and it is read ONCE, at stop. That is what lets a name typed in the
    last minute of a call reach lines rendered in the first — which is when
    names are usually typed, because the operator works out who is speaking well
    after that person's first sentence.

    `clock` is read once, when the first real audio arrives: it is the
    recording's start, and both the basename and `recorded_at` come from it.
    `recorded_at` is therefore DIRECT on this path (§11) — the proxy that
    shipped across 670 batched transcripts does not recur here, because we start
    the recording ourselves.
    """

    def __init__(
        self,
        archive_dir: Path,
        *,
        bus: EventBus,
        operator_name: str,
        names: Optional[Callable[[], Mapping[str, str]]] = None,
        clock: Callable[[], datetime] = datetime.now,
    ):
        self._archive_dir = Path(archive_dir)
        self._bus = bus
        self._operator_name = operator_name or ""
        self._names = names
        self._clock = clock

        # Guards the turn table, the sample counter and the writer. Nothing
        # publishes to the bus while holding it: `EventBus.publish` calls
        # subscribers under its own lock, and this object IS a subscriber, so
        # publishing from inside the lock would invert the acquisition order
        # between the capture thread and the transcriber thread.
        self._lock = RLock()
        # Separate from `_lock` and held across the whole of `stop()`'s durable
        # section, so a second `stop()` BLOCKS until the artifacts are on disk
        # instead of returning while the first is still transcoding.
        self._finalise_lock = RLock()

        self._turns: Dict[Tuple[str, int], _Turn] = {}
        self._samples_written = 0
        self._handle = None
        self._writer: Optional[wave.Wave_write] = None
        # The file we OPENED, and the clock reading taken when we opened it.
        # Neither is a recording yet — see `_commit_locked`.
        self._pending_path: Optional[Path] = None
        self._pending_started_at: Optional[datetime] = None
        # The recording that EXISTS: set once audio has actually landed on
        # disk, and moved to the `.mp3` once the transcode has succeeded.
        self._audio_path: Optional[Path] = None
        self._started_at: Optional[datetime] = None
        self._transcript_path: Optional[Path] = None
        self._recording_stopped = False
        self._subscribed = False
        self._stopped = False

    # -- public surface ---------------------------------------------------

    @property
    def audio_path(self) -> Optional[Path]:
        """The surviving recording, or None until audio has actually landed.

        This is the file that EXISTS, which is not the same as the file we
        opened. A `wave` handle opened at the first frame and never successfully
        written leaves a 0-byte file, and `wave.open` on one of those raises
        `EOFError` — a recording that never happened, with a transcript beside
        it describing it and the controller announcing it as saved (FR-1.5).
        So the path only appears here once a write has succeeded.

        After a successful transcode it names the `.mp3`; when no encoder is
        installed, or the transcode failed, it names the `.wav` that was kept
        instead (FR-2.2d/e). Either way it names a file that is really there,
        which is what `LiveSessionController._finalise_archive` reads to decide
        whether a session can be announced as saved and under what name.
        """
        return self._audio_path

    @property
    def wav_path(self) -> Optional[Path]:
        """The surviving recording. Retained spelling of `audio_path`.

        The archived artifact is a `.mp3` whenever an encoder is installed, so
        the name is now historical — but it is the spelling
        `live_server.LiveSessionController` reads, and a property that named the
        intermediate WAV would name a file this module has already deleted.
        """
        return self._audio_path

    @property
    def transcript_path(self) -> Optional[Path]:
        """The transcript, or None until stop has written it."""
        return self._transcript_path

    def __enter__(self) -> "LiveArchive":
        with self._lock:
            already = self._subscribed or self._stopped
            if not already:
                self._subscribed = True
        if not already:
            self._bus.subscribe(self._on_event)
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        self.stop()
        return False

    def write(self, frame: Frame) -> None:
        """Append one captured frame to the recording. Never raises.

        A frame carrying no audio is not a recording (FR-1.5), so the WAV is
        opened lazily on the first frame that has bytes: pressing `l` and
        pressing it again is an ordinary thing to do, and it must not leave a
        zero-frame file in the archive, which is indistinguishable from a
        recording of silence.

        An archive that cannot be written costs the RECORDING, never the
        SESSION (FR-ERR-1/2) — the transcript on screen is still worth having —
        so failures are announced once and recording is stopped, rather than
        propagating out of here and unwinding the capture loop.
        """
        message: Optional[str] = None
        with self._lock:
            if self._stopped or self._recording_stopped:
                return
            payload = _interleave(frame.near, frame.far)
            if not payload:
                return
            if self._writer is None:
                message = self._open_locked()
            if message is None:
                message = self._append_locked(payload)
        if message is not None:
            self._publish(message, Severity.ERROR)

    def stop(self) -> None:
        """Finalise the recording, transcode it, then render and write the
        transcript. Idempotent.

        Stop arrives up to three times on an ordinary session — the `l`
        keystroke, `__exit__`, then the shutdown handler — and each extra render
        would rewrite a file the Drive mount may already have uploaded, from
        state a previous teardown has dismantled.

        The transcode happens HERE and never inline (FR-2.2c): the capture loop
        is fed ~10 chunks/sec and must not wait on a subprocess. By this point
        the loop has ended and the writer is closed, so the WAV is a finished
        file rather than one still being appended to.

        The transcript is written HERE and only here (FR-2.5): a partial
        transcript in the archive is indexed by the pipeline as a complete one,
        and the call is then permanently represented by its first few minutes.
        """
        # `_finalise_lock` is held across the whole durable section, and is NOT
        # `_lock`. The distinction is the entire point: `stop()` used to take
        # `_lock` only long enough to set `_stopped`, then transcode, render and
        # write the transcript OUTSIDE it. A second caller hit `if self._stopped:
        # return` and came straight back while the first was still working — so
        # `stop()` was idempotent by RETURNING rather than by BLOCKING.
        #
        # Two things went wrong with that, both silent. The durable work runs on
        # the daemon pump thread, so on app shutdown `main()` returned, the
        # interpreter exited, and the thread was killed mid-finalisation: no
        # transcript at all, for a call the device kept no recording of. And the
        # controller's teardown went on to clear the surface's name map while
        # the render had not happened yet, so a speaker the operator NAMED
        # rendered as `**Speaker 1**`.
        #
        # Blocking here makes "stop() returned" mean "the artifacts are on disk".
        with self._finalise_lock:
            with self._lock:
                if self._stopped:
                    return
                self._stopped = True
                turns = list(self._turns.values())
                samples = self._samples_written
                audio_path = self._audio_path
                started_at = self._started_at
                self._close_writer_locked()

            # Snapshot the operator's names BEFORE anything downstream can clear
            # them. Blocking above already orders this correctly, but the map is
            # the operator's own typed input and is not reconstructible, so it
            # does not depend on that ordering alone.
            names_snapshot = self._speaker_names(turns) if turns else {}

            if self._subscribed:
                self._bus.unsubscribe(self._on_event)
                self._subscribed = False

            if audio_path is None or started_at is None:
                # No audio ever reached the disk — an empty session, or an
                # archive that could not be written and has already said so.
                # Either way there is no recording for a transcript to describe
                # (FR-1.5), and `audio_path` stays None so nothing announces a
                # saved session.
                return

            audio_path, notice = self._finalise_audio(audio_path, started_at, samples)
            with self._lock:
                self._audio_path = audio_path
            if notice is not None:
                self._publish(notice, Severity.WARNING)

            if not turns:
                # Audio but no turns: nobody spoke, or the bridge produced
                # nothing. The recording is the artifact that cannot be
                # reconstructed and it is kept; an empty transcript beside it
                # would assert that the call was transcribed and stop anything
                # from ever revisiting the audio.
                log.info("live: no turns to render for the recording just archived")
                return
            self._write_transcript(
                audio_path, started_at, turns, samples, names=names_snapshot
            )

    # -- bus --------------------------------------------------------------

    def _on_event(self, event) -> None:
        """Bus subscriber. Records turns; never publishes, never raises.

        Called by `EventBus.publish` under the bus's own lock, so this path must
        not touch the bus again — see `_lock`.
        """
        if isinstance(event, LiveTurn):
            self._record_turn(event)
        elif isinstance(event, LiveSpeakerRevision):
            self._revise_speaker(event)

    def _record_turn(self, event: LiveTurn) -> None:
        key = (LiveChannel(event.channel).value, event.turn_order)
        with self._lock:
            if self._stopped:
                return
            existing = self._turns.get(key)
            if existing is None:
                self._turns[key] = _Turn(
                    channel=event.channel,
                    turn_order=event.turn_order,
                    # The offset into OUR file, taken at the turn's FIRST
                    # appearance: frames written before it arrived, divided by
                    # the frame rate. Never a clock.
                    start_ms=self._offset_ms_locked(),
                    text=event.text,
                    label=event.speaker,
                )
                return
            existing.text = event.text
            if event.speaker is not None:
                existing.label = event.speaker

    def _revise_speaker(self, event: LiveSpeakerRevision) -> None:
        """The provider re-clustered and reassigned an earlier turn's label.

        Only the far channel produces these. Applying them is what keeps the
        archived attribution equal to what the operator saw on screen; ignoring
        them would archive a label the surface had already corrected.
        """
        key = (LiveChannel(event.channel).value, event.turn_order)
        with self._lock:
            turn = self._turns.get(key)
            if turn is not None:
                turn.label = event.speaker

    def _publish(self, message: str, severity: Severity) -> None:
        self._bus.publish(
            Error(message=message, severity=severity, context=ERROR_CONTEXT)
        )

    # -- recording --------------------------------------------------------

    def _offset_ms_locked(self) -> int:
        return self._samples_written * 1000 // SAMPLE_RATE_HZ

    def _open_locked(self) -> Optional[str]:
        """Open the WAV. Returns an operator-facing message on failure, else None.

        Naming and pathing come from `offload` (FR-1.2): the same
        `YYYY-MM-DD_HHMMSS` basename in the same `YYYY/MM` subdirectory, placed
        through `_unique_archive_path` so that a collision with a device
        recording for the same session gets the established `-1` suffix. If the
        device ever does produce its own file, both land side by side — choosing
        between them is a judgement this module deliberately does not automate,
        and overwriting would make that choice silently, destructively.

        Opening is not recording. The path is held as PENDING here and only
        becomes `audio_path` once a write has landed, so a first write that
        fails leaves nothing behind rather than a 0-byte WAV with a transcript
        describing it (FR-1.5).
        """
        started = self._clock()
        if started.tzinfo is None:
            # The renderer emits `isoformat()`, and a naive value drops the UTC
            # offset from the frontmatter — which then reads as UTC to anything
            # that parses it, wrong by the local offset.
            started = started.astimezone()
        # We started this recording, so its start time is the primary signal
        # rather than a fallback (§11: `recorded_at` is direct on this path).
        basename = _archive_basename(started, None, WAV_EXTENSION)

        path: Optional[Path] = None
        try:
            path = _unique_archive_path(self._archive_dir, basename, started)
            handle = open(path, "wb")
        except OSError as exc:
            self._recording_stopped = True
            return self._unwritable_message(exc)

        try:
            writer = wave.open(handle, "wb")
            writer.setnchannels(CHANNELS)
            writer.setsampwidth(BYTES_PER_SAMPLE)
            writer.setframerate(SAMPLE_RATE_HZ)
        except Exception as exc:  # noqa: BLE001 -- `wave.Error` is not an OSError
            _close_quietly(handle)
            _safe_unlink(path)
            self._recording_stopped = True
            return self._unwritable_message(exc)

        self._handle = handle
        self._writer = writer
        self._pending_path = path
        self._pending_started_at = started
        log.info("live: recording to %s", path)
        return None

    def _append_locked(self, payload: bytes) -> Optional[str]:
        """Append one interleaved chunk. Returns a message on terminal failure.

        `writeframes` (not `writeframesraw`) patches the header's length fields
        after every chunk, and the flush pushes them out — so a process killed
        mid-call leaves a playable prefix rather than a file whose header claims
        zero frames, which is indistinguishable from silence (FR-1.4).

        **The payload is never re-written.** `wave.writeframes` is
        `writeframesraw` — which writes the data and increments the writer's own
        frame count — followed by `_patchheader`, which seeks and writes twice
        more; and the flush comes after all of that. A failure anywhere past the
        first of those writes leaves the chunk COMMITTED, so handing it back to
        `writeframes` a second time appends it twice while this object's counter
        records it once. Measured before this was fixed: 40.0s of audio archived
        as a file whose header claimed 30, every turn stamped ten seconds early,
        and not one error surfaced — the retry turned a transient write error
        into silent corruption of both artifacts. §3.5's "retry once" is
        therefore not implemented as a re-write; the write is attempted once and
        a failure is terminal for the RECORDING only, which is what FR-ERR-2
        already permits (the session and its live transcript continue).

        The frame count comes back from the writer rather than from the length
        of the payload we handed it, on the success path and the failure path
        alike. `Wave_write.getnframes()` is its own account of what it committed
        and cannot disagree with the file the way a locally-incremented counter
        can; adopting it after a partial failure keeps the transcript's offsets
        describing the audio that is actually there.
        """
        writer = self._writer
        handle = self._handle
        if writer is None or handle is None:
            return None
        try:
            writer.writeframes(payload)
            handle.flush()
        except OSError as exc:
            self._samples_written = writer.getnframes()
            self._commit_locked()
            self._stop_recording_locked()
            if exc.errno == errno.ENOSPC:
                # Disk-full is neither transient nor retryable, and is named as
                # the operator-actionable condition it is (FR-ERR-4).
                return self._disk_full_message()
            return self._write_failed_message(exc)
        self._samples_written = writer.getnframes()
        self._commit_locked()
        return None

    def _commit_locked(self) -> None:
        """Promote the file we opened to a recording that exists.

        Called after every write attempt, successful or not: the discriminator
        is whether the writer says frames landed, not whether the call returned
        cleanly. A `writeframes` that committed its data and then failed to
        patch the header has produced real, playable audio, and audio is the
        artifact that cannot be reconstructed (FR-ERR-3).
        """
        if self._audio_path is None and self._samples_written > 0:
            self._audio_path = self._pending_path
            self._started_at = self._pending_started_at

    def _stop_recording_locked(self) -> None:
        """Stop recording, keep the session, keep the audio captured so far."""
        self._recording_stopped = True
        self._close_writer_locked()

    def _close_writer_locked(self) -> None:
        writer, handle = self._writer, self._handle
        self._writer = None
        self._handle = None
        if writer is not None:
            try:
                writer.close()
            except Exception as exc:  # noqa: BLE001 -- a close must not mask the session
                log.warning("live: finalising the recording failed: %s", exc)
        _close_quietly(handle)
        if self._audio_path is None and self._pending_path is not None:
            # Opened, never written to. `wave.open` on a 0-byte file raises
            # `EOFError`, so leaving it behind puts an unopenable "recording" in
            # the archive — and, until this was fixed, a transcript and an
            # `.aai.json` describing it (FR-1.5).
            _safe_unlink(self._pending_path)
            self._pending_path = None
            self._pending_started_at = None

    # -- transcode --------------------------------------------------------

    def _finalise_audio(
        self, wav_path: Path, started_at: datetime, samples: int
    ) -> Tuple[Path, Optional[str]]:
        """Transcode the finished WAV to MP3. Returns `(survivor, notice)`.

        The invariant is that there is never a moment with neither file
        (FR-2.2d): the MP3 is built under a temp name, checked non-empty, moved
        into place, and only then is the WAV removed. Every failure path returns
        the WAV — it is the durable artifact until the MP3 demonstrably exists —
        and says so, because a `.wav` in the archive is a real difference the
        operator would otherwise discover downstream, when voice-print matching
        silently fails to find `<stem>.mp3` (FR-2.2e).
        """
        if not wav_path.exists():
            return wav_path, None
        encoder = _find_encoder()
        if encoder is None:
            return wav_path, self._no_encoder_message(wav_path)
        name, executable, builder = encoder

        target = _unique_archive_path(
            self._archive_dir, wav_path.stem + MP3_EXTENSION, started_at
        )
        tmp = target.with_name(target.name + _PARTIAL_SUFFIX)
        timeout = max(
            _ENCODE_TIMEOUT_FLOOR_S,
            (samples / SAMPLE_RATE_HZ) * _ENCODE_TIMEOUT_PER_AUDIO_SECOND,
        )
        try:
            completed = subprocess.run(  # noqa: S603 -- argv list, no shell
                builder(executable, wav_path, tmp),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                timeout=timeout,
            )
        except subprocess.TimeoutExpired:
            _safe_unlink(tmp)
            return wav_path, self._transcode_failed_message(
                wav_path, f"{name} did not finish within {timeout:.0f}s"
            )
        except OSError as exc:
            _safe_unlink(tmp)
            return wav_path, self._transcode_failed_message(wav_path, str(exc))

        if completed.returncode != 0:
            _log_encoder_failure(name, completed.stderr)
            _safe_unlink(tmp)
            return wav_path, self._transcode_failed_message(
                wav_path, f"{name} exited {completed.returncode}"
            )
        try:
            written = tmp.stat().st_size
        except OSError:
            written = 0
        if written == 0:
            # An encoder that reports success and produces nothing would
            # otherwise cost the recording outright, since the WAV is deleted on
            # the strength of this check (FR-2.2d).
            _safe_unlink(tmp)
            return wav_path, self._transcode_failed_message(
                wav_path, f"{name} produced an empty file"
            )
        try:
            os.replace(tmp, target)
        except OSError as exc:
            _safe_unlink(tmp)
            return wav_path, self._transcode_failed_message(wav_path, str(exc))

        # Both files exist at this instant, which is the point: only now is the
        # WAV redundant.
        try:
            wav_path.unlink()
        except OSError as exc:
            log.warning("live: the intermediate WAV could not be removed: %s", exc)
            return target, self._stray_wav_message(wav_path, target)
        log.info("live: archived %s as %s", wav_path.name, target.name)
        return target, None

    # -- transcript -------------------------------------------------------

    def _write_transcript(
        self,
        audio_path: Path,
        started_at: datetime,
        turns: List[_Turn],
        samples: int,
        names: Optional[Dict[str, str]] = None,
    ) -> None:
        """Render through the emitter and write the transcript beside the audio.

        Nothing here deletes the recording (FR-ERR-3). Audio without a
        transcript is recoverable — re-render it, or run it through the batch
        path. A transcript without audio is not: the device never persisted the
        live session, so our file is the only copy that exists.
        """
        target, conflict = self._sidecar_target(
            audio_path, TRANSCRIPT_SUFFIX, started_at
        )
        transcript = self._build_response(turns, samples, self._transcript_id(target))
        speaker_names = self._speaker_names(turns) if names is None else names
        try:
            text = render_markdown(
                transcript,
                source_filename=audio_path.name,
                recorded_at=started_at,
                speaker_names=speaker_names,
            )
        except Exception as exc:  # noqa: BLE001 -- the audio must survive any shape error
            log.warning("live: rendering the transcript failed: %s", type(exc).__name__)
            self._publish(self._transcript_failed_message(audio_path, exc), Severity.ERROR)
            return

        try:
            _atomic_write_text(target, text)
        except OSError as exc:
            self._publish(self._transcript_failed_message(audio_path, exc), Severity.ERROR)
            return
        self._transcript_path = target
        log.info("live: transcript written for %s", audio_path.name)
        if conflict is not None:
            self._publish(conflict, Severity.WARNING)
        self._write_raw_response(target, transcript)

    def _sidecar_target(
        self, audio_path: Path, suffix: str, started_at: datetime
    ) -> Tuple[Path, Optional[str]]:
        """A sidecar path derived from the DE-CONFLICTED audio stem that lands on
        nothing already in the archive.

        `_unique_archive_path` de-conflicts one name, and audio extensions
        differ across the two producers — the device writes `.mp3`, and this
        module wrote `.wav` while the call ran — so a stem that is free for the
        audio says nothing about the `.md` and the `.aai.json` beside it. Those
        were written at the un-suffixed stem with `os.replace`, which meant a
        same-second collision silently destroyed a device recording's transcript
        and its PAID `.aai.json`. §2.1's "both land side by side" was only ever
        true of the audio.

        The `-1` suffix is minted by offload's helper here too, so a
        de-conflicted transcript is named by the same convention as a
        de-conflicted recording rather than by a second scheme invented here.
        """
        target = audio_path.with_name(audio_path.stem + suffix)
        if not target.exists():
            return target, None
        alternative = _unique_archive_path(self._archive_dir, target.name, started_at)
        return alternative, (
            f"{target.name} already existed in the archive and was not "
            f"overwritten — this live session's transcript is "
            f"{alternative.name}, beside {audio_path.name}."
        )

    def _transcript_id(self, transcript_path: Path) -> str:
        """The `assemblyai_id` value. Non-empty, stable, and unmistakable.

        `render.py:62` emits `assemblyai_id: {id or ''}`, so a `None` here
        rendered a BLANK field — a shape present in zero of the 2209 archived
        transcripts, and one the operator's index misreads rather than skips:
        `hinotes_index.py`'s `^assemblyai_id:\\s*(.+?)\\s*$` under `re.M` has
        `\\s*` consume the newline after the empty value and captures the NEXT
        frontmatter line, filing every live call under the literal id
        `language_code: null`. So the field must never be empty.

        PRD §11 wants the FAR streaming session's id — near needs no
        diarization, so far is the identifying one — and marks the field a
        proxy for exactly that reason. Nothing publishes it today:
        `LiveTranscriptionStarted` carries only channel names, and no event on
        the bus carries a provider session id at all. Rather than assert a
        provider id we do not hold, the field carries `live-<archive stem>`:
        unique in the archive because the stem is de-conflicted, identical
        across the `.md` and its `.aai.json`, stable under re-reads, and
        self-describing enough that anything trying to resolve it against
        AssemblyAI fails loudly instead of quietly resolving the wrong job. When
        the bridge does publish the far session's id, this is the one place that
        changes.
        """
        return LIVE_ID_PREFIX + transcript_path.stem

    def _write_raw_response(self, transcript_path: Path, transcript: dict) -> None:
        """Best-effort `.aai.json`, for archival parity only (FR-2.6).

        `sync_sales_archive.py` merely `shutil.copy2`s this file and treats its
        absence as a warning, so nothing parses it. It is written because every
        other recording in the archive has one, and it is explicitly not
        load-bearing — a failure here costs parity, never the transcript.

        Named off the TRANSCRIPT it belongs to, so a transcript that had to
        de-conflict takes its parity file with it instead of leaving it on the
        un-suffixed stem, where it would overwrite a paid response belonging to
        a device recording. If something is already there, it is left alone:
        this file is parity, and parity is never worth destroying a response
        somebody paid for.
        """
        target = transcript_path.with_name(transcript_path.stem + RAW_RESPONSE_SUFFIX)
        if target.exists():
            self._publish(
                f"{target.name} already existed in the archive and was left "
                "untouched; the live session's transcript and recording are "
                "both written. Nothing downstream reads this file.",
                Severity.WARNING,
            )
            return
        try:
            _atomic_write_text(
                target, json.dumps(transcript, indent=2, ensure_ascii=False) + "\n"
            )
        except OSError as exc:
            self._publish(
                f"Archival parity file {target.name} could not be written: {exc}. "
                "The recording and its transcript are both intact; nothing "
                "downstream reads this file.",
                Severity.WARNING,
            )

    def _build_response(
        self, turns: List[_Turn], samples: int, transcript_id: str
    ) -> dict:
        """Assemble the live turns into the AAI-response shape the emitter reads.

        Ordered by `start` across BOTH channels (FR-2.2). The two live sessions
        number their turns independently, so grouping by channel and
        concatenating would produce a document whose lines are individually
        correct and collectively out of order — a conversation that reads as two
        monologues. The sort is stable, so turns that begin at the same offset
        keep their arrival order.

        The sort is deliberately kept even though it is currently a no-op:
        `_turns` is a single insertion-ordered table and `_samples_written` only
        ever grows, so arrival order already IS start order and replacing this
        with `list(turns)` passes every test. That makes FR-2.2 true by
        construction here — but only for as long as those two properties hold,
        and neither is stated anywhere a reader of this line would look. Sorting
        makes the requirement hold on its own terms rather than as a consequence
        of a data-structure choice made elsewhere in the file.

        `language_code` is left unset on purpose: it is not reported to us and
        the emitter renders the key as `null`, which is a stated absence rather
        than a guess. `id` is NOT left unset — see `_transcript_id` for what an
        empty one does to the operator's index.
        """
        ordered = sorted(turns, key=lambda turn: turn.start_ms)
        return {
            "id": transcript_id,
            "audio_duration": samples // SAMPLE_RATE_HZ,
            "language_code": None,
            "utterances": [
                {
                    "speaker": _speaker_key(turn.channel, turn.label),
                    "start": turn.start_ms,
                    "text": turn.text,
                }
                for turn in ordered
            ],
            "auto_highlights_result": None,
        }

    def _speaker_names(self, turns: List[_Turn]) -> Dict[str, str]:
        """The `provider key -> display name` map handed to the emitter.

        The near channel is the operator by construction. A far label the
        operator named renders as that name. **A far label the operator did NOT
        name is left out entirely** — the emitter then emits its own
        `Speaker {num}`, a digit, which is the only form
        `speaker_id_daemon.py:364` will act on. Passing the surface's own
        `Speaker A` through here would render a label that is readable on screen
        and invisible to the one thing that would ever fix it.
        """
        supplied: Mapping[str, str] = {}
        if self._names is not None:
            try:
                candidate = self._names()
            except Exception as exc:  # noqa: BLE001 -- names must not cost the transcript
                log.warning(
                    "live: the speaker-name map could not be read (%s); "
                    "unnamed speakers will be numbered",
                    type(exc).__name__,
                )
                candidate = None
            if isinstance(candidate, Mapping):
                supplied = candidate

        names: Dict[str, str] = {}
        if self._operator_name.strip():
            names[NEAR_SPEAKER_KEY] = self._operator_name
        for turn in turns:
            if turn.channel == LiveChannel.NEAR or not turn.label:
                continue
            name = supplied.get(turn.label)
            if isinstance(name, str) and name.strip():
                names[_speaker_key(turn.channel, turn.label)] = name
        return names

    # -- operator-facing messages -----------------------------------------
    #
    # Every one names a path and nothing else. No audio bytes and no transcript
    # text reach an `Error` message any more than they reach a log record.

    def _unwritable_message(self, exc: Exception) -> str:
        if isinstance(exc, OSError) and exc.errno == errno.ENOSPC:
            return self._disk_full_message()
        return (
            f"Cannot record this live session into {self._archive_dir}: {exc}. "
            "The live transcript keeps running, but no audio is being saved — "
            "check that the archive directory exists and is writable."
        )

    def _disk_full_message(self) -> str:
        return (
            f"The disk holding {self._archive_dir} is full, so live recording "
            "has stopped. The live transcript keeps running; free space and "
            "restart the session to record again."
        )

    def _write_failed_message(self, exc: OSError) -> str:
        return (
            f"Live recording stopped after a write error under "
            f"{self._archive_dir}: {exc}. The live transcript keeps running and "
            "the audio captured up to this point is kept."
        )

    def _no_encoder_message(self, wav_path: Path) -> str:
        return (
            f"This live session was archived as {wav_path.name}, not MP3: no "
            f"MP3 encoder ({' or '.join(name for name, _ in _ENCODERS)}) is "
            "installed. The recording is about five times larger than the "
            "archive's usual files, and voice-print matching looks for a .mp3 "
            "beside the transcript, so it will not identify anyone on this "
            "call. Installing lame or ffmpeg makes the next one an MP3."
        )

    def _transcode_failed_message(self, wav_path: Path, detail: str) -> str:
        return (
            f"This live session could not be converted to MP3 ({detail}), so it "
            f"is archived as {wav_path.name}. The recording and its transcript "
            "are both intact; the file is larger than the archive's usual ones "
            "and voice-print matching will not identify anyone on this call."
        )

    def _stray_wav_message(self, wav_path: Path, mp3_path: Path) -> str:
        return (
            f"This live session is archived as {mp3_path.name}, but the "
            f"intermediate {wav_path.name} could not be deleted and is still "
            "taking up space beside it. Removing it by hand is safe — the MP3 "
            "holds the same audio."
        )

    def _transcript_failed_message(self, audio_path: Path, exc: Exception) -> str:
        return (
            f"The live transcript for {audio_path.name} could not be written: "
            f"{exc}. The recording itself is intact at {audio_path} and can be "
            "transcribed by the normal path."
        )


def _log_encoder_failure(name: str, stderr: Optional[bytes]) -> None:
    """Log the encoder's own last line. It describes a file, never its contents."""
    if not stderr:
        return
    lines = [line for line in stderr.decode("utf-8", "replace").splitlines() if line.strip()]
    if lines:
        log.warning("live: %s failed: %s", name, lines[-1].strip())


def _close_quietly(handle) -> None:
    if handle is None:
        return
    try:
        handle.close()
    except OSError as exc:
        log.warning("live: closing the recording file failed: %s", exc)
