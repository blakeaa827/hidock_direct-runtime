"""Live session archival — the WAV we record ourselves, and the transcript the
emitter renders from it.

Phase 1d. PRD: projects/hidock_direct/planning/live_archive_prd.md (§6 U-1..U-15).

WHY THESE TESTS LOOK LIKE THIS
------------------------------
Three properties are load-bearing, and each one shapes the file.

1. **We open the file, so every time in the transcript is arithmetic.** The device
   does not persist live-session audio at all (operator hardware test 2026-08-27:
   power cycle + rescan produced nothing to offload), so phases 1a–1c bought a
   live transcript at the cost of the recording. This module writes the frames
   itself. That makes a turn's start time *bytes written before it arrived,
   divided by the frame rate* — a fact about our own file — rather than a
   wall-clock reading correlated against a recording somebody else started. So
   `test_turn_offsets_are_a_position_in_our_own_wav_and_never_a_clock` writes two
   minutes of audio in a few milliseconds of real time: an implementation that
   substituted a clock renders `(00:00)` for both turns and the test says so.

2. **The transcript format belongs to `diarize_audio.render.render_markdown`.**
   It is vendored here, so it is importable, so there is no boundary that would
   force a re-implementation — and a hand-written renderer in `hidock_direct`
   would be a second copy of a convention another artifact owns, drifting the
   first time that artifact changed. The tests therefore assert BOTH that the
   emitter is called and that the file's bytes equal what the emitter returns for
   the arguments it was called with. The second half is what forbids the rejected
   design in §2.2: string surgery on rendered markdown would leave the call in
   place and change the bytes.

   Names reach the document through the emitter's own `speaker_names` parameter
   (added in the canonical `diarize_audio` repo and re-vendored), never by
   substitution afterwards — re-deriving the first-appearance numbering
   `render_markdown` already did internally is that second implementation again,
   wearing a disguise.

3. **An unnamed speaker must render with a DIGIT, inside the first 2000
   characters.** The consumer is
   `personal_assistant/execution/speaker_id_daemon.py:364-366`, which is in
   another repo and cannot be imported, so its predicate is carried here
   VERBATIM — *both* lines of it:

       head = transcript.read_text(encoding='utf-8')[:2000]
       if not re.search(r'\\*\\*Speaker \\d+\\*\\*', head):
           skipped += 1
           continue

   …logged as "already tagged". A transcript full of real names is *correctly*
   skipped by voice-DB identification. But the regex wants digits, and the live
   surface renders an unnamed far label as `Speaker A` — a LETTER. Written to the
   archive verbatim, a speaker the operator never named would be counted "already
   tagged" and then identified by nothing at all, live or later. U-6 asserts our
   output against that predicate rather than against a paraphrase of its intent,
   and U-7 asserts the letter form appears nowhere.

   The `[:2000]` SLICE is half that predicate and was previously dropped here,
   which made the tests answer a question the daemon never asks. It is restored,
   and it has a consequence this file now states out loud: see
   `test_a_long_named_opening_pushes_the_digit_out_of_the_daemons_head_window`.

4. **The archived audio is MP3 whenever an encoder exists, and the WAV is an
   intermediate.** The archive holds 795 `.mp3` and zero `.wav`, and the
   pipeline hardcodes `<stem>.mp3` in five places, so a surviving `.wav` is a
   live call voice-print matching can never locate. `lame`/`ffmpeg` are
   presence-detected system tools, not dependencies, so **whether one exists is
   a property of the machine the suite runs on** — and a test whose expected
   filename extension depends on the developer's `$PATH` is not a test. Every
   test here therefore states which world it is in: an autouse fixture removes
   both encoders by default (FR-2.2e's supported configuration, and the world in
   which the WAV survives and can be read back sample by sample), and the tests
   that care install a fake one through the `encoder` fixture. Two further tests
   run the operator's REAL `lame`/`ffmpeg` when present and skip when not,
   because a shim cannot reject a flag the encoder would.

THE CONTRACT THESE TESTS PIN
----------------------------
    LiveArchive(archive_dir, *, bus, operator_name, names=None, clock=datetime.now)

    with LiveArchive(...) as rec:
        for frame in capture.frames():
            rec.write(frame)      # audio, incrementally, never buffered whole

    rec.wav_path          -> Path | None   (None until real audio has LANDED)
    rec.audio_path        -> Path | None   (the same object; the survivor's name)
    rec.transcript_path   -> Path | None   (None until stop has written it)
    rec.stop()            -> None          (idempotent; finalise + transcode +
                                            render + write)

`wav_path` names the file that EXISTS, which is not the file that was opened: a
`wave` handle opened at the first frame and never successfully written leaves a
0-byte file, `wave.open` on one of those raises `EOFError`, and a transcript
beside it would describe a recording that never happened. After a successful
transcode it names the `.mp3`; with no encoder, or a failed transcode, it names
the `.wav` that was kept instead. Either way it names a file that is really
there.

* **Turns arrive on the bus.** §7 hands this module the bus and §5 says it holds
  "the ordered turn list"; `LiveTurn` on the bus is the only place turns exist.
  It subscribes on entry and unsubscribes at stop, exactly as the controller does
  for the surface.
* **`names` is a zero-argument callable returning `label -> name`, read ONCE at
  stop** (§5: "the `label → name` map read from the surface at stop"). Reading it
  at stop is what lets a name typed in the last minute of a call reach lines
  rendered in the first.
* **`clock` is read once, when the first audio arrives.** It is the recording's
  start — the basename and `recorded_at` both come from it — and §11 records that
  `recorded_at` is *direct* on this path, unlike the proxy that shipped across
  670 batched transcripts, because here we start the recording ourselves.

Doubles are not `**kwargs`-permissive: every stand-in binds against the real
collaborator's signature before doing anything, per `test_live_server.py` and
`test_live_transcribe.py`. Seven permissive doubles hid the 2026-08-20
`transcribe_file` bug.

EVERY test below carries a `MUTATION:` comment naming the single-line change to
the implementation that makes it fail. A test with no such mutation pins nothing.

Nothing here touches a device, the network, a browser, or the operator's archive.
Every write is under `tmp_path`.
"""

from __future__ import annotations

import ast
import errno
import inspect
import io
import json
import logging
import re
import shutil
import stat
import struct
import sys
import wave
from collections import deque
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable, Dict, List, Mapping, Optional

import pytest

from diarize_audio.render import render_markdown

from hidock_direct import offload as offload_module
from hidock_direct.device import DeviceFile
from hidock_direct.events import (
    DownloadComplete,
    Error,
    EventBus,
    LiveChannel,
    LiveSpeakerRevision,
    LiveTranscriptionStarted,
    LiveTranscriptionStopped,
    LiveTurn,
    Severity,
    TranscribeSkipped,
)
from hidock_direct.offload import (
    LIVE_SESSION_HISTORY,
    LIVE_SESSION_LEAD_TOLERANCE,
    LiveSessionLog,
    LiveSessionWindow,
    Offloader,
)
from hidock_direct.realtime import CHANNELS, SAMPLE_RATE_HZ, Frame
from hidock_direct.state import DeviceKey, StateStore

SRC = Path(__file__).resolve().parent.parent / "src" / "hidock_direct"

# ---------------------------------------------------------------------------
# The module under test, imported so that its ABSENCE is red rather than fatal
# ---------------------------------------------------------------------------
#
# A plain module-level `from hidock_direct import live_archive` is the natural
# way to write this, and during the red phase it raises at COLLECTION — which
# pytest answers with `Interrupted: 1 error during collection`, running nothing
# at all. The whole suite then reports zero tests instead of 554 passing and
# this file failing, and a real regression landing in the same window would be
# invisible until phase 1d shipped. Hiding the rest of the suite behind an
# expected failure is the exact harm the "fix collection errors first" rule
# names.
#
# So the import is caught and re-raised per test by the autouse fixture below.
# Nothing is weakened: every test in this file is red until the module exists,
# each with a message that names what is missing. Once it lands the fixture is a
# no-op, and the guard can be deleted along with it.
try:
    from hidock_direct import live_archive as live_archive_module
    from hidock_direct.live_archive import (
        LIVE_ID_PREFIX,
        MP3_BITRATE_KBPS,
        RAW_RESPONSE_SUFFIX,
        TRANSCRIPT_SUFFIX,
        LiveArchive,
        _PARTIAL_SUFFIX,
    )
except ImportError as exc:  # phase 1d has not been implemented yet
    live_archive_module = None  # type: ignore[assignment]
    LiveArchive = None  # type: ignore[assignment]
    LIVE_ID_PREFIX = "live-"
    MP3_BITRATE_KBPS = 96
    RAW_RESPONSE_SUFFIX = ".aai.json"
    TRANSCRIPT_SUFFIX = ".md"
    _PARTIAL_SUFFIX = ".tmp"
    _MODULE_IMPORT_ERROR: Optional[ImportError] = exc
else:
    _MODULE_IMPORT_ERROR = None


@pytest.fixture(autouse=True)
def _the_module_under_test_exists():
    if _MODULE_IMPORT_ERROR is not None:
        pytest.fail(
            "hidock_direct.live_archive does not exist yet — phase 1d "
            f"(live_archive_prd.md) is unimplemented: {_MODULE_IMPORT_ERROR}"
        )

# ---------------------------------------------------------------------------
# The consumer's own predicate, copied rather than paraphrased
# ---------------------------------------------------------------------------
#
# `personal_assistant/execution/speaker_id_daemon.py:364-366`, re-read 2026-08-28.
# It is in another repo with no import path from here, so PRD §10 marks this row
# `derived-from-consumer` and requires the predicate to be carried verbatim. If
# the daemon changes, these constants — and the tests that use them — must be
# re-derived from it, not adjusted to match whatever we happen to emit.
#
#     head = transcript.read_text(encoding='utf-8')[:2000]
#     if not re.search(r'\*\*Speaker \d+\*\*', head):
#         skipped += 1
#         continue
#
# BOTH lines are the predicate. An earlier revision of this file carried only the
# regex, which asks "is there a digit label anywhere in the document" — a
# question the daemon never asks. It reads a fixed 2000-character HEAD window,
# and a digit label past that window is exactly as invisible to it as a letter
# label would be.
SPEAKER_ID_DAEMON_GENERIC_LABEL = re.compile(r'\*\*Speaker \d+\*\*')
SPEAKER_ID_DAEMON_HEAD_CHARS = 2000


def speaker_id_daemon_would_identify(text: str) -> bool:
    """The daemon's own admission test, slice and all.

    True means "this transcript is queued for voice identification"; False means
    it is counted `skipped += 1` and logged "already tagged" — correct for a
    document of real names, and a permanent loss of attribution for a document
    whose only generic label sits past character 2000.
    """
    head = text[:SPEAKER_ID_DAEMON_HEAD_CHARS]
    return bool(SPEAKER_ID_DAEMON_GENERIC_LABEL.search(head))


# `personal_assistant/lib/hinotes_index.py:24` and `:48`, read 2026-08-28. The
# operator's index resolves a transcript by this field, over `read_text()[:3000]`
# and under `re.M`. Carried verbatim for the same reason as the predicate above:
# it is the thing an empty `assemblyai_id` misleads, and it does not fail — it
# succeeds, on the WRONG line.
HINOTES_INDEX_ASSEMBLYAI_ID = re.compile(r'^assemblyai_id:\s*(.+?)\s*$', re.M)

# The label shape the LIVE SURFACE renders for an unnamed far label (`Speaker A`,
# `live_server.py:1300`). Correct on screen, and fatal in the archive: it fails
# the regex above, so the daemon counts the call "already tagged" and no speaker
# is ever identified.
SPEAKER_LETTER_LABEL = re.compile(r'\*\*Speaker [A-Za-z]')

# ---------------------------------------------------------------------------
# The batch format's frontmatter keys
# ---------------------------------------------------------------------------
#
# Emitted by `render.py:57-71`, in this order. Cross-checked 2026-08-27 against a
# real archived transcript in the operator's Drive archive
# (`2026/07/2026-07-02_090555.md`) — read only, nothing written there.
#
# Most files in that archive carry FIVE more keys after these — `title`,
# `summary`, `active_attendees`, `outcomes`, `deliverables` — and they are
# deliberately absent from this list: they are appended downstream by the
# operator's own pipeline, not by the emitter, and a live transcript that
# pre-declared them would be asserting facts nothing has derived yet. The
# un-enriched file above is what the emitter alone produces.
BATCH_FRONTMATTER_KEYS = [
    "recorded_at",
    "duration_seconds",
    "audio_duration_minutes",
    "source_filename",
    "assemblyai_id",
    "language_code",
    "speaker_count",
    "auto_highlights",
]

# ---------------------------------------------------------------------------
# Fixture audio. Asymmetric on purpose.
# ---------------------------------------------------------------------------
#
# Near and far carry different sample values with different SIGNS, so a channel
# swap cannot pass by symmetry — which a fixture of silence, or of the same tone
# in both ends, would let through silently. The de-interleaver in `realtime.py`
# says so directly: "a misaligned de-interleave silently swaps the channels,
# which would attribute the operator's words to the far end."
NEAR_SAMPLE = 4660       # 0x1234 -> b"\x34\x12"
FAR_SAMPLE = -4660       # 0xedcc -> b"\xcc\xed"

OPERATOR = "Blake Anderson"

# Fixed so basenames, subdirectories and `recorded_at` are all derivable in the
# assertions rather than sampled from the machine's clock mid-test.
FIXED_START = datetime(2026, 8, 27, 14, 32, 5)
FIXED_BASENAME = "2026-08-27_143205"
FIXED_SUBDIR = Path("2026") / "08"

BYTES_PER_SAMPLE = 2


def mono(sample: int, count: int) -> bytes:
    """`count` mono s16le samples, all of one value."""
    return struct.pack("<h", sample) * count


def samples_for(seconds: float) -> int:
    return int(round(seconds * SAMPLE_RATE_HZ))


def make_frame(seconds: float, *, seq: int,
               near: int = NEAR_SAMPLE, far: int = FAR_SAMPLE) -> Frame:
    count = samples_for(seconds)
    return Frame(near=mono(near, count), far=mono(far, count), seq=seq)


def read_wav(path: Path):
    """(nchannels, sampwidth, framerate, nframes, interleaved_bytes)."""
    with wave.open(str(path), "rb") as handle:
        return (
            handle.getnchannels(),
            handle.getsampwidth(),
            handle.getframerate(),
            handle.getnframes(),
            handle.readframes(handle.getnframes()),
        )


def interleaved_samples(raw: bytes) -> List[int]:
    return list(struct.unpack(f"<{len(raw) // BYTES_PER_SAMPLE}h", raw))


# ---------------------------------------------------------------------------
# Document helpers
# ---------------------------------------------------------------------------

_FRONTMATTER_KEY = re.compile(r"^([A-Za-z_][A-Za-z0-9_]*):")
_TURN_LINE = re.compile(r"^\*\*(?P<label>.+?)\*\* \((?P<offset>\d{2}:\d{2})\): (?P<text>.*)$")


def frontmatter_keys(text: str) -> List[str]:
    """Top-level keys between the opening and closing `---` fences, in order.

    Column-anchored, so the indented `  - "..."` items under `auto_highlights`
    are not mistaken for keys.
    """
    lines = text.splitlines()
    assert lines and lines[0] == "---", "the document has no opening frontmatter fence"
    keys: List[str] = []
    for line in lines[1:]:
        if line == "---":
            return keys
        match = _FRONTMATTER_KEY.match(line)
        if match:
            keys.append(match.group(1))
    raise AssertionError("the frontmatter fence was never closed")


def frontmatter(text: str) -> Dict[str, str]:
    lines = text.splitlines()
    out: Dict[str, str] = {}
    for line in lines[1:]:
        if line == "---":
            break
        match = _FRONTMATTER_KEY.match(line)
        if match:
            out[match.group(1)] = line[len(match.group(1)) + 1:].strip()
    return out


def turn_lines(text: str) -> List[tuple]:
    """Every rendered turn as (label, mm:ss, text), in document order."""
    out = []
    for line in text.splitlines():
        match = _TURN_LINE.match(line)
        if match:
            out.append((match.group("label"), match.group("offset"), match.group("text")))
    return out


def labels_of(text: str) -> List[str]:
    return [label for label, _offset, _text in turn_lines(text)]


def offsets_of(text: str) -> List[str]:
    return [offset for _label, offset, _text in turn_lines(text)]


def texts_of(text: str) -> List[str]:
    return [body for _label, _offset, body in turn_lines(text)]


# ---------------------------------------------------------------------------
# Structural helpers — AST over the real module, never a source grep
# ---------------------------------------------------------------------------


def module_tree() -> ast.AST:
    return ast.parse(inspect.getsource(live_archive_module))


def imported_names(tree: ast.AST) -> Dict[str, List[str]]:
    """`{module: [names]}` for every `from X import a, b` in the module."""
    out: Dict[str, List[str]] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            module = ("." * node.level) + (node.module or "")
            out.setdefault(module, []).extend(alias.name for alias in node.names)
    return out


def called_names(tree: ast.AST) -> set:
    return {
        node.func.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }


def called_attributes(tree: ast.AST) -> set:
    return {
        node.func.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
    }


def string_constants(tree: ast.AST) -> set:
    return {
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
    }


def _bus_subscribers(bus: EventBus) -> List[object]:
    """Everything the bus would still call. Reads `EventBus._subs` deliberately.

    There is no public way to ask a bus what it holds, and "did this object let
    go" is not answerable from the outside: a still-subscribed recorder that has
    stopped behaves identically to an unsubscribed one on every observable
    surface, right up until the NEXT session's turns start reaching it.
    """
    return list(bus._subs)


def retained_bytes(root: object, depth: int = 4) -> int:
    """Total length of every bytes-like object reachable from `root`.

    Bounded by depth and by an identity set, so it terminates on the cyclic
    object graph a bus subscription creates.
    """
    seen: set = set()
    total = 0

    def walk(obj: object, remaining: int) -> None:
        nonlocal total
        if remaining < 0 or id(obj) in seen:
            return
        seen.add(id(obj))
        if isinstance(obj, (bytes, bytearray)):
            total += len(obj)
            return
        if isinstance(obj, memoryview):
            total += obj.nbytes
            return
        if isinstance(obj, (list, tuple, set, frozenset, deque)):
            for item in obj:
                walk(item, remaining - 1)
            return
        if isinstance(obj, dict):
            for value in obj.values():
                walk(value, remaining - 1)
            return
        namespace = getattr(obj, "__dict__", None)
        if isinstance(namespace, dict):
            for value in namespace.values():
                walk(value, remaining - 1)

    walk(root, depth)
    return total


# ---------------------------------------------------------------------------
# Which world are we in: is an MP3 encoder installed?
# ---------------------------------------------------------------------------
#
# `lame` and `ffmpeg` are presence-detected system tools (FR-2.2b), never pip
# dependencies — this is a public clone-and-run app and an encoder is something
# the operator may or may not have. That makes "is there an encoder" a fact about
# the machine running the suite, and a test whose expected filename extension
# depends on the developer's `$PATH` passes on one laptop and fails on the next.
#
# So every test states its world. The autouse fixture removes both encoders,
# which is FR-2.2e's supported configuration and the one in which the WAV
# survives and can be read back sample by sample. Tests that need an encoder ask
# for the `encoder` fixture and install a shim; two ask for the operator's real
# one and skip when it is absent.

_MP3_ENCODERS = ("lame", "ffmpeg")

# Bound at import, BEFORE any fixture patches it. `live_archive` imports the
# `shutil` module rather than the function, so the only place to intercept
# presence detection is `shutil.which` itself — which means a test that wants to
# ask the machine the real question has to hold the real function from before.
_REAL_WHICH = shutil.which


def _patch_which(monkeypatch, table: Dict[str, str]) -> None:
    """Make `shutil.which` answer `table` for the encoders and the truth for all
    else. Patched on the module object `live_archive` actually calls through."""

    def which(cmd, *args, **kwargs):
        if cmd in _MP3_ENCODERS:
            return table.get(cmd)
        return _REAL_WHICH(cmd, *args, **kwargs)

    monkeypatch.setattr(live_archive_module.shutil, "which", which)


@pytest.fixture(autouse=True)
def _no_mp3_encoder_unless_a_test_installs_one(monkeypatch):
    if live_archive_module is None:
        yield
        return
    _patch_which(monkeypatch, {})
    yield


# Behaviours a shim encoder can have. Each one is a real failure mode
# `_finalise_audio` names, and each is unreachable with a real encoder.
_SHIM = {
    # Writes plausible bytes and succeeds.
    "ok": "printf 'ID3\\3\\0\\0\\0fake mp3 payload' > \"$TARGET\"\nexit 0\n",
    # Succeeds and produces a zero-byte file. The WAV is deleted on the strength
    # of the non-empty check, so this is the shape that would cost the recording.
    "empty": ": > \"$TARGET\"\nexit 0\n",
    # Succeeds and writes nothing at all.
    "missing": "exit 0\n",
    # Fails the way an encoder fails: non-zero, a line on stderr, no output file.
    "fail": "echo 'lame: unsupported sample format' >&2\nexit 3\n",
}


class Encoder:
    """A fake `lame`/`ffmpeg` on the PATH `live_archive` consults."""

    def __init__(self, monkeypatch, tmp_path: Path):
        self._monkeypatch = monkeypatch
        self._dir = tmp_path / "fake-bin"
        self._dir.mkdir(parents=True, exist_ok=True)
        self.argv_log = self._dir / "argv.txt"
        self.name: Optional[str] = None

    def install(self, behaviour: str = "ok", *, name: str = "lame") -> "Encoder":
        script = self._dir / name
        script.write_text(
            "#!/bin/sh\n"
            f'printf "%s\\n" "$@" >> "{self.argv_log}"\n'
            'eval "TARGET=\\${$#}"\n'
            + _SHIM[behaviour]
        )
        script.chmod(script.stat().st_mode | stat.S_IXUSR)
        _patch_which(self._monkeypatch, {name: str(script)})
        self.name = name
        return self

    def use_real(self, name: str) -> "Encoder":
        """The operator's own encoder. A shim cannot reject a flag; this can."""
        found = _REAL_WHICH(name)
        if found is None:  # pragma: no cover - machine-dependent
            pytest.skip(f"{name} is not installed on this machine")
        _patch_which(self._monkeypatch, {name: found})
        self.name = name
        return self

    def argv(self) -> List[str]:
        if not self.argv_log.exists():
            return []
        return self.argv_log.read_text().splitlines()

    def invocations(self) -> int:
        """How many times the encoder ran. `--quiet` opens every lame argv."""
        return sum(1 for arg in self.argv() if arg in ("--quiet", "-nostdin"))


@pytest.fixture
def encoder(monkeypatch, tmp_path) -> Encoder:
    return Encoder(monkeypatch, tmp_path)


# ---------------------------------------------------------------------------
# A file handle that fails one write, the way a full disk or a dropped mount does
# ---------------------------------------------------------------------------


class _FailingWrites:
    """Wraps the real binary handle and fails ONE `write`, chosen by position.

    Where the failure lands is the whole point, because `wave.Wave_write` commits
    in stages. `writeframes(data)` is `writeframesraw` — which writes the payload
    and increments the writer's own frame count — followed by `_patchheader`,
    which seeks and writes twice more. So:

    * `when="payload"` fails ON the payload write. Nothing is committed and the
      writer's count does not move.
    * `when="after-payload"` fails on the FIRST write after the payload lands,
      i.e. inside `_patchheader`. The audio IS on disk and the writer HAS counted
      it, and the call still raises.

    The second case is the one that produced the corruption: handing that payload
    back to `writeframes` a second time appends it twice while a locally
    incremented counter records it once. Selecting by payload SIZE rather than by
    call ordinal, because the number of writes `_write_header` makes is a CPython
    implementation detail and counting them would make the fixture brittle.
    """

    def __init__(self, real, *, payload_size: int, when: str, nth: int = 1,
                 error: Optional[OSError] = None):
        self._real = real
        self._payload_size = payload_size
        self._when = when
        self._nth = nth
        self._error = error or OSError(errno.EIO, "input/output error")
        self._payloads_seen = 0
        self._armed = False
        self.raised = False

    def write(self, data):
        is_payload = len(data) == self._payload_size
        if is_payload:
            self._payloads_seen += 1
        if self._when == "payload" and is_payload and self._payloads_seen == self._nth:
            self.raised = True
            raise self._error
        if self._when == "after-payload":
            if self._armed:
                self._armed = False
                self.raised = True
                raise self._error
            if is_payload and self._payloads_seen == self._nth:
                written = self._real.write(data)
                self._armed = True
                return written
        return self._real.write(data)

    # Everything else is the real file.
    def __getattr__(self, name):
        return getattr(self._real, name)


def failing_handle(monkeypatch, *, payload_size: int, when: str, nth: int = 1,
                   error: Optional[OSError] = None) -> Dict[str, _FailingWrites]:
    """Shadow the `open` that `_open_locked` calls, for binary writes only.

    A module-global `open` shadows the builtin the module resolves, so this
    reaches the real production call site rather than a seam invented for the
    test. Text-mode opens (`_atomic_write_text`) are left completely alone.
    """
    box: Dict[str, _FailingWrites] = {}
    real_open = open

    def opener(file, mode="r", *args, **kwargs):
        handle = real_open(file, mode, *args, **kwargs)
        if "b" not in mode or "w" not in mode:
            return handle
        wrapped = _FailingWrites(
            handle, payload_size=payload_size, when=when, nth=nth, error=error
        )
        box["handle"] = wrapped
        return wrapped

    monkeypatch.setattr(live_archive_module, "open", opener, raising=False)
    return box


def payload_bytes(seconds: float) -> int:
    """The interleaved stereo payload one `make_frame(seconds)` produces."""
    return samples_for(seconds) * CHANNELS * BYTES_PER_SAMPLE


# ---------------------------------------------------------------------------
# Harness
# ---------------------------------------------------------------------------


class Harness:
    """One `LiveArchive` plus the bus it listens on, driven the way §7 drives it."""

    def __init__(self, archive_dir: Path, *, names: Dict[str, str],
                 operator_name: str, clock: Callable[[], datetime],
                 bus: EventBus, events: List[object]):
        self.dir = archive_dir
        self.names = names
        self.bus = bus
        self.events = events
        self.rec = LiveArchive(
            archive_dir,
            bus=bus,
            operator_name=operator_name,
            names=lambda: self.names,
            clock=clock,
        )
        self._seq = 0
        self._orders: Dict[str, int] = {"near": 0, "far": 0}

    # -- driving ----------------------------------------------------------

    def write(self, seconds: float = 0.1, *,
              near: int = NEAR_SAMPLE, far: int = FAR_SAMPLE) -> Frame:
        self._seq += 1
        frame = make_frame(seconds, seq=self._seq, near=near, far=far)
        self.rec.write(frame)
        return frame

    def write_frame(self, frame: Frame) -> None:
        self.rec.write(frame)

    def turn(self, text: str, *, channel: LiveChannel = LiveChannel.FAR,
             label: Optional[str] = "A", order: Optional[int] = None,
             final: bool = True) -> LiveTurn:
        """Publish one `LiveTurn`, numbering per channel the way the bridge does."""
        key = channel.value
        if order is None:
            order = self._orders[key]
            self._orders[key] += 1
        speaker = None if channel is LiveChannel.NEAR else label
        event = LiveTurn(
            channel=channel, text=text, speaker=speaker,
            turn_order=order, is_final=final,
        )
        self.bus.publish(event)
        return event

    def near(self, text: str, **kwargs) -> LiveTurn:
        return self.turn(text, channel=LiveChannel.NEAR, label=None, **kwargs)

    # -- reading ----------------------------------------------------------

    def errors(self) -> List[Error]:
        return [event for event in self.events if isinstance(event, Error)]

    def messages(self) -> str:
        return "\n".join(error.message for error in self.errors())

    def transcript_text(self) -> str:
        path = self.rec.transcript_path
        assert path is not None, "no transcript was written"
        return path.read_text(encoding="utf-8")

    def files(self) -> List[Path]:
        if not self.dir.is_dir():
            return []
        return sorted(p for p in self.dir.rglob("*") if p.is_file())

    def suffixed(self, suffix: str) -> List[Path]:
        return [p for p in self.files() if p.name.endswith(suffix)]


@pytest.fixture
def sessions(tmp_path):
    """Factory for `LiveArchive` harnesses; every one stopped after the test."""
    made: List[Harness] = []
    counter = {"n": 0}

    def build(*, archive_dir: Optional[Path] = None,
              names: Optional[Dict[str, str]] = None,
              operator_name: str = OPERATOR,
              clock: Optional[Callable[[], datetime]] = None) -> Harness:
        counter["n"] += 1
        if archive_dir is None:
            archive_dir = tmp_path / f"archive{counter['n']}"
        bus = EventBus()
        events: List[object] = []
        bus.subscribe(events.append)
        harness = Harness(
            archive_dir,
            names=dict(names or {}),
            operator_name=operator_name,
            clock=clock or (lambda: FIXED_START),
            bus=bus,
            events=events,
        )
        made.append(harness)
        return harness

    yield build

    for harness in made:
        try:
            harness.rec.stop()
        except Exception:
            pass


# ==========================================================================
# U-1 — the audio lands as a 16 kHz stereo WAV, near on channel 1
# ==========================================================================


def test_frames_land_as_a_sixteen_kilohertz_two_channel_wav(sessions):
    """FR-1.1. The stream is already 16 kHz 16-bit stereo and already
    de-interleaved; the archive keeps the separation, which the batch path never
    had. A mono downmix here would throw away the only structural speaker
    attribution this project has."""
    # MUTATION: `writer.setnchannels(2)` -> `writer.setnchannels(1)` (or
    # `setframerate(16000)` -> `setframerate(48000)` to match the flash recorder).
    harness = sessions()

    with harness.rec:
        harness.write(0.25)

    channels, width, rate, nframes, _raw = read_wav(harness.rec.wav_path)

    assert channels == CHANNELS == 2
    assert width == BYTES_PER_SAMPLE
    assert rate == SAMPLE_RATE_HZ == 16000
    assert nframes == samples_for(0.25)


def test_channel_one_is_the_near_end_and_channel_two_is_the_far_end(sessions):
    """FR-1.1, and the reason the fixture is asymmetric.

    `realtime.py` proves ch1 = near / ch2 = far on hardware — a 1 kHz tone played
    out through the P1 showed a 38 dB peak in channel 2 only while channel 1
    *fell*, the echo canceller removing it from the mic path. Everything
    downstream inherits that: the near end is the operator by construction, which
    is why speaker attribution on this path is structural rather than inferred.
    Silence in both ends, or the same tone in both, would let a swap through.
    """
    # MUTATION: interleave far-then-near — `for n, f in zip(near, far)` becomes
    # `zip(far, near)` — which attributes the operator's words to the far end for
    # every live call ever archived.
    harness = sessions()

    with harness.rec:
        harness.write(0.05)
        harness.write(0.05)

    _ch, _w, _rate, _n, raw = read_wav(harness.rec.wav_path)
    samples = interleaved_samples(raw)

    assert samples[0::2] == [NEAR_SAMPLE] * samples_for(0.1), "channel 1 is not the near end"
    assert samples[1::2] == [FAR_SAMPLE] * samples_for(0.1), "channel 2 is not the far end"
    # And the two really are distinguishable, so the assertions above are not
    # both satisfied by one buffer copied into both channels.
    assert NEAR_SAMPLE != FAR_SAMPLE


def test_two_frames_are_appended_in_arrival_order_not_overwritten(sessions):
    """FR-1.3. `frames()` yields ~10 chunks/sec for the length of the call; each
    one extends the file."""
    # MUTATION: reopen the WAV per frame (`wave.open(path, "wb")` inside `write`)
    # so every chunk truncates the last — a whole call collapses to 100 ms.
    harness = sessions()
    first, second = 1000, -1000

    with harness.rec:
        harness.write(0.05, near=first, far=first)
        harness.write(0.05, near=second, far=second)

    _ch, _w, _rate, _n, raw = read_wav(harness.rec.wav_path)
    near_samples = interleaved_samples(raw)[0::2]

    half = samples_for(0.05)
    assert near_samples[:half] == [first] * half
    assert near_samples[half:] == [second] * half


# ==========================================================================
# U-2 — the file on disk is playable before anything finalises it
# ==========================================================================


def test_the_wav_is_playable_after_an_abrupt_stop_that_never_reached_close(sessions):
    """FR-1.4. A crash mid-call must leave a playable prefix, not a file whose
    header claims zero frames — which is indistinguishable from silence, and would
    read as "the recording never happened" for a call that was in fact captured.

    Nothing is closed here on purpose: this reads the file while the session is
    still open, which is exactly the state a killed process leaves behind.
    """
    # MUTATION: `writer.writeframes(chunk)` -> `writer.writeframesraw(chunk)`.
    # `writeframes` patches the header's length fields after every chunk;
    # `writeframesraw` does not, so a file that is never closed reports 0 frames
    # and every byte captured is unreachable.
    harness = sessions()

    harness.rec.__enter__()
    harness.write(0.2)
    harness.write(0.2)

    channels, _w, rate, nframes, raw = read_wav(harness.rec.wav_path)

    assert nframes == samples_for(0.4), (
        "the header does not describe the audio already written; a process killed "
        "here leaves a recording that reads as empty"
    )
    assert len(raw) == samples_for(0.4) * channels * BYTES_PER_SAMPLE
    assert rate == SAMPLE_RATE_HZ


# ==========================================================================
# U-3 — naming and pathing come from `offload`, never restated
# ==========================================================================


def test_the_recording_is_named_and_placed_by_the_offload_convention(sessions):
    """FR-1.2. A live recording is an ordinary archive recording: same
    `YYYY-MM-DD_HHMMSS` basename, same `YYYY/MM` subdirectory. Anything else and
    the operator has two naming schemes in one folder and the pipeline's
    basename-derived `recorded_at` stops resolving."""
    # MUTATION: `path = archive_dir / f"live-{when:%Y%m%d-%H%M%S}.wav"` — a local
    # naming scheme that never collides with `offload`'s and never gets the `-1`.
    harness = sessions()

    with harness.rec:
        harness.write(0.1)

    wav = harness.rec.wav_path
    assert wav.name == f"{FIXED_BASENAME}.wav"
    assert wav.parent == harness.dir / FIXED_SUBDIR
    # The same basename the offload path would have minted for this instant.
    assert wav.stem == FIXED_START.strftime(offload_module.ARCHIVE_BASENAME_FORMAT)
    # And it parses back through offload's own inverse, which is what makes a
    # live recording indistinguishable from a batched one downstream.
    assert offload_module._recorded_at_from_basename(wav) == FIXED_START


def test_a_basename_collision_gets_the_established_suffix_and_never_an_overwrite(sessions):
    """FR-1.2 / §2.1. If the device ever DOES produce its own recording for a live
    session, both land side by side — `_unique_archive_path` already appends
    `-1`. Choosing between them is a judgement this PRD deliberately does not
    automate: there is one observation of the device's behaviour, not a model of
    it. Overwriting would make that choice silently, in the destructive
    direction."""
    # MUTATION: `path = subdir / basename` instead of calling
    # `offload._unique_archive_path(...)`.
    harness = sessions()
    squatter = harness.dir / FIXED_SUBDIR / f"{FIXED_BASENAME}.wav"
    squatter.parent.mkdir(parents=True, exist_ok=True)
    squatter.write_bytes(b"the device's own recording of this same session")

    with harness.rec:
        harness.write(0.1)
        harness.turn("hello", label="A")

    assert harness.rec.wav_path.name == f"{FIXED_BASENAME}-1.wav"
    assert squatter.read_bytes() == b"the device's own recording of this same session"
    # The transcript follows the audio it describes, suffix and all.
    assert harness.rec.transcript_path.name == f"{FIXED_BASENAME}-1.md"


def test_naming_and_pathing_are_imported_from_offload_and_never_restated(sessions):
    """NFR-1. The PRD says both are "imported, not restated". A second copy of the
    strftime format is a second implementation of the archive's naming convention
    and drifts the first time the convention moves."""
    # MUTATION: `basename = when.strftime("%Y-%m-%d_%H%M%S") + ".wav"` inline,
    # dropping the import.
    tree = module_tree()
    from_offload = [
        name
        for module, names in imported_names(tree).items()
        if module.endswith("offload")
        for name in names
    ]

    assert "_unique_archive_path" in from_offload, (
        f"live_archive does not import offload's collision helper: {from_offload}"
    )
    assert any(
        name in from_offload
        for name in ("ARCHIVE_BASENAME_FORMAT", "_archive_basename")
    ), f"the basename convention is not imported from offload: {from_offload}"
    assert offload_module.ARCHIVE_BASENAME_FORMAT not in string_constants(tree), (
        "the archive basename format is restated as a literal in live_archive"
    )


# ==========================================================================
# U-4 — the transcript is the emitter's output, byte for byte
# ==========================================================================


def _recording_emitter(monkeypatch, calls: List[tuple]):
    """Wrap the module's `render_markdown` so the call is observable.

    Delegates to the real emitter and binds against its real signature first, so
    the double is never more permissive than production. Deliberately patches the
    MODULE ATTRIBUTE rather than injecting a seam: a seam the test supplied would
    be satisfied by a module that never imports the emitter at all.
    """
    real = live_archive_module.render_markdown

    def recorder(transcript, **kwargs):
        inspect.signature(render_markdown).bind(transcript, **kwargs)
        calls.append((json.loads(json.dumps(transcript)), dict(kwargs)))
        return real(transcript, **kwargs)

    monkeypatch.setattr(live_archive_module, "render_markdown", recorder)
    return real


def test_the_module_renders_through_the_real_emitter_and_not_a_local_copy(
    sessions, monkeypatch
):
    """FR-2.1 / §2.2. `render_markdown` DEFINES the transcript format, and it is
    vendored here, so nothing forces a re-implementation.

    Two assertions, and the second is the one that matters: the call alone is
    satisfied by a module that calls the emitter and then rewrites its output —
    which is precisely the rejected design in §2.2, string surgery on rendered
    markdown. The bytes on disk have to BE what the emitter returned.
    """
    # MUTATION: `text = render_markdown(...)` followed by
    # `text = text.replace(f"**Speaker {n}**", f"**{name}**")` — the call survives,
    # the byte-equality does not.
    calls: List[tuple] = []
    real = _recording_emitter(monkeypatch, calls)
    assert real is render_markdown, (
        "live_archive.render_markdown is not diarize_audio.render.render_markdown; "
        "the module holds a second implementation of another artifact's format"
    )
    harness = sessions(names={"A": "Dana"})

    with harness.rec:
        harness.write(1.0)
        harness.turn("we should ship it", label="A")
        harness.near("agreed")

    assert len(calls) == 1, f"the emitter was called {len(calls)} times, not once"
    transcript, kwargs = calls[0]
    assert kwargs["source_filename"] == harness.rec.wav_path.name
    assert isinstance(kwargs["recorded_at"], datetime)
    assert isinstance(kwargs["speaker_names"], Mapping)

    assert harness.transcript_text() == render_markdown(transcript, **kwargs), (
        "the archived transcript is not byte-identical to the emitter's output "
        "for the same input; something rewrote the document after rendering"
    )


def test_live_turns_are_assembled_into_an_aai_shaped_response(sessions, monkeypatch):
    """FR-2.2. The emitter consumes an AAI-response-shaped dict, so the assembly
    happens before the call rather than the format being assembled by hand."""
    # MUTATION: pass `{"turns": [...]}` (the live vocabulary) instead of
    # `utterances` — the emitter renders a document with no turns at all and a
    # `speaker_count` of 0, silently.
    calls: List[tuple] = []
    _recording_emitter(monkeypatch, calls)
    harness = sessions()

    with harness.rec:
        harness.write(2.0)
        harness.turn("first thing", label="A")
        harness.write(1.0)
        harness.near("second thing")

    transcript, _kwargs = calls[0]
    utterances = transcript["utterances"]

    assert [u["text"] for u in utterances] == ["first thing", "second thing"]
    assert all("speaker" in u and "start" in u for u in utterances)
    assert transcript["audio_duration"] == 3, (
        "audio_duration is not the length of the audio we wrote; the frontmatter's "
        "duration_seconds is derived from it"
    )
    # Distinct provider keys for the two ends — the near channel is a speaker in
    # its own right and must not merge with a far label that happens to be named
    # the same thing.
    assert len({u["speaker"] for u in utterances}) == 2


def test_the_emitter_is_imported_from_the_vendored_diarize_tree(sessions):
    """§2.2 / NFR-1. Structural, because the behavioural test above can be
    satisfied by any callable bound to that name."""
    # MUTATION: `def render_markdown(...)` defined locally in live_archive.
    tree = module_tree()
    sources = [
        module for module, names in imported_names(tree).items()
        if "render_markdown" in names
    ]

    assert sources, "live_archive does not import render_markdown at all"
    assert all("render" in module for module in sources), sources
    assert all("diarize_audio" in module for module in sources), (
        f"render_markdown is imported from {sources}, not from the emitter"
    )
    assert "render_markdown" in called_names(tree)
    # And it is not shadowed by a local definition of the same name.
    defined = {
        node.name for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    assert "render_markdown" not in defined


# ==========================================================================
# U-5 — names: the operator, and a far label the operator named
# ==========================================================================


def test_the_near_channel_renders_as_the_operator_and_a_named_label_as_its_name(
    sessions,
):
    """§2.3, rows one and two. Both are `**<name>**`, and both are then correctly
    SKIPPED by the voice-DB daemon: the operator is the operator, and a named
    speaker is already identified."""
    # MUTATION: drop the near-channel entry from the map handed to
    # `speaker_names`, so the operator renders as `**Speaker 1**` and every live
    # call the operator ever recorded is queued for voice identification.
    harness = sessions(names={"A": "Dana"})

    with harness.rec:
        harness.write(1.0)
        harness.near("morning")
        harness.turn("morning to you", label="A")

    text = harness.transcript_text()

    assert labels_of(text) == [OPERATOR, "Dana"]
    assert f"**{OPERATOR}**" in text
    assert "**Dana**" in text
    # Both are named, so the daemon correctly finds nothing to identify.
    assert not SPEAKER_ID_DAEMON_GENERIC_LABEL.search(text)


def test_the_name_map_is_read_at_stop_so_a_late_name_reaches_the_earliest_line(
    sessions,
):
    """§5: "the `label → name` map read from the surface at stop".

    The operator types a name when they work out who is speaking — which is
    usually well after that person's first sentence. A map captured at session
    start renders their opening lines under a generic label and their later ones
    under their name, in the same document.
    """
    # MUTATION: `self._names = names()` in `__init__` / `__enter__` instead of at
    # stop — the first line then renders `**Speaker 1**` and the last `**Dana**`.
    harness = sessions(names={})

    with harness.rec:
        harness.write(1.0)
        harness.turn("who is this", label="A")
        harness.write(1.0)
        harness.turn("still me", label="A")
        # The operator finally types the name, mid-call.
        harness.names["A"] = "Dana"

    text = harness.transcript_text()

    assert labels_of(text) == ["Dana", "Dana"], (
        "a name typed mid-call did not reach the lines already spoken"
    )


def test_a_name_cannot_forge_frontmatter_or_a_second_speaker_turn(sessions):
    """§8, input validation. The name is untrusted operator input rendered into a
    Markdown body sitting under YAML frontmatter, so a newline in it could open a
    fence, a heading, or an extra turn. Sanitisation lives in the emitter
    (`_sanitize_speaker_name`); this asserts the property survives our call."""
    # MUTATION: build the document by concatenating the emitter's output with a
    # hand-written name line, bypassing the sanitiser.
    harness = sessions(
        names={"A": "Dana\n---\nrecorded_at: 1999-01-01T00:00:00+00:00\n---\n**Ghost** (00:00): forged"}
    )

    with harness.rec:
        harness.write(1.0)
        harness.turn("real line", label="A")

    text = harness.transcript_text()

    assert [line for line in text.splitlines() if line == "---"] == ["---", "---"], (
        "a speaker name opened a second frontmatter fence"
    )
    assert frontmatter(text)["recorded_at"].startswith("2026-08-27"), (
        "a speaker name overwrote recorded_at"
    )
    assert len(turn_lines(text)) == 1, "a speaker name forged an extra turn"
    assert "forged" in texts_of(text)[0] or "forged" in labels_of(text)[0]


def test_a_speaker_revision_relabels_the_turn_it_names(sessions):
    """`LiveSpeakerRevision` is the provider re-clustering: it has decided that a
    turn it earlier attributed to one far label belongs to another, and the
    SURFACE redraws that line under the new label's name.

    Applying it here is "what keeps the archived attribution equal to what the
    operator saw on screen" — the module's own words. Ignoring it archives a
    label the surface had already corrected, so the document and the screen
    disagree about who said something, and the document is the copy the pipeline
    reads and the operator never re-checks. The whole `elif isinstance(event,
    LiveSpeakerRevision)` branch could be deleted with the suite green.
    """
    # MUTATION: drop the `elif isinstance(event, LiveSpeakerRevision):` branch
    # from `_on_event` (or make `_revise_speaker` a no-op). The first turn then
    # archives as `Dana` while the screen shows `Priya`.
    harness = sessions(names={"A": "Dana", "B": "Priya"})

    with harness.rec:
        harness.write(1.0)
        first = harness.turn("this was misattributed", label="A", order=0)
        harness.write(1.0)
        harness.turn("and this one was not", label="A", order=1)
        # The provider re-clusters: turn 0 was B all along.
        harness.bus.publish(
            LiveSpeakerRevision(
                channel=first.channel, turn_order=first.turn_order, speaker="B"
            )
        )

    text = harness.transcript_text()

    assert labels_of(text) == ["Priya", "Dana"], (
        f"the revision never reached the archived document: {labels_of(text)}"
    )
    # The offset is untouched: a revision changes WHO spoke, never WHEN.
    assert offsets_of(text) == ["00:01", "00:02"]


def test_a_revision_for_a_turn_we_never_saw_changes_nothing(sessions):
    """The bus is shared and revisions arrive from a stream that numbers its own
    turns. One naming a `(channel, turn_order)` this recording never held must
    not mint a turn — an utterance with no text and no offset would render a
    blank line in a document nothing can correct."""
    # MUTATION: `self._turns.setdefault(key, _Turn(...))` in `_revise_speaker`,
    # or dropping the `if turn is not None` guard.
    harness = sessions(names={"A": "Dana"})

    with harness.rec:
        harness.write(1.0)
        harness.turn("the only thing said", label="A", order=0)
        harness.bus.publish(
            LiveSpeakerRevision(channel=LiveChannel.FAR, turn_order=41, speaker="Z")
        )

    text = harness.transcript_text()

    assert texts_of(text) == ["the only thing said"]
    assert labels_of(text) == ["Dana"]


def test_a_names_callable_that_raises_costs_the_names_and_not_the_transcript(
    sessions, caplog
):
    """`_speaker_names` guards the call because `names` is a callable owned by
    ANOTHER object — `LiveSurface.speaker_names`, read at stop, on the teardown
    path, after the surface may already have begun shutting down. A raise there
    must cost the NAMES, never the transcript: the audio and the words are what
    cannot be reconstructed, and a name is a label over them.

    Unnamed then means unnamed, so every speaker renders as a digit — which is
    the daemon-actionable form, so even the degraded document stays recoverable
    rather than becoming permanently anonymous.
    """
    # MUTATION: delete the `try/except` around `candidate = self._names()` in
    # `_speaker_names`. The exception unwinds `stop()`, and the call loses its
    # transcript AND its `.aai.json` over a name lookup.
    caplog.set_level(logging.DEBUG)
    harness = sessions()

    def exploding_names():
        raise RuntimeError("the surface is already shutting down")

    harness.rec._names = exploding_names

    with harness.rec:
        harness.write(1.0)
        harness.turn("still said out loud", label="A")
        harness.near("and answered")

    text = harness.transcript_text()

    assert texts_of(text) == ["still said out loud", "and answered"]
    # The operator's name does NOT come from that map — it is the
    # `operator_name` this object was constructed with — so it survives, and
    # only the far speakers degrade to numbers.
    assert labels_of(text) == ["Speaker 1", OPERATOR], (
        f"a names failure produced labels nothing can act on: {labels_of(text)}"
    )
    assert speaker_id_daemon_would_identify(text), (
        "the degraded document carries no digit label in the daemon's head "
        "window, so the speakers can never be recovered later either"
    )
    assert harness.suffixed(".aai.json"), "the parity file was lost with the names"
    blob = "\n".join(record.getMessage() for record in caplog.records)
    assert "RuntimeError" in blob, (
        "the names failure was swallowed without a trace; a silently unnamed "
        "archive is indistinguishable from a call nobody named"
    )


def test_a_names_callable_returning_something_that_is_not_a_map_is_ignored(sessions):
    """The same seam, the other way it breaks. `None` and a list both answer the
    call without raising, and `supplied.get(...)` on either is an
    `AttributeError` raised from inside the render path — where there is no
    guard, because the guard is on the CALL."""
    # MUTATION: `supplied = candidate` unconditionally, dropping the
    # `isinstance(candidate, Mapping)` check.
    harness = sessions()
    harness.rec._names = lambda: ["A", "Dana"]

    with harness.rec:
        harness.write(1.0)
        harness.turn("unnamed by a broken map", label="A")

    text = harness.transcript_text()

    assert texts_of(text) == ["unnamed by a broken map"]
    assert labels_of(text) == ["Speaker 1"]


# ==========================================================================
# U-6 — an UNNAMED far label renders with a DIGIT
# ==========================================================================


def test_an_unnamed_far_label_renders_a_digit_the_voice_daemon_will_act_on(sessions):
    """§2.3, row three — the finding this whole PRD section exists for.

    The live surface renders an unnamed far label as `Speaker A`. Written to the
    archive verbatim, `**Speaker A**` fails the daemon's regex, the call is
    counted "already tagged", and a speaker the operator never named is
    identified by NOTHING — not live, not later.

    Asserted against the consumer's own predicate, carried verbatim, rather than
    against a paraphrase of what we think it wants.
    """
    # MUTATION: hand the surface's own label through — `speaker_names[key] =
    # f"Speaker {label}"` — which renders `**Speaker A**`: still a label, still
    # readable, and invisible to the only thing that would ever fix it.
    harness = sessions(names={})

    with harness.rec:
        harness.write(1.0)
        harness.turn("nobody named me", label="A")
        harness.near("mm hm")

    text = harness.transcript_text()

    assert speaker_id_daemon_would_identify(text), (
        "no `**Speaker <digit>**` in the daemon's 2000-character head window: "
        "speaker_id_daemon.py:364-366 will count this transcript 'already "
        "tagged' and never identify the speaker"
    )
    # First appearance in the document is the unnamed far speaker, so the
    # emitter's first-appearance numbering makes it 1 — the §2.3 table's value.
    assert labels_of(text) == ["Speaker 1", OPERATOR]


def test_the_digit_survives_the_operator_speaking_first(sessions):
    """The same property under the ordering that actually happens on a call the
    operator initiates.

    The number itself is the emitter's business — `render_markdown` numbers ALL
    speakers in first-appearance order and deliberately does not renumber when a
    neighbour is named, so the operator speaking first makes the unnamed far
    speaker `2`. What must hold either way is that it is a DIGIT.
    """
    # MUTATION: number only the UNNAMED speakers, restarting at 1 — a second
    # implementation of the emitter's numbering rule, and the reason §2.2 rejects
    # post-render substitution.
    harness = sessions(names={})

    with harness.rec:
        harness.write(1.0)
        harness.near("hi, thanks for joining")
        harness.turn("no problem", label="A")

    text = harness.transcript_text()

    assert speaker_id_daemon_would_identify(text)
    generic = [label for label in labels_of(text) if label != OPERATOR]
    assert generic and all(re.fullmatch(r"Speaker \d+", label) for label in generic), generic


def test_a_named_speaker_does_not_hide_an_unnamed_one_from_the_daemon(sessions):
    """The mixed case the PoL runs: name one speaker, leave another unnamed. The
    daemon reads the document HEAD, so an unnamed speaker whose generic label only
    appears late is exactly as invisible as one rendered with a letter."""
    # MUTATION: emit `speaker_names` for every label, defaulting unnamed ones to
    # the far label's own text.
    harness = sessions(names={"A": "Dana"})

    with harness.rec:
        harness.write(1.0)
        harness.turn("Dana here", label="A")
        harness.turn("and I am the other one", label="B")

    text = harness.transcript_text()

    assert "**Dana**" in text
    assert speaker_id_daemon_would_identify(text), (
        "the unnamed second speaker carries no digit label inside the head window"
    )
    assert sorted(labels_of(text)) == ["Dana", "Speaker 2"]


def test_a_long_named_opening_pushes_the_digit_out_of_the_daemons_head_window(
    sessions,
):
    """CHARACTERISES AN OPEN DEFECT. Read the whole docstring before touching it.

    The daemon does not scan the document. It reads
    `transcript.read_text(encoding='utf-8')[:2000]` and searches THAT
    (`speaker_id_daemon.py:364-365`). A transcript whose first 2000 characters
    contain only NAMED speakers is counted `skipped += 1`, "already tagged" —
    even when an unnamed speaker renders `**Speaker 2**` further down.

    That is the same harm §2.3 exists to prevent, reached by another route. §2.3
    stopped a LETTER label from making an unnamed speaker invisible to the
    daemon; the head window makes an unnamed speaker invisible to it with the
    correct digit label, purely by where they first speak. It is reachable on an
    ordinary call: the near channel is the operator and the operator is always
    named, so any call whose first ~1750 characters are the operator talking —
    an intro, a demo, a status update before anyone answers — lands here.

    Nothing in `live_archive.py` is wrong. The module renders the digit it is
    supposed to render, and it is in the document. What is unsound is the
    consumer's fixed head window against a document whose speaker order it does
    not control, and no fix belongs in this module: it is `/investigate-bug`
    territory spanning two repos.

    WHEN THAT IS FIXED, THE SECOND ASSERTION BELOW WILL FAIL, AND THAT IS THE
    SIGNAL. Delete this test then — do not relax it.
    """
    # MUTATION: `_speaker_names` emitting a name for unnamed far labels (e.g.
    # `names[key] = f"Speaker {label}"`) — the document then carries no digit
    # label at all and the first assertion fails.
    harness = sessions(names={"A": "Dana"})
    monologue = (
        "so the way this works is the recording comes off the device over usb "
        "and then it gets transcribed, which is the part that used to take all "
        "morning and now takes about four minutes end to end "
    )

    with harness.rec:
        harness.write(1.0)
        # The operator opens the call, alone, for long enough to fill the window.
        for n in range(9):
            harness.near(f"{monologue} ({n})")
            harness.write(1.0)
        harness.turn("Dana here, sorry I was muted", label="A")
        harness.turn("and I am the other one, nobody named me", label="B")

    text = harness.transcript_text()

    hit = SPEAKER_ID_DAEMON_GENERIC_LABEL.search(text)
    assert hit, "the unnamed far speaker carries no digit label anywhere"
    assert hit.start() > SPEAKER_ID_DAEMON_HEAD_CHARS, (
        "this fixture no longer pushes the digit past the head window, so it "
        "characterises nothing — lengthen the opening or delete the test"
    )
    assert not speaker_id_daemon_would_identify(text), (
        "the daemon's head window now finds this document's digit label — the "
        "defect this test characterises is fixed, so delete this test"
    )


# ==========================================================================
# U-7 — no `Speaker <letter>` anywhere in the document
# ==========================================================================


def test_no_speaker_letter_label_appears_anywhere_in_the_output(sessions):
    """FR-2.3, asserted over the WHOLE document and not only the turn labels.

    The surface's vocabulary (`Speaker A`) is correct on screen and wrong in the
    archive, and it can leak in through more than the label position — a
    frontmatter value, a heading, a name defaulted from the provider label.
    """
    # MUTATION: `speaker_names[key] = f"Speaker {label}"` for unnamed far labels,
    # i.e. write the surface's own display string into the archive.
    harness = sessions(names={"B": "Priya"})

    with harness.rec:
        harness.write(1.0)
        harness.turn("first speaker, unnamed", label="A")
        harness.turn("second speaker, named", label="B")
        harness.turn("third speaker, unnamed", label="C")
        harness.near("and me")

    text = harness.transcript_text()

    assert not SPEAKER_LETTER_LABEL.search(text), (
        f"a letter-form speaker label reached the archive: "
        f"{SPEAKER_LETTER_LABEL.search(text).group(0)!r}"
    )
    for letter in ("A", "B", "C"):
        assert f"Speaker {letter}" not in text
    # And the document really did carry three far speakers, so the assertion
    # above is not vacuously true of an empty transcript.
    assert len(turn_lines(text)) == 4


# ==========================================================================
# U-8 — turn offsets are a byte position in OUR file, never a clock
# ==========================================================================


def test_turn_offsets_are_a_position_in_our_own_wav_and_never_a_clock(sessions):
    """§2.1 / §5. Recording the stream ourselves is what makes this arithmetic
    instead of inference: bytes written before a turn arrived, divided by the
    frame rate, is the offset into a file we opened.

    The test writes two minutes of audio inside a few milliseconds of real time,
    so a clock and the byte position disagree by the whole duration. An
    implementation that read `time.monotonic()` — or the wall clock, or the
    session's elapsed seconds — renders `(00:00)` for both turns.
    """
    # MUTATION: `start_ms = int((time.monotonic() - self._started_at) * 1000)`
    # instead of `int(self._samples_written / SAMPLE_RATE_HZ * 1000)`.
    harness = sessions()

    with harness.rec:
        harness.write(90.0)
        harness.turn("ninety seconds in", label="A")
        harness.write(30.0)
        harness.near("two minutes in")

    text = harness.transcript_text()

    assert offsets_of(text) == ["01:30", "02:00"], (
        f"offsets {offsets_of(text)} do not match the audio written before each "
        "turn arrived; something substituted a clock"
    )
    assert frontmatter(text)["duration_seconds"] == "120"
    assert frontmatter(text)["audio_duration_minutes"] == "2.00"


def test_both_channels_reach_one_interleaved_document(sessions):
    """FR-2.2. The two live sessions are separate provider streams and each
    numbers its own turns from zero; the archived document is ONE conversation,
    carrying both, each line stamped with the offset it arrived at.

    This test does NOT pin the sort — see the next one for why it cannot. Its
    MUTATION comment used to claim `utterances = near_turns + far_turns`, a shape
    that corresponds to no line in the implementation and that, on these turns,
    produces the same document anyway.
    """
    # MUTATION: `if turn.channel == LiveChannel.NEAR: continue` in the
    # `utterances` comprehension — the near end (the operator, i.e. half the
    # conversation) silently vanishes from every archived live transcript.
    harness = sessions()

    with harness.rec:
        harness.write(10.0)
        harness.turn("first", label="A")
        harness.write(10.0)
        harness.near("second")
        harness.write(10.0)
        harness.turn("third", label="A")

    text = harness.transcript_text()

    assert texts_of(text) == ["first", "second", "third"]
    assert offsets_of(text) == ["00:10", "00:20", "00:30"]


def test_the_merge_is_sorted_by_start_and_not_left_to_arrival_order(sessions):
    """FR-2.2's sort, pinned in the only state that can falsify it.

    The sort in `_build_response` is a no-op on every path this suite can drive,
    and the implementation says so itself: `_turns` is one insertion-ordered
    table and `_samples_written` only ever grows, so arrival order already IS
    start order and `list(turns)` renders an identical document. A test that
    drives turns through the bus therefore cannot tell the two apart, which is
    exactly what the previous version of this test did not do.

    So the table is put into the one state that disagrees — directly, through the
    module's own `_Turn`, because nothing else can produce it today. That is not
    a hypothetical: the requirement is "ordered by start", and the two properties
    making it free are stated nowhere a reader of that line would look. Either
    one moving (a second table, a counter that can be adopted downward from
    `getnframes()` after a partial write) takes the guarantee with it.
    """
    # MUTATION: `ordered = sorted(turns, key=lambda turn: turn.start_ms)` ->
    # `ordered = list(turns)` in `_build_response`.
    harness = sessions()

    harness.rec.__enter__()
    harness.write(30.0)
    harness.turn("spoken third, seen first", label="A")

    # A turn whose start is EARLIER than one already in the table. Insertion
    # order and start order now disagree, which is the whole point.
    early = live_archive_module._Turn(
        channel=LiveChannel.NEAR,
        turn_order=99,
        start_ms=5_000,
        text="spoken first, seen last",
        label=None,
    )
    harness.rec._turns[("near", 99)] = early

    harness.rec.stop()
    text = harness.transcript_text()

    assert texts_of(text) == ["spoken first, seen last", "spoken third, seen first"], (
        "the document is in arrival order, not start order"
    )
    assert offsets_of(text) == ["00:05", "00:30"]


def test_a_final_turn_keeps_the_offset_of_the_partial_it_replaces(sessions):
    """FR-2.2. `start` is when the turn STARTED. The bridge emits a partial as
    soon as a speaker begins and the final when they stop, so taking the final's
    arrival would time every turn at its END — and would also render the same
    sentence twice if the partial were kept alongside it."""
    # MUTATION: append every `LiveTurn` rather than replacing on the
    # `(channel, turn_order)` identity — the document gains a duplicate line per
    # turn, timed at the end of the sentence.
    harness = sessions()

    with harness.rec:
        harness.write(60.0)
        harness.turn("half a sen", label="A", order=0, final=False)
        harness.write(30.0)
        harness.turn("half a sentence, then the rest", label="A", order=0, final=True)

    text = harness.transcript_text()

    assert texts_of(text) == ["half a sentence, then the rest"]
    assert offsets_of(text) == ["01:00"]


# ==========================================================================
# U-9 — a capture that produced no audio writes nothing at all
# ==========================================================================


def test_zero_frames_writes_no_wav_no_transcript_and_raises_no_error(sessions):
    """FR-1.5. An empty WAV in the archive is a recording that never happened, and
    a transcript beside it asserts a call took place. Pressing `l` and pressing it
    again is an ordinary thing to do, not a failure — so no error either."""
    # MUTATION: open the WAV in `__enter__` rather than lazily on the first frame;
    # every accidental `l` press then leaves a 0-frame recording in the archive.
    harness = sessions()

    with harness.rec:
        pass

    assert harness.files() == [], f"a session with no audio wrote {harness.files()}"
    assert harness.rec.wav_path is None
    assert harness.rec.transcript_path is None
    assert harness.errors() == [], harness.messages()


def test_frames_carrying_no_audio_are_not_a_recording_either(sessions):
    """FR-1.5, the same rule at the frame level: a stream that produced only
    empty payloads produced no audio."""
    # MUTATION: treat "a frame arrived" as "audio arrived" — `if frame:` rather
    # than `if frame.near or frame.far:`.
    harness = sessions()

    with harness.rec:
        for seq in range(1, 6):
            harness.write_frame(Frame(near=b"", far=b"", seq=seq))

    assert harness.files() == []
    assert harness.rec.wav_path is None
    assert harness.errors() == []


def test_a_session_with_audio_but_no_turns_keeps_the_audio_and_writes_no_transcript(
    sessions,
):
    """The complement, and it runs in both directions.

    Nobody said anything the bridge could transcribe — a call that was all
    listening, or one where the bridge failed outright. The audio is the artifact
    that cannot be reconstructed, so it is kept.

    And NO transcript is written. An empty `.md` beside it would assert to the
    pipeline that this call has been transcribed, which stops anything from ever
    revisiting the audio — the recording survives and becomes unreachable, which
    is the worse half of the same failure. The `if not turns: return` guard is
    the only thing standing between those two outcomes, and asserting only the
    WAV left it freely deletable.
    """
    # MUTATION: `_safe_unlink(audio_path)` added to `stop()`'s `if not turns:`
    # branch — "an empty transcript is pointless, so bin the recording", which
    # deletes the only copy of a call that happened. Or DELETE that guard
    # entirely, and a call nobody spoke on is filed as transcribed for good.
    harness = sessions()

    with harness.rec:
        harness.write(1.0)

    assert harness.rec.wav_path is not None and harness.rec.wav_path.exists()
    assert read_wav(harness.rec.wav_path)[3] == samples_for(1.0)

    assert harness.rec.transcript_path is None, (
        "a transcript was claimed for a call with no turns"
    )
    assert harness.suffixed(".md") == [], (
        "an empty transcript was written; the pipeline reads that as 'already "
        "transcribed' and the recording is never revisited"
    )
    assert harness.suffixed(".aai.json") == []


# ==========================================================================
# U-10 — an unwritable archive costs the recording, never the session
# ==========================================================================


def test_an_unwritable_archive_leaves_the_session_running_and_names_the_path(
    sessions, tmp_path
):
    """FR-ERR-1 / FR-ERR-2 / §3.5. The transcript on screen is still worth having,
    so a bad archive path stops RECORDING and not the session — and says so once,
    naming the path, rather than once per frame at ten frames a second.

    The archive root is a regular file here, so `mkdir(parents=True)` fails the
    way a read-only mount or a Drive folder that has not mounted yet fails.
    """
    # MUTATION: let the `OSError` from `_unique_archive_path` propagate out of
    # `write()` — it unwinds the capture loop and kills the live session, which is
    # the one thing §3.5 says must not happen.
    blocked = tmp_path / "archive-is-a-file"
    blocked.write_text("not a directory\n")
    harness = sessions(archive_dir=blocked)

    with harness.rec:
        for _ in range(10):
            harness.write(0.1)
        harness.turn("the session kept going", label="A")

    errors = harness.errors()
    assert len(errors) == 1, (
        f"the failure was announced {len(errors)} times; at ~10 frames/sec that "
        f"buries the activity log: {harness.messages()}"
    )
    assert str(blocked) in errors[0].message, (
        f"the message does not name the path the operator has to fix: "
        f"{errors[0].message!r}"
    )
    assert errors[0].severity is Severity.ERROR
    assert errors[0].context == "live", (
        "the error is not routed to the live surface, which forwards only "
        "`context == 'live'` — so it never reaches the window the operator is "
        "looking at"
    )
    assert harness.rec.wav_path is None
    assert harness.rec.transcript_path is None


# ==========================================================================
# U-10b — the write that fails MID-SESSION (PRD §3.5 rows 2-3, FR-ERR-2/4)
# ==========================================================================
#
# U-10 above covers a failure at OPEN: the archive path is bad, nothing is ever
# recorded, and the session runs transcript-only. The other half — the archive
# was fine, twenty minutes of a call are already on disk, and THEN the mount
# drops or the volume fills — had no test at all. The ENOSPC branch, the terminal
# stop and the whole `except OSError` in `_append_locked` were deletable with the
# suite green.
#
# It is also where §3.5's "retry once" was implemented and then measured wrong.
# `wave.writeframes` is `writeframesraw` (which writes the payload and increments
# the writer's own frame count) followed by `_patchheader` (two more writes), and
# the flush comes after all of that. A failure past the first of those writes
# leaves the chunk COMMITTED — so handing it back to `writeframes` appends it
# twice while a locally-incremented counter records it once. Measured before the
# fix: 40.0s of audio in a file whose header claimed 30, every turn stamped ten
# seconds early, and not one error raised. The retry turned a transient write
# error into silent corruption of BOTH artifacts.


def test_a_write_that_fails_mid_session_stops_recording_once_and_keeps_what_landed(
    sessions, monkeypatch
):
    """FR-ERR-2 / §3.5 row 2. The mount drops twenty minutes into a call.

    Three things must hold at once, and each was independently deletable:
    recording STOPS (the writer is finished with, not retried per frame), the
    SESSION does not (the live transcript is still worth having), and the audio
    captured up to that moment is KEPT and described by a transcript. Announced
    ONCE, because `frames()` yields ~10 chunks/sec and a per-frame message buries
    the activity log inside a second.
    """
    # MUTATION: delete the `except OSError` in `_append_locked` — the error
    # unwinds `write()`, propagates into the capture loop, and kills the session.
    # Or drop `self._stop_recording_locked()` from that branch, and every
    # subsequent frame re-raises and re-announces.
    harness = sessions()
    failing_handle(monkeypatch, payload_size=payload_bytes(0.5), when="payload", nth=2)

    with harness.rec:
        harness.write(0.5)
        harness.turn("before the mount dropped", label="A")
        for _ in range(8):
            harness.write(0.5)
        harness.turn("and the session kept going", label="A")

    problems = [e for e in harness.errors() if e.severity is Severity.ERROR]
    assert len(problems) == 1, (
        f"one write failure produced {len(problems)} messages across nine "
        f"frames: {harness.messages()}"
    )
    assert str(harness.dir) in problems[0].message, (
        f"the message does not name the archive: {problems[0].message!r}"
    )
    assert problems[0].context == "live", (
        "the error never reaches the window the operator is looking at"
    )

    wav = harness.rec.wav_path
    assert wav is not None and wav.exists(), "the audio captured before the failure was lost"
    assert read_wav(wav)[3] == samples_for(0.5), (
        "recording did not stop at the failure; later frames kept being appended"
    )
    # The session outlived it: both turns are in the document.
    assert texts_of(harness.transcript_text()) == [
        "before the mount dropped",
        "and the session kept going",
    ]


def test_a_full_disk_is_named_as_a_full_disk_and_not_as_a_write_error(
    sessions, monkeypatch
):
    """FR-ERR-4. ENOSPC is neither transient nor retryable and the remedy is the
    operator's: free space. A generic "write error" sends them looking for a bug
    in the app instead, and `errno.ENOSPC` is the only signal that distinguishes
    the two — nothing downstream can recover it once it has been flattened into
    prose."""
    # MUTATION: `if exc.errno == errno.ENOSPC:` -> `return
    # self._write_failed_message(exc)` for every errno, collapsing the two
    # branches into one message.
    harness = sessions()
    failing_handle(
        monkeypatch,
        payload_size=payload_bytes(0.5),
        when="payload",
        nth=2,
        error=OSError(errno.ENOSPC, "No space left on device"),
    )

    with harness.rec:
        harness.write(0.5)
        for _ in range(3):
            harness.write(0.5)
        harness.turn("the call carried on", label="A")

    problems = [e for e in harness.errors() if e.severity is Severity.ERROR]
    assert len(problems) == 1, harness.messages()
    message = problems[0].message
    assert "full" in message.lower(), (
        f"a full disk is announced as something else: {message!r}"
    )
    assert str(harness.dir) in message
    assert "free space" in message.lower(), (
        f"the message does not say what the operator is supposed to do: {message!r}"
    )
    # And it is not the generic write-error line, which says the opposite thing
    # about what will happen next.
    assert "write error" not in message.lower()
    assert harness.rec.wav_path is not None and harness.rec.wav_path.exists()


def test_a_payload_the_writer_already_committed_is_never_written_a_second_time(
    sessions, monkeypatch
):
    """THE fix this section exists for. `wave.writeframes` commits in stages, and
    a failure after the payload write leaves the audio ON DISK and COUNTED by the
    writer while the call still raises.

    §3.5 says "retry once", and implemented as a re-write that is silent
    corruption: the payload lands twice, the object's own counter records it
    once, the header disagrees with the file, and every turn is stamped early by
    the length of the duplicated chunk. Nothing raises. Both artifacts are wrong
    and neither says so.

    The failure is injected at the header patch — i.e. AFTER `writeframesraw`
    both wrote the data and incremented the frame count — because that is the
    only place the two can disagree, and it is the place the retry was measured
    at 80000 frames on disk against 64000 counted.
    """
    # MUTATION: add `writer.writeframes(payload)` at the top of `_append_locked`'s
    # `except OSError` block — §3.5's "retry once", read literally.
    harness = sessions()
    failing_handle(
        monkeypatch, payload_size=payload_bytes(1.0), when="after-payload", nth=2
    )

    with harness.rec:
        harness.write(1.0)
        harness.write(1.0)
        harness.turn("two seconds of audio, not three", label="A")

    wav = harness.rec.wav_path
    assert wav is not None
    on_disk = read_wav(wav)[3]
    assert on_disk == samples_for(2.0), (
        f"{on_disk} frames on disk for {samples_for(2.0)} frames handed in; the "
        "chunk that failed was appended more than once"
    )
    # The counter the transcript is stamped from agrees with the file, which is
    # the property the duplicate broke — not the frame count on its own.
    meta = frontmatter(harness.transcript_text())
    assert meta["duration_seconds"] == str(on_disk // SAMPLE_RATE_HZ) == "2"


def test_the_frame_count_after_a_failed_write_is_the_writers_own_account(
    sessions, monkeypatch
):
    """The other side of the same fix: where the count comes from.

    When the failure lands ON the payload write, nothing is committed and the
    writer's frame count does not move. A counter incremented locally from the
    length of the payload we HANDED IN would count it anyway — and every turn
    after that point would be stamped a chunk late against a file that is a chunk
    shorter. `Wave_write.getnframes()` is the writer's own account of what it
    committed and cannot disagree with the file.

    Both directions of the disagreement are therefore covered: the test above
    fails after the commit, this one fails before it, and the same line has to be
    right for both.
    """
    # MUTATION: `self._samples_written = writer.getnframes()` ->
    # `self._samples_written += len(payload) // BYTES_PER_WAV_FRAME` in the
    # `except OSError` branch.
    harness = sessions()
    failing_handle(monkeypatch, payload_size=payload_bytes(1.0), when="payload", nth=2)

    with harness.rec:
        harness.write(1.0)
        harness.write(1.0)  # never lands
        harness.turn("one second of audio, not two", label="A")

    wav = harness.rec.wav_path
    on_disk = read_wav(wav)[3]
    assert on_disk == samples_for(1.0), (
        f"{on_disk} frames on disk; the payload that failed was committed anyway"
    )
    meta = frontmatter(harness.transcript_text())
    assert meta["duration_seconds"] == "1", (
        f"the transcript describes {meta['duration_seconds']}s of audio for a "
        f"file holding {on_disk / SAMPLE_RATE_HZ}s"
    )
    # The turn arrived after the failed write, so it is stamped at the end of the
    # audio that really exists — not at the end of the audio we tried to write.
    assert offsets_of(harness.transcript_text()) == ["00:01"]


def test_a_first_write_that_fails_leaves_no_recording_and_claims_none(
    sessions, monkeypatch
):
    """FR-1.5. Opening is not recording.

    A `wave` handle opened at the first frame and never successfully written
    leaves a 0-byte file. `wave.open` on one of those raises `EOFError`, so it is
    an unopenable "recording" — and until this was fixed it came with a
    transcript and an `.aai.json` describing it, and a controller announcing the
    session as saved. Three artifacts asserting a call that has no audio at all.

    The path only becomes `audio_path` once the writer reports frames landed, and
    the file that was opened and never written is unlinked.
    """
    # MUTATION: `self._audio_path = path` in `_open_locked` (i.e. promote at OPEN
    # rather than at the first successful write), or drop the `_safe_unlink` from
    # `_close_writer_locked`'s never-written branch.
    harness = sessions()
    failing_handle(monkeypatch, payload_size=payload_bytes(0.5), when="payload", nth=1)

    with harness.rec:
        for _ in range(4):
            harness.write(0.5)
        harness.turn("said, but never recorded", label="A")

    assert harness.rec.wav_path is None, (
        "a recording was claimed for a file nothing ever wrote to"
    )
    assert harness.rec.audio_path is None
    assert harness.rec.transcript_path is None
    assert harness.files() == [], (
        f"a 0-byte recording (or a sidecar describing one) was left in the "
        f"archive: {harness.files()}"
    )
    problems = [e for e in harness.errors() if e.severity is Severity.ERROR]
    assert len(problems) == 1, harness.messages()
    assert str(harness.dir) in problems[0].message


# ==========================================================================
# U-11 — a transcript failure never costs the audio
# ==========================================================================


@pytest.mark.parametrize("failure", ["render raises", "transcript path unwritable"])
def test_a_transcript_failure_leaves_the_wav_intact(sessions, monkeypatch, failure):
    """FR-ERR-3. Audio without a transcript is recoverable — re-render it, or run
    it through the batch path. A transcript without audio is not: the device never
    persisted the live session, so our file is the only copy that exists.

    Both halves of "write the transcript" are covered, because they fail
    independently: the render can raise on a data shape, and the write can raise
    on the filesystem.

    The write half used to be provoked by putting a DIRECTORY where the `.md`
    goes. That stopped being a write failure the moment sidecars started
    de-conflicting: `_sidecar_target` sees the name is taken, mints
    `<stem>-1.md`, and the write succeeds — so the parametrisation was silently
    testing the collision path and asserting nothing about a failed write. The
    directory is made unwritable instead, which is how a read-only mount and a
    Drive folder that dropped mid-call both fail.
    """
    # MUTATION: `_safe_unlink(audio_path)` added to `_write_transcript`'s render
    # `except` — the "clean up after yourself" reflex, which here deletes the
    # only recording of the call. (On the unwritable-directory parametrisation
    # that unlink cannot succeed either, so the second mutation for it is
    # `self._transcript_path = target` hoisted above the `try`, which claims a
    # transcript that was never written — for both parametrisations.)
    harness = sessions()
    subdir = harness.dir / FIXED_SUBDIR

    if failure == "render raises":
        def exploding(transcript, **kwargs):
            inspect.signature(render_markdown).bind(transcript, **kwargs)
            raise ValueError("utterance 3 has no speaker")

        monkeypatch.setattr(live_archive_module, "render_markdown", exploding)

    harness.rec.__enter__()
    harness.write(1.5)
    harness.turn("said out loud, exactly once", label="A")

    if failure == "transcript path unwritable":
        # Read-only, and only from here: the WAV is already on disk, so this
        # lands on the transcript write and on nothing before it.
        subdir.chmod(0o555)
    try:
        harness.rec.stop()
    finally:
        subdir.chmod(0o755)

    wav = harness.rec.wav_path
    assert wav is not None and wav.exists(), "the recording was deleted"
    assert read_wav(wav)[3] == samples_for(1.5), "the recording was truncated"
    assert harness.suffixed(".md") == [], "a partial transcript was left behind"
    assert harness.rec.transcript_path is None, (
        "a transcript path was claimed for a transcript that was never written"
    )
    assert harness.errors(), "the transcript failure was never surfaced"
    assert wav.name in harness.messages(), (
        f"the message does not name the audio that survived: {harness.messages()!r}"
    )


def test_the_transcript_is_written_once_at_stop_and_not_as_turns_arrive(sessions):
    """FR-2.5. A partial transcript in the archive is indexed by the pipeline as a
    complete one — the call is then permanently represented by its first two
    minutes, and nothing ever revisits it."""
    # MUTATION: render and write inside the `LiveTurn` handler.
    harness = sessions()

    harness.rec.__enter__()
    harness.write(1.0)
    harness.turn("mid-session", label="A")
    harness.write(1.0)

    assert harness.suffixed(".md") == [], "a transcript existed before the session ended"
    assert harness.rec.transcript_path is None

    harness.rec.__exit__(None, None, None)

    assert len(harness.suffixed(".md")) == 1
    written = harness.transcript_text()
    assert "mid-session" in written


def test_the_recorder_lets_go_of_the_bus_and_refuses_late_turns_independently(
    sessions,
):
    """Two separate defences, and both were unpinned.

    The old assertion here re-read the `.md` after publishing a turn and checked
    the bytes had not changed — but NOTHING rewrites that file after stop, so it
    holds however the module behaves. The unsubscribe and the `_stopped` guard
    were each deletable with the whole suite green.

    They defend different things and neither implies the other:

    * **Unsubscribe** is about the NEXT session. A recorder that stayed
      subscribed keeps receiving every turn of every later call — an object that
      holds one call's audio path and another call's words, retained for as long
      as the bus is.
    * **The `_stopped` guard** is about THIS one. `EventBus.publish` calls
      subscribers under its own lock while `stop()` runs on another thread, so a
      turn already in flight can reach `_on_event` after the table has been
      snapshotted. It lands in a table nothing will ever render — the sentence
      is silently dropped rather than reaching a document — but the object also
      keeps growing after it is finished with.

    So they are asserted separately: the bus is asked what it still holds, and
    the handler is called the way the race calls it.
    """
    # MUTATION: delete `self._bus.unsubscribe(self._on_event)` from `stop()`
    # (first assertion), or the `if self._stopped: return` guard at the top of
    # `_record_turn` (second).
    harness = sessions()

    with harness.rec:
        harness.write(1.0)
        harness.turn("mid-session", label="A")

    # 1. The bus no longer holds this recorder at all.
    still_subscribed = [
        callback
        for callback in _bus_subscribers(harness.bus)
        if getattr(callback, "__self__", None) is harness.rec
    ]
    assert still_subscribed == [], (
        "the recorder is still subscribed after stop; every turn of every later "
        "session goes on reaching a finished object"
    )

    # 2. And the handler itself refuses a turn that arrives anyway — the race a
    #    concurrent `publish` really produces, driven deterministically.
    before = dict(harness.rec._turns)
    harness.rec._on_event(
        LiveTurn(
            channel=LiveChannel.FAR,
            text="after the session ended",
            speaker="A",
            turn_order=77,
            is_final=True,
        )
    )
    assert harness.rec._turns == before, (
        "a turn that arrived after stop was recorded into a table nothing will "
        "ever render"
    )
    assert "after the session ended" not in harness.transcript_text()


def test_stop_is_idempotent_and_renders_exactly_once(sessions, monkeypatch):
    """Stop arrives three times on an ordinary session: the `l` keystroke, then
    `__exit__`, then the shutdown handler. Each extra render rewrites a file the
    Drive mount may already have uploaded — bumping its mtime and re-syncing it —
    and does so from state a previous teardown has already dismantled.

    Counted at the emitter rather than inferred from the file, because a second
    render writes the same bytes to the same path: the damage is invisible in the
    result and visible only in the fact that it happened.
    """
    # MUTATION: drop the `if self._stopped: return` guard at the top of `stop()`.
    calls: List[tuple] = []
    _recording_emitter(monkeypatch, calls)
    harness = sessions()

    with harness.rec:
        harness.write(1.0)
        harness.turn("once", label="A")

    first = harness.transcript_text()
    harness.rec.stop()
    harness.rec.stop()

    assert len(calls) == 1, f"the transcript was rendered {len(calls)} times"
    assert len(harness.suffixed(".md")) == 1
    assert len(harness.suffixed(".wav")) == 1
    assert harness.transcript_text() == first


# ==========================================================================
# U-12 — the frontmatter is the batch format, key for key
# ==========================================================================


def test_the_frontmatter_keys_match_the_batch_format_exactly(sessions):
    """FR-2.1 / §12. The pipeline reads batched and live transcripts with the same
    code; a key that is missing, extra, or reordered here is a live-only shape
    nothing downstream was written against.

    The expected list is derived from the EMITTER — rendered here from a
    batch-shaped response — and cross-checked against `BATCH_FRONTMATTER_KEYS`,
    which was read off a real archived transcript. Two independent derivations of
    the same list, so a change in either is visible.
    """
    # MUTATION: add `live: true` to the frontmatter by post-processing the
    # emitter's output — a reasonable-sounding provenance marker that makes the
    # live document a different shape from every other file in the archive.
    batch = render_markdown(
        {
            "id": "batch-transcript-id",
            "audio_duration": 120,
            "language_code": "en_us",
            "utterances": [{"speaker": "A", "start": 0, "text": "batched"}],
            "auto_highlights_result": None,
        },
        source_filename="2026-08-27_143205.wav",
        recorded_at=FIXED_START.astimezone(),
    )
    assert frontmatter_keys(batch) == BATCH_FRONTMATTER_KEYS, (
        "the emitter no longer produces the key list read off the archive on "
        "2026-08-27; re-derive BATCH_FRONTMATTER_KEYS from a real transcript"
    )

    harness = sessions(names={"A": "Dana"})
    with harness.rec:
        harness.write(2.0)
        harness.turn("live", label="A")

    assert frontmatter_keys(harness.transcript_text()) == BATCH_FRONTMATTER_KEYS


def test_recorded_at_carries_a_timezone_and_the_source_filename_is_our_wav(sessions):
    """§11. `recorded_at` is DIRECT on this path — we open the file, so we know
    when the recording started — which is exactly what it was not across the 670
    batched transcripts that took it from an mtime.

    A naive datetime renders an offset-less `isoformat()`, which reads as UTC to
    anything that parses it and is wrong by the local offset. `source_filename`
    must name the file this transcript actually describes, `-1` suffix and all.
    """
    # MUTATION: pass the naive `clock()` value straight to `render_markdown`
    # instead of normalising with `.astimezone()`, the way `pipeline.py` does at
    # its single choke point.
    harness = sessions()

    with harness.rec:
        harness.write(1.0)
        harness.turn("hello", label="A")

    meta = frontmatter(harness.transcript_text())

    assert re.fullmatch(r"2026-08-27T14:32:05[+-]\d{2}:\d{2}", meta["recorded_at"]), (
        f"recorded_at is not a timezone-aware isoformat: {meta['recorded_at']!r}"
    )
    assert meta["source_filename"] == harness.rec.wav_path.name
    assert harness.rec.transcript_path.parent == harness.rec.wav_path.parent
    assert harness.rec.transcript_path.stem == harness.rec.wav_path.stem


def test_the_assemblyai_id_is_never_blank_and_resolves_to_itself_in_the_index(
    sessions,
):
    """`render.py:62` emits `assemblyai_id: {id or ''}`, so a `None` id rendered a
    BLANK field — a shape present in zero of the 2209 archived transcripts.

    A blank field does not fail the operator's index. It SUCCEEDS, on the wrong
    line: `hinotes_index.py`'s `^assemblyai_id:\\s*(.+?)\\s*$` under `re.M` has
    `\\s*` consume the newline after the empty value and captures whatever comes
    next, filing every live call under the literal id `language_code: null` —
    every one of them under the SAME id, so they collide with each other as well
    as being wrong. That is why the assertion is made through the consumer's own
    regex and not by checking the field is non-empty: non-empty is not the
    property, resolving to itself is.

    Nothing publishes a provider id today (`LiveTranscriptionStarted` carries
    only channel names), so the field carries `live-<archive stem>`: unique
    because the stem is de-conflicted, identical across the `.md` and its
    `.aai.json`, and self-describing enough that anything trying to resolve it
    against AssemblyAI fails loudly rather than quietly resolving another job.
    """
    # MUTATION: `return LIVE_ID_PREFIX + transcript_path.stem` -> `return None`
    # (or `""`), which is what shipped before this was found.
    harness = sessions()

    with harness.rec:
        harness.write(1.0)
        harness.turn("a live call the index has to find", label="A")

    text = harness.transcript_text()
    stem = harness.rec.transcript_path.stem

    # Read exactly the way the index reads it: the head of the file, `re.M`.
    head = text[:3000]
    assert head.startswith("---\n"), "the index skips anything without a fence"
    match = HINOTES_INDEX_ASSEMBLYAI_ID.search(head)
    assert match, "the index finds no assemblyai_id at all in a live transcript"
    captured = match.group(1).strip().strip('"\'')

    assert captured == f"{LIVE_ID_PREFIX}{stem}", (
        f"the operator's index resolves this transcript as {captured!r}"
    )
    assert "language_code" not in captured, (
        "the id field is empty, so the index captured the NEXT frontmatter line "
        "and every live call is filed under the same wrong id"
    )
    assert frontmatter(text)["assemblyai_id"] == captured

    # The parity file carries the same id, so the two describe one transcript.
    sidecar = harness.suffixed(RAW_RESPONSE_SUFFIX)[0]
    assert json.loads(sidecar.read_text(encoding="utf-8"))["id"] == captured


def test_the_raw_response_is_written_beside_the_transcript_for_archival_parity(
    sessions,
):
    """FR-2.6. `.aai.json` is best-effort parity: `sync_sales_archive.py` only
    `shutil.copy2`s it and treats absence as a warning, so nothing parses it. It
    is written because every other recording in the archive has one, and a live
    call that did not would be the odd one out for no reason."""
    # MUTATION: delete the `.aai.json` write.
    harness = sessions()

    with harness.rec:
        harness.write(1.0)
        harness.turn("recorded", label="A")

    sidecars = harness.suffixed(".aai.json")
    assert len(sidecars) == 1, f"expected one .aai.json, got {sidecars}"
    assert sidecars[0].stem == harness.rec.wav_path.stem + ".aai"
    payload = json.loads(sidecars[0].read_text(encoding="utf-8"))
    assert [u["text"] for u in payload["utterances"]] == ["recorded"]


def test_a_failed_parity_file_is_a_warning_that_names_it_and_never_an_error(
    sessions, monkeypatch
):
    """FR-2.6, the branch nobody asserted. `.aai.json` is parity: nothing parses
    it, `sync_sales_archive.py` only `shutil.copy2`s it and treats absence as a
    warning. So a failure here must be

    * not an ERROR — the recording and the transcript are both intact and there
      is nothing for the operator to do about a file nothing reads, and an ERROR
      on the live surface during a call reads as "the call went wrong";
    * not SILENT — the archive now has one recording that is shaped differently
      from the other 2209, and a difference nobody was told about is discovered
      later by whoever is debugging something else;
    * naming the file, and saying explicitly that the two artifacts that matter
      survived.

    Only that middle severity is correct, and neither edge was pinned, so both
    were available.
    """
    # MUTATION: `except OSError: pass` around the `.aai.json` write (silence), or
    # `Severity.ERROR` in that publish (alarm). Both keep every other test green.
    # The failure has to land on the WRITE, and only on the parity file. Making
    # the directory unwritable would take the `.md` with it, and occupying the
    # name with a directory would take the `target.exists()` branch instead —
    # which publishes a WARNING of its own, so the test would pass through the
    # wrong branch and this one would stay deletable.
    real_replace = live_archive_module.os.replace
    parity_name = f"{FIXED_BASENAME}{RAW_RESPONSE_SUFFIX}"

    def replace_failing_only_the_parity_file(src, dst, *args, **kwargs):
        if Path(dst).name == parity_name:
            raise OSError(errno.EIO, "input/output error")
        return real_replace(src, dst, *args, **kwargs)

    monkeypatch.setattr(
        live_archive_module.os, "replace", replace_failing_only_the_parity_file
    )

    harness = sessions()
    with harness.rec:
        harness.write(1.0)
        harness.turn("recorded", label="A")

    assert harness.rec.transcript_path is not None, "the transcript was lost too"
    assert "recorded" in harness.transcript_text()

    about_parity = [e for e in harness.errors() if parity_name in e.message]
    assert about_parity, (
        f"the parity failure was silent; the archive now holds a live recording "
        f"shaped unlike every other one and nothing said so: {harness.messages()!r}"
    )
    assert all(e.severity is Severity.WARNING for e in about_parity), (
        f"a file nothing reads was announced as an ERROR: "
        f"{[e.severity for e in about_parity]}"
    )
    assert any(
        "transcript" in e.message and "recording" in e.message for e in about_parity
    ), f"the message does not say what survived: {[e.message for e in about_parity]}"
    # And nothing partial was left where the parity file goes.
    assert harness.suffixed(".aai.json") == []


def test_an_existing_paid_raw_response_is_left_alone_and_the_operator_is_told(
    sessions,
):
    """FR-2.6 / §2.1. A `.aai.json` at the stem we want belongs to a device
    recording of the same second, and somebody PAID for it — it is the provider's
    own response for a batch transcription. Ours is parity for a file nothing
    reads. Parity is never worth destroying a paid artifact, so it is left
    exactly as it stands, and the operator is told rather than left to discover a
    replaced response later."""
    # MUTATION: drop the `if target.exists()` guard in `_write_raw_response` —
    # `_atomic_write_text` ends in `os.replace`, which happily overwrites.
    harness = sessions()
    paid = harness.dir / FIXED_SUBDIR / f"{FIXED_BASENAME}.aai.json"
    paid.parent.mkdir(parents=True, exist_ok=True)
    paid.write_text('{"id": "a-paid-assemblyai-response"}', encoding="utf-8")

    with harness.rec:
        harness.write(1.0)
        harness.turn("the live call", label="A")

    assert json.loads(paid.read_text(encoding="utf-8")) == {
        "id": "a-paid-assemblyai-response"
    }, "a paid AssemblyAI response was overwritten by a parity file"
    warnings = [
        e for e in harness.errors()
        if paid.name in e.message and e.severity is Severity.WARNING
    ]
    assert len(warnings) == 1, f"the operator was not told: {harness.messages()!r}"


def test_a_sidecar_is_staged_under_a_name_the_pipeline_would_never_index(
    sessions, monkeypatch
):
    """`_atomic_write_text`'s stated property: "the temp name deliberately does
    not end in the target's suffix, so a failed write never leaves something the
    pipeline would index as a transcript".

    Writing straight to the target passes every other test in this file — the
    happy path produces identical bytes at an identical path. What it changes is
    what a machine that dies mid-write leaves behind: a `.md` holding the first
    half of a call, which `hinotes_index.py` reads as a complete transcript and
    files under whatever `assemblyai_id` it can see. The pipeline has no notion
    of a partial transcript, so there is no later correction.

    Asserted at the `os.replace` the staging implies, and then again on the
    failure path — a replace that raises must leave the directory with nothing
    indexable in it.
    """
    # MUTATION: `open(path, "w")` + `handle.write(text)` directly in
    # `_atomic_write_text`, dropping the temp file and the `os.replace`.
    seen: List[tuple] = []
    real_replace = live_archive_module.os.replace

    def recording_replace(src, dst, *args, **kwargs):
        seen.append((Path(src), Path(dst)))
        return real_replace(src, dst, *args, **kwargs)

    monkeypatch.setattr(live_archive_module.os, "replace", recording_replace)

    harness = sessions()
    with harness.rec:
        harness.write(1.0)
        harness.turn("staged", label="A")

    assert seen, (
        "nothing was staged through os.replace; a write interrupted halfway "
        "leaves a partial file at the name the pipeline indexes"
    )
    for src, dst in seen:
        assert src.parent == dst.parent, f"the temp file is not a neighbour: {src}"
        assert src != dst
        for indexed in (TRANSCRIPT_SUFFIX, RAW_RESPONSE_SUFFIX, ".mp3", ".wav"):
            assert not src.name.endswith(indexed), (
                f"the staging name {src.name!r} ends in {indexed!r}, so an "
                "interrupted write leaves something downstream would pick up"
            )
    assert {dst.name for _src, dst in seen} == {
        f"{FIXED_BASENAME}.md",
        f"{FIXED_BASENAME}.aai.json",
    }


def test_a_replace_that_fails_leaves_nothing_the_pipeline_would_index(
    sessions, monkeypatch
):
    """The other half of the same property, on the path that motivates it."""
    # MUTATION: drop the `_safe_unlink(tmp)` from `_atomic_write_text`'s
    # `except OSError` — the `.md.tmp` survives, which is harmless, until the
    # temp name is changed to `.tmp.md` or the suffix rule is forgotten.
    def exploding_replace(src, dst, *args, **kwargs):
        raise OSError(errno.EIO, "input/output error")

    monkeypatch.setattr(live_archive_module.os, "replace", exploding_replace)

    harness = sessions()
    with harness.rec:
        harness.write(1.0)
        harness.turn("never landed", label="A")

    left = [p.name for p in harness.files()]
    assert [name for name in left if name.endswith(TRANSCRIPT_SUFFIX)] == [], left
    assert [name for name in left if name.endswith(RAW_RESPONSE_SUFFIX)] == [], left
    assert [name for name in left if name.endswith(_PARTIAL_SUFFIX)] == [], (
        f"a staging file was left in the archive: {left}"
    )
    # The recording is untouched by any of it.
    assert harness.rec.wav_path is not None and harness.rec.wav_path.exists()
    assert harness.rec.transcript_path is None


@pytest.mark.parametrize("occupied", [TRANSCRIPT_SUFFIX, RAW_RESPONSE_SUFFIX])
def test_a_sidecar_never_replaces_one_that_is_already_there(sessions, occupied):
    """§2.1's "both land side by side" was only ever true of the AUDIO.

    `_unique_archive_path` de-conflicts ONE name, and the two producers use
    different audio extensions — the device writes `.mp3`, this module wrote
    `.wav` while the call ran — so a stem that is free for the audio says nothing
    about the `.md` and the `.aai.json` beside it. Written at the un-suffixed
    stem with `os.replace`, a same-second collision silently destroyed a device
    recording's transcript and its PAID `.aai.json`.

    The `-1` comes from offload's own helper, so a de-conflicted transcript is
    named by the same convention as a de-conflicted recording rather than by a
    second scheme invented here.
    """
    # MUTATION: `target = audio_path.with_name(audio_path.stem + suffix)` returned
    # unconditionally from `_sidecar_target`, i.e. drop the `target.exists()`
    # branch.
    harness = sessions()
    squatter = harness.dir / FIXED_SUBDIR / f"{FIXED_BASENAME}{occupied}"
    squatter.parent.mkdir(parents=True, exist_ok=True)
    squatter.write_text("the device recording's own sidecar", encoding="utf-8")

    with harness.rec:
        harness.write(1.0)
        harness.turn("the live call", label="A")

    assert squatter.read_text(encoding="utf-8") == "the device recording's own sidecar"

    if occupied == TRANSCRIPT_SUFFIX:
        assert harness.rec.transcript_path.name == f"{FIXED_BASENAME}-1.md", (
            f"the transcript did not de-conflict: {harness.rec.transcript_path}"
        )
        # The parity file follows the TRANSCRIPT it belongs to, not the audio —
        # otherwise a de-conflicted transcript leaves its `.aai.json` on the
        # un-suffixed stem, on top of whatever is already there.
        assert harness.suffixed(RAW_RESPONSE_SUFFIX) == [
            harness.dir / FIXED_SUBDIR / f"{FIXED_BASENAME}-1.aai.json"
        ]
        assert harness.rec.wav_path.name == f"{FIXED_BASENAME}.wav", (
            "the audio was de-conflicted by a collision that was not its own"
        )
        warned = [e for e in harness.errors() if squatter.name in e.message]
        assert warned and warned[0].severity is Severity.WARNING, (
            f"the operator was not told the transcript moved: {harness.messages()!r}"
        )
    else:
        # A `.aai.json` collision does not move the transcript: nothing reads the
        # parity file, so the un-suffixed `.md` is still the right name for it.
        assert harness.rec.transcript_path.name == f"{FIXED_BASENAME}.md"


# ==========================================================================
# U-12b — the archived file is an MP3 (§2.2b, FR-2.2b .. FR-2.2f)
# ==========================================================================
#
# Two independent reasons, and the second is the one that bites.
#
# SIZE, measured not assumed: the device's own files run 96 kbps / 43 MB per
# hour across 769 recordings; our 16 kHz stereo PCM is 512 kbps / 230 MB per
# hour. 5.3x, into a Drive-synced folder, so it costs storage on two machines and
# the bandwidth between them.
#
# CORRECTNESS: the archive holds 795 `.mp3` and zero `.wav`, and the pipeline
# hardcodes `<stem>.mp3` in five places including `auto_speaker_id.py:3869`. A
# `.wav` is therefore a live call voice-print matching can never LOCATE — which
# silently undoes the capability §2.3 spent this whole PRD preserving. That is
# why "no encoder" is a WARNING with a consequence in it and not a log line.
#
# The WAV is an intermediate: recorded incrementally while the call runs,
# transcoded once at stop, and deleted only after the MP3 exists and is
# non-empty. There is never a moment with neither file, and every failure path
# keeps the WAV — audio is the artifact that cannot be reconstructed.


def test_an_installed_encoder_archives_the_call_as_an_mp3_and_removes_the_wav(
    sessions, encoder
):
    """FR-2.2b / FR-2.2d. The MP3 is the artifact; the WAV was scaffolding."""
    # MUTATION: return `wav_path` unconditionally from `_finalise_audio` (i.e.
    # never transcode) — every live call is then 5.3x its size and invisible to
    # voice-print matching, and nothing anywhere says so.
    encoder.install("ok")
    harness = sessions()

    with harness.rec:
        harness.write(2.0)
        harness.turn("archived as mp3", label="A")

    audio = harness.rec.audio_path
    assert audio is not None and audio.exists()
    assert audio.name == f"{FIXED_BASENAME}.mp3", f"archived as {audio.name}"
    assert audio.stat().st_size > 0
    assert harness.suffixed(".wav") == [], (
        f"the intermediate WAV is still in the archive: {harness.suffixed('.wav')}"
    )
    # `wav_path` is the retained spelling the controller reads; it must name the
    # survivor, not a file this module has already deleted.
    assert harness.rec.wav_path == audio

    # And every derived name follows the survivor.
    meta = frontmatter(harness.transcript_text())
    assert meta["source_filename"] == f"{FIXED_BASENAME}.mp3", (
        "the transcript names a file that is not in the archive, so voice-print "
        "matching resolves `<stem>.mp3` against nothing"
    )
    assert harness.rec.transcript_path.name == f"{FIXED_BASENAME}.md"
    assert [p.name for p in harness.suffixed(RAW_RESPONSE_SUFFIX)] == [
        f"{FIXED_BASENAME}.aai.json"
    ]
    # A successful transcode is not news.
    assert harness.errors() == [], harness.messages()


def test_the_encoder_is_asked_for_the_archives_own_bitrate_and_a_neutral_temp_name(
    sessions, encoder
):
    """FR-2.2b. 96 kbps is read off the device's own files, so a live call is the
    same size class as a batched one rather than a new one — and the target it is
    handed is a TEMP name that does not end in `.mp3`, so an interrupted or
    killed encode never leaves something the pipeline would index as a
    recording."""
    # MUTATION: drop `-b`/`str(MP3_BITRATE_KBPS)` from `_lame_command`, or hand
    # `target` to the encoder directly instead of `tmp`.
    encoder.install("ok")
    harness = sessions()

    with harness.rec:
        harness.write(1.0)
        harness.turn("encoded once", label="A")

    argv = encoder.argv()
    assert argv, "the encoder was never run"
    assert encoder.invocations() == 1, f"the encoder ran {encoder.invocations()} times"
    assert str(MP3_BITRATE_KBPS) in argv, (
        f"the archive's 96 kbps never reached the encoder's argv: {argv}"
    )
    source, target = Path(argv[-2]), Path(argv[-1])
    assert source.name == f"{FIXED_BASENAME}.wav"
    assert not target.name.endswith(".mp3"), (
        f"the encoder writes straight to an indexable name: {target.name}"
    )
    assert target.name.startswith(f"{FIXED_BASENAME}.mp3")
    assert target.parent == harness.rec.audio_path.parent
    # And nothing partial survived the successful run.
    assert [p.name for p in harness.files() if _PARTIAL_SUFFIX in p.name] == []


def test_no_encoder_keeps_the_wav_and_names_the_consequence_once(sessions):
    """FR-2.2e. Absence is a supported configuration — this is a public
    clone-and-run app and an encoder is a system tool, not a dependency — so it
    is not an ERROR and it does not cost the recording.

    It is also not silent. The operator now has a file the pipeline's hardcoded
    `<stem>.mp3` cannot find, and they would otherwise discover that months later
    as a call nobody was identified on. The message has to say what happened, why
    it matters, and what makes the next one an MP3.
    """
    # MUTATION: `return wav_path, None` when `_find_encoder()` is None — the WAV
    # is kept, everything "works", and the operator is never told the call is
    # invisible to speaker identification.
    harness = sessions()  # the autouse fixture removes both encoders

    with harness.rec:
        harness.write(1.0)
        harness.turn("kept as wav", label="A")

    audio = harness.rec.audio_path
    assert audio.name == f"{FIXED_BASENAME}.wav" and audio.exists()
    assert read_wav(audio)[3] == samples_for(1.0)
    assert frontmatter(harness.transcript_text())["source_filename"] == audio.name

    notices = [e for e in harness.errors() if audio.name in e.message]
    assert len(notices) == 1, f"expected one notice, got {harness.messages()!r}"
    assert notices[0].severity is Severity.WARNING, (
        "a supported configuration was announced as an error"
    )
    lowered = notices[0].message.lower()
    for name in _MP3_ENCODERS:
        assert name in lowered, f"the message does not name {name}: {lowered!r}"
    assert "voice" in lowered or "identif" in lowered, (
        f"the message does not name the consequence: {notices[0].message!r}"
    )


@pytest.mark.parametrize("behaviour", ["fail", "empty", "missing"])
def test_a_transcode_that_does_not_produce_a_real_mp3_never_costs_the_wav(
    sessions, encoder, behaviour
):
    """FR-2.2d. The WAV is deleted on the strength of one check — that the encoder
    produced a non-empty file — so every way that check can be wrong is a way to
    lose the only recording of the call.

    Three shapes, and only the first announces itself: a non-zero exit, a zero
    exit with a zero-byte output, and a zero exit with no output file at all. The
    last two are the dangerous ones, because `returncode == 0` is the thing a
    reasonable implementation would trust.
    """
    # MUTATION: `wav_path.unlink()` moved above the `written == 0` check in
    # `_finalise_audio`, or `if completed.returncode != 0` dropped. Either one
    # loses the recording outright on a bad encode.
    encoder.install(behaviour)
    harness = sessions()

    with harness.rec:
        harness.write(1.0)
        harness.turn("survived a bad encoder", label="A")

    audio = harness.rec.audio_path
    assert audio.name == f"{FIXED_BASENAME}.wav", f"archived as {audio.name}"
    assert audio.exists() and read_wav(audio)[3] == samples_for(1.0), (
        "the recording was deleted on the strength of a transcode that produced "
        "no usable file"
    )
    assert harness.suffixed(".mp3") == [], (
        f"a broken transcode left an .mp3 the pipeline would index: "
        f"{harness.suffixed('.mp3')}"
    )
    assert [p.name for p in harness.files() if _PARTIAL_SUFFIX in p.name] == [], (
        f"a partial encode was left in the archive: {harness.files()}"
    )

    notices = [e for e in harness.errors() if audio.name in e.message]
    assert len(notices) == 1, f"expected one notice, got {harness.messages()!r}"
    assert notices[0].severity is Severity.WARNING
    assert "mp3" in notices[0].message.lower()
    # The transcript describes the file that survived, and only that one.
    assert frontmatter(harness.transcript_text())["source_filename"] == audio.name


def test_an_existing_mp3_for_the_same_second_is_not_overwritten_by_the_transcode(
    sessions, encoder
):
    """§2.1, applied to the transcode's own output. The `.mp3` name is the one a
    DEVICE recording of the same second lands on, so the transcode asks
    `_unique_archive_path` for its target too — a live session must not overwrite
    the device's own file any more than it overwrites its transcript."""
    # MUTATION: `target = wav_path.with_suffix(MP3_EXTENSION)` instead of calling
    # `_unique_archive_path` — `os.replace` then destroys the device's recording.
    encoder.install("ok")
    harness = sessions()
    squatter = harness.dir / FIXED_SUBDIR / f"{FIXED_BASENAME}.mp3"
    squatter.parent.mkdir(parents=True, exist_ok=True)
    squatter.write_bytes(b"the device's own recording of this same second")

    with harness.rec:
        harness.write(1.0)
        harness.turn("mine, not theirs", label="A")

    assert squatter.read_bytes() == b"the device's own recording of this same second"
    assert harness.rec.audio_path.name == f"{FIXED_BASENAME}-1.mp3"
    # Sidecars follow the DE-CONFLICTED audio stem, not the un-suffixed one.
    assert harness.rec.transcript_path.name == f"{FIXED_BASENAME}-1.md"
    assert [p.name for p in harness.suffixed(RAW_RESPONSE_SUFFIX)] == [
        f"{FIXED_BASENAME}-1.aai.json"
    ]


@pytest.mark.parametrize("name", ["lame", "ffmpeg"])
def test_the_real_encoder_accepts_the_command_this_module_builds(
    sessions, encoder, name
):
    """The one thing a shim cannot do: reject a flag.

    `_lame_command` and `_ffmpeg_command` are argv lists written by hand against
    two different CLIs, and a shim answers every one of them identically. ffmpeg
    in particular is given `-f wav` and `-f mp3` on BOTH sides precisely because
    the target is a temp name it would otherwise pick a muxer from — a claim only
    ffmpeg itself can confirm.

    Skipped when the encoder is not installed, which is the same
    presence-detection FR-2.2b does; `afconvert` is deliberately not in the list
    (macOS decodes MP3 and does not encode it, and `afconvert` LISTS `.mp3` while
    failing with `('cfmt') failed ('fmt?')` — do not re-derive that from `-hf`).
    """
    # MUTATION: `-codec:a libmp3lame` -> `-codec:a mp3` in `_ffmpeg_command`, or
    # drop either `-f` — the shim never notices and the real encoder does.
    encoder.use_real(name)
    harness = sessions()

    with harness.rec:
        harness.write(2.0)
        harness.turn("really encoded", label="A")

    audio = harness.rec.audio_path
    assert audio.name.endswith(".mp3"), (
        f"{name} could not encode the command this module builds: "
        f"{harness.messages()!r}"
    )
    assert harness.errors() == [], harness.messages()
    body = audio.read_bytes()
    assert body, "the encoder produced an empty file"
    assert body[:3] == b"ID3" or body[0] == 0xFF, (
        f"{name} produced something that is not an MP3 stream: {body[:8]!r}"
    )
    # 96 kbps against 512 kbps of PCM. Asserting a real ratio rather than
    # "smaller", because a truncated file is also smaller.
    assert len(body) < samples_for(2.0) * CHANNELS * BYTES_PER_SAMPLE / 3
    assert harness.suffixed(".wav") == []


# ==========================================================================
# U-13 — no audio bytes and no transcript text in any log record
# ==========================================================================


def test_no_log_record_carries_audio_bytes_or_transcript_text(sessions, caplog):
    """NFR-3. This module handles both halves of a private conversation, and a
    debug line that dumps a frame or a turn puts them in a file the operator never
    thinks about — one that outlives the session and is not on the Drive mount
    they know is synced."""
    # MUTATION: `log.debug("wrote frame %r", frame)` in `write()`, or
    # `log.debug("rendered %s", md_text)` at stop.
    caplog.set_level(logging.DEBUG)
    marker = b"\xde\xad\xbe\xef"
    secret = "the wire transfer goes to account nine one four seven"
    harness = sessions()

    with harness.rec:
        harness.write_frame(Frame(near=marker * 400, far=marker * 400, seq=1))
        harness.turn(secret, label="A")

    blob = "\n".join(record.getMessage() for record in caplog.records)

    assert secret not in blob
    assert secret not in harness.messages()
    # Three renderings, because a leak arrives in whichever one the formatting
    # produced. `repr(marker)[2:-1]` is the escaped BODY without the `b'` and the
    # closing quote: a long buffer reprs as `b'\\xde\\xad\\xbe\\xef\\xde\\xad...'`,
    # which contains the body but never the delimited form — so searching for
    # `repr(marker)` whole finds nothing and the test would pass over a
    # `log.debug("%r", frame)` that dumps the entire call.
    for form in (marker.hex(), repr(marker)[2:-1], marker.decode("latin-1")):
        assert form not in blob, f"audio bytes reached a log record as {form!r}"


# ==========================================================================
# U-14 — suppression is per-file, and never a ledger entry
# ==========================================================================


def test_a_whole_live_session_leaves_the_offload_ledger_untouched(sessions, tmp_path):
    """FR-3.2. `state.is_processed` gates the OFFLOAD, not the transcription. A
    pre-seeded entry would skip the DOWNLOAD and lose the device's own recording
    outright — trading a double-billing bug for a data-loss bug, in a PRD whose
    entire premise is that live audio was being lost.

    The ledger is compared byte for byte, so an entry written and then removed is
    caught as well as one left behind. It sits at the path production uses —
    `config.py:106`, `<archive>/.state/offload_state.json` — because a ledger
    anywhere else is one this module could not find even if it tried, and the
    test would then be unfalsifiable.
    """
    # MUTATION: `store.record_processed(key, device_filename=..., ...)` at stop to
    # mark the session handled.
    harness = sessions()
    ledger = harness.dir / ".state" / "offload_state.json"
    ledger.parent.mkdir(parents=True, exist_ok=True)
    store = StateStore(ledger)
    key = DeviceKey(model="hidock-p1", serial="HDP1252405573")
    store.register_device(key)
    before = ledger.read_bytes()

    with harness.rec:
        harness.write(2.0)
        harness.turn("a whole call", label="A")

    assert ledger.read_bytes() == before, "the live session mutated the offload ledger"
    fresh = StateStore(ledger)
    for candidate in ("REC001.hda", f"{FIXED_BASENAME}.wav", harness.rec.wav_path.name):
        assert not fresh.is_processed(key, candidate), (
            f"{candidate} was pre-seeded into the ledger; its download would be "
            "skipped and the device's own recording lost"
        )


def test_the_module_cannot_write_the_ledger_at_all(sessions):
    """FR-3.2, structurally. The behavioural test above only covers the paths it
    drives; this covers the ones nobody thought to drive. Suppression is per-file
    transcription suppression, so the ledger is not this module's to touch in any
    direction."""
    # MUTATION: `from .state import StateStore` plus a `record_processed(...)`
    # call anywhere in the module.
    tree = module_tree()
    modules = list(imported_names(tree))
    names = [name for values in imported_names(tree).values() for name in values]

    assert not any(module.endswith("state") for module in modules), (
        f"live_archive imports the ledger module: {modules}"
    )
    assert "StateStore" not in names and "DeviceKey" not in names

    mutators = {"record_processed", "mark_processed", "_mutate", "register_device"}
    assert not (mutators & called_attributes(tree)), (
        f"live_archive calls a ledger mutator: {sorted(mutators & called_attributes(tree))}"
    )
    assert not (mutators & called_names(tree))


# ==========================================================================
# U-14b — the suppression itself (FR-3.1 / FR-3.3), in `offload.py`
# ==========================================================================
#
# The two tests above are negatives: `live_archive` must not touch the offload
# ledger, in either direction. Both were vacuously true of a module that had no
# suppression at all — which is what it had until FR-3.1/3.3 landed, so the whole
# section pinned the absence of a feature and nothing about the feature.
#
# What suppression IS: a live call is transcribed and BILLED as it happens. If
# the device ever also keeps its own recording of it, offloading that recording
# would send the same conversation to AssemblyAI a second time. The oracle is the
# bridge's own `LiveTranscriptionStarted`/`LiveTranscriptionStopped` pair, which
# `LiveTranscriber` publishes only when both provider sessions really open and
# close, so a window means "this stretch of wall-clock was transcribed, and paid
# for". Two directions in it are deliberate and each has a test below:
#
#   START-in-window, never overlap — a recording already running when `l` was
#   pressed holds pre-live audio nothing else transcribed, so suppressing it
#   would DESTROY a transcript rather than avoid a duplicate one.
#
#   Only CLOSED windows are consultable — an open one treated as extending to
#   "now" would, after a stop event that never arrived, suppress transcription
#   for every subsequent offload forever, with no symptom but the absence of
#   transcripts. Failing the other way costs one duplicated transcription, which
#   is in the log the moment it happens.

LIVE_WINDOW_OPENED = datetime(2026, 8, 27, 10, 0, 0)
LIVE_WINDOW_CLOSED = datetime(2026, 8, 27, 10, 30, 0)


def _wav_bytes(seconds: float = 1.0) -> bytes:
    """A real, readable WAV — `audio_duration_minutes` opens what we hand it."""
    buffer = io.BytesIO()
    with wave.open(buffer, "wb") as writer:
        writer.setnchannels(CHANNELS)
        writer.setsampwidth(BYTES_PER_SAMPLE)
        writer.setframerate(SAMPLE_RATE_HZ)
        writer.writeframes(_interleave_for_test(seconds))
    return buffer.getvalue()


def _interleave_for_test(seconds: float) -> bytes:
    count = samples_for(seconds)
    return b"".join(
        struct.pack("<hh", NEAR_SAMPLE, FAR_SAMPLE) for _ in range(count)
    )


class _OneFileAdapter:
    """A device holding exactly one recording. Binds against the real Protocol."""

    def __init__(self, name: str, payload: bytes, device_mtime: Optional[datetime]):
        self.file = DeviceFile(name=name, size=len(payload), device_mtime=device_mtime)
        self._payload = payload
        self.deleted: List[str] = []

    def list_files(self) -> List[DeviceFile]:
        inspect.signature(offload_module.DeviceAdapter.list_files).bind(None)
        return [self.file]

    def download_file(self, name, size, *, on_chunk, on_progress=None,
                      cancel_event=None) -> None:
        inspect.signature(offload_module.DeviceAdapter.download_file).bind(
            None, name, size, on_chunk=on_chunk, on_progress=on_progress,
            cancel_event=cancel_event,
        )
        on_chunk(self._payload)
        if on_progress is not None:
            on_progress(len(self._payload), size)

    def delete_file(self, name: str) -> None:
        inspect.signature(offload_module.DeviceAdapter.delete_file).bind(None, name)
        self.deleted.append(name)


class Offloads:
    """One `Offloader` on a real `StateStore`, with the transcription seam spied.

    The bridge is the only double: `transcribe_file` is a paid network call, and
    it is precisely the call whose ABSENCE is the thing under test.
    """

    def __init__(self, tmp_path: Path, monkeypatch, *, with_live_log: bool = True,
                 clock: Optional[Callable[[], datetime]] = None):
        self.dir = tmp_path / "offload-archive"
        self.dir.mkdir(parents=True, exist_ok=True)
        self.bus = EventBus()
        self.events: List[object] = []
        self.bus.subscribe(self.events.append)
        self.store = StateStore(self.dir / ".state" / "offload_state.json")
        self.key = DeviceKey(model="hidock-p1", serial="HDP1252405573")
        self.store.register_device(self.key)
        self.transcribed: List[tuple] = []
        self.ledger_writes: List[str] = []
        self.now = clock or (lambda: LIVE_WINDOW_OPENED)

        real_record = self.store.record_processed

        def spy(device_key, **kwargs):
            inspect.signature(StateStore.record_processed).bind(
                None, device_key, **kwargs
            )
            self.ledger_writes.append(kwargs["device_filename"])
            return real_record(device_key, **kwargs)

        monkeypatch.setattr(self.store, "record_processed", spy)

        import hidock_direct.transcribe as transcribe_module

        def fake_transcribe(audio_path, archive_dir, **kwargs):
            inspect.signature(transcribe_module.transcribe_file).bind(
                audio_path, archive_dir, **kwargs
            )
            self.transcribed.append((Path(audio_path), dict(kwargs)))
            return "fake-transcript-id"

        monkeypatch.setattr(transcribe_module, "transcribe_file", fake_transcribe)

        self.live_sessions = (
            LiveSessionLog(self.bus, clock=lambda: self.now())
            if with_live_log
            else None
        )
        self.adapter: Optional[_OneFileAdapter] = None
        self._monkeypatch = monkeypatch
        self._tmp = tmp_path

    def live_session(self, opened: datetime, closed: Optional[datetime]) -> None:
        """Drive one live session through the bus, exactly as the bridge does."""
        self.now = lambda: opened
        self.bus.publish(LiveTranscriptionStarted(channels=("near", "far")))
        if closed is None:
            return
        self.now = lambda: closed
        self.bus.publish(
            LiveTranscriptionStopped(
                near_seconds=60.0, far_seconds=60.0, reason="operator stopped it"
            )
        )

    def offload(self, *, device_mtime: datetime, name: str = "REC001.wav"):
        self.adapter = _OneFileAdapter(name, _wav_bytes(), device_mtime)
        offloader = Offloader(
            adapter=self.adapter,
            store=self.store,
            bus=self.bus,
            archive_dir=self.dir,
            tmp_dir=self._tmp / "offload-tmp",
            delete_after_offload=False,
            transcribe_on_offload=True,
            live_sessions=self.live_sessions,
        )
        return offloader.offload(device_key=self.key, file=self.adapter.file)

    def skips(self) -> List[TranscribeSkipped]:
        return [e for e in self.events if isinstance(e, TranscribeSkipped)]

    def completions(self) -> List[DownloadComplete]:
        return [e for e in self.events if isinstance(e, DownloadComplete)]


@pytest.fixture
def offloads(tmp_path, monkeypatch):
    def build(**kwargs) -> Offloads:
        return Offloads(tmp_path, monkeypatch, **kwargs)

    return build


def test_a_recording_that_starts_inside_a_closed_live_session_is_not_billed_twice(
    offloads,
):
    """FR-3.1. The call was transcribed and paid for while it happened."""
    # MUTATION: delete the `live_window is not None` branch from `Offloader.offload`
    # (i.e. always call `_transcribe`) — the same conversation is sent to
    # AssemblyAI a second time, and nothing but the invoice records it.
    runs = offloads()
    runs.live_session(LIVE_WINDOW_OPENED, LIVE_WINDOW_CLOSED)

    result = runs.offload(device_mtime=LIVE_WINDOW_OPENED + timedelta(minutes=5))

    assert runs.transcribed == [], (
        "the recording of a call that was already transcribed live was sent to "
        "AssemblyAI again"
    )
    # And everything that KEEPS the audio still ran. The suppression declines the
    # paid second pass and nothing above it.
    assert result.archive_path.exists()
    assert result.archive_path.read_bytes() == _wav_bytes()
    assert len(runs.completions()) == 1


def test_the_suppression_is_announced_and_names_the_session_the_file_and_the_remedy(
    offloads,
):
    """FR-3.3. A wrong guess here costs a transcript, permanently and silently —
    the skip writes no failed ledger entry, so `r` will never offer the file. So
    it is not a silent skip: the operator reads which live session matched, where
    the audio is, and what to do if this was NOT that call."""
    # MUTATION: `return` instead of publishing `TranscribeSkipped` in the
    # suppression branch. Every other assertion in this section still passes, and
    # a mis-suppressed call becomes a transcript nobody ever notices is missing.
    runs = offloads()
    runs.live_session(LIVE_WINDOW_OPENED, LIVE_WINDOW_CLOSED)

    result = runs.offload(device_mtime=LIVE_WINDOW_OPENED + timedelta(minutes=5))

    skips = runs.skips()
    assert len(skips) == 1, f"expected one skip, got {skips}"
    assert skips[0].device_filename == "REC001.wav"
    reason = skips[0].reason
    assert "live" in reason.lower()
    assert "10:00:00" in reason and "10:30:00" in reason, (
        f"the message does not name the session that matched: {reason!r}"
    )
    assert str(result.archive_path) in reason, (
        f"the message does not say where the audio is: {reason!r}"
    )
    assert "by hand" in reason and "r will" in reason, (
        f"the message does not name the remedy, and `r` cannot offer it: {reason!r}"
    )


def test_a_suppressed_offload_writes_exactly_one_ledger_entry_and_no_transcript(
    offloads,
):
    """FR-3.2 from the offloader's side. The suppression declines a paid API call
    and touches nothing else: the file is downloaded, verified, renamed and
    recorded ONCE, exactly as an ordinary offload is.

    A second write here is not cosmetic — `record_processed` is what
    `is_processed` reads, and it gates the DOWNLOAD. An entry written by the
    suppression path, on a path that could ever run before the transfer, is a
    lost recording.
    """
    # MUTATION: add a `self._store.record_processed(...)` call inside the
    # suppression branch to "mark it handled".
    runs = offloads()
    runs.live_session(LIVE_WINDOW_OPENED, LIVE_WINDOW_CLOSED)

    runs.offload(device_mtime=LIVE_WINDOW_OPENED + timedelta(minutes=5))

    assert runs.ledger_writes == ["REC001.wav"], (
        f"the offload ledger was written {len(runs.ledger_writes)} times"
    )
    fresh = StateStore(runs.dir / ".state" / "offload_state.json")
    assert fresh.is_processed(runs.key, "REC001.wav")
    # No transcript, no parity file, no diarize state: the second pass never ran.
    assert list(runs.dir.rglob("*.md")) == []
    assert list(runs.dir.rglob("*.aai.json")) == []


@pytest.mark.parametrize(
    "offset, why",
    [
        (timedelta(hours=-1), "an hour before the session even opened"),
        (timedelta(minutes=45), "a quarter of an hour after it closed"),
    ],
)
def test_a_recording_outside_every_live_window_is_transcribed_normally(
    offloads, offset, why
):
    """The suppression is narrow on purpose. A recording that has nothing to do
    with a live session must reach the batch path untouched — the cost of a false
    positive here is a transcript that is never produced and never missed."""
    # MUTATION: `covers_start_of` -> `return True`, or widen the tolerance to
    # hours. Nothing else in this file notices.
    runs = offloads()
    runs.live_session(LIVE_WINDOW_OPENED, LIVE_WINDOW_CLOSED)

    result = runs.offload(device_mtime=LIVE_WINDOW_OPENED + offset)

    assert runs.skips() == [], f"a recording {why} was suppressed"
    assert [path for path, _kwargs in runs.transcribed] == [result.archive_path]


def test_a_recording_already_running_when_l_was_pressed_is_still_transcribed(
    offloads,
):
    """The START, not an overlap, and this is the case that decides it.

    A device recording that began an hour before `l` was pressed overlaps the
    live window at its leading edge — but it holds an hour of audio the live
    session never saw and nothing else ever transcribed. Suppressing it would
    DESTROY a transcript rather than avoid a duplicate one, which is the opposite
    of the trade this feature exists to make.
    """
    # MUTATION: `covers_start_of` rewritten as an overlap test (`recorded_at <=
    # self.stopped_at and recording_end >= self.started_at`, or simply dropping
    # the lower bound) — an hour of unique audio is then silently never
    # transcribed, and the only symptom is a transcript that does not exist.
    runs = offloads()
    runs.live_session(LIVE_WINDOW_OPENED, LIVE_WINDOW_CLOSED)

    result = runs.offload(device_mtime=LIVE_WINDOW_OPENED - timedelta(hours=1))

    assert runs.skips() == []
    assert [path for path, _kwargs in runs.transcribed] == [result.archive_path]


@pytest.mark.parametrize(
    "lead, suppressed",
    [
        (timedelta(seconds=59), True),
        (timedelta(seconds=61), False),
    ],
)
def test_the_lead_tolerance_covers_the_bridge_handshake_and_nothing_wider(
    offloads, lead, suppressed
):
    """Pressing `l` sends CMD 32 START — the device enters realtime mode THEN —
    and `LiveTranscriptionStarted` is published only once both AssemblyAI
    websockets have connected. A device recording stamped at the first of those
    two moments is slightly ahead of the window we observed, so the window's
    leading edge is `started_at - LIVE_SESSION_LEAD_TOLERANCE`.

    A minute covers provider connection setup with room to spare and stays far
    below any plausible gap between an operator starting a recording ON the
    device and then pressing `l` — which is the case above, and the one this
    tolerance must NOT swallow.
    """
    # MUTATION: `LIVE_SESSION_LEAD_TOLERANCE = timedelta(seconds=0)` (the 59s case
    # is then transcribed twice) or `timedelta(minutes=5)` (the 61s case, and
    # anything up to five minutes of pre-live audio, is silently lost).
    assert LIVE_SESSION_LEAD_TOLERANCE == timedelta(seconds=60)
    runs = offloads()
    runs.live_session(LIVE_WINDOW_OPENED, LIVE_WINDOW_CLOSED)

    runs.offload(device_mtime=LIVE_WINDOW_OPENED - lead)

    assert bool(runs.skips()) is suppressed, (
        f"a recording starting {lead} before the window was "
        f"{'not ' if suppressed else ''}suppressed"
    )


def test_an_open_live_session_suppresses_nothing_at_all(offloads):
    """FAIL OPEN, deliberately. An open window would have to be treated as
    extending to `now`, and a `LiveTranscriptionStopped` that never arrived —
    a crash, a provider hang up, a teardown that raised — would then suppress
    transcription for every recording the app ever offloaded again. A permanent,
    self-inflicted outage whose only symptom is the absence of transcripts.

    Failing the other way costs one duplicated transcription of one call, which
    is visible in the log the moment it happens. Nothing is lost by it in
    practice either: `_run_scan_and_drain` and `_offload_pending` both refuse the
    device while a live session holds it, so no offload completes inside an open
    window anyway.
    """
    # MUTATION: have `window_covering` fall back to
    # `LiveSessionWindow(self._open_since, self._clock())` when `_open_since` is
    # set. The test suite goes green today and the app stops transcribing
    # anything, forever, the first time a stop event is dropped.
    runs = offloads()
    runs.live_session(LIVE_WINDOW_OPENED, None)  # started, never stopped
    # Wall-clock moves on while the window stays open — which is the whole shape
    # of the defect: "now" grows without bound, so an open window read as
    # `(started, now)` swallows every recording made after it.
    runs.now = lambda: LIVE_WINDOW_CLOSED

    result = runs.offload(device_mtime=LIVE_WINDOW_OPENED + timedelta(minutes=5))

    assert runs.skips() == []
    assert [path for path, _kwargs in runs.transcribed] == [result.archive_path]


def test_a_composition_with_no_live_session_log_transcribes_everything(offloads):
    """`live_sessions=None` means "this composition never runs live sessions", so
    nothing can have been transcribed twice. It is the shape every non-`__main__`
    caller has, and it must not suppress by accident."""
    # MUTATION: `if self._live_sessions is None: return None` -> return a window.
    runs = offloads(with_live_log=False)

    result = runs.offload(device_mtime=LIVE_WINDOW_OPENED + timedelta(minutes=5))

    assert runs.skips() == []
    assert [path for path, _kwargs in runs.transcribed] == [result.archive_path]


def test_a_stop_with_no_start_invents_no_window(offloads):
    """One endpoint is not a stretch of time. A `LiveTranscriptionStopped` that
    this log never saw the start of — it was constructed mid-session, or a start
    was dropped — must not become a window, because the only start it could be
    given is a guess, and the guess suppresses real transcriptions."""
    # MUTATION: drop the `if started is None: return` guard and use `self._clock()`
    # for both endpoints.
    runs = offloads()
    runs.now = lambda: LIVE_WINDOW_CLOSED
    runs.bus.publish(
        LiveTranscriptionStopped(near_seconds=1.0, far_seconds=1.0, reason="stopped")
    )

    result = runs.offload(device_mtime=LIVE_WINDOW_CLOSED)

    assert runs.skips() == []
    assert [path for path, _kwargs in runs.transcribed] == [result.archive_path]


def test_the_window_history_is_bounded_and_keeps_the_most_recent(offloads):
    """An app left running for weeks accumulates one window per `l` press. Only
    recent ones are ever consulted, so the list is bounded — and the ones kept are
    the RECENT ones, because a device that produced a file for the call that just
    ended is the case this exists for."""
    # MUTATION: `del self._windows[:-LIVE_SESSION_HISTORY]` deleted (unbounded
    # growth), or `del self._windows[LIVE_SESSION_HISTORY:]` (which keeps the
    # OLDEST and drops the session that just happened — suppression then stops
    # working while every test that opens one window still passes).
    runs = offloads()
    for n in range(LIVE_SESSION_HISTORY + 4):
        opened = LIVE_WINDOW_OPENED + timedelta(hours=n)
        runs.live_session(opened, opened + timedelta(minutes=10))

    windows = runs.live_sessions._windows
    assert len(windows) == LIVE_SESSION_HISTORY, (
        f"{len(windows)} windows retained for a bound of {LIVE_SESSION_HISTORY}"
    )

    latest = LIVE_WINDOW_OPENED + timedelta(hours=LIVE_SESSION_HISTORY + 3)
    runs.offload(device_mtime=latest + timedelta(minutes=1))

    assert len(runs.skips()) == 1, "the most recent live session was dropped first"


def test_the_start_compared_is_the_recordings_start_and_never_the_files_mtime(
    offloads, monkeypatch
):
    """`resolve_recorded_at` is the shared start-time resolver, and using it is
    what makes this comparison mean anything.

    A freshly offloaded file's mtime is recording-END plus transfer time, so it
    places every device recording AFTER the session that produced it — a
    suppression that consulted it would fire on almost nothing, and the failure
    would look exactly like a device that keeps no live recordings, which is what
    everybody already believes. The device's own reported start is used instead,
    and the archive basename minted from it is the durable fallback.
    """
    # MUTATION: `resolve_recorded_at(target_path, device_mtime)` ->
    # `datetime.fromtimestamp(target_path.stat().st_mtime)`.
    calls: List[tuple] = []
    real_resolve = offload_module.resolve_recorded_at

    def spy(archive_path, device_mtime=None):
        calls.append((Path(archive_path), device_mtime))
        return real_resolve(archive_path, device_mtime)

    monkeypatch.setattr(offload_module, "resolve_recorded_at", spy)

    runs = offloads()
    runs.live_session(LIVE_WINDOW_OPENED, LIVE_WINDOW_CLOSED)
    started = LIVE_WINDOW_OPENED + timedelta(minutes=5)

    result = runs.offload(device_mtime=started)

    assert calls, "the suppression never asked when the recording started"
    assert calls[0] == (result.archive_path, started)
    assert len(runs.skips()) == 1
    # The file's own mtime is NOW, which is nowhere near the 2026-08-27 window —
    # so a suppression that read it could not have fired.
    mtime = datetime.fromtimestamp(result.archive_path.stat().st_mtime)
    assert not LiveSessionWindow(
        LIVE_WINDOW_OPENED.astimezone(), LIVE_WINDOW_CLOSED.astimezone()
    ).covers_start_of(mtime.astimezone())


# ==========================================================================
# U-15 — a long call is never held in memory
# ==========================================================================


def test_the_audio_reaches_the_disk_while_the_call_is_still_running(sessions):
    """FR-1.3. "Written incrementally, never buffered whole." A long call must not
    hold its audio in RAM, and a crash must leave a playable prefix rather than
    nothing — which is only true if the bytes are already on disk before the
    session ends."""
    # MUTATION: `self._pending.append(frame)` in `write()` and one `writeframes`
    # at stop — every test that only inspects the finished file still passes.
    harness = sessions()
    seconds = 20.0
    expected = samples_for(seconds) * CHANNELS * BYTES_PER_SAMPLE

    harness.rec.__enter__()
    for _ in range(200):
        harness.write(seconds / 200)

    on_disk = harness.rec.wav_path.stat().st_size
    assert on_disk >= expected, (
        f"only {on_disk} of {expected} bytes had reached the disk mid-session"
    )


def test_a_long_session_retains_no_meaningful_audio_in_memory(sessions):
    """NFR-4 / U-15. The frame that was just written is the only audio this object
    has any reason to hold; anything proportional to call length is a buffer."""
    # MUTATION: keep `self._frames: List[Frame]` for a "re-render if the write
    # fails" retry — the retained bytes then grow with the call.
    harness = sessions()
    seconds = 20.0
    written = samples_for(seconds) * CHANNELS * BYTES_PER_SAMPLE

    harness.rec.__enter__()
    for _ in range(200):
        harness.write(seconds / 200)

    retained = retained_bytes(harness.rec)

    assert retained < 65536, (
        f"{retained} bytes of audio are retained after writing {written}; a "
        "one-hour call would hold ~700 MB"
    )


# ==========================================================================
# NFR-1 / NFR-2 — structural guards on the module itself
# ==========================================================================


def test_the_module_lives_outside_the_vendored_trees(sessions):
    """NFR-1. `jensen/` and `diarize_audio/` are overwritten wholesale by their
    refresh scripts, so anything placed there is deleted by a routine re-vendor —
    and this PRD's own change to `render.py` triggers exactly that re-vendor."""
    # MUTATION: implement this inside `src/diarize_audio/`.
    path = Path(live_archive_module.__file__)

    assert path.parent == SRC, f"live_archive lives at {path}"
    assert "jensen" not in path.parts and "diarize_audio" not in path.parts


def test_the_module_adds_no_third_party_dependency(sessions):
    """NFR-2. This is a public clone-and-run app; a new import changes every
    existing user's install for a feature they may never press. Recording needs
    stdlib `wave` and nothing else."""
    # MUTATION: `import soundfile` / `import numpy` for the WAV writing.
    tree = module_tree()
    roots: List[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            roots.extend(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            roots.append(node.module.split(".")[0])

    allowed = set(sys.stdlib_module_names) | {"hidock_direct", "diarize_audio"}
    foreign = sorted(set(roots) - allowed)

    assert foreign == [], f"live_archive imports non-stdlib packages: {foreign}"
    assert "wave" in roots, "the WAV writer is not stdlib `wave`"


# --------------------------------------------------------------------------
# stop() must be idempotent by BLOCKING, not by returning
# --------------------------------------------------------------------------


def test_a_second_stop_waits_for_the_first_to_finish_writing(sessions, monkeypatch):
    """`stop()` used to return while the first call was still finalising.

    It took `_lock` only long enough to set `_stopped`, then transcoded,
    rendered and wrote the transcript OUTSIDE it — so a second caller hit
    `if self._stopped: return` and came straight back. The durable work runs on
    the DAEMON pump thread, so on app shutdown `main()` returned, the
    interpreter exited, and the thread was killed mid-finalisation: no
    transcript at all, for a call the device kept no recording of.

    MUTATION: in `stop()`, move `with self._finalise_lock:` so it wraps only the
    inner `with self._lock:` block — the second stop then returns early and this
    test fails on the elapsed-time assertion.
    """
    import threading
    import time

    h = sessions(names={"A": "Dana"})
    h.rec.__enter__()                    # entered by hand: the test owns stop()
    h.write(0.2)
    h.turn("so about the timeline")

    real = h.rec._finalise_audio

    def slow(*a, **kw):
        time.sleep(0.4)                  # stand-in for a real transcode
        return real(*a, **kw)

    monkeypatch.setattr(h.rec, "_finalise_audio", slow)

    first = threading.Thread(target=h.rec.stop, name="first-stop")
    first.start()
    time.sleep(0.05)                     # let it enter the durable section

    began = time.monotonic()
    h.rec.stop()                         # the SECOND stop
    waited = time.monotonic() - began
    first.join(timeout=5)

    assert waited >= 0.2, (
        "the second stop() returned while the first was still finalising — "
        f"it waited only {waited:.3f}s"
    )
    assert h.rec.transcript_path is not None and h.rec.transcript_path.is_file(), (
        "stop() returned before the transcript was on disk"
    )


def test_names_typed_by_the_operator_survive_a_map_cleared_during_finalisation(
    sessions, monkeypatch
):
    """The controller's teardown clears the surface's name map on stop.

    Because `stop()` returned early, that clear could land while the render had
    not happened, so a speaker the operator NAMED was written to the permanent
    archive as `**Speaker 1**` — silently, and only on calls long enough for the
    transcode to outlast the controller's five-second join. The names are the
    operator's own typed input and are reconstructible from nothing.

    MUTATION: in `stop()`, drop `names=names_snapshot` from the
    `_write_transcript` call so it re-reads the map at render time — this test
    fails, rendering `**Speaker 1**` instead of `**Dana**`.
    """
    h = sessions(names={"A": "Dana"})
    h.rec.__enter__()                    # entered by hand: the test owns stop()
    h.write(0.2)
    h.turn("so about the timeline")

    real = h.rec._finalise_audio

    def clears_the_map(*a, **kw):
        h.names.clear()                  # exactly what surface.stop() does
        return real(*a, **kw)

    monkeypatch.setattr(h.rec, "_finalise_audio", clears_the_map)
    h.rec.stop()

    text = h.rec.transcript_path.read_text()
    assert "**Dana**" in text, (
        "the operator's typed name was lost because the map was cleared "
        f"mid-finalisation:\n{text}"
    )
    assert "**Speaker 1**" not in text


# ---------------------------------------------------------------------------
# Post-session naming (phase 1f) -- a name typed after the call ends
# ---------------------------------------------------------------------------
#
# The constraint that shapes every test below is that the archived `.md` has a
# SECOND writer. `personal_assistant/execution/auto_speaker_id.py:3176-3202`,
# read 2026-09-11, reads this file, substitutes the voice database's name for a
# generic label, and writes it back:
#
#     content = Path(filepath).read_text()
#     for sp in speakers:
#         if sp.identified_name and sp.identified_name != sp.label:
#             old_a = f"**{sp.label}**"
#             new_a = f"**{sp.identified_name}**"
#             if old_a in content:
#                 content = content.replace(old_a, new_a)
#                 replacements += 1
#     if replacements > 0:
#         Path(filepath).write_text(content)
#
# Carried verbatim for the same reason as the daemon's predicate above: it is
# the thing our own write must not destroy, it lives in another repo with no
# import path from here, and a paraphrase would drift.


def voice_id_pipeline_writes_back(path: Path, identified: Dict[str, str]) -> None:
    """The other writer, driven exactly as its own source drives it.

    A faithful double of a CONSUMER, not of a collaborator we control: it takes
    the same substitution, the same anchor shape and the same non-atomic
    `write_text`. A test that simulated it with an atomic rewrite would be
    testing a race that does not exist.
    """
    content = path.read_text()
    replacements = 0
    for label, name in identified.items():
        old_a = f"**{label}**"
        new_a = f"**{name}**"
        if old_a in content:
            content = content.replace(old_a, new_a)
            replacements += 1
    if replacements > 0:
        path.write_text(content)


def _finished_call(sessions, *, names=None):
    """One stopped session that actually wrote a transcript, with two far
    speakers and the operator, so a rename has both a target and a bystander."""
    harness = sessions(names=dict(names or {}))
    with harness.rec:
        harness.write(1.0)
        harness.near("morning")
        harness.turn("this is the first far speaker", label="A")
        harness.turn("and this is a different one", label="B")
    return harness


def test_a_name_typed_after_the_call_reaches_the_archived_transcript(sessions):
    """FR-3.1. The defect, stated as the behaviour that replaces it.

    Before phase 1f the transcript was written once at stop and the window went
    down with it, so a name typed afterwards had nowhere to go.

    MUTATION: return `RenameOutcome(applied=False, ...)` without writing.
    """
    harness = _finished_call(sessions)
    before = harness.transcript_text()
    assert "**Speaker 1**" in before or "**Speaker 2**" in before

    outcome = harness.rec.rename_speaker("A", "Dana")

    assert outcome.applied is True
    assert "**Dana**" in harness.transcript_text()


def test_the_rename_substitutes_and_does_not_re_render_the_document(sessions):
    """FR-2.1/FR-2.2, and the reason the whole feature is built this way.

    Asserted as byte equality on everything that is NOT the renamed label: a
    re-render would reproduce the document from our turns and could differ
    anywhere -- which is exactly how it would erase another writer's work.

    MUTATION: re-render through `render_markdown` with the updated map instead
    of substituting. That passes a naive "the name is in the file" assertion.
    """
    harness = _finished_call(sessions)
    before = harness.transcript_text()
    target = labels_of(before)[1]  # the first far speaker's rendered label

    harness.rec.rename_speaker("A", "Dana")
    after = harness.transcript_text()

    assert before.replace(f"**{target}**", "**Dana**") == after


def test_a_speaker_the_voice_pipeline_already_identified_is_left_alone(sessions):
    """FR-2.4, and the finding that shaped the PRD.

    The voice-ID pipeline runs against this same file and writes real names into
    it. Renaming a DIFFERENT speaker afterwards must not cost it that work --
    the naive implementation (re-render from our turns) destroys it silently,
    because our turns have never heard of Rodney.

    MUTATION: re-render on rename; or substitute with `old_anchor` computed
    from the CURRENT map for every speaker rather than only the renamed one.
    """
    harness = _finished_call(sessions)
    first, second = labels_of(harness.transcript_text())[1], None
    text = harness.transcript_text()
    far_labels = [lbl for lbl in labels_of(text) if lbl.startswith("Speaker")]
    assert len(far_labels) >= 2, far_labels
    second = far_labels[1]

    # The other writer identifies the SECOND far speaker from its voice DB.
    voice_id_pipeline_writes_back(harness.rec.transcript_path, {second: "Rodney"})
    assert "**Rodney**" in harness.transcript_text()

    # The operator now names the first one, in the window that is still open.
    outcome = harness.rec.rename_speaker("A", "Dana")

    assert outcome.applied is True
    text = harness.transcript_text()
    assert "**Dana**" in text
    assert "**Rodney**" in text, (
        "the voice-identification pipeline's write-back was destroyed by our "
        f"own rename:\n{text}"
    )


def test_a_label_the_other_writer_already_renamed_is_refused_never_guessed(
    sessions,
):
    """FR-2.3. The anchor is gone, so something else owns that line now.

    The one thing that must NOT happen is a fallback rewrite: the fallback is
    precisely the destructive path. Refuse, say why, and leave the file alone.

    MUTATION: fall back to `render_markdown` when the anchor is missing; or
    return `applied=True` with no message.
    """
    harness = _finished_call(sessions)
    far_labels = [lbl for lbl in labels_of(harness.transcript_text())
                  if lbl.startswith("Speaker")]
    voice_id_pipeline_writes_back(
        harness.rec.transcript_path, {far_labels[0]: "Rodney"}
    )
    settled = harness.transcript_text()

    outcome = harness.rec.rename_speaker("A", "Dana")

    assert outcome.applied is False
    assert outcome.reason == "anchor-gone"
    assert harness.transcript_text() == settled, "the file was written anyway"
    assert "Dana" in outcome.message
    assert far_labels[0] in outcome.message
    assert "Dana" not in settled


def test_renaming_to_the_same_name_twice_writes_once(sessions):
    """NFR-4 and FR-3.2. Idempotent by not writing, not merely by not changing.

    The archive is a Drive-synced mount, so a no-op that still replaces the file
    is a sync event for nothing. mtime is the observable that separates the two.

    MUTATION: drop the `old_anchor == new_anchor` short-circuit.
    """
    harness = _finished_call(sessions)
    harness.rec.rename_speaker("A", "Dana")
    settled = harness.transcript_text()
    stamp = harness.rec.transcript_path.stat().st_mtime_ns

    outcome = harness.rec.rename_speaker("A", "Dana")

    assert outcome.applied is False
    assert outcome.reason == "unchanged"
    assert harness.transcript_text() == settled
    assert harness.rec.transcript_path.stat().st_mtime_ns == stamp


def test_renaming_twice_anchors_on_the_name_it_last_wrote(sessions):
    """A correction. `Dana` -> `Dana Okafor` must find `**Dana**`, not `**Speaker 1**`.

    The anchor tracks what is ON THE PAGE, which is the point of keeping the map
    that was rendered rather than the map the session started with.

    MUTATION: anchor on `self._speaker_names(turns)` (the session's map) every
    time, which is correct once and wrong on every later rename.
    """
    harness = _finished_call(sessions)
    harness.rec.rename_speaker("A", "Dana")

    outcome = harness.rec.rename_speaker("A", "Dana Okafor")

    assert outcome.applied is True
    text = harness.transcript_text()
    assert "**Dana Okafor**" in text
    assert "**Dana**" not in text.replace("**Dana Okafor**", "")


def test_clearing_a_name_after_the_call_restores_the_number_the_daemon_wants(
    sessions,
):
    """Clearing must return the label to the form the voice daemon acts on.

    A cleared name that left a blank or a letter behind would withdraw the
    speaker from voice identification permanently -- the §2.3 defect, arriving
    by a new route.
    """
    harness = _finished_call(sessions, names={"A": "Dana"})
    assert "**Dana**" in harness.transcript_text()

    outcome = harness.rec.rename_speaker("A", None)

    assert outcome.applied is True
    text = harness.transcript_text()
    assert "**Dana**" not in text
    assert SPEAKER_LETTER_LABEL.search(text) is None
    assert speaker_id_daemon_would_identify(text)


def test_renaming_one_speaker_leaves_the_others_findable_by_the_daemon(sessions):
    """FR-2.5. Naming withdraws ONE speaker from voice-ID, never the rest.

    Asserted against the consumer's own predicate, head slice and all.

    MUTATION: substitute `**Speaker \\d+**` globally rather than the one anchor.
    """
    harness = _finished_call(sessions)

    harness.rec.rename_speaker("A", "Dana")

    text = harness.transcript_text()
    assert "**Dana**" in text
    assert speaker_id_daemon_would_identify(text), (
        "every generic label vanished, so the remaining speakers will never be "
        f"identified:\n{text[:SPEAKER_ID_DAEMON_HEAD_CHARS]}"
    )


def test_a_rename_racing_another_writer_aborts_rather_than_discarding_it(
    sessions, monkeypatch,
):
    """FR-2.7 -- the hazard `os.replace` creates and does not report.

    Both writers are last-writer-wins. If the voice-ID pipeline writes between
    our read and our write, an unguarded `os.replace` discards it with no error,
    no exception and no trace.

    The other writer is fired in the window the guard covers: after
    `rename_speaker` has read the document it is about to substitute into, and
    before the comparison read that authorises the replace. That is the window a
    real interleaving lands in -- the compare and the `os.replace` are adjacent
    statements, and the sliver between THOSE cannot be closed without a lock the
    other writer does not take.

    MUTATION: use `_atomic_write_text` instead of `_rewrite_if_unchanged`; or
    compare mtime/size instead of content -- a name substitution can be
    same-size, and the substitution here is chosen to be exactly that.
    """
    harness = _finished_call(sessions)
    far_labels = [lbl for lbl in labels_of(harness.transcript_text())
                  if lbl.startswith("Speaker")]
    path = harness.rec.transcript_path
    # Exactly as long as the label it replaces, so the file's SIZE is unchanged
    # and only a content comparison can see the other writer at all. Derived
    # from the label rather than written out, so it cannot drift out of being
    # the same length.
    intruder = "R" * len(far_labels[1])
    before_size = path.stat().st_size

    original = Path.read_text
    reads = {"n": 0}

    def racing_read(self, *args, **kwargs):
        text = original(self, *args, **kwargs)
        if self == path:
            reads["n"] += 1
            if reads["n"] == 1:
                voice_id_pipeline_writes_back(path, {far_labels[1]: intruder})
        return text

    monkeypatch.setattr(Path, "read_text", racing_read)
    outcome = harness.rec.rename_speaker("A", "Dana")
    monkeypatch.undo()

    assert reads["n"] >= 2, "the guard never re-read; the race was not tested"
    assert outcome.applied is False
    assert outcome.reason == "raced"
    text = harness.transcript_text()
    assert path.stat().st_size == before_size, (
        "the interfering write changed the file's size, so a size check would "
        "have caught it and this test no longer pins the content comparison"
    )
    assert f"**{intruder}**" in text, (
        f"the other writer's change was discarded by ours:\n{text}"
    )
    assert "**Dana**" not in text
    assert "still in the panel" in outcome.message


def test_a_raced_rename_leaves_no_temp_file_behind(sessions, monkeypatch):
    """A refused write must not litter the archive the pipeline indexes.

    `.tmp` is deliberately not the transcript's own suffix, so a stray one is
    not indexed as a transcript -- but it is still a file appearing in a
    Drive-synced directory for a write that did not happen.
    """
    harness = _finished_call(sessions)
    path = harness.rec.transcript_path
    original = Path.read_text
    reads = {"n": 0}

    def racing_read(self, *args, **kwargs):
        text = original(self, *args, **kwargs)
        if self == path:
            reads["n"] += 1
            if reads["n"] == 1:
                path.write_text(text + "\n<!-- someone else -->\n")
        return text

    monkeypatch.setattr(Path, "read_text", racing_read)
    outcome = harness.rec.rename_speaker("A", "Dana")
    monkeypatch.undo()

    assert outcome.reason == "raced"
    assert harness.suffixed(_PARTIAL_SUFFIX) == []


def test_a_rename_before_the_transcript_exists_is_a_stated_no_op(sessions):
    """FR-3.5. Not a failure: the stop-time snapshot has not been taken yet and
    will carry the name instead, which is how in-session naming already works.

    MUTATION: raise, or report `applied=True`, when there is no transcript.
    """
    harness = sessions(names={})
    with harness.rec:
        harness.write(1.0)
        harness.turn("mid call", label="A")

        outcome = harness.rec.rename_speaker("A", "Dana")

        assert outcome.applied is False
        assert outcome.reason == "no-transcript"
        assert outcome.message is None
        assert harness.rec.transcript_path is None

        # And the name is not lost: the stop-time snapshot carries it, exactly
        # as it did before this feature existed.
        harness.names["A"] = "Dana"

    assert "**Dana**" in harness.transcript_text()


def test_a_session_that_wrote_no_transcript_reports_no_transcript(sessions):
    """A call where nobody spoke writes audio and no document (FR-1.5)."""
    harness = sessions(names={})
    with harness.rec:
        harness.write(1.0)

    outcome = harness.rec.rename_speaker("A", "Dana")

    assert outcome.applied is False
    assert outcome.reason == "no-transcript"


def test_a_label_that_never_reached_the_document_is_reported_not_written(
    sessions,
):
    """A panel row can exist for a label that produced no final utterance."""
    harness = _finished_call(sessions)
    settled = harness.transcript_text()

    outcome = harness.rec.rename_speaker("ZZ", "Nobody")

    assert outcome.applied is False
    assert outcome.reason == "not-in-transcript"
    assert harness.transcript_text() == settled


def test_a_rename_that_cannot_be_written_says_so_and_keeps_the_recording(
    sessions, monkeypatch,
):
    """FR-ERR-1/FR-ERR-3. A disk problem costs a message, never the artifacts."""
    harness = _finished_call(sessions)
    settled = harness.transcript_text()
    monkeypatch.setattr(
        live_archive_module.os, "replace",
        lambda *a, **kw: (_ for _ in ()).throw(OSError(28, "No space left on device")),
    )

    outcome = harness.rec.rename_speaker("A", "Dana")

    monkeypatch.undo()
    assert outcome.applied is False
    assert outcome.reason == "unwritable"
    assert "No space left on device" in outcome.message
    assert harness.transcript_text() == settled
    assert harness.rec.audio_path is not None and harness.rec.audio_path.exists()
    assert harness.suffixed(".tmp") == []


def test_a_hostile_post_session_name_cannot_forge_a_turn_or_frontmatter(
    sessions,
):
    """FR-NFR: the substitution path must not bypass the emitter's sanitiser.

    A rename writes operator input straight into a Markdown body under YAML
    frontmatter. The `**...**` anchor is the only structure it is allowed to
    occupy.

    MUTATION: interpolate `name` into the anchor without going through
    `speaker_labels`, which is what applies `_sanitize_speaker_name`.
    """
    hostile = "Dana**\n---\nrecorded_at: 1999-01-01T00:00:00+00:00\n**Speaker 9"
    harness = _finished_call(sessions)
    before_keys = frontmatter_keys(harness.transcript_text())
    before_turns = len(turn_lines(harness.transcript_text()))

    harness.rec.rename_speaker("A", hostile)

    text = harness.transcript_text()
    assert frontmatter_keys(text) == before_keys
    assert len(turn_lines(text)) == before_turns
    # The invariant is structural, not lexical: `---` inside a turn's text is
    # prose, and a fence is `---` alone on a line. Exactly the two real fences
    # must survive, and the forged `recorded_at` must never start a line.
    assert len(re.findall(r"^---$", text, re.M)) == 2
    assert re.search(r"^recorded_at: 1999", text, re.M) is None


def test_rename_messages_carry_no_transcript_text(sessions):
    """NFR-3. The operator's messages name files and labels, never content."""
    secret = "the acquisition closes on tuesday"
    harness = sessions(names={})
    with harness.rec:
        harness.write(1.0)
        harness.turn(secret, label="A")

    messages = [
        harness.rec.rename_speaker("A", "Dana").message,
        harness.rec.rename_speaker("ZZ", "Nobody").message,
    ]

    for message in messages:
        assert message is None or secret not in message


def test_the_guarded_rewrite_is_its_own_function_not_the_sidecar_writer(sessions):
    """FR-2.9, read off the source.

    `_atomic_write_text`'s contract is a path "`_sidecar_target` only ever hands
    back a name nothing occupies". That is true of the transcript when it is
    written and stops being true minutes later, once the voice-ID pipeline also
    holds it. Reusing it here would be reusing a contract that no longer applies.

    MUTATION: point `rename_speaker` at `_atomic_write_text`.
    """
    source = (SRC / "live_archive.py").read_text()
    body = source.split("def rename_speaker")[1].split("\n    def ")[0]
    assert "_rewrite_if_unchanged" in body
    assert "_atomic_write_text" not in body
