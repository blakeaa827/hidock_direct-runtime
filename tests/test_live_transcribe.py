"""Live transcription bridge — Frames to two AssemblyAI streaming sessions.

The load-bearing property under test is that a turn's CHANNEL is structural.
The device hands us the two ends of the call as separate buffers, so which end
a turn came from is a fact we hold before transcription starts. The SDK's own
`ChannelStreamer` sums both ends to mono and re-derives attribution from an
energy-ratio VAD whose answer may be "unknown"; the PRD (§2) rejects it for
exactly that reason. These tests pin the certainty that decision buys —
especially `test_channel_is_structural_not_derived_from_content`, which makes
both sessions emit IDENTICAL text so that any content-derived implementation
fails.

The doubles here are deliberately NOT `**kwargs`-permissive. A double that
accepts calls the real collaborator would refuse cannot fail on a contract
disagreement — the 2026-08-20 `transcribe_file` bug shipped through seven such
doubles. `_bind_against` runs `inspect.signature(real).bind(...)` before every
delegate, so these tests fail if the SDK's signature and our call ever diverge.

PRD: projects/hidock_direct/planning/live_transcription_prd.md
"""

from __future__ import annotations

import inspect
import logging
from typing import Callable, Dict, List, Optional, Tuple

import pytest

from assemblyai.streaming.v3 import (
    BeginEvent,
    StreamingClient,
    StreamingError,
    StreamingEvents,
    TerminationEvent,
    TurnEvent,
    Word,
)

from hidock_direct.events import (
    Error,
    EventBus,
    LiveChannel,
    LiveSpeakerRevision,
    LiveTranscriptionStarted,
    LiveTranscriptionStopped,
    LiveTurn,
    Severity,
)
from hidock_direct.live_transcribe import LiveTranscriber, LiveTranscriptionError
from hidock_direct.realtime import Frame, SAMPLE_RATE_HZ

# Distinctive so a leak into a log record is unambiguous, not a coincidence.
SECRET_TEXT = "quarterly revenue was four million eight hundred thousand"


def pcm(sample: int, count: int = 8) -> bytes:
    """A mono s16le buffer of one repeated sample."""
    return sample.to_bytes(2, "little", signed=True) * count


SILENT = pcm(0)
LOUD = pcm(9000)


def _bind_against(real: Callable, *args, **kwargs) -> None:
    """Raise TypeError unless this call binds against the real SDK signature.

    `real` is an unbound method, so `self` is supplied positionally as None.
    """
    inspect.signature(real).bind(None, *args, **kwargs)


class FakeStreamingClient:
    """A faithful stand-in for `StreamingClient` — never more permissive.

    Every method binds its call against the real class's signature before
    doing anything, so a drift between our call site and the SDK surfaces here
    rather than in production.
    """

    def __init__(self, channel: str):
        self.channel = channel
        self.params = None
        self.streamed: List[bytes] = []
        self.disconnects: List[bool] = []
        self.connected = False
        self._handlers: Dict[StreamingEvents, List[Callable]] = {}
        self.connect_error: Optional[BaseException] = None
        self.disconnect_error: Optional[BaseException] = None
        self.stream_error: Optional[BaseException] = None
        self.termination_seconds: Optional[int] = 42

    # -- the real surface -------------------------------------------------

    def connect(self, params) -> None:
        _bind_against(StreamingClient.connect, params)
        if self.connect_error is not None:
            raise self.connect_error
        self.params = params
        self.connected = True
        self.emit(StreamingEvents.Begin, BeginEvent(id="s-1", expires_at=0))

    def stream(self, data) -> None:
        _bind_against(StreamingClient.stream, data)
        if self.stream_error is not None:
            raise self.stream_error
        self.streamed.append(bytes(data))

    def disconnect(self, terminate: bool = False) -> None:
        _bind_against(StreamingClient.disconnect, terminate=terminate)
        self.disconnects.append(terminate)
        if self.disconnect_error is not None:
            raise self.disconnect_error
        if terminate:
            # A graceful close ALWAYS produces a Termination event; whether it
            # carries a duration is a separate question, and
            # `audio_duration_seconds` is Optional upstream. Emitting the event
            # even when the field is None is what exercises the handler's
            # None-vs-0.0 branch — the fake used to skip the event entirely and
            # a mutation returning 0.0 survived undetected.
            self.emit(
                StreamingEvents.Termination,
                TerminationEvent(audio_duration_seconds=self.termination_seconds),
            )
        self.connected = False

    def on(self, event, handler) -> None:
        _bind_against(StreamingClient.on, event, handler)
        self._handlers.setdefault(event, []).append(handler)

    # -- test driving -----------------------------------------------------

    def emit(self, event, payload) -> None:
        for handler in self._handlers.get(event, []):
            handler(self, payload)


def turn(text: str, order: int = 0, final: bool = True,
         speaker: Optional[str] = None) -> TurnEvent:
    return TurnEvent(
        type="Turn",
        turn_order=order,
        turn_is_formatted=True,
        end_of_turn=final,
        transcript=text,
        end_of_turn_confidence=0.9,
        words=[Word(start=0, end=1, confidence=0.9, text=text or "x",
                    word_is_final=final, speaker=speaker)],
        speaker_label=speaker,
    )


class Harness:
    """A transcriber wired to fakes, with the bus drained into a list."""

    def __init__(self, **kwargs):
        self.bus = EventBus()
        self.events: List[object] = []
        self.bus.subscribe(self.events.append)
        self.clients: Dict[str, FakeStreamingClient] = {}
        self.transcriber = LiveTranscriber(
            self.bus,
            api_key="k-test",
            client_factory=self._factory,
            **kwargs,
        )

    def _factory(self, channel: str) -> FakeStreamingClient:
        client = FakeStreamingClient(channel)
        self.clients[channel] = client
        return client

    @property
    def near(self) -> FakeStreamingClient:
        return self.clients["near"]

    @property
    def far(self) -> FakeStreamingClient:
        return self.clients["far"]

    def of(self, kind) -> List[object]:
        return [e for e in self.events if isinstance(e, kind)]


# --------------------------------------------------------------------------
# U-1/U-2/U-3 — the channel contract
# --------------------------------------------------------------------------


def test_near_bytes_reach_the_near_session_and_far_bytes_the_far_session():
    """Asymmetric on purpose: a crossed wiring cannot pass by symmetry."""
    h = Harness()
    with h.transcriber:
        h.transcriber.feed(Frame(near=SILENT, far=LOUD, seq=1))

    assert h.near.streamed == [SILENT]
    assert h.far.streamed == [LOUD]


def test_near_session_requests_no_diarization_and_far_session_does():
    h = Harness(max_speakers=4)
    with h.transcriber:
        pass

    assert not h.near.params.speaker_labels
    assert h.near.params.max_speakers is None
    assert h.far.params.speaker_labels is True
    assert h.far.params.max_speakers == 4


def test_channel_is_structural_not_derived_from_content():
    """Both sessions emit byte-identical text; only the source distinguishes.

    Any implementation that infers channel from the turn's content — the
    approach `ChannelStreamer` takes — fails here.
    """
    h = Harness()
    with h.transcriber:
        h.near.emit(StreamingEvents.Turn, turn("identical words", order=7))
        h.far.emit(StreamingEvents.Turn, turn("identical words", order=7))

    turns = h.of(LiveTurn)
    assert [t.channel for t in turns] == [LiveChannel.NEAR, LiveChannel.FAR]
    assert {t.text for t in turns} == {"identical words"}


def test_live_channel_cannot_express_an_unknown_channel():
    """Pins the type against a well-meaning future addition."""
    assert {c.value for c in LiveChannel} == {"near", "far"}


def test_near_turns_carry_no_speaker_and_module_emits_no_display_names():
    h = Harness()
    with h.transcriber:
        # Even if the provider volunteered a label on the near channel.
        h.near.emit(StreamingEvents.Turn, turn("mine", speaker="A"))
        h.far.emit(StreamingEvents.Turn, turn("theirs", speaker="B"))

    near_turn, far_turn = h.of(LiveTurn)
    assert near_turn.speaker is None
    assert far_turn.speaker == "B"


def test_sample_rate_is_taken_from_the_capture_module_by_reference(monkeypatch):
    """A duplicated literal is a place for the two to drift apart silently.

    Asserting `== SAMPLE_RATE_HZ` alone would pass against a hardcoded 16000,
    which is the very thing FR-1.4 forbids. Moving the capture module's value
    and requiring the parameters to follow is what actually pins the reference.
    """
    import hidock_direct.live_transcribe as module

    h = Harness()
    with h.transcriber:
        pass
    assert h.near.params.sample_rate == SAMPLE_RATE_HZ
    assert h.far.params.sample_rate == SAMPLE_RATE_HZ

    monkeypatch.setattr(module, "SAMPLE_RATE_HZ", 12345)
    moved = Harness()
    with moved.transcriber:
        pass
    assert moved.near.params.sample_rate == 12345
    assert moved.far.params.sample_rate == 12345


def test_frames_are_forwarded_verbatim_with_no_conversion():
    h = Harness()
    payload = pcm(1234, count=64)
    with h.transcriber:
        h.transcriber.feed(Frame(near=payload, far=payload, seq=1))
    assert h.near.streamed == [payload]


# --------------------------------------------------------------------------
# U-6..U-9 — the stop guarantee
# --------------------------------------------------------------------------


def test_both_sessions_terminate_when_the_capture_iterator_raises():
    h = Harness()

    def capture():
        yield Frame(near=SILENT, far=LOUD, seq=1)
        raise RuntimeError("device stopped responding")

    with pytest.raises(RuntimeError):
        with h.transcriber:
            for frame in capture():
                h.transcriber.feed(frame)

    assert h.near.disconnects == [True]
    assert h.far.disconnects == [True]


def test_both_sessions_terminate_when_a_bus_consumer_raises():
    """A subscriber's exception must not leave a metered session open."""
    h = Harness()

    def explode(event):
        if isinstance(event, LiveTurn):
            raise ValueError("subscriber blew up")

    # Subscribe directly so the raise happens inside publish, not in a test loop.
    h.bus.subscribe(explode)

    with h.transcriber:
        h.far.emit(StreamingEvents.Turn, turn("hello"))

    assert h.near.disconnects == [True]
    assert h.far.disconnects == [True]


def test_a_failing_disconnect_on_one_channel_still_disconnects_the_other():
    h = Harness()
    with h.transcriber:
        h.clients["near"].disconnect_error = OSError("socket already gone")

    assert h.near.disconnects == [True]
    assert h.far.disconnects == [True]


def test_a_raising_disconnect_does_not_replace_the_original_exception():
    h = Harness()

    with pytest.raises(RuntimeError, match="original"):
        with h.transcriber:
            h.clients["near"].disconnect_error = OSError("secondary")
            h.clients["far"].disconnect_error = OSError("secondary")
            raise RuntimeError("original")


def test_start_disconnects_the_first_session_when_the_second_fails_to_connect():
    """Otherwise a half-open start leaves a session billing with no owner."""
    bus = EventBus()
    clients: Dict[str, FakeStreamingClient] = {}

    def factory(channel: str) -> FakeStreamingClient:
        client = FakeStreamingClient(channel)
        if channel == "far":
            client.connect_error = StreamingError("connection refused")
        clients[channel] = client
        return client

    transcriber = LiveTranscriber(bus, api_key="k", client_factory=factory)
    with pytest.raises(LiveTranscriptionError):
        transcriber.start()

    assert clients["near"].disconnects == [True]


# --------------------------------------------------------------------------
# U-10/U-11 — failure classification
# --------------------------------------------------------------------------


def test_invalid_api_key_is_terminal_operator_actionable_and_never_retried():
    """AAI reports a bad key as `Invalid API key`, not 401 — captured 2026-08-20."""
    bus = EventBus()
    seen: List[object] = []
    bus.subscribe(seen.append)
    attempts: List[str] = []

    def factory(channel: str) -> FakeStreamingClient:
        attempts.append(channel)
        client = FakeStreamingClient(channel)
        client.connect_error = StreamingError(
            "Failed to upload audio file: Invalid API key"
        )
        return client

    transcriber = LiveTranscriber(bus, api_key="bad", client_factory=factory)
    with pytest.raises(LiveTranscriptionError):
        transcriber.start()

    assert attempts == ["near"], "must not try the second channel after auth failure"
    errors = [e for e in seen if isinstance(e, Error)]
    assert errors and errors[0].severity is Severity.ERROR
    assert "ASSEMBLYAI_API_KEY" in errors[0].message


def test_dead_balance_selects_the_balance_remedy_not_the_key_remedy():
    """A bad key and a dead balance are both terminal but need different actions."""
    bus = EventBus()
    seen: List[object] = []
    bus.subscribe(seen.append)

    def factory(channel: str) -> FakeStreamingClient:
        client = FakeStreamingClient(channel)
        client.connect_error = StreamingError(
            "Your current account balance is negative. Please top up to continue."
        )
        return client

    transcriber = LiveTranscriber(bus, api_key="k", client_factory=factory)
    with pytest.raises(LiveTranscriptionError):
        transcriber.start()

    message = [e for e in seen if isinstance(e, Error)][0].message
    assert "top up" in message
    assert "ASSEMBLYAI_API_KEY" not in message
    assert "press r" not in message, "that is the retry surface's action, not this one"


@pytest.mark.parametrize("foreign", [
    OSError("connection reset by peer"),
    TimeoutError("handshake timed out"),
    # The SDK's own transport stack: `client.py` catches and re-raises from
    # httpx and websockets, so both can surface at our boundary.
    __import__("httpx").ConnectError("name resolution failed"),
    __import__("websockets").exceptions.WebSocketException("closed abnormally"),
])
def test_foreign_transport_exceptions_are_translated_not_leaked(foreign):
    """Doubles that only raise OUR exception type create false confidence.

    These are the library's and the stdlib's types, which is what actually
    arrives — `StreamingClient` re-raises from httpx, websockets, OSError,
    RuntimeError and TimeoutError.
    """
    bus = EventBus()
    seen: List[object] = []
    bus.subscribe(seen.append)

    def factory(channel: str) -> FakeStreamingClient:
        client = FakeStreamingClient(channel)
        client.connect_error = foreign
        return client

    transcriber = LiveTranscriber(bus, api_key="k", client_factory=factory)
    with pytest.raises(LiveTranscriptionError) as excinfo:
        transcriber.start()

    # Translated into our type, with the channel named, and the original kept.
    assert "near" in str(excinfo.value)
    assert excinfo.value.__cause__ is foreign
    assert [e for e in seen if isinstance(e, Error)]


def test_a_mid_session_drop_on_one_channel_stops_the_whole_session():
    """Half a conversation, silently, is worse than a stopped one."""
    h = Harness()
    with pytest.raises(LiveTranscriptionError, match="far"):
        with h.transcriber:
            h.transcriber.feed(Frame(near=SILENT, far=LOUD, seq=1))
            h.far.stream_error = StreamingError("connection closed")
            h.transcriber.feed(Frame(near=SILENT, far=LOUD, seq=2))

    assert h.near.disconnects == [True]
    assert h.far.disconnects == [True]


def test_feed_before_start_is_refused():
    h = Harness()
    with pytest.raises(LiveTranscriptionError):
        h.transcriber.feed(Frame(near=SILENT, far=LOUD, seq=1))


# --------------------------------------------------------------------------
# U-12 — revisions
# --------------------------------------------------------------------------


def test_speaker_revision_emits_one_event_per_revised_turn_and_mutates_nothing():
    """`SpeakerRevisionEvent.revisions` is a list; each item names its own turn."""
    from assemblyai.streaming.v3 import SpeakerRevisionEvent, SpeakerRevisionItem

    h = Harness()
    with h.transcriber:
        h.far.emit(StreamingEvents.Turn, turn("first", order=0, speaker="A"))
        h.far.emit(StreamingEvents.Turn, turn("second", order=1, speaker="A"))
        h.far.emit(
            StreamingEvents.SpeakerRevision,
            SpeakerRevisionEvent(revisions=[
                SpeakerRevisionItem(turn_order=0, speaker_label="B"),
                SpeakerRevisionItem(turn_order=1, speaker_label="C"),
            ]),
        )

    revisions = h.of(LiveSpeakerRevision)
    assert [(r.turn_order, r.speaker) for r in revisions] == [(0, "B"), (1, "C")]
    assert all(r.channel is LiveChannel.FAR for r in revisions)
    # The already-published turns are frozen and untouched.
    assert [t.speaker for t in h.of(LiveTurn)] == ["A", "A"]


# --------------------------------------------------------------------------
# Response-variant coverage matrix
# --------------------------------------------------------------------------


def test_begin_event_publishes_started_exactly_once():
    h = Harness()
    with h.transcriber:
        pass
    started = h.of(LiveTranscriptionStarted)
    assert len(started) == 1
    assert set(started[0].channels) == {"near", "far"}


def test_partial_and_final_turns_are_both_emitted_and_distinguished():
    h = Harness()
    with h.transcriber:
        h.far.emit(StreamingEvents.Turn, turn("partial thought", final=False))
        h.far.emit(StreamingEvents.Turn, turn("partial thought complete", final=True))

    assert [t.is_final for t in h.of(LiveTurn)] == [False, True]


def test_empty_transcript_is_dropped_rather_than_rendered_as_a_blank_line():
    h = Harness()
    with h.transcriber:
        h.far.emit(StreamingEvents.Turn, turn("", final=True))
    assert h.of(LiveTurn) == []


def test_termination_event_supplies_the_billed_seconds_per_channel():
    h = Harness()
    with h.transcriber:
        h.clients["near"].termination_seconds = 100
        h.clients["far"].termination_seconds = 100

    stopped = h.of(LiveTranscriptionStopped)[0]
    assert stopped.near_seconds == 100
    assert stopped.far_seconds == 100
    assert stopped.reason == "stopped"


def test_missing_billed_seconds_is_reported_as_unknown_not_as_zero():
    """0.0 would understate a real bill; None says the provider did not tell us."""
    h = Harness()
    with h.transcriber:
        h.clients["near"].termination_seconds = None
        h.clients["far"].termination_seconds = None

    stopped = h.of(LiveTranscriptionStopped)[0]
    assert stopped.near_seconds is None
    assert stopped.far_seconds is None


def test_an_unknown_provider_event_is_skipped_without_stopping_the_stream():
    h = Harness()
    with h.transcriber:
        h.far.emit(StreamingEvents.Turn, object())  # not a TurnEvent
        h.far.emit(StreamingEvents.Turn, turn("still working"))

    assert [t.text for t in h.of(LiveTurn)] == ["still working"]


def test_provider_error_event_stops_the_session():
    h = Harness()
    with pytest.raises(LiveTranscriptionError):
        with h.transcriber:
            h.far.emit(StreamingEvents.Error, StreamingError("stream died"))
            h.transcriber.feed(Frame(near=SILENT, far=LOUD, seq=1))


# --------------------------------------------------------------------------
# U-13/U-15 — containment and secrecy
# --------------------------------------------------------------------------


def test_a_full_cycle_writes_no_file_anywhere(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    h = Harness()
    with h.transcriber:
        h.transcriber.feed(Frame(near=SILENT, far=LOUD, seq=1))
        h.far.emit(StreamingEvents.Turn, turn(SECRET_TEXT))

    assert list(tmp_path.iterdir()) == []


def test_no_log_record_carries_turn_text_the_api_key_or_audio_bytes(caplog):
    caplog.set_level(logging.DEBUG)
    h = Harness()
    with h.transcriber:
        h.transcriber.feed(Frame(near=SILENT, far=pcm(2222, 32), seq=1))
        h.far.emit(StreamingEvents.Turn, turn(SECRET_TEXT))
        h.far.emit(StreamingEvents.Turn, object())  # forces the warning path

    blob = "\n".join(r.getMessage() for r in caplog.records)
    assert SECRET_TEXT not in blob
    assert "k-test" not in blob
    assert "\\x" not in blob and "b'" not in blob


def test_error_messages_do_not_carry_turn_text_or_the_api_key():
    h = Harness()
    with pytest.raises(LiveTranscriptionError) as excinfo:
        with h.transcriber:
            h.far.emit(StreamingEvents.Turn, turn(SECRET_TEXT))
            h.far.stream_error = StreamingError("connection closed")
            h.transcriber.feed(Frame(near=SILENT, far=LOUD, seq=1))

    assert SECRET_TEXT not in str(excinfo.value)
    assert "k-test" not in str(excinfo.value)


def test_assemblyai_is_bounded_and_its_floor_can_import_the_streaming_api():
    """The old `>=0.30` floor named a version where `streaming.v3` does not exist.

    A floor that cannot satisfy the imports is worse than a loose one: it reads
    as a considered minimum while describing an install that does not work. The
    ceiling matters for the same audience — this is a public clone-and-run app,
    so an unbounded major means a stranger's fresh install picks up an API
    nobody here has executed.
    """
    import tomllib
    from pathlib import Path

    import assemblyai

    pyproject = Path(__file__).resolve().parent.parent / "pyproject.toml"
    with pyproject.open("rb") as handle:
        deps = tomllib.load(handle)["project"]["dependencies"]

    spec = next(d for d in deps if d.startswith("assemblyai"))
    assert "<" in spec, f"assemblyai must carry an upper bound; got {spec!r}"

    floor = spec.split(">=")[1].split(",")[0]
    as_tuple = lambda v: tuple(int(p) for p in v.split(".") if p.isdigit())  # noqa: E731
    assert as_tuple(floor) <= as_tuple(assemblyai.__version__), (
        f"declared floor {floor} is above the installed {assemblyai.__version__}, "
        "so the suite has never run against the version the floor names"
    )
    # The floor must be able to satisfy what this module actually imports.
    assert as_tuple(floor) >= (0, 64), (
        f"floor {floor} predates assemblyai.streaming.v3 — the constraint would "
        "permit an install where live transcription cannot import"
    )


def test_the_module_never_touches_the_ledger_or_the_archive():
    """FR-4.2 — pre-seeding `is_processed` would skip the OFFLOAD and lose the file.

    Structural on purpose. A grep over the source would also match the module
    docstring, which *names* `state.is_processed` in order to explain why this
    module must never write it — the one place that hazard is written down.
    A test that forces the deletion of its own rationale is the wrong test, so
    this reads the AST: what the module imports, and what it calls.
    """
    import ast

    import hidock_direct.live_transcribe as module

    tree = ast.parse(inspect.getsource(module))

    imported: List[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            imported.append(node.module)
        elif isinstance(node, ast.Import):
            imported.extend(a.name for a in node.names)
    for banned in ("state", "offload", "pipeline", "transcribe", "pathlib", "os"):
        assert banned not in imported, f"live_transcribe must not import {banned}"

    called = {
        node.func.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }
    for banned in ("open", "Path"):
        assert banned not in called, f"live_transcribe must not call {banned}()"


def test_an_sdk_without_speaker_revisions_degrades_visibly_instead_of_dying():
    """Observed live 2026-08-27: the whole session died at construction.

    `StreamingEvents.SpeakerRevision` postdates 0.64.3, a version this project
    has really been run against. Subscribing unconditionally raised
    AttributeError inside `_ChannelSession.__init__`, so a missing ENRICHMENT
    event killed the entire live session — and reported it as
    `type object 'StreamingEvents' has no attribute 'SpeakerRevision'`, which
    tells the operator nothing about the actual remedy.

    MUTATION: drop the `getattr(..., None)` guard in `_ChannelSession.__init__`
    and subscribe unconditionally — this test raises AttributeError.
    """
    import hidock_direct.live_transcribe as module

    class OldEnum:
        Begin = StreamingEvents.Begin
        Turn = StreamingEvents.Turn
        Termination = StreamingEvents.Termination
        Error = StreamingEvents.Error
        # No SpeakerRevision — exactly the 0.64.3 surface.

    h = Harness()
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(module, "StreamingEvents", OldEnum)
        with h.transcriber:
            h.far.emit(StreamingEvents.Turn, turn("still transcribing"))

    # The session ran.
    assert [t.text for t in h.of(LiveTurn)] == ["still transcribing"]

    # And the operator was told, in terms they can act on.
    warnings = [e for e in h.of(Error) if e.severity is Severity.WARNING]
    assert warnings, "the degraded branch was silent"
    said = warnings[0].message
    assert "assemblyai" in said.lower()
    assert "bootstrap.sh" in said or "pip install" in said
