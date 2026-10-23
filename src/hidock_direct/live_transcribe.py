"""Live transcription bridge — `Frame`s to AssemblyAI, turns to the event bus.

Consumes the two-channel stream `realtime.RealtimeSession` produces and emits
`LiveTurn` events carrying a **structural** channel origin. Nothing here is
persisted: the 48 kHz flash recording remains the authoritative artifact, and
this module writes no file, no ledger entry, and no archive path. It must never
pre-seed the ledger either — `state.is_processed` gates the *download*, so an
entry written here would skip the offload and lose the recording outright.

TWO SESSIONS, NOT ONE — the decision this module exists to implement:

The SDK ships a multi-channel coordinator (`ChannelStreamer`). It **sums both
channels into one mono stream** and then re-derives attribution client-side from
an energy-ratio VAD (`dominance_ratio 4.0`), whose per-word answer may come back
`"unknown"`. Our channels arrive as physically separate buffers — the device
already told us which end is which, and `realtime._deinterleave` refuses to
realign a partial frame precisely to keep that true. Routing them through
`ChannelStreamer` would destroy that certainty and buy a weaker version back
statistically, degrading exactly during overtalk: the moment the operator most
often asks "who said that?", and the moment live speaker naming would have to
render the operator's OWN line as an unknown speaker.

It bills 1x against two sessions' 2x, because it sums to mono before the wire
and the vendor bills multichannel per channel. Priced out that is $0.42/hr
against $0.27/hr — **$0.15/hr**, about $6/month at 40 call-hours. The only
argument for it did not survive being quantified. See
`live_transcription_prd.md` §2, which also names the revisit trigger:
server-side *structural* attribution would dominate and should reopen it.

Two measured facts make the two-session shape clean. The device's echo canceller
already separates the ends (a 1 kHz tone played out produced +38 dB on channel 2
while channel 1 *fell*), so the sessions do not double-transcribe the same
speech. And BlueCatch means the near channel is one person, so it requests no
diarization at all — it cannot emit a wrong speaker label because it emits none.
That assumption is topology-specific: a future room-mic mode would break it.
"""

from __future__ import annotations

import logging
from typing import Callable, Dict, List, Optional

from assemblyai.streaming.v3 import (
    Encoding,
    StreamingClient,
    StreamingClientOptions,
    StreamingEvents,
    StreamingParameters,
)

from .events import (
    Error,
    EventBus,
    LiveChannel,
    LiveSpeakerRevision,
    LiveTranscriptionStarted,
    LiveTranscriptionStopped,
    LiveTurn,
    Severity,
)
from .config import SPEAKER_COUNT_DEFAULT
from .realtime import Frame, SAMPLE_RATE_HZ
from .retry import CLASS_OPERATOR_ACTIONABLE, classify_failure, redact, remediation

log = logging.getLogger(__name__)

# What the operator does after topping up a dead balance on THIS surface. The
# retry path says "press r"; here the session is already gone.
_LIVE_NEXT_ACTION = "then start live transcription again."

# The app's default, imported rather than restated (live_speaker_count_ten_prd.md
# FR-1.2). Callers normally pass the operator's per-session count.
DEFAULT_MAX_SPEAKERS = SPEAKER_COUNT_DEFAULT

_ORDER = (LiveChannel.NEAR, LiveChannel.FAR)

# The provider's "no speaker assigned yet" placeholder for a turn with less than
# about a second of audio. It is a sentinel, not a person, and `None` is already
# what every surface renders as attributed-to-nobody. Mapped HERE, at the one
# boundary where provider labels enter, so no consumer ever sees it. Exact
# matches only: anything else the provider sends is a real label.
#   PENDING  captured: archived live sessions 2026-09-11_133233, 2026-09-11_090036
#            and 2026-10-21_105858 each carry a `far-PENDING` speaker.
#   UNKNOWN  unobserved-but-reachable: the vendor's earlier streaming
#            diarization docs name it as the label for the same short-turn case.
# bug_report_live_pending_label_treated_as_speaker.md
_PLACEHOLDER_LABELS = frozenset({"PENDING", "UNKNOWN"})


def _provider_label(label: Optional[str]) -> Optional[str]:
    return None if label in _PLACEHOLDER_LABELS else label


# How often the provider re-clusters the far channel's speakers DURING the call.
# The operator names speakers while watching them talk; with the provider default
# (0) the only re-clustering is at session end, after the naming is done, so a
# label born on one voice can carry another for the whole call. 120000 ms is the
# vendor's documented minimum ("Min 120000; recommended 300000") and the
# operator's "every couple of minutes" (live_speaker_revisions_sdk_upgrade_prd.md
# FR-2.1/2.2). The minimum is an authored copy of a vendor doc: it is not
# readable at runtime, and is deletable once the SDK exposes it.
SPEAKER_REVISION_INTERVAL_MS = 120_000

# Far-channel turn detection: the vendor's "aggressive" preset for short, rapid
# exchanges (end on 160 ms of silence when confident, force at 400 ms; defaults
# 400 / 1280). A turn carries ONE speaker label, so a quick back-and-forth with
# no pause used to arrive as one turn under one person (16 of 222 far turns on
# 2026-10-23). Shorter turns let each voice carry its own label; the live
# archive rejoins consecutive same-speaker lines so the document still reads as
# paragraphs (live_diarization_turns_prd.md). The vendor's confidence threshold
# default (0.4) already equals the preset, so it is not sent. These are authored
# copies of a vendor doc, deletable once the SDK exposes named presets.
FAR_MIN_TURN_SILENCE_MS = 160
FAR_MAX_TURN_SILENCE_MS = 400
_REVISION_INTERVAL_FIELD = "speaker_labels_revision_interval_ms"


def _revision_interval_supported() -> bool:
    """Whether the INSTALLED SDK can send the interval at all.

    Read from the SDK's own model rather than its version string: below 1.6.1
    the field does not exist, and pydantic silently IGNORES an unknown kwarg
    (0.64.21's `model_config` is `{}`), so the session would run with no mid-call
    revisions and no error.
    """
    return _REVISION_INTERVAL_FIELD in StreamingParameters.model_fields


class LiveTranscriptionError(RuntimeError):
    """A live session could not be started, or died mid-call."""


class _ChannelSession:
    """One provider session bound to one physical channel.

    The binding is the whole point: every turn this session reports is, by
    construction, from `channel`. Nothing downstream infers it.
    """

    def __init__(self, channel: LiveChannel, client, on_event: Callable):
        self.channel = channel
        self.client = client
        self.billed_seconds: Optional[float] = None
        self.live = False
        self._on_event = on_event
        client.on(StreamingEvents.Begin, self._handle_begin)
        client.on(StreamingEvents.Turn, self._handle_turn)
        client.on(StreamingEvents.Termination, self._handle_termination)
        client.on(StreamingEvents.Error, self._handle_error)
        # Optional, and NOT a formality: `SpeakerRevision` was added to the
        # streaming enum after 0.64.3, which is a version this project has
        # actually been run against (see
        # bug_report_assemblyai_sdk_dependency_unbounded — 0.64.3 / 0.64.21 /
        # 0.64.32 across three machines). Subscribing unconditionally raised
        # `AttributeError` here, at session construction, on the older SDK.
        # That killed the whole live session for the sake of an enrichment
        # event, and reported it as an unreadable type error rather than
        # "your SDK is too old" — observed live 2026-08-27.
        #
        # Degrade instead: everything except far-channel re-clustering works
        # identically. `_missing_revision_support` is surfaced by the caller so
        # this is visible rather than silent.
        revision_event = getattr(StreamingEvents, "SpeakerRevision", None)
        if revision_event is not None:
            client.on(revision_event, self._handle_revision)
        self.revisions_supported = revision_event is not None

    # -- provider callbacks ----------------------------------------------

    def _handle_begin(self, _client, _event) -> None:
        self.live = True

    def _handle_turn(self, _client, event) -> None:
        text = getattr(event, "transcript", None)
        if text is None or not hasattr(event, "turn_order"):
            # Not a Turn we understand. Name the TYPE only — the payload is
            # conversation audio or its transcript either way.
            log.warning(
                "live: skipping unrecognised %s payload on the %s channel",
                type(event).__name__, self.channel.value,
            )
            return
        if not text.strip():
            # A silent turn boundary, not something to show. Dropping it keeps
            # blank lines out of the operator's scrollback.
            return
        self._on_event(LiveTurn(
            channel=self.channel,
            text=text,
            # Never a display name: the label -> name map is the surface's, so
            # that a later revision reassigns lines without invalidating it.
            speaker=self._speaker_of(event),
            turn_order=event.turn_order,
            is_final=bool(getattr(event, "end_of_turn", False)),
        ))

    def _handle_revision(self, _client, event) -> None:
        # `revisions` is a LIST — one provider event corrects N earlier turns.
        for item in getattr(event, "revisions", None) or []:
            turn_order = getattr(item, "turn_order", None)
            if turn_order is None or not hasattr(item, "speaker_label"):
                # Enrichment, not the transcript: skip this one item, keep the
                # rest and the session. Name the TYPE only — an item carries
                # words from the conversation.
                log.warning(
                    "live: skipping a malformed speaker revision item (%s) on the %s channel",
                    type(item).__name__, self.channel.value,
                )
                continue
            self._on_event(LiveSpeakerRevision(
                channel=self.channel,
                turn_order=turn_order,
                speaker=_provider_label(item.speaker_label),
            ))

    def _handle_termination(self, _client, event) -> None:
        seconds = getattr(event, "audio_duration_seconds", None)
        # Left as None when the provider does not say. Reporting 0.0 would
        # understate a real bill on the one surface that reports cost.
        self.billed_seconds = None if seconds is None else float(seconds)

    def _handle_error(self, _client, event) -> None:
        self._on_event(_ChannelFailure(self.channel, str(event)))

    def _speaker_of(self, event) -> Optional[str]:
        if self.channel is LiveChannel.NEAR:
            # Single speaker by construction. Refusing the provider's label even
            # if one arrives keeps "near is the operator" true by structure
            # rather than by configuration.
            return None
        return _provider_label(getattr(event, "speaker_label", None))

    # -- lifecycle --------------------------------------------------------

    def close(self) -> Optional[float]:
        """Terminate gracefully. Never raises — a raise here would replace the
        real failure with its own, and would strand the OTHER session open."""
        try:
            self.client.disconnect(terminate=True)
        except Exception as exc:  # noqa: BLE001 -- must not mask the original
            log.warning(
                "live: %s channel did not disconnect cleanly: %s",
                self.channel.value, redact(str(exc)),
            )
        return self.billed_seconds


class _ChannelFailure:
    """An internal signal that one channel died. Never published."""

    def __init__(self, channel: LiveChannel, detail: str):
        self.channel = channel
        self.detail = detail


class LiveTranscriber:
    """Two provider sessions, one per physical channel.

    Use as a context manager. `__exit__` is the stop guarantee: it covers every
    raise site between start and stop, including exceptions raised by the
    capture iterator or by a consumer of the events. That guarantee is the most
    important error requirement here, because a session left open keeps billing.
    """

    def __init__(
        self,
        bus: EventBus,
        *,
        api_key: str,
        max_speakers: int = DEFAULT_MAX_SPEAKERS,
        client_factory: Optional[Callable[[str], object]] = None,
        redact_pii: bool = False,
        redact_pii_policies: Optional[List[object]] = None,
    ):
        self._bus = bus
        self._api_key = api_key
        self._max_speakers = max_speakers
        self._client_factory = client_factory or self._default_client
        # Off by default deliberately: goal 1 is catching details mid-call —
        # names, numbers — and redaction removes exactly the content the
        # feature exists to surface. Exposed for calls that warrant it.
        self._redact_pii = redact_pii
        self._redact_pii_policies = redact_pii_policies
        self._sessions: Dict[LiveChannel, _ChannelSession] = {}
        self._failure: Optional[_ChannelFailure] = None
        self._started = False
        self._stopped = False

    # -- lifecycle --------------------------------------------------------

    def start(self) -> None:
        if self._started:
            raise LiveTranscriptionError("a live session is already active")

        opened: List[_ChannelSession] = []
        try:
            for channel in _ORDER:
                opened.append(self._connect(channel))
        except BaseException:
            # A half-open start would leave the first session billing with
            # nobody holding it.
            for session in opened:
                session.close()
            raise

        self._sessions = {s.channel: s for s in opened}
        self._started = True
        self._bus.publish(LiveTranscriptionStarted(
            channels=tuple(c.value for c in _ORDER),
        ))

        if any(not s.revisions_supported for s in opened):
            # Observable, not silent. The session is fully usable — only the
            # provider's after-the-fact speaker re-clustering is missing — but
            # the operator must be told, because names they assign will not be
            # retroactively corrected and they would otherwise never know why.
            import assemblyai
            self._bus.publish(Error(
                message=(
                    f"Live transcription is running, but this AssemblyAI SDK "
                    f"({assemblyai.__version__}) is too old for speaker revisions — "
                    f"names will not be corrected if the provider re-clusters. "
                    f"Fix: ./scripts/bootstrap.sh, or "
                    f"pip install -U 'assemblyai>=1.6.1,<2' in the venv you launch from."
                ),
                severity=Severity.WARNING,
                context="live",
            ))
        elif not _revision_interval_supported():
            # The SDK knows the revision EVENT but not the interval PARAMETER
            # (0.64.x-1.5.x): the provider then re-clusters only at session end,
            # and the kwarg in `_params` was dropped without an error. Same
            # remedy, said once, so the operator knows why names lag.
            import assemblyai
            self._bus.publish(Error(
                message=(
                    f"Live transcription is running, but this AssemblyAI SDK "
                    f"({assemblyai.__version__}) cannot ask for speakers to be "
                    f"re-checked every {SPEAKER_REVISION_INTERVAL_MS // 60_000} "
                    f"minutes — labels will only be corrected when the call ends. "
                    f"Fix: ./scripts/bootstrap.sh, or "
                    f"pip install -U 'assemblyai>=1.6.1,<2' in the venv you launch from."
                ),
                severity=Severity.WARNING,
                context="live",
            ))

    def stop(self, reason: str = "stopped") -> None:
        """Idempotent. Terminates BOTH sessions even if one fails to close."""
        if not self._started or self._stopped:
            return
        self._stopped = True

        billed: Dict[LiveChannel, Optional[float]] = {}
        for channel in _ORDER:
            session = self._sessions.get(channel)
            billed[channel] = session.close() if session else None

        self._bus.publish(LiveTranscriptionStopped(
            near_seconds=billed.get(LiveChannel.NEAR),
            far_seconds=billed.get(LiveChannel.FAR),
            reason="failed" if self._failure is not None else reason,
        ))

    def __enter__(self) -> "LiveTranscriber":
        self.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        self.stop()
        return False

    # -- capture ----------------------------------------------------------

    def feed(self, frame: Frame) -> None:
        """Route one frame: near bytes to the near session, far to the far.

        No resampling and no format conversion — the frame is already 16 kHz
        mono s16le per channel, which is exactly what the sessions declare. A
        conversion step is a place a channel swap could hide.
        """
        if not self._started or self._stopped:
            raise LiveTranscriptionError("no live session is active; call start() first")
        self._raise_if_failed()

        for channel, payload in (
            (LiveChannel.NEAR, frame.near),
            (LiveChannel.FAR, frame.far),
        ):
            try:
                self._sessions[channel].client.stream(payload)
            except Exception as exc:  # noqa: BLE001 -- classified below
                self._record_failure(_ChannelFailure(channel, str(exc)))
                break
        self._raise_if_failed()

    # -- failure handling --------------------------------------------------

    def _on_session_event(self, event) -> None:
        if isinstance(event, _ChannelFailure):
            self._record_failure(event)
            return
        self._bus.publish(event)

    def _record_failure(self, failure: _ChannelFailure) -> None:
        if self._failure is None:
            self._failure = failure
            self._publish_error(failure.detail, failure.channel)

    def _raise_if_failed(self) -> None:
        if self._failure is None:
            return
        # One channel down stops the whole session. A scrollback silently
        # missing one side of a conversation is worse than a stopped one,
        # because the operator cannot see that it is incomplete.
        raise LiveTranscriptionError(
            f"live transcription stopped: the {self._failure.channel.value} channel "
            f"failed ({redact(self._failure.detail)})"
        )

    def _publish_error(self, detail: str, channel: LiveChannel) -> None:
        actionable = classify_failure(detail) == CLASS_OPERATOR_ACTIONABLE
        advice = (
            remediation(detail, next_action=_LIVE_NEXT_ACTION)
            if actionable else redact(detail)
        )
        self._bus.publish(Error(
            message=f"Live transcription stopped on the {channel.value} channel — {advice}",
            severity=Severity.ERROR if actionable else Severity.WARNING,
            context="live",
        ))

    # -- provider plumbing -------------------------------------------------

    def _connect(self, channel: LiveChannel) -> _ChannelSession:
        client = self._client_factory(channel.value)
        session = _ChannelSession(channel, client, self._on_session_event)
        try:
            client.connect(self._params(channel))
        except Exception as exc:  # noqa: BLE001 -- classified for the operator
            detail = str(exc)
            self._publish_error(detail, channel)
            raise LiveTranscriptionError(
                f"could not start live transcription on the {channel.value} "
                f"channel ({redact(detail)})"
            ) from exc
        return session

    def _params(self, channel: LiveChannel) -> StreamingParameters:
        diarize = channel is LiveChannel.FAR
        return StreamingParameters(
            # By reference, never a second literal: a duplicate would let the
            # capture rate and the declared rate drift apart in silence.
            sample_rate=SAMPLE_RATE_HZ,
            encoding=Encoding.pcm_s16le,
            speaker_labels=True if diarize else None,
            max_speakers=self._max_speakers if diarize else None,
            speaker_labels_revision_interval_ms=(
                SPEAKER_REVISION_INTERVAL_MS if diarize else None
            ),
            min_turn_silence=FAR_MIN_TURN_SILENCE_MS if diarize else None,
            max_turn_silence=FAR_MAX_TURN_SILENCE_MS if diarize else None,
            redact_pii=True if self._redact_pii else None,
            redact_pii_policies=self._redact_pii_policies if self._redact_pii else None,
        )

    def _default_client(self, _channel: str) -> StreamingClient:
        return StreamingClient(StreamingClientOptions(api_key=self._api_key))
