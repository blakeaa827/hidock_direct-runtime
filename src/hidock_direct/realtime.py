"""Live two-channel audio capture over the Jensen realtime endpoint.

Turns device bytes into two independent 16 kHz mono PCM streams: the near end
(the operator's own voice) and the far end (everyone else). Nothing here is
persisted — the 48 kHz flash recording remains the authoritative artifact and
this module writes no file, no ledger entry, and no archive path.

WIRE CONTRACT — measured on real hardware 2026-08-26 (HiDock P1
HDP1252405573, firmware 1.4.5), NOT taken from upstream, which is wrong in
four places (see the note below)::

    START     CMD 33  body 00000001 00000002   -> ack 00
    TRANSFER  CMD 34  body <empty>             -> 6408 B chunks, ~10/sec
    STOP      CMD 33  body 00000000 00000000   -> ack 00

Stream: 16 kHz, 16-bit, **stereo**. Channel 1 is the near end, channel 2 the
far end — proven by playing a 1 kHz tone out through the P1 and observing a
38 dB peak in channel 2 only, while channel 1 *fell* (the device's echo
canceller removing it from the mic path).

Where upstream `sgeraldes/hidock-next` disagrees, it is wrong: its
`startRealtime` body is refused by real firmware, its stop body is wrong, it
sends a u32 read cursor the device does not want, and its `getRealtimeSettings`
hardcodes ``sampleRate: 16000, channels: 1`` in a function that never reads the
device. It has never been executed against hardware. Treat it as a hypothesis
source, never a protocol oracle.
"""

from __future__ import annotations

import logging
import struct
import time
from dataclasses import dataclass
from typing import Callable, Iterator, Optional, Sequence, Tuple

log = logging.getLogger(__name__)

# CMD 33 body. The trailing u32 is a SAMPLE-RATE SELECTOR, not a magic constant:
# 1 -> 8 kHz, 2 -> 16 kHz, 4 -> invalid (times out AND wedges the device into a
# state that needs a power cycle, not merely a replug -- observed twice).
# Deliberately not a parameter: a value the module cannot send cannot be sent
# wrongly, and only 16 kHz is useful.
START_BODY = bytes.fromhex("0000000100000002")
STOP_BODY = bytes.fromhex("0000000000000000")

_CMD_REALTIME_CONTROL = 33
_CMD_REALTIME_TRANSFER = 34

_HEADER_BYTES = 4       # leading 4 bytes of each transfer body; NOT audio
_BYTES_PER_FRAME = 4    # stereo * int16
SAMPLE_RATE_HZ = 16000
CHANNELS = 2


class RealtimeUnavailable(RuntimeError):
    """The realtime stream could not be started, or died mid-session."""


@dataclass(frozen=True)
class Frame:
    """One de-interleaved chunk of live audio.

    Deliberately carries no timestamp. A field named for a time would assert a
    fact the device does not report — the shape that produced the `recorded_at`
    defect across 670 transcripts. `seq` asserts ordering only.
    """

    near: bytes   # 16 kHz mono s16le — the operator
    far: bytes    # 16 kHz mono s16le — the far end
    seq: int


def _default_usb_errors() -> Tuple[type, ...]:
    """pyusb error types, resolved lazily so this module imports without it."""
    try:
        import usb.core  # lazy
    except ImportError:
        # pyusb is a declared runtime dependency, so this is unreachable in a
        # correct install. Say so rather than degrading silently: a falsy return
        # here means USB transport errors propagate raw instead of being
        # translated into the power-cycle remedy.
        log.warning(
            "pyusb unavailable — USB transport errors will not be translated "
            "into operator-actionable text"
        )
        return ()
    return (usb.core.USBError,)


def _deinterleave(payload: bytes) -> Optional[Tuple[bytes, bytes]]:
    """Split stereo s16le into two mono buffers, or None if not frame-aligned.

    Never realigns a partial frame: a misaligned de-interleave silently swaps
    the channels, which would attribute the operator's words to the far end.
    """
    if len(payload) % _BYTES_PER_FRAME:
        return None
    count = len(payload) // _BYTES_PER_FRAME
    samples = struct.unpack(f"<{count * CHANNELS}h", payload)
    near = struct.pack(f"<{count}h", *samples[0::2])
    far = struct.pack(f"<{count}h", *samples[1::2])
    return near, far


class RealtimeSession:
    """A single live-capture session against one connected device.

    Use as a context manager. `__exit__` is the stop guarantee: it covers every
    raise site between start and stop, including exceptions raised by the
    consumer of `frames()`. Driving `start()`/`frames()` without the context
    manager forfeits that guarantee and is not the supported shape.
    """

    def __init__(
        self,
        adapter,
        *,
        settle_seconds: float = 1.0,
        max_consecutive_misses: int = 5,
        sleep: Callable[[float], None] = time.sleep,
        usb_errors: Optional[Sequence[type]] = None,
    ):
        self._adapter = adapter
        self._settle = max(0.0, settle_seconds)
        self._max_misses = max(1, max_consecutive_misses)
        self._sleep = sleep
        self._usb_errors = tuple(usb_errors) if usb_errors is not None else _default_usb_errors()
        self._active = False
        self._seq = 0
        self._last_stop_at: Optional[float] = None

    # -- lifecycle -------------------------------------------------------

    def start(self) -> None:
        if self._active:
            # Never silently restart: rapid start/stop cycling is the observed
            # cause of device wedging.
            raise RealtimeUnavailable("a realtime session is already active")
        if not self._adapter.is_connected():
            # Connecting has a side effect on the device and belongs to the
            # caller's lifecycle, not to this module.
            raise RealtimeUnavailable("device is not connected")

        self._await_settle()
        ack = self._control(START_BODY)
        if ack[:1] != b"\x00":
            raise RealtimeUnavailable(
                f"device refused realtime start (ack={ack.hex() or 'empty'}); "
                "the firmware may not support it in this mode"
            )
        self._active = True
        self._seq = 0

    def stop(self) -> None:
        """Idempotent, and never raises.

        A stop that raises from inside a `finally` replaces the real failure
        with its own, so transport errors here are logged and swallowed.
        """
        if not self._active:
            return
        self._active = False
        self._last_stop_at = time.monotonic()
        try:
            self._control(STOP_BODY)
        except Exception as exc:  # noqa: BLE001 -- must not mask the original
            log.warning("realtime stop failed: %s", exc)

    def __enter__(self) -> "RealtimeSession":
        self.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        self.stop()
        return False

    # -- capture ---------------------------------------------------------

    def frames(self) -> Iterator[Frame]:
        """Drain the device until it stops producing audio.

        Polls as fast as the bus allows and does NOT sleep between reads while
        data is available — a fixed 100 ms interval dropped ~50% of samples in
        probe runs against the real device, which emits ~10 chunks/sec.

        Ends normally on a run of empty responses (the device has nothing to
        send). Raises `RealtimeUnavailable` on a run of *failed* responses —
        no reply at all, or payloads that are not frame-aligned. Those are
        different conditions and are deliberately not collapsed.
        """
        if not self._active:
            raise RealtimeUnavailable("session is not active; call start() first")

        empty_streak = 0
        error_streak = 0

        while True:
            try:
                response = self._transfer()
            except self._usb_errors as exc:  # type: ignore[misc]
                raise RealtimeUnavailable(
                    f"device stopped responding ({type(exc).__name__}); "
                    "power cycle the device — a replug is not sufficient"
                ) from exc

            if response is None:
                error_streak += 1
                empty_streak = 0
                if error_streak > self._max_misses:
                    raise RealtimeUnavailable(
                        f"device stopped responding after {error_streak} attempts"
                    )
                continue

            if len(response) <= _HEADER_BYTES:
                # A valid answer meaning "nothing available" -- not a failure.
                empty_streak += 1
                error_streak = 0
                if empty_streak > self._max_misses:
                    return
                continue

            # The leading 4 bytes are a header whose meaning is UNKNOWN:
            # observed values cycle 0/1/2, which contradicts upstream's "bytes
            # remaining" claim. Discarded, and deliberately not used for flow
            # control.
            split = _deinterleave(response[_HEADER_BYTES:])
            if split is None:
                error_streak += 1
                empty_streak = 0
                log.warning(
                    "realtime payload not frame-aligned (%d audio bytes); dropping",
                    len(response) - _HEADER_BYTES,
                )
                if error_streak > self._max_misses:
                    raise RealtimeUnavailable(
                        f"device sent {error_streak} misaligned payloads in a row"
                    )
                continue

            empty_streak = 0
            error_streak = 0
            near, far = split
            self._seq += 1
            yield Frame(near=near, far=far, seq=self._seq)

    # -- transport -------------------------------------------------------

    def _control(self, body: bytes) -> bytes:
        response = self._jensen()._send_and_receive(
            _CMD_REALTIME_CONTROL, body, timeout_ms=5000
        )
        return bytes((response or {}).get("body") or b"")

    def _transfer(self) -> Optional[bytes]:
        # EMPTY body. Upstream sends a u32 read cursor; the device does not want
        # one -- all 240 vendor requests carried blen=0.
        response = self._jensen()._send_and_receive(
            _CMD_REALTIME_TRANSFER, b"", timeout_ms=3000
        )
        if response is None:
            return None
        return bytes(response.get("body") or b"")

    def _jensen(self):
        jensen = getattr(self._adapter, "_jensen", None)
        if jensen is None:
            raise RealtimeUnavailable("adapter exposes no Jensen transport")
        return jensen

    def _await_settle(self) -> None:
        """A wedged device needs a power cycle, so pace restarts in-module
        rather than trusting every caller to remember."""
        if self._last_stop_at is None or self._settle <= 0:
            return
        remaining = self._settle - (time.monotonic() - self._last_stop_at)
        if remaining > 0:
            self._sleep(remaining)
