"""Jensen realtime capture — device bytes to two 16 kHz mono PCM streams.

Wire constants measured on real hardware 2026-08-26 (P1 HDP1252405573, fw 1.4.5):

    START     CMD 33  body 00000001 00000002   -> ack 00, 64,040 B/s
    TRANSFER  CMD 34  body <empty>             -> 6408 B chunks, ~10/sec
    STOP      CMD 33  body 00000000 00000000   -> ack 00

Stream is 16 kHz 16-bit STEREO: channel 1 = near end (operator mic),
channel 2 = far end (what the Mac plays out through the P1). Proven by a 1 kHz
tone played out producing a 38 dB peak in channel 2 ONLY while channel 1 fell.

Upstream sgeraldes/hidock-next is wrong in four places (its start body is
refused by real firmware, its stop body is wrong, it sends a u32 offset the
device does not want, and it hardcodes `sampleRate: 16000, channels: 1` in a
function that never reads the device). These tests pin the MEASURED contract.

PRD: projects/hidock_direct/planning/realtime_capture_prd.md
"""

from __future__ import annotations

import struct
from typing import List, Optional, Tuple

import pytest

from hidock_direct.realtime import (
    Frame,
    RealtimeSession,
    RealtimeUnavailable,
    START_BODY,
    STOP_BODY,
)

CMD_CONTROL = 33
CMD_TRANSFER = 34


def interleave(near: List[int], far: List[int]) -> bytes:
    """Build a stereo s16le payload from two mono channels."""
    assert len(near) == len(far)
    out = bytearray()
    for n, f in zip(near, far):
        out += struct.pack("<hh", n, f)
    return bytes(out)


class FakeJensen:
    """Scripted Jensen. Records every (cmd, body) so tests can pin the wire."""

    def __init__(self, transfers: Optional[List[Optional[bytes]]] = None,
                 start_ack: bytes = b"\x00", stop_ack: bytes = b"\x00"):
        self.sent: List[Tuple[int, bytes]] = []
        self._transfers = list(transfers or [])
        self._start_ack = start_ack
        self._stop_ack = stop_ack
        self.raise_on_transfer: Optional[BaseException] = None

    def _send_and_receive(self, cmd, body=b"", timeout_ms=5000):
        self.sent.append((cmd, bytes(body)))
        if cmd == CMD_CONTROL:
            ack = self._start_ack if bytes(body) == START_BODY else self._stop_ack
            return {"id": cmd, "body": ack}
        if cmd == CMD_TRANSFER:
            if self.raise_on_transfer is not None:
                raise self.raise_on_transfer
            if not self._transfers:
                return {"id": cmd, "body": b""}
            nxt = self._transfers.pop(0)
            return None if nxt is None else {"id": cmd, "body": nxt}
        raise AssertionError(f"unexpected command {cmd}")

    # -- helpers for assertions ---------------------------------------

    def control_bodies(self) -> List[bytes]:
        return [b for c, b in self.sent if c == CMD_CONTROL]

    def transfer_bodies(self) -> List[bytes]:
        return [b for c, b in self.sent if c == CMD_TRANSFER]


class FakeAdapter:
    def __init__(self, jensen: FakeJensen, connected: bool = True):
        self._jensen = jensen
        self._connected = connected

    def is_connected(self) -> bool:
        return self._connected


def payload(near: List[int], far: List[int], header: bytes = b"\x00\x00\x00\x00") -> bytes:
    return header + interleave(near, far)


def session(jensen: FakeJensen, **kw) -> RealtimeSession:
    kw.setdefault("settle_seconds", 0.0)
    kw.setdefault("sleep", lambda _s: None)
    kw.setdefault("idle_timeout_seconds", 0.05)
    kw.setdefault("poll_interval_seconds", 0.0)
    return RealtimeSession(FakeAdapter(jensen), **kw)


# -- wire contract (the measured constants) ------------------------------


def test_start_body_is_the_measured_constant():
    """`trailing=1` yields 8 kHz and `4` WEDGES the device into needing a power
    cycle. Pin the exact bytes, not 'some body was sent'."""
    assert START_BODY == bytes.fromhex("0000000100000002")


def test_stop_body_is_the_measured_constant():
    """Upstream's stop body is [0,0,0,2,0,0,0,1]; real firmware wants zeros."""
    assert STOP_BODY == bytes.fromhex("0000000000000000")


def test_start_sends_exactly_the_start_body():
    j = FakeJensen()
    with session(j):
        pass
    assert j.control_bodies()[0] == bytes.fromhex("0000000100000002")


def test_stop_sends_exactly_the_stop_body():
    j = FakeJensen()
    with session(j):
        pass
    assert j.control_bodies()[-1] == bytes.fromhex("0000000000000000")


def test_transfer_request_body_is_empty_not_an_offset():
    """Upstream sends a u32 read cursor. The device does not want one -- all 240
    vendor requests carried blen=0."""
    j = FakeJensen(transfers=[payload([1, 2], [3, 4]), b""])
    with session(j) as s:
        list(s.frames())
    assert j.transfer_bodies(), "no transfer was attempted"
    assert all(b == b"" for b in j.transfer_bodies()), (
        f"transfer must send an empty body; got {j.transfer_bodies()!r}"
    )


# -- start / stop lifecycle ----------------------------------------------


def test_non_zero_start_ack_raises_with_the_ack_byte():
    j = FakeJensen(start_ack=b"\x01")
    with pytest.raises(RealtimeUnavailable) as exc:
        with session(j):
            pass
    assert "01" in str(exc.value)


def test_refused_start_does_not_send_stop():
    """Nothing was started, so there is nothing to stop -- and sending stop would
    mutate device state we never entered."""
    j = FakeJensen(start_ack=b"\x01")
    with pytest.raises(RealtimeUnavailable):
        with session(j):
            pass
    assert j.control_bodies() == [START_BODY]


@pytest.mark.parametrize("where", ["consumer", "transfer"])
def test_stop_runs_when_an_exception_is_raised(where):
    """FR-1.2: stop from a finally covering EVERY raise site. Precedent 22061ab,
    where a terminal event had one publisher on the happy path and four raise
    sites."""
    j = FakeJensen(transfers=[payload([1], [2]), payload([3], [4]), b""])
    if where == "transfer":
        j.raise_on_transfer = RuntimeError("boom")
    with pytest.raises(RuntimeError):
        with session(j) as s:
            for _frame in s.frames():
                if where == "consumer":
                    raise RuntimeError("boom")
    assert j.control_bodies()[-1] == STOP_BODY, "stop did not run on the exception path"


def test_stop_is_idempotent():
    j = FakeJensen()
    s = session(j)
    s.start()
    s.stop()
    n = len(j.control_bodies())
    s.stop()
    assert len(j.control_bodies()) == n, "second stop re-sent the control command"


def test_a_raising_stop_does_not_mask_the_original_exception():
    """A stop that raises inside a finally replaces the real failure with its own."""
    class Angry(FakeJensen):
        def _send_and_receive(self, cmd, body=b"", timeout_ms=5000):
            if cmd == CMD_CONTROL and bytes(body) == STOP_BODY:
                raise OSError("stop exploded")
            return super()._send_and_receive(cmd, body, timeout_ms)

    j = Angry(transfers=[payload([1], [2])])
    with pytest.raises(RuntimeError, match="original"):
        with session(j) as s:
            for _f in s.frames():
                raise RuntimeError("original")


def test_second_start_while_active_raises():
    j = FakeJensen()
    s = session(j)
    s.start()
    try:
        with pytest.raises(RealtimeUnavailable):
            s.start()
    finally:
        s.stop()


def test_settle_interval_is_enforced_between_sessions():
    """Rapid start/stop cycling wedges the device (observed twice, 2026-08-26)."""
    slept: List[float] = []
    j = FakeJensen()
    s = RealtimeSession(FakeAdapter(j), settle_seconds=1.0, sleep=slept.append)
    s.start(); s.stop()
    s.start(); s.stop()
    assert slept and any(v > 0 for v in slept), "no settle interval was observed"


def test_start_requires_a_connected_adapter():
    """FR-4.1: connecting has a device side effect and belongs to the caller."""
    j = FakeJensen()
    s = RealtimeSession(FakeAdapter(j, connected=False), settle_seconds=0.0,
                        sleep=lambda _s: None)
    with pytest.raises(RealtimeUnavailable):
        s.start()
    assert j.sent == [], "a disconnected adapter must not be driven"


# -- framing / de-interleave ---------------------------------------------


def test_channels_are_de_interleaved_exactly():
    near = [100, 200, 300]
    far = [-100, -200, -300]
    j = FakeJensen(transfers=[payload(near, far), b""])
    with session(j) as s:
        frames = list(s.frames())
    assert len(frames) == 1
    assert frames[0].near == struct.pack("<3h", *near)
    assert frames[0].far == struct.pack("<3h", *far)


def test_channel_order_is_not_reversed():
    """THE assertion a byte-count check cannot make. An asymmetric fixture --
    near silent, far loud -- fails loudly if the de-interleave is swapped, which
    would attribute the operator's words to the far end."""
    near = [0, 0, 0, 0]
    far = [9000, -9000, 9000, -9000]
    j = FakeJensen(transfers=[payload(near, far), b""])
    with session(j) as s:
        frame = next(iter(s.frames()))
    assert set(frame.near) == {0}, "near channel picked up far-end audio"
    assert set(frame.far) != {0}, "far channel is silent -- channels are swapped"


def test_four_byte_header_is_discarded():
    j = FakeJensen(transfers=[payload([7], [8], header=b"\xde\xad\xbe\xef"), b""])
    with session(j) as s:
        frame = next(iter(s.frames()))
    assert b"\xde\xad\xbe\xef" not in frame.near + frame.far


def test_header_value_does_not_drive_flow_control():
    """Observed header values cycle 0/1/2, contradicting upstream's 'bytes
    remaining' claim. A reader that trusted it would stop early."""
    frames_in = [
        payload([1], [1], header=struct.pack(">I", 0)),
        payload([2], [2], header=struct.pack(">I", 0)),
        payload([3], [3], header=struct.pack(">I", 0)),
        b"",
    ]
    j = FakeJensen(transfers=frames_in)
    with session(j) as s:
        frames = list(s.frames())
    assert len(frames) == 3, "reader stopped early on a zero header"


def test_sequence_numbers_are_monotonic():
    j = FakeJensen(transfers=[payload([1], [1]), payload([2], [2]), b""])
    with session(j) as s:
        seqs = [f.seq for f in s.frames()]
    assert seqs == sorted(seqs) and len(set(seqs)) == len(seqs)


def test_frame_carries_no_timestamp():
    """A field named for a time would assert a fact the device does not report --
    the shape that produced the recorded_at defect across 670 transcripts."""
    assert not any(
        "time" in f.lower() or "_at" in f.lower() for f in Frame.__dataclass_fields__
    ), f"Frame fields: {list(Frame.__dataclass_fields__)}"


# -- response-variant matrix (PRD §6) ------------------------------------


@pytest.mark.parametrize("body", [b"", b"\x00\x00\x00\x00"], ids=["empty", "header-only"])
def test_short_bodies_yield_no_frame(body):
    j = FakeJensen(transfers=[body, payload([5], [6]), b""])
    with session(j) as s:
        frames = list(s.frames())
    assert len(frames) == 1


def test_misaligned_payload_is_dropped_not_realigned(caplog):
    """Realigning silently swaps the channels. Drop and warn instead."""
    bad = b"\x00\x00\x00\x00" + b"\x01\x02\x03"  # 3 bytes: not a whole stereo frame
    j = FakeJensen(transfers=[bad, payload([4], [5]), b""])
    with session(j) as s:
        frames = list(s.frames())
    assert len(frames) == 1, "misaligned payload produced a frame"
    assert frames[0].near == struct.pack("<h", 4)


def test_none_response_is_transient_then_terminates():
    j = FakeJensen(transfers=[None] * 50)
    with pytest.raises(RealtimeUnavailable):
        with session(j, max_consecutive_misses=3) as s:
            list(s.frames())
    assert j.control_bodies()[-1] == STOP_BODY


def test_large_backlog_is_drained_in_one_pass():
    """FR-2.4: no fixed sleep WHILE DATA IS AVAILABLE.

    A fixed 100 ms interval between reads dropped ~50% of samples in probe runs
    against the real device. Idle polling may sleep -- that is what bounds a hot
    spin once the device has nothing buffered -- so the invariant is about
    sleeping *between frames*, not about sleeping at all.
    """
    events: List[str] = []
    j = FakeJensen(transfers=[payload([i], [i]) for i in range(1, 21)] + [b""])
    s = RealtimeSession(
        FakeAdapter(j),
        settle_seconds=0.0,
        sleep=lambda v: events.append("sleep") if v > 0 else None,
        idle_timeout_seconds=0.05,
        poll_interval_seconds=0.01,
    )
    s.start()
    try:
        frames = []
        for f in s.frames():
            frames.append(f)
            events.append("frame")
    finally:
        s.stop()
    assert len(frames) == 20
    last_frame = len(events) - 1 - events[::-1].index("frame")
    assert "sleep" not in events[:last_frame], (
        f"slept while data was still available: {events[:last_frame]}"
    )


def test_usb_error_terminates_and_names_the_power_cycle_remedy():
    """A wedged device needs a power cycle -- a replug is NOT sufficient."""
    class FakeUSBError(Exception):
        pass

    j = FakeJensen(transfers=[payload([1], [1])])
    j.raise_on_transfer = FakeUSBError("Operation timed out")
    with pytest.raises(RealtimeUnavailable) as exc:
        with session(j, usb_errors=(FakeUSBError,)) as s:
            list(s.frames())
    assert "power" in str(exc.value).lower() and "cycle" in str(exc.value).lower()
    assert j.control_bodies()[-1] == STOP_BODY


# -- no audio in diagnostics ---------------------------------------------


def test_exception_messages_carry_no_audio_bytes():
    """This module handles live conversation audio. Byte counts and ack codes
    only -- never payload content."""
    secret = struct.pack("<4h", 1234, 5678, 4321, 8765)
    bad = b"\x00\x00\x00\x00" + secret + b"\x01"
    j = FakeJensen(transfers=[bad] * 50)
    with pytest.raises(RealtimeUnavailable) as exc:
        with session(j, max_consecutive_misses=3) as s:
            list(s.frames())
    assert secret.hex() not in str(exc.value)
    assert secret not in str(exc.value).encode("latin-1", "ignore")


# -- conditional-branch visibility (Gate 1 step 7) -----------------------


def test_missing_pyusb_yields_no_usb_error_types_and_says_so(monkeypatch, caplog):
    """`_default_usb_errors` degrades to () when pyusb is absent. pyusb IS a
    declared runtime dependency, so this branch is unreachable in a correct
    install -- but an untested degraded branch that returns a falsy value
    silently is the shape of the H-2 silence bug, so cover it and make it
    speak."""
    import builtins

    from hidock_direct import realtime

    real_import = builtins.__import__

    def no_usb(name, *a, **kw):
        if name.startswith("usb"):
            raise ImportError("no pyusb")
        return real_import(name, *a, **kw)

    monkeypatch.setattr(builtins, "__import__", no_usb)
    with caplog.at_level("WARNING"):
        assert realtime._default_usb_errors() == ()
    assert any("pyusb" in r.message for r in caplog.records), (
        "degraded branch returned () with no operator-visible signal"
    )
