"""Live transcription surface — the local page, the wiring, and the mutual exclusion.

Phase 1c. PRD: `projects/hidock_direct/planning/live_surface_prd.md`.

Three properties shape every design decision in this module:

1. **It is an access-controlled network service.** A local HTTP server serving a
   live conversation transcript is readable by any process running as the
   operator, and — at a guessable `http://127.0.0.1:PORT` — by any page they
   have open. Loopback binding is NOT an access control. Hence a per-session
   256-bit token required on every route but `/health`, an OS-assigned port, and
   a server that dies with the session (FR-1.1/1.2/1.3/1.6, §8).

   **The token never appears in a URL.** Launching the window means handing a
   URL to a browser as a command-line argument, and argv is world-readable to
   every process running as the operator (`ps -axww`) — so a URL-borne token is
   readable by exactly the class of process the token exists to exclude. What
   goes into the URL instead is a single-use LAUNCH TICKET with a ten-second
   life; `GET /` exchanges it, once, for the session token in an `HttpOnly`
   cookie, and the SSE stream and the name POSTs accept that cookie and nothing
   else. `mint_ticket` and `launch_app_window` carry the residual risk.

2. **Naming is a render-time projection.** The ring stores the provider's own
   `label`; the operator's `label -> name` map is applied when a turn is SENT.
   That is what makes a name typed mid-call reach lines already on screen, and
   what lets a `LiveSpeakerRevision` re-render a line under another label's name
   without touching the map (FR-3.1..3.5).

3. **The device is claimed by exactly one consumer at a time.** `App._worker_loop`
   polls `get_file_count()` and `RealtimeSession` issues CMD 33/34 through the
   same Jensen endpoint. Two threads issuing Jensen commands concurrently
   interleave request/response pairs, so a live session suspends offload polling
   for its duration and releases it on **every** exit path (§3.6).

The SURFACE persists nothing. No file, no ledger entry, no archive path: its
state is the turn ring, the name map, the token, the port and the subscriber set,
all discarded on stop (NFR-6).

The SESSION does persist, since phase 1d, and the two facts are not in conflict.
`LiveSessionController` composes a `LiveArchive` alongside the capture and the
bridge, because the device **stops its own recording while it streams and never
persists the live-session audio** (operator hardware test 2026-08-27: a power
cycle and a rescan produced nothing to offload) — so phases 1a–1c bought a live
transcript at the cost of the recording, and nobody chose that trade. Every byte
that reaches the disk is written by `live_archive.py`; this module hands it
frames as they arrive and, at stop, the operator's `label -> name` map. The split
is what keeps NFR-6 true of the surface while `live_archive_prd.md` FR-1.1 is
true of the session.

Nothing here logs transcript text, audio bytes, the token, or the session URL
(which carries the token). The URL reaches the operator over the event bus, which
is the TUI's activity log, not the logging module (NFR-4).
"""

from __future__ import annotations

import contextlib
import http.cookies
import http.server
import json
import logging
import queue
import secrets
import socketserver
import subprocess
import sys
import threading
import time
import urllib.parse
import webbrowser
from collections import deque
from typing import Callable, Dict, List, Optional

from .config import parse_speaker_count
from .events import (
    Error,
    EventBus,
    LiveChannel,
    LiveSpeakerRevision,
    LiveTranscriptionStopped,
    LiveTurn,
    Severity,
)

log = logging.getLogger(__name__)

# FR-1.5. Bounded on purpose: unbounded growth on a long call is a memory leak in
# a process that is also holding a USB stream open.
DEFAULT_RING_SIZE = 2000

# FR-4.2. Neutral and true for every user of a public clone. Never the
# maintainer's name, and never derived from the machine account — an account name
# is frequently a handle, so deriving it would put a stranger's login in their own
# transcript.
DEFAULT_OPERATOR_NAME = "Me"

# The ceiling a composition that supplies none falls back to — 1b's value,
# carried through so the two phases cannot disagree. NOT what the operator
# sees: `__main__` always passes `config.live_max_speakers`, which is what
# the `l` prompt prepopulates with.
DEFAULT_MAX_SPEAKERS = 6

# How long the launcher waits on a browser process before deciding the window is
# up. `l` runs on the TUI's keyboard thread, so the wait has to be bounded.
LAUNCH_TIMEOUT_SECONDS = 3.0

# How often an idle SSE connection writes a comment line. Comments are ignored by
# `EventSource`; they exist so a dead socket is discovered during a quiet stretch
# rather than at the next turn, and so a client reading with its own receive
# timeout never trips it — a tripped timeout poisons the socket for good on some
# clients, which turns a quiet call into a silently dead page.
_KEEPALIVE_SECONDS = 0.25

# A stream that has carried nothing but keep-alives for this long is dropped, and
# the page reconnects with `Last-Event-ID` (which is why the reconnect costs
# nothing: the server replays only what the page actually missed). Without this a
# half-dead socket — a closed lid, a killed browser — is held open for the rest of
# the call by a process that is also holding a USB stream.
_STREAM_IDLE_SECONDS = 4.0

# Reconnect delay handed to `EventSource`, in milliseconds. The default is several
# seconds, which is long enough to lose turns from the middle of a sentence.
_RETRY_MS = 250

_QUEUE_POLL_SECONDS = 0.1

# How long a stalled connection may hold a handler thread. Set as a class
# attribute on the handler so `StreamRequestHandler.setup()` puts it on the
# socket, which makes it a PER-OPERATION deadline, not a deadline on the
# connection: the SSE response writes a keep-alive every `_KEEPALIVE_SECONDS`
# and never reads again after its request line, so a live stream can never
# approach it. What it does bound is a connection that opens and then says
# nothing — reachable BEFORE any authentication check by any local process, and
# otherwise able to park a thread for the rest of the call.
_REQUEST_TIMEOUT_SECONDS = 10.0

# The session credential rides in a cookie, never in a URL (see `mint_ticket`).
_COOKIE_NAME = "hidock_live"

# The only body type `/names` accepts. `application/json` is NOT a CORS "simple"
# content type, so a cross-origin page cannot send one without a preflight this
# server never answers — while `text/plain` (or a plain `<form>` encoding) would
# sail straight through with the ambient cookie attached. Requiring JSON is the
# half of the CSRF defence that does not depend on a request header at all.
_JSON_CONTENT_TYPE = "application/json"

# `Sec-Fetch-Site` is set by the browser and is a forbidden header name, so page
# script cannot write it. `same-origin` is the only value this surface's own page
# can produce on a POST.
_SAME_ORIGIN = "same-origin"

# The launch ticket goes into `--app=<url>`, i.e. into the browser's argv, where
# any process running as the operator can read it out of `ps -axww`. Ten seconds
# is the whole budget between spawning the browser and the browser's first GET.
_LAUNCH_TICKET_TTL_SECONDS = 10.0

# The ticket surfaced in the activity log for the operator to open by hand when
# no browser could be launched (FR-ERR-2/FR-5.4). It never enters argv and never
# reaches the logging module — it is on the operator's own screen, alongside the
# transcript it would unlock — so it is allowed to outlive the launch attempt by
# long enough to be read and typed.
_MANUAL_TICKET_TTL_SECONDS = 300.0

_CLOSE = object()


def _as_bytes(value) -> Optional[bytes]:
    """Encode an attacker-supplied credential for `secrets.compare_digest`.

    `compare_digest` raises `TypeError` on a non-ASCII `str`, and every value
    compared here arrives from the network. Raising inside the handler thread
    would produce no response at all — no `403`, just a dead thread and a
    traceback on stderr — so a malformed credential is normalised to `None` here
    and takes the ordinary refusal path (FR-ERR-5, §3.8).
    """
    if not isinstance(value, str) or not value:
        return None
    try:
        return value.encode("utf-8")
    except UnicodeError:
        return None


class LiveSessionError(RuntimeError):
    """A live session could not be started, or was refused."""


# ---------------------------------------------------------------------------
# The page — one self-contained document (FR-2.1, NFR-5)
# ---------------------------------------------------------------------------
#
# No CDN, no external font, no external script: the page must render with the
# network down, and must not announce the operator's call to a third party by
# fetching an asset. It is also STATIC — turns arrive over SSE and are inserted
# with `textContent`, never as markup, because transcript content is
# third-party-derived text arriving over the network (FR-2.6).
#
# Search is `⌘F` find-in-page (FR-2.4). It is better than anything specified
# here, it is the reason a web surface was chosen at all, and it is free.

PAGE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>HiDock Live</title>
<style>
  :root { color-scheme: dark; }
  * { box-sizing: border-box; }
  body {
    margin: 0; height: 100vh; display: flex; flex-direction: column;
    font-family: ui-sans-serif, system-ui, -apple-system, "Helvetica Neue", sans-serif;
    background: #14161a; color: #e8eaed;
  }
  header {
    display: flex; align-items: center; gap: 1rem; padding: .6rem 1rem;
    border-bottom: 1px solid #2b2f36; background: #191c21;
  }
  /* The `hidden` ATTRIBUTE only works through the UA stylesheet's
     `[hidden] { display: none }`, which ANY author rule with an explicit
     `display` outranks — an id selector trivially so. `#live-indicator` sets
     `display: inline-flex`, so `indicator.hidden = true` set the attribute and
     changed nothing: the session ended and the page still read
     "LIVE — audio is streaming to AssemblyAI" beside "Session ended".
     Observed by the operator 2026-08-27.

     Global and `!important` on purpose. Scoping it to #live-indicator would fix
     the element that happens to have been caught and leave the next one to be
     found the same way — by someone reading a screenshot. */
  [hidden] { display: none !important; }
  #live-indicator {
    font-weight: 600; letter-spacing: .02em; color: #ff5f56;
    display: inline-flex; align-items: center; gap: .45rem;
  }
  #live-indicator .dot {
    width: .6rem; height: .6rem; border-radius: 50%; background: #ff5f56;
    animation: pulse 1.6s ease-in-out infinite;
  }
  @keyframes pulse { 0%,100% { opacity: 1 } 50% { opacity: .25 } }
  #conn { margin-left: auto; font-size: .8rem; color: #98a0ab; }
  #conn.ok { color: #6bbf73; }
  #conn.bad { color: #e0a33e; }
  #billed { font-size: .8rem; color: #98a0ab; }
  /* Deliberately NOT styled like the live indicator: no red, no pulsing dot.
     The two states must not be mistakable for one another at a glance, which
     is the whole reason the indicator clears. */
  #ended {
    font-weight: 600; letter-spacing: .02em; color: #6bbf73;
    display: inline-flex; align-items: center; gap: .45rem;
  }
  #panel-ended {
    margin: 0 0 .6rem; font-size: .78rem; color: #6bbf73; line-height: 1.45;
  }
  main { flex: 1; display: flex; min-height: 0; }
  #feed { flex: 1; overflow-y: auto; padding: 1rem 1.25rem; scroll-behavior: smooth; }
  aside {
    width: 17rem; border-left: 1px solid #2b2f36; background: #191c21;
    padding: 1rem; overflow-y: auto;
  }
  aside h2 { font-size: .75rem; text-transform: uppercase; letter-spacing: .08em;
             color: #98a0ab; margin: 0 0 .75rem; }
  #panel-empty { font-size: .8rem; color: #6d747e; line-height: 1.4; }
  .row { display: flex; flex-direction: column; gap: .2rem; margin-bottom: .7rem; }
  .row .tag { font-size: .7rem; color: #98a0ab; }
  .row input {
    background: #14161a; border: 1px solid #333941; border-radius: 5px;
    color: #e8eaed; padding: .35rem .5rem; font: inherit; font-size: .85rem;
  }
  .line { margin: 0 0 .7rem; line-height: 1.5; display: flex; gap: .6rem; }
  .line .who { flex: 0 0 8.5rem; text-align: right; color: #7fb2f0; font-weight: 600; }
  .line.near .who { color: #6bbf73; }
  .line.anon .who { color: #6d747e; }
  .line.partial .text { color: #98a0ab; font-style: italic; }
  .line.problem { color: #e0a33e; font-size: .85rem; }
  .text { white-space: pre-wrap; }
</style>
</head>
<body>
<header>
  <span id="live-indicator"><span class="dot"></span>LIVE — audio is streaming to AssemblyAI</span>
  <span id="ended" hidden>Call ended — names can still be changed</span>
  <span id="billed" hidden></span>
  <span id="conn">connecting</span>
</header>
<main>
  <section id="feed" aria-label="transcript"></section>
  <aside aria-label="speakers">
    <h2>Speakers</h2>
    <p id="panel-ended" hidden>The call is over. A name typed now is also written into the
      saved transcript.</p>
    <p id="panel-empty">Rows appear here as people speak. Type a name to label every line
      that speaker has said, past and future.</p>
    <div id="rows"></div>
    <template id="speaker-row">
      <div class="row">
        <span class="tag"></span>
        <input type="text" maxlength="64" placeholder="name this speaker" autocomplete="off">
      </div>
    </template>
  </aside>
</main>
<script>
(function () {
  // No credential lives in this script or in this URL. The window is opened at
  // `/?k=<launch ticket>`; the server exchanges that ticket for an HttpOnly
  // session cookie on the response to `/`, and every later request — the SSE
  // stream and the name POSTs — authenticates with that cookie, which this
  // script cannot read and therefore cannot leak. Drop the spent ticket out of
  // the address bar so a reload (which the cookie already authorises) does not
  // keep re-presenting a consumed one.
  try { window.history.replaceState({}, "", "/"); } catch (e) { /* not fatal */ }
  var feed = document.getElementById("feed");
  var rowsBox = document.getElementById("rows");
  var panelEmpty = document.getElementById("panel-empty");
  var indicator = document.getElementById("live-indicator");
  var ended = document.getElementById("ended");
  var panelEnded = document.getElementById("panel-ended");
  var billed = document.getElementById("billed");
  var conn = document.getElementById("conn");
  var tpl = document.getElementById("speaker-row");
  var names = {};
  var labels = [];
  var lines = {};
  var rowsByLabel = {};

  function keyOf(channel, order) { return channel + "#" + order; }

  // The projection, client side: near is always the operator, an unmapped label
  // is the provider's own label, and an unlabelled far line is attributed to
  // nobody rather than to a blank name.
  function nameFor(line) {
    if (line.channel === "near") { return line.display; }
    if (!line.label) { return null; }
    return names[line.label] || ("Speaker " + line.label);
  }

  function paintNames() {
    Object.keys(lines).forEach(function (key) {
      var line = lines[key];
      var shown = nameFor(line);
      line.who.textContent = shown === null ? "" : shown;
      line.el.classList.toggle("anon", shown === null);
    });
  }

  // FR-2.2: follow the conversation only while the operator is already at the
  // bottom. Scrolling away from where someone is reading is the failure this
  // feature exists to prevent.
  function atBottom() {
    return feed.scrollHeight - feed.scrollTop - feed.clientHeight < 48;
  }

  function onTurn(turn) {
    var stick = atBottom();
    var key = keyOf(turn.channel, turn.turn_order);
    var line = lines[key];
    if (!line) {
      var el = document.createElement("article");
      el.className = "line " + turn.channel;
      var who = document.createElement("span");
      who.className = "who";
      var body = document.createElement("span");
      body.className = "text";
      el.append(who, body);
      feed.append(el);
      line = lines[key] = { el: el, who: who, body: body };
    }
    line.channel = turn.channel;
    line.label = turn.label;
    // The server withholds the `names` push for the session's FIRST far label,
    // on the stated grounds that the turn which introduced it already carried
    // it (see `_note_label`). That is only true if the page registers it HERE.
    // Without this, a call with a single remote speaker never grows a control
    // panel row and that speaker can never be named — the exact failure FR-2.3
    // exists to prevent, and the common 1:1 case. Mirrors `onRevision`, and is
    // scoped to the far channel because the near speaker is configuration, not
    // a per-call row (FR-4.3), so the server's canonical list never holds it.
    if (turn.channel === "far" && turn.label && labels.indexOf(turn.label) === -1) {
      labels.push(turn.label);
      paintPanel();
    }
    line.display = turn.display_name;
    line.body.textContent = turn.text;
    line.el.classList.toggle("partial", !turn.is_final);
    var shown = nameFor(line);
    line.who.textContent = shown === null ? "" : shown;
    line.el.classList.toggle("anon", shown === null);
    if (stick) { feed.scrollTop = feed.scrollHeight; }
  }

  function onRevision(revision) {
    // The label is registered even when the line is unknown here: the server
    // broadcasts a revision whose turn has been evicted from its bounded ring,
    // because THIS page never prunes its transcript and may still be showing
    // the line the provider has just corrected away from.
    if (revision.label && labels.indexOf(revision.label) === -1) {
      labels.push(revision.label);
      paintPanel();
    }
    var line = lines[keyOf(revision.channel, revision.turn_order)];
    if (!line) { return; }
    line.label = revision.label;
    paintNames();
  }

  // Reconciled in place, never rebuilt. The server pushes a `names` payload on
  // every SSE connection, and the idle recycle makes reconnection ROUTINE — so
  // recreating the inputs here would silently discard a half-typed name and
  // steal focus mid-word during ordinary use (PRD §6.3 step 4, FR-2.3/FR-3.1).
  // Two rules make that impossible: a label keeps the row (and the very input
  // element) it already has, and an input is never written to while it has
  // focus, so the operator's in-progress typing always wins.
  function paintPanel() {
    var wanted = {};
    labels.forEach(function (label) {
      wanted[label] = true;
      var row = rowsByLabel[label];
      if (!row) {
        var el = tpl.content.cloneNode(true).querySelector(".row");
        var input = el.querySelector("input");
        el.querySelector(".tag").textContent = "Speaker " + label;
        input.addEventListener("change", function () { send(label, input.value); });
        row = rowsByLabel[label] = { el: el, input: input };
        rowsBox.append(el);
      }
      var want = names[label] || "";
      if (document.activeElement !== row.input && row.input.value !== want) {
        row.input.value = want;
      }
    });
    Object.keys(rowsByLabel).forEach(function (label) {
      if (wanted[label]) { return; }
      var stale = rowsByLabel[label];
      delete rowsByLabel[label];
      if (stale.el.parentNode) { stale.el.parentNode.removeChild(stale.el); }
    });
    panelEmpty.hidden = labels.length > 0;
  }

  function send(label, value) {
    var trimmed = value.trim();
    fetch("/names", {
      method: "POST",
      credentials: "same-origin",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ label: label, name: trimmed ? trimmed : null })
    });
  }

  function seconds(value) {
    return (value === null || value === undefined) ? "unknown" : Math.round(value) + "s";
  }

  // The indicator tracks the SESSION, not traffic (§5). A quiet call emits no
  // turns for minutes and the indicator must stay lit: audio is still leaving
  // the machine.
  function onStatus(status) {
    if (status.live) {
      indicator.hidden = false;
      ended.hidden = true;
      panelEnded.hidden = true;
      billed.hidden = true;
      return;
    }
    indicator.hidden = true;
    ended.hidden = false;
    panelEnded.hidden = false;
    // Only the message that CARRIES the durations may write them. The end of a
    // call produces two of these — the bridge's, with the billed seconds, and
    // the controller's, without — in no guaranteed order, and a status that
    // does not know the durations must not overwrite "near 612s" with
    // "near unknown". A metered feature that reports "unknown" for a number it
    // was told is worse than one that stays quiet.
    if ("near_seconds" in status || "far_seconds" in status) {
      billed.textContent = "near " + seconds(status.near_seconds)
        + ", far " + seconds(status.far_seconds);
      billed.hidden = false;
    }
  }

  function onProblem(problem) {
    var el = document.createElement("article");
    el.className = "line problem";
    el.textContent = problem.message;
    feed.append(el);
    feed.scrollTop = feed.scrollHeight;
  }

  // Status keys on the SSE connection, never on turn arrival: a server alive
  // with nobody speaking must read as connected, not as stalled. The warning is
  // held back briefly because an idle stream is recycled by design and the
  // browser resumes it in milliseconds — announcing that would be noise.
  var dropped = null;
  var stream = new EventSource("/events");
  stream.onopen = function () {
    if (dropped !== null) { clearTimeout(dropped); dropped = null; }
    conn.textContent = "connected";
    conn.className = "ok";
  };
  stream.onerror = function () {
    if (dropped !== null) { return; }
    dropped = setTimeout(function () {
      conn.textContent = "reconnecting";
      conn.className = "bad";
    }, 1500);
  };
  stream.onmessage = function (event) {
    var message = JSON.parse(event.data);
    if (message.kind === "turn") { onTurn(message); }
    else if (message.kind === "names") {
      names = message.names || {};
      labels = message.labels || [];
      paintPanel();
      paintNames();
    }
    else if (message.kind === "revision") { onRevision(message); }
    else if (message.kind === "status") { onStatus(message); }
    else if (message.kind === "error") { onProblem(message); }
  };
})();
</script>
</body>
</html>
"""


# ---------------------------------------------------------------------------
# Server plumbing
# ---------------------------------------------------------------------------


class _StoredTurn:
    """One turn as HELD: the provider's label, never a display name.

    Mutable so a final replaces its own partial in place and a revision can move
    the line to another label without disturbing the scrollback's order.
    """

    __slots__ = ("channel", "label", "text", "turn_order", "is_final", "elapsed",
                 "emit_id")

    def __init__(self, channel: str, label: Optional[str], text: str,
                 turn_order: int, is_final: bool, elapsed: float):
        self.channel = channel
        self.label = label
        self.text = text
        self.turn_order = turn_order
        self.is_final = is_final
        self.elapsed = elapsed
        # The stream id this turn was last sent under, so a reconnecting page
        # replays what changed since it dropped and nothing else.
        self.emit_id = 0


class _Subscriber:
    """One open `/events` connection."""

    __slots__ = ("outbox",)

    def __init__(self) -> None:
        self.outbox: "queue.Queue" = queue.Queue()


class _Server(socketserver.ThreadingMixIn, http.server.HTTPServer):
    """Threaded so one held-open SSE stream cannot block the page or `/health`.

    `daemon_threads` plus `block_on_close = False` is what makes `stop()` return:
    an SSE handler is parked on its queue, and a `server_close()` that joined it
    would hang the operator's stop keystroke.
    """

    daemon_threads = True
    block_on_close = False

    surface: "LiveSurface"


class _Handler(http.server.BaseHTTPRequestHandler):
    # The default access log writes the request line to stderr. The session
    # token has no URL form at all any more (see the module docstring), so the
    # request line carries `?k=<launch ticket>` — still a credential, still
    # short-lived, and still not something to write anywhere. Silenced, not
    # redirected: there is nowhere safe to put it (NFR-4).
    server_version = "hidock-live"
    sys_version = ""

    # A connection that opens and never finishes its request line would
    # otherwise park this thread for the rest of the call, and it can do that
    # before any authentication runs. Cannot affect the SSE response: the
    # timeout is per blocking socket operation, and that response writes far
    # more often than this and never reads.
    timeout = _REQUEST_TIMEOUT_SECONDS

    def log_message(self, fmt, *args) -> None:  # noqa: A003 - base class name
        return

    # -- routing ----------------------------------------------------------

    def do_GET(self) -> None:  # noqa: N802 - base class name
        path, ticket = _split(self.path)
        surface = self.server.surface  # type: ignore[attr-defined]

        # `/health` is the launcher's readiness wait, so it must answer before
        # the operator has any credential — and therefore carries nothing else.
        if path == "/health":
            self._respond(200, b'{"status":"ok"}', "application/json")
            return

        # `/` is the ONLY route that accepts a ticket, and it accepts the cookie
        # first: a reload re-presents the ticket it was opened with, which by
        # then is spent, and a reload has to keep working (the window is the
        # operator's whole view of a live call). Trying the cookie first also
        # means an ordinary reload never burns a ticket that is still valid.
        if path == "/":
            if self._authorised(surface):
                self._page()
                return
            if surface.redeem_ticket(ticket):
                self._page(set_cookie=surface.token)
                return
            self._refuse()
            return

        # Every other route is cookie-only. A credential in the query string
        # authenticates nothing here — that is the property that lets the ticket
        # be the only thing ever written into a URL.
        if not self._authorised(surface):
            self._refuse()
            return

        if path == "/events":
            self._stream(surface)
            return
        self._respond(404, b"not found", "text/plain; charset=utf-8")

    def do_POST(self) -> None:  # noqa: N802 - base class name
        path, _ticket = _split(self.path)
        surface = self.server.surface  # type: ignore[attr-defined]

        # Cookie only — a ticket is a key to the door, not to the state.
        if not self._authorised(surface):
            # The rejection is logged; the submitted value never is (FR-ERR-5).
            log.warning("live surface: rejected an unauthenticated request")
            self._refuse()
            return
        if path != "/names":
            self._respond(404, b"not found", "text/plain; charset=utf-8")
            return

        # Holding a valid cookie is NOT evidence that the operator asked for
        # this. The cookie is ambient: the browser attaches it to any request
        # aimed at this host, including one issued by a page on some other
        # loopback port (`_page` says why `SameSite=Strict` does not stop that).
        # So the write path demands proof of origin, and it demands it BEFORE
        # reading a byte of the body — a refused request must never have its
        # value in this process at all, let alone in a log record (FR-ERR-5).
        if not self._json_body_type():
            # Named apart from the origin refusal because they are different
            # facts, and a log line that reports the wrong one sends the next
            # reader after the wrong thing.
            log.warning("live surface: rejected a /names write with a non-JSON body")
            self._refuse()
            return
        if not self._same_origin_post():
            log.warning("live surface: rejected a /names write from another origin")
            self._refuse()
            return

        payload = self._body()
        if not isinstance(payload, dict) or not isinstance(payload.get("label"), str):
            self._respond(400, b'{"error":"expected {label, name}"}', "application/json")
            return
        name = payload.get("name")
        if name is not None and not isinstance(name, str):
            self._respond(400, b'{"error":"name must be text or null"}', "application/json")
            return

        surface.set_name(payload["label"], name)
        self._respond(200, b'{"status":"ok"}', "application/json")

    # -- the SSE stream ---------------------------------------------------

    def _stream(self, surface: "LiveSurface") -> None:
        subscriber = surface.attach(_last_event_id(self.headers.get("Last-Event-ID")))
        try:
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(f"retry: {_RETRY_MS}\n\n".encode("utf-8"))
            self.wfile.flush()
            last_write = time.monotonic()
            last_payload = last_write
            while True:
                try:
                    item = subscriber.outbox.get(timeout=_QUEUE_POLL_SECONDS)
                except queue.Empty:
                    now = time.monotonic()
                    if not surface.running or now - last_payload >= _STREAM_IDLE_SECONDS:
                        break
                    if now - last_write >= _KEEPALIVE_SECONDS:
                        self.wfile.write(b": keep-alive\n\n")
                        self.wfile.flush()
                        last_write = now
                    continue
                if item is _CLOSE:
                    break
                stream_id, payload = item
                self.wfile.write(
                    f"id: {stream_id}\ndata: {json.dumps(payload)}\n\n".encode("utf-8")
                )
                self.wfile.flush()
                last_write = time.monotonic()
                last_payload = last_write
        except (BrokenPipeError, ConnectionResetError, OSError, ValueError):
            # Closing the window is not a stop command — `l` is. Drop this
            # subscriber and leave the session and every other page alone.
            pass
        finally:
            surface.detach(subscriber)

    # -- credentials ------------------------------------------------------

    def _session_cookies(self) -> List[str]:
        """EVERY `hidock_live` value the browser presented, in header order.

        Deliberately not `SimpleCookie().load(<whole header>)`. That is dict
        assignment, so of two morsels with the same name the LAST one wins and
        the first is discarded — which makes a duplicate appended after the real
        cookie a denial of service: the request is refused while a valid session
        token is sitting in the very same header. Cookies are scoped by HOST and
        not by port (see `_page`), so any page on any other `http://127.0.0.1:*`
        origin can plant that duplicate, and the operator is then locked out of
        their own live window with no way to see why.

        Every pair is therefore parsed on its own and every match is returned;
        `_authorised` accepts the request if ANY of them is this session's token.
        Per-pair parsing also means one malformed pair can no longer take the
        rest of the header down with it. Parsing stays defensive because the
        header is attacker-controlled, and a `CookieError` escaping here would
        leave the request with no response at all rather than the `403` §3.8
        specifies.
        """
        values: List[str] = []
        for raw in self.headers.get_all("Cookie") or []:
            for pair in raw.split(";"):
                pair = pair.strip()
                if not pair:
                    continue
                jar = http.cookies.SimpleCookie()
                try:
                    jar.load(pair)
                except http.cookies.CookieError:
                    continue
                morsel = jar.get(_COOKIE_NAME)
                if morsel is not None:
                    values.append(morsel.value)
        return values

    def _authorised(self, surface: "LiveSurface") -> bool:
        """Did the browser present this session's token anywhere in its jar?"""
        return any(surface.authorises(value) for value in self._session_cookies())

    def _json_body_type(self) -> bool:
        """Is the body declared `application/json`?

        Half of the CSRF defence, and the half that needs no request header from
        the browser: JSON is not a CORS-simple content type, so a cross-origin
        page cannot send one without a preflight this server never answers. A
        `text/plain` or form-encoded body — which a plain `<form>` or a
        no-preflight `fetch` can send with the ambient cookie attached — is
        refused here rather than parsed.
        """
        declared = (self.headers.get("Content-Type") or "").split(";", 1)[0]
        return declared.strip().lower() == _JSON_CONTENT_TYPE

    def _same_origin_post(self) -> bool:
        """Is this write demonstrably from the page this server itself served?

        `SameSite=Strict` does NOT answer that question, and believing it does is
        what left this route open. Same-site is computed on scheme and
        registrable host and IGNORES the PORT, so `http://127.0.0.1:9999` — any
        other local server, or any page it serves — is same-site with this
        surface, and the browser attaches the session cookie to its requests.
        That is precisely the origin class §8 names: "any page they have open ...
        at a guessable `http://127.0.0.1:PORT`". The request has to prove its
        origin instead of inheriting it from an ambient credential.

        Two signals, either sufficient, both browser-set and unwritable from page
        script:

        - `Sec-Fetch-Site: same-origin`.
        - `Origin` equal to the origin this request was addressed to, taken from
          the `Host` header — which carries the port. Comparing against `Host`
          rather than a hardcoded `127.0.0.1:<port>` keeps a window the operator
          opened at `http://localhost:<port>` working, and costs nothing: an
          attacker cannot make `Origin` and `Host` agree without already being
          this origin.

        A signal that is present and WRONG refuses. A request carrying neither
        signal also refuses: every browser sends `Origin` on a cross-origin POST
        and every current one sends `Sec-Fetch-Site` as well, so requiring a
        positive answer is what makes this a check rather than a suggestion.
        """
        matched = False

        site = self.headers.get("Sec-Fetch-Site")
        if site is not None:
            if site.strip().lower() != _SAME_ORIGIN:
                return False
            matched = True

        origin = self.headers.get("Origin")
        if origin is not None:
            host = (self.headers.get("Host") or "").strip()
            if not host:
                return False
            if origin.strip().rstrip("/").lower() != f"http://{host}".lower():
                return False
            matched = True

        return matched

    # -- responses --------------------------------------------------------

    def _page(self, *, set_cookie: Optional[str] = None) -> None:
        cookie = None
        if set_cookie:
            # `HttpOnly` so the page's own script cannot read the token and
            # therefore cannot leak it; `Path=/` because every route is under it.
            #
            # `SameSite=Strict` keeps the cookie off requests initiated from a
            # DIFFERENT SITE, and that is the whole of what it does. It is NOT
            # an access control against the origin class this feature actually
            # runs beside: same-site is computed on scheme and registrable host
            # and IGNORES the port, so every `http://127.0.0.1:*` page — any
            # other local server the operator has open — is same-site with this
            # surface and its requests carry this cookie. The earlier claim here
            # that `SameSite` stopped "another origin the operator has open" from
            # riding the cookie was false, and it contradicted the residual-risk
            # note three lines below it. What actually defends the one route that
            # WRITES is `_same_origin_post` plus the JSON content type; the read
            # routes are defended by CORS, which stops a cross-origin page from
            # seeing a response it has no `Access-Control-Allow-Origin` for.
            #
            # Deliberately NOT `Secure`, but NOT for the reason you might
            # assume. Chromium — the vehicle PRD §2.1 selects — treats
            # `http://127.0.0.1` as a potentially-trustworthy origin and would
            # accept and return a `Secure` cookie here, so adding it breaks
            # nothing in the shipped configuration. It is omitted because it
            # buys nothing on loopback (there is no network path to downgrade)
            # while making the cookie silently vanish for anyone reaching the
            # surface over a non-trustworthy alias. An earlier version of this
            # comment claimed a `Secure` cookie would "break every route on the
            # page"; that was measured and is false. Left corrected rather than
            # deleted, because a comment that is wrong about its own runtime is
            # worse than no comment.
            #
            # Residual risk, written down rather than implied: cookies are
            # scoped by HOST, not by port, so this cookie is also sent to any
            # other `http://127.0.0.1:*` origin the same browser visits while
            # the session is alive. It is a credential for one session on a
            # server that dies with it, not a standing secret.
            cookie = f"{_COOKIE_NAME}={set_cookie}; Path=/; HttpOnly; SameSite=Strict"
        self._respond(
            200, PAGE.encode("utf-8"), "text/html; charset=utf-8", set_cookie=cookie
        )

    def _body(self):
        try:
            length = int(self.headers.get("Content-Length") or 0)
        except ValueError:
            return None
        if length <= 0 or length > 8192:
            return None
        try:
            return json.loads(self.rfile.read(length).decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            return None

    def _refuse(self) -> None:
        self._respond(403, b'{"error":"forbidden"}', "application/json")

    def _respond(self, status: int, body: bytes, content_type: str,
                 *, set_cookie: Optional[str] = None) -> None:
        # Written by hand rather than through `send_error`, whose body is built
        # from request-derived text — and this request's line carries a ticket.
        try:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            if set_cookie is not None:
                self.send_header("Set-Cookie", set_cookie)
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass


def _last_event_id(raw: Optional[str]) -> Optional[int]:
    """The page's resume cursor, or None for a first connection."""
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def _split(target: str):
    """(path, launch ticket) from a request target. Never logged, never echoed.

    `k`, not `t`: the query string carries a ticket and only a ticket. The
    session token has no URL form at all, which is what keeps it out of argv.
    """
    parsed = urllib.parse.urlsplit(target)
    values = urllib.parse.parse_qs(parsed.query)
    tickets = values.get("k") or []
    return parsed.path, (tickets[0] if tickets else None)


# ---------------------------------------------------------------------------
# LiveSurface
# ---------------------------------------------------------------------------


class LiveSurface:
    """The page, the SSE stream, the ring, and the operator's name map."""

    def __init__(
        self,
        *,
        operator_name: str = DEFAULT_OPERATOR_NAME,
        ring_size: int = DEFAULT_RING_SIZE,
        host: str = "127.0.0.1",
        port: int = 0,
    ) -> None:
        self._operator_name = operator_name
        self._ring_size = ring_size
        # FR-1.1: loopback only. This serves live conversation transcripts, so
        # `0.0.0.0` would put them on the LAN.
        self._host = host
        # FR-1.2: the OS picks a free port. A fixed one collides with whatever
        # else the operator runs and turns a working feature into an
        # intermittent one; the chosen port is read back from the socket.
        self._requested_port = port
        self._lock = threading.RLock()
        self._ring: "deque" = deque(maxlen=ring_size)
        self._names: Dict[str, str] = {}
        self._labels: List[str] = []
        self._subscribers: List[_Subscriber] = []
        self._token = ""
        # ticket -> monotonic expiry. Single-use and short-lived; see
        # `mint_ticket` for why the session token never takes their place.
        self._tickets: Dict[str, float] = {}
        self._server: Optional[_Server] = None
        self._thread: Optional[threading.Thread] = None
        self._running = False
        self._session_live = False
        # Where a name goes once the transcript exists on disk. Attached by the
        # controller when it builds the recorder, and dropped on `stop()` so a
        # torn-down surface can never call into a finalised archive.
        self._rename_sink: Optional[Callable[[str, Optional[str]], None]] = None
        self._started_at = time.monotonic()
        self._seq = 0

    # -- lifecycle --------------------------------------------------------

    def start(self) -> str:
        """Bind, serve, and return the page's credential-free address.

        This is NOT enough to open a first window — see `launch_url`, which is
        what the caller hands to a browser.
        """
        if self._running:
            return self.url

        # Bind FIRST. A bind failure must leave the surface exactly as it was —
        # no session state, no minted token, and (upstream) no suspended offload
        # worker (FR-ERR-1).
        server = _Server((self._host, self._requested_port), _Handler)
        server.surface = self

        with self._lock:
            self._ring = deque(maxlen=self._ring_size)
            self._names = {}
            self._labels = []
            self._subscribers = []
            self._seq = 0
            self._tickets = {}
            # FR-1.3: a fresh 256-bit token per session. A token that outlived
            # its session would be a credential for a transcript nobody owns.
            self._token = secrets.token_urlsafe(32)
            self._started_at = time.monotonic()
            self._session_live = True

        self._server = server
        self._running = True
        self._thread = threading.Thread(
            target=server.serve_forever, name="hidock-live-surface", daemon=True
        )
        self._thread.start()
        return self.url

    def attach_rename_sink(
        self, sink: Optional[Callable[[str, Optional[str]], None]]
    ) -> None:
        """Where `set_name` forwards, so a name can reach the archived document.

        Attached rather than constructed here because the recorder is optional —
        a composition with no archive directory records nothing — and a surface
        that had to know that would have to know about archives.
        """
        self._rename_sink = sink

    def end_session(self) -> None:
        """The call is over. The window stays up so names can still be changed.

        This is the reversal argued in `post_session_naming_prd.md` §5, and the
        narrower half of what `stop()` does: the live indicator clears, but the
        server keeps serving, the ring is kept, the token stays valid and the
        name map survives. The operator can still type, and what they type now
        reaches the transcript on disk instead of a render that already
        happened.

        The indicator clearing is not cosmetic. It is a security control
        (`live_surface_prd.md` FR-2.5) and leaving it lit while no audio is
        streaming would be worse than shutting the window: an indicator that
        lies about a metered third-party egress path trains the operator to
        disbelieve it.

        Idempotent, and never raises — it runs on the same teardown path as
        `stop()`, where a raise would strand the offload worker suspended.
        """
        if not self._running or not self._session_live:
            return
        self._session_live = False
        self._broadcast(self._status_payload())

    def stop(self) -> None:
        """Idempotent, and never raises.

        Runs from the controller's teardown alongside `resume_polling`; a raise
        here would strand the offload worker suspended (FR-6.3). FR-1.6 names
        three things and all three happen: the thread stops, the ring is
        dropped, the token is invalidated.
        """
        if not self._running:
            return
        self._running = False
        self._session_live = False
        self._rename_sink = None

        with self._lock:
            subscribers = list(self._subscribers)
            self._subscribers = []
            self._ring = deque(maxlen=self._ring_size)
            self._names = {}
            self._labels = []
            # An unredeemed ticket must not survive the session it was minted
            # for; `redeem_ticket` also refuses once `_running` is False.
            self._tickets = {}
            # The token is retired rather than blanked: `authorises` refuses the
            # moment the session ends, and the next session mints its own. Held
            # so nothing has to reason about an empty credential — a blank token
            # that compared equal to a blank query parameter would authorise
            # everyone.
        for subscriber in subscribers:
            subscriber.outbox.put(_CLOSE)

        server, self._server = self._server, None
        thread, self._thread = self._thread, None
        if server is not None:
            try:
                server.shutdown()
            except Exception as exc:  # noqa: BLE001 - stop must not raise
                log.warning("live surface: shutdown was not clean (%s)", type(exc).__name__)
            try:
                server.server_close()
            except Exception as exc:  # noqa: BLE001 - stop must not raise
                log.warning("live surface: close was not clean (%s)", type(exc).__name__)
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=3.0)

    # -- identity ---------------------------------------------------------

    @property
    def url(self) -> str:
        """The page's address, carrying NO credential.

        Openable by a browser that already holds the session cookie — a reload,
        or a second window in the same profile — and by nothing else. Handing
        the operator this URL alone is not enough to open a first window; that
        is what `launch_url` is for.
        """
        return f"http://{self._host}:{self.port}/"

    @property
    def port(self) -> int:
        server = self._server
        if server is None:
            return 0
        return int(server.server_address[1])

    @property
    def token(self) -> str:
        return self._token

    @property
    def running(self) -> bool:
        return self._running

    def authorises(self, token: Optional[str]) -> bool:
        """Does this credential — always the cookie — belong to this session?"""
        current = self._token
        if not self._running or not current:
            return False
        supplied = _as_bytes(token)
        if supplied is None:
            # Missing, empty, or not encodable. A non-ASCII `str` would make
            # `compare_digest` raise `TypeError` inside the handler thread,
            # which is a request with no response rather than a `403`.
            return False
        return secrets.compare_digest(supplied, current.encode("utf-8"))

    # -- launch tickets ---------------------------------------------------

    def mint_ticket(self, ttl: float = _LAUNCH_TICKET_TTL_SECONDS) -> str:
        """One short-lived, single-use key to `GET /`, and to nothing else.

        Launching a chromeless window means passing a URL to a browser as a
        command-line argument, and argv is readable by every process running as
        the operator. A session token placed there would be readable by exactly
        the class of process §8 says the token exists to exclude — and readable
        for the whole call. A ticket is readable for its TTL, buys one page
        load, and cannot be replayed.

        **Residual risk, stated rather than implied.** A process polling `ps`
        can still race the browser for a ticket inside its TTL and open the page
        itself. That is a large reduction — seconds instead of a whole session,
        one use instead of unlimited, and no access to `/events` or `/names`
        without completing the exchange first — but it is a reduction, not an
        elimination. Eliminating it needs a channel that is not argv (a
        `--user-data-dir` seeded cookie jar, or a browser that reads the URL
        from stdin), and no Chromium flag offers one.
        """
        ticket = secrets.token_urlsafe(24)
        with self._lock:
            now = time.monotonic()
            self._tickets = {t: e for t, e in self._tickets.items() if e > now}
            self._tickets[ticket] = now + ttl
        return ticket

    def launch_url(self, ttl: float = _LAUNCH_TICKET_TTL_SECONDS) -> str:
        """`url` plus a fresh ticket — the only address that opens a new window."""
        return f"{self.url}?k={self.mint_ticket(ttl)}"

    def redeem_ticket(self, ticket: Optional[str]) -> bool:
        """Spend a ticket. True at most once per ticket, and never after its TTL."""
        supplied = _as_bytes(ticket)
        if not self._running or supplied is None:
            return False
        with self._lock:
            now = time.monotonic()
            self._tickets = {t: e for t, e in self._tickets.items() if e > now}
            # Scanned with `compare_digest` rather than looked up, so a wrong
            # ticket costs the same regardless of how much of it was right.
            match = None
            for candidate in self._tickets:
                if secrets.compare_digest(candidate.encode("utf-8"), supplied):
                    match = candidate
            if match is None:
                return False
            del self._tickets[match]
            return True

    # -- subscribers ------------------------------------------------------

    def attach(self, last_event_id: Optional[int] = None) -> _Subscriber:
        """Register a page and hand it the session so far.

        The snapshot is built under the same lock that serialises `publish`, so
        a turn arriving mid-connect is either in the backlog or in the stream —
        never both, and never neither.

        `last_event_id` is what the browser sends when it reconnects a dropped
        stream: only turns emitted after that point are replayed. A first
        connection has none and gets the whole ring, which is what makes a
        window opened (or reloaded) mid-call render the scrollback it missed.

        The replay deliberately does NOT hand `stored` to `_send`, so a turn's
        `emit_id` is left where its broadcast put it. `emit_id` records where a
        turn sits in the SHARED stream; moving it here would rewrite it to a
        position only THIS page has reached, and every other connected page's
        `Last-Event-ID` would then be behind the whole ring — so its next
        reconnect replays the entire scrollback rather than the handful of turns
        it actually missed. `_STREAM_IDLE_SECONDS` recycles every idle stream, so
        with two windows open that is not an edge case but the ordinary path.
        This page's own cursor stays correct regardless: the ids it receives are
        drawn from the same monotonic `_seq`, so anything broadcast after its
        replay still sorts above it.
        """
        subscriber = _Subscriber()
        with self._lock:
            self._subscribers.append(subscriber)
            self._send(subscriber, self._status_payload())
            self._send(subscriber, self._names_payload())
            for stored in list(self._ring):
                if last_event_id is not None and stored.emit_id <= last_event_id:
                    continue
                self._send(subscriber, self._turn_payload(stored))
        return subscriber

    def detach(self, subscriber: _Subscriber) -> None:
        with self._lock:
            try:
                self._subscribers.remove(subscriber)
            except ValueError:
                pass

    # -- naming -----------------------------------------------------------

    def set_name(self, label: str, name: Optional[str]) -> None:
        """Map (or clear) one provider label. FR-3.5 bounds the value at 64.

        Never written into stored turns: the map is consulted when a turn is
        sent, which is what makes this reach lines already on screen and lets
        clearing revert to the provider's own label.

        The page is updated FIRST and the archive second, on purpose. The page
        is what the operator is looking at and it cannot fail; the archive is a
        file on a Drive mount that can be busy, unwritable, or already changed
        by another writer. Ordering it this way means a disk problem costs a
        message, never the typed name.
        """
        with self._lock:
            cleaned = (name or "").strip()[:64]
            if cleaned:
                self._names[label] = cleaned
            else:
                self._names.pop(label, None)
            payload = self._names_payload()
        self._broadcast(payload)

        # OUTSIDE `_lock`, and that is a correctness requirement rather than
        # tidiness. `LiveArchive.stop()` holds `_finalise_lock` while it renders,
        # and rendering reads `speaker_names()`, which takes THIS lock. The sink
        # takes `_finalise_lock`. Calling it from inside `_lock` would let the
        # teardown thread hold `_finalise_lock` wanting `_lock` while this thread
        # holds `_lock` wanting `_finalise_lock` — a deadlock between the
        # operator's keystroke and the end of their call.
        #
        # It is called unconditionally, not only once the session has ended: the
        # archive knows whether a transcript exists and the surface does not, and
        # two components deciding separately when a call is "over" is how they
        # come to disagree. Before the transcript is written this is a no-op, and
        # correctly so — the snapshot `stop()` takes will carry the name instead.
        sink = self._rename_sink
        if sink is None:
            return
        try:
            sink(label, cleaned or None)
        except Exception as exc:  # noqa: BLE001 - a name is not worth a 500
            log.warning(
                "live: the archived transcript was not renamed (%s)",
                type(exc).__name__,
            )

    def speaker_names(self) -> Dict[str, str]:
        """The operator's `label -> name` map, as a snapshot.

        The archive reads this ONCE, at stop (`live_archive_prd.md` §5), which is
        what lets a name typed in the last minute of a call reach lines rendered
        in the first — the same property `_display_name` gives the page, applied
        to the document.

        Keyed by the PROVIDER's label, which is what
        `render_markdown(speaker_names=...)` matches against, so it is handed
        across unchanged rather than pre-resolved here. A copy, taken under the
        lock that serialises `set_name`: the caller renders a whole document from
        it while the operator may still be typing into the panel.
        """
        with self._lock:
            return dict(self._names)

    # -- inbound events ---------------------------------------------------

    def publish(self, event) -> None:
        """Accept one bus event. Only live traffic reaches the page.

        The surface subscribes to the whole bus, which carries the entire
        offload pipeline; a USB replug notice landing in the middle of a live
        transcript is noise the operator cannot act on there.
        """
        if isinstance(event, LiveTurn):
            self._on_turn(event)
        elif isinstance(event, LiveSpeakerRevision):
            self._on_revision(event)
        elif isinstance(event, LiveTranscriptionStopped):
            self._on_stopped(event)
        elif isinstance(event, Error) and event.context == "live":
            self._broadcast({"kind": "error", "message": event.message})

    def _on_turn(self, event: LiveTurn) -> None:
        # A silent turn boundary is not something to show: a blank row in the
        # scrollback is indistinguishable from a dropped line.
        if not event.text.strip():
            return
        channel = _channel_of(event.channel)
        label = event.speaker
        with self._lock:
            stored = self._find(channel, event.turn_order)
            if stored is None:
                stored = _StoredTurn(
                    channel=channel,
                    label=label,
                    text=event.text,
                    turn_order=event.turn_order,
                    is_final=event.is_final,
                    # §11: measured from session start by this process. A
                    # wall-clock stamp would assert when a person spoke using a
                    # proxy for when bytes arrived.
                    elapsed=round(time.monotonic() - self._started_at, 3),
                )
                self._ring.append(stored)
            else:
                # A final replaces its own partial rather than appending twice.
                # Identity is (channel, turn_order): the two sessions number
                # their turns independently.
                stored.label = label
                stored.text = event.text
                stored.is_final = event.is_final
            names_payload = self._note_label(channel, label)
            turn_payload = self._turn_payload(stored)
        self._broadcast(turn_payload, stored)
        if names_payload is not None:
            self._broadcast(names_payload)

    def _on_revision(self, event: LiveSpeakerRevision) -> None:
        """The provider reassigned a LINE from one label to another.

        The line then renders with whatever name the new label carries; the
        operator's map is untouched, because the map is keyed by label and it is
        the turn that moved.

        Broadcast even when the ring no longer holds the turn. The ring is
        bounded (FR-1.5) but the PAGE never prunes its transcript, so on a long
        call the line the provider just corrected is still on screen — and
        withholding the revision would leave it under the label it was moved
        AWAY from, inheriting whatever name the operator typed for that label.
        A miss here means only that a page connecting later cannot be replayed
        the correction, which is already true of the turn itself.
        """
        channel = _channel_of(event.channel)
        with self._lock:
            stored = self._find(channel, event.turn_order)
            if stored is not None:
                stored.label = event.speaker
            names_payload = self._note_label(channel, event.speaker)
            payload = {
                "kind": "revision",
                "channel": channel,
                "turn_order": event.turn_order,
                "label": event.speaker,
            }
        # Carries `stored` so the reassigned line is replayed to a page that
        # reconnects after the revision: it is the turn that moved.
        self._broadcast(payload, stored)
        if names_payload is not None:
            self._broadcast(names_payload)

    def _on_stopped(self, event: LiveTranscriptionStopped) -> None:
        self._session_live = False
        self._broadcast({
            "kind": "status",
            "live": False,
            # Nullable on purpose: the provider's duration is Optional upstream,
            # and a metered feature reporting 0 when it does not know
            # understates a real bill.
            "near_seconds": event.near_seconds,
            "far_seconds": event.far_seconds,
        })

    # -- projection -------------------------------------------------------

    def _find(self, channel: str, turn_order: int) -> Optional[_StoredTurn]:
        for stored in self._ring:
            if stored.turn_order == turn_order and stored.channel == channel:
                return stored
        return None

    def _note_label(self, channel: str, label: Optional[str]) -> Optional[dict]:
        """Record a control-panel row, and say whether it must be pushed.

        Rows accumulate within a session and are never derived from the ring: a
        speaker who stopped talking is still on the call, and a long call evicts
        their first turn. Near-channel turns never produce a row — the operator
        is configuration, not a per-call decision (FR-4.3).

        The push is skipped for the session's FIRST row because the only thing
        it would tell an already-connected page is a label that page just
        received on the turn itself. From the second row onward the panel has an
        ORDER, which is server-side state, so the canonical set is sent.
        """
        if label is None or channel != LiveChannel.FAR.value:
            return None
        if label in self._labels:
            return None
        self._labels.append(label)
        if len(self._labels) < 2:
            return None
        return self._names_payload()

    def _display_name(self, channel: str, label: Optional[str]) -> Optional[str]:
        # FR-4.1: phase 1b emits `speaker=None` on the near channel deliberately
        # and leaves this resolution to the renderer.
        if channel == LiveChannel.NEAR.value:
            return self._operator_name
        if label is None:
            # Attributed to nobody, which is honest — never to an empty string,
            # which reads as a rendering fault.
            return None
        # FR-3.4: an unmapped label renders as the provider's own label, never
        # as a guess and never as blank.
        return self._names.get(label) or f"Speaker {label}"

    def _turn_payload(self, stored: _StoredTurn) -> dict:
        return {
            "kind": "turn",
            "channel": stored.channel,
            "label": stored.label,
            "display_name": self._display_name(stored.channel, stored.label),
            "text": stored.text,
            "turn_order": stored.turn_order,
            "is_final": stored.is_final,
            "elapsed_seconds": stored.elapsed,
        }

    def _names_payload(self) -> dict:
        return {"kind": "names", "names": dict(self._names), "labels": list(self._labels)}

    def _status_payload(self) -> dict:
        # The indicator's set path is the SESSION's lifetime, so a page that
        # reloads during a quiet stretch still finds it lit. `url` carries no
        # credential — the page that receives it already holds the cookie.
        return {"kind": "status", "live": self._session_live, "url": self.url}

    def _send(self, subscriber: _Subscriber, payload: dict) -> None:
        """Queue one payload to ONE subscriber. Caller holds the lock.

        This is the single-page path — `attach`'s snapshot and replay — and it
        takes no `_StoredTurn`, on purpose. `emit_id` records where a turn sits
        in the SHARED stream, so only `_broadcast`, which reaches every page at
        once, may write it. A one-page catch-up that moved it would make every
        OTHER page's resume cursor lie, and the rule is enforced by there being
        no parameter to break it with rather than by a caller remembering.
        """
        self._seq += 1
        subscriber.outbox.put((self._seq, payload))

    def _broadcast(self, payload: dict, stored: Optional[_StoredTurn] = None) -> None:
        with self._lock:
            self._seq += 1
            if stored is not None:
                stored.emit_id = self._seq
            item = (self._seq, payload)
            subscribers = list(self._subscribers)
        for subscriber in subscribers:
            subscriber.outbox.put(item)


def _channel_of(channel) -> str:
    return channel.value if isinstance(channel, LiveChannel) else str(channel)


# ---------------------------------------------------------------------------
# FR-5.4 — launching the window
# ---------------------------------------------------------------------------

# `--app=` is what makes this a window rather than a browser tab: no omnibox
# showing a localhost URL, no tab strip, and `⌘F` find-in-page still works.
#
# Chrome is NOT a dependency. This is a public clone-and-run app and most users
# will not have it. Order is declared, not incidental: whichever answers first is
# the browser the operator's call renders in for the rest of the session.
_MAC_CANDIDATES = (
    ("Google Chrome", "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"),
    ("Brave Browser", "/Applications/Brave Browser.app/Contents/MacOS/Brave Browser"),
    ("Microsoft Edge", "/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge"),
    ("Chromium", "/Applications/Chromium.app/Contents/MacOS/Chromium"),
)

_PATH_CANDIDATES = (
    ("Google Chrome", "google-chrome"),
    ("Brave Browser", "brave-browser"),
    ("Microsoft Edge", "microsoft-edge"),
    ("Chromium", "chromium"),
)


def _candidates():
    return _MAC_CANDIDATES if sys.platform == "darwin" else _PATH_CANDIDATES


def _spawn(argv, **kwargs):
    """Default runner: `subprocess.run`-shaped, but it does not kill the window.

    `subprocess.run(timeout=...)` kills the child when the wait expires, and on a
    cold start THIS process is the browser — so the bounded wait would close the
    window it just opened. `Popen` plus `wait(timeout=...)` bounds the call the
    same way and leaves a still-running browser alone.
    """
    timeout = kwargs.pop("timeout", None)
    process = subprocess.Popen(argv, **kwargs)
    try:
        return subprocess.CompletedProcess(argv, process.wait(timeout=timeout))
    except subprocess.TimeoutExpired:
        # Still running: the window is up and this process is holding it.
        return subprocess.CompletedProcess(argv, 0)


def launch_app_window(url: str, *, runner: Optional[Callable] = None) -> str:
    """Open `url` in a chromeless window. Returns a note; NEVER raises.

    Called on the start path of a metered session, from the keyboard thread. A
    raise here would end a paid call because a window did not appear, so every
    failure degrades: the next candidate, then the default browser, then the URL
    the caller has already surfaced (FR-ERR-2, §3.8).

    **`url` becomes argv, and argv is public.** Every path below puts it on a
    command line — the Chromium candidates directly, and `webbrowser.open` via
    whatever helper it spawns — where `ps -axww` reads it. So callers must pass
    a `LiveSurface.launch_url`, never anything carrying the session token; the
    ticket that URL holds is single-use and expires in seconds, and that bound
    is the whole mitigation. `mint_ticket` states the residual risk.
    """
    run = runner if runner is not None else _spawn
    try:
        for label, binary in _candidates():
            try:
                completed = run(
                    [binary, f"--app={url}"],
                    timeout=LAUNCH_TIMEOUT_SECONDS,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
            except FileNotFoundError:
                continue  # not installed — try the next vendor
            except Exception:  # noqa: BLE001 - a crashing launcher is not fatal
                continue
            if getattr(completed, "returncode", 1) == 0:
                return f"opened in {label}"
            # Present but refusing (`--app` unsupported, profile locked) is a
            # different failure from absent and degrades the same way.
        try:
            if webbrowser.open(url):
                return "opened in the default browser"
        except Exception:  # noqa: BLE001 - no display, no browser, no problem
            pass
        return "no browser could be opened; use the URL in the activity log"
    except Exception:  # noqa: BLE001 - the floor: this function cannot raise
        return "no browser could be opened; use the URL in the activity log"


# ---------------------------------------------------------------------------
# LiveSessionController
# ---------------------------------------------------------------------------


# FR-6.4 / §3.8 "terminal-expected": a detach mid-session ends the session WITH A
# STATED REASON. These are the raw texts a detach actually produces, and none of
# them states a reason an operator can act on.
#
# `adapter exposes no Jensen transport` is `realtime._jensen()` reporting that
# the adapter no longer holds a transport object — which is what an unplug looks
# like from inside this module. `device is not connected` is `RealtimeSession.
# start()` finding the same thing one layer up. Both are true sentences about
# this program's internals and neither names the device, the unplug, or a remedy.
_DEVICE_GONE_MARKERS = (
    "exposes no Jensen transport",
    "device is not connected",
)


def _translate_live_failure(raw: str) -> str:
    """Rewrite a live-session failure into the operator's next move.

    Same shape as `app._translate_connect_error`: match a marker class, replace
    it with cause-plus-remedy, and keep the raw text appended so the failure is
    still diagnosable from a screenshot. Anything that does not match passes
    through UNCHANGED — `live_transcribe` already routes its own vendor failures
    through `retry.remediation`, and `realtime`'s device-stopped-responding text
    already names the power cycle, so a catch-all rewrite here would replace
    good guidance with worse.

    Translating at THIS boundary, not in `realtime.py`, is deliberate: that
    module is a transport and its callers are not all operator-facing. The
    operator-language obligation belongs to the surface that shows the operator
    the message.
    """
    if any(marker in raw for marker in _DEVICE_GONE_MARKERS):
        return (
            "the HiDock is no longer reachable over USB. The device was "
            "unplugged, or it lost its connection; reconnect it and press l "
            "again to start a new live session. The transcript above is kept. "
            "Raw error: " + raw
        )
    return raw


def _noop() -> None:
    return


def _never_busy() -> bool:
    return False


def _default_surface_factory(**kwargs) -> LiveSurface:
    return LiveSurface(**kwargs)


def _default_capture_factory(adapter, **kwargs):
    from .realtime import RealtimeSession

    return RealtimeSession(adapter, **kwargs)


def _default_transcriber_factory(bus, **kwargs):
    from .live_transcribe import LiveTranscriber

    return LiveTranscriber(bus, **kwargs)


def _default_archive_factory(archive_dir, **kwargs):
    # Imported inside the call, like the two factories above: `live_archive`
    # reaches the vendored `diarize_audio` renderer, and an app that never
    # presses `l` should not pay for that import graph at startup.
    from .live_archive import LiveArchive

    return LiveArchive(archive_dir, **kwargs)


class LiveSessionController:
    """Owns a live session: capture, bridge, surface, and the device claim.

    Start is always explicit (`l`), never on device attach and never on call
    detection: it is metered and it is an egress path.
    """

    def __init__(
        self,
        *,
        bus: EventBus,
        adapter,
        api_key: str,
        operator_name: str = DEFAULT_OPERATOR_NAME,
        max_speakers: int = DEFAULT_MAX_SPEAKERS,
        # Where a live session's recording and transcript are written. Supplied
        # by the composition root from `config.archive_dir` — at composition
        # time, deliberately, rather than read from a process global at use
        # time, which is the shape that produced the `INBOX_DIRS` defect.
        #
        # `None` means this composition archives nothing, which is the pre-1d
        # behaviour and is NOT a production configuration: `__main__` always
        # passes the archive, and the composition-root sweep in
        # `test_live_server.py` fails the moment it stops.
        archive_dir=None,
        keep_wav_dir=None,
        suspend_polling: Optional[Callable[[], None]] = None,
        resume_polling: Optional[Callable[[], None]] = None,
        busy_predicate: Optional[Callable[[], bool]] = None,
        surface_factory: Optional[Callable[..., object]] = None,
        capture_factory: Optional[Callable[..., object]] = None,
        transcriber_factory: Optional[Callable[..., object]] = None,
        archive_factory: Optional[Callable[..., object]] = None,
        launch_browser: Optional[Callable[..., str]] = None,
    ) -> None:
        self._bus = bus
        self._adapter = adapter
        self._api_key = api_key
        self._operator_name = operator_name
        self._max_speakers = max_speakers
        self._archive_dir = archive_dir
        # Diagnostic WAV retention, handed straight through to the recorder.
        # None — the default — is "delete the intermediate as always".
        self._keep_wav_dir = keep_wav_dir
        self._suspend = suspend_polling or _noop
        self._resume = resume_polling or _noop
        self._busy = busy_predicate or _never_busy
        self._surface_factory = surface_factory or _default_surface_factory
        self._capture_factory = capture_factory or _default_capture_factory
        self._transcriber_factory = transcriber_factory or _default_transcriber_factory
        self._archive_factory = archive_factory or _default_archive_factory
        self._launch_browser = launch_browser or launch_app_window

        self._lock = threading.RLock()
        self._live = False
        self._stopping = False
        self._torn_down = True
        # Monotonically increasing, one value per session. `_teardown` is
        # identified BY it, so a thread that outlived its own session cannot
        # tear down the next one — see `_teardown`.
        self._generation = 0
        # Initialised here as well as in `start()`: `_teardown` reads it, and a
        # controller that has never started must not raise AttributeError on a
        # teardown path whose whole job is releasing the device claim.
        self._stop_reason = ""
        self._surface = None
        self._capture = None
        # Held so a teardown that the pump never reached still finalises the
        # recording — `stop()` joins that pump for five seconds and then tears
        # down regardless. Cleared with the rest of the session state.
        self._archive = None
        self._thread: Optional[threading.Thread] = None
        # The window that outlives the call. `_teardown` ends the SESSION and
        # leaves the surface serving so late diarization labels can still be
        # named; `close_window()` is what actually takes it down, from the next
        # `l` or from app exit. Never two at once — see `start()`.
        self._ended_surface = None
        self._ended_archive = None

    # -- operator control -------------------------------------------------

    @property
    def is_live(self) -> bool:
        return self._live

    @property
    def default_max_speakers(self) -> int:
        """What the `l` prompt prepopulates with — the CONFIGURED value.

        Read by the TUI so the prompt has ONE source for its default, wired by
        `__main__` to `config.live_max_speakers`. It is deliberately not the
        previous session's answer: a 10-person call settling on 10 and silently
        carrying that into the next 1:1 splits one remote voice across several
        labels — this PRD's own defect, reintroduced from the other direction.
        """
        return self._max_speakers

    def start_refusal(self) -> Optional[str]:
        """Why a session could not start right now, or None if one could.

        Answerable WITHOUT starting anything, because the TUI asks it before
        opening the prompt (FR-ERR-2): refusing after the operator has chosen a
        number wastes the decision and reads as though the number caused the
        failure. `start()` raises exactly this string, so the reason the prompt
        shows and the reason a start gives are one implementation rather than
        two kept in step by convention.
        """
        if self._live:
            return "a live session is already running; press l to stop it"
        # FR-6.2. Refuse and say why; do NOT queue — a silent queue means the
        # window appears minutes later with no explanation.
        if self._busy():
            return (
                "cannot start live transcription while an offload transfer is in "
                "flight — the live stream and the transfer share one USB endpoint; "
                "press l again once the offload finishes"
            )
        return None

    def toggle(self, max_speakers: Optional[int] = None) -> None:
        """One key, both directions — and it never raises.

        `l` is read on the TUI's keyboard thread, where the reader swallows
        exceptions: a refusal that raised there would be a keypress that did
        nothing, with no message.

        `max_speakers` is the count the operator just typed at the prompt. It is
        optional because the STOP half of the same key has no number to carry.
        """
        try:
            if self._live:
                self.stop("stopped by the operator")
            else:
                self.start(max_speakers=max_speakers)
        except Exception as exc:  # noqa: BLE001 - the refusal must be visible
            self._say(str(exc), Severity.WARNING)

    def start(self, max_speakers: Optional[int] = None) -> None:
        """Open a live session, optionally under a ceiling chosen for THIS call.

        `max_speakers` is per-session and is never written back onto the
        controller (FR-2.5): the next `l` prompts again from the configured
        default. `None` means "no operator answer" — the shutdown path and any
        future non-prompt caller — and takes the configured default rather than
        putting a `None` on the wire.
        """
        refusal = self.start_refusal()
        if refusal is not None:
            raise LiveSessionError(refusal)

        # FR-1.4: the previous call's window goes down BEFORE this one comes up,
        # and before anything subscribes to the bus. Two surfaces subscribed at
        # once would put this session's turns on the last session's page — which
        # is a transcript leaking across calls, not a cosmetic overlap. Done
        # here rather than in `_teardown` because that is exactly the point:
        # between the two, the operator still has a window to type into.
        self.close_window()

        # Refused, never clamped, and refused HERE rather than only at the
        # prompt, because the prompt is not the only caller. Past the ceiling the
        # vendor MERGES additional speakers into the closest existing label, so a
        # silently-clamped session destroys a distinction rather than degrading
        # it — on a call the operator is paying for, with nothing above this
        # layer able to report the substitution.
        if max_speakers is None:
            session_max_speakers = self._max_speakers
        else:
            session_max_speakers, reason = parse_speaker_count(str(max_speakers))
            if session_max_speakers is None:
                raise LiveSessionError(
                    f"cannot start live transcription — {reason}"
                )

        surface = self._surface_factory(operator_name=self._operator_name)
        try:
            surface.start()
            # Two tickets, not one, and neither carries the session token. The
            # launch ticket enters the browser's argv and is scoped to that; the
            # manual one never does, and lives long enough for the operator to
            # read it off the activity log and open the window by hand when no
            # browser could be launched (FR-5.4, FR-ERR-2).
            manual_url = surface.launch_url(_MANUAL_TICKET_TTL_SECONDS)
            launch_target = surface.launch_url(_LAUNCH_TICKET_TTL_SECONDS)
        except Exception as exc:  # noqa: BLE001 - classified for the operator
            try:
                surface.stop()
            except Exception:  # noqa: BLE001 - the bind already failed
                pass
            # Polling was never suspended: the claim is only taken once the
            # session is certain to start (FR-ERR-1).
            raise LiveSessionError(
                f"could not start the live surface ({exc})"
            ) from exc

        with self._lock:
            self._generation += 1
            generation = self._generation
            self._surface = surface
            self._stopping = False
            self._torn_down = False
            self._stop_reason = ""
            self._live = True

        # EVERYTHING that could raise between taking the device claim and the
        # pump owning it lives inside this block. Outside it, a raise from
        # `subscribe`, from constructing the Thread, or from `start()` itself
        # would leak the FR-6.3 suspension permanently AND leave `_live` True —
        # unrecoverably, because the next `l` would then join a Thread that was
        # never started and raise out of `stop()` before `_teardown` ran.
        try:
            self._suspend()
            self._bus.subscribe(surface.publish)
            capture = self._capture_factory(self._adapter)
            self._capture = capture
            transcriber = self._transcriber_factory(
                self._bus, api_key=self._api_key, max_speakers=session_max_speakers
            )
            archive = self._new_archive(surface)
            self._archive = archive
            if archive is not None:
                # A name typed after the call reaches the document through here.
                # Attached only when there IS a recorder: a transcript-only
                # session has nothing on disk for a rename to change, and a sink
                # that silently discarded names would be worse than none.
                surface.attach_rename_sink(
                    lambda label, name: self._rename_in_archive(archive, label, name)
                )
            thread = threading.Thread(
                target=self._pump, args=(capture, transcriber, generation),
                kwargs={"archive": archive},
                name="hidock-live-pump", daemon=True,
            )
            self._thread = thread
            thread.start()
        except Exception as exc:  # noqa: BLE001 - releases the claim below
            self._teardown(generation)
            raise LiveSessionError(f"could not start live transcription ({exc})") from exc

        # The pump owns the session from here, and it can already have failed
        # and torn down — capture raises on its first Jensen command far more
        # often than it does later. Announcing a running session and opening a
        # window onto an already-dead server would put the operator's NEWEST log
        # lines in contradiction with the failure printed just above them.
        with self._lock:
            if self._generation != generation or not self._live:
                return

        # FR-5.4 says ALWAYS, not on failure: the operator closes the window and
        # the log is the only place the URL can come from.
        self._say(f"Live transcription is running — {manual_url}", Severity.INFO)
        try:
            note = self._launch_browser(launch_target)
        except Exception as exc:  # noqa: BLE001 - a window is not the session
            self._say(
                f"Could not open the live window ({exc}); open the URL above.",
                Severity.WARNING,
            )
        else:
            self._say(f"Live window: {note}.", Severity.INFO)

    def stop(self, reason: str = "stopped") -> None:
        """Stop the live session. `reason` is what the operator is told.

        Both production call sites pass something meaningful — the `l`
        keystroke says "stopped", shutdown says "app shutting down" — and the
        parameter was previously accepted and discarded, so the operator saw
        the same line either way and could not tell a deliberate stop from the
        app going down under them.
        """
        if not self._live:
            return
        self._stop_reason = reason
        with self._lock:
            generation = self._generation
        self._stopping = True

        capture = self._capture
        if capture is not None:
            try:
                capture.stop()
            except Exception as exc:  # noqa: BLE001 - teardown continues
                log.warning("live: capture did not stop cleanly (%s)", type(exc).__name__)

        thread = self._thread
        # `is_alive()` gates the join: a Thread that was constructed but never
        # started raises `RuntimeError` from `join()`, and this is the operator's
        # stop keystroke — it has to reach `_teardown` and release the claim.
        if (thread is not None
                and thread is not threading.current_thread()
                and thread.is_alive()):
            thread.join(timeout=5.0)
        self._teardown(generation)

    def close_window(self) -> None:
        """Take down the window a finished session left up. Never raises.

        Separate from `stop()` because they answer to different events: `stop()`
        is the operator ending a CALL, after which they may still want to name
        the people on it; this is the window itself going away, which happens on
        the next `l` and at app exit (FR-1.4) and at no other time. There is no
        indefinite lifetime and nothing survives a restart.
        """
        with self._lock:
            surface = self._ended_surface
            self._ended_surface = None
            self._ended_archive = None
        if surface is None:
            return
        try:
            surface.attach_rename_sink(None)
            self._bus.unsubscribe(surface.publish)
        except Exception as exc:  # noqa: BLE001 - the window still has to go
            log.warning("live: unsubscribing the window failed (%s)", type(exc).__name__)
        try:
            surface.stop()
        except Exception as exc:  # noqa: BLE001 - never raises out of teardown
            log.warning("live: the window did not close cleanly (%s)", type(exc).__name__)

    def shutdown(self, reason: str = "app shutting down") -> None:
        """End any live session AND close the window. For app exit only.

        `stop()` deliberately leaves the window up; on the way out there is
        nothing left to leave it up for, and a served page outliving the process
        that owns it is the disclosure surface `live_surface_prd.md` FR-1.6 was
        written against. Safe with nothing running, and safe from a signal
        handler: both halves return immediately when there is nothing to do.
        """
        self.stop(reason=reason)
        self.close_window()

    # -- archival ---------------------------------------------------------

    def _rename_in_archive(self, archive, label: str, name: Optional[str]) -> None:
        """Put one post-session name into the archived transcript, and say so.

        FR-3.3: the operator is changing a file the call-processing pipeline
        reads, so the write is reported rather than silent. Reported on the bus,
        which means it lands on the very page they typed it into.

        Only `applied` is INFO. Every other outcome that carries a message is a
        WARNING because it means the document on disk does NOT say what the
        panel says — and the two that carry none (nothing written yet, already
        says that) are not events at all.
        """
        outcome = archive.rename_speaker(label, name)
        if outcome.message is None:
            return
        self._say(
            outcome.message,
            Severity.INFO if outcome.applied else Severity.WARNING,
        )

    def _new_archive(self, surface):
        """This session's recorder, or None when the composition archives nothing.

        Never raises. The recording is worth having — the device does not keep
        one for a live session, so ours is the only copy that will exist — but it
        is not worth the call: a composition that cannot build a recorder says so
        and the session goes ahead transcript-only (FR-ERR-1).

        `names` is `surface.speaker_names`, the CALLABLE and not a snapshot. The
        archive reads it once at stop (§5), which is what lets a name typed in
        the last minute of a call reach lines rendered in the first. Passing a
        dict here would freeze the map at session start and archive a document
        whose early lines say `Speaker 1` and whose later ones say `Dana`.
        """
        if self._archive_dir is None:
            return None
        try:
            return self._archive_factory(
                self._archive_dir,
                bus=self._bus,
                operator_name=self._operator_name,
                names=surface.speaker_names,
                keep_wav_dir=self._keep_wav_dir,
            )
        except Exception as exc:  # noqa: BLE001 - transcript-only, not no session
            self._say(
                f"Live audio is NOT being recorded to {self._archive_dir} "
                f"({exc}). The live transcript still works, but this call will "
                "leave no recording.",
                Severity.ERROR,
            )
            return None

    def _record(self, archive, frame, generation: int) -> bool:
        """Append one frame. Returns whether recording continues.

        FR-ERR-2: a write failure stops the RECORDING, not the session, and says
        so ONCE — `frames()` yields ~10 chunks/sec, so a per-frame message would
        bury the activity log in seconds. `live_archive` already classifies the
        failures it expects and announces them itself; this is the floor under
        the ones it does not, because an exception escaping here unwinds the
        capture loop and ends a metered call over a file write.
        """
        try:
            archive.write(frame)
            return True
        except Exception as exc:  # noqa: BLE001 - the session outlives this
            self._say_unless_stale(
                generation,
                f"Live audio is no longer being recorded ({exc}). The live "
                "transcript continues; audio written before the failure is kept.",
                Severity.ERROR,
            )
            return False

    def _finalise_archive(self, archive, generation: int) -> Optional[str]:
        """Close the recording and say where it went, or None if there is nothing.

        Called with the surface still alive, because `stop()` renders the
        transcript from the surface's name map and `LiveSurface.stop()` clears
        it. Ordinarily a second stop — the pump's `with` already finalised the
        recording — and `LiveArchive.stop()` is idempotent for exactly that.
        """
        if archive is None:
            return None
        try:
            archive.stop()
        except Exception as exc:  # noqa: BLE001 - must not strand the claim
            log.warning("live: the archive did not stop cleanly (%s)", type(exc).__name__)
            self._say_unless_stale(
                generation,
                f"The live recording could not be finalised ({exc}).",
                Severity.ERROR,
            )
        wav = archive.wav_path
        transcript = archive.transcript_path
        if wav is None:
            # FR-1.5: a session that captured no audio wrote nothing at all, and
            # announcing a saved recording would name a file that is not there.
            return None
        if transcript is None:
            # FR-ERR-3: audio survives a transcript failure — which `live_archive`
            # has already surfaced — and naming the file is what makes it
            # recoverable by hand or through the batch path.
            return f"Live audio saved — {wav}. No transcript was written."
        return f"Live session saved — audio {wav}, transcript {transcript}."

    # -- the pump ---------------------------------------------------------

    def _pump(self, capture, transcriber, generation: int, *, archive=None) -> None:
        """Frames out of the device, into the bridge, until something ends it.

        Every exit path — a clean stop, a capture failure, a bridge failure, an
        exception raised by the teardown itself — runs `_teardown` from the
        `finally`, because a permanently suspended offload worker silently stops
        discovering recordings, and the only symptom is the absence of something
        (FR-6.3).

        `generation` is this thread's session. It is carried all the way to the
        `finally` because this thread can outlive `stop()`'s bounded join, and a
        teardown that identified only "a session" would then dismantle the NEXT
        one — resuming polling while its capture is driving the same Jensen
        endpoint, which is the collision FR-6.1 exists to prevent.

        `archive` is this session's recorder, passed rather than read off `self`
        for the same reason: a stale pump reading `self._archive` would finalise
        the NEXT session's recording. It is keyword-only and defaults to None
        because a composition with no archive directory has none to pass.
        """
        # The archive is the OUTERMOST context, so it is the LAST thing stopped.
        # `live_archive_prd.md` §7's sketch lists it innermost; that would
        # finalise the transcript BEFORE `transcriber.__exit__` closes the two
        # provider sessions — and that close delivers turns. Read from the SDK
        # rather than assumed: `StreamingClient.disconnect(terminate=True)`
        # (`assemblyai/streaming/v3/client.py`) enqueues `TerminateSession` and
        # then waits, commented "the server sends the final Turn and
        # TerminationEvent after receiving Terminate ... waiting on it here lets
        # those messages dispatch before teardown". Innermost would drop the last
        # thing each speaker said from the archived document while leaving it on
        # screen — and the document is the copy the pipeline reads.
        recorder = archive if archive is not None else contextlib.nullcontext()
        recording = archive is not None
        try:
            with recorder:
                with capture:
                    with transcriber:
                        while not self._stopping and not self._stale(generation):
                            delivered = False
                            for frame in capture.frames():
                                if self._stopping or self._stale(generation):
                                    break
                                # Audio to the disk BEFORE the wire (§7). Ours
                                # is the only recording that will exist, and
                                # `feed` is the call that can end the session.
                                if recording:
                                    recording = self._record(archive, frame, generation)
                                transcriber.feed(frame)
                                delivered = True
                            if self._stopping or self._stale(generation):
                                break
                            if not delivered:
                                # `frames()` bounds emptiness by wall clock, so
                                # an empty drain is the device having stopped
                                # sending — a conversational pause still
                                # produces samples.
                                self._say_unless_stale(
                                    generation,
                                    "Live transcription stopped — the device "
                                    "stopped sending audio.",
                                    Severity.WARNING,
                                )
                                break
        except Exception as exc:  # noqa: BLE001 - surfaced, then released
            # Translated, not forwarded. This string is the operator's only
            # account of why a live call stopped, and the commonest cause — the
            # device being unplugged (FR-6.4) — arrives here as internal
            # transport jargon that names no cause and no remedy.
            self._say_unless_stale(
                generation,
                f"Live transcription stopped — {_translate_live_failure(str(exc))}",
                Severity.ERROR,
            )
        finally:
            self._teardown(generation)

    def _stale(self, generation: int) -> bool:
        """Has this thread's session been replaced while it was still running?

        The companion to `_teardown`'s guard. `_stopping` alone cannot answer
        it: `start()` resets that flag, so a pump outliving `stop()`'s bounded
        join reads the NEXT session's False and keeps going. Reading the
        generation needs no lock — it is a monotonic int, and a stale reader
        that is one increment behind is stale either way.
        """
        return self._generation != generation

    def _say_unless_stale(self, generation: int, message: str,
                          severity: Severity) -> None:
        # A dead session's parting words landing on top of a live one's log
        # would tell the operator their running session had stopped — the same
        # newest-lines-contradict-reality failure `start()` guards against.
        if not self._stale(generation):
            self._say(message, severity)

    # -- teardown ---------------------------------------------------------

    def _teardown(self, generation: int) -> None:
        """Runs exactly once per session, from whichever path gets there first.

        `resume_polling` is idempotent on the App side, but calling it from both
        the pump's `finally` and `stop()` would let a missing call in one hide
        behind the other — so the guard lives here.

        The guard is the SESSION's generation, not a bare `_torn_down` flag.
        `stop()` joins the pump for five seconds and then tears down regardless;
        a pump blocked longer than that on a device read is still alive, still
        holds a reference to `self`, and eventually reaches this `finally`. By
        then the operator may have pressed `l` again, and a session-blind
        teardown would stop that session's surface, clear `_live`, and call
        `resume_polling` while its capture is mid-stream on the shared Jensen
        endpoint (FR-6.1). Stamped identity makes that a no-op instead.
        """
        with self._lock:
            if generation != self._generation or self._torn_down:
                return
            self._torn_down = True
            self._live = False
            surface = self._surface
            archive = self._archive
            self._surface = None
            self._archive = None
            self._capture = None
            self._thread = None

        # BEFORE the surface's session ends, because the recording's transcript
        # is rendered from the operator's `label -> name` map. `end_session`
        # keeps that map now — `stop()` is what clears it, and that no longer
        # runs here — but the ordering is kept deliberately: it is what makes
        # the snapshot the LAST word on the names, so a rename arriving after
        # this point finds a transcript on disk to substitute into rather than
        # racing the render that produces it.
        #
        # Normally a no-op — the pump's `with` already finalised it — but
        # `stop()` joins that pump for five seconds and then tears down
        # regardless, and a recording finalised without the map is a document of
        # `Speaker 1`s for a call whose speakers the operator had already named.
        #
        # Wrapped whole: everything below releases the device claim, and a raise
        # from here would strand the offload worker suspended (FR-6.3) over a
        # file write.
        try:
            saved = self._finalise_archive(archive, generation)
        except Exception as exc:  # noqa: BLE001 - must not strand the claim
            log.warning("live: the recording was not finalised (%s)", type(exc).__name__)
            saved = None

        # The SESSION ends here; the WINDOW may not. This is the reversal argued
        # in `post_session_naming_prd.md` §5. AssemblyAI's diarization improves
        # as a call proceeds and revises labels late, so the speakers most worth
        # naming are the ones that appear as the call ends — and taking the page
        # down at exactly that moment is what made the operator type a name into
        # a dead window and watch nothing happen.
        #
        # It is kept only when there is a TRANSCRIPT to name into, which is the
        # thing the window is being kept FOR. A start that raised before the
        # pump owned the session, a call where nobody spoke, an archive that
        # could not be written — none of those produced a document, so naming
        # would change nothing and a bound server serving an empty page is the
        # disclosure surface FR-1.6 named with none of the benefit.
        try:
            keep = archive is not None and archive.transcript_path is not None
        except Exception as exc:  # noqa: BLE001 - an unreadable path is not a window
            log.warning("live: could not read the transcript path (%s)", type(exc).__name__)
            keep = False

        # The bus subscription STAYS while the window does: `rename_speaker`'s
        # result is published as a live-context event, and the page the operator
        # is reading it on is this one. `close_window()` unsubscribes.
        try:
            if surface is not None and keep:
                surface.end_session()
                with self._lock:
                    self._ended_surface = surface
                    self._ended_archive = archive
            elif surface is not None:
                surface.attach_rename_sink(None)
                self._bus.unsubscribe(surface.publish)
                surface.stop()
        except Exception as exc:  # noqa: BLE001 - must not strand the claim
            log.warning("live: surface did not end cleanly (%s)", type(exc).__name__)
        finally:
            self._resume()

        # WHERE the call went. The operator has just been told by this project
        # that live audio was being lost; "it is saved, here" is the line that
        # closes that, and it names paths rather than a count so the file can be
        # opened without going looking for it.
        #
        # Published after the surface is gone, deliberately: an `Error` with
        # `context == "live"` is forwarded to the page, which renders every one
        # of them in its problem style — so announcing it a moment earlier would
        # put an amber warning on screen saying the recording succeeded.
        if saved:
            self._say_unless_stale(generation, saved, Severity.INFO)

        # Say WHY, using what the caller passed. "stopped" and "app shutting
        # down" are different events and the operator can act on the difference;
        # collapsing them into one line was the defect. Published after the
        # claim is released so the log order matches the real order.
        reason = self._stop_reason
        if reason:
            # Whether the window survived is the operator's next action, so it
            # belongs in the line that tells them the call is over rather than
            # in a second line they have to connect to this one.
            #
            # A FRESH ticket, because the one minted at session start has long
            # expired and the most likely thing an operator does when a call
            # ends is close the window. FR-5.4 already names this exactly —
            # "the operator closes the window and the log is the only place the
            # URL can come from" — and until now that was only true at the
            # start of a call, which is the half where they still have it open.
            tail = ""
            if keep:
                tail = " The window is still open — speaker names can still be changed."
                try:
                    tail += f" Reopen it at {surface.launch_url(_MANUAL_TICKET_TTL_SECONDS)}"
                except Exception as exc:  # noqa: BLE001 - a URL is not the window
                    log.warning(
                        "live: could not mint a reopen ticket (%s)", type(exc).__name__
                    )
            self._say_unless_stale(
                generation,
                f"Live transcription ended — {reason}. The transcript is kept.{tail}",
                Severity.INFO,
            )

    def _say(self, message: str, severity: Severity) -> None:
        self._bus.publish(Error(message=message, severity=severity, context="live"))
