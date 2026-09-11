"""Entry point for `python -m hidock_direct` and the `hidock-direct` script.

Wires config -> lock -> store -> adapter -> watcher -> offloader -> app -> TUI
and blocks until SIGINT/SIGTERM.
"""

from __future__ import annotations

import signal
import sys

from .app import App
from .config import ensure_tls_trust_store, load_config, load_env_file_into_environ
from .device import JensenDeviceAdapter
from .events import Error, EventBus, RetryCandidatesDetected, Severity, TranscribeSkipped
from .live_archive import LiveArchive
from .live_server import LiveSessionController, LiveSurface, launch_app_window
from .live_transcribe import LiveTranscriber
from .locks import FileLock, LockHeld
from .offload import LiveSessionLog, Offloader
from .realtime import RealtimeSession
from .state import StateStore
from .tui import TUI
from .usb_watcher import PollingUSBWatcher


def _preflight_transcribe(bus: EventBus) -> None:
    """Fail loud if `TRANSCRIBE_ON_OFFLOAD=true` but diarize_audio is missing.

    Publishes a `TranscribeSkipped` so the TUI surfaces it, and prints a
    banner to stderr so the operator sees it even before the TUI takes
    over the screen.
    """
    try:
        import diarize_audio  # noqa: F401
        return
    except ImportError as exc:
        msg = (
            "⚠️  TRANSCRIBE_ON_OFFLOAD=true but `diarize_audio` is not importable "
            f"({exc}).\n   Offloads will succeed but will NOT be transcribed.\n"
            "   Likely cause: incomplete install — diarize_audio is vendored under\n"
            "   src/ and ships with the app, so this means the package isn't installed\n"
            "   in the active interpreter.\n"
            "   Remediation: re-run ./scripts/bootstrap.sh and launch from the project\n"
            "   venv (./.venv/bin/python -m hidock_direct)."
        )
        print(msg, file=sys.stderr, flush=True)
        bus.publish(
            TranscribeSkipped(
                device_filename="(startup)",
                reason="diarize_audio not importable at startup",
            )
        )


def load_retry_candidates(archive_dir, *, state=None):
    """Failed ledger entries under `archive_dir`, for the `r` binding.

    `state` is a test seam; production reads the real transcription ledger.
    Raises `LedgerUnavailable` when the ledger or the archive cannot be read —
    callers must surface that, never render it as "nothing to retry".
    """
    from pathlib import Path

    from .retry import find_retry_candidates, load_diarize_state

    return find_retry_candidates(
        load_diarize_state(archive_dir) if state is None else state, Path(archive_dir)
    )


def publish_retry_candidate_count(bus: EventBus, provider) -> None:
    """Seed the footer badge from the ledger at startup.

    A ledger we could not read is published as an `Error` naming the path — not
    as a count of zero. "Nothing to retry" has to mean the ledger said so; the
    2026-08-13 recovery run reported `0 candidates` against the wrong archive
    and read as a clean bill of health.
    """
    try:
        candidates = provider()
    except Exception as exc:
        bus.publish(
            Error(
                message=f"retry candidates unavailable: {exc}",
                severity=Severity.WARNING,
                context="startup",
            )
        )
        return
    bus.publish(RetryCandidatesDetected(count=len(candidates)))


def main(argv: list[str] | None = None) -> int:  # noqa: ARG001 — argv kept for future flags
    # Load the clone-local .env into the process environment FIRST, so both
    # hidock's config and the vendored diarize_audio's Config.from_env() (which
    # reads os.environ for ASSEMBLYAI_API_KEY / DRIVE_ENABLED) see all settings.
    load_env_file_into_environ()
    # Before anything opens a TLS connection. On a python.org macOS build the
    # OpenSSL default CA paths are empty until `Install Certificates.command`
    # is run, which breaks the LIVE path (websockets -> default context) while
    # leaving the offload path (httpx -> certifi) working — so the operator
    # sees months of successful transcription and a live session that dies on
    # CERTIFICATE_VERIFY_FAILED. Reported, not silent: a repaired trust store
    # is a fact about the run worth being able to see in a screenshot.
    _repaired_ca_bundle = ensure_tls_trust_store()
    try:
        config = load_config()
    except ValueError as exc:
        print(f"Config error: {exc}", file=sys.stderr)
        return 2

    config.archive_dir.mkdir(parents=True, exist_ok=True)
    config.state_dir.mkdir(parents=True, exist_ok=True)
    config.tmp_dir.mkdir(parents=True, exist_ok=True)

    lock = FileLock(config.lock_path)
    try:
        lock.acquire()
    except LockHeld as exc:
        print(str(exc), file=sys.stderr)
        return 1

    bus = EventBus()
    if _repaired_ca_bundle:
        bus.publish(Error(
            message=(
                "This Python had no CA certificates, so TLS was pointed at certifi "
                "(live transcription would otherwise fail to connect). To fix it "
                "permanently, run Install Certificates.command for your Python."
            ),
            severity=Severity.WARNING,
            context="startup",
        ))
    store = StateStore(config.state_path)
    adapter = JensenDeviceAdapter()
    watcher = PollingUSBWatcher()
    # `live_archive_prd.md` FR-3.1..FR-3.3. Constructed against the SAME bus the
    # live bridge publishes its start/stop on and the offloader publishes its
    # skip on — a log wired to a different bus, or to none, would be inert and
    # its only symptom would be a duplicated AssemblyAI charge. Built before the
    # `Offloader` so no window can be missed between the two.
    live_sessions = LiveSessionLog(bus)
    offloader = Offloader(
        adapter=adapter,
        store=store,
        bus=bus,
        archive_dir=config.archive_dir,
        tmp_dir=config.tmp_dir,
        delete_after_offload=config.delete_from_device_after_offload,
        transcribe_on_offload=config.transcribe_on_offload,
        live_sessions=live_sessions,
    )
    app = App(
        adapter=adapter,
        watcher=watcher,
        offloader=offloader,
        store=store,
        bus=bus,
        poll_interval_seconds=config.poll_interval_seconds,
    )
    from .retry import run_retry_batch

    retry_provider = lambda: load_retry_candidates(config.archive_dir)  # noqa: E731
    # The live-transcription stack (PRD live_surface_prd.md FR-5.1). Every seam
    # the controller accepts is supplied here with the REAL collaborator, not
    # left to a default: `ad98cbc` shipped the retry surface fully tested and
    # completely unreachable because this file supplied no provider, and a
    # permissive default that only the test suite ever fills is that bug wearing
    # a kwarg. `busy_predicate` reads `app.device_busy` live rather than
    # snapshotting it — the offload worker and live capture share one Jensen
    # endpoint (FR-6.1..6.3), so the answer must be current at the keypress.
    live = LiveSessionController(
        bus=bus,
        adapter=adapter,
        api_key=config.assemblyai_api_key,
        operator_name=config.operator_name,
        keep_wav_dir=config.live_keep_wav_dir,
        # The PREPOPULATED speaker count, not a fixed ceiling: the controller
        # exposes it as `default_max_speakers`, the `l` prompt opens with it, and
        # the operator can override it for that call alone. Supplied here rather
        # than left to the controller's own fallback for the `ad98cbc` reason —
        # a default only the test suite ever fills is an inert feature wearing a
        # kwarg, and `HIDOCK_LIVE_MAX_SPEAKERS` would set nothing the operator
        # ever sees.
        max_speakers=config.live_max_speakers,
        # The same directory the offload path writes to, handed over at
        # composition time rather than read from a process global at use time
        # (`live_archive_prd.md`; the INBOX_DIRS defect is what that shape
        # costs). A live session's recording is an ordinary archive recording —
        # same `YYYY/MM` folder, same `YYYY-MM-DD_HHMMSS` basename — because the
        # device keeps none of its own while it streams, so ours is the only
        # copy of the call that will ever exist.
        archive_dir=config.archive_dir,
        suspend_polling=app.suspend_device_polling,
        resume_polling=app.resume_device_polling,
        busy_predicate=lambda: app.device_busy,
        surface_factory=LiveSurface,
        capture_factory=RealtimeSession,
        transcriber_factory=LiveTranscriber,
        archive_factory=LiveArchive,
        launch_browser=launch_app_window,
    )
    tui = TUI(
        bus=bus,
        app=app,
        pending_whispers_provider=lambda: app._pending_whispers,
        pending_unknowns_provider=lambda: app._pending_unknowns,
        retry_candidates_provider=retry_provider,
        retry_runner=lambda selected: run_retry_batch(
            selected, config.archive_dir, bus=bus
        ),
        live_controller=live,
    )

    if config.transcribe_on_offload:
        _preflight_transcribe(bus)

    # Seed the footer badge before the render thread starts. The TUI subscribes
    # to the bus in its constructor, so this reaches it even pre-start().
    publish_retry_candidate_count(bus, retry_provider)

    shutting_down = False

    def _shutdown(signum, frame):  # noqa: ARG001
        # Signals are delivered on the main thread, so a second ^C arriving
        # while this handler is still unwinding re-enters it right here. Latch
        # before doing any work: the second pass would otherwise re-drive a
        # device teardown the first pass is still in the middle of.
        nonlocal shutting_down
        if shutting_down:
            return
        shutting_down = True
        # ORDER IS LOAD-BEARING, and it is the opposite of the order the exit
        # path reads in. `app.stop()` disconnects the adapter, and
        # `JensenDeviceAdapter.disconnect()` drops `_jensen` — after that the
        # live session has no transport left to send the realtime STOP opcode
        # on, and the HiDock stays in streaming mode until it is power-cycled.
        # So the live session stops FIRST, while the claim it is streaming
        # over is still open (FR-1.6, FR-6.3).
        #
        # Safe to call from a signal handler: `stop()` returns immediately when
        # no session is running, and its wait on the pump thread is capped at
        # 5s, so the handler is bounded rather than blocking indefinitely.
        try:
            # `shutdown`, not `stop`: `stop` deliberately leaves the naming
            # window serving after a call so late diarization labels can still
            # be named, and on the way out there is nothing left to leave it up
            # for. A page outliving the process that owns it is the disclosure
            # surface the surface PRD was written against.
            live.shutdown(reason="app shutting down")
        finally:
            # Unconditional: a live teardown that failed must still end the
            # app, and this is the only call that releases `app.run()`.
            app.stop()

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    tui.start()
    try:
        app.run()
    finally:
        # Defence in depth, for the paths that do NOT go through `_shutdown`:
        # `app.run()` returning on its own, or raising. A live session outlives
        # `app.run()` on its own thread, so those paths would otherwise leave a
        # metered third-party stream and a suspended offload worker to be torn
        # down by process death rather than by the code that owns them
        # (FR-1.6, FR-6.3).
        #
        # On the signal path this is already a no-op — `_shutdown` stopped the
        # session before anything disconnected the adapter, so `is_live` is
        # False here. That is the correct outcome and not a missed teardown: by
        # this point `app.stop()` has dropped the Jensen handle, so a stop
        # attempted from here could no longer reach the device at all.
        #
        # Nested so a raise here can never skip `lock.release()` — a stranded
        # lock file makes the NEXT launch fail, which is a worse failure than
        # the one being handled.
        try:
            # Unconditional, where this used to be gated on `is_live`: a
            # session that has already ended can still be holding its naming
            # window open, and that window is exactly what has to come down here.
            # `shutdown` is a no-op when there is neither.
            live.shutdown(reason="app shutting down")
        finally:
            tui.stop()
            lock.release()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
