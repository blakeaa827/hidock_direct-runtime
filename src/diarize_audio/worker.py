"""Inbox-watcher main loop."""

from __future__ import annotations

import logging
import signal
import time
from dataclasses import dataclass

from .config import Config
from .inbox import find_candidates
from .pipeline import process_file
from .state import State

log = logging.getLogger(__name__)

# Approximate AAI list prices (USD/min) as of 2026-04. Used only for visibility,
# not billing — update when AAI pricing changes.
_PRICE_PER_MIN = {"best": 0.37 / 60, "nano": 0.12 / 60}


@dataclass
class LoopSummary:
    scanned: int
    processed: int
    errors: int
    cumulative_minutes: float

    def estimated_cost_usd(self, speech_model: str) -> float:
        rate = _PRICE_PER_MIN.get(speech_model, _PRICE_PER_MIN["best"])
        return self.cumulative_minutes * rate

    def format(self, speech_model: str) -> str:
        return (
            f"Loop: scanned {self.scanned} files, processed {self.processed}, "
            f"errors {self.errors}, cumulative_minutes {self.cumulative_minutes:.2f}, "
            f"estimated_cost ${self.estimated_cost_usd(speech_model):.4f}"
        )


class StopSignal:
    def __init__(self):
        self._stop = False
        self.reason = ""

    def request(self, reason: str) -> None:
        self._stop = True
        self.reason = reason

    def should_stop(self) -> bool:
        return self._stop


def run_iteration(
    cfg: Config,
    state: State,
    aai_client,
    drive_sync,
) -> LoopSummary:
    state.reset_stale_in_flight(cfg.in_flight_ttl_minutes)
    state.update_last_run()
    state.save()
    processed = 0
    errors = 0
    scanned = 0
    for path, key in find_candidates(
        cfg.inbox_dirs,
        state,
        in_flight_ttl_minutes=cfg.in_flight_ttl_minutes,
        include_keys=True,
    ):
        scanned += 1
        log.info("discovered", extra={"key": key})
        result = process_file(path, key, state, cfg, aai_client, drive_sync)
        if result.status == "done":
            processed += 1
        else:
            errors += 1
    return LoopSummary(
        scanned=scanned,
        processed=processed,
        errors=errors,
        cumulative_minutes=float(state.data["global"]["cumulative_audio_minutes"]),
    )


def run_loop(cfg: Config, state: State, aai_client, drive_sync) -> None:
    stop = StopSignal()

    def _handler(signum, _frame):
        stop.request(signal.Signals(signum).name)

    signal.signal(signal.SIGINT, _handler)
    signal.signal(signal.SIGTERM, _handler)

    log.info("worker_started", extra={"inbox_dirs": [str(p) for p in cfg.inbox_dirs]})

    while not stop.should_stop():
        summary = run_iteration(cfg, state, aai_client, drive_sync)
        log.info(summary.format(cfg.speech_model))
        # Sleep in small ticks so shutdown is responsive.
        for _ in range(cfg.poll_interval_seconds):
            if stop.should_stop():
                break
            time.sleep(1)

    log.info("worker_stopping", extra={"reason": stop.reason})
    state.save()
