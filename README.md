# hidock-direct

A foreground macOS TUI that pulls audio recordings directly off the HiDock device over USB and writes them to a local archive directory. Offload only — transcription and diarization live in a separate worker (`diarize_audio`).

## Requirements

- macOS (Apple Silicon or Intel). Linux/Windows are out of scope.
- Python 3.11+.
- `libusb` via Homebrew: `brew install libusb`.

## Install

```bash
scripts/bootstrap.sh
```

This creates `.venv-$(hostname -s)/` inside the repo, installs dependencies in editable mode, and applies the iCloud `.pth` fix (`chflags nohidden`) so editable installs actually resolve. The hostname suffix lets the repo ride iCloud across machines without cross-polluting venvs.

## Run

```bash
# TUI
.venv-$(hostname -s)/bin/python -m hidock_direct

# Or via installed entry point
.venv-$(hostname -s)/bin/hidock-direct
```

Environment variables (see `config.py`):

| Variable | Default | Purpose |
|---|---|---|
| `HIDOCK_ARCHIVE_DIR` | `~/HiDock/archive` | Where `.wav` files are written |
| `POLL_INTERVAL_SECONDS` | `10` | Device re-scan cadence while connected |
| `DELETE_FROM_DEVICE_AFTER_OFFLOAD` | `false` | Delete files from device after successful archive |
| `LOG_LEVEL` | `info` | One of `debug`, `info`, `warning`, `error` |

Secrets/config lives at `forge/projects/hidock_direct/secrets/.env` (forge convention). The runtime loads it at startup.

## Tests

```bash
.venv-$(hostname -s)/bin/pytest tests/ -v
```

Unit tests mock the device at the Jensen adapter boundary. Hardware is not required for the test suite. Full integration (real HiDock attached) is documented in `scripts/manual_verify.sh` — no CI path covers that.

## Architecture

- `src/hidock_direct/` — application
  - `app.py` — state machine + lifecycle controller
  - `usb_watcher.py` — IOKit attach/detach via pyobjc
  - `offload.py` — per-file pipeline (size-stable, chunked download, SHA verify, atomic rename)
  - `state.py` — `offload_state.json` read/write (atomic, .bak)
  - `events.py` — typed event classes + in-process bus
  - `tui.py` — rich presenter subscribed to the bus
  - `config.py` — `.env` loader
  - `locks.py` — flock helper
  - `device.py` — adapter over the vendored Jensen layer
  - `jensen/` — **vendored** from [sgeraldes/hidock-next](https://github.com/sgeraldes/hidock-next) at pin `89cb03acd6e56e38e310d7f01b111cc6a92bc042`. See `jensen/README.md`.
- `tests/` — pytest suite + in-memory mock device
- `scripts/` — `bootstrap.sh`, `refresh_jensen.sh`, `manual_verify.sh`

The event bus is load-bearing for the phase-2 menu-bar app. Business logic never lives in the TUI.

## Non-goals (MVP)

- Slack / ntfy notifications
- `launchd` auto-launch on USB attach
- Drive sync (lives in `diarize_audio`)
- Transcription, diarization, Markdown rendering (lives in `diarize_audio`)
- Speaker name remapping
- Multi-platform
