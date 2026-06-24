# hidock-direct

A foreground macOS TUI that pulls audio recordings off the HiDock device over USB into a local archive **and transcribes them** (speaker-diarized) via AssemblyAI. Transcription is provided by `diarize_audio`, which is **vendored into this repo** (`src/diarize_audio/`) — so a single clone is fully self-contained.

New here? See **[docs/SETUP.md](docs/SETUP.md)** for a from-scratch walkthrough.

## Requirements

- macOS (Apple Silicon or Intel). Linux/Windows are out of scope.
- Python 3.11+.
- `libusb` via Homebrew: `brew install libusb`.
- An [AssemblyAI](https://www.assemblyai.com/) API key (for transcription).

## Install

```bash
./scripts/bootstrap.sh        # creates .venv, installs everything (vendored diarize_audio included)
cp .env.example .env          # then edit .env and paste your ASSEMBLYAI_API_KEY
```

`bootstrap.sh` picks a Python ≥3.11, installs `libusb` (if Homebrew is present), creates a machine-local `.venv`, and installs the app in editable mode. No Google Drive or private-repo access is required.

## Run

```bash
./.venv/bin/python -m hidock_direct     # or: ./.venv/bin/hidock-direct
```

Plug in the HiDock; recordings offload to `HIDOCK_ARCHIVE_DIR` and transcribe automatically. Artifacts land as `YYYY/MM/YYYY-MM-DD_HHMMSS.{mp3,md,aai.json}`.

## Configuration

All settings live in a single clone-local `.env` (copy from `.env.example`). hidock loads it into the environment at startup so the vendored `diarize_audio` sees the same values.

| Variable | Default | Purpose |
|---|---|---|
| `ASSEMBLYAI_API_KEY` | *(required)* | AssemblyAI key for transcription |
| `HIDOCK_ARCHIVE_DIR` | `~/HiDock/archive` | Where audio + transcripts are written |
| `TRANSCRIBE_ON_OFFLOAD` | `true` | Transcribe each recording after offload |
| `DRIVE_ENABLED` | `false` | Upload transcripts to Google Drive (see below) |
| `DELETE_FROM_DEVICE_AFTER_OFFLOAD` | `false` | Delete from device after successful archive |
| `POLL_INTERVAL_SECONDS` | `10` | Device re-scan cadence while connected |
| `LOG_LEVEL` | `info` | One of `debug`, `info`, `warning`, `error` |

Discovery order for the `.env`: `$HIDOCK_DIRECT_ENV_FILE` → `./.env` (clone-local, primary) → a forge-sibling secrets file (maintainer-only fallback).

### Google Drive (optional)

Drive upload is **off by default** and needs the private `blake-commons` dependency:

```bash
./.venv/bin/pip install ".[drive]"    # requires SSH access to the blake-commons repo
# then set DRIVE_ENABLED=true in .env
```

Without it, transcripts are written locally only — no GCP credentials needed.

## Tests

```bash
./.venv/bin/python -m pytest tests/ -q
```

Unit tests mock the device at the Jensen adapter boundary — no hardware required. Full integration with a real HiDock attached is documented in `scripts/manual_verify.sh` (no CI path covers that).

## Architecture

- `src/hidock_direct/` — application
  - `app.py` — state machine + lifecycle controller
  - `usb_watcher.py` — USB attach/detach via `pyusb`/`libusb` polling
  - `offload.py` — per-file pipeline (chunked download, header-sniff, atomic rename)
  - `transcribe.py` — in-process bridge to the vendored `diarize_audio` pipeline
  - `state.py` — `offload_state.json` read/write (atomic, `.bak`)
  - `events.py` — typed event classes + in-process bus
  - `tui.py` — rich presenter subscribed to the bus
  - `config.py` — `.env` loader (+ `load_env_file_into_environ` for diarize)
  - `locks.py` — flock helper
  - `device.py` — adapter over the vendored Jensen layer
  - `jensen/` — **vendored** from [sgeraldes/hidock-next](https://github.com/sgeraldes/hidock-next); pin in `jensen/VENDORED_COMMIT`. Re-vendor: `scripts/refresh_jensen.sh`.
- `src/diarize_audio/` — **vendored** transcription package; pin in `diarize_audio/VENDORED_COMMIT`, owned by the canonical `diarize_audio-runtime` repo. Re-vendor: `scripts/refresh_diarize.sh <commit>`.
- `tests/` — pytest suite + in-memory mock device
- `scripts/` — `bootstrap.sh`, `refresh_jensen.sh`, `refresh_diarize.sh`, `manual_verify.sh`

The event bus is load-bearing for the phase-2 menu-bar app. Business logic never lives in the TUI.

## Non-goals (MVP)

- Slack / ntfy notifications
- `launchd` auto-launch on USB attach
- Speaker name remapping
- Multi-platform (macOS only)
