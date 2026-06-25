# hidock-direct

A foreground macOS TUI that pulls audio recordings off your HiDock device over USB into a local archive **and transcribes them** (speaker-diarized) via AssemblyAI.

New here? The **[click-by-click install guide](https://blakeaa827.github.io/hidock_direct-runtime/install-guide.html)** (hosted on GitHub Pages) walks non-technical teammates from a fresh Mac to a working setup — Claude Code → AssemblyAI → run. Prefer the terminal? **[docs/SETUP.md](docs/SETUP.md)** is the concise version. (The guide's source is **[docs/install-guide.html](docs/install-guide.html)**.)

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

`bootstrap.sh` picks a Python ≥3.11, installs `libusb` (if Homebrew is present), creates a `.venv`, and installs the app.

## Run

```bash
./.venv/bin/python -m hidock_direct     # or: ./.venv/bin/hidock-direct
```

Plug in the HiDock; recordings offload to `HIDOCK_ARCHIVE_DIR` and transcribe automatically. Artifacts land as `YYYY/MM/YYYY-MM-DD_HHMMSS.{mp3,md,aai.json}`.

For a one-word launch, add a shell alias once (adjust the path to your install dir):

```bash
echo 'alias hidock="$HOME/hidock-direct/.venv/bin/python -m hidock_direct"' >> ~/.zprofile && source ~/.zprofile
```

Then just type `hidock` in any Terminal.

## Configuration

All settings live in a single `.env` file — copy it from `.env.example` and edit.

| Variable | Default | Purpose |
|---|---|---|
| `ASSEMBLYAI_API_KEY` | *(required)* | AssemblyAI key for transcription |
| `HIDOCK_ARCHIVE_DIR` | `~/hidock-archive` | Where audio + transcripts are written |
| `TRANSCRIBE_ON_OFFLOAD` | `true` | Transcribe each recording after offload |
| `DRIVE_ENABLED` | `false` | Off by default; Google Drive upload needs extra setup |
| `DELETE_FROM_DEVICE_AFTER_OFFLOAD` | `false` | Delete from device after successful archive |
| `POLL_INTERVAL_SECONDS` | `10` | Device re-scan cadence while connected |
| `LOG_LEVEL` | `info` | One of `debug`, `info`, `warning`, `error` |

Discovery order: `$HIDOCK_DIRECT_ENV_FILE`, then `./.env`.

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
- `src/diarize_audio/` — **vendored** transcription package; pin in `diarize_audio/VENDORED_COMMIT`. Re-vendor: `scripts/refresh_diarize.sh <commit>`.
- `tests/` — pytest suite + in-memory mock device
- `scripts/` — `bootstrap.sh`, `refresh_jensen.sh`, `refresh_diarize.sh`, `manual_verify.sh`

Business logic never lives in the TUI — the event bus decouples it.

## Non-goals (MVP)

- Slack / ntfy notifications
- `launchd` auto-launch on USB attach
- Speaker name remapping
- Multi-platform (macOS only)
