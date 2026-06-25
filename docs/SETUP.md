# HiDock Direct — Setup (from scratch)

This walks a brand-new user from a fresh machine to offloading and transcribing
their own HiDock recordings.

## 1. Prerequisites

- **macOS** (Apple Silicon or Intel).
- **Python 3.11+**. Check with `python3 --version`. If it's older, install a
  newer one: `brew install python@3.12`.
- **Homebrew** (https://brew.sh) — used to install `libusb`.
- An **AssemblyAI API key** — sign up at https://www.assemblyai.com/, then copy
  the key from the dashboard. (Transcription is billed per audio-hour by
  AssemblyAI; the key is yours.)

## 2. Clone and bootstrap

```bash
git clone <hidock_direct-runtime repo URL>
cd hidock_direct-runtime
./scripts/bootstrap.sh
```

`bootstrap.sh` will:
- pick a Python ≥3.11,
- `brew install libusb` if it's missing (needed for USB),
- create a machine-local `.venv`,
- install HiDock Direct and the **vendored** `diarize_audio` transcription
  package (no second repo to clone).

You should see `ok: hidock_direct …` and `ok: diarize_audio (vendored)` at the end.

## 3. Configure

```bash
cp .env.example .env
```

Open `.env` and set your key:

```
ASSEMBLYAI_API_KEY=<paste your key here>
```

That's the only required value. Optionally change `HIDOCK_ARCHIVE_DIR` (where
recordings + transcripts are saved; defaults to `~/hidock-archive`). Leave the
rest at their defaults.

## 4. Run

```bash
./.venv/bin/python -m hidock_direct
```

**Optional — a one-word launch.** Add a shell alias so you can start it by
typing `hidock` from any Terminal (adjust the path if you cloned elsewhere):

```bash
echo 'alias hidock="$HOME/hidock-direct/.venv/bin/python -m hidock_direct"' >> ~/.zprofile && source ~/.zprofile
```

Plug in the HiDock over USB. The TUI shows the device connecting, lists
recordings, and offloads them. Each recording is transcribed automatically
right after it downloads.

### What success looks like

- The TUI moves through `waiting → connected → offloading → transcribing → done`.
- Files appear under your archive dir, organized by date:
  ```
  <HIDOCK_ARCHIVE_DIR>/2026/06/2026-06-24_145900.mp3   # audio
  <HIDOCK_ARCHIVE_DIR>/2026/06/2026-06-24_145900.md     # speaker-diarized transcript
  <HIDOCK_ARCHIVE_DIR>/2026/06/2026-06-24_145900.aai.json  # raw AssemblyAI result
  ```
- Press `Ctrl-C` to quit; in-flight work finishes and state is saved.

## 5. Troubleshooting

- **"Access denied to device" / device won't connect.** Another app is holding
  the USB claim — close any open HiDock web interface (Chrome/WebUSB) tab and
  replug. On first plug-in a brief re-enumeration race is normal; replug after
  ~5 seconds.
- **"diarize_audio is not importable" banner.** Your install is incomplete —
  re-run `./scripts/bootstrap.sh` and always launch via the project venv
  (`./.venv/bin/python -m hidock_direct`), not a system Python.
- **"ASSEMBLYAI_API_KEY is required" / transcription fails.** The key isn't set
  or is wrong. Confirm `.env` exists in the repo root and contains a valid key.
  (If you've exhausted AssemblyAI credits, transcription will fail until you top
  up — the offload itself still succeeds.)
- **"need Python >=3.11".** Install a newer Python (`brew install python@3.12`)
  and re-run bootstrap.
