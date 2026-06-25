# Vendored: sgeraldes/hidock-next

This directory is a **pinned copy** of the Python modules that implement the
reverse-engineered "Jensen" USB protocol for the HiDock device, from
[sgeraldes/hidock-next](https://github.com/sgeraldes/hidock-next).

## License

Upstream is **MIT-licensed** (Copyright © 2025 Sebastian Geraldes). The full
license text is preserved in [LICENSE](LICENSE), as MIT requires the copyright
and permission notice to ship with any copy. Our only local change (relative
imports) does not affect that.

## Pinned commit

See [VENDORED_COMMIT](VENDORED_COMMIT).

## What's vendored

Five files from `apps/desktop/src/` in the upstream repo:

| File | Upstream role |
|---|---|
| `hidock_device.py` | `HiDockJensen` class — USB attach, packet framing, stream_file, list_files, delete_file, etc. |
| `jensen_protocol_extensions.py` | `ExtendedJensenProtocol` subclass for hardware info/perf metrics (not required by MVP, retained for completeness) |
| `hta_converter.py` | `HTAConverter` — `.hda` → `.wav`/`.mp3` conversion |
| `config_and_logger.py` | `logger` global used by the vendored code |
| `constants.py` | USB VID/PID filter list, Jensen command IDs, config file name |

## Local modifications

1. **Relative imports** — all intra-package imports rewritten `from X import ...` → `from .X import ...` so the module namespace is self-contained (`hidock_direct.jensen.*`).
2. **No other edits** — source is otherwise byte-identical to the upstream commit.

## How to refresh

Run [`scripts/refresh_jensen.sh`](../../../scripts/refresh_jensen.sh) with a new commit hash. It re-downloads the five files, re-applies the import rewrite, and updates `VENDORED_COMMIT`. Review the diff before committing; upstream's API may have drifted.

## Security posture

This is third-party code inside our trust boundary. Treat every re-vendor as a supply-chain review:

- Diff against the previous pin.
- Verify the commit exists on the upstream repo (not a fork) and is signed or has recent activity.
- Re-run the full test suite after refresh.
