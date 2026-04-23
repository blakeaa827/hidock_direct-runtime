# Task Checklist — Meetings vs Whispers Classification

PRD: `~/.../forge-hidock_direct/projects/hidock_direct/planning/2026-04-23-meetings-vs-whispers-prd.md`

## PRD requirements

- [x] §2.1 `classify.py` module with `RecordingKind` enum + `classify_recording()` regex
- [x] §2.1 Full test matrix in `tests/test_classify.py` (21 parametrized cases)
- [x] §2.2 `SCHEMA_VERSION` bumped 1 → 2
- [x] §2.2 v1 → v2 auto-migration on load (writes `.bak`)
- [x] §2.2 Idempotent v2 load (no rewrite-on-read)
- [x] §2.2 `record_processed(..., kind=...)` writes `kind` field
- [x] §2.2 `StateStore.kind_for(device_key, device_filename)` helper
- [x] §2.2 `_delete_after_offload` filters to `kind == MEETING`
- [x] §2.3 `ScanResult` dataclass
- [x] §2.3 `Offloader.scan_pending_files()` returns typed bundle
- [x] §2.3 `Offloader.offload_one(file, kind)` explicit-intent entry
- [x] §2.3 Whispers/unknowns skip size-stable check
- [x] §2.4 `<archive>/whispers/` routing for kind=WHISPER
- [x] §2.5 `WhispersDetected`, `UnknownsDetected`, `WhisperOffloadRequested`, `UnknownRouted` events
- [x] §2.5 `ScanComplete.new_file_count` = meeting count
- [x] §2.6 TUI footer line counts (`format_pending_footer`)
- [x] §2.6 Whisper selector state (`WhisperSelectionState`) + handler
- [x] §2.6 Unknown prompt handler (`handle_unknown_prompt`)
- [x] §2.6 State gating (`keys_active_in_state` — CONNECTED_IDLE only)
- [x] §2.7 Operator-actionable error messages (whisper_offload / unknown_route / archive_setup)

## Universal skill steps

- [x] Prereqs: forge + runtime clean, baseline 66/66 pass
- [x] task.md created
- [x] TDD: tests written → red → green (119 pass)
- [x] Gate 1: module tests + full suite pass (119/119)
- [x] Gate 1 step 7: conditional branch visibility audit — no silent no-op paths added
- [x] Gate 1 step 8: operator-actionable error messages audit — all 3 new Error emissions translated
- [x] Gate 2: requirement audit vs PRD — all §2-§2.7 met, no stubs/TODOs
- [x] Gate 2 step 4: state JSON matches PRD §4.2 example (verified with live generation)
- [x] Gate 3: security audit — no secrets, literal paths, translated errors don't leak stack traces
- [x] Gate 4: proof of life — `test_classify_integration.py` E2E: mixed scan → auto-drain meetings → route whisper + unknown → re-scan verifies processed don't reappear
- [x] diarize_audio regression test for `walk_wavs` whispers sibling (66/66 pass)
- [ ] Commit + push runtime + diarize_audio
- [ ] Update forge SESSION.md + TODO.md
- [ ] Commit forge
