"""The composition root — that every surface the TUI exposes is actually connected.

`f6d1333` built the retry surface inside `tui.py` and left `__main__.py`
untouched, so the feature shipped inert: `r` always reported "no failed
transcriptions" and the badge count could never leave 0. Every retry test
injected `retry_candidates_provider` directly — which is precisely the wiring
production omits, so no test could see the gap.

The two structural sweeps here are the durable guards: they check the whole
class (every provider, every consumed event), not the one instance that broke.
"""

from __future__ import annotations

import ast
import inspect
import os
import pathlib
import re

import pytest

from hidock_direct import __main__ as main_mod
from hidock_direct.events import Error, EventBus, RetryCandidatesDetected
from hidock_direct.retry import LedgerUnavailable, load_diarize_state
from hidock_direct.tui import TUI

SRC = pathlib.Path(__file__).resolve().parent.parent / "src" / "hidock_direct"


def _tui_call_kwargs() -> list[str]:
    """The kwargs `__main__` passes when it constructs the TUI."""
    tree = ast.parse((SRC / "__main__.py").read_text())
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and getattr(node.func, "id", None) == "TUI":
            return [kw.arg for kw in node.keywords]
    raise AssertionError("__main__.py does not construct a TUI at all")


def test_every_tui_provider_kwarg_is_supplied_by_main():
    """Structural sweep over the constructor. A provider the TUI accepts but the
    entry point never passes is a feature wired to nothing — which is exactly how
    the retry surface shipped inert. Catches the next one too."""
    # Enumerate-and-classify rather than match a naming convention: keying on a
    # `_provider` suffix would have missed `retry_runner`, an injection point of
    # exactly the shape that shipped the feature inert.
    intentionally_defaulted = {
        "console": "production uses the real Console; tests inject a StringIO one",
        "keyboard": "production uses the real KeyboardReader; tests inject a fake",
    }
    injectable = [
        name
        for name, param in inspect.signature(TUI.__init__).parameters.items()
        if param.default is None
    ]
    assert injectable, "no injectable kwargs found — has TUI.__init__ changed?"

    passed = _tui_call_kwargs()
    missing = [
        name
        for name in injectable
        if name not in passed and name not in intentionally_defaulted
    ]

    assert not missing, (
        f"TUI accepts {missing} but __main__ never passes them; they will "
        f"silently fall back to their inert defaults. Wire them, or add them to "
        f"intentionally_defaulted with a reason."
    )


def test_every_consumed_event_has_a_production_publisher():
    """Structural sweep over the bus. An event the TUI handles but nothing
    publishes is a dead branch: `RetryCandidatesDetected` was imported, handled,
    and never sent, so `_failed_count` was pinned at 0 forever."""
    consumed = sorted(
        set(re.findall(r"isinstance\(event, (\w+)\)", (SRC / "tui.py").read_text()))
    )
    assert consumed, "no consumed events found — has _on_event changed shape?"

    # AST, not substring: `bus.publish(\n    TranscribeSkipped(...))` is a real
    # publisher that a `publish(Name(` text match would call missing.
    published: set[str] = set()
    for path in SRC.rglob("*.py"):
        if path.name == "events.py":
            continue
        for node in ast.walk(ast.parse(path.read_text())):
            if not (isinstance(node, ast.Call) and node.args):
                continue
            func = node.func
            if not (isinstance(func, ast.Attribute) and func.attr == "publish"):
                continue
            arg = node.args[0]
            if isinstance(arg, ast.Call) and isinstance(arg.func, ast.Name):
                published.add(arg.func.id)

    unpublished = [name for name in consumed if name not in published]

    assert not unpublished, (
        f"{unpublished} are handled by the TUI but never published by any "
        f"production code — the handler can never fire"
    )


class _FakeState:
    def __init__(self, files: dict):
        self.data = {"files": files}


def _errored(error: str = "boom") -> dict:
    return {"status": "error", "error": error, "retry_count": 0}


def test_retry_provider_returns_the_ledgers_failed_entries(tmp_path):
    """The provider `__main__` installs must actually reach the ledger. Injecting
    the state keeps the test hermetic; the live PoL covers the real ledger."""
    (tmp_path / "2026" / "08").mkdir(parents=True)
    (tmp_path / "2026" / "08" / "a.mp3").write_bytes(b"\x00")
    state = _FakeState({"2026/08/a.mp3": _errored()})

    candidates = main_mod.load_retry_candidates(tmp_path, state=state)

    assert [c.state_key for c in candidates] == ["2026/08/a.mp3"]


def test_pressing_r_reaches_the_ledger_through_the_wired_provider(tmp_path):
    """End-to-end through the operator's entry point: build the provider the way
    `main()` does, hand it to a TUI, dispatch the key, and require the confirm to
    open. Before the fix this logged 'no failed transcriptions to retry'."""
    (tmp_path / "2026" / "08").mkdir(parents=True)
    (tmp_path / "2026" / "08" / "a.mp3").write_bytes(b"\x00")
    state = _FakeState({"2026/08/a.mp3": _errored()})

    tui = TUI(
        bus=EventBus(),
        retry_candidates_provider=lambda: main_mod.load_retry_candidates(
            tmp_path, state=state
        ),
    )
    tui._state = "IDLE_DISCONNECTED"

    tui._on_key("r")

    assert tui._retry_confirm is not None, "confirm did not open"
    assert [c.state_key for c in tui._retry_confirm] == ["2026/08/a.mp3"]


def test_startup_publishes_the_retry_candidate_count(tmp_path):
    """The contract `events.py` documents ('Published at startup') must be real.
    Without it the badge count is pinned at 0 no matter what the ledger holds."""
    (tmp_path / "2026" / "08").mkdir(parents=True)
    for name in ("a.mp3", "b.mp3"):
        (tmp_path / "2026" / "08" / name).write_bytes(b"\x00")
    state = _FakeState({"2026/08/a.mp3": _errored(), "2026/08/b.mp3": _errored()})

    bus = EventBus()
    seen: list = []
    bus.subscribe(seen.append)

    main_mod.publish_retry_candidate_count(
        bus, lambda: main_mod.load_retry_candidates(tmp_path, state=state)
    )

    counts = [e.count for e in seen if isinstance(e, RetryCandidatesDetected)]
    assert counts == [2]


def test_startup_surfaces_an_unreadable_ledger_instead_of_reporting_zero():
    """The distinction the 2026-08-13 recovery run got wrong: 'I could not read
    your data' must never render as a confident all-clear. An unreadable ledger
    publishes an Error, NOT RetryCandidatesDetected(count=0)."""
    bus = EventBus()
    seen: list = []
    bus.subscribe(seen.append)

    def boom():
        raise LedgerUnavailable("archive directory not found: /nope")

    main_mod.publish_retry_candidate_count(bus, boom)

    assert not [e for e in seen if isinstance(e, RetryCandidatesDetected)], (
        "an unreadable ledger must not publish a candidate count"
    )
    errors = [e for e in seen if isinstance(e, Error)]
    assert errors, "an unreadable ledger must surface an Error"
    assert "/nope" in errors[0].message, "the resolved path must be named"


def test_ledger_path_agrees_between_the_retry_reader_and_the_transcribe_writer(
    tmp_path, monkeypatch
):
    """Convention agreement, asserted by running both real implementations.

    "Which ledger belongs to this archive" must have ONE answer. This test used
    to hand-copy the writer's rule inline — `monkeypatch.setenv("INBOX_DIRS",
    ...)` then `Config.from_env()` — which made it a third implementation rather
    than an assertion about the second: it would have stayed green if
    `transcribe.py` changed entirely. It now calls the writer's real code path.

    `INBOX_DIRS` is pointed at a decoy third directory throughout, because the
    original only ever proved the two agree when the environment is unset or
    already correct — the two cases where the bug cannot appear.
    """
    archive = tmp_path / "archive"
    (archive / ".state").mkdir(parents=True)
    (archive / ".state" / "transcribe_state.json").write_text(
        '{"schema_version": 1, "files": {}}'
    )
    decoy = tmp_path / "operators-inbox"
    decoy.mkdir()
    monkeypatch.setenv("ASSEMBLYAI_API_KEY", "test-key")
    monkeypatch.setenv("INBOX_DIRS", str(decoy))

    # 1. The retry reader.
    reader_path = pathlib.Path(load_diarize_state(archive).path)

    # 2. The transcribe writer — the real rule, not a replica of it.
    from hidock_direct.config import diarize_config_for_archive

    writer_path = diarize_config_for_archive(archive).state_path

    assert reader_path == writer_path, (
        f"retry reads {reader_path} but transcription writes {writer_path}"
    )
    assert reader_path.parent.parent == archive, (
        f"both agreed on {reader_path}, which is not under the archive they were "
        f"given ({archive}) — agreeing on the wrong file is not agreement"
    )


def test_transcribe_ledger_follows_the_archive_it_was_given(tmp_path, monkeypatch):
    """The bug: `os.environ.setdefault` means "point at this archive UNLESS
    someone already pointed somewhere else", which inverts the intent at exactly
    the moment it matters. The audio lands under the archive; the ledger recording
    its status lands under whatever the operator had exported."""
    archive = tmp_path / "hidock-archive"
    archive.mkdir()
    elsewhere = tmp_path / "operators-inbox"
    elsewhere.mkdir()
    monkeypatch.setenv("ASSEMBLYAI_API_KEY", "test-key")
    monkeypatch.setenv("INBOX_DIRS", str(elsewhere))

    from hidock_direct.config import diarize_config_for_archive

    cfg = diarize_config_for_archive(archive)

    assert cfg.state_path.parent.parent == archive, (
        f"ledger resolved to {cfg.state_path}, which is not under {archive}"
    )
    assert elsewhere not in cfg.state_path.parents


def test_transcribe_does_not_mutate_the_process_environment(tmp_path, monkeypatch):
    """The stronger property, and the one that keeps this fixed: not "the value
    is right" but "there is no process-global side effect at all". A global
    mutation from a local call makes the result depend on call order, which is
    why nothing caught the original."""
    archive = tmp_path / "archive"
    archive.mkdir()
    elsewhere = tmp_path / "operators-inbox"
    elsewhere.mkdir()
    monkeypatch.setenv("ASSEMBLYAI_API_KEY", "test-key")
    monkeypatch.setenv("INBOX_DIRS", str(elsewhere))

    from hidock_direct.config import diarize_config_for_archive

    before = dict(os.environ)
    diarize_config_for_archive(archive)
    after = dict(os.environ)

    changed = {
        k: (before.get(k), after.get(k))
        for k in set(before) | set(after)
        if before.get(k) != after.get(k)
    }
    assert not changed, (
        f"building the config mutated the process environment: {changed}"
    )
    assert os.environ["INBOX_DIRS"] == str(elsewhere), (
        "the operator's own INBOX_DIRS must survive untouched"
    )


def test_the_archive_binding_expands_a_tilde_like_from_env_does(tmp_path, monkeypatch):
    """`Config.from_env()` applies `.expanduser()` to what it reads
    (diarize_audio/config.py:77). An explicit binding that skips it disagrees
    with the env path on `~`-prefixed archives. Production is safe today only
    because hidock's own config expands upstream — safe by accident, not by
    construction, and this pins it."""
    monkeypatch.setenv("ASSEMBLYAI_API_KEY", "test-key")
    monkeypatch.delenv("INBOX_DIRS", raising=False)

    from hidock_direct.config import diarize_config_for_archive

    cfg = diarize_config_for_archive("~/some-archive")

    assert "~" not in str(cfg.state_path), f"tilde survived into {cfg.state_path}"
    assert cfg.state_path == pathlib.Path("~/some-archive").expanduser() / ".state" / "transcribe_state.json"


def test_no_module_in_the_package_mutates_os_environ():
    """Structural pin on the class, not the instance. The bug report's sweep found
    exactly one `os.environ` write in the package; this keeps it at zero, so the
    next one has to be argued for rather than merely added.

    Parsed with `ast`, not grepped. A line scanner reports this module's own
    docstrings — the first draft of this test failed on `config.py`'s prose
    explaining the very mutation it had removed.

    Coverage boundary, stated rather than implied: this catches direct syntactic
    mutation (`os.environ[k] = v`, `.setdefault`, `.update`, `.pop`, `.clear`).
    It does NOT catch mutation through an alias or a library —
    `load_env_file_into_environ` writes the clone-local `.env` into the
    environment via `dotenv.load_dotenv`, and is invisible here. That one is
    sanctioned anyway: process-wide startup config, not a per-call parameter
    smuggled through a global.
    """
    import hidock_direct

    _MUTATORS = {"setdefault", "update", "pop", "clear", "setitem", "popitem"}

    def _is_os_environ(node) -> bool:
        return (
            isinstance(node, ast.Attribute)
            and node.attr == "environ"
            and isinstance(node.value, ast.Name)
            and node.value.id == "os"
        )

    pkg = pathlib.Path(hidock_direct.__file__).parent
    offenders = []
    for path in sorted(pkg.rglob("*.py")):
        if "jensen" in path.parts:
            continue  # vendored; not ours to constrain
        tree = ast.parse(path.read_text(), filename=str(path))
        for node in ast.walk(tree):
            hit = None
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr in _MUTATORS
                and _is_os_environ(node.func.value)
            ):
                hit = f"os.environ.{node.func.attr}(...)"
            elif isinstance(node, (ast.Assign, ast.AugAssign)):
                targets = node.targets if isinstance(node, ast.Assign) else [node.target]
                for t in targets:
                    if isinstance(t, ast.Subscript) and _is_os_environ(t.value):
                        hit = "os.environ[...] = ..."
            elif isinstance(node, ast.Delete):
                for t in node.targets:
                    if isinstance(t, ast.Subscript) and _is_os_environ(t.value):
                        hit = "del os.environ[...]"
            if hit:
                offenders.append(f"{path.relative_to(pkg)}:{node.lineno}: {hit}")

    assert offenders == [], (
        "process-global environment mutation in the package — pass the value as "
        "an argument instead:\n  " + "\n  ".join(offenders)
    )


def test_reader_refuses_an_archive_with_no_ledger_rather_than_returning_empty(
    tmp_path, monkeypatch
):
    """An archive that has never been transcribed has no ledger. `State.load`
    would happily return an empty one (and create the directory), which reads as
    'nothing failed' instead of 'there is nothing here'."""
    archive = tmp_path / "fresh"
    archive.mkdir()
    monkeypatch.setenv("ASSEMBLYAI_API_KEY", "test-key")
    monkeypatch.delenv("INBOX_DIRS", raising=False)

    with pytest.raises(LedgerUnavailable) as exc:
        load_diarize_state(archive)

    assert "no transcription ledger" in str(exc.value)
    assert not (archive / ".state").exists(), "must not create state dirs while reading"


def _main_body_calls() -> set[str]:
    """Names called directly inside `main()`."""
    tree = ast.parse((SRC / "__main__.py").read_text())
    fn = next(
        n
        for n in tree.body
        if isinstance(n, ast.FunctionDef) and n.name == "main"
    )
    return {
        n.func.id
        for n in ast.walk(fn)
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
    }


def test_every_wiring_helper_is_actually_called_by_main():
    """Defining a helper is not wiring it.

    Caught by mutation: deleting the `publish_retry_candidate_count(...)` call
    from `main()` left every other test green, because the publisher-sweep only
    proves the publish *statement exists somewhere in src/* — satisfied by a
    function nobody invokes. That is the very shape of this bug (built, tested,
    never connected), reproduced inside its own regression suite."""
    tree = ast.parse((SRC / "__main__.py").read_text())
    helpers = [
        n.name
        for n in tree.body
        if isinstance(n, ast.FunctionDef)
        and n.name not in ("main",)
        and not n.name.startswith("_")
    ]
    assert helpers, "no public wiring helpers found in __main__.py"

    called = _main_body_calls()
    # A helper may be reached indirectly (e.g. through a lambda main passes on);
    # accept either a direct call or a reference anywhere in main's body.
    referenced = {
        n.id
        for n in ast.walk(
            next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "main")
        )
        if isinstance(n, ast.Name)
    }
    orphans = [h for h in helpers if h not in called and h not in referenced]

    assert not orphans, (
        f"{orphans} are defined in __main__.py but never reached from main() — "
        f"a wiring helper nothing calls is exactly this bug's shape"
    )


def test_every_injection_seam_default_accepts_what_its_call_site_passes():
    """Third structural sweep: an injectable collaborator's *production default*
    must accept the arguments its own module passes.

    `run_retry_batch` called its `transcribe` seam with `max_retries` and
    `reuse_existing_transcript` — both correct against `process_file`, which had
    gained them for exactly this feature — while the production default,
    `transcribe_file`, accepted neither. Every double in the suite was
    `**kw`-permissive, so a double was strictly more forgiving than the real
    collaborator and 278 tests could not see it. The retry execution path
    shipped dead and stayed dead through a merge to master.

    Sweeping seams rather than asserting one signature is the point: this covers
    the seam nobody has written yet.
    """
    from hidock_direct import transcribe as transcribe_mod
    from hidock_direct.retry import _default_load_entry
    from tests.conftest import call_sites_of, injection_seams

    module = SRC / "retry.py"

    # Enumerate-and-classify, matching the sweeps above: a newly-added seam
    # fails here until someone states what its production default resolves to,
    # rather than being silently skipped.
    resolvers = {
        "transcribe": lambda: transcribe_mod.transcribe_file,
        "load_entry": lambda: _default_load_entry(pathlib.Path("/archive")),
    }
    found = {name for name, _ in injection_seams(module)}
    assert found == set(resolvers), (
        f"injection seams in retry.py changed: {found ^ set(resolvers)}. "
        "Add the new seam's production default to `resolvers` so its contract "
        "is checked, or remove the stale entry."
    )

    for name, resolve in resolvers.items():
        real = resolve()
        signature = inspect.signature(real)
        for positional, kwargs in call_sites_of(module, name):
            try:
                signature.bind(*([None] * positional), **{k: None for k in kwargs})
            except TypeError as exc:
                raise AssertionError(
                    f"`{name}(...)` in retry.py passes {kwargs} but its production "
                    f"default {real.__module__}.{real.__qualname__} accepts "
                    f"{sorted(signature.parameters)} — {exc}"
                ) from None
