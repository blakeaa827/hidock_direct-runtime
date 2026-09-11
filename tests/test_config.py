"""Config loader: defaults, env override, .env file, type coercion."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from hidock_direct import config as hconfig
from hidock_direct.config import load_config


@pytest.fixture
def clean_environ():
    """Snapshot/restore os.environ — load_dotenv writes directly to os.environ
    (outside monkeypatch's tracking), so tests that load a .env must restore it."""
    snapshot = dict(os.environ)
    try:
        yield
    finally:
        os.environ.clear()
        os.environ.update(snapshot)


def test_load_config_defaults(tmp_path: Path, monkeypatch):
    monkeypatch.delenv("HIDOCK_ARCHIVE_DIR", raising=False)
    monkeypatch.delenv("POLL_INTERVAL_SECONDS", raising=False)
    monkeypatch.delenv("DELETE_FROM_DEVICE_AFTER_OFFLOAD", raising=False)
    monkeypatch.delenv("LOG_LEVEL", raising=False)
    monkeypatch.setenv("HIDOCK_DIRECT_ENV_FILE", str(tmp_path / "missing.env"))
    cfg = load_config()
    assert cfg.archive_dir == Path.home() / "hidock-archive"
    assert cfg.poll_interval_seconds == 10
    assert cfg.delete_from_device_after_offload is False
    assert cfg.log_level == "info"


def test_load_config_env_overrides(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("HIDOCK_ARCHIVE_DIR", str(tmp_path / "archive"))
    monkeypatch.setenv("POLL_INTERVAL_SECONDS", "3")
    monkeypatch.setenv("DELETE_FROM_DEVICE_AFTER_OFFLOAD", "true")
    monkeypatch.setenv("LOG_LEVEL", "debug")
    cfg = load_config()
    assert cfg.archive_dir == tmp_path / "archive"
    assert cfg.poll_interval_seconds == 3
    assert cfg.delete_from_device_after_offload is True
    assert cfg.log_level == "debug"


def test_load_config_env_file(tmp_path: Path, monkeypatch):
    monkeypatch.delenv("HIDOCK_ARCHIVE_DIR", raising=False)
    monkeypatch.delenv("POLL_INTERVAL_SECONDS", raising=False)
    env_path = tmp_path / "hidock.env"
    env_path.write_text(f"HIDOCK_ARCHIVE_DIR={tmp_path / 'arch'}\nPOLL_INTERVAL_SECONDS=7\n")
    cfg = load_config(env_file=env_path)
    assert cfg.archive_dir == tmp_path / "arch"
    assert cfg.poll_interval_seconds == 7
    assert cfg.source == str(env_path)


def test_load_config_invalid_poll(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("POLL_INTERVAL_SECONDS", "0")
    with pytest.raises(ValueError):
        load_config()


def test_load_config_invalid_log_level(monkeypatch):
    monkeypatch.setenv("LOG_LEVEL", "louder")
    with pytest.raises(ValueError):
        load_config()


# --- FR-C2: env-file discovery (explicit override, then clone-local .env) ----

def test_discover_finds_clone_local(tmp_path, monkeypatch):
    monkeypatch.delenv("HIDOCK_DIRECT_ENV_FILE", raising=False)
    clone = tmp_path / "runtime"
    clone.mkdir()
    (clone / ".env").write_text("HIDOCK_ARCHIVE_DIR=/x\n")
    monkeypatch.setattr(hconfig, "RUNTIME_ROOT", clone)
    assert hconfig._discover_env_file() == clone / ".env"


def test_discover_returns_none_when_no_env(tmp_path, monkeypatch):
    monkeypatch.delenv("HIDOCK_DIRECT_ENV_FILE", raising=False)
    clone = tmp_path / "runtime"
    clone.mkdir()  # no .env present
    monkeypatch.setattr(hconfig, "RUNTIME_ROOT", clone)
    assert hconfig._discover_env_file() is None


def test_discover_explicit_env_file_overrides_both(tmp_path, monkeypatch):
    clone = tmp_path / "runtime"
    clone.mkdir()
    (clone / ".env").write_text("X=1\n")
    explicit = tmp_path / "explicit.env"
    explicit.write_text("X=2\n")
    monkeypatch.setattr(hconfig, "RUNTIME_ROOT", clone)
    monkeypatch.setenv("HIDOCK_DIRECT_ENV_FILE", str(explicit))
    assert hconfig._discover_env_file() == explicit


# --- FR-C1: the clone-local .env is loaded into os.environ for diarize -------

def test_load_env_file_into_environ_populates_os_environ(tmp_path, clean_environ):
    os.environ.pop("ASSEMBLYAI_API_KEY", None)
    os.environ.pop("DRIVE_ENABLED", None)
    env = tmp_path / ".env"
    env.write_text("ASSEMBLYAI_API_KEY=secret-xyz\nDRIVE_ENABLED=false\n")
    os.environ["HIDOCK_DIRECT_ENV_FILE"] = str(env)
    loaded = hconfig.load_env_file_into_environ()
    assert loaded == env
    # the vendored diarize_audio reads os.environ — it must now see the key
    assert os.environ["ASSEMBLYAI_API_KEY"] == "secret-xyz"
    assert os.environ["DRIVE_ENABLED"] == "false"


def test_load_env_file_into_environ_does_not_override_process_env(tmp_path, clean_environ):
    os.environ["ASSEMBLYAI_API_KEY"] = "real-process-key"
    env = tmp_path / ".env"
    env.write_text("ASSEMBLYAI_API_KEY=file-key\n")
    os.environ["HIDOCK_DIRECT_ENV_FILE"] = str(env)
    hconfig.load_env_file_into_environ()
    # override=False: a value already in the real process env wins over the file
    assert os.environ["ASSEMBLYAI_API_KEY"] == "real-process-key"


# --- FR-C4: .env.example is the complete, secret-free onboarding template ----

def test_env_example_lists_all_recognized_vars():
    example = Path(__file__).resolve().parents[1] / ".env.example"
    text = example.read_text()
    for var in (
        "ASSEMBLYAI_API_KEY",
        "HIDOCK_ARCHIVE_DIR",
        "DRIVE_ENABLED",
        "TRANSCRIBE_ON_OFFLOAD",
        "DELETE_FROM_DEVICE_AFTER_OFFLOAD",
        "POLL_INTERVAL_SECONDS",
        "LOG_LEVEL",
    ):
        assert f"{var}=" in text, f".env.example is missing {var}"


def test_env_example_has_no_real_api_key():
    example = Path(__file__).resolve().parents[1] / ".env.example"
    for line in example.read_text().splitlines():
        if line.startswith("ASSEMBLYAI_API_KEY="):
            assert line.strip() == "ASSEMBLYAI_API_KEY=", "a real key leaked into .env.example"
            break
    else:
        pytest.fail("ASSEMBLYAI_API_KEY line not found in .env.example")


# --------------------------------------------------------------------------
# TLS trust store — the live path's silent portability trap
# --------------------------------------------------------------------------


def test_an_empty_ca_store_is_repaired_from_certifi(monkeypatch):
    """Observed live 2026-08-27 on a python.org Python.

    The offload path uses httpx (certifi, always works); the live path uses
    websockets, which the AAI SDK calls without an `ssl` argument, so it uses
    OpenSSL's default paths. Those are empty on a python.org build until
    `Install Certificates.command` runs — batch transcription succeeds for
    months and the live session dies on CERTIFICATE_VERIFY_FAILED.

    MUTATION: make `ensure_tls_trust_store` return None unconditionally and
    this test fails.
    """
    import ssl as ssl_mod

    from hidock_direct.config import ensure_tls_trust_store

    monkeypatch.delenv("SSL_CERT_FILE", raising=False)

    class Empty:
        def get_ca_certs(self):
            return []

    monkeypatch.setattr(ssl_mod, "create_default_context", lambda *a, **k: Empty())

    repaired = ensure_tls_trust_store()
    import certifi

    assert repaired == certifi.where()
    assert os.environ["SSL_CERT_FILE"] == certifi.where()


def test_a_working_ca_store_is_left_alone(monkeypatch):
    """MUTATION: drop the `if ...get_ca_certs(): return None` guard -> fails."""
    from hidock_direct.config import ensure_tls_trust_store

    monkeypatch.delenv("SSL_CERT_FILE", raising=False)
    assert ensure_tls_trust_store() is None
    assert "SSL_CERT_FILE" not in os.environ


def test_an_operator_chosen_bundle_is_never_overridden(monkeypatch):
    """A process-global mutation must not out-vote a deliberate setting.

    This is the INBOX_DIRS lesson applied: that defect was a setdefault whose
    semantics inverted the operator's intent.

    MUTATION: drop the `if os.environ.get("SSL_CERT_FILE")` guard -> fails.
    """
    import ssl as ssl_mod

    from hidock_direct.config import ensure_tls_trust_store

    monkeypatch.setenv("SSL_CERT_FILE", "/operator/chosen/bundle.pem")

    class Empty:
        def get_ca_certs(self):
            return []

    monkeypatch.setattr(ssl_mod, "create_default_context", lambda *a, **k: Empty())

    assert ensure_tls_trust_store() is None
    assert os.environ["SSL_CERT_FILE"] == "/operator/chosen/bundle.pem"


# --------------------------------------------------------------------------
# Per-session speaker count — the PREPOPULATED default and the vendor range
# (live_speaker_count_prompt_prd.md §5 U-12, FR-2.1, FR-2.4, NFR-4)
#
# `HIDOCK_LIVE_MAX_SPEAKERS` stops being a fixed ceiling here and becomes the
# value the `l` prompt is prepopulated with. Two consequences this section
# pins:
#
#   1. The default moves 6 -> 8. The operator asked for 8 verbatim ("so the
#      user can just hit enter if they choose not to override it"), and the
#      vendor's own guidance is headroom above the expected count.
#   2. The range check 1..10 is the VENDOR's, quoted in the PRD ("a strict
#      limit, not a hint"), and it now has to serve two callers — this loader
#      at startup and the prompt at keypress time. It lives here, once, so the
#      two cannot drift. A second copy of a vocabulary is precisely what
#      produced the 2026-08-20 `Invalid API key` defect.
# --------------------------------------------------------------------------


def _dashes(text: str) -> str:
    """Normalise en/em dashes so a range written `1–10` reads as `1-10`.

    The tests pin that the reason NAMES the range, not which dash character the
    implementer typed.
    """
    return text.replace("–", "-").replace("—", "-")


def _speaker_config(tmp_path: Path, monkeypatch, value=None):
    """`load_config` with a guaranteed-absent env file, so only `value` decides."""
    monkeypatch.delenv("HIDOCK_LIVE_MAX_SPEAKERS", raising=False)
    monkeypatch.delenv("HIDOCK_DIRECT_ENV_FILE", raising=False)
    if value is not None:
        monkeypatch.setenv("HIDOCK_LIVE_MAX_SPEAKERS", value)
    return load_config(env_file=tmp_path / "absent.env")


def test_the_prepopulated_speaker_default_is_eight(tmp_path, monkeypatch):
    """U-12 / FR-2.4. The operator's stated requirement, quoted in the PRD:
    "prepopulated with a default of 8 so the user can just hit enter if they
    choose not to override it."

    This is no longer a fixed ceiling that silently governs every call — it is
    the number the prompt shows, and Enter alone commits it. 6 was live on the
    10-person call that collapsed 2-3 people into one label.

    MUTATION: `_resolve("HIDOCK_LIVE_MAX_SPEAKERS", "6", ...)` — restore the old
    default literal in `load_config` and this test fails.
    """
    assert _speaker_config(tmp_path, monkeypatch).live_max_speakers == 8


def test_the_speaker_default_is_still_overridable_and_typed(tmp_path, monkeypatch):
    """FR-2.4 changes what the variable MEANS, not whether it is read.

    MUTATION: drop the `int(...)` coercion — the prompt would then prepopulate
    with the string "3" and the far session's ceiling would reach the SDK as
    text.
    """
    config = _speaker_config(tmp_path, monkeypatch, "3")

    assert config.live_max_speakers == 3
    assert isinstance(config.live_max_speakers, int)


def test_a_prepopulated_default_above_the_vendor_cap_fails_loudly(tmp_path, monkeypatch):
    """FR-2.1's range is the vendor's hard cap, and the loader is now the source
    of a value the operator commits with a single Enter.

    An `HIDOCK_LIVE_MAX_SPEAKERS=11` that survived startup would prepopulate the
    prompt with a number the prompt itself must refuse — Enter alone could never
    start a session, and the operator would have no way to learn why from the
    refusal. The existing stance for this variable is already "a typo is a loud
    startup failure naming the variable"; out-of-range is the same class.

    MUTATION: keep only the `if speakers_int <= 0` check and let 11 through.
    """
    with pytest.raises(ValueError) as excinfo:
        _speaker_config(tmp_path, monkeypatch, "11")

    message = _dashes(str(excinfo.value))
    assert "HIDOCK_LIVE_MAX_SPEAKERS" in message
    assert "1-10" in message, f"the failure does not name the vendor range: {message}"


def test_a_prepopulated_default_below_the_vendor_cap_fails_loudly(tmp_path, monkeypatch):
    """The other end of the same range. Zero speakers is not a call.

    MUTATION: change the lower bound to `< 0`, which admits 0 — a far session
    that may emit no speaker label at all.
    """
    with pytest.raises(ValueError) as excinfo:
        _speaker_config(tmp_path, monkeypatch, "0")

    assert "HIDOCK_LIVE_MAX_SPEAKERS" in str(excinfo.value)


def test_a_non_numeric_prepopulated_default_still_fails_loudly(tmp_path, monkeypatch):
    """Unchanged behaviour, restated because the range check is being rewritten
    around it and a rewrite is where a branch gets dropped.

    MUTATION: `int(value) if value.isdigit() else 8` — the typo silently becomes
    the default and the operator never learns their setting was ignored.
    """
    with pytest.raises(ValueError) as excinfo:
        _speaker_config(tmp_path, monkeypatch, "six")

    assert "HIDOCK_LIVE_MAX_SPEAKERS" in str(excinfo.value)


# -- the shared validator: one implementation, two callers -------------------


def test_parse_speaker_count_accepts_the_whole_vendor_range():
    """FR-2.1. The accepted set is exactly 1..10, inclusive at both ends.

    MUTATION: `range(1, 10)` instead of `1 <= n <= 10` — 10, the value a large
    call needs and the one the vendor caps at, is then refused.
    """
    from hidock_direct.config import parse_speaker_count

    for n in range(1, 11):
        value, reason = parse_speaker_count(str(n))
        assert value == n, f"{n} was refused: {reason}"
        assert reason == "", f"{n} was accepted with a complaint: {reason}"


@pytest.mark.parametrize("text", ["0", "11", "-1", "abc", "", "   ", "1.5", "8x", "١"])
def test_parse_speaker_count_refuses_everything_outside_the_range(text):
    """U-7. Each of these is a thing an operator can actually type at the prompt.

    `"1.5"` and `"8x"` are here because `int()` rejects them but a permissive
    parser (a regex that finds the first digit, say) would silently accept 1 and
    8 — a ceiling the operator did not enter. `"١"` is an Arabic-Indic digit:
    `str.isdigit()` says True and `int()` accepts it, so a length-plus-isdigit
    guard would admit a value the operator cannot read back off their own screen.

    MUTATION: return the clamped value instead of None on the out-of-range
    branch — `min(10, max(1, n))` — and "0" and "11" start sessions.
    """
    from hidock_direct.config import parse_speaker_count

    value, reason = parse_speaker_count(text)

    assert value is None, f"{text!r} was accepted as {value}"
    assert reason, f"{text!r} was refused with no reason to show the operator"


@pytest.mark.parametrize("text", ["0", "11", "-1", "abc"])
def test_a_refusal_names_the_range_the_operator_must_type(text):
    """U-7's second clause. "Invalid" tells the operator nothing; the refusal has
    to carry the vendor's range, because 1-10 is not guessable from the number
    they typed.

    MUTATION: `return None, "invalid speaker count"` — refused, but unactionable.
    """
    from hidock_direct.config import parse_speaker_count

    _value, reason = parse_speaker_count(text)

    assert "1-10" in _dashes(reason), f"the refusal does not name the range: {reason}"


def test_parse_speaker_count_never_clamps():
    """U-8, stated as an absence. A clamp is the failure mode this whole PRD
    exists to prevent: it starts a METERED session under a ceiling the operator
    neither chose nor saw, and the vendor merges every speaker past that ceiling
    into the closest existing label — destroying the distinction rather than
    degrading it.

    MUTATION: `return min(10, max(1, int(text))), ""` on the numeric branch.
    """
    from hidock_direct.config import parse_speaker_count

    assert parse_speaker_count("11")[0] is None
    assert parse_speaker_count("0")[0] is None
    assert parse_speaker_count("-1")[0] is None
    assert parse_speaker_count("100")[0] is None


def test_the_loader_and_the_prompt_share_one_range_implementation(tmp_path, monkeypatch):
    """An agreement test that calls BOTH implementations rather than restating
    either — the `10fba18` lesson, whose old agreement test hand-copied the rule
    it claimed to check and stayed green across a total rewrite.

    Every value `parse_speaker_count` refuses must also be refused by
    `load_config`, and every value it accepts must load. Two range checks kept in
    agreement by convention is the state that produced the bug this project
    already paid for once.

    MUTATION: leave `load_config`'s own `if speakers_int <= 0` in place instead of
    routing it through `parse_speaker_count`; "11" then loads and the prompt
    refuses it.
    """
    from hidock_direct.config import parse_speaker_count

    for text in ("1", "8", "10", "0", "11", "-1", "six"):
        parsed, _reason = parse_speaker_count(text)
        try:
            loaded = _speaker_config(tmp_path, monkeypatch, text).live_max_speakers
        except ValueError:
            loaded = None
        assert loaded == parsed, (
            f"{text!r}: the prompt parses it as {parsed!r} and the loader as "
            f"{loaded!r} — two range checks that disagree"
        )


# -- NFR-4: the onboarding template documents what the variable now means -----


def _env_example_block() -> tuple[str, str]:
    """(the HIDOCK_LIVE_MAX_SPEAKERS assignment, the comment block above it)."""
    example = Path(__file__).resolve().parents[1] / ".env.example"
    lines = example.read_text().splitlines()
    for i, line in enumerate(lines):
        if line.startswith("HIDOCK_LIVE_MAX_SPEAKERS="):
            comment = []
            j = i - 1
            while j >= 0 and lines[j].startswith("#"):
                comment.insert(0, lines[j])
                j -= 1
            return line, "\n".join(comment)
    pytest.fail("HIDOCK_LIVE_MAX_SPEAKERS is not documented in .env.example")


def test_the_env_example_value_is_the_loader_default(tmp_path, monkeypatch):
    """Derived, not restated: the template's number is read back and compared to
    what `load_config` actually defaults to, so the two cannot drift.

    MUTATION: change the default in `load_config` without touching
    `.env.example` (or the reverse) — a clone would then start with a
    prepopulated value the documentation contradicts.
    """
    assignment, _comment = _env_example_block()

    documented = assignment.split("=", 1)[1].strip()

    assert int(documented) == _speaker_config(tmp_path, monkeypatch).live_max_speakers


def test_the_env_example_says_it_is_a_prepopulated_default_not_a_ceiling():
    """NFR-4. The old comment calls it "a CEILING, not a hint" and tells the
    operator to set it to participants-minus-one — advice this PRD supersedes,
    because the value is now per-call and typed at the prompt.

    MUTATION: leave the pre-1e comment block in place. Every behavioural test
    above still passes and the template still instructs the operator to solve
    per-call variance with a startup variable.
    """
    _assignment, comment = _env_example_block()
    comment = _dashes(comment).lower()

    assert "prepopulat" in comment, (
        f".env.example does not say the value is prepopulated at the prompt:\n{comment}"
    )
    assert "1-10" in comment, (
        f".env.example does not state the vendor range the prompt enforces:\n{comment}"
    )
