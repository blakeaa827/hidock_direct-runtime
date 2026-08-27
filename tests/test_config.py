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
