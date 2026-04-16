"""Config loader: defaults, env override, .env file, type coercion."""

from __future__ import annotations

from pathlib import Path

import pytest

from hidock_direct.config import load_config


def test_load_config_defaults(tmp_path: Path, monkeypatch):
    monkeypatch.delenv("HIDOCK_ARCHIVE_DIR", raising=False)
    monkeypatch.delenv("POLL_INTERVAL_SECONDS", raising=False)
    monkeypatch.delenv("DELETE_FROM_DEVICE_AFTER_OFFLOAD", raising=False)
    monkeypatch.delenv("LOG_LEVEL", raising=False)
    monkeypatch.setenv("HIDOCK_DIRECT_ENV_FILE", str(tmp_path / "missing.env"))
    cfg = load_config()
    assert cfg.archive_dir == Path.home() / "HiDock" / "archive"
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
