"""Thin wrapper around blake_commons.google.DriveClient for YYYY/MM folder tree."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)


class DriveSync:
    """Idempotent uploader that lays files under `{root}/{year}/{month}/`."""

    def __init__(self, root_folder_name: str, client: Any | None = None):
        self.root_folder_name = root_folder_name
        self._client = client  # may be None until first use
        self._folder_cache: dict[tuple[str, str | None], str] = {}

    @classmethod
    def default(cls, root_folder_name: str) -> "DriveSync":
        try:
            from blake_commons.google import DriveClient  # lazy import (optional `drive` extra)
        except ImportError as exc:
            from .config import ConfigError
            raise ConfigError(
                "DRIVE_ENABLED=true requires the Drive extra. Install it with: "
                "pip install diarize-audio[drive] — or set DRIVE_ENABLED=false "
                "to transcribe without Google Drive upload."
            ) from exc
        return cls(root_folder_name=root_folder_name, client=DriveClient.default())

    def _ensure_folder(self, name: str, parent_id: str | None) -> str:
        key = (name, parent_id)
        cached = self._folder_cache.get(key)
        if cached:
            return cached
        folder = self._client.find_or_create_folder(name, parent_id=parent_id)
        fid = folder["id"]
        self._folder_cache[key] = fid
        return fid

    def upload(self, local_path: Path, *, year: str, month: str) -> dict[str, Any]:
        """Upload `local_path` to `{root}/{year}/{month}/`.

        Retries once on transient failure before propagating.
        """
        local_path = Path(local_path)
        root_id = self._ensure_folder(self.root_folder_name, None)
        year_id = self._ensure_folder(year, root_id)
        month_id = self._ensure_folder(month, year_id)
        last_exc: Exception | None = None
        for attempt in (1, 2):
            try:
                return self._client.upload(
                    local_path,
                    folder_id=month_id,
                )
            except Exception as exc:
                last_exc = exc
                log.warning(
                    "drive upload attempt %d failed for %s: %s",
                    attempt, local_path.name, exc,
                )
        assert last_exc is not None
        raise last_exc
