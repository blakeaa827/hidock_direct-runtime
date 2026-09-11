"""Configuration loading from environment and a clone-local `.env`.

`load_env_file_into_environ()` runs once at startup (called by `__main__`) to
load the discovered `.env` into `os.environ`, so the vendored `diarize_audio`'s
`Config.from_env()` sees the same variables (ASSEMBLYAI_API_KEY, DRIVE_ENABLED,
…). `load_config()` then reads the typed hidock values.

Env-file discovery order (first existing file wins):
  1. `$HIDOCK_DIRECT_ENV_FILE` if set.
  2. `./.env` in the runtime root (clone-local; copy from .env.example).

Per-variable precedence within `load_config`: real process env > `.env` file >
default. DRIVE_ENABLED and the rest of the transcription pipeline's settings are
consumed by the vendored diarize_audio straight from `os.environ`.
ASSEMBLYAI_API_KEY is read in *both* places: diarize_audio still takes it from
`os.environ` on the offload path, and it is typed here as well so the live
transcription bridge can be handed a value at composition time instead of
reading a process global at use time — the shape that produced the INBOX_DIRS
defect (see `diarize_config_for_archive` below).
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from dotenv import dotenv_values


RUNTIME_ROOT = Path(__file__).resolve().parents[2]


def ensure_tls_trust_store() -> Optional[str]:
    """Point OpenSSL at `certifi` when this interpreter has no usable CA store.

    Why a clone-and-run app needs this, and why the symptom is so confusing:
    the OFFLOAD path reaches AssemblyAI through `httpx`, which defaults to
    certifi and therefore always works. The LIVE path reaches it through
    `websockets`, which the AAI SDK calls WITHOUT an `ssl` argument
    (`assemblyai/streaming/v3/client.py:78`), so it falls back to
    `ssl.create_default_context()` and OpenSSL's default paths. On a python.org
    macOS build those paths are EMPTY until `Install Certificates.command` has
    been run — Homebrew builds are fine, which is why this is invisible on some
    machines and fatal on others.

    The result is a user whose transcription has worked for months and whose
    live session dies with `CERTIFICATE_VERIFY_FAILED: unable to get local
    issuer certificate`. Observed on the operator's second Mac 2026-08-27;
    reproduced exactly here by pointing SSL_CERT_FILE at an empty bundle.

    Deliberately conservative, and NOT the `INBOX_DIRS` shape that bit this
    project before: it mutates a process global only when the store is provably
    unusable, never overrides a value the operator or environment already set,
    and returns what it did so the caller can say so out loud.
    """
    if os.environ.get("SSL_CERT_FILE"):
        return None                      # somebody already chose; respect it.
    try:
        import ssl

        if ssl.create_default_context().get_ca_certs():
            return None                  # the interpreter is fine as-is.
    except Exception:                    # noqa: BLE001 - never block startup
        return None
    try:
        import certifi
    except ImportError:
        # certifi arrives transitively with assemblyai/httpx, so this is
        # unreachable in a correct install. Say nothing rather than guess.
        return None

    os.environ["SSL_CERT_FILE"] = certifi.where()
    return certifi.where()

_TRUE_SET = {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Config:
    archive_dir: Path
    poll_interval_seconds: int
    delete_from_device_after_offload: bool
    transcribe_on_offload: bool
    log_level: str
    source: str  # "env" or "<path>" — for diagnostics
    operator_name: str  # HIDOCK_OPERATOR_NAME — the live surface's near-channel identity
    # HIDOCK_LIVE_KEEP_WAV_DIR — where a live session's intermediate WAV is kept
    # for diagnosis instead of being deleted. `None` is the default and means
    # "delete it", which is the behaviour every clone gets.
    live_keep_wav_dir: Optional[Path]
    live_max_speakers: int  # HIDOCK_LIVE_MAX_SPEAKERS — the `l` prompt's prepopulated value
    # Never in the repr: `Config` is printed in diagnostics, and a live key
    # reached a session transcript that way on 2026-08-22. No field carries a
    # dataclass default — `load_config` is the single place a default is
    # resolved, so the two cannot drift.
    assemblyai_api_key: str = field(repr=False)

    @property
    def state_dir(self) -> Path:
        return self.archive_dir / ".state"

    @property
    def tmp_dir(self) -> Path:
        return self.archive_dir / ".tmp"

    @property
    def state_path(self) -> Path:
        return self.state_dir / "offload_state.json"

    @property
    def state_bak_path(self) -> Path:
        return self.state_dir / "offload_state.json.bak"

    @property
    def lock_path(self) -> Path:
        return self.state_dir / "hidock_direct.lock"


def _discover_env_file() -> Optional[Path]:
    explicit = os.environ.get("HIDOCK_DIRECT_ENV_FILE")
    if explicit:
        p = Path(explicit).expanduser()
        return p if p.is_file() else None
    # Clone-local `.env` (copy from .env.example).
    local = RUNTIME_ROOT / ".env"
    return local if local.is_file() else None


def load_env_file_into_environ() -> Optional[Path]:
    """Load the discovered `.env` into `os.environ` (override=False) once at
    startup, so BOTH hidock's config AND the vendored `diarize_audio`'s
    `Config.from_env()` (which reads `os.environ` for `ASSEMBLYAI_API_KEY`,
    `DRIVE_ENABLED`, etc.) see every variable from the single clone-local file.

    `override=False` preserves precedence: a value already set in the real
    process environment wins over the file. Returns the loaded path (or None).
    """
    env_path = _discover_env_file()
    if env_path and env_path.is_file():
        from dotenv import load_dotenv

        load_dotenv(env_path, override=False)
    return env_path


def diarize_config_for_archive(archive_dir):
    """The one rule for "which diarize config belongs to this archive".

    Binds `inbox_dirs` explicitly instead of routing the archive through the
    process-global `INBOX_DIRS`. Both the transcription writer
    (`transcribe._run_pipeline`) and the retry reader (`retry.load_diarize_state`)
    call this, so the answer cannot drift between them — it is one
    implementation, not two copies kept in agreement by convention.

    Previously the writer used `os.environ.setdefault("INBOX_DIRS", ...)`, which
    means "point at this archive UNLESS someone already pointed somewhere else"
    — the inverse of the intent, at exactly the moment it matters. It also made
    the result depend on call order, because a process-global mutation from a
    local call is only a no-op the *second* time.

    `.expanduser()` matches what `Config.from_env()` applies to the values it
    reads (`diarize_audio/config.py:77`); an explicit binding that skipped it
    would disagree with the env path on a `~`-prefixed archive. Production
    happens to be safe without it because `load_config` expands
    `HIDOCK_ARCHIVE_DIR` upstream, but that is safe by accident rather than by
    construction.

    Note `inbox_dirs` also feeds `pipeline._relpath_for`, which derives the Drive
    upload's year/month folder — so this binds the *scan root*, not merely the
    ledger location. That consumer is inert while `DRIVE_ENABLED=false`, and
    normalizes paths itself, so narrowing the list to the single archive changes
    nothing observable today.
    """
    from dataclasses import replace

    from diarize_audio.config import Config

    return replace(
        Config.from_env(), inbox_dirs=[Path(archive_dir).expanduser()]
    )


# The vendor's own hard cap, quoted in `live_speaker_count_prompt_prd.md` §1:
# "A hard cap on the number of speaker labels in the audio stream (integer, 1-10).
#  This is a strict limit, not a hint — once it is reached, any additional speakers
#  are merged into the closest existing label rather than given a new one."
#
# It is AssemblyAI's number, not ours, and it is not re-derived anywhere else.
SPEAKER_COUNT_MIN = 1
SPEAKER_COUNT_MAX = 10

# Named once so a refusal cannot say one range while the check enforces another.
_SPEAKER_RANGE = f"{SPEAKER_COUNT_MIN}-{SPEAKER_COUNT_MAX}"

_ASCII_DIGITS = frozenset("0123456789")


def parse_speaker_count(text: str) -> tuple[Optional[int], str]:
    """Parse an operator-typed speaker count. Returns `(value, reason)`.

    One implementation, two callers: `load_config` at startup and the `l`
    prompt at keypress time. A second copy of a vocabulary kept in agreement by
    convention is exactly what produced the 2026-08-20 `Invalid API key` defect,
    and a range that drifted between the two would let a value load at startup
    that the prompt then refuses — so Enter alone could never start a session
    and the operator would have no way to learn why.

    On success: `(n, "")`. On refusal: `(None, reason)`, where `reason` names
    the vendor's range, because 1-10 is not guessable from the number typed.

    It NEVER clamps. A clamp starts a metered session under a ceiling the
    operator neither chose nor saw, and past that ceiling the vendor MERGES
    additional speakers into the closest existing label — destroying a
    distinction rather than degrading it.

    The accepted alphabet is ASCII digits only. `int()` accepts `"١"` and
    `str.isdigit()` calls it a digit, so a length-plus-isdigit guard would admit
    a ceiling the operator cannot read back off their own screen; `"1.5"` and
    `"8x"` are refused for the same reason a first-digit regex would be wrong —
    it would silently accept 1 and 8.
    """
    candidate = (text or "").strip()
    if not candidate or not set(candidate) <= _ASCII_DIGITS:
        typed = repr(candidate) if candidate else "nothing"
        return None, (
            f"{typed} is not a speaker count — "
            f"type a whole number from {_SPEAKER_RANGE}"
        )
    value = int(candidate)
    if not (SPEAKER_COUNT_MIN <= value <= SPEAKER_COUNT_MAX):
        return None, (
            f"{value} is outside AssemblyAI's hard cap — "
            f"type a whole number from {_SPEAKER_RANGE}"
        )
    return value, ""


def _resolve(name: str, default: str, env_values: dict, overlay: Optional[dict]) -> str:
    if overlay is not None and name in overlay:
        return str(overlay[name])
    if name in os.environ:
        return os.environ[name]
    if name in env_values:
        return env_values[name] or default
    return default


def load_config(env_file: Optional[os.PathLike[str] | str] = None, overlay: Optional[dict] = None) -> Config:
    """Load and return a Config. Missing optional values fall back to defaults."""
    if env_file is not None:
        env_path: Optional[Path] = Path(env_file).expanduser()
    else:
        env_path = _discover_env_file()

    env_values: dict = {}
    if env_path and env_path.is_file():
        env_values = {k: v for k, v in dotenv_values(env_path).items() if v is not None}

    archive = _resolve("HIDOCK_ARCHIVE_DIR", "~/hidock-archive", env_values, overlay)
    poll = _resolve("POLL_INTERVAL_SECONDS", "10", env_values, overlay)
    delete = _resolve("DELETE_FROM_DEVICE_AFTER_OFFLOAD", "false", env_values, overlay)
    transcribe = _resolve("TRANSCRIBE_ON_OFFLOAD", "true", env_values, overlay)
    log = _resolve("LOG_LEVEL", "info", env_values, overlay).lower()
    # An exported-but-empty variable is "unset" for FR-4.2's purposes: a blank
    # operator name would attribute the operator's own lines to nobody, which
    # reads as a rendering fault rather than as a default. "Me" is neutral and
    # true for every user of a public clone — never the maintainer's name, and
    # never derived from the OS account, which is frequently a handle.
    operator = _resolve("HIDOCK_OPERATOR_NAME", "Me", env_values, overlay).strip() or "Me"
    # The PREPOPULATED value the `l` prompt opens with, not a fixed ceiling: the
    # right number is per-call and only the operator knows it. 8 is the
    # operator's own stated requirement — "prepopulated with a default of 8 so
    # the user can just hit enter if they choose not to override it" — and it
    # sits where the vendor's guidance points, a little headroom above a typical
    # call. 6 was live on the 10-person call that collapsed 2-3 people into one
    # label.
    # Diagnostic only, and off unless a path is named. It is a PATH rather than
    # a boolean because there is no safe default location: inside the archive is
    # the one place it must never go — `diarize_config_for_archive` binds that
    # directory as `inbox_dirs` and `diarize_audio/inbox.py` scans for `.wav`
    # specifically, so a kept WAV there is a paid duplicate transcription of a
    # call already billed live — and anywhere else would be a directory this app
    # invented on someone's disk without asking. Making the operator name it
    # also means they know where the raw call audio is accumulating.
    keep_wav = _resolve("HIDOCK_LIVE_KEEP_WAV_DIR", "", env_values, overlay).strip()
    speakers = _resolve("HIDOCK_LIVE_MAX_SPEAKERS", "8", env_values, overlay)
    api_key = _resolve("ASSEMBLYAI_API_KEY", "", env_values, overlay)

    try:
        poll_int = int(poll)
    except ValueError as exc:
        raise ValueError(f"POLL_INTERVAL_SECONDS must be an integer, got {poll!r}") from exc
    if poll_int <= 0:
        raise ValueError(f"POLL_INTERVAL_SECONDS must be > 0, got {poll_int}")

    # Same shape as POLL_INTERVAL_SECONDS: a typo is a loud startup failure
    # naming the variable, not a silent fallback to the default (which the
    # operator would never learn about) and not a TypeError from inside a paid
    # live session.
    #
    # Through `parse_speaker_count`, not a second range check beside it: this
    # value now PREPOPULATES the prompt, so a setting the loader admits and the
    # prompt refuses would be a value Enter alone could never commit, with no
    # refusal text anywhere to explain it.
    speakers_int, speakers_reason = parse_speaker_count(str(speakers))
    if speakers_int is None:
        raise ValueError(f"HIDOCK_LIVE_MAX_SPEAKERS: {speakers_reason}")

    delete_bool = str(delete).strip().lower() in _TRUE_SET
    transcribe_bool = str(transcribe).strip().lower() in _TRUE_SET
    if log not in ("debug", "info", "warning", "error"):
        raise ValueError(f"LOG_LEVEL must be one of debug/info/warning/error, got {log!r}")

    return Config(
        archive_dir=Path(archive).expanduser(),
        live_keep_wav_dir=Path(keep_wav).expanduser() if keep_wav else None,
        poll_interval_seconds=poll_int,
        delete_from_device_after_offload=delete_bool,
        transcribe_on_offload=transcribe_bool,
        log_level=log,
        source=str(env_path) if env_path else "env",
        operator_name=operator,
        live_max_speakers=speakers_int,
        assemblyai_api_key=api_key,
    )
