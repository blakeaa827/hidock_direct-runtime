"""diarize_audio — generic AssemblyAI transcription worker.

Public API:
    Config, State, FileLock, AlreadyLocked, TranscriptionError,
    AAIClient, DriveSync, process_file, PipelineResult,
    render_markdown, run_loop, run_iteration, LoopSummary.
"""

from .assemblyai_client import AAIClient, TranscriptionError
from .config import Config, ConfigError
from .drive_sync import DriveSync
from .inbox import find_candidates
from .locks import AlreadyLocked, FileLock
from .pipeline import PipelineResult, process_file
from .render import render_markdown
from .state import State, StateError
from .worker import LoopSummary, run_iteration, run_loop

__version__ = "0.1.0"

__all__ = [
    "__version__",
    "AAIClient", "TranscriptionError",
    "Config", "ConfigError",
    "DriveSync",
    "find_candidates",
    "AlreadyLocked", "FileLock",
    "PipelineResult", "process_file",
    "render_markdown",
    "State", "StateError",
    "LoopSummary", "run_iteration", "run_loop",
]
