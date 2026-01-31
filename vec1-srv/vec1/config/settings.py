import os
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True, slots=True)
class GitHubSettings:
    token: str
    owner: str
    repo: str


@dataclass(frozen=True, slots=True)
class LoggingSettings:
    backend: str
    name: str
    logfire_token: Optional[str]


@dataclass(frozen=True, slots=True)
class QueueSettings:
    max_size: Optional[int]


@dataclass(frozen=True, slots=True)
class ChunkerSettings:
    diff_max_tokens: int


@dataclass(frozen=True, slots=True)
class JobSettings:
    poll_interval: float
    chunk_get_timeout: float
    default_lookback_hours: int


@dataclass(frozen=True, slots=True)
class StateSettings:
    base_dir: str
    save_state_in_dev: bool


@dataclass(frozen=True, slots=True)
class Settings:
    github: GitHubSettings
    logging: LoggingSettings
    queue: QueueSettings
    chunker: ChunkerSettings
    jobs: JobSettings
    state: StateSettings


def load_settings() -> Settings:
    from dotenv import load_dotenv

    load_dotenv()

    github_token = _ge_env_or_default("GITHUB_TOKEN")
    github_owner = _ge_env_or_default("VEC1_REPO_OWNER")
    github_repo = _ge_env_or_default("VEC1_REPO_NAME")

    logging_backend = _ge_env_or_default("VEC1_LOGGER_BACKEND", "console").lower()
    logging_name = _ge_env_or_default("VEC1_LOGGER_NAME", "vec1")
    logfire_token = _ge_env_or_default("VEC1_LOGFIRE_TOKEN")

    queue_max_size = _env_optional_int("VEC1_QUEUE_MAX_SIZE")
    diff_max_tokens = _env_int("VEC1_DIFF_MAX_TOKENS", 2000)
    poll_interval = _env_float("VEC1_POLL_INTERVAL", 60.0)
    chunk_get_timeout = _env_float("VEC1_CHUNK_GET_TIMEOUT", 1.0)
    default_lookback_hours = _env_int("VEC1_DEFAULT_LOOKBACK_HOURS", 24)
    state_dir = _ge_env_or_default("VEC1_STATE_DIR", ".vec1/state")
    save_state_in_dev = _env_bool("SAVE_STATE_IN_DEV", False)

    return Settings(
        github=GitHubSettings(
            token=github_token,
            owner=github_owner,
            repo=github_repo,
        ),
        logging=LoggingSettings(
            backend=logging_backend,
            name=logging_name,
            logfire_token=logfire_token,
        ),
        queue=QueueSettings(max_size=queue_max_size),
        chunker=ChunkerSettings(diff_max_tokens=diff_max_tokens),
        jobs=JobSettings(
            poll_interval=poll_interval,
            chunk_get_timeout=chunk_get_timeout,
            default_lookback_hours=default_lookback_hours,
        ),
        state=StateSettings(base_dir=state_dir, save_state_in_dev=save_state_in_dev),
    )


def _ge_env_or_default(name: str, default: any = None) -> str:
    value = os.getenv(name)
    if not value:
        return default
    return value


def _ge_env_str(name: str, default: str = "") -> str:
    return _ge_env_or_default(name, default)


def _env_int(name: str, default) -> int:
    return int(_ge_env_or_default(name, default) or 0)


def _env_optional_int(name: str) -> Optional[int]:
    value = _ge_env_or_default(name)
    if value is None:
        return None
    return int(value)


def _env_float(name: str, default: float) -> float:
    return float(_ge_env_or_default(name, default) or 0)


def _env_bool(name: str, default: bool) -> bool:
    return str(_ge_env_or_default(name, default) or "").upper() == "TRUE"
