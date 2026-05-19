from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from platformdirs import user_config_dir


@dataclass(frozen=True)
class FlareSolverrConfig:
    # Enabled by default so CLI routes via FlareSolverr unless explicitly disabled.
    enabled: bool = True
    url: str = "http://localhost:8191"
    max_timeout_ms: int = 60_000


@dataclass(frozen=True)
class HttpConfig:
    timeout_seconds: float = 30.0
    user_agent: str = "mcosint/0.1.0 (+https://github.com/)"


@dataclass(frozen=True)
class AppConfig:
    http: HttpConfig = field(default_factory=HttpConfig)
    flaresolverr: FlareSolverrConfig = field(default_factory=FlareSolverrConfig)


def default_config_path() -> Path:
    cfg_dir = Path(user_config_dir("mcosint"))
    return cfg_dir / "config.json"


def _bool_from_env(value: str | None) -> bool | None:
    if value is None:
        return None
    v = value.strip().lower()
    if v in {"1", "true", "yes", "on"}:
        return True
    if v in {"0", "false", "no", "off"}:
        return False
    return None


def load_config(path: Path | None = None) -> AppConfig:
    """Load config from disk and apply environment overrides.

    Env overrides:
      - MCOSINT_FLARESOLVERR_URL
      - MCOSINT_USE_FLARESOLVERR
    """

    cfg_path = path or default_config_path()

    cfg_dict: dict[str, Any] = {}
    if cfg_path.exists():
        cfg_dict = json.loads(cfg_path.read_text(encoding="utf-8"))

    http = HttpConfig(**cfg_dict.get("http", {}))
    flaresolverr = FlareSolverrConfig(**cfg_dict.get("flaresolverr", {}))

    # Environment overrides
    env_fs_url = os.getenv("MCOSINT_FLARESOLVERR_URL")
    env_fs_enabled = _bool_from_env(os.getenv("MCOSINT_USE_FLARESOLVERR"))

    if env_fs_url:
        flaresolverr = FlareSolverrConfig(
            enabled=flaresolverr.enabled,
            url=env_fs_url,
            max_timeout_ms=flaresolverr.max_timeout_ms,
        )

    if env_fs_enabled is not None:
        flaresolverr = FlareSolverrConfig(
            enabled=env_fs_enabled,
            url=flaresolverr.url,
            max_timeout_ms=flaresolverr.max_timeout_ms,
        )

    return AppConfig(http=http, flaresolverr=flaresolverr)


def save_default_config(path: Path | None = None, *, overwrite: bool = False) -> Path:
    cfg_path = path or default_config_path()
    cfg_path.parent.mkdir(parents=True, exist_ok=True)

    if cfg_path.exists() and not overwrite:
        return cfg_path

    cfg = AppConfig()
    cfg_path.write_text(json.dumps(asdict(cfg), indent=2), encoding="utf-8")
    return cfg_path
