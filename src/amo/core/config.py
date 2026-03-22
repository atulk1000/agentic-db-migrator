from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict

import yaml
from dotenv import load_dotenv


_ENV_VAR_PATTERN = re.compile(r"\$\{([A-Z0-9_]+)\}")


def load_env(env_path: str | Path = ".env") -> None:
    """
    Loads environment variables from a .env file (if present).
    Safe to call multiple times.
    """
    load_dotenv(dotenv_path=env_path, override=False)


def _substitute_env_vars(obj: Any) -> Any:
    """
    Recursively substitute ${VAR} in YAML strings using os.environ.
    Raises a clear error if VAR is missing.
    """
    if isinstance(obj, dict):
        return {k: _substitute_env_vars(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_substitute_env_vars(v) for v in obj]
    if isinstance(obj, str):
        def repl(match: re.Match) -> str:
            key = match.group(1)
            val = os.environ.get(key)
            if val is None:
                raise ValueError(f"Missing required environment variable: {key}")
            return val

        return _ENV_VAR_PATTERN.sub(repl, obj)
    return obj


def load_config(config_path: str | Path) -> Dict[str, Any]:
    """
    Load YAML config and substitute ${ENV_VAR} placeholders.
    """
    p = Path(config_path)
    if not p.exists():
        raise FileNotFoundError(f"Config file not found: {p}")

    try:
        raw_text = p.read_text(encoding="utf-8-sig")
    except UnicodeDecodeError as exc:
        raise ValueError(
            f"Config file {p} is not valid UTF-8. Re-save it as UTF-8 and try again."
        ) from exc

    raw = yaml.safe_load(raw_text) or {}
    return _substitute_env_vars(raw)

