"""Configuration helpers for the trading system."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict

try:
    import yaml  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - fallback for minimal environments
    yaml = None


_ENV_PREFIX = "TS_"


def _apply_override(config: Dict[str, Any], path: str, value: str) -> None:
    keys = path.split("__")
    cursor = config
    for key in keys[:-1]:
        if key not in cursor or not isinstance(cursor[key], dict):
            cursor[key] = {}
        cursor = cursor[key]
    cursor[keys[-1]] = _coerce_env_value(value)


def _coerce_env_value(value: str) -> Any:
    lowered = value.lower()
    if lowered in {"true", "false"}:
        return lowered == "true"
    try:
        if "." in value:
            return float(value)
        return int(value)
    except ValueError:
        return value


def load_settings(path: str | Path = "config/settings.yaml") -> Dict[str, Any]:
    """Load YAML settings and merge environment overrides."""

    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(path)
    with path.open("r", encoding="utf-8") as handle:
        if yaml is not None:
            config = yaml.safe_load(handle) or {}
        else:
            import json

            config = json.load(handle)
    for key, value in os.environ.items():
        if not key.startswith(_ENV_PREFIX):
            continue
        override_path = key[len(_ENV_PREFIX) :]
        _apply_override(config, override_path, value)
    return config


__all__ = ["load_settings"]
