import os
from pathlib import Path
from typing import Any

import yaml


_config_cache: dict | None = None
_PROJECT_ROOT = Path(__file__).parent.parent.resolve()


def load_config(path: str = "config.yaml") -> dict[str, Any]:
    global _config_cache
    if _config_cache is not None:
        return _config_cache

    config_path = (_PROJECT_ROOT / path).resolve()
    if not str(config_path).startswith(str(_PROJECT_ROOT)):
        raise ValueError(f"허용되지 않은 설정 파일 경로입니다: {path}")

    with open(config_path, "r", encoding="utf-8") as f:
        _config_cache = yaml.safe_load(f)
    return _config_cache


def get_watchlist(config: dict | None = None) -> list[str]:
    config = config or load_config()
    stocks = config.get("stocks", {})
    if isinstance(stocks, list):
        return stocks
    return stocks.get("watchlist", [])


def is_discovery_enabled(config: dict | None = None) -> bool:
    config = config or load_config()
    stocks = config.get("stocks", {})
    if isinstance(stocks, list):
        return False
    return stocks.get("discovery", {}).get("enabled", False)


def get_discovery_config(config: dict | None = None) -> dict:
    config = config or load_config()
    return config.get("stocks", {}).get("discovery", {})


def get_sector_trend_config(config: dict | None = None) -> dict:
    config = config or load_config()
    return config.get("sector_trend", {})


def reload_config(path: str = "config.yaml") -> dict[str, Any]:
    global _config_cache
    _config_cache = None
    return load_config(path)

