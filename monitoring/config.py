from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    out = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = deep_merge(out[key], value)
        else:
            out[key] = value
    return out


def resolve_path(value: str | Path, *, root: Path | None = None) -> Path:
    base = root or repo_root()
    path = Path(value).expanduser()
    return path if path.is_absolute() else (base / path).resolve()


def defaults() -> dict[str, Any]:
    return {
        "paths": {
            "target_csv": "data/seed_target_table.csv",
            "items_py": "lists/screening_super_full.py",
            "state_json": "monitoring_runtime/state.json",
            "latest_matches_csv": "monitoring_runtime/seed_matches_latest.csv",
            "latest_opportunities_csv": "monitoring_runtime/opportunities_latest.csv",
            "progress_csv": "monitoring_runtime/progress_latest.csv",
        },
        "monitoring": {
            "duration_minutes": 300,
            "target_batch_size": 1,
            "query_substring": "",
            "max_batches": None,
            "max_cycles": None,
            "cycle_sleep_sec": 0.0,
            "clear_latest_outputs_on_start": True,
        },
        "steam": {
            "currency": 3,
            "limit": 100,
            "max_listings_per_item": 200,
            "request_timeout_sec": 45.0,
            "retry_attempts": 2,
            "retry_sleep_min_sec": 2.0,
            "retry_sleep_max_sec": 7.0,
            "delay_between_items_min_sec": 2.0,
            "delay_between_items_max_sec": 5.0,
            "delay_between_pages_min_sec": 2.0,
            "delay_between_pages_max_sec": 5.0,
            "log_progress": True,
        },
        "filters": {
            "min_edge_ratio": 0.7,
            "max_ask": None,
            "min_expected_overpay_mult": None,
            "min_overpay_gap_mult": None,
            "min_overall_confidence": None,
            "item_substring": "",
        },
        "telegram": {
            "enabled": False,
            "cooldown_hours": 24.0,
            "sleep_sec": 0.6,
            "max_alerts_per_target": None,
            "disable_web_page_preview": True,
        },
    }


def load_config(path: Path | None) -> dict[str, Any]:
    cfg = defaults()
    if path is None:
        return cfg
    if not path.is_file():
        raise FileNotFoundError(path)
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return deep_merge(cfg, raw)


def path_from_config(cfg: dict[str, Any], key: str) -> Path:
    value = cfg.get("paths", {}).get(key)
    if not value:
        raise KeyError(f"Missing paths.{key}")
    return resolve_path(value)
