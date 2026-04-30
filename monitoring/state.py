from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def targets_signature(targets: list[dict[str, Any]]) -> str:
    parts: list[str] = []
    for target in targets:
        parts.append(str(target.get("query_name") or ""))
        parts.append(json.dumps(target.get("seed_to_overpay") or {}, sort_keys=True))
        parts.append(json.dumps(target.get("seed_to_confidence") or {}, sort_keys=True))
    return hashlib.sha256("\n".join(parts).encode("utf-8")).hexdigest()


def default_state(targets: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "version": 1,
        "targets_signature": targets_signature(targets),
        "targets_count": len(targets),
        "target_pointer": 0,
        "cycle_count": 0,
        "last_run_started_at_utc": None,
        "last_run_finished_at_utc": None,
        "last_target": None,
        "last_status": None,
        "last_error": None,
        "sent_alerts": {},
    }


def load_state(path: Path, targets: list[dict[str, Any]]) -> dict[str, Any]:
    if not path.is_file():
        return default_state(targets)
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default_state(targets)
    if not isinstance(state, dict):
        return default_state(targets)

    sig = targets_signature(targets)
    count = len(targets)
    if state.get("targets_signature") != sig or int(state.get("targets_count") or -1) != count:
        fresh = default_state(targets)
        if isinstance(state.get("sent_alerts"), dict):
            fresh["sent_alerts"] = state["sent_alerts"]
        return fresh
    if not isinstance(state.get("sent_alerts"), dict):
        state["sent_alerts"] = {}
    return state


def save_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def select_targets(
    targets: list[dict[str, Any]], state: dict[str, Any], batch_size: int
) -> tuple[list[dict[str, Any]], int, int, bool]:
    if not targets:
        return [], 0, 0, False
    total = len(targets)
    size = max(1, min(int(batch_size), total))
    start = int(state.get("target_pointer") or 0) % total
    batch = [targets[(start + offset) % total] for offset in range(size)]
    next_pointer = (start + size) % total
    full_cycle_done = size >= total or next_pointer <= start
    return batch, start, next_pointer, full_cycle_done


def mark_run_started(state: dict[str, Any]) -> dict[str, Any]:
    out = dict(state)
    out["last_run_started_at_utc"] = utc_now_iso()
    out["last_status"] = "running"
    out["last_error"] = None
    return out


def mark_target_done(
    state: dict[str, Any],
    *,
    target: dict[str, Any],
    next_pointer: int,
    full_cycle_done: bool,
    status: str,
    rows_seen: int,
    opportunities: int,
    alerts_sent: int,
    error: str | None = None,
) -> dict[str, Any]:
    out = dict(state)
    out["target_pointer"] = int(next_pointer)
    if full_cycle_done:
        out["cycle_count"] = int(out.get("cycle_count") or 0) + 1
    out["last_target"] = str(target.get("query_name") or "")
    out["last_status"] = status
    out["last_error"] = error
    out["last_target_finished_at_utc"] = utc_now_iso()
    out["last_rows_seen"] = int(rows_seen)
    out["last_opportunities"] = int(opportunities)
    out["last_alerts_sent"] = int(alerts_sent)
    return out


def mark_run_finished(state: dict[str, Any], *, status: str, error: str | None = None) -> dict[str, Any]:
    out = dict(state)
    out["last_run_finished_at_utc"] = utc_now_iso()
    out["last_status"] = status
    out["last_error"] = error
    return out


def _parse_iso(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def should_send_alert(state: dict[str, Any], key: str, cooldown_hours: float) -> bool:
    sent = state.get("sent_alerts")
    if not isinstance(sent, dict):
        return True
    previous = sent.get(key)
    if not isinstance(previous, dict):
        return True
    if cooldown_hours < 0:
        return False
    sent_at = _parse_iso(previous.get("sent_at_utc"))
    if sent_at is None:
        return True
    return datetime.now(timezone.utc) - sent_at >= timedelta(hours=float(cooldown_hours))


def mark_alert_sent(state: dict[str, Any], key: str, row: dict[str, Any]) -> dict[str, Any]:
    out = dict(state)
    sent = out.get("sent_alerts")
    if not isinstance(sent, dict):
        sent = {}
    sent[key] = {
        "sent_at_utc": utc_now_iso(),
        "query_name": str(row.get("query_name") or ""),
        "market_hash_name": str(row.get("market_hash_name") or ""),
        "listing_id": str(row.get("listing_id") or ""),
        "paint_seed": row.get("paint_seed"),
        "ask": row.get("ask"),
        "edge_ratio": row.get("edge_ratio"),
    }
    out["sent_alerts"] = sent
    out["last_alert_sent_at_utc"] = utc_now_iso()
    return out
