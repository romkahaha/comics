from __future__ import annotations

import argparse
import importlib.util
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from monitoring.config import load_config, path_from_config, repo_root
from monitoring.state import (
    load_state,
    mark_alert_sent,
    mark_run_finished,
    mark_run_started,
    mark_target_done,
    save_state,
    select_targets,
    should_send_alert,
    utc_now_iso,
)
from monitoring.steam_client import SteamClient
from monitoring.telegram import alert_key, format_alert, maybe_sleep, send_message, telegram_credentials


def configure_stdio() -> None:
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass


def _parse_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _safe_float(value: Any, *, positive: bool = False) -> float | None:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(out):
        return None
    if positive and out <= 0:
        return None
    return out


def _safe_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(str(value).replace(",", ".").strip()))
        except (TypeError, ValueError):
            return None


def _parse_seed_map(raw: Any) -> dict[int, float]:
    if isinstance(raw, dict):
        data = raw
    else:
        text = "" if raw is None else str(raw).strip()
        if not text or text.lower() == "nan":
            return {}
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            return {}
    out: dict[int, float] = {}
    for key, value in data.items():
        seed = _safe_int(key)
        val = _safe_float(value)
        if seed is not None and val is not None:
            out[seed] = val
    return out


def load_targets(target_csv: Path) -> list[dict[str, Any]]:
    df = pd.read_csv(target_csv)
    targets: list[dict[str, Any]] = []
    for rec in df.to_dict(orient="records"):
        query_name = str(rec.get("query_name") or "").strip()
        if not query_name:
            continue
        seed_to_overpay = _parse_seed_map(rec.get("seed_to_overpay_json"))
        if not seed_to_overpay:
            continue
        targets.append(
            {
                "query_name": query_name,
                "seed_to_overpay": seed_to_overpay,
                "seed_to_confidence": _parse_seed_map(rec.get("seed_to_confidence_json")),
                "overall_confidence": _safe_float(rec.get("overall_confidence")) or 0.0,
                "pattern_families": str(rec.get("pattern_families") or ""),
                "notes": str(rec.get("notes") or ""),
            }
        )
    return targets


def load_items_from_py(path: Path) -> list[str]:
    spec = importlib.util.spec_from_file_location(f"seed_monitor_items_{path.stem}", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot import {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    items = getattr(module, "ITEMS", None)
    if not isinstance(items, list):
        raise ValueError(f"{path} must define ITEMS = [...]")
    return [str(item) for item in items]


def match_items_for_query(items: list[str], query_name: str) -> list[str]:
    needle = query_name.lower().strip()
    if not needle:
        return []
    matched: list[str] = []
    for item in items:
        hay = item.lower().strip()
        candidates = [hay]
        if hay.startswith("stattrak"):
            candidates.append(hay[len("stattrak") :].lstrip("tm™? ").strip())
        if hay.startswith("souvenir "):
            candidates.append(hay[len("souvenir ") :].strip())
        if any(candidate.startswith(needle) for candidate in candidates):
            matched.append(item)
    return matched


def append_rows(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame(rows)
    frame.to_csv(path, mode="a", header=not path.exists(), index=False)


def append_progress(path: Path, row: dict[str, Any]) -> None:
    payload = dict(row)
    payload["finished_at_utc"] = utc_now_iso()
    append_rows(path, [payload])


def reset_latest_outputs(paths: list[Path]) -> None:
    for path in paths:
        if path.exists():
            path.unlink()


def compute_seed_rows(
    *,
    target: dict[str, Any],
    exact_item: str,
    fetched_rows: list[dict[str, Any]],
    meta: dict[str, Any],
) -> list[dict[str, Any]]:
    seed_to_overpay: dict[int, float] = target["seed_to_overpay"]
    seed_to_confidence: dict[int, float] = target["seed_to_confidence"]
    base_samples: list[float] = []
    for fetched in fetched_rows:
        ask = _safe_float(fetched.get("ask"), positive=True)
        if ask is None:
            continue
        base_samples.append(ask)
        if len(base_samples) >= 10:
            break
    base_ask = sum(base_samples) / len(base_samples) if base_samples else None
    base_n = len(base_samples)

    rows: list[dict[str, Any]] = []
    for fetched in fetched_rows:
        paint_seed = _safe_int(fetched.get("paint_seed"))
        if paint_seed is None:
            continue
        expected_overpay = seed_to_overpay.get(paint_seed)
        if expected_overpay is None:
            continue
        ask = _safe_float(fetched.get("ask"), positive=True)
        ask_seller_net = _safe_float(fetched.get("ask_seller_net"), positive=True)
        market_overpay = ask / base_ask if ask is not None and base_ask not in (None, 0) else None
        expected_price = base_ask * expected_overpay if base_ask not in (None, 0) else None
        edge_abs = expected_price - ask if expected_price is not None and ask is not None else None
        edge_ratio = expected_overpay / market_overpay - 1.0 if market_overpay not in (None, 0) else None
        rows.append(
            {
                "query_name": target["query_name"],
                "market_hash_name": exact_item,
                "pattern_families": target["pattern_families"],
                "paint_seed": paint_seed,
                "float_value": fetched.get("float_value"),
                "ask": ask,
                "ask_seller_net": ask_seller_net,
                "base_ask_mean_10": base_ask,
                "base_ask_n": base_n,
                "market_overpay_mult": market_overpay,
                "expected_overpay_mult": expected_overpay,
                "overpay_gap_mult": expected_overpay - market_overpay if market_overpay is not None else None,
                "expected_price": expected_price,
                "edge_abs": edge_abs,
                "edge_ratio": edge_ratio,
                "overall_confidence": target["overall_confidence"],
                "seed_confidence": seed_to_confidence.get(paint_seed),
                "listing_id": fetched.get("listing_id"),
                "asset_id": fetched.get("asset_id"),
                "scm_total_listings": meta.get("total_count"),
                "converted_price": fetched.get("converted_price"),
                "converted_fee": fetched.get("converted_fee"),
                "converted_currencyid": fetched.get("converted_currencyid"),
                "target_notes": target["notes"],
            }
        )
    return rows


def apply_filters(rows: list[dict[str, Any]], cfg: dict[str, Any]) -> list[dict[str, Any]]:
    if not rows:
        return []
    frame = pd.DataFrame(rows)
    mask = pd.Series(True, index=frame.index)

    if cfg.get("min_edge_ratio") is not None:
        mask &= pd.to_numeric(frame.get("edge_ratio"), errors="coerce") >= float(cfg["min_edge_ratio"])
    if cfg.get("max_ask") is not None:
        mask &= pd.to_numeric(frame.get("ask"), errors="coerce") <= float(cfg["max_ask"])
    if cfg.get("min_expected_overpay_mult") is not None:
        mask &= pd.to_numeric(frame.get("expected_overpay_mult"), errors="coerce") >= float(cfg["min_expected_overpay_mult"])
    if cfg.get("min_overpay_gap_mult") is not None:
        mask &= pd.to_numeric(frame.get("overpay_gap_mult"), errors="coerce") >= float(cfg["min_overpay_gap_mult"])
    if cfg.get("min_overall_confidence") is not None:
        mask &= pd.to_numeric(frame.get("overall_confidence"), errors="coerce") >= float(cfg["min_overall_confidence"])

    item_substring = str(cfg.get("item_substring") or "").strip().lower()
    if item_substring:
        mask &= frame["market_hash_name"].fillna("").astype(str).str.lower().str.contains(item_substring, regex=False)

    out = frame[mask.fillna(False)].copy()
    sort_cols = [col for col in ["edge_ratio", "edge_abs", "ask"] if col in out.columns]
    if sort_cols:
        out = out.sort_values(sort_cols, ascending=[False, False, True][: len(sort_cols)], na_position="last")
    return out.to_dict(orient="records")


def process_target(
    *,
    target: dict[str, Any],
    items: list[str],
    client: SteamClient,
    filters_cfg: dict[str, Any],
    latest_matches_csv: Path,
    latest_opportunities_csv: Path,
    progress_csv: Path,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    query_name = str(target["query_name"])
    matched_items = match_items_for_query(items, query_name)
    all_seed_rows: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []

    print(f"\n=== target: {query_name} | exact items: {len(matched_items)} ===", flush=True)
    if not matched_items:
        append_progress(
            progress_csv,
            {
                "query_name": query_name,
                "matched_items": 0,
                "seed_rows": 0,
                "opportunities": 0,
                "status": "no_items_matched_query",
            },
        )
        return [], [], [{"query_name": query_name, "error": "no_items_matched_query"}]

    for index, exact_item in enumerate(matched_items, start=1):
        label = f"{query_name} :: {index}/{len(matched_items)} {exact_item}"
        try:
            fetched_rows, meta = client.fetch_top_listings(exact_item, label=label)
        except Exception as exc:
            errors.append({"query_name": query_name, "market_hash_name": exact_item, "error": str(exc)})
            fetched_rows = []
            meta = {"success": False, "note": str(exc), "total_count": None}

        if not fetched_rows and meta.get("note") != "no_offers":
            errors.append({"query_name": query_name, "market_hash_name": exact_item, "meta": meta})

        seed_rows = compute_seed_rows(target=target, exact_item=exact_item, fetched_rows=fetched_rows, meta=meta)
        all_seed_rows.extend(seed_rows)
        if index < len(matched_items):
            client.sleep_between_items()

    opportunities = apply_filters(all_seed_rows, filters_cfg)
    append_rows(latest_matches_csv, all_seed_rows)
    append_rows(latest_opportunities_csv, opportunities)
    append_progress(
        progress_csv,
        {
            "query_name": query_name,
            "matched_items": len(matched_items),
            "seed_rows": len(all_seed_rows),
            "opportunities": len(opportunities),
            "errors": len(errors),
            "status": "completed",
        },
    )
    print(
        f"target done: {query_name} seed_rows={len(all_seed_rows)} "
        f"opportunities={len(opportunities)} errors={len(errors)}",
        flush=True,
    )
    return all_seed_rows, opportunities, errors


def send_alerts_for_target(
    *,
    opportunities: list[dict[str, Any]],
    state: dict[str, Any],
    state_path: Path,
    telegram_cfg: dict[str, Any],
    mode: str,
) -> tuple[dict[str, Any], int, int]:
    if mode == "off" or not opportunities:
        return state, 0, 0

    dry_run = mode == "dry-run"
    token = chat = None
    if not dry_run:
        token, chat = telegram_credentials()

    cooldown_hours = float(telegram_cfg.get("cooldown_hours", 24.0))
    sleep_sec = float(telegram_cfg.get("sleep_sec", 0.6))
    max_alerts = telegram_cfg.get("max_alerts_per_target")
    max_alerts = None if max_alerts is None else int(max_alerts)
    sent = 0
    skipped = 0

    for row in opportunities:
        key = alert_key(row)
        if not should_send_alert(state, key, cooldown_hours):
            skipped += 1
            continue
        message = format_alert(row)
        if dry_run:
            print("=" * 72)
            print(message)
        else:
            assert token is not None and chat is not None
            send_message(
                message,
                bot_token=token,
                chat_id=chat,
                disable_web_page_preview=_parse_bool(telegram_cfg.get("disable_web_page_preview", True)),
            )
        state = mark_alert_sent(state, key, row)
        save_state(state_path, state)
        sent += 1
        if max_alerts is not None and sent >= max_alerts:
            break
        if not dry_run:
            maybe_sleep(sleep_sec)
    return state, sent, skipped


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    root = repo_root()
    parser = argparse.ArgumentParser(description="Long-running seed opportunity monitor.")
    parser.add_argument("--config", type=Path, default=root / "monitoring" / "config.json")
    parser.add_argument("--duration-minutes", type=float, default=None)
    parser.add_argument("--batch-size", type=int, default=None)
    parser.add_argument("--max-batches", type=int, default=None)
    parser.add_argument("--max-cycles", type=int, default=None)
    parser.add_argument("--query-substring", type=str, default=None)
    parser.add_argument("--limit", type=int, default=None, help="Override Steam /render/ page size.")
    parser.add_argument("--max-listings-per-item", type=int, default=None, help="Override listings fetched per exact Steam item.")
    parser.add_argument("--no-delays", action="store_true", help="Disable Steam sleeps for local smoke tests.")
    parser.add_argument(
        "--telegram-mode",
        choices=("real", "dry-run", "off"),
        default=None,
        help="Override config telegram.enabled. real sends messages, dry-run prints, off disables.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print plan without fetching Steam.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    configure_stdio()
    args = parse_args(argv)
    cfg = load_config(args.config.resolve() if args.config else None)
    monitoring_cfg = cfg.get("monitoring", {})
    steam_cfg = cfg.get("steam", {})
    filters_cfg = cfg.get("filters", {})
    telegram_cfg = cfg.get("telegram", {})

    target_csv = path_from_config(cfg, "target_csv")
    items_py = path_from_config(cfg, "items_py")
    state_path = path_from_config(cfg, "state_json")
    latest_matches_csv = path_from_config(cfg, "latest_matches_csv")
    latest_opportunities_csv = path_from_config(cfg, "latest_opportunities_csv")
    progress_csv = path_from_config(cfg, "progress_csv")

    duration_minutes = float(args.duration_minutes if args.duration_minutes is not None else monitoring_cfg.get("duration_minutes", 300))
    batch_size = int(args.batch_size if args.batch_size is not None else monitoring_cfg.get("target_batch_size", 1))
    max_batches = args.max_batches if args.max_batches is not None else monitoring_cfg.get("max_batches")
    max_cycles = args.max_cycles if args.max_cycles is not None else monitoring_cfg.get("max_cycles")
    query_substring = str(args.query_substring if args.query_substring is not None else monitoring_cfg.get("query_substring", "")).strip()
    if args.limit is not None:
        steam_cfg["limit"] = args.limit
    if args.max_listings_per_item is not None:
        steam_cfg["max_listings_per_item"] = args.max_listings_per_item
    if args.no_delays:
        for key in (
            "delay_between_items_min_sec",
            "delay_between_items_max_sec",
            "delay_between_pages_min_sec",
            "delay_between_pages_max_sec",
            "retry_sleep_min_sec",
            "retry_sleep_max_sec",
        ):
            steam_cfg[key] = 0.0

    targets = load_targets(target_csv)
    if query_substring:
        needle = query_substring.lower()
        targets = [target for target in targets if needle in str(target["query_name"]).lower()]
    items = load_items_from_py(items_py)
    if not targets:
        raise RuntimeError(f"No targets loaded from {target_csv}")
    if not items:
        raise RuntimeError(f"No ITEMS loaded from {items_py}")

    if _parse_bool(monitoring_cfg.get("clear_latest_outputs_on_start", True)) and not args.dry_run:
        reset_latest_outputs([latest_matches_csv, latest_opportunities_csv, progress_csv])

    if args.telegram_mode:
        telegram_mode = args.telegram_mode
    elif _parse_bool(telegram_cfg.get("enabled", False)):
        telegram_mode = "real"
    else:
        telegram_mode = "off"

    print(f"config: {args.config.resolve()}")
    print(f"target csv: {target_csv} rows={len(targets)}")
    print(f"items py: {items_py} items={len(items)}")
    print(f"duration: {duration_minutes:.1f} minutes")
    print(f"target batch size: {batch_size}")
    print(f"telegram mode: {telegram_mode}")
    print(f"filter min_edge_ratio: {filters_cfg.get('min_edge_ratio')}")
    print(f"state: {state_path}")
    print(f"dry run: {args.dry_run}")

    if telegram_mode == "real" and not args.dry_run:
        telegram_credentials()

    if args.dry_run:
        state = load_state(state_path, targets)
        batch, start, next_pointer, full_cycle_done = select_targets(targets, state, batch_size)
        print(f"next batch pointer: start={start} next={next_pointer} full_cycle_done={full_cycle_done}")
        for index, target in enumerate(batch, start=1):
            matched = match_items_for_query(items, str(target["query_name"]))
            print(f"  {index}. {target['query_name']} -> {len(matched)} exact items")
        return 0

    state = load_state(state_path, targets)
    state = mark_run_started(state)
    save_state(state_path, state)

    client = SteamClient(steam_cfg)
    started = time.monotonic()
    batches_run = 0
    cycles_done = 0
    total_alerts = 0

    try:
        while True:
            elapsed = (time.monotonic() - started) / 60.0
            if elapsed >= duration_minutes:
                print(f"duration reached before next target: {elapsed:.1f}m")
                break
            if max_batches is not None and batches_run >= int(max_batches):
                print(f"max batches reached: {batches_run}")
                break
            if max_cycles is not None and cycles_done >= int(max_cycles):
                print(f"max cycles reached: {cycles_done}")
                break

            state = load_state(state_path, targets)
            batch, start, next_pointer, batch_full_cycle_done = select_targets(targets, state, batch_size)
            if not batch:
                break
            print(
                f"\n=== monitor batch {batches_run + 1} "
                f"start_pointer={start} next_pointer={next_pointer} ===",
                flush=True,
            )

            for offset, target in enumerate(batch):
                target_index = (start + offset) % len(targets)
                target_next = (target_index + 1) % len(targets)
                target_full_cycle_done = target_next == 0
                rows, opportunities, errors = process_target(
                    target=target,
                    items=items,
                    client=client,
                    filters_cfg=filters_cfg,
                    latest_matches_csv=latest_matches_csv,
                    latest_opportunities_csv=latest_opportunities_csv,
                    progress_csv=progress_csv,
                )
                state = load_state(state_path, targets)
                state, alerts_sent, alerts_skipped = send_alerts_for_target(
                    opportunities=opportunities,
                    state=state,
                    state_path=state_path,
                    telegram_cfg=telegram_cfg,
                    mode=telegram_mode,
                )
                total_alerts += alerts_sent
                status = "ok_with_errors" if errors else "ok"
                state = mark_target_done(
                    state,
                    target=target,
                    next_pointer=target_next,
                    full_cycle_done=target_full_cycle_done,
                    status=status,
                    rows_seen=len(rows),
                    opportunities=len(opportunities),
                    alerts_sent=alerts_sent,
                    error=None if not errors else f"{len(errors)} item errors",
                )
                save_state(state_path, state)
                print(f"alerts: sent={alerts_sent} skipped={alerts_skipped}", flush=True)
                if target_full_cycle_done:
                    cycles_done += 1

                elapsed = (time.monotonic() - started) / 60.0
                if elapsed >= duration_minutes:
                    print(f"duration reached after target: {elapsed:.1f}m")
                    break

            batches_run += 1
            if batch_full_cycle_done and float(monitoring_cfg.get("cycle_sleep_sec", 0.0) or 0.0) > 0:
                sleep_sec = float(monitoring_cfg.get("cycle_sleep_sec") or 0.0)
                if (time.monotonic() - started + sleep_sec) / 60.0 < duration_minutes:
                    print(f"sleeping after full cycle: {sleep_sec:.1f}s", flush=True)
                    time.sleep(sleep_sec)

        state = load_state(state_path, targets)
        state = mark_run_finished(state, status="ok")
        save_state(state_path, state)
        print(
            f"monitoring completed at {datetime.now(timezone.utc).isoformat()} "
            f"batches={batches_run} cycles={cycles_done} alerts={total_alerts}"
        )
        return 0
    except Exception as exc:
        state = load_state(state_path, targets)
        state = mark_run_finished(state, status="error", error=str(exc))
        save_state(state_path, state)
        print(f"monitoring failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
