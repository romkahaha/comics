"""
Batch scan Steam SCM listings for many query groups from seed_target_table.csv.

For each query_name:
  - match all market_hash_name values that contain the query substring
  - fetch Steam listings once per exact market_hash_name
  - keep only listings whose paint_seed is present in the target table
  - compute base ask from the first 10 asks of that exact market_hash_name
  - compute market overpay vs base and compare it with expected seed overpay
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import random
import sys
import time
from pathlib import Path
from typing import Any
from datetime import datetime, UTC

import pandas as pd


SCRIPT_DIR = Path(__file__).resolve().parent
PARENT_DIR = SCRIPT_DIR.parent
REPO_ROOT = PARENT_DIR.parent
DEFAULT_RUNTIME_JSON = SCRIPT_DIR / "seed_batch_scan_runtime.json"
DEFAULT_ITEMS_PY = REPO_ROOT / "lists" / "screening_super_full.py"
DEFAULT_TARGET_CSV = PARENT_DIR / "data" / "seed_target_table.csv"
DEFAULT_OUT_CSV = SCRIPT_DIR / "data" / "seed_batch_scan_matches.csv"


def _runtime_path() -> Path:
    raw = os.environ.get("SEED_BATCH_SCAN_RUNTIME_CONFIG")
    if raw:
        return Path(raw).expanduser().resolve()
    return DEFAULT_RUNTIME_JSON


def _load_runtime() -> dict[str, Any]:
    path = _runtime_path()
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def _rt_str(cfg: dict[str, Any], key: str, default: str) -> str:
    value = cfg.get(key, default)
    text = str(value).strip()
    return text or default


def _rt_int(cfg: dict[str, Any], key: str, default: int | None) -> int | None:
    value = cfg.get(key, default)
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _rt_bool(cfg: dict[str, Any], key: str, default: bool) -> bool:
    value = cfg.get(key, default)
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return bool(default)


def _parse_bool_arg(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    raise argparse.ArgumentTypeError(f"invalid bool value: {value!r}")


def _resolve_path(raw: str | os.PathLike[str], base: Path) -> Path:
    path = Path(raw).expanduser()
    if not path.is_absolute():
        path = (base / path).resolve()
    return path


def _load_items_from_py(path: Path) -> list[str]:
    spec = importlib.util.spec_from_file_location(f"seed_batch_scan_items_{path.stem}", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot import {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    items = getattr(module, "ITEMS", None)
    if not isinstance(items, list):
        raise ValueError(f"{path} must define ITEMS = [...]")
    return [str(x) for x in items]


def _import_steam_scm_listings():
    module_path = REPO_ROOT / "steam_listings" / "steam_scm_listings.py"
    spec = importlib.util.spec_from_file_location("seed_batch_scan_steam_scm", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot import {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _safe_float(value: Any) -> float | None:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if out <= 0:
        return None
    return out


def _parse_seed_map(raw: Any) -> dict[int, float]:
    if isinstance(raw, dict):
        data = raw
    else:
        text = str(raw).strip()
        if not text:
            return {}
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            return {}
    out: dict[int, float] = {}
    for key, value in data.items():
        try:
            seed = int(key)
            mult = float(value)
        except (TypeError, ValueError):
            continue
        out[seed] = mult
    return out


def _load_targets(target_csv: Path) -> list[dict[str, Any]]:
    df = pd.read_csv(target_csv)
    targets: list[dict[str, Any]] = []
    for rec in df.to_dict(orient="records"):
        query_name = str(rec.get("query_name") or "").strip()
        if not query_name:
            continue
        targets.append(
            {
                "query_name": query_name,
                "seed_to_overpay": _parse_seed_map(rec.get("seed_to_overpay_json")),
                "seed_to_confidence": _parse_seed_map(rec.get("seed_to_confidence_json")),
                "overall_confidence": float(rec.get("overall_confidence") or 0.0),
                "pattern_families": str(rec.get("pattern_families") or ""),
                "notes": str(rec.get("notes") or ""),
            }
        )
    return targets


def _progress_csv_path(out_csv: Path) -> Path:
    return out_csv.with_name(f"{out_csv.stem}_progress.csv")


def _prepare_output_paths(out_csv: Path, write_mode: str) -> Path:
    progress_csv = _progress_csv_path(out_csv)
    mode = str(write_mode).strip().lower()
    if mode == "create":
        if out_csv.exists():
            out_csv.unlink()
        if progress_csv.exists():
            progress_csv.unlink()
    return progress_csv


def _load_completed_queries(out_csv: Path, progress_csv: Path, skip_completed_queries: bool) -> set[str]:
    if not skip_completed_queries:
        return set()

    completed: set[str] = set()
    if out_csv.exists():
        try:
            use_cols = ["query_name"]
            existing = pd.read_csv(out_csv, usecols=use_cols)
            completed.update(
                str(x).strip()
                for x in existing["query_name"].dropna().astype(str).tolist()
                if str(x).strip()
            )
        except Exception:
            pass
    if progress_csv.exists():
        try:
            progress = pd.read_csv(progress_csv)
            if "query_name" in progress.columns:
                completed.update(
                    str(x).strip()
                    for x in progress["query_name"].dropna().astype(str).tolist()
                    if str(x).strip()
                )
        except Exception:
            pass
    return completed


def _append_output_rows(out_csv: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    df = pd.DataFrame(rows)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    write_header = not out_csv.exists()
    df.to_csv(out_csv, mode="a", header=write_header, index=False)


def _append_query_progress(
    progress_csv: Path,
    *,
    query_name: str,
    matched_items: int,
    rows_written: int,
    status: str,
) -> None:
    progress_csv.parent.mkdir(parents=True, exist_ok=True)
    row = pd.DataFrame(
        [
            {
                "query_name": query_name,
                "matched_items": matched_items,
                "rows_written": rows_written,
                "status": status,
                "completed_at_utc": datetime.now(UTC).isoformat(),
            }
        ]
    )
    write_header = not progress_csv.exists()
    row.to_csv(progress_csv, mode="a", header=write_header, index=False)


def _sleep_between_items(scm: Any, *, enabled: bool) -> None:
    if not enabled:
        return
    try:
        d_lo = float(scm._effective("delay_between_skins_min_sec"))
        d_hi = float(scm._effective("delay_between_skins_max_sec"))
    except Exception:
        return
    wait_s = random.uniform(d_lo, d_hi)
    try:
        scm._batch_log(
            f"  [seed_batch_scan] pause between items {wait_s:.1f}s (delay_between_skins_*)"
        )
    except Exception:
        pass
    time.sleep(wait_s)


def _match_items_for_query(items: list[str], query_name: str) -> list[str]:
    needle = query_name.lower().strip()
    if not needle:
        return []
    out: list[str] = []
    for item in items:
        hay = item.lower().strip()
        candidates = [hay]
        if hay.startswith("stattrak"):
            candidates.append(hay[len("stattrak") :].lstrip("™? ").strip())
        if hay.startswith("souvenir "):
            candidates.append(hay[len("souvenir ") :].strip())
        if any(candidate.startswith(needle) for candidate in candidates):
            out.append(item)
    return out


def build_embedded_steam_runtime(cfg: dict[str, Any], base_dir: Path) -> Path:
    payload: dict[str, Any] = {}
    mapping = {
        "steam_currency": "steam_currency",
        "request_timeout_sec": "request_timeout_sec",
        "retry_attempts": "retry_attempts",
        "retry_sleep_min_sec": "retry_sleep_min_sec",
        "retry_sleep_max_sec": "retry_sleep_max_sec",
        "delay_between_skins_min_sec": "delay_between_skins_min_sec",
        "delay_between_skins_max_sec": "delay_between_skins_max_sec",
        "delay_between_render_pages_min_sec": "delay_between_render_pages_min_sec",
        "delay_between_render_pages_max_sec": "delay_between_render_pages_max_sec",
        "batch_log_progress": "batch_log_progress",
        "float_decimal_places": "float_decimal_places",
        "include_asset_properties_json": "include_asset_properties_json",
    }
    for dst_key, src_key in mapping.items():
        if src_key in cfg:
            payload[dst_key] = cfg[src_key]
    path = base_dir / "_embedded_steam_runtime.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def run_seed_batch_scan(
    *,
    target_csv: Path,
    items_py: Path,
    out_csv: Path,
    limit: int | None,
    max_listings: int | None,
    steam_runtime_path: Path | None,
    query_substring: str | None = None,
    max_queries: int | None = None,
    write_mode: str = "create",
    skip_completed_queries: bool = False,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    if steam_runtime_path is not None:
        os.environ["STEAM_SCM_RUNTIME_CONFIG"] = str(steam_runtime_path)

    progress_csv = _prepare_output_paths(out_csv, write_mode)

    scm = _import_steam_scm_listings()
    items = _load_items_from_py(items_py)
    targets = _load_targets(target_csv)

    if query_substring:
        needle = query_substring.lower().strip()
        targets = [target for target in targets if needle in target["query_name"].lower()]
    if max_queries is not None and max_queries > 0:
        targets = targets[:max_queries]

    completed_queries = _load_completed_queries(out_csv, progress_csv, skip_completed_queries)
    if completed_queries:
        targets = [target for target in targets if target["query_name"] not in completed_queries]

    session = scm._session()
    total_queries = len(targets)
    errors: list[dict[str, Any]] = []
    cache: dict[str, tuple[list[dict[str, Any]], dict[str, Any], float | None, int]] = {}
    matched_item_total = 0
    rows_saved_total = 0

    for query_idx, target in enumerate(targets, start=1):
        query_name = target["query_name"]
        seed_to_overpay = target["seed_to_overpay"]
        seed_to_confidence = target["seed_to_confidence"]
        matched_items = _match_items_for_query(items, query_name)
        matched_item_total += len(matched_items)
        query_rows: list[dict[str, Any]] = []

        if not matched_items:
            errors.append({"query_name": query_name, "error": "no_items_matched_query"})
            _append_query_progress(
                progress_csv,
                query_name=query_name,
                matched_items=0,
                rows_written=0,
                status="no_items_matched_query",
            )
            continue

        for item_idx, item in enumerate(matched_items, start=1):
            if item in cache:
                fetched_rows, meta, base_ask_mean_10, base_ask_n = cache[item]
            else:
                fetched_rows, meta = scm.fetch_steam_scm_top_listings(
                    item,
                    limit=limit,
                    max_listings=max_listings,
                    session=session,
                    log_skin_label=f"{query_idx}/{total_queries} {query_name} :: {item_idx}/{len(matched_items)} {item}",
                )
                base_ask_samples: list[float] = []
                for fetched in fetched_rows:
                    ask_value = _safe_float(fetched.get("ask"))
                    if ask_value is None:
                        continue
                    base_ask_samples.append(ask_value)
                    if len(base_ask_samples) >= 10:
                        break
                base_ask_mean_10 = (
                    sum(base_ask_samples) / len(base_ask_samples) if base_ask_samples else None
                )
                base_ask_n = len(base_ask_samples)
                cache[item] = (fetched_rows, meta, base_ask_mean_10, base_ask_n)
                has_more_items = item_idx < len(matched_items)
                has_more_queries = query_idx < total_queries
                _sleep_between_items(scm, enabled=has_more_items or has_more_queries)

            if not fetched_rows and meta.get("note") != "no_offers":
                errors.append({"query_name": query_name, "market_hash_name": item, "meta": meta})

            total_count = meta.get("total_count")
            for fetched in fetched_rows:
                seed_raw = fetched.get("paint_seed")
                try:
                    paint_seed = int(seed_raw)
                except (TypeError, ValueError):
                    continue
                expected_overpay_mult = seed_to_overpay.get(paint_seed)
                if expected_overpay_mult is None:
                    continue

                ask = _safe_float(fetched.get("ask"))
                ask_seller_net = _safe_float(fetched.get("ask_seller_net"))
                market_overpay_mult = (
                    ask / base_ask_mean_10 if ask is not None and base_ask_mean_10 not in (None, 0) else None
                )
                market_overpay_pct = (
                    market_overpay_mult - 1.0 if market_overpay_mult is not None else None
                )
                expected_price = (
                    base_ask_mean_10 * expected_overpay_mult
                    if base_ask_mean_10 not in (None, 0)
                    else None
                )
                expected_overpay_pct = expected_overpay_mult - 1.0
                overpay_gap_mult = (
                    expected_overpay_mult - market_overpay_mult
                    if market_overpay_mult is not None
                    else None
                )
                overpay_gap_pct = (
                    expected_overpay_pct - market_overpay_pct
                    if market_overpay_pct is not None
                    else None
                )
                edge_abs = (
                    expected_price - ask
                    if expected_price is not None and ask is not None
                    else None
                )
                edge_ratio = (
                    expected_overpay_mult / market_overpay_mult - 1.0
                    if market_overpay_mult not in (None, 0)
                    else None
                )

                query_rows.append(
                    {
                        "query_name": query_name,
                        "market_hash_name": item,
                        "pattern_families": target["pattern_families"],
                        "paint_seed": paint_seed,
                        "seed_confidence": seed_to_confidence.get(paint_seed),
                        "overall_confidence": target["overall_confidence"],
                        "ask": ask,
                        "ask_seller_net": ask_seller_net,
                        "base_ask_mean_10": base_ask_mean_10,
                        "base_ask_n": base_ask_n,
                        "market_overpay_mult": market_overpay_mult,
                        "market_overpay_pct": market_overpay_pct,
                        "expected_overpay_mult": expected_overpay_mult,
                        "expected_overpay_pct": expected_overpay_pct,
                        "overpay_gap_mult": overpay_gap_mult,
                        "overpay_gap_pct": overpay_gap_pct,
                        "expected_price": expected_price,
                        "edge_abs": edge_abs,
                        "edge_ratio": edge_ratio,
                        "float_value": fetched.get("float_value"),
                        "listing_id": fetched.get("listing_id"),
                        "asset_id": fetched.get("asset_id"),
                        "scm_total_listings": total_count,
                        "converted_price": fetched.get("converted_price"),
                        "converted_fee": fetched.get("converted_fee"),
                        "converted_currencyid": fetched.get("converted_currencyid"),
                        "asset_properties_json": fetched.get("asset_properties_json"),
                        "target_notes": target["notes"],
                    }
                )

        _append_output_rows(out_csv, query_rows)
        rows_saved_total += len(query_rows)
        _append_query_progress(
            progress_csv,
            query_name=query_name,
            matched_items=len(matched_items),
            rows_written=len(query_rows),
            status="completed",
        )

    if out_csv.exists():
        df = pd.read_csv(out_csv)
        if not df.empty:
            sort_cols = [col for col in ["overpay_gap_mult", "edge_abs", "ask"] if col in df.columns]
            ascending = [False, False, True][: len(sort_cols)]
            df = df.sort_values(sort_cols, ascending=ascending, na_position="last").reset_index(drop=True)
    else:
        df = pd.DataFrame()

    meta = {
        "targets_total": len(targets),
        "queries_skipped": len(completed_queries),
        "matched_item_total": matched_item_total,
        "unique_items_fetched": len(cache),
        "rows_saved": len(df),
        "rows_saved_new": rows_saved_total,
        "errors": len(errors),
        "out_csv": str(out_csv),
        "target_csv": str(target_csv),
        "progress_csv": str(progress_csv),
    }
    return df, meta


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    cfg = _load_runtime()
    parser = argparse.ArgumentParser(description="Batch scan Steam listings from seed_target_table.csv")
    parser.add_argument(
        "--target-csv",
        default=_rt_str(cfg, "target_csv", str(DEFAULT_TARGET_CSV)),
        help="Grouped target table CSV",
    )
    parser.add_argument(
        "--items-py",
        default=_rt_str(cfg, "items_py", str(DEFAULT_ITEMS_PY)),
        help="Python file with ITEMS = [...]",
    )
    parser.add_argument(
        "--out",
        default=_rt_str(cfg, "out_csv", str(DEFAULT_OUT_CSV)),
        help="Output CSV path",
    )
    parser.add_argument(
        "--query-substring",
        default=str(cfg.get("query_substring", "")).strip(),
        help="Optional substring filter for query_name values from target_csv",
    )
    parser.add_argument(
        "--write-mode",
        default=_rt_str(cfg, "write_mode", "create"),
        choices=("create", "merge"),
        help="create = reset output CSV and start fresh, merge = append new completed queries",
    )
    parser.add_argument(
        "--skip-completed-queries",
        type=_parse_bool_arg,
        default=_rt_bool(cfg, "skip_completed_queries", True),
        help="When merge mode is used, skip queries already present in output/progress",
    )
    parser.add_argument("--max-queries", type=int, default=_rt_int(cfg, "max_queries", None))
    parser.add_argument("--limit", type=int, default=_rt_int(cfg, "limit", None))
    parser.add_argument("--max-listings", type=int, default=_rt_int(cfg, "max_listings", None))
    parser.add_argument(
        "--steam-runtime",
        default=str(cfg.get("steam_runtime", "")).strip(),
        help="Optional path to steam_scm_runtime.json",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    cfg = _load_runtime()
    target_csv = _resolve_path(args.target_csv, Path.cwd())
    items_py = _resolve_path(args.items_py, Path.cwd())
    out_csv = _resolve_path(args.out, Path.cwd())
    steam_runtime = str(args.steam_runtime).strip()
    if steam_runtime:
        steam_runtime_path = _resolve_path(steam_runtime, Path.cwd())
    else:
        steam_runtime_path = build_embedded_steam_runtime(cfg, SCRIPT_DIR)

    df, meta = run_seed_batch_scan(
        target_csv=target_csv,
        items_py=items_py,
        out_csv=out_csv,
        limit=args.limit,
        max_listings=args.max_listings,
        steam_runtime_path=steam_runtime_path,
        query_substring=str(args.query_substring).strip() or None,
        max_queries=args.max_queries,
        write_mode=str(args.write_mode),
        skip_completed_queries=bool(args.skip_completed_queries),
    )
    print(
        f"targets_total={meta['targets_total']}  queries_skipped={meta['queries_skipped']}  "
        f"matched_item_total={meta['matched_item_total']}  unique_items_fetched={meta['unique_items_fetched']}  "
        f"rows_saved={meta['rows_saved']}  rows_saved_new={meta['rows_saved_new']}  "
        f"errors={meta['errors']}  out={meta['out_csv']}"
    )
    if not df.empty:
        cols = [
            col
            for col in [
                "market_hash_name",
                "paint_seed",
                "ask",
                "base_ask_mean_10",
                "market_overpay_mult",
                "expected_overpay_mult",
                "overpay_gap_mult",
                "edge_abs",
            ]
            if col in df.columns
        ]
        print(df[cols].head(15).to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
