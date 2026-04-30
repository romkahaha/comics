from __future__ import annotations

import os
import random
import time
import urllib.parse
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

import requests


APP_ID = 730
CONTEXT_ID = "2"
FLOAT_PROPERTY_ID = 2
PATTERN_PROPERTY_ID = 1
PATTERN_NAME_HINTS = ("pattern", "template", "seed")
DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)


class SteamClient:
    def __init__(self, cfg: dict[str, Any]):
        self.cfg = dict(cfg)
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": DEFAULT_UA,
                "Accept-Language": "en-US,en;q=0.9",
            }
        )
        cookies = str(self.cfg.get("steam_cookies") or os.environ.get("STEAM_COOKIES") or "").strip()
        if cookies:
            self.session.headers["Cookie"] = cookies

    def _bool(self, key: str, default: bool = False) -> bool:
        value = self.cfg.get(key, default)
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}

    def log(self, message: str) -> None:
        if self._bool("log_progress", True):
            print(message, flush=True)

    def _sleep_range(self, lo_key: str, hi_key: str) -> None:
        lo = float(self.cfg.get(lo_key, 0.0) or 0.0)
        hi = float(self.cfg.get(hi_key, lo) or lo)
        wait = random.uniform(min(lo, hi), max(lo, hi))
        if wait > 0:
            time.sleep(wait)

    def sleep_between_items(self) -> None:
        self._sleep_range("delay_between_items_min_sec", "delay_between_items_max_sec")

    def _listing_path(self, market_hash_name: str) -> str:
        segment = urllib.parse.quote(market_hash_name, safe="")
        return f"https://steamcommunity.com/market/listings/{APP_ID}/{segment}/render/"

    def fetch_render_raw(self, market_hash_name: str, *, start: int, count: int) -> dict[str, Any]:
        currency = int(self.cfg.get("currency", 3))
        timeout = float(self.cfg.get("request_timeout_sec", 45.0))
        segment = urllib.parse.quote(market_hash_name, safe="")
        self.session.headers["Referer"] = f"https://steamcommunity.com/market/listings/{APP_ID}/{segment}"
        response = self.session.get(
            self._listing_path(market_hash_name),
            params={
                "query": "",
                "start": int(start),
                "count": min(int(count), 100),
                "currency": currency,
                "language": "english",
                "format": "json",
            },
            timeout=timeout,
        )
        response.raise_for_status()
        return response.json()

    def fetch_top_listings(self, market_hash_name: str, *, label: str | None = None) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        chunk = max(1, min(int(self.cfg.get("limit", 100) or 100), 100))
        max_listings = self.cfg.get("max_listings_per_item", chunk)
        total_cap = chunk if max_listings is None else max(1, int(max_listings))
        attempts = max(1, int(self.cfg.get("retry_attempts", 2) or 2))
        log_label = label or market_hash_name

        merged: list[dict[str, Any]] = []
        seen_ids: set[str] = set()
        start = 0
        meta: dict[str, Any] = {
            "success": False,
            "note": None,
            "total_count": None,
            "pages_fetched": 0,
            "listings_target_cap": total_cap,
        }

        while len(merged) < total_cap:
            need = min(chunk, 100, total_cap - len(merged))
            data: dict[str, Any] | None = None
            last_error: str | None = None
            for attempt in range(attempts):
                try:
                    self.log(
                        f'  [steam] "{log_label}": render start={start} count={need} '
                        f"got={len(merged)}/{total_cap}"
                    )
                    data = self.fetch_render_raw(market_hash_name, start=start, count=need)
                except requests.RequestException as exc:
                    last_error = str(exc)
                    if attempt + 1 < attempts:
                        wait = random.uniform(
                            float(self.cfg.get("retry_sleep_min_sec", 2.0) or 2.0),
                            float(self.cfg.get("retry_sleep_max_sec", 7.0) or 7.0),
                        )
                        self.log(f'  [steam] "{log_label}": retry after {wait:.1f}s: {last_error}')
                        time.sleep(wait)
                    continue
                if not data.get("success"):
                    last_error = "success=false"
                    if attempt + 1 < attempts:
                        wait = random.uniform(
                            float(self.cfg.get("retry_sleep_min_sec", 2.0) or 2.0),
                            float(self.cfg.get("retry_sleep_max_sec", 7.0) or 7.0),
                        )
                        self.log(f'  [steam] "{log_label}": retry after {wait:.1f}s: {last_error}')
                        time.sleep(wait)
                    continue
                break
            else:
                meta["note"] = last_error or "failed"
                meta["partial_fetch"] = bool(merged)
                if merged:
                    break
                return [], meta

            assert data is not None
            meta["success"] = True
            meta["total_count"] = data.get("total_count")
            listing_pairs = _iter_listings(data.get("listinginfo"))
            if not listing_pairs:
                meta["note"] = "no_offers" if int(data.get("total_count") or 0) == 0 else "empty_listinginfo"
                break

            page_rows = parse_render_payload(data)
            meta["pages_fetched"] = int(meta.get("pages_fetched") or 0) + 1
            for row in page_rows:
                listing_id = str(row.get("listing_id") or "")
                if not listing_id or listing_id in seen_ids:
                    continue
                seen_ids.add(listing_id)
                merged.append(row)
                if len(merged) >= total_cap:
                    break

            if not page_rows or len(page_rows) < need or len(merged) >= total_cap:
                break
            start += len(page_rows)
            self._sleep_range("delay_between_pages_min_sec", "delay_between_pages_max_sec")

        merged.sort(key=lambda row: (row.get("ask") is None, float(row.get("ask") or 0.0)))
        return merged[:total_cap], meta


def steam_item_url(market_hash_name: str) -> str:
    return "https://steamcommunity.com/market/listings/730/" + urllib.parse.quote(market_hash_name, safe="")


def _asset_map(assets_blob: Any) -> dict[str, dict[str, Any]]:
    if not isinstance(assets_blob, dict):
        return {}
    app = assets_blob.get(str(APP_ID))
    if not isinstance(app, dict):
        return {}
    ctx = app.get(CONTEXT_ID)
    if not isinstance(ctx, dict):
        return {}
    return {str(asset_id): asset for asset_id, asset in ctx.items() if isinstance(asset, dict)}


def _iter_listings(listinginfo: Any) -> list[tuple[str, dict[str, Any]]]:
    if isinstance(listinginfo, dict):
        return [(str(key), value) for key, value in listinginfo.items() if isinstance(value, dict)]
    if isinstance(listinginfo, list):
        out: list[tuple[str, dict[str, Any]]] = []
        for value in listinginfo:
            if isinstance(value, dict) and value.get("listingid") is not None:
                out.append((str(value["listingid"]), value))
        return out
    return []


def _property_id(prop: dict[str, Any]) -> int | None:
    raw = prop.get("propertyid")
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def _paint_seed_from_asset(asset: dict[str, Any]) -> int | None:
    props = asset.get("asset_properties") or []
    for prop in props:
        if not isinstance(prop, dict):
            continue
        name = str(prop.get("name") or "").lower()
        if not any(hint in name for hint in PATTERN_NAME_HINTS):
            continue
        if any(word in name for word in ("sticker", "patch", "keychain")):
            continue
        seed = _as_int(prop.get("int_value"))
        if seed is not None:
            return seed

    for prop in props:
        if not isinstance(prop, dict) or _property_id(prop) != PATTERN_PROPERTY_ID:
            continue
        seed = _as_int(prop.get("int_value") if prop.get("int_value") not in (None, "") else prop.get("string_value"))
        if seed is not None:
            return seed
    return None


def _float_from_asset(asset: dict[str, Any]) -> float | None:
    for prop in asset.get("asset_properties") or []:
        if not isinstance(prop, dict) or _property_id(prop) != FLOAT_PROPERTY_ID:
            continue
        value = prop.get("float_value")
        if value in (None, ""):
            return None
        try:
            quantized = Decimal(str(value).strip()).quantize(Decimal("1e-14"), rounding=ROUND_HALF_UP)
            return float(quantized)
        except Exception:
            try:
                return float(value)
            except (TypeError, ValueError):
                return None
    return None


def _as_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(str(value).replace(",", ".").strip()))
        except (TypeError, ValueError):
            return None


def _major_units(minor_total: int, converted_currencyid: Any) -> float | None:
    try:
        currency_id = int(converted_currencyid)
    except (TypeError, ValueError):
        return None
    if 2001 <= currency_id <= 2010:
        return round(minor_total / 100.0, 2)
    return None


def _seller_net_major(converted_price: Any, converted_currencyid: Any) -> float | None:
    try:
        return _major_units(int(converted_price), converted_currencyid)
    except (TypeError, ValueError):
        return None


def _buyer_pays_major(converted_price: Any, converted_fee: Any, converted_currencyid: Any) -> float | None:
    try:
        price = int(converted_price)
        fee = int(converted_fee) if converted_fee is not None else 0
    except (TypeError, ValueError):
        return None
    return _major_units(price + fee, converted_currencyid)


def parse_render_payload(data: dict[str, Any]) -> list[dict[str, Any]]:
    if not data.get("success"):
        return []
    assets = _asset_map(data.get("assets"))
    rows: list[dict[str, Any]] = []
    for listing_id, info in _iter_listings(data.get("listinginfo")):
        asset_part = info.get("asset") or {}
        asset_id = str(asset_part.get("id") or "")
        asset = assets.get(asset_id) or {}
        converted_price = info.get("converted_price")
        converted_fee = info.get("converted_fee")
        if converted_fee is None and info.get("fee") is not None:
            converted_fee = info.get("fee")
        currency_id = info.get("converted_currencyid")
        rows.append(
            {
                "listing_id": listing_id,
                "asset_id": asset_id or None,
                "market_hash_name": asset.get("market_hash_name"),
                "converted_price": converted_price,
                "converted_fee": converted_fee,
                "converted_currencyid": currency_id,
                "ask": _buyer_pays_major(converted_price, converted_fee, currency_id),
                "ask_seller_net": _seller_net_major(converted_price, currency_id),
                "float_value": _float_from_asset(asset),
                "paint_seed": _paint_seed_from_asset(asset),
            }
        )
    return rows
