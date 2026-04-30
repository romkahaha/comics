from __future__ import annotations

import html
import os
import time
from typing import Any

import requests

from monitoring.steam_client import steam_item_url


def _as_float(value: Any) -> float | None:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out


def money(value: Any) -> str:
    val = _as_float(value)
    return "-" if val is None else f"EUR {val:.2f}"


def ratio(value: Any) -> str:
    val = _as_float(value)
    return "-" if val is None else f"{val:.2f}x"


def pct(value: Any) -> str:
    val = _as_float(value)
    return "-" if val is None else f"{val:.1%}"


def flt(value: Any) -> str:
    val = _as_float(value)
    return "-" if val is None else f"{val:.6f}"


def alert_key(row: dict[str, Any]) -> str:
    listing_id = str(row.get("listing_id") or "").strip()
    if listing_id:
        return f"listing:{listing_id}"
    return "fallback:{query}:{item}:{seed}:{ask}:{float}".format(
        query=row.get("query_name") or "",
        item=row.get("market_hash_name") or "",
        seed=row.get("paint_seed") or "",
        ask=row.get("ask") or "",
        float=row.get("float_value") or "",
    )


def format_alert(row: dict[str, Any]) -> str:
    name = str(row.get("market_hash_name") or row.get("query_name") or "-")
    link = steam_item_url(name)
    listing_id = str(row.get("listing_id") or "-")
    table = [
        ("name", name),
        ("seed", str(row.get("paint_seed") or "-")),
        ("float", flt(row.get("float_value"))),
        ("ask", money(row.get("ask"))),
        ("base x10", money(row.get("base_ask_mean_10"))),
        ("expected", ratio(row.get("expected_overpay_mult"))),
        ("market", ratio(row.get("market_overpay_mult"))),
        ("gap", ratio(row.get("overpay_gap_mult"))),
        ("edge", pct(row.get("edge_ratio"))),
        ("edge EUR", money(row.get("edge_abs"))),
        ("conf", f"{_as_float(row.get('overall_confidence')):.2f}" if _as_float(row.get("overall_confidence")) is not None else "-"),
        ("listing", listing_id),
    ]
    width = max(len(label) for label, _ in table)
    body = "\n".join(f"{label:<{width}}  {value}" for label, value in table)
    families = str(row.get("pattern_families") or "").strip()
    query = str(row.get("query_name") or "").strip()
    title = "Seed opportunity"
    lines = [
        f"<b>{html.escape(title)}</b>",
        "",
        f"<pre>{html.escape(body)}</pre>",
        f'<a href="{html.escape(link)}">Open Steam market page</a>',
    ]
    if query and query != name:
        lines.append(f"Query: <code>{html.escape(query)}</code>")
    if families:
        lines.append(f"Pattern: <code>{html.escape(families)}</code>")
    return "\n".join(lines)


def telegram_credentials(bot_token: str | None = None, chat_id: str | None = None) -> tuple[str, str]:
    token = bot_token or os.environ.get("TELEGRAM_BOT_TOKEN") or os.environ.get("TG_BOT_TOKEN")
    chat = chat_id or os.environ.get("TELEGRAM_CHAT_ID") or os.environ.get("TG_CHAT_ID")
    if not token:
        raise RuntimeError("Missing TELEGRAM_BOT_TOKEN or TG_BOT_TOKEN")
    if not chat:
        raise RuntimeError("Missing TELEGRAM_CHAT_ID or TG_CHAT_ID")
    return token, chat


def send_message(
    text: str,
    *,
    bot_token: str,
    chat_id: str,
    disable_web_page_preview: bool = True,
    timeout: int = 120,
) -> dict[str, Any]:
    response = requests.post(
        f"https://api.telegram.org/bot{bot_token}/sendMessage",
        data={
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": "true" if disable_web_page_preview else "false",
        },
        timeout=timeout,
    )
    if not response.ok:
        raise RuntimeError(f"Telegram sendMessage failed: {response.status_code} {response.text[:800]}")
    payload = response.json()
    if not payload.get("ok"):
        raise RuntimeError(f"Telegram sendMessage failed: {payload}")
    return payload


def maybe_sleep(seconds: float) -> None:
    wait = max(0.0, float(seconds))
    if wait > 0:
        time.sleep(wait)
