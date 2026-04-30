from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


SCRIPT_DIR = Path(__file__).resolve().parent
WATCHLIST_CSV = SCRIPT_DIR / "data" / "rare_seed_watchlist.csv"
OUT_CSV = SCRIPT_DIR / "data" / "seed_target_table.csv"


def _parse_seeds(raw: str) -> list[int]:
    parts = str(raw).replace("/", ";").split(";")
    out: list[int] = []
    for part in parts:
        text = part.strip()
        if not text:
            continue
        try:
            out.append(int(text))
        except ValueError:
            continue
    return out


def _expand_query_names(item: str) -> list[str]:
    special = {
        "Butterfly Knife | Gamma Doppler Emerald / Doppler Ruby / Doppler Sapphire": [
            "Butterfly Knife | Gamma Doppler",
            "Butterfly Knife | Doppler",
        ],
        "M9 Bayonet | Gamma Doppler Emerald / Doppler Ruby / Doppler Sapphire": [
            "M9 Bayonet | Gamma Doppler",
            "M9 Bayonet | Doppler",
        ],
        "Karambit | Doppler Black Pearl / Doppler Ruby / Doppler Sapphire / Gamma Doppler Emerald": [
            "Karambit | Doppler",
            "Karambit | Gamma Doppler",
        ],
        "Glock-18 | Gamma Doppler Emerald": [
            "Glock-18 | Gamma Doppler",
        ],
    }
    return special.get(item, [item])


def _assign_by_tiers(seeds: list[int], tiers: list[tuple[int, float]]) -> dict[int, float]:
    out: dict[int, float] = {}
    pos = 0
    for count, value in tiers:
        for _ in range(count):
            if pos >= len(seeds):
                return out
            out[seeds[pos]] = value
            pos += 1
    while pos < len(seeds):
        out[seeds[pos]] = tiers[-1][1]
        pos += 1
    return out


def _all_same(seeds: list[int], value: float) -> dict[int, float]:
    return {seed: value for seed in seeds}


def _case_hardened_values(query_name: str, seeds: list[int]) -> dict[int, float]:
    if query_name == "AK-47 | Case Hardened":
        return _assign_by_tiers(seeds, [(1, 25.0), (2, 12.0), (99, 8.0)])
    if query_name == "Five-SeveN | Case Hardened":
        return _assign_by_tiers(seeds, [(2, 6.0), (99, 5.0)])
    if query_name == "MAC-10 | Case Hardened":
        return _assign_by_tiers(seeds, [(3, 4.0), (4, 3.5), (99, 3.0)])
    if query_name == "Karambit | Case Hardened":
        return _assign_by_tiers(seeds, [(1, 18.0), (2, 12.0), (3, 10.0), (99, 8.0)])
    if query_name == "Butterfly Knife | Case Hardened":
        return _assign_by_tiers(seeds, [(1, 15.0), (3, 10.0), (99, 8.0)])
    if query_name == "Talon Knife | Case Hardened":
        return _assign_by_tiers(seeds, [(1, 16.0), (2, 12.0), (3, 10.0), (99, 8.0)])
    if query_name == "M9 Bayonet | Case Hardened":
        return _assign_by_tiers(seeds, [(1, 20.0), (99, 15.0)])
    if query_name == "Bayonet | Case Hardened":
        return _assign_by_tiers(seeds, [(1, 10.0), (99, 7.0)])
    if query_name == "Flip Knife | Case Hardened":
        return _assign_by_tiers(seeds, [(1, 8.0), (2, 6.0), (99, 5.0)])
    if query_name in {"Classic Knife | Case Hardened", "Skeleton Knife | Case Hardened", "Nomad Knife | Case Hardened"}:
        return _assign_by_tiers(seeds, [(1, 8.0), (3, 6.0), (99, 5.0)])
    if query_name == "Kukri Knife | Case Hardened":
        return _assign_by_tiers(seeds, [(1, 8.0), (1, 6.0), (99, 5.0)])
    if query_name in {
        "Survival Knife | Case Hardened",
        "Gut Knife | Case Hardened",
        "Huntsman Knife | Case Hardened",
        "Falchion Knife | Case Hardened",
        "Bowie Knife | Case Hardened",
        "Shadow Daggers | Case Hardened",
    }:
        return _assign_by_tiers(seeds, [(1, 5.0), (2, 4.5), (99, 4.0)])
    return _assign_by_tiers(seeds, [(1, 6.0), (99, 4.0)])


def _estimate_overpay(
    query_name: str,
    pattern_family: str,
    seeds: list[int],
) -> dict[int, float]:
    if "Case Hardened" in query_name:
        return _case_hardened_values(query_name, seeds)
    if query_name == "Desert Eagle | Heat Treated":
        return _assign_by_tiers(seeds, [(1, 4.5), (1, 4.0), (99, 3.5)])
    if query_name == "Stiletto Knife | Scorched":
        return _assign_by_tiers(seeds, [(1, 3.0), (99, 2.5)])
    if query_name == "Karambit | Fade":
        return _all_same(seeds, 1.8)
    if query_name == "Butterfly Knife | Fade":
        return _all_same(seeds, 1.7)
    if query_name == "Talon Knife | Fade":
        return _all_same(seeds, 1.6)
    if query_name in {"Classic Knife | Fade", "Nomad Knife | Fade", "Ursus Knife | Fade"}:
        return _all_same(seeds, 1.45)
    if query_name in {"Paracord Knife | Fade", "Stiletto Knife | Fade"}:
        return _all_same(seeds, 1.35)
    if query_name == "Shadow Daggers | Fade":
        return _all_same(seeds, 1.3)
    if query_name == "Karambit | Marble Fade":
        return _assign_by_tiers(seeds, [(11, 4.5), (99, 3.0)])
    if query_name in {"Bayonet | Marble Fade", "Flip Knife | Marble Fade", "Talon Knife | Marble Fade"}:
        return _assign_by_tiers(seeds, [(11, 3.0), (99, 2.2)])
    if query_name == "Butterfly Knife | Gamma Doppler":
        return _assign_by_tiers(seeds, [(4, 7.0), (99, 5.5)])
    if query_name == "Butterfly Knife | Doppler":
        return _assign_by_tiers(seeds, [(4, 6.5), (99, 5.0)])
    if query_name == "M9 Bayonet | Gamma Doppler":
        return _assign_by_tiers(seeds, [(2, 7.0), (99, 5.5)])
    if query_name == "M9 Bayonet | Doppler":
        return _assign_by_tiers(seeds, [(2, 6.0), (99, 5.0)])
    if query_name == "Karambit | Gamma Doppler":
        return _assign_by_tiers(seeds, [(6, 8.0), (99, 6.5)])
    if query_name == "Karambit | Doppler":
        return _assign_by_tiers(seeds, [(6, 7.0), (99, 6.0)])
    if query_name == "Glock-18 | Gamma Doppler":
        return _assign_by_tiers(seeds, [(6, 3.5), (99, 3.0)])
    if query_name == "Five-SeveN | Kami":
        return _assign_by_tiers(seeds, [(1, 8.0), (2, 4.5), (99, 3.0)])
    if query_name == "AWP | PAW":
        if "Gold Cat" in pattern_family:
            return _assign_by_tiers(seeds, [(2, 3.0), (99, 2.2)])
        if "Stoner Cat" in pattern_family:
            return _all_same(seeds, 1.8)
        return _all_same(seeds, 1.6)
    if query_name == "Glock-18 | Moonrise":
        return _all_same(seeds, 1.6)
    if query_name == "Glock-18 | Grinder":
        return _all_same(seeds, 1.5)
    if query_name == "Glock-18 | Reactor":
        return _all_same(seeds, 1.35)
    if query_name in {"Glock-18 | Red Tire", "Glock-18 | High Beam"}:
        return _all_same(seeds, 1.25)
    if query_name == "USP-S | Alpine Camo":
        return _assign_by_tiers(seeds, [(1, 2.5), (99, 2.0)])
    if query_name == "P2000 | Acid Etched":
        return _all_same(seeds, 1.3)
    if query_name == "P250 | Bengal Tiger":
        return _all_same(seeds, 1.6)
    if query_name == "P250 | Nevermore":
        return _assign_by_tiers(seeds, [(2, 2.0), (99, 1.8)])
    if query_name == "P250 | Crimson Kimono":
        return _all_same(seeds, 2.0)
    if query_name == "P250 | Mint Kimono":
        return _all_same(seeds, 1.6)
    if query_name == "Five-SeveN | Neon Kimono":
        return _all_same(seeds, 1.8)
    if query_name == "Five-SeveN | Berries And Cherries":
        return _all_same(seeds, 1.7)
    if query_name == "Tec-9 | Sandstorm":
        return _all_same(seeds, 1.8)
    if query_name == "Tec-9 | Ice Cap":
        return _all_same(seeds, 1.6)
    if query_name == "Tec-9 | Cracked Opal":
        return _all_same(seeds, 1.2)
    if query_name == "Sawed-Off | Brake Light":
        return _all_same(seeds, 1.4)
    if query_name == "XM1014 | Elegant Vines":
        return _all_same(seeds, 1.3)
    if query_name == "XM1014 | Seasons":
        return _assign_by_tiers(seeds, [(1, 5.0), (6, 3.0), (99, 2.5)])
    if query_name == "XM1014 | XOXO":
        return _all_same(seeds, 1.6)
    if query_name == "XM1014 | Ziggy":
        return _all_same(seeds, 1.3)
    if query_name == "Nova | Windblown":
        return _all_same(seeds, 1.25)
    if query_name == "MAG-7 | Sonar":
        return _all_same(seeds, 1.25)
    if query_name == "MAC-10 | Last Dive":
        if "Centered Skull" in pattern_family:
            return _all_same(seeds, 2.0)
        if "Skull and Diver" in pattern_family:
            return _all_same(seeds, 1.8)
        if "Centered Tree" in pattern_family:
            return _all_same(seeds, 1.5)
        return _all_same(seeds, 1.7)
    if query_name == "UMP-45 | Minotaur's Labyrinth":
        return _all_same(seeds, 1.8)
    if query_name == "Galil AR | Phoenix Blacklight":
        return _assign_by_tiers(seeds, [(2, 2.5), (99, 2.0)])
    if query_name == "Galil AR | Caution":
        return _all_same(seeds, 1.35)
    if query_name == "Galil AR | Sandstorm":
        if "Max Purple" in pattern_family:
            return _all_same(seeds, 1.7)
        return _all_same(seeds, 1.3)
    if query_name == "Galil AR | Crimson Tsunami":
        return _all_same(seeds, 1.2)
    if query_name == "M4A4 | Daybreak":
        return _all_same(seeds, 1.9)
    if query_name == "AWP | Electric Hive":
        if "Max Blue" in pattern_family:
            return _all_same(seeds, 2.0)
        return _all_same(seeds, 1.7)
    return _all_same(seeds, 1.5)


def _estimate_confidence(query_name: str, pattern_family: str, scan_priority: str, notes: str) -> float:
    text = f"{query_name} {pattern_family} {notes}".lower()
    if "low-confidence" in text or "manual candidate" in text:
        return 0.30
    if "case hardened" in text:
        return 0.92
    if "marble fade" in text:
        return 0.86
    if " | fade" in text:
        return 0.84
    if "doppler" in text:
        return 0.80
    if query_name == "Desert Eagle | Heat Treated":
        return 0.78
    base = {"A": 0.82, "B": 0.70, "C": 0.52}.get(str(scan_priority).strip().upper(), 0.60)
    if query_name == "Five-SeveN | Kami":
        return 0.88
    if query_name in {"AWP | PAW", "XM1014 | Seasons", "AWP | Electric Hive", "Galil AR | Phoenix Blacklight"}:
        return 0.80
    if query_name in {"Glock-18 | Moonrise", "Tec-9 | Sandstorm", "Tec-9 | Ice Cap", "M4A4 | Daybreak"}:
        return 0.72
    return base


def build_target_table(watchlist_csv: Path = WATCHLIST_CSV, out_csv: Path = OUT_CSV) -> pd.DataFrame:
    raw = pd.read_csv(watchlist_csv)

    rows: list[dict[str, object]] = []
    for rec in raw.to_dict(orient="records"):
        item = str(rec["item"])
        seeds = _parse_seeds(rec["seed_list"])
        pattern_family = str(rec["pattern_family"])
        scan_priority = str(rec["scan_priority"])
        notes = str(rec.get("notes", ""))
        for query_name in _expand_query_names(item):
            multiplier_map = _estimate_overpay(query_name, pattern_family, seeds)
            conf_value = _estimate_confidence(query_name, pattern_family, scan_priority, notes)
            confidence_map = {seed: round(conf_value, 2) for seed in multiplier_map}
            rows.append(
                {
                    "query_name": query_name,
                    "pattern_family": pattern_family,
                    "seed_to_overpay": multiplier_map,
                    "seed_to_confidence": confidence_map,
                    "overall_confidence": round(conf_value, 2),
                    "note": notes,
                }
            )

    grouped: dict[str, dict[str, object]] = {}
    for row in rows:
        query_name = str(row["query_name"])
        bucket = grouped.setdefault(
            query_name,
            {
                "pattern_families": [],
                "seed_to_overpay": {},
                "seed_to_confidence": {},
                "conf_values": [],
                "notes": [],
            },
        )
        bucket["pattern_families"].append(str(row["pattern_family"]))
        bucket["notes"].append(str(row["note"]))
        bucket["conf_values"].append(float(row["overall_confidence"]))
        seed_to_overpay = bucket["seed_to_overpay"]
        seed_to_conf = bucket["seed_to_confidence"]
        for seed, mult in dict(row["seed_to_overpay"]).items():
            prev = seed_to_overpay.get(seed)
            if prev is None or float(mult) > float(prev):
                seed_to_overpay[seed] = round(float(mult), 2)
        for seed, conf in dict(row["seed_to_confidence"]).items():
            prev = seed_to_conf.get(seed)
            if prev is None or float(conf) > float(prev):
                seed_to_conf[seed] = round(float(conf), 2)

    out_rows: list[dict[str, object]] = []
    for query_name, bucket in sorted(grouped.items()):
        seed_to_overpay = {
            str(seed): value
            for seed, value in sorted(bucket["seed_to_overpay"].items(), key=lambda kv: int(kv[0]))
        }
        seed_to_conf = {
            str(seed): value
            for seed, value in sorted(bucket["seed_to_confidence"].items(), key=lambda kv: int(kv[0]))
        }
        notes = "; ".join(dict.fromkeys(x for x in bucket["notes"] if x and x != "nan"))
        families = " | ".join(dict.fromkeys(bucket["pattern_families"]))
        overall = round(sum(bucket["conf_values"]) / len(bucket["conf_values"]), 2)
        out_rows.append(
            {
                "query_name": query_name,
                "seed_count": len(seed_to_overpay),
                "pattern_families": families,
                "seed_to_overpay_json": json.dumps(seed_to_overpay, ensure_ascii=False, sort_keys=True),
                "seed_to_confidence_json": json.dumps(seed_to_conf, ensure_ascii=False, sort_keys=True),
                "overall_confidence": overall,
                "pricing_method": "rough_estimate_from_pattern_tier_and_community_guides",
                "notes": notes,
            }
        )

    out = pd.DataFrame(out_rows).sort_values("query_name").reset_index(drop=True)
    out.to_csv(out_csv, index=False)
    return out


if __name__ == "__main__":
    df = build_target_table()
    print(f"saved {len(df)} rows to {OUT_CSV}")
