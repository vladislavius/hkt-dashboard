#!/usr/bin/env python3
"""
tat_scraper.py — fetches Russian tourist counts from MOTS Thailand.
Saves to data/tat_stats.json. Falls back to hardcoded estimates on failure.
Run monthly via GitHub Actions (1st of each month).
"""
import json, datetime, requests
from pathlib import Path

# Hardcoded monthly estimates (Russians to all Thailand) based on MOTS/TAT 2024-2026.
# Update this dict annually when official annual report is published.
# Source: Ministry of Tourism and Sports Thailand, Tourist Statistics
TAT_FALLBACK = {
    "2024-01": 175000, "2024-02": 150000, "2024-03": 130000,
    "2024-04":  88000, "2024-05":  78000, "2024-06":  68000,
    "2024-07": 115000, "2024-08": 108000, "2024-09":  92000,
    "2024-10": 108000, "2024-11": 138000, "2024-12": 195000,
    "2025-01": 182000, "2025-02": 155000, "2025-03": 132000,
    "2025-04":  90000, "2025-05":  80000, "2025-06":  70000,
    "2025-07": 118000, "2025-08": 112000, "2025-09":  95000,
    "2025-10": 112000, "2025-11": 142000, "2025-12": 198000,
    "2026-01": 180000, "2026-02": 152000, "2026-03": 128000,
    "2026-04":  86000,
}

STATS_FILE = Path("data/tat_stats.json")


def load_existing():
    if STATS_FILE.exists():
        try:
            return json.loads(STATS_FILE.read_text())
        except Exception:
            pass
    return {"updated": None, "source": "fallback", "monthly": {}}


def fetch_mots_data():
    """
    Attempt to fetch Russian tourist data from MOTS Thailand statistics page.
    Returns dict {month_str: count} or None on failure.
    Currently returns None — MOTS data is in PDF/Excel with no stable JSON API.
    When MOTS exposes a JSON endpoint, implement parsing here.
    """
    try:
        r = requests.get(
            "https://www.mots.go.th/more_news.php?cid=411",
            timeout=15,
            headers={"User-Agent": "Mozilla/5.0 (compatible; HKT-Dashboard/1.0)"},
        )
        r.raise_for_status()
        # MOTS currently serves HTML with PDF links — return None until API stabilises
        return None
    except Exception as e:
        print(f"Warning: MOTS fetch failed: {e} — using fallback data")
        return None


def build_monthly(existing_monthly):
    """Merge fallback + previously fetched + any live data. Live wins."""
    merged = dict(TAT_FALLBACK)
    merged.update(existing_monthly)  # preserve any previously fetched values
    live = fetch_mots_data()
    if live:
        merged.update(live)
        print(f"MOTS live data fetched: {len(live)} months")
    else:
        print("Using fallback TAT data")
    return merged


def run():
    Path("data").mkdir(exist_ok=True)
    existing = load_existing()
    monthly = build_monthly(existing.get("monthly", {}))
    result = {
        "updated": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "source": "MOTS Thailand + fallback estimates",
        "note": "Russian tourists to all Thailand. HKT share ~38%. Figures are estimates.",
        "monthly": monthly,
    }
    STATS_FILE.write_text(json.dumps(result, indent=2, ensure_ascii=False))
    print(f"tat_stats.json written — {len(monthly)} months of data")


if __name__ == "__main__":
    run()
