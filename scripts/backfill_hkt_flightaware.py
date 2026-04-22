"""
One-off backfill for HKT past-day evening slot via FlightAware AeroAPI.

Covers the gap left by GTT (which only returns ~24h rolling window, so evening
flights ≥16:00 disappear after 24h). FlightAware AeroAPI preserves full day
history for the Personal tier retention (~10 days).

Usage:
    FLIGHTAWARE_API_KEY=xxx python3 scripts/backfill_hkt_flightaware.py 2026-04-22

Merge policy:
  - Dedup by normalized flight number (e.g. "ZF4067" == "ZF 4067")
  - Existing records win on status conflict (GTT is more authoritative)
  - New flights (evening that GTT dropped) get added
  - Aggregates (count/pax/countries) rebuilt from merged list
"""
import os
import sys
import json
import re
import datetime
import requests
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from collector import DOMESTIC, MAP_C, MAP_RU_CITY, CAP, LOAD_FACTOR  # noqa: E402


API_BASE = "https://aeroapi.flightaware.com/aeroapi"
API_KEY = os.environ.get("FLIGHTAWARE_API_KEY", "")
AIRPORT = "HKT"
ICT_OFFSET = datetime.timedelta(hours=7)  # HKT is UTC+7


def ict_day_to_utc(date_str):
    """Convert ICT date (YYYY-MM-DD) to UTC ISO range covering the full ICT day."""
    dt = datetime.datetime.strptime(date_str, "%Y-%m-%d")
    start_ict = dt.replace(hour=0, minute=0, second=0)
    end_ict = dt.replace(hour=23, minute=59, second=59)
    start_utc = (start_ict - ICT_OFFSET).strftime("%Y-%m-%dT%H:%M:%SZ")
    end_utc = (end_ict - ICT_OFFSET).strftime("%Y-%m-%dT%H:%M:%SZ")
    return start_utc, end_utc


def fetch_airport_flights(direction, date_str):
    """Fetch all arrivals or departures for HKT within the ICT day.
    direction: 'arrivals' or 'departures'.
    """
    start_utc, end_utc = ict_day_to_utc(date_str)
    url = f"{API_BASE}/airports/{AIRPORT}/flights/{direction}"
    all_flights = []
    params = {"start": start_utc, "end": end_utc, "max_pages": 10}
    cursor = None
    for _ in range(20):  # safety cap
        q = dict(params)
        if cursor:
            q["cursor"] = cursor
        r = requests.get(url, headers={"x-apikey": API_KEY}, params=q, timeout=30)
        if r.status_code != 200:
            print(f"❌ {direction} HTTP {r.status_code}: {r.text[:200]}")
            return []
        data = r.json()
        batch = data.get(direction, []) or []
        all_flights.extend(batch)
        links = data.get("links") or {}
        next_url = links.get("next")
        if not next_url:
            break
        m = re.search(r"cursor=([^&]+)", next_url)
        cursor = m.group(1) if m else None
        if not cursor:
            break
    print(f"✅ {direction}: {len(all_flights)} flights")
    return all_flights


def utc_to_ict(iso_utc):
    """Convert UTC ISO (…Z) → ICT ISO local without TZ (YYYY-MM-DDTHH:MM)."""
    if not iso_utc:
        return ""
    try:
        dt = datetime.datetime.strptime(iso_utc.replace("Z", ""), "%Y-%m-%dT%H:%M:%S")
    except Exception:
        return ""
    return (dt + ICT_OFFSET).strftime("%Y-%m-%dT%H:%M")


def map_status(f, direction):
    """Map AeroAPI status/flight_state to collector's status values."""
    if f.get("cancelled"):
        return "cancelled"
    if direction == "arrivals":
        if f.get("actual_on") or f.get("actual_in"):
            return "landed"
    else:
        if f.get("actual_off") or f.get("actual_out"):
            return "departed"
    return "scheduled"


def normalize(raw, direction):
    """Turn FlightAware records into collector's flight_list shape."""
    out = []
    for f in raw:
        origin = (f.get("origin") or {}).get("code_iata") or ""
        dest = (f.get("destination") or {}).get("code_iata") or ""
        if direction == "arrivals" and origin in DOMESTIC:
            continue
        if direction == "departures" and dest in DOMESTIC:
            continue
        fn = (f.get("ident_iata") or f.get("ident") or "").strip()
        if not fn:
            continue
        airline = (f.get("operator_iata") or f.get("operator") or "").strip()
        ac = f.get("aircraft_type") or ""
        pax = int(CAP.get(ac, 180) * LOAD_FACTOR)
        status = map_status(f, direction)
        arr_time = utc_to_ict(f.get("scheduled_on") or f.get("scheduled_in") or "")
        dep_time = utc_to_ict(f.get("scheduled_out") or f.get("scheduled_off") or "")
        rec = {
            "fn": fn,
            "airline": airline,
            "status": status,
            "pax": pax,
            "aircraft": ac,
            "dep_time": dep_time,
            "arr_time": arr_time,
        }
        if direction == "arrivals":
            rec["from"] = origin
            rec["country"] = MAP_C.get(origin, "")
        else:
            rec["to"] = dest
            rec["country"] = MAP_C.get(dest, "")
        out.append(rec)
    return out


_NORM_RE = re.compile(r"\s+")


def _norm_fn(fn):
    return _NORM_RE.sub("", (fn or "").upper())


def merge(existing, incoming):
    """Merge incoming into existing by normalized flight number.
    Existing wins on status conflict (GTT statuses more accurate for live)."""
    by_fn = {_norm_fn(r.get("fn", "")): r for r in existing if r.get("fn")}
    added = 0
    for r in incoming:
        key = _norm_fn(r.get("fn", ""))
        if not key:
            continue
        if key not in by_fn:
            by_fn[key] = r
            added += 1
    return list(by_fn.values()), added


def rebuild_aggregates(flight_list, direction):
    count = 0
    pax = 0
    countries = {}
    for r in flight_list:
        count += 1
        pax += r.get("pax", 0)
        country = r.get("country") or "Other"
        if country == "Russia":
            airport = r.get("from") if direction == "arrivals" else r.get("to")
            city = MAP_RU_CITY.get(airport, airport or country)
            cur = countries.setdefault(city, {"flights": 0, "pax": 0, "country": "Russia"})
        else:
            cur = countries.setdefault(country, {"flights": 0, "pax": 0})
        cur["flights"] += 1
        cur["pax"] += r.get("pax", 0)
    return count, pax, countries


def main():
    if not API_KEY:
        print("❌ FLIGHTAWARE_API_KEY not set")
        sys.exit(1)
    date = sys.argv[1] if len(sys.argv) > 1 else "2026-04-22"
    acc_path = ROOT / "data" / f"accumulated_{date}.json"
    if not acc_path.exists():
        print(f"❌ {acc_path} not found")
        sys.exit(1)

    existing = json.loads(acc_path.read_text())
    e_arr = existing.get("arrivals_list", [])
    e_dep = existing.get("departures_list", [])
    print(f"📂 {acc_path.name}: arr={len(e_arr)}, dep={len(e_dep)}, last_arr={max((r.get('arr_time','') for r in e_arr), default='—')}")

    print(f"🔄 FlightAware AeroAPI for {date} (ICT)...")
    a_raw = fetch_airport_flights("arrivals", date)
    d_raw = fetch_airport_flights("departures", date)
    a_new = normalize(a_raw, "arrivals")
    d_new = normalize(d_raw, "departures")
    print(f"   AeroAPI normalized: arr={len(a_new)} dep={len(d_new)}")

    arr_merged, arr_added = merge(e_arr, a_new)
    dep_merged, dep_added = merge(e_dep, d_new)
    print(f"   Merged: arr +{arr_added}, dep +{dep_added}")

    a_count, a_pax, a_ctry = rebuild_aggregates(arr_merged, "arrivals")
    d_count, d_pax, d_ctry = rebuild_aggregates(dep_merged, "departures")

    existing["arrivals_list"] = sorted(arr_merged, key=lambda r: r.get("arr_time", ""))
    existing["departures_list"] = sorted(dep_merged, key=lambda r: r.get("dep_time", ""))
    existing["arrivals"] = {"count": a_count, "pax": a_pax, "countries": a_ctry}
    existing["departures"] = {"count": d_count, "pax": d_pax, "countries": d_ctry}

    acc_path.write_text(json.dumps(existing, indent=2, ensure_ascii=False))
    print(f"✅ {acc_path.name}: arr={a_count} ({a_pax} pax), dep={d_count} ({d_pax} pax)")
    print(f"   New last_arr_time: {max((r.get('arr_time','') for r in arr_merged), default='—')}")
    print(f"   Now run: python3 collector.py  (to refresh dashboard.json)")


if __name__ == "__main__":
    main()
