"""
One-off backfill for PQC + DAD past-day data via FlightAware AeroAPI.

Covers gap when Webshare proxy creds were rotated 2026-05-23 → 2026-05-30
and PQC/DAD collectors silently wrote zeros.

Usage:
    FLIGHTAWARE_API_KEY=xxx python3 scripts/backfill_pqc_dad_flightaware.py 2026-05-23 2026-05-30

Reads/writes:
    pqc/data/accumulated_<date>.json
    dad/data/accumulated_<date>.json

Cost: ~$0.02-0.05 per request × 2 airports × 2 directions × N days
"""
import os
import sys
import json
import re
import time
import datetime
import requests
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from pqc.mappings import (
    MAP_C, MAP_RU_CITY, CAP, LOAD_FACTOR, DOMESTIC_VN,
)

API_BASE = "https://aeroapi.flightaware.com/aeroapi"
API_KEY = os.environ.get("FLIGHTAWARE_API_KEY", "")
ICT_OFFSET = datetime.timedelta(hours=7)  # ICT (Vietnam) = UTC+7


def ict_day_to_utc(date_str):
    dt = datetime.datetime.strptime(date_str, "%Y-%m-%d")
    start_ict = dt.replace(hour=0, minute=0, second=0)
    end_ict = dt.replace(hour=23, minute=59, second=59)
    return (
        (start_ict - ICT_OFFSET).strftime("%Y-%m-%dT%H:%M:%SZ"),
        (end_ict - ICT_OFFSET).strftime("%Y-%m-%dT%H:%M:%SZ"),
    )


def fetch(airport, direction, date_str):
    start_utc, end_utc = ict_day_to_utc(date_str)
    url = f"{API_BASE}/airports/{airport}/flights/{direction}"
    all_flights = []
    params = {"start": start_utc, "end": end_utc, "max_pages": 10}
    cursor = None
    for _ in range(20):
        q = dict(params)
        if cursor:
            q["cursor"] = cursor
        for attempt in range(5):
            r = requests.get(url, headers={"x-apikey": API_KEY}, params=q, timeout=30)
            if r.status_code == 200:
                break
            if r.status_code == 429:
                wait = 15 * (attempt + 1)
                print(f"  ⏳ {airport} {direction} rate-limited, sleep {wait}s (attempt {attempt+1}/5)")
                time.sleep(wait)
                continue
            print(f"  ❌ {airport} {direction} HTTP {r.status_code}: {r.text[:200]}")
            return []
        if r.status_code != 200:
            print(f"  ❌ {airport} {direction} HTTP {r.status_code} after 5 retries")
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
    return all_flights


def utc_to_ict(iso_utc):
    if not iso_utc:
        return ""
    try:
        dt = datetime.datetime.strptime(iso_utc.replace("Z", ""), "%Y-%m-%dT%H:%M:%S")
    except Exception:
        return ""
    return (dt + ICT_OFFSET).strftime("%Y-%m-%dT%H:%M")


def map_status(f, direction):
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
    out = []
    for f in raw:
        origin = (f.get("origin") or {}).get("code_iata") or ""
        dest = (f.get("destination") or {}).get("code_iata") or ""
        if direction == "arrivals" and origin in DOMESTIC_VN:
            continue
        if direction == "departures" and dest in DOMESTIC_VN:
            continue
        fn = (f.get("ident_iata") or f.get("ident") or "").strip()
        if not fn:
            continue
        airline = (f.get("operator_iata") or f.get("operator") or "").strip()
        ac = (f.get("aircraft_type") or "").strip()
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
            rec["country"] = MAP_C.get(origin, f"Other({origin})")
        else:
            rec["to"] = dest
            rec["country"] = MAP_C.get(dest, f"Other({dest})")
        out.append(rec)
    return out


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


REQUEST_DELAY = 7  # seconds between fetch calls; FA Personal tier has ~10rpm

def backfill_airport(airport_code, dir_name, date_str):
    acc_path = ROOT / dir_name / "data" / f"accumulated_{date_str}.json"
    if not acc_path.exists():
        existing = {
            "date": date_str,
            "arrivals": {"count": 0, "pax": 0, "countries": {}},
            "departures": {"count": 0, "pax": 0, "countries": {}},
            "arrivals_list": [],
            "departures_list": [],
        }
    else:
        existing = json.loads(acc_path.read_text())

    a_raw = fetch(airport_code, "arrivals", date_str)
    time.sleep(REQUEST_DELAY)
    d_raw = fetch(airport_code, "departures", date_str)
    time.sleep(REQUEST_DELAY)
    a_new = normalize(a_raw, "arrivals")
    d_new = normalize(d_raw, "departures")

    # FA is authoritative for backfill — overwrite zeros
    existing["arrivals_list"] = sorted(a_new, key=lambda r: r.get("arr_time", ""))
    existing["departures_list"] = sorted(d_new, key=lambda r: r.get("dep_time", ""))
    a_count, a_pax, a_ctry = rebuild_aggregates(a_new, "arrivals")
    d_count, d_pax, d_ctry = rebuild_aggregates(d_new, "departures")
    existing["arrivals"] = {"count": a_count, "pax": a_pax, "countries": a_ctry}
    existing["departures"] = {"count": d_count, "pax": d_pax, "countries": d_ctry}
    existing["date"] = date_str

    acc_path.parent.mkdir(parents=True, exist_ok=True)
    acc_path.write_text(json.dumps(existing, indent=2, ensure_ascii=False))
    print(f"  ✅ {airport_code} {date_str}: arr={a_count} ({a_pax} pax), dep={d_count} ({d_pax} pax)  [raw arr/dep: {len(a_raw)}/{len(d_raw)}]")


def daterange(start, end):
    s = datetime.datetime.strptime(start, "%Y-%m-%d")
    e = datetime.datetime.strptime(end, "%Y-%m-%d")
    cur = s
    while cur <= e:
        yield cur.strftime("%Y-%m-%d")
        cur += datetime.timedelta(days=1)


def main():
    if not API_KEY:
        print("❌ FLIGHTAWARE_API_KEY not set")
        sys.exit(1)
    if len(sys.argv) < 3:
        print("Usage: backfill_pqc_dad_flightaware.py START END")
        sys.exit(1)
    start, end = sys.argv[1], sys.argv[2]
    print(f"🔄 FlightAware backfill PQC+DAD: {start} → {end}")
    for date in daterange(start, end):
        print(f"\n📅 {date}")
        backfill_airport("PQC", "pqc", date)
        backfill_airport("DAD", "dad", date)
    print("\n✅ Done. Now regenerate dashboards:")
    print("   cd pqc && python3 collector.py")
    print("   cd dad && python3 collector.py")


if __name__ == "__main__":
    main()
