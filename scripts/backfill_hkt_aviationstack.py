"""
One-off backfill for HKT past-day data via Aviationstack historical endpoint.
Use ONLY when GTT fails to return the full day (e.g. evening slot missing).

Usage:
    AVIATIONSTACK_KEY_1=xxx python3 scripts/backfill_hkt_aviationstack.py 2026-04-22

Merges Aviationstack flights into existing data/accumulated_YYYY-MM-DD.json:
- Adds flights not present in GTT list (dedup by flight number)
- Preserves existing GTT statuses (they are more accurate)
- Recalculates arrivals.count / pax / countries after merge
- Regenerates data/dashboard.json via collector's build step
"""
import os
import sys
import json
import re
import requests
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from collector import DOMESTIC, MAP_C, MAP_RU_CITY, CAP, LOAD_FACTOR  # noqa: E402


BASE = "https://api.aviationstack.com/v1/flights"
KEYS = [os.environ.get(f"AVIATIONSTACK_KEY_{i}", "") for i in range(1, 5)]
KEYS = [k for k in KEYS if k]


def fetch(direction, flight_date):
    """Fetch HKT arrivals/departures for a specific historical date."""
    key_param = "arr_iata" if direction == "arrival" else "dep_iata"
    all_flights = []
    for idx, key in enumerate(KEYS):
        offset = 0
        while True:
            r = requests.get(
                BASE,
                params={
                    "access_key": key,
                    key_param: "HKT",
                    "flight_date": flight_date,
                    "limit": 100,
                    "offset": offset,
                },
                timeout=30,
            )
            data = r.json()
            if "error" in data:
                print(f"⚠️ key #{idx+1} {direction} error: {data['error'].get('code')}")
                break
            chunk = data.get("data", [])
            all_flights.extend(chunk)
            pagination = data.get("pagination", {})
            if len(chunk) < 100 or offset + 100 >= pagination.get("total", 0):
                break
            offset += 100
        if all_flights:
            print(f"✅ {direction}: {len(all_flights)} flights via key #{idx+1}")
            return all_flights
    print(f"❌ {direction}: all keys failed")
    return []


def normalize(raw, direction, target_date):
    """Turn Aviationstack records into collector-compatible flight_list entries."""
    out = []
    for f in raw:
        dep = (f.get("departure") or {}).get("iata", "") or ""
        arr = (f.get("arrival") or {}).get("iata", "") or ""
        if direction == "arrival" and dep in DOMESTIC:
            continue
        if direction == "departure" and arr in DOMESTIC:
            continue
        fn_obj = f.get("flight") or {}
        fn = (fn_obj.get("iata") or fn_obj.get("icao") or "").strip()
        if not fn:
            continue
        airline = ((f.get("airline") or {}).get("name") or "").strip()
        ac_iata = ((f.get("aircraft") or {}).get("iata") or "")
        pax = int(CAP.get(ac_iata, 180) * LOAD_FACTOR)
        status = (f.get("flight_status") or "").lower().strip()
        dep_sched = (f.get("departure") or {}).get("scheduled", "") or ""
        arr_sched = (f.get("arrival") or {}).get("scheduled", "") or ""
        rec = {
            "fn": fn,
            "airline": airline,
            "status": status,
            "pax": pax,
            "aircraft": ac_iata,
            "dep_time": dep_sched[:16] if dep_sched else "",
            "arr_time": arr_sched[:16] if arr_sched else "",
        }
        if direction == "arrival":
            rec["from"] = dep
            rec["country"] = MAP_C.get(dep, "")
        else:
            rec["to"] = arr
            rec["country"] = MAP_C.get(arr, "")
        out.append(rec)
    return out


_NORM = re.compile(r"\s+")


def _norm_fn(fn):
    return _NORM.sub("", (fn or "").upper())


def merge(existing, incoming):
    """Merge incoming into existing by normalized flight number.
    Existing records win on status conflict (GTT is more reliable)."""
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
    """Rebuild count/pax/countries from list, matching collector output shape."""
    count = 0
    pax = 0
    countries = {}
    for r in flight_list:
        count += 1
        pax += r.get("pax", 0)
        country = r.get("country") or "Other"
        if country == "Russia":
            airport = r.get("from") if direction == "arrival" else r.get("to")
            city = MAP_RU_CITY.get(airport, airport or country)
            cur = countries.setdefault(city, {"flights": 0, "pax": 0, "country": "Russia"})
        else:
            cur = countries.setdefault(country, {"flights": 0, "pax": 0})
        cur["flights"] += 1
        cur["pax"] += r.get("pax", 0)
    return count, pax, countries


def main():
    if not KEYS:
        print("❌ No AVIATIONSTACK_KEY_1..4 env vars set. Exiting.")
        sys.exit(1)
    date = sys.argv[1] if len(sys.argv) > 1 else "2026-04-22"
    acc_path = ROOT / "data" / f"accumulated_{date}.json"
    if not acc_path.exists():
        print(f"❌ {acc_path} not found")
        sys.exit(1)

    existing = json.loads(acc_path.read_text())
    print(f"📂 Loaded {acc_path.name}: arr={len(existing.get('arrivals_list', []))} dep={len(existing.get('departures_list', []))}")

    print(f"🔄 Fetching Aviationstack historical for {date}...")
    a_raw = fetch("arrival", date)
    d_raw = fetch("departure", date)
    a_new = normalize(a_raw, "arrival", date)
    d_new = normalize(d_raw, "departure", date)
    print(f"   AS normalized: arr={len(a_new)} dep={len(d_new)}")

    arr_merged, arr_added = merge(existing.get("arrivals_list", []), a_new)
    dep_merged, dep_added = merge(existing.get("departures_list", []), d_new)
    print(f"   Merged: arr +{arr_added}, dep +{dep_added}")

    a_count, a_pax, a_ctry = rebuild_aggregates(arr_merged, "arrival")
    d_count, d_pax, d_ctry = rebuild_aggregates(dep_merged, "departure")

    existing["arrivals_list"] = sorted(arr_merged, key=lambda r: r.get("arr_time", ""))
    existing["departures_list"] = sorted(dep_merged, key=lambda r: r.get("dep_time", ""))
    existing["arrivals"] = {"count": a_count, "pax": a_pax, "countries": a_ctry}
    existing["departures"] = {"count": d_count, "pax": d_pax, "countries": d_ctry}

    acc_path.write_text(json.dumps(existing, indent=2, ensure_ascii=False))
    print(f"✅ {acc_path.name}: arr={a_count} ({a_pax} pax), dep={d_count} ({d_pax} pax)")
    print(f"   Last arr_time: {max((r.get('arr_time','') for r in arr_merged), default='—')}")
    print(f"   Now run: python3 collector.py  (to refresh dashboard.json)")


if __name__ == "__main__":
    main()
