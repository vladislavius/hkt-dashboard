"""
Fix off-date records in pqc/data/accumulated_*.json.

Found 4 cases where arr_time/dep_time date != filename date:
  17 May file -> ARR 18 May 07:50 (C65535)   [My Freighter from TAS]
  21 Apr file -> ARR 22 Apr 07:20 (C65535)
  22 Apr file -> ARR 23 Apr 00:05 (WE207)
  14 May file -> ARR 15 May 00:05 (MU6033)

Strategy: move off-date records to the matching date file. Rebuild aggregates
in both source and target files.
"""
import sys
import json
from pathlib import Path
from collections import defaultdict

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from pqc.mappings import MAP_C, MAP_RU_CITY

DATA_DIR = ROOT / "pqc" / "data"


def load(date_str):
    p = DATA_DIR / f"accumulated_{date_str}.json"
    if not p.exists():
        return None, p
    return json.loads(p.read_text()), p


def empty_skeleton(date_str):
    return {
        "date": date_str,
        "arrivals": {"count": 0, "pax": 0, "countries": {}},
        "departures": {"count": 0, "pax": 0, "countries": {}},
        "arrivals_list": [],
        "departures_list": [],
    }


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


def fix():
    moves = defaultdict(lambda: {"arr_in": [], "dep_in": []})  # target_date -> incoming records

    files = sorted(DATA_DIR.glob("accumulated_*.json"))
    for path in files:
        fname_date = path.stem.replace("accumulated_", "")
        data = json.loads(path.read_text())
        kept_arr = []
        kept_dep = []
        for r in data.get("arrivals_list", []):
            t = (r.get("arr_time") or "")[:10]
            if t and t != fname_date:
                moves[t]["arr_in"].append(r)
                print(f"  MOVE ARR {r.get('fn')} {r.get('arr_time')} from {fname_date} -> {t}")
            else:
                kept_arr.append(r)
        for r in data.get("departures_list", []):
            t = (r.get("dep_time") or "")[:10]
            if t and t != fname_date:
                moves[t]["dep_in"].append(r)
                print(f"  MOVE DEP {r.get('fn')} {r.get('dep_time')} from {fname_date} -> {t}")
            else:
                kept_dep.append(r)
        if len(kept_arr) != len(data.get("arrivals_list", [])) or len(kept_dep) != len(data.get("departures_list", [])):
            data["arrivals_list"] = sorted(kept_arr, key=lambda r: r.get("arr_time", ""))
            data["departures_list"] = sorted(kept_dep, key=lambda r: r.get("dep_time", ""))
            a_c, a_p, a_ctry = rebuild_aggregates(kept_arr, "arrivals")
            d_c, d_p, d_ctry = rebuild_aggregates(kept_dep, "departures")
            data["arrivals"] = {"count": a_c, "pax": a_p, "countries": a_ctry}
            data["departures"] = {"count": d_c, "pax": d_p, "countries": d_ctry}
            path.write_text(json.dumps(data, indent=2, ensure_ascii=False))
            print(f"  → rewrote source {path.name}")

    for target_date, payload in moves.items():
        data, path = load(target_date)
        if data is None:
            data = empty_skeleton(target_date)
            print(f"  CREATE new file {path.name}")
        # Merge incoming records (dedup by fn+time to avoid double-add)
        arr_keys = {(r.get("fn", ""), r.get("arr_time", "")) for r in data.get("arrivals_list", [])}
        dep_keys = {(r.get("fn", ""), r.get("dep_time", "")) for r in data.get("departures_list", [])}
        added_a = 0
        added_d = 0
        for r in payload["arr_in"]:
            key = (r.get("fn", ""), r.get("arr_time", ""))
            if key not in arr_keys:
                data["arrivals_list"].append(r)
                added_a += 1
        for r in payload["dep_in"]:
            key = (r.get("fn", ""), r.get("dep_time", ""))
            if key not in dep_keys:
                data["departures_list"].append(r)
                added_d += 1
        data["arrivals_list"].sort(key=lambda r: r.get("arr_time", ""))
        data["departures_list"].sort(key=lambda r: r.get("dep_time", ""))
        a_c, a_p, a_ctry = rebuild_aggregates(data["arrivals_list"], "arrivals")
        d_c, d_p, d_ctry = rebuild_aggregates(data["departures_list"], "departures")
        data["arrivals"] = {"count": a_c, "pax": a_p, "countries": a_ctry}
        data["departures"] = {"count": d_c, "pax": d_p, "countries": d_ctry}
        path.write_text(json.dumps(data, indent=2, ensure_ascii=False))
        print(f"  → target {path.name}: added arr+{added_a} dep+{added_d}")


if __name__ == "__main__":
    fix()
