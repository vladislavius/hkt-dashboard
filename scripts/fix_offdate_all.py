"""
Move off-date records to correct date files across all 4 airports.

Records with arr_time/dep_time date != filename date get migrated to the
correct accumulated_<actual_date>.json (creating it if needed).

Why: live dashboard is used by people meeting/sending off passengers.
A record placed in wrong-day file makes them go to airport on wrong day.

Run:
    python3 scripts/fix_offdate_all.py
"""
import json
import sys
from pathlib import Path
from collections import defaultdict

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from pqc.mappings import MAP_C as MAP_C_VN, MAP_RU_CITY


def get_mappers(airport):
    """HKT (Thai) uses root collector mappings; CXR/PQC/DAD use Vietnamese ones.
    For dedup-aware aggregation we just need MAP_C + MAP_RU_CITY. Use PQC ones
    for all since MAP_C is global airport->country."""
    return MAP_C_VN, MAP_RU_CITY


def rebuild_aggregates(flight_list, direction, mapc, mapru):
    count = 0
    pax = 0
    countries = {}
    for r in flight_list:
        count += 1
        pax += r.get("pax", 0)
        country = r.get("country") or "Other"
        if country == "Russia":
            airport = r.get("from") if direction == "arrivals" else r.get("to")
            city = mapru.get(airport, airport or country)
            cur = countries.setdefault(city, {"flights": 0, "pax": 0, "country": "Russia"})
        else:
            cur = countries.setdefault(country, {"flights": 0, "pax": 0})
        cur["flights"] += 1
        cur["pax"] += r.get("pax", 0)
    return count, pax, countries


def empty(date_str):
    return {
        "date": date_str,
        "arrivals": {"count": 0, "pax": 0, "countries": {}},
        "departures": {"count": 0, "pax": 0, "countries": {}},
        "arrivals_list": [],
        "departures_list": [],
    }


def fix_airport(data_dir, airport_label):
    mapc, mapru = get_mappers(airport_label)
    files = sorted(data_dir.glob("accumulated_*.json"))
    moves = defaultdict(lambda: {"arr_in": [], "dep_in": []})
    rewritten_sources = 0

    for path in files:
        fname_date = path.stem.replace("accumulated_", "")
        data = json.loads(path.read_text())
        kept_arr, kept_dep = [], []
        moved_a = moved_d = 0
        for r in data.get("arrivals_list", []):
            t = (r.get("arr_time") or "")[:10]
            if t and t != fname_date:
                moves[t]["arr_in"].append(r)
                moved_a += 1
            else:
                kept_arr.append(r)
        for r in data.get("departures_list", []):
            t = (r.get("dep_time") or "")[:10]
            if t and t != fname_date:
                moves[t]["dep_in"].append(r)
                moved_d += 1
            else:
                kept_dep.append(r)
        if moved_a or moved_d:
            data["arrivals_list"] = sorted(kept_arr, key=lambda r: r.get("arr_time", ""))
            data["departures_list"] = sorted(kept_dep, key=lambda r: r.get("dep_time", ""))
            a_c, a_p, a_ctry = rebuild_aggregates(kept_arr, "arrivals", mapc, mapru)
            d_c, d_p, d_ctry = rebuild_aggregates(kept_dep, "departures", mapc, mapru)
            data["arrivals"] = {"count": a_c, "pax": a_p, "countries": a_ctry}
            data["departures"] = {"count": d_c, "pax": d_p, "countries": d_ctry}
            path.write_text(json.dumps(data, indent=2, ensure_ascii=False))
            rewritten_sources += 1

    created = 0
    for target_date, payload in moves.items():
        path = data_dir / f"accumulated_{target_date}.json"
        if path.exists():
            data = json.loads(path.read_text())
        else:
            data = empty(target_date)
            created += 1
        arr_keys = {(r.get("fn", ""), r.get("arr_time", "")) for r in data.get("arrivals_list", [])}
        dep_keys = {(r.get("fn", ""), r.get("dep_time", "")) for r in data.get("departures_list", [])}
        for r in payload["arr_in"]:
            k = (r.get("fn", ""), r.get("arr_time", ""))
            if k not in arr_keys:
                data["arrivals_list"].append(r)
                arr_keys.add(k)
        for r in payload["dep_in"]:
            k = (r.get("fn", ""), r.get("dep_time", ""))
            if k not in dep_keys:
                data["departures_list"].append(r)
                dep_keys.add(k)
        data["arrivals_list"].sort(key=lambda r: r.get("arr_time", ""))
        data["departures_list"].sort(key=lambda r: r.get("dep_time", ""))
        a_c, a_p, a_ctry = rebuild_aggregates(data["arrivals_list"], "arrivals", mapc, mapru)
        d_c, d_p, d_ctry = rebuild_aggregates(data["departures_list"], "departures", mapc, mapru)
        data["arrivals"] = {"count": a_c, "pax": a_p, "countries": a_ctry}
        data["departures"] = {"count": d_c, "pax": d_p, "countries": d_ctry}
        path.write_text(json.dumps(data, indent=2, ensure_ascii=False))

    total_moved = sum(len(p["arr_in"]) + len(p["dep_in"]) for p in moves.values())
    print(f"[{airport_label}] source files rewritten: {rewritten_sources}, target dates touched: {len(moves)}, new files created: {created}, total records moved: {total_moved}")


def main():
    targets = [
        (ROOT / "data", "HKT"),
        (ROOT / "cxr" / "data", "CXR"),
        (ROOT / "pqc" / "data", "PQC"),
        (ROOT / "dad" / "data", "DAD"),
    ]
    for data_dir, label in targets:
        fix_airport(data_dir, label)


if __name__ == "__main__":
    main()
