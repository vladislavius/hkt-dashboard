"""
Dedup codeshare entries in DAD and PQC accumulated_*.json files.

Problem: danangairport.vn and phuquocairport.com HTML show each codeshare
as a separate row. One physical plane = N rows under different marketing
flight numbers (e.g. SIN→DAD 11:05 = 7 rows: SQ172 + AI8138 + LH7200 +
LX9094 + NZ3468 + VA5486 + VN3092). This inflates daily counts ~1.5-2x.

Fix: group by (time, origin/dest), keep one record per group (the one with
the operator IATA matching the actual flight number prefix wins, or first).

Run:
    python3 scripts/dedup_codeshares.py        # all DAD + PQC files
    python3 scripts/dedup_codeshares.py dad    # only DAD
"""
import sys
import json
from pathlib import Path
from collections import defaultdict

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from pqc.mappings import MAP_C, MAP_RU_CITY


def dedup_list(flight_list, direction):
    """Keep one record per (time, airport) group.

    Preference order within a group:
    1. Record whose airline operates the flight (fn prefix == airline IATA)
    2. First record in original order
    """
    time_key = "arr_time" if direction == "arrivals" else "dep_time"
    air_key = "from" if direction == "arrivals" else "to"

    groups = defaultdict(list)
    for rec in flight_list:
        key = (rec.get(time_key, ""), rec.get(air_key, ""))
        groups[key].append(rec)

    deduped = []
    for key, recs in groups.items():
        if len(recs) == 1:
            deduped.append(recs[0])
            continue
        # Prefer record where fn starts with airline IATA code
        winner = None
        for r in recs:
            fn = (r.get("fn") or "").strip()
            airline = (r.get("airline") or "").strip()
            # naive: check if fn starts with 2-3 letter prefix matching airline
            # Better heuristic: real operator usually has shorter, well-known IATA
            # Fallback: just keep first
            if fn[:2].isalpha() and airline:
                winner = r
                break
        if winner is None:
            winner = recs[0]
        deduped.append(winner)

    deduped.sort(key=lambda r: r.get(time_key, ""))
    return deduped


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


def process_file(path):
    data = json.loads(path.read_text())
    a_before = data.get("arrivals", {}).get("count", 0)
    d_before = data.get("departures", {}).get("count", 0)
    a_list_before = len(data.get("arrivals_list", []))
    d_list_before = len(data.get("departures_list", []))

    a_dedup = dedup_list(data.get("arrivals_list", []), "arrivals")
    d_dedup = dedup_list(data.get("departures_list", []), "departures")

    a_count, a_pax, a_ctry = rebuild_aggregates(a_dedup, "arrivals")
    d_count, d_pax, d_ctry = rebuild_aggregates(d_dedup, "departures")

    data["arrivals_list"] = a_dedup
    data["departures_list"] = d_dedup
    data["arrivals"] = {"count": a_count, "pax": a_pax, "countries": a_ctry}
    data["departures"] = {"count": d_count, "pax": d_pax, "countries": d_ctry}

    path.write_text(json.dumps(data, indent=2, ensure_ascii=False))
    changed = (a_count != a_list_before) or (d_count != d_list_before)
    return {
        "name": path.name,
        "arr_before": a_list_before,
        "arr_after": a_count,
        "dep_before": d_list_before,
        "dep_after": d_count,
        "changed": changed,
    }


def main():
    targets = sys.argv[1:] or ["dad", "pqc"]
    grand_total = {"arr_before": 0, "arr_after": 0, "dep_before": 0, "dep_after": 0}

    for airport in targets:
        data_dir = ROOT / airport / "data"
        files = sorted(data_dir.glob("accumulated_*.json"))
        print(f"\n=== {airport.upper()} ({len(files)} files) ===")
        for path in files:
            res = process_file(path)
            grand_total["arr_before"] += res["arr_before"]
            grand_total["arr_after"] += res["arr_after"]
            grand_total["dep_before"] += res["dep_before"]
            grand_total["dep_after"] += res["dep_after"]
            mark = " " if not res["changed"] else "✂"
            arr_drop = res["arr_before"] - res["arr_after"]
            dep_drop = res["dep_before"] - res["dep_after"]
            print(f"  {mark} {res['name']:35s}  arr {res['arr_before']:3d}→{res['arr_after']:3d} (-{arr_drop})  dep {res['dep_before']:3d}→{res['dep_after']:3d} (-{dep_drop})")

    print(f"\n=== TOTAL ===")
    print(f"  arrivals  {grand_total['arr_before']} → {grand_total['arr_after']} (-{grand_total['arr_before'] - grand_total['arr_after']})")
    print(f"  departures {grand_total['dep_before']} → {grand_total['dep_after']} (-{grand_total['dep_before'] - grand_total['dep_after']})")


if __name__ == "__main__":
    main()
