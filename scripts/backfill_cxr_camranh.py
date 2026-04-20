"""Backfill CXR historical data from camranh.aero REST API.

Скачивает arrivals+departures за каждый день в диапазоне [start_date, end_date]
и сохраняет в cxr/data/accumulated_YYYY-MM-DD.json + пересобирает dashboard.json.

Запуск:
    python3 scripts/backfill_cxr_camranh.py 2025-09-01 2026-04-19

Supabase писать НЕ будем из скрипта — после прогона экспортируется SQL-дамп
в scripts/_cxr_backfill_upsert.sql для применения через Supabase MCP/Editor.
"""
import datetime
import json
import sys
import time
from pathlib import Path

import requests

# cxr.collector — для analyze_camranh
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from cxr.collector import (  # noqa: E402
    fetch_flights_camranh, analyze_camranh,
    make_by_days, make_by_weeks, make_by_months,
)

CXR_DATA_DIR = Path("cxr/data")
SQL_DUMP = Path("scripts/_cxr_backfill_upsert.sql")


def daterange(start, end):
    d = start
    while d <= end:
        yield d
        d += datetime.timedelta(days=1)


def fetch_day(date_str):
    """Скачать день и вернуть готовую accumulated JSON-структуру (same shape as collector)."""
    a_raw, d_raw = fetch_flights_camranh(date_str)
    if a_raw is None and d_raw is None:
        return None
    a_res = analyze_camranh(a_raw or [], "arrival",   date_str)
    d_res = analyze_camranh(d_raw or [], "departure", date_str)
    a = a_res.get(date_str, {"count": 0, "pax": 0, "countries": {}, "flight_list": []})
    d = d_res.get(date_str, {"count": 0, "pax": 0, "countries": {}, "flight_list": []})

    acc = {
        "date": date_str,
        "arrivals": {
            "count": a["count"], "pax": a["pax"], "countries": a["countries"],
        },
        "departures": {
            "count": d["count"], "pax": d["pax"], "countries": d["countries"],
        },
        "arrivals_list":   a.get("flight_list", []),
        "departures_list": d.get("flight_list", []),
    }
    return acc


def sql_escape(obj):
    """Экранирует строку/JSON для вставки в SQL."""
    if isinstance(obj, (dict, list)):
        s = json.dumps(obj, ensure_ascii=False)
    else:
        s = str(obj)
    return s.replace("'", "''")


def gen_upsert_sql(rows):
    """Генерирует один multi-VALUES UPSERT для cxr_flight_daily."""
    if not rows:
        return ""
    values = []
    for r in rows:
        v = (
            f"("
            f"'{r['date']}',"
            f"{r['arrivals']['count']},"
            f"{r['arrivals']['pax']},"
            f"{r['departures']['count']},"
            f"{r['departures']['pax']},"
            f"'{sql_escape(r['arrivals']['countries'])}'::jsonb,"
            f"'{sql_escape(r['departures']['countries'])}'::jsonb,"
            f"NOW()"
            f")"
        )
        values.append(v)
    return (
        "INSERT INTO cxr_flight_daily "
        "(date, arrivals_count, arrivals_pax, departures_count, departures_pax, "
        "arrivals_countries, departures_countries, updated_at) VALUES\n"
        + ",\n".join(values)
        + "\nON CONFLICT (date) DO UPDATE SET "
        "arrivals_count     = EXCLUDED.arrivals_count, "
        "arrivals_pax       = EXCLUDED.arrivals_pax, "
        "departures_count   = EXCLUDED.departures_count, "
        "departures_pax     = EXCLUDED.departures_pax, "
        "arrivals_countries = EXCLUDED.arrivals_countries, "
        "departures_countries = EXCLUDED.departures_countries, "
        "updated_at         = NOW();"
    )


def rebuild_dashboard(all_days_data, today_str):
    """Пересобирает cxr/data/dashboard.json поверх накопленных данных."""
    from cxr.collector import load_period_stats, ICT_VN

    CXR_DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Собираем available_dates (все даты которые у нас на диске)
    available_dates = sorted([
        p.stem.replace("accumulated_", "")
        for p in CXR_DATA_DIR.glob("accumulated_*.json")
    ])
    if not available_dates:
        return

    latest = available_dates[-1]
    yesterday = (datetime.date.fromisoformat(latest) - datetime.timedelta(days=1)).isoformat()

    def load_day(dk):
        fp = CXR_DATA_DIR / f"accumulated_{dk}.json"
        if not fp.exists():
            return {
                "arrivals":   {"count": 0, "pax": 0, "countries": {}},
                "departures": {"count": 0, "pax": 0, "countries": {}},
                "arrivals_list": [], "departures_list": [],
            }
        return json.loads(fp.read_text())

    today_acc = load_day(latest)
    y_acc = load_day(yesterday)

    w_a, w_d, w_daily = load_period_stats(latest, 7,   )
    m_a, m_d, m_daily = load_period_stats(latest, 30,  )
    q_a, q_d, q_daily = load_period_stats(latest, 90,  )
    h_a, h_d, h_daily = load_period_stats(latest, 180, )
    y_a, y_d, y_daily = load_period_stats(latest, 365, )

    def fmt_all(ctry):
        sorted_c = sorted(ctry.items(), key=lambda x: x[1]["flights"], reverse=True)
        result = []
        for c, v in sorted_c:
            item = {"name": c, "flights": v["flights"], "pax": v["pax"]}
            if "country" in v:
                item["country"] = v["country"]
            result.append(item)
        return result

    def to_web(arr, dep, breakdown=None, arrivals_list=None, departures_list=None):
        d = {
            "arrivals":   {"count": arr["count"], "pax": arr["pax"], "all": fmt_all(arr["countries"])},
            "departures": {"count": dep["count"], "pax": dep["pax"], "all": fmt_all(dep["countries"])},
        }
        if breakdown:
            d.update(breakdown)
        if arrivals_list:
            d["arrivals_list"] = arrivals_list
        if departures_list:
            d["departures_list"] = departures_list
        return d

    now = datetime.datetime.now(ICT_VN)
    dashboard_data = {
        "updated":          now.isoformat(),
        "available_dates":  available_dates,
        "yesterday": to_web(y_acc["arrivals"], y_acc["departures"],
                            arrivals_list=y_acc.get("arrivals_list", []),
                            departures_list=y_acc.get("departures_list", [])),
        "today":     to_web(today_acc["arrivals"], today_acc["departures"],
                            arrivals_list=today_acc.get("arrivals_list", []),
                            departures_list=today_acc.get("departures_list", [])),
        "week":      to_web(w_a, w_d, {"by_days":   make_by_days(w_daily)}),
        "month":     to_web(m_a, m_d, {"by_weeks":  make_by_weeks(m_daily)}),
        "quarter":   to_web(q_a, q_d, {"by_months": make_by_months(q_daily)}),
        "halfyear":  to_web(h_a, h_d, {"by_months": make_by_months(h_daily)}),
        "year":      to_web(y_a, y_d, {"by_months": make_by_months(y_daily)}),
    }
    (CXR_DATA_DIR / "dashboard.json").write_text(json.dumps(dashboard_data, ensure_ascii=False, indent=2))
    print(f"✅ Rebuilt dashboard.json ({len(available_dates)} dates)")


def main():
    if len(sys.argv) < 3:
        print("Usage: python3 scripts/backfill_cxr_camranh.py START_DATE END_DATE")
        print("       python3 scripts/backfill_cxr_camranh.py 2025-09-01 2026-04-19")
        sys.exit(1)
    start = datetime.date.fromisoformat(sys.argv[1])
    end   = datetime.date.fromisoformat(sys.argv[2])

    CXR_DATA_DIR.mkdir(parents=True, exist_ok=True)

    all_days = []
    fetched = skipped = errored = 0
    total = (end - start).days + 1

    print(f"📅 Backfilling {total} days: {start} → {end}")

    for i, d in enumerate(daterange(start, end), 1):
        date_str = d.isoformat()
        out_file = CXR_DATA_DIR / f"accumulated_{date_str}.json"

        # Skip if already exists and has flights (idempotent)
        if out_file.exists():
            try:
                cached = json.loads(out_file.read_text())
                if cached.get("arrivals", {}).get("count", 0) > 0 or cached.get("departures", {}).get("count", 0) > 0:
                    all_days.append(cached)
                    skipped += 1
                    if i % 20 == 0:
                        print(f"  [{i}/{total}] {date_str} skipped (cached)")
                    continue
            except Exception:
                pass

        try:
            acc = fetch_day(date_str)
            if acc is None:
                errored += 1
                print(f"  [{i}/{total}] {date_str} ❌ fetch returned None")
                continue
            out_file.write_text(json.dumps(acc, indent=2, ensure_ascii=False))
            all_days.append(acc)
            fetched += 1
            a, dd = acc["arrivals"]["count"], acc["departures"]["count"]
            print(f"  [{i}/{total}] {date_str} ✅ {a} arr / {dd} dep")
            time.sleep(0.4)  # быть вежливым к API
        except Exception as e:
            errored += 1
            print(f"  [{i}/{total}] {date_str} ❌ {e}")

    print(f"\n📊 SUMMARY: fetched={fetched}, skipped={skipped}, errored={errored}")

    # SQL dump для Supabase
    sql = gen_upsert_sql(all_days)
    SQL_DUMP.write_text(sql)
    print(f"📝 SQL dump written to {SQL_DUMP} ({len(all_days)} rows)")

    # Rebuild dashboard.json
    today_str = end.isoformat()
    rebuild_dashboard(all_days, today_str)


if __name__ == "__main__":
    main()
