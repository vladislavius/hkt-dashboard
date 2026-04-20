"""Агрегация периодных статистик из накопленных JSON-файлов."""
import datetime
import json
from pathlib import Path

from core.mappings import DAY_NAMES_RU, MONTH_NAMES_RU


def load_period_stats(today_str, days, data_dir='data'):
    """Читает `{data_dir}/accumulated_YYYY-MM-DD.json` за последние `days` и агрегирует.

    Returns:
        (total_arrivals, total_departures, daily_dict)
        где daily_dict = {date_str: {arrivals, departures, arrivals_by, departures_by}}
    """
    tot_a = {"count": 0, "pax": 0, "countries": {}}
    tot_d = {"count": 0, "pax": 0, "countries": {}}
    daily = {}

    for i in range(days):
        dt = (datetime.datetime.fromisoformat(today_str) - datetime.timedelta(days=i)).date()
        dt_str = dt.isoformat()
        fp = Path(data_dir) / f"accumulated_{dt_str}.json"
        if not fp.exists():
            continue
        try:
            d = json.loads(fp.read_text())
            a_count = d.get("arrivals", {}).get("count", 0)
            d_count = d.get("departures", {}).get("count", 0)
            a_ctry = d.get("arrivals", {}).get("countries", {})
            d_ctry = d.get("departures", {}).get("countries", {})

            def flatten(ctry):
                out = {}
                for c, v in ctry.items():
                    out[c] = {"n": v.get("flights", 0)}
                    if "country" in v:
                        out[c]["country"] = v["country"]
                return out

            daily[dt_str] = {
                "arrivals": a_count,
                "departures": d_count,
                "arrivals_by": flatten(a_ctry),
                "departures_by": flatten(d_ctry),
            }
            for side, tot in [("arrivals", tot_a), ("departures", tot_d)]:
                x = d.get(side, {})
                tot["count"] += x.get("count", 0)
                tot["pax"] += x.get("pax", 0)
                for c, v in x.get("countries", {}).items():
                    if c not in tot["countries"]:
                        tot["countries"][c] = {"flights": 0, "pax": 0}
                    tot["countries"][c]["flights"] += v.get("flights", 0)
                    tot["countries"][c]["pax"] += v.get("pax", 0)
                    if "country" in v:
                        tot["countries"][c]["country"] = v["country"]
        except Exception:
            continue
    return tot_a, tot_d, daily


def _merge_by(target, source):
    """Сливает source {name: {n, ?country}} в target."""
    for c, v in source.items():
        n = v["n"] if isinstance(v, dict) else v
        if c not in target:
            target[c] = {"n": 0}
            if isinstance(v, dict) and "country" in v:
                target[c]["country"] = v["country"]
        target[c]["n"] += n


def make_by_days(daily):
    result = []
    for date_str in sorted(daily.keys()):
        dt = datetime.date.fromisoformat(date_str)
        label = f"{DAY_NAMES_RU[dt.weekday()]} {dt.day:02d}.{dt.month:02d}"
        result.append({
            "date": date_str, "label": label,
            "arrivals": daily[date_str]["arrivals"],
            "departures": daily[date_str]["departures"],
            "arrivals_by": daily[date_str].get("arrivals_by", {}),
            "departures_by": daily[date_str].get("departures_by", {}),
        })
    return result


def make_by_weeks(daily):
    weeks = {}
    for date_str in sorted(daily.keys()):
        dt = datetime.date.fromisoformat(date_str)
        iso_year, iso_week, _ = dt.isocalendar()
        key = f"{iso_year}-W{iso_week:02d}"
        if key not in weeks:
            weeks[key] = {"key": key,
                          "label": f"Нед {iso_week} ({MONTH_NAMES_RU[dt.month]})",
                          "arrivals": 0, "departures": 0,
                          "arrivals_by": {}, "departures_by": {}}
        weeks[key]["arrivals"] += daily[date_str]["arrivals"]
        weeks[key]["departures"] += daily[date_str]["departures"]
        _merge_by(weeks[key]["arrivals_by"], daily[date_str].get("arrivals_by", {}))
        _merge_by(weeks[key]["departures_by"], daily[date_str].get("departures_by", {}))
    return [v for _, v in sorted(weeks.items())]


def make_by_months(daily):
    months = {}
    for date_str in sorted(daily.keys()):
        dt = datetime.date.fromisoformat(date_str)
        key = f"{dt.year}-{dt.month:02d}"
        if key not in months:
            months[key] = {"key": key,
                           "label": f"{MONTH_NAMES_RU[dt.month]} {dt.year}",
                           "arrivals": 0, "departures": 0,
                           "arrivals_by": {}, "departures_by": {}}
        months[key]["arrivals"] += daily[date_str]["arrivals"]
        months[key]["departures"] += daily[date_str]["departures"]
        _merge_by(months[key]["arrivals_by"], daily[date_str].get("arrivals_by", {}))
        _merge_by(months[key]["departures_by"], daily[date_str].get("departures_by", {}))
    return [v for _, v in sorted(months.items())]
