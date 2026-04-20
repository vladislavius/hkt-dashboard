"""Cam Ranh (CXR, Vietnam) flight data collector — isolated from HKT pipeline.

Pipeline (2 sources, первая успешная побеждает):
1. camranh.aero official REST API — основной. Прямой HTTP без авторизации:
   GET https://beta.camranh.aero/api/adapters/{arrivals,departures}?date=YYYY-MM-DD
2. Aviationstack API с IATA=CXR — fallback (если основной упал).

Storage:
- cxr/data/accumulated_YYYY-MM-DD.json (merged flight lists)
- cxr/data/dashboard.json (сводка для фронта)
- Supabase cxr_flight_daily (см. scripts/migrate_supabase_cxr.sql)

Telegram: префикс [CXR] в сообщении.
"""
import os
import json
import datetime
import pytz
import requests
from collections import defaultdict
from pathlib import Path

# ── Self-contained mappings (не импорт из HKT) ───────────────────────────
from cxr.mappings import (
    COUNTRY_FLAGS, MAP_C, MAP_RU_CITY, CAP, LOAD_FACTOR,
    DOMESTIC_VN as DOMESTIC, MONTH_NAMES_RU, DAY_NAMES_RU,
)

# ── Constants ────────────────────────────────────────────────────────────
AIRPORT = 'CXR'
ICT_VN = pytz.timezone('Asia/Ho_Chi_Minh')

# Пути относительно cxr/ (когда запускается `python3 -m cxr.collector` из корня)
CXR_DATA_DIR = 'cxr/data'

# ── Environment ──────────────────────────────────────────────────────────
API_KEYS = [
    os.environ.get("AVIATIONSTACK_KEY_1", ""),
    os.environ.get("AVIATIONSTACK_KEY_2", ""),
    os.environ.get("AVIATIONSTACK_KEY_3", ""),
    os.environ.get("AVIATIONSTACK_KEY_4", ""),
]
TG_TOKEN = os.environ.get("TG_TOKEN", "")
TG_CHAT_ID = os.environ.get("TG_CHAT_ID", "")
SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

AVIATIONSTACK_BASE = "https://api.aviationstack.com/v1/flights"


# ── Aviationstack source ─────────────────────────────────────────────────
def get_api_key():
    """Ротация по времени ICT (Vietnam): 4 ключа × 3 окна = 12 runs/day."""
    now = datetime.datetime.now(ICT_VN)
    h = now.hour
    if h < 7:    return API_KEYS[0], 1
    elif h < 13: return API_KEYS[1], 2
    elif h < 19: return API_KEYS[2], 3
    else:        return API_KEYS[3], 4


def fetch_flights_aviationstack(direction, primary_idx=0):
    """Тянет рейсы по IATA=CXR через Aviationstack (с fallback по ключам)."""
    key_param = "arr_iata" if direction == "arrival" else "dep_iata"
    keys_valid = [k for k in API_KEYS if k]
    if not keys_valid:
        print("⚠️ Aviationstack: нет ключей в env")
        return [], 0
    primary = keys_valid[primary_idx] if primary_idx < len(keys_valid) else keys_valid[0]
    keys_to_try = [primary] + [k for k in keys_valid if k != primary]

    for i, k in enumerate(keys_to_try):
        try:
            r = requests.get(
                AVIATIONSTACK_BASE,
                params={"access_key": k, key_param: AIRPORT, "limit": 100},
                timeout=20,
            )
            data = r.json()
            if "error" in data:
                code = data["error"].get("code", "")
                print(f"⚠️ Aviationstack key #{i+1} error: {code} — trying next")
                continue
            r.raise_for_status()
            if i > 0:
                print(f"ℹ️ Aviationstack: fallback to key #{i+1}")
            return data.get("data", []), i + 1
        except Exception as e:
            print(f"⚠️ Aviationstack key #{i+1} exception: {e} — trying next")
            continue

    print("❌ Aviationstack: все ключи упали")
    return [], 0


def analyze_aviationstack(flights, direction):
    """Анализ Aviationstack ответа → {date: {count, pax, countries, stats, flight_list}}."""
    by_date = {}
    for f in flights:
        d = f.get("flight_date")
        if d:
            by_date.setdefault(d, []).append(f)

    res = {}
    for date, fl in by_date.items():
        cnt, pax = 0, 0
        st = {"completed": 0, "upcoming": 0, "cancelled": 0}
        ctry = defaultdict(lambda: {"flights": 0, "pax": 0})
        flight_list = []

        for f in fl:
            dep = (f.get("departure") or {}).get("iata", "")
            arr = (f.get("arrival") or {}).get("iata", "")
            if (direction == "arrival" and dep in DOMESTIC) or (direction == "departure" and arr in DOMESTIC):
                continue
            cnt += 1
            ac_iata = (f.get("aircraft") or {}).get("iata", "")
            flight_pax = int(CAP.get(ac_iata, 180) * LOAD_FACTOR)
            pax += flight_pax
            airport = dep if direction == "arrival" else arr
            country = MAP_C.get(airport, f"Other({airport})")
            if country == "Russia":
                city = MAP_RU_CITY.get(airport, airport)
                ctry[city]["flights"] += 1
                ctry[city]["pax"] += flight_pax
                ctry[city]["country"] = "Russia"
            else:
                ctry[country]["flights"] += 1
                ctry[country]["pax"] += flight_pax

            s = (f.get("flight_status") or "").lower().strip()
            if direction == "departure":
                if s in ("departed", "landed", "diverted", "active", "en route", "en-route",
                         "incidents", "taxi-out", "pushback", "returned", "taxi"):
                    st["completed"] += 1
                elif s in ("scheduled", "taxiing", "boarding", "expected", "estimated", "gate", "holding"):
                    st["upcoming"] += 1
                elif s == "cancelled":
                    st["cancelled"] += 1
            else:
                if s in ("landed", "arrived", "diverted"):
                    st["completed"] += 1
                elif s in ("scheduled", "active", "en route", "en-route", "taxiing", "taxi",
                           "boarding", "expected", "estimated"):
                    st["upcoming"] += 1
                elif s == "cancelled":
                    st["cancelled"] += 1

            fn = (f.get("flight") or {}).get("iata") or (f.get("flight") or {}).get("icao", "")
            airline = (f.get("airline") or {}).get("name", "")
            dep_sched = (f.get("departure") or {}).get("scheduled", "")
            arr_sched = (f.get("arrival") or {}).get("scheduled", "")
            rec = {
                "fn": fn,
                "airline": airline,
                "status": s,
                "pax": flight_pax,
                "dep_time": dep_sched[:16] if dep_sched else "",
                "arr_time": arr_sched[:16] if arr_sched else "",
            }
            if direction == "arrival":
                rec["from"] = dep
                rec["country"] = MAP_C.get(dep, "")
            else:
                rec["to"] = arr
                rec["country"] = MAP_C.get(arr, "")
            flight_list.append(rec)

        res[date] = {"count": cnt, "pax": pax, "countries": dict(ctry),
                     "stats": st, "flight_list": flight_list, "source": "aviationstack"}
    return res


# ── camranh.aero source (official CXR REST API) ─────────────────────────
CAMRANH_API = "https://beta.camranh.aero/api/adapters"

# camranh.aero status → collector category
_CR_COMPLETED = {"arrived", "landed", "departed", "diverted"}
_CR_CANCELLED = {"cancelled", "canceled"}
# всё остальное (On Time, Scheduled, Boarding, ...) → upcoming


def fetch_flights_camranh(date_str):
    """Прямой REST API camranh.aero — отдаёт список рейсов JSON без авторизации.

    Returns (arrivals_list, departures_list) или (None, None) при ошибке.
    Endpoints:
        GET https://beta.camranh.aero/api/adapters/arrivals?date=YYYY-MM-DD
        GET https://beta.camranh.aero/api/adapters/departures?date=YYYY-MM-DD
    """
    def _fetch(kind):
        try:
            r = requests.get(
                f"{CAMRANH_API}/{kind}",
                params={"date": date_str},
                headers={
                    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
                    "Accept": "application/json",
                    "Referer": "https://camranh.aero/",
                },
                timeout=20,
            )
            r.raise_for_status()
            data = r.json()
            if not isinstance(data, list):
                print(f"⚠️ camranh {kind}: unexpected payload type {type(data).__name__}")
                return None
            return data
        except Exception as e:
            print(f"⚠️ camranh {kind} error: {e}")
            return None

    arrivals = _fetch("arrivals")
    departures = _fetch("departures")
    if arrivals is None and departures is None:
        return None, None
    return arrivals or [], departures or []


def analyze_camranh(flights, direction, date_str):
    """Нормализует camranh.aero payload в {date_str: {count, pax, countries, stats, flight_list}}.

    Поля camranh: FlightId, Airline, Route (IATA), City, Country (UPPERCASE),
    ScheduledDatetime "YYYY-MM-DD HH:MM:SS", StatusArr/StatusDep, Registration.
    """
    cnt, pax_total = 0, 0
    st = {"completed": 0, "upcoming": 0, "cancelled": 0}
    ctry = defaultdict(lambda: {"flights": 0, "pax": 0})
    flight_list = []
    seen = set()  # dedup по (fn_normalized, route_iata)

    for f in (flights or []):
        route = (f.get("Route") or "").strip().upper()
        if not route or route in DOMESTIC:
            continue

        fn_raw = (f.get("FlightId") or "").strip()
        fn_norm = fn_raw.replace(" ", "").upper()
        dedup_key = (fn_norm, route)
        if dedup_key in seen:
            continue
        seen.add(dedup_key)

        cnt += 1
        # camranh не отдаёт IATA самолёта — fallback на 180 мест
        flight_pax = int(180 * LOAD_FACTOR)
        pax_total += flight_pax

        # Country: сначала MAP_C по IATA (даёт корректные Title-case имена для флагов),
        # fallback на .title() от camranh Country
        country = MAP_C.get(route) or (f.get("Country") or "").title() or f"Other({route})"
        if country == "Russia":
            city = MAP_RU_CITY.get(route, (f.get("City") or "").title() or route)
            ctry[city]["flights"] += 1
            ctry[city]["pax"] += flight_pax
            ctry[city]["country"] = "Russia"
        else:
            ctry[country]["flights"] += 1
            ctry[country]["pax"] += flight_pax

        status_field = "StatusArr" if direction == "arrival" else "StatusDep"
        s = (f.get(status_field) or "").lower().strip()
        if s in _CR_COMPLETED:
            st["completed"] += 1
        elif s in _CR_CANCELLED:
            st["cancelled"] += 1
        else:
            st["upcoming"] += 1

        # ScheduledDatetime "2026-04-20 00:35:00" → ISO-16char "2026-04-20T00:35"
        sched_raw = (f.get("ScheduledDatetime") or "")[:16].replace(" ", "T")
        est_raw = (f.get("EstimatedTime") or "")[:16].replace(" ", "T")
        # EstimatedTime иногда "1753-01-01 12:00:00" (sentinel) — игнорим
        est_iso = est_raw if est_raw.startswith("20") else ""

        airline = (f.get("Airline") or "").title()

        rec = {
            "fn": fn_raw,
            "airline": airline,
            "status": s,
            "pax": flight_pax,
            "aircraft": (f.get("Registration") or ""),
            "dep_time": sched_raw if direction == "departure" else (est_iso or ""),
            "arr_time": sched_raw if direction == "arrival"   else (est_iso or ""),
        }
        if direction == "arrival":
            rec["from"] = route
            rec["country"] = country
        else:
            rec["to"] = route
            rec["country"] = country
        flight_list.append(rec)

    return {
        date_str: {
            "count": cnt,
            "pax": pax_total,
            "countries": dict(ctry),
            "stats": st,
            "flight_list": flight_list,
            "source": "camranh",
        }
    }


# ── Telegram ─────────────────────────────────────────────────────────────
def send_telegram(text):
    """Шлёт HTML-сообщение в Telegram с префиксом [CXR]."""
    if not TG_TOKEN or not TG_CHAT_ID:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            json={"chat_id": TG_CHAT_ID, "text": "[CXR] " + text, "parse_mode": "HTML"},
            timeout=10,
        )
    except Exception:
        pass


# ── Supabase (cxr_flight_daily) ──────────────────────────────────────────
def save_to_supabase(date_str, arrivals, departures):
    """Upsert в cxr_flight_daily (PK=date)."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        return
    try:
        payload = {
            "date": date_str,
            "arrivals_count": arrivals.get("count", 0),
            "arrivals_pax": arrivals.get("pax", 0),
            "departures_count": departures.get("count", 0),
            "departures_pax": departures.get("pax", 0),
            "arrivals_countries": arrivals.get("countries", {}),
            "departures_countries": departures.get("countries", {}),
            "updated_at": datetime.datetime.now(ICT_VN).isoformat(),
        }
        r = requests.post(
            f"{SUPABASE_URL}/rest/v1/cxr_flight_daily",
            headers={
                "apikey": SUPABASE_KEY,
                "Authorization": f"Bearer {SUPABASE_KEY}",
                "Content-Type": "application/json",
                "Prefer": "resolution=merge-duplicates",
            },
            json=payload,
            timeout=15,
        )
        if r.status_code in (200, 201):
            print(f"✅ Supabase cxr_flight_daily: {date_str} сохранён.")
        else:
            print(f"⚠️ Supabase error {r.status_code}: {r.text[:200]}")
    except Exception as e:
        print(f"⚠️ Supabase exception: {e}")


# ── Period aggregation ───────────────────────────────────────────────────
def load_period_stats(today_str, days):
    """Читает cxr/data/accumulated_*.json за последние `days` дней."""
    tot_a = {"count": 0, "pax": 0, "countries": {}}
    tot_d = {"count": 0, "pax": 0, "countries": {}}
    daily = {}

    for i in range(days):
        dt = (datetime.datetime.fromisoformat(today_str) - datetime.timedelta(days=i)).date()
        dt_str = dt.isoformat()
        fp = Path(CXR_DATA_DIR) / f"accumulated_{dt_str}.json"
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
            weeks[key] = {"key": key, "label": f"Нед {iso_week} ({MONTH_NAMES_RU[dt.month]})",
                          "arrivals": 0, "departures": 0, "arrivals_by": {}, "departures_by": {}}
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
            months[key] = {"key": key, "label": f"{MONTH_NAMES_RU[dt.month]} {dt.year}",
                           "arrivals": 0, "departures": 0, "arrivals_by": {}, "departures_by": {}}
        months[key]["arrivals"] += daily[date_str]["arrivals"]
        months[key]["departures"] += daily[date_str]["departures"]
        _merge_by(months[key]["arrivals_by"], daily[date_str].get("arrivals_by", {}))
        _merge_by(months[key]["departures_by"], daily[date_str].get("departures_by", {}))
    return [v for _, v in sorted(months.items())]


def fmt_top10(ctry):
    lines = []
    sorted_c = sorted(ctry.items(), key=lambda x: x[1]["flights"], reverse=True)[:10]
    for i, (c, v) in enumerate(sorted_c, 1):
        flag = COUNTRY_FLAGS.get(v.get("country", c), "")
        lines.append(f"{i}. {flag} {c}: {v['flights']} рейсов (~{v['pax']:,} пасс.)")
    return "\n".join(lines) if lines else "Нет данных"


# ── Main orchestration ───────────────────────────────────────────────────
def run():
    now = datetime.datetime.now(ICT_VN)
    today = now.date().isoformat()

    # ── Source #1: camranh.aero official REST API (primary) ──────────────
    source = "aviationstack"
    a_res, d_res = None, None

    print("📡 Пробуем camranh.aero REST API...")
    a_raw, d_raw = fetch_flights_camranh(today)
    if a_raw is not None and d_raw is not None and (len(a_raw) > 0 or len(d_raw) > 0):
        a_res = analyze_camranh(a_raw, "arrival",   today)
        d_res = analyze_camranh(d_raw, "departure", today)
        source = "camranh"
        print(f"✅ camranh: {len(a_raw)} arrivals raw, {len(d_raw)} departures raw")

    if a_res is None or d_res is None:
        # ── Source #2: Aviationstack (fallback) ──────────────────────────
        key, knum = get_api_key()
        a_fl, _ = fetch_flights_aviationstack("arrival",   knum - 1)
        d_fl, _ = fetch_flights_aviationstack("departure", knum - 1)
        a_res = analyze_aviationstack(a_fl, "arrival")
        d_res = analyze_aviationstack(d_fl, "departure")
        print(f"ℹ️ Using AviationStack fallback (key #{knum})")

    a_cur = a_res.get(today, {"count": 0, "pax": 0, "countries": {}, "stats": {}, "flight_list": []})
    d_cur = d_res.get(today, {"count": 0, "pax": 0, "countries": {}, "stats": {}, "flight_list": []})

    # ── Accumulate today's data (merge with dedup) ───────────────────────
    Path(CXR_DATA_DIR).mkdir(parents=True, exist_ok=True)
    acc_file = Path(CXR_DATA_DIR) / f"accumulated_{today}.json"
    acc = {"date": today,
           "arrivals":   {"count": 0, "pax": 0, "countries": {}},
           "departures": {"count": 0, "pax": 0, "countries": {}}}
    if acc_file.exists():
        try:
            acc = json.loads(acc_file.read_text())
        except Exception:
            pass

    def _norm_fn(fn):
        return (fn or "").replace(" ", "").upper()

    # Arrivals merge
    cur_arr_by_fn = {_norm_fn(r.get("fn", "")): r for r in a_cur.get("flight_list", []) if r.get("fn")}
    existing_arr = acc.get("arrivals_list", [])
    for r in existing_arr:
        fresh = cur_arr_by_fn.get(_norm_fn(r.get("fn", "")))
        if fresh:
            if not r.get("airline") and fresh.get("airline"): r["airline"] = fresh["airline"]
            if not r.get("arr_time") and fresh.get("arr_time"): r["arr_time"] = fresh["arr_time"]
            if not r.get("dep_time") and fresh.get("dep_time"): r["dep_time"] = fresh["dep_time"]
            r["status"] = fresh.get("status", r.get("status", ""))
    seen_arr = {(r.get("from", ""), r.get("arr_time", "")) for r in existing_arr if r.get("arr_time")}
    seen_arr_fns = {_norm_fn(r["fn"]) for r in existing_arr if r.get("fn")}
    new_arr = []
    for r in a_cur.get("flight_list", []):
        key = (r.get("from", ""), r.get("arr_time", ""))
        if _norm_fn(r.get("fn", "")) in seen_arr_fns: continue
        if r.get("arr_time") and key in seen_arr: continue
        new_arr.append(r)
        seen_arr.add(key)
        seen_arr_fns.add(_norm_fn(r.get("fn", "")))
    acc["arrivals_list"] = existing_arr + new_arr

    # Auto-update stale "scheduled"/"active" → "landed"
    now_tz = datetime.datetime.now(ICT_VN)
    for r in acc["arrivals_list"]:
        if r.get("status") in ("scheduled", "active") and r.get("arr_time"):
            try:
                arr_dt = datetime.datetime.fromisoformat(r["arr_time"])
                if arr_dt.tzinfo is None:
                    arr_dt = ICT_VN.localize(arr_dt)
                if arr_dt < now_tz - datetime.timedelta(minutes=45):
                    r["status"] = "landed"
            except Exception:
                pass

    # Departures merge
    seen_dep = {(r.get("to", ""), r.get("dep_time", "")) for r in acc.get("departures_list", []) if r.get("dep_time")}
    seen_dep_fns = {_norm_fn(r["fn"]) for r in acc.get("departures_list", []) if r.get("fn")}
    new_dep = []
    for r in d_cur.get("flight_list", []):
        key = (r.get("to", ""), r.get("dep_time", ""))
        if _norm_fn(r.get("fn", "")) in seen_dep_fns: continue
        if r.get("dep_time") and key in seen_dep: continue
        new_dep.append(r)
        seen_dep.add(key)
        seen_dep_fns.add(_norm_fn(r.get("fn", "")))
    acc["departures_list"] = acc.get("departures_list", []) + new_dep

    for r in acc["departures_list"]:
        if r.get("status") == "scheduled" and r.get("dep_time"):
            try:
                dep_dt = datetime.datetime.fromisoformat(r["dep_time"])
                if dep_dt.tzinfo is None:
                    dep_dt = ICT_VN.localize(dep_dt)
                if dep_dt < now_tz - datetime.timedelta(minutes=45):
                    r["status"] = "departed"
            except Exception:
                pass

    # Recompute countries from dedup'd lists
    def _recompute_countries(flight_list, airport_field):
        ctry = {}
        for r in flight_list:
            airport = r.get(airport_field, "")
            country_key = MAP_C.get(airport, "")
            if not country_key:
                continue
            if country_key == "Russia":
                key = MAP_RU_CITY.get(airport, airport)
                if key not in ctry:
                    ctry[key] = {"flights": 0, "pax": 0, "country": "Russia"}
                ctry[key]["flights"] += 1
                ctry[key]["pax"] += r.get("pax", 0)
            else:
                if country_key not in ctry:
                    ctry[country_key] = {"flights": 0, "pax": 0}
                ctry[country_key]["flights"] += 1
                ctry[country_key]["pax"] += r.get("pax", 0)
        return ctry

    arr_c = _recompute_countries(acc["arrivals_list"], "from")
    acc["arrivals"] = {
        "count": sum(v["flights"] for v in arr_c.values()),
        "pax":   sum(v["pax"]     for v in arr_c.values()),
        "countries": arr_c,
    }
    dep_c = _recompute_countries(acc["departures_list"], "to")
    acc["departures"] = {
        "count": sum(v["flights"] for v in dep_c.values()),
        "pax":   sum(v["pax"]     for v in dep_c.values()),
        "countries": dep_c,
    }

    acc_file.write_text(json.dumps(acc, indent=2, ensure_ascii=False))

    # Supabase
    save_to_supabase(today, acc.get("arrivals", {}), acc.get("departures", {}))

    a_acc = acc.get("arrivals",   {"count": 0, "pax": 0, "countries": {}})
    d_acc = acc.get("departures", {"count": 0, "pax": 0, "countries": {}})

    # Period stats
    w_a, w_d, w_daily = load_period_stats(today, 7)
    m_a, m_d, m_daily = load_period_stats(today, 30)
    q_a, q_d, q_daily = load_period_stats(today, 90)
    h_a, h_d, h_daily = load_period_stats(today, 180)
    y_a, y_d, y_daily = load_period_stats(today, 365)

    yesterday = (datetime.datetime.fromisoformat(today) - datetime.timedelta(days=1)).date().isoformat()
    yest_file = Path(CXR_DATA_DIR) / f"accumulated_{yesterday}.json"
    if yest_file.exists():
        yest_data = json.loads(yest_file.read_text())
        v_a = yest_data.get("arrivals",   {"count": 0, "pax": 0, "countries": {}})
        v_d = yest_data.get("departures", {"count": 0, "pax": 0, "countries": {}})
        v_arrivals_list   = yest_data.get("arrivals_list", [])
        v_departures_list = yest_data.get("departures_list", [])
    else:
        v_a = {"count": 0, "pax": 0, "countries": {}}
        v_d = {"count": 0, "pax": 0, "countries": {}}
        v_arrivals_list, v_departures_list = [], []

    # Telegram
    def block(title, a, d):
        lines = [f"<b>{title}</b>",
                 f"✈️ Всего: {a['count']+d['count']} | 👥 ~{a['pax']+d['pax']:,}",
                 "<b>🛬 ПРИЛЁТЫ (Топ-10)</b>", fmt_top10(a['countries']),
                 "<b>🛫 ВЫЛЕТЫ (Топ-10)</b>", fmt_top10(d['countries']), ""]
        return "\n".join(lines)

    src_label = source.upper()
    msg = f"🌊 <b>CAM RANH CXR STATISTICS</b>\n🕒 {now.strftime('%H:%M')} | {src_label}\n━━━━━━━━━━━━━━━━━━━━\n"
    msg += block("🔴 СЕГОДНЯ (накопл.)", a_acc, d_acc)
    msg += block("📊 НЕДЕЛЯ (7 дней)", w_a, w_d)
    msg += block("📈 МЕСЯЦ (30 дней)", m_a, m_d)
    msg += "📍 <i>CXR | Cam Ranh International Airport · Вьетнам</i>"

    print(msg)
    send_telegram(msg)

    # Dashboard JSON
    def fmt_all(ctry):
        sorted_c = sorted(ctry.items(), key=lambda x: x[1]["flights"], reverse=True)
        result = []
        for c, v in sorted_c:
            item = {"name": c, "flights": v["flights"], "pax": v["pax"]}
            if "country" in v:
                item["country"] = v["country"]
            result.append(item)
        return result

    def to_web_fmt(arr, dep, breakdown=None, arrivals_list=None, departures_list=None):
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

    available_dates = sorted([
        p.stem.replace("accumulated_", "")
        for p in Path(CXR_DATA_DIR).glob("accumulated_*.json")
    ])

    dashboard_data = {
        "updated":          now.isoformat(),
        "available_dates":  available_dates,
        "yesterday": to_web_fmt(v_a, v_d, arrivals_list=v_arrivals_list, departures_list=v_departures_list),
        "today":     to_web_fmt(a_acc, d_acc, arrivals_list=acc.get("arrivals_list", []), departures_list=acc.get("departures_list", [])),
        "week":      to_web_fmt(w_a, w_d, {"by_days":   make_by_days(w_daily)}),
        "month":     to_web_fmt(m_a, m_d, {"by_weeks":  make_by_weeks(m_daily)}),
        "quarter":   to_web_fmt(q_a, q_d, {"by_months": make_by_months(q_daily)}),
        "halfyear":  to_web_fmt(h_a, h_d, {"by_months": make_by_months(h_daily)}),
        "year":      to_web_fmt(y_a, y_d, {"by_months": make_by_months(y_daily)}),
    }
    Path(CXR_DATA_DIR, "dashboard.json").write_text(json.dumps(dashboard_data, ensure_ascii=False, indent=2))
    print(f"✅ {CXR_DATA_DIR}/dashboard.json обновлён.")


if __name__ == "__main__":
    run()
