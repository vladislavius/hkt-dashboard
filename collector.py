"""HKT (Phuket International Airport) flight data collector.

Pipeline:
1. GTT GraphQL via Playwright (local Mac, residential IP) — primary
2. GTT via CapSolver/2captcha Turnstile token — fallback
3. Aviationstack API — final fallback

Общий код (маппинги, analyze_aviationstack, Telegram, Supabase,
период-агрегации) вынесен в `core/`. Здесь остаётся HKT-specific:
GTT pipeline, Turnstile solver, russian-transit estimation, run() orchestrator.
"""
import os
import json
import datetime
import pytz
import requests
from pathlib import Path
from collections import Counter, defaultdict

from core.mappings import CAP, LOAD_FACTOR, MAP_C, MAP_RU_CITY, DOMESTIC_TH as DOMESTIC
from core.aviation import fetch_flights_aviationstack, analyze_aviationstack
from core.telegram import send_telegram as _send_tg
from core.supabase import save_daily
from core.aggregate_periods import load_period_stats, make_by_days, make_by_weeks, make_by_months
from core.formatters import fmt_top10


# ── Environment ──────────────────────────────────────────────────────────
API_KEYS = [
    os.environ["AVIATIONSTACK_KEY_1"],
    os.environ["AVIATIONSTACK_KEY_2"],
    os.environ["AVIATIONSTACK_KEY_3"],
    os.environ["AVIATIONSTACK_KEY_4"],
]
TG_TOKEN = os.environ.get("TG_TOKEN", "")
TG_CHAT_ID = os.environ.get("TG_CHAT_ID", "")
SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")
TWOCAPTCHA_KEY = os.environ.get("TWOCAPTCHA_KEY", "")
CAPSOLVER_KEY = os.environ.get("CAPSOLVER_KEY", "")

# ── HKT constants ────────────────────────────────────────────────────────
AIRPORT = 'HKT'
ICT = pytz.timezone('Asia/Bangkok')

# ── GTT GraphQL (AOT official flight board) ──────────────────────────────
GTT_ENDPOINT = "https://gtt-prod.sawasdeebyaot.com/graphql"
GTT_QUERY = """query HKTFlightBoard($site: String!, $start: String!, $end: String!) {
  arrivals: webAOTFetchFlightBoard(site: $site, type: "A", schedule_start: $start, schedule_end: $end) {
    success message code
    payload {
      flights {
        number
        flight_departure { scheduled_at flight_status }
        flight_arrival   { scheduled_at flight_status }
        origin_airport      { iata_code city }
        destination_airport { iata_code city }
        airline  { iata name }
        aircraft { iata name }
        flight_status
      }
    }
  }
  departures: webAOTFetchFlightBoard(site: $site, type: "D", schedule_start: $start, schedule_end: $end) {
    success message code
    payload {
      flights {
        number
        flight_departure { scheduled_at flight_status }
        flight_arrival   { scheduled_at flight_status }
        origin_airport      { iata_code city }
        destination_airport { iata_code city }
        airline  { iata name }
        aircraft { iata name }
        flight_status
      }
    }
  }
}"""

_GTT_QUERY_ONE = """query HKTFlightBoardOne($site: String!, $type: String!, $start: String!, $end: String!) {
  webAOTFetchFlightBoard(site: $site, type: $type, schedule_start: $start, schedule_end: $end) {
    success message code
    payload {
      flights {
        number
        flight_departure { scheduled_at flight_status }
        flight_arrival   { scheduled_at flight_status }
        origin_airport      { iata_code city }
        destination_airport { iata_code city }
        airline  { iata name }
        aircraft { iata name }
        flight_status
      }
    }
  }
}"""

# ── Russian transit estimation (HKT-specific) ────────────────────────────
HKT_RUSSIAN_SHARE = 0.38   # Пхукет получает ~38% россиян, въезжающих в Таиланд
                            # Источник: TAT/MOTS годовые отчёты 2023-2024. Проверять ежегодно.

TRANSIT_HUBS = ['Turkey', 'China', 'India']  # хабы с транзитным российским трафиком

BKK_ANNUAL_RUSSIANS = 150_000
BKK_SEASONAL = {
    1: 1.40,  # Январь: пик после НГ
    2: 1.25,
    3: 1.00,
    4: 0.80,
    5: 0.55,
    6: 0.45,
    7: 0.60,
    8: 0.70,
    9: 0.40,
    10: 0.85,
    11: 1.20,
    12: 1.60,
}
_BKK_NORM = 12 / sum(BKK_SEASONAL.values())


def load_tat_stats():
    """Загружает data/tat_stats.json. Возвращает {} при ошибке."""
    p = Path("data/tat_stats.json")
    if not p.exists():
        print("Warning: data/tat_stats.json not found — russia transit will be 0")
        return {}
    try:
        return json.loads(p.read_text()).get("monthly", {})
    except Exception as e:
        print(f"Warning: Failed to read tat_stats.json: {e}")
        return {}


def tat_monthly_avg(tat_monthly, ref_date, lookback=3):
    """Среднемесячное кол-во россиян в Таиланде за lookback месяцев."""
    counts = []
    for i in range(1, lookback + 4):
        m = ref_date.replace(day=1) - datetime.timedelta(days=28 * i)
        key = f"{m.year}-{m.month:02d}"
        if key in tat_monthly:
            counts.append(tat_monthly[key])
        if len(counts) == lookback:
            break
    if not counts:
        return 130000.0
    return sum(counts) / len(counts)


def calc_bkk_transit(period_days, ref_date):
    month = ref_date.month
    monthly_pax = (BKK_ANNUAL_RUSSIANS / 12) * BKK_SEASONAL[month] * _BKK_NORM
    daily_pax = monthly_pax / 30.44
    pax = int(round(daily_pax * period_days))
    flights = max(1, round(pax / (180 * LOAD_FACTOR)))
    return {"pax": pax, "flights": flights, "estimated": True}


def calc_russian_transit(period_days, countries_arrivals, tat_monthly, ref_date):
    """Оценивает общий транзитный поток россиян на Пхукет (TAT hubs + BKK)."""
    avg_flight_pax = 180 * LOAD_FACTOR

    monthly_avg = tat_monthly_avg(tat_monthly, ref_date)
    daily_hkt = (monthly_avg * HKT_RUSSIAN_SHARE) / 30.44
    period_hkt_russians = daily_hkt * period_days

    direct_pax = sum(
        v.get("pax", 0)
        for c, v in countries_arrivals.items()
        if v.get("country") == "Russia" or c == "Russia"
    )
    tat_pax = int(max(0.0, period_hkt_russians - direct_pax))

    bkk = calc_bkk_transit(period_days, ref_date)
    bkk_pax = bkk["pax"]

    total_pax = tat_pax + bkk_pax
    total_flights = max(1, round(total_pax / avg_flight_pax))

    return {
        "pax": total_pax,
        "flights": total_flights,
        "tat_pax": tat_pax,
        "bkk_pax": bkk_pax,
        "estimated": True,
    }


# ── Aviationstack key rotation by time-of-day ────────────────────────────
def get_api_key():
    """12 runs/day ÷ 4 keys = 3 runs/key/day = ~90 calls/key/month (limit 400)."""
    now = datetime.datetime.now(ICT)
    h = now.hour
    if h < 7:    return API_KEYS[0], 1
    elif h < 13: return API_KEYS[1], 2
    elif h < 19: return API_KEYS[2], 3
    else:        return API_KEYS[3], 4


def send_telegram(text):
    """Шлёт в Telegram с префиксом [HKT]."""
    _send_tg(text, TG_TOKEN, TG_CHAT_ID)


# ── Turnstile captcha solvers (HKT-specific sitekey) ─────────────────────
_TURNSTILE_SITEKEY = "0x4AAAAAACVJKKHJ8u9nXinM"
_TURNSTILE_PAGE_URL = "https://phuket.airportthai.co.th/flight?type=a"

_POPUP = (
    "button:has-text('Accept'), button:has-text('ACCEPT'), "
    "button:has-text('ยอมรับ'), button:has-text('Submit'), "
    "button:has-text('I Agree'), .btn-accept, #accept-btn"
)


def _solve_via_capsolver():
    """CapSolver — specialised Cloudflare solver, higher Turnstile success rate."""
    payload = {
        "clientKey": CAPSOLVER_KEY,
        "task": {
            "type":    "AntiTurnstileTaskProxyLess",
            "websiteURL": _TURNSTILE_PAGE_URL,
            "websiteKey": _TURNSTILE_SITEKEY,
        },
    }
    r = requests.post("https://api.capsolver.com/createTask", json=payload, timeout=15)
    task_id = r.json().get("taskId")
    if not task_id:
        raise RuntimeError(f"CapSolver createTask failed: {r.text[:200]}")

    for _ in range(40):
        import time
        time.sleep(3)
        r2 = requests.post(
            "https://api.capsolver.com/getTaskResult",
            json={"clientKey": CAPSOLVER_KEY, "taskId": task_id},
            timeout=10,
        )
        res = r2.json()
        if res.get("status") == "ready":
            return res["solution"]["token"]
        if res.get("status") == "failed":
            raise RuntimeError(f"CapSolver task failed: {res}")
    raise RuntimeError("CapSolver timeout")


def get_turnstile_token():
    """Solve Cloudflare Turnstile via CapSolver → 2captcha fallback."""
    if CAPSOLVER_KEY:
        print("🔐 Solving Turnstile via CapSolver...")
        try:
            token = _solve_via_capsolver()
            if token:
                print(f"✅ Turnstile token obtained ({len(token)} chars)")
                return token
        except Exception as e:
            print(f"⚠️ CapSolver error: {e} — trying 2captcha fallback")

    if TWOCAPTCHA_KEY:
        print("🔐 Solving Turnstile via 2captcha...")
        try:
            from twocaptcha import TwoCaptcha
            result = TwoCaptcha(TWOCAPTCHA_KEY).turnstile(
                sitekey=_TURNSTILE_SITEKEY,
                url=_TURNSTILE_PAGE_URL,
            )
            token = result.get("code", "")
            if token:
                print(f"✅ Turnstile token obtained ({len(token)} chars)")
                return token
        except Exception as e:
            print(f"⚠️ 2captcha error: {e}")

    print("⚠️ No captcha solver configured — skipping GTT source")
    return None


# ── GTT fetchers (HKT-specific) ──────────────────────────────────────────
def fetch_flights_gtt_playwright(date_str):
    """Open browser, navigate to AOT flight board for type=a then type=d.

    Each page load auto-solves Turnstile and fires a GTT request. We intercept
    each response with expect_response() — no token extraction needed.
    Returns (arrivals_list, departures_list) or raises on failure.
    """
    from playwright.sync_api import sync_playwright

    def _flights_from_response(data, label):
        board = (data.get("data") or {}).get("webAOTFetchFlightBoard") or {}
        if not board.get("success"):
            raise RuntimeError(f"GTT {label}: {board.get('message')}")
        return (board.get("payload") or {}).get("flights") or []

    with sync_playwright() as p:
        try:
            browser = p.chromium.launch(
                channel="chrome",
                headless=False,
                args=["--disable-blink-features=AutomationControlled"],
            )
        except Exception:
            browser = p.chromium.launch(
                headless=False,
                args=["--no-sandbox", "--disable-blink-features=AutomationControlled"],
            )
        ctx = browser.new_context(viewport={"width": 1280, "height": 800})
        page = ctx.new_page()
        page.add_init_script("""
            Object.defineProperty(navigator,'webdriver',{get:()=>undefined});
            window.chrome={runtime:{}};
        """)

        results = {}
        for flight_type, label in [("a", "arrivals"), ("d", "departures")]:
            url = f"https://phuket.airportthai.co.th/flight?type={flight_type}"
            with page.expect_response(
                lambda r: "gtt-prod.sawasdeebyaot.com/graphql" in r.url,
                timeout=60000,
            ) as resp_info:
                page.goto(url, wait_until="domcontentloaded", timeout=30000)
                try:
                    page.wait_for_selector(_POPUP, timeout=5000)
                    page.click(_POPUP)
                    print("🖱️ Dismissed consent popup")
                except Exception:
                    pass

            data = resp_info.value.json()
            results[label] = _flights_from_response(data, label)
            print(f"✅ GTT {label}: {len(results[label])} flights")

        browser.close()

    return results["arrivals"], results["departures"]


def fetch_flights_gtt_one(token, date_str, flight_type):
    """Fetch a single direction (flight_type='A' or 'D') with one token."""
    schedule_start = f"{date_str} 00:00:00"
    schedule_end   = f"{date_str} 23:59:59"
    try:
        resp = requests.post(
            GTT_ENDPOINT,
            json={
                "query": _GTT_QUERY_ONE,
                "variables": {
                    "site":  "hkt",
                    "type":  flight_type,
                    "start": schedule_start,
                    "end":   schedule_end,
                },
            },
            headers={
                "Content-Type":    "application/json",
                "Authorization":   token,
                "api-name":        "WebAOTFetchFlightBoard",
                "origin":          "https://phuket.airportthai.co.th",
                "referer":         "https://phuket.airportthai.co.th/",
                "accept":          "*/*",
                "accept-language": "en-US,en;q=0.9",
                "user-agent":      (
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
                ),
            },
            timeout=20,
        )
        data = resp.json()
        if data.get("errors"):
            print(f"⚠️ GTT {flight_type} errors: {data['errors']}")
            return None
        board = (data.get("data") or {}).get("webAOTFetchFlightBoard") or {}
        if not board.get("success"):
            print(f"⚠️ GTT {flight_type} not successful: {board.get('message')}")
            return None
        return (board.get("payload") or {}).get("flights") or []
    except Exception as e:
        print(f"⚠️ GTT {flight_type} fetch error: {e}")
        return None


def fetch_flights_gtt(token, date_str):
    """Fetch arrivals AND departures in a single GraphQL request."""
    schedule_start = f"{date_str} 00:00:00"
    schedule_end   = f"{date_str} 23:59:59"
    try:
        resp = requests.post(
            GTT_ENDPOINT,
            json={
                "query": GTT_QUERY,
                "variables": {
                    "site":  "hkt",
                    "start": schedule_start,
                    "end":   schedule_end,
                },
            },
            headers={
                "Content-Type":    "application/json",
                "Authorization":   token,
                "api-name":        "WebAOTFetchFlightBoard",
                "origin":          "https://phuket.airportthai.co.th",
                "referer":         "https://phuket.airportthai.co.th/",
                "accept":          "*/*",
                "accept-language": "en-US,en;q=0.9",
                "user-agent":      (
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
                ),
                "sec-fetch-mode":  "cors",
                "sec-fetch-site":  "cross-site",
                "sec-fetch-dest":  "empty",
            },
            timeout=20,
        )
        data = resp.json()
        if data.get("errors"):
            print(f"⚠️ GTT GraphQL errors: {data['errors']}")
            return None, None
        d = data.get("data") or {}
        arr_board = d.get("arrivals") or {}
        dep_board = d.get("departures") or {}
        if not arr_board.get("success") or not dep_board.get("success"):
            print(f"⚠️ GTT not successful: arr={arr_board.get('message')} dep={dep_board.get('message')}")
            return None, None
        a_flights = (arr_board.get("payload") or {}).get("flights") or []
        d_flights = (dep_board.get("payload") or {}).get("flights") or []
        return a_flights, d_flights
    except Exception as e:
        print(f"⚠️ GTT fetch error: {e}")
        return None, None


# GTT flight_status → collector category
_GTT_COMPLETED = {"departed", "landed", "arrived", "diverted", "completed"}
_GTT_UPCOMING  = {"scheduled", "on-time", "expected", "estimated",
                  "boarding", "gate", "check-in", "delay", "delayed"}
_GTT_CANCELLED = {"cancelled", "canceled"}


def analyze_gtt(flights, direction, date_str):
    """Convert GTT flight list into the same structure as analyze_aviationstack().

    HKT-specific: dedupes codeshare flights by (normalized fn, airport).
    """
    cnt, pax_total = 0, 0
    st   = {"completed": 0, "upcoming": 0, "cancelled": 0}
    ctry = defaultdict(lambda: {"flights": 0, "pax": 0})
    flight_list = []
    seen_flights = set()

    for f in (flights or []):
        if direction == "arrival":
            airport_iata = (f.get("origin_airport") or {}).get("iata_code", "")
            status_raw = (f.get("flight_arrival") or {}).get("flight_status") or f.get("flight_status", "")
        else:
            airport_iata = (f.get("destination_airport") or {}).get("iata_code", "")
            status_raw = (f.get("flight_departure") or {}).get("flight_status") or f.get("flight_status", "")

        if not airport_iata:
            continue
        if airport_iata in DOMESTIC:
            continue

        fn_raw = f.get("number", "")
        fn_norm = fn_raw.replace(" ", "").upper()
        dedup_key = (fn_norm, airport_iata)
        if dedup_key in seen_flights:
            continue
        seen_flights.add(dedup_key)

        cnt += 1
        ac_iata = (f.get("aircraft") or {}).get("iata", "")
        flight_pax = int(CAP.get(ac_iata, 180) * LOAD_FACTOR)
        pax_total += flight_pax

        country = MAP_C.get(airport_iata, f"Other({airport_iata})")
        if country == "Russia":
            city = MAP_RU_CITY.get(airport_iata, airport_iata)
            ctry[city]["flights"] += 1
            ctry[city]["pax"]     += flight_pax
            ctry[city]["country"]  = "Russia"
        else:
            ctry[country]["flights"] += 1
            ctry[country]["pax"]     += flight_pax

        s = status_raw.lower().strip()
        if s in _GTT_COMPLETED:
            st["completed"] += 1
        elif s in _GTT_CANCELLED:
            st["cancelled"] += 1
        else:
            st["upcoming"] += 1

        dep_t = (f.get("flight_departure") or {}).get("scheduled_at", "")
        arr_t = (f.get("flight_arrival")   or {}).get("scheduled_at", "")
        fn      = f.get("number", "")
        airline = (f.get("airline") or {}).get("name", "")

        rec = {
            "fn":       fn,
            "airline":  airline,
            "status":   s,
            "pax":      flight_pax,
            "aircraft": ac_iata,
            "dep_time": dep_t[:16] if dep_t else "",
            "arr_time": arr_t[:16] if arr_t else "",
        }
        if direction == "arrival":
            rec["from"]    = airport_iata
            rec["country"] = MAP_C.get(airport_iata, "")
        else:
            rec["to"]      = airport_iata
            rec["country"] = MAP_C.get(airport_iata, "")
        flight_list.append(rec)

    return {
        date_str: {
            "count":       cnt,
            "pax":         pax_total,
            "countries":   dict(ctry),
            "stats":       st,
            "flight_list": flight_list,
            "source":      "gtt",
        }
    }


# ── Thin wrappers to match old API ────────────────────────────────────────
def fetch_flights(direction, api_key):
    """Backward-compat shim — HKT uses Aviationstack with IATA=HKT."""
    primary_idx = API_KEYS.index(api_key) if api_key in API_KEYS else 0
    return fetch_flights_aviationstack(AIRPORT, direction, API_KEYS, primary_idx)


def analyze(flights, direction):
    """Backward-compat shim — analyze Aviationstack flights for HKT."""
    return analyze_aviationstack(flights, direction, DOMESTIC)


def save_to_supabase(date_str, arrivals, departures):
    """Backward-compat shim — saves HKT to flight_daily."""
    save_daily(AIRPORT, date_str, arrivals, departures, SUPABASE_URL, SUPABASE_KEY, tz='Asia/Bangkok')


# ── Main orchestration ───────────────────────────────────────────────────
def run():
    now = datetime.datetime.now(ICT)
    today = now.date().isoformat()
    today_date = now.date()
    tat_monthly = load_tat_stats()

    # ── Try GTT GraphQL (primary source) ─────────────────────────────────
    source = "aviationstack"
    a_fl_gtt = d_fl_gtt = None

    # 1. Playwright (local Mac): makes fetch from within browser — CF token bound to context
    try:
        import playwright  # noqa: F401
        print("🔐 Fetching GTT via Playwright (in-browser fetch)...")
        try:
            a_fl_gtt, d_fl_gtt = fetch_flights_gtt_playwright(today)
            print(f"✅ GTT Playwright: {len(a_fl_gtt)} arrivals, {len(d_fl_gtt)} departures")
        except Exception as e:
            print(f"⚠️ GTT Playwright error: {e} — trying captcha service")
    except ImportError:
        pass  # Playwright not installed (production env)

    # 2. Captcha service: token is single-use → two tokens, one per direction
    if a_fl_gtt is None:
        arr_token = get_turnstile_token()
        if arr_token:
            a_fl_gtt = fetch_flights_gtt_one(arr_token, today, "A")
            dep_token = get_turnstile_token()
            if dep_token:
                d_fl_gtt = fetch_flights_gtt_one(dep_token, today, "D")

    if a_fl_gtt is not None and d_fl_gtt is not None:
        a_res = analyze_gtt(a_fl_gtt, "arrival",   today)
        d_res = analyze_gtt(d_fl_gtt, "departure", today)
        source = "gtt"
    else:
        # ── Fallback: AviationStack ─────────────────────────────────────
        key, knum = get_api_key()
        a_fl, _ = fetch_flights_aviationstack(AIRPORT, "arrival",   API_KEYS, knum - 1)
        d_fl, _ = fetch_flights_aviationstack(AIRPORT, "departure", API_KEYS, knum - 1)
        a_res = analyze_aviationstack(a_fl, "arrival",   DOMESTIC)
        d_res = analyze_aviationstack(d_fl, "departure", DOMESTIC)
        print(f"ℹ️ Using AviationStack fallback (key #{knum})")

    knum = 0 if source == "gtt" else locals().get("knum", "?")

    a_cur = a_res.get(today, {"count":0, "pax":0, "countries":{}, "stats":{}})
    d_cur = d_res.get(today, {"count":0, "pax":0, "countries":{}, "stats":{}})

    # ── Accumulate today's data (merge, not overwrite) ──────────────────
    Path("data").mkdir(exist_ok=True)
    acc_file = Path(f"data/accumulated_{today}.json")
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

    # Merge arrivals flight list — deduplicate code-shares by (from, arr_time)
    cur_arr_by_fn = {_norm_fn(r.get("fn","")): r for r in a_cur.get("flight_list", []) if r.get("fn")}
    existing_arr = acc.get("arrivals_list", [])
    for r in existing_arr:
        fresh = cur_arr_by_fn.get(_norm_fn(r.get("fn","")))
        if fresh:
            if not r.get("airline") and fresh.get("airline"): r["airline"] = fresh["airline"]
            if not r.get("arr_time") and fresh.get("arr_time"): r["arr_time"] = fresh["arr_time"]
            if not r.get("dep_time") and fresh.get("dep_time"): r["dep_time"] = fresh["dep_time"]
            r["status"] = fresh.get("status", r.get("status",""))
    seen_arr = {(r.get("from",""), r.get("arr_time","")) for r in existing_arr if r.get("arr_time")}
    seen_arr_fns = {_norm_fn(r["fn"]) for r in existing_arr if r.get("fn")}
    new_arr = []
    for r in a_cur.get("flight_list", []):
        key = (r.get("from",""), r.get("arr_time",""))
        if _norm_fn(r.get("fn","")) in seen_arr_fns: continue
        if r.get("arr_time") and key in seen_arr: continue
        new_arr.append(r)
        seen_arr.add(key)
        seen_arr_fns.add(_norm_fn(r.get("fn","")))
    acc["arrivals_list"] = existing_arr + new_arr

    # Auto-update stale arrivals: if arr_time > 45min ago → "landed"
    now_ict = datetime.datetime.now(ICT)
    for r in acc["arrivals_list"]:
        if r.get("status") in ("scheduled", "active") and r.get("arr_time"):
            try:
                arr_dt = datetime.datetime.fromisoformat(r["arr_time"])
                if arr_dt.tzinfo is None:
                    arr_dt = ICT.localize(arr_dt)
                if arr_dt < now_ict - datetime.timedelta(minutes=45):
                    r["status"] = "landed"
            except Exception:
                pass

    # Merge departures flight list — deduplicate code-shares by (to, dep_time)
    seen_dep = {(r.get("to",""), r.get("dep_time","")) for r in acc.get("departures_list", []) if r.get("dep_time")}
    seen_dep_fns = {_norm_fn(r["fn"]) for r in acc.get("departures_list", []) if r.get("fn")}
    new_dep = []
    for r in d_cur.get("flight_list", []):
        key = (r.get("to",""), r.get("dep_time",""))
        if _norm_fn(r.get("fn","")) in seen_dep_fns: continue
        if r.get("dep_time") and key in seen_dep: continue
        new_dep.append(r)
        seen_dep.add(key)
        seen_dep_fns.add(_norm_fn(r.get("fn","")))
    acc["departures_list"] = acc.get("departures_list", []) + new_dep

    # Auto-update stale "scheduled" departures: if dep_time > 45min ago → "departed"
    for r in acc["departures_list"]:
        if r.get("status") == "scheduled" and r.get("dep_time"):
            try:
                dep_dt = datetime.datetime.fromisoformat(r["dep_time"])
                if dep_dt.tzinfo is None:
                    dep_dt = ICT.localize(dep_dt)
                if dep_dt < now_ict - datetime.timedelta(minutes=45):
                    r["status"] = "departed"
            except Exception:
                pass

    # ── Recompute countries from deduplicated flight lists ───────────────
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
        "count":     sum(v["flights"] for v in arr_c.values()),
        "pax":       sum(v["pax"]     for v in arr_c.values()),
        "countries": arr_c,
    }
    dep_c = _recompute_countries(acc["departures_list"], "to")
    acc["departures"] = {
        "count":     sum(v["flights"] for v in dep_c.values()),
        "pax":       sum(v["pax"]     for v in dep_c.values()),
        "countries": dep_c,
    }

    acc_file.write_text(json.dumps(acc, indent=2, ensure_ascii=False))

    # Persist to Supabase (via core)
    save_daily(AIRPORT, today, acc.get("arrivals", {}), acc.get("departures", {}),
               SUPABASE_URL, SUPABASE_KEY, tz='Asia/Bangkok')

    # Read accumulated today for dashboard
    a_acc = acc.get("arrivals",   {"count": 0, "pax": 0, "countries": {}})
    d_acc = acc.get("departures", {"count": 0, "pax": 0, "countries": {}})

    # ── Period stats (via core) ──────────────────────────────────────────
    w_a, w_d, w_daily = load_period_stats(today, 7,   data_dir='data')
    m_a, m_d, m_daily = load_period_stats(today, 30,  data_dir='data')
    q_a, q_d, q_daily = load_period_stats(today, 90,  data_dir='data')
    h_a, h_d, h_daily = load_period_stats(today, 180, data_dir='data')
    y_a, y_d, y_daily = load_period_stats(today, 365, data_dir='data')

    # ── Yesterday ────────────────────────────────────────────────────────
    yesterday = (datetime.datetime.fromisoformat(today) - datetime.timedelta(days=1)).date().isoformat()
    yest_file = Path(f"data/accumulated_{yesterday}.json")
    if yest_file.exists():
        yest_data = json.loads(yest_file.read_text())
        v_a = yest_data.get("arrivals",   {"count":0, "pax":0, "countries":{}})
        v_d = yest_data.get("departures", {"count":0, "pax":0, "countries":{}})
        v_arrivals_list   = yest_data.get("arrivals_list",   [])
        v_departures_list = yest_data.get("departures_list", [])
    else:
        v_a = {"count":0, "pax":0, "countries":{}}
        v_d = {"count":0, "pax":0, "countries":{}}
        v_arrivals_list   = []
        v_departures_list = []

    # ── Russian transit estimates ─────────────────────────────────────────
    yest_date = today_date - datetime.timedelta(days=1)
    rt_today     = calc_russian_transit(1,   a_acc["countries"], tat_monthly, today_date)
    rt_yesterday = calc_russian_transit(1,   v_a["countries"],   tat_monthly, yest_date)
    rt_week      = calc_russian_transit(7,   w_a["countries"],   tat_monthly, today_date)
    rt_month     = calc_russian_transit(30,  m_a["countries"],   tat_monthly, today_date)
    rt_quarter   = calc_russian_transit(90,  q_a["countries"],   tat_monthly, today_date)
    rt_halfyear  = calc_russian_transit(180, h_a["countries"],   tat_monthly, today_date)
    rt_year      = calc_russian_transit(365, y_a["countries"],   tat_monthly, today_date)

    # ── Telegram ─────────────────────────────────────────────────────────
    def block(title, a, d):
        lines = [f"<b>{title}</b>",
                 f"✈️ Всего: {a['count']+d['count']} | 👥 ~{a['pax']+d['pax']:,}",
                 "<b>🛬 ПРИЛЁТЫ (Топ-10)</b>", fmt_top10(a['countries']),
                 "<b>🛫 ВЫЛЕТЫ (Топ-10)</b>", fmt_top10(d['countries']), ""]
        return "\n".join(lines)

    src_label = "GTT" if source == "gtt" else f"AS #{knum}"
    msg = f"🌴 <b>PHUKET HKT STATISTICS</b>\n🕒 {now.strftime('%H:%M')} | {src_label}\n━━━━━━━━━━━━━━━━━━━━\n"
    msg += block("🔴 СЕГОДНЯ (накопл.)", a_acc, d_acc)
    msg += block("📊 НЕДЕЛЯ (7 дней)", w_a, w_d)
    msg += block("📈 МЕСЯЦ (30 дней)", m_a, m_d)
    msg += "📍 <i>HKT | Phuket International Airport</i>"

    print(msg)
    send_telegram(msg)

    # ── Dashboard JSON ────────────────────────────────────────────────────
    def fmt_all(ctry):
        sorted_c = sorted(ctry.items(), key=lambda x: x[1]["flights"], reverse=True)
        result = []
        for c, v in sorted_c:
            item = {"name": c, "flights": v["flights"], "pax": v["pax"]}
            if "country" in v:
                item["country"] = v["country"]
            result.append(item)
        return result

    def to_web_fmt(arr, dep, breakdown=None, russia_transit=None, arrivals_list=None, departures_list=None):
        d = {
            "arrivals":   {"count": arr["count"], "pax": arr["pax"], "all": fmt_all(arr["countries"])},
            "departures": {"count": dep["count"], "pax": dep["pax"], "all": fmt_all(dep["countries"])},
        }
        if breakdown:
            d.update(breakdown)
        if russia_transit:
            d["russia_transit"] = russia_transit
        if arrivals_list:
            d["arrivals_list"] = arrivals_list
        if departures_list:
            d["departures_list"] = departures_list
        return d

    available_dates = sorted([
        p.stem.replace("accumulated_", "")
        for p in Path("data").glob("accumulated_*.json")
    ])

    dashboard_data = {
        "updated":          now.isoformat(),
        "available_dates":  available_dates,
        "yesterday": to_web_fmt(v_a,  v_d,  russia_transit=rt_yesterday, arrivals_list=v_arrivals_list, departures_list=v_departures_list),
        "today":     to_web_fmt(a_acc, d_acc, russia_transit=rt_today,     arrivals_list=acc.get("arrivals_list", []), departures_list=acc.get("departures_list", [])),
        "week":      to_web_fmt(w_a,  w_d,  {"by_days":   make_by_days(w_daily)},   russia_transit=rt_week),
        "month":     to_web_fmt(m_a,  m_d,  {"by_weeks":  make_by_weeks(m_daily)},  russia_transit=rt_month),
        "quarter":   to_web_fmt(q_a,  q_d,  {"by_months": make_by_months(q_daily)}, russia_transit=rt_quarter),
        "halfyear":  to_web_fmt(h_a,  h_d,  {"by_months": make_by_months(h_daily)}, russia_transit=rt_halfyear),
        "year":      to_web_fmt(y_a,  y_d,  {"by_months": make_by_months(y_daily)}, russia_transit=rt_year),
    }
    Path("data/dashboard.json").write_text(json.dumps(dashboard_data, ensure_ascii=False, indent=2))
    print("✅ dashboard.json обновлён.")


if __name__ == "__main__":
    run()
