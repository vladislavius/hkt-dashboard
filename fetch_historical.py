"""
fetch_historical.py — качает исторические данные с GTT API (HKT airport)
Стратегия:
  1. Playwright открывает сайт аэропорта
  2. Мы ПЕРЕХВАТЫВАЕМ и ОТМЕНЯЕМ GTT-запрос страницы → токен не сжигается
  3. Используем токен для 2 запросов: arrivals + departures нужной даты
  4. Сохраняем в data/accumulated_YYYY-MM-DD.json

Запуск: python3 fetch_historical.py [START_DATE] [END_DATE]
  По умолчанию: 2026-01-01 → 2026-04-05
"""
import os, sys, json, time, datetime, requests
from pathlib import Path
from collections import defaultdict

# Заглушки для AVIATIONSTACK ключей (не используются, но collector импортирует)
for k in ('AVIATIONSTACK_KEY_1','AVIATIONSTACK_KEY_2','AVIATIONSTACK_KEY_3','AVIATIONSTACK_KEY_4'):
    os.environ.setdefault(k, 'x')

from collector import analyze_gtt, load_tat_stats, calc_russian_transit, ICT

GTT_ENDPOINT = "https://gtt-prod.sawasdeebyaot.com/graphql"
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

GTT_QUERY = """query HKTFlightBoard($site: String!, $start: String!, $end: String!) {
  arrivals: webAOTFetchFlightBoard(site: $site, type: "A", schedule_start: $start, schedule_end: $end) {
    success message
    payload { flights {
      number
      flight_departure { scheduled_at flight_status }
      flight_arrival   { scheduled_at flight_status }
      origin_airport      { iata_code city }
      destination_airport { iata_code city }
      airline  { iata name }
      aircraft { iata name }
      flight_status
    } }
  }
  departures: webAOTFetchFlightBoard(site: $site, type: "D", schedule_start: $start, schedule_end: $end) {
    success message
    payload { flights {
      number
      flight_departure { scheduled_at flight_status }
      flight_arrival   { scheduled_at flight_status }
      origin_airport      { iata_code city }
      destination_airport { iata_code city }
      airline  { iata name }
      aircraft { iata name }
      flight_status
    } }
  }
}"""

GTT_HEADERS = {
    "Content-Type":    "application/json",
    "api-name":        "WebAOTFetchFlightBoard",
    "origin":          "https://phuket.airportthai.co.th",
    "referer":         "https://phuket.airportthai.co.th/",
    "accept":          "*/*",
    "accept-language": "en-US,en;q=0.9",
    "user-agent":      "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
}


def gtt_request(token, date_str):
    """Один комбинированный запрос A+D для конкретной даты (один токен на оба направления)."""
    resp = requests.post(
        GTT_ENDPOINT,
        json={
            "query": GTT_QUERY,
            "variables": {
                "site":  "hkt",
                "start": f"{date_str} 00:00:00",
                "end":   f"{date_str} 23:59:59",
            },
        },
        headers={**GTT_HEADERS, "Authorization": token},
        timeout=30,
    )
    data = resp.json()
    if data.get("errors"):
        raise RuntimeError(f"GTT errors: {data['errors']}")
    d = data.get("data") or {}
    arr_board = d.get("arrivals") or {}
    dep_board = d.get("departures") or {}
    arr = (arr_board.get("payload") or {}).get("flights") or []
    dep = (dep_board.get("payload") or {}).get("flights") or []
    return arr, dep


SINGLE_QUERY = """query HKTFlightBoardOne($site: String!, $type: String!, $start: String!, $end: String!) {
  webAOTFetchFlightBoard(site: $site, type: $type, schedule_start: $start, schedule_end: $end) {
    success message
    payload { flights {
      number
      flight_departure { scheduled_at flight_status }
      flight_arrival   { scheduled_at flight_status }
      origin_airport      { iata_code city }
      destination_airport { iata_code city }
      airline  { iata name }
      aircraft { iata name }
      flight_status
    } }
  }
}"""


def _intercept_and_fetch(page, date_str, flight_type, url_type):
    """
    Навигирует на страницу airport/?type=url_type, перехватывает GTT-запрос,
    подменяет тело на нашу дату и flight_type, возвращает список рейсов.
    """
    import json as _json

    flights = []

    def handle_route(route, request):
        new_body = {
            "query": SINGLE_QUERY,
            "variables": {
                "site":  "hkt",
                "type":  flight_type,
                "start": f"{date_str} 00:00:00",
                "end":   f"{date_str} 23:59:59",
            },
        }
        route.continue_(
            post_data=_json.dumps(new_body),
            headers={**request.headers, "Content-Type": "application/json"},
        )

    page.route("**/gtt-prod.sawasdeebyaot.com/**", handle_route)

    try:
        with page.expect_response(
            lambda r: "gtt-prod.sawasdeebyaot.com/graphql" in r.url,
            timeout=70000,
        ) as resp_info:
            try:
                page.goto(
                    f"https://phuket.airportthai.co.th/flight?type={url_type}",
                    wait_until="commit", timeout=60000,
                )
            except Exception:
                pass

        data = resp_info.value.json()
        board = (data.get("data") or {}).get("webAOTFetchFlightBoard") or {}
        flights = (board.get("payload") or {}).get("flights") or []
    except Exception as e:
        print(f"  ⚠️  {flight_type} error: {e}")
    finally:
        page.unroute("**/gtt-prod.sawasdeebyaot.com/**")

    return flights


def fetch_day_playwright(date_str):
    """
    Один браузер, два навигирования (?type=a и ?type=d) → два токена → arrivals + departures.
    """
    from playwright.sync_api import sync_playwright

    arr_raw = dep_raw = []

    with sync_playwright() as p:
        try:
            browser = p.chromium.launch(
                channel="chrome", headless=False,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--window-position=-3000,0",
                    "--window-size=1280,800",
                ],
            )
        except Exception:
            browser = p.chromium.launch(
                headless=False,
                args=[
                    "--no-sandbox",
                    "--disable-blink-features=AutomationControlled",
                    "--window-position=-3000,0",
                    "--window-size=1280,800",
                ],
            )

        ctx = browser.new_context(viewport={"width": 1280, "height": 800})
        page = ctx.new_page()
        page.add_init_script("""
            Object.defineProperty(navigator,'webdriver',{get:()=>undefined});
            window.chrome={runtime:{}};
        """)

        arr_raw = _intercept_and_fetch(page, date_str, "A", "a")
        dep_raw = _intercept_and_fetch(page, date_str, "D", "d")

        browser.close()

    return arr_raw, dep_raw


def fetch_day(date_str):
    """
    Полный цикл для одной даты: токен → 2 запроса (A+D) → обработка → сохранение.
    Возвращает True если успешно.
    """
    out_path = DATA_DIR / f"accumulated_{date_str}.json"
    if out_path.exists():
        print(f"  ⏭  {date_str} — уже есть, пропускаем")
        return True

    print(f"\n{'─'*50}")
    print(f"  📅 {date_str}")
    print(f"  → Запрашиваем прилёты + вылеты (подмена запроса)...")
    arr_raw, dep_raw = fetch_day_playwright(date_str)
    if arr_raw is None: arr_raw = []
    if dep_raw is None: dep_raw = []
    print(f"  ✅ Arrivals: {len(arr_raw)} | Departures: {len(dep_raw)}")

    if not arr_raw and not dep_raw:
        print(f"  ⚠️  Нет данных для {date_str} — возможно токен сгорел")
        return False

    # Обработка через существующую логику collector.py
    arr_analyzed = analyze_gtt(arr_raw, "arrival", date_str).get(date_str, {})
    dep_analyzed = analyze_gtt(dep_raw, "departure", date_str).get(date_str, {})

    # Транзит РФ
    tat_monthly = load_tat_stats()
    ref_date = datetime.date.fromisoformat(date_str)
    russia_transit = calc_russian_transit(
        period_days=1,
        countries_arrivals=arr_analyzed.get("countries", {}),
        tat_monthly=tat_monthly,
        ref_date=ref_date,
    )

    acc = {
        "date": date_str,
        "arrivals": {
            "count":     arr_analyzed.get("count", 0),
            "pax":       arr_analyzed.get("pax", 0),
            "countries": arr_analyzed.get("countries", {}),
        },
        "departures": {
            "count":     dep_analyzed.get("count", 0),
            "pax":       dep_analyzed.get("pax", 0),
            "countries": dep_analyzed.get("countries", {}),
        },
        "arrivals_list":   arr_analyzed.get("flight_list", []),
        "departures_list": dep_analyzed.get("flight_list", []),
        "russia_transit":  russia_transit,
    }

    out_path.write_text(json.dumps(acc, ensure_ascii=False, indent=2))
    arr_c = acc["arrivals"]["count"]
    dep_c = acc["departures"]["count"]
    print(f"  💾 Сохранено: {arr_c} прилётов, {dep_c} вылетов → {out_path.name}")
    return True


def date_range(start_str, end_str):
    """Генератор дат от start до end включительно."""
    start = datetime.date.fromisoformat(start_str)
    end   = datetime.date.fromisoformat(end_str)
    cur   = start
    while cur <= end:
        yield cur.isoformat()
        cur += datetime.timedelta(days=1)


def fix_departures():
    """Второй проход: только дни где departures.count == 0."""
    zero_dep = []
    for f in sorted(DATA_DIR.glob("accumulated_202*.json")):
        d = json.loads(f.read_text())
        if d.get("departures", {}).get("count", 0) == 0:
            zero_dep.append(f.stem.replace("accumulated_", ""))

    print(f"=== FIX DEPARTURES ===")
    print(f"Дней с нулевыми вылетами: {len(zero_dep)}")
    if not zero_dep:
        print("✅ Все вылеты на месте!")
        return

    ok, fail = 0, 0
    for i, date_str in enumerate(zero_dep, 1):
        print(f"\n[{i}/{len(zero_dep)}] {'─'*44}")
        print(f"  📅 {date_str} — добираем вылеты...")
        dep_raw = _fetch_single_direction(date_str, "D", "d")
        if not dep_raw:
            print(f"  ❌ Не удалось — пропускаем")
            fail += 1
            continue

        out_path = DATA_DIR / f"accumulated_{date_str}.json"
        acc = json.loads(out_path.read_text())
        ref_date = datetime.date.fromisoformat(date_str)
        dep_analyzed = analyze_gtt(dep_raw, "departure", date_str).get(date_str, {})
        acc["departures"] = {
            "count":     dep_analyzed.get("count", 0),
            "pax":       dep_analyzed.get("pax", 0),
            "countries": dep_analyzed.get("countries", {}),
        }
        acc["departures_list"] = dep_analyzed.get("flight_list", [])
        out_path.write_text(json.dumps(acc, ensure_ascii=False, indent=2))
        print(f"  ✅ Сохранено {dep_analyzed.get('count',0)} вылетов")
        ok += 1
        if i < len(zero_dep):
            time.sleep(2)

    print(f"\n{'='*50}")
    print(f"✅ Починено: {ok} | Провалено: {fail}")


def _fetch_single_direction(date_str, flight_type, url_type):
    """Один браузер, один направление."""
    from playwright.sync_api import sync_playwright
    flights = []
    with sync_playwright() as p:
        try:
            browser = p.chromium.launch(
                channel="chrome", headless=False,
                args=["--disable-blink-features=AutomationControlled",
                      "--window-position=-3000,0", "--window-size=1280,800"],
            )
        except Exception:
            browser = p.chromium.launch(
                headless=False,
                args=["--no-sandbox", "--disable-blink-features=AutomationControlled",
                      "--window-position=-3000,0", "--window-size=1280,800"],
            )
        ctx = browser.new_context(viewport={"width": 1280, "height": 800})
        page = ctx.new_page()
        page.add_init_script("""
            Object.defineProperty(navigator,'webdriver',{get:()=>undefined});
            window.chrome={runtime:{}};
        """)
        flights = _intercept_and_fetch(page, date_str, flight_type, url_type)
        browser.close()
    return flights


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "--fix-departures":
        fix_departures()
        return

    start = sys.argv[1] if len(sys.argv) > 1 else "2026-01-01"
    end   = sys.argv[2] if len(sys.argv) > 2 else "2026-04-05"

    dates = list(date_range(start, end))
    existing = [d for d in dates if (DATA_DIR / f"accumulated_{d}.json").exists()]
    missing  = [d for d in dates if d not in existing]

    print(f"=== HISTORICAL FETCH ===")
    print(f"Период: {start} → {end} ({len(dates)} дней)")
    print(f"Уже есть: {len(existing)} | Нужно скачать: {len(missing)}")
    if not missing:
        print("✅ Все данные уже есть!")
        return

    print(f"\nОценка времени: ~{len(missing) * 2} минут\n")

    ok, fail = 0, 0
    for i, date_str in enumerate(missing, 1):
        print(f"[{i}/{len(missing)}]", end="")
        if fetch_day(date_str):
            ok += 1
        else:
            fail += 1
        if i < len(missing):
            time.sleep(3)

    print(f"\n{'='*50}")
    print(f"✅ Готово: {ok} дней скачано, {fail} провалено")
    print(f"Запусти collector.py (функцию build_dashboard) чтобы обновить dashboard.json")


if __name__ == "__main__":
    main()
