"""
Тест исторических данных GTT:
1. Playwright перехватывает ИСХОДЯЩИЙ запрос → вытаскиваем Authorization токен
2. С токеном делаем прямые GraphQL запросы на исторические даты
"""
import os, json, requests
os.environ.setdefault('AVIATIONSTACK_KEY_1', 'x')
os.environ.setdefault('AVIATIONSTACK_KEY_2', 'x')
os.environ.setdefault('AVIATIONSTACK_KEY_3', 'x')
os.environ.setdefault('AVIATIONSTACK_KEY_4', 'x')

GTT_ENDPOINT = "https://gtt-prod.sawasdeebyaot.com/graphql"

QUERY = """query HKTFlightBoardOne($site: String!, $type: String!, $start: String!, $end: String!) {
  webAOTFetchFlightBoard(site: $site, type: $type, schedule_start: $start, schedule_end: $end) {
    success message
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


def extract_token_via_playwright():
    """Открываем страницу, перехватываем исходящий GTT-запрос, извлекаем Authorization."""
    from playwright.sync_api import sync_playwright
    token = None
    with sync_playwright() as p:
        browser = p.chromium.launch(
            channel="chrome", headless=False,
            args=["--disable-blink-features=AutomationControlled"]
        )
        ctx = browser.new_context(viewport={"width": 1280, "height": 800})
        page = ctx.new_page()
        page.add_init_script("""
            Object.defineProperty(navigator,'webdriver',{get:()=>undefined});
            window.chrome={runtime:{}};
        """)

        def on_request(req):
            nonlocal token
            if "gtt-prod.sawasdeebyaot.com/graphql" in req.url and not token:
                auth = req.headers.get("authorization", "")
                if auth:
                    token = auth
                    print(f"✅ Токен перехвачен: {auth[:40]}...")

        page.on("request", on_request)

        print("→ Открываем сайт аэропорта...")
        try:
            page.goto("https://phuket.airportthai.co.th/flight?type=a",
                      wait_until="commit", timeout=60000)
        except Exception as e:
            print(f"⚠️ goto exception (продолжаем): {e}")

        # Ждём пока GTT-запрос уйдёт (до 45 сек)
        print("→ Ждём GTT-запрос...")
        for _ in range(45):
            page.wait_for_timeout(1000)
            if token:
                print("→ Токен получен, закрываем браузер")
                break

        if not token:
            # Попробуем dismiss popup и подождём ещё
            for sel in ["button:has-text('Accept')", "button:has-text('ยอมรับ')", ".btn-accept"]:
                try:
                    page.click(sel, timeout=500)
                except:
                    pass
            page.wait_for_timeout(10000)
        browser.close()
    return token


def query_gtt(token, date_str=None, flight_type="A", start_override=None, end_override=None):
    """Прямой GraphQL запрос с произвольной датой или диапазоном."""
    start = f"{start_override} 00:00:00" if start_override else f"{date_str} 00:00:00"
    end   = f"{end_override} 23:59:59"   if end_override   else f"{date_str} 23:59:59"
    resp = requests.post(
        GTT_ENDPOINT,
        json={
            "query": QUERY,
            "variables": {
                "site":  "hkt",
                "type":  flight_type,
                "start": start,
                "end":   end,
            },
        },
        headers={
            "Content-Type":    "application/json",
            "Authorization":   token,
            "api-name":        "WebAOTFetchFlightBoard",
            "origin":          "https://phuket.airportthai.co.th",
            "referer":         "https://phuket.airportthai.co.th/",
            "accept":          "*/*",
            "user-agent":      "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        },
        timeout=30,
    )
    data = resp.json()
    if data.get("errors"):
        return None, f"errors: {data['errors']}"
    board = (data.get("data") or {}).get("webAOTFetchFlightBoard") or {}
    if not board.get("success"):
        return None, f"not success: {board.get('message')}"
    flights = (board.get("payload") or {}).get("flights") or []
    return flights, None


# ── MAIN ──────────────────────────────────────────────────────────────
print("=== HISTORICAL GTT TEST v2 ===\n")

token = extract_token_via_playwright()
if not token:
    print("❌ Токен не перехвачен — сайт не отправил GTT запрос")
    exit(1)

print(f"\n→ Один запрос: весь диапазон Jan 1 → Apr 6 2026...\n")

# Токен single-use — тратим его на ОДИН широкий запрос
flights, err = query_gtt(token, start_override="2026-01-01", end_override="2026-04-06", flight_type="A")
if err:
    print(f"❌ Ошибка: {err}")
else:
    from collections import Counter
    dates_count = Counter()
    for f in (flights or []):
        ts = (f.get('flight_arrival') or {}).get('scheduled_at') or \
             (f.get('flight_departure') or {}).get('scheduled_at') or ''
        if ts:
            dates_count[ts[:10]] += 1

    print(f"✅ Итого рейсов в ответе: {len(flights)}")
    print(f"   Диапазон дат: {min(dates_count) if dates_count else '?'} → {max(dates_count) if dates_count else '?'}")
    print(f"   Уникальных дат: {len(dates_count)}")
    if dates_count:
        print(f"   Примеры (первые/последние):")
        for d in sorted(dates_count)[:5]:
            print(f"     {d}: {dates_count[d]} рейсов")
        print("     ...")
        for d in sorted(dates_count)[-3:]:
            print(f"     {d}: {dates_count[d]} рейсов")
    if flights:
        print(f"\n   Пример рейса: {flights[0].get('number')} из {(flights[0].get('origin_airport') or {}).get('iata_code','?')}")

print("\n=== ВЫВОД ===")
print("Много дат с января → API хранит всю историю, можно качать ✅")
print("Только апрель → хранит ~7 дней, нужен другой источник ❌")
