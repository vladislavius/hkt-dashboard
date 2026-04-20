"""Разведка flightera.net для исторических рейсов DAD/PQC.

URL паттерн: flightera.net/en/airport/{City}/{ICAO}/{arrival|departure}/YYYY-MM-DD_HH_MM

Проверяем:
  - проходит ли Cloudflare challenge через Playwright (без CapSolver для начала)
  - есть ли у страницы реальные данные за прошлые даты
  - структуру таблицы (селекторы, поля)
"""
from playwright.sync_api import sync_playwright

TARGETS = [
    ("DAD 2025-10-15 arr",  "https://www.flightera.net/en/airport/Da%20Nang/VVDN/arrival/2025-10-15_00_00"),
    ("DAD 2026-04-19 arr",  "https://www.flightera.net/en/airport/Da%20Nang/VVDN/arrival/2026-04-19_00_00"),
    ("PQC 2025-10-15 arr",  "https://www.flightera.net/en/airport/Phu%20Quoc/VVPQ/arrival/2025-10-15_00_00"),
    ("PQC 2026-04-19 arr",  "https://www.flightera.net/en/airport/Phu%20Quoc/VVPQ/arrival/2026-04-19_00_00"),
]


def probe():
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
        ctx = browser.new_context(
            viewport={"width": 1280, "height": 900},
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        )
        page = ctx.new_page()

        for label, url in TARGETS:
            print(f"\n{'='*70}\n📡 {label}\n   {url}\n{'='*70}")
            try:
                resp = page.goto(url, wait_until="domcontentloaded", timeout=30000)
                page.wait_for_timeout(4000)  # wait for possible CF challenge
                status = resp.status if resp else "no-resp"
                title = page.title()
                # count table rows / flight entries
                n_rows = page.locator("table tr").count()
                n_flight = page.locator("a[href*='/flight/']").count()
                # sample text
                body_text = page.locator("body").inner_text()[:400]
                print(f"  status: {status}")
                print(f"  title:  {title!r}")
                print(f"  <tr>:   {n_rows}")
                print(f"  flight links: {n_flight}")
                print(f"  body head: {body_text!r}")
            except Exception as e:
                print(f"  ⚠️ error: {e}")

        browser.close()


if __name__ == "__main__":
    probe()
