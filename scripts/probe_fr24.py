"""Разведка flightradar24.com — ищем XHR с историей рейсов DAD/PQC."""
from playwright.sync_api import sync_playwright

URLS = [
    "https://www.flightradar24.com/data/airports/dad/arrivals",
    "https://www.flightradar24.com/data/airports/dad/departures",
    "https://www.flightradar24.com/data/airports/pqc/arrivals",
    "https://www.flightradar24.com/data/airports/pqc/departures",
]


def probe():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(
            viewport={"width": 1280, "height": 900},
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        )
        page = ctx.new_page()

        captured = []

        def on_response(resp):
            try:
                url = resp.url
                ctype = resp.headers.get("content-type", "")
                if any(url.endswith(ext) for ext in (".png", ".jpg", ".svg", ".woff", ".woff2", ".ttf", ".ico", ".css")):
                    return
                if "json" in ctype.lower() or "/_json/" in url or "api" in url.lower():
                    body = ""
                    try:
                        body = resp.text()[:300]
                    except Exception:
                        pass
                    captured.append({"url": url, "status": resp.status, "ctype": ctype, "body": body})
            except Exception:
                pass

        page.on("response", on_response)

        for url in URLS:
            print(f"\n{'='*70}\n📡 {url}\n{'='*70}")
            try:
                page.goto(url, wait_until="networkidle", timeout=45000)
                page.wait_for_timeout(3500)
            except Exception as e:
                print(f"  error: {e}")

        print(f"\n\n📊 json-like captured: {len(captured)}")
        seen = set()
        for c in captured:
            base = c["url"].split("?")[0]
            if base in seen: continue
            seen.add(base)
            print(f"  [{c['status']}] {c['ctype'][:40]:40} {c['url'][:120]}")
            print(f"      body: {c['body'][:200]!r}")
        browser.close()


if __name__ == "__main__":
    probe()
