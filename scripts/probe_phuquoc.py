"""Разведка сайтов Phu Quoc International Airport — ловим XHR endpoint'ы рейсов.

Запуск:  python3 scripts/probe_phuquoc.py

Цель: найти URL и формат ответа API, отдающего таблицу рейсов PQC (Phu Quoc, VVPQ).
По образцу probe_camranh.py.
"""
from playwright.sync_api import sync_playwright

URLS = [
    "https://pqia.vietnamairport.vn/",
    "https://pqia.vietnamairport.vn/flights-flight-status-arrival",
    "https://pqia.vietnamairport.vn/flights-flight-status-departure",
]

RELEVANT_CONTENT_TYPES = ("json", "javascript", "graphql", "xml")


def probe():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(viewport={"width": 1280, "height": 800})
        page = ctx.new_page()

        captured = []

        def on_response(resp):
            try:
                url = resp.url
                if any(url.endswith(ext) for ext in (".png", ".jpg", ".svg", ".woff", ".woff2", ".ttf", ".ico", ".css")):
                    return
                ctype = resp.headers.get("content-type", "")
                if not any(t in ctype.lower() for t in RELEVANT_CONTENT_TYPES) and "application" not in ctype.lower():
                    return
                body = ""
                try:
                    body = resp.text()[:600]
                except Exception:
                    pass
                captured.append({
                    "url": url,
                    "method": resp.request.method,
                    "status": resp.status,
                    "ctype": ctype,
                    "body_preview": body,
                })
            except Exception as e:
                captured.append({"error": str(e), "url": resp.url})

        page.on("response", on_response)

        for url in URLS:
            print(f"\n{'='*70}\n📡 PROBING: {url}\n{'='*70}")
            try:
                page.goto(url, wait_until="networkidle", timeout=45000)
                page.wait_for_timeout(3000)
            except Exception as e:
                print(f"⚠️ goto error: {e}")

        api_like = [
            c for c in captured
            if c.get("ctype") and ("json" in c["ctype"].lower() or "graphql" in c["ctype"].lower() or "/api/" in c.get("url", "") or "ajax" in c.get("url", "").lower())
        ]

        print(f"\n\n{'='*70}\n📊 RESULTS\n{'='*70}")
        print(f"Total responses captured: {len(captured)}")
        print(f"API-like responses:     {len(api_like)}")

        for i, c in enumerate(api_like, 1):
            print(f"\n--- [{i}] {c['method']} {c['status']} {c['url']}")
            print(f"    content-type: {c['ctype']}")
            preview = c.get("body_preview", "")[:400]
            print(f"    preview: {preview!r}")

        print(f"\n\n{'='*70}\n📄 ALL captured (url + content-type)\n{'='*70}")
        for c in captured:
            if "error" in c:
                continue
            print(f"  [{c['status']}] {c['method']:<5} {c['ctype']:<40} {c['url']}")

        browser.close()


if __name__ == "__main__":
    probe()
