"""Debug: capture ALL network requests and cookies on the AOT flight page."""
import time
from playwright.sync_api import sync_playwright

URL = "https://phuket.airportthai.co.th/flight?type=a"

requests_log = []

def handle_request(request):
    requests_log.append({
        "url": request.url,
        "method": request.method,
        "auth": request.headers.get("authorization", ""),
        "origin": request.headers.get("origin", ""),
    })

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

    page.on("request", handle_request)
    page.goto(URL, wait_until="domcontentloaded", timeout=30000)

    # Dismiss consent popup
    try:
        page.wait_for_selector(
            "button:has-text('Accept'), button:has-text('ACCEPT'), "
            "button:has-text('ยอมรับ'), button:has-text('I Agree')",
            timeout=5000,
        )
        page.click(
            "button:has-text('Accept'), button:has-text('ACCEPT'), "
            "button:has-text('ยอมรับ'), button:has-text('I Agree')",
        )
        print("🖱️ Dismissed consent popup")
    except Exception:
        pass

    print("Waiting 30s for all requests...")
    time.sleep(30)

    # Dump all cookies
    print("\n=== COOKIES ===")
    for c in ctx.cookies():
        val_preview = c['value'][:60] + "..." if len(c['value']) > 60 else c['value']
        print(f"  {c['name']} ({len(c['value'])} chars): {val_preview}")

    # Dump localStorage
    try:
        ls = page.evaluate("() => JSON.stringify({...localStorage})")
        print(f"\n=== localStorage ===\n  {ls[:500]}")
    except Exception as e:
        print(f"\nlocalStorage error: {e}")

    # Dump requests to GTT or auth-related
    print("\n=== ALL REQUESTS (filtered) ===")
    for r in requests_log:
        if any(kw in r["url"] for kw in ["gtt", "sawasdee", "graphql", "turnstile", "auth", "token"]):
            auth_preview = r["auth"][:80] + "..." if len(r["auth"]) > 80 else r["auth"]
            print(f"  [{r['method']}] {r['url'][:100]}")
            print(f"    auth: {auth_preview!r}")

    print(f"\nTotal requests captured: {len(requests_log)}")

    browser.close()
