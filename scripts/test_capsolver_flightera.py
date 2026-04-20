"""Тест CapSolver AntiCloudflareTaskProxyLess на flightera.net.

Цель: убедиться что CapSolver выдаёт валидный cf_clearance cookie,
    которым можно вытянуть HTML архивной страницы.

Запуск:  source .env.local && python3 scripts/test_capsolver_flightera.py
"""
import os, time, json, requests, sys

CAPSOLVER_KEY = os.environ.get("CAPSOLVER_KEY", "").strip()
if not CAPSOLVER_KEY:
    sys.exit("❌ CAPSOLVER_KEY not set — run `source .env.local` first")

TARGET_URL = "https://www.flightera.net/en/airport/Da%20Nang/VVDN/arrival/2025-10-15_00_00"
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"


def create_task():
    payload = {
        "clientKey": CAPSOLVER_KEY,
        "task": {
            "type":    "AntiCloudflareTaskProxyLess",
            "websiteURL": TARGET_URL,
            "proxy": "",
        },
    }
    r = requests.post("https://api.capsolver.com/createTask", json=payload, timeout=30)
    j = r.json()
    print("createTask:", json.dumps(j, indent=2)[:500])
    if j.get("errorId"):
        sys.exit(f"❌ createTask error: {j}")
    return j["taskId"]


def poll(task_id, max_s=120):
    t0 = time.time()
    while time.time() - t0 < max_s:
        r = requests.post(
            "https://api.capsolver.com/getTaskResult",
            json={"clientKey": CAPSOLVER_KEY, "taskId": task_id},
            timeout=30,
        )
        j = r.json()
        status = j.get("status")
        print(f"  poll +{int(time.time()-t0)}s status={status} err={j.get('errorCode')}")
        if status == "ready":
            return j["solution"]
        if j.get("errorId"):
            sys.exit(f"❌ getTaskResult error: {j}")
        time.sleep(4)
    sys.exit("❌ timeout waiting for solution")


def main():
    print(f"🎯 Target: {TARGET_URL}")
    print(f"🔑 Key: {CAPSOLVER_KEY[:6]}…{CAPSOLVER_KEY[-4:]}  ({len(CAPSOLVER_KEY)} chars)")

    task_id = create_task()
    print(f"📝 taskId: {task_id}")

    sol = poll(task_id)
    print("\n✅ solution keys:", list(sol.keys()))
    print("   user-agent:", sol.get("userAgent") or sol.get("user_agent"))
    cookies = sol.get("cookies") or {}
    print("   cookies:", list(cookies.keys()) if isinstance(cookies, dict) else cookies)

    ua = sol.get("userAgent") or sol.get("user_agent") or UA
    # try to fetch the page with returned cookies
    if isinstance(cookies, dict):
        cookie_header = "; ".join(f"{k}={v}" for k, v in cookies.items())
    elif isinstance(cookies, list):
        cookie_header = "; ".join(f"{c['name']}={c['value']}" for c in cookies)
    else:
        cookie_header = ""

    print("\n🌐 Fetching page with cookies…")
    r = requests.get(
        TARGET_URL,
        headers={"User-Agent": ua, "Cookie": cookie_header, "Accept-Language": "en-US,en;q=0.9"},
        timeout=30,
        allow_redirects=True,
    )
    print(f"  HTTP {r.status_code}, {len(r.text)} bytes")
    snippet = r.text[:500]
    print(f"  head: {snippet!r}")

    has_table = "flightTable" in r.text or "<table" in r.text
    has_cf = "Just a moment" in r.text or "cloudflare" in r.text.lower() and "challenge" in r.text.lower()
    print(f"\n📊 has <table>: {has_table}")
    print(f"   still CF challenge: {has_cf}")


if __name__ == "__main__":
    main()
