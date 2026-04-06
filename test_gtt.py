import os
os.environ.setdefault('AVIATIONSTACK_KEY_1', 'x')
os.environ.setdefault('AVIATIONSTACK_KEY_2', 'x')
os.environ.setdefault('AVIATIONSTACK_KEY_3', 'x')
os.environ.setdefault('AVIATIONSTACK_KEY_4', 'x')

from collector import fetch_flights_gtt_playwright
import datetime, pytz, time

print("=== LOCAL GTT TEST ===")
today = datetime.datetime.now(pytz.timezone('Asia/Bangkok')).date().isoformat()
print(f"Testing for date: {today}")

t0 = time.time()
try:
    a, d = fetch_flights_gtt_playwright(today)
    print(f"✅ SUCCESS in {time.time()-t0:.1f}s — Arrivals: {len(a)}, Departures: {len(d)}")
    if a:
        f = a[0]
        print(f"  Sample: {f.get('number')} ac={f.get('aircraft',{}).get('iata')} from={f.get('origin_airport',{}).get('iata_code')}")
except Exception as e:
    print(f"❌ FAILED in {time.time()-t0:.1f}s: {e}")
