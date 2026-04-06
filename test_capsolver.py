"""Test CapSolver token → fetch_flights_gtt (Python requests, no browser)."""
import os, sys
from dotenv import load_dotenv
load_dotenv()

os.environ.setdefault('AVIATIONSTACK_KEY_1', 'x')
os.environ.setdefault('AVIATIONSTACK_KEY_2', 'x')
os.environ.setdefault('AVIATIONSTACK_KEY_3', 'x')
os.environ.setdefault('AVIATIONSTACK_KEY_4', 'x')

key = os.environ.get("CAPSOLVER_KEY", "")
if not key:
    print("❌ CAPSOLVER_KEY not set — add it to .env")
    sys.exit(1)

from collector import get_turnstile_token, fetch_flights_gtt_one
import datetime, pytz, time

today = datetime.datetime.now(pytz.timezone('Asia/Bangkok')).date().isoformat()
print(f"Testing CapSolver → GTT (two tokens) for {today}")

t0 = time.time()
arr_token = get_turnstile_token()
a = fetch_flights_gtt_one(arr_token, today, "A") if arr_token else None

dep_token = get_turnstile_token()
d = fetch_flights_gtt_one(dep_token, today, "D") if dep_token else None

print(f"Done in {time.time()-t0:.1f}s")
if a is not None and d is not None:
    print(f"✅ SUCCESS — Arrivals: {len(a)}, Departures: {len(d)}")
    if a:
        f = a[0]
        print(f"  Sample: {f.get('number')} ac={f.get('aircraft',{}).get('iata')} from={f.get('origin_airport',{}).get('iata_code')}")
else:
    print(f"❌ FAILED — arrivals={'ok' if a else 'None'}, departures={'ok' if d else 'None'}")
