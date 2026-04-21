"""Разведка Wayback Machine — какое покрытие по дням март-апрель 2026 для:
- phuquocairport.com arrivals/departures
- danangairport.vn arrivals/departures
"""
import requests
from collections import defaultdict

CDX = "https://web.archive.org/cdx/search/cdx"
FROM = "20260301"
TO   = "20260421"

TARGETS = [
    ("PQC arr", "phuquocairport.com/flight-status-arrivals-departures/"),
    ("PQC dep", "phuquocairport.com/flight-status-departures/"),
    ("DAD arr", "danangairport.vn/flights-flight-status-arrival"),
    ("DAD dep", "danangairport.vn/flights-flight-status-departure"),
]

def probe(label, url):
    params = {
        "url": url,
        "from": FROM, "to": TO,
        "output": "json",
        "collapse": "timestamp:8",  # 1 snapshot per day
        "fl": "timestamp,statuscode,original",
        "filter": "statuscode:200",
    }
    r = requests.get(CDX, params=params, timeout=30)
    rows = r.json()[1:] if r.ok else []
    by_day = defaultdict(list)
    for ts, status, orig in rows:
        day = ts[:8]
        by_day[day].append(ts)
    dates = sorted(by_day.keys())
    print(f"\n=== {label} ===  {url}")
    print(f"  days with snapshots: {len(dates)}")
    if dates:
        print(f"  first: {dates[0]} | last: {dates[-1]}")
        # show distribution by month
        by_month = defaultdict(int)
        for d in dates:
            by_month[d[:6]] += 1
        for m, n in sorted(by_month.items()):
            print(f"    {m[:4]}-{m[4:]}: {n} days")
    return dates


all_dates = {}
for label, url in TARGETS:
    all_dates[label] = probe(label, url)

print("\n\n=== OVERLAP (days covered by ALL 4 feeds) ===")
if all(all_dates.values()):
    overlap = set(all_dates[TARGETS[0][0]])
    for label, _ in TARGETS[1:]:
        overlap &= set(all_dates[label])
    print(f"  {len(overlap)} days")
    print(f"  sample: {sorted(overlap)[:5]} ... {sorted(overlap)[-5:]}")
