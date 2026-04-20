"""Общий Supabase persister для multi-airport collectors.

Таблица `flight_daily`:
    airport VARCHAR(3) NOT NULL
    date    DATE NOT NULL
    PRIMARY KEY (airport, date)
    arrivals_count, arrivals_pax, departures_count, departures_pax — ints
    arrivals_countries, departures_countries — JSONB
    updated_at — timestamptz
"""
import datetime
import pytz
import requests


def save_daily(airport, date_str, arrivals, departures, supabase_url, supabase_key, tz='UTC'):
    """Сохраняет агрегированные данные дня в `flight_daily` с upsert по (airport, date).

    Args:
        airport: IATA код аэропорта (например 'HKT', 'CXR')
        date_str: ISO дата 'YYYY-MM-DD'
        arrivals: dict {"count": int, "pax": int, "countries": {...}}
        departures: dict {"count": int, "pax": int, "countries": {...}}
        supabase_url: SUPABASE_URL без trailing slash
        supabase_key: service_role key
        tz: таймзона для updated_at (например 'Asia/Bangkok', 'Asia/Ho_Chi_Minh')
    """
    if not supabase_url or not supabase_key:
        return
    try:
        tzinfo = pytz.timezone(tz) if tz else pytz.UTC
        payload = {
            "airport": airport,
            "date": date_str,
            "arrivals_count": arrivals.get("count", 0),
            "arrivals_pax": arrivals.get("pax", 0),
            "departures_count": departures.get("count", 0),
            "departures_pax": departures.get("pax", 0),
            "arrivals_countries": arrivals.get("countries", {}),
            "departures_countries": departures.get("countries", {}),
            "updated_at": datetime.datetime.now(tzinfo).isoformat(),
        }
        r = requests.post(
            f"{supabase_url.rstrip('/')}/rest/v1/flight_daily",
            headers={
                "apikey": supabase_key,
                "Authorization": f"Bearer {supabase_key}",
                "Content-Type": "application/json",
                "Prefer": "resolution=merge-duplicates",
            },
            json=payload,
            timeout=15,
        )
        if r.status_code in (200, 201):
            print(f"✅ Supabase: {airport} {date_str} сохранён.")
        else:
            print(f"⚠️ Supabase error {r.status_code}: {r.text[:200]}")
    except Exception as e:
        print(f"⚠️ Supabase exception: {e}")
