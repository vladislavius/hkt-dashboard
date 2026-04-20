"""Обобщённый Aviationstack-клиент и analyze для любого аэропорта."""
from collections import defaultdict
import requests

from core.mappings import CAP, LOAD_FACTOR, MAP_C, MAP_RU_CITY

BASE_URL = "https://api.aviationstack.com/v1/flights"


def fetch_flights_aviationstack(iata, direction, api_keys, primary_idx=0):
    """Тянет рейсы через Aviationstack API с автоматическим fallback на остальные ключи.

    Args:
        iata: IATA код аэропорта (HKT, CXR, ...)
        direction: "arrival" или "departure"
        api_keys: список API ключей для ротации
        primary_idx: индекс ключа, который пробуем первым

    Returns:
        (flights_list, key_number_used) или ([], 0) при полном отказе
    """
    key_param = "arr_iata" if direction == "arrival" else "dep_iata"
    primary = api_keys[primary_idx] if primary_idx < len(api_keys) else api_keys[0]
    keys_to_try = [primary] + [k for k in api_keys if k != primary]

    for i, k in enumerate(keys_to_try):
        try:
            r = requests.get(
                BASE_URL,
                params={"access_key": k, key_param: iata, "limit": 100},
                timeout=20,
            )
            data = r.json()
            if "error" in data:
                code = data["error"].get("code", "")
                print(f"⚠️ Aviationstack key #{i+1} error: {code} — trying next")
                continue
            r.raise_for_status()
            if i > 0:
                print(f"ℹ️ Aviationstack used fallback key #{i+1}")
            return data.get("data", []), i + 1
        except Exception as e:
            print(f"⚠️ Aviationstack key #{i+1} exception: {e} — trying next")
            continue

    print("❌ All Aviationstack keys failed")
    return [], 0


def analyze_aviationstack(flights, direction, domestic):
    """Анализирует Aviationstack flights, группирует по датам и странам.

    Args:
        flights: raw flight list от Aviationstack
        direction: "arrival" или "departure"
        domestic: set внутренних IATA-кодов (исключаются)

    Returns:
        dict {date_str: {count, pax, countries, stats, flight_list}}
    """
    by_date = {}
    for f in flights:
        d = f.get("flight_date")
        if d:
            by_date.setdefault(d, []).append(f)

    res = {}
    for date, fl in by_date.items():
        cnt, pax = 0, 0
        st = {"completed": 0, "upcoming": 0, "cancelled": 0}
        ctry = defaultdict(lambda: {"flights": 0, "pax": 0})
        flight_list = []

        for f in fl:
            dep = (f.get("departure") or {}).get("iata", "")
            arr = (f.get("arrival") or {}).get("iata", "")
            if (direction == "arrival" and dep in domestic) or (direction == "departure" and arr in domestic):
                continue
            cnt += 1
            ac_iata = (f.get("aircraft") or {}).get("iata", "")
            flight_pax = int(CAP.get(ac_iata, 180) * LOAD_FACTOR)
            pax += flight_pax
            airport = dep if direction == "arrival" else arr
            country = MAP_C.get(airport, f"Other({airport})")
            if country == "Russia":
                city = MAP_RU_CITY.get(airport, airport)
                ctry[city]["flights"] += 1
                ctry[city]["pax"] += flight_pax
                ctry[city]["country"] = "Russia"
            else:
                ctry[country]["flights"] += 1
                ctry[country]["pax"] += flight_pax

            s = (f.get("flight_status") or "").lower().strip()
            if direction == "departure":
                if s in ("departed", "landed", "diverted", "active", "en route", "en-route",
                         "incidents", "taxi-out", "pushback", "returned", "taxi"):
                    st["completed"] += 1
                elif s in ("scheduled", "taxiing", "boarding", "expected", "estimated", "gate", "holding"):
                    st["upcoming"] += 1
                elif s == "cancelled":
                    st["cancelled"] += 1
            else:
                if s in ("landed", "arrived", "diverted"):
                    st["completed"] += 1
                elif s in ("scheduled", "active", "en route", "en-route", "taxiing", "taxi",
                           "boarding", "expected", "estimated"):
                    st["upcoming"] += 1
                elif s == "cancelled":
                    st["cancelled"] += 1

            fn = (f.get("flight") or {}).get("iata") or (f.get("flight") or {}).get("icao", "")
            airline = (f.get("airline") or {}).get("name", "")
            dep_sched = (f.get("departure") or {}).get("scheduled", "")
            arr_sched = (f.get("arrival") or {}).get("scheduled", "")
            rec = {
                "fn": fn,
                "airline": airline,
                "status": s,
                "pax": flight_pax,
                "dep_time": dep_sched[:16] if dep_sched else "",
                "arr_time": arr_sched[:16] if arr_sched else "",
            }
            if direction == "arrival":
                rec["from"] = dep
                rec["country"] = MAP_C.get(dep, "")
            else:
                rec["to"] = arr
                rec["country"] = MAP_C.get(arr, "")
            flight_list.append(rec)

        res[date] = {"count": cnt, "pax": pax, "countries": dict(ctry),
                     "stats": st, "flight_list": flight_list}
    return res
