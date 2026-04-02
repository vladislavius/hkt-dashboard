# test_api.py — минимальный рабочий тест
import requests
import json

API_KEY = "f0ab95f5da9a66b28d6d9fdbcf4cdd02"
URL = "https://api.aviationstack.com/v1/flights"

params = {
    "access_key": API_KEY,
    "arr_iata": "HKT",  # 👈 Только arr_iata, без flight_status и flight_date
    "limit": 5
}

resp = requests.get(URL, params=params)
print(f"Status: {resp.status_code}")

if resp.status_code == 200:
    data = resp.json()
    print(f"✅ Всего рейсов в ответе API: {data['pagination']['total']}")
    print(f"✅ Получено записей: {len(data['data'])}")
    if data['data']:
        f = data['data'][0]
        print(f"🛬 Пример: {f['departure']['iata']} → {f['arrival']['iata']} | {f['flight_status']}")
else:
    print(f"❌ Ошибка: {resp.text}")
