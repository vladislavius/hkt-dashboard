# aggregate.py — агрегация данных из локальных JSON
import json, glob, datetime
from collections import defaultdict, Counter

def load_all():
    files = sorted(glob.glob("data/flights_*.json"))
    daily = []
    for f in files:
        with open(f, "r", encoding="utf-8") as fp:
            daily.append(json.load(fp))
    return daily

def aggregate_period(daily_data, start_date: str = None, end_date: str = None):
    total_arr, total_dep = 0, 0
    countries_arr, countries_dep = Counter(), Counter()
    by_day = {}
    
    for d in daily_data:
        date = d["date"]
        if start_date and date < start_date:
            continue
        if end_date and date > end_date:
            continue
        
        meta = d["meta"]
        arr_count = meta.get("arrivals_count", 0)
        dep_count = meta.get("departures_count", 0)
        
        total_arr += arr_count
        total_dep += dep_count
        by_day[date] = {"arrivals": arr_count, "departures": dep_count}
        
        for c, n in d.get("arrivals_by_country", {}).items():
            countries_arr[c] += n
        for c, n in d.get("departures_by_country", {}).items():
            countries_dep[c] += n
    
    return {
        "period": f"{start_date or 'start'} → {end_date or 'end'}",
        "total_arrivals": total_arr,
        "total_departures": total_dep,
        "days_count": len(by_day),
        "avg_arrivals_per_day": round(total_arr / max(len(by_day), 1), 1),
        "avg_departures_per_day": round(total_dep / max(len(by_day), 1), 1),
        "top_arrival_countries": dict(countries_arr.most_common(5)),
        "top_departure_countries": dict(countries_dep.most_common(5)),
        "by_day": by_day
    }

if __name__ == "__main__":
    import sys
    
    daily = load_all()
    if not daily:
        print("⚠️ Нет данных в папке data/. Запустите collector.py хотя бы 1 день.")
        exit()
    
    # Аргументы: python3 aggregate.py [start_date] [end_date]
    start = sys.argv[1] if len(sys.argv) > 1 else None
    end = sys.argv[2] if len(sys.argv) > 2 else None
    
    result = aggregate_period(daily, start, end)
    
    print(f"\n📊 Статистика: {result['period']}")
    print(f"📅 Дней в выборке: {result['days_count']}")
    print(f"✈️ Всего прилётов: {result['total_arrivals']} (среднее: {result['avg_arrivals_per_day']}/день)")
    print(f"🛫 Всего вылетов: {result['total_departures']} (среднее: {result['avg_departures_per_day']}/день)")
    
    if result['top_arrival_countries']:
        print("\n🌍 Топ стран прилёта (за период):")
        for c, n in result['top_arrival_countries'].items():
            print(f"  🇺🇳 {c}: {n}")
    
    if result['top_departure_countries']:
        print("\n🌍 Топ стран вылета (за период):")
        for c, n in result['top_departure_countries'].items():
            print(f"  🇺🇳 {c}: {n}")
    
    # Экспорт в CSV по дням (опционально)
    if result['by_day']:
        import csv
        with open("phuket_daily_summary.csv", "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["date", "arrivals", "departures", "total"])
            for date in sorted(result['by_day'].keys()):
                d = result['by_day'][date]
                w.writerow([date, d['arrivals'], d['departures'], d['arrivals']+d['departures']])
        print(f"\n✅ Детали по дням сохранены в phuket_daily_summary.csv")
