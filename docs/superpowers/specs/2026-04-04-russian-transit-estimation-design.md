# Дизайн: Оценка транзитных российских туристов

**Дата:** 2026-04-04  
**Статус:** Approved

---

## Проблема

AviationStack API возвращает только последний аэропорт вылета. Рейс Москва→Дубай→Пхукет отображается как "UAE". Реальные российские туристы, летящие через хабы, не попадают в статистику России.

## Цель

Добавить расчётную строку "Россия (транзит~)" в рейтинг стран — визуально отличную от прямых рейсов, с пометкой что это оценка.

---

## Архитектура

```
TAT Intelligence Center (ежемесячно)
        ↓
   tat_scraper.py  →  data/tat_stats.json
        ↓
   collector.py читает TAT + наши накопленные данные
   → считает russia_transit динамически
   → добавляет поле russia_transit в accumulated_YYYY-MM-DD.json
        ↓
   dashboard.json: russia_direct + russia_transit отдельными полями
        ↓
   index.html: две строки 🇷🇺, транзит выделен визуально
```

---

## Логика расчёта коэффициентов

### Источник данных
TAT Intelligence Center публикует ежемесячную статистику въезда иностранных туристов по странам с задержкой ~30-45 дней.

### Формула

```
TAT_russians_thailand   = X  (из tat_stats.json за последний доступный месяц)
HKT_share               = 0.38  (Пхукет получает ~38% российских туристов Таиланда)
hkt_russians_estimated  = X * HKT_share

direct_russia_pax       = сумма pax по всем рейсам из MAP_C['Russia'] за период
transit_russians        = max(0, hkt_russians_estimated - direct_russia_pax)

TRANSIT_HUBS = ['UAE', 'Turkey', 'Qatar', 'China', 'India']
hub_total_pax = сумма pax по всем TRANSIT_HUBS за период
для каждого hub:
    russia_via_hub = transit_russians * (hub_pax / hub_total_pax)
```

Итог: `russia_transit_total = sum(russia_via_hub for all hubs)`  
Количество рейсов: `russia_transit_flights = transit_russians / avg_pax_per_flight`

### Константа HKT_share
Начальное значение 0.38 основано на данных Ростуризма и TAT за 2023-2024. Хранится в `collector.py` как `HKT_RUSSIAN_SHARE = 0.38` — обновляется вручную при необходимости.

---

## Компоненты реализации

### 1. `tat_scraper.py` (новый файл)

Парсит TAT Intelligence Center, извлекает количество российских туристов за последний доступный месяц. Сохраняет в `data/tat_stats.json`:

```json
{
  "updated": "2026-04-01T00:00:00",
  "source": "TAT Intelligence Center",
  "monthly": [
    {"month": "2026-01", "russian_tourists": 145230},
    {"month": "2025-12", "russian_tourists": 189400}
  ]
}
```

Fallback: если парсинг недоступен — использует последнее успешное значение из файла. Если файл пустой — применяет захардкоженный дефолт `{"2026-01": 130000}` (среднемесячное значение за 2024).

### 2. Изменения в `collector.py`

Добавляется функция `calc_russian_transit(period_data, tat_data)`:
- Загружает `data/tat_stats.json`
- Берёт среднее за последние 3 доступных месяца TAT (сглаживание)
- Применяет формулу выше
- Возвращает `{"flights": N, "pax": M, "estimated": True}`

В `accumulated_YYYY-MM-DD.json` добавляется поле:
```json
"russia_transit": {"flights": 34, "pax": 4900, "estimated": true}
```

В `dashboard.json` в каждом периоде (today/week/month/...) добавляется:
```json
"russia_transit": {"flights": 34, "pax": 4900, "estimated": true}
```

### 3. `.github/workflows/tat-scrape.yml` (новый файл)

Cron job: `0 2 1 * *` (1-го числа каждого месяца в 02:00 UTC).  
Запускает `python tat_scraper.py`, коммитит обновлённый `data/tat_stats.json`.

### 4. Изменения в `index.html`

**CSS:** новый класс `.rank-row-estimated`:
```css
.rank-row-estimated {
  opacity: 0.72;
  border-left: 2px dashed var(--gold-border);
  padding-left: 10px;
}
.rank-row-estimated .rank-num { color: var(--gold); }
```

**Рендер:** после строки прямой России вставляется строка транзита:
```
🇷🇺 Россия (транзит~)    34 →  /  ~4 900 чел.
```
Числа предваряются символом `~`. Tooltip при наведении: "Оценка на основе данных TAT · Пхукет ~38% от общего турпотока из РФ".

---

## Что НЕ входит в скоуп

- Airline-level fingerprinting (отдельная задача)
- Парсинг других источников кроме TAT
- Разбивка транзита по городам России
- Автообновление HKT_RUSSIAN_SHARE

---

## Риски

| Риск | Митигация |
|------|-----------|
| TAT меняет структуру сайта | Fallback на последнее сохранённое значение |
| HKT_share устаревает | Захардкоженный комментарий с датой последней проверки |
| transit < 0 (прямых больше чем TAT-оценка) | `max(0, ...)` — показываем 0, не падаем |
| TAT данные только по всему Таиланду | Принимаем HKT_share как допущение, пометка "оценка" снимает претензии |
