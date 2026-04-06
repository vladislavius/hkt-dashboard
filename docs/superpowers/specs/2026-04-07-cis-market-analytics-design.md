# CIS Market Analytics — Design Spec

## Context
Phuket tourism agency needs deeper analytics on CIS tourist arrivals to correlate with advertising, transfers, and sales. Dashboard already has 128 days of accumulated flight data (Dec 2025 – Apr 2026). Three new analysis blocks are added to the existing CIS panel.

## Blocks

### 1. Heatmap: CIS Arrivals by Day-of-Week / Time
- Grid: 7 columns (Mon–Sun) x 4 rows (Night 00–06, Morning 06–12, Day 12–18, Evening 18–24)
- Cell color: intensity scale (dark → bright sky blue) based on flight count
- Cell content: number of CIS flights in that slot
- Data source: all accumulated files for selected period; parse `arrivals_list` entries, filter CIS, bucket by weekday + 4h time slot
- Title: "Тепловая карта · Прилёты СНГ"

### 2. Russian Cities by Month
- **Chart:** stacked bar — X axis = months (Dec 25, Jan 26, ..., Apr 26), Y axis = flight count, segments = top 5 cities + "Остальные"
- **Table:** rows = cities sorted by total desc, columns = months + total column
- Data source: accumulated files grouped by month; filter entries with `country: "Russia"`, group by city name (using MAP_RU_CITY_JS mapping from collector)
- Title: "Города РФ · помесячный тренд"

### 3. CIS Seasonality
- **Chart:** line — X axis = months, Y axis = total CIS flights
- Single line for current season (Dec 2025 – Apr 2026); second YoY line when data accumulates
- Below chart: mini KPI strip — avg flights/day per month
- Data source: accumulated files grouped by month, filter CIS countries
- Title: "Сезонность · Прилёты СНГ по месяцам"

## Placement
All three blocks appended to `#cis-app` inside `renderCIS()`, after existing donut charts and rank lists. Full container width. Order: heatmap → cities → seasonality.

## Data Pipeline
1. On CIS panel open, fetch all accumulated JSON files (already cached in `data[dateKey]` after first load)
2. For files not yet loaded: batch-fetch in parallel (same as `loadAndMergeRange`)
3. Aggregate once into three structures: heatmap buckets, city-month matrix, monthly totals
4. Render using Chart.js (bar, line) and DOM table (heatmap, city table)

## File to Modify
- `/Users/vtsv/phuket_tracker/index.html` — HTML/CSS/JS (single file app)

## Key Existing Code to Reuse
- `fmtCountries()` — country aggregation
- `CIS` Set, `isCIS()` — CIS country filter
- `MAP_RU_CITY_JS` — IATA → Russian city name mapping
- `CIS_COLORS`, `CIS_LABELS` — color/label maps
- Chart.js already loaded (donut charts exist)
- `loadAndMergeRange()` — parallel file fetching pattern
- `mkCard()`, `make()`, `txt()`, `ap()` — DOM helpers

## Verification
1. Open CIS panel, verify all three blocks render below existing content
2. Switch period via calendar → blocks update with new data
3. Check heatmap colors scale correctly (busiest slot = brightest)
4. Verify city table sums match chart segments
5. Test on mobile (≤768px) — blocks should stack single-column
