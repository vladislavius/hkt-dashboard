# CIS Market Analytics Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add three analytics blocks (heatmap, Russian cities by month, seasonality) to the CIS panel in the Phuket flight dashboard.

**Architecture:** All three blocks are rendered at the end of `renderCIS()` in index.html. Data is aggregated from existing accumulated JSON files (already loaded or fetched on demand). Chart.js renders bar/line charts. Heatmap is pure DOM. Full mobile adaptation at all breakpoints.

**Tech Stack:** Vanilla JS, Chart.js (already loaded), CSS Grid, existing DOM helpers.

---

### Task 1: Data aggregation function

**Files:**
- Modify: `/Users/vtsv/phuket_tracker/index.html` (JS section, after `loadAndMergeRange()` ~line 2630)

- [ ] **Step 1: Add `aggregateCISAnalytics()` function**

Insert after the `loadAndMergeRange()` function (after its closing `}`):

```javascript
    // ── CIS Market Analytics aggregation ────────────────────────────────
    async function aggregateCISAnalytics() {
      const avail = (data && data.available_dates) ? data.available_dates : [];
      if (!avail.length) return null;

      // Fetch all accumulated files not yet in cache
      await Promise.all(avail.map(dk => {
        if (data[dk]) return Promise.resolve();
        return fetch('./data/accumulated_' + dk + '.json')
          .then(r => r.ok ? r.json() : null)
          .then(acc => {
            if (!acc || !data) return;
            data[dk] = {
              arrivals:   { count: acc.arrivals.count, pax: acc.arrivals.pax, all: fmtCountries(acc.arrivals.countries) },
              departures: { count: acc.departures.count, pax: acc.departures.pax, all: fmtCountries(acc.departures.countries) },
              arrivals_list: acc.arrivals_list || [], departures_list: acc.departures_list || [],
            };
          }).catch(() => {});
      }));

      // Heatmap: 7 days x 4 time slots (CIS arrivals only)
      // slots: 0=Night(00-06), 1=Morning(06-12), 2=Day(12-18), 3=Evening(18-24)
      const heatmap = Array.from({length: 7}, () => [0,0,0,0]);

      // Cities: { cityName: { 'YYYY-MM': flights } }
      const cities = {};

      // Seasonality: { 'YYYY-MM': { flights: 0, days: Set } }
      const seasonal = {};

      avail.forEach(dk => {
        const p = data[dk];
        if (!p) return;
        const dt = new Date(dk + 'T00:00:00');
        let dow = dt.getDay(); // 0=Sun
        dow = dow === 0 ? 6 : dow - 1; // 0=Mon
        const monthKey = dk.slice(0, 7); // 'YYYY-MM'

        // Seasonality: count CIS flights per month
        if (!seasonal[monthKey]) seasonal[monthKey] = { flights: 0, days: new Set() };
        const cisFl = (p.arrivals.all || []).filter(isCIS);
        const cisCount = cisFl.reduce((s, x) => s + (x.flights || 0), 0);
        seasonal[monthKey].flights += cisCount;
        seasonal[monthKey].days.add(dk);

        // Heatmap + Cities from arrivals_list
        (p.arrivals_list || []).forEach(f => {
          const c = f.country || '';
          if (!CIS.has(c)) return;

          // Heatmap: bucket by arrival time
          const t = f.arr_time || f.dep_time || '';
          const hm = t.match(/T(\d{2})/);
          if (hm) {
            const h = parseInt(hm[1], 10);
            const slot = h < 6 ? 0 : h < 12 ? 1 : h < 18 ? 2 : 3;
            heatmap[dow][slot]++;
          }

          // Russian cities
          if (c === 'Russia') {
            const ap = f.from || '';
            const city = MAP_RU_CITY_JS[ap] || ap;
            if (!city) return;
            if (!cities[city]) cities[city] = {};
            if (!cities[city][monthKey]) cities[city][monthKey] = 0;
            cities[city][monthKey]++;
          }
        });
      });

      return { heatmap, cities, seasonal };
    }
```

- [ ] **Step 2: Commit**

```bash
git add index.html
git commit -m "feat: add aggregateCISAnalytics() data function"
```

---

### Task 2: CSS for three analytics blocks + mobile

**Files:**
- Modify: `/Users/vtsv/phuket_tracker/index.html` (CSS section)

- [ ] **Step 1: Add CSS styles**

Insert before the `/* ── Metrics strip ── */` comment:

```css
    /* ── CIS Analytics: Heatmap ── */
    .cis-heatmap-grid {
      display: grid;
      grid-template-columns: 60px repeat(7, 1fr);
      gap: 3px;
      margin-top: 12px;
    }
    .cis-hm-label {
      font-family: 'DM Mono', monospace;
      font-size: 0.68rem;
      color: var(--muted-2);
      display: flex;
      align-items: center;
      justify-content: flex-end;
      padding-right: 8px;
      white-space: nowrap;
    }
    .cis-hm-dow {
      font-family: 'Outfit', sans-serif;
      font-size: 0.65rem;
      font-weight: 600;
      color: var(--muted-2);
      text-align: center;
      text-transform: uppercase;
      letter-spacing: 0.5px;
    }
    .cis-hm-cell {
      aspect-ratio: 1.6;
      border-radius: 6px;
      display: flex;
      align-items: center;
      justify-content: center;
      font-family: 'DM Mono', monospace;
      font-size: 0.72rem;
      font-weight: 600;
      color: var(--ivory);
      transition: transform 0.15s;
      min-height: 36px;
    }
    .cis-hm-cell:hover { transform: scale(1.08); }

    /* ── CIS Analytics: Cities table ── */
    .cis-cities-table {
      width: 100%;
      border-collapse: collapse;
      margin-top: 12px;
      font-family: 'DM Mono', monospace;
      font-size: 0.72rem;
    }
    .cis-cities-table th {
      color: var(--muted-2);
      font-size: 0.62rem;
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.5px;
      padding: 8px 6px;
      text-align: right;
      border-bottom: 1px solid var(--border);
      white-space: nowrap;
    }
    .cis-cities-table th:first-child { text-align: left; }
    .cis-cities-table td {
      padding: 6px;
      text-align: right;
      color: var(--ivory);
      border-bottom: 1px solid rgba(255,255,255,0.03);
    }
    .cis-cities-table td:first-child {
      text-align: left;
      color: var(--gold-light);
      font-family: 'Outfit', sans-serif;
      font-weight: 500;
    }
    .cis-cities-table .cis-ct-total {
      color: var(--sky);
      font-weight: 600;
    }
    .cis-cities-table tr:hover td { background: rgba(255,255,255,0.02); }

    /* ── CIS Analytics: Seasonality KPI strip ── */
    .cis-season-kpis {
      display: flex;
      gap: 10px;
      margin-top: 12px;
      flex-wrap: wrap;
    }
    .cis-season-kpi {
      background: rgba(255,255,255,0.03);
      border: 1px solid var(--border);
      border-radius: 10px;
      padding: 10px 14px;
      flex: 1;
      min-width: 80px;
      text-align: center;
    }
    .cis-season-kpi-val {
      font-family: 'DM Mono', monospace;
      font-size: 1.1rem;
      font-weight: 600;
      color: var(--sky);
    }
    .cis-season-kpi-lbl {
      font-family: 'Outfit', sans-serif;
      font-size: 0.6rem;
      color: var(--muted-2);
      margin-top: 2px;
      text-transform: uppercase;
      letter-spacing: 0.5px;
    }

    /* ── CIS Analytics: Chart container ── */
    .cis-chart-wrap {
      position: relative;
      width: 100%;
      height: 260px;
      margin-top: 12px;
    }

    /* ── CIS Analytics: Mobile ── */
    @media (max-width: 768px) {
      .cis-heatmap-grid {
        grid-template-columns: 48px repeat(7, 1fr);
        gap: 2px;
      }
      .cis-hm-label { font-size: 0.58rem; padding-right: 4px; }
      .cis-hm-dow { font-size: 0.55rem; }
      .cis-hm-cell { font-size: 0.6rem; min-height: 28px; border-radius: 4px; }
      .cis-cities-table { font-size: 0.62rem; }
      .cis-cities-table th { font-size: 0.55rem; padding: 6px 4px; }
      .cis-cities-table td { padding: 5px 4px; }
      .cis-chart-wrap { height: 200px; }
      .cis-season-kpis { gap: 6px; }
      .cis-season-kpi { padding: 8px 10px; min-width: 60px; }
      .cis-season-kpi-val { font-size: 0.9rem; }
      .cis-season-kpi-lbl { font-size: 0.52rem; }
    }
    @media (max-width: 480px) {
      .cis-heatmap-grid { grid-template-columns: 40px repeat(7, 1fr); }
      .cis-hm-cell { font-size: 0.52rem; min-height: 24px; }
      .cis-hm-label { font-size: 0.5rem; }
      .cis-chart-wrap { height: 180px; }
      .cis-cities-table { font-size: 0.55rem; }
      .cis-cities-table th { font-size: 0.5rem; }
    }
```

- [ ] **Step 2: Commit**

```bash
git add index.html
git commit -m "feat: CSS for CIS analytics blocks + mobile adaptation"
```

---

### Task 3: Heatmap rendering

**Files:**
- Modify: `/Users/vtsv/phuket_tracker/index.html` (JS section, inside or after `renderCIS()` at ~line 3884)

- [ ] **Step 1: Add `renderCISHeatmap()` function**

Insert right before the `function getDefaultPeriod()` line:

```javascript
    // ── CIS Heatmap renderer ───────────────────────────────────────────
    function renderCISHeatmap(heatmap, container) {
      const card = mkCard('🗓', 'Тепловая карта · Прилёты СНГ', '0.05s', 'card-full');
      const grid = make('div', 'cis-heatmap-grid');

      const DOWS = ['Пн','Вт','Ср','Чт','Пт','Сб','Вс'];
      const SLOTS = ['Ночь\n00–06','Утро\n06–12','День\n12–18','Вечер\n18–24'];

      // Find max for color scaling
      let maxVal = 0;
      heatmap.forEach(row => row.forEach(v => { if (v > maxVal) maxVal = v; }));

      // Header row: empty corner + 7 day labels
      grid.appendChild(make('div', '')); // empty corner
      DOWS.forEach(d => {
        const el = make('div', 'cis-hm-dow');
        el.textContent = d;
        grid.appendChild(el);
      });

      // 4 rows x 7 cols
      SLOTS.forEach((slotLabel, si) => {
        const label = make('div', 'cis-hm-label');
        label.textContent = slotLabel.replace('\n', ' ');
        grid.appendChild(label);
        for (let di = 0; di < 7; di++) {
          const val = heatmap[di][si];
          const cell = make('div', 'cis-hm-cell');
          cell.textContent = val || '';
          const intensity = maxVal > 0 ? val / maxVal : 0;
          const r = Math.round(11 + intensity * 80);
          const g = Math.round(15 + intensity * 169);
          const b = Math.round(28 + intensity * 217);
          cell.style.background = 'rgba(' + r + ',' + g + ',' + b + ',' + (0.15 + intensity * 0.55) + ')';
          if (intensity > 0.7) cell.style.color = '#fff';
          cell.title = DOWS[di] + ', ' + SLOTS[si].replace('\n',' ') + ': ' + val + ' рейсов';
          grid.appendChild(cell);
        }
      });

      card.appendChild(grid);
      container.appendChild(card);
    }
```

- [ ] **Step 2: Commit**

```bash
git add index.html
git commit -m "feat: CIS heatmap rendering function"
```

---

### Task 4: Russian cities chart + table rendering

**Files:**
- Modify: `/Users/vtsv/phuket_tracker/index.html` (JS section, after heatmap function)

- [ ] **Step 1: Add `renderCISCities()` function**

Insert right after `renderCISHeatmap()`:

```javascript
    // ── CIS Russian Cities renderer ────────────────────────────────────
    var activeCitiesChart = null;
    function renderCISCities(cities, container) {
      if (activeCitiesChart) { activeCitiesChart.destroy(); activeCitiesChart = null; }
      const card = mkCard('🏙', 'Города РФ · помесячный тренд', '0.1s', 'card-full');

      // Get sorted month keys
      const allMonths = new Set();
      Object.values(cities).forEach(m => Object.keys(m).forEach(k => allMonths.add(k)));
      const months = [...allMonths].sort();
      if (!months.length) return;

      const RU_MON = {
        '01':'Янв','02':'Фев','03':'Мар','04':'Апр','05':'Май','06':'Июн',
        '07':'Июл','08':'Авг','09':'Сен','10':'Окт','11':'Ноя','12':'Дек'
      };
      const monthLabels = months.map(m => {
        const parts = m.split('-');
        return (RU_MON[parts[1]] || parts[1]) + ' ' + parts[0].slice(2);
      });

      // Sort cities by total flights desc
      const cityTotals = Object.entries(cities).map(([name, byMonth]) => ({
        name,
        total: Object.values(byMonth).reduce((s, v) => s + v, 0),
        byMonth,
      })).sort((a, b) => b.total - a.total);

      // Top 5 + rest
      const top5 = cityTotals.slice(0, 5);
      const rest = cityTotals.slice(5);
      const COLORS = ['#E86060','#5BB8F5','#72C98A','#E8A462','#C97BE8','#8A8480'];

      // Stacked bar chart
      const chartWrap = make('div', 'cis-chart-wrap');
      const canvas = document.createElement('canvas');
      canvas.id = 'cisCitiesChart';
      chartWrap.appendChild(canvas);
      card.appendChild(chartWrap);

      const datasets = top5.map((c, i) => ({
        label: c.name,
        data: months.map(m => c.byMonth[m] || 0),
        backgroundColor: COLORS[i] + 'BF',
        borderColor: COLORS[i],
        borderWidth: 1,
        borderRadius: 3,
      }));
      if (rest.length) {
        datasets.push({
          label: 'Остальные',
          data: months.map(m => rest.reduce((s, c) => s + (c.byMonth[m] || 0), 0)),
          backgroundColor: COLORS[5] + 'BF',
          borderColor: COLORS[5],
          borderWidth: 1,
          borderRadius: 3,
        });
      }

      setTimeout(() => {
        const ctx = document.getElementById('cisCitiesChart');
        if (!ctx) return;
        activeCitiesChart = new Chart(ctx.getContext('2d'), {
          type: 'bar',
          data: { labels: monthLabels, datasets: datasets },
          options: {
            responsive: true, maintainAspectRatio: false,
            plugins: {
              legend: { display: true, labels: { color:'#EDE8DF', font:{ family:'DM Mono', size:10 }, boxWidth:10, padding:10 } },
              tooltip: {
                backgroundColor:'rgba(10,15,28,0.95)', borderColor:'rgba(200,169,110,0.2)', borderWidth:1,
                titleColor:'#EDE8DF', bodyColor:'#8A8480', padding:10, cornerRadius:8,
              }
            },
            scales: {
              x: { stacked:true, ticks:{ color:'#8A8480', font:{family:'DM Mono',size:11} }, grid:{display:false} },
              y: { stacked:true, ticks:{ color:'#8A8480', font:{family:'DM Mono',size:11} }, grid:{color:'rgba(255,255,255,0.04)'}, beginAtZero:true }
            }
          }
        });
      }, 60);

      // Table
      const table = document.createElement('table');
      table.className = 'cis-cities-table';
      const thead = document.createElement('thead');
      const headRow = document.createElement('tr');
      ['Город', ...monthLabels, 'Итого'].forEach(h => {
        const th = document.createElement('th');
        th.textContent = h;
        headRow.appendChild(th);
      });
      thead.appendChild(headRow);
      table.appendChild(thead);

      const tbody = document.createElement('tbody');
      cityTotals.slice(0, 15).forEach(c => {
        const tr = document.createElement('tr');
        const tdName = document.createElement('td');
        tdName.textContent = c.name;
        tr.appendChild(tdName);
        months.forEach(m => {
          const td = document.createElement('td');
          const v = c.byMonth[m] || 0;
          td.textContent = v || '—';
          if (!v) td.style.color = 'var(--muted)';
          tr.appendChild(td);
        });
        const tdTotal = document.createElement('td');
        tdTotal.className = 'cis-ct-total';
        tdTotal.textContent = c.total;
        tr.appendChild(tdTotal);
        tbody.appendChild(tr);
      });
      table.appendChild(tbody);
      card.appendChild(table);

      container.appendChild(card);
    }
```

- [ ] **Step 2: Commit**

```bash
git add index.html
git commit -m "feat: CIS Russian cities chart + table rendering"
```

---

### Task 5: Seasonality chart rendering

**Files:**
- Modify: `/Users/vtsv/phuket_tracker/index.html` (JS section, after cities function)

- [ ] **Step 1: Add `renderCISSeasonality()` function**

Insert right after `renderCISCities()`:

```javascript
    // ── CIS Seasonality renderer ───────────────────────────────────────
    var activeSeasonChart = null;
    function renderCISSeasonality(seasonal, container) {
      if (activeSeasonChart) { activeSeasonChart.destroy(); activeSeasonChart = null; }
      const card = mkCard('📈', 'Сезонность · Прилёты СНГ по месяцам', '0.15s', 'card-full');

      const months = Object.keys(seasonal).sort();
      if (!months.length) return;

      const RU_MON = {
        '01':'Янв','02':'Фев','03':'Мар','04':'Апр','05':'Май','06':'Июн',
        '07':'Июл','08':'Авг','09':'Сен','10':'Окт','11':'Ноя','12':'Дек'
      };
      const labels = months.map(m => {
        const parts = m.split('-');
        return (RU_MON[parts[1]] || parts[1]) + ' ' + parts[0].slice(2);
      });
      const values = months.map(m => seasonal[m].flights);

      // Line chart
      const chartWrap = make('div', 'cis-chart-wrap');
      const canvas = document.createElement('canvas');
      canvas.id = 'cisSeasonChart';
      chartWrap.appendChild(canvas);
      card.appendChild(chartWrap);

      setTimeout(() => {
        const ctx = document.getElementById('cisSeasonChart');
        if (!ctx) return;
        activeSeasonChart = new Chart(ctx.getContext('2d'), {
          type: 'line',
          data: {
            labels: labels,
            datasets: [{
              label: 'Рейсы СНГ',
              data: values,
              borderColor: '#5BB8F5',
              backgroundColor: 'rgba(91,184,245,0.12)',
              borderWidth: 3,
              pointRadius: 6,
              pointHoverRadius: 9,
              pointBackgroundColor: '#5BB8F5',
              tension: 0.35,
              fill: true,
            }]
          },
          options: {
            responsive: true, maintainAspectRatio: false,
            plugins: {
              legend: { display: false },
              tooltip: {
                backgroundColor:'rgba(10,15,28,0.95)', borderColor:'rgba(91,184,245,0.3)', borderWidth:1,
                titleColor:'#EDE8DF', bodyColor:'#8A8480', padding:12, cornerRadius:10,
                callbacks: { label: c => ' ' + c.parsed.y + ' рейсов СНГ' }
              }
            },
            scales: {
              x: { ticks:{ color:'#8A8480', font:{family:'DM Mono',size:11} }, grid:{display:false} },
              y: { ticks:{ color:'#8A8480', font:{family:'DM Mono',size:11} }, grid:{color:'rgba(255,255,255,0.04)'}, beginAtZero:true }
            }
          }
        });
      }, 120);

      // Mini KPIs: avg flights/day per month
      const kpiStrip = make('div', 'cis-season-kpis');
      months.forEach(m => {
        const s = seasonal[m];
        const avg = s.days.size > 0 ? (s.flights / s.days.size).toFixed(1) : '0';
        const parts = m.split('-');
        const kpi = make('div', 'cis-season-kpi');
        ap(kpi,
          txt(make('div', 'cis-season-kpi-val'), avg),
          txt(make('div', 'cis-season-kpi-lbl'), (RU_MON[parts[1]] || parts[1]) + ' ' + parts[0].slice(2) + '\nрейс/день')
        );
        kpiStrip.appendChild(kpi);
      });
      card.appendChild(kpiStrip);

      container.appendChild(card);
    }
```

- [ ] **Step 2: Commit**

```bash
git add index.html
git commit -m "feat: CIS seasonality chart + KPI strip rendering"
```

---

### Task 6: Wire into renderCIS() + cleanup

**Files:**
- Modify: `/Users/vtsv/phuket_tracker/index.html` (end of `renderCIS()` function, ~line 3883)

- [ ] **Step 1: Add analytics call at the end of `renderCIS()`**

Find the closing of `renderCIS()` (the line `}` right before `function getDefaultPeriod()`). Insert before that closing brace:

```javascript
      // ── Market analytics blocks ──
      aggregateCISAnalytics().then(analytics => {
        if (!analytics) return;
        const cisAppEl2 = document.getElementById('cis-app');
        if (!cisAppEl2) return;
        renderCISHeatmap(analytics.heatmap, cisAppEl2);
        renderCISCities(analytics.cities, cisAppEl2);
        renderCISSeasonality(analytics.seasonal, cisAppEl2);
      });
```

- [ ] **Step 2: Add chart cleanup in the CIS toggle handler**

Find the existing chart cleanup code (where `activeCisChart` and `activeCisChart2` are destroyed in the CIS toggle handler). Add after them:

```javascript
            if (activeCitiesChart) { activeCitiesChart.destroy(); activeCitiesChart = null; }
            if (activeSeasonChart) { activeSeasonChart.destroy(); activeSeasonChart = null; }
```

- [ ] **Step 3: Commit**

```bash
git add index.html
git commit -m "feat: wire CIS analytics into renderCIS + chart cleanup"
```

---

### Task 7: Test end-to-end + push

- [ ] **Step 1: Open page locally and verify**
  - Open CIS panel (click СНГ button)
  - Scroll down past donuts and rank lists
  - Verify: heatmap grid 4x7 with colored cells
  - Verify: stacked bar chart with city segments
  - Verify: city table with month columns and totals
  - Verify: seasonality line chart
  - Verify: KPI strip with avg flights/day per month

- [ ] **Step 2: Test mobile**
  - Resize to 375px width
  - Verify all three blocks stack properly
  - Verify tables scroll or shrink
  - Verify charts resize

- [ ] **Step 3: Test period switching**
  - Select "3 месяца" preset → analytics should show 3 months
  - Select "Сегодня" → analytics still shows full historical data (because it uses all available_dates)

- [ ] **Step 4: Final commit and push**

```bash
git push
```
