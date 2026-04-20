# CXR (Камрань) integration — todo

План: `/Users/vtsv/.claude/plans/crispy-singing-eclipse.md`

## Этапы

- [ ] **Этап 1:** Фикс СНГ в HKT — заменить CIS Set в `index.html:2586` с 15 → 12 стран (убрать Прибалтику)
- [ ] **Этап 2:** Вынести общий код в `core/` (mappings, aviation, telegram, supabase, analyze, aggregate_periods)
- [ ] **Этап 3:** Supabase миграция — `ALTER TABLE flight_daily ADD COLUMN airport` + backfill HKT с 2025-09
- [ ] **Этап 4:** CXR collector — `cxr/collector.py` со Scrapling + Aviationstack fallback
- [ ] **Этап 5:** CXR frontend — `cxr/index.html` (копия + 5 замен), `cxr/api/data.js`
- [ ] **Этап 6:** Vercel routing — rewrites для `/cxr` в `vercel.json`
- [ ] **Этап 7:** GitHub Actions — второй job `update-cxr` в `update.yml`
- [ ] **Этап 8:** E2E + docs — прогон, deploy, README, `.env.example`

## Key decisions

- Архитектура C (подпапка `cxr/`)
- СНГ = 12 стран: Russia, Belarus, Kazakhstan, Kyrgyzstan, Tajikistan, Armenia, Azerbaijan, Moldova, Uzbekistan, Turkmenistan, Ukraine, Georgia
- Supabase: ALTER + composite PK `(airport, date)`
- Один Telegram-бот, префиксы `[HKT]` / `[CXR]`
- Один GitHub workflow с двумя job'ами

## Review (заполнится после завершения)

(пусто)
