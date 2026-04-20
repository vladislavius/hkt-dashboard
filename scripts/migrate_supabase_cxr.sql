-- Cam Ranh (CXR) — isolated Supabase table.
-- Не трогает существующую flight_daily (HKT). Применить один раз в Supabase SQL Editor.

CREATE TABLE IF NOT EXISTS cxr_flight_daily (
    date                  DATE PRIMARY KEY,
    arrivals_count        INTEGER NOT NULL DEFAULT 0,
    arrivals_pax          INTEGER NOT NULL DEFAULT 0,
    departures_count      INTEGER NOT NULL DEFAULT 0,
    departures_pax        INTEGER NOT NULL DEFAULT 0,
    arrivals_countries    JSONB  NOT NULL DEFAULT '{}'::jsonb,
    departures_countries  JSONB  NOT NULL DEFAULT '{}'::jsonb,
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_cxr_flight_daily_date ON cxr_flight_daily(date DESC);

-- RLS enabled (closed by default, только service_role пишет/читает — как у flight_daily)
ALTER TABLE cxr_flight_daily ENABLE ROW LEVEL SECURITY;

-- Проверка:
-- SELECT COUNT(*), MIN(date), MAX(date) FROM cxr_flight_daily;
-- Applied 2026-04-20 via mcp__supabase__apply_migration (name=create_cxr_flight_daily)
