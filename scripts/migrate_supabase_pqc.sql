-- Phu Quoc (PQC) — isolated Supabase table.
-- Применить один раз в Supabase SQL Editor.

CREATE TABLE IF NOT EXISTS pqc_flight_daily (
    date                  DATE PRIMARY KEY,
    arrivals_count        INTEGER NOT NULL DEFAULT 0,
    arrivals_pax          INTEGER NOT NULL DEFAULT 0,
    departures_count      INTEGER NOT NULL DEFAULT 0,
    departures_pax        INTEGER NOT NULL DEFAULT 0,
    arrivals_countries    JSONB  NOT NULL DEFAULT '{}'::jsonb,
    departures_countries  JSONB  NOT NULL DEFAULT '{}'::jsonb,
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pqc_flight_daily_date ON pqc_flight_daily(date DESC);

ALTER TABLE pqc_flight_daily ENABLE ROW LEVEL SECURITY;
