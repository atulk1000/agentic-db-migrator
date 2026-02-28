-- seed/source.sql (V2)
-- Creates demo schemas + tables + sample data in SOURCE DB
-- Includes: PK/FK, non-PK indexes, partitioned table, materialized view (+ index), and a UDF.

-- ------------------------------------------------------------
-- Schemas
-- ------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS demo;
CREATE SCHEMA IF NOT EXISTS analytics;

-- ------------------------------------------------------------
-- Base tables
-- ------------------------------------------------------------

-- Small table
CREATE TABLE IF NOT EXISTS demo.users (
  user_id      BIGSERIAL PRIMARY KEY,
  email        TEXT UNIQUE NOT NULL,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Medium-ish table with FK
CREATE TABLE IF NOT EXISTS demo.orders (
  order_id     BIGSERIAL PRIMARY KEY,
  user_id      BIGINT NOT NULL REFERENCES demo.users(user_id),
  amount_usd   NUMERIC(10,2) NOT NULL,
  status       TEXT NOT NULL,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- "Bigger" table for planning decisions
CREATE TABLE IF NOT EXISTS analytics.events (
  event_id     BIGSERIAL PRIMARY KEY,
  user_id      BIGINT,
  event_type   TEXT NOT NULL,
  event_ts     TIMESTAMPTZ NOT NULL,
  payload      JSONB
);

-- ------------------------------------------------------------
-- Extra objects to test V2 capabilities
-- ------------------------------------------------------------

-- Non-PK / non-constraint indexes (V2 should discover + recreate these)
CREATE INDEX IF NOT EXISTS idx_events_event_ts     ON analytics.events (event_ts);
CREATE INDEX IF NOT EXISTS idx_orders_created_at   ON demo.orders (created_at);

-- A simple UDF (V2 can recreate if migration.include_udfs=true)
CREATE OR REPLACE FUNCTION demo.is_paid(status TEXT)
RETURNS BOOLEAN
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT status = 'paid'
$$;

-- Partitioned table (V2 should detect parent, then plan/copy children)
-- Note: composite PK includes partition key, which is typical for range partitioning.
CREATE TABLE IF NOT EXISTS analytics.events_p (
  event_id   BIGSERIAL,
  user_id    BIGINT,
  event_type TEXT NOT NULL,
  event_ts   TIMESTAMPTZ NOT NULL,
  payload    JSONB,
  PRIMARY KEY (event_id, event_ts)
) PARTITION BY RANGE (event_ts);

-- Create a couple of month partitions (use 2026 to keep values "recent")
CREATE TABLE IF NOT EXISTS analytics.events_p_2026_01
  PARTITION OF analytics.events_p
  FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');

CREATE TABLE IF NOT EXISTS analytics.events_p_2026_02
  PARTITION OF analytics.events_p
  FOR VALUES FROM ('2026-02-01') TO ('2026-03-01');

-- Helpful index on partitioned table (creates indexes on each partition in modern PG)
CREATE INDEX IF NOT EXISTS idx_events_p_event_ts ON analytics.events_p (event_ts);

-- ------------------------------------------------------------
-- Seed data
-- ------------------------------------------------------------

-- Seed users
INSERT INTO demo.users (email)
SELECT 'user' || g || '@example.com'
FROM generate_series(1, 200) g
ON CONFLICT DO NOTHING;

-- Seed orders
INSERT INTO demo.orders (user_id, amount_usd, status)
SELECT
  (random() * 199 + 1)::bigint,
  round((random() * 500)::numeric, 2),
  (ARRAY['created','paid','shipped','refunded'])[1 + (random()*3)::int]
FROM generate_series(1, 2000)
ON CONFLICT DO NOTHING;

-- Seed events (non-partitioned)
INSERT INTO analytics.events (user_id, event_type, event_ts, payload)
SELECT
  (random() * 199 + 1)::bigint,
  (ARRAY['page_view','click','purchase','logout'])[1 + (random()*3)::int],
  now() - (random() * interval '30 days'),
  jsonb_build_object('source', 'seed', 'rand', random())
FROM generate_series(1, 20000)
ON CONFLICT DO NOTHING;

-- Seed partitioned events (spread across Jan/Feb 2026 so both partitions get rows)
INSERT INTO analytics.events_p (user_id, event_type, event_ts, payload)
SELECT
  (random() * 199 + 1)::bigint,
  (ARRAY['page_view','click','purchase','logout'])[1 + (random()*3)::int],
  TIMESTAMPTZ '2026-01-01' + (random() * interval '50 days'),
  jsonb_build_object('source', 'seed_partitioned', 'rand', random())
FROM generate_series(1, 5000)
ON CONFLICT DO NOTHING;

-- ------------------------------------------------------------
-- Materialized view + index (V2 should rebuild after tables)
-- ------------------------------------------------------------

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_events_by_type AS
SELECT event_type, count(*) AS n
FROM analytics.events
GROUP BY event_type
WITH DATA;

CREATE INDEX IF NOT EXISTS idx_mv_events_by_type ON analytics.mv_events_by_type (event_type);
