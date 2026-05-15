-- seed/source.sql
-- Creates demo schemas + tables + sample data in SOURCE DB

CREATE SCHEMA IF NOT EXISTS demo;
CREATE SCHEMA IF NOT EXISTS analytics;

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

-- "Bigger" table for planning decisions (rowcount/partitioning)
CREATE TABLE IF NOT EXISTS analytics.events (
  event_id     BIGSERIAL PRIMARY KEY,
  user_id      BIGINT,
  event_type   TEXT NOT NULL,
  event_ts     TIMESTAMPTZ NOT NULL,
  payload      JSONB
);

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

-- Seed events
INSERT INTO analytics.events (user_id, event_type, event_ts, payload)
SELECT
  (random() * 199 + 1)::bigint,
  (ARRAY['page_view','click','purchase','logout'])[1 + (random()*3)::int],
  now() - (random() * interval '30 days'),
  jsonb_build_object('source', 'seed', 'rand', random())
FROM generate_series(1, 20000)
ON CONFLICT DO NOTHING;
