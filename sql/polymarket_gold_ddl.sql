-- Polymarket GOLD model for NeonDB
-- Dimensions + fact tables for analytical queries (Tableau / API)

CREATE SCHEMA IF NOT EXISTS polymarket;

DROP TABLE IF EXISTS polymarket.fact_market_outcome CASCADE;
DROP TABLE IF EXISTS polymarket.fact_market_snapshot CASCADE;
DROP TABLE IF EXISTS polymarket.fact_event_tag CASCADE;
DROP TABLE IF EXISTS polymarket.dim_market CASCADE;
DROP TABLE IF EXISTS polymarket.dim_event CASCADE;
DROP TABLE IF EXISTS polymarket.dim_series CASCADE;
DROP TABLE IF EXISTS polymarket.dim_tag CASCADE;
DROP TABLE IF EXISTS polymarket.dim_time CASCADE;

CREATE TABLE polymarket.dim_time (
  time_id INTEGER PRIMARY KEY,
  date DATE NOT NULL UNIQUE,
  year INTEGER,
  quarter INTEGER,
  month INTEGER,
  day INTEGER
);

CREATE TABLE polymarket.dim_series (
  series_id BIGINT PRIMARY KEY,
  title TEXT,
  slug TEXT,
  ticker TEXT,
  series_type TEXT,
  recurrence TEXT
);

CREATE TABLE polymarket.dim_event (
  event_id BIGINT PRIMARY KEY,
  series_id BIGINT,
  title TEXT,
  category TEXT,
  start_ts TIMESTAMPTZ,
  end_ts TIMESTAMPTZ,
  active BOOLEAN,
  closed BOOLEAN,
  archived BOOLEAN,
  CONSTRAINT fk_dim_event_series FOREIGN KEY (series_id) REFERENCES polymarket.dim_series(series_id)
);

CREATE TABLE polymarket.dim_tag (
  tag_id BIGINT PRIMARY KEY,
  name TEXT,
  slug TEXT,
  parent_tag_id BIGINT,
  CONSTRAINT fk_dim_tag_parent FOREIGN KEY (parent_tag_id) REFERENCES polymarket.dim_tag(tag_id)
);

CREATE TABLE polymarket.dim_market (
  market_id BIGINT PRIMARY KEY,
  event_id BIGINT,
  series_id BIGINT,
  question TEXT,
  category TEXT,
  subcategory TEXT,
  start_ts TIMESTAMPTZ,
  end_ts TIMESTAMPTZ,
  active BOOLEAN,
  closed BOOLEAN,
  archived BOOLEAN,
  resolution_source TEXT,
  CONSTRAINT fk_dim_market_event FOREIGN KEY (event_id) REFERENCES polymarket.dim_event(event_id),
  CONSTRAINT fk_dim_market_series FOREIGN KEY (series_id) REFERENCES polymarket.dim_series(series_id)
);

-- Main analytical fact (grain: market + snapshot timestamp)
CREATE TABLE polymarket.fact_market_snapshot (
  snapshot_id BIGSERIAL PRIMARY KEY,
  market_id BIGINT NOT NULL,
  event_id BIGINT,
  series_id BIGINT,
  snapshot_ts TIMESTAMPTZ NOT NULL,
  snapshot_time_id INTEGER,
  end_time_id INTEGER,
  tag_ids_json TEXT,
  outcome_labels_json TEXT,
  outcome_prices_json TEXT,
  yes_probability DOUBLE PRECISION,
  no_probability DOUBLE PRECISION,
  implied_total_probability DOUBLE PRECISION,
  arbitrage_gap DOUBLE PRECISION,
  active BOOLEAN,
  closed BOOLEAN,
  archived BOOLEAN,
  liquidity DOUBLE PRECISION,
  volume DOUBLE PRECISION,
  volume24hr DOUBLE PRECISION,
  volume1wk DOUBLE PRECISION,
  volume1mo DOUBLE PRECISION,
  volume1yr DOUBLE PRECISION,
  best_bid DOUBLE PRECISION,
  best_ask DOUBLE PRECISION,
  last_trade_price DOUBLE PRECISION,
  spread DOUBLE PRECISION,
  one_day_price_change DOUBLE PRECISION,
  one_week_price_change DOUBLE PRECISION,
  one_month_price_change DOUBLE PRECISION,
  one_year_price_change DOUBLE PRECISION,
  CONSTRAINT uq_fact_market_snapshot UNIQUE (market_id, snapshot_ts),
  CONSTRAINT fk_fact_snapshot_market FOREIGN KEY (market_id) REFERENCES polymarket.dim_market(market_id),
  CONSTRAINT fk_fact_snapshot_event FOREIGN KEY (event_id) REFERENCES polymarket.dim_event(event_id),
  CONSTRAINT fk_fact_snapshot_series FOREIGN KEY (series_id) REFERENCES polymarket.dim_series(series_id),
  CONSTRAINT fk_fact_snapshot_snapshot_time FOREIGN KEY (snapshot_time_id) REFERENCES polymarket.dim_time(time_id),
  CONSTRAINT fk_fact_snapshot_end_time FOREIGN KEY (end_time_id) REFERENCES polymarket.dim_time(time_id)
);

CREATE INDEX idx_dim_market_event ON polymarket.dim_market(event_id);
CREATE INDEX idx_dim_event_category ON polymarket.dim_event(category);
CREATE INDEX idx_fact_snapshot_ts ON polymarket.fact_market_snapshot(snapshot_ts);
CREATE INDEX idx_fact_snapshot_volume ON polymarket.fact_market_snapshot(volume DESC);
CREATE INDEX idx_fact_snapshot_liquidity ON polymarket.fact_market_snapshot(liquidity DESC);
CREATE INDEX idx_fact_snapshot_yes_prob ON polymarket.fact_market_snapshot(yes_probability DESC);
