import json
import os
from pathlib import Path

import pandas as pd
from deltalake import DeltaTable
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent
load_dotenv(BASE_DIR / ".env")

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("Missing DATABASE_URL. Set it in api/.env")

DELTA_ROOT = Path(os.getenv("DELTA_ROOT", PROJECT_ROOT / "polymarket" / "nba"))
DDL_FILE = BASE_DIR / "sql" / "polymarket_gold_ddl.sql"
SCHEMA = "polymarket"


def read_delta(name: str) -> pd.DataFrame:
    path = DELTA_ROOT / name
    if not path.exists():
        raise FileNotFoundError(f"Missing Delta table path: {path}")
    return DeltaTable(str(path)).to_pandas()


def parse_json_cell(cell):
    if cell is None:
        return []
    if isinstance(cell, float) and pd.isna(cell):
        return []
    if isinstance(cell, (list, dict)):
        return cell
    if isinstance(cell, str):
        value = cell.strip()
        if not value:
            return []
        try:
            return json.loads(value)
        except Exception:
            return []
    return []


def to_bool(series: pd.Series) -> pd.Series:
    def conv(v):
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return None
        if isinstance(v, bool):
            return v
        s = str(v).strip().lower()
        if s in {"true", "1", "t", "yes", "y"}:
            return True
        if s in {"false", "0", "f", "no", "n"}:
            return False
        return None

    return series.map(conv)


def to_num(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def to_ts(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce", utc=True)


def time_id_from_ts(series: pd.Series) -> pd.Series:
    dt = pd.to_datetime(series, errors="coerce", utc=True)
    return dt.dt.strftime("%Y%m%d").where(dt.notna(), None)


def normalize_probability(value):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    v = float(value)
    if 0.0 <= v <= 1.0:
        return v
    if 1.0 < v <= 100.0:
        return v / 100.0
    return None


def serialize_json(value) -> str:
    return json.dumps(value, ensure_ascii=False)


def probability_from_outcomes(outcomes, prices, target_label):
    if not isinstance(outcomes, list) or not isinstance(prices, list):
        return None
    for idx, label in enumerate(outcomes):
        if str(label).strip().lower() == target_label:
            if idx >= len(prices):
                return None
            val = pd.to_numeric(prices[idx], errors="coerce")
            if pd.isna(val):
                return None
            return normalize_probability(float(val))
    return None


def build_dimensions(markets: pd.DataFrame, events: pd.DataFrame, tags: pd.DataFrame):
    events = events.copy()
    tags = tags.copy()
    markets = markets.copy()

    events["event_id"] = pd.to_numeric(events.get("id"), errors="coerce")
    events = events[events["event_id"].notna()].copy()
    events["event_id"] = events["event_id"].astype("int64")

    series_rows = []
    event_series_id = {}
    for _, row in events.iterrows():
        event_id = int(row["event_id"])
        payload = parse_json_cell(row.get("series"))
        if isinstance(payload, list) and payload:
            s = payload[0] if isinstance(payload[0], dict) else {}
            sid = pd.to_numeric(s.get("id"), errors="coerce")
            if pd.notna(sid):
                sid = int(sid)
                event_series_id[event_id] = sid
                series_rows.append(
                    {
                        "series_id": sid,
                        "title": s.get("title"),
                        "slug": s.get("slug") or row.get("seriesSlug"),
                        "ticker": s.get("ticker"),
                        "series_type": s.get("seriesType"),
                        "recurrence": s.get("recurrence"),
                    }
                )

    dim_series = pd.DataFrame(series_rows).drop_duplicates(subset=["series_id"]) if series_rows else pd.DataFrame(
        columns=["series_id", "title", "slug", "ticker", "series_type", "recurrence"]
    )

    dim_event = pd.DataFrame(
        {
            "event_id": events["event_id"],
            "series_id": events["event_id"].map(event_series_id),
            "title": events.get("title"),
            "category": events.get("category"),
            "start_ts": to_ts(events.get("startDate")),
            "end_ts": to_ts(events.get("endDate")),
            "active": to_bool(events.get("active")),
            "closed": to_bool(events.get("closed")),
            "archived": to_bool(events.get("archived")),
        }
    ).drop_duplicates(subset=["event_id"])

    tag_id_col = "id" if "id" in tags.columns else "tag_id"
    name_col = "name" if "name" in tags.columns else "label"
    parent_col = "parentId" if "parentId" in tags.columns else ("parent_id" if "parent_id" in tags.columns else None)

    dim_tag = pd.DataFrame(
        {
            "tag_id": pd.to_numeric(tags.get(tag_id_col), errors="coerce"),
            "name": tags.get(name_col),
            "slug": tags.get("slug"),
            "parent_tag_id": pd.to_numeric(tags.get(parent_col), errors="coerce") if parent_col else None,
        }
    )
    dim_tag = dim_tag[dim_tag["tag_id"].notna()].copy()
    dim_tag["tag_id"] = dim_tag["tag_id"].astype("int64")
    dim_tag = dim_tag.drop_duplicates(subset=["tag_id"])

    if "event_id" not in markets.columns:
        def extract_event_id(cell):
            payload = parse_json_cell(cell)
            if isinstance(payload, list) and payload and isinstance(payload[0], dict):
                return pd.to_numeric(payload[0].get("id"), errors="coerce")
            if isinstance(payload, dict):
                return pd.to_numeric(payload.get("id"), errors="coerce")
            return None

        markets["event_id"] = markets.get("events").map(extract_event_id)

    markets["market_id"] = pd.to_numeric(markets.get("id"), errors="coerce")
    markets["event_id"] = pd.to_numeric(markets.get("event_id"), errors="coerce")
    markets["series_id"] = markets["event_id"].map(event_series_id)
    markets["snapshot_ts"] = to_ts(markets.get("_ingestion_ts")).fillna(to_ts(markets.get("updatedAt")))

    # Some market rows can reference events absent in the events Delta set.
    # Create minimal event stubs to preserve FK integrity.
    missing_event_ids = (
        markets["event_id"].dropna().astype("int64").drop_duplicates()
    )
    missing_event_ids = missing_event_ids[~missing_event_ids.isin(dim_event["event_id"])]
    if not missing_event_ids.empty:
        stubs = pd.DataFrame(
            {
                "event_id": missing_event_ids,
                "series_id": missing_event_ids.map(event_series_id),
                "title": None,
                "category": None,
                "start_ts": pd.NaT,
                "end_ts": pd.NaT,
                "active": None,
                "closed": None,
                "archived": None,
            }
        )
        dim_event = pd.concat([dim_event, stubs], ignore_index=True).drop_duplicates(subset=["event_id"])

    latest_per_market = (
        markets.sort_values(["market_id", "snapshot_ts"], ascending=[True, False])
        .dropna(subset=["market_id"])
        .drop_duplicates(subset=["market_id"])
    )

    dim_market = pd.DataFrame(
        {
            "market_id": latest_per_market["market_id"].astype("int64"),
            "event_id": latest_per_market["event_id"],
            "series_id": latest_per_market["series_id"],
            "question": latest_per_market.get("question"),
            "category": latest_per_market.get("category"),
            "subcategory": latest_per_market.get("subcategory"),
            "start_ts": to_ts(latest_per_market.get("startDate")),
            "end_ts": to_ts(latest_per_market.get("endDate")),
            "active": to_bool(latest_per_market.get("active")),
            "closed": to_bool(latest_per_market.get("closed")),
            "archived": to_bool(latest_per_market.get("archived")),
            "resolution_source": latest_per_market.get("resolutionSource"),
        }
    )

    return dim_series, dim_event, dim_tag, dim_market, markets


def build_facts(markets: pd.DataFrame, dim_event: pd.DataFrame, events: pd.DataFrame):
    event_to_series = dict(zip(dim_event["event_id"].astype(int), dim_event["series_id"]))
    event_tag_map = {}
    for _, row in events.iterrows():
        event_id = pd.to_numeric(row.get("id"), errors="coerce")
        if pd.isna(event_id):
            continue
        tag_payload = parse_json_cell(row.get("tags"))
        tag_ids = []
        if isinstance(tag_payload, list):
            for t in tag_payload:
                if isinstance(t, dict):
                    tid = pd.to_numeric(t.get("id"), errors="coerce")
                    if pd.notna(tid):
                        tag_ids.append(int(tid))
        event_tag_map[int(event_id)] = tag_ids

    src = markets.copy()
    src = src[src["market_id"].notna()].copy()
    src["market_id"] = src["market_id"].astype("int64")
    src["event_id"] = pd.to_numeric(src.get("event_id"), errors="coerce")
    src["series_id"] = src["event_id"].map(event_to_series)
    src["snapshot_ts"] = to_ts(src.get("snapshot_ts")).fillna(to_ts(src.get("updatedAt"))).fillna(pd.Timestamp.utcnow())
    src["end_ts"] = to_ts(src.get("endDate"))
    src["outcomes_list"] = src.get("outcomes").map(parse_json_cell)
    src["outcome_prices_list"] = src.get("outcomePrices").map(parse_json_cell)
    src["tag_ids_list"] = src["event_id"].map(lambda e: event_tag_map.get(int(e), []) if pd.notna(e) else [])
    src["yes_probability"] = src.apply(
        lambda r: probability_from_outcomes(r["outcomes_list"], r["outcome_prices_list"], "yes"), axis=1
    )
    src["no_probability"] = src.apply(
        lambda r: probability_from_outcomes(r["outcomes_list"], r["outcome_prices_list"], "no"), axis=1
    )
    src["implied_total_probability"] = src[["yes_probability", "no_probability"]].sum(axis=1, min_count=1)
    src["arbitrage_gap"] = src["implied_total_probability"].map(
        lambda v: (1.0 - float(v)) if pd.notna(v) else None
    )

    fact_market_snapshot = pd.DataFrame(
        {
            "market_id": src["market_id"],
            "event_id": src["event_id"],
            "series_id": src["series_id"],
            "snapshot_ts": src["snapshot_ts"],
            "snapshot_time_id": time_id_from_ts(src["snapshot_ts"]),
            "end_time_id": time_id_from_ts(src["end_ts"]),
            "tag_ids_json": src["tag_ids_list"].map(serialize_json),
            "outcome_labels_json": src["outcomes_list"].map(serialize_json),
            "outcome_prices_json": src["outcome_prices_list"].map(serialize_json),
            "yes_probability": src["yes_probability"],
            "no_probability": src["no_probability"],
            "implied_total_probability": src["implied_total_probability"],
            "arbitrage_gap": src["arbitrage_gap"],
            "active": to_bool(src.get("active")),
            "closed": to_bool(src.get("closed")),
            "archived": to_bool(src.get("archived")),
            "liquidity": to_num(src.get("liquidity")),
            "volume": to_num(src.get("volume")),
            "volume24hr": to_num(src.get("volume24hr")),
            "volume1wk": to_num(src.get("volume1wk")),
            "volume1mo": to_num(src.get("volume1mo")),
            "volume1yr": to_num(src.get("volume1yr")),
            "best_bid": to_num(src.get("bestBid")),
            "best_ask": to_num(src.get("bestAsk")),
            "last_trade_price": to_num(src.get("lastTradePrice")),
            "spread": to_num(src.get("spread")),
            "one_day_price_change": to_num(src.get("oneDayPriceChange")),
            "one_week_price_change": to_num(src.get("oneWeekPriceChange")),
            "one_month_price_change": to_num(src.get("oneMonthPriceChange")),
            "one_year_price_change": to_num(src.get("oneYearPriceChange")),
        }
    )
    fact_market_snapshot = fact_market_snapshot.drop_duplicates(subset=["market_id", "snapshot_ts"])

    return fact_market_snapshot


def build_dim_time(*date_series: pd.Series) -> pd.DataFrame:
    stacked = pd.concat([pd.to_datetime(s, errors="coerce", utc=True) for s in date_series], ignore_index=True)
    dates = pd.DataFrame({"date": stacked.dt.date}).dropna().drop_duplicates()
    dates["time_id"] = pd.to_datetime(dates["date"]).dt.strftime("%Y%m%d").astype(int)
    dates["year"] = pd.to_datetime(dates["date"]).dt.year
    dates["quarter"] = pd.to_datetime(dates["date"]).dt.quarter
    dates["month"] = pd.to_datetime(dates["date"]).dt.month
    dates["day"] = pd.to_datetime(dates["date"]).dt.day
    return dates[["time_id", "date", "year", "quarter", "month", "day"]].sort_values("date")


def load_to_neon(engine, dim_time, dim_series, dim_event, dim_tag, dim_market, fact_market_snapshot):
    ddl = DDL_FILE.read_text(encoding="utf-8")

    with engine.begin() as conn:
        conn.execute(text(ddl))
        conn.execute(
            text(
                """
                TRUNCATE TABLE
                    polymarket.fact_market_snapshot,
                    polymarket.dim_market,
                    polymarket.dim_event,
                    polymarket.dim_series,
                    polymarket.dim_tag,
                    polymarket.dim_time
                RESTART IDENTITY;
                """
            )
        )

    dim_time.to_sql("dim_time", engine, schema=SCHEMA, if_exists="append", index=False, chunksize=5000)
    dim_series.to_sql("dim_series", engine, schema=SCHEMA, if_exists="append", index=False, chunksize=5000)
    dim_event.to_sql("dim_event", engine, schema=SCHEMA, if_exists="append", index=False, chunksize=5000)
    dim_tag.to_sql("dim_tag", engine, schema=SCHEMA, if_exists="append", index=False, chunksize=5000)
    dim_market.to_sql("dim_market", engine, schema=SCHEMA, if_exists="append", index=False, chunksize=5000)
    fact_market_snapshot.to_sql("fact_market_snapshot", engine, schema=SCHEMA, if_exists="append", index=False, chunksize=5000)


def main():
    print(f"Loading Delta from: {DELTA_ROOT}")
    markets = read_delta("markets")
    events = read_delta("events")
    tags = read_delta("tags")

    dim_series, dim_event, dim_tag, dim_market, markets_prepared = build_dimensions(markets, events, tags)
    fact_market_snapshot = build_facts(markets_prepared, dim_event, events)

    dim_time = build_dim_time(
        dim_market["start_ts"],
        dim_market["end_ts"],
        dim_event["start_ts"],
        dim_event["end_ts"],
        fact_market_snapshot["snapshot_ts"],
    )

    print("Rows to load:")
    print(f"  dim_time: {len(dim_time)}")
    print(f"  dim_series: {len(dim_series)}")
    print(f"  dim_event: {len(dim_event)}")
    print(f"  dim_tag: {len(dim_tag)}")
    print(f"  dim_market: {len(dim_market)}")
    print(f"  fact_market_snapshot: {len(fact_market_snapshot)}")

    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    load_to_neon(
        engine,
        dim_time,
        dim_series,
        dim_event,
        dim_tag,
        dim_market,
        fact_market_snapshot,
    )
    print("Done. NeonDB schema polymarket refreshed with dimensions and fact tables.")


if __name__ == "__main__":
    main()
