from fastapi import FastAPI, Query, HTTPException
from sqlalchemy import text
from db import engine

app = FastAPI(
    title="Polymarket NBA API (NeonDB)",
    version="1.0.0",
    description="API analítica sobre mercados NBA (subset) cargados en NeonDB."
)

# -------------------------
# Healthcheck
# -------------------------
@app.get("/health")
def health():
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    return {"status": "ok"}

# -------------------------
# 1) Top markets por liquidez
# -------------------------
@app.get("/markets/top-liquidity")
def top_liquidity(limit: int = Query(10, ge=1, le=100)):
    q = text("""
        SELECT market_id, question, category, liquidity, volume, active, closed, event_id, end_ts
        FROM polymarket.dim_market
        ORDER BY liquidity DESC NULLS LAST
        LIMIT :limit
    """)
    with engine.connect() as conn:
        rows = conn.execute(q, {"limit": limit}).mappings().all()
    return {"count": len(rows), "data": rows}

# -------------------------
# 2) Top markets por volumen
# (si volume está a NULL en muchos, igual sirve)
# -------------------------
@app.get("/markets/top-volume")
def top_volume(limit: int = Query(10, ge=1, le=100)):
    q = text("""
        SELECT market_id, question, category, liquidity, volume, active, closed, event_id, end_ts
        FROM polymarket.dim_market
        ORDER BY volume DESC NULLS LAST
        LIMIT :limit
    """)
    with engine.connect() as conn:
        rows = conn.execute(q, {"limit": limit}).mappings().all()
    return {"count": len(rows), "data": rows}

# -------------------------
# 3) Buscar markets por texto
# -------------------------
@app.get("/markets/search")
def search_markets(
    query: str = Query(..., min_length=2),
    limit: int = Query(50, ge=1, le=200)
):
    q = text("""
        SELECT market_id, question, category, liquidity, volume, active, closed, event_id, end_ts
        FROM polymarket.dim_market
        WHERE question ILIKE :pattern
        ORDER BY liquidity DESC NULLS LAST
        LIMIT :limit
    """)
    with engine.connect() as conn:
        rows = conn.execute(q, {"pattern": f"%{query}%", "limit": limit}).mappings().all()
    return {"count": len(rows), "data": rows}

# -------------------------
# 4) Events closing soon (24-48h)
# -------------------------
@app.get("/events/closing-soon")
def closing_soon(hours: int = Query(48, ge=1, le=168)):
    q = text("""
        SELECT event_id, title, category, end_ts
        FROM polymarket.dim_event
        WHERE end_ts IS NOT NULL
          AND end_ts >= (NOW() AT TIME ZONE 'UTC')
          AND end_ts <= (NOW() AT TIME ZONE 'UTC') + (:hours || ' hours')::interval
        ORDER BY end_ts ASC
        LIMIT 200
    """)
    with engine.connect() as conn:
        rows = conn.execute(q, {"hours": hours}).mappings().all()
    return {"count": len(rows), "data": rows}

# -------------------------
# 5) Markets de un evento
# -------------------------
@app.get("/events/{event_id}/markets")
def markets_by_event(
    event_id: int,
    limit: int = Query(200, ge=1, le=500)
):
    with engine.connect() as conn:
        ev = conn.execute(
            text("""
                SELECT event_id, title, category, end_ts
                FROM polymarket.dim_event
                WHERE event_id = :id
            """),
            {"id": event_id}
        ).mappings().first()

        if not ev:
            raise HTTPException(status_code=404, detail="event_id no encontrado")

        mk = conn.execute(
            text("""
                SELECT market_id, question, liquidity, volume, active, closed, end_ts
                FROM polymarket.dim_market
                WHERE event_id = :id
                ORDER BY liquidity DESC NULLS LAST
                LIMIT :limit
            """),
            {"id": event_id, "limit": limit}
        ).mappings().all()

    return {"event": ev, "markets_count": len(mk), "data": mk}

# -------------------------
# 6) Tags search (búsqueda por nombre)
# -------------------------
@app.get("/tags/search")
def search_tags(name: str = Query(..., min_length=2), limit: int = Query(50, ge=1, le=200)):
    q = text("""
        SELECT tag_id, name, slug, parent_tag_id
        FROM polymarket.dim_tag
        WHERE name ILIKE :pattern
        ORDER BY name ASC
        LIMIT :limit
    """)
    with engine.connect() as conn:
        rows = conn.execute(q, {"pattern": f"%{name}%", "limit": limit}).mappings().all()
    return {"count": len(rows), "data": rows}

# -------------------------
# 7) Resumen KPI rápido (para dashboards)
# -------------------------
@app.get("/kpi/summary")
def kpi_summary():
    q = text("""
        SELECT
          (SELECT COUNT(*) FROM polymarket.dim_market) AS markets,
          (SELECT COUNT(*) FROM polymarket.dim_event)  AS events,
          (SELECT COUNT(*) FROM polymarket.dim_tag)    AS tags,
          (SELECT COUNT(*) FROM polymarket.dim_time)   AS days,
          (SELECT COUNT(*) FROM polymarket.dim_market WHERE closed = true) AS closed_markets,
          (SELECT COUNT(*) FROM polymarket.dim_market WHERE closed = false) AS open_markets,
          (SELECT MAX(liquidity) FROM polymarket.dim_market) AS max_liquidity
    """)
    with engine.connect() as conn:
        row = conn.execute(q).mappings().first()
    return row