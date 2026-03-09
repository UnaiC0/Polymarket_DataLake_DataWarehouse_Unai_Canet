# Polymarket NBA API (NeonDB)

API en FastAPI para consultar el Data Warehouse (schema `polymarket`) con datos NBA de Polymarket.

## Estructura del proyecto

```text
api/
|- api.py                    # Endpoints FastAPI
|- db.py                     # Conexion SQLAlchemy
|- load_gold_to_neon.py      # ETL: carga Delta -> NeonDB (modelo GOLD)
|- requirements.txt          # Dependencias API
|- requirements_etl.txt      # Dependencias ETL
|- env.example               # Plantilla de variables de entorno
`- sql/
   `- polymarket_gold_ddl.sql
```

## Requisitos

- Python 3.10+
- Acceso a NeonDB (PostgreSQL)
- Variable `DATABASE_URL` en `api/.env`

## Configuracion

1. Crear entorno virtual.
2. Instalar dependencias.

```bash
pip install -r requirements.txt
```

3. Copiar `env.example` a `.env` y completar la URL real:

```bash
DATABASE_URL=postgresql+psycopg2://USER:PASSWORD@HOST/neondb?sslmode=require
```

## Ejecutar API

```bash
uvicorn api:app --reload --host 0.0.0.0 --port 8000
```

Endpoints disponibles:

- `GET /health`
- `GET /markets/top-liquidity?limit=10`
- `GET /markets/top-volume?limit=10`
- `GET /markets/search?query=lakers&limit=50`
- `GET /events/closing-soon?hours=48`
- `GET /events/{event_id}/markets?limit=200`
- `GET /tags/search?name=nba&limit=50`
- `GET /kpi/summary`

Documentacion interactiva:

- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`

## ETL GOLD a NeonDB

Instalar dependencias ETL:

```bash
pip install -r requirements_etl.txt
```

Ejecutar carga:

```bash
python load_gold_to_neon.py
```

El script:

- Lee Delta tables desde `DELTA_ROOT` (por defecto `../polymarket/nba`).
- Recrea esquema/tablas con `sql/polymarket_gold_ddl.sql`.
- Inserta `dim_time`, `dim_series`, `dim_event`, `dim_tag`, `dim_market`, `fact_market_snapshot`.

## Variables de entorno

- `DATABASE_URL` (obligatoria): cadena de conexion SQLAlchemy a NeonDB.
- `DELTA_ROOT` (opcional): ruta base de las tablas Delta para la carga ETL.

## Nota de seguridad

No subir el archivo `.env` al repositorio. El `.gitignore` ya lo excluye.
