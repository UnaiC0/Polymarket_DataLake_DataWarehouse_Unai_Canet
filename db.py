import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine

# Carga .env ubicado en la misma carpeta que este archivo (api/.env)
BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("Falta DATABASE_URL. Crea api/.env basado en env.example")

# pool_pre_ping evita conexiones muertas
engine = create_engine(DATABASE_URL, pool_pre_ping=True)
