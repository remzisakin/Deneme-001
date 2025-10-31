"""FastAPI application entrypoint."""
from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .routers import analyze, ingest, nlsql

LOG_DIR = Path("logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)

handler = RotatingFileHandler(LOG_DIR / "app.log", maxBytes=5_000_000, backupCount=3)
logging.basicConfig(level=logging.INFO, handlers=[handler])
logger = logging.getLogger(__name__)

app = FastAPI(title="Sales Reporting & LLM Analysis API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"]
    ,
    allow_headers=["*"],
)

app.include_router(ingest.router)
app.include_router(analyze.router)
app.include_router(nlsql.router)


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}

