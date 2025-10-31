"""ETL helpers for normalising uploaded sales reports."""
from __future__ import annotations

import csv
import io
import json
import os
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Tuple

import duckdb
import pandas as pd
from pandas import DataFrame
from pypdf import PdfReader

from ..db.duck import get_connection

REQUIRED_COLUMNS = {
    "date",
    "order_id",
    "product",
    "category",
    "region",
    "sales_amount",
    "quantity",
    "unit_price",
}

OPTIONAL_COLUMNS = {"customer", "salesperson", "currency"}
DEFAULT_CURRENCY = os.getenv("DEFAULT_CURRENCY", "EUR")


class IngestionError(RuntimeError):
    """Raised when a file cannot be ingested."""


def generate_ingestion_id() -> str:
    return uuid.uuid4().hex


def _normalise_dataframe(df: DataFrame, source_file: str, ingestion_id: str) -> DataFrame:
    df = df.rename(columns={col: col.lower().strip() for col in df.columns})

    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise IngestionError(f"Eksik zorunlu kolonlar: {', '.join(sorted(missing))}")

    for col in OPTIONAL_COLUMNS:
        if col not in df.columns:
            df[col] = None

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    if df["date"].isna().any():
        raise IngestionError("Tarih kolonunda hatalı değerler mevcut.")

    for col in ["quantity", "unit_price", "sales_amount"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
        if df[col].isna().any():
            raise IngestionError(f"{col} kolonunda sayısal olmayan değerler var.")

    needs_sales_amount = df["sales_amount"].isna() | (df["sales_amount"] == 0)
    if needs_sales_amount.any():
        df.loc[needs_sales_amount, "sales_amount"] = (
            df.loc[needs_sales_amount, "quantity"] * df.loc[needs_sales_amount, "unit_price"]
        )

    if "currency" not in df.columns:
        df["currency"] = DEFAULT_CURRENCY
    df["currency"] = df["currency"].fillna(DEFAULT_CURRENCY)

    if "customer" not in df.columns:
        df["customer"] = None
    if "salesperson" not in df.columns:
        df["salesperson"] = None

    df["source_file"] = source_file
    df["ingestion_id"] = ingestion_id
    ordered_cols = [
        "date",
        "order_id",
        "product",
        "category",
        "region",
        "customer",
        "salesperson",
        "quantity",
        "unit_price",
        "sales_amount",
        "currency",
        "source_file",
        "ingestion_id",
    ]
    return df[ordered_cols]


def _iter_pdf_rows(path: Path) -> Iterator[Dict[str, str]]:
    reader = PdfReader(str(path))
    for page in reader.pages:
        text = page.extract_text() or ""
        for line in text.splitlines():
            parts = [part.strip() for part in csv.reader([line]).__next__()]
            if len(parts) < 6:
                parts = [p for p in line.split(" ") if p]
            if len(parts) < 6:
                continue
            row = {
                "date": parts[0],
                "order_id": parts[1],
                "product": parts[2],
                "category": parts[3],
                "region": parts[4],
                "quantity": parts[5],
                "unit_price": parts[6] if len(parts) > 6 else "0",
                "sales_amount": parts[7] if len(parts) > 7 else "0",
            }
            yield row


def _pdf_to_dataframe(path: Path) -> DataFrame:
    rows = list(_iter_pdf_rows(path))
    if not rows:
        raise IngestionError("PDF dosyasından veri çıkarılamadı.")
    df = pd.DataFrame(rows)
    return df


def _read_chunks(path: Path, filetype: str, chunk_size: int = 5000) -> Iterable[DataFrame]:
    if filetype == "csv":
        for chunk in pd.read_csv(path, chunksize=chunk_size):
            yield chunk
    elif filetype == "xlsx":
        df = pd.read_excel(path)
        yield df
    elif filetype == "pdf":
        yield _pdf_to_dataframe(path)
    else:
        raise IngestionError(f"Desteklenmeyen dosya tipi: {filetype}")


def ingest_file(path: Path, ingestion_id: Optional[str] = None) -> Tuple[str, int]:
    ingestion_id = ingestion_id or generate_ingestion_id()
    source_file = path.name
    filetype = path.suffix.lower().lstrip(".")

    conn = get_connection()
    total_rows = 0

    for chunk in _read_chunks(path, filetype):
        normalised = _normalise_dataframe(chunk, source_file, ingestion_id)
        conn.register("df", normalised)
        conn.execute("INSERT INTO fact_sales SELECT * FROM df")
        total_rows += len(normalised)

    return ingestion_id, total_rows


def extract_pdf_context(path: Path, limit: int = 5) -> List[str]:
    """Extract key paragraphs from a PDF for RAG style prompts."""
    reader = PdfReader(str(path))
    paragraphs: List[str] = []
    for page in reader.pages:
        text = (page.extract_text() or "").strip()
        for block in text.split("\n\n"):
            clean = " ".join(block.split())
            if clean:
                paragraphs.append(clean)
        if len(paragraphs) >= limit:
            break
    return paragraphs[:limit]


def list_recent_sources(limit: int = 20) -> List[Dict[str, str]]:
    conn = get_connection(readonly=True)
    query = """
        SELECT DISTINCT source_file, ingestion_id,
               MIN(date) AS min_date, MAX(date) AS max_date,
               COUNT(*) AS row_count
        FROM fact_sales
        GROUP BY source_file, ingestion_id
        ORDER BY MAX(date) DESC
        LIMIT ?
    """
    rows = conn.execute(query, [limit]).fetchall()
    return [
        {
            "source_file": row[0],
            "ingestion_id": row[1],
            "min_date": row[2].isoformat() if row[2] else None,
            "max_date": row[3].isoformat() if row[3] else None,
            "row_count": row[4],
        }
        for row in rows
    ]

