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


def _read_excel_with_engines(path: str | io.BufferedIOBase) -> pd.DataFrame:
    """Read an Excel worksheet trying multiple engines when required.

    When the Streamlit UI sends an in-memory ``BytesIO`` buffer, pandas cannot
    infer the appropriate engine from a file extension.  In that scenario we
    explicitly attempt to parse the workbook first with ``openpyxl`` (for
    ``.xlsx``) and then fall back to ``xlrd`` (for legacy ``.xls`` exports).

    Parameters
    ----------
    path:
        Either a filesystem path or a file-like object.
    """

    if isinstance(path, (str, os.PathLike)):
        return pd.read_excel(path, sheet_name=0, header=None)

    # When we receive an in-memory buffer we have to pick an engine manually.
    last_exc: Exception | None = None
    for engine in ("openpyxl", "xlrd"):
        try:
            if hasattr(path, "seek"):
                path.seek(0)
            return pd.read_excel(path, sheet_name=0, header=None, engine=engine)
        except ImportError as exc:
            last_exc = exc
        except ValueError as exc:
            last_exc = exc

    raise IngestionError(f"Excel dosyası okunamadı: {last_exc}")


def parse_cpi_excel(path: str | io.BufferedIOBase) -> pd.DataFrame:
    """Normalise CPI salesman Excel exports into a consistent dataframe.

    The CPI reports arrive with the first row acting as the header and
    inconsistent spacing in column names. This helper promotes the first row to
    headers, cleans up the textual fields and extracts the trailing MTD metric
    columns as numeric values.

    Parameters
    ----------
    path:
        File-system path or file-like object pointing to the CPI Excel export.

    Returns
    -------
    pandas.DataFrame
        Dataframe containing the canonical columns required by the UI layer.
    """

    # Read the worksheet without assuming headers so we can promote the first
    # row to be the canonical header row.
    df_raw = _read_excel_with_engines(path)
    if df_raw.empty:
        return pd.DataFrame(columns=["company", "customer", "sales_engineer", "OR_MTD", "OI_MTD"])

    header = df_raw.iloc[0].tolist()
    df = df_raw.iloc[1:].copy()
    df.columns = [str(h).strip() for h in header]

    def _find_col(prefix: str) -> Optional[str]:
        for column in df.columns:
            if str(column).strip().lower().startswith(prefix.lower()):
                return column
        return None

    col_company = _find_col("Operational Company")
    col_customer = _find_col("Customer")
    col_sales = _find_col("Sales Representative")

    # Extract the dimensional columns if available; fall back to None when the
    # CPI export omits the information (rare but observed in the field).
    out = pd.DataFrame(
        {
            "company": df[col_company] if col_company else None,
            "customer": df[col_customer] if col_customer else None,
            "sales_engineer": df[col_sales] if col_sales else None,
        }
    )

    if df.shape[1] < 2:
        # Not enough columns to pick the MTD metrics; return empty frame.
        return pd.DataFrame(columns=["company", "customer", "sales_engineer", "OR_MTD", "OI_MTD"])

    # CPI exports use the last two columns as the month-to-date metrics.
    or_col, oi_col = df.columns[-2], df.columns[-1]
    out["OR_MTD"] = pd.to_numeric(df[or_col], errors="coerce")
    out["OI_MTD"] = pd.to_numeric(df[oi_col], errors="coerce")

    # Drop rows where both metrics are completely missing. Remaining NaNs are
    # treated as zero for downstream aggregation safety.
    out = out.dropna(subset=["OR_MTD", "OI_MTD"], how="all").fillna({"OR_MTD": 0, "OI_MTD": 0})

    # Normalise textual dimensions by stripping whitespace and ensuring string
    # dtype. A future hook for ID->name replacements can be slotted here.
    for column in ["company", "customer", "sales_engineer"]:
        if column in out.columns:
            out[column] = out[column].astype(str).str.strip()

    # Example hook for future ID to name mapping, e.g. Sales Representative IDs.
    # mapping: dict[str, str] = {}
    # out["sales_engineer"] = out["sales_engineer"].replace(mapping)

    return out[["company", "customer", "sales_engineer", "OR_MTD", "OI_MTD"]]

