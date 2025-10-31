from __future__ import annotations

import io
from pathlib import Path

import pandas as pd
import pytest

from backend.db.duck import get_connection, reset_database
from backend.services import etl


@pytest.fixture(autouse=True)
def _reset_db(tmp_path: Path):
    reset_database()
    yield
    reset_database()


def _write_csv(tmp_path: Path) -> Path:
    df = pd.DataFrame(
        {
            "date": ["2024-01-01", "2024-01-02"],
            "order_id": ["A", "B"],
            "product": ["Widget", "Gadget"],
            "category": ["Tools", "Tools"],
            "region": ["EMEA", "EMEA"],
            "quantity": [2, 3],
            "unit_price": [10.0, 20.0],
            "sales_amount": [20.0, 60.0],
        }
    )
    path = tmp_path / "sample.csv"
    df.to_csv(path, index=False)
    return path


def _write_xlsx(tmp_path: Path) -> Path:
    df = pd.DataFrame(
        {
            "date": ["2024-02-01"],
            "order_id": ["C"],
            "product": ["Widget"],
            "category": ["Tools"],
            "region": ["APAC"],
            "quantity": [5],
            "unit_price": [15.0],
            "sales_amount": [75.0],
        }
    )
    path = tmp_path / "sample.xlsx"
    df.to_excel(path, index=False)
    return path


def test_ingest_csv_and_xlsx(tmp_path: Path):
    csv_path = _write_csv(tmp_path)
    xlsx_path = _write_xlsx(tmp_path)

    etl.ingest_file(csv_path, ingestion_id="csv")
    etl.ingest_file(xlsx_path, ingestion_id="xlsx")

    conn = get_connection(readonly=True)
    count = conn.execute("SELECT COUNT(*) FROM fact_sales").fetchone()[0]
    assert count == 3


def test_ingest_pdf_with_monkeypatch(tmp_path: Path, monkeypatch):
    fake_pdf_path = tmp_path / "sample.pdf"
    fake_pdf_path.write_bytes(b"%PDF-1.4\n")

    class FakePage:
        def extract_text(self):
            return "2024-03-01,INV-1,Widget,Tools,EMEA,1,10,10"

    class FakeReader:
        pages = [FakePage()]

        def __init__(self, path):
            pass

    monkeypatch.setattr(etl, "PdfReader", FakeReader)

    etl.ingest_file(fake_pdf_path, ingestion_id="pdf")
    conn = get_connection(readonly=True)
    count = conn.execute("SELECT COUNT(*) FROM fact_sales").fetchone()[0]
    assert count == 1


def test_extract_pdf_context(monkeypatch, tmp_path: Path):
    fake_pdf_path = tmp_path / "context.pdf"
    fake_pdf_path.write_bytes(b"%PDF-1.4\n")

    class FakePage:
        def __init__(self, text):
            self._text = text

        def extract_text(self):
            return self._text

    class FakeReader:
        def __init__(self, path):
            self.pages = [
                FakePage("Paragraf 1\n\nParagraf 2"),
                FakePage("Paragraf 3"),
            ]

    monkeypatch.setattr(etl, "PdfReader", FakeReader)
    paragraphs = etl.extract_pdf_context(fake_pdf_path, limit=2)
    assert len(paragraphs) == 2
    assert paragraphs[0].startswith("Paragraf 1")

