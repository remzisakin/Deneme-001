from __future__ import annotations

from datetime import datetime

import pandas as pd

from backend.db.duck import get_connection, reset_database
from backend.services.anomalies import detect_anomalies
from backend.services.etl import ingest_file
from backend.models.schemas import AnalysisFilters


def setup_module(module):
    reset_database()


def teardown_module(module):
    reset_database()


def _seed_data():
    conn = get_connection()
    data = pd.DataFrame(
        {
            "date": pd.date_range("2024-01-01", periods=10, freq="D"),
            "order_id": [f"ORD-{i}" for i in range(10)],
            "product": ["Widget"] * 10,
            "category": ["Tools"] * 10,
            "region": ["EMEA"] * 10,
            "customer": ["Acme"] * 10,
            "salesperson": ["Alice"] * 10,
            "quantity": [1] * 10,
            "unit_price": [10.0] * 10,
            "sales_amount": [10.0] * 9 + [200.0],
            "currency": ["EUR"] * 10,
            "source_file": ["seed.csv"] * 10,
            "ingestion_id": ["seed"] * 10,
        }
    )
    conn.register("seed", data)
    conn.execute("INSERT INTO fact_sales SELECT * FROM seed")


def test_detect_anomalies():
    _seed_data()
    filters = AnalysisFilters()
    anomalies = detect_anomalies(filters)
    assert anomalies, "At least one anomaly should be detected"
    assert any(point.sales_amount > 100 for point in anomalies)

