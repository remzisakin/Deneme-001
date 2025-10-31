"""Simple anomaly detection utilities."""
from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd

from ..db.duck import get_connection
from ..models.schemas import AnalysisFilters, AnomalyPoint
from .stats import _filters_to_sql, _filters_values


def detect_anomalies(filters: AnalysisFilters, z_threshold: float = 2.5) -> List[AnomalyPoint]:
    conn = get_connection(readonly=True)
    where = _filters_to_sql(filters)
    values = _filters_values(filters)
    query = f"""
        SELECT product, region, date, sales_amount
        FROM fact_sales
        WHERE {where}
    """
    df = conn.execute(query, values).df()
    if df.empty:
        return []

    grouped = df.groupby(["product", "region"])
    records: List[AnomalyPoint] = []
    for (product, region), group in grouped:
        mean = group["sales_amount"].mean()
        std = group["sales_amount"].std(ddof=0) or 0
        if std == 0:
            continue
        z_scores = (group["sales_amount"] - mean) / std
        outliers = group[z_scores.abs() >= z_threshold]
        for _, row in outliers.iterrows():
            records.append(
                AnomalyPoint(
                    product=str(product),
                    region=str(region),
                    date=pd.to_datetime(row["date"]).to_pydatetime(),
                    sales_amount=float(row["sales_amount"]),
                    score=float(((row["sales_amount"] - mean) / std)),
                )
            )
    return records


def anomalies_as_json(anomalies: List[AnomalyPoint]) -> List[Dict[str, Any]]:
    return [
        {
            "product": anomaly.product,
            "region": anomaly.region,
            "date": anomaly.date.isoformat(),
            "sales_amount": anomaly.sales_amount,
            "score": anomaly.score,
        }
        for anomaly in anomalies
    ]

