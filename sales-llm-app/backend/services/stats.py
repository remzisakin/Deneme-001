"""Aggregate statistics and analytics on top of DuckDB."""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from ..db.duck import get_connection
from ..models.schemas import AnalysisFilters, KPIResponse, TrendSeries


def _filters_to_sql(filters: AnalysisFilters) -> str:
    clauses = ["1=1"]
    if filters.start_date:
        clauses.append("date >= DATE ?")
    if filters.end_date:
        clauses.append("date <= DATE ?")
    if filters.region:
        clauses.append("region = ?")
    if filters.category:
        clauses.append("category = ?")
    return " AND ".join(clauses)


def _filters_values(filters: AnalysisFilters) -> List[Any]:
    values: List[Any] = []
    if filters.start_date:
        values.append(filters.start_date)
    if filters.end_date:
        values.append(filters.end_date)
    if filters.region:
        values.append(filters.region)
    if filters.category:
        values.append(filters.category)
    return values


def compute_kpis(filters: AnalysisFilters) -> KPIResponse:
    conn = get_connection(readonly=True)
    where = _filters_to_sql(filters)

    totals = conn.execute(
        f"""
        SELECT
            COALESCE(SUM(sales_amount), 0) AS total_sales,
            COALESCE(SUM(quantity), 0) AS total_quantity
        FROM fact_sales
        WHERE {where}
        """,
        _filters_values(filters),
    ).fetchone()

    if not totals:
        return KPIResponse()

    total_sales, total_quantity = totals
    avg_basket = float(total_sales) / float(total_quantity) if total_quantity else 0.0

    top_product_row = conn.execute(
        f"""
        SELECT product, SUM(sales_amount) AS s
        FROM fact_sales
        WHERE {where}
        GROUP BY product
        ORDER BY s DESC
        LIMIT 1
        """,
        _filters_values(filters),
    ).fetchone()

    top_region_row = conn.execute(
        f"""
        SELECT region, SUM(sales_amount) AS s
        FROM fact_sales
        WHERE {where}
        GROUP BY region
        ORDER BY s DESC
        LIMIT 1
        """,
        _filters_values(filters),
    ).fetchone()

    return KPIResponse(
        total_sales=float(total_sales or 0),
        total_quantity=float(total_quantity or 0),
        average_basket=avg_basket,
        top_product=top_product_row[0] if top_product_row else None,
        top_region=top_region_row[0] if top_region_row else None,
    )


def compute_time_series(filters: AnalysisFilters, granularity: str = "day") -> TrendSeries:
    conn = get_connection(readonly=True)
    where = _filters_to_sql(filters)
    values = _filters_values(filters)

    if granularity not in {"day", "week", "month"}:
        granularity = "day"

    bucket = {
        "day": "DATE_TRUNC('day', date)",
        "week": "DATE_TRUNC('week', date)",
        "month": "DATE_TRUNC('month', date)",
    }[granularity]

    query = f"""
        SELECT
            bucket,
            total_sales,
            total_quantity,
            AVG(total_sales) OVER (
                ORDER BY bucket
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) AS moving_average
        FROM (
            SELECT
                {bucket} AS bucket,
                SUM(sales_amount) AS total_sales,
                SUM(quantity) AS total_quantity
            FROM fact_sales
            WHERE {where}
            GROUP BY 1
        )
        ORDER BY bucket
    """
    df = conn.execute(query, values).df()
    records = df.to_dict(orient="records") if not df.empty else []
    return TrendSeries(granularity=granularity, series=records)


def compute_segment_breakdown(dimension: str, filters: AnalysisFilters) -> List[Dict[str, Any]]:
    if dimension not in {"product", "category", "region", "customer", "salesperson"}:
        raise ValueError("Unsupported breakdown dimension")
    conn = get_connection(readonly=True)
    where = _filters_to_sql(filters)
    values = _filters_values(filters)
    query = f"""
        SELECT {dimension} AS key, SUM(sales_amount) AS total_sales, SUM(quantity) AS total_quantity
        FROM fact_sales
        WHERE {where}
        GROUP BY {dimension}
        ORDER BY total_sales DESC
        LIMIT 20
    """
    df = conn.execute(query, values).df()
    return df.to_dict(orient="records") if not df.empty else []


def compute_period_delta(filters: AnalysisFilters, days: int) -> Optional[float]:
    if days <= 0:
        return None
    conn = get_connection(readonly=True)
    end_date = filters.end_date or datetime.utcnow().date()
    start_period = end_date - timedelta(days=days)

    query = """
        WITH current AS (
            SELECT SUM(sales_amount) AS value
            FROM fact_sales
            WHERE date BETWEEN DATE ? AND DATE ?
        ),
        previous AS (
            SELECT SUM(sales_amount) AS value
            FROM fact_sales
            WHERE date BETWEEN DATE ? AND DATE ?
        )
        SELECT current.value, previous.value
    """
    current_row = conn.execute(
        query,
        [start_period, end_date, start_period - timedelta(days=days), start_period],
    ).fetchone()
    if not current_row:
        return None
    current_value, previous_value = current_row
    if not previous_value:
        return None
    return (float(current_value or 0) - float(previous_value or 0)) / float(previous_value)


def make_summaries(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Produce CPI sales summaries for the Streamlit dashboard."""

    if df.empty:
        # Return empty frames with the expected schema so the UI can render
        # consistent tables even without data.
        empty_cols = ["sales_engineer", "OR_MTD", "OI_MTD"]
        by_engineer = pd.DataFrame(columns=empty_cols)
        by_customer = pd.DataFrame(columns=["customer", "OR_MTD", "OI_MTD"])
        totals = pd.DataFrame({"Metric": [], "Tutar": []})
        return by_engineer, by_customer, totals

    by_engineer = (
        df.groupby("sales_engineer", dropna=False)[["OR_MTD", "OI_MTD"]]
        .sum()
        .sort_values("OR_MTD", ascending=False)
        .reset_index()
    )

    by_customer = (
        df.groupby("customer", dropna=False)[["OR_MTD", "OI_MTD"]]
        .sum()
        .sort_values("OR_MTD", ascending=False)
        .reset_index()
    )

    totals = pd.DataFrame(
        {
            "Metric": ["Toplam OR_MTD", "Toplam OI_MTD"],
            "Tutar": [df["OR_MTD"].sum(), df["OI_MTD"].sum()],
        }
    )

    return by_engineer, by_customer, totals

