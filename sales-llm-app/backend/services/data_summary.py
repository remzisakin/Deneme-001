"""Data profiling helpers used when generating textual dataset summaries."""
from __future__ import annotations

from typing import Dict, Iterable, Tuple

import pandas as pd


def _describe_with_datetime(df: pd.DataFrame) -> pd.DataFrame:
    """Return ``DataFrame.describe`` with graceful ``datetime`` support.

    Newer pandas versions expose the ``datetime_is_numeric`` keyword that treats
    ``datetime64`` columns as numeric values during aggregation.  Older releases
    raise ``TypeError`` when the argument is provided.  The production
    environment for the app still runs on an older pandas build which caused the
    Streamlit "Veri Özeti" section to crash.  The helper tries the new keyword
    first and transparently falls back to the classic behaviour when the
    signature is incompatible.
    """

    try:
        return df.describe(include="all", datetime_is_numeric=True)
    except TypeError:
        # ``datetime_is_numeric`` landed in pandas 1.1.0.  On older installs the
        # keyword is rejected, therefore we retry without it to preserve
        # backwards compatibility.
        return df.describe(include="all")


def profile_data(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Compute missing value ratios and descriptive statistics for ``df``."""

    if df.empty:
        missing = pd.DataFrame(
            {
                "column": list(df.columns),
                "missing_count": [0] * len(df.columns),
                "missing_ratio": [0.0] * len(df.columns),
            }
        )
        stats = pd.DataFrame()
        return missing, stats

    missing = df.isna().sum().reset_index()
    missing.columns = ["column", "missing_count"]
    missing["missing_ratio"] = missing["missing_count"] / len(df)

    stats_raw = _describe_with_datetime(df)
    if stats_raw.empty:
        stats = pd.DataFrame()
    else:
        stats = stats_raw.transpose().reset_index().rename(columns={"index": "column"})

    return missing, stats


def summarize_dataframe(df: pd.DataFrame, schema: Dict[str, Dict[str, str]] | None = None) -> str:
    """Generate a human-readable summary for ``df`` respecting ``schema``.

    Parameters
    ----------
    df:
        The dataframe to describe.
    schema:
        Optional mapping describing the dataframe columns.  Each key is the
        column name and the value is a dictionary with free-form metadata such as
        ``{"description": "Satış tutarı"}``.
    """

    if df.empty:
        return "Veri kümesi boş; analiz edilecek satır bulunamadı."

    missing, stats = profile_data(df)

    lines: list[str] = []
    lines.append(f"Toplam satır sayısı: {len(df):,}")

    if schema:
        described_columns = ", ".join(sorted(schema))
        lines.append(f"Şema kolonları: {described_columns}")

    numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
    if numeric_cols:
        summary_parts = []
        for col in numeric_cols[:5]:  # keep the summary compact
            summary_parts.append(f"{col}: ort={df[col].mean():.2f}, std={df[col].std(ddof=0):.2f}")
        lines.append("Öne çıkan sayısal kolonlar: " + "; ".join(summary_parts))

    datetime_cols = df.select_dtypes(include=["datetime", "datetimetz"]).columns.tolist()
    if datetime_cols:
        min_date = df[datetime_cols[0]].min()
        max_date = df[datetime_cols[0]].max()
        lines.append(f"Tarih aralığı ({datetime_cols[0]}): {min_date.date()} → {max_date.date()}")

    missing_hot = missing[missing["missing_count"] > 0]
    if not missing_hot.empty:
        parts = [f"{row.column}: %{row.missing_ratio * 100:.1f}" for row in missing_hot.itertuples()]
        lines.append("Eksik veri oranları: " + ", ".join(parts))
    else:
        lines.append("Eksik veri bulunmuyor.")

    if schema:
        descriptions = [
            f"{col}: {meta.get('description', 'Açıklama yok')}"
            for col, meta in schema.items()
            if meta
        ]
        if descriptions:
            lines.append("Kolon açıklamaları:\n" + "\n".join(f"- {item}" for item in descriptions))

    return "\n".join(lines)

