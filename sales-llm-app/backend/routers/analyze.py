"""Endpoints for KPI, trend and LLM analysis."""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException

from ..models.schemas import AnalysisFilters, AnalysisResponse, PromptContext
from ..services import anomalies, etl, prompts, stats

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/analyze", tags=["analyze"])


@router.post("/run", response_model=AnalysisResponse)
async def run_analysis(filters: AnalysisFilters) -> AnalysisResponse:
    kpis = stats.compute_kpis(filters)
    trend = stats.compute_time_series(filters)
    anomaly_points = anomalies.detect_anomalies(filters)

    stats_json = {
        "kpis": kpis.dict(),
        "trend": trend.dict(),
        "breakdowns": {
            "product": stats.compute_segment_breakdown("product", filters),
            "region": stats.compute_segment_breakdown("region", filters),
        },
    }
    anomalies_json = anomalies.anomalies_as_json(anomaly_points)

    pdf_context: list[str] = []
    recent = etl.list_recent_sources(limit=3)
    for item in recent:
        path = Path("data/uploads") / item["source_file"]
        if path.suffix.lower() == ".pdf" and path.exists():
            pdf_context.extend(etl.extract_pdf_context(path))

    context = PromptContext(stats_json=stats_json, anomalies_json=anomalies_json, pdf_context=pdf_context[:5])
    try:
        insight = prompts.run_analysis(context)
    except Exception as exc:  # pragma: no cover - network errors
        logger.exception("LLM analizi başarısız", exc_info=exc)
        raise HTTPException(status_code=500, detail="LLM analizi başarısız oldu") from exc

    return AnalysisResponse(
        kpis=kpis,
        trends=trend,
        anomalies=anomaly_points,
        insight=insight,
    )

