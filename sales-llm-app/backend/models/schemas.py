"""Pydantic models shared between FastAPI routers and services."""
from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator


class IngestionResponse(BaseModel):
    ingestion_id: str
    rows_ingested: int
    source_file: str


class AnalysisFilters(BaseModel):
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    region: Optional[str] = None
    category: Optional[str] = None

    @field_validator("region", "category", mode="before")
    def empty_string_to_none(cls, value: Optional[str]) -> Optional[str]:
        if value in {None, ""}:
            return None
        return value


class KPIResponse(BaseModel):
    total_sales: float = 0.0
    total_quantity: float = 0.0
    average_basket: float = 0.0
    top_product: Optional[str] = None
    top_region: Optional[str] = None


class TrendSeries(BaseModel):
    granularity: str
    series: List[Dict[str, Any]]


class AnomalyPoint(BaseModel):
    product: str
    region: str
    date: datetime
    sales_amount: float
    score: float


class LLMInsight(BaseModel):
    summary: str
    highlights: List[str]
    risks: List[str]
    actions: List[str]


class AnalysisResponse(BaseModel):
    kpis: KPIResponse
    trends: TrendSeries
    anomalies: List[AnomalyPoint]
    insight: LLMInsight


class NLQueryRequest(BaseModel):
    question: str = Field(..., min_length=3)
    limit: int = Field(10, ge=1, le=500)


class NLQueryResult(BaseModel):
    sql: str
    rows: List[Dict[str, Any]]


class PromptContext(BaseModel):
    stats_json: Dict[str, Any]
    anomalies_json: List[Dict[str, Any]]
    pdf_context: List[str] = Field(default_factory=list)

