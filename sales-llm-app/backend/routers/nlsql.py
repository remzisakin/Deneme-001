"""Endpoints for natural language SQL queries."""
from __future__ import annotations

from fastapi import APIRouter, HTTPException

from ..models.schemas import NLQueryRequest, NLQueryResult
from ..services import nlsql

router = APIRouter(prefix="/nlsql", tags=["nlsql"])


@router.post("/query", response_model=NLQueryResult)
async def run_query(request: NLQueryRequest) -> NLQueryResult:
    try:
        sql = nlsql.generate_sql(request.question)
        result = nlsql.execute_sql(sql, limit=request.limit)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return NLQueryResult(**result)

