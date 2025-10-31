"""FastAPI router for ingestion endpoints."""
from __future__ import annotations

import logging
from pathlib import Path
from typing import List

from fastapi import APIRouter, BackgroundTasks, File, HTTPException, UploadFile

from ..models.schemas import IngestionResponse
from ..services import etl

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/ingest", tags=["ingest"])
UPLOAD_DIR = Path("data/uploads")
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)


@router.post("/upload", response_model=IngestionResponse)
async def upload_file(background_tasks: BackgroundTasks, file: UploadFile = File(...)) -> IngestionResponse:
    if file.content_type not in {
        "text/csv",
        "application/vnd.ms-excel",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "application/pdf",
    }:
        raise HTTPException(status_code=400, detail="Desteklenmeyen dosya tipi.")

    if file.size and file.size > 100 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="Dosya boyutu 100MB sınırını aşıyor.")

    safe_name = file.filename or "sales_report"
    destination = UPLOAD_DIR / safe_name
    data = await file.read()
    destination.write_bytes(data)

    ingestion_id = etl.generate_ingestion_id()

    def _process() -> None:
        try:
            etl.ingest_file(destination, ingestion_id=ingestion_id)
            logger.info("Ingestion completed", extra={"ingestion_id": ingestion_id, "file": safe_name})
        except Exception:  # pragma: no cover - logged in background
            logger.exception("Ingestion failed", extra={"ingestion_id": ingestion_id})

    background_tasks.add_task(_process)

    return IngestionResponse(ingestion_id=ingestion_id, rows_ingested=0, source_file=safe_name)


@router.get("/recent", response_model=List[dict])
async def recent_uploads() -> List[dict]:
    return etl.list_recent_sources()

