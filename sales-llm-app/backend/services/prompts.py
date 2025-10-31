"""Prompt helpers and validation for LLM outputs."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

from pydantic import BaseModel, ValidationError

from ..models.schemas import LLMInsight, PromptContext
from . import llm_provider

PROMPT_DIR = Path(__file__).resolve().parents[2] / "prompts"
SYSTEM_PROMPT_PATH = PROMPT_DIR / "system_prompt.txt"
ANALYSIS_PROMPT_PATH = PROMPT_DIR / "analysis_prompt.txt"


class LLMInsightSchema(BaseModel):
    summary: str
    highlights: list[str]
    risks: list[str]
    actions: list[str]


def _read_prompt(path: Path) -> str:
    if not path.exists():
        raise FileNotFoundError(f"Prompt dosyası bulunamadı: {path}")
    return path.read_text(encoding="utf-8")


def build_analysis_prompt(context: PromptContext) -> str:
    prompt_template = _read_prompt(ANALYSIS_PROMPT_PATH)
    prompt = prompt_template.format(
        stats_json=json.dumps(context.stats_json, ensure_ascii=False, indent=2),
        anomaly_json=json.dumps(context.anomalies_json, ensure_ascii=False, indent=2),
    )
    if context.pdf_context:
        prompt += "\n\nPDF BAĞLAMI:\n" + "\n".join(context.pdf_context)
    return prompt


def run_analysis(context: PromptContext) -> LLMInsight:
    system_prompt = _read_prompt(SYSTEM_PROMPT_PATH)
    prompt = build_analysis_prompt(context)
    raw = llm_provider.generate(prompt=prompt, system=system_prompt)
    try:
        data = json.loads(raw)
        result = LLMInsightSchema.model_validate(data)
    except (json.JSONDecodeError, ValidationError) as exc:
        raise RuntimeError("LLM çıktısı beklenen şemaya uymuyor") from exc
    return LLMInsight(**result.model_dump())

