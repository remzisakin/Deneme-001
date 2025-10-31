from __future__ import annotations

import json
import os

import pytest

from backend.models.schemas import PromptContext
from backend.services import prompts


@pytest.fixture(autouse=True)
def _set_mock_env(monkeypatch):
    monkeypatch.setenv("LLM_PROVIDER", "mock")
    yield


def test_build_analysis_prompt_contains_sections(tmp_path, monkeypatch):
    context = PromptContext(
        stats_json={"total_sales": 100},
        anomalies_json={"items": []},
        pdf_context=["Önemli paragraf"],
    )
    prompt_text = prompts.build_analysis_prompt(context)
    assert "BAĞLAM" in prompt_text
    assert "Önemli paragraf" in prompt_text


def test_run_analysis_returns_schema(monkeypatch):
    context = PromptContext(stats_json={}, anomalies_json={}, pdf_context=[])
    insight = prompts.run_analysis(context)
    assert insight.summary
    assert len(insight.actions) == 3

