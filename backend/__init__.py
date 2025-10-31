"""Compatibility package for ``uvicorn backend.main``.

This shim exposes the FastAPI backend package located under
``sales-llm-app/backend`` so that importing ``backend`` from the
repository root works without adjusting ``PYTHONPATH``.
"""
from __future__ import annotations

from pathlib import Path

_BACKEND_DIR = Path(__file__).resolve().parent.parent / "sales-llm-app" / "backend"
if not _BACKEND_DIR.exists():  # pragma: no cover - defensive programming
    raise ImportError(f"Expected backend sources at {_BACKEND_DIR}")

# Expose the actual backend package directory so that submodules such as
# ``backend.main`` resolve to the real implementation under
# ``sales-llm-app/backend``.
__path__ = [str(_BACKEND_DIR)]
