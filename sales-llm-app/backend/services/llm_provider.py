"""Provider-agnostic interface for Large Language Model calls."""
from __future__ import annotations

import json
import os
from functools import lru_cache
from typing import Any, Dict, Optional

import httpx
from dotenv import load_dotenv

load_dotenv()


class LLMError(RuntimeError):
    """Raised when the LLM provider cannot be reached or returns an error."""


def _default_headers() -> Dict[str, str]:
    headers: Dict[str, str] = {}
    api_key = os.getenv("LLM_API_KEY")
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    return headers


def _post_json(url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    timeout = httpx.Timeout(60.0)
    try:
        response = httpx.post(url, json=payload, headers=_default_headers(), timeout=timeout)
        response.raise_for_status()
        return response.json()
    except httpx.HTTPError as exc:  # pragma: no cover - network errors handled gracefully
        raise LLMError(str(exc)) from exc


@lru_cache(maxsize=32)
def _provider() -> str:
    return os.getenv("LLM_PROVIDER", "mock").lower()


@lru_cache(maxsize=32)
def _model() -> str:
    return os.getenv("LLM_MODEL", "gpt-4o-mini")


@lru_cache(maxsize=32)
def _api_base() -> Optional[str]:
    return os.getenv("LLM_API_BASE")


def generate(prompt: str, system: Optional[str] = None, **kwargs: Any) -> str:
    """Generate a response using the configured LLM provider."""

    provider = _provider()
    if provider == "mock":
        return _mock_response(prompt)

    if provider == "openai":
        return _call_openai(prompt, system, **kwargs)
    if provider == "anthropic":
        return _call_anthropic(prompt, system, **kwargs)
    if provider == "azure_openai":
        return _call_azure_openai(prompt, system, **kwargs)
    if provider == "vllm_http":
        return _call_vllm_http(prompt, system, **kwargs)

    raise LLMError(f"Unsupported LLM provider: {provider}")


def _mock_response(prompt: str) -> str:
    """Offline deterministic response, handy for tests and development."""
    return json.dumps(
        {
            "summary": "Veriler, incelenen dönemde istikrarlı bir büyüme gösteriyor.",
            "highlights": [
                "Toplam satışlar önceki döneme göre %5 arttı.",
                "EMEA bölgesi toplam cironun %42'sini oluşturdu."
            ],
            "risks": [
                "APAC bölgesinde sipariş iptallerinde hafif artış gözlendi."
            ],
            "actions": [
                "EMEA kampanyasını genişlet.",
                "APAC iade sürecini gözden geçir.",
                "Yüksek performanslı ürünleri çapraz satışı teşvik etmek için paketle."
            ],
        }
    )


def _call_openai(prompt: str, system: Optional[str], **kwargs: Any) -> str:
    base_url = _api_base() or "https://api.openai.com/v1/chat/completions"
    payload = {
        "model": _model(),
        "messages": _compose_messages(prompt, system),
        "temperature": float(os.getenv("LLM_TEMPERATURE", kwargs.get("temperature", 0.2))),
        "max_tokens": int(os.getenv("LLM_MAX_TOKENS", kwargs.get("max_tokens", 800))),
    }
    data = _post_json(base_url, payload)
    try:
        return data["choices"][0]["message"]["content"]
    except (KeyError, IndexError) as exc:
        raise LLMError(f"Unexpected OpenAI response: {data}") from exc


def _call_anthropic(prompt: str, system: Optional[str], **kwargs: Any) -> str:
    base_url = _api_base() or "https://api.anthropic.com/v1/messages"
    payload = {
        "model": _model(),
        "system": system or "",
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": int(os.getenv("LLM_MAX_TOKENS", kwargs.get("max_tokens", 800))),
    }
    data = _post_json(base_url, payload)
    return data.get("content", [{}])[0].get("text", "")


def _call_azure_openai(prompt: str, system: Optional[str], **kwargs: Any) -> str:
    deployment = os.getenv("AZURE_OPENAI_DEPLOYMENT", _model())
    base_url = _api_base() or ""
    if not base_url:
        raise LLMError("LLM_API_BASE is required for Azure OpenAI")
    url = f"{base_url}/openai/deployments/{deployment}/chat/completions?api-version=2024-02-15-preview"
    payload = {
        "messages": _compose_messages(prompt, system),
        "temperature": float(os.getenv("LLM_TEMPERATURE", kwargs.get("temperature", 0.2))),
        "max_tokens": int(os.getenv("LLM_MAX_TOKENS", kwargs.get("max_tokens", 800))),
    }
    data = _post_json(url, payload)
    try:
        return data["choices"][0]["message"]["content"]
    except (KeyError, IndexError) as exc:
        raise LLMError(f"Unexpected Azure OpenAI response: {data}") from exc


def _call_vllm_http(prompt: str, system: Optional[str], **kwargs: Any) -> str:
    base_url = _api_base()
    if not base_url:
        raise LLMError("LLM_API_BASE is required for vLLM HTTP provider")
    payload = {
        "prompt": prompt if system is None else f"{system}\n\n{prompt}",
        "temperature": float(os.getenv("LLM_TEMPERATURE", kwargs.get("temperature", 0.2))),
        "max_tokens": int(os.getenv("LLM_MAX_TOKENS", kwargs.get("max_tokens", 800))),
    }
    data = _post_json(base_url, payload)
    return data.get("text", "")


def _compose_messages(prompt: str, system: Optional[str]) -> Any:
    messages = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": prompt})
    return messages

