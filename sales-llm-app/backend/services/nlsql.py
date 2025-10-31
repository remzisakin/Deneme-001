"""Natural language to SQL generation with validation."""
from __future__ import annotations

import re
from typing import Any, Dict

from . import llm_provider

ALLOWED_KEYWORDS = {"select", "from", "where", "group", "by", "order", "limit", "asc", "desc", "and", "or", "sum", "avg", "count"}
ALLOWED_TABLE = "fact_sales"
SQL_PATTERN = re.compile(r"^\s*select\s+.+", re.IGNORECASE | re.DOTALL)


def _is_safe_sql(sql: str) -> bool:
    lowered = sql.lower()
    if "fact_sales" not in lowered:
        return False
    if any(keyword in lowered for keyword in ["drop", "delete", "update", "insert"]):
        return False
    tokens = re.findall(r"[a-z_]+", lowered)
    return all(token in ALLOWED_KEYWORDS or token == ALLOWED_TABLE or not token.isalpha() for token in tokens)


def generate_sql(question: str) -> str:
    system = (
        "Kıdemli veri analisti gibi davran. Sadece DuckDB uyumlu SELECT sorgusu üret. "
        "DROP/INSERT/UPDATE yasak. Tek tablo: fact_sales."
    )
    prompt = (
        "Kullanıcı sorusu: "
        + question
        + "\nYalnızca SQL döndür (açıklama yazma)."
    )
    sql = llm_provider.generate(prompt=prompt, system=system)
    sql = sql.strip()
    if not SQL_PATTERN.match(sql) or not _is_safe_sql(sql):
        raise ValueError("LLM tarafından üretilen SQL güvenli değil.")
    return sql


def execute_sql(sql: str, limit: int = 100) -> Dict[str, Any]:
    from ..db.duck import get_connection

    conn = get_connection(readonly=True)
    limited_sql = sql
    if "limit" not in sql.lower():
        limited_sql = f"{sql.rstrip(';')} LIMIT {limit}"
    df = conn.execute(limited_sql).df()
    return {
        "sql": limited_sql,
        "rows": df.to_dict(orient="records"),
    }

