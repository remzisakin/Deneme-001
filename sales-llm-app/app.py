"""Streamlit front-end for the Sales & LLM analysis platform."""
from __future__ import annotations

import json
import os
from datetime import date
from typing import Any, Dict, List

import pandas as pd
import plotly.express as px
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

BACKEND_URL = os.getenv("BACKEND_URL", "http://localhost:8000")

st.set_page_config(page_title="Satış Raporlama ve LLM Analiz", layout="wide", page_icon="📊")

st.title("📊 Satış Raporlama ve LLM Analiz")
st.caption("Çok formatlı satış raporlarını yükleyin, otomatik analiz ve LLM içgörüleri alın.")


def _api_post(path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    response = st.session_state.http_client.post(f"{BACKEND_URL}{path}", json=payload)
    response.raise_for_status()
    return response.json()


def _api_get(path: str) -> Dict[str, Any]:
    response = st.session_state.http_client.get(f"{BACKEND_URL}{path}")
    response.raise_for_status()
    return response.json()


def _init_http_client() -> None:
    import httpx

    if "http_client" not in st.session_state:
        st.session_state.http_client = httpx.Client(timeout=60.0)


_init_http_client()

with st.sidebar:
    st.header("Yükleme ve Filtreler")
    uploaded_file = st.file_uploader("Satış raporu yükle", type=["csv", "xlsx", "pdf"])
    if uploaded_file is not None:
        with st.spinner("Dosya yükleniyor..."):
            files = {"file": (uploaded_file.name, uploaded_file.getvalue(), uploaded_file.type)}
            response = st.session_state.http_client.post(f"{BACKEND_URL}/ingest/upload", files=files)
            if response.status_code == 200:
                st.success("Dosya başarıyla kuyruğa alındı. Analiz tamamlandığında tabloya eklenecek.")
            else:
                st.error(f"Yükleme başarısız: {response.text}")

    st.markdown("---")
    start_date = st.date_input("Başlangıç tarihi", value=None)
    end_date = st.date_input("Bitiş tarihi", value=None)
    region = st.text_input("Bölge filtresi")
    category = st.text_input("Kategori filtresi")
    run_analysis = st.button("Analizi Çalıştır")

    st.markdown("---")
    st.subheader("Son Yüklemeler")
    try:
        recent = _api_get("/ingest/recent")
        st.write(pd.DataFrame(recent))
    except Exception:
        st.info("Henüz yüklenmiş veri yok.")

if run_analysis:
    filters = {
        "start_date": start_date.isoformat() if isinstance(start_date, date) else None,
        "end_date": end_date.isoformat() if isinstance(end_date, date) else None,
        "region": region or None,
        "category": category or None,
    }
    with st.spinner("Analiz çalıştırılıyor..."):
        try:
            data = _api_post("/analyze/run", filters)
            st.session_state["analysis_result"] = data
        except Exception as exc:  # pragma: no cover - network issues
            st.error(f"Analiz başarısız: {exc}")

result = st.session_state.get("analysis_result")

if not result:
    st.info("Filtreleri ayarlayıp 'Analizi Çalıştır' butonuna basın.")
    st.stop()

kpi_cols = st.columns(4)
kpis = result["kpis"]
kpi_cols[0].metric("Toplam Satış", f"€{kpis['total_sales']:,.2f}")
kpi_cols[1].metric("Toplam Adet", f"{kpis['total_quantity']:,.0f}")
kpi_cols[2].metric("Ortalama Sepet", f"€{kpis['average_basket']:,.2f}")
kpi_cols[3].metric("En İyi Ürün", kpis.get("top_product") or "-")

trend_tab, anomalies_tab, insight_tab, nlsql_tab = st.tabs(
    ["Trendler", "Anomaliler", "LLM İçgörüleri", "Doğal Dil Sorgu"]
)

with trend_tab:
    trend_df = pd.DataFrame(result["trends"]["series"])
    if trend_df.empty:
        st.warning("Trend verisi bulunamadı.")
    else:
        fig = px.line(trend_df, x="bucket", y="total_sales", title="Satış Trendleri")
        fig.add_scatter(x=trend_df["bucket"], y=trend_df["moving_average"], mode="lines", name="Moving Average")
        st.plotly_chart(fig, use_container_width=True)

with anomalies_tab:
    anomalies_df = pd.DataFrame(result["anomalies"])
    if anomalies_df.empty:
        st.success("Anomali tespit edilmedi.")
    else:
        st.dataframe(anomalies_df)
        fig = px.scatter(
            anomalies_df,
            x="date",
            y="sales_amount",
            color="score",
            hover_data=["product", "region"],
            title="Anomaliler",
        )
        st.plotly_chart(fig, use_container_width=True)

with insight_tab:
    insight = result["insight"]
    st.subheader("Yönetici Özeti")
    st.write(insight["summary"])
    st.subheader("Öne Çıkanlar")
    for item in insight["highlights"]:
        st.markdown(f"- {item}")
    st.subheader("Riskler")
    for item in insight["risks"]:
        st.markdown(f"- ⚠️ {item}")
    st.subheader("Aksiyonlar")
    for item in insight["actions"]:
        st.markdown(f"- ✅ {item}")
    if st.button("Yeniden Yaz"):
        if run_analysis:
            st.warning("Analiz zaten çalıştırıldı. Yeni sonuçlar için tekrar çalıştırın.")
        else:
            st.info("Analiz sonuçlarını yenilemek için sol panelden tekrar çalıştırın.")

with nlsql_tab:
    st.subheader("Doğal Dil Sorgusu")
    question = st.text_input("Sorunuzu yazın", key="nlsql-question")
    limit = st.slider("Kayıt limiti", min_value=5, max_value=200, value=20, step=5)
    if st.button("Sorguyu Çalıştır"):
        try:
            payload = {"question": question, "limit": limit}
            response = _api_post("/nlsql/query", payload)
            df = pd.DataFrame(response["rows"])
            st.code(response["sql"])
            if df.empty:
                st.warning("Sorgu sonucu boş döndü.")
            else:
                st.dataframe(df)
                try:
                    chart = px.bar(df.head(10), x=df.columns[0], y=df.columns[1])
                    st.plotly_chart(chart, use_container_width=True)
                except Exception:
                    st.write("Grafik oluşturulamadı, tabloyu kullanın.")
        except Exception as exc:  # pragma: no cover
            st.error(f"Sorgu çalıştırılamadı: {exc}")

st.markdown("---")
st.caption("© 2024 Satış Analiz Platformu · Açık/Koyu tema desteği Streamlit ayarlarında mevcuttur.")

