import pandas as pd

from backend.services.data_summary import profile_data, summarize_dataframe


def test_profile_data_handles_legacy_pandas(monkeypatch):
    df = pd.DataFrame(
        {
            "sale_date": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
            "amount": [10, 20, 30],
            "region": ["Ege", "Marmara", "Akdeniz"],
        }
    )

    original_describe = pd.DataFrame.describe

    def fake_describe(self, *args, **kwargs):
        if "datetime_is_numeric" in kwargs:
            raise TypeError("unexpected keyword argument 'datetime_is_numeric'")
        return original_describe(self, *args, **kwargs)

    monkeypatch.setattr(pd.DataFrame, "describe", fake_describe, raising=False)

    missing, stats = profile_data(df)

    # Ensure missing ratios are calculated and the stats dataframe contains the
    # expected column name despite the fallback.
    assert "sale_date" in stats["column"].tolist()
    assert missing.loc[missing["column"] == "amount", "missing_ratio"].item() == 0


def test_summarize_dataframe_includes_schema_descriptions():
    df = pd.DataFrame(
        {
            "sale_date": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
            "amount": [10, 20, 30],
            "region": ["Ege", None, "Akdeniz"],
        }
    )

    schema = {
        "sale_date": {"description": "Satış tarihi"},
        "amount": {"description": "Satış tutarı"},
        "region": {"description": "Satış bölgesi"},
    }

    summary = summarize_dataframe(df, schema=schema)

    assert "Toplam satır sayısı: 3" in summary
    assert "Şema kolonları" in summary
    assert "Eksik veri oranları" in summary
    assert "Kolon açıklamaları" in summary
