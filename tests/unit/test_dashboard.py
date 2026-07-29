import pandas as pd

from src.dashboard.metrics import format_reference, latest_metric


def test_latest_metric_uses_latest_non_null_value() -> None:
    df = pd.DataFrame(
        {
            "mes_ref": pd.to_datetime(
                ["2026-05-01", "2026-06-01", "2026-07-01"]
            ),
            "pib_valor_mes": [100.0, 110.0, None],
        }
    )

    value, reference = latest_metric(df, "pib_valor_mes")

    assert value == 110.0
    assert reference == pd.Timestamp("2026-06-01")


def test_latest_metric_handles_column_without_data() -> None:
    df = pd.DataFrame(
        {
            "mes_ref": pd.to_datetime(["2026-06-01", "2026-07-01"]),
            "pib_valor_mes": [None, None],
        }
    )

    value, reference = latest_metric(df, "pib_valor_mes")

    assert value is None
    assert reference is None
    assert format_reference(reference) == "Sem referência"
