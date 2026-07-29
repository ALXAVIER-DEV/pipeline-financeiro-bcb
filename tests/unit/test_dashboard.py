import pandas as pd
import pytest

from src.dashboard.metrics import (
    filter_latest_months,
    format_reference,
    latest_metric,
    latest_metric_change,
)


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


def test_latest_metric_change_ignores_nulls() -> None:
    df = pd.DataFrame(
        {
            "mes_ref": pd.to_datetime(
                ["2026-04-01", "2026-05-01", "2026-06-01"]
            ),
            "ipca_12m": [4.5, 4.2, None],
        }
    )

    value, change, reference = latest_metric_change(df, "ipca_12m")

    assert value == 4.2
    assert change == pytest.approx(-0.3)
    assert reference == pd.Timestamp("2026-05-01")


def test_filter_latest_months_orders_and_limits_rows() -> None:
    df = pd.DataFrame(
        {
            "mes_ref": pd.to_datetime(
                ["2026-03-01", "2026-01-01", "2026-02-01"]
            ),
            "valor": [3, 1, 2],
        }
    )

    result = filter_latest_months(df, 2)

    assert result["valor"].tolist() == [2, 3]
