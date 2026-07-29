from typing import Any

import pandas as pd


def valid_metric_rows(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """Ordena as observações não nulas de uma métrica."""
    return df.loc[df[column].notna(), ["mes_ref", column]].sort_values(
        "mes_ref"
    )


def latest_metric(
    df: pd.DataFrame,
    column: str,
) -> tuple[Any, pd.Timestamp | None]:
    """Retorna o valor e a competência da observação não nula mais recente."""
    valid_rows = valid_metric_rows(df, column)

    if valid_rows.empty:
        return None, None

    latest_row = valid_rows.iloc[-1]
    return latest_row[column], pd.Timestamp(latest_row["mes_ref"])


def latest_metric_change(
    df: pd.DataFrame,
    column: str,
) -> tuple[Any, Any, pd.Timestamp | None]:
    """Retorna último valor, variação contra a observação anterior e referência."""
    valid_rows = valid_metric_rows(df, column)
    if valid_rows.empty:
        return None, None, None

    latest_row = valid_rows.iloc[-1]
    previous_value = valid_rows.iloc[-2][column] if len(valid_rows) > 1 else None
    change = (
        float(latest_row[column]) - float(previous_value)
        if previous_value is not None
        else None
    )
    return (
        latest_row[column],
        change,
        pd.Timestamp(latest_row["mes_ref"]),
    )


def filter_latest_months(df: pd.DataFrame, months: int | None) -> pd.DataFrame:
    """Limita o DataFrame à quantidade solicitada de competências mensais."""
    ordered = df.sort_values("mes_ref")
    if months is None:
        return ordered
    return ordered.tail(months)


def format_reference(reference: pd.Timestamp | None) -> str:
    """Formata uma competência mensal para exibição no dashboard."""
    if reference is None:
        return "Sem referência"

    return reference.strftime("%m/%Y")
