from typing import Any

import pandas as pd


def latest_metric(
    df: pd.DataFrame,
    column: str,
) -> tuple[Any, pd.Timestamp | None]:
    """Retorna o valor e a competência da observação não nula mais recente."""
    valid_rows = df.loc[df[column].notna(), ["mes_ref", column]]

    if valid_rows.empty:
        return None, None

    latest_row = valid_rows.sort_values("mes_ref").iloc[-1]
    return latest_row[column], pd.Timestamp(latest_row["mes_ref"])


def format_reference(reference: pd.Timestamp | None) -> str:
    """Formata uma competência mensal para exibição no dashboard."""
    if reference is None:
        return "Sem referência"

    return reference.strftime("%m/%Y")
