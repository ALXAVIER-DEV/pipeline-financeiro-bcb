from typing import Any

import pandas as pd
import streamlit as st
from pyspark.errors import AnalysisException

from src.dashboard.metrics import (
    filter_latest_months,
    format_reference,
    latest_metric_change,
)
from src.utils.spark_session import get_spark

GOLD_TABLE = "local.gold.indicadores_macroeconomicos"


@st.cache_data(ttl=3600)
def load_data() -> pd.DataFrame:
    """Carrega a camada Gold e libera os recursos do cluster em seguida."""
    spark = get_spark("financial-dashboard")
    try:
        return spark.table(GOLD_TABLE).orderBy("mes_ref").toPandas()
    finally:
        spark.stop()


def format_metric(value: Any, prefix: str = "", suffix: str = "") -> str:
    if pd.isna(value):
        return "Sem dados"
    return f"{prefix}{float(value):.2f}{suffix}"


def render_metric(
    container: Any,
    df: pd.DataFrame,
    label: str,
    column: str,
    prefix: str = "",
    suffix: str = "",
    delta_suffix: str = "",
) -> None:
    value, change, reference = latest_metric_change(df, column)
    delta = (
        None
        if pd.isna(change)
        else f"{float(change):+.2f}{delta_suffix}"
    )
    container.metric(
        f"{label} · {format_reference(reference)}",
        format_metric(value, prefix=prefix, suffix=suffix),
        delta=delta,
    )


def render_insights(df: pd.DataFrame) -> None:
    """Apresenta leituras descritivas baseadas nas últimas observações."""
    _, ipca_change, _ = latest_metric_change(df, "ipca_12m")
    real_rate, _, _ = latest_metric_change(df, "juros_real_estimado")
    _, dollar_change, _ = latest_metric_change(df, "dollar_medio_mes")

    insights = []
    if not pd.isna(ipca_change):
        direction = "acelerou" if ipca_change > 0 else "desacelerou"
        insights.append(
            f"O IPCA em 12 meses {direction} "
            f"{abs(float(ipca_change)):.2f} p.p. na última observação."
        )
    if not pd.isna(real_rate):
        signal = "positivo" if real_rate > 0 else "negativo"
        insights.append(
            f"O juro real anual estimado está {signal}, "
            f"em {float(real_rate):.2f}%."
        )
    if not pd.isna(dollar_change):
        direction = "subiu" if dollar_change > 0 else "recuou"
        insights.append(
            f"O dólar médio {direction} R$ "
            f"{abs(float(dollar_change)):.2f} frente ao mês anterior."
        )

    for insight in insights:
        st.markdown(f"- {insight}")


def render_dashboard(df: pd.DataFrame) -> None:
    if df.empty:
        st.info(
            "A tabela Gold existe, mas ainda não possui dados. "
            "Execute o pipeline e o modelo dbt."
        )
        return

    df = df.copy()
    df["mes_ref"] = pd.to_datetime(df["mes_ref"])
    period_label = st.sidebar.selectbox(
        "Período de análise",
        ("6 meses", "12 meses", "24 meses", "Todo o período"),
        index=1,
    )
    period_map = {
        "6 meses": 6,
        "12 meses": 12,
        "24 meses": 24,
        "Todo o período": None,
    }
    filtered = filter_latest_months(df, period_map[period_label])
    latest_reference = filtered["mes_ref"].max()

    st.caption(
        "Dados mensais consolidados da API SGS do Banco Central · "
        f"janela até {latest_reference:%m/%Y}"
    )

    col1, col2, col3 = st.columns(3)
    render_metric(
        col1,
        filtered,
        "Selic anualizada",
        "selic_anualizada_mes",
        suffix="%",
        delta_suffix=" p.p.",
    )
    render_metric(
        col2,
        filtered,
        "IPCA 12 meses",
        "ipca_12m",
        suffix="%",
        delta_suffix=" p.p.",
    )
    render_metric(
        col3,
        filtered,
        "Juro real estimado",
        "juros_real_estimado",
        suffix="%",
        delta_suffix=" p.p.",
    )

    col4, col5, col6 = st.columns(3)
    render_metric(
        col4,
        filtered,
        "Dólar médio",
        "dollar_medio_mes",
        prefix="R$ ",
        delta_suffix="",
    )
    render_metric(
        col5,
        filtered,
        "PIB mensal",
        "pib_valor_mes",
        prefix="R$ ",
        suffix=" mi",
        delta_suffix=" mi",
    )
    render_metric(
        col6,
        filtered,
        "Inadimplência",
        "inadimplencia_media_mes",
        suffix="%",
        delta_suffix=" p.p.",
    )

    overview, interest, exchange, activity, data_tab = st.tabs(
        (
            "Visão geral",
            "Inflação e juros",
            "Câmbio",
            "Atividade e crédito",
            "Dados",
        )
    )

    with overview:
        st.subheader("Leitura do cenário")
        render_insights(filtered)
        st.line_chart(
            filtered.set_index("mes_ref")[
                ["selic_anualizada_mes", "ipca_12m", "juros_real_estimado"]
            ],
            y_label="Percentual ao ano",
        )

    with interest:
        st.subheader("Juros nominais, inflação e juro real")
        st.line_chart(
            filtered.set_index("mes_ref")[
                ["selic_anualizada_mes", "ipca_12m", "juros_real_estimado"]
            ],
            y_label="Percentual ao ano",
        )
        st.caption(
            "Juro real estimado pela equação de Fisher: "
            "(1 + Selic) / (1 + IPCA 12m) − 1."
        )
        st.subheader("IPCA mensal")
        st.bar_chart(
            filtered.set_index("mes_ref")["ipca_acumulado_mes"],
            y_label="Percentual no mês",
        )

    with exchange:
        st.subheader("Câmbio USD/BRL")
        st.line_chart(
            filtered.set_index("mes_ref")[
                ["dollar_medio_mes", "dollar_max_mes"]
            ],
            y_label="R$ por US$",
        )

    with activity:
        left, right = st.columns(2)
        with left:
            st.subheader("PIB mensal")
            st.line_chart(
                filtered.set_index("mes_ref")["pib_valor_mes"],
                y_label="R$ milhões",
            )
        with right:
            st.subheader("Inadimplência")
            st.line_chart(
                filtered.set_index("mes_ref")["inadimplencia_media_mes"],
                y_label="Percentual",
            )

    with data_tab:
        st.subheader("Dados consolidados")
        missing = filtered.isna().sum().rename("valores_ausentes")
        st.dataframe(
            missing.to_frame().query("valores_ausentes > 0"),
            use_container_width=True,
        )
        st.dataframe(
            filtered.sort_values("mes_ref", ascending=False),
            use_container_width=True,
            hide_index=True,
        )


def main() -> None:
    st.set_page_config(
        page_title="Financial Lakehouse Dashboard",
        page_icon="📊",
        layout="wide",
    )
    st.title("Cenário Macroeconômico Brasileiro")
    st.markdown(
        "Análise integrada de juros, inflação, câmbio, atividade e crédito."
    )

    try:
        render_dashboard(load_data())
    except AnalysisException:
        st.warning(
            "A tabela Gold ainda não está disponível. "
            "Execute o pipeline Bronze/Silver e depois `dbt run`."
        )
    except Exception as exc:
        st.error(f"Não foi possível carregar os indicadores: {exc}")


if __name__ == "__main__":
    main()
