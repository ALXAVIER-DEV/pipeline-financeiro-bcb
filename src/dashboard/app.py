import pandas as pd
import streamlit as st
from pyspark.errors import AnalysisException

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


def format_metric(value: object, prefix: str = "", suffix: str = "") -> str:
    if pd.isna(value):
        return "Sem dados"
    return f"{prefix}{float(value):.2f}{suffix}"


def render_dashboard(df: pd.DataFrame) -> None:
    if df.empty:
        st.info(
            "A tabela Gold existe, mas ainda não possui dados. "
            "Execute o pipeline e o modelo dbt."
        )
        return

    latest = df.iloc[-1]
    col1, col2, col3 = st.columns(3)
    col1.metric(
        "Selic média",
        format_metric(latest["selic_media_mes"], suffix="%"),
    )
    col2.metric(
        "IPCA no mês",
        format_metric(latest["ipca_acumulado_mes"], suffix="%"),
    )
    col3.metric(
        "Dólar médio",
        format_metric(latest["dollar_medio_mes"], prefix="R$ "),
    )

    st.caption(
        "Última referência disponível: "
        f"{pd.Timestamp(latest['mes_ref']):%m/%Y}"
    )
    st.subheader("Juro real estimado")
    st.area_chart(
        df.set_index("mes_ref")["juros_real_estimado"],
        y_label="Percentual",
    )


def main() -> None:
    st.set_page_config(
        page_title="Financial Lakehouse Dashboard",
        layout="wide",
    )
    st.title("Indicadores Macroeconômicos — BCB Data Lakehouse")

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
