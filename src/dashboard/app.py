import pandas as pd
import streamlit as st

from src.utils.spark_session import get_spark

st.set_page_config(page_title="Financial Lakehouse Dashboard", layout="wide")
st.title("Indicadores Macroeconômicos — BCB Data Lakehouse")

@st.cache_data(ttl=3600)
def load_data() -> pd.DataFrame:
    spark = get_spark()
    return spark.table("local.gold.indicadores_macroeconomicos").toPandas()

df = load_data()

col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Selic atual", f"{df['selic_media_mes'].iloc[0]:.2f}%")
with col2:
    st.metric("IPCA ultimo mes ", f"{df['ipca_acumulado_mes'].iloc[0]:.2f}%")
with col3:
    st.metric("Dólar medio", f"R$ {df['dolar_medio_mes'].iloc[0]:.2f}")

st.subheader("Juros real Estimado")
st.area_chart(df.set_index("mes_ref")["juros_real_estimado"])

