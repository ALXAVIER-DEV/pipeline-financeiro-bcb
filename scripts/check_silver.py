from src.utils.spark_session import get_spark

spark = get_spark()

queries = {
    "selic": """
         SELECT
             MIN(data)            AS data_inicio,
             MAX(data)            AS data_fim,
             COUNT(*)             AS total_linhas,
             ROUND(AVG(valor), 2) AS selic_media,
             COUNT(DISTINCT nivel_taxa) AS niveis_distintos
         FROM local.silver.selic
     """,
    "ipca": """
         SELECT
             MIN(data)                   AS data_inicio,
             MAX(data)                   AS data_fim,
             COUNT(*)                    AS total_linhas,
             ROUND(AVG(acumulado_12m),2) AS inflacao_12m_media
         FROM local.silver.ipca
     """,
    "dollar": """
         SELECT
             MIN(data)            AS data_inicio,
             MAX(data)            AS data_fim,
             COUNT(*)             AS total_linhas,
             ROUND(AVG(valor), 2) AS dolar_medio,
             ROUND(MAX(valor), 2) AS dolar_max
         FROM local.silver.dollar
     """,
}

for serie, query in queries.items():
    print(f"\n=== silver.{serie} ===")
    try:
        spark.sql(query).show()
    except Exception as e:
        print(f"Erro: {e}")

