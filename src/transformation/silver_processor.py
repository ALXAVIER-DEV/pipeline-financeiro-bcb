from pyspark.sql import SparkSession
from pyspark.sql.functions import col, dayofweek, lag, month, year
from pyspark.sql.functions import round as spark_round
from pyspark.sql.window import Window


def process_silver_selic(spark: SparkSession):
    """
    Processa os dados da taxa Selic na camada Silver.

    Args:
        spark (SparkSession): A sessão do Spark.

    Returns:
        DataFrame: DataFrame processado com as colunas adicionais.
    """    
    df = spark.table("local.bronze.selic")

    window = Window.orderBy("data")

    df_silver = (
        df
        .filter(col("valor").isNotNull())
        .filter(col("valor") > 0)
        .withColumn("variacao_pp", col("valor") - lag("valor", 1).over(window))
        .withColumn("ano", year("data"))
        .withColumn("mes", month("data"))
        .withColumn("dia_semana", dayofweek("data"))
        .withColumn(
            "taxa_anualizada", spark_round((1 + col("valor") / 100) ** 252 - 1, 4)
        )
        .drop("_source", "_ingested_at")
    )

    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.silver.selic (
            data DATE,
            valor DOUBLE,
            variacao_pp DOUBLE,
            taxa_anualizada DOUBLE,
            ano INT,
            mes INT,
            dia_semana INT,
            _serie STRING
            )
        USING iceberg 
            PARTITIONED BY (ano, mes)        
        """)

    df_silver.writeTo("local.silver.selic").overwritePartitions()