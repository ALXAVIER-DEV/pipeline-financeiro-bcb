from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lag, lit, month, quarter, when, year
from pyspark.sql.functions import round as spark_round
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.window import Window

from src.transformation.base_processor import BaseProcessor


class IpcaProcessor(BaseProcessor):
    def __init__(self):
        super().__init__("ipca")

    def _transform(self, df: DataFrame) -> DataFrame:
        window_order = Window.orderBy("data")
        window_12m   = Window.orderBy("data").rowsBetween( -11, 0)
        window_year  = Window.partitionBy(year("data")).orderBy("data")

        return (
            df
            .filter(col("valor").isNotNull())

            .withColumn("ano",          year("data"))
            .withColumn("mes",          month("data"))
            .withColumn("trimestre",    quarter("data"))

            # variacao mes a mes em pontos percentuais
            .withColumn("variacao_pp",                        
                        spark_round(col("valor") - lag("valor", 1)
                                    .over(window_order), 4)
            )

            #acumulado 12 meses (inflacao anual )
            .withColumn("acumulado_12m",
                        spark_round(spark_sum("valor").over(window_12m), 4)
            )

            # acumulado no ano
            .withColumn("acumulado_ano",
                        spark_round(spark_sum("valor").over(window_year), 4)
            )
            # pressao inflacionaria
            .withColumn("pressao",
                when(col("valor") < 0,        "deflacao" )
                    .when(col("valor") < 0.3, "controlada" )
                    .when(col("valor") < 0.6, "moderada" )
                    .otherwise(lit("elevada"))
                )
            .drop("_ingested_at", "_source", "_serie")
            .orderBy("data")

        )

    def _create_table(self):
        self.spark.sql("CREATE NAMESPACE IF NOT EXISTS local.silver")
        self.spark.sql("""
            CREATE TABLE IF NOT EXISTS local.silver.ipca (
            data DATE,
            valor DOUBLE,
            variacao_pp DOUBLE,
            acumulado_12m DOUBLE,
            acumulado_ano DOUBLE,
            pressao STRING,
            ano INT,
            mes INT,
            trimestre INT
        )
        USING iceberg
        PARTITIONED BY (ano, mes)
        
        """)
                        
        
