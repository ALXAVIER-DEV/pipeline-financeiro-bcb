from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, col, lag, month, quarter, when, year
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import min as spark_min
from pyspark.sql.functions import round as spark_round
from pyspark.sql.window import Window

from src.transformation.base_processor import BaseProcessor


class DollarProcessor(BaseProcessor):
    def __init__(self):
        super().__init__("dollar")

    def _transform(self, df: DataFrame) -> DataFrame:
        window_order = Window.orderBy("data")
        window_30d = Window.orderBy("data").rowsBetween(-29, 0)

        return (
            df
            .filter(col("valor").isNotNull())
            .filter(col("valor") > 0)
            .withColumn("ano", year("data"))
            .withColumn("mes", month("data"))
            .withColumn("trimestre", quarter("data"))
            .withColumn(
                "variacao_abs",
                spark_round(col("valor") - lag("valor", 1).over(window_order), 4),
            )
            .withColumn(
                "variacao_pct",
                spark_round(
                    (col("valor") - lag("valor", 1).over(window_order))
                    / lag("valor", 1).over(window_order) * 100,
                    4,
                ),
            )
            .withColumn(
                "media_movel_30d",
                spark_round(avg("valor").over(window_30d), 4),
            )
            .withColumn(
                "max_30d",
                spark_round(spark_max("valor").over(window_30d), 4),
            )
            .withColumn(
                "min_30d",
                spark_round(spark_min("valor").over(window_30d), 4),
            )
            .withColumn(
                "tendencia",
                when(col("variacao_abs") > 0.05, "alta")
                .when(col("variacao_abs") < -0.05, "queda")
                .otherwise("estavel"),
            )
            .drop("_ingested_at", "_source", "_serie")
            .orderBy("data")
        )

    def _create_table(self):
        self.spark.sql("CREATE NAMESPACE IF NOT EXISTS local.silver")
        self.spark.sql("""
                  CREATE TABLE IF NOT EXISTS local.silver.dollar (
                      data             DATE,
                      valor            DOUBLE,
                      variacao_abs     DOUBLE,
                      variacao_pct     DOUBLE,
                      media_movel_30d  DOUBLE,
                      max_30d          DOUBLE,
                      min_30d          DOUBLE,
                      tendencia        STRING,
                      ano              INT,
                      mes              INT,
                      trimestre        INT
                  )
                  USING iceberg
                  PARTITIONED BY (ano, mes)
              """)

