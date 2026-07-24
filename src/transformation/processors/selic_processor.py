from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    avg,
    col,
    dayofweek,
    lag,
    month,
    quarter,
    stddev,
    when,
    year,
)
from pyspark.sql.functions import round as spark_round
from pyspark.sql.window import Window

from src.transformation.base_processor import BaseProcessor


class SelicProcessor(BaseProcessor):
    def __init__(self):
        super().__init__("selic")

    def _transform(self, df: DataFrame):
        window_order = Window.orderBy("data")
        window_30d   = Window.orderBy("data").rowsBetween(-29, 0)
        window_year  = Window.partitionBy(year("data")).orderBy("data")

        return (
            # Removendo valores null e invalidos
            df
            .filter(col("valor").isNotNull())
            .filter(col("valor") > 0)
            .filter(col("valor") < 100)

            # Dimensoes temporais
            .withColumn("ano",        year("data"))
            .withColumn("mes",        month("data"))
            .withColumn('trimestre',  quarter("data"))
            .withColumn("dia_semana", dayofweek("data"))

            # variacao diaria
            .withColumn("variacao_pp",
                        spark_round(col("valor") - lag("valor", 1).over(window_order),4)
            )

            # taxa anualizada ( regara de 252 dias uteis )
            .withColumn("taxa_anualizada",
                        spark_round((1 + col("valor") / 100 ) ** 252 -1, 6)
            )

            # media movel 30 dias 
            .withColumn("media_movel_30d",
                        spark_round(avg("valor").over(window_30d), 4)
            )

            #Desvio padrao 30 dias (volatilidade)
            .withColumn("volatilidade_30d",
                        spark_round(stddev("valor").over(window_30d), 6)
            )
            # acumulado ano
            .withColumn("acumulado_ano",
                        spark_round(avg("valor").over(window_year), 4)
            )

            # Classificacao nivel de taxa
            .withColumn("nivel_taxa",
                when(col("valor")  < 6, "baixo")
                .when(col("valor") < 10, "moderado")
                .when(col("valor") < 14, "alto")
                .otherwise("muito_alto")
            )

             # remove colunas de auditoria Bronze
              .drop("_ingested_at", "_source", "_serie")

              .orderBy("data")
    )

    
    def _create_table(self):
        self.spark.sql("CREATE NAMESPACE IF NOT EXISTS local.silver")
        self.spark.sql("""
            CREATE TABLE IF NOT EXISTS local.silver.selic (
                data              DATE,
                valor             DOUBLE,
                variacao_pp       DOUBLE,
                taxa_anualizada   DOUBLE,
                media_movel_30d   DOUBLE,
                volatilidade_30d  DOUBLE,
                acumulado_ano     DOUBLE,
                nivel_taxa        STRING,
                ano               INT,
                mes               INT,
                trimestre         INT,
                dia_semana        INT
            )
            USING iceberg
            PARTITIONED BY (ano, mes)
            TBLPROPERTIES (
                'write.format.default'            = 'parquet',
                'write.parquet.compression-codec' = 'snappy',
                'write.target-file-size-bytes'    = '134217728'
            )
        """)


        
        
