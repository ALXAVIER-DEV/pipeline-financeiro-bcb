from abc import ABC, abstractmethod

from loguru import logger
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

from src.utils.spark_session import get_spark


class BaseProcessor(ABC):
    def __init__(self, serie_name: str):
        self.serie_name = serie_name
        self.spark: SparkSession = get_spark(f"silver-{serie_name}")

    def run(self) -> DataFrame:
        logger.info(f"Processando camada silver.{self.serie_name}")
        df_bronze = self._read_bronze()
        logger.info(f"Camada Bronze lida: {df_bronze.count()} linhas")

        df_silver = self._transform(df_bronze)
        logger.info(f"Camada Silver transformada: {df_silver.count()} linhas")

        self._create_table()
        self._write(df_silver)
        logger.info(f"Camada Silver {self.serie_name} gravada")
        return df_silver

    def _read_bronze(self) -> DataFrame:
        df = self.spark.table(f"local.bronze.{self.serie_name}")
        latest_ingestion = Window.partitionBy("data").orderBy(
            col("_ingested_at").desc()
        )
        return (
            df.withColumn("_ingestion_rank", row_number().over(latest_ingestion))
            .filter(col("_ingestion_rank") == 1)
            .drop("_ingestion_rank")
        )

    @abstractmethod
    def _transform(self, df: DataFrame) -> DataFrame:
        raise NotImplementedError

    @abstractmethod
    def _create_table(self) -> None:
        raise NotImplementedError

    def _write(self, df: DataFrame) -> None:
        df.writeTo(f"local.silver.{self.serie_name}").overwritePartitions()

