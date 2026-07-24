import json
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import pandas as pd
from loguru import logger
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, to_date, year

from src.utils.spark_session import get_spark

DEFAULT_CONTROL_TABLE_PATH = Path("data/control/control_table.jsonl")


def create_bronze_table(spark: SparkSession, table_name: str):
    """
    Cria uma tabela bronze no Iceberg a partir de um DataFrame do Pandas.

    Args:
        spark (SparkSession): A sessão do Spark.
        df (pd.DataFrame): O DataFrame do Pandas contendo os dados.
        table_name (str): O nome da tabela bronze a ser criada.
    """
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS local.bronze.{table_name} (
            data         DATE,
            valor        DOUBLE,
            _ingested_at TIMESTAMP,
            _source      STRING, 
            _serie       STRING,
            ano          INT
            )
            USING iceberg
            PARTITIONED BY(ano)
            TBLPROPERTIES(
                    'write.format.default' = 'parquet',
                    'write.parquet.compression-codec' = 'snappy')
    """)
    logger.info(f"Tabela local.bronze.{table_name} criada ou existente ")

def writer_bronze(df_pandas: pd.DataFrame, serie_name:str):
    spark = get_spark(app_name=f"bronze-{serie_name}")

    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.bronze")

    create_bronze_table(spark, serie_name)

    df = spark.createDataFrame(df_pandas)

    df_bronze = (
        df
        .withColumn("data", to_date("data"))
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source", lit("bcb_api"))
        .withColumn("_serie", lit(serie_name))
        .withColumn("ano", year("data"))
    )

    # Na bronze use append para manter todo o historico
    df_bronze.writeTo(f"local.bronze.{serie_name}").append()

    count = df_bronze.count()
    logger.info(f"Bronze {serie_name}: {count} linhas gravadas")
    return count


def build_control_record(
    table_name: str,
    process_name: str,
    source: str,
    layer: str,
    row_count: int,
    started_at: datetime,
    finished_at: datetime,
    status: str,
    metadata: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """Monta o registro de controle de uma execução de ingestão/processamento."""
    return {
        "table_name": table_name,
        "process_name": process_name,
        "source": source,
        "layer": layer,
        "row_count": row_count,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "duration_seconds": (finished_at - started_at).total_seconds(),
        "status": status,
        "metadata": metadata or {},
    }


def write_control_record(
    record: dict[str, Any], control_table_path: Path = DEFAULT_CONTROL_TABLE_PATH
) -> None:
    """Acrescenta um registro de controle à tabela de controle (JSONL)."""
    control_table_path = Path(control_table_path)
    control_table_path.parent.mkdir(parents=True, exist_ok=True)
    with control_table_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")

