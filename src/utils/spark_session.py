import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession

JARS_DIR = Path(__file__).parent.parent.parent / "jars"
HADOOP_CONF_DIR = Path(__file__).parent.parent.parent / "conf"
PROJECT_DIR = Path(__file__).resolve().parent.parent.parent
SPARK_LOCAL_DIR = PROJECT_DIR / "data" / "tmp" / "spark"
HADOOP_TMP_DIR = PROJECT_DIR / "data" / "tmp" / "hadoop"
S3A_BUFFER_DIR = PROJECT_DIR / "data" / "tmp" / "s3a"

def get_spark(app_name: str = "financial-pipeline") -> SparkSession:
    for temp_dir in (SPARK_LOCAL_DIR, HADOOP_TMP_DIR, S3A_BUFFER_DIR):
        temp_dir.mkdir(parents=True, exist_ok=True)

    jars = ",".join(str(j) for j in JARS_DIR.glob("*.jar"))
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    os.environ.setdefault("HADOOP_CONF_DIR", str(HADOOP_CONF_DIR))

    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[4]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.extraClassPath", str(HADOOP_CONF_DIR))
        .config("spark.executor.extraClassPath", str(HADOOP_CONF_DIR))
        .config("spark.local.dir", str(SPARK_LOCAL_DIR))
        .config("spark.hadoop.hadoop.tmp.dir", str(HADOOP_TMP_DIR))
        .config("spark.hadoop.fs.s3a.buffer.dir", str(S3A_BUFFER_DIR))
        .config("spark.python.worker.connectionTimeout", "60s")
        # Iceberg extensions
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        )
        # Catálogo Iceberg apontando para S3/LocalStack
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config(
            "spark.sql.catalog.local.warehouse",
            "s3a://financial-lakehouse/warehouse"
        )
        # S3 apontando para LocalStack
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566")
        )
        .config(
            "spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "test")
        )
        .config(
            "spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "test")
        )
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.jars", jars)
        # Performance
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
      )
