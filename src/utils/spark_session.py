import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession

PROJECT_DIR = Path(__file__).resolve().parent.parent.parent
JARS_DIR = PROJECT_DIR / "jars"
HADOOP_CONF_DIR = PROJECT_DIR / "conf"
SPARK_LOCAL_DIR = PROJECT_DIR / "data" / "tmp" / "spark"
HADOOP_TMP_DIR = PROJECT_DIR / "data" / "tmp" / "hadoop"
S3A_BUFFER_DIR = PROJECT_DIR / "data" / "tmp" / "s3a"


def get_spark(app_name: str = "financial-pipeline") -> SparkSession:
    """Cria uma SparkSession para execução local ou no cluster Docker."""
    for temp_dir in (SPARK_LOCAL_DIR, HADOOP_TMP_DIR, S3A_BUFFER_DIR):
        temp_dir.mkdir(parents=True, exist_ok=True)

    master_url = os.getenv("SPARK_MASTER_URL", "local[4]")
    driver_host = os.getenv("SPARK_DRIVER_HOST", "127.0.0.1")
    driver_bind_address = os.getenv(
        "SPARK_DRIVER_BIND_ADDRESS",
        "127.0.0.1",
    )
    driver_port = os.getenv("SPARK_DRIVER_PORT", "40440")
    block_manager_port = os.getenv(
        "SPARK_BLOCK_MANAGER_PORT",
        "40441",
    )

    aws_endpoint = os.getenv(
        "AWS_ENDPOINT_URL",
        "http://localhost:4566",
    )
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID", "test")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "test")
    aws_region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
    spark_warehouse = os.getenv(
        "SPARK_WAREHOUSE",
        "s3a://financial-lakehouse/warehouse",
    )

    python_executable = sys.executable
    os.environ["PYSPARK_PYTHON"] = python_executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_executable
    os.environ.setdefault("HADOOP_CONF_DIR", str(HADOOP_CONF_DIR))

    builder = (
        SparkSession.builder.appName(app_name)
        .master(master_url)
        .config("spark.executorEnv.PYSPARK_PYTHON", python_executable)
        .config("spark.driver.extraClassPath", str(HADOOP_CONF_DIR))
        .config("spark.executor.extraClassPath", str(HADOOP_CONF_DIR))
        .config("spark.local.dir", str(SPARK_LOCAL_DIR))
        .config("spark.hadoop.hadoop.tmp.dir", str(HADOOP_TMP_DIR))
        .config("spark.hadoop.fs.s3a.buffer.dir", str(S3A_BUFFER_DIR))
        .config("spark.python.worker.connectionTimeout", "60s")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions."
            "IcebergSparkSessionExtensions",
        )
        .config(
            "spark.sql.catalog.local",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", spark_warehouse)
        .config("spark.hadoop.fs.s3a.endpoint", aws_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key)
        .config("spark.hadoop.fs.s3a.endpoint.region", aws_region)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .config(
            "spark.hadoop.fs.s3a.connection.ssl.enabled",
            str(aws_endpoint.startswith("https://")).lower(),
        )
        .config("spark.sql.adaptive.enabled", "true")
        .config(
            "spark.sql.adaptive.coalescePartitions.enabled",
            "true",
        )
        .config("spark.sql.session.timeZone", "UTC")
    )

    if not master_url.startswith("local"):
        builder = (
            builder.config("spark.driver.host", driver_host)
            .config(
                "spark.driver.bindAddress",
                driver_bind_address,
            )
            .config("spark.driver.port", driver_port)
            .config(
                "spark.blockManager.port",
                block_manager_port,
            )
        )

    jar_files = sorted(JARS_DIR.glob("*.jar"))
    if jar_files:
        builder = builder.config(
            "spark.jars",
            ",".join(str(jar) for jar in jar_files),
        )

    return builder.getOrCreate()
