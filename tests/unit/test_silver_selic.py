import pytest
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DateType, DoubleType, IntegerType

@pytest.fixture(scope="session")
def spark():
  return (
      SparkSession.builder
      .appName("test-silver")
      .master("local[1]")
      .getOrCreate()
  )

@pytest.fixture
def df_bronze_mock(spark):
  schema = StructType([
      StructField("data",  DateType(),   True),
      StructField("valor", DoubleType(), True),
      StructField("ano",   IntegerType(), True),
  ])
  data = [
      (date(2024, 1, 2), 11.75, 2024),
      (date(2024, 1, 3), 11.75, 2024),
      (date(2024, 1, 4), 11.75, 2024),
      (date(2024, 1, 5), 12.25, 2024),
      (date(2024, 1, 8), 12.25, 2024),
  ]
  return spark.createDataFrame(data, schema)

def test_remove_nulos(spark, df_bronze_mock):
  from pyspark.sql.functions import lit
  df_com_nulo = df_bronze_mock.union(
      spark.createDataFrame(
          [(date(2024, 1, 9), None, 2024)],
          df_bronze_mock.schema
      )
  )
  df_filtrado = df_com_nulo.filter("valor is not null")
  assert df_filtrado.count() == 5

def test_variacao_calculada(spark, df_bronze_mock):
  from pyspark.sql.functions import lag, col
  from pyspark.sql.window import Window

  w = Window.orderBy("data")
  df = df_bronze_mock.withColumn(
      "variacao_pp", col("valor") - lag("valor", 1).over(w)
  )
  # primeira linha não tem variação (null)
  assert df.filter("variacao_pp is null").count() == 1

def test_nivel_taxa_classificado(spark, df_bronze_mock):
  from pyspark.sql.functions import when, col
  df = df_bronze_mock.withColumn(
      "nivel_taxa",
      when(col("valor") < 6, "baixo")
      .when(col("valor") < 10, "moderado")
      .when(col("valor") < 14, "alto")
      .otherwise("muito_alto")
  )
  )
niveis = [r.nivel_taxa for r in df.select("nivel_taxa").collect()]
assert all(n == "alto" for n in niveis)
