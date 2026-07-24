from src.utils.spark_session import get_spark

spark = get_spark()

for serie in ["selic", "ipca", "dollar"]:
    try:
        df = spark.table(f"local.bronze.{serie}")
        print(f"\n=== {serie} ===")
        print(f"Total linhas: {df.count()}")
        anos = df.select("ano").distinct().orderBy("ano").collect()
        print(f"Partições (ano): {anos}")
        df.show(5)
    except Exception as e:
        print(f"{serie}: tabela não encontrada — {e}")
