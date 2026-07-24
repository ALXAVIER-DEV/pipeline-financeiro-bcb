from datetime import datetime, timedelta

from dagster import (
    AssetExecutionContext,
    Definitions,
    ScheduleDefinition,
    asset,
    define_asset_job,
)


@asset(group_name="bronze")
def bronze_selic(context: AssetExecutionContext):
    """
    Asset que representa a camada Bronze para a série Selic.
    """
    from src.ingestion.bcb_client import SERIES, fetch_bcb_data
    from src.ingestion.bronze_writer import writer_bronze

    # Definir o intervalo de datas para os últimos 30 dias
    hoje = datetime.today().date()
    data_ini = (hoje - timedelta(days=30)).strftime('%d/%m/%Y')
    data_fim = hoje.strftime('%d/%m/%Y')

    # Buscar dados da API do BCB
    df = fetch_bcb_data(SERIES["selic"], data_ini, data_fim)
    writer_bronze(df, "selic")
    context.log.info(
        f"Dados da série Selic: {len(df)} rows escritos na camada Bronze "
        f"para o período de {data_ini} a {data_fim}."
    )
    return {"rows": len(df)}



@asset(group_name="bronze")
def bronze_ipca(context: AssetExecutionContext):
    """
    Asset que representa a camada Bronze para a série IPCA.
    """
    from src.ingestion.bcb_client import SERIES, fetch_bcb_data
    from src.ingestion.bronze_writer import writer_bronze

    # Definir o intervalo de datas
    hoje = datetime.today()
    data_ini = "01/01/2020"  # Data de início fixa para IPCA
    data_fim = hoje.strftime('%d/%m/%Y')

    # Buscar dados da API do BCB
    df = fetch_bcb_data(SERIES["ipca"], data_ini, data_fim)
    writer_bronze(df, "ipca")
    context.log.info(
        f"Dados da série IPCA: {len(df)} rows escritos na camada Bronze "
        f"para o período de {data_ini} a {data_fim}."
    )
    return {"rows": len(df)}


@asset(group_name="bronze")
def bronze_dollar(context: AssetExecutionContext):
    """Ingere a cotação do dólar na camada Bronze."""
    from src.ingestion.bcb_client import SERIES, fetch_bcb_data
    from src.ingestion.bronze_writer import writer_bronze

    hoje = datetime.today().date()
    data_ini = (hoje - timedelta(days=30)).strftime("%d/%m/%Y")
    data_fim = hoje.strftime("%d/%m/%Y")

    df = fetch_bcb_data(SERIES["dollar"], data_ini, data_fim)
    writer_bronze(df, "dollar")
    context.log.info(
        f"Dados da série Dollar: {len(df)} rows escritos na camada Bronze "
        f"para o período de {data_ini} a {data_fim}."
    )
    return {"rows": len(df)}


@asset(group_name="silver", deps=[bronze_selic])
def silver_selic(context: AssetExecutionContext):
    """
    Asset que representa a camada Silver para a série Selic.
    """
    from src.transformation.silver_processor import process_silver_selic
    from src.utils.spark_session import get_spark
    spark = get_spark()
    process_silver_selic(spark)
    context.log.info("Processamento da camada Silver para a série Selic concluído.")


@asset(group_name="silver", deps=[bronze_dollar])
def silver_dollar(context: AssetExecutionContext):
    """Transforma a cotação do dólar da camada Bronze para a Silver."""
    from src.transformation.processors.dollar_processor import DollarProcessor

    DollarProcessor().run()
    context.log.info(
        "Processamento da camada Silver para a série Dollar concluído."
    )


financial_job = define_asset_job(
    "financial_pipeline",
    selection=[
        "bronze_selic",
        "bronze_ipca",
        "bronze_dollar",
        "silver_selic",
        "silver_dollar",
    ],
)

# Executa de segunda a sexta-feira às 08:00 UTC
daily_schedule = ScheduleDefinition(job=financial_job, cron_schedule="0 8 * * 1-5")

defs = Definitions(
    assets=[
        bronze_selic,
        bronze_ipca,
        bronze_dollar,
        silver_selic,
        silver_dollar,
    ],
    jobs=[financial_job],
    schedules=[daily_schedule],
)

  # rodar Dagster UI local
 # dagster dev -f src/orchestration/pipeline.py
  # acessa http://localhost:3000
