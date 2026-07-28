import os
import subprocess
import sys
from collections.abc import Sequence
from datetime import date, timedelta
from pathlib import Path

from dagster import (
    AssetExecutionContext,
    Definitions,
    MaterializeResult,
    MetadataValue,
    ScheduleDefinition,
    asset,
    define_asset_job,
    in_process_executor,
)
from loguru import logger
from pyspark.sql import SparkSession

DBT_PROJECT_DIR = Path("/app/dbt")
DEFAULT_INGESTION_DAYS = 365


def _configure_stdout_logging() -> None:
    """Envia logs da aplicação ao stdout capturado pelo Docker/Dagster."""
    logger.remove()
    logger.add(
        sys.stdout,
        colorize=False,
        enqueue=True,
        format=(
            "{time:YYYY-MM-DD HH:mm:ss.SSS} | {level:<8} | "
            "{name}:{function}:{line} | {message}"
        ),
    )


def _stop_active_spark(context: AssetExecutionContext) -> None:
    session = SparkSession.getActiveSession()
    if session is not None:
        context.log.info("Liberando sessão Spark e recursos do Worker.")
        session.stop()


def _ingestion_period(days: int = DEFAULT_INGESTION_DAYS) -> tuple[str, str]:
    end = date.today()
    start = end - timedelta(days=days)
    return start.strftime("%d/%m/%Y"), end.strftime("%d/%m/%Y")


def _ingest_series(
    context: AssetExecutionContext,
    series_name: str,
    start_date: str,
    end_date: str,
) -> MaterializeResult:
    from src.ingestion.bcb_client import SERIES, fetch_bcb_data
    from src.ingestion.bronze_writer import writer_bronze

    context.log.info(
        "API BCB | série=%s | período=%s..%s",
        series_name,
        start_date,
        end_date,
    )
    try:
        dataframe = fetch_bcb_data(
            SERIES[series_name],
            start_date,
            end_date,
            raise_on_error=True,
        )
        if dataframe.empty:
            context.log.warning(
                "API BCB não retornou registros para %s; gravação ignorada.",
                series_name,
            )
            rows = 0
        else:
            rows = writer_bronze(dataframe, series_name)

        context.log.info(
            "Bronze concluída | série=%s | linhas=%s",
            series_name,
            rows,
        )
        return MaterializeResult(
            metadata={
                "serie": series_name,
                "linhas": rows,
                "data_inicial": start_date,
                "data_final": end_date,
            }
        )
    finally:
        _stop_active_spark(context)


def _process_silver(
    context: AssetExecutionContext,
    series_name: str,
) -> MaterializeResult:
    from src.transformation.process_silver import PROCESSORS

    context.log.info("Silver iniciada | série=%s", series_name)
    processor = PROCESSORS[series_name]()
    try:
        dataframe = processor.run()
        rows = dataframe.count()
        context.log.info(
            "Silver concluída | série=%s | linhas=%s",
            series_name,
            rows,
        )
        return MaterializeResult(
            metadata={"serie": series_name, "linhas": rows}
        )
    finally:
        _stop_active_spark(context)


def _run_dbt(
    context: AssetExecutionContext,
    arguments: Sequence[str],
) -> None:
    command = [
        "dbt",
        *arguments,
        "--project-dir",
        str(DBT_PROJECT_DIR),
        "--profiles-dir",
        str(DBT_PROJECT_DIR),
        "--no-use-colors",
    ]
    context.log.info("Executando: %s", " ".join(command))

    process = subprocess.Popen(
        command,
        cwd=DBT_PROJECT_DIR,
        env={**os.environ, "PYTHONUNBUFFERED": "1"},
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    assert process.stdout is not None
    for line in process.stdout:
        context.log.info("dbt | %s", line.rstrip())

    return_code = process.wait()
    if return_code != 0:
        raise RuntimeError(
            f"dbt {' '.join(arguments)} falhou com código {return_code}"
        )


@asset(group_name="bronze", compute_kind="BCB API + Spark")
def bronze_selic(context: AssetExecutionContext) -> MaterializeResult:
    start, end = _ingestion_period()
    return _ingest_series(context, "selic", start, end)


@asset(group_name="bronze", compute_kind="BCB API + Spark")
def bronze_ipca(context: AssetExecutionContext) -> MaterializeResult:
    start, end = _ingestion_period()
    return _ingest_series(context, "ipca", start, end)


@asset(group_name="bronze", compute_kind="BCB API + Spark")
def bronze_dollar(context: AssetExecutionContext) -> MaterializeResult:
    start, end = _ingestion_period()
    return _ingest_series(context, "dollar", start, end)


@asset(group_name="bronze", compute_kind="BCB API + Spark")
def bronze_pib(context: AssetExecutionContext) -> MaterializeResult:
    start, end = _ingestion_period()
    return _ingest_series(context, "pib", start, end)


@asset(group_name="bronze", compute_kind="BCB API + Spark")
def bronze_inadimplencia(
    context: AssetExecutionContext,
) -> MaterializeResult:
    start, end = _ingestion_period()
    return _ingest_series(context, "inadimplencia", start, end)


@asset(group_name="silver", deps=[bronze_selic], compute_kind="Spark")
def silver_selic(context: AssetExecutionContext) -> MaterializeResult:
    return _process_silver(context, "selic")


@asset(group_name="silver", deps=[bronze_ipca], compute_kind="Spark")
def silver_ipca(context: AssetExecutionContext) -> MaterializeResult:
    return _process_silver(context, "ipca")


@asset(group_name="silver", deps=[bronze_dollar], compute_kind="Spark")
def silver_dollar(context: AssetExecutionContext) -> MaterializeResult:
    return _process_silver(context, "dollar")


@asset(group_name="silver", deps=[bronze_pib], compute_kind="Spark")
def silver_pib(context: AssetExecutionContext) -> MaterializeResult:
    return _process_silver(context, "pib")


@asset(
    group_name="silver",
    deps=[bronze_inadimplencia],
    compute_kind="Spark",
)
def silver_inadimplencia(
    context: AssetExecutionContext,
) -> MaterializeResult:
    return _process_silver(context, "inadimplencia")


@asset(
    group_name="gold",
    deps=[
        silver_selic,
        silver_ipca,
        silver_dollar,
        silver_pib,
        silver_inadimplencia,
    ],
    compute_kind="dbt + Spark Thrift",
)
def gold_indicadores_macroeconomicos(
    context: AssetExecutionContext,
) -> MaterializeResult:
    context.log.info("Gold iniciada | modelo=indicadores_macroeconomicos")
    _run_dbt(context, ["run", "--select", "indicadores_macroeconomicos"])
    _run_dbt(
        context,
        ["test", "--select", "indicadores_macroeconomicos"],
    )
    context.log.info("Gold concluída e testada com sucesso.")
    return MaterializeResult(
        metadata={
            "tabela": MetadataValue.text(
                "local.gold.indicadores_macroeconomicos"
            ),
            "validação": MetadataValue.text("dbt run + dbt test"),
        }
    )


ALL_ASSETS = [
    bronze_selic,
    bronze_ipca,
    bronze_dollar,
    bronze_pib,
    bronze_inadimplencia,
    silver_selic,
    silver_ipca,
    silver_dollar,
    silver_pib,
    silver_inadimplencia,
    gold_indicadores_macroeconomicos,
]

financial_job = define_asset_job(
    "financial_pipeline",
    selection=ALL_ASSETS,
    executor_def=in_process_executor,
    description="Pipeline completo: API BCB → Bronze → Silver → Gold.",
)

daily_schedule = ScheduleDefinition(
    job=financial_job,
    cron_schedule="0 8 * * 1-5",
    execution_timezone="UTC",
)

_configure_stdout_logging()

defs = Definitions(
    assets=ALL_ASSETS,
    jobs=[financial_job],
    schedules=[daily_schedule],
)
