"""Dados determinísticos para validar Bronze -> Silver -> Gold no Docker."""

import argparse
from datetime import date

import pandas as pd

from src.ingestion.bronze_writer import writer_bronze
from src.transformation.process_silver import PROCESSORS
from src.utils.spark_session import get_spark

FIXTURES = {
    "selic": [
        ("2025-01-02", 0.0400),
        ("2025-01-03", 0.0410),
        ("2025-02-03", 0.0420),
    ],
    "ipca": [
        ("2025-01-01", 0.16),
        ("2025-02-01", 1.31),
    ],
    "dollar": [
        ("2025-01-02", 6.16),
        ("2025-01-03", 6.12),
        ("2025-02-03", 5.84),
    ],
    "pib": [
        ("2025-01-01", 1_000.0),
        ("2025-02-01", 1_010.0),
    ],
    "inadimplencia": [
        ("2025-01-01", 3.1),
        ("2025-02-01", 3.2),
    ],
}


def _dataframe(rows: list[tuple[str, float]]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "data": [date.fromisoformat(raw_date) for raw_date, _ in rows],
            "valor": [value for _, value in rows],
        }
    )


def seed_and_process() -> None:
    for series_name, rows in FIXTURES.items():
        written = writer_bronze(_dataframe(rows), series_name)
        if written != len(rows):
            raise AssertionError(
                f"Bronze {series_name}: esperado={len(rows)}, obtido={written}"
            )

    for series_name, processor_factory in PROCESSORS.items():
        processed = processor_factory().run().count()
        if processed < len(FIXTURES[series_name]):
            raise AssertionError(
                f"Silver {series_name}: esperado>={len(FIXTURES[series_name])}, "
                f"obtido={processed}"
            )

    get_spark("e2e-seed").stop()


def validate_gold() -> None:
    spark = get_spark("e2e-validate-gold")
    try:
        table = "local.gold.indicadores_macroeconomicos"
        rows = spark.table(table)
        if rows.count() < 2:
            raise AssertionError(f"{table} deveria conter ao menos dois meses")

        january = rows.where("mes_ref = DATE '2025-01-01'").collect()
        if len(january) != 1:
            raise AssertionError("Gold deveria conter exatamente janeiro de 2025")

        result = january[0].asDict()
        required = (
            "selic_media_mes",
            "ipca_acumulado_mes",
            "dollar_medio_mes",
            "pib_valor_mes",
            "inadimplencia_media_mes",
            "juros_real_estimado",
        )
        missing = [column for column in required if result[column] is None]
        if missing:
            raise AssertionError(f"Gold contém valores nulos em: {missing}")

        print("E2E Bronze -> Silver -> Gold concluído com sucesso.")
    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("seed", "validate"))
    args = parser.parse_args()

    if args.action == "seed":
        seed_and_process()
    else:
        validate_gold()


if __name__ == "__main__":
    main()
