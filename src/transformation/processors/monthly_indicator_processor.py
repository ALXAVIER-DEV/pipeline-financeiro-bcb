from pyspark.sql import DataFrame

from src.transformation.base_processor import BaseProcessor


class MonthlyIndicatorProcessor(BaseProcessor):
    """Normaliza séries mensais que não exigem indicadores derivados."""

    def _transform(self, df: DataFrame) -> DataFrame:
        return df.select("data", "valor")

    def _create_table(self) -> None:
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS local.silver.{self.serie_name} (
                data DATE,
                valor DOUBLE
            )
            USING iceberg
        """)


class PibProcessor(MonthlyIndicatorProcessor):
    def __init__(self) -> None:
        super().__init__("pib")


class InadimplenciaProcessor(MonthlyIndicatorProcessor):
    def __init__(self) -> None:
        super().__init__("inadimplencia")
