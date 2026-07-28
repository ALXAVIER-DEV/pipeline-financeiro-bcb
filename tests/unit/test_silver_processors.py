from datetime import date, datetime
from unittest.mock import MagicMock, patch

from pyspark.sql import DataFrame

from src.transformation.base_processor import BaseProcessor
from src.transformation.processors.dollar_processor import DollarProcessor
from src.transformation.processors.ipca_processor import IpcaProcessor
from src.transformation.processors.monthly_indicator_processor import (
    PibProcessor,
)
from src.transformation.processors.selic_processor import SelicProcessor


def _dataframe(spark, values: list[tuple[date, float | None]]) -> DataFrame:
    return spark.createDataFrame(values, ["data", "valor"])


def test_selic_processor_aplica_transformacoes_reais(spark):
    source = _dataframe(
        spark,
        [
            (date(2024, 1, 2), 5.0),
            (date(2024, 1, 3), 8.0),
            (date(2024, 1, 4), 12.0),
            (date(2024, 1, 5), 15.0),
            (date(2024, 1, 6), None),
        ],
    )

    processor = SelicProcessor.__new__(SelicProcessor)
    result = processor._transform(source)
    rows = result.select("valor", "variacao_pp", "nivel_taxa").collect()

    assert result.count() == 4
    assert [row.nivel_taxa for row in rows] == [
        "baixo",
        "moderado",
        "alto",
        "muito_alto",
    ]
    assert rows[0].variacao_pp is None
    assert rows[1].variacao_pp == 3.0
    assert "media_movel_30d" in result.columns
    assert "volatilidade_30d" in result.columns


def test_ipca_processor_calcula_acumulados_e_pressao(spark):
    source = _dataframe(
        spark,
        [
            (date(2024, 1, 1), -0.1),
            (date(2024, 2, 1), 0.2),
            (date(2024, 3, 1), 0.4),
            (date(2024, 4, 1), 0.8),
        ],
    )

    processor = IpcaProcessor.__new__(IpcaProcessor)
    rows = processor._transform(source).select(
        "pressao", "acumulado_12m", "acumulado_ano"
    ).collect()

    assert [row.pressao for row in rows] == [
        "deflacao",
        "controlada",
        "moderada",
        "elevada",
    ]
    assert rows[-1].acumulado_12m == 1.3
    assert rows[-1].acumulado_ano == 1.3


def test_dollar_processor_calcula_tendencia(spark):
    source = _dataframe(
        spark,
        [
            (date(2024, 1, 1), 5.0),
            (date(2024, 1, 2), 5.1),
            (date(2024, 1, 3), 5.0),
        ],
    )

    processor = DollarProcessor.__new__(DollarProcessor)
    rows = processor._transform(source).select(
        "variacao_abs", "variacao_pct", "tendencia"
    ).collect()

    assert rows[0].variacao_abs is None
    assert rows[0].tendencia == "estavel"
    assert rows[1].tendencia == "alta"
    assert rows[2].tendencia == "queda"
    assert rows[1].variacao_pct == 2.0


def test_monthly_indicator_mantem_data_e_valor(spark):
    source = spark.createDataFrame(
        [(date(2024, 1, 1), 100.0, "metadado")],
        ["data", "valor", "_source"],
    )
    processor = PibProcessor.__new__(PibProcessor)

    result = processor._transform(source)

    assert result.columns == ["data", "valor"]
    assert result.first().valor == 100.0


class _TestProcessor(BaseProcessor):
    def __init__(self, bronze: MagicMock, silver: MagicMock):
        self.serie_name = "test"
        self._bronze = bronze
        self._silver = silver
        self.created = False

    def _read_bronze(self):
        return self._bronze

    def _transform(self, df):
        assert df is self._bronze
        return self._silver

    def _create_table(self):
        self.created = True


class _ReadProcessor(BaseProcessor):
    def _transform(self, df):
        return df

    def _create_table(self):
        pass


def test_base_processor_run_orquestra_as_etapas():
    bronze = MagicMock()
    silver = MagicMock()
    bronze.count.return_value = 3
    silver.count.return_value = 3
    processor = _TestProcessor(bronze, silver)

    with patch.object(processor, "_write") as write:
        result = processor.run()

    assert result is silver
    assert processor.created is True
    write.assert_called_once_with(silver)


def test_read_bronze_mantem_ingestao_mais_recente_por_data(spark):
    source = spark.createDataFrame(
        [
            (date(2024, 1, 1), 10.0, datetime(2024, 1, 2, 10)),
            (date(2024, 1, 1), 11.0, datetime(2024, 1, 2, 11)),
            (date(2024, 1, 2), 12.0, datetime(2024, 1, 2, 11)),
        ],
        ["data", "valor", "_ingested_at"],
    )
    processor = _ReadProcessor.__new__(_ReadProcessor)
    processor.serie_name = "selic"
    processor.spark = MagicMock()
    processor.spark.table.return_value = source

    rows = processor._read_bronze().orderBy("data").collect()

    assert [(row.data, row.valor) for row in rows] == [
        (date(2024, 1, 1), 11.0),
        (date(2024, 1, 2), 12.0),
    ]
