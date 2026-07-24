from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd

MOCK_DF = pd.DataFrame({
    "data": [date(2024, 1, 1), date(2024, 1, 2)],
    "valor": [12.25, 12.25]
})

def test_write_bronze_call_spark():
    mock_spark = MagicMock()
    mock_df_spark = MagicMock()
    mock_spark.createDataFrame.return_value = mock_df_spark
    mock_df_spark.withColumn.return_value = mock_df_spark
    mock_df_spark.count.return_value = 2

    with (
        patch(
            "src.ingestion.bronze_writer.get_spark",
            return_value=mock_spark,
        ),
        patch("src.ingestion.bronze_writer.to_date"),
        patch("src.ingestion.bronze_writer.current_timestamp"),
        patch("src.ingestion.bronze_writer.lit"),
        patch("src.ingestion.bronze_writer.year"),
    ):
        from src.ingestion.bronze_writer import writer_bronze

        count = writer_bronze(MOCK_DF, "selic")
    assert count == 2
    mock_spark.sql.assert_called()
    mock_df_spark.writeTo.assert_called_once_with("local.bronze.selic")
    mock_df_spark.writeTo.return_value.append.assert_called_once()

