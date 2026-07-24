import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from datetime import date

MOCK_DF = pd.DataFrame({
    "data": [date(2024, 1, 1), date(2024, 1, 2)],
    "valor": [12.25, 12.25]
})

def test_write_bronze_call_spark(monkeypath):
    mock_spark = MagicMock()
    mock_df_spark = MagicMock()
    mock_spark.createDataFrame.return_value = mock_df_spark
    mock_df_spark.withColumn.return_value = mock_df_spark
    mock_df_spark.count.return_value = 2

    with patch("src.ingestion.bronze_writer.get_spark", return_value=mock_spark):
        from src.ingestion.bronze_writer import writer_bronze
        count = writer_bronze(MOCK_DF, "selic")
    assert count == 2
    mock_spark.sql.assert_called()

# )
# import json
# from datetime import datetime, timezone

# from src.ingestion.bronze_writer import build_control_record, write_control_record


# def test_build_control_record_includes_summary_and_metadata():
#     started_at = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
#     finished_at = datetime(2024, 1, 1, 12, 0, 5, tzinfo=timezone.utc)

#     record = build_control_record(
#         table_name="selic_bronze",
#         process_name="write_bronze",
#         source="bcb_api",
#         layer="bronze",
#         row_count=42,
#         started_at=started_at,
#         finished_at=finished_at,
#         status="success",
#         metadata={"partition": "2024-01-01"},
#     )

#     assert record["table_name"] == "selic_bronze"
#     assert record["process_name"] == "write_bronze"
#     assert record["row_count"] == 42
#     assert record["status"] == "success"
#     assert record["duration_seconds"] == 5
#     assert record["metadata"]["partition"] == "2024-01-01"


# def test_write_control_record_persists_jsonl(tmp_path):
#     record = build_control_record(
#         table_name="ipca_bronze",
#         process_name="write_bronze",
#         source="bcb_api",
#         layer="bronze",
#         row_count=7,
#         started_at=datetime.now(timezone.utc),
#         finished_at=datetime.now(timezone.utc),
#         status="success",
#     )

#     output_path = tmp_path / "control_table.jsonl"
#     write_control_record(record, control_table_path=output_path)

#     assert output_path.exists()
#     lines = output_path.read_text(encoding="utf-8").strip().splitlines()
#     saved = json.loads(lines[0])
#     assert saved["table_name"] == "ipca_bronze"
#     assert saved["row_count"] == 7
