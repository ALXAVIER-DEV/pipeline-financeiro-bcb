from unittest.mock import MagicMock, patch

from src.ingestion.bcb_client import fetch_bcb_data


def test_fetch_bcb_data_retorna_dataframe():
    mock_data = [
        {"data": "01/01/2024", "valor": "12.25"},
        {"data": "02/01/2024", "valor": "12.25"},
    ]
    with patch("requests.get") as mock_get:
        mock_get.return_value.json.return_value = mock_data
        mock_get.return_value.raise_for_status = MagicMock()
        df = fetch_bcb_data(11, "01/01/2024", "02/01/2024")

    assert len(df) == 2
    assert "data" in df.columns
    assert "valor" in df.columns
    assert df["valor"].dtype == float

def test_fetch_bcb_data_trata_valor_invalido():
    mock_data = [{"data": "01/01/2024", "valor": "N/D"}]
    with patch("requests.get") as mock_get:
        mock_get.return_value.json.return_value = mock_data
        mock_get.return_value.raise_for_status = MagicMock()
        df = fetch_bcb_data(11, "01/01/2024", "01/01/2024")

    assert df["valor"].isna().all()
