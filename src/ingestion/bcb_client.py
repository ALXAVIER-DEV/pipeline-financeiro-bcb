## API pública, sem autenticação, dados reais:

import pandas as pd
import requests
from loguru import logger

BCB_API_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{}/dados?formato=json"

SERIES = {
    "selic": 11,
    "ipca": 433,
    "dollar": 1,
    "pib": 4380,
    "inadimplencia": 21082
}

def fetch_bcb_data(
    codigo: int,
    data_ini: str,
    data_fim: str,
    *,
    raise_on_error: bool = False,
) -> pd.DataFrame:
    """
    Busca dados da API do BCB para um código de série e um intervalo de datas.

    Args:
        codigo (int): O código da série para a qual buscar os dados.
        data_ini (str): A data de início no formato 'DD/MM/AAAA'.
        data_fim (str):  A data de término no formato 'DD/MM/AAAA'.

    Returns:
        pd.DataFrame: Um DataFrame contendo os dados recuperados.
    """
    url = BCB_API_URL.format(codigo)
    params = {
        "formato": "json",
        "dataInicial": data_ini,
        "dataFinal": data_fim
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if not data:
            logger.warning(
                f"No data found for series code {codigo} "
                f"between {data_ini} and {data_fim}."
            )
            return pd.DataFrame()
        
        df = pd.DataFrame(data)
        df['data'] = pd.to_datetime(df['data'], format='%d/%m/%Y')
        df['valor'] = pd.to_numeric(df['valor'], errors='coerce')
        logger.info(f"Fetched {len(df)} rows for series {codigo}")
        
        return df
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data from BCB API: {e}")
        if raise_on_error:
            raise
        return pd.DataFrame()
