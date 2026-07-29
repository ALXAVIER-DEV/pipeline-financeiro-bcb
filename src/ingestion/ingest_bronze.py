import sys
from datetime import datetime, timedelta

from loguru import logger

from src.ingestion.bcb_client import SERIES, fetch_bcb_data
from src.ingestion.bronze_writer import writer_bronze


def ingest_all(dias: int = 30):
    hoje = datetime.today()
    inicio = (hoje - timedelta(days=dias)).strftime("%d/%m/%Y")
    fim = hoje.strftime("%d/%m/%Y")

    logger.info(f"Ingestão: {inicio} → {fim}")

    results = {}
    for nome, codigo in SERIES.items():
        try:
            df = fetch_bcb_data(codigo, inicio, fim)
            if df.empty:
                logger.warning(f"Sem dados para {nome} no período; pulando gravação.")
                results[nome] = {"status": "sem_dados", "rows": 0}
                continue
            count = writer_bronze(df, nome)
            results[nome] = {"status": "ok", "rows": count}
        except Exception as e:
            logger.error(f"Falha em {nome}: {e}")
            results[nome] = {"status": "erro", "mensagem": str(e)}

    return results


if __name__ == "__main__":
    dias = int(sys.argv[1]) if len(sys.argv) > 1 else 30
    resultado = ingest_all(dias)

    print("\n=== Resultado ===")
    for serie, info in resultado.items():
        status = "OK" if info["status"] in ("ok", "sem_dados") else "ERRO"
        print(f"{status} {serie}: {info}")

    if any(info["status"] == "erro" for info in resultado.values()):
        sys.exit(1)
