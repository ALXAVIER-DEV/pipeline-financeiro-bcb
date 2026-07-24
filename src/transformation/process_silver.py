from loguru import logger
from src.transformation.processors.selic_processor import SelicProcessor
from src.transformation.processors.ipca_processor import IpcaProcessor
from src.transformation.processors.dollar_processor import DollarProcessor
import sys

PROCESSORS = {
    "selic": SelicProcessor,
    "dollar": DollarProcessor,
    "ipca": IpcaProcessor,
}

def process_all():
    results = {}
    for name, processor_class in PROCESSORS.items():
        try:
            processor_class().run()
            results[name] = "OK"
        except Exception as e:
            logger.error(f"Falha no processamento da silver.{name}: {e}")
            results[name] = f"erro: {e}"
    return results

if __name__ == "__main__":
    serie = sys.argv[1] if len(sys.argv) > 1 else None

    if serie:
        if serie not in PROCESSORS:
            print(f"Serie desconhecida: {serie}. Opcoes: {list(PROCESSORS.keys())}")
            sys.exit(1)
        PROCESSORS[serie]().run()
    else:
        results = process_all()
        print("\n=== Resultados Silver ===")
        for k, v in results.items():
            status = "OK" if v == "OK" else "ERRO"
            print(f"{status} {k}: {v}")
