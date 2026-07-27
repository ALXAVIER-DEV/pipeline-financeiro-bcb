#!/usr/bin/env bash
set -euo pipefail

DAYS="${1:-30}"

if ! [[ "${DAYS}" =~ ^[1-9][0-9]*$ ]]; then
  echo "Uso: $0 [quantidade-de-dias]" >&2
  echo "A quantidade de dias deve ser um número inteiro maior que zero." >&2
  exit 2
fi

echo "Iniciando ingestão Bronze para os últimos ${DAYS} dias..."
python -m src.ingestion.ingest_bronze "${DAYS}"

echo "Iniciando processamento Silver..."
python -m src.transformation.process_silver

echo "Pipeline Bronze → Silver concluído com sucesso."
