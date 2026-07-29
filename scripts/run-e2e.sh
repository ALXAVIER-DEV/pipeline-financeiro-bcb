#!/usr/bin/env bash
set -euo pipefail

echo "Validando sessão Spark e catálogo Iceberg..."
docker compose exec -T pipeline python -c \
  "from src.utils.spark_session import get_spark; s=get_spark('ci-smoke'); assert s.sparkContext.master == 'spark://spark-master:7077'; assert s.range(10).count() == 10; s.sql('CREATE NAMESPACE IF NOT EXISTS local.bronze'); s.sql('CREATE NAMESPACE IF NOT EXISTS local.silver'); s.sql('CREATE NAMESPACE IF NOT EXISTS local.gold'); s.stop()"

echo "Materializando fixtures Bronze e Silver..."
docker compose exec -T pipeline python scripts/e2e_pipeline.py seed

echo "Materializando e testando a camada Gold..."
docker compose run --rm dbt run
docker compose run --rm dbt test

echo "Validando o resultado Gold..."
docker compose exec -T pipeline python scripts/e2e_pipeline.py validate
