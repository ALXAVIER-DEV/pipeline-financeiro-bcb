#!/usr/bin/env bash
set -euo pipefail

docker compose config
docker compose up --build -d
docker compose ps