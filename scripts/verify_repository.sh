#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
echo '1/4 Python syntax'; python -m compileall -q sunset warehouse oracle streaming rag airflow/dags
echo '2/4 Pytest'; python -m pytest -q tests
echo '3/4 Source quality'; python -m sunset.quality --input data/sample --scope full --report reports/quality_local.json
echo '4/4 SQLite join reconciliation'; python -m sunset.offline_demo --input data/sample --report reports/offline_demo_verified.json
printf '\nNOTE: Parquet, Oracle, PostgreSQL, Docker, Airflow, Kafka, Spark and Mantis require external runtime validation.\n'
