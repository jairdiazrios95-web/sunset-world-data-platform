# Phase 2 checkpoint — verified vs unverified

## Implementation
- `warehouse/loader.py`: parameter-derived CSV headers and natural keys;
  guarded SQL identifiers; bounded `COPY` per batch; transactional replay-safe
  upsert into raw tables; CSV malformed-row and in-batch duplicate-key checks.
- `tests/test_warehouse_loader.py`: five local tests with no database needed.
- `README.md`: usage and limitations.

## Verified in this environment
- `python -m pytest -q tests`: **10 passed** (includes five existing tests).

## NOT verified
- `pyarrow` execution and row reconciliation of Parquet datasets.
- PostgreSQL COPY/UPSERT against a running database; transaction behavior on
  real server and repeated loading.
- Container startup, Airflow DAG execution, large-scale volumes, actual Oracle,
  Kafka/Spark and RAG integration.

## Decision log
The user previously chose modular Compose profiles (option A); no new
architectural decision was necessary for this checkpoint. The existing V0.1
archive is preserved unchanged, and V0.2 is an additive consolidated snapshot.
