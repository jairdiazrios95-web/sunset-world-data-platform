"""Fail-closed GitHub run acceptance for actual service evidence.

This is not a production certification. A result only passes when every live
service verifier has emitted a typed proof with source-specific measurements.
The aggregation workflow downloads immutable artifacts from the SAME run.
Oracle 11g remains external unless an authorized real system is available.
"""
from __future__ import annotations
import argparse
import json
from pathlib import Path

REQUIREMENTS = {
    'PostgreSQL/Parquet/dbt/pgvector': 'live_postgres_parquet_verified.json',
    'MinIO': 'live_minio_verified.json',
    'Kafka/Spark': 'real_streaming_evidence.json',
    'Airflow': 'real_airflow_quality.json',
    'RAG con modelo real': 'real_rag_verified.json',
}


def _positive_int(value):
    return type(value) is int and value > 0


def _credible(component: str, data: dict) -> bool:
    if data.get('status') != 'PASS' or data.get('simulation') is not False:
        return False
    if not isinstance(data.get('engine'), str) or not data['engine'].strip():
        return False
    if component == 'PostgreSQL/Parquet/dbt/pgvector':
        counts = data.get('counts')
        return (data.get('dbt_executed') is True
                and bool(data.get('vector_extension'))
                and isinstance(counts, dict)
                and len(counts.get('raw', {})) >= 10
                and isinstance(counts.get('parquet'), dict))
    if component == 'MinIO':
        return (_positive_int(data.get('files')) and _positive_int(data.get('parquet_rows'))
                and data.get('replay_objects') == data.get('files'))
    if component == 'Kafka/Spark':
        return (data.get('events') == 5 and data.get('alerts') == 2
                and data.get('postgres_rows_after_replay') == 5
                and data.get('postgres_alerts_after_replay') == 2)
    if component == 'Airflow':
        return (_positive_int(data.get('dbt_test_count'))
                and data.get('source_count') == 10
                and isinstance(data.get('relation_rows'), dict)
                and all(_positive_int(v) for v in data['relation_rows'].values())
                and data.get('orphan_sales') == 0)
    if component == 'RAG con modelo real':
        first = data.get('first_run', {})
        replay = data.get('replay', {})
        return (data.get('embedding_dimension') == 384
                and isinstance(first, dict) and first.get('embedded') == 2
                and isinstance(replay, dict) and replay.get('embedded') == 0
                and data.get('retrieved') == 2)
    return False


def acceptance(folder: Path) -> dict:
    status = {}
    for name, filename in REQUIREMENTS.items():
        path = folder / filename
        if not path.is_file():
            status[name] = {'status': 'MISSING', 'file': filename}
            continue
        try:
            data = json.loads(path.read_text(encoding='utf-8'))
            credible = isinstance(data, dict) and _credible(name, data)
        except (json.JSONDecodeError, OSError, TypeError, ValueError):
            credible = False
        status[name] = {'status': 'PASS' if credible else 'FAIL', 'file': filename}
    passed = all(v['status'] == 'PASS' for v in status.values())
    return {
        'status': 'PASS' if passed else 'BLOCKED',
        'technology_proof': status,
        'oracle11g_real_connection': 'NOT_TESTED_NO_VENDOR_ACCESS',
        'scope': 'Actual synthetic CI integrations; no production guarantee',
        'approved_for_production': False,
    }


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument('--evidence', type=Path, default=Path('reports'))
    p.add_argument('--report', type=Path, default=Path('reports/acceptance_summary.json'))
    a = p.parse_args()
    result = acceptance(a.evidence)
    a.report.parent.mkdir(parents=True, exist_ok=True)
    a.report.write_text(json.dumps(result, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')
    print(json.dumps(result, indent=2, ensure_ascii=False))
    raise SystemExit(0 if result['status'] == 'PASS' else 2)


if __name__ == '__main__':
    main()
