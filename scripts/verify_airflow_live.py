"""Fail-closed evidence wrapper for an actually executed Airflow 2 DAG.

The source quality report alone proves only a CSV check. This script also
requires a successful dbt test artifact and a real PostgreSQL connection
with persisted raw/warehouse data and ingestion audit entries.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path


def verify(quality_path: Path, dbt_path: Path, report_path: Path) -> dict:
    quality = json.loads(quality_path.read_text(encoding='utf-8'))
    tests = json.loads(dbt_path.read_text(encoding='utf-8'))
    if quality.get('status') != 'PASS':
        raise AssertionError('Airflow quality task returned a non-PASS result')
    results = tests.get('results', [])
    if not results:
        raise AssertionError('Missing actual dbt test executions')
    failed = [r for r in results if r.get('status') not in ('pass', 'success')]
    if failed:
        raise AssertionError(f'dbt test failures: {len(failed)}')
    import psycopg
    dsn = os.environ['SUNSET_PG_DSN']
    with psycopg.connect(dsn) as connection:
        with connection.cursor() as cursor:
            counts = {}
            for relation in ('raw.leads_ventas', 'raw.fx_historico',
                             'dw.fact_ventas', 'dw.fact_tours',
                             'dw.fact_fx', 'dw.fact_interacciones'):
                cursor.execute(f'SELECT count(*) FROM {relation}')
                counts[relation] = cursor.fetchone()[0]
                if counts[relation] < 1:
                    raise AssertionError(f'No data in real PostgreSQL relation {relation}')
            cursor.execute("SELECT count(DISTINCT source_name) FROM ops.batch_audit WHERE status='PASS'")
            sources_audited = cursor.fetchone()[0]
            if sources_audited < 10:
                raise AssertionError(f'Only {sources_audited}/10 raw sources audited')
            cursor.execute('''SELECT count(*) FROM dw.fact_ventas f
              LEFT JOIN dw.dim_cliente d ON f.cliente_key=d.cliente_key
              WHERE d.cliente_key IS NULL''')
            orphan_sales = cursor.fetchone()[0]
            if orphan_sales:
                raise AssertionError(f'{orphan_sales} orphan sales')
    proof = {
        'status': 'PASS', 'simulation': False,
        'engine': 'Apache Airflow 2 TaskFlow + dbt Core + PostgreSQL',
        'quality_status': quality['status'],
        'dbt_test_count': len(results),
        'source_count': sources_audited,
        'relation_rows': counts,
        'orphan_sales': orphan_sales,
    }
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(proof, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')
    return proof


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument('--quality', type=Path, required=True)
    p.add_argument('--dbt-results', type=Path, required=True)
    p.add_argument('--report', type=Path, required=True)
    args = p.parse_args()
    print(json.dumps(verify(args.quality, args.dbt_results, args.report), indent=2))


if __name__ == '__main__':
    main()
