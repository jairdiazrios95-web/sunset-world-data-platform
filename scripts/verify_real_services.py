"""Live PyArrow -> PostgreSQL/pgvector -> dbt acceptance.

Never substitutes an in-memory stub. This script fails closed if dependencies, DB
connection, Parquet files, referential constraints, or reconciliation fail.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import subprocess
import sys
import time
from pathlib import Path


def _cmd(*argv: str) -> None:
    p = subprocess.run(argv, text=True, capture_output=True, check=False)
    if p.returncode:
        raise RuntimeError(f"{' '.join(argv[:5])} failed: {p.stderr[-2000:]}")


def execute(root: Path, report: Path, repeats: int = 2, dbt: bool = False) -> dict:
    if repeats < 2:
        raise ValueError('Must check at least one replay')
    root = root.resolve()
    os.chdir(root)
    from sunset.config import load
    import psycopg
    import pyarrow.parquet as pq
    from sunset.parquet import convert
    from warehouse.loader import load_parquet
    from warehouse.build_marts import build

    config = load('config/pipeline.json')
    dsn = os.getenv('SUNSET_PG_DSN')
    if not dsn:
        raise RuntimeError('SUNSET_PG_DSN required for live testing')
    starts = time.monotonic()
    source_dir = root / 'data' / 'sample'
    lake_dir = root / 'data' / 'parquet_ci'
    sources = list(config['sources'])
    expected = {}
    for src in sources:
        with (source_dir / f'{src}.csv').open(newline='', encoding='utf-8') as f:
            expected[src] = sum(1 for _ in csv.DictReader(f))

    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            # SQL initialization really runs against pgvector-enabled Postgres.
            for path in ('sql/001_init.sql','sql/003_upsert_patterns.sql','sql/005_streaming.sql'):
                cur.execute((root/path).read_text(encoding='utf-8'), prepare=False)
            cur.execute("SELECT extversion FROM pg_extension WHERE extname='vector'")
            vector_version = cur.fetchone()[0]
            if not vector_version:
                raise AssertionError('PostgreSQL pgvector extension absent')
            # Test real pgvector insert and distance search with a controlled fixture.
            # This fixture is explicitly NOT a semantic embedding-model test.
            zero_vector = '[' + ','.join('0.1' if i == 0 else '0' for i in range(384)) + ']'
            cur.execute("""INSERT INTO ai.document_chunk
               (chunk_id,source_id,source_type,content,content_hash,model_id,embedding)
               VALUES (%s,%s,%s,%s,%s,%s,%s::vector)
               ON CONFLICT(chunk_id) DO UPDATE SET embedding=excluded.embedding""",
               ('LIVE-CI-FIXTURE', 'SYNTHETIC-CI', 'integration_fixture',
                'Synthetic pgvector smoke document', hashlib.sha256(b'ci').hexdigest(),
                'fixture-not-a-model', zero_vector))
            cur.execute("""SELECT chunk_id, embedding <=> %s::vector AS cosine_distance
               FROM ai.document_chunk WHERE model_id=%s
               ORDER BY embedding <=> %s::vector LIMIT 1""",
               (zero_vector, 'fixture-not-a-model', zero_vector))
            match=cur.fetchone()
            if not match or match[0]!='LIVE-CI-FIXTURE' or abs(match[1])>1e-7:
                raise AssertionError(f'pgvector retrieval failed: {match}')

    passes = []
    for attempt in range(1, repeats+1):
        results = {}
        for source in sources:
            written = convert(source_dir,lake_dir,root/'config/pipeline.json',source)[source]
            parquet_files = sorted((lake_dir / config['sources'][source]['target']).rglob('*.parquet'))
            physical_rows = sum(pq.read_metadata(f).num_rows for f in parquet_files)
            if physical_rows != expected[source] or written['rows'] != physical_rows:
                raise AssertionError(f'Parquet reconciliation failed for {source}')
            rows = load_parquet(lake_dir,source,config,dsn,5000)
            if rows['parquet_rows'] != expected[source]:
                raise AssertionError(f'PostgreSQL landing mismatch for {source}')
            results[source] = {'csv_rows':expected[source],'parquet_rows':physical_rows,
                               'postgres_rows':rows['destination_rows'],'files':len(parquet_files)}
        marts = build(dsn,root/'sql/004_marts.sql',config.get('warehouse',{}).get('fiscal_year_start_month',1))
        with psycopg.connect(dsn) as conn:
            with conn.cursor() as cur:
                pg_checks = {}
                for source in sources:
                    # source is allowlisted in pipeline.json
                    cur.execute(f'SELECT COUNT(*) FROM raw."{source}"')
                    pg_checks[source] = cur.fetchone()[0]
                    if pg_checks[source] != expected[source]:
                        raise AssertionError(f'Raw reconciliation mismatch: {source}')
                cur.execute("""SELECT COUNT(*) FROM ops.call_center_alert""")
                if cur.fetchone()[0] != 0:
                    raise AssertionError('Expected no alerts before streaming')
                cur.execute("""SELECT COUNT(*) FROM dw.fact_ventas v
                  LEFT JOIN dw.dim_cliente c ON v.cliente_key=c.cliente_key
                  WHERE c.cliente_key IS NULL""")
                orphans = cur.fetchone()[0]
                if orphans:
                    raise AssertionError(f'Fact orphans: {orphans}')
        passes.append({'pass':attempt,'raw':pg_checks,'warehouse':marts,'parquet':results,'orphans':orphans})
        if attempt>1 and passes[-1]['warehouse']['row_counts'] != passes[0]['warehouse']['row_counts']:
            raise AssertionError('Replay changed DW counts')
    if dbt:
        _cmd('dbt','run','--project-dir','dbt','--profiles-dir','dbt')
        _cmd('dbt','test','--project-dir','dbt','--profiles-dir','dbt')
    result={'status':'PASS','engine':'PostgreSQL + pgvector + PyArrow', 'simulation':False,
            'vector_extension':vector_version,'repeats':repeats,'counts':passes[-1],
            'dbt_executed':dbt,'elapsed_seconds':round(time.monotonic()-starts,3)}
    report.parent.mkdir(parents=True,exist_ok=True)
    report.write_text(json.dumps(result,indent=2,default=str)+'\n',encoding='utf-8')
    return result


def main() -> None:
    p=argparse.ArgumentParser()
    p.add_argument('--root',type=Path,default=Path('.'))
    p.add_argument('--report',type=Path,default=Path('reports/live_postgres_parquet_verified.json'))
    p.add_argument('--repeats',type=int,default=2)
    p.add_argument('--dbt',action='store_true')
    a=p.parse_args()
    print(json.dumps(execute(a.root,a.report,a.repeats,a.dbt),indent=2,default=str))

if __name__=='__main__': main()
