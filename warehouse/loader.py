"""Safe, repeatable CSV -> PostgreSQL raw landing ingestion.

This module intentionally does not claim to be the dimensional ETL: raw landing
is reconciled first, then downstream SQL/dbt materializes the star schema.
"""
from __future__ import annotations
import argparse
import csv
import json
import os
import re
import time
from uuid import uuid4
from pathlib import Path
from typing import Iterable

IDENT = re.compile(r"^[a-z][a-z0-9_]*$")


def identifier(value: str) -> str:
    if not IDENT.fullmatch(value):
        raise ValueError(f"Unsafe SQL identifier: {value!r}")
    return '"' + value + '"'


def source_contract(config: dict, source: str, csv_path: Path) -> tuple[list[str], list[str]]:
    if source not in config['sources']:
        raise ValueError(f"Undeclared source: {source}")
    with csv_path.open(newline='', encoding='utf-8') as stream:
        columns = csv.DictReader(stream).fieldnames or []
    if not columns or len(columns) != len(set(columns)):
        raise ValueError(f"Missing/duplicate columns: {csv_path}")
    for column in columns:
        identifier(column)
    keys = config['sources'][source]['primary_key']
    if not keys or not set(keys).issubset(columns):
        raise ValueError(f"Missing primary-key columns in {csv_path}: {keys}")
    for column in keys:
        identifier(column)
    return columns, keys


def statements(source: str, columns: list[str], keys: list[str]) -> dict[str, str]:
    table = identifier(source)
    fields = ','.join(map(identifier, columns))
    keys_sql = ','.join(map(identifier, keys))
    defs = ','.join(f'{identifier(c)} TEXT NOT NULL' if c in keys else f'{identifier(c)} TEXT' for c in columns)
    staging = f'"stage_{source}"'
    changed = [c for c in columns if c not in keys]
    update = ', '.join(f'{identifier(c)}=EXCLUDED.{identifier(c)}' for c in changed)
    on_conflict = f'DO UPDATE SET {update}' if changed else 'DO NOTHING'
    return {
        'create_schema': 'CREATE SCHEMA IF NOT EXISTS raw',
        'create': f'CREATE TABLE IF NOT EXISTS raw.{table} ({defs}, PRIMARY KEY({keys_sql}))',
        'temp': f'CREATE TEMP TABLE {staging} (LIKE raw.{table} INCLUDING DEFAULTS) ON COMMIT DROP',
        'copy': f'COPY {staging} ({fields}) FROM STDIN',
        'merge': (f'INSERT INTO raw.{table} ({fields}) SELECT {fields} FROM {staging} '
                  f'WHERE TRUE ON CONFLICT ({keys_sql}) {on_conflict}'),
        'count': f'SELECT COUNT(*) FROM {staging}',
        'duplicate_keys': f'SELECT COUNT(*) FROM (SELECT {keys_sql} FROM {staging} GROUP BY {keys_sql} HAVING COUNT(*) > 1) dup',
    }


def row_batches(path: Path, columns: list[str], batch_size: int) -> Iterable[list[tuple]]:
    if batch_size < 1:
        raise ValueError('batch_size must be positive')
    with path.open(newline='', encoding='utf-8') as stream:
        reader = csv.DictReader(stream)
        if reader.fieldnames != columns:
            raise ValueError('Input header has changed during execution')
        batch = []
        for row in reader:
            if None in row or any(not row.get(k) for k in columns if k):
                # Missing nullable values are represented by empty strings in the source.
                if None in row:
                    raise ValueError('Malformed CSV row with excess columns')
            batch.append(tuple(row[c] for c in columns))
            if len(batch) >= batch_size:
                yield batch
                batch = []
        if batch:
            yield batch


def load_csv(input_file: Path, source: str, config: dict, dsn: str, batch_size: int = 5000) -> dict:
    """Batch-staged UPSERT. Transactions are per batch; replay is idempotent.

    Note: per-batch commits permit partial progress if subsequent batches fail.
    Re-running the source from the beginning reconciles the full snapshot.
    """
    if not dsn:
        raise ValueError('SUNSET_PG_DSN must be supplied as an environment secret')
    if batch_size <= 0:
        raise ValueError('batch_size must be positive')
    try:
        import psycopg
    except ImportError as exc:
        raise RuntimeError('Install psycopg[binary] from requirements.txt') from exc
    columns, keys = source_contract(config, source, input_file)
    sql = statements(source, columns, keys)
    started = time.monotonic()
    ingested = 0
    batches = 0
    # Avoid logging DSN, which may contain a password.
    with psycopg.connect(dsn) as con:
        with con.cursor() as cur:
            cur.execute(sql['create_schema'])
            cur.execute(sql['create'])
            con.commit()
        for chunk in row_batches(input_file, columns, batch_size):
            with con.transaction():
                with con.cursor() as cur:
                    cur.execute(sql['temp'])
                    with cur.copy(sql['copy']) as cp:
                        for record in chunk:
                            cp.write_row(record)
                    cur.execute(sql['count'])
                    staged = cur.fetchone()[0]
                    if staged != len(chunk):
                        raise AssertionError(f'COPY count mismatch: {staged} vs {len(chunk)}')
                    cur.execute(sql['duplicate_keys'])
                    duplicates = cur.fetchone()[0]
                    if duplicates:
                        raise ValueError(f'Duplicate natural keys in CSV batch for {source}: {duplicates}')
                    cur.execute(sql['merge'])
                    cur.execute(f'DROP TABLE "stage_{source}"')
            ingested += len(chunk)
            batches += 1
    return {'source': source, 'rows_read': ingested, 'batches': batches,
            'load_type': 'replay_safe_upsert', 'elapsed_seconds': round(time.monotonic()-started,3),
            'result': 'LOAD_COMPLETED_NOT_FULLY_RECONCILED'}


def main():
    from sunset.config import load
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', default='config/pipeline.json')
    parser.add_argument('--source', required=True)
    parser.add_argument('--input-dir', default='data/sample')
    parser.add_argument('--batch-size', type=int, default=5000)
    args = parser.parse_args()
    cfg = load(args.config)
    result = load_csv(Path(args.input_dir)/f'{args.source}.csv', args.source, cfg,
                      os.environ.get('SUNSET_PG_DSN', ''), args.batch_size)
    print(json.dumps(result, indent=2))

if __name__ == '__main__':
    main()


def load_parquet(input_root: Path, source: str, config: dict, dsn: str,
                 batch_size: int = 5000) -> dict:
    """Load BRONZE Parquet partitions -> PostgreSQL raw, bounded memory.

    The manifest is committed only after all row groups are complete. This
    snapshot UPSERT intentionally does not delete previously loaded keys.
    """
    if not dsn:raise ValueError('SUNSET_PG_DSN must be supplied as an environment secret')
    if source not in config['sources']:raise ValueError(f'Undeclared source: {source}')
    if batch_size<=0:raise ValueError('batch_size must be positive')
    try:
        import psycopg
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise RuntimeError('Install psycopg and pyarrow from requirements.txt') from exc
    folder = input_root/config['sources'][source]['target']
    manifest_file=folder/'_manifest.json'
    if not manifest_file.exists():raise FileNotFoundError(manifest_file)
    manifest=json.loads(manifest_file.read_text(encoding='utf-8'))
    if manifest['source']!=source:raise ValueError('Manifest/source mismatch')
    columns=manifest['schema_columns']
    keys=config['sources'][source]['primary_key']
    if not set(keys).issubset(columns):raise ValueError('Missing natural keys from manifest')
    statements_sql=statements(source,columns,keys)
    files=sorted(folder.rglob('*.parquet'))
    if len(files)!=manifest['files']:raise AssertionError('Parquet file count mismatch')
    started=time.monotonic()
    loaded=0
    with psycopg.connect(dsn) as con:
        with con.cursor() as cur:
            cur.execute(statements_sql['create_schema'])
            cur.execute(statements_sql['create'])
            con.commit()
        for f in files:
            pf=pq.ParquetFile(f)
            if pf.schema_arrow.names!=columns:
                raise ValueError(f'Parquet schema drift: {f}')
            for batch in pf.iter_batches(batch_size=batch_size):
                records=batch.to_pylist()
                with con.transaction():
                    with con.cursor() as cur:
                        cur.execute(statements_sql['temp'])
                        with cur.copy(statements_sql['copy']) as cp:
                            for row in records:
                                if any(not row[k] for k in keys):raise ValueError('Null natural key')
                                cp.write_row(tuple(row[c] for c in columns))
                        cur.execute(statements_sql['count'])
                        if cur.fetchone()[0]!=len(records):raise AssertionError('COPY mismatch')
                        cur.execute(statements_sql['duplicate_keys'])
                        if cur.fetchone()[0]:raise ValueError('Duplicate natural key inside row group')
                        cur.execute(statements_sql['merge'])
                        cur.execute(f'DROP TABLE "stage_{source}"')
                loaded+=len(records)
        with con.cursor() as cur:
            cur.execute(f'SELECT count(*) FROM raw.{identifier(source)}')
            current_count=cur.fetchone()[0]
    if loaded!=manifest['rows']:
        raise AssertionError(f'Landing reconciliation {loaded} != {manifest["rows"]}')
    if current_count<loaded:
        raise AssertionError(f'Landing unique keys less than source rows: {current_count}<{loaded}')
    # An immutable run record for operational dashboards.
    with psycopg.connect(dsn) as con:
        with con.cursor() as cur:
            cur.execute('''INSERT INTO ops.batch_audit
                 (run_id,source_name,extracted,loaded,rejected,elapsed_seconds,status)
                 VALUES (%s,%s,%s,%s,%s,%s,%s)''',
                 (os.getenv('SUNSET_RUN_ID',str(uuid4())),source,loaded,loaded,0,
                  round(time.monotonic()-started,3),'PASS'))
    return {'source':source,'parquet_rows':loaded,'destination_rows':current_count,
            'elapsed_seconds':round(time.monotonic()-started,3),'mode':'replay_safe_upsert',
            'status':'PASS'}
