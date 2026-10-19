"""Streaming CSV -> partitioned Parquet. Source-level publish, idempotent retries.

All source columns are stored as UTF-8 strings in the BRONZE tier. Typed casts
and constraints are enforced downstream; this avoids schema inference drift
across CSV chunks. In a real object store, use a manifest pointer or Iceberg
commit instead of POSIX directory renames.
"""
from __future__ import annotations
import argparse
import csv
import json
import os
import shutil
from collections import defaultdict
from datetime import date
from pathlib import Path
from uuid import uuid4


def period_for(value: str) -> tuple[str, str]:
    try:
        d = date.fromisoformat(value[:10])
    except (ValueError, TypeError) as exc:
        raise ValueError(f'Invalid ISO date for partition: {value!r}') from exc
    return str(d.year), f'{d.month:02d}'


def csv_batches(path: Path, batch_size: int):
    if batch_size < 1:
        raise ValueError('batch_size must be > 0')
    with path.open(newline='', encoding='utf-8') as fp:
        reader = csv.DictReader(fp)
        columns = reader.fieldnames
        if not columns or len(set(columns)) != len(columns):
            raise ValueError(f'Invalid or duplicated CSV header: {path}')
        chunk = []
        for row in reader:
            if None in row or any(v is None for v in row.values()):
                raise ValueError(f'Malformed CSV row: {path}')
            chunk.append(row)
            if len(chunk) >= batch_size:
                yield columns, chunk
                chunk = []
        if chunk:
            yield columns, chunk
        elif not path.stat().st_size:
            raise ValueError(f'Empty CSV: {path}')


def convert(input_dir: Path, output_dir: Path, config_path: Path, source: str | None = None) -> dict:
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise RuntimeError('pyarrow required for actual Parquet: pip install -r requirements.txt') from exc
    config = json.loads(Path(config_path).read_text(encoding='utf-8'))
    sources = config['sources']
    if source is not None and source not in sources:
        raise ValueError(f'Undeclared source: {source}')
    output_dir.mkdir(parents=True, exist_ok=True)
    result = {}
    settings = config['parquet']
    max_rows = int(settings['max_rows_per_file'])
    if max_rows <= 0:
        raise ValueError('max_rows_per_file must be positive')
    for name in ([source] if source else sources):
        spec = sources[name]
        csv_path = input_dir / f'{name}.csv'
        if not csv_path.is_file():
            raise FileNotFoundError(csv_path)
        base = output_dir / spec['target']
        stage = output_dir / f'.{spec["target"]}.{uuid4().hex}.tmp'
        backup = output_dir / f'.{spec["target"]}.{uuid4().hex}.backup'
        writers = {}
        totals = defaultdict(int)
        part_counts = defaultdict(int)
        total_rows = 0
        schema = None
        try:
            for columns, chunk in csv_batches(csv_path, int(settings['batch_size'])):
                if schema is None:
                    if not set(spec['primary_key'] + spec.get('required', [])).issubset(columns):
                        raise ValueError(f'{name}: missing required columns')
                    if spec['date_column'] and spec['date_column'] not in columns:
                        raise ValueError(f'{name}: missing date column')
                    schema = pa.schema([pa.field(column, pa.string()) for column in columns])
                groups = defaultdict(list)
                for row in chunk:
                    if any(not (row[key] or '').strip() for key in spec['primary_key']):
                        raise ValueError(f'{name}: missing natural key')
                    if any(not (row[key] or '').strip() for key in spec.get('required', [])):
                        raise ValueError(f'{name}: missing required value')
                    the_date = row[spec['date_column']] if spec['date_column'] else spec['snapshot_date']
                    groups[period_for(the_date)].append(row)
                for (year, month), values in groups.items():
                    period = (year, month)
                    # Rotate files when the configured row limit is reached.
                    for start in range(0, len(values), max_rows):
                        piece = values[start:start + max_rows]
                        if period in writers and part_counts[period] + len(piece) > max_rows:
                            writers.pop(period).close()
                            part_counts[period] = 0
                        if period not in writers:
                            folder = stage / f'year={year}' / f'month={month}'
                            folder.mkdir(parents=True, exist_ok=True)
                            filename = folder / f'part-{totals[period]:06d}.parquet'
                            writers[period] = pq.ParquetWriter(filename, schema, compression=settings['compression'])
                            totals[period] += 1
                        table = pa.Table.from_pylist(piece, schema=schema)
                        writers[period].write_table(table)
                        part_counts[period] += len(piece)
                        total_rows += len(piece)
            for writer in writers.values():
                writer.close()
            writers.clear()
            files = list(stage.rglob('*.parquet'))
            reconciled = sum(pq.read_metadata(p).num_rows for p in files)
            if reconciled != total_rows or not files:
                raise AssertionError(f'{name}: Parquet reconcile {reconciled} != {total_rows}')
            # Prepare manifest before commit; never delete last good version first.
            manifest = {'source':name, 'rows':reconciled, 'files':len(files),
                        'partitions':len(totals),'schema_columns':schema.names,
                        'mode':'source_snapshot_replace','format':'Parquet'}
            (stage / '_manifest.json').write_text(json.dumps(manifest, indent=2)+'\n')
            if base.exists():
                base.rename(backup)
            try:
                stage.rename(base)
            except Exception:
                if backup.exists() and not base.exists():
                    backup.rename(base)
                raise
            if backup.exists():
                shutil.rmtree(backup)
            result[name] = manifest
        except Exception:
            for writer in writers.values():
                writer.close()
            if stage.exists():
                shutil.rmtree(stage)
            raise
    return result


def main():
    p=argparse.ArgumentParser()
    p.add_argument('--input',type=Path,default=Path('data/sample'))
    p.add_argument('--output',type=Path,default=Path('data/parquet'))
    p.add_argument('--config',type=Path,default=Path('config/pipeline.json'))
    p.add_argument('--source')
    a=p.parse_args()
    print(json.dumps(convert(a.input,a.output,a.config,a.source),indent=2))

if __name__=='__main__':main()
