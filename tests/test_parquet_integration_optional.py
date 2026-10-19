"""Real PyArrow integration tests. Skipped locally if PyArrow isn't installed.

Run in a CI runner which installs pyarrow from requirements.txt.
"""
from pathlib import Path
import csv
import json
import pytest

pa=pytest.importorskip('pyarrow',reason='Real Parquet tests require PyArrow')
pq=pytest.importorskip('pyarrow.parquet')


def test_parquet_original_sources_and_idempotent_snapshot(tmp_path:Path):
    from sunset.parquet import convert
    root=Path(__file__).parents[1]
    cfg=root/'config/pipeline.json'
    samples=root/'data/sample'
    out=tmp_path/'lake'
    for name in ('leads_ventas','fx_historico'):
        original=sum(1 for _ in csv.DictReader((samples/f'{name}.csv').open(encoding='utf-8',newline='')))
        result=convert(samples,out,cfg,name)
        key=json.loads(cfg.read_text())['sources'][name]['target']
        manifest=result[name]
        assert manifest['format']=='Parquet'
        assert manifest['rows']==original
        actual=sum(pq.read_metadata(p).num_rows for p in (out/key).rglob('*.parquet'))
        assert actual==original
        assert all('year=' in str(p) and 'month=' in str(p) for p in (out/key).rglob('*.parquet'))
        assert all(field.type == pa.string() for field in pq.read_schema(next((out/key).rglob('*.parquet'))))
        second=convert(samples,out,cfg,name)
        assert second[name]['rows']==original
        assert sum(pq.read_metadata(p).num_rows for p in (out/key).rglob('*.parquet'))==original
