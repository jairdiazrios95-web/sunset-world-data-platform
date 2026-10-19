import csv
import pytest
from warehouse.loader import identifier, row_batches, source_contract, statements, load_csv
from sunset.config import load


def test_identifiers_guard_against_sql_injection():
    assert identifier('lead_id') == '"lead_id"'
    for invalid in ['drop table', 'x;drop', '"quoted"', 'A', '', 'a-b']:
        with pytest.raises(ValueError):
            identifier(invalid)


def test_contract_rejects_missing_pk_and_unknown_source(tmp_path):
    path = tmp_path / 'sample.csv'
    path.write_text('sale_id,cliente_id\nV1,C1\n')
    cfg = load('config/pipeline.json')
    with pytest.raises(ValueError, match='Missing primary-key'):
        source_contract(cfg, 'leads_ventas', path)
    with pytest.raises(ValueError, match='Undeclared'):
        source_contract(cfg, 'evil', path)


def test_batched_csv_reconciles_and_preserves_values(tmp_path):
    path = tmp_path / 'rows.csv'
    with path.open('w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['lead_id','notes'])
        writer.writerows([(f'L{i}', 'hello, world') for i in range(7)])
    chunks = list(row_batches(path, ['lead_id','notes'], 3))
    assert list(map(len, chunks)) == [3,3,1]
    assert chunks[0][0] == ('L0','hello, world')


def test_statement_upsert_is_whitelisted_and_keyed():
    q = statements('leads_ventas', ['lead_id','cliente_id'], ['lead_id'])
    assert 'ON CONFLICT ("lead_id") DO UPDATE' in q['merge']
    assert 'EXCLUDED."cliente_id"' in q['merge']
    assert 'CREATE TEMP TABLE' in q['temp']


def test_missing_credentials_fail_before_db():
    with pytest.raises(ValueError, match='SUNSET_PG_DSN'):
        load_csv(__import__('pathlib').Path('data/sample/leads_ventas.csv'), 'leads_ventas',
                 load('config/pipeline.json'), '')
