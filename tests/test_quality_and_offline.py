import csv
import shutil
from pathlib import Path
import pytest
from sunset.generator import build
from sunset.quality import check, inspect_csv
from sunset.offline_demo import demonstrate
from sunset.config import load

@pytest.fixture
def samples(tmp_path):
    build(tmp_path,80,32,90,seed=77,chunk_size=7)
    return tmp_path

def test_offline_joins(samples):
    result=demonstrate(samples)
    assert result['status']=='PASS'
    assert result['join_statistics']['payments_with_contract']==32
    assert result['join_statistics']['comments_with_tour']==90
    assert result['join_statistics']['sales_with_fx']==result['join_statistics']['sales']

def test_quality_14_checks(samples):
    result=check(samples)
    assert result['checks_evaluated_at_least']>=14
    assert result['status']=='PASS'

def change_field(path,column,value):
    with path.open(encoding='utf-8',newline='') as stream:
        reader=csv.DictReader(stream);cols=reader.fieldnames;rows=list(reader)
    rows[0][column]=value
    with path.open('w',encoding='utf-8',newline='') as stream:
        writer=csv.DictWriter(stream,fieldnames=cols)
        writer.writeheader();writer.writerows(rows)

@pytest.mark.parametrize('source,column,value,issue',[ 
   ('leads_ventas','cliente_id','', 'missing_required'),
   ('leads_ventas','moneda','XYZ','invalid_currency'),
   ('fx_historico','tipo_cambio','-1','invalid_amounts'),
   ('fx_historico','fecha','2025-13-99','invalid_dates'),
])
def test_bad_inputs_fail(samples,source,column,value,issue):
    change_field(samples/f'{source}.csv',column,value)
    result=check(samples,scope='core')
    assert result['status']=='FAIL'
    assert any(issue in err for err in result['errors'])

def test_orphan_blocks_full_scope(samples):
    change_field(samples/'pagos.csv','contrato_id','K-NOT-FOUND')
    result=check(samples,scope='full')
    assert result['status']=='FAIL'
    assert result['cross_system_audit']['orphans']['pagos.contrato_id->contratos.contrato_id']==1

def test_unique_key_duplicate_detected(samples):
    p=samples/'fx_historico.csv'
    with p.open(encoding='utf-8') as f:
        header=f.readline();first=f.readline()
    with p.open('a',encoding='utf-8') as f:f.write(first)
    result=check(samples,scope='core')
    assert result['status']=='FAIL'
    assert any('duplicate_keys' in e for e in result['errors'])

def test_generator_reproducible_seed(tmp_path):
    left=tmp_path/'left';right=tmp_path/'right'
    kw=dict(start_date='2024-01-01',end_date='2024-04-30',seed=5,chunk_size=8)
    build(left,110,70,150,**kw);build(right,110,70,150,**kw)
    for name in ['leads_ventas','fx_historico','comentarios']:
        assert (left/f'{name}.csv').read_bytes()==(right/f'{name}.csv').read_bytes()
    assert check(left)['status']=='PASS'

def test_bad_generation_window(tmp_path):
    with pytest.raises(ValueError):build(tmp_path,10,2,3,start_date='2025-01-01',end_date='2024-01-01')
