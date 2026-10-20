from datetime import datetime,timezone,timedelta
from pathlib import Path
import pytest
from sunset.parquet import period_for,csv_batches
from streaming.rules import detect
from streaming.producer import make_events
from oracle.snapshot_to_lake import make_sql,interval_run_id,oracle_utc_naive
from rag.build_embeddings import redact,chunk_identity,digest,chunks
from warehouse.build_marts import REQUIRED
from sunset.config import load

def test_dates_partition_edges():
    assert period_for('2024-02-29')==('2024','02')
    assert period_for('2025-03-01T03:04:05')==('2025','03')
    for invalid in ('2024-02-30','','2024-20-10'):
        with pytest.raises(ValueError):period_for(invalid)

def test_csv_batches_missing_chunks(tmp_path):
    path=tmp_path/'a.csv'
    path.write_text('id,date\n1,2024-01-01\n2,2024-01-02\n3,2024-02-10\n',encoding='utf-8')
    assert [len(x[1]) for x in csv_batches(path,2)]==[2,1]
    with pytest.raises(ValueError):list(csv_batches(path,0))

def test_kafka_policy_rules():
    assert detect('Le garantizo rentabilidad')==['guaranteed_return']
    assert 'zero_risk' in detect('Esto es sin ningún riesgo')
    assert detect('Le enviamos el contrato')==[]

def test_kafka_sequence_event_identity():
    events=make_events('CALL-A','2026-10-16T00:00:00Z')
    assert len(events)==5
    assert len({e['event_id'] for e in events})==len(events)
    assert all(e['call_id']=='CALL-A' for e in events)
    assert events[0]['event_id']==make_events('CALL-A','2026-10-17T00:00:00Z')[0]['event_id']

def test_oracle_allowlist():
    assert 'LEADS_VENTAS' in make_sql('leads_ventas')
    assert 'FX_HISTORICO' in make_sql('fx_historico')
    with pytest.raises(ValueError):make_sql('x;DROP TABLE')
    with pytest.raises(ValueError):make_sql('leads_ventas','UPDATED_AT; DROP')

def test_oracle_stable_interval_id():
    a=datetime(2025,1,1,tzinfo=timezone.utc);b=datetime(2025,1,2,tzinfo=timezone.utc)
    local_zone = timezone(timedelta(hours=-6))
    assert interval_run_id('fx_historico',a,b)==interval_run_id('fx_historico',a.astimezone(local_zone),b.astimezone(local_zone))
    assert interval_run_id('fx_historico',a,b)!=interval_run_id('leads_ventas',a,b)

def test_rag_redaction_and_change_tracking():
    cleaned=redact('Contactar demo@example.com al 5512345678')
    assert 'demo@example.com' not in cleaned
    assert '5512345678' not in cleaned
    assert digest('a')!=digest('b')
    assert chunk_identity('comment','c1',0,'v1')!=chunk_identity('comment','c1',0,'v2')
    assert chunks('abcdefghij',6,2)==['abcdef','efghij']

def test_dwh_required_sources_declared():
    config=load('config/pipeline.json')
    assert set(REQUIRED)==set(config['sources'])
    assert config['parquet']['batch_size']>0
    assert config['profiles']['volume']['comments']==10_000_000

def test_sql_files_have_dimensional_contract():
    ddl=Path('sql/001_init.sql').read_text()
    etl=Path('sql/004_marts.sql').read_text()
    for n in ['fact_ventas','fact_fx','fact_interacciones','dim_cliente','dim_tiempo','dim_sala','dim_fuente','dim_producto']:
        assert n in ddl
    assert 'valid_to=now()' in etl
    assert 'ON CONFLICT' in etl
    assert 'raise exception' in etl.lower()

def test_orchestration_invokes_real_dbt():
    dag=Path('airflow/dags/etl_sunset.py').read_text()
    assert 'run_dbt' in dag and "'dbt','run'" in dag and "'dbt','test'" in dag
    assert "'extract_leads'" in dag and "'extract_fx'" in dag
    assert 'on_failure_callback' in dag

def test_psi_distribution():
    from sunset.drift import categorical_psi
    assert categorical_psi(['a','a','b'],['a','a','b'])==0
    assert categorical_psi(['a','a','b'],['b','b','a'])>0
    with pytest.raises(ValueError):categorical_psi([],['a'])


def test_oracle_aware_intervals_normalize_equivalent_instants():
    utc_start = datetime(2026, 10, 19, 8, 0, tzinfo=timezone.utc)
    mx_start = datetime(2026, 10, 19, 2, 0, tzinfo=timezone(timedelta(hours=-6)))
    utc_end = utc_start + timedelta(hours=1)
    mx_end = mx_start + timedelta(hours=1)
    assert oracle_utc_naive(mx_start) == datetime(2026, 10, 19, 8, 0)
    assert interval_run_id('leads_ventas', utc_start, utc_end) == interval_run_id(
        'leads_ventas', mx_start, mx_end)
    with pytest.raises(ValueError):
        oracle_utc_naive(datetime(2026, 10, 19, 8, 0))
