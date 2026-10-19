"""Static GitHub Actions contracts; these tests DO NOT pretend to run Actions."""
import json
from pathlib import Path
import yaml
import pytest
from scripts.acceptance_gate import acceptance, REQUIREMENTS

ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT/'.github'/'workflows'


def document(name):
    return yaml.load((WORKFLOWS/name).read_text(encoding='utf-8'), Loader=yaml.BaseLoader)


def test_master_invokes_all_real_stacks_in_same_run():
    doc = document('real-acceptance.yml')
    assert 'workflow_dispatch' in doc['on']
    jobs = doc['jobs']
    for target in ('platform', 'minio', 'airflow', 'streaming', 'rag'):
        assert jobs[target]['uses'] == f'./.github/workflows/live-{target}.yml'
        assert 'workflow_call' in document(f'live-{target}.yml')['on']
    assert jobs['acceptance']['needs'] == ['platform','minio','airflow','streaming','rag']
    steps = jobs['acceptance']['steps']
    assert any(s.get('uses','').startswith('actions/download-artifact@') for s in steps)
    assert any('--evidence reports/github-real-evidence' in s.get('run','') for s in steps)


def test_compose_workflows_provide_environment_variables_for_all_services():
    for name in ('live-minio.yml','live-airflow.yml','live-streaming.yml'):
        doc = document(name)
        for job in doc['jobs'].values():
            env=job.get('env',{})
            for required in ('POSTGRES_PASSWORD','MINIO_ROOT_USER','MINIO_ROOT_PASSWORD','GRAFANA_ADMIN_PASSWORD'):
                assert env.get(required), (name,required)


def test_non_airflow_worker_evidence_must_be_real():
    doc=document('live-platform.yml')
    run='\n'.join(step.get('run','') for step in doc['jobs']['live-postgres-parquet']['steps'])
    assert '--dbt --repeats 2' in run
    assert 'not j[\'simulation\']' in run


def test_gate_fails_if_only_status_pass_and_false_simulation(tmp_path):
    for file in REQUIREMENTS.values():
        (tmp_path/file).write_text(json.dumps({'status':'PASS','simulation':False,'engine':'X'}))
    actual=acceptance(tmp_path)
    assert actual['status']=='BLOCKED'
    assert all(x['status']=='FAIL' for x in actual['technology_proof'].values())


def test_gate_fails_if_simulation_field_missing(tmp_path):
    for file in REQUIREMENTS.values():
        (tmp_path/file).write_text(json.dumps({'status':'PASS','engine':'X'}))
    assert acceptance(tmp_path)['status']=='BLOCKED'


def test_gate_accepts_complete_typed_proofs(tmp_path):
    payloads = {
        'live_postgres_parquet_verified.json': {'dbt_executed': True,'vector_extension':'0.8','counts':{'raw':{str(x):10 for x in range(10)},'parquet':{'leads':{}}}},
        'live_minio_verified.json': {'files':2,'parquet_rows':100,'replay_objects':2},
        'real_streaming_evidence.json': {'events':5,'alerts':2,'postgres_rows_after_replay':5,'postgres_alerts_after_replay':2},
        'real_airflow_quality.json': {'dbt_test_count':4,'source_count':10,'relation_rows':{'raw.leads_ventas':100},'orphan_sales':0},
        'real_rag_verified.json': {'embedding_dimension':384,'first_run':{'embedded':2},'replay':{'embedded':0},'retrieved':2},
    }
    for name,payload in payloads.items():
        (tmp_path/name).write_text(json.dumps({'status':'PASS','simulation':False,'engine':'real integration',**payload}))
    result=acceptance(tmp_path)
    assert result['status']=='PASS'
    assert result['approved_for_production'] is False


def test_airflow_verifier_fails_closed_without_real_db_or_dbt(monkeypatch,tmp_path):
    from scripts.verify_airflow_live import verify
    qa=tmp_path/'qa.json'
    dbt=tmp_path/'dbt.json'
    qa.write_text(json.dumps({'status':'PASS'}))
    dbt.write_text(json.dumps({'results':[]}))
    with pytest.raises(AssertionError, match='Missing actual dbt test'):
        verify(qa,dbt,tmp_path/'proof.json')
