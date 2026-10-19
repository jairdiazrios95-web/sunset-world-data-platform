"""Dependency-free contract tests. Not a substitute for the live integration workflow."""
from pathlib import Path
from importlib.util import spec_from_file_location, module_from_spec
import pytest

ROOT = Path(__file__).resolve().parents[1]


def test_call_center_sink_ddl_has_both_tables():
    ddl=(ROOT/'sql/005_streaming.sql').read_text()
    assert 'ops.call_center_processed' in ddl
    assert 'ops.call_center_alert' in ddl
    assert 'ON CONFLICT(event_id)' in (ROOT/'streaming/structured_streaming.py').read_text()


def test_live_integration_does_not_fallback_to_sqlite():
    s=(ROOT/'scripts/verify_real_services.py').read_text()
    assert 'psycopg.connect' in s
    assert 'pyarrow.parquet' in s
    assert 'load_parquet(' in s
    assert "'status':'PASS'" in s
    assert 'sqlite3' not in s


def test_runner_requires_real_services():
    s=(ROOT/'.github/workflows/live-platform.yml').read_text()
    assert 'pgvector/pgvector:pg16' in s
    assert 'verify_real_services.py --dbt --repeats 2' in s
    assert 'actions/upload-artifact' in s


def test_no_fake_pass_when_missing_db_connection(monkeypatch):
    # Importing module should not require third-party packages; invoking it does.
    path=ROOT/'scripts/verify_real_services.py'
    spec=spec_from_file_location('real_verify',path)
    mod=module_from_spec(spec)
    spec.loader.exec_module(mod)
    with pytest.raises(ValueError):
        mod.execute(ROOT, ROOT/'reports'/'unused.json',repeats=1)


def test_real_streaming_has_physical_broker_and_sink_gate():
    w=(ROOT/'.github/workflows/live-streaming.yml').read_text()
    v=(ROOT/'scripts/verify_streaming_live.py').read_text()
    assert 'docker compose --profile streaming up -d --build spark' in w
    assert 'confluent_kafka' in v
    assert 'ops.call_center_processed' in v
    assert 'ops.call_center_alert' in v
    assert 'postgres_rows_after_replay' in v


def test_minio_real_s3_roundtrip_and_checksum_contract():
    w=(ROOT/'.github/workflows/live-minio.yml').read_text()
    v=(ROOT/'scripts/verify_minio_live.py').read_text()
    assert 'docker compose --profile lake up -d minio' in w
    assert 'pyarrow.parquet' in v and 'client.upload_file' in v and 'client.download_file' in v
    assert 'sha256_file' in v


def test_pgvector_real_insert_and_query_contract():
    v=(ROOT/'scripts/verify_real_services.py').read_text()
    assert 'embedding <=> %s::vector' in v
    assert 'INSERT INTO ai.document_chunk' in v
    assert 'fixture-not-a-model' in v


def test_airflow_real_dag_execution_gate():
    w=(ROOT/'.github/workflows/live-airflow.yml').read_text()
    assert 'airflow dags test etl_sunset' in w
    assert 'docker compose cp airflow:' in w
    assert 'airflow_quality.json' in w


def test_real_rag_model_required_and_no_fake_vector():
    w=(ROOT/'.github/workflows/live-rag.yml').read_text()
    v=(ROOT/'scripts/verify_rag_live.py').read_text()
    assert 'sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2' in w
    assert 'SentenceTransformer(model_id)' in v
    assert 'index_records(' in v and 'search(' in v


def test_final_release_gate_must_block_without_real_ci_evidence(tmp_path):
    from importlib.util import spec_from_file_location, module_from_spec
    spec=spec_from_file_location('acceptance_gate',ROOT/'scripts/acceptance_gate.py')
    mod=module_from_spec(spec)
    spec.loader.exec_module(mod)
    result=mod.acceptance(tmp_path)
    assert result['status']=='BLOCKED'
    assert result['approved_for_production'] is False
    assert all(v['status']=='MISSING' for v in result['technology_proof'].values())
