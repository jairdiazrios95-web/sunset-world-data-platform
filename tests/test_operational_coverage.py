"""Behavioral tests for operational code that was previously uninstrumented.

External clients are faked at the API boundary: business logic, DB statements,
idempotency decisions and failure semantics execute as written. Live tests
remain mandatory in their own GitHub Actions workflows.
"""
from __future__ import annotations

import json
import sys
import types
from pathlib import Path

import pytest


class Cursor:
    def __init__(self, counts=None, hashes=None):
        self.counts = counts or {}
        self.hashes = hashes if hashes is not None else {}
        self.sql = []
        self.answer = (1,)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, sql, params=None, **kw):
        self.sql.append((sql, params))
        if "SELECT content_hash" in sql:
            self.answer = ((self.hashes.get(params[0]),)
                           if params[0] in self.hashes else None)
        elif "INSERT INTO ai.document_chunk" in sql:
            self.hashes[params[0]] = params[4]
            self.answer = None
        elif "to_regclass" in sql:
            self.answer = ("raw.table",)
        elif "WHERE d.cliente_key IS NULL" in sql:
            self.answer = (0,)
        elif "select count" in sql.lower():
            table = sql.split("FROM")[-1].strip().split()[0]
            self.answer = (self.counts.get(table, 10),)
        else:
            self.answer = (1,)

    def fetchone(self):
        return self.answer

    def fetchall(self):
        return [("sample1", "comment", "Payments", 0.8)]


class Conn:
    def __init__(self, cur):
        self.cur = cur
        self.commits = 0

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def cursor(self):
        return self.cur

    def transaction(self):
        return self

    def commit(self):
        self.commits += 1


def fake_psycopg(monkeypatch, cur):
    monkeypatch.setitem(sys.modules, "psycopg",
                        types.SimpleNamespace(connect=lambda *_a, **_k: Conn(cur)))


def test_minio_manifest_pointer_publishes_only_after_uploads(tmp_path, monkeypatch):
    from sunset.minio import publish
    bucket = []
    objects = []
    class S3:
        def list_buckets(self):
            return {"Buckets": [{"Name": x} for x in bucket]}
        def create_bucket(self, Bucket):
            bucket.append(Bucket)
        def upload_file(self, source, dest, key):
            assert Path(source).is_file()
            objects.append((dest, key))
        def put_object(self, Bucket, Key, Body):
            assert objects and len(objects) == 2
            data = json.loads(Body)
            assert data["status"] == "COMPLETE"
            assert data["file_count"] == 1
            objects.append((Bucket, Key))
    monkeypatch.setitem(sys.modules, "boto3",
                        types.SimpleNamespace(client=lambda *_a, **_k: S3()))
    monkeypatch.setitem(sys.modules, "botocore", types.ModuleType("botocore"))
    mock_config = types.ModuleType("botocore.config")
    mock_config.Config = lambda **_kwargs: None
    monkeypatch.setitem(sys.modules, "botocore.config", mock_config)
    monkeypatch.setenv("MINIO_ROOT_USER", "ephemeral")
    monkeypatch.setenv("MINIO_ROOT_PASSWORD", "ephemeral")
    folder = tmp_path/"year=2024"/"month=01"
    folder.mkdir(parents=True)
    (folder/"part-000.parquet").write_bytes(b"real-file-placeholder")
    (tmp_path/"_manifest.json").write_text('{"source":"leads"}')
    proof = publish(tmp_path, "lake", "http://localhost:9000", "run-1")
    assert proof["file_count"] == 1
    assert [key for _, key in objects][-1] == "manifests/latest.json"
    # Replay is logically safe: same run ID uses identical object keys.
    objects.clear()
    publish(tmp_path, "lake", "http://localhost:9000", "run-1")
    assert [key for _, key in objects][-1] == "manifests/latest.json"
    for invalid in ("", "../escape", "part/run"):
        with pytest.raises(ValueError, match="Unsafe"):
            publish(tmp_path, "lake", "http://localhost:9000", invalid)


def test_mart_builder_runs_real_contract_and_reconciles(tmp_path, monkeypatch):
    from warehouse.build_marts import build, REQUIRED
    cursor = Cursor()
    fake_psycopg(monkeypatch, cursor)
    monkeypatch.setitem(sys.modules, "sqlparse",
                        types.SimpleNamespace(split=lambda _sql: ["SELECT 1;", "  "]))
    sql = tmp_path/"marts.sql"
    sql.write_text("SELECT 1;")
    proof = build("postgresql://fake", sql, fiscal_start_month=3)
    assert proof["status"] == "PASS"
    assert len(proof["row_counts"]) == 9
    assert sum("to_regclass" in query for query, _ in cursor.sql) == len(REQUIRED)
    assert any("app.fiscal_start_month" in query for query, _ in cursor.sql)
    with pytest.raises(ValueError, match="required"):
        build("", sql)
    with pytest.raises(ValueError, match="Invalid fiscal"):
        build("postgresql://fake", sql, fiscal_start_month=13)
    cursor.counts["raw.leads_ventas"] = 11
    with pytest.raises(AssertionError, match="Sales reconciliation"):
        build("postgresql://fake", sql)


def test_rag_embedding_upsert_changed_and_idempotent(monkeypatch):
    from rag.build_embeddings import index_records
    hashes = {}
    cur = Cursor(hashes=hashes)
    fake_psycopg(monkeypatch, cur)
    class Vector:
        def tolist(self):
            return [0.1]*384
    class Model:
        def get_sentence_embedding_dimension(self):
            return 384
        def encode(self, *_a, **_k):
            return Vector()
    row = [{"comentario_id": "C1", "texto": "Comunicarme a test@example.com para pagos"}]
    first = index_records(row, Model(), "demo", "postgresql://fake", commit_every=1)
    second = index_records(row, Model(), "demo", "postgresql://fake", commit_every=1)
    assert first["embedded"] == 1 and second["embedded"] == 0
    assert second["unchanged_skipped"] == 1
    assert "[EMAIL]" in next(
        args[3] for sql, args in cur.sql if "INSERT INTO ai.document_chunk" in sql
    )
    changed = index_records(
        [{"comentario_id": "C1", "texto": "Otra respuesta"}],
        Model(), "demo", "postgresql://fake", commit_every=1)
    assert changed["embedded"] == 1
    assert index_records(
        [{"comentario_id": "C1", "texto": ""}],
        Model(), "demo", "postgresql://fake")["embedded"] == 0
    class Wrong(Model):
        def get_sentence_embedding_dimension(self):
            return 768
    with pytest.raises(ValueError, match="384"):
        index_records(row, Wrong(), "demo", "postgresql://fake")


def test_rag_retrieval_uses_real_vector_sql_and_validates_limits(monkeypatch):
    from rag.search import search
    cursor = Cursor()
    fake_psycopg(monkeypatch, cursor)
    class Model:
        def __init__(self, name):
            self.name = name
        def encode(self, *_a, **_k):
            return types.SimpleNamespace(tolist=lambda: [0.3]*384)
    monkeypatch.setitem(sys.modules, "sentence_transformers",
                        types.SimpleNamespace(SentenceTransformer=Model))
    hits = search("¿Qué dijo de los pagos?", "postgresql://fake", "model", 3)
    assert hits[0]["source_id"] == "sample1"
    assert hits[0]["similarity"] == 0.8
    assert any("embedding <=>" in sql for sql, _ in cursor.sql)
    for query, limit in [("", 5), ("ok", 0), ("ok", 21)]:
        with pytest.raises(ValueError):
            search(query, "postgresql://fake", "model", limit)


def test_operational_dependency_report_is_discovery_not_fake_pass(tmp_path):
    from scripts.check_execution_environment import run
    target = tmp_path/"reports"/"environment.json"
    result = run(target)
    assert result["status"] == "ENVIRONMENT_DISCOVERY"
    assert result["real_technology_tests"] == "NOT_EXECUTED_IN_THIS_ENVIRONMENT"
    assert json.loads(target.read_text()) == result


@pytest.mark.parametrize("action", ["check-config", "generate", "audit", "parquet"])
def test_cli_actions_take_config_and_invoke_correct_boundary(monkeypatch, tmp_path, capsys, action):
    from sunset import cli
    config = {
        "profiles": {"sample": {"leads": 2, "payments": 1, "comments": 2}},
        "generation": {},
    }
    calls = []
    monkeypatch.setattr(cli, "load", lambda *_a: config)
    monkeypatch.setattr(cli, "build", lambda *a, **k: (calls.append(("generate", a)), {"built": True})[1])
    monkeypatch.setattr(cli, "audit", lambda *a, **k: (calls.append(("audit", a)), {"status": "PASS"})[1])
    monkeypatch.setattr(cli, "convert", lambda *a, **k: (calls.append(("parquet", a)), {"rows": 2})[1])
    monkeypatch.setattr(sys, "argv", ["sunset", action, "--profile", "sample",
                                     "--output", str(tmp_path/"out")])
    if action == "audit":
        with pytest.raises(SystemExit) as exc:
            cli.main()
        assert exc.value.code == 0
    else:
        cli.main()
    assert capsys.readouterr().out.strip().startswith("{")
    if action != "check-config":
        assert calls[0][0] == action


def test_real_airflow_verifier_checks_data_and_dbt(monkeypatch, tmp_path):
    from scripts.verify_airflow_live import verify
    quality = tmp_path/"quality.json"
    dbt = tmp_path/"dbt.json"
    quality.write_text('{"status":"PASS"}')
    dbt.write_text('{"results":[{"status":"pass"},{"status":"success"}]}')
    cursor = Cursor()
    fake_psycopg(monkeypatch, cursor)
    monkeypatch.setenv("SUNSET_PG_DSN", "postgresql://fake")
    report = tmp_path/"reports"/"airflow.json"
    result = verify(quality, dbt, report)
    assert result["status"] == "PASS" and result["dbt_test_count"] == 2
    assert report.is_file()
    quality.write_text('{"status":"FAIL"}')
    with pytest.raises(AssertionError, match="non-PASS"):
        verify(quality, dbt, report)
    quality.write_text('{"status":"PASS"}')
    dbt.write_text('{"results":[]}')
    with pytest.raises(AssertionError, match="Missing actual dbt"):
        verify(quality, dbt, report)
