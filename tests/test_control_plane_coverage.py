"""Control-plane regression tests for real-service verifiers.

Network boundaries are mocked only in these unit tests. Real Kafka, Airflow
and RAG integrations remain separate required jobs in the master CI workflow.
"""
from __future__ import annotations
import json
import os
import sys
import types
from pathlib import Path

import pytest


def test_two_run_auditor_validates_distinct_ids_same_commit(tmp_path, monkeypatch):
    from scripts import verify_two_acceptance_runs as audit
    sha = "fixed-commit"
    runs = [{
        "id": 11, "name": audit.WORKFLOW, "event": "push",
        "head_sha": sha,
    }]
    second = [{
        "id": 12, "name": audit.WORKFLOW, "event": "workflow_dispatch",
        "head_sha": sha,
    }]
    def fake_api(route, method="GET", fields=()):
        if method == "POST":
            runs.extend(second)
            return None
        if route.endswith("/actions/runs?head_sha=fixed-commit&per_page=100"):
            return {"workflow_runs": list(runs)}
        if route.endswith("/actions/runs/11") or route.endswith("/actions/runs/12"):
            run_id = int(route.rsplit("/", 1)[1])
            return {
                "head_sha": sha, "status": "completed",
                "conclusion": "success", "html_url": f"https://github.local/{run_id}",
            }
        if route.endswith("/jobs?per_page=100"):
            return {"jobs": [
                {"name": name, "conclusion": "success"}
                for name in sorted(audit.EXPECTED_JOBS)
            ]}
        if route.endswith("/artifacts?per_page=100"):
            return {"artifacts": [{"name": "sunset-real-acceptance-summary"}]}
        raise AssertionError("Unexpected API: " + route)
    monkeypatch.setattr(audit, "api", fake_api)
    monkeypatch.setenv("GITHUB_REPOSITORY", "owner/project")
    monkeypatch.setenv("GITHUB_SHA", sha)
    monkeypatch.chdir(tmp_path)
    audit.main()
    report = json.loads(Path("reports/audit_two_passes.json").read_text())
    assert report["status"] == "PASS"
    assert [entry["run_id"] for entry in report["independent_runs"]] == [11, 12]
    assert report["production_approved"] is False
    assert audit.matching(runs, sha, "push", {11}) is None


def test_github_auditor_fails_closed_for_incomplete_runs(monkeypatch):
    from scripts import verify_two_acceptance_runs as audit
    data = {"head_sha": "sha", "status": "completed",
            "conclusion": "success", "html_url": "url"}
    def fake_api(route, method="GET", fields=()):
        if route.endswith("/jobs?per_page=100"):
            return {"jobs": [{"name": "acceptance", "conclusion": "success"}]}
        if route.endswith("/artifacts?per_page=100"):
            return {"artifacts": []}
        return data
    monkeypatch.setattr(audit, "api", fake_api)
    with pytest.raises(AssertionError, match="missing successful jobs"):
        audit.validate_run("owner/repo", 123, "sha", audit.time.monotonic()+2)
    with pytest.raises(AssertionError, match="Unexpected SHA"):
        audit.validate_run("owner/repo", 123, "different", audit.time.monotonic()+2)


def test_real_rag_verifier_reuses_vectors_and_reports_results(monkeypatch, tmp_path):
    from scripts.verify_rag_live import verify
    from rag import build_embeddings, search as rag_search
    calls = []
    class Model:
        def __init__(self, *_args):
            pass
        def get_sentence_embedding_dimension(self):
            return 384
    monkeypatch.setitem(sys.modules, "sentence_transformers",
                        types.SimpleNamespace(SentenceTransformer=Model))
    monkeypatch.setitem(sys.modules, "psycopg", types.ModuleType("psycopg"))
    def index(records, model, model_id, dsn):
        calls.append(tuple(row["comentario_id"] for row in records))
        return ({"embedded": 2} if len(calls) == 1
                else {"embedded": 0, "unchanged_skipped": 2})
    monkeypatch.setattr(build_embeddings, "index_records", index)
    monkeypatch.setattr(rag_search, "search", lambda *_a: [
        {"similarity": 0.9}, {"similarity": 0.7}
    ])
    monkeypatch.setenv("SUNSET_PG_DSN", "fake-local")
    report = tmp_path/"rag.json"
    proof = verify(report)
    assert proof["status"] == "PASS"
    assert proof["retrieved"] == 2
    assert proof["embedding_dimension"] == 384
    assert len(calls) == 2


def test_streaming_sink_executes_both_alert_and_nonalert_upserts(monkeypatch):
    from streaming.structured_streaming import write_db_batch
    statement_log = []
    class Cursor:
        def __enter__(self):
            return self
        def __exit__(self, *_):
            return False
        def execute(self, sql, params):
            statement_log.append(sql)
    class Connection:
        def __enter__(self):
            return self
        def __exit__(self, *_):
            return False
        def cursor(self):
            return Cursor()
        def commit(self):
            return None
    monkeypatch.setitem(sys.modules, "psycopg",
                        types.SimpleNamespace(connect=lambda _: Connection()))
    monkeypatch.setenv("SUNSET_PG_DSN", "ci-only")
    class Event:
        def __init__(self, alert):
            self.alert = alert
        def asDict(self, recursive=True):
            return {
                "event_id": f"id-{self.alert}", "call_id": "CI", "sequence": 1,
                "event_time": "2026-01-01T00:00:00Z", "transcript": "demo",
                "has_alert": self.alert, "matched_pattern": "risk" if self.alert else None
            }
    class Frame:
        def foreachPartition(self, fn):
            fn(iter([Event(True), Event(False)]))
    write_db_batch(Frame(), 1)
    assert sum("INSERT INTO ops.call_center_processed" in q for q in statement_log) == 2
    assert any("INSERT INTO ops.call_center_alert" in q for q in statement_log)
    assert any("DELETE FROM ops.call_center_alert" in q for q in statement_log)


def test_real_streaming_verifier_requires_ack_and_replay(monkeypatch, tmp_path):
    from scripts import verify_streaming_live as m
    from streaming.producer import make_events
    collection = []
    events = make_events("CI-SIM", "2026-01-01T00:00:00Z")
    class Message:
        def __init__(self, topic, payload):
            self._topic, self._payload = topic, payload
        def error(self):
            return None
        def value(self):
            return json.dumps(self._payload).encode()
        def topic(self):
            return self._topic
    class Consumer:
        def __init__(self, cfg):
            self.closed = False
        def subscribe(self, topics):
            self.topics = topics
        def assignment(self):
            return ["assigned"]
        def poll(self, timeout):
            return collection.pop(0) if collection else None
        def close(self):
            self.closed = True
    class Producer:
        def __init__(self, cfg):
            self.once = False
        def produce(self, topic, key, value):
            record = json.loads(value)
            if not self.once:
                collection.append(Message("transcripciones_procesadas", record))
                if record["sequence"] in (2, 4):
                    collection.append(Message("alertas", record))
        def flush(self, t):
            self.once = True
            return 0
    class Cursor:
        sql = ""
        def __enter__(self):
            return self
        def __exit__(self, *_):
            return False
        def execute(self, sql, values=None):
            self.sql = sql
        def fetchone(self):
            return (2,) if "call_center_alert" in self.sql else (5,)
    class Conn:
        def __enter__(self):
            return self
        def __exit__(self, *_):
            return False
        def cursor(self):
            return Cursor()
    monkeypatch.setitem(sys.modules, "psycopg",
                        types.SimpleNamespace(connect=lambda *_a: Conn()))
    monkeypatch.setitem(sys.modules, "confluent_kafka",
                        types.SimpleNamespace(Consumer=Consumer, Producer=Producer,
                                              KafkaException=Exception))
    monkeypatch.setattr(m.time, "sleep", lambda _: None)
    monkeypatch.setenv("SUNSET_PG_DSN", "temporary")
    monkeypatch.setattr(m, "make_events", lambda *_a: events)
    result = m.verify(5, tmp_path/"streaming.json")
    assert result["status"] == "PASS"
    assert result["processed_topic_events"] == 5
    assert result["alert_topic_events"] == 2
    with pytest.raises(ValueError):
        m.verify(4, tmp_path/"invalid.json")


def test_warehouse_parquet_cli_passes_config_and_dsns(monkeypatch, capsys):
    from warehouse import load_parquet
    from sunset import config
    from warehouse import loader
    monkeypatch.setattr(load_parquet, "load", lambda *_a: {"sources": {}})
    monkeypatch.setattr(load_parquet, "load_parquet", lambda *_a: {"status": "PASS"})
    monkeypatch.setenv("SUNSET_PG_DSN", "private-test")
    monkeypatch.setattr(sys, "argv", [
        "load_parquet", "--source", "leads_ventas", "--batch-size", "12"])
    load_parquet.main()
    assert json.loads(capsys.readouterr().out)["status"] == "PASS"


def test_acceptance_gate_validates_all_typed_technologies(tmp_path):
    from scripts.acceptance_gate import acceptance, REQUIREMENTS
    proof = {
        "PostgreSQL/Parquet/dbt/pgvector": {
            "status": "PASS", "engine": "PostgreSQL", "simulation": False,
            "dbt_executed": True, "vector_extension": "0.8",
            "counts": {"raw": {str(x): 1 for x in range(10)}, "parquet": {}},
        },
        "MinIO": {"status": "PASS", "engine": "S3", "simulation": False,
                  "files": 2, "parquet_rows": 4, "replay_objects": 2},
        "Kafka/Spark": {"status": "PASS", "engine": "Spark", "simulation": False,
                        "events": 5, "alerts": 2,
                        "postgres_rows_after_replay": 5,
                        "postgres_alerts_after_replay": 2},
        "Airflow": {"status": "PASS", "engine": "Airflow", "simulation": False,
                    "dbt_test_count": 3, "source_count": 10,
                    "relation_rows": {"test": 1}, "orphan_sales": 0},
        "RAG con modelo real": {"status": "PASS", "engine": "RAG", "simulation": False,
                               "embedding_dimension": 384,
                               "first_run": {"embedded": 2},
                               "replay": {"embedded": 0},
                               "retrieved": 2},
    }
    for name, filename in REQUIREMENTS.items():
        (tmp_path/filename).write_text(json.dumps(proof[name]))
    assert acceptance(tmp_path)["status"] == "PASS"
    (tmp_path/REQUIREMENTS["RAG con modelo real"]).write_text("{}")
    assert acceptance(tmp_path)["status"] == "BLOCKED"
