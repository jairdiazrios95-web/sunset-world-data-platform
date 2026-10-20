"""Exercise actual CLI and configuration code while isolating external I/O."""
from __future__ import annotations

import json
import runpy
import sys
import types
from pathlib import Path

import pytest


def test_pdf_preprocessor_wraps_wide_markdown_tables(tmp_path, monkeypatch):
    from scripts.pdf_preprocess import cells, prepare, main
    assert cells("| A | B |") == ["A", "B"]
    content = (
        "# Report\n\n"
        "| Component | Finding | Status |\n"
        "| --- | --- | --- |\n"
        "| Lake | Parquet and manifest | PASS |\n"
        "| Warehouse | Raw and curated | PASS |\n\n"
        "## Closing\n"
    )
    rendered = prepare(content)
    assert "**Lake.**" in rendered
    assert "**Finding:** Parquet and manifest" in rendered
    assert "**Status:** PASS" in rendered
    assert "| --- |" not in rendered
    assert "## Closing" in rendered
    source = tmp_path/"original.md"
    output = tmp_path/"output"/"exam.md"
    source.write_text(content)
    monkeypatch.setattr(sys, "argv", ["preprocess", str(source), str(output)])
    main()
    assert output.read_text() == rendered


def test_real_slide_builder_creates_five_slide_deck(tmp_path):
    pptx = pytest.importorskip("pptx")
    import scripts.build_presentation as builder
    deck = pptx.Presentation(builder.BASE + "/deliverables/Sunset_World_Presentacion_Ejecutiva.pptx")
    assert len(deck.slides) == 5
    slide_titles = [
        shape.text for slide in deck.slides for shape in slide.shapes
        if shape.has_text_frame and shape.text
    ]
    assert any("plataforma" in title.lower() for title in slide_titles)
    # Verify slide helper functions draw shapes and write text.
    s = builder.slide("Control", "Evidencia", 1)
    before = len(s.shapes)
    builder.card(s, 0.5, 1.5, 5, 3, "Alertas", "Sin duplicados")
    assert len(s.shapes) > before


def test_runtime_cli_validar_local_runs_full_synthetic_sqlite(tmp_path, monkeypatch, capsys):
    from scripts.validar_local import main
    report = tmp_path/"reports"/"local.json"
    monkeypatch.setattr(sys, "argv", [
        "validar_local",
        "--leads", "20", "--payments", "12", "--comments", "25",
        "--workdir", str(tmp_path/"runs"), "--report", str(report),
    ])
    main()
    result = json.loads(report.read_text())
    assert result["status"] == "PASS"
    assert result["requested_main_source_counts"]["leads_ventas"] == 20
    assert json.loads(capsys.readouterr().out)["idempotent"] is True
    monkeypatch.setattr(sys, "argv", [
        "validar_local", "--leads", "0", "--workdir", str(tmp_path/"bad")])
    with pytest.raises(ValueError, match="positive"):
        main()


def test_original_csv_stress_cli_generates_small_real_files(tmp_path, monkeypatch):
    from stress.run_original_csv_generation import main
    config = tmp_path/"profile.json"
    config.write_text(json.dumps({
        "leads": 15, "pagos": 7, "comentarios": 11,
        "seed": 5, "chunk_size": 4}))
    report = tmp_path/"reports"/"csv_metrics.json"
    csv_root = tmp_path/"csv"
    monkeypatch.setattr(sys, "argv", [
        "original_stress", "--config", str(config),
        "--out", str(csv_root), "--report", str(report)])
    main()
    measured = json.loads(report.read_text())
    assert measured["status"] == "PASS"
    assert measured["total_bytes"] > 0
    assert (csv_root/"leads_ventas.csv").is_file()


def test_oracle_cursor_readonly_and_allowlist(monkeypatch):
    from oracle.oracle_adapter import extract_rows
    class Cursor:
        arraysize = None
        description = [("LEAD_ID",), ("UPDATED_AT",)]
        def __init__(self):
            self.retrieved = 0
            self.sql = ""
        def __enter__(self):
            return self
        def __exit__(self, *_):
            return False
        def execute(self, sql, **_):
            self.sql = sql
        def fetchmany(self, count):
            self.retrieved += 1
            return [("L1", "2026-01-01")] if self.retrieved == 1 else []
    class Connect:
        def __enter__(self):
            return self
        def __exit__(self, *_):
            return False
        def cursor(self):
            return Cursor()
    init = []
    fake = types.SimpleNamespace(
        init_oracle_client=lambda **k: init.append(k),
        connect=lambda **_: Connect(),
    )
    monkeypatch.setitem(sys.modules, "oracledb", fake)
    monkeypatch.setenv("ORACLE_USER", "demo")
    monkeypatch.setenv("ORACLE_PASSWORD", "ephemeral")
    monkeypatch.setenv("ORACLE_DSN", "private")
    monkeypatch.setenv("ORACLE_CLIENT_LIB_DIR", "/fake-client")
    rows = list(extract_rows("LEADS_VENTAS", "2026-01-01", 7))
    assert rows == [(["LEAD_ID", "UPDATED_AT"], [("L1", "2026-01-01")])]
    assert init == [{"lib_dir": "/fake-client"}]
    with pytest.raises(ValueError, match="unsafe"):
        list(extract_rows("X;DROP", "2026-01-01"))


def test_streaming_producer_delivers_and_rejects_unacked(monkeypatch, capsys):
    from streaming import producer
    class Producer:
        pending = 0
        instance = None
        def __init__(self, config):
            assert config["enable.idempotence"] is True
            self.events = []
            Producer.instance = self
        def produce(self, topic, key, value):
            self.events.append(json.loads(value))
            assert topic == "transcripciones_raw"
            assert key == "CALL-TEST"
        def flush(self, _seconds):
            return self.pending
    monkeypatch.setitem(sys.modules, "confluent_kafka", types.SimpleNamespace(Producer=Producer))
    monkeypatch.setattr(sys, "argv", ["producer", "--call-id", "CALL-TEST"])
    producer.main()
    assert len(Producer.instance.events) == 5
    assert "Published 5" in capsys.readouterr().out
    Producer.pending = 1
    with pytest.raises(RuntimeError, match="not acknowledged"):
        producer.main()
    Producer.pending = 0
    with pytest.raises(ValueError):
        producer.build_event("", 5, "hello", "now")


def test_postgres_bootstrap_executes_versioned_sql_and_verifies_pgvector(tmp_path, monkeypatch):
    from scripts.bootstrap_db import bootstrap
    for name in ("001_init.sql", "003_upsert_patterns.sql", "005_streaming.sql"):
        folder = tmp_path/"sql"
        folder.mkdir(exist_ok=True)
        (folder/name).write_text("SELECT 1;")
    class Cur:
        calls = []
        def __enter__(self):
            return self
        def __exit__(self, *_):
            return False
        def execute(self, sql, **_):
            self.calls.append(sql)
        def fetchone(self):
            return ("0.8.0",)
    class Conn:
        def __enter__(self):
            return self
        def __exit__(self, *_):
            return False
        def cursor(self):
            return Cur()
    fake = types.SimpleNamespace(connect=lambda *_a, **_k: Conn())
    monkeypatch.setitem(sys.modules, "psycopg", fake)
    monkeypatch.setenv("SUNSET_PG_DSN", "postgresql://ephemeral")
    assert bootstrap(tmp_path) == "0.8.0"
    assert len(Cur.calls) == 4


def test_semantic_audit_runner_and_cli_with_sqlite_proxy(tmp_path, monkeypatch):
    from stress import check_semantic_consistency as audit
    class Result:
        def fetchone(self):
            return (0,)
    class DB:
        def execute(self, sql):
            return Result()
        def close(self):
            return None
    monkeypatch.setattr(audit.sqlite3, "connect", lambda *_a, **_k: DB())
    result = audit.check(tmp_path/"not-real.sqlite")
    assert result["status"] == "PASS"
    assert len(result["violations"]) == 5
    out = tmp_path/"report.json"
    monkeypatch.setattr(sys, "argv", [
        "semantics", "--db", str(tmp_path/"not-real.sqlite"),
        "--report", str(out)])
    with pytest.raises(SystemExit) as exc:
        audit.main()
    assert exc.value.code == 0
    assert json.loads(out.read_text())["status"] == "PASS"
