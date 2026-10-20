"""Boundary-isolated tests for physical loader and incremental Oracle extractor.

These tests check real transformations, SQL statement generation, validation
and retry decisions. Full PostgreSQL and Oracle connectivity are verified
separately and must never be inferred from these offline doubles.
"""
from __future__ import annotations

import json
import sys
import types
from datetime import datetime, timezone
from pathlib import Path

import pytest


class CopyRows:
    def __init__(self, cur):
        self.cur = cur
    def __enter__(self):
        return self
    def __exit__(self, *args):
        return False
    def write_row(self, row):
        self.cur.staging.append(row)


class FakeCursor:
    def __init__(self):
        self.staging = []
        self.rows = {}
        self.keys = set()
        self.answer = (0,)
        self.statements = []
    def __enter__(self):
        return self
    def __exit__(self, *args):
        return False
    def execute(self, sql, params=None, **kwargs):
        self.statements.append(sql)
        query = sql.lower()
        if query.startswith("create temp table"):
            self.staging = []
        elif query.startswith("select count(*) from \"stage_"):
            self.answer = (len(self.staging),)
        elif "having count(*) > 1" in query:
            self.answer = (0,)
        elif "insert into raw." in query:
            self.keys.update(record[0] for record in self.staging)
            self.rows["leads_ventas"] = len(self.keys)
        elif "select count(*) from raw." in query:
            self.answer = (self.rows.get("leads_ventas", 0),)
    def fetchone(self):
        return self.answer
    def copy(self, sql):
        return CopyRows(self)


class FakeConn:
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


def attach_database(monkeypatch, cursor):
    monkeypatch.setitem(sys.modules, "psycopg",
                        types.SimpleNamespace(connect=lambda *_a, **_k: FakeConn(cursor)))


def create_small_snapshot(tmp_path):
    cfg = {
        "dataset_mode": "synthetic",
        "profiles": {"sample": {"leads": 1, "payments": 1, "comments": 1}},
        "sources": {
            "leads_ventas": {
                "target": "leads",
                "primary_key": ["lead_id"],
                "required": ["cliente_id", "fecha_creacion"],
                "date_column": "fecha_creacion",
            }
        },
        "parquet": {
            "batch_size": 3, "max_rows_per_file": 4, "compression": "snappy"
        },
    }
    path = tmp_path/"pipeline.json"
    path.write_text(json.dumps(cfg))
    sample = tmp_path/"input"
    sample.mkdir()
    (sample/"leads_ventas.csv").write_text(
        "lead_id,cliente_id,fecha_creacion\nL1,C1,2024-01-01\nL2,C2,2024-01-02\n"
    )
    return cfg, path, sample


def test_postgres_csv_loader_stages_and_replays(tmp_path, monkeypatch):
    from warehouse.loader import load_csv
    cfg, path, sample = create_small_snapshot(tmp_path)
    cursor = FakeCursor()
    attach_database(monkeypatch, cursor)
    first = load_csv(sample/"leads_ventas.csv", "leads_ventas", cfg, "dsn", batch_size=1)
    second = load_csv(sample/"leads_ventas.csv", "leads_ventas", cfg, "dsn", batch_size=2)
    assert first["rows_read"] == 2 and first["batches"] == 2
    assert second["rows_read"] == 2 and second["batches"] == 1
    assert any("ON CONFLICT" in sql for sql in cursor.statements)
    with pytest.raises(ValueError):
        load_csv(sample/"leads_ventas.csv", "unknown", cfg, "dsn")


def test_real_pyarrow_to_staged_postgres_load(tmp_path, monkeypatch):
    pytest.importorskip("pyarrow")
    from sunset.parquet import convert
    from warehouse.loader import load_parquet
    cfg, path, sample = create_small_snapshot(tmp_path)
    lake = tmp_path/"lake"
    proof = convert(sample, lake, path, "leads_ventas")
    assert proof["leads_ventas"]["rows"] == 2
    cursor = FakeCursor()
    attach_database(monkeypatch, cursor)
    result = load_parquet(lake, "leads_ventas", cfg, "dsn", batch_size=1)
    assert result["status"] == "PASS"
    assert result["parquet_rows"] == 2
    assert result["destination_rows"] >= 2
    assert any("ops.batch_audit" in sql for sql in cursor.statements)


def test_oracle_snapshot_extract_uses_bounded_fetch_and_utc_dates(tmp_path, monkeypatch):
    from oracle import snapshot_to_lake as oracle
    from sunset import parquet
    cfg, cfgfile, _ = create_small_snapshot(tmp_path)
    captured = []
    class OracleCursor:
        description = [
            ("LEAD_ID",), ("CLIENTE_ID",), ("FECHA_CREACION",), ("UPDATED_AT",)
        ]
        arraysize = 0
        prefetchrows = 0
        def __init__(self):
            self.calls = 0
        def __enter__(self):
            return self
        def __exit__(self, *_):
            return False
        def execute(self, sql, **params):
            captured.append((sql, params))
        def fetchmany(self, size):
            self.calls += 1
            if self.calls == 1:
                return [("L1", "C1", "2024-01-01", "2024-01-01")]
            return []
    class Connection:
        def __enter__(self):
            return self
        def __exit__(self, *_):
            return False
        def cursor(self):
            return OracleCursor()
    class MockDatabaseError(Exception):
        pass
    fake = types.SimpleNamespace(
        DatabaseError=MockDatabaseError,
        init_oracle_client=lambda **_k: None,
        connect=lambda **_k: Connection(),
    )
    monkeypatch.setitem(sys.modules, "oracledb", fake)
    monkeypatch.setenv("ORACLE_USER", "ci")
    monkeypatch.setenv("ORACLE_PASSWORD", "ephemeral")
    monkeypatch.setenv("ORACLE_DSN", "internal")
    def fake_parquet_commit(input_dir, output, config, source):
        # The real converter creates the snapshot directory before publishing.
        output.mkdir(parents=True, exist_ok=True)
        return {source: {"rows": 1}}
    monkeypatch.setattr(parquet, "convert", fake_parquet_commit)
    monkeypatch.setattr(oracle, "convert", parquet.convert)
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    end = datetime(2026, 1, 2, tzinfo=timezone.utc)
    root = tmp_path/"oracle-out"
    report = oracle.run("leads_ventas", start, end, root, cfgfile, max_retries=1)
    assert report["status"] == "PASS" and report["extracted"] == 1
    assert captured[0][1]["start_time"].tzinfo is None
    assert (root/"incremental"/report["run_id"]/"_published.json").exists()
    with pytest.raises(ValueError, match="increasing"):
        oracle.run("leads_ventas", end, start, root, cfgfile)
    with pytest.raises(ValueError, match="batch"):
        oracle.run("leads_ventas", start, end, root, cfgfile, fetch_size=0)


def test_oracle_snapshot_reports_empty_batches(tmp_path, monkeypatch):
    from oracle import snapshot_to_lake as oracle
    cfg, cfgfile, _ = create_small_snapshot(tmp_path)
    class Cursor:
        description = [
            ("LEAD_ID",), ("CLIENTE_ID",), ("FECHA_CREACION",)
        ]
        def __enter__(self):
            return self
        def __exit__(self, *_):
            return False
        def execute(self, *_a, **_k):
            pass
        def fetchmany(self, *_):
            return []
    class Conn:
        def __enter__(self):
            return self
        def __exit__(self, *_):
            return False
        def cursor(self):
            return Cursor()
    monkeypatch.setitem(sys.modules, "oracledb", types.SimpleNamespace(
        DatabaseError=Exception, init_oracle_client=lambda **_: None,
        connect=lambda **_: Conn()
    ))
    monkeypatch.setenv("ORACLE_USER", "ci")
    monkeypatch.setenv("ORACLE_PASSWORD", "ephemeral")
    monkeypatch.setenv("ORACLE_DSN", "ci")
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    end = datetime(2026, 1, 2, tzinfo=timezone.utc)
    report = oracle.run("leads_ventas", start, end, tmp_path/"out", cfgfile)
    assert report["status"] == "EMPTY"
