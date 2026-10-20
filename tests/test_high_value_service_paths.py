"""Additional behavioral coverage of verified service adapters and stress logic.

Mocks isolate only Postgres/S3 network boundaries; validations and all control
flow in the code under test still execute. Physical service acceptance is kept
as an independent mandatory GitHub Actions workflow.
"""
from __future__ import annotations

import json
import os
import sys
import types
from contextlib import contextmanager
from pathlib import Path

import pytest


def test_minio_live_roundtrip_and_mismatch(tmp_path, monkeypatch):
    from scripts.verify_minio_live import verify, sha256_file
    sample = tmp_path/"data"/"sample"
    sample.mkdir(parents=True)
    (tmp_path/"config").mkdir()
    (tmp_path/"config"/"pipeline.json").write_text("{}")
    store = {}
    class ClientError(Exception):
        def __init__(self, status):
            self.response = {"ResponseMetadata": {"HTTPStatusCode": status}}
    class Pager:
        def paginate(self, Bucket, Prefix):
            return [{"Contents": [{"Key": k} for k in store if k.startswith(Prefix)]}]
    class S3:
        def __init__(self, corrupt=False):
            self.corrupt = corrupt
        def head_bucket(self, Bucket):
            raise ClientError(404)
        def create_bucket(self, Bucket):
            assert Bucket == "lake"
        def upload_file(self, filename, bucket, key):
            store[key] = Path(filename).read_bytes()
        def download_file(self, bucket, key, filename):
            data=store[key]
            Path(filename).write_bytes(b"tampered" if self.corrupt else data)
        def list_objects_v2(self, **_):
            return {}
        def get_paginator(self, name):
            return Pager()
    client = S3()
    monkeypatch.setitem(sys.modules, "boto3", types.SimpleNamespace(
        client=lambda *_a, **_k: client))
    monkeypatch.setitem(sys.modules, "botocore", types.SimpleNamespace(
        exceptions=types.SimpleNamespace(ClientError=ClientError)))
    monkeypatch.setitem(sys.modules, "pyarrow", types.ModuleType("pyarrow"))
    monkeypatch.setitem(sys.modules, "pyarrow.parquet", types.SimpleNamespace(
        read_metadata=lambda path: types.SimpleNamespace(num_rows=2)))
    from sunset import parquet as conversion
    from sunset import config as config_module
    monkeypatch.setattr(config_module, "load", lambda *_: {
        "sources": {"leads_ventas": {"target": "leads"},
                    "fx_historico": {"target": "fx"}}})
    def conversion_stub(_src, lake, _cfg, source):
        folder=lake/({"leads_ventas": "leads", "fx_historico": "fx"}[source])/"year=2026"/"month=10"
        folder.mkdir(parents=True, exist_ok=True)
        (folder/"part.parquet").write_bytes(source.encode())
        return {source: {"rows": 2}}
    monkeypatch.setattr(conversion, "convert", conversion_stub)
    monkeypatch.setenv("SUNSET_S3_ENDPOINT", "http://localhost:9000")
    monkeypatch.setenv("SUNSET_S3_BUCKET", "lake")
    monkeypatch.setenv("MINIO_ROOT_USER", "test-user")
    monkeypatch.setenv("MINIO_ROOT_PASSWORD", "test-pass")
    report = tmp_path/"evidence"/"minio.json"
    result=verify(tmp_path, report)
    assert result["status"] == "PASS"
    assert result["files"] == result["replay_objects"] == 2
    assert result["parquet_rows"] == 4
    assert len(result["objects"]) == 2
    assert json.loads(report.read_text())["status"] == "PASS"
    import hashlib
    assert sha256_file(report) == hashlib.sha256(report.read_bytes()).hexdigest()
    client.corrupt = True
    with pytest.raises(AssertionError, match="SHA-256 mismatch"):
        verify(tmp_path, report)


def test_postgres_physical_stress_control_flow_and_failure_checks(tmp_path, monkeypatch):
    from stress import postgres_live_stress as m
    config=tmp_path/"profiles.json"
    config.write_text(json.dumps({"profiles": {"tiny": {
        "leads": 5, "payments": 8, "comments": 11}}}))
    assert list(m.batches(5, 2)) == [(1, 2), (3, 4), (5, 5)]
    with pytest.raises(ValueError):
        list(m.batches(10, 0))
    with pytest.raises(ValueError):
        m.insert_sql("invalid;drop")
    class Cur:
        def __init__(self, db):
            self.db=db
            self.value=(0,)
        def __enter__(self):
            return self
        def __exit__(self, *_):
            return False
        def execute(self, statement, args=None):
            sql=statement.lower()
            if "insert into stress." in sql and args is not None:
                table=statement.split("stress.")[1].split("(")[0]
                self.db.counts[table]=max(self.db.counts[table], args[-1])
            elif "select count(*) from stress." in sql and "left join" not in sql:
                table=sql.split("stress.")[1].split()[0]
                self.value=(self.db.counts[table],)
            elif "left join" in sql:
                self.value=(self.db.orphans,)
            elif "pg_database_size" in sql:
                self.value=(8192,)
            else:
                self.value=(1,)
        def fetchone(self):
            return self.value
    class Conn:
        def __init__(self, orphans=0, mismatch=False):
            self.orphans=orphans
            self.counts={"leads":0,"payments":0,"comments":0}
            self.commits=0
            self.mismatch=mismatch
        def __enter__(self):
            return self
        def __exit__(self, *_):
            return False
        def cursor(self):
            return Cur(self)
        def commit(self):
            self.commits+=1
    conn=Conn()
    monkeypatch.setitem(sys.modules, "psycopg",
                        types.SimpleNamespace(connect=lambda _: conn))
    report=tmp_path/"reports"/"stress.json"
    out=m.execute("fake", "tiny", 4, report, config)
    assert out["status"] == "PASS"
    assert [out["counts"][s]["rows"] for s in m.TABLES] == [5,8,11]
    assert out["postgres_database_bytes"] == 8192
    assert conn.commits >= 9
    assert json.loads(report.read_text())["simulation"] is False
    with pytest.raises(ValueError):
        m.execute("fake", "unknown", 1, report, config)
    bad=tmp_path/"bad.json"
    bad.write_text('{"profiles":{"tiny":{"leads":true,"payments":1,"comments":1}}}')
    with pytest.raises(ValueError, match="Invalid PostgreSQL stress profile"):
        m.read_profiles(bad)
    broken=Conn(orphans=1)
    monkeypatch.setitem(sys.modules, "psycopg",
                        types.SimpleNamespace(connect=lambda _: broken))
    with pytest.raises(AssertionError, match="Foreign-key"):
        m.execute("fake", "tiny", 4, report, config)


def test_population_drift_cli_and_rejection(tmp_path, monkeypatch, capsys):
    import runpy
    from sunset.drift import categorical_psi
    assert categorical_psi(["A","B","A"], ["A","B","A"]) == pytest.approx(0)
    assert categorical_psi(["A","A"],["B","B"]) > 5
    for pair in [([],["A"]), (["A"],[])]:
        with pytest.raises(ValueError):
            categorical_psi(*pair)
    ref=tmp_path/"reference.csv"
    new=tmp_path/"new.csv"
    ref.write_text("source\nA\nA\nB\n")
    new.write_text("source\nA\nB\nB\n")
    monkeypatch.setattr(sys,"argv",["drift","--reference",str(ref),
                                    "--current",str(new),"--column","source"])
    runpy.run_module("sunset.drift",run_name="__main__")
    assert json.loads(capsys.readouterr().out)["psi"] > 0
    monkeypatch.setattr(sys,"argv",["drift","--reference",str(ref),
                                    "--current",str(new),"--column","missing"])
    with pytest.raises(ValueError,match="Unknown column"):
        runpy.run_module("sunset.drift",run_name="__main__")


def test_streaming_transformation_main_builds_checkpointed_two_sink_queries(monkeypatch):
    from streaming import structured_streaming as m
    # Fake the Spark DSL, run its complete logical-graph construction and
    # assert that DB and Kafka streams have disjoint checkpoints.
    operations=[]
    class Col:
        def __init__(self,label=""):
            self.label=label
        def cast(self, *_):
            return self
        def alias(self, *_):
            return self
        def isNotNull(self):
            return self
        def rlike(self, *_):
            return self
        def __and__(self, _):
            return self
    class F:
        @staticmethod
        def col(name): return Col(name)
        @staticmethod
        def from_json(*args): return Col("parsed")
        @staticmethod
        def lit(_): return Col("literal")
        @staticmethod
        def when(*args): return F
        @staticmethod
        def otherwise(*args): return Col("otherwise")
        @staticmethod
        def to_json(*args): return Col("json")
        @staticmethod
        def struct(*args): return Col("struct")
    class Data:
        columns=["call_id","event_id","has_alert","transcript"]
        def format(self,n): operations.append(("format",n));return self
        def option(self,k,v): operations.append(("option",k,v));return self
        def load(self): return self
        def select(self,*_):return self
        def filter(self,*_):return self
        def withColumn(self,*_):return self
        @property
        def writeStream(self):return self
        def foreachBatch(self,callback):
            operations.append(("foreachBatch",callback))
            return self
        def start(self):operations.append(("start",));return self
        def isEmpty(self):return False
        @property
        def write(self):return self
        def save(self):operations.append(("save",))
    data=Data()
    class Session:
        builder=types.SimpleNamespace(appName=lambda *_: types.SimpleNamespace(
            getOrCreate=lambda: session))
        def __init__(self):
            self.sparkContext=types.SimpleNamespace(setLogLevel=lambda x: operations.append(("log",x)))
            self.readStream=data
            self.streams=types.SimpleNamespace(awaitAnyTermination=lambda: operations.append(("await",)))
    session=Session()
    class T:
        @staticmethod
        def StructType(fields):return fields
        @staticmethod
        def StructField(*args):return args
        @staticmethod
        def StringType():return "string"
        @staticmethod
        def LongType():return "long"
        @staticmethod
        def IntegerType():return "integer"
    spark_sql=types.ModuleType("pyspark.sql")
    spark_sql.SparkSession=Session
    spark_sql.functions=F
    spark_sql.types=T
    monkeypatch.setitem(sys.modules,"pyspark",types.ModuleType("pyspark"))
    monkeypatch.setitem(sys.modules,"pyspark.sql",spark_sql)
    monkeypatch.setenv("CHECKPOINT_DIR","/tmp/checkpoints")
    m.main()
    ckpts=[entry[2] for entry in operations if len(entry) == 3
           and entry[0] == "option" and entry[1] == "checkpointLocation"]
    assert ckpts == ["/tmp/checkpoints/postgres","/tmp/checkpoints/kafka"]
    assert len([x for x in operations if x[0]=="start"]) == 2
    assert ("await",) in operations
    publish=[x[1] for x in operations if x[0]=="foreachBatch"][-1]
    publish(data,0)
    assert len([x for x in operations if x[0]=="save"]) == 2
