"""Full path unit tests for Airflow TaskFlow DAG and SQL-Parquet acceptance.

The integration CI still runs physical services. These tests probe failure
branches and orchestration with replaced network/process boundaries.
"""
from __future__ import annotations

import importlib.util
import json
import os
import sys
import types
from pathlib import Path

import pytest


def test_airflow_dag_taskflow_success_and_all_failure_branches(monkeypatch, tmp_path):
    registry={}
    class DAG:
        def __init__(self, *args, **kwargs):
            self.kwargs=kwargs
        def __enter__(self):
            return self
        def __exit__(self, *_):
            return False
    class Wrapper:
        def __init__(self, fn):
            self.fn=fn
        def override(self, **_kw):
            return self
        def __call__(self, *_a, **_kw):
            # Decorated TaskFlow syntax registers graph, does not execute work.
            return types.SimpleNamespace(placeholder=True)
    def task(fn):
        registry[fn.__name__]=fn
        return Wrapper(fn)
    class AirflowException(Exception):
        pass
    fake_airflow=types.ModuleType("airflow")
    fake_airflow.DAG=DAG
    fake_decorators=types.ModuleType("airflow.decorators")
    fake_decorators.task=task
    fake_exceptions=types.ModuleType("airflow.exceptions")
    fake_exceptions.AirflowException=AirflowException
    fake_pendulum=types.ModuleType("pendulum")
    import datetime
    fake_pendulum.datetime=lambda *a, **k: datetime.datetime(*a)
    for name,value in {
        "airflow":fake_airflow, "airflow.decorators":fake_decorators,
        "airflow.exceptions":fake_exceptions, "pendulum":fake_pendulum
    }.items():
        monkeypatch.setitem(sys.modules, name, value)
    spec=importlib.util.spec_from_file_location("sunset_ci_dag", 
                                                  "airflow/dags/etl_sunset.py")
    module=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert set(registry) == {
        "ingest_source", "ingest_support_sources", "run_marts", "run_dbt",
        "validate_quality"
    }
    assert module.dag.kwargs["schedule"] == "0 2 * * *"
    assert module.DEFAULT_ARGS["retries"] >= 0
    calls=[]
    def run(*args):
        calls.append(args)
        if "sunset.parquet" in args:
            source=args[args.index("--source")+1]
            return json.dumps({source:{"rows":3}})
        if "warehouse.load_parquet" in args:
            return json.dumps({"status":"PASS","parquet_rows":3})
        if "warehouse.build_marts" in args:
            return json.dumps({"status":"PASS", "row_counts":{"dw.fact_ventas":3}})
        return "checked"
    monkeypatch.setattr(module,"run",run)
    leads=registry["ingest_source"]("leads_ventas")
    fx=registry["ingest_source"]("fx_historico")
    assert leads["warehouse"]["status"] == "PASS"
    support=registry["ingest_support_sources"](leads,fx)
    assert len(support["support_counts"]) == 8
    assert support["leads"] == 3 and support["fx"] == 3
    marts=registry["run_marts"](support)
    assert marts["marts"]["status"] == "PASS"
    dbt=registry["run_dbt"](marts)
    assert dbt["dbt"] == "PASS"
    assert registry["validate_quality"](dbt)["quality"] == "PASS"
    assert sum("dbt" in call[0] for call in calls) == 2
    monkeypatch.setattr(module,"SOURCE_MODE","oracle")
    with pytest.raises(AirflowException, match="Oracle mode"):
        registry["ingest_source"]("leads_ventas")
    monkeypatch.setattr(module,"SOURCE_MODE","csv")
    with pytest.raises(AirflowException, match="incomplete"):
        registry["ingest_support_sources"](
            {"warehouse":{"status":"FAIL"},"lake":{"rows":1}},fx)
    monkeypatch.setattr(module,"run",lambda *_: json.dumps({"status":"FAIL"}))
    with pytest.raises(AirflowException, match="Mart reconciliation"):
        registry["run_marts"]({})
    with pytest.raises(AirflowException, match="dbt not successful"):
        registry["validate_quality"]({"dbt":"FAIL"})
    monkeypatch.setattr(module,"run",lambda *_: "not JSON")
    with pytest.raises(json.JSONDecodeError):
        registry["ingest_source"]("leads_ventas")
    # Exercise subprocess logging, nonzero exit and optional alert callback.
    class Process:
        returncode=0
        stdout="success"*1000
        stderr=""
    monkeypatch.setattr(module.subprocess,"run",lambda *_a,**_k: Process())
    assert len(module.run("python","--version")) == 7000
    class Failure(Process):
        returncode=1
        stderr="unit-test-failure"
    monkeypatch.setattr(module.subprocess,"run",lambda *_a,**_k: Failure())
    with pytest.raises(AirflowException,match="unit-test-failure"):
        module.run("python","bad")
    monkeypatch.delenv("ALERT_WEBHOOK_URL",raising=False)
    module.alert({})
    monkeypatch.setenv("ALERT_WEBHOOK_URL","http://localhost/hook")
    messages=[]
    class Response:
        def __enter__(self):return self
        def __exit__(self,*_):return False
        def read(self):return b"ok"
    def urlopen(request, timeout):
        messages.append(json.loads(request.data))
        return Response()
    monkeypatch.setattr(module,"urlopen",urlopen)
    module.alert({"dag":types.SimpleNamespace(dag_id="etl_sunset"),
                  "task_instance":types.SimpleNamespace(task_id="extract_fx"),
                  "run_id":"ci"})
    assert "extract_fx" in messages[0]["text"]


def test_real_platform_acceptance_two_passes_and_failure_modes(tmp_path, monkeypatch):
    from scripts import verify_real_services as m
    from sunset import config as cfg
    from sunset import parquet as conversion
    from warehouse import loader, build_marts
    source=tmp_path/"data"/"sample"
    source.mkdir(parents=True)
    cfg_dir=tmp_path/"config"
    cfg_dir.mkdir()
    (cfg_dir/"pipeline.json").write_text("{}")
    sql_dir=tmp_path/"sql"
    sql_dir.mkdir()
    for name in ("001_init.sql","003_upsert_patterns.sql","005_streaming.sql"):
        (sql_dir/name).write_text("SELECT 1;")
    for name in ("leads_ventas","fx_historico"):
        (source/f"{name}.csv").write_text("id,value\n1,A\n2,B\n")
    config={"sources":{"leads_ventas":{"target":"leads"},
                       "fx_historico":{"target":"fx"}},
            "warehouse":{"fiscal_year_start_month":4}}
    monkeypatch.setattr(cfg,"load",lambda *_:config)
    monkeypatch.setenv("SUNSET_PG_DSN","postgresql://ci-only")
    meta=types.ModuleType("pyarrow.parquet")
    meta.read_metadata=lambda _: types.SimpleNamespace(num_rows=2)
    monkeypatch.setitem(sys.modules,"pyarrow.parquet",meta)
    pyarrow=types.ModuleType("pyarrow")
    pyarrow.__path__=[]
    monkeypatch.setitem(sys.modules,"pyarrow",pyarrow)
    def convert(src,lake,_config,name):
        target=lake/config["sources"][name]["target"]/"year=2026"/"month=10"
        target.mkdir(parents=True,exist_ok=True)
        (target/"part.parquet").write_bytes(b"parquet-data")
        return {name:{"rows":2}}
    monkeypatch.setattr(conversion,"convert",convert)
    monkeypatch.setattr(loader,"load_parquet",lambda *_a: {
        "parquet_rows":2,"destination_rows":2})
    monkeypatch.setattr(build_marts,"build",lambda *_a: {
        "status":"PASS","row_counts":{"fact_ventas":2}})
    class Cursor:
        def __init__(self):
            self.answer=(0,)
            self.sql=[]
        def __enter__(self):return self
        def __exit__(self,*_):return False
        def execute(self,s,*args,**kwargs):
            self.sql.append(s)
            if "SELECT extversion" in s:
                self.answer=("0.8.0",)
            elif "SELECT chunk_id" in s:
                self.answer=("LIVE-CI-FIXTURE",0.0)
            elif "COUNT(*) FROM raw." in s:
                self.answer=(2,)
            else:
                self.answer=(0,)
        def fetchone(self):return self.answer
    cur=Cursor()
    class Conn:
        def __enter__(self):return self
        def __exit__(self,*_):return False
        def cursor(self):return cur
    monkeypatch.setitem(sys.modules,"psycopg",types.SimpleNamespace(
        connect=lambda *_a,**_kw:Conn()))
    start=Path.cwd()
    monkeypatch.setattr(m,"_cmd",lambda *args: cur.sql.append(" ".join(args)))
    try:
        report=tmp_path/"reports"/"live.json"
        result=m.execute(tmp_path,report,repeats=2,dbt=True)
        assert result["status"] == "PASS"
        assert result["vector_extension"] == "0.8.0"
        assert result["repeats"] == 2 and result["dbt_executed"]
        assert sum("dbt run" in s or "dbt test" in s for s in cur.sql) == 2
        assert json.loads(report.read_text())["simulation"] is False
        with pytest.raises(ValueError,match="replay"):
            m.execute(tmp_path,report,repeats=1)
        monkeypatch.delenv("SUNSET_PG_DSN")
        with pytest.raises(RuntimeError,match="SUNSET_PG_DSN"):
            m.execute(tmp_path,report,repeats=2)
        monkeypatch.setenv("SUNSET_PG_DSN","postgresql://ci-only")
        monkeypatch.setattr(loader,"load_parquet",lambda *_a:{"parquet_rows":0})
        with pytest.raises(AssertionError,match="landing mismatch"):
            m.execute(tmp_path,report,repeats=2)
    finally:
        os.chdir(start)


def test_platform_dbt_subprocess_exposes_errors(monkeypatch):
    from scripts import verify_real_services as m
    class Result:
        returncode=1
        stderr="Database failure in dbt SQL"
    monkeypatch.setattr(m.subprocess,"run",lambda *_a,**_k:Result())
    with pytest.raises(RuntimeError,match="Database failure"):
        m._cmd("dbt","test","--project-dir","dbt")
    Result.returncode=0
    m._cmd("dbt","run")
