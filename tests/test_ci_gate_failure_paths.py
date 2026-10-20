"""Prove CI gate fails closed on missing or invalid evidence and API faults."""
from __future__ import annotations

import json
import sys
from pathlib import Path
from urllib.error import HTTPError

import pytest


def test_github_rest_api_handles_empty_dispatch_and_cli_errors(monkeypatch):
    from scripts import verify_two_acceptance_runs as g
    class Response:
        status=204
        def __enter__(self):return self
        def __exit__(self,*_a):return False
    calls=[]
    def urlopen(req,timeout):
        calls.append((req.method,req.full_url,timeout))
        return Response()
    monkeypatch.setattr("urllib.request.urlopen",urlopen)
    monkeypatch.setenv("GH_TOKEN","example-token-local-only")
    assert g.api("repos/owner/project/actions/workflows/ci.yml/dispatches",
                 method="POST",fields=("ref=main",)) is None
    assert calls[0][0]=="POST"
    def bad(_req,timeout=None):
        raise HTTPError("https://api.github.com",403,"Forbidden",{},None)
    monkeypatch.setattr("urllib.request.urlopen",bad)
    with pytest.raises(RuntimeError,match="HTTP 403"):
        g.api("repos/owner/project/actions/workflows/ci.yml/dispatches",
              method="POST",fields=("ref=main",))
    class Result:
        returncode=0
        stdout='{"workflow_runs":[]}'
        stderr=""
    monkeypatch.setattr(g.subprocess,"run",lambda *_a,**_kw:Result())
    assert g.api("repos/owner/actions/runs")["workflow_runs"]==[]
    Result.stdout=" "
    assert g.api("repos/owner/actions/runs") is None
    Result.returncode=1
    Result.stderr="Not Found"
    with pytest.raises(RuntimeError,match="GitHub API"):
        g.api("repos/owner/actions/runs")


def test_github_poll_run_timeout_and_bad_status(monkeypatch):
    from scripts import verify_two_acceptance_runs as g
    monkeypatch.setattr(g,"list_runs",lambda *_a:[])
    with pytest.raises(TimeoutError,match="independent"):
        g.poll_run("repo","sha","push",set(),g.time.monotonic()-1)
    monkeypatch.setattr(g,"list_runs",lambda *_a:[
        {"name":g.WORKFLOW,"event":"push","head_sha":"sha","id":21}])
    assert g.poll_run("repo","sha","push",set(),g.time.monotonic()+1)==21
    run={"head_sha":"sha","status":"completed","conclusion":"failure",
         "html_url":"local"}
    monkeypatch.setattr(g,"api",lambda *_a,**_kw:run)
    with pytest.raises(AssertionError,match="finished"):
        g.validate_run("repo",21,"sha",g.time.monotonic()+1)
    with pytest.raises(TimeoutError):
        g.validate_run("repo",21,"sha",g.time.monotonic()-1)
    def api_missing(route,*_a,**_k):
        if route.endswith("/jobs?per_page=100"):
            return {"jobs":[{"name":n,"conclusion":"success"}
                            for n in g.EXPECTED_JOBS]}
        if route.endswith("/artifacts?per_page=100"):
            return {"artifacts":[]}
        return {**run,"conclusion":"success"}
    monkeypatch.setattr(g,"api",api_missing)
    with pytest.raises(AssertionError,match="lacks acceptance"):
        g.validate_run("repo",21,"sha",g.time.monotonic()+1)


def _real_proofs():
    return {
        "PostgreSQL/Parquet/dbt/pgvector":{
            "status":"PASS","simulation":False,"engine":"PostgreSQL",
            "dbt_executed":True,"vector_extension":"0.8",
            "counts":{"raw":{str(i):1 for i in range(10)},"parquet":{}}},
        "MinIO":{"status":"PASS","simulation":False,"engine":"S3",
                 "files":3,"parquet_rows":20,"replay_objects":3},
        "Kafka/Spark":{"status":"PASS","simulation":False,"engine":"Kafka+Spark",
                       "events":5,"alerts":2,"postgres_rows_after_replay":5,
                       "postgres_alerts_after_replay":2},
        "Airflow":{"status":"PASS","simulation":False,"engine":"Airflow",
                   "dbt_test_count":8,"source_count":10,
                   "relation_rows":{"dw.fact_ventas":10},"orphan_sales":0},
        "RAG con modelo real":{"status":"PASS","simulation":False,
          "engine":"HuggingFace+pgvector","embedding_dimension":384,
          "first_run":{"embedded":2},"replay":{"embedded":0},"retrieved":2},
    }


def test_strict_acceptance_gate_all_types_and_missing_corrupt_proofs(tmp_path,monkeypatch):
    from scripts import acceptance_gate as gate
    proofs=_real_proofs()
    for name,filename in gate.REQUIREMENTS.items():
        (tmp_path/filename).write_text(json.dumps(proofs[name]))
        assert gate._credible(name,proofs[name])
    assert gate.acceptance(tmp_path)["status"]=="PASS"
    assert not gate._credible("unknown",{"status":"PASS","engine":"yes",
                                       "simulation":False})
    wrong=[
        ("MinIO",{"files":True}),
        ("MinIO",{"replay_objects":5}),
        ("Kafka/Spark",{"events":4}),
        ("Airflow",{"orphan_sales":10}),
        ("Airflow",{"relation_rows":{"x":False}}),
        ("RAG con modelo real",{"embedding_dimension":768}),
        ("RAG con modelo real",{"first_run":{"embedded":1}}),
        ("PostgreSQL/Parquet/dbt/pgvector",{"dbt_executed":False}),
        ("PostgreSQL/Parquet/dbt/pgvector",{"counts":{"raw":{},"parquet":{}}}),
    ]
    for name,change in wrong:
        data={**proofs[name],**change}
        assert not gate._credible(name,data),name
    name="MinIO"
    filename=gate.REQUIREMENTS[name]
    path=tmp_path/filename
    path.write_text("{invalid json")
    result=gate.acceptance(tmp_path)
    assert result["status"]=="BLOCKED"
    assert result["technology_proof"][name]["status"]=="FAIL"
    path.unlink()
    assert gate.acceptance(tmp_path)["technology_proof"][name]["status"]=="MISSING"
    output=tmp_path/"reports"/"gate.json"
    monkeypatch.setattr(sys,"argv",["gate","--evidence",str(tmp_path),
                                   "--report",str(output)])
    with pytest.raises(SystemExit) as exc:
        gate.main()
    assert exc.value.code==2
    assert json.loads(output.read_text())["approved_for_production"] is False
