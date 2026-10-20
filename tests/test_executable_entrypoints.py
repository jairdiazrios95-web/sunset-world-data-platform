"""Exercise real CLI control flow and synthetic SQLite with no private access."""
import json
import runpy
import sys
from pathlib import Path
import pytest


def test_quality_cli_both_exit_codes(tmp_path,monkeypatch,capsys):
    from sunset import quality as m
    out=tmp_path/"quality.json"
    monkeypatch.setattr(sys,"argv",["quality","--input",str(tmp_path),
                        "--scope","core","--report",str(out)])
    for status,code in [("PASS",0),("FAIL",1)]:
        monkeypatch.setattr(m,"check",lambda *_a, status=status: {
            "status":status,"checks_evaluated_at_least":14,"errors":[]})
        with pytest.raises(SystemExit) as exc:m.main()
        assert exc.value.code==code
        assert json.loads(out.read_text())["status"]==status


def test_rag_cli_output_and_invalid_chunks(tmp_path,monkeypatch,capsys):
    from rag import build_embeddings as m
    monkeypatch.setenv("SUNSET_PG_DSN","ci-only")
    monkeypatch.setattr(m,"index_parquet",lambda *_args:{
        "status":"PASS","dimension":384,"embedded":2})
    monkeypatch.setattr(sys,"argv",["rag","--lake",str(tmp_path),
                        "--model","fixture","--chunk-size","120","--overlap","10"])
    m.main()
    assert json.loads(capsys.readouterr().out)["embedded"]==2
    with pytest.raises(ValueError):m.chunks("x",size=0)
    with pytest.raises(ValueError):m.chunks("x",size=5,overlap=5)


def test_loader_cli_invokes_real_boundary(tmp_path,monkeypatch,capsys):
    from warehouse import loader
    from sunset import config
    monkeypatch.setattr(config,"load",lambda *_:{
        "sources":{"demo":{"primary_key":["id"]}}})
    monkeypatch.setattr(loader,"load_csv",lambda *_args:{
        "source":"demo","rows_read":2,"batches":1,"result":"LOAD_COMPLETED_NOT_FULLY_RECONCILED"})
    monkeypatch.setattr(sys,"argv",["loader","--source","demo",
                        "--input-dir",str(tmp_path),"--batch-size","2"])
    loader.main()
    assert json.loads(capsys.readouterr().out)["rows_read"]==2
    for identifier in ["bad-name","", "DROP TABLE"]:
        with pytest.raises(ValueError):loader.identifier(identifier)


def test_verifier_clis_parse_args_and_emit_json(tmp_path,monkeypatch,capsys):
    from scripts import verify_real_services as platform
    from scripts import verify_airflow_live as airflow
    monkeypatch.setattr(platform,"execute",lambda _root,_report,repeats,dbt:{
        "status":"PASS","repeats":repeats,"dbt_executed":dbt})
    monkeypatch.setattr(sys,"argv",["verify","--root",str(tmp_path),
        "--report",str(tmp_path/"r.json"),"--repeats","3","--dbt"])
    platform.main()
    assert json.loads(capsys.readouterr().out)["dbt_executed"] is True
    monkeypatch.setattr(airflow,"verify",lambda *_args:{"status":"PASS"})
    monkeypatch.setattr(sys,"argv",["airflow","--quality",str(tmp_path/"a"),
        "--dbt-results",str(tmp_path/"b"),"--report",str(tmp_path/"c")])
    airflow.main()
    assert json.loads(capsys.readouterr().out)["status"]=="PASS"


def test_offline_services_main_real_sqlite_idempotent(tmp_path,monkeypatch,capsys):
    source=tmp_path/"comments.csv"
    source.write_text("comentario_id,texto\\nC1,Excelente atención\\nC2,Información de pagos\\n")
    report=tmp_path/"offline.json"
    monkeypatch.setattr(sys,"argv",["offline-services","--comments",str(source),
        "--db",str(tmp_path/"events.sqlite"),"--report",str(report)])
    runpy.run_module("sunset.offline_services",run_name="__main__")
    data=json.loads(report.read_text())
    assert data["status"]=="PASS"
    assert data["replay_idempotent"] is True
    assert data["kafka"]=="NOT_EXECUTED"


def test_offline_demo_main_sqlite_is_explicit_not_postgres(tmp_path,monkeypatch,capsys):
    report=tmp_path/"demo.json"
    monkeypatch.setattr(sys,"argv",["offline-demo","--input","data/sample",
        "--config","config/pipeline.json","--report",str(report)])
    runpy.run_module("sunset.offline_demo",run_name="__main__")
    result=json.loads(report.read_text())
    assert result["status"]=="PASS"
    assert result["postgres_execution"]=="NOT_EXECUTED"
