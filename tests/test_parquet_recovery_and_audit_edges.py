"""Edge-path integration of partitioning, manifest publication and quality.

Uses genuine CSV, PyArrow files, SQLite audit and temporary directories.
"""
from __future__ import annotations

import csv
import json
import shutil
import sys
from pathlib import Path

import pytest


def _parquet_env(root):
    input_dir=root/"input"
    output_dir=root/"lake"
    input_dir.mkdir(exist_ok=True)
    config=root/"pipeline.json"
    spec={"sources":{"data":{"target":"bronze_data","primary_key":["id"],
        "required":["required"],"date_column":"updated","snapshot_date":"2026-10-01"}},
        "parquet":{"max_rows_per_file":2,"batch_size":3,"compression":"snappy"}}
    config.write_text(json.dumps(spec))
    path=input_dir/"data.csv"
    path.write_text("id,required,updated\n1,x,2026-10-01\n2,y,2026-10-02\n3,z,2026-10-03\n4,z,2026-10-03\n")
    return input_dir,output_dir,config,path,spec


def test_parquet_real_snapshot_replay_partition_rotation_and_failed_commit(tmp_path,monkeypatch):
    import pyarrow.parquet as pq
    from sunset.parquet import convert
    input_dir,lake,conf,path,spec=_parquet_env(tmp_path)
    first=convert(input_dir,lake,conf,"data")["data"]
    assert first["rows"] == 4
    assert first["files"] == 2
    assert first["partitions"] == 1
    base=lake/"bronze_data"
    assert json.loads((base/"_manifest.json").read_text()) == first
    assert sum(pq.read_metadata(f).num_rows for f in base.rglob("*.parquet"))==4
    # Replacement not append: staging and prior snapshot disappear on success.
    path.write_text("id,required,updated\n5,t,2026-11-01\n")
    second=convert(input_dir,lake,conf,"data")["data"]
    assert second["rows"] == 1
    assert sorted(p.name for p in base.iterdir() if p.is_dir())==["year=2026"]
    assert not list(lake.glob("*.backup"))
    # Simulated POSIX commit failure must restore the last good snapshot.
    original=Path.rename
    def broken_rename(self, target):
        if self.name.endswith(".tmp"):
            raise OSError("cannot atomically publish")
        return original(self,target)
    monkeypatch.setattr(Path,"rename",broken_rename)
    path.write_text("id,required,updated\n6,t,2026-12-01\n")
    with pytest.raises(OSError,match="cannot atomically publish"):
        convert(input_dir,lake,conf,"data")
    assert json.loads((base/"_manifest.json").read_text())["rows"]==1
    assert not list(lake.glob(".*.tmp"))


@pytest.mark.parametrize("bad,expected",[
    ("id,required,updated\n,ok,2026-10-01\n","natural key"),
    ("id,required,updated\n1,,2026-10-01\n","required value"),
    ("id,required,updated\n1,ok,not-a-date\n","Invalid ISO"),
    ("id,required,updated\n1,ok\n","Malformed CSV"),
    ("id,required,updated\n1,ok,2026-10-01,too-long\n","Malformed CSV"),
    ("id,id,required,updated\n1,1,ok,2026-10-01\n","Invalid or duplicated"),
    ("id,required\n1,ok\n","missing date column"),
    ("required,updated\nok,2026-10-01\n","missing required columns"),
])
def test_parquet_rejects_invalid_rows_without_publishing(tmp_path,bad,expected):
    from sunset.parquet import convert
    input_dir,lake,conf,path,_=_parquet_env(tmp_path)
    path.write_text(bad)
    with pytest.raises(ValueError,match=expected):
        convert(input_dir,lake,conf,"data")
    assert not (lake/"bronze_data").exists()


def test_parquet_contract_errors_and_command_line(tmp_path,monkeypatch,capsys):
    from sunset import parquet
    input_dir,lake,conf,path,spec=_parquet_env(tmp_path)
    with pytest.raises(ValueError,match="Undeclared"):
        parquet.convert(input_dir,lake,conf,"unknown")
    path.unlink()
    with pytest.raises(FileNotFoundError):
        parquet.convert(input_dir,lake,conf,"data")
    path.write_text("id,required,updated\n1,ok,2026-10-01\n")
    spec["parquet"]["max_rows_per_file"]=0
    conf.write_text(json.dumps(spec))
    with pytest.raises(ValueError,match="max_rows_per_file"):
        parquet.convert(input_dir,lake,conf,"data")
    spec["parquet"]["max_rows_per_file"]=2
    conf.write_text(json.dumps(spec))
    monkeypatch.setattr(sys,"argv",["parquet","--input",str(input_dir),
      "--output",str(lake),"--config",str(conf),"--source","data"])
    parquet.main()
    assert json.loads(capsys.readouterr().out)["data"]["rows"]==1
    for bad_date in ["not-a-date",None]:
        with pytest.raises(ValueError,match="Invalid ISO"):
            parquet.period_for(bad_date)


def test_audit_detects_corruption_and_duplicates_in_real_sample(tmp_path):
    from sunset.audit import audit
    src=Path("data/sample")
    shutil.copytree(src,tmp_path/"sample")
    sample=tmp_path/"sample"
    good=audit(sample)
    assert good["status"]=="PASS"
    csv_path=sample/"fuentes.csv"
    with csv_path.open("r",newline="") as f:
        rows=list(csv.DictReader(f))
        fields=list(rows[0])
    with csv_path.open("a",newline="") as f:
        writer=csv.DictWriter(f,fields)
        writer.writerow(rows[0])
    damaged=audit(sample)
    assert damaged["status"]=="FAIL"
    assert damaged["duplicate_keys"]["fuentes"]>=1
    csv_path.write_text("invalid,header\nA,B\n")
    with pytest.raises(ValueError,match="Cabecera"):
        audit(sample)


def test_quality_contract_invalid_source_and_empty_inputs(tmp_path):
    from sunset.quality import inspect_csv
    source=tmp_path/"empty.csv"
    source.write_text("id,date\n")
    data,errors=inspect_csv(source,{"primary_key":["id"],"required":["id"],"date_column":"date"},"synthetic")
    assert data["rows"]==0
    assert any("empty" in error for error in errors)
    source.write_text("id,date\nA,2026-99-99\nA,wrong\n")
    data,errors=inspect_csv(source,{"primary_key":["id"],"date_column":"date"},"synthetic")
    assert data["duplicate_keys"]==1
    assert data["invalid_dates"]==2
    assert len(errors)>=2


def test_rag_chunk_overlap_and_parquet_source_reader(tmp_path):
    import pyarrow as pa
    import pyarrow.parquet as pq
    from rag.build_embeddings import (
        chunks, redact, iter_parquet_comments, chunk_identity, digest
    )
    assert len(chunks("abcdefghij",size=6,overlap=2))==2
    assert chunks("")==[]
    with pytest.raises(ValueError):
        chunks("hello",size=4,overlap=4)
    assert "[EMAIL]" in redact("Contact test@example.com")
    assert chunk_identity("comment","X",0,"m")!=chunk_identity("comment","X",1,"m")
    assert len(digest("hello"))==64
    lake=tmp_path/"lake"
    folder=lake/"comentarios"/"year=2026"/"month=10"
    folder.mkdir(parents=True)
    with pytest.raises(FileNotFoundError):
        list(iter_parquet_comments(lake))
    (lake/"comentarios"/"_manifest.json").write_text('{"source":"comentarios"}')
    pq.write_table(pa.table({"comentario_id":["A","B"],"texto":["hello","world"]}),
                   folder/"data.parquet")
    rows=list(iter_parquet_comments(lake,batch_size=1))
    assert [r["comentario_id"] for r in rows]==["A","B"]
