"""Acceptance tests for the lightweight SQLite-only stress proxy.
They do not test actual PostgreSQL, Oracle, Kafka, Spark or Parquet APIs.
"""
from __future__ import annotations
import json
from pathlib import Path
import sqlite3
import pytest
from stress.sqlite_load import DEFAULTS, REFERENCES, connect, execute, resume_safety


def small():
    return dict(leads=1000, pagos=1500, comentarios=2200,chunk_size=300,date_days=21,seed=42)


def test_stress_batch_resume_and_warehouse_idempotence(tmp_path:Path):
    db=tmp_path/'resume.db';cfg=small()
    with pytest.raises(InterruptedError):
        execute(db,cfg,None,interrupt_after=3)
    c=connect(db)
    assert c.execute('SELECT count(*) FROM clientes').fetchone()[0]==333
    assert c.execute('SELECT count(*) FROM leads').fetchone()[0]==300
    c.close()
    result=execute(db,cfg,tmp_path/'report.json')
    assert result['status']=='PASS'
    assert result['source_counts']['pagos']==cfg['pagos']
    assert result['warehouse_counts']['fact_interacciones']==cfg['comentarios']
    assert all(not value for value in result['orphan_counts'].values())
    repeat=execute(db,cfg,None)
    assert repeat['status']=='PASS'
    assert all(x['inserted_this_run']==0 for x in repeat['sources'].values())
    assert repeat['warehouse_counts']==result['warehouse_counts']


def test_checkpoint_mismatch_detected(tmp_path:Path):
    db=tmp_path/'corrupt.db';cfg=small()
    execute(db,cfg,None,skip_warehouse=True)
    c=connect(db)
    with c:c.execute('DELETE FROM leads WHERE lead_id=1000')
    with pytest.raises(RuntimeError,match='Checkpoint mismatch'):
        resume_safety(c,'leads')
    c.close()


def test_bad_fk_rejected(tmp_path:Path):
    db=tmp_path/'fk.db';cfg=small()
    execute(db,cfg,None,skip_warehouse=True)
    c=connect(db)
    with pytest.raises(sqlite3.IntegrityError):
        with c:c.execute('INSERT INTO comentarios VALUES(999999,-1,1,1,0)')
    assert c.execute('PRAGMA foreign_key_check').fetchone() is None
    c.close()


def test_profile_has_more_rows_than_original_exam():
    cfg=json.loads((Path(__file__).parents[1]/'config/stress.json').read_text())
    cfg2=json.loads((Path(__file__).parents[1]/'config/stress_2x.json').read_text())
    for key,ref in REFERENCES.items():
        assert cfg[key]>ref
        assert cfg2[key]>=2*ref


def test_fx_conversion_not_zero(tmp_path:Path):
    cfg=small();db=tmp_path/'fx.db'
    execute(db,cfg,None)
    c=connect(db)
    n,minimum=c.execute('SELECT count(*),min(monto_mxn_cent) FROM fact_ventas').fetchone()
    assert n>0 and minimum>0
    c.close()


def test_incompatible_config_reuse_blocked(tmp_path:Path):
    from stress.sqlite_load import validate_config_identity
    db=tmp_path/'config_guard.db';cfg=small()
    c=connect(db)
    assert validate_config_identity(c,cfg)
    with pytest.raises(RuntimeError,match='Different parameters'):
        validate_config_identity(c,{**cfg,'leads':cfg['leads']+1})
    c.close()
