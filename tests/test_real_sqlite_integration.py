"""Integration tests of a real local SQLite engine (NOT PostgreSQL runtime)."""
from pathlib import Path
import csv
import json
import shutil
import sqlite3
import pytest
from sunset.generator import build
from sunset.verify_end_to_end_sqlite import verify, marts, reconcile, load_raw
from sunset.config import load

CFG=Path(__file__).resolve().parents[1]/'config/pipeline.json'


def test_end_to_end_and_exact_replay(tmp_path):
    source=tmp_path/'src'
    build(source,150,110,210,seed=13,chunk_size=19)
    outcome=verify(source,tmp_path/'test.sqlite',CFG,23)
    assert outcome['status']=='PASS'
    assert outcome['reconciliation']['warehouse_counts']['fact_ventas']==21
    assert outcome['reconciliation']['warehouse_counts']['fact_tours']==105
    assert outcome['reconciliation']['warehouse_counts']['fact_interacciones']==210
    assert outcome['reconciliation']['warehouse_counts']['fact_fx']==1462
    assert outcome['reconciliation']['foreign_key_violations']==0
    assert outcome['identical_replay_hash'] is True
    assert outcome['second_run']['scd2_versions_inserted']=={'cliente':0,'sala':0}


def test_scd2_change_preserves_historical_facts(tmp_path):
    source=tmp_path/'src';build(source,120,70,130,seed=7)
    db_path=tmp_path/'scenario.sqlite'
    verify(source,db_path,CFG,30)
    with sqlite3.connect(db_path) as db:
        old=db.execute("SELECT cliente_key FROM dim_cliente WHERE cliente_id='C000000001' AND is_current=1").fetchone()[0]
        before=db.execute('SELECT COUNT(*) FROM fact_interacciones WHERE cliente_key=?',(old,)).fetchone()[0]
        db.execute("UPDATE raw_clientes SET segmento='VIP_TEST' WHERE cliente_id='C000000001'")
        result=marts(db,'2026-10-18T00:00:00')
        assert result['scd2_versions_inserted']['cliente']==1
        history=db.execute("SELECT cliente_key,segmento,valid_from,valid_to,is_current FROM dim_cliente WHERE cliente_id='C000000001' ORDER BY cliente_key").fetchall()
        assert len(history)==2
        assert history[0][3]=='2026-10-18T00:00:00' and history[0][4]==0
        assert history[1][1]=='VIP_TEST' and history[1][4]==1
        # Historical 2024-2025 interactions stay linked to historical version.
        assert db.execute('SELECT COUNT(*) FROM fact_interacciones WHERE cliente_key=?',(old,)).fetchone()[0]==before
        assert db.execute('PRAGMA foreign_key_check').fetchall()==[]


def test_quality_blocks_corrupt_payment(tmp_path):
    src=tmp_path/'src';build(src,70,50,65)
    p=src/'pagos.csv'
    with p.open(newline='',encoding='utf-8') as f:
        r=csv.DictReader(f); cols=r.fieldnames; rows=list(r)
    rows[0]['contrato_id']='MISSING_ID'
    with p.open('w',newline='',encoding='utf-8') as f:
        wr=csv.DictWriter(f,fieldnames=cols);wr.writeheader();wr.writerows(rows)
    with pytest.raises(ValueError,match='Quality gate blocks warehouse'):
        verify(src,tmp_path/'reject.sqlite',CFG,100)
