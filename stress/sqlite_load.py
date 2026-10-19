"""Persistent, bounded-memory SQL load STRESS HARNESS; NOT PostgreSQL/Spark/Parquet.

A deliberately independent relational proxy to test deterministic synthetic volume,
referential integrity, idempotent batch resumes, and SQL joins without external services.
Data are materialized as real rows in SQLite. Not an equivalence/compatibility test.
"""
from __future__ import annotations
import argparse
import hashlib
import json
import os
from pathlib import Path
import resource
import sqlite3
import sys
import time
from datetime import datetime, timezone

TABLE_SQL = {
 'clientes': "INSERT INTO clientes WITH RECURSIVE n(i) AS (SELECT ? UNION ALL SELECT i+1 FROM n WHERE i<?) SELECT i,CASE WHEN i%4=0 THEN 'Recurrente' ELSE 'Nuevo' END FROM n",
 'leads': "INSERT INTO leads WITH RECURSIVE n(i) AS (SELECT ? UNION ALL SELECT i+1 FROM n WHERE i<?) SELECT i,((i-1)%?)+1, ((i-1)%3)+1, ((i*13+?)%?)+1,CASE WHEN i%2=1 THEN 'USD' ELSE 'MXN' END,CASE WHEN i<=? AND i%5=0 THEN (4500+(i%12)*150)*100 ELSE 0 END FROM n",
 'tours': "INSERT INTO tours WITH RECURSIVE n(i) AS (SELECT ? UNION ALL SELECT i+1 FROM n WHERE i<?) SELECT i,i FROM n",
 'contratos': "INSERT INTO contratos WITH RECURSIVE n(i) AS (SELECT ? UNION ALL SELECT i+1 FROM n WHERE i<?) SELECT i,i*5,((i*5-1)%?)+1 FROM n",
 'pagos': "INSERT INTO pagos WITH RECURSIVE n(i) AS (SELECT ? UNION ALL SELECT i+1 FROM n WHERE i<?) SELECT i,((i-1)%?)+1,35000, ((i*17+?)%?)+1 FROM n",
 'comentarios': "INSERT INTO comentarios WITH RECURSIVE n(i) AS (SELECT ? UNION ALL SELECT i+1 FROM n WHERE i<?) SELECT i,((i-1)%?)+1,(((i-1)%? )%?)+1, ((i*13+?)%?)+1, i%5 FROM n",
}

DDL="""
CREATE TABLE IF NOT EXISTS run_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS progress (source TEXT PRIMARY KEY, last_id INTEGER NOT NULL DEFAULT 0, updated_utc TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS clientes (cliente_id INTEGER PRIMARY KEY, segmento TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS fx (day INTEGER NOT NULL, moneda TEXT NOT NULL, rate_bps INTEGER NOT NULL CHECK(rate_bps>0), PRIMARY KEY(day,moneda));
CREATE TABLE IF NOT EXISTS leads (
 lead_id INTEGER PRIMARY KEY, cliente_id INTEGER NOT NULL REFERENCES clientes(cliente_id),
 sala_id INTEGER NOT NULL, day INTEGER NOT NULL, moneda TEXT NOT NULL,
 monto_cent INTEGER NOT NULL CHECK(monto_cent>=0));
CREATE TABLE IF NOT EXISTS tours (tour_id INTEGER PRIMARY KEY, lead_id INTEGER NOT NULL REFERENCES leads(lead_id));
CREATE TABLE IF NOT EXISTS contratos (
 contrato_id INTEGER PRIMARY KEY, lead_id INTEGER NOT NULL REFERENCES leads(lead_id),
 cliente_id INTEGER NOT NULL REFERENCES clientes(cliente_id));
CREATE TABLE IF NOT EXISTS pagos (
 pago_id INTEGER PRIMARY KEY, contrato_id INTEGER NOT NULL REFERENCES contratos(contrato_id),
 monto_cent INTEGER NOT NULL CHECK(monto_cent>0), day INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS comentarios (
 comentario_id INTEGER PRIMARY KEY, tour_id INTEGER NOT NULL REFERENCES tours(tour_id),
 cliente_id INTEGER NOT NULL REFERENCES clientes(cliente_id), day INTEGER NOT NULL, text_code INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS fact_ventas (
 venta_id INTEGER PRIMARY KEY, contrato_id INTEGER NOT NULL, cliente_id INTEGER NOT NULL,
 sala_id INTEGER NOT NULL, moneda TEXT NOT NULL, monto_cent INTEGER NOT NULL,
 monto_mxn_cent INTEGER NOT NULL, day INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS fact_pagos (
 pago_id INTEGER PRIMARY KEY, contrato_id INTEGER NOT NULL, monto_cent INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS fact_interacciones (
 comentario_id INTEGER PRIMARY KEY, tour_id INTEGER NOT NULL, cliente_id INTEGER NOT NULL, text_code INTEGER NOT NULL);
"""

DEFAULTS={'leads': 5_000_000,'pagos': 7_500_000,'comentarios': 11_000_000,
          'chunk_size': 100_000, 'date_days': 731, 'seed': 20261016}
REFERENCES={'leads':4_500_000,'pagos':6_900_000,'comentarios':10_000_000}

def now():return datetime.now(timezone.utc).isoformat(timespec='seconds')

def metrics():
    # On Linux ru_maxrss is KiB (process high-water mark).
    return {'peak_process_rss_mib':round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss/1024,2)}

def connect(path:Path)->sqlite3.Connection:
    c=sqlite3.connect(str(path),timeout=90)
    c.execute('PRAGMA foreign_keys=ON')
    c.execute('PRAGMA journal_mode=WAL')
    c.execute('PRAGMA synchronous=NORMAL') # benchmarking profile, not power-loss durability guarantee
    c.execute('PRAGMA cache_size=-32768')
    c.execute('PRAGMA temp_store=FILE')
    c.execute('PRAGMA mmap_size=0')
    c.executescript(DDL)
    return c

def ids_for(source,cfg):
    leads=cfg['leads'];tours=leads*7//10;sales=tours//5
    return {'clientes':max(3,leads//3),'leads':leads,
            'tours':tours,'contratos':sales,'pagos':cfg['pagos'],
            'comentarios':cfg['comentarios']}[source]

def sqlargs(source,start,end,cfg):
    nclient=max(3,cfg['leads']//3); ntours=cfg['leads']*7//10
    nsales=ntours//5;days=cfg['date_days'];seed=cfg['seed']
    return {
      'clientes':(start,end),
      'leads':(start,end,nclient,seed,days,ntours),
      'tours':(start,end),
      'contratos':(start,end,nclient),
      'pagos':(start,end,nsales,seed,days),
      'comentarios':(start,end,ntours,ntours,nclient,seed,days)
    }[source]

def validate_config_identity(c,cfg):
    signature=hashlib.sha256(json.dumps(cfg,sort_keys=True,separators=(',',':')).encode()).hexdigest()
    row=c.execute("SELECT value FROM run_meta WHERE key='config_sha256'").fetchone()
    if row and row[0]!=signature:
        raise RuntimeError('Different parameters for existing run; start with a new --db path')
    if not row:
        with c:
            c.execute("INSERT INTO run_meta(key,value) VALUES('config_sha256',?)",(signature,))
    return signature

def init_fx(c,cfg):
    if c.execute('SELECT count(*) FROM fx').fetchone()[0]:return
    days=cfg['date_days'];seed=cfg['seed']
    with c:
        c.executemany('INSERT INTO fx(day,moneda,rate_bps) VALUES (?,?,?)',(
          (day,moneda,10000 if moneda=='MXN' else 172000+((day*7+seed%19)%19)*1200)
          for day in range(1,days+1) for moneda in ['MXN','USD']))

def checkpoint(c,source)->int:
    r=c.execute('SELECT last_id FROM progress WHERE source=?',(source,)).fetchone()
    return r[0] if r else 0

def resume_safety(c,source):
    ck=checkpoint(c,source)
    pk={'clientes':'cliente_id','leads':'lead_id','tours':'tour_id','contratos':'contrato_id','pagos':'pago_id','comentarios':'comentario_id'}[source]
    count,mi,ma=c.execute(f'SELECT count(*),min({pk}),max({pk}) FROM {source}').fetchone()
    if count!=ck or (ck and (mi!=1 or ma!=ck)):
        raise RuntimeError(f'Checkpoint mismatch {source}: last_id={ck}, count={count}, min={mi}, max={ma}')
    return ck

def load_source(c,source,cfg,interrupt_after,info):
    target=ids_for(source,cfg)
    last=resume_safety(c,source)
    start_t=time.perf_counter();first=last
    while last<target:
        end=min(last+cfg['chunk_size'],target)
        with c:
            c.execute(TABLE_SQL[source],sqlargs(source,last+1,end,cfg))
            c.execute('INSERT INTO progress(source,last_id,updated_utc) VALUES(?,?,?) ON CONFLICT(source) DO UPDATE SET last_id=excluded.last_id,updated_utc=excluded.updated_utc',(source,end,now()))
        last=end
        info['batches_committed']+=1
        if info['batches_committed']%20==0:
            print(json.dumps({'stage':source,'loaded':last,'target':target,'batches':info['batches_committed'],'rss_mib':metrics()['peak_process_rss_mib']}),flush=True)
        if interrupt_after and info['batches_committed']>=interrupt_after:
            raise InterruptedError(f'Injected interruption after committed batch {info["batches_committed"]}; safe to resume')
    info['sources'][source]={'target':target,'inserted_this_run':last-first,'total_confirmed':last,'seconds':round(time.perf_counter()-start_t,3)}

def validate(c,cfg,fast=False):
    expected={t:ids_for(t,cfg) for t in ['clientes','leads','tours','contratos','pagos','comentarios']}
    expected['fx']=cfg['date_days']*2
    results={};violations={}
    pk={'clientes':'cliente_id','leads':'lead_id','tours':'tour_id','contratos':'contrato_id','pagos':'pago_id','comentarios':'comentario_id'}
    for table, target in expected.items():
        n=c.execute(f'SELECT count(*) FROM {table}').fetchone()[0]
        results[table]=n
        if n!=target:raise AssertionError(f'Count mismatched {table}: {n} != {target}')
        if not fast and table in pk:
            count,lo,hi,sum_ids=c.execute(f'SELECT count(*),min({pk[table]}),max({pk[table]}),sum({pk[table]}) FROM {table}').fetchone()
            if (count,lo,hi,sum_ids)!=(target,1,target,target*(target+1)//2):
                raise AssertionError(f'ID sequence corrupted {table}')
    if not fast:
        fk=c.execute('PRAGMA foreign_key_check').fetchmany(5)
        if fk: raise AssertionError(f'Foreign key failures: {fk}')
    # Cross-engine-independent SQL joins: cheap only indexed keys and tables.
    checks={
       'contratos_sin_lead':'SELECT count(*) FROM contratos k LEFT JOIN leads l ON l.lead_id=k.lead_id WHERE l.lead_id IS NULL',
       'pagos_sin_contrato':'SELECT count(*) FROM pagos p LEFT JOIN contratos k ON k.contrato_id=p.contrato_id WHERE k.contrato_id IS NULL',
       'comentarios_sin_tour':'SELECT count(*) FROM comentarios x LEFT JOIN tours t ON t.tour_id=x.tour_id WHERE t.tour_id IS NULL',
       'ventas_sin_fx':"SELECT count(*) FROM contratos k JOIN leads l ON l.lead_id=k.lead_id LEFT JOIN fx f ON f.day=l.day AND f.moneda=l.moneda WHERE f.day IS NULL"
    }
    for name,sql in checks.items():
        n=c.execute(sql).fetchone()[0]
        violations[name]=n
        if n:raise AssertionError(f'Integrity violation {name}={n}')
    return results,violations

def warehouse_fingerprint(c):
    return {
      'sales_mxn_sum':c.execute('SELECT sum(monto_mxn_cent) FROM fact_ventas').fetchone()[0],
      'payment_sum_cents':c.execute('SELECT sum(monto_cent) FROM fact_pagos').fetchone()[0],
      'interaction_id_sum':c.execute('SELECT sum(comentario_id) FROM fact_interacciones').fetchone()[0]
    }

def upsert_warehouse(c,cfg):
    before=time.perf_counter(); facts={}
    # Proxy SQL: SQL syntax and relational logic, NOT PostgreSQL's exact SQL dialect.
    with c:
        c.execute('''INSERT OR REPLACE INTO fact_ventas
           SELECT k.contrato_id,k.contrato_id,k.cliente_id,l.sala_id,l.moneda,l.monto_cent,
           CAST(ROUND((l.monto_cent*1.0*f.rate_bps)/10000.0) AS INTEGER),l.day
           FROM contratos k JOIN leads l ON k.lead_id=l.lead_id
           JOIN fx f ON f.day=l.day AND f.moneda=l.moneda''')
    facts['fact_ventas']=c.execute('SELECT count(*) FROM fact_ventas').fetchone()[0]
    with c:
        c.execute('INSERT OR REPLACE INTO fact_pagos SELECT pago_id,contrato_id,monto_cent FROM pagos')
    facts['fact_pagos']=c.execute('SELECT count(*) FROM fact_pagos').fetchone()[0]
    with c:
        c.execute('INSERT OR REPLACE INTO fact_interacciones SELECT comentario_id,tour_id,cliente_id,text_code FROM comentarios')
    facts['fact_interacciones']=c.execute('SELECT count(*) FROM fact_interacciones').fetchone()[0]
    expected={'fact_ventas': ids_for('contratos',cfg),'fact_pagos':cfg['pagos'],'fact_interacciones':cfg['comentarios']}
    if facts!=expected:raise AssertionError(f'Warehouse counts mismatch: {facts} vs {expected}')
    return facts, round(time.perf_counter()-before,3)

def probe_idempotence(c,cfg):
    # Retries the first 1000 records of the large sources using IGNORE; assertion counts don't change.
    # Existing rows MUST remain untouched, including identical business attributes.
    result={}
    for table in ['leads','pagos','comentarios']:
        sql=TABLE_SQL[table].replace('INSERT INTO','INSERT OR IGNORE INTO',1)
        before=c.total_changes
        with c:
            c.execute(sql,sqlargs(table,1,min(cfg['chunk_size'],1000),cfg))
        changes=c.total_changes-before
        if changes:raise AssertionError(f'Non-idempotent replay: {table} +{changes}')
        result[table]={'replayed_rows':min(1000,cfg['chunk_size']),'unexpected_inserts':changes}
    return result

def probe_corruption(c):
    for table,sql in [('pagos','INSERT INTO pagos(pago_id,contrato_id,monto_cent,day) VALUES (-1,-1,1,1)'),
                      ('comentarios','INSERT INTO comentarios(comentario_id,tour_id,cliente_id,day,text_code) VALUES (-1,-1,1,1,0)')]:
        try:
            with c: c.execute(sql)
        except sqlite3.IntegrityError:
            continue
        raise AssertionError(f'Failed negative FK test in {table}')
    return {'orphan_payment_rejected':True,'orphan_comment_rejected':True}

def execute(db:Path,cfg:dict,report:Path|None,interrupt_after=0,fast=False,skip_warehouse=False):
    db.parent.mkdir(parents=True,exist_ok=True)
    begun=time.perf_counter();info={'scenario':'physical_sqlite_proxy_not_postgresql','started_utc':now(),
      'input_parameters':cfg,'reference':REFERENCES,'sources':{},'batches_committed':0}
    c=connect(db)
    try:
        info['config_sha256']=validate_config_identity(c,cfg)
        init_fx(c,cfg)
        for source in ['clientes','leads','tours','contratos','pagos','comentarios']:
            load_source(c,source,cfg,interrupt_after,info)
        rows,errors=validate(c,cfg,fast=fast)
        info['source_counts']=rows;info['orphan_counts']=errors
        info['failure_injection']=probe_corruption(c)
        info['replay']=probe_idempotence(c,cfg)
        if not skip_warehouse:
            facts,seconds=upsert_warehouse(c,cfg)
            info['warehouse_counts']=facts;info['warehouse_seconds']=seconds
            # Second run must not change warehouse cardinality (large replay).
            second,sec=upsert_warehouse(c,cfg)
            if second!=facts: raise AssertionError('Warehouse not idempotent')
            info['warehouse_fingerprint']=warehouse_fingerprint(c)
            info['warehouse_replay_seconds']=sec
        c.execute('PRAGMA wal_checkpoint(TRUNCATE)')
        info.update({'status':'PASS','finished_utc':now(),'elapsed_seconds':round(time.perf_counter()-begun,3),**metrics(),'db_bytes':db.stat().st_size})
        if report:
            report.parent.mkdir(parents=True,exist_ok=True)
            report.write_text(json.dumps(info,ensure_ascii=False,indent=2),encoding='utf-8')
        return info
    finally:
        c.close()

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--db',type=Path,default=Path('data/stress/sunset_proxy.sqlite'))
    p.add_argument('--report',type=Path,default=Path('reports/stress_sqlite_proxy.json'))
    p.add_argument('--config',type=Path,default=Path('config/stress.json'))
    p.add_argument('--interrupt-after',type=int,default=0,help='Simulate stop after N committed batches; resume with same command without flag')
    p.add_argument('--fast',action='store_true',help='Skip full expensive sequence/fk audit (still performs joins)')
    p.add_argument('--skip-warehouse',action='store_true')
    args=p.parse_args()
    cfg=json.loads(args.config.read_text(encoding='utf-8'))
    for name in ['leads','pagos','comentarios','chunk_size','date_days']:
        if int(cfg[name])<1:raise ValueError(name+' must be positive')
    if int(cfg['leads'])<10:raise ValueError('Need at least 10 leads')
    if int(cfg['chunk_size'])>1000000:raise ValueError('Use <= 1000000 for bounded memory')
    try:
        result=execute(args.db,cfg,args.report,args.interrupt_after,args.fast,args.skip_warehouse)
        print(json.dumps({'status':result['status'],'sources':result['source_counts'],'warehouse':result.get('warehouse_counts'),'seconds':result['elapsed_seconds'],'peak_rss_mib':result['peak_process_rss_mib']},ensure_ascii=False),flush=True)
    except InterruptedError as exc:
        print(json.dumps({'status':'INTERRUPTED_AS_DESIGNED','detail':str(exc)}),flush=True)
        sys.exit(75)

if __name__=='__main__':main()
