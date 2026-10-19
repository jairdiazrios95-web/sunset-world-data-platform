"""Dependency-free INTEGRATION TEST HARNESS, not a PostgreSQL replacement.

Runs data quality -> CSV raw ingestion -> SCD2 dimensions -> FX/tours/sales/
interaction facts -> reconciliations -> exact idempotent replay using SQLite.
This harness exercises representative business and load logic using real SQL,
but does NOT execute native PostgreSQL DDL/dbt/Airflow/Spark/Parquet/pgvector.
"""
from __future__ import annotations
import argparse
import csv
import hashlib
import json
import re
import sqlite3
import time
from datetime import date, timedelta, datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from sunset.config import load
from sunset.quality import check

SAFE = re.compile(r'^[a-z][a-z0-9_]*$')


def quote(s):
    if not SAFE.fullmatch(s):
        raise ValueError(f'Invalid SQL identifier: {s!r}')
    return '"' + s + '"'


def init_db(db):
    db.execute('PRAGMA foreign_keys=ON')
    db.execute('PRAGMA journal_mode=WAL')
    db.executescript('''
      CREATE TABLE IF NOT EXISTS dim_cliente (
        cliente_key INTEGER PRIMARY KEY, cliente_id TEXT NOT NULL, segmento TEXT NOT NULL,
        valid_from TEXT NOT NULL, valid_to TEXT, is_current INTEGER NOT NULL,
        UNIQUE(cliente_id,valid_from));
      CREATE UNIQUE INDEX IF NOT EXISTS cliente_current ON dim_cliente(cliente_id) WHERE is_current=1;
      CREATE TABLE IF NOT EXISTS dim_sala (
        sala_key INTEGER PRIMARY KEY, sala_id TEXT NOT NULL, destino TEXT NOT NULL,
        nombre_sala TEXT NOT NULL, valid_from TEXT NOT NULL, valid_to TEXT,
        is_current INTEGER NOT NULL, UNIQUE(sala_id,valid_from));
      CREATE UNIQUE INDEX IF NOT EXISTS sala_current ON dim_sala(sala_id) WHERE is_current=1;
      CREATE TABLE IF NOT EXISTS dim_fuente (fuente_key INTEGER PRIMARY KEY,fuente_id TEXT UNIQUE NOT NULL,nombre_fuente TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS dim_producto (producto_key INTEGER PRIMARY KEY,producto_id TEXT UNIQUE NOT NULL,nombre_producto TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS dim_tiempo (tiempo_key INTEGER PRIMARY KEY,fecha TEXT UNIQUE NOT NULL,anio INT NOT NULL,mes INT NOT NULL);
      CREATE TABLE IF NOT EXISTS fact_fx (fecha TEXT NOT NULL,moneda_origen TEXT NOT NULL,moneda_destino TEXT NOT NULL,
        tipo_cambio TEXT NOT NULL,PRIMARY KEY(fecha,moneda_origen,moneda_destino));
      CREATE TABLE IF NOT EXISTS fact_tours (tour_id TEXT PRIMARY KEY,lead_id TEXT NOT NULL,
        cliente_key INT NOT NULL REFERENCES dim_cliente(cliente_key),sala_key INT NOT NULL REFERENCES dim_sala(sala_key),
        tiempo_key INT NOT NULL REFERENCES dim_tiempo(tiempo_key),status_tour TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS fact_ventas (venta_id TEXT PRIMARY KEY,lead_id TEXT NOT NULL,tour_id TEXT,
        cliente_key INT NOT NULL REFERENCES dim_cliente(cliente_key),sala_key INT NOT NULL REFERENCES dim_sala(sala_key),
        tiempo_key INT NOT NULL REFERENCES dim_tiempo(tiempo_key),
        fuente_key INT NOT NULL REFERENCES dim_fuente(fuente_key),producto_key INT NOT NULL REFERENCES dim_producto(producto_key),
        moneda TEXT NOT NULL,monto_venta TEXT NOT NULL,monto_mxn TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS fact_interacciones (interaccion_id TEXT PRIMARY KEY,tour_id TEXT NOT NULL,
        cliente_key INT NOT NULL REFERENCES dim_cliente(cliente_key),tiempo_key INT NOT NULL REFERENCES dim_tiempo(tiempo_key),
        contenido TEXT NOT NULL);
    ''')


def load_raw(db, input_dir: Path, cfg: dict, batch_size: int) -> dict:
    if batch_size < 1:
        raise ValueError('batch_size must be positive')
    seen = {}
    for source, spec in cfg['sources'].items():
        with (input_dir / (source+'.csv')).open(newline='', encoding='utf-8') as f:
            rd = csv.DictReader(f)
            cols = rd.fieldnames
            if not cols or len(cols)!=len(set(cols)):
                raise ValueError(f'{source} malformed headers')
            fields = ','.join(quote(c)+' TEXT' for c in cols)
            pk=','.join(quote(c) for c in spec['primary_key'])
            t = quote('raw_'+source)
            db.execute(f'CREATE TABLE IF NOT EXISTS {t} ({fields}, PRIMARY KEY({pk}))')
            colsql=','.join(map(quote,cols))
            placeholders=','.join('?' for _ in cols)
            updates=','.join(f'{quote(c)}=excluded.{quote(c)}' for c in cols if c not in spec['primary_key'])
            on_conflict='DO UPDATE SET '+updates if updates else 'DO NOTHING'
            sql=f'INSERT INTO {t} ({colsql}) VALUES ({placeholders}) ON CONFLICT({pk}) {on_conflict}'
            batch=[]
            count=0
            for row in rd:
                if None in row or any(v is None for v in row.values()):
                    raise ValueError(f'{source} has malformed row')
                batch.append(tuple(row[c] for c in cols))
                count+=1
                if len(batch)==batch_size:
                    db.executemany(sql,batch)
                    batch=[]
            if batch:db.executemany(sql,batch)
            stored=db.execute(f'SELECT COUNT(*) FROM {t}').fetchone()[0]
            if count!=stored:
                raise AssertionError(f'{source} input={count} destination={stored}, duplicates or stale data')
            seen[source]=stored
    return seen


def scd2(db, name, attributes, timestamp):
    table='dim_'+name
    key=name+'_id'
    surrogate=name+'_key'
    sql='SELECT '+','.join([key]+attributes)+' FROM raw_'+('clientes' if name=='cliente' else 'salas')
    changed=0
    for record in db.execute(sql):
        identity,*values=record
        old=db.execute(f'SELECT {surrogate},'+','.join(attributes)+f' FROM {table} WHERE {key}=? AND is_current=1',(identity,)).fetchone()
        if old is not None and tuple(old[1:])==tuple(values):continue
        if old is not None:
            db.execute(f'UPDATE {table} SET valid_to=?, is_current=0 WHERE {surrogate}=?',(timestamp,old[0]))
        columns=[key]+attributes+['valid_from','is_current']
        marks=','.join('?' for _ in columns)
        db.execute(f'INSERT INTO {table} ('+','.join(columns)+f') VALUES ({marks})',
                   [identity]+values+[timestamp if old else '1900-01-01T00:00:00',1])
        changed+=1
    return changed


def marts(db, timestamp):
    dimensions_changed={
        'cliente':scd2(db,'cliente',['segmento'],timestamp),
        'sala':scd2(db,'sala',['destino','nombre_sala'],timestamp),
    }
    for tbl,key,col in [('fuente','fuente_id','nombre_fuente'),('producto','producto_id','nombre_producto')]:
        db.execute(f'''INSERT INTO dim_{tbl}({key},{col}) SELECT {key},{col} FROM raw_{tbl}s WHERE TRUE
         ON CONFLICT({key}) DO UPDATE SET {col}=excluded.{col}''')
    span=db.execute('SELECT MIN(fecha),MAX(fecha) FROM raw_fx_historico').fetchone()
    lower=date.fromisoformat(span[0]);upper=date.fromisoformat(span[1])
    db.executemany('INSERT OR IGNORE INTO dim_tiempo(tiempo_key,fecha,anio,mes) VALUES (?,?,?,?)',
                   ((int((lower+timedelta(days=i)).strftime('%Y%m%d')),
                      (lower+timedelta(days=i)).isoformat(),
                      (lower+timedelta(days=i)).year,(lower+timedelta(days=i)).month)
                    for i in range((upper-lower).days+1)))
    db.execute('''INSERT INTO fact_fx(fecha,moneda_origen,moneda_destino,tipo_cambio)
      SELECT fecha,moneda_origen,moneda_destino,tipo_cambio FROM raw_fx_historico WHERE TRUE
      ON CONFLICT(fecha,moneda_origen,moneda_destino) DO UPDATE SET tipo_cambio=excluded.tipo_cambio''')
    db.execute('''INSERT INTO fact_tours(tour_id,lead_id,cliente_key,sala_key,tiempo_key,status_tour)
      SELECT t.tour_id,t.lead_id,c.cliente_key,s.sala_key,
        CAST(replace(t.fecha_tour,'-','') AS INT),t.status_tour
      FROM raw_tours t JOIN dim_cliente c ON t.cliente_id=c.cliente_id AND t.fecha_tour>=substr(c.valid_from,1,10) AND (c.valid_to IS NULL OR t.fecha_tour<substr(c.valid_to,1,10))
      JOIN dim_sala s ON t.sala_id=s.sala_id AND t.fecha_tour>=substr(s.valid_from,1,10) AND (s.valid_to IS NULL OR t.fecha_tour<substr(s.valid_to,1,10)) WHERE TRUE
      ON CONFLICT(tour_id) DO UPDATE SET lead_id=excluded.lead_id,
      cliente_key=excluded.cliente_key,sala_key=excluded.sala_key,
      tiempo_key=excluded.tiempo_key,status_tour=excluded.status_tour''')
    db.execute('''INSERT INTO fact_interacciones(interaccion_id,tour_id,cliente_key,tiempo_key,contenido)
      SELECT cm.comentario_id,cm.tour_id,c.cliente_key,
       CAST(replace(cm.fecha_comentario,'-','') AS INT),cm.texto
      FROM raw_comentarios cm JOIN dim_cliente c ON cm.cliente_id=c.cliente_id AND cm.fecha_comentario>=substr(c.valid_from,1,10) AND (c.valid_to IS NULL OR cm.fecha_comentario<substr(c.valid_to,1,10))
      WHERE TRUE ON CONFLICT(interaccion_id) DO UPDATE SET tour_id=excluded.tour_id,
      cliente_key=excluded.cliente_key,tiempo_key=excluded.tiempo_key,contenido=excluded.contenido''')
    sale_rows=db.execute("""SELECT l.venta_id,l.lead_id,l.tour_id,c.cliente_key,s.sala_key,
      CAST(replace(l.fecha_venta,'-','') AS INT),f.fuente_key,p.producto_key,
      l.moneda,l.monto_venta,fx.tipo_cambio FROM raw_leads_ventas l
      JOIN dim_cliente c ON l.cliente_id=c.cliente_id AND l.fecha_venta>=substr(c.valid_from,1,10) AND (c.valid_to IS NULL OR l.fecha_venta<substr(c.valid_to,1,10))
      JOIN dim_sala s ON l.sala_id=s.sala_id AND l.fecha_venta>=substr(s.valid_from,1,10) AND (s.valid_to IS NULL OR l.fecha_venta<substr(s.valid_to,1,10))
      JOIN dim_fuente f ON l.fuente_id=f.fuente_id
      JOIN dim_producto p ON l.producto_id=p.producto_id
      JOIN fact_fx fx ON l.fecha_venta=fx.fecha AND l.moneda=fx.moneda_origen
         AND fx.moneda_destino='MXN' WHERE l.venta_id<>''""").fetchall()
    sales=[]
    for row in sale_rows:
        amount=(Decimal(row[9])*Decimal(row[10])).quantize(Decimal('.01'),rounding=ROUND_HALF_UP)
        sales.append(tuple(row[:10])+(str(amount),))
    db.executemany('''INSERT INTO fact_ventas(venta_id,lead_id,tour_id,cliente_key,sala_key,
      tiempo_key,fuente_key,producto_key,moneda,monto_venta,monto_mxn)
      VALUES (?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(venta_id) DO UPDATE SET
       lead_id=excluded.lead_id,tour_id=excluded.tour_id,cliente_key=excluded.cliente_key,
       sala_key=excluded.sala_key,tiempo_key=excluded.tiempo_key,fuente_key=excluded.fuente_key,
       producto_key=excluded.producto_key,moneda=excluded.moneda,monto_venta=excluded.monto_venta,
       monto_mxn=excluded.monto_mxn''', sales)
    return {'scd2_versions_inserted':dimensions_changed,'sales_transformed':len(sales)}


def counts(db):
    return {t:db.execute('SELECT COUNT(*) FROM '+quote(t)).fetchone()[0] for t in [
        'dim_cliente','dim_sala','dim_fuente','dim_producto','dim_tiempo',
        'fact_fx','fact_tours','fact_ventas','fact_interacciones']}


def signature(db):
    digest=hashlib.sha256()
    for t in ['dim_cliente','dim_sala','dim_fuente','dim_producto','dim_tiempo',
              'fact_fx','fact_tours','fact_ventas','fact_interacciones']:
        digest.update(t.encode())
        for record in db.execute('SELECT * FROM '+quote(t)+' ORDER BY 1'):
            digest.update(json.dumps(record,ensure_ascii=False,sort_keys=True).encode())
    return digest.hexdigest()


def reconcile(db, raw):
    stats=counts(db)
    reference={
        'fact_ventas':db.execute("SELECT COUNT(*) FROM raw_leads_ventas WHERE venta_id<>''").fetchone()[0],
        'fact_tours':raw['tours'],
        'fact_fx':raw['fx_historico'],
        'fact_interacciones':raw['comentarios']}
    for k,v in reference.items():
        if stats[k]!=v:raise AssertionError(f'{k}: {stats[k]} != {v}')
    bad=db.execute('PRAGMA foreign_key_check').fetchall()
    if bad:raise AssertionError(f'FK violations: {bad[:5]}')
    if db.execute('SELECT COUNT(*) FROM (SELECT cliente_id FROM dim_cliente WHERE is_current=1 GROUP BY cliente_id HAVING COUNT(*)>1)').fetchone()[0]:
        raise AssertionError('SCD2 duplicate current client')
    return {'warehouse_counts':stats,'source_expected_facts':reference,'foreign_key_violations':len(bad)}


def verify(input_dir:Path,db_file:Path,config:Path,batch_size=None,effective_at=None):
    started=time.perf_counter()
    quality=check(input_dir,config,'full')
    if quality['status']!='PASS':raise ValueError('Quality gate blocks warehouse: '+repr(quality['errors']))
    cfg=load(config)
    batch_size = int(batch_size or cfg['parquet']['batch_size'])
    effective_at = effective_at or datetime.now(timezone.utc).isoformat(timespec='seconds')
    db_file.parent.mkdir(parents=True,exist_ok=True)
    # Fresh db by default; rerun itself validates replay. Existing databases deliberately
    # supported for incremental/manual SCD2 scenarios.
    db=sqlite3.connect(str(db_file))
    db.execute('PRAGMA busy_timeout=10000')
    try:
        with db:
            init_db(db)
            raw=load_raw(db,input_dir,cfg,batch_size)
            change=marts(db,effective_at)
            initial=reconcile(db,raw)
        fp1=signature(db)
        with db:
            again=load_raw(db,input_dir,cfg,batch_size)
            replay=marts(db,effective_at)
            subsequent=reconcile(db,again)
        fp2=signature(db)
        if fp1!=fp2 or initial!=subsequent:
            raise AssertionError('Replay altered logical output: NOT IDEMPOTENT')
        result={
          'status':'PASS', 'mode':'REAL_SQLITE_EXECUTION_NOT_POSTGRESQL',
          'quality':'PASS','quality_checks':quality['checks_evaluated_at_least'],
          'raw_rows':raw,'reconciliation':initial,
          'first_run':change,'second_run':replay,
          'identical_replay_hash':fp1==fp2,'sha256':fp2,
          'elapsed_seconds':round(time.perf_counter()-started,3),
          'postgresql':'NOT_EXECUTED','parquet':'NOT_EXECUTED',
          'airflow':'NOT_EXECUTED','kafka_spark':'NOT_EXECUTED','rag_embeddings':'NOT_EXECUTED'}
        return result
    finally:
        db.close()


def main():
    p=argparse.ArgumentParser()
    p.add_argument('--input',type=Path,default=Path('data/sample'))
    p.add_argument('--config',type=Path,default=Path('config/pipeline.json'))
    p.add_argument('--db',type=Path,default=Path('reports/runs/sqlite_dw.sqlite'))
    p.add_argument('--report',type=Path,default=Path('reports/local_integration_verified.json'))
    p.add_argument('--batch-size',type=int,default=None)
    p.add_argument('--effective-at',default=None,help='UTC ISO timestamp; defaults to the current clock')
    a=p.parse_args()
    result=verify(a.input,a.db,a.config,a.batch_size,a.effective_at)
    a.report.parent.mkdir(parents=True,exist_ok=True)
    a.report.write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    print(json.dumps(result,ensure_ascii=False,indent=2))

if __name__=='__main__':main()
