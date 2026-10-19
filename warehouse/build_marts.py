"""PostgreSQL dimensional mart builder - transactional and replay safe.
Run after the ten synthetic landing datasets are loaded to raw.*.
"""
from __future__ import annotations
import argparse
import json
import os
from pathlib import Path

REQUIRED=['clientes','leads_ventas','tours','contratos','pagos','comentarios','fx_historico','salas','fuentes','productos']

def build(dsn: str, sql_path: Path=Path('sql/004_marts.sql'), fiscal_start_month:int=1) -> dict:
    if not dsn:raise ValueError('SUNSET_PG_DSN is required')
    if not 1<=fiscal_start_month<=12:raise ValueError('Invalid fiscal start month')
    import psycopg
    import sqlparse
    statements=sqlparse.split(sql_path.read_text(encoding='utf-8'))
    with psycopg.connect(dsn) as conn:
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute("SELECT set_config('app.fiscal_start_month',%s,true)",
                            (str(fiscal_start_month),))
                for source in REQUIRED:
                    cur.execute('SELECT to_regclass(%s)',(f'raw.{source}',))
                    if cur.fetchone()[0] is None:
                        raise ValueError(f'Missing raw source: {source}')
                for statement in statements:
                    if statement.strip():cur.execute(statement,prepare=False)
                result={}
                for table in ['dim_cliente','dim_sala','dim_fuente','dim_producto','dim_tiempo',
                              'fact_ventas','fact_tours','fact_fx','fact_interacciones']:
                    cur.execute(f'SELECT count(*) FROM dw.{table}')
                    result[table]=cur.fetchone()[0]
                cur.execute("SELECT count(*) FROM raw.leads_ventas WHERE venta_id <> ''")
                expected_sales=cur.fetchone()[0]
                if expected_sales!=result['fact_ventas']:
                    raise AssertionError(f'Sales reconciliation {expected_sales}!={result["fact_ventas"]}')
                cur.execute('SELECT count(*) FROM raw.tours')
                if cur.fetchone()[0]!=result['fact_tours']:
                    raise AssertionError('Tour reconciliation failed')
                cur.execute('SELECT count(*) FROM raw.comentarios')
                if cur.fetchone()[0]!=result['fact_interacciones']:
                    raise AssertionError('Interaction reconciliation failed')
                cur.execute('SELECT count(*) FROM raw.fx_historico')
                if cur.fetchone()[0]!=result['fact_fx']:
                    raise AssertionError('FX reconciliation failed')
    return {'status':'PASS','row_counts':result,'sales_expected':expected_sales}

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--sql',type=Path,default=Path('sql/004_marts.sql'))
    a=p.parse_args()
    from sunset.config import load
    month=load('config/pipeline.json').get('warehouse',{}).get('fiscal_year_start_month',1)
    print(json.dumps(build(os.environ.get('SUNSET_PG_DSN',''),a.sql,month),indent=2))
