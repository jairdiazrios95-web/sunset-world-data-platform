"""Sunset World: 02:00 AM daily batch, Airflow 2.10.x TaskFlow.

Each task returns only compact metadata via XCom. Commands run within the
Airflow container against mounted project files and PostgreSQL. Oracle source
is designed in a separate adapter; DEMO_SOURCE_MODE=csv is mandatory for this
synthetic evaluation environment. dbt is actually invoked (never stubbed).
"""
from __future__ import annotations
import json
import os
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from urllib.request import Request, urlopen
import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowException

ROOT=Path(os.getenv('SUNSET_PROJECT_ROOT','/opt/airflow/project'))
CONFIG=os.getenv('SUNSET_CONFIG','config/pipeline.json')
SOURCE_MODE=os.getenv('DEMO_SOURCE_MODE','csv')


def alert(context):
    webhook=os.getenv('ALERT_WEBHOOK_URL','')
    if not webhook:return
    msg={'text':f"Sunset ETL failure: dag={context['dag'].dag_id} task={context['task_instance'].task_id} run={context['run_id']}"}
    try:
        request=Request(webhook,data=json.dumps(msg).encode(),
                        headers={'Content-Type':'application/json'},method='POST')
        with urlopen(request,timeout=8) as response:
            response.read()
    except Exception:
        import logging
        logging.exception('Could not send optional webhook alert')


def run(*args):
    result=subprocess.run(args,cwd=ROOT,capture_output=True,text=True,timeout=7200)
    if result.returncode:
        raise AirflowException(
            f'Command failed ({args[0]}) with code {result.returncode}; '
            f'STDOUT: {result.stdout[-3500:]}; STDERR: {result.stderr[-3500:]}')
    return result.stdout[-8000:]

DEFAULT_ARGS={
 'owner':'sunset-data-platform',
 'retries':int(os.getenv('SUNSET_AIRFLOW_RETRIES','3')),
 'retry_delay':timedelta(seconds=int(os.getenv('SUNSET_AIRFLOW_RETRY_SECONDS','300'))),
 'sla':timedelta(hours=2), 'email_on_failure':bool(os.getenv('ALERT_EMAIL')),
 'email':os.getenv('ALERT_EMAIL',''), 'on_failure_callback':alert
}

with DAG(
 'etl_sunset',start_date=pendulum.datetime(2026,1,1,tz='America/Mexico_City'),
 schedule='0 2 * * *', catchup=False, max_active_runs=1,
 max_active_tasks=3,default_args=DEFAULT_ARGS,
 tags=['sunset','synthetic','lakehouse','dbt','quality'],
 description='CSV/Oracle adapter -> Parquet -> raw PostgreSQL -> DW -> dbt -> quality'
) as dag:

    @task
    def ingest_source(name):
        if SOURCE_MODE!='csv':
            raise AirflowException('Oracle mode needs actual credentials; use dedicated Oracle adapter')
        out=run('python','-m','sunset.parquet','--source',name,
                '--input','data/sample','--output','data/parquet', '--config', CONFIG)
        meta=json.loads(out)
        loaded=run('python','-m','warehouse.load_parquet','--source',name,
                   '--lake','data/parquet','--config',CONFIG)
        return {'source':name,'lake':meta[name],'warehouse':json.loads(loaded)}

    @task
    def ingest_support_sources(leads_meta,fx_meta):
        if not leads_meta['warehouse']['status']=='PASS' or not fx_meta['warehouse']['status']=='PASS':
            raise AirflowException('Core source landing incomplete')
        sources=['clientes','tours','contratos','pagos','comentarios','salas','fuentes','productos']
        counts={}
        for name in sources:
            run('python','-m','sunset.parquet','--source',name,'--input','data/sample',
                '--output','data/parquet','--config',CONFIG)
            counts[name]=json.loads(run('python','-m','warehouse.load_parquet',
                         '--source',name,'--lake','data/parquet','--config',CONFIG))['parquet_rows']
        return {'support_counts':counts,'leads':leads_meta['lake']['rows'],
                'fx':fx_meta['lake']['rows']}

    @task
    def run_marts(meta):
        result=json.loads(run('python','-m','warehouse.build_marts'))
        if result['status']!='PASS':raise AirflowException('Mart reconciliation failed')
        return {'marts':result,'source_counts':meta}

    @task
    def run_dbt(meta):
        run('dbt','run','--project-dir','dbt','--profiles-dir','dbt')
        run('dbt','test','--project-dir','dbt','--profiles-dir','dbt')
        return {'dbt':'PASS','marts':meta['marts']}

    @task
    def validate_quality(meta):
        out=run('python','-m','sunset.quality','--input','data/sample',
                '--scope','full','--report','reports/airflow_quality.json')
        if meta['dbt']!='PASS':raise AirflowException('dbt not successful')
        return {'quality':'PASS','details':out,'facts':meta['marts']['row_counts']}

    validate_quality(run_dbt(run_marts(ingest_support_sources(
        ingest_source.override(task_id='extract_leads')('leads_ventas'),
        ingest_source.override(task_id='extract_fx')('fx_historico')))))
