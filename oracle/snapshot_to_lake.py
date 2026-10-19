"""Oracle 11g Thick mode incremental extractor -> immutable Parquet run snapshot.

This adapter cannot be run without an authorized Oracle 11g environment,
credentials and compatible Oracle Client. It is not a claim of connection.
A Lake manifest is published after Parquet is reconciled; this is NOT the
Warehouse consumer watermark (which advances after DW commit).
"""
from __future__ import annotations
import argparse,csv,hashlib,json,os,re,tempfile,time
from datetime import datetime,timedelta,timezone
from pathlib import Path
from sunset.config import load
from sunset.parquet import convert

SQL_NAME=re.compile(r'^[A-Z][A-Z0-9_]*$')
SOURCE_TABLES={'leads_ventas':'LEADS_VENTAS','fx_historico':'FX_HISTORICO'}

def make_sql(source:str,time_column='UPDATED_AT'):
    if source not in SOURCE_TABLES:raise ValueError('Unsupported Oracle source')
    if not SQL_NAME.fullmatch(time_column):raise ValueError('Invalid Oracle column')
    table=SOURCE_TABLES[source]
    return (f'SELECT * FROM {table} WHERE {time_column} >= :start_time '
            f'AND {time_column} < :end_time ORDER BY {time_column}')

def oracle_utc_naive(value: datetime) -> datetime:
    """Normalize aware time to UTC before binding to timezone-naive Oracle columns.

    Do not simply strip tzinfo: 02:00-06:00 and 08:00+00:00 are the same
    instant, but removing their offsets silently changes the extraction range.
    This adapter assumes the source UPDATED_AT values represent UTC.
    """
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError('Timezone-aware datetime required')
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def interval_run_id(source, start, end):
    message=f'{source}|{oracle_utc_naive(start).isoformat()}|{oracle_utc_naive(end).isoformat()}'
    return hashlib.sha256(message.encode()).hexdigest()[:20]

def run(source:str, lower:datetime, upper:datetime, root:Path,
        config_path:Path, fetch_size:int=5000,max_retries:int=3):
    if source not in SOURCE_TABLES:raise ValueError('Unknown source')
    if lower.tzinfo is None or upper.tzinfo is None or lower.utcoffset() is None or upper.utcoffset() is None or lower >= upper:
        raise ValueError('Timezone-aware, increasing interval required')
    if fetch_size<=0 or max_retries<1:raise ValueError('Invalid batch/retry count')
    # 11g only supports python-oracledb THICK mode with a compatible client.
    try:
        import oracledb
    except ImportError as exc:
        raise RuntimeError('Install oracledb with a compatible Oracle Instant Client') from exc
    lib_dir=os.getenv('ORACLE_CLIENT_LIB_DIR')
    if lib_dir:oracledb.init_oracle_client(lib_dir=lib_dir)
    else:oracledb.init_oracle_client()
    run_id=interval_run_id(source,lower,upper)
    destination=root/'incremental'/run_id
    table=SOURCE_TABLES[source]
    last_error=None
    for attempt in range(1,max_retries+1):
        try:
            with tempfile.TemporaryDirectory(prefix='sunset_oracle_') as scratch:
                csv_path=Path(scratch)/f'{source}.csv'
                with oracledb.connect(user=os.environ['ORACLE_USER'],
                   password=os.environ['ORACLE_PASSWORD'],dsn=os.environ['ORACLE_DSN']) as conn:
                    with conn.cursor() as cur:
                        cur.arraysize=fetch_size
                        cur.prefetchrows=min(fetch_size,1000)
                        cur.execute(make_sql(source),start_time=oracle_utc_naive(lower),
                                    end_time=oracle_utc_naive(upper))
                        headers=[col[0].lower() for col in cur.description]
                        required=load(config_path)['sources'][source]['required']
                        if not set(required).issubset(headers):
                            raise ValueError(f'Schema drift: required source column removed: {source}')
                        count=0
                        with csv_path.open('w',newline='',encoding='utf-8') as stream:
                            writer=csv.writer(stream);writer.writerow(headers)
                            while data:=cur.fetchmany(fetch_size):
                                writer.writerows(data)
                                count+=len(data)
                if count==0:
                    return {'source':source,'run_id':run_id,'extracted':0,'status':'EMPTY'}
                details=convert(Path(scratch),destination,config_path,source)
                if details[source]['rows']!=count:
                    raise AssertionError('Oracle/Parquet reconciliation failed')
                published=destination/'_published.json'
                published.write_text(json.dumps({'run_id':run_id,'source':source,
                     'lower_inclusive':lower.isoformat(),'upper_exclusive':upper.isoformat(),
                     'rows':count,'checkpoint_scope':'LAKE_ONLY'})+'\n')
                return {'source':source,'run_id':run_id,'extracted':count,
                        'parquet_rows':details[source]['rows'],'status':'PASS',
                        'output':str(destination/load(config_path)['sources'][source]['target'])}
        except (OSError,oracledb.DatabaseError) as exc:
            last_error=exc
            if attempt<max_retries:time.sleep(min(2**attempt,10))
    raise RuntimeError(f'Oracle extraction failed after {max_retries} attempts') from last_error


def main():
    p=argparse.ArgumentParser();p.add_argument('--source',required=True,choices=SOURCE_TABLES)
    p.add_argument('--start',required=True,help='ISO datetime with UTC offset')
    p.add_argument('--end',required=True,help='ISO datetime with UTC offset')
    p.add_argument('--root',type=Path,default=Path('data/parquet'))
    p.add_argument('--config',type=Path,default=Path('config/pipeline.json'))
    p.add_argument('--fetch-size',type=int,default=5000)
    a=p.parse_args()
    print(json.dumps(run(a.source,datetime.fromisoformat(a.start),datetime.fromisoformat(a.end),
       a.root,a.config,a.fetch_size),indent=2))
if __name__=='__main__':main()
