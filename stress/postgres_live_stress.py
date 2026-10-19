"""Real PostgreSQL SQL/storage/FK stress with restart and repeatable batches.

This is not an Oracle, Parquet or Spark throughput test. Runs directly against
Postgres 16 on GitHub Actions or an authorized environment. The `double`
profile has 42.8M synthetic primary records and can exhaust standard runners.
"""
from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path

DEFAULT_CONFIG = Path(__file__).resolve().parents[1] / 'config/stress_pg.json'


def read_profiles(path: Path = DEFAULT_CONFIG) -> dict:
    data = json.loads(path.read_text(encoding='utf-8'))
    profiles = data['profiles']
    for name, sources in profiles.items():
        if set(sources) != {'leads', 'payments', 'comments'} or any(
            type(n) is not int or n < 1 for n in sources.values()
        ):
            raise ValueError(f'Invalid PostgreSQL stress profile: {name}')
    return profiles


PROFILES = read_profiles()
TABLES = ('leads', 'payments', 'comments')


def batches(total: int, size: int):
    if total < 0 or size < 1:
        raise ValueError('Invalid total or batch size')
    for start in range(1, total + 1, size):
        yield start, min(total, start + size - 1)


def insert_sql(source: str):
    if source == 'leads':
        return '''INSERT INTO stress.leads(id, cliente_id, created_at)
                  SELECT n, (n%1000000)+1, date '2024-01-01' + (n%365)::integer
                  FROM generate_series(%s::bigint,%s::bigint) n
                  ON CONFLICT(id) DO NOTHING'''
    if source in ('payments','comments'):
        return f'''INSERT INTO stress.{source}(id, lead_id, event_at)
                   SELECT n, ((n-1)%%%s::bigint)+1, date '2024-01-01' + (n%%365)::integer
                   FROM generate_series(%%s::bigint,%%s::bigint) n
                   ON CONFLICT(id) DO NOTHING'''.replace('%%%', '%').replace('%%s', '%s').replace('%%365','%365')
    raise ValueError(f'Unknown stress table {source}')


def execute(dsn: str, profile: str, batch_size: int, report: Path,
            config: Path = DEFAULT_CONFIG):
    import psycopg
    profiles = read_profiles(config)
    if profile not in profiles or batch_size < 1:
        raise ValueError('Unknown profile or invalid batch size')
    started = time.monotonic()
    targets = profiles[profile]
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute('CREATE SCHEMA IF NOT EXISTS stress')
            cur.execute('''CREATE TABLE IF NOT EXISTS stress.leads (
              id bigint PRIMARY KEY, cliente_id bigint NOT NULL, created_at date NOT NULL)''')
            for name in ('payments','comments'):
                cur.execute(f'''CREATE TABLE IF NOT EXISTS stress.{name} (
                  id bigint PRIMARY KEY, lead_id bigint NOT NULL
                    REFERENCES stress.leads(id), event_at date NOT NULL)''')
        conn.commit()
        detail = {}
        for table in TABLES:
            begin = time.monotonic()
            count = targets[table]
            sql = insert_sql(table)
            for lower, upper in batches(count,batch_size):
                with conn.cursor() as cur:
                    if table == 'leads':
                        cur.execute(sql, (lower,upper))
                    else:
                        cur.execute(sql, (targets['leads'],lower,upper))
                conn.commit()  # Checkpoint per batch, safe restart on rerun
            with conn.cursor() as cur:
                cur.execute(f'SELECT count(*) FROM stress.{table}')
                actual = cur.fetchone()[0]
            if actual != count:
                raise AssertionError(f'{table}: rows {actual} != {count}')
            lower = max(1, count - batch_size + 1)
            with conn.cursor() as cur:
                if table=='leads':cur.execute(sql,(lower,count))
                else:cur.execute(sql,(targets['leads'],lower,count))
                cur.execute(f'SELECT count(*) FROM stress.{table}')
                after_replay = cur.fetchone()[0]
            if after_replay != count:
                raise AssertionError(f'{table}: replay changed row count')
            conn.commit()
            detail[table] = {'rows':actual,'replay_rows':after_replay,
                             'elapsed_seconds':round(time.monotonic()-begin,3)}
        with conn.cursor() as cur:
            orphans = {}
            for table in ('payments','comments'):
                cur.execute(f'''SELECT count(*) FROM stress.{table} t
                  LEFT JOIN stress.leads l ON t.lead_id=l.id WHERE l.id IS NULL''')
                orphans[table] = cur.fetchone()[0]
            cur.execute("SELECT pg_database_size(current_database())")
            db_bytes = cur.fetchone()[0]
        if any(orphans.values()):
            raise AssertionError(f'Foreign-key errors: {orphans}')
    outcome = {
        'status':'PASS', 'simulation':False,
        'engine':'Real PostgreSQL 16 physical stress (not end-to-end platform)',
        'profile':profile, 'targets':targets, 'counts':detail,
        'orphans':orphans, 'postgres_database_bytes':db_bytes,
        'elapsed_seconds':round(time.monotonic()-started,3),
    }
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text(json.dumps(outcome, indent=2) + '\n', encoding='utf-8')
    return outcome


if __name__=='__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--profile', choices=PROFILES, default='smoke')
    p.add_argument('--config', type=Path, default=DEFAULT_CONFIG)
    p.add_argument('--batch-size',type=int,default=50000)
    p.add_argument('--report',type=Path,default=Path('reports/live_pg_stress.json'))
    a=p.parse_args()
    print(json.dumps(execute(os.environ['SUNSET_PG_DSN'],a.profile,a.batch_size,a.report,a.config),indent=2))
