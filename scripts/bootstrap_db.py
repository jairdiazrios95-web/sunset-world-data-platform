"""Idempotent live PostgreSQL initialization from versioned DDL."""
from pathlib import Path
import os


def bootstrap(root:Path=Path('.')):
    import psycopg
    dsn=os.environ['SUNSET_PG_DSN']
    with psycopg.connect(dsn,autocommit=True) as conn:
        with conn.cursor() as cur:
            for name in ('001_init.sql','003_upsert_patterns.sql','005_streaming.sql'):
                cur.execute((root/'sql'/name).read_text(encoding='utf-8'),prepare=False)
            cur.execute("SELECT extversion FROM pg_extension WHERE extname='vector'")
            version=cur.fetchone()[0]
            if not version:raise AssertionError('Missing pgvector')
    return version

if __name__=='__main__':
    print('pgvector installed:',bootstrap())
