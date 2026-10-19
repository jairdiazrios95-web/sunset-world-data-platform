"""Read-only Oracle 11g cursor-based extraction; not run against a private system.
Oracle 11g requires python-oracledb Thick mode and compatible installed client.
Identifiers are allowlisted, never passed directly from untrusted input.
"""
import os,re
VALID=re.compile(r'^[A-Z][A-Z0-9_]*$')

def extract_rows(table, watermark, fetch_size=5000):
    import oracledb
    schema_table=table.upper()
    if not VALID.fullmatch(schema_table):raise ValueError('Invalid/unsafe Oracle table identifier')
    lib_dir=os.environ.get('ORACLE_CLIENT_LIB_DIR')
    if lib_dir:oracledb.init_oracle_client(lib_dir=lib_dir)
    else:oracledb.init_oracle_client()
    with oracledb.connect(user=os.environ['ORACLE_USER'],password=os.environ['ORACLE_PASSWORD'],dsn=os.environ['ORACLE_DSN']) as conn:
        with conn.cursor() as cur:
            cur.arraysize=fetch_size
            # READ ONLY; indexed updated_at preferred, verify actual column availability first.
            cur.execute(f'SELECT * FROM {schema_table} WHERE UPDATED_AT >= :watermark ORDER BY UPDATED_AT',watermark=watermark)
            columns=[desc[0] for desc in cur.description]
            while True:
                rows=cur.fetchmany(fetch_size)
                if not rows:break
                yield columns,rows
