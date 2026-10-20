"""Read-only Oracle 11g extractor with a declared identifier allowlist.

Requires an authorized Oracle 11g server and python-oracledb Thick mode.
Table names are not bindable in Oracle; explicitly allowlist identifiers
before constructing the read-only statement.
"""
import os
import re

VALID = re.compile(r'^[A-Z][A-Z0-9_]*$')
# Explicit read-only templates. Selecting a constant by key prevents SQL
# identifier injection without pretending Oracle supports bound table names.
READ_QUERIES = {
    'LEADS_VENTAS': (
        'SELECT * FROM LEADS_VENTAS WHERE UPDATED_AT >= :watermark '
        'ORDER BY UPDATED_AT'),
    'FX_HISTORICO': (
        'SELECT * FROM FX_HISTORICO WHERE UPDATED_AT >= :watermark '
        'ORDER BY UPDATED_AT'),
}
DEFAULT_ALLOWED = frozenset(READ_QUERIES)


def extract_rows(table, watermark, fetch_size=5000):
    import oracledb
    schema_table = table.upper()
    allowed = {
        name.strip().upper() for name in os.getenv(
            'ORACLE_ALLOWED_TABLES', ','.join(sorted(DEFAULT_ALLOWED))
        ).split(',') if name.strip()
    }
    if (not VALID.fullmatch(schema_table) or schema_table not in allowed
            or schema_table not in READ_QUERIES):
        raise ValueError('Invalid/unsafe or non-allowlisted Oracle table identifier')
    if fetch_size < 1:
        raise ValueError('fetch_size must be positive')
    lib_dir = os.environ.get('ORACLE_CLIENT_LIB_DIR')
    if lib_dir:
        oracledb.init_oracle_client(lib_dir=lib_dir)
    else:
        oracledb.init_oracle_client()
    with oracledb.connect(
        user=os.environ['ORACLE_USER'],
        password=os.environ['ORACLE_PASSWORD'],
        dsn=os.environ['ORACLE_DSN']
    ) as conn:
        with conn.cursor() as cur:
            cur.arraysize = fetch_size
            # Constant SQL selected from allowlisted keys. Only the watermark
            # varies at execution time, and it is passed as a bind parameter.
            cur.execute(READ_QUERIES[schema_table], watermark=watermark)
            columns = [desc[0] for desc in cur.description]
            while True:
                rows = cur.fetchmany(fetch_size)
                if not rows:
                    break
                yield columns, rows
