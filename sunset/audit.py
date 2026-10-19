#!/usr/bin/env python3
"""Auditoría reproducible de CSV sintéticos (sin dependencias externas).
Los datos no representan el modelo real de Sunset World.
"""
import argparse
import csv
import json
import sqlite3
import tempfile
from collections import Counter
from datetime import date
from pathlib import Path

FILES = ('clientes','leads_ventas','tours','contratos','pagos','comentarios','fx_historico','salas','fuentes','productos')
PRIMARY_KEYS = {
    'clientes': ('cliente_id',), 'leads_ventas': ('lead_id',),
    'tours': ('tour_id',), 'contratos': ('contrato_id',),
    'pagos': ('pago_id',), 'comentarios': ('comentario_id',),
    'fx_historico': ('fecha','moneda_origen','moneda_destino'),
    'salas': ('sala_id',), 'fuentes': ('fuente_id',),
    'productos': ('producto_id',),
}
RELATIONS = (
    ('leads_ventas','cliente_id','clientes','cliente_id'),
    ('leads_ventas','fuente_id','fuentes','fuente_id'),
    ('leads_ventas','sala_id','salas','sala_id'),
    ('leads_ventas','producto_id','productos','producto_id'),
    ('tours','lead_id','leads_ventas','lead_id'),
    ('tours','cliente_id','clientes','cliente_id'),
    ('contratos','cliente_id','clientes','cliente_id'),
    ('contratos','venta_id','leads_ventas','venta_id'),
    ('pagos','contrato_id','contratos','contrato_id'),
    ('comentarios','tour_id','tours','tour_id'),
    ('comentarios','cliente_id','clientes','cliente_id'),
)

def audit(input_dir: Path):
    total_bytes = sum((input_dir/f'{name}.csv').stat().st_size for name in FILES)
    temp = tempfile.TemporaryDirectory(prefix='sunset_audit_') if total_bytes > 50_000_000 else None
    conn = sqlite3.connect(str(Path(temp.name)/'audit.sqlite') if temp else ':memory:')
    conn.execute('PRAGMA cache_size=-32000')
    conn.execute('PRAGMA temp_store=FILE')
    conn.execute('PRAGMA synchronous=NORMAL')
    counts = {}
    duplicates = {}
    invalid_dates = Counter()
    try:
        for name in FILES:
            with (input_dir / f'{name}.csv').open(newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                columns = reader.fieldnames
                if not columns or not set(PRIMARY_KEYS[name]).issubset(columns):
                    raise ValueError(f'Cabecera inválida: {name}')
                quoted_columns = ','.join('"' + c + '" TEXT' for c in columns)
                conn.execute(f'CREATE TABLE "{name}" ({quoted_columns})')
                names = ','.join('"'+c+'"' for c in columns)
                marks = ','.join('?' for _ in columns)
                rows = []
                total = 0
                for record in reader:
                    total += 1
                    rows.append(tuple(record[c] for c in columns))
                    for c in ('fecha', 'fecha_creacion','fecha_venta','fecha_pago','fecha_tour','fecha_comentario'):
                        if c in record and record[c]:
                            try: date.fromisoformat(record[c])
                            except ValueError: invalid_dates[f'{name}.{c}'] += 1
                    if len(rows) >= 5000:
                        conn.executemany(f'INSERT INTO "{name}" ({names}) VALUES ({marks})', rows)
                        rows.clear()
                        if total % 100000 == 0:
                            conn.commit()
                if rows:
                    conn.executemany(f'INSERT INTO "{name}" ({names}) VALUES ({marks})', rows)
                conn.commit()
                # Indexing during the audit prevents join probes from scanning millions
                # of parent rows. Non-unique on purpose: duplicate detection comes next.
                pkcols = ','.join('\"'+c+'\"' for c in PRIMARY_KEYS[name])
                conn.execute(f'CREATE INDEX idx_audit_{name}_pk ON \"{name}\" ({pkcols})')
                if name == 'leads_ventas':
                    conn.execute('CREATE INDEX idx_audit_venta_id ON leads_ventas(venta_id)')
                counts[name] = total
                key = ','.join('"'+c+'"' for c in PRIMARY_KEYS[name])
                duplicates[name] = conn.execute(f'SELECT COUNT(*) FROM (SELECT {key} FROM "{name}" GROUP BY {key} HAVING COUNT(*)>1)').fetchone()[0]
        orphans = {}
        for child, fk, parent, pk in RELATIONS:
            label = f'{child}.{fk}->{parent}.{pk}'
            orphans[label] = conn.execute(f'''SELECT count(*) FROM "{child}" c
                    LEFT JOIN "{parent}" p ON c."{fk}"=p."{pk}"
                    WHERE c."{fk}" IS NOT NULL AND c."{fk}"<>'' AND p."{pk}" IS NULL''').fetchone()[0]
        orphans['leads_ventas.fecha_venta+moneda->fx_historico'] = conn.execute('''
            SELECT count(*) FROM leads_ventas l LEFT JOIN fx_historico f
            ON l.fecha_venta=f.fecha AND l.moneda=f.moneda_origen AND f.moneda_destino='MXN'
            WHERE l.venta_id<>'' AND f.fecha IS NULL''').fetchone()[0]
        consistency = {
            'tours_cliente_vs_leads': conn.execute('''SELECT count(*) FROM tours t JOIN leads_ventas l ON l.lead_id=t.lead_id WHERE t.cliente_id<>l.cliente_id''').fetchone()[0],
            'contratos_cliente_vs_ventas': conn.execute('''SELECT count(*) FROM contratos c JOIN leads_ventas l ON c.venta_id=l.venta_id WHERE c.cliente_id<>l.cliente_id''').fetchone()[0]
        }
        result = {'mode':'SYNTHETIC', 'row_counts':counts, 'duplicate_keys':duplicates,
                  'orphans':orphans,'consistency_errors':consistency,
                  'invalid_dates':dict(invalid_dates)}
        result['status'] = 'PASS' if all(v==0 for section in ('duplicate_keys','orphans','consistency_errors','invalid_dates') for v in result[section].values()) else 'FAIL'
        return result
    finally:
        conn.close()
        if temp is not None:temp.cleanup()

if __name__ == '__main__':
    p=argparse.ArgumentParser()
    p.add_argument('--input',type=Path,default=Path('data/sample'))
    p.add_argument('--output',type=Path,default=Path('reports/data_audit.json'))
    a=p.parse_args()
    result=audit(a.input)
    a.output.parent.mkdir(parents=True,exist_ok=True)
    a.output.write_text(json.dumps(result,indent=2,ensure_ascii=False)+'\n',encoding='utf-8')
    print(json.dumps(result,indent=2,ensure_ascii=False))
    raise SystemExit(0 if result['status']=='PASS' else 1)
