"""Runnable evidence of join logic, reconciliation and business-grain contracts.
Uses standard-library SQLite; NOT a replacement for executing PostgreSQL marts.
"""
from __future__ import annotations
import argparse,csv,json,sqlite3,tempfile,time
from pathlib import Path
from sunset.quality import check

SOURCES=['clientes','leads_ventas','tours','contratos','pagos','comentarios','fx_historico',
         'salas','fuentes','productos']

def demonstrate(input_dir:Path,config:Path=Path('config/pipeline.json')):
    start=time.monotonic()
    quality=check(input_dir,config,'full')
    if quality['status']!='PASS':raise ValueError('Quality gate blocked offline publication')
    with tempfile.TemporaryDirectory(prefix='sunset_offline_') as root:
        db=sqlite3.connect(str(Path(root)/'demo.sqlite'))
        try:
            counts={}
            for name in SOURCES:
                with (input_dir/f'{name}.csv').open(encoding='utf-8',newline='') as f:
                    reader=csv.DictReader(f);cols=reader.fieldnames
                    quoted=','.join(f'"{column}" TEXT' for column in cols)
                    db.execute(f'CREATE TABLE "{name}" ({quoted})')
                    marks=','.join('?' for _ in cols)
                    column_names=','.join(f'"{c}"' for c in cols)
                    batch=[];count=0
                    for row in reader:
                        batch.append([row[c] for c in cols]);count+=1
                        if len(batch)>=5000:
                            db.executemany(f'INSERT INTO "{name}" ({column_names}) VALUES ({marks})',batch)
                            batch=[]
                    if batch:db.executemany(f'INSERT INTO "{name}" ({column_names}) VALUES ({marks})',batch)
                    counts[name]=count
            db.execute('CREATE INDEX sales_sale_id_idx ON leads_ventas(venta_id)')
            db.execute('CREATE INDEX fx_lookup_idx ON fx_historico(fecha,moneda_origen,moneda_destino)')
            db.execute('CREATE INDEX contract_sale_idx ON contratos(venta_id)')
            db.execute('CREATE INDEX payment_contract_idx ON pagos(contrato_id)')
            db.execute('CREATE INDEX comment_tour_idx ON comentarios(tour_id)')
            def one(sql):return db.execute(sql).fetchone()[0]
            stats={
                'sales':one("SELECT COUNT(*) FROM leads_ventas WHERE venta_id<>''"),
                'qualified_tours':counts['tours'],
                'sales_with_fx':one("""SELECT COUNT(*) FROM leads_ventas l
                    JOIN fx_historico fx ON fx.fecha=l.fecha_venta AND fx.moneda_origen=l.moneda
                    AND fx.moneda_destino='MXN' WHERE l.venta_id<>''"""),
                'contracts_linked_to_sale':one("""SELECT COUNT(*) FROM contratos c
                    JOIN leads_ventas l ON c.venta_id=l.venta_id AND c.cliente_id=l.cliente_id"""),
                'payments_with_contract':one('''SELECT COUNT(*) FROM pagos p
                    JOIN contratos c ON p.contrato_id=c.contrato_id'''),
                'comments_with_tour':one('''SELECT COUNT(*) FROM comentarios cm
                    JOIN tours t ON cm.tour_id=t.tour_id AND cm.cliente_id=t.cliente_id'''),
                'sales_usd_amount_mxn':round(one("""SELECT COALESCE(SUM(CAST(l.monto_venta AS REAL)*CAST(fx.tipo_cambio AS REAL)),0)
                    FROM leads_ventas l JOIN fx_historico fx ON l.fecha_venta=fx.fecha
                    AND l.moneda=fx.moneda_origen AND fx.moneda_destino='MXN'
                    WHERE l.venta_id<>''"""),2),
            }
            requirements={
                'sales_with_fx':stats['sales'],
                'contracts_linked_to_sale':counts['contratos'],
                'payments_with_contract':counts['pagos'],
                'comments_with_tour':counts['comentarios']}
            discrepancies={k:{'actual':stats[k],'expected':v} for k,v in requirements.items()
                           if stats[k]!=v}
            if discrepancies:raise AssertionError(f'Offline join reconciliation failed: {discrepancies}')
            return {'status':'PASS','mode':'SYNTHETIC_SQLITE_ONLY',
                    'source_counts':counts,'join_statistics':stats,
                    'quality_checks':quality['checks_evaluated_at_least'],
                    'joins_reconciled':len(requirements),
                    'elapsed_seconds':round(time.monotonic()-start,3),
                    'postgres_execution':'NOT_EXECUTED','parquet_execution':'NOT_EXECUTED'}
        finally:db.close()

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--input',type=Path,default=Path('data/sample'))
    p.add_argument('--config',type=Path,default=Path('config/pipeline.json'))
    p.add_argument('--report',type=Path,default=Path('reports/offline_demo_verified.json'))
    a=p.parse_args();result=demonstrate(a.input,a.config)
    a.report.parent.mkdir(parents=True,exist_ok=True)
    a.report.write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps(result,ensure_ascii=False,indent=2))
