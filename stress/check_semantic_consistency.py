"""Check cross-system semantic joins against an existing SQLite stress database."""
from __future__ import annotations
import argparse,json,sqlite3,time
from datetime import datetime,timezone
from pathlib import Path

QUERIES={
  'contract_client_mismatch': '''SELECT count(*) FROM contratos k
    JOIN leads l ON k.lead_id=l.lead_id WHERE k.cliente_id<>l.cliente_id''',
  'comment_client_mismatch': '''SELECT count(*) FROM comentarios cm
    JOIN tours t ON cm.tour_id=t.tour_id JOIN leads l ON t.lead_id=l.lead_id
    WHERE cm.cliente_id<>l.cliente_id''',
  'sale_currency_conversion_mismatch': '''SELECT count(*) FROM fact_ventas v
    JOIN fx f ON f.day=v.day AND f.moneda=v.moneda
    WHERE v.monto_mxn_cent<>CAST(ROUND((v.monto_cent*1.0*f.rate_bps)/10000.0) AS INTEGER)''',
  'non_positive_paid_amounts': 'SELECT count(*) FROM pagos WHERE monto_cent<=0',
  'non_positive_sale_amounts': 'SELECT count(*) FROM fact_ventas WHERE monto_cent<=0',
}

def check(path:Path):
    c=sqlite3.connect(f'file:{path}?mode=ro',uri=True)
    t=time.perf_counter();violations={}
    for name,query in QUERIES.items():
        violations[name]=c.execute(query).fetchone()[0]
    facts={t:c.execute(f'SELECT count(*) FROM {t}').fetchone()[0] for t in ['fact_ventas','fact_pagos','fact_interacciones']}
    c.close()
    return {'engine':'SQLite relational proxy, not production PostgreSQL','status':'PASS' if not any(violations.values()) else 'FAIL',
      'violations':violations,'facts':facts,'elapsed_seconds':round(time.perf_counter()-t,3),
      'verified_utc':datetime.now(timezone.utc).isoformat(timespec='seconds')}

def main():
    a=argparse.ArgumentParser();a.add_argument('--db',type=Path,required=True)
    a.add_argument('--report',type=Path,required=True)
    v=a.parse_args();r=check(v.db);v.report.parent.mkdir(parents=True,exist_ok=True)
    v.report.write_text(json.dumps(r,indent=2,ensure_ascii=False),encoding='utf-8')
    print(json.dumps(r,ensure_ascii=False));raise SystemExit(0 if r['status']=='PASS' else 1)
if __name__=='__main__':main()
