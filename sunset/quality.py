"""Dependency-light contract checks for synthetic source snapshots.

Policies: BLOCK on critical integrity/structure failures, WARN on FX freshness
when using historical synthetic fixtures. External datasets require configured
SLAs and should not apply demo freshness thresholds blindly.
"""
from __future__ import annotations
import argparse
import csv
import json
import re
import sqlite3
import tempfile
from collections import Counter
from datetime import date
from pathlib import Path
from sunset.audit import audit
from sunset.config import load

RE_MONEY = re.compile(r'^\d+(?:\.\d{1,8})?$')
ISO = re.compile(r'^\d{4}-\d{2}-\d{2}$')


def inspect_csv(path: Path, spec: dict, source: str) -> tuple[dict, list]:
    keys = spec['primary_key']
    metrics = {'rows':0, 'missing_required':0,'invalid_dates':0,'invalid_amounts':0,
               'invalid_currency':0, 'duplicate_keys':0, 'negative_amounts':0}
    errors=[]
    # Use an on-disk key index for large sources; avoid >10M Python set entries.
    spill=tempfile.TemporaryDirectory(prefix='sunset_unique_') if path.stat().st_size>20_000_000 else None
    key_db=sqlite3.connect(str(Path(spill.name)/'keys.sqlite')) if spill else None
    if key_db:key_db.execute('CREATE TABLE keys(k TEXT PRIMARY KEY)')
    seen=set()
    try:
        with path.open(encoding='utf-8', newline='') as f:
            reader=csv.DictReader(f)
            if not set(keys + spec.get('required',[])).issubset(reader.fieldnames or []):
                return metrics, [f'{source}: missing required columns']
            for row in reader:
                metrics['rows']+=1
                if any(not row.get(k,'').strip() for k in spec.get('required',[])):
                    metrics['missing_required']+=1
                key=tuple(row[k] for k in keys)
                if key_db:
                    try:key_db.execute('INSERT INTO keys(k) VALUES(?)', (json.dumps(key),))
                    except sqlite3.IntegrityError:metrics['duplicate_keys']+=1
                else:
                    if key in seen:metrics['duplicate_keys']+=1
                    seen.add(key)
                column=spec.get('date_column')
                if column:
                    value=row.get(column,'')
                    try:
                        if not ISO.fullmatch(value): raise ValueError()
                        date.fromisoformat(value)
                    except (ValueError,TypeError):metrics['invalid_dates']+=1
                if source == 'fx_historico':
                    rate=row.get('tipo_cambio','')
                    if not RE_MONEY.fullmatch(rate): metrics['invalid_amounts']+=1
                    elif float(rate)<=0: metrics['negative_amounts']+=1
                    if row.get('moneda_origen') not in ('MXN','USD') or row.get('moneda_destino')!='MXN':
                        metrics['invalid_currency']+=1
                if source == 'leads_ventas':
                    if row.get('moneda') not in ('MXN','USD'):metrics['invalid_currency']+=1
                    if row.get('venta_id'):
                        amount=row.get('monto_venta','')
                        if not RE_MONEY.fullmatch(amount):metrics['invalid_amounts']+=1
                        elif float(amount)<=0:metrics['negative_amounts']+=1
    finally:
        if key_db:key_db.close()
        if spill:spill.cleanup()
    for k, v in metrics.items():
        if k!='rows' and v:
            errors.append(f'{source}: {k}={v}')
    if not metrics['rows']:errors.append(f'{source}: source empty')
    return metrics, errors


def check(input_dir: Path, config_path: Path = Path('config/pipeline.json'), scope: str = 'full') -> dict:
    config=load(config_path)
    scoped = ('leads_ventas','fx_historico') if scope=='core' else tuple(config['sources'])
    metrics={}; errors=[]; warnings=[]
    for source in scoped:
        path=input_dir/f'{source}.csv'
        if not path.exists():
            errors.append(f'missing {path}')
            continue
        metrics[source], issues = inspect_csv(path, config['sources'][source], source)
        errors.extend(issues)
        # Freshness: historical fixture is intentionally stale. Warn, don't block.
        column=config['sources'][source].get('date_column')
        threshold=config.get('monitoring',{}).get('max_fx_age_days' if source=='fx_historico' else 'max_lead_age_days')
        if column and threshold is not None:
            try:
                with path.open(encoding='utf-8',newline='') as file:
                    values=(row[column] for row in csv.DictReader(file) if row[column])
                    latest=max(values,default=None)
                if latest:
                    age=(date.today()-date.fromisoformat(latest[:10])).days
                    metrics[source]['freshness_days']=age
                    if age>threshold:warnings.append(f'{source}: historical fixture is {age} days old; live SLA would fail')
            except (ValueError,KeyError) as exc:
                errors.append(f'{source}: freshness check failed: {exc}')
    integrity=None
    if scope=='full' and not errors:
        integrity=audit(input_dir)
        if integrity['status']!='PASS':errors.append('referential integrity audit failed')
    rules_evaluated=sum(7 for name in metrics if name in ('leads_ventas','fx_historico'))
    return {'status':'FAIL' if errors else 'PASS','mode':'SYNTHETIC',
            'checks_evaluated_at_least':rules_evaluated,'metrics':metrics,
            'cross_system_audit':integrity,'warnings':warnings,'errors':errors}


def main():
    p=argparse.ArgumentParser();p.add_argument('--input',type=Path,default=Path('data/sample'))
    p.add_argument('--config',type=Path,default=Path('config/pipeline.json'))
    p.add_argument('--scope',choices=['core','full'],default='full')
    p.add_argument('--report',type=Path,default=Path('reports/quality_local.json'))
    a=p.parse_args();r=check(a.input,a.config,a.scope)
    a.report.parent.mkdir(parents=True,exist_ok=True)
    a.report.write_text(json.dumps(r,indent=2,ensure_ascii=False)+'\n')
    print(json.dumps({'status':r['status'],'checks':r['checks_evaluated_at_least'],'errors':r['errors']}))
    raise SystemExit(0 if r['status']=='PASS' else 1)

if __name__=='__main__':main()
