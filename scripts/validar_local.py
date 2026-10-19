#!/usr/bin/env python3
"""Generate deterministic synthetic records and run *real* SQLite integration.

No third-party dependency or Docker required. Not a Parquet/PostgreSQL execution.
"""
from __future__ import annotations
import argparse
import json
import sys
import time
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from sunset.config import load
from sunset.generator import build
from sunset.verify_end_to_end_sqlite import verify


def main():
    p=argparse.ArgumentParser(description='Sunset reproducible local verification')
    p.add_argument('--config',type=Path,default=ROOT/'config/pipeline.json')
    p.add_argument('--leads',type=int,default=1000)
    p.add_argument('--payments',type=int,default=600)
    p.add_argument('--comments',type=int,default=1200)
    p.add_argument('--batch-size',type=int,default=None)
    p.add_argument('--workdir',type=Path,default=ROOT/'reports/runs/local_verification')
    p.add_argument('--report',type=Path,default=ROOT/'reports/local_pipeline_execution.json')
    a=p.parse_args()
    if min(a.leads,a.payments,a.comments)<1:raise ValueError('Record counts must be positive')
    a.workdir.mkdir(parents=True,exist_ok=True)
    cfg=load(a.config)
    t=time.perf_counter()
    expected=build(a.workdir/'synthetic_sources',a.leads,a.payments,a.comments,
                   **cfg['generation'],catalogs=cfg.get('catalogs'))
    generated_sec=round(time.perf_counter()-t,3)
    actual=verify(a.workdir/'synthetic_sources',a.workdir/'demo.sqlite',a.config,a.batch_size)
    actual['generation_seconds']=generated_sec
    actual['requested_main_source_counts']={'leads_ventas':a.leads,'pagos':a.payments,'comentarios':a.comments}
    actual['generated_counts']=expected
    a.report.parent.mkdir(parents=True,exist_ok=True)
    a.report.write_text(json.dumps(actual,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    print(json.dumps({'status':actual['status'],'main_records':actual['requested_main_source_counts'],
                      'facts':actual['reconciliation']['warehouse_counts'],
                      'idempotent':actual['identical_replay_hash'],
                      'elapsed_seconds':actual['elapsed_seconds'],
                      'generation_seconds':generated_sec,
                      'report':str(a.report)},indent=2))

if __name__=='__main__':main()
