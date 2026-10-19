"""Materialize original V0.4 CSV generator at requested stress volume (streaming batches)."""
import argparse,hashlib,json,resource,time
from pathlib import Path
from datetime import datetime,timezone
from sunset.generator import build

def main():
    p=argparse.ArgumentParser()
    p.add_argument('--config',type=Path,default=Path('config/stress.json'))
    p.add_argument('--out',type=Path,default=Path('data/stress'))
    p.add_argument('--report',type=Path,default=Path('reports/stress_csv_generation.json'))
    a=p.parse_args()
    cfg=json.loads(a.config.read_text())
    t=time.perf_counter()
    counts=build(a.out,cfg['leads'],cfg['pagos'],cfg['comentarios'],
          seed=cfg['seed'],chunk_size=cfg['chunk_size'])
    sizes={p.name:p.stat().st_size for p in a.out.glob('*.csv')}
    report={'type':'actual_v04_csv_generator','status':'PASS','counts':counts,
       'files_bytes':sizes,'total_bytes':sum(sizes.values()),
       'elapsed_seconds':round(time.perf_counter()-t,3),
       'peak_process_rss_mib':round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss/1024,2)}
    a.report.parent.mkdir(parents=True,exist_ok=True)
    a.report.write_text(json.dumps(report,indent=2,ensure_ascii=False),encoding='utf-8')
    print(json.dumps({'status':'PASS','counts':counts,'elapsed_seconds':report['elapsed_seconds'],'rss':report['peak_process_rss_mib']},ensure_ascii=False),flush=True)
if __name__=='__main__':main()
