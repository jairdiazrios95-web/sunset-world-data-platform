"""Unified command line with configuration driven profiles."""
import argparse, json
from pathlib import Path
from .config import load
from .generator import build
from .parquet import convert
from .audit import audit

def main():
    parser=argparse.ArgumentParser(description='Sunset synthetic data pipeline')
    parser.add_argument('action',choices=['generate','audit','parquet','check-config'])
    parser.add_argument('--profile',choices=['sample','volume'],default='sample')
    parser.add_argument('--config',type=Path)
    parser.add_argument('--input',type=Path)
    parser.add_argument('--output',type=Path)
    args=parser.parse_args()
    cfg=load(args.config)
    if args.action=='check-config': print(json.dumps(cfg,indent=2)); return
    if args.action=='generate':
        p=cfg['profiles'][args.profile]; out=args.output or Path('data/volume' if args.profile=='volume' else 'data/sample')
        print(json.dumps(build(out,p['leads'],p['payments'],p['comments'],
                        **cfg.get('generation',{}),catalogs=cfg.get('catalogs')),indent=2)); return
    if args.action=='audit':
        result=audit(args.input or Path('data/sample'))
        print(json.dumps(result,indent=2)); raise SystemExit(0 if result['status']=='PASS' else 1)
    if args.action=='parquet':
        inp=args.input or Path('data/volume' if args.profile=='volume' else 'data/sample')
        out=args.output or Path('data/parquet')
        print(json.dumps(convert(inp,out,args.config or Path('config/pipeline.json')),indent=2));return
if __name__=='__main__': main()
