"""CLI adapter: Parquet lake snapshots -> PostgreSQL raw landing."""
import argparse,json,os
from pathlib import Path
from sunset.config import load
from warehouse.loader import load_parquet

def main():
    p=argparse.ArgumentParser()
    p.add_argument('--lake',type=Path,default=Path('data/parquet'))
    p.add_argument('--source',required=True)
    p.add_argument('--config',type=Path,default=Path('config/pipeline.json'))
    p.add_argument('--batch-size',type=int,default=5000)
    a=p.parse_args()
    print(json.dumps(load_parquet(a.lake,a.source,load(a.config),
           os.environ.get('SUNSET_PG_DSN',''),a.batch_size),indent=2))

if __name__=='__main__':main()
