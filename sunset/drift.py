"""Population stability index for categorical drift, with smoothing."""
from __future__ import annotations
import math
from collections import Counter

def categorical_psi(reference:list[str],current:list[str],smoothing=1e-5)->float:
    if not reference or not current or smoothing<=0:raise ValueError('Need reference and current samples')
    a=Counter(reference);b=Counter(current);categories=set(a)|set(b)
    score=0.0
    for item in categories:
        p=max(a.get(item,0)/len(reference),smoothing)
        q=max(b.get(item,0)/len(current),smoothing)
        score+=(q-p)*math.log(q/p)
    return score

if __name__=='__main__':
 import argparse,csv,json
 from pathlib import Path
 p=argparse.ArgumentParser();p.add_argument('--reference',type=Path,required=True)
 p.add_argument('--current',type=Path,required=True);p.add_argument('--column',required=True)
 a=p.parse_args()
 def values(path):
  with path.open(encoding='utf-8',newline='') as f:
   reader=csv.DictReader(f)
   if a.column not in reader.fieldnames:raise ValueError('Unknown column')
   return [row[a.column] for row in reader]
 psi=categorical_psi(values(a.reference),values(a.current))
 print(json.dumps({'column':a.column,'psi':round(psi,5),
                   'interpretation':'Human review; no universal threshold for seasonal business'}))
