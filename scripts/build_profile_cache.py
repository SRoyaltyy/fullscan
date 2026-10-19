"""Compile all stock profiles once; emit a portable, non-executable JSONL cache."""
import argparse
import gzip
import json
import time
import os
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

from src.filing_evidence import load_section_cache,load_filing_sections
from src.stock_evidence import Catalog,build_profile

_catalog=None


def _init_worker(metadata):
    global _catalog
    _catalog=Catalog(metadata)


def _compile_row(row):
    profile=build_profile(row,_catalog)
    data=vars(profile).copy()
    data['core']=sorted(profile.core)
    data['buckets']={k:sorted(v) for k,v in profile.buckets.items()}
    data['facts']=[f.record() for f in profile.facts]
    data['locations']=sorted(profile.locations)
    data['evidence']=dict(profile.evidence)
    return json.dumps(data,ensure_ascii=True,separators=(',',':'))


def main():
    p=argparse.ArgumentParser()
    p.add_argument('--section-cache',type=Path)
    p.add_argument('--objects',type=Path)
    p.add_argument('--descriptions',type=Path)
    p.add_argument('--as-of')
    p.add_argument('--out',type=Path,required=True)
    p.add_argument('--workers',type=int,default=4)
    a=p.parse_args()
    started=time.perf_counter()
    if a.section_cache:
        rows,coverage=load_section_cache(a.section_cache,as_of=a.as_of,progress=lambda x:print(json.dumps(x),flush=True))
    else:
        if not (a.objects and a.descriptions):p.error('provide --section-cache or --objects and --descriptions')
        from src.link_correlation import load_stocks
        sections,coverage=load_filing_sections(a.objects,as_of=a.as_of,strict=True)
        rows=load_stocks(a.descriptions,None,None,sections)
    a.out.parent.mkdir(parents=True,exist_ok=True)
    metadata=[{k:r.get(k,'') for k in ('ticker','name','industry','sector')} for r in rows]
    temporary=a.out.with_name(a.out.name+'.partial')
    with ProcessPoolExecutor(max_workers=max(1,a.workers),initializer=_init_worker,initargs=(metadata,)) as pool, gzip.open(temporary,'wt',encoding='utf-8') as out:
        out.write(json.dumps({'format':'stock-evidence-1','coverage':coverage,'as_of':a.as_of})+'\n')
        for i,line in enumerate(pool.map(_compile_row,rows,chunksize=2),1):
            out.write(line+'\n')
            if i%50==0:
                print(json.dumps({'companies':i,'seconds':round(time.perf_counter()-started,2)}),flush=True)
    temporary.replace(a.out)
    print(json.dumps({'out':str(a.out),'companies':len(rows),'seconds':time.perf_counter()-started,'coverage':coverage}),flush=True)


if __name__=='__main__':main()
