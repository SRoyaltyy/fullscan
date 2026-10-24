"""Prepare source-first diagnostic passages from hash-pinned stored witnesses.

This is NOT a full-filing recall benchmark. Missing source paragraphs in the
published archive projection are not reconstructed or claimed to be reviewed.
"""
import argparse,gzip,hashlib,json,random,re,urllib.request
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor


def prepare(entry,seed,cache):
    asset=entry['source_asset'];target=cache/(asset['sha256']+'.json.gz')
    if target.exists():raw=target.read_bytes()
    else:
        with urllib.request.urlopen(asset['url'],timeout=60) as r:raw=r.read()
        if hashlib.sha256(raw).hexdigest()!=asset['sha256']:raise ValueError('source asset checksum mismatch')
        cache.mkdir(parents=True,exist_ok=True);target.write_bytes(raw)
    if hashlib.sha256(raw).hexdigest()!=asset['sha256']:raise ValueError('cached source checksum mismatch')
    graph=json.loads(gzip.decompress(raw)); sources={s['id']:s for s in graph['sources']}
    pool=[]
    for p in graph['paragraphs']:
        if p['source_id'] not in sources or p['end']-p['start']!=len(p['text']):raise ValueError('invalid source witness')
        if p.get('sha256') and hashlib.sha256(p['text'].encode()).hexdigest()!=p['sha256']:raise ValueError('witness checksum mismatch')
        if 35<=len(p['text'].split())<=130 and sources[p['source_id']].get('kind')!='machine_data':pool.append(p)
    rng=random.Random(seed+int(entry['issuer_id']))
    # Source-only sampling. No candidate predictions or labels are consulted.
    # A separate mechanism diagnostic for distant explicit definitions. This
    # deliberately enriches definition-bearing cases and is not population sampling.
    from src.stock_evidence_graph import ALIAS,specific
    all_paragraphs=[p for p in graph['paragraphs'] if sources[p['source_id']].get('kind')!='machine_data']
    candidates=[]
    for p in pool:
        for m in ALIAS.finditer(p['text']):
            alias=m.group('alias')
            if not specific(alias) or alias.casefold() in {'notes','agreement','securities','indenture','exchange act','act','board'}:continue
            scope=lambda p:tuple(sources[p['source_id']].get(k,'') for k in ('accession','document','filing_date'))
            others=[q for q in all_paragraphs if q['id']!=p['id'] and scope(q)==scope(p)
                    and 20<=len(q['text'].split())<=150 and alias in q['text']
                    and (q['source_id']!=p['source_id'] or abs(q['start']-p['start'])>500)]
            if others:candidates.append((p,alias,others))
    if candidates:
        first,alias,others=rng.choice(candidates);chosen=[first,rng.choice(others)]
        design='definition-enriched, nonadjacent paragraph pair'; target_alias=alias
    else:
        chosen=rng.sample(sorted(pool,key=lambda p:(p['source_id'],p['start'])),min(2,len(pool)))
        design='random witness paragraphs; no eligible distant definition pair';target_alias=None
    return {'ticker':entry['ticker'],'issuer_id':entry['issuer_id'],'name':graph['name'],
            'split':entry['split'],'asset_sha256':asset['sha256'],
            'design':design,'target_alias':target_alias,
            'coverage':'two existing source witness paragraphs; not full filing text',
            'paragraphs':[{'text':p['text'],'start':p['start'],'end':p['end'],
                'sha256':hashlib.sha256(p['text'].encode()).hexdigest(),
                'source':sources[p['source_id']]} for p in chosen]}


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--draw',type=Path,required=True)
    p.add_argument('--out',type=Path,required=True);p.add_argument('--cache',type=Path,default=Path('run_scratch/witness-assets'))
    a=p.parse_args();draw=json.loads(a.draw.read_text())
    with ThreadPoolExecutor(max_workers=4) as pool:
        cases=list(pool.map(lambda e:prepare(e,draw['seed'],a.cache),draw['issuers']))
    a.out.parent.mkdir(parents=True,exist_ok=True)
    a.out.write_text(json.dumps({'draw_id':draw['id'],'methodology_sha256':draw['methodology_sha256'],
        'scope':'source-witness diagnostic only; source-first labels must be frozen before predictions',
        'cases':cases},indent=2,ensure_ascii=False)+'\n')
    print(json.dumps({'cases':len(cases),'paragraphs':sum(len(c['paragraphs']) for c in cases)}))


if __name__=='__main__':main()
