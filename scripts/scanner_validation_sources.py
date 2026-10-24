"""Materialize immutable existing filing profiles for a reserved validation draw.

No web-page scraping, new filing search, or source-text rewriting. Content and
issuer IDs are verified before a source can be used in a validation reference.
"""
import argparse
import gzip
import hashlib
import json
from pathlib import Path
import re
import urllib.request
from concurrent.futures import ThreadPoolExecutor


def catalog(index):
    base=index['asset_base_url']
    if not re.fullmatch(r'https://raw\.githubusercontent\.com/SRoyaltyy/fullscan/[a-f0-9]{40}/',base):
        raise ValueError('an immutable existing filing-profile revision is required')
    rows=[]
    for ticker,entry in sorted(index['profiles'].items()):
        path=entry['path']
        if '..' in Path(path).parts or Path(path).is_absolute():
            raise ValueError('unsafe stored source path')
        rows.append({'ticker':ticker,'cik':entry['cik'],'filing_backed':True,
                     'source_asset':{'sha256':entry['sha256'],'url':base+path}})
    return rows


def materialize(entry, root):
    asset=entry['source_asset']; url=asset['url']; sha=asset['sha256']
    if not re.fullmatch(r'https://raw\.githubusercontent\.com/SRoyaltyy/fullscan/[a-f0-9]{40}/[A-Za-z0-9_./-]+',url):
        raise ValueError('not an immutable stored source URL')
    if not re.fullmatch('[a-f0-9]{64}',sha):
        raise ValueError('invalid stored source checksum')
    target=Path(root)/(sha+'.json.gz')
    if target.exists():
        raw=target.read_bytes()
    else:
        with urllib.request.urlopen(url,timeout=60) as response:
            raw=response.read()
    if hashlib.sha256(raw).hexdigest()!=sha:
        raise ValueError('stored profile checksum mismatch: '+entry['ticker'])
    graph=json.loads(gzip.decompress(raw))
    if (graph.get('cik') and str(graph['cik']).zfill(10)!=entry['issuer_id']) or (
            not graph.get('cik') and (not url.endswith('/profiles/'+entry['issuer_id']+'.json.gz')
                                    or entry['ticker'] not in graph.get('tickers',[graph.get('ticker')]))):
        raise ValueError('stored profile issuer differs from reserved issuer')
    from src.scanner_validation import validate_stored_source_texts
    validate_stored_source_texts(graph)
    target.parent.mkdir(parents=True,exist_ok=True)
    if not target.exists():
        temporary=target.with_suffix('.tmp');temporary.write_bytes(raw);temporary.replace(target)
    return {'ticker':entry['ticker'],'issuer_id':entry['issuer_id'],'sha256':sha,
            'sources':len(graph['sources']),'paragraphs':len(graph['paragraphs']),
            'characters':sum(s['characters'] for s in graph['sources'])}


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--index',type=Path)
    p.add_argument('--catalog-out',type=Path)
    p.add_argument('--draw',type=Path)
    p.add_argument('--source-root',type=Path,default=Path('run_scratch/validation-sources'))
    p.add_argument('--split',choices=('development','holdout','all'),default='development')
    p.add_argument('--out',type=Path)
    a=p.parse_args()
    if a.index:
        if not a.catalog_out:p.error('--catalog-out required with --index')
        a.catalog_out.parent.mkdir(parents=True,exist_ok=True)
        a.catalog_out.write_text(json.dumps(catalog(json.loads(a.index.read_text())),indent=2)+'\n')
    elif a.draw:
        if not a.out:p.error('--out required with --draw')
        draw=json.loads(a.draw.read_text())
        entries=[e for e in draw['issuers'] if a.split=='all' or e['split']==a.split]
        with ThreadPoolExecutor(max_workers=4) as pool:
            results=list(pool.map(lambda entry:materialize(entry,a.source_root),entries))
        a.out.parent.mkdir(parents=True,exist_ok=True)
        a.out.write_text(json.dumps(results,indent=2)+'\n')
        print(json.dumps(results))
    else:p.error('choose --index or --draw')


if __name__=='__main__':main()
