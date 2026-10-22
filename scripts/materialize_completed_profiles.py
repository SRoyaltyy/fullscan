"""Download every checksum-bound issuer from an immutable disclosure manifest."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
import re
import sys
import urllib.request

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from src.completed_profile_store import validate_manifest, validate_profile


def materialize(manifest, out, *, fetch=None, workers=8):
    import gzip
    validate_manifest(manifest)
    fetch=fetch or (lambda url: urllib.request.urlopen(url,timeout=90).read())
    out=Path(out);out.mkdir(parents=True,exist_ok=True)
    entries={e['path']:e for e in manifest['profiles'].values()}
    def download(item):
        name,entry=item
        path=out/name
        raw=path.read_bytes() if path.is_file() else None
        if raw is None or hashlib.sha256(raw).hexdigest()!=entry['sha256']:
            raw=fetch(manifest['asset_base_url']+name)
        if hashlib.sha256(raw).hexdigest()!=entry['sha256']:
            raise ValueError('disclosure asset checksum mismatch: '+name)
        graph=validate_profile(json.loads(gzip.decompress(raw)))
        if (graph['as_of']!=manifest['as_of'] or len(graph['facts'])!=entry['facts']
                or graph['compiler_sha256']!=manifest['compiler_sha256']):
            raise ValueError('mixed disclosure profiles')
        path.parent.mkdir(parents=True,exist_ok=True)
        temp=path.with_suffix('.tmp');temp.write_bytes(raw);temp.replace(path)
        return len(graph['facts'])
    facts=0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for i,count in enumerate(pool.map(download,sorted(entries.items())),1):
            facts+=count
            if i%100==0:print(json.dumps({'validated_issuers':i,'of':len(entries)}),flush=True)
    if facts!=manifest['published']['relationships']:
        raise ValueError('materialized archive lost relationships')
    # The index becomes available only after all listed issuer files validate.
    raw=json.dumps(manifest,sort_keys=True,separators=(',',':')).encode()
    (out/'index.json.tmp').write_bytes(raw);(out/'index.json.tmp').replace(out/'index.json')
    return manifest


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--manifest-url',required=True)
    p.add_argument('--out',type=Path,required=True)
    a=p.parse_args()
    if not re.fullmatch(r'https://raw\.githubusercontent\.com/SRoyaltyy/fullscan/[a-f0-9]{40}/dashboard/scanner-backtest/completed/manifest.json',a.manifest_url):
        p.error('an immutable approved manifest URL is required')
    with urllib.request.urlopen(a.manifest_url,timeout=30) as stream:manifest=json.load(stream)
    materialize(manifest,a.out)


if __name__=='__main__':main()
