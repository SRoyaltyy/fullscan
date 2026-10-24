"""Compile a checked-out Open English WordNet YAML directory to local lookups.

Build-time only; PyYAML is needed here, never in the scanner. Pin source
hashes in the generated manifest and retain upstream license attribution.
"""
import argparse
import hashlib
import json
import gzip
from pathlib import Path
import yaml


def build(source: Path, destination: Path):
    destination.mkdir(parents=True,exist_ok=True)
    manifest = {'upstream':'https://github.com/globalwordnet/english-wordnet',
                'license':'CC-BY-4.0; see wordnet.LICENSE','files':[],'senses':0}
    for path in sorted(source.glob('*.yaml')):
        raw = path.read_bytes().replace(b'\r\n',b'\n').rstrip(b'\n')+b'\n'
        data = yaml.safe_load(raw)
        rows = []
        for sense, item in data.items():
            words = item.get('members') or []
            gloss = ' '.join(item.get('definition') or [])
            if not words or not gloss:
                continue
            rows.append([sense,words,gloss,item.get('partOfSpeech','')])
        target = destination / (path.stem + '.json.gz')
        decoded=json.dumps(rows,ensure_ascii=True,separators=(',',':')).encode('utf-8')
        packed=bytearray(gzip.compress(decoded,mtime=0))
        # Python 3.11/3.12 delegate mtime=0 to zlib and write a platform OS
        # byte; later Python versions use 255. Canonicalize this metadata so
        # identical pinned dictionaries have identical compressed hashes.
        packed[9]=255
        target.write_bytes(packed)
        manifest['files'].append({'file':target.name,'source':path.name,'source_sha256':hashlib.sha256(raw).hexdigest(),
            'sha256':hashlib.sha256(decoded).hexdigest(),'stored_sha256':hashlib.sha256(target.read_bytes()).hexdigest(),'senses':len(rows)})
        manifest['senses'] += len(rows)
    (destination / 'manifest.json').write_text(json.dumps(manifest,indent=2),encoding='utf-8')
    print(json.dumps({'files':len(manifest['files']),'senses':manifest['senses']}))


if __name__ == '__main__':
    p=argparse.ArgumentParser()
    p.add_argument('source',type=Path)
    p.add_argument('--out',type=Path,default=Path('src/data/senses'))
    a=p.parse_args();build(a.source,a.out)
