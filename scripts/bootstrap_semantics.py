"""One-time, checksum-verified build from public Open English WordNet sources.

Scanning never downloads data. This installer needs PyYAML at build time.
Only the source names and digests in the checked-in manifest are accepted.
"""
import argparse,hashlib,json,tempfile,urllib.request
from pathlib import Path
from scripts.build_semantic_resources import build

UPSTREAM_COMMIT='bff3181fe5c810dcd157cba0eed60322a6e0aaed'

def main():
 p=argparse.ArgumentParser();p.add_argument('--source',type=Path);p.add_argument('--out',type=Path,default=Path('src/data/senses'));a=p.parse_args()
 expected=json.loads((a.out/'manifest.json').read_text(encoding='utf-8'))
 with tempfile.TemporaryDirectory(prefix='fullscan-wordnet-',dir=Path.cwd()) as temporary:
  root=Path(temporary)
  for entry in expected['files']:
   name=entry['source']
   if Path(name).name != name or not name.endswith('.yaml'):raise ValueError('invalid source filename')
   if a.source:
    raw=(a.source/name).read_bytes()
   else:
    url=f'https://raw.githubusercontent.com/globalwordnet/english-wordnet/{UPSTREAM_COMMIT}/src/yaml/{name}'
    with urllib.request.urlopen(url,timeout=60) as response:raw=response.read()
   raw=raw.replace(b'\r\n',b'\n').rstrip(b'\n')+b'\n'
   if hashlib.sha256(raw).hexdigest()!=entry['source_sha256']:raise ValueError('pinned source mismatch: '+name)
   (root/name).write_bytes(raw)
   print('verified '+name,flush=True)
  build(root,a.out)
  actual=json.loads((a.out/'manifest.json').read_text(encoding='utf-8'))
  if {x['file']:x['sha256'] for x in actual['files']}!={x['file']:x['sha256'] for x in expected['files']}:
   raise ValueError('rebuilt sense pack differs from pinned data')

if __name__=='__main__':main()
