"""Select artifacts compatible with the current parser before downloading them."""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import subprocess

INPUTS=('src/filing_relationships.py','src/filing_evidence.py','src/term_inventory.py',
        'scripts/build_filing_profiles.py','requirements-filing-parser.txt')
ROOT=Path(__file__).resolve().parents[1]


def select(full,samples,compatible):
    for pool,scope in ((full,'archive_set'),(samples,'development_sample')):
        for artifact in sorted(pool,key=lambda a:a['created_at'],reverse=True):
            run=artifact.get('workflow_run',{})
            if artifact.get('expired') or run.get('head_branch')!='main' or not run.get('head_sha'):continue
            if compatible(run['head_sha']):
                return {'ready':'true','run':str(run['id']),'artifact':artifact['name'],'scope':scope,
                        'profile_assets':'keywords' if scope=='archive_set' else 'full'}
    return {'ready':'false'}


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--full',type=Path,required=True)
    p.add_argument('--samples',type=Path,required=True)
    p.add_argument('--repository',required=True)
    a=p.parse_args()
    local={path:(ROOT/path).read_bytes() for path in INPUTS}
    digest=hashlib.sha256()
    for path,data in local.items():digest.update(path.encode()+b'\0'+data+b'\0')
    from src.filing_relationships import compiler_fingerprint
    if digest.hexdigest()!=compiler_fingerprint():raise ValueError('artifact selector inputs differ from compiler fingerprint; update INPUTS')
    cache={}
    def compatible(sha):
        if sha in cache:return cache[sha]
        for path,data in local.items():
            result=subprocess.run(['gh','api',f'repos/{a.repository}/contents/{path}?ref={sha}'],text=True,capture_output=True)
            if result.returncode:
                if '404' in result.stderr:cache[sha]=False;return False
                raise RuntimeError('could not verify artifact compiler source: '+sha)
            response=json.loads(result.stdout)
            if base64.b64decode(response['content'])!=data:cache[sha]=False;return False
        cache[sha]=True
        return True
    result=select(json.loads(a.full.read_text())['artifacts'],json.loads(a.samples.read_text())['artifacts'],compatible)
    with open(os.environ['GITHUB_OUTPUT'],'a') as stream:
        for key,value in result.items():stream.write(key+'='+value+'\n')
    print(json.dumps(result))
    if result['ready']=='false':print('Current parser profiles are not available yet; keep the published snapshot until the new sample/rebuild is ready.')


if __name__=='__main__':main()
