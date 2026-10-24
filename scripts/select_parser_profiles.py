"""Select artifacts compatible with the current parser before downloading them."""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import subprocess

INPUTS=('src/filing_relationships.py','src/filing_evidence.py','src/term_inventory.py',
        'src/specific_filing_records.py','src/relationship_frames.py','src/stock_evidence_graph.py',
        'src/stock_document_context.py',
        'scripts/build_filing_profiles.py','requirements-filing-parser.txt')
ROOT=Path(__file__).resolve().parents[1]


def select(full,samples,compatible,*,completed=None,manifest_url='',allow_sample=False):
    for pool,scope in ((full,'archive_set'),):
        for artifact in sorted(pool,key=lambda a:a['created_at'],reverse=True):
            run=artifact.get('workflow_run',{})
            if artifact.get('expired') or run.get('head_branch')!='main' or not run.get('head_sha'):continue
            if compatible(run['head_sha']):
                return {'ready':'true','run':str(run['id']),'artifact':artifact['name'],'scope':scope,
                        'profile_assets':'keywords','input_kind':'grammar_archive','manifest_url':''}
    if completed:
        from src.completed_profile_store import validate_manifest
        try:validate_manifest(completed)
        except ValueError:pass
        else:
            return {'ready':'true','run':str(completed['source_run_id']),'artifact':'',
                'scope':'archive_set','profile_assets':'keywords','input_kind':'completed_review',
                'manifest_url':manifest_url,'profile_base_url':completed['asset_base_url']}
    # A one-issuer development sample must be requested explicitly. It can
    # never silently replace the market archive on the public testing page.
    if allow_sample:
        for artifact in sorted(samples,key=lambda a:a['created_at'],reverse=True):
            run=artifact.get('workflow_run',{})
            if not artifact.get('expired') and run.get('head_branch')=='main' and run.get('head_sha') and compatible(run['head_sha']):
                return {'ready':'true','run':str(run['id']),'artifact':artifact['name'],
                    'scope':'development_sample','profile_assets':'full','input_kind':'grammar_archive','manifest_url':''}
    return {'ready':'false'}


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--full',type=Path,required=True)
    p.add_argument('--samples',type=Path,required=True)
    p.add_argument('--repository',required=True)
    p.add_argument('--completed',type=Path)
    p.add_argument('--manifest-url',default='')
    p.add_argument('--allow-development-sample',action='store_true')
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
    result=select(json.loads(a.full.read_text())['artifacts'],json.loads(a.samples.read_text())['artifacts'],compatible,
        completed=json.loads(a.completed.read_text()) if a.completed else None,
        manifest_url=a.manifest_url,allow_sample=a.allow_development_sample)
    with open(os.environ['GITHUB_OUTPUT'],'a') as stream:
        for key,value in result.items():stream.write(key+'='+value+'\n')
    print(json.dumps(result))
    if result['ready']=='false':print('Current parser profiles are not available yet; keep the published snapshot until the new sample/rebuild is ready.')


if __name__=='__main__':main()
