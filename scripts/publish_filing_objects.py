"""Optional immutable R2 archive publication using designated Actions credentials.

No bucket creation, permission changes, deletions or credential discovery. Public
read/CORS configuration must already permit the test page to read binary assets.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import urllib.request
from urllib.parse import urlparse

REQUIRED = ('R2_ENDPOINT_URL','R2_ACCESS_KEY_ID','R2_SECRET_ACCESS_KEY','R2_BUCKET','R2_PUBLIC_BASE_URL')
ORIGIN = 'https://sroyaltyy.github.io'


def configuration(environ):
    present = [key for key in REQUIRED if environ.get(key)]
    missing = [key for key in REQUIRED if not environ.get(key)]
    if missing:
        return {'configured':False,'missing':missing,'present':present}
    for key in ('R2_ENDPOINT_URL','R2_PUBLIC_BASE_URL'):
        url = urlparse(environ[key])
        if url.scheme != 'https' or not url.netloc or url.username or url.password or url.query or url.fragment:
            raise ValueError(key+' must be a credential-free HTTPS URL')
    return {'configured':True,'missing':[],'present':present}


def object_manifest(directory):
    from src.deep_profile_store import read_index
    root,index,fingerprint = read_index(directory)
    entries = {'index.json':fingerprint}
    for entry in index['profiles'].values():
        path = (root/entry['path']).resolve()
        if not path.is_relative_to(root) or path.is_symlink():
            raise ValueError('unsafe object path')
        if hashlib.sha256(path.read_bytes()).hexdigest() != entry['sha256']:
            raise ValueError('archive object checksum mismatch')
        entries[entry['path']] = entry['sha256']
    return root,entries,'company-filings/profiles/'+fingerprint


def verify_public(url, expected_size):
    req = urllib.request.Request(url,method='HEAD',headers={'Origin':ORIGIN})
    with urllib.request.urlopen(req,timeout=60) as response:
        if response.headers.get('Access-Control-Allow-Origin') not in ('*',ORIGIN):
            raise ValueError('R2 public archive needs CORS GET/HEAD access for the test-page origin')
        if int(response.headers.get('Content-Length','-1')) != expected_size:
            raise ValueError('public archive object size mismatch')
        if response.headers.get('Content-Encoding','identity') != 'identity':
            raise ValueError('public archive must serve .gz as binary, without Content-Encoding')


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument('--directory',type=Path)
    p.add_argument('--preflight',action='store_true')
    a = p.parse_args(argv)
    state = configuration(os.environ)
    print(json.dumps(state),flush=True)  # names only; never values
    if a.preflight:
        return 0
    if not state['configured']:
        raise SystemExit('Object storage is not connected; compact keyword publication remains available.')
    if not a.directory:
        p.error('--directory is required')
    import boto3
    from botocore.exceptions import ClientError
    client = boto3.client('s3',endpoint_url=os.environ['R2_ENDPOINT_URL'],
        aws_access_key_id=os.environ['R2_ACCESS_KEY_ID'],aws_secret_access_key=os.environ['R2_SECRET_ACCESS_KEY'],region_name='auto')
    root,entries,prefix = object_manifest(a.directory)
    bucket = os.environ['R2_BUCKET']
    for name,sha in entries.items():
        path,key = root/name,prefix+'/'+name
        try:
            prior = client.head_object(Bucket=bucket,Key=key)
        except ClientError as exc:
            if exc.response['Error']['Code'] not in ('404','NoSuchKey','NotFound'):
                raise
            prior = None
        if prior:
            if prior.get('Metadata',{}).get('sha256') != sha or prior['ContentLength'] != path.stat().st_size:
                raise ValueError('immutable object collision: '+name)
        else:
            client.upload_file(str(path),bucket,key,ExtraArgs={'ContentType':'application/gzip' if name.endswith('.gz') else 'application/json',
                'CacheControl':'public,max-age=31536000,immutable','Metadata':{'sha256':sha}})
        print('Verified archive object '+name,flush=True)
    base = os.environ['R2_PUBLIC_BASE_URL'].rstrip('/')+'/'+prefix
    for name in ('index.json',next(n for n in entries if n.endswith('.gz'))):
        verify_public(base+'/'+name,(root/name).stat().st_size)
    with open(os.environ['GITHUB_ENV'],'a') as stream:
        stream.write('PROFILE_PUBLIC_BASE_URL='+base+'\n')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
