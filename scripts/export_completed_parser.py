"""Publish saved relationship decisions and their full witness paragraphs.

This is a read-only projection of an existing archive: no SEC requests, spaCy,
inference, or replacement of the recorded extraction version.
"""
import argparse
from bisect import bisect_right
from collections import defaultdict
import gzip
import hashlib
import json
from pathlib import Path

COMPILER = '7953265b88372033f99c4f22f4d722f901fc8b7a0399a6547f7fecacbd377abc'
ARTIFACT = 12321883294
RUN = 39522456843


def encoded(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode()


def project(graph):
    """Keep every decision, every witness, and every overlapping full paragraph."""
    by_source = defaultdict(list)
    for p in graph['paragraphs']:
        by_source[p['source_id']].append(p)
    starts = {}
    for source, paragraphs in by_source.items():
        paragraphs.sort(key=lambda p:p['start'])
        starts[source] = [p['start'] for p in paragraphs]
    selected, source_ids, facts = {}, set(), []
    for original in graph['facts']:
        fact = dict(original)
        witnesses = []
        for ev in fact['evidence']:
            paragraphs = by_source[ev['source_id']]
            i = max(0, bisect_right(starts[ev['source_id']], ev['start'])-1)
            pieces, refs = [], []
            while i < len(paragraphs) and paragraphs[i]['start'] < ev['end']:
                p = paragraphs[i]
                if p['end'] > ev['start']:
                    if hashlib.sha256(p['text'].encode()).hexdigest() != p['sha256']:
                        raise ValueError('paragraph hash mismatch')
                    pieces.append(p['text'][max(ev['start'],p['start'])-p['start']:min(ev['end'],p['end'])-p['start']])
                    refs.append(p['id'])
                    selected[p['id']] = {k:p[k] for k in ('id','source_id','start','end','text','sha256','labels')}
                i += 1
            if not refs or ''.join(pieces) != ev['quote']:
                raise ValueError('decision witness does not match retained source paragraphs')
            source_ids.add(ev['source_id'])
            witnesses.append({**ev,'paragraph_ids':refs})
        if not witnesses:
            raise ValueError('decision has no witness')
        fact['evidence'] = witnesses
        facts.append(fact)
    sources = [s for s in graph['sources'] if s['id'] in source_ids]
    if {s['id'] for s in sources} != source_ids:
        raise ValueError('decision source metadata missing')
    return {'schema':'completed-parser-review-1','ticker':graph['ticker'],
        'tickers':graph['tickers'],'name':graph['name'],'as_of':graph['as_of'],
        'extraction_schema':graph['schema'],'compiler_sha256':graph['compiler_sha256'],
        'parser':graph['parser'],'coverage':graph['coverage'],
        'review_scope':'all recorded relationships and their complete witness paragraphs',
        'term_inventory_available':'term_inventory' in graph,
        'facts':facts,'paragraphs':list(selected.values()),'sources':sources}


def export(root, out, *, expected_compiler=COMPILER, expected_companies=2026,
           expected_tickers=2032, expected_facts=754258, viewer_revision='', read_bytes=None):
    root, out = Path(root).resolve(), Path(out)
    raw_index = read_bytes('index.json') if read_bytes else (root/'index.json').read_bytes()
    index = json.loads(raw_index)
    if not index.get('complete') or index['compiler_sha256'] != expected_compiler:
        raise ValueError('not the pinned completed extraction')
    if len(index['profiles']) != expected_tickers or index['totals']['companies'] != expected_companies or index['totals']['facts'] != expected_facts:
        raise ValueError('completed archive coverage mismatch')
    (out/'profiles').mkdir(parents=True,exist_ok=True)
    profiles, processed, facts, paragraph_count, total_bytes = {}, {}, 0, 0, 0
    for ticker, entry in sorted(index['profiles'].items()):
        cik = str(entry['cik'])
        if not cik.isdigit() or len(cik) != 10:
            raise ValueError('invalid issuer identifier')
        if cik not in processed:
            path = (root/entry['path']).resolve()
            if not path.is_relative_to(root) or path.is_symlink():
                raise ValueError('unsafe archived profile path')
            raw = read_bytes(entry['path']) if read_bytes else path.read_bytes()
            if hashlib.sha256(raw).hexdigest() != entry['sha256']:
                raise ValueError('archived profile checksum mismatch')
            graph = json.loads(gzip.decompress(raw))
            if graph['compiler_sha256'] != expected_compiler or graph['as_of'] != index['as_of'] or len(graph['facts']) != entry['facts']:
                raise ValueError('mixed extraction output')
            payload = project(graph)
            packed = gzip.compress(encoded(payload),compresslevel=6,mtime=0)
            if len(packed) >= 99_000_000:
                raise ValueError('issuer review exceeds publication file budget')
            filename = 'profiles/'+cik+'.json.gz'
            (out/filename).write_bytes(packed)
            processed[cik] = {'path':filename,'sha256':hashlib.sha256(packed).hexdigest(),
                'name':graph['name'],'facts':len(graph['facts']),'cik':cik}
            facts += len(graph['facts'])
            paragraph_count += len(payload['paragraphs'])
            total_bytes += len(packed)
            print(json.dumps({'issuers_exported':len(processed),'of':expected_companies,'review_bytes':total_bytes}),flush=True)
            del payload, graph, raw, packed
        profiles[ticker] = processed[cik]
    if len(processed) != expected_companies or facts != expected_facts:
        raise ValueError('publication omitted completed issuers or relationships')
    manifest = {'schema':'completed-parser-review-1','complete':True,
        'artifact_id':ARTIFACT,'source_run_id':RUN,'compiler_sha256':expected_compiler,
        'extraction_schema':index['schema'],'as_of':index['as_of'],
        'source_index_sha256':hashlib.sha256(raw_index).hexdigest(),
        'viewer_revision':viewer_revision,'profiles':profiles,'archive_totals':index['totals'],
        'published':{'issuers':len(processed),'tickers':len(profiles),'relationships':facts,
            'decision_paragraphs':paragraph_count,'profile_bytes':total_bytes},
        'limitations':['Saved extraction, not a rerun with the latest parser.',
            'Every recorded relationship and its full witness paragraphs are retained; this view does not copy unrelated or machine-data paragraphs.',
            'The newer exhaustive noun/entity occurrence inventory is not available in this extraction.',
            'Relationship labels are filing-parser claims requiring review; missing relationships remain unknown.']}
    data = encoded(manifest)
    if total_bytes+len(data) >= 1_800_000_000:
        raise ValueError('complete review exceeds publication budget; no records were truncated')
    (out/'manifest.json').write_bytes(data)
    return manifest


def export_saved_zip(out, *, repository='SRoyaltyy/fullscan'):
    """Read the pinned ZIP directly, repairing missing members from saved shards.

    The source archive is never re-extracted with NLP. A directory preflight
    runs before a large transfer; each recovered member must match the combined
    index's original hash in export(). No source failure permits dropping rows.
    """
    import contextlib
    import os
    import sys
    import tempfile
    import time
    import zipfile
    sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
    from scripts.materialize_filing_texts import RangeReader, http_fetcher, signed_artifact_url
    from scripts.resilient_filing_texts import download_zip
    shard_ids=[12294494078,12296274014,12317308763,12295757333,12293787917,
        12303678823,12306729150,12306801305,12310477973,12318594086,12321334237,12319619063]
    with contextlib.ExitStack() as stack:
        def remote(ident):
            url=signed_artifact_url(repository,ident)
            def fetch(start,end):
                nonlocal url
                for attempt in range(3):
                    try:return http_fetcher(url)(start,end)
                    except RuntimeError:
                        if attempt==2:raise
                        time.sleep(2**attempt)
                        url=signed_artifact_url(repository,ident)
            reader=stack.enter_context(RangeReader(fetch))
            return stack.enter_context(zipfile.ZipFile(reader))
        preflight=remote(ARTIFACT)
        index=json.loads(preflight.read('index.json'))
        if not index.get('complete') or index.get('compiler_sha256')!=COMPILER or len(index['profiles'])!=2032 or index['totals']['facts']!=754258:
            raise ValueError('ZIP directory is not the pinned completed extraction')
        expected={entry['path'] for entry in index['profiles'].values()}
        missing=expected-set(preflight.namelist())
        print(json.dumps({'archive_profiles':len(expected),'missing_zip_members':len(missing)}),flush=True)
        repairs={}
        if missing:
            for ident in shard_ids:
                archive=remote(ident)
                found=missing&set(archive.namelist())
                for name in found:repairs[name]=archive
                missing-=found
                if not missing:break
            if missing:raise ValueError('completed source shards cannot supply '+str(len(missing))+' indexed profiles')
        with tempfile.TemporaryDirectory(prefix='completed-parser-zip-',dir=Path(out).parent) as temporary:
            path=download_zip(lambda:signed_artifact_url(repository,ARTIFACT),Path(temporary)/'profiles.zip',
                expected_sha256='aae0057b4c74187702daf97e541c9fbdc450196d9906b10a7951d250dfc56593')
            with zipfile.ZipFile(path) as archive:
                def read(name):
                    if name in repairs:return repairs[name].read(name)
                    return archive.read(name)
                return export('.',out,read_bytes=read,viewer_revision=os.environ.get('GITHUB_SHA',''))


def publish_assets(directory):
    """Keep large review files in a data branch; Pages serves a small manifest.

    A private temporary Git index leaves the checked-out site's files untouched.
    Normal fast-forward pushes preserve all earlier data revisions.
    """
    import os
    import subprocess
    import tempfile
    directory=Path(directory).resolve()
    branch='parser-completed-data'
    def git(*args, env=None, data=None):
        return subprocess.check_output(['git',*args],input=data,text=True,env=env).strip()
    prior=git('ls-remote','--heads','origin','refs/heads/'+branch)
    parent=None
    if prior:
        git('fetch','--depth=1','origin',branch)
        parent=git('rev-parse','FETCH_HEAD')
    paths=sorted((directory/'profiles').glob('*.json.gz'))
    if not paths or any(p.is_symlink() or not p.stem.removesuffix('.json').isdigit() for p in paths):
        raise ValueError('unsafe or empty saved review assets')
    with tempfile.TemporaryDirectory(prefix='completed-parser-index-') as temporary:
        env={**os.environ,'GIT_INDEX_FILE':str(Path(temporary)/'index'),
            'GIT_AUTHOR_NAME':'Completed parser page publisher','GIT_AUTHOR_EMAIL':'bot@users.noreply.github.com',
            'GIT_COMMITTER_NAME':'Completed parser page publisher','GIT_COMMITTER_EMAIL':'bot@users.noreply.github.com'}
        git('read-tree','--empty',env=env)
        hashes=git('hash-object','-w','--stdin-paths',data=''.join(str(p)+'\n' for p in paths)).splitlines()
        if len(hashes)!=len(paths):raise ValueError('asset hashing was incomplete')
        git('update-index','--index-info',env=env,data=''.join('100644 '+sha+'\tprofiles/'+p.name+'\n' for p,sha in zip(paths,hashes)))
        tree=git('write-tree',env=env)
        args=['commit-tree',tree]
        if parent:args.extend(['-p',parent])
        commit=git(*args,'-m','Publish completed parser review assets without rerunning extraction',env=env)
        git('push','origin',commit+':refs/heads/'+branch)
    manifest=json.loads((directory/'manifest.json').read_text())
    manifest['asset_base_url']='https://raw.githubusercontent.com/SRoyaltyy/fullscan/'+commit+'/'
    (directory/'manifest.json').write_bytes(encoded(manifest))
    return commit


if __name__ == '__main__':
    import os
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--profiles',type=Path)
    p.add_argument('--saved-zip',action='store_true')
    p.add_argument('--out',type=Path,required=True)
    p.add_argument('--publish-assets',action='store_true')
    a=p.parse_args()
    if a.publish_assets:
        print('Retained completed review assets at '+publish_assets(a.out))
    elif a.saved_zip:
        result=export_saved_zip(a.out)
        print(json.dumps(result['published']))
    else:
        if not a.profiles:p.error('--profiles or --saved-zip is required')
        result=export(a.profiles,a.out,viewer_revision=os.environ.get('GITHUB_SHA',''))
        print(json.dumps(result['published']))
