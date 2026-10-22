"""Publish a keyword-only projection without rerunning cached article scores.

The scoring snapshot, articles, decisions, source facts and evidence stay bound
to their original revision. Only derived retrieval identities and the concrete
connection admission policy are regenerated, using the shared Python compiler.
"""
import argparse
import copy
import gzip
import hashlib
import json
import os
from pathlib import Path
import shutil
import tempfile
from types import SimpleNamespace

from scripts.publish_scanner_snapshot import (ARTIFACTS, INPUT_PATHS, artifact_names,
    git, git_input, validate, validate_budget)
from src.stock_keywords import KeywordIndex, SCANNER_POLICY, annotate_scanner_rows, scanner_visible

ROOT = Path(__file__).resolve().parents[1]
UNCHANGED = ('articles.json', 'results.jsonl', 'ticker-index.json')


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def source_hashes():
    paths = [ROOT/'src'/name for name in ('stock_keywords.py', 'entity_resolution.py',
        'linguistics.py', 'semantic_lookup.py', 'specific_filing_records.py')]
    paths += [ROOT/'scripts/project_scanner_keywords.py',
        ROOT/'dashboard/scanner-backtest/index.html']
    paths += sorted((ROOT/'src/data').rglob('*'))
    return {p.relative_to(ROOT).as_posix(): digest(p.read_bytes()) for p in paths
            if p.is_file() and '__pycache__' not in p.parts}


def project(source, destination, revision):
    meta = json.loads((source/'meta.json').read_text())
    original_revision = meta['snapshot']['source_revision']
    validate(source, original_revision)
    if meta['snapshot'].get('keyword_comparison_mode') != 'browser_preview':
        raise ValueError('cached keyword comparisons require a full rescore')
    meta.pop('asset_base_url', None)
    destination.mkdir(parents=True, exist_ok=False)
    for name in UNCHANGED:
        shutil.copyfile(source/name, destination/name)
    shutil.copytree(source/'filing-profiles', destination/'filing-profiles')
    summaries = json.loads((source/'profiles.json').read_text())
    profiles = {}
    total = visible = 0
    for ticker, summary in sorted(summaries.items()):
        path = source/summary['profile_path']
        raw = path.read_bytes()
        if digest(raw) != summary['profile_sha256']:
            raise ValueError('scoring profile checksum mismatch: '+ticker)
        payload = json.loads(gzip.decompress(raw))
        before = copy.deepcopy(payload)
        annotate_scanner_rows(payload['keywords'], payload['keyword_evidence'])
        profiles[ticker] = SimpleNamespace(name=payload['name'], facts=[],
            keyword_records=payload['keywords'], keyword_evidence=payload['keyword_evidence'],
            keyword_coverage=payload['keyword_coverage'])
        def original_fields(p):
            p = copy.deepcopy(p)
            for row in p['keywords']:
                row.pop('scanner', None)
                row.pop('identity', None)
            return p
        if original_fields(payload) != original_fields(before):
            raise ValueError('projection changed a source record: '+ticker)
        target=destination/summary['profile_path']
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(gzip.compress(json.dumps(payload, ensure_ascii=False, sort_keys=True).encode(),mtime=0))
        total += len(payload['keywords'])
        visible += sum(scanner_visible(row) for row in payload['keywords'])
    # Reuse the exact compiler and identity policy used by the normal exporter.
    index = KeywordIndex(profiles)
    for ticker, profile in profiles.items():
        target=destination/summaries[ticker]['profile_path']
        payload=json.loads(gzip.decompress(target.read_bytes()))
        payload['keywords']=profile.keyword_records
        raw = gzip.compress(json.dumps(payload, ensure_ascii=False, sort_keys=True).encode(), mtime=0)
        target.write_bytes(raw)
        summaries[ticker]['profile_sha256'] = digest(raw)
    payload = index.browser_payload()
    phrases = payload.pop('phrases')
    buckets = {}
    for entry in phrases:
        bucket = digest(entry['phrase'][0].encode())[:2]
        buckets.setdefault(bucket, []).append(entry)
    (destination/'keyword-index').mkdir()
    payload.update(bucket_method='token_sha256_2', buckets={})
    for bucket, entries in sorted(buckets.items()):
        raw = gzip.compress(json.dumps(entries, ensure_ascii=False, separators=(',', ':')).encode(), mtime=0)
        path = 'keyword-index/'+bucket+'.json.gz'
        (destination/path).write_bytes(raw)
        payload['buckets'][bucket] = {'path':path, 'sha256':digest(raw)}
    raw = gzip.compress(json.dumps(payload, ensure_ascii=False, separators=(',', ':')).encode(), mtime=0)
    (destination/'keyword-index.json.gz').write_bytes(raw)
    meta['keyword_index'].update(sha256=digest(raw), phrases=len(phrases))
    projection = {'source_revision':revision, 'policy':SCANNER_POLICY,
        'based_on_snapshot':meta['snapshot']['snapshot_id'], 'source_hashes':source_hashes(),
        'cached_scores_unchanged':True, 'profiles':len(profiles),
        'covered_tickers':len(meta['snapshot']['filing_coverage']['covered_tickers']),
        'retained_records':total, 'visible_current_connections':visible,
        'unchanged_sha256':{name:digest((source/name).read_bytes()) for name in UNCHANGED}}
    projection['projection_id'] = digest(json.dumps(projection, sort_keys=True).encode())
    meta['keyword_projection'] = projection
    (destination/'profiles.json').write_text(json.dumps(summaries, ensure_ascii=False))
    (destination/'meta.json').write_text(json.dumps(meta, indent=2)+'\n')
    # The cached engine still identifies its original source; the UI separately
    # identifies this projection and never advertises the old scores as rerun.
    html = (ROOT/'dashboard/scanner-backtest/index.html').read_text()
    needle = '  renderSnapshot(m.snapshot);'
    if html.count(needle) != 1:
        raise ValueError('unrecognized scanner template')
    html = html.replace(needle, needle+'''
  if (m.keyword_projection) {
    const p=m.keyword_projection;
    document.getElementById("snapshot").append(` · KEYWORD VIEW ${p.policy} @ ${p.source_revision.slice(0,12)} · cached engine scores retain their original revision`);
  }
''')
    html = html.replace('encodeURIComponent(staticMeta.snapshot.snapshot_id)',
        'encodeURIComponent(staticMeta.keyword_projection?.projection_id || staticMeta.snapshot.snapshot_id)')
    html = html.replace('const version = (staticMeta.snapshot || {}).snapshot_id',
        'const version = staticMeta.keyword_projection?.projection_id || (staticMeta.snapshot || {}).snapshot_id')
    (destination/'index.html').write_text(html)
    validate_projection(destination, revision)
    return projection


def validate_projection(directory, revision):
    meta = json.loads((directory/'meta.json').read_text())
    p = meta['keyword_projection']
    expected_id = p['projection_id']
    if digest(json.dumps({k:v for k,v in p.items() if k!='projection_id'}, sort_keys=True).encode()) != expected_id:
        raise ValueError('keyword projection integrity mismatch')
    if p['source_revision'] != revision or p['source_hashes'] != source_hashes():
        raise ValueError('keyword projection source mismatch')
    if p['based_on_snapshot'] != meta['snapshot']['snapshot_id'] or not p['cached_scores_unchanged']:
        raise ValueError('keyword projection lost scoring provenance')
    for name, expected in p['unchanged_sha256'].items():
        if digest((directory/name).read_bytes()) != expected:
            raise ValueError('cached scoring artifact changed: '+name)
    validate(directory, meta['snapshot']['source_revision'])
    return meta


def publish(directory, revision):
    """Fast-forward only derived assets; check both code and input snapshot."""
    meta = validate_projection(directory, revision)
    guard_paths = (*INPUT_PATHS, 'scripts/project_scanner_keywords.py',
        '.github/workflows/scanner-keyword-publish.yml',
        *(f'dashboard/scanner-backtest/{name}' for name in ARTIFACTS))
    for _ in range(3):
        git('fetch', '--depth=1', 'origin', 'main')
        parent = git('rev-parse', 'FETCH_HEAD')
        changed = git('diff', '--name-only', revision, parent, '--', *guard_paths)
        if changed:
            raise ValueError('projection inputs changed during publication: '+changed)
        with tempfile.TemporaryDirectory(prefix='keyword-publication-') as temp:
            env = {**os.environ, 'GIT_INDEX_FILE':str(Path(temp)/'index'),
                'GIT_AUTHOR_NAME':'Scanner keyword publisher', 'GIT_AUTHOR_EMAIL':'bot@users.noreply.github.com',
                'GIT_COMMITTER_NAME':'Scanner keyword publisher', 'GIT_COMMITTER_EMAIL':'bot@users.noreply.github.com'}
            git('read-tree', parent, env=env)
            updates=[]
            for name in artifact_names(directory):
                blob=git('hash-object','-w',str((directory/name).resolve()))
                updates.append('100644 '+blob+'\tdashboard/scanner-backtest/'+name+'\n')
            git_input(['update-index','--index-info'], ''.join(updates), env=env)
            tree=git('write-tree',env=env)
            commit=git_input(['commit-tree',tree,'-p',parent],
                'Publish concrete keyword view '+meta['keyword_projection']['projection_id'][:12]+'\n', env=env)
            import subprocess
            result=subprocess.run(['git','push','origin',commit+':refs/heads/main'],capture_output=True,text=True)
            if result.returncode==0:return commit
            if not any(term in result.stderr for term in ('fetch first','non-fast-forward','failed to update ref')):
                raise RuntimeError('keyword snapshot push failed: '+result.stderr)
    raise RuntimeError('main kept moving; keyword view not published')


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-dir',type=Path,required=True)
    parser.add_argument('--out-dir',type=Path,required=True)
    parser.add_argument('--source-sha',required=True)
    parser.add_argument('--publish',action='store_true')
    args=parser.parse_args()
    receipt=project(args.source_dir,args.out_dir,args.source_sha)
    print(json.dumps(receipt),flush=True)
    if args.publish:print('Published immutable keyword assets '+publish(args.out_dir,args.source_sha),flush=True)


if __name__=='__main__':main()
