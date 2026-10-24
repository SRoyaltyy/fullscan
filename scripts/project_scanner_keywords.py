"""Publish a keyword-only projection without rerunning cached article scores.

The scoring snapshot, articles, decisions, source facts and evidence stay bound
to their original revision. The concrete-connection admission policy is applied
by the shared Python compiler; verified unchanged retrieval identities are reused.
"""
import argparse
import ast
import gzip
import hashlib
import json
import os
from pathlib import Path
import shutil
import tempfile

from scripts.publish_scanner_snapshot import (ARTIFACTS, INPUT_PATHS, artifact_names,
    git, git_input, validate, validate_budget)
from src.stock_keywords import SCANNER_POLICY, SCHEMA, annotate_scanner_rows, scanner_visible
from src.entity_resolution import POLICY, VERSION as ALIGNMENT_VERSION

ROOT = Path(__file__).resolve().parents[1]
UNCHANGED = ('articles.json', 'results.jsonl', 'ticker-index.json')


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def source_hashes():
    paths = [ROOT/'src'/name for name in ('stock_keywords.py', 'entity_resolution.py',
        'linguistics.py', 'semantic_lookup.py', 'specific_filing_records.py', 'filing_relationships.py',
        'relationship_frames.py', 'term_inventory.py', 'stock_evidence_graph.py', 'stock_document_context.py')]
    paths += [ROOT/'scripts/project_scanner_keywords.py',
        ROOT/'dashboard/scanner-backtest/index.html']
    paths += sorted((ROOT/'src/data').rglob('*'))
    return {p.relative_to(ROOT).as_posix(): digest(p.read_bytes()) for p in paths
            if p.is_file() and '__pycache__' not in p.parts}


def ensure_identity_compatibility(meta):
    """Reuse only identities produced by unchanged code and pinned meanings."""
    snapshot=meta['snapshot']
    for name in ('src/entity_resolution.py','src/linguistics.py','src/semantic_lookup.py',
                 'src/data/english_words.txt','src/data/lemma_lookup.json.gz'):
        if snapshot.get('source_hashes',{}).get(name) != digest((ROOT/name).read_bytes()):
            raise ValueError('cached identity method changed; full rebuild required: '+name)
    revision=snapshot['source_revision']
    def original(path):
        try:return git('show',revision+':'+path)
        except Exception:
            git('fetch','--filter=blob:none','--depth=1','origin',revision)
            return git('show',revision+':'+path)
    def normalization_definitions(source):
        return [ast.dump(node,include_attributes=False) for node in ast.parse(source).body
            if isinstance(node,(ast.FunctionDef,ast.AsyncFunctionDef)) and node.name=='normalized_words'
            or isinstance(node,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='TOKEN' for t in node.targets)]
    for path in ('src/filing_relationships.py', 'src/relationship_frames.py', 'src/term_inventory.py', 'src/stock_evidence_graph.py', 'src/stock_document_context.py'):
        try:
            old_source=original(path)
        except Exception as exc:
            raise ValueError('cached extraction provenance missing; full rebuild required: '+path) from exc
        if old_source != (ROOT/path).read_text():
            raise ValueError('cached extraction method changed; full rebuild required: '+path)
    if normalization_definitions(original('src/stock_keywords.py')) != normalization_definitions((ROOT/'src/stock_keywords.py').read_text()):
        raise ValueError('cached keyword normalization changed; full rebuild required')
    old=json.loads(original('src/data/senses/manifest.json'))
    current=json.loads((ROOT/'src/data/senses/manifest.json').read_text())
    if {e['file']:e['sha256'] for e in old['files']} != {e['file']:e['sha256'] for e in current['files']}:
        raise ValueError('cached semantic meanings changed; full rebuild required')


def project(source, destination, revision):
    meta = json.loads((source/'meta.json').read_text())
    original_revision = meta['snapshot']['source_revision']
    print('Validating the complete cached scoring snapshot',flush=True)
    validate(source, original_revision)
    ensure_identity_compatibility(meta)
    print('Verified unchanged identity and normalization methods',flush=True)
    if meta['snapshot'].get('keyword_comparison_mode') != 'browser_preview':
        raise ValueError('cached keyword comparisons require a full rescore')
    meta.pop('asset_base_url', None)
    destination.mkdir(parents=True, exist_ok=False)
    for name in UNCHANGED:
        shutil.copyfile(source/name, destination/name)
    shutil.copytree(source/'filing-profiles', destination/'filing-profiles')
    summaries = json.loads((source/'profiles.json').read_text())
    policies = {}
    total = visible = 0
    for ticker, summary in sorted(summaries.items()):
        path = source/summary['profile_path']
        raw = path.read_bytes()
        if digest(raw) != summary['profile_sha256']:
            raise ValueError('scoring profile checksum mismatch: '+ticker)
        payload = json.loads(gzip.decompress(raw))
        original_records=digest(json.dumps([{k:v for k,v in r.items() if k!='scanner'} for r in payload['keywords']],sort_keys=True).encode())
        annotate_scanner_rows(payload['keywords'], payload['keyword_evidence'])
        if original_records != digest(json.dumps([{k:v for k,v in r.items() if k!='scanner'} for r in payload['keywords']],sort_keys=True).encode()):
            raise ValueError('projection changed a source record: '+ticker)
        policies[ticker]={row['id']:row['scanner'] for row in payload['keywords']}
        target=destination/summary['profile_path']
        target.parent.mkdir(parents=True, exist_ok=True)
        raw=gzip.compress(json.dumps(payload, ensure_ascii=False, sort_keys=True).encode(),compresslevel=6,mtime=0)
        target.write_bytes(raw)
        summaries[ticker]['profile_sha256']=digest(raw)
        total += len(payload['keywords'])
        visible += sum(scanner_visible(row) for row in payload['keywords'])
        if len(policies)%250==0:print('Filtered',len(policies),'of',len(summaries),'stock assets',flush=True)
    # The identity compiler is unchanged (guarded above). Stream its verified
    # index one bucket at a time, applying the shared policy by exact record ID.
    # No cross-company alias, normalized phrase or source identity is guessed.
    original_index=json.loads(gzip.decompress((source/'keyword-index.json.gz').read_bytes()))
    if (original_index.get('schema')!=SCHEMA or original_index.get('identity_policy')!=POLICY
            or original_index.get('alignment_version')!=ALIGNMENT_VERSION
            or original_index.get('bucket_method')!='token_sha256_2'):
        raise ValueError('unsupported cached identity index; full rebuild required')
    (destination/'keyword-index').mkdir()
    payload={**original_index,'scanner_policy':SCANNER_POLICY,'buckets':{}}
    phrase_count=0
    for position,(bucket, definition) in enumerate(sorted(original_index['buckets'].items()),1):
        raw=(source/definition['path']).read_bytes()
        if digest(raw)!=definition['sha256']:raise ValueError('cached keyword bucket checksum mismatch')
        entries=[]
        for entry in json.loads(gzip.decompress(raw)):
            row=entry['row']
            if row['relationship']!='issuer':
                policy=policies.get(row['stock'],{}).get(row['id'])
                if policy is None:raise ValueError('cached keyword is absent from its stock profile')
                if not policy['eligible']:continue
                row['scanner']=policy
            entries.append(entry)
        if not entries:continue
        phrase_count+=len(entries)
        raw = gzip.compress(json.dumps(entries, ensure_ascii=False, separators=(',', ':')).encode(), compresslevel=6,mtime=0)
        path = 'keyword-index/'+bucket+'.json.gz'
        (destination/path).write_bytes(raw)
        payload['buckets'][bucket] = {'path':path, 'sha256':digest(raw)}
        if position%32==0:print('Filtered',position,'of',len(original_index['buckets']),'keyword buckets',flush=True)
    raw = gzip.compress(json.dumps(payload, ensure_ascii=False, separators=(',', ':')).encode(), mtime=0)
    (destination/'keyword-index.json.gz').write_bytes(raw)
    meta['keyword_index'].update(sha256=digest(raw), phrases=phrase_count)
    projection = {'source_revision':revision, 'policy':SCANNER_POLICY,
        'based_on_snapshot':meta['snapshot']['snapshot_id'], 'source_hashes':source_hashes(),
        'cached_scores_unchanged':True, 'profiles':len(summaries),
        'identity_revision':original_revision, 'identities_recompiled':False,
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
    for _ in range(6):
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
            # The parent is already on origin. Sparse partial clones need not
            # download unchanged parent blobs merely to retain their tree IDs.
            # Every replacement blob above was written locally and validated.
            tree=git('write-tree','--missing-ok',env=env)
            commit=git_input(['commit-tree',tree,'-p',parent],
                'Publish concrete keyword view '+meta['keyword_projection']['projection_id'][:12]+'\n', env=env)
            import subprocess
            result=subprocess.run(['git','push','origin',commit+':refs/heads/main'],capture_output=True,text=True)
            if result.returncode==0:return commit
            if not retryable_main_race(result.stderr):
                raise RuntimeError('keyword snapshot push failed: '+result.stderr)
    raise RuntimeError('main kept moving; keyword view not published')


def retryable_main_race(stderr):
    import re
    return any(term in stderr for term in ('fetch first', 'non-fast-forward', 'failed to update ref')) or bool(
        re.search(r"cannot lock ref 'refs/heads/main': is at [0-9a-f]{40} but expected [0-9a-f]{40}", stderr))


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
