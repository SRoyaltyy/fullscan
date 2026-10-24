"""Land only a verified scanner snapshot on main; preserve concurrent writers.

Uses a temporary Git index and a fast-forward push. Refuses to publish if the
parser, intake, catalog, or page has changed since the scoring checkout.
No force push, secret extraction, trading writes, or deployment-gate override.
"""
import argparse
import base64
import json
import os
import subprocess
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor
import urllib.request
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

ARTIFACTS = ('articles.json', 'results.jsonl', 'profiles.json', 'ticker-index.json', 'meta.json')
INPUT_PATHS = ('src/correlation.py', 'src/evidence_matcher.py', 'src/stock_evidence.py',
    'src/stock_keywords.py','src/stock_keyword_store.py',
    'src/entity_resolution.py','src/term_inventory.py','src/completed_profile_store.py','scripts/annotate_article_entities.py',
    'src/filing_relationships.py', 'src/relationship_frames.py', 'src/scanner_validation.py',
    'src/stock_evidence_graph.py',
    'src/stock_document_context.py',
    'src/specific_filing_records.py', 'src/deep_profile_store.py', 'requirements-filing-parser.txt',
    'eval/scanner-failures', 'scripts/check_scanner_methodology.py',
    'scripts/build_filing_profiles.py', 'data/company_filings_full_release.json',
    'src/filing_evidence.py', 'src/filing_business.py', 'src/filing_parts.py',
    'src/names.py', 'src/aliases.py', 'src/linguistics.py', 'src/semantic_lookup.py',
    'src/synonyms.py', 'src/places.py', 'src/spelling.py', 'src/profile_cache.py',
    'src/link_correlation.py', 'src/scanner_backtest.py', 'src/data',
    'scripts/export_backtest_static.py',
    'scripts/materialize_filing_texts.py', 'scripts/bootstrap_semantics.py',
    'scripts/build_semantic_resources.py', '.github/workflows/scanner-backtest-publish.yml',
    'dashboard/scanner-backtest/index.html',
    'excel_bot/data/finviz_with_descriptions.csv', 'data/news_intake/2026-10-17/documents.json')


def artifact_names(directory):
    names = list(ARTIFACTS)
    if (directory/'keyword-index.json.gz').is_file():
        names.append('keyword-index.json.gz')
    for folder in ('scoring-profiles', 'filing-profiles','keyword-index'):
        for path in sorted((directory/folder).rglob('*')):
            if path.is_file():
                if path.is_symlink() or not path.resolve().is_relative_to(directory.resolve()):
                    raise ValueError('profile asset escapes snapshot directory')
                names.append(path.relative_to(directory).as_posix())
    return names


def validate_budget(directory, *, total_limit=1_800_000_000, file_limit=99_000_000):
    """Bound the immutable snapshot; stage_page separately enforces Pages space."""
    total = 0
    for name in artifact_names(directory):
        path = directory/name
        if path.is_symlink():
            raise ValueError('snapshot asset is a symlink')
        size = path.stat().st_size
        if size >= file_limit:
            raise ValueError('snapshot file exceeds safe publication budget: '+name)
        total += size
    if total >= total_limit:
        raise ValueError('snapshot exceeds immutable archive budget; retain previous page and use object storage')
    return total


def stage_page(source, destination, *, snapshot_commit, outside_bytes, site_limit=950_000_000):
    """Use immutable repository assets if the complete page exceeds Pages space."""
    import re,shutil
    if not re.fullmatch(r'[a-f0-9]{40}',snapshot_commit):
        raise ValueError('immutable snapshot commit is required')
    names = artifact_names(source)+['index.html']
    incoming = sum((source/name).stat().st_size for name in names)
    external = outside_bytes+incoming >= site_limit
    if external:
        names = list(ARTIFACTS)+['index.html']
    if outside_bytes+sum((source/name).stat().st_size for name in names) >= site_limit:
        raise ValueError('page bootstrap exceeds safe Pages budget')
    for name in names:
        target = destination/name
        target.parent.mkdir(parents=True,exist_ok=True)
        shutil.copyfile(source/name,target)
    meta_path = destination/'meta.json'
    meta = json.loads(meta_path.read_text())
    if external:
        meta['asset_base_url'] = 'https://raw.githubusercontent.com/SRoyaltyy/fullscan/'+snapshot_commit+'/dashboard/scanner-backtest/'
    meta_path.write_text(json.dumps(meta,indent=2)+'\n')
    return {'external_assets':external,'snapshot_commit':snapshot_commit}


def git(*args, env=None):
    return subprocess.check_output(['git', *args], text=True, env=env, timeout=90).strip()


def git_input(args, payload, *, env=None):
    return subprocess.check_output(['git',*args],input=payload,text=True,env=env,timeout=90).strip()


def github_api(method, endpoint, payload=None):
    token = os.environ.get('GH_TOKEN') or os.environ.get('GITHUB_TOKEN')
    if not token:
        raise RuntimeError('configured GitHub workflow token is required')
    request = urllib.request.Request('https://api.github.com/'+endpoint,
        data=json.dumps(payload).encode() if payload is not None else None,
        method=method, headers={'Authorization': 'Bearer '+token,
            'Accept': 'application/vnd.github+json', 'X-GitHub-Api-Version': '2022-11-28',
            'Content-Type': 'application/json', 'User-Agent': 'Fullscan-scanner-page'})
    with urllib.request.urlopen(request, timeout=90) as response:
        return json.load(response)


def publish_api(directory, source_sha, repository):
    validate(directory, source_sha)
    endpoint = 'repos/'+repository
    def upload(name):
        raw = (directory/name).read_bytes()
        print('Preparing '+name+': '+str(len(raw))+' bytes', flush=True)
        if len(raw) >= 100_000_000:
            raise ValueError('snapshot file exceeds GitHub file limit: '+name)
        blob = github_api('POST', endpoint+'/git/blobs',
                          {'content': base64.b64encode(raw).decode(), 'encoding': 'base64'})
        return {'path': 'dashboard/scanner-backtest/'+name,
                'mode': '100644', 'type': 'blob', 'sha': blob['sha']}
    with ThreadPoolExecutor(max_workers=8) as pool:
        blobs = list(pool.map(upload, artifact_names(directory)))
    for attempt in range(3):
        ref = github_api('GET', endpoint+'/git/ref/heads/main')
        upstream = ref['object']['sha']
        compare = github_api('GET', endpoint+'/compare/'+source_sha+'...'+upstream)
        files = compare.get('files') or []
        if len(files) >= 300:
            raise ValueError('change comparison is capped; rebuild from the current source')
        changed = [f['filename'] for f in files if any(
            f['filename'] == path or f['filename'].startswith(path+'/') for path in INPUT_PATHS)]
        if changed:
            raise ValueError('parser inputs changed during scoring: '+','.join(changed))
        commit = github_api('GET', endpoint+'/git/commits/'+upstream)
        tree = github_api('POST', endpoint+'/git/trees',
                          {'base_tree': commit['tree']['sha'], 'tree': blobs})
        new = github_api('POST', endpoint+'/git/commits',
                         {'message': 'Publish scanner snapshot for '+source_sha[:12],
                          'tree': tree['sha'], 'parents': [upstream]})
        try:
            github_api('PATCH', endpoint+'/git/refs/heads/main', {'sha': new['sha'], 'force': False})
            return new['sha']
        except urllib.error.HTTPError as exc:
            # Never retry an auth/protection failure. A changed ref alone is
            # a safe reason to retry the same five-file transaction.
            if exc.code not in (409, 422):
                raise
            latest = github_api('GET', endpoint+'/git/ref/heads/main')['object']['sha']
            if latest == upstream:
                raise
    raise RuntimeError('main kept moving; snapshot was not published')


def validate(directory, source_sha):
    validate_budget(directory)
    meta = json.loads((directory/'meta.json').read_text())
    snapshot = meta.get('snapshot') or {}
    if snapshot.get('source_revision') != source_sha or not snapshot.get('snapshot_id'):
        raise ValueError('snapshot does not identify the scoring commit')
    if snapshot.get('input_mode') != 'headline':
        raise ValueError('the public parser gate requires headline-only input')
    rows = [json.loads(line) for line in (directory/'results.jsonl').read_text().splitlines() if line.strip()]
    ids = [str(row['id']) for row in rows]
    articles = json.loads((directory/'articles.json').read_text())
    if not rows or len(ids) != len(set(ids)) or len(ids) != meta.get('articles'):
        raise ValueError('missing, duplicate, or inconsistent results')
    if len(articles) != len(ids) or {str(a['id']) for a in articles} != set(ids):
        raise ValueError('display articles do not match scored results')
    if any(row.get('snapshot_id') != snapshot['snapshot_id'] for row in rows):
        raise ValueError('mixed result snapshots')
    if snapshot.get('keyword_comparison_mode')=='browser_preview' and any(
            row.get('keyword_comparison_mode')!='browser_preview' or 'keyword_comparison' in row for row in rows):
        raise ValueError('on-demand browser preview was incorrectly recorded as an offline keyword comparison')
    profiles = json.loads((directory/'profiles.json').read_text())
    if not profiles or not all(p.get('used_for_scoring') for p in profiles.values()):
        raise ValueError('profiles are not the actual scoring board')
    coverage = snapshot.get('filing_coverage') or {}
    if coverage.get('scope') not in ('dated_identity_overlay', 'deep_filing_relationships','archived_relationships_with_current_disclosures') or not coverage.get('complete') or coverage.get('errors'):
        raise ValueError('dated filing identity source was not verified')
    keyword_coverage = {}
    for ticker, profile in profiles.items():
        if profile.get('profile_path'):
            path = (directory/profile['profile_path']).resolve()
            if not path.is_relative_to(directory.resolve()):
                raise ValueError('unsafe scoring profile path')
            import hashlib
            if hashlib.sha256(path.read_bytes()).hexdigest() != profile['profile_sha256']:
                raise ValueError('scoring profile checksum mismatch')
            if snapshot.get('profile_publication',{}).get('mode') == 'keywords':
                import gzip
                payload = json.loads(gzip.decompress(path.read_bytes()))
                records, evidence = payload.get('keywords',[]),payload.get('keyword_evidence',{})
                if any(ref not in evidence for row in records for ref in row['evidence_ids']):
                    raise ValueError('keyword record lost its supporting evidence')
                detail = payload.get('keyword_coverage') or {}
                if detail:
                    if detail['retained_relationships'] != sum(not ref.startswith(('paragraph:','specific:')) for ref in evidence):
                        raise ValueError('keyword evidence retention count mismatch')
                    if detail.get('specific_disclosure_records',0) != sum(ref.startswith('specific:') for ref in evidence):
                        raise ValueError('specific disclosure evidence retention count mismatch')
                    keyword_coverage[ticker] = detail
    keyword_index = meta.get('keyword_index')
    if keyword_index:
        import hashlib
        if keyword_index['path'] != 'keyword-index.json.gz' or hashlib.sha256((directory/keyword_index['path']).read_bytes()).hexdigest() != keyword_index['sha256']:
            raise ValueError('keyword index checksum mismatch')
        import gzip
        manifest=json.loads(gzip.decompress((directory/keyword_index['path']).read_bytes()))
        import re
        if manifest.get('bucket_method') not in (None,'token_sha256_2'):
            raise ValueError('unsupported keyword bucket method')
        bucket_pattern=r'[a-f0-9]{2}' if manifest.get('bucket_method')=='token_sha256_2' else r'[a-f0-9]'
        for bucket,entry in manifest.get('buckets',{}).items():
            if entry['path'] != 'keyword-index/'+bucket+'.json.gz' or not re.fullmatch(bucket_pattern,bucket):
                raise ValueError('unsafe keyword index bucket')
            if hashlib.sha256((directory/entry['path']).read_bytes()).hexdigest() != entry['sha256']:
                raise ValueError('keyword index bucket checksum mismatch')
    if coverage.get('scope') in ('deep_filing_relationships','archived_relationships_with_current_disclosures'):
        from src.deep_profile_store import read_index, read_profile
        root, index, fingerprint = read_index(directory/'filing-profiles')
        if fingerprint != coverage['profile_index_sha256']:
            raise ValueError('deep profile set does not match scoring snapshot')
        if snapshot.get('profile_publication',{}).get('mode') == 'keywords':
            receipt = json.loads((root/'publication.json').read_text())
            if receipt.get('mode') != 'keywords' or receipt.get('profile_index_sha256') != fingerprint or receipt.get('profiles') != keyword_coverage:
                raise ValueError('keyword publication receipt does not match scoring profiles')
            if set(keyword_coverage) != set(coverage['covered_tickers']):
                raise ValueError('keyword publication omitted covered tickers')
            for ticker,detail in keyword_coverage.items():
                if detail['retained_relationships'] != index['profiles'][ticker]['facts']:
                    raise ValueError('keyword publication lost archived relationship candidates')
        else:
            for entry in {e['path']: e for e in index['profiles'].values()}.values():
                read_profile(root, entry)
    json.loads((directory/'ticker-index.json').read_text())
    return meta


def publish(directory, source_sha):
    directory = directory.resolve()
    validate(directory, source_sha)
    # Each attempt starts at current main, not an old checkout's tree.
    for attempt in range(3):
        git('fetch', '--depth=1', 'origin', 'main')
        upstream = git('rev-parse', 'FETCH_HEAD')
        changes = git('diff', '--name-only', source_sha, upstream, '--', *INPUT_PATHS)
        if changes:
            raise ValueError('parser inputs changed during scoring; newer refresh must run: '+changes)
        with tempfile.TemporaryDirectory(prefix='scanner-index-') as temp:
            env = {**os.environ, 'GIT_INDEX_FILE': str(Path(temp)/'index'),
                   'GIT_AUTHOR_NAME': 'Scanner snapshot publisher',
                   'GIT_AUTHOR_EMAIL': 'bot@users.noreply.github.com',
                   'GIT_COMMITTER_NAME': 'Scanner snapshot publisher',
                   'GIT_COMMITTER_EMAIL': 'bot@users.noreply.github.com'}
            git('read-tree', upstream, env=env)
            names = artifact_names(directory)
            current_assets = set(names)
            updates = []
            old = git('ls-tree','-r','--name-only',upstream,'--',
                'dashboard/scanner-backtest/scoring-profiles','dashboard/scanner-backtest/filing-profiles',
                'dashboard/scanner-backtest/keyword-index')
            for path in old.splitlines():
                relative = path.removeprefix('dashboard/scanner-backtest/')
                if relative not in current_assets:
                    updates.append('0 '+'0'*40+'\t'+path+'\0')
            paths = [str(directory/name) for name in names]
            if any('\n' in path or '\r' in path or path.startswith('"') for path in paths):
                raise ValueError('unsupported asset filename')
            blobs = git_input(('hash-object','-w','--stdin-paths'),'\n'.join(paths)+'\n').splitlines()
            if len(blobs) != len(names):
                raise ValueError('asset hashing returned an incomplete batch')
            for name,blob in zip(names,blobs):
                updates.append('100644 '+blob+'\tdashboard/scanner-backtest/'+name+'\0')
            git_input(('update-index','-z','--index-info'),''.join(updates),env=env)
            # Every new asset was hashed and written above. Existing untouched
            # blobs belong to the verified upstream tree; sparse clones need
            # not download them again just to reproduce their unchanged refs.
            tree = git('write-tree','--missing-ok',env=env)
            if tree == git('rev-parse', upstream+'^{tree}'):
                return upstream
            commit = git('commit-tree', tree, '-p', upstream, '-m',
                         'Publish scanner snapshot for '+source_sha[:12], env=env)
        result = subprocess.run(['git', 'push', 'origin', commit+':refs/heads/main'], capture_output=True, text=True, timeout=120)
        if result.returncode == 0:
            return commit
        # Only concurrent non-fast-forward moves may be retried. Auth and
        # protected-branch failures stop here, rather than trying another path.
        if not any(reason in result.stderr for reason in ('fetch first', 'non-fast-forward')):
            raise RuntimeError('snapshot push failed; inspect GitHub permissions/protection')
    raise RuntimeError('main kept moving; snapshot was not published')


def main():
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument('--directory', type=Path, required=True)
    p.add_argument('--source-sha', required=True)
    p.add_argument('--github-api', action='store_true')
    p.add_argument('--repository', default=os.environ.get('GITHUB_REPOSITORY', ''))
    a = p.parse_args()
    if a.github_api and not a.repository:
        p.error('--github-api requires --repository')
    commit = publish_api(a.directory, a.source_sha, a.repository) if a.github_api else publish(a.directory, a.source_sha)
    print(json.dumps({'source_sha': a.source_sha, 'published_commit': commit}))


if __name__ == '__main__':
    main()


