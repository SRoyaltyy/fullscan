"""Analyze public retained articles with existing free-model credentials.

No private checkout, context or evidence is read. Outputs are research hypotheses;
model validation is not a frontier verdict and grants no FACA acceptance credit.
"""
import argparse
import hashlib
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

FIELDS = ('id', 'title', 'body', 'published_at', 'url')
ATTEMPT_FIELDS = ('started_at', 'completed_at', 'status', 'model_stages', 'analysis',
                  'rejected_model_json', 'execution_error', 'producer_source', 'producer_environment')

def retain_attempt(previous, row):
    """A quota block is not a new semantic result. Keep earlier results labeled."""
    if previous and row['status'] == 'blocked_free_daily_quota':
        return {**previous, 'attempts': previous.get('attempts', []) + [
            {k: row.get(k) for k in ATTEMPT_FIELDS}],
            'latest_attempt': {'status': row['status'], 'completed_at': row['completed_at'],
                               'producer_source_sha256': row['producer_source']['sha256']}}
    attempts = previous.get('attempts', []) + [{k: previous.get(k) for k in ATTEMPT_FIELDS}] if previous else []
    return {**row, 'attempts': attempts,
            'latest_attempt': {'status': row['status'], 'completed_at': row['completed_at'],
                               'producer_source_sha256': row['producer_source']['sha256']}}

def public_input(document):
    return {key: document.get(key, '') for key in FIELDS}

def input_key(document):
    return hashlib.sha256(json.dumps(public_input(document), sort_keys=True, ensure_ascii=False).encode()).hexdigest()

def audit_acceptance(stage, accept, rejected):
    """Retain rejected public model JSON without weakening acceptance."""
    if accept is None:
        return None
    def check(parsed):
        accepted = accept(parsed)
        if not accepted and len(rejected) < 12:
            rejected.append({'stage': stage, 'parsed': parsed})
        return accepted
    return check


def pending(root, completed, limit=3, days=8, requested=None, source_sha256=None):
    now = datetime.now(timezone.utc).date()
    cutoff = (now - timedelta(days=days)).isoformat()
    rows = {}
    for path in sorted((root / 'data/news_intake').glob('*/documents.json')):
        if not cutoff <= path.parent.name <= now.isoformat():
            continue
        for document in json.loads(path.read_text()):
            if not isinstance(document, dict) or not document.get('id'):
                continue
            # Explicitly preserve the public source text. Feed-only inputs are
            # excluded; downstream source equivalence still needs review.
            if len(str(document.get('body') or '')) < 500:
                continue
            if document.get('extraction_status') in {'feed_text', 'headline_only', 'failed', 'unresolved'}:
                continue
            key = input_key(document)
            previous = completed.get(key)
            previous_source = (previous or {}).get('producer_source')
            same_source = source_sha256 is None or (isinstance(previous_source, dict) and previous_source.get('sha256') == source_sha256)
            if key in completed and same_source and previous.get('status') != 'blocked_free_daily_quota' and not str(previous.get('status', '')).startswith('execution_failed:') and (previous.get('analysis') or {}).get('reject_reason') not in {'lane_classify_missing', 'lane_meta_missing', 'lane_filter_missing'}:
                continue
            rows[key] = (public_input(document), path.relative_to(root).as_posix())
    candidates = sorted(rows.items(), key=lambda item: (item[1][0].get('published_at') or '', item[0]), reverse=True)
    if requested:
        candidates.sort(key=lambda item: item[1][0]['id'] != requested)
    return candidates[:limit]

def run(root, output, limit=3, requested=None):
    from src import lane_route
    from src.lane_one_shot import LiveLane
    from src.news_impact.one_shot_stack import process_article
    from src.news_impact.finviz_linker import get_index
    from src.news_impact.axioms import load_axioms
    from src.lane_source_identity import source_identity
    import importlib.metadata
    root = Path(root)
    path = Path(output)
    state = json.loads(path.read_text()) if path.exists() else {'records': {}, 'schema_version': 1}
    if not 1 <= limit <= 3:
        raise ValueError('One to three public articles per run')
    producer_source = source_identity(root)
    environment = {'python': sys.version.split()[0], 'packages': {}}
    for package in ('requests', 'urllib3', 'certifi', 'charset-normalizer', 'idna'):
        try:
            environment['packages'][package] = importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError:
            environment['packages'][package] = None
    state['current_source'] = producer_source
    models = [m for template in ('news_classify', 'news_filter', 'news_impact')
              for m in lane_route.primary_models_for('openrouter', template)]
    if not models or any(not lane_route._or_is_free(model) for model in models):
        raise ValueError('Free-model guard failed')
    class FreeLane(LiveLane):
        def _hop(self, stage, prompt, system, accept, hops, tmpl):
            return super()._hop(stage, prompt, system, accept, ['openrouter'], tmpl)
    live = FreeLane()
    live.ctx = {'keys': {'openrouter': live.ctx['keys']['openrouter']} if live.ctx['keys'].get('openrouter') else {},
                'ollama_url': '', 'gh_direct': ''}
    state['runtime_status'] = 'ready' if live.ctx['keys'] else 'free_model_credentials_missing'
    if not live.ctx['keys']:
        chosen = []
    else:
        chosen = pending(root, state['records'], limit, requested=requested, source_sha256=producer_source['sha256'])
    index = get_index(root)
    if not index.rows:
        state['runtime_status'] = 'company_index_missing'
        chosen = []
    index_sha = hashlib.sha256(Path(index.source).read_bytes()).hexdigest() if index.source else ''
    attempted = 0
    responses = 0
    for key, (document, source) in chosen:
        attempted += 1
        stages = []
        rejected = []
        def audited(stage, prompt, system, accept=None):
            parsed, provider, model = live(stage, prompt, system, accept=audit_acceptance(stage, accept, rejected))
            stages.append({'stage': stage, 'provider': provider, 'model': model, 'returned_json': parsed is not None, 'response_json': parsed})
            return parsed, provider, model
        started = datetime.now(timezone.utc).isoformat()
        execution_error = None
        article = {**document, 'article_id': document['id'], 'known_at': document['published_at'],
                   'source_file': source, 'harvest_source': document['url']}
        try:
            analysis = process_article(article, audited, axioms=load_axioms(), root=root, index_names=index.title_names)
            # Full prompts can be reconstructed from the public source input
            # and runtime; retain their hashes without duplicating article text.
            analysis['prompt_log'] = [{k: item.get(k) for k in ('stage', 'sha256', 'bytes', 'lines')}
                                      for item in analysis.get('prompt_log', [])]
            status = 'executed'
        except Exception as exc:
            analysis = None
            status = 'execution_failed:' + type(exc).__name__
            execution_error = {'type': type(exc).__name__, 'missing_module': getattr(exc, 'name', None)}
            import traceback
            execution_error['frames'] = [{'file': Path(frame.filename).name,
                'line': frame.lineno, 'function': frame.name}
                for frame in traceback.extract_tb(exc.__traceback__)[-6:]]
        responses += sum(s['returned_json'] for s in stages)
        if lane_route._OR_DAY_CAPPED:
            status = 'blocked_free_daily_quota'
            state['runtime_status'] = 'free_model_daily_quota_exhausted'
        row = {'public_input': document, 'source_file': source, 'input_sha256': key,
            'started_at': started, 'completed_at': datetime.now(timezone.utc).isoformat(),
            'status': status, 'model_stages': stages, 'analysis': analysis, 'rejected_model_json': rejected,
            'execution_error': execution_error,
            'index_source': index.source, 'index_sha256': index_sha, 'index_rows': len(index.rows),
            'producer_source': producer_source, 'producer_environment': environment,
            'fresh_acceptance_pass': False, 'frontier_verified': False,
            'review_status': 'pending_source_backed_four_axis_review'}
        state['records'][key] = retain_attempt(state['records'].get(key), row)
        if lane_route._OR_DAY_CAPPED:
            break
    state['updated_at'] = datetime.now(timezone.utc).isoformat()
    state['last_run_processed'] = attempted
    state['last_run_model_stage_responses'] = responses
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, ensure_ascii=False, indent=2))
    print(json.dumps({'runtime_status': state['runtime_status'], 'processed': attempted,
        'model_stage_responses': responses,
        'frontier_passes': 0}))
    return state

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=ROOT)
    parser.add_argument('--output', type=Path, default=ROOT / 'data/lane_public_analysis/results.json')
    parser.add_argument('--limit', type=int, default=3)
    parser.add_argument('--document-id')
    args = parser.parse_args()
    run(args.root, args.output, args.limit, args.document_id)
