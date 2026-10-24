"""Cumulative scanner validation. Unknown, skipped and unavailable never mean pass.

Historical examples remain regressions. Each new candidate also needs disjoint
real issuers, frozen source documents, source-first labels and per-mode results.
This module does not invent labels or infer semantic accuracy from unit tests.
"""
from __future__ import annotations

import hashlib
import json
import random
import secrets
import math
import gzip
from pathlib import Path

SCHEMA = 'scanner-validation-1'
ARTICLE_ONLY_MODES = {'SRC-09','SRC-18','ID-07','ID-08','ID-09',
                      'LANE-01','LANE-02','LANE-03','LANE-04','LANE-05',
                      'IMPACT-01','IMPACT-02'}


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
                                     separators=(',', ':')).encode()).hexdigest()


def methodology_fingerprint(root):
    root = Path(root)
    # The sense manifest pins the generated dictionary bytes. Including ignored
    # build products makes the same code hash differently before/after bootstrap.
    resources = {p for p in root.glob('src/data/**/*') if p.is_file()
                 and p.suffix in ('.json', '.gz', '.txt')
                 and not (p.parent.name == 'senses' and p.suffix == '.gz')}
    paths = sorted(set(root.glob('src/**/*.py')) | set(root.glob('scripts/**/*.py')) | resources |
                   set(root.glob('requirements*.txt')) |
                   {root/'dashboard/scanner-backtest/index.html'})
    return digest({p.relative_to(root).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
                   for p in paths if p.is_file()})


def validate_register(register, previous=None):
    modes = register['modes']
    ids = [m['id'] for m in modes]
    if not ids or len(ids) != len(set(ids)):
        raise ValueError('empty register or duplicate failure IDs')
    for mode in modes:
        if not all(mode.get(k) for k in ('id', 'stage', 'mode', 'example', 'expected', 'sources')):
            raise ValueError('incomplete failure definition: '+mode.get('id', '?'))
    if previous:
        old = {m['id']: m for m in previous['modes']}
        current = {m['id']: m for m in modes}
        if old.keys()-current.keys():
            raise ValueError('failure IDs cannot be removed')
        for ident, mode in old.items():
            # Add clarifications and new incidents; do not rewrite a failing
            # original input or expected behavior to make a test pass.
            if any(current[ident][k] != mode[k] for k in ('example', 'expected')):
                raise ValueError('original failure changed: '+ident)
    return ids


def reserve(catalog, ledger, *, fingerprint, count=20, seed=None):
    """Draw unique issuers, not ticker aliases. Reserve BEFORE reading examples.

    Callers must commit the returned ledger before running/reviewing a sample.
    Failed and abandoned runs stay consumed. Exhaustion is an error, not reuse.
    """
    if count < 2 or not fingerprint:
        raise ValueError('need a candidate fingerprint and at least two issuers')
    used = set(ledger.get('excluded_issuer_ids', []))
    used_tickers = set(ledger.get('excluded_tickers', []))
    for run in ledger.get('runs', []):
        used.update(p['issuer_id'] for p in run['issuers'])
        used_tickers.update(p['ticker'] for p in run['issuers'])
    by_issuer = {}
    for row in catalog:
        if not row.get('filing_backed'):
            continue
        issuer = str(row.get('cik') or row.get('issuer_id') or '')
        if not issuer:
            raise ValueError('filing-backed issuer lacks a stable identity')
        issuer = issuer.zfill(10) if issuer.isdigit() else issuer
        if issuer in used or row['ticker'] in used_tickers:
            used.add(issuer)
        by_issuer.setdefault(issuer, []).append(row)
    eligible = sorted(set(by_issuer)-used)
    if len(eligible) < count:
        raise ValueError('fresh issuer pool exhausted; never silently reuse holdouts')
    seed = secrets.randbits(128) if seed is None else seed
    draw = random.Random(seed).sample(eligible, count)
    issuers = []
    for position, issuer in enumerate(draw):
        row = sorted(by_issuer[issuer], key=lambda x: x['ticker'])[0]
        issuers.append({'issuer_id': issuer, 'ticker': row['ticker'],
                        'split': 'development' if position < count//2 else 'holdout',
                        'source_asset': row.get('source_asset')})
    run = {'schema': SCHEMA, 'seed': seed, 'methodology_sha256': fingerprint,
           'catalog_sha256': digest(catalog), 'eligible_issuers': len(eligible),
           'issuers': issuers, 'state': 'reserved',
           'previous_ledger_sha256': digest(ledger)}
    run['id'] = digest(run)
    return run, {**ledger, 'runs': [*ledger.get('runs', []), run]}


def exact_terms(rows, sources):
    found = {}
    for row in rows:
        source_id, start, end = row['source_id'], row['start'], row['end']
        source = sources[source_id]
        if type(start) is not int or type(end) is not int or not 0 <= start < end <= len(source['text']):
            raise ValueError('invalid term source span')
        if source['text'][start:end] != row['text']:
            raise ValueError('complete term does not match its original source')
        key = (source_id, start, end)
        if key in found:
            raise ValueError('duplicate occurrence in score denominator')
        found[key] = row
    return found


def relation_set(row):
    """Direction, capacity, assertion, time and multiple roles all count."""
    return {json.dumps({k: rel[k] for k in
                       ('subject', 'predicate', 'object', 'capacity', 'assertion', 'time')},
                       sort_keys=True, ensure_ascii=False)
            for rel in row.get('relationships', [])}


def score_sources(reference, predictions):
    """Exact complete-span scores on independently reviewed full stored sources.

    Empty/unreviewed references fail. These are occurrence scores, explicitly
    separate from optional canonical-identity/alias consolidation scores.
    """
    if reference.get('reference_basis') != 'full_stored_sources' or not reference.get('reviewer'):
        raise ValueError('reference must be source-first and reviewed')
    sources = {s['id']: s for s in reference['sources']}
    if len(sources) != len(reference['sources']) or not sources:
        raise ValueError('empty or duplicate source inventory')
    for s in sources.values():
        if not s.get('review_complete') or not s.get('document_complete') or not s.get('date'):
            raise ValueError('source review or source coverage incomplete')
        if hashlib.sha256(s['text'].encode()).hexdigest() != s['sha256']:
            raise ValueError('source checksum mismatch')
    gold = exact_terms(reference['terms'], sources)
    actual = exact_terms(predictions['terms'], sources)
    isolated = exact_terms(predictions.get('classification_on_verified_terms', []), sources)
    if not gold:
        raise ValueError('empty reference is not a perfect recall score')
    tp = len(gold.keys() & actual.keys())
    precision = tp/len(actual) if actual else 0.0
    recall = tp/len(gold)
    def correct(key, rows):
        return (key in rows and bool(gold[key].get('relationships'))
                and relation_set(rows[key]) == relation_set(gold[key]))
    isolated_correct = sum(correct(k, isolated) for k in gold)
    pipeline_correct = sum(correct(k, actual) for k in gold)
    def interval(hits, total):
        if not total:
            return None
        z=1.959963984540054
        rate=hits/total; denominator=1+z*z/total
        center=(rate+z*z/(2*total))/denominator
        radius=z*math.sqrt(rate*(1-rate)/total+z*z/(4*total*total))/denominator
        return [max(0,center-radius),min(1,center+radius)]
    return {'unit': 'exact source term occurrences; no fuzzy partial-name credit',
            'gold_terms': len(gold), 'predicted_terms': len(actual), 'true_positive_terms': tp,
            'extraction_precision': precision, 'extraction_recall': recall,
            'extraction_precision_wilson95': interval(tp,len(actual)),
            'extraction_recall_wilson95': interval(tp,len(gold)),
            'isolated_classification_correct': isolated_correct,
            'isolated_classification_accuracy': isolated_correct/len(gold),
            'end_to_end_correct': pipeline_correct,
            'end_to_end_accuracy': pipeline_correct/len(gold),
            'classification_abstentions': sum(not isolated.get(k, {}).get('relationships') for k in gold
                                             if gold[k].get('relationships')),
            'reference_terms_without_relationship_judgments': sum(not g.get('relationships') for g in gold.values()),
            'missed_terms': [gold[k] for k in sorted(gold.keys()-actual.keys())],
            'extra_terms': [actual[k] for k in sorted(actual.keys()-gold.keys())],
            'extraction_target_met': precision >= .98 and recall >= .98}


def evaluate_modes(register, report, *, fingerprint):
    """Walk EVERY mode, recursively including all incident/check descendants.

    Checks contain independently reviewed expected/actual JSON and an immutable
    evidence hash. Missing instrumentation remains pending. No `all([])` pass.
    """
    ids = validate_register(register)
    evidence = report.get('mode_results', {})
    if set(evidence)-set(ids):
        raise ValueError('unknown mode IDs in report')
    results = []
    def check_tree(node):
        children = node.get('checks', [])
        if children:
            return all([check_tree(c) for c in children])
        return (node.get('reviewed') is True and bool(node.get('evidence_sha256'))
                and 'expected' in node and 'actual' in node and digest(node['actual']) == digest(node['expected']))
    def source_witness(node, issuer):
        if node.get('checks'):
            return all([source_witness(c, issuer) for c in node['checks']])
        refs=[s['reference'] for s in report.get('source_scores',[]) if s['issuer_id']==issuer]
        sources={s['id']:s for r in refs for s in r['sources']}
        try:
            text=sources[node['source_id']]['text'][node['start']:node['end']]
            return bool(text) and text==node['text'] and hashlib.sha256(text.encode()).hexdigest()==node['evidence_sha256']
        except (KeyError,TypeError):
            return False
    for mode in register['modes']:
        if mode['id'] in ARTICLE_ONLY_MODES:
            results.append({'id':mode['id'],'mode':mode['mode'],'status':'out_of_scope',
                'reasons':['Retained in the cumulative register; article/Lane/impact work does not block this stock-only milestone.']})
            continue
        row = evidence.get(mode['id'], {})
        reasons = []
        for field in ('original_reproducer', 'fresh_real_issuer', 'useful_positive_control', 'changed_decisions'):
            if not row.get(field) or not check_tree(row[field]):
                reasons.append(field+' missing, unreviewed or failed')
        if row.get('methodology_sha256') != fingerprint:
            reasons.append('result is not bound to current methodology')
        if row.get('fresh_run_id') != report.get('run_id') or not row.get('fresh_run_id'):
            reasons.append('fresh test not bound to this reserved draw')
        if row.get('fresh_real_issuer', {}).get('provenance') != 'stored_source':
            reasons.append('fresh test is not real stored source text')
        original = row.get('original_reproducer', {}).get('issuer_id')
        fresh = row.get('fresh_real_issuer', {}).get('issuer_id')
        allowed = {x['issuer_id'] for x in report.get('reserved_issuers', []) if x.get('split') == 'holdout'}
        if not fresh or fresh == original or fresh not in allowed:
            reasons.append('fresh issuer is not a different reserved holdout issuer')
        if not source_witness(row.get('fresh_real_issuer',{}),fresh):
            reasons.append('fresh reproducer lacks a verified unchanged source quotation')
        results.append({'id': mode['id'], 'mode': mode['mode'],
                        'status': 'pass' if not reasons else 'open', 'reasons': reasons})
    return results


def validate_stored_source_texts(graph):
    """Source integrity is independent of whether the OLD parser ran or worked."""
    sources={s['id']:s for s in graph['sources']}
    if len(sources)!=len(graph['sources']) or not sources:
        raise ValueError('empty or duplicate original source inventory')
    groups={ident:[] for ident in sources}
    for p in graph['paragraphs']:
        if p['source_id'] not in groups:
            raise ValueError('paragraph refers to absent original source')
        groups[p['source_id']].append(p)
    texts={}
    for ident,source in sources.items():
        cursor=0; pieces=[]
        for p in sorted(groups[ident],key=lambda p:p['start']):
            if p['start']!=cursor or p['end']-p['start']!=len(p['text']):
                raise ValueError('original source paragraph gap, overlap or bad span')
            pieces.append(p['text']);cursor=p['end']
        text=''.join(pieces)
        if cursor!=source['characters'] or hashlib.sha256(text.encode()).hexdigest()!=source['sha256']:
            raise ValueError('original source text inventory is incomplete')
        texts[ident]=text
    return texts


def verify_source_reference(reference, source_asset, source_root):
    """Compare labels to hash-pinned ORIGINAL profiles, not pasted source flags."""
    if not source_asset or not source_root:
        raise ValueError('original stored source asset required')
    sha=source_asset['sha256']
    if len(sha)!=64 or any(c not in '0123456789abcdef' for c in sha):
        raise ValueError('invalid original source asset checksum')
    raw=(Path(source_root)/(sha+'.json.gz')).read_bytes()
    if hashlib.sha256(raw).hexdigest()!=sha:
        raise ValueError('original stored asset checksum mismatch')
    graph=json.loads(gzip.decompress(raw))
    originals={s['id']:s for s in graph['sources']}
    texts=validate_stored_source_texts(graph)
    reviewed={s['id']:s for s in reference['sources']}
    if originals.keys()!=reviewed.keys():
        raise ValueError('source-first reference omitted original source sections')
    for ident,source in originals.items():
        if (texts[ident]!=reviewed[ident]['text'] or len(texts[ident])!=source['characters']
                or hashlib.sha256(texts[ident].encode()).hexdigest()!=source['sha256']
                or reviewed[ident]['date']!=source['filing_date']):
            raise ValueError('reference differs from original stored source')
    return texts


def release_decision(register, report, *, fingerprint, ledger=None, source_root=None):
    modes = evaluate_modes(register, report, fingerprint=fingerprint)
    reasons = []
    if report.get('schema') != SCHEMA or report.get('methodology_sha256') != fingerprint:
        reasons.append('missing or stale validation report')
    if not report.get('candidate_frozen_before_holdout_review'):
        reasons.append('candidate not frozen before holdout review')
    if not report.get('draw_committed_before_review'):
        reasons.append('random draw not retained before review')
    runs = (ledger or {}).get('runs', [])
    run = next((r for r in runs if r.get('id') == report.get('run_id')), None)
    if (not run or run != runs[-1]
            or digest({k:v for k,v in run.items() if k != 'id'}) != run.get('id')
            or run.get('methodology_sha256') != fingerprint
            or run.get('issuers') != report.get('reserved_issuers')):
        reasons.append('fresh draw absent, superseded, altered or bound to different code')
    scores = report.get('source_scores', [])
    holdout = {p['issuer_id'] for p in report.get('reserved_issuers', []) if p.get('split') == 'holdout'}
    if not holdout or {s.get('issuer_id') for s in scores} != holdout:
        reasons.append('complete source-based scores required for every reserved issuer')
    for score in scores:
        # Recompute from raw references and outputs; never trust pasted metrics.
        entry=next((p for p in (run or {}).get('issuers',[]) if p['issuer_id']==score['issuer_id']),{})
        try:
            verify_source_reference(score['reference'], entry.get('source_asset'), source_root)
        except (OSError,ValueError,KeyError) as exc:
            reasons.append('unverified original source: '+score['issuer_id']+': '+str(exc))
        measured = score_sources(score['reference'], score['predictions'])
        if not measured['extraction_target_met']:
            reasons.append('extraction below 98% precision/recall: '+score['issuer_id'])
        if measured['reference_terms_without_relationship_judgments']:
            reasons.append('missing classification reference judgments: '+score['issuer_id'])
    if any(m['status'] not in ('pass','out_of_scope') for m in modes):
        reasons.append('one or more cumulative failure modes remains open')
    return {'schema': SCHEMA, 'methodology_sha256': fingerprint,
            'release_allowed': not reasons, 'reasons': reasons, 'modes': modes,
            'classification_threshold': None,
            'classification_policy': 'Report isolated and end-to-end scores; no user-specified aggregate threshold.'}
