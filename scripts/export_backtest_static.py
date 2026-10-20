"""Export provenance-bound scanner results and the exact profiles used to score them.

Headline-only is default. Use a new --out-dir for changed code, inputs, or filings.
No SEC fetch, LLM, or network operation occurs while scoring.
"""
from __future__ import annotations
import argparse
import gzip
import hashlib
import json
import os
import time
from pathlib import Path
from src.correlation import link_articles
from src.link_correlation import ROOT
from src.scanner_backtest import (ENGINE, load_test_articles, open_program, present,
                                  profile_view, select_input, snapshot_metadata)

OUT_DIR = ROOT / 'dashboard/scanner-backtest'
_BOARD = _NAMES = _SNAPSHOT = None
_INPUT_MODE = 'headline'


def _init_worker(descriptions, sectors, universe, identity_filings, as_of, input_mode, snapshot, filing_profiles=None):
    global _BOARD, _NAMES, _INPUT_MODE, _SNAPSHOT
    _BOARD, _NAMES = open_program(descriptions, sectors, universe,
                                 identity_filings=identity_filings, as_of=as_of, filing_profiles=filing_profiles)
    if snapshot_metadata(_BOARD, input_mode)['snapshot_id'] != snapshot['engine_snapshot_id']:
        raise ValueError('scoring inputs changed after snapshot freeze')
    _INPUT_MODE, _SNAPSHOT = input_mode, snapshot


def _score_shard(shard):
    selected = [select_input(article, _INPUT_MODE) for article in shard]
    linked = link_articles(_BOARD, selected, names=_NAMES, engine=ENGINE)
    if len(linked) != len(selected):
        raise ValueError('engine returned an incomplete shard')
    results = []
    for article, item in zip(selected, linked):
        row = present(item)
        row.update(id=article['id'], input_mode=_INPUT_MODE,
                   input_sha256=hashlib.sha256(article['text'].encode()).hexdigest(),
                   snapshot_id=_SNAPSHOT['snapshot_id'])
        results.append(row)
    return results


def export_articles(articles, text_limit):
    return [{**{k: a.get(k, '') for k in ('id', 'title', 'url', 'source', 'published_at')},
             'text': str(a.get('text') or '')[:text_limit],
             'truncated': len(str(a.get('text') or '')) > text_limit,
             'full_text': a.get('input_mode') == 'full-text',
             'input_mode': a.get('input_mode', 'headline')}
            for a in articles]


def read_results(path, snapshot_id):
    if not path.is_file():
        return []
    rows, seen = [], set()
    for line in path.read_text(encoding='utf-8').splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        if row.get('snapshot_id') != snapshot_id:
            raise ValueError('unversioned or incompatible result cache; use a new --out-dir')
        key = str(row['id'])
        if key in seen:
            raise ValueError('duplicate article id in result cache')
        seen.add(key)
        rows.append(row)
    return rows


def validate_resume(out_dir, snapshot):
    meta_path = out_dir / 'meta.json'
    existing = any((out_dir / name).is_file() for name in
                   ('results.jsonl', 'articles.json', 'profiles.json', 'ticker-index.json'))
    if existing and not meta_path.is_file():
        raise ValueError('unversioned artifacts; use a new --out-dir')
    if meta_path.is_file():
        meta = json.loads(meta_path.read_text(encoding='utf-8'))
        if meta.get('snapshot', {}).get('snapshot_id') != snapshot['snapshot_id']:
            raise ValueError('snapshot mismatch; use a new --out-dir')
    return read_results(out_dir / 'results.jsonl', snapshot['snapshot_id'])


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument('--news', type=Path, default=ROOT / 'data/news_intake/2026-10-17/documents.json')
    p.add_argument('--descriptions', type=Path, default=ROOT / 'excel_bot/data/finviz_with_descriptions.csv')
    p.add_argument('--sectors', type=Path)
    p.add_argument('--universe', type=Path)
    p.add_argument('--identity-filings', type=Path)
    p.add_argument('--filing-profiles', type=Path)
    p.add_argument('--as-of')
    p.add_argument('--input-mode', choices=('headline', 'full-text'), default='headline')
    p.add_argument('--limit', type=int, default=0)
    p.add_argument('--offset', type=int, default=0)
    p.add_argument('--workers', type=int, default=2)
    p.add_argument('--shard-size', type=int, default=100)
    p.add_argument('--text-limit', type=int, default=3000)
    p.add_argument('--out-dir', type=Path, default=OUT_DIR)
    a = p.parse_args(argv)
    if min(a.workers, a.shard_size, a.text_limit) < 1 or min(a.limit, a.offset) < 0:
        p.error('workers/shard-size/text-limit must be positive; limit/offset nonnegative')
    if (a.identity_filings or a.filing_profiles) and not a.as_of:
        p.error('--identity-filings/--filing-profiles requires --as-of')
    board, names = open_program(a.descriptions, a.sectors, a.universe,
                                identity_filings=a.identity_filings, as_of=a.as_of, filing_profiles=a.filing_profiles)
    snapshot = snapshot_metadata(board, a.input_mode)
    snapshot['engine_snapshot_id'] = snapshot.pop('snapshot_id')
    snapshot['source_revision'] = os.environ.get('SCANNER_SOURCE_SHA') or os.environ.get('GITHUB_SHA') or ''
    snapshot['news_sha256'] = hashlib.sha256(a.news.read_bytes()).hexdigest()
    snapshot['snapshot_id'] = hashlib.sha256(json.dumps(snapshot, sort_keys=True).encode()).hexdigest()
    rows = validate_resume(a.out_dir, snapshot)
    articles = load_test_articles(a.news)
    by_id = {str(article['id']): article for article in articles}
    if len(by_id) != len(articles):
        raise ValueError('duplicate article ids in input')
    done = {str(row['id']) for row in rows}
    if done - by_id.keys():
        raise ValueError('cached article absent from intake')
    todo = [article for article in articles[a.offset:] if str(article['id']) not in done]
    if a.limit:
        todo = todo[:a.limit]
    a.out_dir.mkdir(parents=True, exist_ok=True)
    meta = {'engine': ENGINE, 'intake': a.news.parent.name, 'snapshot': snapshot}
    (a.out_dir / 'meta.json').write_text(json.dumps(meta, indent=2) + '\n', encoding='utf-8')
    started = time.time()
    if todo:
        import concurrent.futures as futures
        shards = [todo[i:i + a.shard_size] for i in range(0, len(todo), a.shard_size)]
        with futures.ProcessPoolExecutor(max_workers=a.workers, initializer=_init_worker,
                initargs=(a.descriptions, a.sectors, a.universe, a.identity_filings,
                          a.as_of, a.input_mode, snapshot, a.filing_profiles)) as pool, \
                (a.out_dir / 'results.jsonl').open('a', encoding='utf-8') as out:
            for shard in pool.map(_score_shard, shards):
                for row in shard:
                    out.write(json.dumps(row, ensure_ascii=False) + '\n')
                out.flush()
                rows.extend(shard)
                print(f'scored {len(rows)} articles ({time.time() - started:.0f}s)', flush=True)
    elif not (a.out_dir / 'results.jsonl').exists():
        (a.out_dir / 'results.jsonl').touch()
    display = export_articles([select_input(by_id[str(r['id'])], a.input_mode) for r in rows], a.text_limit)
    # Lazy per-ticker assets keep page loading practical without deleting any
    # facts, quotations or source records from the exported scoring profiles.
    profiles = {}
    asset_root = a.out_dir/'scoring-profiles'
    asset_root.mkdir(exist_ok=True)
    for ticker, profile in board.evidence_board.profiles.items():
        full = profile_view(profile)
        raw = gzip.compress(json.dumps(full, ensure_ascii=False, sort_keys=True).encode(), mtime=0)
        path = 'scoring-profiles/'+hashlib.sha256(ticker.encode()).hexdigest()+'.json.gz'
        (a.out_dir/path).write_bytes(raw)
        profiles[ticker] = {key: full[key] for key in ('name','industry','sector','source','enriched','used_for_scoring')}
        profiles[ticker].update(profile_path=path, profile_sha256=hashlib.sha256(raw).hexdigest(),
                               fact_counts={kind: len(entries) for kind, entries in full['facts'].items()})
    if a.filing_profiles:
        import shutil
        shutil.copytree(a.filing_profiles, a.out_dir/'filing-profiles', dirs_exist_ok=True)
    index = {}
    for row in rows:
        for stock in row['passes']:
            index.setdefault(stock['ticker'], []).append({'id': row['id'], 'decision': row['decision'],
                'tier': stock['tier'], 'reasons': stock['reasons']})
    for name, payload in [('articles.json', display), ('profiles.json', profiles), ('ticker-index.json', index)]:
        (a.out_dir / name).write_text(json.dumps(payload, ensure_ascii=False), encoding='utf-8')
    meta.update(articles=len(rows), articles_with_passes=sum(bool(r['pass_count']) for r in rows),
                generated_at=time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()))
    (a.out_dir / 'meta.json').write_text(json.dumps(meta, indent=2) + '\n', encoding='utf-8')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())


