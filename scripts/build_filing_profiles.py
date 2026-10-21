"""Compile all available whole SEC text objects into deterministic exposure graphs.

Run offline after materialize_filing_texts. No arbitrary issuer/filing/text/fact
limits; one issuer at a time bounds memory. Output is resumable per issuer.
"""
from __future__ import annotations
import argparse
import csv
import gzip
import hashlib
import json
import os
import tempfile
from collections import Counter
from pathlib import Path
from datetime import date

from src.filing_evidence import load_filing_sections
from src.filing_relationships import VERSION, SPACY_VERSION, MODEL_VERSION, extract_sections, load_parser, compiler_fingerprint, validate_paragraph_coverage


def encoded(payload):
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode()


def atomic_json(path, payload, compressed=False):
    path.parent.mkdir(parents=True, exist_ok=True)
    data = encoded(payload)
    if compressed:
        data = gzip.compress(data, mtime=0)
    temp = path.with_suffix(path.suffix+'.tmp')
    temp.write_bytes(data)
    os.replace(temp, path)


def catalog(path):
    if not path:
        return {}
    with Path(path).open(encoding='utf-8-sig', newline='') as stream:
        return {str(row.get('Ticker') or row.get('ticker') or '').upper(): row for row in csv.DictReader(stream)}


def build(objects, out, *, as_of, descriptions=None, nlp=None):
    date.fromisoformat(as_of)
    objects, out = Path(objects), Path(out)
    root = objects/'company-filings/full-v1'
    if not root.is_dir():
        root = objects
    companies = sorted(root.glob('*/company.json'))
    if not companies:
        raise ValueError('no full filing manifests')
    rows = catalog(descriptions)
    nlp = nlp or load_parser()
    compiler_hash = compiler_fingerprint()
    entries, totals = {}, Counter()
    for position, path in enumerate(companies, 1):
        company = json.loads(path.read_text())
        ciks = {path.parent.name}
        sections, report = load_filing_sections(objects, as_of=as_of, strict=True, ciks=ciks)
        tickers = sorted(sections)
        if not tickers:
            raise ValueError('issuer has no ticker association: '+path.parent.name)
        ticker = tickers[0]
        parts = sections[ticker]
        name = rows.get(ticker, {}).get('Company') or rows.get(ticker, {}).get('name') or company.get('name') or ticker
        fingerprint = hashlib.sha256(encoded({'version': VERSION, 'compiler_sha256': compiler_hash, 'spacy': SPACY_VERSION,
            'model': MODEL_VERSION, 'name': name, 'as_of': as_of,
            'sources': [(s['accession'], s['document'], s['section'], hashlib.sha256(s['text'].encode()).hexdigest()) for s in parts]})).hexdigest()
        target = out/'issuers'/f'{path.parent.name}.json.gz'
        profile = None
        if target.is_file():
            old = json.loads(gzip.decompress(target.read_bytes()))
            if old.get('source_fingerprint') == fingerprint:
                profile = old
        fresh = profile is None
        if fresh:
            profile = extract_sections(parts, ticker=ticker, name=name, nlp=nlp)
            profile.update(cik=path.parent.name, tickers=tickers, source_fingerprint=fingerprint, compiler_sha256=compiler_hash,
                           as_of=as_of, archive_coverage=report)
        source_key = lambda s: (s['accession'], s['document'], s['section'], s.get('sha256') or hashlib.sha256(s['text'].encode()).hexdigest())
        if Counter(source_key(s) for s in parts) != Counter(source_key(s) for s in profile['sources']):
            raise ValueError('paragraph source inventory differs from the verified archive')
        originals = {source_key(s): s['text'] for s in parts}
        validate_paragraph_coverage(profile, {s['id']: originals[source_key(s)] for s in profile['sources']})
        if profile['coverage']['characters_read'] != report['characters']:
            raise ValueError('paragraph coverage does not account for all archived characters')
        if fresh:
            atomic_json(target, profile, compressed=True)
        digest = hashlib.sha256(target.read_bytes()).hexdigest()
        for alias in tickers:
            entries[alias] = {'path': target.relative_to(out).as_posix(), 'sha256': digest,
                             'cik': path.parent.name, 'facts': len(profile['facts']),
                             'paragraphs': len(profile['paragraphs']), 'name': name}
        totals.update({key: report[key] for key in ('companies', 'filings', 'documents', 'characters')})
        totals['facts'] += len(profile['facts'])
        totals['paragraphs'] += len(profile['paragraphs'])
        totals['term_occurrences'] += profile['coverage'].get('term_occurrences',0)
        totals['paragraphs_requiring_review'] += profile['coverage']['paragraphs_requiring_review']
        print(json.dumps({'issuer': position, 'of': len(companies), 'tickers': tickers,
                          'filings': report['filings'], 'facts': len(profile['facts']),
                          'term_occurrences':profile['coverage'].get('term_occurrences',0),
                          'paragraphs': len(profile['paragraphs']), 'paragraph_outcomes': profile['coverage']['paragraph_outcomes']}), flush=True)
        # A checkpoint remains useful if the next issuer fails. Never mark an
        # incomplete shard complete, and never publish it as full coverage.
        atomic_json(out/'index.json', {'schema': VERSION, 'as_of': as_of, 'complete': False,
                    'profiles': entries, 'totals': dict(totals)})
    result = {'schema': VERSION, 'as_of': as_of, 'complete': True, 'compiler_sha256': compiler_hash,
              'source_revision': os.environ.get('GITHUB_SHA', ''),
              'profiles': entries, 'totals': dict(totals),
              'limitations': ['Explicit relation rules are incomplete; missing facts are unknown, not absent.',
                             'Asserted statements are filing claims, not independent verification.',
                             'Machine-readable attachments retained in source inventory; not treated as prose.']}
    atomic_json(out/'index.json', result)
    return result


def merge(shards, out, *, expected_companies=None, expected_filings=None):
    import shutil
    out = Path(out)
    entries, totals, as_of, schema, compiler_hash = {}, Counter(), None, None, None
    ciks = set()
    for path in sorted(Path(shards).glob('*/index.json')):
        index = json.loads(path.read_text())
        if not index.get('complete'):
            raise ValueError('incomplete filing profile shard: '+str(path))
        if schema is not None and (index['schema'], index['as_of']) != (schema, as_of):
            raise ValueError('mixed parser versions or filing dates')
        schema, as_of = index['schema'], index['as_of']
        if compiler_hash is not None and compiler_hash != index['compiler_sha256']:
            raise ValueError('mixed relationship compiler source')
        compiler_hash = index['compiler_sha256']
        for ticker, entry in index['profiles'].items():
            if ticker in entries:
                raise ValueError('duplicate ticker across shards: '+ticker)
            entries[ticker] = entry
        shard_ciks = {e['cik'] for e in index['profiles'].values()}
        if ciks & shard_ciks:
            raise ValueError('duplicate issuer across shards')
        ciks.update(shard_ciks)
        for entry in {e['path']: e for e in index['profiles'].values()}.values():
            src = path.parent/entry['path']
            if hashlib.sha256(src.read_bytes()).hexdigest() != entry['sha256']:
                raise ValueError('profile hash mismatch')
            validate_paragraph_coverage(json.loads(gzip.decompress(src.read_bytes())))
            dst = out/entry['path']
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(src, dst)
        totals.update(index['totals'])
    if not entries:
        raise ValueError('no complete profile shards')
    if expected_companies is not None and len(ciks) != expected_companies:
        raise ValueError(f'issuer coverage mismatch: {len(ciks)} != {expected_companies}')
    if expected_filings is not None and totals['filings'] != expected_filings:
        raise ValueError(f'filing coverage mismatch: {totals["filings"]} != {expected_filings}')
    result = {'schema': schema, 'as_of': as_of, 'complete': True, 'profiles': entries, 'compiler_sha256': compiler_hash,
              'totals': dict(totals), 'source_revision': os.environ.get('GITHUB_SHA', '')}
    atomic_json(out/'index.json', result)
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--objects', type=Path)
    parser.add_argument('--merge', type=Path)
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--as-of')
    parser.add_argument('--descriptions', type=Path)
    parser.add_argument('--expected-companies', type=int)
    parser.add_argument('--expected-filings', type=int)
    args = parser.parse_args(argv)
    if bool(args.objects) == bool(args.merge):
        parser.error('choose --objects or --merge')
    if args.merge:
        result = merge(args.merge, args.out, expected_companies=args.expected_companies, expected_filings=args.expected_filings)
    else:
        if not args.as_of:
            parser.error('--objects requires --as-of')
        result = build(args.objects, args.out, as_of=args.as_of, descriptions=args.descriptions)
    print(json.dumps({k: v for k, v in result.items() if k != 'profiles'}), flush=True)


if __name__ == '__main__':
    main()
