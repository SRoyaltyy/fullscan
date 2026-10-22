"""Validate a current disclosure projection over the preserved full archive.

This is a separate input type, never a claim that the old grammar or exhaustive
term inventory was rebuilt. Every available issuer is required by the manifest.
"""
from bisect import bisect_right
import hashlib
import re
from collections import defaultdict

SCHEMA = 'completed-parser-review-1'
ARCHIVE_COMPILER = '7953265b88372033f99c4f22f4d722f901fc8b7a0399a6547f7fecacbd377abc'


def validate_manifest(index):
    from .specific_filing_records import VERSION, source_fingerprint
    if index.get('schema') != SCHEMA or not index.get('complete'):
        raise ValueError('incomplete disclosure archive')
    if index.get('compiler_sha256') != ARCHIVE_COMPILER:
        raise ValueError('unrecognized preserved grammar compiler')
    if index.get('detail_extractor') != VERSION or index.get('detail_source_sha256') != source_fingerprint():
        raise ValueError('disclosure archive is not from the current extraction logic')
    entries = index['profiles']
    issuers = {e['cik'] for e in entries.values()}
    published, original = index['published'], index['archive_totals']
    if (len(entries) != published['tickers'] or len(issuers) != published['issuers']
            or len(issuers) != original['companies'] or published['relationships'] != original['facts']):
        raise ValueError('disclosure archive omitted issuers or relationships')
    for e in entries.values():
        if not re.fullmatch(r'\d{10}', e['cik']) or e['path'] != 'profiles/'+e['cik']+'.json.gz':
            raise ValueError('unsafe disclosure archive entry')
    if not re.fullmatch(r'https://raw\.githubusercontent\.com/SRoyaltyy/fullscan/[a-f0-9]{40}/', index.get('asset_base_url','')):
        raise ValueError('immutable disclosure assets are required')
    return index


def validate_profile(graph):
    from .specific_filing_records import VERSION, source_fingerprint, validate_details
    if graph.get('schema') != SCHEMA or graph.get('compiler_sha256') != ARCHIVE_COMPILER:
        raise ValueError('unsupported preserved disclosure profile')
    records = graph['specific_records']
    if records.get('version') != VERSION or records.get('source_sha256') != source_fingerprint():
        raise ValueError('stale disclosure extraction')
    if records.get('paragraph_scope') != 'complete_source_paragraphs':
        raise ValueError('disclosures were not scanned over the complete archived source')
    sources = {s['id']:s for s in graph['sources']}
    paragraphs = graph['paragraphs']
    if len(sources) != len(graph['sources']) or len({p['id'] for p in paragraphs}) != len(paragraphs):
        raise ValueError('duplicate disclosure source or paragraph')
    by_source = defaultdict(list)
    for p in paragraphs:
        if (p['source_id'] not in sources or p['end']-p['start'] != len(p['text'])
                or hashlib.sha256(p['text'].encode()).hexdigest() != p['sha256']):
            raise ValueError('disclosure paragraph integrity failure')
        by_source[p['source_id']].append(p)
    for blocks in by_source.values():blocks.sort(key=lambda p:p['start'])
    starts={key:[p['start'] for p in blocks] for key,blocks in by_source.items()}
    for fact in graph['facts']:
        if not fact.get('evidence'):raise ValueError('relationship without evidence')
        for ev in fact['evidence']:
            blocks=by_source[ev['source_id']]
            first=max(0,bisect_right(starts[ev['source_id']],ev['start'])-1)
            last=bisect_right(starts[ev['source_id']],ev['end']-1)
            pieces = [p['text'][max(ev['start'],p['start'])-p['start']:min(ev['end'],p['end'])-p['start']]
                for p in blocks[first:last] if p['start'] < ev['end'] and p['end'] > ev['start']]
            if ''.join(pieces) != ev['quote']:
                raise ValueError('preserved relationship quote differs from source paragraphs')
    validate_details(graph, records)
    return graph
