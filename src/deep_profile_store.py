"""Read precompiled filing graphs without importing spaCy or using the network."""
import gzip
import hashlib
import json
from pathlib import Path

from .stock_evidence import Fact, index_profiles

KINDS = {'buys_from': 'supplier', 'sells_to': 'customer', 'uses_input': 'input',
         'depends_on': 'dependency', 'competes_with': 'competitor', 'partners_with': 'partner',
         'licenses': 'license', 'offers': 'offering', 'subject_to': 'regulatory_exposure',
         'regulated_by': 'regulatory_exposure', 'approval_from': 'regulatory_exposure',
         'risk_mentions': 'policy_exposure', 'differentiation_claim': 'differentiation_claim',
         'exposed_to': 'material_exposure',
         'operates_in': 'geography', 'owns_subsidiary': 'subsidiary',
         'acquires': 'acquisition', 'divests': 'divestiture'}


def read_index(directory):
    root = Path(directory).resolve()
    raw = (root/'index.json').read_bytes()
    index = json.loads(raw)
    from .filing_relationships import VERSION, compiler_fingerprint
    if not index.get('complete') or index.get('schema') != VERSION:
        raise ValueError('incomplete or unsupported deep filing profile set')
    if index.get('compiler_sha256') != compiler_fingerprint():
        raise ValueError('deep filing profiles were built by a different compiler; rebuild them')
    return root, index, hashlib.sha256(raw).hexdigest()


def read_profile(root, entry):
    path = (root/entry['path']).resolve()
    if not path.is_relative_to(root):
        raise ValueError('profile path escapes root')
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != entry['sha256']:
        raise ValueError('deep filing profile checksum mismatch')
    graph = json.loads(gzip.decompress(raw))
    from .filing_relationships import validate_paragraph_coverage
    validate_paragraph_coverage(graph)
    return graph


def latest_claims(graph, as_of):
    sources = {s['id']: s for s in graph['sources']}
    candidates = {}
    for fact in graph['facts']:
        evidence = fact.get('evidence') or []
        if not evidence:
            raise ValueError('relationship has no supporting evidence')
        for ev in evidence:
            source = sources[ev['source_id']]
            filing_date = source.get('filing_date')
            if not filing_date:
                raise ValueError('undated deep filing claim')
            if filing_date > as_of:
                continue
            key = (fact['subject'], fact['predicate'], fact['object'].casefold(), fact['object_type'])
            # Same-date contradictory claims remain inspectable but cannot
            # become current dependency assertions merely due to sort order.
            order = (filing_date, fact['status'] != 'asserted', fact['id'])
            if key not in candidates or order > candidates[key][0]:
                candidates[key] = (order, fact, ev, source)
    return [value[1:] for _, value in sorted(candidates.items())]


def attach_profiles(board, directory, *, as_of, catalog):
    root, index, fingerprint = read_index(directory)
    if index['as_of'] > as_of:
        raise ValueError('profile set is newer than the requested cutoff; rebuild it for this date')
    loaded, missing = [], []
    for ticker, entry in sorted(index['profiles'].items()):
        if ticker not in board.profiles:
            missing.append(ticker)
            continue
        graph = read_profile(root, entry)
        profile = board.profiles[ticker]
        profile.sources.extend(graph['sources'])
        profile.sources.append({'source': 'deep_filing_profile', 'sha256': entry['sha256'],
                                'section': graph['schema'], 'filing_date': graph['as_of']})
        profile.deep_profile = {'path': 'filing-profiles/'+entry['path'], 'sha256': entry['sha256'],
                                'coverage': graph['coverage'], 'as_of': graph['as_of']}
        for claim, evidence, source in latest_claims(graph, as_of):
            kind = KINDS.get(claim['predicate'], 'filing_relationship')
            if kind == 'regulatory_exposure' and claim['object_type'] not in ('agency', 'law'):
                kind = 'exposure'
            # Preserve uncertain/latest negative claims on the board for
            # inspection without indexing them as current identities/exposures.
            active = claim['status'] == 'asserted'
            fact = Fact(kind if active else 'inactive_'+kind, claim['object'], evidence['quote'],
                        source='SEC deep relationship', filing_date=source['filing_date'],
                        accession=source['accession'], section=source['section'],
                        confidence=claim['confidence'])
            fact.relationship_id = claim['id']
            fact.predicate = claim['predicate']
            fact.subject = ticker if claim['subject'] in graph['tickers'] else claim['subject']
            fact.status = claim['status']
            fact.object_type = claim['object_type']
            fact.qualifiers = claim['qualifiers']
            fact.document = source['document']
            fact.source_url = source.get('url', '')
            fact.evidence_span = {'start': evidence['start'], 'end': evidence['end'], 'source_id': source['id']}
            if active and kind in ('supplier', 'customer', 'competitor', 'partner'):
                found = catalog.find(claim['object'])
                if len(found) == 1:
                    fact.target_ticker = found[0][0]
            profile.facts.append(fact)
        loaded.append(ticker)
    rebuilt = index_profiles(board.profiles)
    return rebuilt, {'scope': 'deep_filing_relationships', 'complete': True,
        'compiler_version': index['schema'],
        'source_coverage_complete': True, 'extraction_complete': False,
        'all_paragraphs_classified': True, 'all_prose_dependency_analyzed': True,
        'full_business_coverage': False, 'errors': [], 'as_of': as_of,
        'covered_tickers': loaded, 'archive_tickers': len(index['profiles']),
        'catalog_tickers': len(board.profiles), 'archive_tickers_outside_catalog': missing,
        'profile_index_sha256': fingerprint, **index['totals']}
