"""Keyword/scoring projection over immutable exhaustive filing graphs.

This adapter intentionally does not change the grammar compiler or its fingerprint.
"""
from .deep_profile_store import KINDS, read_index, read_profile, latest_claims
from .stock_evidence import Fact, index_profiles
from .stock_keywords import SCHEMA, compile_keywords, quality_issues


def attach_keyword_profiles(board, directory, *, as_of, catalog):
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
        bundle = compile_keywords(graph, as_of=as_of, ticker=ticker)
        profile.keyword_records = bundle['records']
        profile.keyword_evidence = bundle['evidence']
        profile.keyword_coverage = bundle['coverage']
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
            issues = quality_issues(claim['predicate'], claim['object'], evidence['quote'])
            active = claim['status'] == 'asserted' and not issues
            fact = Fact(kind if active else 'inactive_'+kind, claim['object'], evidence['quote'],
                        source='SEC deep relationship', filing_date=source['filing_date'],
                        accession=source['accession'], section=source['section'],
                        confidence=claim['confidence'])
            fact.relationship_id = claim['id']
            fact.predicate = claim['predicate']
            fact.subject = ticker if claim['subject'] in graph['tickers'] else claim['subject']
            fact.status = claim['status']
            fact.review_required = bool(issues)
            fact.review_reasons = issues
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
        'compiler_version': index['schema'], 'keyword_schema': SCHEMA,
        'source_coverage_complete': True, 'extraction_complete': False,
        'all_paragraphs_classified': True, 'all_prose_dependency_analyzed': True,
        'full_business_coverage': False, 'errors': [], 'as_of': as_of,
        'covered_tickers': loaded, 'archive_tickers': len(index['profiles']),
        'catalog_tickers': len(board.profiles), 'archive_tickers_outside_catalog': missing,
        'profile_index_sha256': fingerprint, **index['totals']}

