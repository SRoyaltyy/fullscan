"""Check every extracted quote against verified full filing text, not AI examples."""
import argparse
import hashlib
import json
from collections import Counter
from pathlib import Path
from src.deep_profile_store import read_index, read_profile
from src.filing_evidence import load_filing_sections
from src.filing_relationships import validate_paragraph_coverage


def audit(profiles, objects):
    root, index, _ = read_index(profiles)
    checked = 0
    for entry in {e['cik']: e for e in index['profiles'].values()}.values():
        graph = read_profile(root, entry)
        data, report = load_filing_sections(Path(objects), as_of=graph['as_of'], strict=True, ciks={entry['cik']})
        sections = data[graph['tickers'][0]]
        texts = {(s['accession'], s['document'], s['section'], hashlib.sha256(s['text'].encode()).hexdigest()): s['text'] for s in sections}
        sources = {s['id']: s for s in graph['sources']}
        if graph['coverage']['characters_read'] != report['characters']:
            raise ValueError('not all available characters were read')
        verified_texts = {s['id']: texts[(s['accession'], s['document'], s['section'], s['sha256'])] for s in graph['sources']}
        paragraphs_checked = validate_paragraph_coverage(graph, verified_texts)
        if not graph['coverage']['all_prose_dependency_analyzed'] or graph['coverage'].get('prose_characters_analyzed', 0) != report['characters']-graph['coverage'].get('machine_characters', 0):
            raise ValueError('not all prose received dependency analysis')
        for fact in graph['facts']:
            for ev in fact['evidence']:
                source = sources[ev['source_id']]
                text = texts[(source['accession'], source['document'], source['section'], source['sha256'])]
                if text[ev['start']:ev['end']] != ev['quote']:
                    raise ValueError('evidence does not exactly match the filing: '+fact['id'])
                checked += 1
        print(json.dumps({'tickers': graph['tickers'], 'archive': report, 'extraction': graph['coverage'],
                          'all_quotes_verified': True, 'all_paragraphs_verified': True,
                          'paragraphs_verified': paragraphs_checked}), flush=True)
        # Examples are a bounded audit preview only; the exported graph is uncapped.
        important = sorted(graph['facts'], key=lambda f: (f['predicate'] not in ('buys_from','competes_with','subject_to','regulated_by'), f['id']))
        for fact in important[:35]:
            ev = fact['evidence'][0]
            source = sources[ev['source_id']]
            print(json.dumps({'subject': fact['subject'], 'predicate': fact['predicate'], 'object': fact['object'],
                'status': fact['status'], 'quote': ev['quote'], 'filing_date': source['filing_date'],
                'document': source['document'], 'accession': source['accession']}, ensure_ascii=False), flush=True)
    return checked


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--profiles', type=Path, required=True)
    parser.add_argument('--objects', type=Path, required=True)
    args = parser.parse_args()
    print('Verified exact evidence spans:', audit(args.profiles, args.objects))


if __name__ == '__main__':
    main()
