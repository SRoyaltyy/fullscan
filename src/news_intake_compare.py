"""Compare an independently selected event checklist with actual intake.

Input CSV: event_id,title,keywords,published_at,importance
Keywords are semicolon-separated required terms. Freeze the checklist before
reading results. Matches are evidence candidates requiring human confirmation.
python -m src.news_intake_compare --date 2026-10-02 --expected checklist.csv
"""
import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path

from .news_intake import ROOT, norm, parse_time, write_json


def compare(documents: list[dict], expected: list[dict]) -> dict:
    results = []
    for event in expected:
        terms = [norm(t) for t in event.get('keywords', '').split(';') if norm(t)]
        matches = []
        for doc in documents:
            # Exact normalized titles or explicitly supplied AND terms. Do not
            # grade using generic query class membership or an LLM essay.
            title = norm(doc['title'].rsplit(' - ', 1)[0])
            matched = all(t in title for t in terms) if terms else title == norm(event.get('title', ''))
            if matched:
                matches.append(doc)
        publication = parse_time(event.get('published_at', ''))
        first = min((parse_time(d['first_seen']) for d in matches), default=None)
        latency = (first-publication).total_seconds() if first and publication else None
        results.append({**event, 'status': 'possible_match_needs_confirmation' if matches else 'not_found',
                        'first_seen': first.isoformat() if first else '', 'discovery_latency_seconds': latency,
                        'evidence': [{'id': d['id'], 'title': d['title'], 'url': d.get('url','')} for d in matches[:20]]})
    return {'expected_count': len(expected), 'possible_matches': sum(bool(r['evidence']) for r in results),
            'verified_recall': None, 'reason': 'Human event-identity confirmation required', 'results': results}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--date', required=True)
    parser.add_argument('--expected', required=True)
    args = parser.parse_args()
    datetime.fromisoformat(args.date)
    path = ROOT / 'data/news_intake' / args.date
    docs = json.loads((path / 'documents.json').read_text())
    with Path(args.expected).open(newline='', encoding='utf-8') as handle:
        expected = list(csv.DictReader(handle))
    report = compare(docs, expected)
    report['generated_at'] = datetime.now(timezone.utc).isoformat()
    write_json(path / 'comparison.json', report)
    print(json.dumps({k:v for k,v in report.items() if k != 'results'}, indent=2))


if __name__ == '__main__':
    main()
