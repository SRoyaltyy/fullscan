"""Extract broad lexical candidates from a UTF-8 filing text."""
import argparse
import hashlib
import json
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from src.filing_relationships import load_parser, windows
from src.filing_term_candidates import extract_candidates, VERSION
from src.term_inventory import relocate


def extract_text(text, nlp):
    terms, coverage, contexts = [], [], {}
    input_hash = hashlib.sha256(text.encode()).hexdigest()
    for offset, value in windows(text):
        result = extract_candidates(nlp(value))
        for row in relocate(result['terms'], offset):
            context = row.pop('sentence')
            context_id = hashlib.sha256(
                f"{input_hash}:{context['start']}:{context['end']}".encode()).hexdigest()
            contexts.setdefault(context_id, {'id': context_id, **context})
            row['sentence_id'] = context_id
            terms.append(row)
        coverage.append({'start': offset, 'end': offset+len(value),
                         'sha256': result['input_sha256']})
    for row in terms:
        row['id'] = hashlib.sha256(
            f"{input_hash}:{row['start']}:{row['end']}".encode()).hexdigest()
    return {'schema': VERSION, 'input_sha256': input_hash,
            'characters': len(text), 'windows': coverage,
            'contexts': list(contexts.values()), 'terms': terms,
            'candidate_occurrences': len(terms),
            'unique_normalized_strings': len({t['normalized'] for t in terms}),
            'relevance_assessed': False, 'relationships_assessed': False,
            'limitations': ['Named entities, noun phrases, nominal sequences, hyphen compounds and non-filler content words.',
                            'Normalized strings are lexical grouping keys, not resolved identities.',
                            'All candidates remain available; no business-specificity filter is applied.']}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--input', type=pathlib.Path, required=True)
    p.add_argument('--out', type=pathlib.Path, required=True)
    args = p.parse_args()
    with args.input.open('r', encoding='utf-8', newline='') as stream:
        text = stream.read()
    result = extract_text(text, load_parser())
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(result, ensure_ascii=False, indent=2)+'\n', encoding='utf-8')
    print(json.dumps({k: result[k] for k in
                     ('input_sha256', 'characters', 'candidate_occurrences', 'unique_normalized_strings')}))


if __name__ == '__main__':
    main()
