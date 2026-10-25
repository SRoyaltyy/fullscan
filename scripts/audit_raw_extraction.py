"""Replay existing source-first labels against raw and cleaned lexical candidates.

This is a consumed diagnostic replay, not fresh holdout or whole-filing accuracy.
"""
import hashlib
import gzip
import json
import pathlib
import sys
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from src.filing_relationships import load_parser
from src.term_inventory import parse_terms
from src.filing_term_candidates import extract_candidates

ROOT = pathlib.Path(__file__).resolve().parents[1]
DATA = ROOT/'eval/raw-extraction'
def key(text):
    return ' '.join(text.split()).casefold()


def main():
    source_bytes=(DATA/'source-sample.json').read_bytes()
    labels_bytes=(DATA/'reference-labels.json').read_bytes()
    source=json.loads(source_bytes)
    labels=json.loads(labels_bytes)
    assert labels['source_sample_sha256']==hashlib.sha256(source_bytes).hexdigest()
    nlp=load_parser()
    output={'scope':'Existing eight witness paragraphs / four issuers / 19 reference terms; consumed replay.',
            'annotation':labels['annotation'],
            'source_sha256':hashlib.sha256(source_bytes).hexdigest(),
            'labels_sha256':hashlib.sha256(labels_bytes).hexdigest(),
            'raw_inventory_code_sha256':hashlib.sha256((ROOT/'src/term_inventory.py').read_bytes()).hexdigest(),
            'spacy_version':__import__('spacy').__version__, 'model_version':nlp.meta['version'],
            'candidate_code_sha256':hashlib.sha256((ROOT/'src/filing_term_candidates.py').read_bytes()).hexdigest(),
            'fresh_holdout':False,'global_accuracy_certified':False,
            'summary':{},'cases':[],'predictions':[]}
    for case in labels['cases']:
        inputs=next(c for c in source['cases'] if c['ticker']==case['ticker'])
        paragraphs=[]
        for paragraph in inputs['paragraphs']:
            doc=nlp(paragraph['text'])
            paragraphs.append({'raw':parse_terms(doc)['terms'],
                               'candidate':extract_candidates(doc)['terms']})
        result={'ticker':case['ticker'],'terms':[]}
        for term in case['terms']:
            row={'name':term['name']}
            accepted={key(x) for x in term['accepted_names']}
            for method in ('raw','candidate'):
                hits=[{'paragraph':i,'text':t['text'],'start':t['start'],'end':t['end']}
                      for i,p in enumerate(paragraphs) for t in p[method] if key(t['text']) in accepted]
                covering=[{'paragraph':w['paragraph'],'expected':w['text'],'text':t['text']}
                          for w in term['witnesses'] for t in paragraphs[w['paragraph']][method]
                          if t['start']<=w['start'] and t['end']>=w['end']]
                row[method]={'exact':bool(hits),'complete_span_covered':bool(covering),
                             'exact_hits':hits,'covering_hits':covering[:5]}
            result['terms'].append(row)
        output['cases'].append(result)
        output['predictions'].append({'ticker':case['ticker'],'paragraphs':paragraphs})
    all_terms=[r for c in output['cases'] for r in c['terms']]
    for method in ('raw','candidate'):
        output['summary'][method]={'reference_terms':len(all_terms),'precision':None,
           'exact_recovered':sum(t[method]['exact'] for t in all_terms),
           'complete_span_covered':sum(t[method]['complete_span_covered'] for t in all_terms),
           'exact_missed':[t['name'] for t in all_terms if not t[method]['exact']],
           'uncovered':[t['name'] for t in all_terms if not t[method]['complete_span_covered']]}
    output['limitations']=['Reference terms include concrete operating/financial concepts under the earlier annotation contract.',
       'Candidate precision under the broader extract-first scope has not been reviewed.',
       'Containing a full span in a larger noun phrase is reported separately from exact term recovery.',
       'No relationship correctness is scored. This is not an independent or population accuracy estimate.']
    predictions=output.pop('predictions')
    raw_predictions=json.dumps(predictions,ensure_ascii=False,sort_keys=True).encode()
    (DATA/'diagnostic-predictions.json.gz').write_bytes(gzip.compress(raw_predictions,mtime=0))
    output['predictions_sha256']=hashlib.sha256(raw_predictions).hexdigest()
    output['candidate_occurrences']=sum(len(p['candidate']) for c in predictions for p in c['paragraphs'])
    (DATA/'diagnostic-replay.json').write_text(json.dumps(output,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    print(json.dumps(output['summary'],ensure_ascii=False,indent=2))


if __name__=='__main__':
    main()
