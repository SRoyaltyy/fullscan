"""Score a frozen source-first extraction benchmark without changing its parser."""
import argparse,csv,gzip,hashlib,json,pathlib,statistics
from collections import defaultdict
ROOT=pathlib.Path(__file__).resolve().parents[1]
DATA=ROOT/'eval/raw-extraction/benchmark-500'
import sys
sys.path.insert(0,str(ROOT))
from src.filing_relationships import load_parser
from src.term_inventory import parse_terms
from src.filing_term_candidates import extract_candidates

def sha(raw):return hashlib.sha256(raw).hexdigest()
def key(text):return ' '.join(text.split()).casefold()
def read(path):return json.loads(path.read_text(encoding='utf-8'))

def aggregate(rows):
    n=len(rows)
    return {'references':n,
      **{field:sum(bool(r[field]) for r in rows) for field in
        ('raw_exact','new_exact','raw_complete_span','new_complete_span',
         'raw_canonical_exact','new_canonical_exact','raw_canonical_span','new_canonical_span')},
      **{field+'_rate':sum(bool(r[field]) for r in rows)/n if n else None for field in
         ('raw_exact','new_exact','raw_complete_span','new_complete_span')}}

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--label-freeze',required=True)
    args=parser.parse_args()
    frozen=read(DATA/'frozen-parser.json')
    for file in frozen['files']:
        assert sha((ROOT/file['path']).read_bytes())==file['sha256'],file['path']
    draw=read(DATA/'draw.json')
    labels=read(DATA/'reference-labels.json')
    assert labels['reference_terms']>=500
    nlp=load_parser()
    assert nlp.meta['version']==frozen['model_version']
    by_issuer={c['ticker']:c for c in labels['cases']}
    rows,predictions,precision_sample=[],[],[]
    source_receipts=[]
    for issuer_index,issuer in enumerate(draw['selected']):
        ticker=issuer['ticker'];case=by_issuer[ticker]
        path=DATA/'sources'/f'{ticker}.json'
        assert sha(path.read_bytes())==case['source_file_sha256']
        source=read(path)
        parsed=[]
        for paragraph_index,p in enumerate(source['paragraphs']):
            assert sha(p['text'].encode())==p['sha256']
            doc=nlp(p['text'])
            methods={'raw':parse_terms(doc)['terms'],'new':extract_candidates(doc)['terms']}
            for method,terms in methods.items():
                for t in terms:
                    assert p['text'][t['start']:t['end']]==t['text']
                methods[method]=[{k:t[k] for k in
                   ('start','end','text','normalized','kinds','entity_types') if k in t} for t in terms]
            parsed.append(methods)
        predictions.append({'ticker':ticker,'paragraphs':parsed})
        for term in case['terms']:
            accepted={key(s) for s in term['accepted_names']}
            canonical=key(term['name'])
            for w in term['witnesses']:
                assert source['paragraphs'][w['paragraph']]['text'][w['start']:w['end']]==w['text']
            row={'ticker':ticker,'name':term['name'],
                 'stratum':'single_word' if len(term['name'].split())==1 else 'multiword'}
            for method in ('raw','new'):
                same=[(pi,t) for pi,p in enumerate(parsed) for t in p[method] if key(t['text']) in accepted]
                row[method+'_exact']=bool(same)
                row[method+'_canonical_exact']=any(key(t['text'])==canonical for pi,t in same)
                covered=[];canonical_covered=[]
                for w in term['witnesses']:
                    matches=[t for t in parsed[w['paragraph']][method]
                             if t['start']<=w['start'] and t['end']>=w['end']]
                    if matches:covered.extend({'paragraph':w['paragraph'],'candidate':t['text'],
                       'reference':w['text']} for t in matches[:2])
                    if key(w['text'])==canonical and matches:canonical_covered.extend(matches)
                row[method+'_complete_span']=bool(covered)
                row[method+'_canonical_span']=bool(canonical_covered)
                if method=='new':
                    row['exact_evidence']=[{'paragraph':pi,'text':t['text'],'start':t['start'],'end':t['end']} for pi,t in same[:4]]
                    row['covering_evidence']=covered[:4]
            rows.append(row)
        groups={}
        for pi,p in enumerate(parsed):
            for term in p['new']:
                normalized=term['normalized']
                group=groups.setdefault(normalized,{'ticker':ticker,'normalized':normalized,
                     'forms':[],'occurrences':[]})
                if term['text'] not in group['forms']:group['forms'].append(term['text'])
                group['occurrences'].append({'paragraph':pi,'start':term['start'],'end':term['end'],'text':term['text']})
        ranked=sorted(groups.values(),key=lambda g:sha(
            (draw['seed']+':precision:'+ticker+':'+g['normalized']).encode()))
        quota=42 if issuer_index<8 else 41
        assert len(ranked)>=quota,(ticker,len(ranked),quota)
        for group in ranked[:quota]:
            precision_sample.append({'id':len(precision_sample)+1,**group})
        source_receipts.append({'ticker':ticker,'paragraphs':len(source['paragraphs']),
          'characters':sum(len(p['text']) for p in source['paragraphs']),
          'profile_url':source['profile_url'],'profile_sha256':source['profile_sha256'],
          'candidate_groups':len(groups),
          'raw_occurrences':sum(len(p['raw']) for p in parsed),
          'new_occurrences':sum(len(p['new']) for p in parsed)})
    assert len(precision_sample)==500
    assert len(rows)==labels['reference_terms']
    report={'schema':'extraction-500-results-1','parser_revision':frozen['revision'],
       'label_freeze_commit':args.label_freeze,
       'labels_sha256':sha((DATA/'reference-labels.json').read_bytes()),
       'draw_sha256':sha((DATA/'draw.json').read_bytes()),
       'frozen_parser_sha256':sha((DATA/'frozen-parser.json').read_bytes()),
       'scorer_sha256':sha(pathlib.Path(__file__).read_bytes()),
       'spacy_version':__import__('spacy').__version__,'model_version':nlp.meta['version'],
       'scope':'Source-first lexical extraction on retained filing passages, not whole filings or resolved identities.',
       'summary':aggregate(rows),
       'by_stock':{ticker:aggregate([r for r in rows if r['ticker']==ticker]) for ticker in by_issuer},
       'by_stratum':{stratum:aggregate([r for r in rows if r['stratum']==stratum]) for stratum in ('single_word','multiword')},
       'source_receipts':source_receipts,
       'precision':{'status':'awaiting source review of sampled outputs','sample_groups':500},
       'global_accuracy_certified':False,
       'limitations':['Assistant labels, not independent human certification.',
          'Stratified issuer sample and overlapping term units are clustered; this is not a market-wide guarantee.',
          'Stored profiles contain retained passages; full-filing coverage is not established.',
          'Exact term/alias recovery and containment in a longer phrase are separate metrics.',
          'Source aliases can recover an identity without recovering its complete canonical name.',
          'Relationships and semantic identity are outside this extraction test.']}
    report['macro_new_exact_rate']=statistics.mean(v['new_exact_rate'] for v in report['by_stock'].values())
    report['macro_new_complete_span_rate']=statistics.mean(v['new_complete_span_rate'] for v in report['by_stock'].values())
    (DATA/'recall-results.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    (DATA/'term-results.json').write_text(json.dumps(rows,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    (DATA/'precision-sample.json').write_text(json.dumps(precision_sample,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    raw=json.dumps(predictions,ensure_ascii=False,sort_keys=True).encode()
    (DATA/'predictions.json.gz').write_bytes(gzip.compress(raw,mtime=0))
    with (DATA/'term-results.csv').open('w',encoding='utf-8',newline='') as stream:
        fields=['ticker','name','stratum','raw_exact','new_exact','raw_complete_span','new_complete_span','new_canonical_exact','new_canonical_span']
        writer=csv.DictWriter(stream,fieldnames=fields,extrasaction='ignore');writer.writeheader();writer.writerows(rows)
    print(json.dumps({'summary':report['summary'],'by_stock':report['by_stock'],
       'by_stratum':report['by_stratum'],'sample_groups':500,
       'candidate_groups':sum(s['candidate_groups'] for s in source_receipts)},indent=2))
if __name__=='__main__':main()
