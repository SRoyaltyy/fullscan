"""Verify benchmark receipts, exact source spans and the random review sample."""
import csv,gzip,hashlib,json,pathlib
ROOT=pathlib.Path(__file__).resolve().parents[1]
DATA=ROOT/'eval/raw-extraction/benchmark-500'
def read(path):return json.loads(path.read_text(encoding='utf-8'))
def sha(raw):return hashlib.sha256(raw).hexdigest()
def key(text):return ' '.join(text.split()).casefold()
def main():
    refs=read(DATA/'reference-labels.json');report=read(DATA/'summary.json')
    predictions=json.loads(gzip.decompress((DATA/'predictions.json.gz').read_bytes()))
    rows=read(DATA/'term-results.json');sample=read(DATA/'precision-sample.json')
    draw=read(DATA/'draw.json');judgments=read(DATA/'precision-judgments.json')
    sources={c['ticker']:read(DATA/'sources'/(c['ticker']+'.json')) for c in refs['cases']}
    assert len(sources)==12 and len({s['cik'] for s in sources.values()})==12
    assert len(rows)==refs['reference_terms']==851>=500
    assert len(sample)==len(judgments['judgments'])==500
    assert {s['id'] for s in sample}=={j['id'] for j in judgments['judgments']}==set(range(1,501))
    for case in refs['cases']:
        path=DATA/'sources'/(case['ticker']+'.json')
        assert sha(path.read_bytes())==case['source_file_sha256']
        for p in sources[case['ticker']]['paragraphs']:assert sha(p['text'].encode())==p['sha256']
        for term in case['terms']:
            assert term['witnesses']
            for w in term['witnesses']:
                text=sources[case['ticker']]['paragraphs'][w['paragraph']]['text']
                assert text[w['start']:w['end']]==w['text']
    groups={};span_count=0;lookup={}
    for case in predictions:
        ticker=case['ticker'];groups[ticker]={}
        for index,p in enumerate(case['paragraphs']):
            text=sources[ticker]['paragraphs'][index]['text']
            for method in ('raw','new'):
                for term in p[method]:
                    assert text[term['start']:term['end']]==term['text'];span_count+=1
                lookup[ticker,index,method]=p[method]
            for term in p['new']:groups[ticker].setdefault(term['normalized'],[]).append(term)
    for issuer_index,issuer in enumerate(draw['selected']):
        ticker=issuer['ticker'];quota=42 if issuer_index<8 else 41
        expected=sorted(groups[ticker],key=lambda n:sha((draw['seed']+':precision:'+ticker+':'+n).encode()))[:quota]
        actual=[s['normalized'] for s in sample if s['ticker']==ticker]
        assert actual==expected,(ticker,'review sample changed')
    indexed={(r['ticker'],r['name']):r for r in rows}
    for case in refs['cases']:
        ticker=case['ticker']
        for term in case['terms']:
            row=indexed[ticker,term['name']];accepted={key(s) for s in term['accepted_names']}
            for method in ('raw','new'):
                exact=any(key(t['text']) in accepted for i in range(len(sources[ticker]['paragraphs'])) for t in lookup[ticker,i,method])
                contained=any(t['start']<=w['start'] and t['end']>=w['end']
                    for w in term['witnesses'] for t in lookup[ticker,w['paragraph'],method])
                assert row[method+'_exact']==exact
                assert row[method+'_complete_span']==contained
    with (DATA/'term-results.csv').open(encoding='utf-8',newline='') as stream:csvrows=list(csv.DictReader(stream))
    assert {(r['ticker'],r['name']) for r in csvrows}==set(indexed) and len(csvrows)==851
    assert sum(r['new_exact'] for r in rows)==report['summary']['new_exact']==754
    assert sum(r['new_complete_span'] for r in rows)==report['summary']['new_complete_span']==785
    assert report['precision']['valid']+report['precision']['invalid']+report['precision']['uncertain']==500
    assert sha((DATA/'reference-labels.json').read_bytes())==report['labels_sha256']
    assert sha((DATA/'precision-judgments.json').read_bytes())==report['precision']['judgments_sha256']
    receipt={'schema':'extraction-500-integrity-1','reference_pairs':851,'issuers':12,
       'reviewed_output_groups':500,'exact_source_spans_verified':span_count,
       'seeded_sample_reproduced':True,'metrics_recomputed_from_predictions':True,
       'csv_and_json_agree':True,'checksums_verified':True}
    (DATA/'integrity.json').write_text(json.dumps(receipt,indent=2)+'\n',encoding='utf-8')
    print(json.dumps(receipt,indent=2))
if __name__=='__main__':main()
