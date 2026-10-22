"""Report frozen challenge-set results without tuning the production resolver.

This measures link precision/recall, ambiguity safety and abstention separately.
It is a synthetic stress test, not population accuracy or a stock-return test.
"""
import argparse
from collections import Counter
import hashlib
import json
import subprocess
from pathlib import Path
from types import SimpleNamespace

from src.entity_resolution import VERSION,surface
from src.stock_keywords import KeywordIndex,compile_keywords

ROOT=Path(__file__).resolve().parents[1]


def profile(name,ticker,claims):
    facts=[]
    for ident,predicate,value,kind,quote in claims:
        facts.append({'id':ident,'subject':ticker,'predicate':predicate,'object':value,'object_type':kind,
                      'status':'asserted','qualifiers':{},'confidence':'synthetic_explicit',
                      'evidence':[{'source_id':'source','start':0,'end':len(quote),'quote':quote}]})
    graph={'ticker':ticker,'facts':facts,'sources':[{'id':'source','filing_date':'2026-01-01','accession':'synthetic',
        'section':'synthetic business','document':'synthetic.txt'}],'paragraphs':[],'coverage':{}}
    bundle=compile_keywords(graph,ticker=ticker,as_of='2026-04-01')
    return SimpleNamespace(ticker=ticker,name=name,keyword_records=bundle['records'],keyword_evidence=bundle['evidence'])


def build_fixture(variant):
    person='John Alan McDonald' if variant in ('middle','same_initial') else 'John McDonald'
    claims=[('john','has_officer',person,'PERSON',person+' is the chief executive officer of Acme Industries.'),
            ('apple','buys_from','Apple','ORG','We purchase computer hardware from Apple.'),
            ('cloud','uses_input','cloud','concept','We use cloud computing for storage and software infrastructure.'),
            ('chip','uses_input','chips','concept','We use semiconductor chips from silicon foundries and processor suppliers.')]
    if variant=='same_surname':claims.append(('jane','has_officer','Jane McDonald','PERSON','Jane McDonald is the chief financial officer of Acme Industries.'))
    if variant=='same_initial':claims.append(('john2','has_officer','John David McDonald','PERSON','John David McDonald is a director of Acme Industries.'))
    profiles={'ACME':profile('Acme Industries Inc.','ACME',claims),
              'AAPL':profile('Apple Inc.','AAPL',[]),'MCD':profile("McDonald's Corporation",'MCD',[])}
    if variant=='same_name':profiles['OTHER']=profile('Other Dynamics Inc.','OTHER',[
        ('otherjohn','has_officer','John McDonald','PERSON','John McDonald is chief executive officer of Other Dynamics.')])
    return KeywordIndex(profiles)


def target_rows(comparison,case):
    keyword=case.get('keyword','John McDonald' if case['category']=='person' else 'Apple')
    def target(row):
        if row['stock']!='ACME':return False
        if case['category']=='person':return row['keyword'].startswith('John ') and row['relationship']=='officer'
        return row['keyword']==keyword and row['relationship']==('supplier' if case['category']=='organization' else 'input')
    return {key:[r for r in comparison[key] if target(r)] for key in ('matches','unresolved_matches','rejected_matches')}


def evaluate_case(index,case,annotation=None,comparison=None):
    if comparison is None:comparison=index.compare(case['text'],entity_annotations=annotation)
    rows=target_rows(comparison,case)
    matched=bool(rows['matches'])
    state='match' if matched else 'unresolved' if rows['unresolved_matches'] else 'rejected' if rows['rejected_matches'] else 'not_retrieved'
    expected=case['expected']
    return {**case,'predicted':state,'link_correct':matched==(expected=='match'),
            'strict_correct':state=={'match':'match','different':'rejected','ambiguous':'unresolved'}[expected],
            'candidate_count':sum(map(len,rows.values())),
            'candidates':[{'keyword':r['keyword'],'identity':r['identity']['id'],
                           'span':r['article_span'],'alignment':r['alignment']} for group in rows.values() for r in group],
            'mentions':annotation['mentions'] if annotation else []}


def summarize(rows):
    gold=Counter(r['expected'] for r in rows)
    tp=sum(r['expected']=='match' and r['predicted']=='match' for r in rows)
    fp=sum(r['expected']!='match' and r['predicted']=='match' for r in rows)
    fn=gold['match']-tp
    def ratio(a,b):return round(a/b,6) if b else None
    return {'cases':len(rows),'gold':dict(gold),'true_links':tp,'false_links':fp,'missed_links':fn,
            'precision':ratio(tp,tp+fp),'recall':ratio(tp,gold['match']),
            'link_accuracy':ratio(sum(r['link_correct'] for r in rows),len(rows)),
            'strict_three_way_accuracy':ratio(sum(r['strict_correct'] for r in rows),len(rows)),
            'false_links_on_different_entities':sum(r['expected']=='different' and r['predicted']=='match' for r in rows),
            'unsafe_links_on_ambiguous_cases':sum(r['expected']=='ambiguous' and r['predicted']=='match' for r in rows),
            'abstentions':sum(r['predicted'] in ('unresolved','not_retrieved') for r in rows),
            'no_candidate_cases':sum(r['predicted']=='not_retrieved' for r in rows)}


def source_probes(cases,nlp):
    from src.filing_relationships import extract_sections
    from src.stock_evidence import compile_board
    results=[]
    for case in cases:
        section={'text':case['text'],'kind':'business','source':'synthetic SEC-style probe','form':'10-K',
                 'filing_date':'2026-01-01','accession':'synthetic-'+case['id'],'section':'Item 1','document':'synthetic.txt'}
        board=compile_board([{'ticker':'ACME','name':'Acme Industries Inc.','industry':'Industrial','filing_sections':[section]}])
        graph=extract_sections([section],ticker='ACME',name='Acme Industries Inc.',nlp=nlp)
        bundle=compile_keywords(graph,ticker='ACME',as_of='2026-04-01')
        p=board.profiles['ACME'];p.keyword_records=bundle['records'];p.keyword_evidence=bundle['evidence']
        KeywordIndex(board.profiles)
        wanted={surface(o) for o in case['objects']}
        found=[r for r in p.keyword_records if r['predicate']==case['predicate'] and surface(r['keyword']) in wanted]
        recovered=any(r['identity']['type']==case['type'] and r['assertion']==case['assertion'] for r in found)
        results.append({**case,'recovered':recovered,'candidate_rows':found,
                        'all_extracted':[{'keyword':r['keyword'],'predicate':r['predicate'],'type':r['identity']['type'],
                                          'assertion':r['assertion']} for r in p.keyword_records]})
    return results


def main(argv=None):
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--cases',type=Path,default=ROOT/'eval/entity-alignment-stress-v1.json')
    p.add_argument('--out',type=Path,required=True)
    p.add_argument('--spacy',action='store_true')
    p.add_argument('--source-revision',required=True)
    a=p.parse_args(argv)
    raw=a.cases.read_bytes();data=json.loads(raw);cases=data['cases']
    assert len(cases)==80 and len({c['id'] for c in cases})==80
    assert Counter(c['expected'] for c in cases)=={'match':30,'different':30,'ambiguous':20}
    fixtures={v:build_fixture(v) for v in {c.get('variant','single') for c in cases}}
    modes={'deterministic':[]}
    for case in cases:modes['deterministic'].append(evaluate_case(fixtures[case.get('variant','single')],case))
    browser=subprocess.run(['node',str(ROOT/'scripts/evaluate_entity_alignment_browser.mjs')],
        input=json.dumps({'fixtures':{v:i.browser_payload() for v,i in fixtures.items()},'cases':cases}),
        text=True,capture_output=True,check=True,timeout=60)
    browser_results=json.loads(browser.stdout)
    modes['browser']=[evaluate_case(None,c,comparison=browser_results[c['id']]) for c in cases]
    parity=all(a['predicted']==b['predicted'] and a['candidates']==b['candidates']
               for a,b in zip(modes['deterministic'],modes['browser']))
    if not parity:raise ValueError('Browser and Python disagree on frozen stress cases')
    probes=[]
    parser=None
    if a.spacy:
        from src.filing_relationships import load_parser,SPACY_VERSION,MODEL,MODEL_VERSION
        from scripts.annotate_article_entities import annotate
        nlp=load_parser();parser={'spacy':SPACY_VERSION,'model':MODEL,'model_version':MODEL_VERSION}
        modes['spacy']=[]
        for case,doc in zip(cases,nlp.pipe((c['text'] for c in cases),batch_size=32)):
            modes['spacy'].append(evaluate_case(fixtures[case.get('variant','single')],case,annotate(doc,case['text'])))
        probes=source_probes(data['source_probes'],nlp)
    report={'schema':'entity-alignment-evaluation-1','source_revision':a.source_revision,'alignment_version':VERSION,
            'cases_sha256':hashlib.sha256(raw).hexdigest(),'parser':parser,
            'case_labels_sha256':hashlib.sha256(json.dumps(cases,sort_keys=True,separators=(',',':')).encode()).hexdigest(),
            'source_sha256':{name:hashlib.sha256((ROOT/name).read_bytes()).hexdigest() for name in
                ('src/entity_resolution.py','src/stock_keywords.py','src/evidence_matcher.py','dashboard/scanner-backtest/index.html')},
            'browser_python_parity':{'cases':len(cases),'passed':parity},
            'limitations':data['description'],
            'metrics':{mode:summarize(rows) for mode,rows in modes.items()},
            'by_category':{mode:{category:summarize([r for r in rows if r['category']==category])
                                for category in sorted({r['category'] for r in rows})} for mode,rows in modes.items()},
            'results':modes,'source_probes':probes,
            'source_probe_recovery':{'recovered':sum(r['recovered'] for r in probes),'expected':len(probes),
                                    'scope':'Targeted relationship/type/assertion recovery; gold targets are not an exhaustive precision label set.'}}
    a.out.parent.mkdir(parents=True,exist_ok=True)
    a.out.write_text(json.dumps(report,indent=2,ensure_ascii=False)+'\n')
    print(json.dumps({'metrics':report['metrics'],'by_category':report['by_category'],
                      'source_probe_recovery':report['source_probe_recovery']},indent=2))


if __name__=='__main__':main()
