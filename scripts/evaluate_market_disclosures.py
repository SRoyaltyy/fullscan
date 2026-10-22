"""Measure named legal entities and issuer attachment on a fixed sector sample.

Gold labels are source-based, model-authored annotations, not human certification.
The sampling frame is the published retained paragraphs, not undisclosed law or
unretained prose. A passing score applies only to this stated benchmark scope.
"""
import argparse
from collections import Counter, defaultdict
import gzip
import hashlib
import json
from pathlib import Path
import urllib.request

from src.specific_filing_records import details, validate_details, source_fingerprint

ROOT=Path(__file__).resolve().parents[1]
CATEGORIES={'law','authority','named_case','named_permit'}
ATTACHMENT={'issuer_disclosure':'issuer','issuer_context':'issuer',
    'potential_or_conditional':'conditional','issuer_risk_context':'context',
    'mention_only':'mention','third_party_context':'third_party','explicitly_out_of_scope':'excluded'}


def score(cases, predicted, threshold=.90):
    counts=Counter();failures=[];sectors=defaultdict(Counter)
    for case in cases:
        expected=case['expected'];rows=predicted[case['id']];used=set();errors=[]
        counts['cases']+=1;counts['gold_entities']+=len(expected);counts['predicted_entities']+=len(rows)
        for gold in expected:
            choices=[(i,r) for i,r in enumerate(rows) if i not in used and r['category']==gold['category']
                and gold['span'] in r['spans']]
            if not choices:
                errors.append({'missing':gold});continue
            i,row=choices[0];used.add(i);counts['true_positives']+=1
            correct=row['attachment']==gold['attachment'] and row['direct']==gold['direct']
            counts['correct_attachments']+=correct
            if not correct:errors.append({'gold':gold,'observed':row})
        counts['false_positives']+=len(rows)-len(used)
        counts['false_negatives']+=len(expected)-len(used)
        if len(used)!=len(rows):errors.extend({'unexpected':r} for i,r in enumerate(rows) if i not in used)
        counts['exact_cases']+=not errors
        sectors[case['sector']]['cases']+=1;sectors[case['sector']]['exact_cases']+=not errors
        if errors:failures.append({'id':case['id'],'errors':errors})
    metrics={'precision':counts['true_positives']/max(1,counts['predicted_entities']),
        'recall':counts['true_positives']/max(1,counts['gold_entities']),
        'attachment_accuracy':counts['correct_attachments']/max(1,counts['true_positives']),
        'exact_case_accuracy':counts['exact_cases']/max(1,counts['cases'])}
    return {'threshold':threshold,'passed':all(v>=threshold for v in metrics.values()),
        'metrics':metrics,'counts':dict(counts),'sectors':dict(sectors),'failures':failures}


def evaluate(benchmark,cache,*,offline=False):
    cache=Path(cache);cache.mkdir(parents=True,exist_ok=True)
    predicted={}
    groups=defaultdict(list)
    for case in benchmark['cases']:groups[case['ticker']].append(case)
    for ticker,cases in groups.items():
        entry=benchmark['profiles'][ticker];path=cache/(entry['sha256']+'.json.gz')
        if not path.is_file():
            if offline:raise ValueError('benchmark source missing in offline mode: '+ticker)
            with urllib.request.urlopen(benchmark['asset_base_url']+entry['path'],timeout=90) as stream:raw=stream.read()
            if hashlib.sha256(raw).hexdigest()!=entry['sha256']:raise ValueError('benchmark source checksum mismatch')
            path.write_bytes(raw)
        raw=path.read_bytes()
        if hashlib.sha256(raw).hexdigest()!=entry['sha256']:raise ValueError('benchmark cache checksum mismatch')
        graph=json.loads(gzip.decompress(raw));paragraphs={p['id']:p for p in graph['paragraphs']}
        extracted=details(graph,paragraph_scope='published_review_paragraphs')
        validate_details(graph,extracted)
        for case in cases:
            p=paragraphs[case['paragraph_id']];start,end=case['source_span']
            if p['source_id']!=case['source']['id'] or p['text'][start-p['start']:end-p['start']]!=case['text']:
                raise ValueError('benchmark annotation differs from the exact source: '+case['id'])
            rows=[]
            for r in extracted['records']:
                if r['category'] not in CATEGORIES or r['source_id']!=case['source']['id']:continue
                fields=[r['evidence']['anchor'],*r['alias_evidence']]
                spans=sorted({(f['start']-start,f['end']-start) for f in fields if start<=f['start']<f['end']<=end})
                if spans:rows.append({'entity':r['entity'],'category':r['category'],'spans':[list(s) for s in spans],
                    'attachment':ATTACHMENT[r['applicability']],'direct':r['direct_obligation_asserted']})
            predicted[case['id']]=rows
    result=score(benchmark['cases'],predicted,benchmark['threshold'])
    result.update(schema='market-disclosure-evaluation-1',extractor_source_sha256=source_fingerprint(),
        issuers=len(groups),sectors_tested=len({c['sector'] for c in benchmark['cases']}),
        annotation=benchmark['annotation'],scope=benchmark['scope'],
        market_wide_accuracy_established=False,full_business_extraction_accuracy_established=False)
    return result


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--benchmark',type=Path,default=ROOT/'eval/market-disclosure-benchmark.json')
    p.add_argument('--cache',type=Path,default=ROOT/'.cache/disclosure-benchmark')
    p.add_argument('--out',type=Path,required=True)
    p.add_argument('--offline',action='store_true')
    p.add_argument('--require-pass',action='store_true')
    a=p.parse_args();raw=a.benchmark.read_bytes()
    result=evaluate(json.loads(raw),a.cache,offline=a.offline)
    result['benchmark_sha256']=hashlib.sha256(raw).hexdigest()
    a.out.parent.mkdir(parents=True,exist_ok=True);a.out.write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps({k:v for k,v in result.items() if k not in ('failures','sectors')},sort_keys=True))
    return int(a.require_pass and not result['passed'])


if __name__=='__main__':raise SystemExit(main())
