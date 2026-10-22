"""Check a frozen extractor on untouched issuer paragraphs; no applicability score.

The surface challenge is separate from the attachment-aware development test.
Neither model-authored benchmark establishes a full-system acceptance pass.
"""
import argparse
import hashlib
import json
from pathlib import Path
from .evaluate_market_disclosures import evaluate, ROOT
from src.specific_filing_records import source_fingerprint


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--benchmark',type=Path,default=ROOT/'eval/market-disclosure-fresh-benchmark.json')
    parser.add_argument('--cache',type=Path,default=ROOT/'.cache/disclosure-benchmark')
    parser.add_argument('--out',type=Path,required=True)
    parser.add_argument('--offline',action='store_true')
    args=parser.parse_args();raw=args.benchmark.read_bytes();benchmark=json.loads(raw)
    if benchmark['frozen_extractor_source_sha256']!=source_fingerprint():
        raise ValueError('This challenge is pinned to a frozen extractor; a modified method requires a separately reported development evaluation')
    for case in benchmark['cases']:
        for gold in case['expected']:
            start,end=gold['span']
            if case['text'][start:end]!=gold['text']:
                raise ValueError('Gold span differs from literal source')
            if any(gold is not other and start<other['span'][1] and other['span'][0]<end for other in case['expected']):
                raise ValueError('Gold spans overlap')
    measured=evaluate(benchmark,args.cache,offline=args.offline)
    # The shared evaluator also lists unmatched attachment labels. Scope was
    # deliberately unadjudicated here: retain only missed and extra surfaces.
    failures=[{'id':row['id'],'errors':[e for e in row['errors'] if 'missing' in e or 'unexpected' in e]}
              for row in measured['failures']]
    failures=[row for row in failures if row['errors']]
    report={'schema':'fresh-market-surface-evaluation-1','extractor_source_sha256':source_fingerprint(),
        'frozen_source_revision':benchmark['frozen_source_revision'],'benchmark_sha256':hashlib.sha256(raw).hexdigest(),
        'issuers':measured['issuers'],'sectors':measured['sectors_tested'],'cases':len(benchmark['cases']),
        'gold_entities':measured['counts']['gold_entities'],'predicted_entities':measured['counts']['predicted_entities'],
        'precision':measured['metrics']['precision'],'recall':measured['metrics']['recall'],
        'exact_surface_case_accuracy':1-len(failures)/len(benchmark['cases']),
        'market_wide_accuracy_established':False,'applicability_accuracy_assessed':False,
        'annotation':benchmark['annotation'],'scope':benchmark['scope'],'failures':failures}
    args.out.parent.mkdir(parents=True,exist_ok=True)
    args.out.write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps({k:v for k,v in report.items() if k not in ('failures','annotation','scope')}))
    return 0


if __name__=='__main__':raise SystemExit(main())
