#!/usr/bin/env python3
"""Verify all shard reports against the pinned issuer/ticker inventory."""
import argparse,json,pathlib

def verify(universe,reports,shards=12):
    expected={r['ticker']:r['cik'] for r in universe['companies']}
    seen_shards=set();seen_tickers={};seen_ciks=set();companies=[];errors=[]
    for report in reports:
        shard=report['shard']
        if shard in seen_shards:errors.append('duplicate_shard:'+str(shard))
        seen_shards.add(shard)
        if report['shards']!=shards:errors.append('shard_configuration_mismatch')
        if report['universe_snapshot']!=universe['snapshot_date']:errors.append('universe_snapshot_mismatch')
        if report['unprocessed_ciks']:errors.append('unprocessed_ciks_in_shard:'+str(shard))
        if report['processed_issuers']!=report['expected_issuers']:errors.append('issuer_count_mismatch_in_shard:'+str(shard))
        for company in report['companies']:
            companies.append(company)
            if company['cik'] in seen_ciks:errors.append('duplicate_issuer:'+company['cik'])
            seen_ciks.add(company['cik'])
            for ticker in company['tickers']:
                if ticker in seen_tickers:errors.append('duplicate_ticker:'+ticker)
                seen_tickers[ticker]=company['cik']
            if not company.get('raw_complete'):errors.append('originals_incomplete:'+company['cik'])
            if not company.get('text_complete'):errors.append('text_incomplete:'+company['cik'])
            filings=company.get('filings',[])
            actual=[r['accession'] for r in filings]
            selected=(company.get('inventory') or {}).get('expected_accessions',[])
            if not selected or len(actual)!=len(set(actual)) or set(actual)!=set(selected) or len(actual)!=company.get('expected_filings'):
                errors.append('filing_inventory_mismatch:'+company['cik'])
            if company.get('failures') or any(not r.get('raw_complete') or not r.get('text_complete') or not r.get('filing_date') for r in filings):
                errors.append('filing_incomplete_or_undated:'+company['cik'])
        for name in ('expected_filings','raw_complete_filings','text_complete_filings'):
            if report[name]!=sum(c.get(name,0) for c in report['companies']):errors.append('inconsistent_count:'+str(shard)+':'+name)
    if seen_shards!=set(range(shards)):errors.append('missing_shard_reports')
    missing=sorted(set(expected)-set(seen_tickers));extra=sorted(set(seen_tickers)-set(expected))
    wrong=[t for t in expected if t in seen_tickers and expected[t]!=seen_tickers[t]]
    if missing or extra or wrong:errors.append('ticker_membership_mismatch')
    return {'complete':not errors,'durable':bool(reports) and all(r['durable'] for r in reports),
        'universe_snapshot':universe['snapshot_date'],'expected_tickers':len(expected),
        'covered_tickers':len(seen_tickers),'expected_issuers':len(set(expected.values())),
        'covered_issuers':len({c['cik'] for c in companies}),'expected_filings':sum(r['expected_filings'] for r in reports),
        'raw_complete_filings':sum(r['raw_complete_filings'] for r in reports),
        'text_complete_filings':sum(r['text_complete_filings'] for r in reports),
        'files':sum(r['files'] for r in reports),'original_bytes':sum(r['original_bytes'] for r in reports),
        'cached_filings':sum(r['cached_filings'] for r in reports),'missing_tickers':missing,'extra_tickers':extra,
        'wrong_cik_tickers':wrong,'errors':errors,'llm_calls':sum(r['llm_calls'] for r in reports),'companies':companies}

def main():
    p=argparse.ArgumentParser();p.add_argument('--reports',type=pathlib.Path,required=True)
    p.add_argument('--universe',type=pathlib.Path,default=pathlib.Path('data/company_filings_universe_2026-10-16.json'))
    p.add_argument('--output',type=pathlib.Path,default=pathlib.Path('data/company_filings_full_report.json'));p.add_argument('--shards',type=int,default=12)
    args=p.parse_args();reports=[json.loads(f.read_text()) for f in args.reports.rglob('report.json')]
    result=verify(json.loads(args.universe.read_text()),reports,args.shards)
    args.output.parent.mkdir(parents=True,exist_ok=True);args.output.write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps({k:v for k,v in result.items() if k!='companies'},indent=2))
    return 0 if result['complete'] else 1

if __name__=='__main__':raise SystemExit(main())
