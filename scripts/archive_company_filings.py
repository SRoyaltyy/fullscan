#!/usr/bin/env python3
"""Archive whole dated submissions for the pinned, audited liquid universe."""
from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import pathlib
import shutil
import sys
import threading

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from src.company_filings.full_archive import (ARCHIVE_VERSION, LocalObjects, R2Objects,
    WholeClient, archive_company, filing_inventory, json_bytes, r2_configured, timestamp,
    read_original, archive_url)


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--universe', type=pathlib.Path, default=ROOT/'data/company_filings_universe_2026-10-16.json')
    p.add_argument('--output', type=pathlib.Path, default=ROOT/'data/company_filings_full')
    p.add_argument('--ticker', action='append')
    p.add_argument('--shard', type=int, default=0)
    p.add_argument('--shards', type=int, default=1)
    p.add_argument('--workers', type=int, default=2)
    p.add_argument('--per-sec', type=float, default=1.5)
    p.add_argument('--storage', choices=['auto','local','r2'], default='auto')
    p.add_argument('--inventory-only', action='store_true')
    p.add_argument('--verify-binary-samples', action='store_true', help='Independently compare up to three embedded assets per issuer with their individual SEC URLs')
    args = p.parse_args()
    if not (0 <= args.shard < args.shards) or not (0 < args.per_sec <= 8): p.error('Invalid shard or request pace')
    universe = json.loads(args.universe.read_text(encoding='utf-8'))
    by_cik = {}
    for company in universe['companies']:
        if args.ticker and company['ticker'] not in args.ticker: continue
        row = by_cik.setdefault(company['cik'], {'cik': company['cik'], 'tickers': []})
        row['tickers'].append(company['ticker'])
    selected = [r for cik,r in sorted(by_cik.items()) if int(hashlib.sha256(cik.encode()).hexdigest(),16)%args.shards==args.shard]
    args.output.mkdir(parents=True, exist_ok=True)
    objects = R2Objects() if args.storage=='r2' or args.storage=='auto' and r2_configured() else LocalObjects(args.output/'objects')
    client = WholeClient(per_sec=args.per_sec)
    if not client.relay_url: raise SystemExit('SEC relay is required for this runner')
    results, lock = [], threading.Lock()
    expected_tickers = sorted(t for r in selected for t in r['tickers'])
    def report():
        done = {r['cik'] for r in results}
        value = {'archive_version': ARCHIVE_VERSION, 'generated_at': timestamp(),
            'universe_snapshot': universe['snapshot_date'], 'universe_source_run': universe['source_run'],
            'shard': args.shard, 'shards': args.shards, 'durable': objects.durable,
            'expected_issuers': len(selected), 'expected_tickers': expected_tickers,
            'processed_issuers': len(results), 'unprocessed_ciks': [r['cik'] for r in selected if r['cik'] not in done],
            'raw_complete_issuers': sum(bool(r.get('raw_complete')) for r in results),
            'text_complete_issuers': sum(bool(r.get('text_complete')) for r in results),
            'expected_filings': sum(r.get('expected_filings',0) for r in results),
            'raw_complete_filings': sum(r.get('raw_complete_filings',0) for r in results),
            'text_complete_filings': sum(r.get('text_complete_filings',0) for r in results),
            'files': sum(r.get('files',0) for r in results),
            'original_bytes': sum(r.get('original_bytes',0) for r in results),
            'cached_filings': sum(r.get('cached_filings',0) for r in results),
            'llm_calls': 0, 'companies': results}
        temp = args.output/'report.partial'
        temp.write_bytes(json_bytes(value));temp.replace(args.output/'report.json')
        return value
    def work(company):
        if not objects.durable and shutil.disk_usage(args.output).free < 4_000_000_000:
            return {**company, 'raw_complete':False,'text_complete':False,'failures':[{'error_type':'InsufficientDiskSpaceNoFilesTruncated'}]}
        try:
            if args.inventory_only:
                filings, inventory = filing_inventory(client, company['cik'])
                return {**company, 'inventory': inventory, 'expected_filings': len(filings), 'filings': filings,
                        'inventory_complete': bool(filings), 'raw_complete': False, 'text_complete': False}
            result=archive_company(company, client, objects)
            if args.verify_binary_samples:
                candidates=[]
                for filing in result['filings']:
                    manifest=objects.maybe_json(filing['manifest_key'])
                    candidates.extend((manifest,item) for item in manifest['files'] if item['encoding']=='uuencoded')
                samples=[candidates[i] for i in sorted({0,len(candidates)//2,len(candidates)-1})] if candidates else []
                checks=[]
                for manifest,item in samples:
                    original=client.whole_url(archive_url(company['cik'],manifest['accession'],item['filename']))
                    equal=original==read_original(objects,company['cik'],manifest['accession'],item['filename'])
                    checks.append({'accession':manifest['accession'],'filename':item['filename'],'matches_individual_sec_file':equal})
                    if not equal:
                        result['raw_complete']=False;result['text_complete']=False
                        result['failures'].append({'error_type':'DecodedAssetMismatch','accession':manifest['accession'],'filename':item['filename']})
                result['independent_binary_checks']=checks
            return result
        except Exception as exc: return {**company,'raw_complete':False,'text_complete':False,'failures':[{'error_type':type(exc).__name__}]}
    report()
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
        for future in concurrent.futures.as_completed([pool.submit(work,c) for c in selected]):
            result=future.result()
            with lock: results.append(result);snapshot=report()
            print(json.dumps({k:snapshot[k] for k in ['processed_issuers','expected_issuers','raw_complete_issuers','text_complete_issuers','original_bytes']}),flush=True)
    final=report()
    print(json.dumps({k:v for k,v in final.items() if k not in ('companies','expected_tickers')},indent=2),flush=True)
    passed = all(r.get('inventory_complete') for r in results) if args.inventory_only else all(r.get('raw_complete') and r.get('text_complete') for r in results)
    return 0 if len(results)==len(selected) and passed else 1


if __name__=='__main__': raise SystemExit(main())
