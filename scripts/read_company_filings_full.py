#!/usr/bin/env python3
"""List filing dates or export an entire original/full-text view, without excerpts."""
import argparse,json,pathlib,sys

ROOT=pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from src.company_filings.full_archive import LocalObjects,R2Objects,PREFIX,read_filing,read_original
from src.company_filings.full_extract import _filename,file_bytes


def main():
    p=argparse.ArgumentParser(description=__doc__)
    identity=p.add_mutually_exclusive_group(required=True)
    identity.add_argument('--ticker');identity.add_argument('--cik')
    p.add_argument('--objects',type=pathlib.Path,default=ROOT/'data/company_filings_full/objects')
    p.add_argument('--r2',action='store_true')
    p.add_argument('--accession');p.add_argument('--filename')
    mode=p.add_mutually_exclusive_group()
    mode.add_argument('--original',action='store_true',help='Export original submission/file bytes instead of the full text view')
    mode.add_argument('--all-originals',action='store_true',help='Export every embedded file, the complete submission, and dates to a new directory')
    p.add_argument('--output',type=pathlib.Path)
    args=p.parse_args()
    cik=str(args.cik or '').zfill(10)
    if args.ticker:
        universe=json.loads((ROOT/'data/company_filings_universe_2026-10-16.json').read_text())
        found=next((r for r in universe['companies'] if r['ticker']==args.ticker.upper()),None)
        if not found:p.error('Ticker is not in the pinned universe')
        cik=found['cik']
    objects=R2Objects() if args.r2 else LocalObjects(args.objects)
    if not args.accession:
        company=objects.maybe_json(f'{PREFIX}/{cik}/company.json')
        if not company:p.error('Issuer is not present in this object store/shard')
        rows=[{k:f.get(k) for k in ('accession','form','filing_date','report_date','raw_complete','text_complete')} for f in company['filings']]
        print(json.dumps({'cik':cik,'tickers':company['tickers'],'filings':rows,'failures':company['failures']},indent=2))
        return
    if not args.output:p.error('Export requires --output; a whole filing may be hundreds of MB')
    if args.all_originals:
        if args.filename:p.error('--all-originals cannot be combined with --filename')
        manifest=objects.maybe_json(f'{PREFIX}/{cik}/{args.accession}/manifest.json')
        if not manifest:p.error('Accession is not present in this object store/shard')
        raw=read_original(objects,cik,args.accession)
        args.output.mkdir(parents=True,exist_ok=False)
        documents=args.output/'originals';documents.mkdir()
        for item in manifest['files']:
            (documents/_filename(item['filename'])).write_bytes(file_bytes(raw,item))
        (args.output/'complete-submission.txt').write_bytes(raw)
        (args.output/'manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2),encoding='utf-8')
        print(json.dumps({'cik':cik,'accession':args.accession,'output':str(args.output),'embedded_files':len(manifest['files']),'whole_document':True}))
        return
    if args.original:
        data=read_original(objects,cik,args.accession,args.filename)
    else:
        manifest,texts=read_filing(objects,cik,args.accession)
        if args.filename:
            if args.filename not in texts:p.error('Filename is not in this accession')
            data=texts[args.filename]['text'].encode('utf-8')
        else:data=json.dumps({'manifest':manifest,'documents':texts},ensure_ascii=False,indent=2).encode('utf-8')
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_bytes(data)
    print(json.dumps({'cik':cik,'accession':args.accession,'output':str(args.output),'bytes':len(data),'whole_document':True}))


if __name__=='__main__':main()
