#!/usr/bin/env python3
"""Move preserved full-source artifacts to R2 without downloading SEC files again."""
import argparse,gzip,json,pathlib,sys

ROOT=pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from src.company_filings.full_archive import LocalObjects,R2Objects,PREFIX,_verify_existing,json_bytes,sha256


def migrate(source,destination):
    if not destination.durable:raise ValueError('Migration destination must be durable')
    records=[];verified_sources=set()
    # Commit each manifest only after originals and full text verify in R2.
    for path in sorted((source.root/PREFIX).glob('*/*/manifest.json')):
        manifest=json.loads(path.read_text(encoding='utf-8'))
        if not _verify_existing(source,manifest):raise ValueError('Unsupported source archive version')
        key=path.relative_to(source.root).as_posix()
        existing=destination.maybe_json(key)
        reused=bool(existing and existing.get('durable')
                    and existing.get('source',{}).get('sha256')==manifest['source']['sha256']
                    and existing.get('texts',{}).get('sha256')==manifest['texts']['sha256']
                    and _verify_existing(destination,existing))
        if not reused:
            for field in ('source','texts'):
                entry=manifest[field]
                storage=destination.put(entry['key'],source.get(entry['key']))
                entry['object']=storage
            if not _verify_existing(destination,manifest):raise ValueError('Destination verification failed')
            manifest['durable']=True
            destination.put(key,json_bytes(manifest))
        verified_sources.add(manifest['source']['key'])
        records.append({'cik':manifest['cik'],'accession':manifest['accession'],'reused':reused})
    downloads=0
    # Include original-only checkpoints whose reader failed. These must survive
    # artifact expiry too, without being mislabeled as parsed filings.
    for path in sorted((source.root/PREFIX).glob('*/*/download.json')):
        checkpoint=json.loads(path.read_text(encoding='utf-8'));entry=checkpoint['source']
        if entry['key'] not in verified_sources:
            compressed=source.get(entry['key']);raw=gzip.decompress(compressed)
            if sha256(raw)!=entry['sha256'] or len(raw)!=entry['byte_count']:raise ValueError('Downloaded source checksum mismatch')
            destination.put(entry['key'],compressed)
            if sha256(gzip.decompress(destination.get(entry['key'])))!=entry['sha256']:raise ValueError('Migrated original checksum mismatch')
        destination.put(path.relative_to(source.root).as_posix(),json_bytes(checkpoint))
        downloads+=1
    moved={(r['cik'],r['accession']) for r in records}
    for path in sorted((source.root/PREFIX).glob('*/company.json')):
        company=json.loads(path.read_text(encoding='utf-8'))
        if not all((company['cik'],f['accession']) in moved for f in company.get('filings',[])):
            raise ValueError('Company references an unmigrated filing')
        company['durable']=True
        destination.put(path.relative_to(source.root).as_posix(),json_bytes(company))
    return {'migrated_or_verified_filings':len(records),'preserved_original_checkpoints':downloads,'reused_filings':sum(r['reused'] for r in records),'sec_requests':0}


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--objects',type=pathlib.Path,required=True)
    args=p.parse_args()
    print(json.dumps(migrate(LocalObjects(args.objects),R2Objects()),indent=2))


if __name__=='__main__':main()
