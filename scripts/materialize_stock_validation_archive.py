"""Read only selected issuers' full texts using the existing range ZIP reader.

Runs inside GitHub Actions with its normal read-only Actions token. The host
never needs a credential or to download a multi-gigabyte source ZIP in full.
"""
import argparse,json,re,zipfile
from pathlib import Path
from scripts.materialize_filing_texts import RangeReader,http_fetcher,signed_artifact_url,materialize


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--draw',type=Path,required=True);p.add_argument('--release',type=Path,required=True)
    p.add_argument('--out',type=Path,required=True);p.add_argument('--repository',default='SRoyaltyy/fullscan')
    p.add_argument('--as-of')
    a=p.parse_args();draw=json.loads(a.draw.read_text());release=json.loads(a.release.read_text())
    requested={e['issuer_id']:e for e in draw['issuers']};covered=set();reports=[]
    for shard in release['source_artifacts']:
        remote=RangeReader(http_fetcher(signed_artifact_url(a.repository,shard['id'])))
        with zipfile.ZipFile(remote) as archive:
            ciks={re.fullmatch(r'objects/company-filings/full-v1/(\d{10})/company.json',n).group(1)
                  for n in archive.namelist() if re.fullmatch(r'objects/company-filings/full-v1/\d{10}/company.json',n)}
            wanted=set(requested)&ciks
            if wanted:
                # Resolve archive ticker names by CIK, rather than assuming an
                # old source release uses the current catalog ticker alias.
                names=set()
                for cik in wanted:
                    company=json.loads(archive.read('objects/company-filings/full-v1/'+cik+'/company.json'))
                    names.update(company['tickers'])
                report=materialize(archive,a.out,as_of=a.as_of,tickers=names)
                covered.update(wanted);reports.append({'artifact_id':shard['id'],**report})
                print(json.dumps({'shard':shard['shard'],'selected_issuers':len(wanted),'downloaded_bytes':remote.bytes_received}),flush=True)
        if covered==set(requested):break
    if covered!=set(requested):raise ValueError('full source manifests missing for CIKs: '+','.join(sorted(set(requested)-covered)))
    a.out.mkdir(parents=True,exist_ok=True)
    (a.out/'selection-report.json').write_text(json.dumps({'draw_id':draw['id'],'covered_ciks':sorted(covered),'reports':reports},indent=2)+'\n')


if __name__=='__main__':main()
