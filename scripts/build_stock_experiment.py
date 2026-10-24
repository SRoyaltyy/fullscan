"""Build an explicitly experimental stock-only view from complete stored texts."""
import argparse,gzip,hashlib,json,os,time
from pathlib import Path
from src.filing_evidence import load_filing_sections
from src.filing_relationships import extract_sections,load_parser
from src.stock_evidence_graph import VERSION

STOCKS={
 'SDGR':('0001490978','Schrodinger, Inc.'), 'UNIT':('0002020795','Uniti Group Inc.'),
 'IREN':('0001878848','IREN Limited'), 'OLLI':('0001639300',"Ollie's Bargain Outlet Holdings, Inc."),
 'MIRM':('0001759425','Mirum Pharmaceuticals, Inc.'),
 'SLDE':('0001886428','Slide Insurance Holdings, Inc.'),
 'LZ':('0001286139','LegalZoom.com, Inc.'), 'LRCX':('0000707549','Lam Research Corporation')}

def main():
 p=argparse.ArgumentParser();p.add_argument('--ticker',choices=STOCKS,required=True);p.add_argument('--objects',type=Path,required=True);p.add_argument('--out',type=Path,required=True);a=p.parse_args()
 cik,name=STOCKS[a.ticker];sections,receipt=load_filing_sections(a.objects,strict=True,ciks={cik})
 selected=next(iter(sections.values()));assert selected
 started=time.monotonic();g=extract_sections(selected,ticker=a.ticker,name=name,nlp=load_parser())
 evidence=g['stock_evidence'];assert evidence['schema']==VERSION
 # Include a real source excerpt for every occurrence, even when classification abstains.
 paragraphs={}
 for row in g['paragraphs']:paragraphs.setdefault(row['source_id'],[]).append(row)
 for o in evidence['occurrences']:
  for row in paragraphs.get(o['source_id'],[]):
   if row['start']<=o['start']<row['end']:
    text=row['text'];relative=o['start']-row['start'];lo=max(0,relative-300);hi=min(len(text),relative+len(o['text'])+500)
    o['excerpt']=text[lo:hi];o['excerpt_start']=row['start']+lo;break
 result={'experimental':True,'accuracy_certified':False,'source_sha':os.environ['CANDIDATE_SHA'],'ticker':a.ticker,'name':name,'cik':cik,'method':VERSION,'sources':g['sources'],'source_receipt':receipt,'parser_coverage':g['coverage'],'evidence':evidence,'elapsed_seconds':round(time.monotonic()-started,2)}
 a.out.mkdir(parents=True,exist_ok=True);raw=json.dumps(result,ensure_ascii=False,separators=(',',':')).encode();blob=gzip.compress(raw,mtime=0)
 (a.out/(a.ticker+'.json.gz')).write_bytes(blob)
 summary={k:result[k] for k in ('ticker','name','source_sha','method','source_receipt','elapsed_seconds')}
 summary.update(entities=len(evidence['entities']),assertions=len(evidence['assertions']),asset=a.ticker+'.json.gz',sha256=hashlib.sha256(blob).hexdigest(),bytes=len(blob))
 (a.out/(a.ticker+'.summary.json')).write_text(json.dumps(summary,indent=2)+'\n');print(json.dumps(summary),flush=True)

if __name__=='__main__':main()
