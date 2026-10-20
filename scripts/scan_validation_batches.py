"""Public-input inference only; reuse boards, preserve each sample's batch."""
import argparse,json
from pathlib import Path
from src.correlation import build_board,link_articles
from src.link_correlation import load_stocks,load_articles,summarize
from src.names import NameIndex,catalog_from_csv
from src.aliases import profiles_from_csv
from src.filing_evidence import load_filing_sections

def write_batch(board,articles,path,engine,names,profiles,coverage=None):
 linked=link_articles(board,articles,engine=engine,names=names,profiles=profiles)
 payload={'summary':summarize(linked),'links':linked}
 if coverage:payload['identity_source_coverage']=coverage
 path.write_text(json.dumps(payload,ensure_ascii=False),encoding='utf-8')
 # Keep logs free of predicted tickers until private labels are frozen.
 print(json.dumps({'file':str(path),'cases':len(linked)}),flush=True)

def main():
 p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True);p.add_argument('--descriptions',type=Path,required=True);p.add_argument('--identity-filings',type=Path);p.add_argument('--as-of',default='2026-10-19');a=p.parse_args()
 stocks=load_stocks(a.descriptions,None,None,{})
 names=NameIndex(catalog_from_csv(a.descriptions));profiles=profiles_from_csv(a.descriptions)
 batches=[(a.root/sub,load_articles(a.root/sub/'headline-sample.json',0,headline_only=True)) for sub in ('','fresh','holdout','holdout4','holdout5','holdout6','holdout7','holdout8') if (a.root/sub/'headline-sample.json').exists()]
 for engine in ('evidence','legacy'):
  board=build_board(stocks,engine=engine)
  for directory,articles in batches:write_batch(board,articles,directory/(engine+'-headline-links.json'),engine,names,profiles)
 if a.identity_filings:
  sources,coverage=load_filing_sections(a.identity_filings,as_of=a.as_of,strict=True)
  covered=[];enhanced=[]
  for row in stocks:
   sections=sources.get(row['ticker'],[])
   if sections:covered.append(row['ticker'])
   enhanced.append({**row,'filing_sections':list(row.get('filing_sections') or [])+[{**s,'kind':'identity_source'} for s in sections]})
  coverage={**coverage,'scope':'Explicit glossary/legal identities only; Finviz business evidence unchanged','covered_tickers':sorted(covered),'catalog_tickers':len(stocks),'full_business_coverage':False}
  (a.root/'identity-overlay-coverage.json').write_text(json.dumps(coverage,indent=2))
  board=build_board(enhanced,engine='evidence')
  for directory,articles in batches:write_batch(board,articles,directory/'evidence-full-identity-headline-links.json','evidence',names,profiles,coverage)

if __name__=='__main__':main()
