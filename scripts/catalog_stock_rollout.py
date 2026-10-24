"""Resumable catalog-wide candidate rollout with a pinned served baseline.

Output counts and differences are not accuracy scores. Full filing text is
verified against the stored archive; description-only stocks stay explicit.
"""
import argparse,base64,csv,gzip,hashlib,io,json,os,re,shutil,subprocess,tempfile,time,zipfile
from bisect import bisect_right
from pathlib import Path
from scripts.materialize_filing_texts import RangeReader,http_fetcher,signed_artifact_url,materialize
from scripts.materialize_completed_profiles import fetch_url

REPO='SRoyaltyy/fullscan'
BASELINE='7adb09dd5091fb51ac68859645a26e2292951e46'
OLD_ASSETS='https://raw.githubusercontent.com/'+REPO+'/ecd210bc5eb1930bbf9b6e7a3020e9947abe4f7c/dashboard/scanner-backtest/'
OLD_INDEX_SHA='40762da4e701e358b6fc5249d3c7f46782078609dfd811cc527c236633628ce8'
REVIEW_URL='https://raw.githubusercontent.com/'+REPO+'/d9cc8d142050dd7229989a269b410ed3c1fe2efb/dashboard/scanner-backtest/completed/manifest.json'
AS_OF='2026-10-20'

def sha(b):return hashlib.sha256(b).hexdigest()
def write(path,obj):
 path=Path(path);path.parent.mkdir(parents=True,exist_ok=True);path.write_text(json.dumps(obj,ensure_ascii=False,separators=(',',':'))+'\n')
def remote_zip(artifact):
 url=signed_artifact_url(REPO,artifact)
 def fetch(start,end):
  nonlocal url
  for attempt in range(4):
   try:return http_fetcher(url)(start,end)
   except RuntimeError:
    if attempt==3:raise
    time.sleep(2**attempt);url=signed_artifact_url(REPO,artifact)
 return zipfile.ZipFile(RangeReader(fetch))

def plan(out,release,source_sha):
 from src.filing_relationships import compiler_fingerprint
 assert release['complete'] and release['coverage']['expected_issuers']==2026
 raw=fetch_url(OLD_ASSETS+'profiles.json');assert sha(raw)==OLD_INDEX_SHA
 old=json.loads(raw);review=json.loads(fetch_url(REVIEW_URL));assert len(old)==5996 and len(review['profiles'])==2032
 csvraw=fetch_url('https://raw.githubusercontent.com/'+REPO+'/'+BASELINE+'/excel_bot/data/finviz_with_descriptions.csv')
 descriptions={}
 for r in csv.DictReader(io.StringIO(csvraw.decode('utf-8-sig'))):
  ticker=str(r.get('Ticker') or r.get('ticker') or '').upper().strip()
  if ticker:descriptions.setdefault(ticker,r)
 ciks={e['cik'] for e in review['profiles'].values()};assert len(ciks)==2026
 chunks=[];seen=set()
 for shard in release['source_artifacts']:
  with remote_zip(shard['id']) as z:
   present=sorted({m.group(1) for n in z.namelist() if (m:=re.fullmatch(r'objects/company-filings/full-v1/(\d{10})/company.json',n))})
  assert not seen.intersection(present);seen.update(present)
  for i in range(0,len(present),16):chunks.append({'id':'filings-'+str(shard['shard'])+'-'+str(i//16),'artifact_id':shard['id'],'ciks':present[i:i+16],'kind':'filings'})
  print(json.dumps({'planned_shard':shard['shard'],'issuers':len(present)}),flush=True)
 assert seen==ciks
 description_tickers=sorted(set(old)-set(review['profiles']));assert len(description_tickers)==3964
 # Separate short-description work into bounded, independently resumable jobs.
 for i in range(0,len(description_tickers),400):chunks.append({'id':'descriptions-'+str(i//400),'tickers':description_tickers[i:i+400],'kind':'description_only'})
 assert len(chunks)<=256
 catalog={}
 for t,s in old.items():
  d=descriptions.get(t,{})
  catalog[t]={**s,'ticker':t,'description':d.get('Finviz_Description') or d.get('description') or '', 'coverage':'filing_backed' if t in review['profiles'] else 'description_only'}
  if t in review['profiles']:catalog[t]['cik']=review['profiles'][t]['cik']
 for chunk in chunks:
  chunk['branch']='scanner-rollout-'+source_sha[:12]+'-'+chunk['id']
  for t,row in catalog.items():
   if (chunk['kind']=='filings' and row.get('cik') in chunk['ciks']) or (chunk['kind']=='description_only' and t in chunk['tickers']):row['batch']=chunk['id'];row['candidate_branch']=chunk['branch']
 assert all('batch' in row for row in catalog.values())
 result={'schema':'stock-catalog-comparison-1','source_sha':source_sha,'compiler_sha256':compiler_fingerprint(),'baseline_source_sha':BASELINE,'baseline_asset_base':OLD_ASSETS,'baseline_index_sha256':OLD_INDEX_SHA,'as_of':AS_OF,'expected_stocks':5996,'expected_filing_tickers':2032,'expected_issuers':2026,'expected_description_only':3964,'description_csv_sha256':sha(csvraw),'archive_release':release,'review':review,'catalog':catalog,'batches':chunks,'accuracy_certified':False}
 write(out/'plan.json',result)
 with open(os.environ.get('GITHUB_OUTPUT',os.devnull),'a') as f:f.write('matrix='+json.dumps({'include':[{'id':c['id']} for c in chunks]},separators=(',',':'))+'\n')
 return result

def git(*args,env=None,data=None):return subprocess.check_output(['git',*args],text=True,input=data,env=env).strip()
def auth_env():
 env=dict(os.environ);token=env.get('GH_TOKEN') or env.get('GITHUB_TOKEN');assert token
 env.update(GIT_CONFIG_COUNT='1',GIT_CONFIG_KEY_0='http.https://github.com/.extraheader',GIT_CONFIG_VALUE_0='AUTHORIZATION: basic '+base64.b64encode(('x-access-token:'+token).encode()).decode())
 env.update(GIT_AUTHOR_NAME='Stock catalog publisher',GIT_AUTHOR_EMAIL='bot@users.noreply.github.com',GIT_COMMITTER_NAME='Stock catalog publisher',GIT_COMMITTER_EMAIL='bot@users.noreply.github.com')
 return env
def retain(directory,branch):
 """A separate data tree preserves checkpoints without checking out large assets."""
 env=auth_env();prior=git('ls-remote','--heads','origin','refs/heads/'+branch,env=env);parent=None
 if prior:git('fetch','--depth=1','origin',branch,env=env);parent=git('rev-parse','FETCH_HEAD')
 with tempfile.TemporaryDirectory() as temp:
  env['GIT_INDEX_FILE']=str(Path(temp)/'index');git('read-tree','--empty',env=env)
  paths=sorted(p for p in directory.rglob('*') if p.is_file());assert all(not p.is_symlink() and p.stat().st_size<99_000_000 for p in paths)
  blobs=git('hash-object','-w','--stdin-paths',data=''.join(str(p.resolve())+'\n' for p in paths),env=env).splitlines()
  git('update-index','--index-info',data=''.join('100644 '+b+'\t'+p.relative_to(directory).as_posix()+'\n' for b,p in zip(blobs,paths)),env=env)
  tree=git('write-tree',env=env);args=['commit-tree',tree]
  if parent:args+=['-p',parent]
  commit=git(*args,'-m','Retain stock rollout checkpoint '+branch,env=env);git('push','origin',commit+':refs/heads/'+branch,env=env)
 return commit

def payload(graph,receipt,ticker,name,cik,plan,coverage):
 from src.stock_evidence_graph import VERSION
 e=graph['stock_evidence'];assert e['schema']==VERSION
 by_source={}
 for p in graph['paragraphs']:by_source.setdefault(p['source_id'],[]).append(p)
 for rows in by_source.values():rows.sort(key=lambda x:x['start'])
 texts={sid:''.join(p['text'] for p in rows) for sid,rows in by_source.items()}
 for o in e['occurrences']:
  text=texts[o['source_id']];assert text[o['start']:o['end']]==o['text']
  lo=max(0,o['start']-300);hi=min(len(text),o['end']+500);o['excerpt']=text[lo:hi];o['excerpt_start']=lo
 return {'source_sha':plan['source_sha'],'baseline_source_sha':BASELINE,'ticker':ticker,'name':name,'cik':cik,'method':VERSION,'coverage_kind':coverage,'as_of':AS_OF,'source_receipt':receipt,'sources':graph['sources'],'parser_coverage':graph['coverage'],'evidence':e,'experimental':True,'accuracy_certified':False}

def batch(plan,ident,out):
 from src.filing_evidence import load_filing_sections
 from src.filing_relationships import extract_sections,load_parser,compiler_fingerprint
 chunk=next(c for c in plan['batches'] if c['id']==ident);out.mkdir(parents=True,exist_ok=True);index_path=out/'index.json'
 index={'source_sha':plan['source_sha'],'compiler_sha256':compiler_fingerprint(),'batch':ident,'complete':False,'profiles':{}}
 try:
  old=json.loads(fetch_url('https://raw.githubusercontent.com/'+REPO+'/'+chunk['branch']+'/index.json'))
  assert old['source_sha']==plan['source_sha'] and old['compiler_sha256']==compiler_fingerprint()
  for ticker,r in old['profiles'].items():
   target=out/r['asset'];target.parent.mkdir(parents=True,exist_ok=True)
   if not target.exists():target.write_bytes(fetch_url('https://raw.githubusercontent.com/'+REPO+'/'+chunk['branch']+'/'+r['asset']))
   assert sha(target.read_bytes())==r['sha256'];index['profiles'][ticker]=r
 except __import__('urllib.error',fromlist=['HTTPError']).HTTPError as error:
  if error.code!=404:raise
 units=[];nlp=None
 for unit in chunk.get('ciks') or chunk['tickers']:
  groups={}
  for t,r in plan['catalog'].items():
   if (r.get('cik')==unit if chunk['kind']=='filings' else t==unit):groups.setdefault((r['name'],r['description']),[]).append(t)
  for aliases in groups.values():units.append((unit,sorted(aliases)))
 for unit,aliases in units:
  assert aliases
  if all(t in index['profiles'] for t in aliases):continue
  if nlp is None:nlp=load_parser()
  ticker=aliases[0];row=plan['catalog'][ticker];started=time.monotonic()
  baseline_raw=fetch_url(OLD_ASSETS+row['profile_path']);assert sha(baseline_raw)==row['profile_sha256']
  baseline_payload=json.loads(gzip.decompress(baseline_raw))
  old_descriptions=[s['sha256'] for s in baseline_payload['sources'] if s.get('source')=='finviz' and s.get('section')=='description']
  assert old_descriptions==([sha(row['description'].encode())] if row['description'] else []),'old/new descriptions differ'
  with tempfile.TemporaryDirectory() as tmp:
   if chunk['kind']=='filings':
    with remote_zip(chunk['artifact_id']) as z:
     company=json.loads(z.read('objects/company-filings/full-v1/'+unit+'/company.json'))
     materialize(z,Path(tmp),as_of=AS_OF,tickers=set(company['tickers']))
    sections,receipt=load_filing_sections(Path(tmp)/'objects',as_of=AS_OF,strict=True,ciks={unit});parts=next(iter(sections.values()))
    # The old review preserves the old full-source coverage receipt, even though
    # its public evidence projection includes witness paragraphs only.
    ref=plan['review']['profiles'][ticker];raw=fetch_url(plan['review']['asset_base_url']+ref['path']);assert sha(raw)==ref['sha256'];old=gzip.decompress(raw);old=json.loads(old)
    assert old['coverage']['characters_read']==receipt['characters'],'old/new stored text coverage differs'
   else:
    text=row['description'];parts=[{'source':'catalog_description','kind':'business','section':'stored description','document':'catalog description','form':'DESCRIPTION','filing_date':'','complete':False,'text':text}]
    receipt={'companies':1,'filings':0,'documents':1 if text else 0,'characters':len(text),'full_source_archive':False,'complete':False,'description_only':True,'description_sha256':sha(text.encode()),'description_csv_sha256':plan['description_csv_sha256']}
   if chunk['kind']=='filings' and row['description']:
    parts.append({'source':'catalog_description','kind':'business','section':'stored description','document':'catalog description','form':'DESCRIPTION','filing_date':'','complete':False,'text':row['description']})
   receipt.update(description_characters=len(row['description']),description_sha256=sha(row['description'].encode()),description_csv_sha256=plan['description_csv_sha256'])
   graph=extract_sections(parts,ticker=ticker,name=row['name'],nlp=nlp)
   result=payload(graph,receipt,ticker,row['name'],unit if chunk['kind']=='filings' else None,plan,'filing_backed' if chunk['kind']=='filings' else 'description_only')
   blob=gzip.compress(json.dumps(result,ensure_ascii=False,separators=(',',':')).encode(),mtime=0);asset='profiles/'+sha((unit+'\0'+row['name']+'\0'+row['description']).encode())+'.json.gz';target=out/asset;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(blob)
   for t in aliases:index['profiles'][t]={'asset':asset,'sha256':sha(blob),'canonical_ticker':ticker,'coverage':result['coverage_kind'],'entities':len(result['evidence']['entities']),'assertions':len(result['evidence']['assertions']),'source_receipt':receipt}
   print(json.dumps({'batch':ident,'ticker':ticker,'aliases':aliases,'elapsed':round(time.monotonic()-started,2),'complete_stocks':len(index['profiles'])}),flush=True)
  del graph,result
  write(index_path,index)
  # Every filing issuer is durable; descriptions checkpoint after each 100 stocks.
  if chunk['kind']=='filings' or len(index['profiles'])%100==0:retain(out,chunk['branch'])
 assert set(index['profiles'])=={t for t,r in plan['catalog'].items() if r['batch']==ident}
 index['complete']=True;write(index_path,index);commit=retain(out,chunk['branch'])
 return {'id':ident,'commit':commit,'stocks':len(index['profiles']),'complete':True}

def assemble(plan,out,complete=False):
 """Never mark the catalog complete from partial batches or stale parser data."""
 public={k:v for k,v in plan.items() if k not in ('archive_release','review')}
 ready={};batch_info=[]
 refs=git('ls-remote','--heads','origin','refs/heads/scanner-rollout-'+plan['source_sha'][:12]+'-*',env=auth_env())
 heads={line.split()[1].removeprefix('refs/heads/'):line.split()[0] for line in refs.splitlines()}
 for c in plan['batches']:
  index=None
  commit=heads.get(c['branch'])
  try:
   if commit:index=json.loads(fetch_url('https://raw.githubusercontent.com/'+REPO+'/'+commit+'/index.json'))
  except __import__('urllib.error',fromlist=['HTTPError']).HTTPError as error:
   if error.code!=404:raise
  if index:
   assert index['source_sha']==plan['source_sha'] and index['compiler_sha256']==plan['compiler_sha256']
   for t,r in index['profiles'].items():
    assert t in plan['catalog'] and plan['catalog'][t]['batch']==c['id'] and t not in ready
    assert r['coverage']==plan['catalog'][t]['coverage']
    ready[t]={**r,'url':'https://raw.githubusercontent.com/'+REPO+'/'+commit+'/'+r['asset']}
  batch_info.append({'id':c['id'],'branch':c['branch'],'complete':bool(index and index['complete']),'stocks_ready':len(index['profiles']) if index else 0})
 filing_count=sum(plan['catalog'][t]['coverage']=='filing_backed' for t in ready);description_count=sum(plan['catalog'][t]['coverage']=='description_only' for t in ready)
 all_done=len(ready)==5996 and filing_count==2032 and description_count==3964 and all(c['complete'] for c in batch_info)
 if complete:assert all_done,'catalog rebuild incomplete'
 public.update(complete=all_done,ready_stocks=len(ready),ready_filing_tickers=filing_count,ready_description_only=description_count,profiles=ready,batches=batch_info)
 for row in public['catalog'].values():row.pop('description',None)
 write(out/'catalog.json',public)
 return public

def main():
 p=argparse.ArgumentParser();p.add_argument('command',choices=['plan','batch','assemble','site']);p.add_argument('--plan',type=Path);p.add_argument('--out',type=Path,required=True);p.add_argument('--batch');p.add_argument('--require-complete',action='store_true');a=p.parse_args()
 if a.command=='plan':plan(a.out,json.loads(Path('data/company_filings_full_release.json').read_text()),os.environ['CANDIDATE_SHA'])
 else:
  frozen=json.loads(a.plan.read_text())
  if a.command=='batch':print(json.dumps(batch(frozen,a.batch,a.out)))
  else:
   result=assemble(frozen,a.out,a.require_complete)
   if a.command=='site':
    for name in ('index.html','comparison.mjs'):shutil.copyfile(Path('dashboard/stock-comparison')/name,a.out/name)
    # A stable subpage survives unrelated legacy snapshot publishers.
    (a.out/'compare').mkdir(exist_ok=True)
    for name in ('index.html','comparison.mjs','catalog.json'):shutil.copyfile(a.out/name,a.out/'compare'/name)
   print(json.dumps({k:v for k,v in result.items() if k in ('complete','ready_stocks','ready_filing_tickers','ready_description_only')}))

if __name__=='__main__':main()
