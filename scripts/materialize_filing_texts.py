"""Build-time selective ZIP reader for the existing whole-filing artifacts.

GitHub credentials stay on api.github.com. Only company manifests, filing
manifests and their complete verified text objects are copied. Originals stay
in the source archive. This module is never imported by the scanner.
"""
import argparse,gzip,hashlib,io,json,os,re,shutil,urllib.error,urllib.parse,urllib.request,zipfile
from collections import OrderedDict
from pathlib import Path,PurePosixPath

class NoRedirect(urllib.request.HTTPRedirectHandler):
 def redirect_request(self,*args,**kwargs):return None

def signed_artifact_url(repository,artifact_id):
 if not re.fullmatch(r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+',repository):raise ValueError('invalid repository')
 token=os.environ.get('GH_TOKEN') or os.environ.get('GITHUB_TOKEN')
 if not token:raise ValueError('GitHub Actions token required')
 url=f'https://api.github.com/repos/{repository}/actions/artifacts/{int(artifact_id)}/zip'
 req=urllib.request.Request(url,headers={'Authorization':'Bearer '+token,'Accept':'application/vnd.github+json','X-GitHub-Api-Version':'2022-11-28'})
 try:
  with urllib.request.build_opener(NoRedirect).open(req,timeout=60):
   raise ValueError('expected signed artifact redirect')
 except urllib.error.HTTPError as exc:
  if exc.code!=302:raise RuntimeError('GitHub artifact API returned HTTP '+str(exc.code)) from None
  location=exc.headers.get('Location','')
 parts=urllib.parse.urlsplit(location)
 if parts.scheme!='https' or not (parts.hostname or '').endswith(('.blob.core.windows.net','.amazonaws.com','.githubusercontent.com')):
  raise ValueError('unexpected artifact download host')
 return location

class RangeReader(io.RawIOBase):
 """Seekable bounded-memory HTTP range reader; rejects ignored ranges."""
 def __init__(self,fetch,*,block_size=1024*1024,cache_blocks=16):
  self.fetch=fetch;self.block_size=block_size;self.cache_blocks=cache_blocks
  self.position=0;self.cache=OrderedDict();self.bytes_received=0
  data,total=fetch(0,0)
  if len(data)!=1 or total<1:raise ValueError('invalid range probe')
  self.size=total;self.bytes_received+=len(data)
 def readable(self):return True
 def seekable(self):return True
 def tell(self):return self.position
 def seek(self,offset,whence=io.SEEK_SET):
  value=offset if whence==io.SEEK_SET else self.position+offset if whence==io.SEEK_CUR else self.size+offset if whence==io.SEEK_END else -1
  if value<0:raise ValueError('invalid seek')
  self.position=value;return value
 def read(self,size=-1):
  remaining=max(0,self.size-self.position) if size<0 else min(size,max(0,self.size-self.position))
  parts=[]
  while remaining:
   block=self.position//self.block_size
   if block not in self.cache:
    start=block*self.block_size;end=min(self.size-1,start+self.block_size-1)
    data,total=self.fetch(start,end)
    if total!=self.size or len(data)!=end-start+1:raise ValueError('inconsistent range response')
    self.bytes_received+=len(data);self.cache[block]=data
    if len(self.cache)>self.cache_blocks:self.cache.popitem(last=False)
   self.cache.move_to_end(block)
   data=self.cache[block];offset=self.position%self.block_size;n=min(remaining,len(data)-offset)
   parts.append(data[offset:offset+n]);self.position+=n;remaining-=n
  return b''.join(parts)

def http_fetcher(url):
 def fetch(start,end):
  req=urllib.request.Request(url,headers={'Range':f'bytes={start}-{end}','Accept-Encoding':'identity'})
  try:
   with urllib.request.urlopen(req,timeout=60) as response:
    expected=re.fullmatch(r'bytes (\d+)-(\d+)/(\d+)',response.headers.get('Content-Range',''))
    if response.status!=206 or not expected or tuple(map(int,expected.groups()[:2]))!=(start,end):
     raise ValueError('artifact server did not honor requested range')
    data=response.read(end-start+2)
    if len(data)!=end-start+1:raise ValueError('artifact range length mismatch')
    return data,int(expected.group(3))
  except urllib.error.URLError:raise RuntimeError('artifact range request failed') from None
 return fetch

def safe_member(name):
 p=PurePosixPath(name)
 if '\\' in name or p.is_absolute() or '..' in p.parts or ':' in name:raise ValueError('unsafe archive member')
 return p

def copy_member(archive,name,destination):
 p=safe_member(name);target=destination.joinpath(*p.parts)
 target.parent.mkdir(parents=True,exist_ok=True)
 with archive.open(name) as source,target.open('wb') as output:shutil.copyfileobj(source,output)
 return target

def digest(path,*,uncompressed=False):
 h=hashlib.sha256()
 opener=gzip.open if uncompressed else open
 with opener(path,'rb') as stream:
  for block in iter(lambda:stream.read(1024*1024),b''):h.update(block)
 return h.hexdigest()

def materialize(archive,destination,*,as_of=None,tickers=None):
 destination=Path(destination).resolve();destination.mkdir(parents=True,exist_ok=True)
 members=set(archive.namelist());companies=filings=text_bytes=0;copied=set()
 company_names=sorted(n for n in members if re.fullmatch(r'objects/company-filings/full-v1/\d{10}/company.json',n))
 if not company_names:raise ValueError('no full-source company manifests')
 requested={t.upper() for t in tickers or []};covered=set()
 if requested and 'report.json' in members:
  inventory=json.loads(archive.read('report.json'))
  wanted_ciks={str(c['cik']).zfill(10) for c in inventory['companies'] if requested&set(c.get('tickers',[]))}
  company_names=[n for n in company_names if PurePosixPath(n).parent.name in wanted_ciks]
 for name in company_names:
  company=json.loads(archive.read(name))
  if requested and not requested&set(company.get('tickers',[])):continue
  covered.update(company.get('tickers',[]))
  if company.get('failures') or len(company.get('filings',[]))<company.get('expected_filings',0):raise ValueError('incomplete company source')
  copy_member(archive,name,destination);companies+=1
  for filing in company.get('filings',[]):
   date=str(filing.get('filing_date') or '')
   if as_of and (not date or date>as_of):continue
   if not filing.get('text_complete'):raise ValueError('incomplete filing text')
   manifest_name='objects/'+filing['manifest_key'];safe_member(manifest_name)
   manifest=json.loads(archive.read(manifest_name))
   if manifest.get('truncated') or not manifest.get('text_complete'):raise ValueError('incomplete filing manifest')
   copy_member(archive,manifest_name,destination)
   text=manifest['texts'];key='objects/'+text['key'];safe_member(key)
   if key not in copied:
    target=copy_member(archive,key,destination)
    if text.get('object',{}).get('sha256') and digest(target)!=text['object']['sha256']:raise ValueError('compressed text checksum mismatch')
    if digest(target,uncompressed=True)!=text['sha256']:raise ValueError('full text checksum mismatch')
    text_bytes+=target.stat().st_size;copied.add(key)
   filings+=1
 if requested-covered:raise ValueError('requested tickers missing from source artifact: '+','.join(sorted(requested-covered)))
 return {'companies':companies,'filings':filings,'text_objects':len(copied),'compressed_text_bytes':text_bytes,
  'requested_tickers':sorted(requested),'covered_tickers':sorted(covered),
  'as_of':as_of,'text_objects_verified':True,'originals_copied':False}

def main():
 p=argparse.ArgumentParser();p.add_argument('--repository',required=True);p.add_argument('--artifact-id',type=int,required=True)
 p.add_argument('--out',type=Path,required=True);p.add_argument('--as-of');p.add_argument('--ticker',action='append');a=p.parse_args()
 remote=RangeReader(http_fetcher(signed_artifact_url(a.repository,a.artifact_id)))
 with zipfile.ZipFile(remote) as archive:report=materialize(archive,a.out,as_of=a.as_of,tickers=a.ticker)
 report.update({'artifact_id':a.artifact_id,'artifact_bytes':remote.size,'downloaded_bytes':remote.bytes_received})
 (a.out/'text-materialization-report.json').write_text(json.dumps(report,indent=2),encoding='utf-8')
 print(json.dumps(report))

if __name__=='__main__':main()
