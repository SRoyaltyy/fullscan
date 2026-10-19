import gzip,hashlib,io,json,tempfile,unittest,zipfile
from unittest.mock import patch
from pathlib import Path
from scripts.materialize_filing_texts import RangeReader,materialize,safe_member,http_fetcher

class TextMaterializationTests(unittest.TestCase):
 def test_range_reader_supports_zip_without_reading_unrelated_payload(self):
  stream=io.BytesIO()
  with zipfile.ZipFile(stream,'w',compression=zipfile.ZIP_STORED) as z:
   z.writestr('huge-original',b'x'*1000000);z.writestr('text','complete text')
  raw=stream.getvalue();calls=[]
  def fetch(a,b):calls.append((a,b));return raw[a:b+1],len(raw)
  reader=RangeReader(fetch,block_size=1024,cache_blocks=4)
  with zipfile.ZipFile(reader) as z:self.assertEqual(z.read('text'),b'complete text')
  self.assertLess(reader.bytes_received,20000)
 def test_changing_range_response_is_rejected(self):
  reader=RangeReader(lambda a,b:(b'x',10),block_size=4)
  with self.assertRaisesRegex(ValueError,'inconsistent'):reader.read(4)
 def test_paths_cannot_escape_destination(self):
  for name in ('../x','/x','objects/../x','C:/x','objects\\x'):
   with self.assertRaises(ValueError):safe_member(name)
 def test_storage_requests_do_not_forward_github_credentials(self):
  seen=[]
  class Response:
   status=206;headers={'Content-Range':'bytes 0-0/10'}
   def __enter__(self):return self
   def __exit__(self,*args):pass
   def read(self,n):return b'x'
  def open_request(request,**kwargs):seen.extend(request.header_items());return Response()
  with patch('urllib.request.urlopen',open_request),patch.dict('os.environ',{'GH_TOKEN':'dummy-secret'}):
   self.assertEqual(http_fetcher('https://example.blob.core.windows.net/data')(0,0),(b'x',10))
  self.assertFalse(any(k.lower()=='authorization' for k,v in seen))
 def fixture(self,wrong_hash=False):
  stream=io.BytesIO();key='company-filings/full-v1/0000000001/acc/text/v1/text.json.gz'
  raw=json.dumps({'a.htm':{'text':'Whole text'}}).encode();compressed=gzip.compress(raw)
  manifest={'text_complete':True,'texts':{'key':key,'sha256':'bad' if wrong_hash else hashlib.sha256(raw).hexdigest(),'object':{'sha256':hashlib.sha256(compressed).hexdigest()}}}
  company={'expected_filings':1,'failures':[],'tickers':['OWN'],'filings':[{'filing_date':'2026-01-01','text_complete':True,'manifest_key':'company-filings/full-v1/0000000001/acc/manifest.json'}]}
  with zipfile.ZipFile(stream,'w') as z:
   z.writestr('objects/company-filings/full-v1/0000000001/company.json',json.dumps(company))
   z.writestr('objects/company-filings/full-v1/0000000001/acc/manifest.json',json.dumps(manifest))
   z.writestr('objects/'+key,compressed);z.writestr('objects/originals/do-not-copy',b'original')
  stream.seek(0);return zipfile.ZipFile(stream)
 def test_complete_text_copied_and_verified_without_original(self):
  with self.fixture() as z,tempfile.TemporaryDirectory(dir=Path.cwd()) as d:
   r=materialize(z,d,as_of='2026-10-19')
   self.assertEqual(r['filings'],1);self.assertTrue(r['text_objects_verified'])
   self.assertFalse((Path(d)/'objects/originals').exists())
 def test_bad_hash_fails(self):
  with self.fixture(wrong_hash=True) as z,tempfile.TemporaryDirectory(dir=Path.cwd()) as d:
   with self.assertRaisesRegex(ValueError,'checksum'):materialize(z,d)
 def test_explicit_probe_cannot_succeed_with_missing_ticker(self):
  with self.fixture() as z,tempfile.TemporaryDirectory(dir=Path.cwd()) as d:
   with self.assertRaisesRegex(ValueError,'missing from source'):materialize(z,d,tickers=['MISSING'])

if __name__=='__main__':unittest.main()
