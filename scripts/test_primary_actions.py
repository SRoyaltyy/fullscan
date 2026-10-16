import json,os,pathlib,tempfile,unittest
from datetime import datetime,timezone,timedelta
from unittest.mock import patch
from src.primary_actions import HTTP_AUDIT,Ledger,collect,feed_rows,fetch_source,source_metadata,index_rows,allowed_public_source,get,OfficialRedirects,public_get_audit_ok

ASOF=datetime(2026,10,15,12,tzinfo=timezone.utc)
SOURCE={'id':'agency-rss','name':'Official Agency','jurisdiction':'US','family':'decision_documents','url':'https://example.gov/rss.xml','adapter':'rss','enabled':True}
RSS=b'<rss><channel><item><title>Agency issues Order 42</title><link>https://example.gov/orders/42?utm_source=rss</link><guid>42</guid><pubDate>Thu, 15 Oct 2026 10:00:00 GMT</pubDate></item></channel></rss>'

class PrimaryActionsTests(unittest.TestCase):
 def test_rss_and_atom_preserve_headline_issuer_link_and_date(self):
  r=feed_rows(RSS,SOURCE['url'])[0];self.assertEqual(r['title'],'Agency issues Order 42');self.assertEqual(r['url'],'https://example.gov/orders/42');self.assertEqual(r['published_at'],'2026-10-15T10:00:00+00:00')
  a=b'<feed xmlns="http://www.w3.org/2005/Atom"><entry><title>Court opinion</title><link href="/opinion/1"/><id>case-1</id><updated>2026-10-15T09:00:00Z</updated></entry></feed>'
  self.assertEqual(feed_rows(a,'https://court.gov/feed')[0]['url'],'https://court.gov/opinion/1')

 def test_html_is_not_mistaken_for_a_valid_empty_feed(self):
  with self.assertRaises(ValueError):feed_rows(b'<html><body>Cloud challenge</body></html>',SOURCE['url'])

 def test_proposal_and_enrolled_bill_do_not_become_enacted_policy(self):
  self.assertEqual(source_metadata(SOURCE,{'document_type':'Proposed Rule'})['document_stage'],'proposal_published')
  self.assertEqual(source_metadata(SOURCE|{'document_type':'enrolled_bill'},{})['document_stage'],'bill_enrolled_not_yet_law')
  self.assertIsNone(source_metadata(SOURCE,{'document_type':'Rule'})['concrete_public_action'])

 def test_document_dedup_persists_and_preserves_multiple_origins(self):
  with tempfile.TemporaryDirectory() as d:
   p=pathlib.Path(d)/'ledger.db';l=Ledger(p);row=feed_rows(RSS,SOURCE['url'])[0]
   a,f=l.retain(SOURCE,row,'first');self.assertTrue(f);l.db.commit();l.close();l=Ledger(p)
   b,f=l.retain(SOURCE,row,'second');self.assertFalse(f);self.assertEqual(a,b)
   c,f=l.retain(SOURCE|{'id':'other-portal'},row|{'native_id':'another-id'},'second');self.assertEqual(a,c);self.assertFalse(f)
   l.db.commit();saved=l.documents()[0];self.assertEqual(saved['source_ids'],['agency-rss','other-portal']);self.assertEqual(saved['first_seen'],'first');l.close()

 def test_source_scoped_native_id_is_stable_across_url_changes(self):
  with tempfile.TemporaryDirectory() as d:
   l=Ledger(pathlib.Path(d)/'l.db');row=feed_rows(RSS,SOURCE['url'])[0];a,_=l.retain(SOURCE,row,'one');b,f=l.retain(SOURCE,row|{'url':'https://example.gov/new/42'},'two')
   self.assertEqual(a,b);self.assertFalse(f);l.close()

 def test_collect_repeat_and_failures_are_visible_without_llm_calls(self):
  def fetch(url,**kw):return (403,b'',{},url) if 'blocked' in url else (200,RSS,{},url)
  with tempfile.TemporaryDirectory() as d:
   l=Ledger(pathlib.Path(d)/'l.db');sources={'sources':[SOURCE,SOURCE|{'id':'blocked','url':'https://blocked.gov/rss.xml'}]}
   first=collect(sources,l,pathlib.Path(d)/'out',fetch=fetch,asof=ASOF);second=collect(sources,l,pathlib.Path(d)/'out',fetch=fetch,asof=ASOF)
   self.assertEqual(first['counts']['new'],1);self.assertEqual(second['counts']['new'],0);self.assertEqual(second['counts']['duplicates'],1)
   self.assertEqual(first['sources_failed'],1);self.assertEqual(first['coverage_status'],'degraded');self.assertEqual(first['llm_calls'],0);l.close()

 def test_old_future_and_undated_do_not_become_today_action_headlines(self):
  b=b'<rss><channel>'+b''.join(b'<item><title>'+t+b'</title><link>https://example.gov/'+t+b'</link>'+p+b'</item>' for t,p in [(b'old',b'<pubDate>2020-01-01</pubDate>'),(b'future',b'<pubDate>2030-01-01</pubDate>'),(b'unknown',b'')])+b'</channel></rss>'
  with tempfile.TemporaryDirectory() as d:
   l=Ledger(pathlib.Path(d)/'l.db');r=collect({'sources':[SOURCE]},l,pathlib.Path(d)/'out',fetch=lambda u,**kw:(200,b,{},u),asof=ASOF)
   self.assertEqual(r['counts']['old'],1);self.assertEqual(r['counts']['future'],1);self.assertEqual(r['new_undated_review'],1);self.assertEqual(r['current_dated_documents'],0);l.close()

 def test_conditional_304_keeps_status_and_avoids_refetching_pages(self):
  calls=[]
  def fetch(url,**kw):calls.append(kw);return 304,b'',{},url
  rows,state=fetch_source(SOURCE,ASOF-timedelta(days=3),ASOF,{'status':'ok','etag':'public-tag','requests':4,'http_status':200},fetch)
  self.assertEqual(state['status'],'unchanged');self.assertEqual(state['requests'],1);self.assertEqual(calls[0]['headers'],{'If-None-Match':'public-tag'});self.assertFalse(rows)

 def test_structured_pagination_keeps_issuer_and_finality_metadata(self):
  one={'title':'Agency rule','document_number':'2026-1','html_url':'https://www.federalregister.gov/documents/2026/10/15/2026-1/x','publication_date':'2026-10-15','type':'Rule','agencies':[{'name':'Public Agency','id':7}]}
  pages=[{'results':[one],'next_page_url':'https://www.federalregister.gov/api/v1/documents.json?page=2','count':2},{'results':[one|{'document_number':'2026-2','type':'Proposed Rule'}],'count':2}]
  def fetch(url,**kw):return 200,json.dumps(pages.pop(0)).encode(),{},url
  r,h=fetch_source(SOURCE|{'adapter':'federal_register','url':'https://www.federalregister.gov/api/v1/documents.json'},ASOF-timedelta(days=3),ASOF,fetch=fetch)
  self.assertEqual(len(r),2);self.assertEqual(r[0]['issuer'],'Public Agency');self.assertEqual(r[1]['document_type'],'Proposed Rule');self.assertEqual(h['requests'],2)

 def test_api_zero_results_and_broken_html_selector_have_different_health(self):
  s=SOURCE|{'adapter':'govuk_search','document_type':'decision','url':'https://www.gov.uk/api/search.json'}
  r,h=fetch_source(s,ASOF-timedelta(days=3),ASOF,fetch=lambda u,**kw:(200,b'{"results":[],"total":0}',{},u));self.assertEqual(h['status'],'valid_empty')
  r,h=fetch_source(SOURCE|{'adapter':'html_index','link_pattern':'/orders/'},ASOF-timedelta(days=3),ASOF,fetch=lambda u,**kw:(200,b'<html><a href="/about">About us</a></html>',{},u));self.assertEqual(h['status'],'selector_empty')

 def test_telemetry_and_external_redirects_are_blocked_before_network(self):
  for u in ('https://dc.services.visualstudio.com/v2/track','https://example.gov.evil.com/feed'):
   self.assertFalse(allowed_public_source(u))
   with patch('urllib.request.build_opener') as opener:
    with self.assertRaises(ValueError):get(u)
    opener.assert_not_called()
  with self.assertRaises(ValueError):OfficialRedirects().redirect_request(None,None,302,'',{},'https://dc.services.visualstudio.com/v2/track')
  self.assertTrue(allowed_public_source('https://www.federalregister.gov/api/v1/documents.json'))

 def test_raw_agency_name_without_directory_id_is_supported(self):
  one={'title':'Agency rule','document_number':'2026-1','html_url':'https://www.federalregister.gov/documents/1','publication_date':'2026-10-15','type':'Rule','agencies':[{'raw_name':'Unresolved agency'}]}
  rows,h=fetch_source(SOURCE|{'adapter':'federal_register'},ASOF-timedelta(days=3),ASOF,fetch=lambda u,**kw:(200,json.dumps({'results':[one]}).encode(),{},u))
  self.assertEqual(rows[0]['issuer'],'Unresolved agency');self.assertEqual(rows[0]['issuer_ids'],[]);self.assertEqual(h['status'],'ok')

 def test_index_date_from_table_and_ofac_url(self):
  source=SOURCE|{'link_pattern':'/orders/courtorders/.*pdf$'}
  b=b'<table><tr><td>10/15/26</td><td><a href="/orders/courtorders/101526.pdf">Order list for October 15</a></td></tr></table>'
  self.assertEqual(index_rows(b,source)[0]['published_at'],'2026-10-15T00:00:00+00:00')
  source=SOURCE|{'link_pattern':'/recent-actions/[0-9]{8}','url':'https://ofac.treasury.gov/recent-actions'}
  self.assertEqual(index_rows(b'<a href="/recent-actions/20261015">Sanctions list update today</a>',source)[0]['published_at'],'2026-10-15T00:00:00+00:00')

 def test_revisions_keep_stable_identity_and_previous_payload(self):
  with tempfile.TemporaryDirectory() as d:
   l=Ledger(pathlib.Path(d)/'l.db');row=feed_rows(RSS,SOURCE['url'])[0]
   a,_=l.retain(SOURCE,row,'first');b,f=l.retain(SOURCE,row|{'title':'Revised order 42'},'second')
   self.assertEqual(a,b);self.assertFalse(f);self.assertEqual(l.documents()[0]['title'],'Revised order 42')
   old,new=l.db.execute('SELECT previous_payload,new_payload FROM revisions').fetchone();self.assertEqual(json.loads(old)['title'],row['title']);self.assertEqual(json.loads(new)['title'],'Revised order 42');l.close()

 def test_url_alias_dedup_across_sources_and_no_metadata_oscillation(self):
  with tempfile.TemporaryDirectory() as d:
   l=Ledger(pathlib.Path(d)/'l.db');row=feed_rows(RSS,SOURCE['url'])[0]
   a,_=l.retain(SOURCE,row,'first');new=row|{'url':'https://example.gov/new/42'};l.retain(SOURCE,new,'second')
   other=SOURCE|{'id':'other'};b,f=l.retain(other,new|{'title':'Other portal title','native_id':'other-id'},'third')
   self.assertEqual(a,b);self.assertFalse(f);l.retain(other,new|{'title':'Other portal title','native_id':'other-id'},'fourth')
   self.assertEqual(l.documents()[0]['title'],row['title']);self.assertEqual(l.db.execute('SELECT COUNT(*) FROM revisions').fetchone()[0],1);l.close()

 def test_reference_only_channels_excluded_but_policy_publications_kept(self):
  from build_primary_action_catalog import channel_scope
  self.assertTrue(channel_scope(SOURCE|{'name':'Euro foreign exchange reference rates'}));self.assertTrue(channel_scope(SOURCE|{'document_type':'farming_grant'}))
  self.assertIsNone(channel_scope(SOURCE|{'name':'Agency decisions','url':'https://example.gov/publications/decisions/rss.xml'}))

 def test_new_queue_does_not_repeat_and_disabled_sources_leave_current_view(self):
  with tempfile.TemporaryDirectory() as d:
   l=Ledger(pathlib.Path(d)/'l.db');out=pathlib.Path(d)/'out';fake=lambda u,**kw:(200,RSS,{},u)
   collect({'sources':[SOURCE]},l,out,fetch=fake,asof=ASOF);self.assertEqual(len(json.loads((out/'pending_headlines.json').read_text())),1)
   collect({'sources':[SOURCE]},l,out,fetch=fake,asof=ASOF);self.assertEqual(json.loads((out/'pending_headlines.json').read_text()),[]);self.assertEqual(len(json.loads((out/'current_documents.json').read_text())),1)
   r=collect({'sources':[SOURCE|{'enabled':False}]},l,out,fetch=fake,asof=ASOF);self.assertEqual(r['current_dated_documents'],0);self.assertEqual(len(l.documents()),1);l.close()

 def test_refresh_fingerprints_and_branch_aware_state_promotion(self):
  import hashlib
  from primary_action_ops import refresh_needed,previous_artifact
  with tempfile.TemporaryDirectory() as d:
   root=pathlib.Path(d)
   for p,c in [('config/primary_action_seeds.json','{}'),('src/primary_actions.py','core'),('scripts/build_primary_action_catalog.py','builder'),('data/primary_actions/institution_registry.json','{}')]:
    dest=root/p;dest.parent.mkdir(parents=True,exist_ok=True);dest.write_text(c)
   self.assertTrue(refresh_needed(root));(root/'config/primary_action_sources.json').write_text(json.dumps({'generated_at':datetime.now(timezone.utc).isoformat(),'seed_sha256':hashlib.sha256(b'{}').hexdigest(),'implementation_sha256':hashlib.sha256(b'corebuilder').hexdigest()}));self.assertFalse(refresh_needed(root))
   (root/'src/primary_actions.py').write_text('changed');self.assertTrue(refresh_needed(root))
  a={'name':'primary-actions-state','expired':False,'created_at':'2026-10-15','workflow_run':{'head_branch':'codex/primary-action-scanner-20261015'},'id':1}
  self.assertEqual(previous_artifact([a],'main')['id'],1);self.assertIsNone(previous_artifact([a],'unrelated'))
  main=a|{'id':2,'created_at':'2026-10-14','workflow_run':{'head_branch':'main'}};self.assertEqual(previous_artifact([a,main],'main')['id'],2)

 def test_hkma_documented_sort_and_pagination(self):
  calls=[];pages=[[{'title':'Adjustment of Base Rate','date':'2026-10-15','link':'https://www.hkma.gov.hk/eng/news/1/'}],[]]
  def fetch(url,**kw):calls.append(url);return 200,json.dumps({'header':{'success':True},'result':{'records':pages.pop(0)}}).encode(),{},url
  rows,h=fetch_source(SOURCE|{'adapter':'hkma_press','url':'https://api.hkma.gov.hk/public/press-releases','page_size':1},ASOF-timedelta(days=3),ASOF,fetch=fetch)
  self.assertEqual(rows[0]['issuer'],'Hong Kong Monetary Authority');self.assertEqual(h['requests'],2);self.assertIn('sortby=date',calls[0]);self.assertIn('offset=1',calls[1])

 def test_congress_bill_number_uses_act_name_as_headline(self):
  body=b'<rss><channel><item><title>S.2403</title><description>Retire through Ownership Act</description><link>https://www.congress.gov/bill/119th-congress/senate-bill/2403</link><guid>S.2403</guid></item></channel></rss>'
  row=feed_rows(body,'https://www.congress.gov/rss/presented-to-president.xml')[0]
  self.assertEqual(row['title'],'Retire through Ownership Act');self.assertIsNone(row['published_at']);self.assertEqual(row['native_id'],'S.2403')

 def test_treasury_auction_uses_announcement_pdf_not_the_json_index(self):
  blob=[{'cusip':'912797VX2','securityType':'Bill','securityTerm':'4-Week','announcementDate':'2026-10-13T00:00:00','auctionDate':'2026-10-15T00:00:00','offeringAmount':'110000000000','highYield':'','bidToCoverRatio':'','pdfFilenameAnnouncement':'A_20261013_1.pdf'}]
  rows,health=fetch_source(SOURCE|{'adapter':'treasury_announced','url':'https://www.treasurydirect.gov/TA_WS/securities/announced?format=json','document_type':'treasury_auction'},ASOF-timedelta(days=3),ASOF,fetch=lambda u,**kw:(200,json.dumps(blob).encode(),{},u))
  self.assertEqual(health['status'],'ok');self.assertEqual(rows[0]['url'],'https://www.treasurydirect.gov/instit/annceresult/press/preanre/2026/A_20261013_1.pdf')
  self.assertEqual(rows[0]['title'],'Treasury announces 4-Week Bill ($110 billion offering)');self.assertEqual(source_metadata(SOURCE,rows[0])['document_stage'],'fiscal_auction_announced')
  rows[0]['highYield']='4.1'
  resulted=dict(blob[0],highYield='4.100',bidToCoverRatio='2.61')
  rows,health=fetch_source(SOURCE|{'adapter':'treasury_announced','url':'https://www.treasurydirect.gov/TA_WS/securities/announced?format=json'},ASOF-timedelta(days=3),ASOF,fetch=lambda u,**kw:(200,json.dumps([resulted]).encode(),{},u))
  self.assertTrue(rows[0]['title'].startswith('Treasury auction result'));self.assertEqual(rows[0]['published_at'],'2026-10-15T00:00:00+00:00')

 def test_us_seed_feeds_replace_retired_doj_url(self):
  seeds=json.loads(pathlib.Path('config/primary_action_seeds.json').read_text())['sources']
  by={s['id']:s for s in seeds}
  self.assertNotIn('https://www.justice.gov/feeds/opa/justice-news.xml', by['us-doj']['feed_urls'])
  self.assertIn('https://www.justice.gov/news/rss', by['us-doj']['feed_urls'])
  self.assertIn('https://www.congress.gov/rss/house-floor-today.xml', by['us-congress']['feed_urls'])
  self.assertIn('https://www.congress.gov/rss/senate-floor-today.xml', by['us-congress']['feed_urls'])
  self.assertIn('https://www.ustr.gov/rss.xml', by['us-ustr']['feed_urls'])
  self.assertIn('https://www.cisa.gov/cybersecurity-advisories/all.xml', by['us-cisa']['feed_urls'])
  self.assertIn('https://www.federalreserve.gov/feeds/speeches.xml', by['us-fed-speeches']['feed_urls'])
  self.assertEqual(by['us-fed-speeches']['document_type'],'official_statement')
  self.assertIn('https://www.cbo.gov/publications/all/rss.xml', by['us-cbo']['feed_urls'])
  self.assertIn('https://apps.bea.gov/rss/rss.xml', by['us-bea']['feed_urls'])
  self.assertIn('https://www.census.gov/economic-indicators/indicator.xml', by['us-census']['feed_urls'])
  self.assertNotIn('https://www.doi.gov/rss/news.xml', by['us-doi']['feed_urls'])

 def test_public_inspection_and_openfda_keep_dated_actions_only(self):
  calls=[]
  one={'title':'Treasury notice','document_number':'2026-21105','html_url':'https://www.federalregister.gov/public-inspection/2026-21105/x','publication_date':'2026-10-16','type':'Notice','agencies':[{'name':'Treasury Department','id':497}],'pdf_url':'https://public-inspection.federalregister.gov/2026-21105.pdf'}
  def fetch(url,**kw):calls.append(url);return 200,json.dumps({'results':[one],'count':1}).encode(),{},url
  rows,health=fetch_source(SOURCE|{'adapter':'public_inspection','url':'https://www.federalregister.gov/api/v1/public-inspection-documents.json'},ASOF-timedelta(days=3),ASOF,fetch=fetch)
  self.assertIn('public-inspection-documents.json',calls[0]);self.assertNotIn('publication_date',calls[0]);self.assertEqual(rows[0]['native_id'],'2026-21105');self.assertEqual(health['status'],'ok')
  blob={'meta':{'results':{'total':1}},'results':[{'application_number':'ANDA091433','sponsor_name':'Hikma','submissions':[{'submission_status':'AP','submission_number':'1','submission_status_date':'20110923','application_docs':[{'url':'http://www.accessdata.fda.gov/old.pdf'}]},{'submission_status':'AP','submission_number':'2','submission_status_date':'20261014','application_docs':[{'url':'https://www.accessdata.fda.gov/bad file.pdf'},{'url':'https://www.accessdata.fda.gov/letter.pdf'}]},{'submission_status':'TA','submission_number':'3','submission_status_date':'20261014'}]}]}
  rows,health=fetch_source(SOURCE|{'adapter':'openfda_approvals','url':'https://api.fda.gov/drug/drugsfda.json'},ASOF-timedelta(days=3),ASOF,fetch=lambda u,**kw:(200,json.dumps(blob).encode(),{},u))
  self.assertEqual(len(rows),1);self.assertEqual(rows[0]['url'],'https://www.accessdata.fda.gov/letter.pdf');self.assertEqual(source_metadata(SOURCE,rows[0])['document_stage'],'approval_published')
  self.assertEqual(rows[0]['native_id'],'ANDA091433:2:20261014');self.assertEqual(rows[0]['published_raw'],'20261014')
  rows,health=fetch_source(SOURCE|{'adapter':'openfda_approvals','url':'https://api.fda.gov/drug/drugsfda.json'},ASOF-timedelta(days=3),ASOF,fetch=lambda u,**kw:(404,b'{"error":{"code":"NOT_FOUND","message":"No matches found!"}}',{},u))
  self.assertEqual(health['status'],'valid_empty');self.assertEqual(rows,[])

 def test_named_official_statements_stay_when_research_speeches_are_excluded(self):
  from build_primary_action_catalog import channel_scope
  speech={'name':'Federal Reserve speeches','url':'https://www.federalreserve.gov/feeds/speeches.xml','document_type':'official_statement'}
  self.assertIsNone(channel_scope(speech));self.assertTrue(channel_scope(dict(speech,document_type='official_announcement')))

 def test_public_feed_can_disable_unsupported_conditional_headers(self):
  calls=[]
  def fetch(url,**kw):calls.append(kw['headers']);return 200,RSS,{},url
  fetch_source(SOURCE|{'conditional_cache':False},ASOF-timedelta(days=3),ASOF,{'etag':'tag','last_modified':'old'},fetch)
  self.assertEqual(calls,[{}])

 def test_data_gov_key_parses_bills_dockets_and_laws_without_entering_the_url(self):
  with patch.dict(os.environ,{},clear=False):
   os.environ.pop('DATA_GOV_API_KEY',None)
   rows,health=fetch_source(SOURCE|{'adapter':'congress_bills','url':'https://api.congress.gov/v3/bill'},ASOF-timedelta(days=3),ASOF,fetch=lambda *a,**k:(_ for _ in ()).throw(AssertionError('fetched without a key')))
  self.assertEqual(rows,[]);self.assertEqual(health['status'],'key_not_configured');self.assertEqual(health['requests'],0)
  calls=[]
  def fetch(url,**kw):
   calls.append(url)
   self.assertNotIn('secret-value',url);self.assertNotIn('api_key',url)
   if 'api.congress.gov' in url and 'offset' not in url:
    body={'bills':[{'congress':119,'type':'HR','number':'1','title':'Example Act','latestAction':{'actionDate':'2026-10-14','text':'Became Public Law No: 119-1.'}},{'congress':119,'type':'S','number':'9','title':'Old bill','latestAction':{'actionDate':'2020-01-01','text':'Introduced'}},{'congress':119,'type':'S','number':'10','title':'Pending bill','latestAction':{'actionDate':'2026-10-15','text':'Presented to President.'}}],'pagination':{'next':'https://api.congress.gov/v3/bill?offset=250&api_key=secret-value&format=json'}}
   elif 'offset=250' in url:body={'bills':[]}
   elif 'api.regulations.gov' in url and 'page%5Bnumber%5D=2' not in url and 'page[number]=2' not in url:
    body={'data':[{'id':'EPA-HQ-OAR-2026-0001-0001','attributes':{'title':'Methane standards','documentType':'Proposed Rule','postedDate':'2026-10-14','agencyId':'EPA'}},{'id':'EPA-HQ-OAR-2026-0001-0002','attributes':{'title':'Supporting spreadsheet','documentType':'Supporting & Related Material','postedDate':'2026-10-14','agencyId':'EPA'}}],'meta':{'hasNextPage':True}}
   elif 'api.regulations.gov' in url:body={'data':[],'meta':{'hasNextPage':False}}
   elif 'offsetMark=abc' in url:body={'packages':[],'nextPage':None}
   else:body={'packages':[{'packageId':'PLAW-119publ1','title':'Example Act','dateIssued':'2026-10-14'}],'nextPage':'https://api.govinfo.gov/collections/PLAW/2026-10-12T00:00:00Z?offsetMark=abc&pageSize=100&api_key=secret-value'}
   return 200,json.dumps(body).encode(),{},url
  with patch.dict(os.environ,{'DATA_GOV_API_KEY':'secret-value'}):
   rows,health=fetch_source(SOURCE|{'adapter':'congress_bills','url':'https://api.congress.gov/v3/bill'},ASOF-timedelta(days=3),ASOF,fetch=fetch)
   self.assertEqual([r['native_id'] for r in rows],['119/HR/1','119/S/10'])
   self.assertEqual(rows[0]['url'],'https://www.congress.gov/bill/119th-congress/house-bill/1')
   self.assertEqual(source_metadata(SOURCE,rows[0])['document_stage'],'law_published')
   self.assertEqual(source_metadata(SOURCE,rows[1])['document_stage'],'bill_enrolled_not_yet_law')
   self.assertTrue(any('offset=250' in u and 'secret-value' not in u for u in calls))
   calls.clear()
   rows,health=fetch_source(SOURCE|{'adapter':'regulations_documents','url':'https://api.regulations.gov/v4/documents'},ASOF-timedelta(days=3),ASOF,fetch=fetch)
   self.assertEqual(len(rows),1);self.assertEqual(rows[0]['url'],'https://www.regulations.gov/document/EPA-HQ-OAR-2026-0001-0001');self.assertEqual(source_metadata(SOURCE,rows[0])['document_stage'],'proposal_published')
   calls.clear()
   rows,health=fetch_source(SOURCE|{'adapter':'govinfo_packages','url':'https://api.govinfo.gov/collections/PLAW'},ASOF-timedelta(days=3),ASOF,fetch=fetch)
   self.assertEqual(rows[0]['url'],'https://www.govinfo.gov/app/details/PLAW-119publ1');self.assertEqual(source_metadata(SOURCE,rows[0])['document_stage'],'law_published')
   self.assertTrue(any('offsetMark=abc' in u and 'api_key' not in u for u in calls))

 def test_api_key_header_is_audited_without_the_secret(self):
  captured={}
  class Resp:
   status=200;headers={};url='https://api.congress.gov/v3/bill?format=json'
   def read(self,n):return b'{}'
   def __enter__(self):return self
   def __exit__(self,*a):return False
  def open(req,timeout=None):
   captured['url']=req.full_url;captured['headers']=dict(req.header_items());return Resp()
  HTTP_AUDIT.clear()
  with patch.dict(os.environ,{'DATA_GOV_API_KEY':'secret-value'}):
   with patch('urllib.request.build_opener') as opener:
    opener.return_value.open=open
    get('https://api.congress.gov/v3/bill?format=json&api_key=secret-value')
   with self.assertRaises(ValueError):get('https://www.federalregister.gov/api/v1/documents.json',headers={'Authorization':'Bearer secret-value'})
  self.assertNotIn('secret-value',HTTP_AUDIT[0]['url']);self.assertNotIn('api_key',HTTP_AUDIT[0]['url']);self.assertTrue(HTTP_AUDIT[0]['authorization_sent'])
  self.assertNotIn('secret-value',captured['url']);self.assertEqual(captured['headers'].get('X-api-key'),'secret-value')
  self.assertTrue(public_get_audit_ok(HTTP_AUDIT[0]))
  self.assertFalse(public_get_audit_ok({'url':'https://api.congress.gov/v3/bill?api_key=secret-value','method':'GET','body_bytes':0,'authorization_sent':True}))
  self.assertFalse(public_get_audit_ok({'url':'https://www.federalregister.gov/documents','method':'GET','body_bytes':0,'authorization_sent':True}))

 def test_redirects_scope_credentials_to_the_original_https_origin(self):
  import urllib.request
  request=urllib.request.Request('https://api.congress.gov/v3/bill',headers={'X-Api-Key':'secret-value','If-None-Match':'private-etag'})
  HTTP_AUDIT.clear()
  same=OfficialRedirects().redirect_request(request,None,302,'Found',{},'https://api.congress.gov/v3/bill/?api_key=secret-value')
  self.assertEqual(same.get_header('X-api-key'),'secret-value');self.assertNotIn('secret-value',same.full_url)
  for target in ('https://www.congress.gov/','https://api.govinfo.gov/collections/PLAW','https://api.congress.gov:444/v3/bill'):
   redirected=OfficialRedirects().redirect_request(request,None,302,'Found',{},target)
   self.assertIsNone(redirected.get_header('X-api-key'));self.assertIsNone(redirected.get_header('If-none-match'))
  for target in ('http://api.congress.gov/v3/bill','https://user:secret-value@www.congress.gov/','https://outside.example/'):
   with self.assertRaises(ValueError):OfficialRedirects().redirect_request(request,None,302,'Found',{},target)
  self.assertEqual(len(HTTP_AUDIT),4);self.assertTrue(all(public_get_audit_ok(r) for r in HTTP_AUDIT))
  self.assertNotIn('secret-value',json.dumps(HTTP_AUDIT))

 def test_key_never_sent_over_http_or_to_unrelated_api_data_gov(self):
  for target in ('http://api.congress.gov/v3/bill','https://api.govinfo.gov:444/collections/PLAW'):
   with patch('urllib.request.build_opener') as opener:
    with self.assertRaises(ValueError):get(target)
    opener.assert_not_called()
  captured=[]
  class Resp:
   status=200;headers={};url='https://api.data.gov/'
   def read(self,n):return b'{}'
   def __enter__(self):return self
   def __exit__(self,*a):return False
  with patch.dict(os.environ,{'DATA_GOV_API_KEY':'secret-value'}),patch('urllib.request.build_opener') as opener:
   opener.return_value.open.side_effect=lambda req,**kw:(captured.append(dict(req.header_items())) or Resp())
   get('https://api.data.gov/')
  self.assertNotIn('X-api-key',captured[0])
  self.assertFalse(public_get_audit_ok({'url':'http://api.congress.gov/v3/bill','method':'GET','body_bytes':0,'authorization_sent':True}))

 def test_api_error_codes_and_retry_after_do_not_echo_server_messages(self):
  cases=[(403,'API_KEY_INVALID','key_invalid'),(403,'API_KEY_UNVERIFIED','key_unverified'),(403,'API_KEY_UNAUTHORIZED','key_unauthorized'),(403,'API_KEY_DISABLED','key_disabled'),(403,'API_KEY_MISSING','key_not_configured'),(429,'OVER_RATE_LIMIT','rate_limited')]
  with patch.dict(os.environ,{'DATA_GOV_API_KEY':'secret-value'}):
   for status,code,expected in cases:
    body=json.dumps({'error':{'code':code,'message':'echo secret-value'}}).encode()
    rows,state=fetch_source(SOURCE|{'adapter':'congress_bills','url':'https://api.congress.gov/v3/bill'},ASOF-timedelta(days=3),ASOF,fetch=lambda u,**kw:(status,body,{'Retry-After':'3600'},u))
    self.assertEqual(state['status'],expected);self.assertEqual(state['api_error_code'],code);self.assertEqual(state['retry_after_seconds'],3600)
    self.assertNotIn('secret-value',json.dumps(state))

 def test_http_error_retains_bounded_body_for_error_classification(self):
  import io,urllib.error
  body=b'{"error":{"code":"API_KEY_INVALID"}}'
  error=urllib.error.HTTPError('https://api.congress.gov/v3/bill',403,'Forbidden',{},io.BytesIO(body))
  with patch.dict(os.environ,{'DATA_GOV_API_KEY':'secret-value'}),patch('urllib.request.build_opener') as opener:
   opener.return_value.open.side_effect=error
   status,received,headers,url=get('https://api.congress.gov/v3/bill?api_key=secret-value')
  self.assertEqual(status,403);self.assertEqual(received,body);self.assertNotIn('secret-value',url)

 def test_fda_404_without_no_matches_error_is_a_failure(self):
  for body in (b'',b'<html>Not found</html>',b'{"error":{"code":"NOT_FOUND","message":"Unknown endpoint"}}'):
   rows,state=fetch_source(SOURCE|{'adapter':'openfda_approvals','url':'https://api.fda.gov/drug/drugsfda.json'},ASOF-timedelta(days=3),ASOF,fetch=lambda u,**kw:(404,body,{},u))
   self.assertEqual(state['status'],'fetch_failed')

 def test_credential_changes_refresh_catalog_and_key_errors_remain_retryable(self):
  from build_primary_action_catalog import verification_result
  from primary_action_ops import refresh_needed
  import hashlib
  with tempfile.TemporaryDirectory() as d,patch.dict(os.environ,{'DATA_GOV_API_KEY':''}):
   root=pathlib.Path(d)
   for path,value in [('src/primary_actions.py','core'),('scripts/build_primary_action_catalog.py','builder'),('config/primary_action_seeds.json','{}'),('data/primary_actions/institution_registry.json','{}')]:
    dest=root/path;dest.parent.mkdir(parents=True,exist_ok=True);dest.write_text(value)
   (root/'config/primary_action_sources.json').write_text(json.dumps({'generated_at':datetime.now(timezone.utc).isoformat(),'seed_sha256':hashlib.sha256(b'{}').hexdigest(),'implementation_sha256':hashlib.sha256(b'corebuilder').hexdigest(),'credential_configuration':{'data_gov_api_key_configured':False}}))
   self.assertFalse(refresh_needed(root))
   os.environ['DATA_GOV_API_KEY']='secret-value';self.assertTrue(refresh_needed(root))
  for status in ('key_not_configured','key_invalid','key_disabled','key_unverified','key_unauthorized','rate_limited'):
   row=verification_result(SOURCE|{'adapter':'congress_bills'},[],{'status':status},'checked')
   self.assertTrue(row['enabled']);self.assertFalse(row['verified_healthy']);self.assertEqual(row['verification']['status'],status)
  excluded=verification_result(SOURCE|{'adapter':'congress_bills','document_type':'farming_grant'},[],{'status':'key_not_configured'},'checked')
  self.assertFalse(excluded['enabled'])

 def test_406_conditional_request_recovers_with_a_plain_get(self):
  calls=[]
  def fetch(url,**kw):
   calls.append(kw.get('headers'))
   return (406,b'',{},url) if kw.get('headers') else (200,RSS,{},url)
  rows,state=fetch_source(SOURCE,ASOF-timedelta(days=3),ASOF,previous={'etag':'old'},fetch=fetch)
  self.assertEqual(calls,[{'If-None-Match':'old'},{}]);self.assertEqual(state['status'],'ok');self.assertEqual(state['requests'],2);self.assertEqual(len(rows),1)
  calls.clear()
  rows,state=fetch_source(SOURCE,ASOF-timedelta(days=3),ASOF,fetch=lambda u,**kw:(calls.append(kw) or (403,b'',{},u)))
  self.assertEqual(len(calls),1);self.assertEqual(state['status'],'blocked')

 def test_keyed_setup_failure_does_not_stop_public_collection_or_repeat_ids(self):
  keyed=SOURCE|{'id':'congress','adapter':'congress_bills','url':'https://api.congress.gov/v3/bill'}
  with tempfile.TemporaryDirectory() as d,patch.dict(os.environ,{'DATA_GOV_API_KEY':''}):
   ledger=Ledger(pathlib.Path(d)/'ledger.db');output=pathlib.Path(d)/'out'
   try:
    report=collect({'sources':[SOURCE,keyed]},ledger,output,fetch=lambda u,**kw:(200,RSS,{},u),asof=ASOF)
    self.assertEqual(report['sources_ok'],1);self.assertEqual(report['sources_failed'],1);self.assertEqual(report['sources'][1]['status'],'key_not_configured')
    self.assertEqual(report['llm_calls'],0);self.assertEqual(len(json.loads((output/'pending_headlines.json').read_text())),1)
    report=collect({'sources':[SOURCE,keyed]},ledger,output,fetch=lambda u,**kw:(200,RSS,{},u),asof=ASOF)
    self.assertEqual(report['counts']['new'],0);self.assertEqual(json.loads((output/'pending_headlines.json').read_text()),[])
    self.assertNotIn('secret-value',(output/'report.json').read_text())
   finally:ledger.close()

 def test_fed_plain_406_retries_once_with_rss_accept_and_audits_each_attempt(self):
  import io,urllib.error
  captured=[]
  class Resp:
   status=200;headers={};url='https://www.federalreserve.gov/feeds/press_orders.xml'
   def read(self,n):return RSS
   def __enter__(self):return self
   def __exit__(self,*args):return False
  def open_request(request,**kw):
   captured.append(dict(request.header_items()))
   if len(captured)==1:raise urllib.error.HTTPError(request.full_url,406,'Not Acceptable',{},io.BytesIO(b'Not Acceptable'))
   return Resp()
  HTTP_AUDIT.clear()
  with patch('urllib.request.build_opener') as opener,patch('src.primary_actions.time.sleep') as delay:
   opener.return_value.open.side_effect=open_request
   status,body,headers,url=get(Resp.url)
  self.assertEqual(status,200);self.assertEqual(len(captured),2);self.assertTrue(captured[1]['Accept'].startswith('application/rss+xml'))
  self.assertEqual(len(HTTP_AUDIT),2);self.assertTrue(all(public_get_audit_ok(r) for r in HTTP_AUDIT));delay.assert_called_once_with(1)
  with patch('urllib.request.build_opener') as opener,patch('src.primary_actions.time.sleep'):
   opener.return_value.open.side_effect=lambda req,**kw:(_ for _ in ()).throw(urllib.error.HTTPError(req.full_url,406,'Not Acceptable',{},io.BytesIO(b'no')))
   status,_,_,_=get(Resp.url);self.assertEqual(status,406);self.assertEqual(opener.return_value.open.call_count,2)

 def test_duplicate_source_ids_fail_before_collection(self):
  with tempfile.TemporaryDirectory() as d:
   ledger=Ledger(pathlib.Path(d)/'ledger.db')
   try:
    with self.assertRaisesRegex(ValueError,'Duplicate source ID'):
     collect({'sources':[SOURCE,SOURCE|{'adapter':'congress_bills'}]},ledger,pathlib.Path(d)/'out',fetch=lambda *a,**kw:self.fail('Network must not be called'),asof=ASOF)
   finally:ledger.close()

 def test_catalog_keeps_govinfo_api_and_rss_ids_distinct_and_explicit_feed_types(self):
  import build_primary_action_catalog as builder
  seeds=[{'id':'us-fed','name':'Federal Reserve Board','jurisdiction':'US','family':'monetary','discovery_url':'https://www.federalreserve.gov/feeds/feeds.htm'}, {'id':'us-fed-speeches','name':'Federal Reserve speeches','jurisdiction':'US','family':'official_statements','document_type':'official_statement','discovery_url':'https://www.federalreserve.gov/feeds/feeds.htm','feed_urls':['https://www.federalreserve.gov/feeds/speeches.xml']}]
  def read_url(url):
   if url==builder.DIRECTORIES['us_agencies']:return b'[]',url
   if url==builder.DIRECTORIES['uk_organisations']:return b'{"pages":1,"total":0,"results":[]}',url
   if url==builder.DIRECTORIES['us_court_feeds']:return b'<a href="https://www.govinfo.gov/rss/plaw.xml">Public Laws</a>',url
   return b'<html></html>',url
  def get_page(url,**kw):return 200,b'<link rel="alternate" type="application/rss+xml" href="https://www.federalreserve.gov/feeds/speeches.xml">',{},url
  with tempfile.TemporaryDirectory() as d:
   root=pathlib.Path(d)
   for name,text in [('config/primary_action_seeds.json',json.dumps({'sources':seeds})),('src/primary_actions.py','core'),('scripts/build_primary_action_catalog.py','builder')]:
    path=root/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_text(text)
   with patch.object(builder,'read_url',side_effect=read_url),patch.object(builder,'get',side_effect=get_page),patch.object(builder,'fetch_source',return_value=([],{'status':'valid_empty','requests':1})):
    builder.build(root)
   catalog=json.loads((root/'config/primary_action_sources.json').read_text());sources=catalog['sources'];ids=[s['id'] for s in sources]
   self.assertEqual(len(ids),len(set(ids)));self.assertIn('us-govinfo-plaw-api',ids);self.assertIn('us-govinfo-plaw',ids)
   speech=next(s for s in sources if s['url'].endswith('/speeches.xml'))
   self.assertEqual(speech['document_type'],'official_statement');self.assertTrue(speech['enabled'])

 def test_public_inspection_rule_is_not_a_published_final_rule(self):
  metadata=source_metadata(SOURCE|{'adapter':'public_inspection'},{'document_type':'Rule'})
  self.assertEqual(metadata['document_stage'],'public_inspection_pending_publication')

if __name__=='__main__':unittest.main()
