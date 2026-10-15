import json,pathlib,tempfile,unittest
from datetime import datetime,timezone,timedelta
from unittest.mock import patch
from src.primary_actions import Ledger,collect,feed_rows,fetch_source,source_metadata,index_rows,allowed_public_source,get,OfficialRedirects

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

 def test_public_feed_can_disable_unsupported_conditional_headers(self):
  calls=[]
  def fetch(url,**kw):calls.append(kw['headers']);return 200,RSS,{},url
  fetch_source(SOURCE|{'conditional_cache':False},ASOF-timedelta(days=3),ASOF,{'etag':'tag','last_modified':'old'},fetch)
  self.assertEqual(calls,[{}])

if __name__=='__main__':unittest.main()

