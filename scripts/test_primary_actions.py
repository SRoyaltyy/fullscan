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

if __name__=='__main__':unittest.main()
