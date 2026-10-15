"""Discover official institutions/publication channels and verify parsers live.

Institution-directory membership is not a claim that a distinct website/feed works.
Unverified/blocked endpoints remain visible and disabled, never padded into coverage.
"""
import argparse,concurrent.futures,hashlib,json,pathlib,re,threading,urllib.parse
from collections import Counter
from src.primary_actions import VERSION,Links,get,feed_rows,index_rows,now,iso,atomic_json,fetch_source,HTTP_AUDIT,allowed_public_source
from datetime import timedelta

DIRECTORIES={
 'us_agencies':'https://www.federalregister.gov/api/v1/agencies.json',
 'uk_organisations':'https://www.gov.uk/api/organisations?page=1',
 'us_court_feeds':'https://www.govinfo.gov/feeds',
 'central_banks':'https://www.bis.org/about/organisation/members',
 'us_states':'https://www.usa.gov/state-governments'
}
UK_TYPES=['decision','regulation','statutory_instrument','consultation_outcome','government_response','employment_tribunal_decision','employment_appeal_tribunal_decision','residential_property_tribunal_decision','tax_tribunal_decision','utaac_decision','traffic_commissioner_regulatory_decision','asylum_support_decision','medical_safety_alert','drug_safety_update','product_safety_alert_report_recall','international_treaty','written_statement','farming_grant','business_finance_support_scheme','notice']

def read_url(url):
 status,body,headers,resolved=get(url)
 if status!=200:raise RuntimeError('Directory HTTP '+str(status))
 return body,resolved

def channel_scope(source):
 # Retain verification in the catalog, but do not collect reference-only feeds daily.
 label=(source['url']+' '+source['name']).lower()
 if source.get('document_type') in ('farming_grant','business_finance_support_scheme'):return 'funding guidance/opportunity listing; not an award decision'
 if 'federalreserve.gov/feeds/' in label and re.search(r'/feeds/(?:s_t_|m_w_|feds(?:_|\.)|clp\.|g\d|h\d|prates\.|testimony\.)',label):return 'Fed research, speeches/testimony or reference data; policy releases remain enabled'
 if any(x in label for x in ('exchange rate','exchangerate','exchange_rate','eurofxref','reference rate','research paper','working paper','discussion paper','statistical release','currentfaqs','/speeches','press_speeches','/research/','/blog','/podcast')):return 'reference data, research, speeches or commentary'
 return None

def build(root):
 seeds=json.loads((root/'config/primary_action_seeds.json').read_text(encoding='utf-8'))['sources']
 institutions=[];sources=[];evidence=[];gaps=[];stamp=iso(now())
 def evidence_row(url,body):evidence.append({'url':url,'verified_at':stamp,'sha256':hashlib.sha256(body).hexdigest(),'bytes':len(body)})
 body,_=read_url(DIRECTORIES['us_agencies']);evidence_row(DIRECTORIES['us_agencies'],body)
 for agency in json.loads(body):
  institutions.append({'id':'us-fr:'+str(agency['id']),'name':agency['name'],'jurisdiction':'US','website':agency.get('agency_url') or agency['url'],'official_directory_url':agency['url'],'status':'listed; active/historical status not supplied by directory','shared_publication_source_ids':['us-federal-register'],'parent_id':('us-fr:'+str(agency['parent_id'])) if agency.get('parent_id') else None})
 sources.append({'id':'us-federal-register','name':'Federal Register issuing agencies','jurisdiction':'US','family':'rules_notices_and_presidential_documents','url':'https://www.federalregister.gov/api/v1/documents.json','adapter':'federal_register','discovery_url':'https://www.federalregister.gov/developers/documentation/api/v1','candidate_origin':'documented_official_api'})
 sources.append({'id':'us-treasury-auctions','name':'TreasuryDirect announced auctions','jurisdiction':'US','family':'fiscal_operations','document_type':'treasury_auction','url':'https://www.treasurydirect.gov/TA_WS/securities/announced?format=json','adapter':'treasury_announced','discovery_url':'https://www.treasurydirect.gov/webapis/webapisindex.htm','candidate_origin':'documented_official_api'})
 sources.append({'id':'hk-hkma-public-press','name':'Hong Kong Monetary Authority official press releases','jurisdiction':'HK','family':'monetary_and_supervisory','url':'https://api.hkma.gov.hk/public/press-releases','adapter':'hkma_press','discovery_url':'https://apidocs.hkma.gov.hk/documentation/press-releases/','candidate_origin':'documented_official_api; descending dates, client-side window and offset pagination'})
 body,_=read_url(DIRECTORIES['uk_organisations']);evidence_row(DIRECTORIES['uk_organisations'],body);first=json.loads(body)
 pages=[first]
 def uk_page(n):
  url='https://www.gov.uk/api/organisations?page='+str(n);b,_=read_url(url);return url,b,json.loads(b)
 with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
  for url,b,page in pool.map(uk_page,range(2,first['pages']+1)):evidence_row(url,b);pages.append(page)
 orgs=[r for page in pages for r in page['results']]
 if len(orgs)!=first['total']:raise ValueError('Incomplete GOV.UK registry pagination')
 for org in orgs:
  detail=org['details'];institutions.append({'id':'uk-gov:'+detail['slug'],'name':org['title'],'jurisdiction':'UK','website':org['web_url'],'official_directory_url':org['id'],'status':detail.get('govuk_status') or 'unknown','closed_at':detail.get('closed_at'),'institution_type':org.get('format'),'shared_publication_source_ids':['uk-govuk-'+t for t in UK_TYPES] if detail.get('govuk_status')!='closed' else []})
 for kind in UK_TYPES:
  sources.append({'id':'uk-govuk-'+kind,'name':'GOV.UK '+kind.replace('_',' '),'jurisdiction':'UK','family':'typed_government_documents','document_type':kind,'url':'https://www.gov.uk/api/search.json','adapter':'govuk_search','discovery_url':'https://docs.publishing.service.gov.uk/repos/search-api/using-the-search-api.html','candidate_origin':'documented_official_api; verified one document type per query'})
 body,resolved=read_url(DIRECTORIES['us_court_feeds']);evidence_row(DIRECTORIES['us_court_feeds'],body);parser=Links();parser.feed(body.decode())
 selected={'plaw':'public_law','bills':'bill','bills-enr':'enrolled_bill','dcpd':'presidential_document','chrg':'hearing_record','crpt':'committee_report'}
 seen=set()
 for href,title in parser.links:
  url=urllib.parse.urljoin(resolved,href);match=re.search(r'/rss/([a-z0-9-]+)\.xml$',url)
  if not match or url in seen:continue
  code=match[1]
  if code not in selected and not code.startswith('uscourts-'):continue
  seen.add(url);court=code.startswith('uscourts-');ident='us-govinfo-'+code
  sources.append({'id':ident,'name':title or code,'jurisdiction':'US','family':'judicial_decisions' if court else 'legislative_and_presidential_documents','document_type':'court_opinion' if court else selected[code],'url':url,'adapter':'rss','discovery_url':DIRECTORIES['us_court_feeds'],'candidate_origin':'official_directory_link','saturation_limit':100})
  if court:institutions.append({'id':ident,'name':title or code,'jurisdiction':'US','website':url,'official_directory_url':DIRECTORIES['us_court_feeds'],'status':'selected court participating in GovInfo; not all courts','shared_publication_source_ids':[ident]})
 # Central-bank and state directories add institution identities, not speculative feed URLs.
 for group in ('central_banks','us_states'):
  try:
   body,resolved=read_url(DIRECTORIES[group]);evidence_row(DIRECTORIES[group],body);p=Links();p.feed(body.decode('utf-8','replace'));seen_dir=set()
   for href,title in p.links:
    url=urllib.parse.urljoin(resolved,href);host=urllib.parse.urlsplit(url).hostname or ''
    selected_link=(group=='central_banks' and href.startswith('http') and 'bis.org' not in host and len(title)>5 and not any(x in host for x in ('youtube','linkedin','twitter','facebook'))) or (group=='us_states' and re.fullmatch(r'https://www\.usa\.gov/states/[a-z-]+/?',url))
    if not selected_link or url in seen_dir:continue
    seen_dir.add(url);institutions.append({'id':group+':'+hashlib.sha256(url.encode()).hexdigest()[:16],'name':title,'jurisdiction':'INT' if group=='central_banks' else 'US-state','website':url,'official_directory_url':DIRECTORIES[group],'status':'directory_identity_only; publication channel requires verification','shared_publication_source_ids':[]})
   if not seen_dir:gaps.append({'directory':group,'url':DIRECTORIES[group],'status':'directory_selector_matched_zero; not claimed as coverage'})
  except Exception as exc:gaps.append({'directory':group,'url':DIRECTORIES[group],'status':'directory_fetch_failed','exception_type':type(exc).__name__})
 discovery=[];candidate_urls={s['url'] for s in sources}
 def seed_page(seed):
  try:
   status,body,_,resolved=get(seed['discovery_url'])
   if status!=200:return seed,status,[],None
   parser=Links();parser.feed(body.decode('utf-8','replace'))
   found=list(parser.feeds);base=urllib.parse.urljoin(resolved,parser.base_href or '')
   for href,title in parser.links:
    if re.search(r'(?:\.(?:rss|atom|xml)(?:[?#]|$)|/feed/?(?:[?#]|$)|data\.feed|/rss(?:/|\?|$))',href,re.I):found.append((href,title))
   found.sort(key=lambda x:0 if re.search(r'\.(?:xml|rss|atom)(?:[?#]|$)',x[0],re.I) else 1)
   return seed,status,[(urllib.parse.urljoin(base,u),t) for u,t in found[:50]],hashlib.sha256(body).hexdigest()
  except Exception as exc:return seed,type(exc).__name__,[],None
 with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
  for seed,status,found,sha in pool.map(seed_page,seeds):
   discovery.append({'institution_id':seed['id'],'name':seed['name'],'jurisdiction':seed['jurisdiction'],'family':seed['family'],'discovery_url':seed['discovery_url'],'http_status':status,'response_sha256':sha,'feed_links_found':len(found),'notes':seed.get('notes')})
   institutions.append({'id':seed['id'],'name':seed['name'],'jurisdiction':seed['jurisdiction'],'website':seed['discovery_url'],'official_directory_url':seed['discovery_url'],'status':'official_publication_page_checked' if status==200 else 'page_unavailable; verification pending','shared_publication_source_ids':[]})
   for url,title in [(u,None) for u in seed.get('feed_urls',[])]+found:
    if not url.startswith(('https://','http://')) or url in candidate_urls:continue
    if not allowed_public_source(url):
     gaps.append({'institution_id':seed['id'],'url':url,'status':'external_feed_host_not_approved; not requested'});continue
    candidate_urls.add(url);sources.append({'id':seed['id']+'-'+hashlib.sha256(url.encode()).hexdigest()[:10],'name':seed['name']+(' — '+title if title else ''),'jurisdiction':seed['jurisdiction'],'family':seed['family'],'url':url,'adapter':'rss','document_type':seed.get('document_type','official_announcement'),'discovery_url':seed['discovery_url'],'candidate_origin':'official_page_feed_link' if any(u==url for u,_ in found) else 'proposed_seed_url_requires_live_check','saturation_limit':100})
   if seed.get('index_url'):
    url=seed['index_url']
    sources.append({'id':seed['id']+'-index','name':seed['name'],'jurisdiction':seed['jurisdiction'],'family':seed['family'],'url':url,'adapter':'html_index','document_type':seed.get('document_type','official_announcement'),'link_pattern':seed['link_pattern'],'discovery_url':seed['discovery_url'],'candidate_origin':'specific_publication_link_selector'})
 locks={}
 for s in sources:locks.setdefault(urllib.parse.urlsplit(s['url']).hostname,threading.Semaphore(2))
 def verify(source):
  try:
   if 'www.federalreserve.gov/feeds/' in source['url']:
    # The public server accepts ordinary GETs but rejects some conditional requests with HTTP 406.
    source=source|{'conditional_cache':False}
   with locks[urllib.parse.urlsplit(source['url']).hostname]:
    # RSS/index verification retains only parser metadata; API verification is date-bounded.
    rows,state=fetch_source(source,now()-timedelta(days=3),now(),max_pages=1)
   good=state['status'] in ('ok','valid_empty')
   if source['adapter']=='html_index':good=good and bool(rows)
   exclusion=channel_scope(source)
   return source|{'enabled':good and not exclusion,'scope_exclusion':exclusion,'verification':state|{'checked_at':stamp,'sample_titles':[r['title'] for r in rows[:3]],'parsed_rows':len(rows)}}
  except Exception as exc:return source|{'enabled':False,'verification':{'status':'invalid_feed_or_network_error','checked_at':stamp,'exception_type':type(exc).__name__}}
 verified=[]
 with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
  for result in pool.map(verify,sources):verified.append(result)
 # Preserve institution-to-channel links without claiming that every registered body has recent actions.
 by_seed={s['id']:[v['id'] for v in verified if v['id'].startswith(s['id']+'-')] for s in seeds}
 for institution in institutions:
  if institution['id'] in by_seed:institution['shared_publication_source_ids']=by_seed[institution['id']]
 registry={'version':'primary-action-institutions-v1','generated_at':stamp,'scope':'Directory identities include historical/closed bodies. Shared portals are not counted as thousands of distinct working websites. Publication-channel health is separate.','institutions':institutions,'directory_evidence':evidence,'directory_gaps':gaps}
 catalog={'version':VERSION,'generated_at':stamp,'seed_sha256':hashlib.sha256((root/'config/primary_action_seeds.json').read_bytes()).hexdigest(),'implementation_sha256':hashlib.sha256((root/'src/primary_actions.py').read_bytes()+(root/'scripts/build_primary_action_catalog.py').read_bytes()).hexdigest(),'scope':'Official publication endpoints; enabled only after successful live parsing. Source status never proves enactment of every document.','sources':verified,'discovery_pages':discovery}
 atomic_json(root/'data/primary_actions/institution_registry.json',registry);atomic_json(root/'config/primary_action_sources.json',catalog)
 summary={'institutions':len(institutions),'by_jurisdiction':dict(Counter(i['jurisdiction'] for i in institutions)),'uk_status':dict(Counter(i['status'] for i in institutions if i['id'].startswith('uk-gov:'))),'candidate_channels':len(verified),'enabled_channels':sum(v['enabled'] for v in verified),'channel_statuses':dict(Counter(v['verification']['status'] for v in verified)),'directory_gaps':gaps}
 summary.update(institution_records=len(institutions),count_caution='Overlapping directory identities and channel owners, including closed/historical bodies; not unique active institutions or working websites.',directory_record_groups=dict(Counter(i['id'].split(':')[0] if ':' in i['id'] else 'govinfo_court' if i['id'].startswith('us-govinfo-') else 'curated_channel_owner' for i in institutions)),excluded_reference_channels=sum(bool(v.get('scope_exclusion')) for v in verified))
 atomic_json(root/'docs/primary-actions/catalog_summary.json',summary)
 lines=['# Primary-origin source catalog','',f"Verified {stamp}. {summary['institutions']} institution records; {summary['enabled_channels']} enabled publication channels out of {len(verified)} checked candidates.",'','Institution records include historical/closed bodies and shared portals. This is a maintained coverage catalog, not an exhaustive worldwide directory. Disabled/blocked channels remain visible below.','', '| Institution / publication channel | Jurisdiction | Family | Adapter | Enabled | Verification |','|---|---|---|---|---|---|']
 for s in verified:lines.append('| '+s['name'].replace('|','/')+' | '+s['jurisdiction']+' | '+s['family']+' | ['+s['adapter']+']('+s['url']+') | '+str(s['enabled'])+' | '+s['verification']['status']+('; '+s['scope_exclusion'] if s.get('scope_exclusion') else '')+' |')
 lines+=['','## Institution directory','', '| Institution | Jurisdiction | Status | Official directory / website |','|---|---|---|---|']
 for i in institutions:lines.append('| '+i['name'].replace('|','/')+' | '+i['jurisdiction']+' | '+i['status']+' | [source]('+i['website']+') |')
 p=root/'docs/primary-actions/source_catalog.md';p.parent.mkdir(parents=True,exist_ok=True);p.write_text('\n'.join(lines)+'\n',encoding='utf-8')
 return summary

def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--root',type=pathlib.Path,default=pathlib.Path('.'));a=p.parse_args()
 try:print(json.dumps(build(a.root),indent=2))
 finally:atomic_json(a.root/'research/http_request_audit.json',HTTP_AUDIT)
if __name__=='__main__':main()

