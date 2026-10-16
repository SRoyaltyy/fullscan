"""Primary-origin action documents. Collection/parsing uses no LLM or paid API.

Official publication is provenance, not proof that a proposed policy is enacted.
Only source-provided document metadata is used for stage labels.
"""
import argparse,concurrent.futures,hashlib,html,json,os,pathlib,re,sqlite3,threading,time,urllib.parse,urllib.request,urllib.error
import xml.etree.ElementTree as ET
from datetime import datetime,timedelta,timezone
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser

VERSION='primary-actions-v1'
USER_AGENT='FullscanPrimaryActionScanner/1.0 (+https://github.com/SRoyaltyy/fullscan; public documents; no article-body crawling)'
MAX_BYTES=8_000_000
HTTP_AUDIT=[]
RELAY_URL=os.environ.get('PRIMARY_ACTION_RELAY_URL','').strip().rstrip('/')
RELAY_RETRY_STATUSES=(401,403,406,407,429)
OFFICIAL_SUFFIXES=('.gov','.gov.uk','.gc.ca','.gov.au','.govt.nz','.gov.hk','.go.jp','.go.kr','.gov.in','.gov.cn','.europa.eu','.gov.sg','.gov.my','.go.th','.gov.ph','.go.id','.gov.br','.gov.za','.gouv.fr','.gov.tw','.parliament.uk')
OFFICIAL_DOMAINS=('bankofengland.co.uk','bankofcanada.ca','rbi.org.in','snb.ch','riksbank.se','norges-bank.no','bundesbank.de','cnmc.es','banxico.org.mx','resbank.co.za','bis.org','wto.org','imf.org','worldbank.org','un.org','thegazette.co.uk','bot.or.th','bancaditalia.it','bnm.gov.my','newyorkfed.org','boj.or.jp','fca.org.uk','scc-csc.ca','sfc.hk','judiciary.hk','bundeskartellamt.de','autoritedelaconcurrence.fr')

def allowed_public_source(url):
    host=(urllib.parse.urlsplit(url).hostname or '').lower()
    return any(host.endswith(s) for s in OFFICIAL_SUFFIXES) or any(host==s or host.endswith('.'+s) for s in OFFICIAL_DOMAINS)

class OfficialRedirects(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,req,fp,code,msg,headers,newurl):
        if not allowed_public_source(newurl):raise ValueError('Redirect outside explicit official-source host policy')
        return super().redirect_request(req,fp,code,msg,headers,newurl)

def now():return datetime.now(timezone.utc)
def iso(dt):return dt.astimezone(timezone.utc).isoformat()
def digest(value):return hashlib.sha256(value.encode('utf-8')).hexdigest()
def clean(value):return ' '.join(html.unescape(re.sub('<[^>]+>',' ',str(value or ''))).split())
def date(value):
    if not value:return None
    try:d=datetime.fromisoformat(value.strip().replace('Z','+00:00'))
    except (ValueError,TypeError):
        try:d=parsedate_to_datetime(value)
        except (ValueError,TypeError,OverflowError):return None
    return d.replace(tzinfo=timezone.utc) if d.tzinfo is None else d.astimezone(timezone.utc)

def canonical_url(url):
    u=urllib.parse.urlsplit(url)
    if u.scheme not in ('https','http') or not u.hostname or u.username or u.password:raise ValueError('Public HTTP URL required')
    if u.hostname in ('localhost','127.0.0.1','::1','169.254.169.254'):raise ValueError('Local/metadata URL forbidden')
    q=[(k,v) for k,v in urllib.parse.parse_qsl(u.query,keep_blank_values=True) if not k.lower().startswith('utm_') and k.lower() not in ('fbclid','gclid','oc')]
    return urllib.parse.urlunsplit((u.scheme,u.netloc.lower(),u.path or '/',urllib.parse.urlencode(sorted(q)),''))

def relayed_get(url,timeout=20):
    # Optional edge relay (Cloudflare Worker in relay/worker.js): fetch the same official
    # URL from a clean egress IP when this network's range is WAF-blocked. Inert unless
    # PRIMARY_ACTION_RELAY_URL is set; target host policy is identical to direct fetches.
    if not RELAY_URL:return None
    relay=RELAY_URL+'/?url='+urllib.parse.quote(url,safe='')
    HTTP_AUDIT.append({'url':url,'method':'GET','body_bytes':0,'authorization_sent':False,'via_relay':True})
    request=urllib.request.Request(relay,headers={'User-Agent':USER_AGENT,'Accept':'application/json, application/atom+xml, application/rss+xml, text/html;q=0.8'})
    with urllib.request.build_opener(OfficialRedirects()).open(request,timeout=timeout+15) as response:
        body=response.read(MAX_BYTES+1)
        if len(body)>MAX_BYTES:raise ValueError('Response exceeds public-document metadata size limit')
        return response.status,body,dict(response.headers),url

def jina_get(url,timeout=20):
    # Tier-3 fallback: hosted headless-browser reader (r.jina.ai) that renders JS
    # challenges (Cloudflare/Akamai bot walls) which the edge relay also cannot pass.
    # Only ever called for URLs that already passed allowed_public_source() in get().
    target='https://r.jina.ai/'+urllib.parse.quote(url,safe='')
    HTTP_AUDIT.append({'url':url,'method':'GET','body_bytes':0,'authorization_sent':False,'via_jina':True})
    request=urllib.request.Request(target,headers={'User-Agent':USER_AGENT,'X-Respond-With':'html'})
    with urllib.request.build_opener().open(request,timeout=timeout+40) as response:
        body=response.read(MAX_BYTES+1)
        if len(body)>MAX_BYTES:raise ValueError('Response exceeds public-document metadata size limit')
        return response.status,body,dict(response.headers),url

def google_news_get(url,headers=None,timeout=20):
    # Discovery mirror for official domains that geo-block all datacenter IPs (e.g.
    # rbnz.govt.nz). Hardcoded to news.google.com RSS search endpoints whose query is
    # site:<official-host>; item links are Google redirect URLs resolving to the
    # official domain. Not an official host, so the official-source policy in get()
    # does not apply; this function is the only sanctioned path to that host.
    u=urllib.parse.urlsplit(url)
    if u.hostname!='news.google.com' or not u.path.startswith('/rss/') or 'site%3A' not in u.query.replace('site%3a','site%3A'):
        raise ValueError('google_news adapter only for news.google.com/rss/search with site: query')
    HTTP_AUDIT.append({'url':url,'method':'GET','body_bytes':0,'authorization_sent':False,'via_google_news':True})
    request=urllib.request.Request(url,headers={'User-Agent':USER_AGENT,'Accept':'application/rss+xml, application/atom+xml;q=0.9'})
    with urllib.request.build_opener().open(request,timeout=timeout) as response:
        body=response.read(MAX_BYTES+1)
        if len(body)>MAX_BYTES:raise ValueError('Response exceeds public-document metadata size limit')
        return response.status,body,dict(response.headers),response.url

def get(url,headers=None,timeout=20):
    canonical_url(url)
    if not allowed_public_source(url):raise ValueError('URL outside explicit official-source host policy')
    if any(k.lower() not in ('if-none-match','if-modified-since') for k in (headers or {})):raise ValueError('Only public conditional-cache headers are accepted')
    HTTP_AUDIT.append({'url':url,'method':'GET','body_bytes':0,'authorization_sent':False})
    request=urllib.request.Request(url,headers={'User-Agent':USER_AGENT,'Accept':'application/json, application/atom+xml, application/rss+xml, text/html;q=0.8',**(headers or {})})
    def _fallback():
        try:relayed=relayed_get(url,timeout)
        except Exception:relayed=None
        if relayed and relayed[0]==200:return relayed
        try:return jina_get(url,timeout)
        except Exception:return None
    try:
        with urllib.request.build_opener(OfficialRedirects()).open(request,timeout=timeout) as response:
            body=response.read(MAX_BYTES+1)
            if len(body)>MAX_BYTES:raise ValueError('Response exceeds public-document metadata size limit')
            return response.status,body,dict(response.headers),response.url
    except urllib.error.HTTPError as exc:
        if exc.code in RELAY_RETRY_STATUSES:
            fallback=_fallback()
            if fallback:return fallback
        return exc.code,b'',dict(exc.headers),url
    except (urllib.error.URLError,TimeoutError,OSError):
        fallback=_fallback()
        if fallback:return fallback
        raise

def text(element,name):
    found=next((e for e in element if e.tag.split('}')[-1]==name),None)
    return clean(''.join(found.itertext())) if found is not None else ''

def feed_rows(body,base):
    root=ET.fromstring(body)
    if root.tag.split('}')[-1] not in ('rss','feed','RDF'):raise ValueError('Not RSS/Atom')
    rows=[]
    for item in root.iter():
        if item.tag.split('}')[-1] not in ('item','entry'):continue
        title=text(item,'title');url=text(item,'link')
        if not url:
            links=[e for e in item if e.tag.split('}')[-1]=='link' and e.get('rel','alternate')=='alternate']
            url=links[0].get('href','') if links else ''
        if not title or not url:continue
        url=urllib.parse.urljoin(base,url)
        try:url=canonical_url(url)
        except ValueError:continue
        published=text(item,'pubDate') or text(item,'published') or text(item,'updated') or text(item,'date')
        identifier=text(item,'guid') or text(item,'id') or url
        rows.append({'title':title,'url':url,'native_id':identifier,'published_at':iso(date(published)) if date(published) else None,'published_raw':published,'summary':(text(item,'description') or text(item,'summary'))[:1200]})
    return rows

class Links(HTMLParser):
    def __init__(self):super().__init__(convert_charrefs=True);self.links=[];self.feeds=[];self.href=None;self.parts=[];self.base_href=None
    def handle_starttag(self,tag,attrs):
        a=dict(attrs)
        if tag=='base' and a.get('href'):self.base_href=a['href']
        if tag=='link' and ('rss' in a.get('type','') or 'atom' in a.get('type','')) and a.get('href'):self.feeds.append((a['href'],a.get('title','')))
        if tag=='a':self.href=a.get('href');self.parts=[]
    def handle_data(self,s):
        if self.href:self.parts.append(s)
    def handle_endtag(self,tag):
        if tag=='a' and self.href:self.links.append((self.href,clean(' '.join(self.parts))));self.href=None

class DatedLinks(Links):
    def __init__(self):super().__init__();self.contexts=[];self.dated=[]
    def handle_starttag(self,tag,attrs):
        if tag in ('tr','article','li'):self.contexts.append({'tag':tag,'parts':[],'links':[],'time':None})
        if tag=='time' and self.contexts:self.contexts[-1]['time']=dict(attrs).get('datetime')
        super().handle_starttag(tag,attrs)
    def handle_data(self,s):
        if self.contexts:self.contexts[-1]['parts'].append(s)
        super().handle_data(s)
    def handle_endtag(self,tag):
        before=len(self.links);super().handle_endtag(tag)
        if len(self.links)>before and self.contexts:self.contexts[-1]['links'].append(self.links[-1])
        if self.contexts and tag==self.contexts[-1]['tag']:
            context=self.contexts.pop();raw=context['time'];joined=clean(' '.join(context['parts']))
            if not raw:
                match=re.search(r'\b(20\d{2}-\d{2}-\d{2})\b',joined)
                if match:raw=match[1]
            if not raw:
                match=re.search(r'\b(\d{1,2}/\d{1,2}/(?:20)?\d{2})\b',joined)
                if match:
                    parts=list(map(int,match[1].split('/')))
                    if parts[2]<100:parts[2]+=2000
                    try:raw=iso(datetime(parts[2],parts[0],parts[1],tzinfo=timezone.utc))
                    except ValueError:pass
            if not raw:
                match=re.search(r'\b(\d{1,2})\s+(January|February|March|April|May|June|July|August|September|October|November|December|Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+(20\d{2})\b',joined,re.I)
                if match:
                    months={'jan':1,'feb':2,'mar':3,'apr':4,'may':5,'jun':6,'jul':7,'aug':8,'sep':9,'oct':10,'nov':11,'dec':12}
                    try:raw=iso(datetime(int(match[3]),months[match[2][:3].lower()],int(match[1]),tzinfo=timezone.utc))
                    except ValueError:pass
            for href,title in context['links']:self.dated.append((href,title,raw))

def index_rows(body,source):
    # Explicit publication-link selectors only. Generic navigation is never a headline feed.
    parser=DatedLinks();parser.feed(body.decode('utf-8','replace'));pattern=re.compile(source['link_pattern'])
    rows=[];seen=set()
    dates={href:raw for href,title,raw in parser.dated if date(raw)}
    for href,title in parser.links:
        url=urllib.parse.urljoin(urllib.parse.urljoin(source['url'],parser.base_href or ''),href)
        if len(title)<12 or not pattern.search(urllib.parse.urlsplit(url).path) or url in seen:continue
        seen.add(url);published=iso(date(dates[href])) if href in dates else None
        title=re.sub(r'^\d{1,2}\s+(?:January|February|March|April|May|June|July|August|September|October|November|December|Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+20\d{2}\s*[|\u2013\u2014-]?\s*','',title,flags=re.I).strip() or title
        match=re.search(r'/(20\d{2})[/\-](\d{2})[/\-](\d{2})(?:/|\b)',url)
        if match and not published:
            try:published=iso(datetime(*map(int,match.groups()),tzinfo=timezone.utc))
            except ValueError:pass
        match=re.search(r'/recent-actions/(20\d{2})(\d{2})(\d{2})(?:/|$)',urllib.parse.urlsplit(url).path)
        if match and not published:
            try:published=iso(datetime(*map(int,match.groups()),tzinfo=timezone.utc))
            except ValueError:pass
        rows.append({'title':title,'url':url,'native_id':url,'published_at':published,'published_raw':dates.get(href) or ('date in publication URL' if published else None),'summary':''})
    return rows

def source_metadata(source,row):
    # Typed source status is retained without translating proposal into enactment.
    kind=row.get('document_type') or source.get('document_type') or 'official_announcement'
    stage={'Rule':'final_rule_published','Proposed Rule':'proposal_published','Notice':'notice_published','Presidential Document':'presidential_document_published','decision':'decision_published','employment_tribunal_decision':'judicial_decision_published','tax_tribunal_decision':'judicial_decision_published','public_law':'law_published','court_opinion':'judicial_opinion_published','enrolled_bill':'bill_enrolled_not_yet_law','bill':'legislative_document_published','statutory_instrument':'instrument_published','consultation_outcome':'consultation_response_published'}.get(kind,'requires_semantic_review')
    if kind in ('employment_appeal_tribunal_decision','residential_property_tribunal_decision','utaac_decision','asylum_support_decision'):stage='judicial_decision_published'
    if kind=='traffic_commissioner_regulatory_decision':stage='decision_published'
    if kind=='court_order':stage='judicial_order_published'
    if kind=='presidential_document':stage='presidential_document_published'
    return {'document_type':kind,'document_stage':stage,'concrete_public_action':None,'classification_status':'source_metadata_only; semantic action verdict not inferred','issuer':row.get('issuer') or source['name'],'jurisdiction':source['jurisdiction'],'family':source['family']}

def fetch_source(source,start,end,previous=None,fetch=get,max_pages=30):
    headers={}
    # Conditional caching is safe for fixed feed URLs, not moving date-filtered APIs.
    if previous and source.get('conditional_cache',True) and source['adapter'] in ('rss','html_index'):
        if previous.get('etag'):headers['If-None-Match']=previous['etag']
        if previous.get('last_modified'):headers['If-Modified-Since']=previous['last_modified']
    adapter=source['adapter'];url=source['url'];rows=[];pages=0;warnings=[];status=None;response_headers={};first_hash=None;oldest=None
    if adapter=='federal_register':
        query={'per_page':1000,'order':'newest','conditions[publication_date][gte]':start.date().isoformat(),'conditions[publication_date][lte]':end.date().isoformat()}
        url+='?'+urllib.parse.urlencode(query)
    elif adapter=='govuk_search':
        url+='?'+urllib.parse.urlencode({'filter_content_store_document_type':source['document_type'],'filter_public_timestamp':'from:'+iso(start)+',to:'+iso(end),'count':1000,'order':'-public_timestamp','start':0})
    elif adapter=='hkma_press':
        url+='?'+urllib.parse.urlencode({'lang':'en','sortby':'date','sortorder':'desc','pagesize':source.get('page_size',100),'offset':0})
    seen_urls=set()
    while url:
        if url in seen_urls:raise ValueError('Pagination cycle')
        seen_urls.add(url);pages+=1
        if pages>max_pages:warnings.append('pagination_budget_reached');break
        fetch_fn=google_news_get if adapter=='google_news_rss' else fetch
        status,body,response_headers,resolved=fetch_fn(url,headers=headers if pages==1 else {})
        if status==304:return [],{**(previous or {}),'status':'unchanged','http_status':304,'requests':pages}
        if status!=200:return rows,{'status':'blocked' if status in (401,403,429) else 'fetch_failed','http_status':status,'requests':pages,'warnings':warnings,'fetched_rows':len(rows)}
        if first_hash is None:first_hash=hashlib.sha256(body).hexdigest()
        if adapter in ('rss','google_news_rss','html_index'):
            rows=feed_rows(body,resolved) if adapter!='html_index' else index_rows(body,source)
            dates=[date(r.get('published_at')) for r in rows if date(r.get('published_at'))]
            if adapter=='rss' and len(rows)>=source.get('saturation_limit',100) and dates and min(dates)>=start:warnings.append('feed_at_capacity_in_requested_window; snapshots may miss overflow')
            url=None
        elif adapter=='hkma_press':
            blob=json.loads(body)
            if blob.get('header',{}).get('success') is not True or not isinstance(blob.get('result',{}).get('records'),list):raise ValueError('Expected HKMA successful structured records')
            page=blob['result']['records']
            for r in page:
                rows.append({'title':clean(r['title']),'url':r['link'],'native_id':r['link'],'published_at':iso(date(r['date'])) if date(r.get('date')) else None,'published_raw':r.get('date'),'issuer':'Hong Kong Monetary Authority','summary':''})
            publication_dates=[date(r.get('date')) for r in page]
            ordered=bool(publication_dates) and all(publication_dates) and publication_dates==sorted(publication_dates,reverse=True)
            if len(page)>=source.get('page_size',100) and not (ordered and min(publication_dates)<start):
                query=dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(url).query));query['offset']=int(query.get('offset',0))+len(page);url=source['url']+'?'+urllib.parse.urlencode(query)
            else:url=None
        else:
            blob=json.loads(body)
            if not isinstance(blob.get('results'),list):raise ValueError('Expected structured results')
            if adapter=='federal_register':
                for r in blob['results']:
                    rows.append({'title':clean(r['title']),'url':r['html_url'],'native_id':r['document_number'],'published_at':iso(date(r['publication_date'])),'published_raw':r['publication_date'],'document_type':r['type'],'issuer':'; '.join(a.get('name') or a.get('raw_name') or 'Issuing agency unresolved' for a in r.get('agencies',[])),'issuer_ids':[f"us-fr:{a['id']}" for a in r.get('agencies',[]) if a.get('id') is not None],'summary':clean(r.get('abstract',''))[:1200],'official_pdf_url':r.get('pdf_url')})
                url=blob.get('next_page_url')
                if blob.get('count',0)>2000:warnings.append('federal_register_2000_result_limit; use narrower date windows')
            else:
                for r in blob['results']:
                    rows.append({'title':clean(r['title']),'url':urllib.parse.urljoin('https://www.gov.uk',r['link']),'native_id':r.get('content_id') or r['link'],'published_at':iso(date(r['public_timestamp'])) if date(r.get('public_timestamp')) else None,'published_raw':r.get('public_timestamp'),'document_type':source['document_type'],'issuer':'; '.join(o.get('title','') for o in r.get('organisations',[])) or 'GOV.UK issuing body unresolved','issuer_ids':['uk-gov:'+o['slug'] for o in r.get('organisations',[]) if o.get('slug')],'summary':clean(r.get('description',''))[:1200]})
                next_start=blob.get('start',0)+len(blob['results'])
                if next_start<blob.get('total',0) and blob['results']:
                    query=dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(url).query));query['start']=next_start;url=source['url']+'?'+urllib.parse.urlencode(query)
                else:url=None
    dates=[date(r.get('published_at')) for r in rows if date(r.get('published_at'))]
    latest=max(dates) if dates else None
    health={'status':'ok' if rows else 'valid_empty','http_status':status,'requests':pages,'fetched_rows':len(rows),'latest_published_at':iso(latest) if latest else None,'warnings':warnings,'etag':response_headers.get('ETag') or response_headers.get('Etag'),'last_modified':response_headers.get('Last-Modified'),'response_sha256':first_hash}
    if latest and latest<start:health['warnings'].append('stale_feed_outside_requested_window')
    if adapter=='html_index' and not rows:health.update(status='selector_empty',warnings=warnings+['HTML selector matched no publication links; not healthy zero activity'])
    return rows,health

class Ledger:
    def __init__(self,path):
        path=pathlib.Path(path);path.parent.mkdir(parents=True,exist_ok=True);self.db=sqlite3.connect(path);self.changed_ids=set()
        self.db.executescript('CREATE TABLE IF NOT EXISTS documents(id INTEGER PRIMARY KEY,canonical_key TEXT UNIQUE NOT NULL,title TEXT NOT NULL,url TEXT NOT NULL,published_at TEXT,first_seen TEXT NOT NULL,last_seen TEXT NOT NULL,payload TEXT NOT NULL); CREATE TABLE IF NOT EXISTS observations(document_id INTEGER NOT NULL,source_id TEXT NOT NULL,native_id TEXT NOT NULL,last_seen TEXT NOT NULL,PRIMARY KEY(source_id,native_id)); CREATE TABLE IF NOT EXISTS source_state(source_id TEXT PRIMARY KEY,payload TEXT NOT NULL);')
        self.db.executescript('CREATE TABLE IF NOT EXISTS revisions(id INTEGER PRIMARY KEY,document_id INTEGER NOT NULL,observed_at TEXT NOT NULL,source_id TEXT NOT NULL,previous_payload TEXT NOT NULL,new_payload TEXT NOT NULL); CREATE TABLE IF NOT EXISTS url_aliases(canonical_key TEXT PRIMARY KEY,document_id INTEGER NOT NULL); CREATE INDEX IF NOT EXISTS documents_published ON documents(published_at); CREATE INDEX IF NOT EXISTS observations_document ON observations(document_id); INSERT OR IGNORE INTO url_aliases SELECT canonical_key,id FROM documents;')
    def state(self,source_id):
        row=self.db.execute('SELECT payload FROM source_state WHERE source_id=?',(source_id,)).fetchone();return json.loads(row[0]) if row else None
    def retain(self,source,row,asof):
        url=canonical_url(row['url']);key=digest(url);row=dict(row,url=url,source_metadata=source_metadata(source,row),primary_source_id=source['id']);native=str(row.get('native_id') or url);payload=json.dumps(row,ensure_ascii=False)
        existing=self.db.execute('SELECT document_id FROM observations WHERE source_id=? AND native_id=?',(source['id'],native)).fetchone()
        existing=existing or self.db.execute('SELECT document_id FROM url_aliases WHERE canonical_key=?',(key,)).fetchone()
        if existing:
            ident=existing[0];previous=self.db.execute('SELECT payload FROM documents WHERE id=?',(ident,)).fetchone()[0];old=json.loads(previous)
            owner=old.get('primary_source_id') or self.db.execute('SELECT source_id FROM observations WHERE document_id=? ORDER BY rowid LIMIT 1',(ident,)).fetchone()[0]
            if owner==source['id']:
                if any(old.get(k)!=row.get(k) for k in ('title','published_at','document_type','summary','url')):
                    self.db.execute('INSERT INTO revisions(document_id,observed_at,source_id,previous_payload,new_payload) VALUES (?,?,?,?,?)',(ident,asof,source['id'],previous,payload));self.changed_ids.add(ident)
                # Refresh adapter metadata without inventing a source-content revision.
                self.db.execute('UPDATE documents SET title=?,url=?,published_at=?,payload=? WHERE id=?',(row['title'],url,row.get('published_at'),payload,ident))
            self.db.execute('UPDATE documents SET last_seen=? WHERE id=?',(asof,ident));fresh=False
        else:
            ident=self.db.execute('INSERT INTO documents(canonical_key,title,url,published_at,first_seen,last_seen,payload) VALUES (?,?,?,?,?,?,?)',(key,row['title'],url,row.get('published_at'),asof,asof,payload)).lastrowid;fresh=True
        self.db.execute('INSERT OR IGNORE INTO url_aliases VALUES (?,?)',(key,ident))
        self.db.execute('INSERT INTO observations VALUES (?,?,?,?) ON CONFLICT(source_id,native_id) DO UPDATE SET last_seen=excluded.last_seen',(ident,source['id'],native,asof))
        return ident,fresh
    def documents(self,identifiers=None,since=None):
        if identifiers is not None:
            identifiers=list(set(identifiers))
            if not identifiers:return []
            if len(identifiers)>400:return [r for i in range(0,len(identifiers),400) for r in self.documents(identifiers[i:i+400])]
        query='SELECT id,payload,first_seen,last_seen FROM documents';args=[]
        if identifiers is not None:
            if not identifiers:return []
            query+=' WHERE id IN ('+','.join('?' for _ in identifiers)+')';args=list(identifiers)
        elif since:query+=' WHERE published_at>=?';args=[since]
        output=[]
        for ident,payload,first,last in self.db.execute(query,args):
            value=json.loads(payload);origins=[r[0] for r in self.db.execute('SELECT source_id FROM observations WHERE document_id=? ORDER BY source_id',(ident,))]
            output.append(value|{'n':ident,'first_seen':first,'last_seen':last,'source_ids':origins})
        return output
    def save_state(self,source_id,state):self.db.execute('INSERT OR REPLACE INTO source_state VALUES (?,?)',(source_id,json.dumps(state)))
    def close(self):self.db.close()

def atomic_json(path,data):
    path=pathlib.Path(path);path.parent.mkdir(parents=True,exist_ok=True);temp=path.with_suffix(path.suffix+'.tmp');temp.write_text(json.dumps(data,ensure_ascii=False,indent=2)+'\n',encoding='utf-8');temp.replace(path)

def collect(catalog,ledger,output,lookback_days=3,workers=8,fetch=get,asof=None):
    ledger.changed_ids.clear();asof=asof or now();start=asof-timedelta(days=lookback_days);sources=[s for s in catalog['sources'] if s.get('enabled')];health=[];fresh_ids=[];counts={'observed':0,'old':0,'future':0,'undated':0,'new':0,'duplicates':0};locks={}
    for s in sources:locks.setdefault(urllib.parse.urlsplit(s['url']).hostname,threading.Semaphore(2))
    states={s['id']:ledger.state(s['id']) for s in sources}
    def task(source):
        try:
            with locks[urllib.parse.urlsplit(source['url']).hostname]:return source,*fetch_source(source,start,asof,states[source['id']],fetch)
        except Exception as exc:return source,[],{'status':'parse_or_network_error','exception_type':type(exc).__name__,'requests':1}
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        for source,rows,state in pool.map(task,sources):
            health.append({'source_id':source['id'],'url':source['url'],**state});ledger.save_state(source['id'],state)
            for row in rows:
                counts['observed']+=1;published=date(row.get('published_at'))
                if not published:counts['undated']+=1
                elif published<start:counts['old']+=1;continue
                elif published>asof+timedelta(days=1):counts['future']+=1;continue
                try:ident,fresh=ledger.retain(source,row,iso(asof))
                except (ValueError,KeyError):state.setdefault('warnings',[]).append('invalid_publication_row');continue
                counts['new' if fresh else 'duplicates']+=1
                if fresh:fresh_ids.append(ident)
            ledger.db.commit()
    new=ledger.documents(fresh_ids);all_docs=ledger.documents(since=iso(start));enabled_ids={s['id'] for s in sources}
    # Unknown dates stay visible in a separate queue, rather than masquerading as today's actions.
    current=[r for r in all_docs if enabled_ids.intersection(r['source_ids']) and date(r.get('published_at')) and start<=date(r['published_at'])<=asof+timedelta(days=1)]
    current.sort(key=lambda r:(r['published_at'],r['n']),reverse=True)
    unresolved=[r for r in new if not r.get('published_at')]
    groups={}
    for r in current:
        key=r['source_metadata']['document_stage'];groups[key]=groups.get(key,0)+1
    failures=[h for h in health if h['status'] not in ('ok','valid_empty','unchanged')]
    report={'version':VERSION,'generated_at':iso(asof),'window_start':iso(start),'window_end':iso(asof),'scope':'official primary publication titles and supplied metadata; no article bodies or inferred policy enactment','enabled_sources':len(sources),'sources_ok':len(health)-len(failures),'sources_failed':len(failures),'coverage_status':'degraded' if failures or any(h.get('warnings') for h in health) else 'configured_sources_checked_not_exhaustive_global_coverage','counts':counts,'current_dated_documents':len(current),'new_undated_review':len(unresolved),'stage_counts':groups,'llm_calls':0,'paid_api_calls':0,'sources':health}
    output=pathlib.Path(output);output.mkdir(parents=True,exist_ok=True)
    atomic_json(output/'report.json',report);atomic_json(output/'new_items.json',new);atomic_json(output/'current_documents.json',current);atomic_json(output/'undated_review.json',unresolved)
    report.update(catalog_disabled_channels=len(catalog['sources'])-len(sources),changed_existing_documents=len(ledger.changed_ids))
    atomic_json(output/'report.json',report)
    atomic_json(output/'pending_headlines.json',[{'n':r['n'],'title':r['title'],'url':r['url'],'published_at':r['published_at'],'official_issuer':r['source_metadata']['issuer'],'document_type':r['source_metadata']['document_type'],'document_stage':r['source_metadata']['document_stage'],'source_ids':r['source_ids']} for r in new if r.get('published_at')])
    final_stages={'final_rule_published','decision_published','judicial_decision_published','judicial_opinion_published','judicial_order_published','law_published','instrument_published','presidential_document_published'}
    atomic_json(output/'action_documents.json',[r for r in current if r['source_metadata']['document_stage'] in final_stages])
    atomic_json(output/'proposals_and_procedure.json',[r for r in current if r['source_metadata']['document_stage'] not in final_stages and r['source_metadata']['document_stage']!='requires_semantic_review'])
    atomic_json(output/'updated_documents_review.json',ledger.documents(ledger.changed_ids))
    lines=['# Primary-origin action documents','',f"Updated {iso(asof)}. {len(current)} dated documents in the configured {lookback_days}-day window; {counts['new']} newly retained; {counts['duplicates']} repeat observations.",f"Source health: {len(health)-len(failures)}/{len(sources)} checked successfully. Collection uses zero LLM calls.",'','Document stage comes from publisher metadata. Proposed rules, bills and announcements are not automatically labelled enacted policy or concrete public actions.','']
    for stage in sorted(groups):
        lines+=['## '+stage,'']
        for r in (x for x in current if x['source_metadata']['document_stage']==stage):
            title=r['title'].replace('[','(').replace(']',')');lines.append(f"- [{title}]({r['url']}) — {r['source_metadata']['issuer']}; {r['published_at']}")
        lines.append('')
    lines+=['## Source failures and coverage warnings','']
    for h in health:
        if h in failures or h.get('warnings'):lines.append(f"- {h['source_id']}: {h['status']}; "+'; '.join(h.get('warnings',[])))
    (output/'headlines.md').write_text('\n'.join(lines)+'\n',encoding='utf-8')
    return report

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--catalog',type=pathlib.Path,default=pathlib.Path('config/primary_action_sources.json'));p.add_argument('--state',type=pathlib.Path,default=pathlib.Path('data/primary_actions/ledger.sqlite3'));p.add_argument('--output',type=pathlib.Path,default=pathlib.Path('data/primary_actions/latest'));p.add_argument('--days',type=int,default=3);p.add_argument('--workers',type=int,default=8);a=p.parse_args()
    if not 1<=a.days<=14 or not 1<=a.workers<=16:p.error('days must be 1..14 and workers 1..16')
    ledger=Ledger(a.state)
    try:r=collect(json.loads(a.catalog.read_text(encoding='utf-8')),ledger,a.output,a.days,a.workers)
    finally:
        ledger.close();atomic_json(a.output/'http_request_audit.json',HTTP_AUDIT)
    print(json.dumps({k:v for k,v in r.items() if k!='sources'},indent=2))
    if r['sources_ok']==0:raise SystemExit('No configured source succeeded; health report retained')

if __name__=='__main__':main()

