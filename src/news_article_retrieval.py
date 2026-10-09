"""Free bounded publisher retrieval with exact provenance and explicit failure."""
import json,re,time,urllib.request,hashlib
from datetime import datetime,timezone
from html.parser import HTMLParser
from urllib.parse import urljoin,urlsplit,urlunsplit

class SourceLinks(HTMLParser):
    def __init__(self):
        super().__init__();self.hidden=0;self.article=0;self.anchor=None;self.links=[]
    def handle_starttag(self,tag,attrs):
        if tag in ('script','style','noscript','nav','aside','footer'):self.hidden+=1
        if tag=='article':self.article+=1
        if tag=='a' and not self.hidden:
            href=dict(attrs).get('href')
            if href:self.anchor={'href':href,'text':[],'article':bool(self.article)}
    def handle_data(self,data):
        if self.anchor and not self.hidden:self.anchor['text'].append(data)
    def handle_endtag(self,tag):
        if tag=='a' and self.anchor:
            self.links.append(self.anchor);self.anchor=None
        if tag in ('script','style','noscript','nav','aside','footer'):self.hidden=max(0,self.hidden-1)
        if tag=='article':self.article=max(0,self.article-1)

def source_links(page,base_url):
    """Preserve publisher citations, never assert their truth or authority."""
    parser=SourceLinks();parser.feed(page)
    rows=[x for x in parser.links if x['article']] or parser.links
    out=[];seen=set()
    for row in rows:
        url=urlsplit(urljoin(base_url,row['href']))
        label=' '.join(' '.join(row['text']).split())
        if url.scheme not in ('http','https') or not url.hostname or url.username or url.password or not label:continue
        absolute=urlunsplit((url.scheme,url.netloc,url.path,url.query,''))
        if absolute in seen:continue
        seen.add(absolute)
        host=url.hostname.lower()
        official=host.endswith(('.gov','.gov.uk','.europa.eu'))
        out.append({'url':absolute,'anchor_text':label[:400],
            'link_scope':'article_element' if row['article'] else 'visible_page_candidate',
            'official_domain_candidate':official,'source_verified':False})
    return out[:100]

class PublicationMetadata(HTMLParser):
    def __init__(self):super().__init__();self.times=[]
    def handle_starttag(self,tag,attrs):
        a=dict(attrs)
        if tag=='meta' and (a.get('property') or a.get('name','')).lower() in ('article:published_time','datepublished','date'):
            if a.get('content'):self.times.append(a['content'])

def publication_metadata(page):
    """Publisher date is separate from feed time, never silently overwrites it."""
    p=PublicationMetadata();p.feed(page)
    for m in re.finditer(r'<script\b[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',page,re.S|re.I):
        try:
            def visit(x):
                if isinstance(x,dict):
                    types=x.get('@type',[]);types=[types] if isinstance(types,str) else types
                    if any(t in ('Article','NewsArticle','ReportageNewsArticle') for t in types) and isinstance(x.get('datePublished'),str):p.times.append(x['datePublished'])
                    for child in x.values():visit(child)
                elif isinstance(x,list):
                    for child in x:visit(child)
            visit(json.loads(m[1]))
        except ValueError:pass
    values=sorted(set(p.times))
    return {'publisher_publication_candidates':values,'publisher_published_at':values[0] if len(values)==1 else None,
            'publisher_publication_status':'unique_structured_candidate' if len(values)==1 else 'ambiguous_or_absent'}

class PageText(HTMLParser):
    def __init__(self):
        super().__init__();self.parts=[];self.article_parts=[];self.hidden=0;self.article=0
    def handle_starttag(self,tag,attrs):
        if tag in ('script','style','noscript','nav','aside','footer'):self.hidden+=1
        if tag=='article':self.article+=1
    def handle_endtag(self,tag):
        if tag in ('script','style','noscript','nav','aside','footer'):self.hidden=max(0,self.hidden-1)
        if tag=='article':self.article=max(0,self.article-1)
    def handle_data(self,data):
        if not self.hidden and data.strip():
            self.parts.append(data.strip())
            if self.article:self.article_parts.append(data.strip())

def get_page(url):
    req=urllib.request.Request(url,headers={'User-Agent':'FullscanNewsIntake/1.0 (+https://github.com/SRoyaltyy/fullscan)','Accept':'text/html,application/json,*/*'})
    with urllib.request.urlopen(req,timeout=12) as response:
        data=response.read(8_000_001)
        if len(data)>8_000_000:raise ValueError('Response exceeds byte cap')
        return data,response.headers.get('Content-Type','')

def extract_page(page):
    p=PageText();p.feed(page)
    candidates=[]
    def visit(value):
        if isinstance(value,dict):
            if value.get('articleBody') and isinstance(value['articleBody'],str):candidates.append(value['articleBody'])
            for child in value.values():visit(child)
        elif isinstance(value,list):
            for child in value:visit(child)
    for m in re.finditer(r'<script\b[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',page,re.S|re.I):
        try:visit(json.loads(m[1]))
        except ValueError:pass
    if candidates:return max(candidates,key=len),'structured_article_body'
    if len(' '.join(p.article_parts))>=600:return ' '.join(p.article_parts),'html_article_candidate'
    return ' '.join(p.parts),'visible_page_candidate'

def enrich_document(doc,resolver,fetch=get_page):
    out=dict(doc);attempt={'attempted_epoch':time.time(),'attempts':doc.get('review_enrichment',{}).get('attempts',0)+1,
                          'status':'attempted','network_requests':[],'historical_body_availability_verified':False}
    try:
        prior=doc.get('review_enrichment',{}).get('network_requests',[])
        direct=doc.get('extracted_url') or next((x['url'] for x in reversed(prior) if 'news.google.com' not in x['url']),None)
        if direct:url=direct
        else:url,_=resolver(doc['url'],fetch,audit=attempt['network_requests'])
        attempt['network_requests'].append({'method':'GET','url':url})
        data,mime=fetch(url)
        if 'html' not in mime:raise ValueError('Publisher response is not HTML')
        page=data.decode('utf-8','replace');body,method=extract_page(page)
        if re.search(r'\b(?:verify you are human|captcha|enable javascript and cookies)\b',body,re.I):raise ValueError('Access challenge; no bypass')
        words={w for w in re.findall(r'\w+',doc['title'].lower()) if len(w)>3}
        overlap=len(words&set(re.findall(r'\w+',body.lower())))/max(len(words),1)
        if len(body)<600 or overlap<.65:raise ValueError('Thin or mismatched publisher text')
        out.update(retained_feed_body=doc.get('retained_feed_body',doc.get('body','')),body=body,
                   extraction_status='page_text',extracted_url=url)
        out.update(publication_metadata(page))
        out['publisher_source_links']=source_links(page,url)
        attempt.update(status='page_text_checked',method=method,title_token_overlap=round(overlap,3),
                       note='Retrieved now; boundary candidate requires independent review')
    except Exception as exc:attempt.update(status='failed',error=str(exc)[:300])
    out['review_enrichment']=attempt
    return out

def retrieve_linked_primary_sources(doc,fetch=get_page,limit=2):
    """Fetch bounded official-domain article citations as unverified evidence.

    Raw text is kept separate from curated facts. A cited government page can
    still be outdated or irrelevant; neither a link nor its domain resolves that.
    """
    out=dict(doc);sources=[];audit=[];seen=set()
    for link in doc.get('publisher_source_links',[]):
        if len(audit)>=limit:break
        url=link.get('url','');parts=urlsplit(url);host=(parts.hostname or '').lower()
        if parts.scheme!='https' or parts.username or parts.password:continue
        if not host.endswith(('.gov','.gov.uk','.europa.eu')) or link.get('link_scope')!='article_element':continue
        if url in seen:continue
        seen.add(url);attempt={'method':'GET','url':url,'status':'attempted'};audit.append(attempt)
        try:
            data,mime=fetch(url)
            if 'html' not in mime:raise ValueError('Linked source is not HTML')
            page=data.decode('utf-8','replace');text,method=extract_page(page)
            if len(text)<250 or re.search(r'\b(?:verify you are human|captcha|enable javascript and cookies)\b',text,re.I):raise ValueError('Thin source or access challenge')
            sources.append({'source_url':url,'cited_by':doc.get('extracted_url') or doc.get('url'),
                'anchor_text':link.get('anchor_text'),'known_at':datetime.now(timezone.utc).isoformat(),
                'text':text[:6000],'original_text_characters':len(text),'truncated':len(text)>6000,
                'fetched_page_sha256':hashlib.sha256(data).hexdigest(),'extraction_method':method,
                'relevance_verified':False,'claims_verified':False,
                'status':'linked_official_domain_source_candidate',**publication_metadata(page)})
            attempt['status']='retrieved_candidate'
        except Exception as exc:attempt.update(status='failed',error=str(exc)[:300])
    out['linked_primary_source_candidates']=sources;out['linked_primary_source_requests']=audit
    return out
