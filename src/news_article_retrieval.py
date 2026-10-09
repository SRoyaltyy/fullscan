"""Free bounded publisher retrieval with exact provenance and explicit failure."""
import json,re,time,urllib.request
from html.parser import HTMLParser

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
        attempt.update(status='page_text_checked',method=method,title_token_overlap=round(overlap,3),
                       note='Retrieved now; boundary candidate requires independent review')
    except Exception as exc:attempt.update(status='failed',error=str(exc)[:300])
    out['review_enrichment']=attempt
    return out
