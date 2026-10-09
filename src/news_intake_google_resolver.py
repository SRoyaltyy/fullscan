"""Bounded Google News publisher URL resolution, stdlib only.

Protocol reference: MIT GoogleNewsDecoder by dbernheisel,
https://github.com/dbernheisel/google_news_decoder (read 2026-10-08).
Uses ordinary public requests; failures/rate limits remain explicit. No CAPTCHA,
consent, login or paywall bypass. Internal endpoint availability is not guaranteed.
"""
import html,json,re,urllib.parse,urllib.request

def post_form(url,form):
    req=urllib.request.Request(url,data=urllib.parse.urlencode(form).encode(),headers={
        'User-Agent':'CompanyResearch-news-review/1.0',
        'Content-Type':'application/x-www-form-urlencoded;charset=UTF-8'})
    with urllib.request.urlopen(req,timeout=15) as response:
        data=response.read(2_000_001)
        if len(data)>2_000_000:raise ValueError('Resolver response exceeds byte limit')
        return data.decode('utf-8')

def resolve_google_url(url,fetch,post=post_form,audit=None):
    audit=[] if audit is None else audit
    parsed=urllib.parse.urlparse(url)
    if parsed.hostname!='news.google.com':return url,[]
    m=re.search(r'/(?:articles|read)/([a-zA-Z0-9_-]+)',parsed.path)
    if not m:raise ValueError('Unsupported Google News URL')
    article_id=m[1];page='https://news.google.com/articles/'+article_id
    audit.append({'method':'GET','url':page})
    data,_=fetch(page);text=data.decode('utf-8')
    sg=re.search(r'data-n-a-sg=["\']([^"\']+)',text)
    ts=re.search(r'data-n-a-ts=["\'](\d+)',text)
    if not sg or not ts:raise ValueError('Google decoding parameters unavailable; no bypass attempted')
    args=['garturlreq',[["X","X",["X","X"],None,None,1,1,"US:en",None,1,None,None,None,None,None,0,1],
                       "X","X",1,[1,1,1],1,1,None,0,0,None,0],article_id,int(ts[1]),html.unescape(sg[1])]
    endpoint='https://news.google.com/_/DotsSplashUi/data/batchexecute'
    audit.append({'method':'POST','url':endpoint})
    response=post(endpoint,{'f.req':json.dumps([[["Fbv4je",json.dumps(args)]]])})
    # Search JSON lines/chunks; verify the expected RPC rather than a random URL.
    for line in response.splitlines():
        try:items=json.loads(line)
        except (ValueError,TypeError):continue
        if not isinstance(items,list):continue
        for row in items:
            if not isinstance(row,list) or len(row)<3 or row[1]!='Fbv4je':continue
            try:inner=json.loads(row[2])
            except (ValueError,TypeError):continue
            if isinstance(inner,list) and len(inner)>1 and inner[0]=='garturlres':
                target=inner[1];p=urllib.parse.urlparse(target)
                if p.scheme!='https' or not p.hostname or p.hostname.endswith('google.com'):
                    raise ValueError('Resolver did not return an HTTPS publisher URL')
                return target,audit
    raise ValueError('Publisher URL absent from decoding response')
