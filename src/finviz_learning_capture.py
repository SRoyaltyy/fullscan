"""Dispatchable, immutable Finviz learning captures. No LLM, cache fallback or ranking.

Full-page parsing is fail-closed: a summary cell, news headline or price fallback
cannot certify a full narrative. Authenticated live validation happens in Actions.
"""
from __future__ import annotations
import argparse,csv,gzip,hashlib,io,json,os,re,time
from collections import Counter
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse,quote
from zoneinfo import ZoneInfo
import requests
from bs4 import BeautifulSoup
from . import finviz_session

ET=ZoneInfo('America/New_York')
SECTORS={'Basic Materials','Communication Services','Consumer Cyclical','Consumer Defensive','Energy','Financial','Healthcare','Industrials','Real Estate','Technology','Utilities'}
EMPTY={'','-','nan','none','n/a','no daily digest available','daily digest is not available','no digest available'}
FULL_OK={'full_panel_captured','source_explicitly_no_digest'}

def now():return datetime.now(ET).isoformat()
def digest_hash(text):return hashlib.sha256(text.encode('utf-8')).hexdigest()
def atomic(path:Path,data:bytes):
    path.parent.mkdir(parents=True,exist_ok=True);tmp=path.with_name(path.name+'.tmp');tmp.write_bytes(data);tmp.replace(path)
def write_json(path,value):atomic(path,(json.dumps(value,ensure_ascii=False,indent=2)+'\n').encode())
def write_rows(path,rows):
    raw=''.join(json.dumps(r,ensure_ascii=False,separators=(',',':'))+'\n' for r in rows).encode()
    atomic(path,gzip.compress(raw,mtime=0))
def read_rows(path):
    return [json.loads(line) for line in gzip.decompress(path.read_bytes()).decode().splitlines() if line.strip()]
def phase_at(stamp):
    d=datetime.fromisoformat(stamp).astimezone(ET);h=d.hour*60+d.minute
    return 'preopen' if h<570 else 'intraday' if h<960 else 'postclose'
def clean_text(value):return str(value or '').strip()
def nonempty(value):return clean_text(value).lower() not in EMPTY

def validate_csv(raw):
    text=raw.decode('utf-8-sig');reader=csv.DictReader(io.StringIO(text))
    required={'Ticker','Sector','Industry','Daily Digest'}
    if not required<=set(reader.fieldnames or []):raise ValueError('export_missing_required_columns')
    rows=list(reader)
    if not rows:raise ValueError('export_empty')
    seen=set()
    for row in rows:
        ticker=clean_text(row.get('Ticker'))
        if not re.fullmatch(r'[A-Za-z0-9.^/_-]{1,30}',ticker):raise ValueError('invalid_export_ticker')
        if ticker in seen:raise ValueError('duplicate_export_ticker')
        seen.add(ticker)
    return rows

def stock_rows(rows):
    return [r for r in rows if r['Sector'] in SECTORS and r['Industry']!='Exchange Traded Fund']

def parse_full_panel(html,ticker,summary=''):
    """Accept only an explicitly labelled Daily Digest panel with distinct body.

    Selectors are candidates, not live-verified site assumptions. If Finviz renders
    its panel through an unsupported script/API, return unverified and stop the fanout.
    """
    if finviz_session.looks_like_login_html(html):return {'status':'login_or_empty_page'}
    summary=summary or ''
    soup=BeautifulSoup(html,'html.parser');candidates=[]
    for node in soup.select('[id], [data-testid], [data-component]'):
        identity=' '.join(str(node.get(k,'')) for k in ['id','data-testid','data-component'])
        if re.search(r'daily[-_ ]?digest',identity,re.I) and node.name not in {'script','td','tr','table'}:
            candidates.append((node,'explicit_daily_digest_identifier'))
    for heading in soup.select('h1,h2,h3,h4,h5,h6'):
        if re.match(r'^Daily\s+Digest(?:\s*[:—–-].*)?$',heading.get_text(' ',strip=True),re.I):
            parent=heading.parent
            for _ in range(3):
                if not parent or parent.name in {'body','html','table','form'}:break
                candidates.append((parent,'daily_digest_heading'));parent=parent.parent
    for node,selector in candidates:
        if node.select_one('table.news-table,#news,form,input') or node.find_parent('table',class_=re.compile('snapshot',re.I)):continue
        if any(re.search(r'^(?:read more|show more|show full|view full|unlock|upgrade)',n.get_text(' ',strip=True),re.I) for n in node.select('button,a')):continue
        copy=BeautifulSoup(str(node),'html.parser')
        for n in copy.select('script,style,button,svg,h1,h2,h3,h4,h5,h6'):n.decompose()
        paragraphs=[p.get_text(' ',strip=True) for p in copy.select('p,li') if p.get_text(' ',strip=True)]
        text='\n\n'.join(paragraphs) if paragraphs else copy.get_text('\n',strip=True)
        text=re.sub(r'^Daily\s+Digest\s*\n?','',text,flags=re.I).strip()
        if text.lower() in EMPTY and text:
            return {'status':'source_explicitly_no_digest','evidence_text':text,'source_field':selector}
        # A teaser mirroring the export is not independently established as full text.
        if len(text)<80 or text==summary.strip():continue
        if not paragraphs:continue
        if copy.select_one('a[href*="login"], a[href*="upgrade"]'):continue
        timestamp=node.select_one('time')
        return {'status':'full_panel_captured','full_text':text,'full_text_sha256':digest_hash(text),'source_field':selector,'fullness_evidence':'distinct paragraphs in explicitly identified Daily Digest panel','source_updated_at':timestamp.get('datetime') if timestamp else None,'source_updated_raw':timestamp.get_text(' ',strip=True) if timestamp else None}
    # A labelled empty snapshot cell alone proves no export digest, not no full panel.
    return {'status':'full_source_structure_unverified','diagnostics':{'daily_digest_headings':[h.get_text(' ',strip=True)[:140] for h in soup.select('h1,h2,h3,h4,h5,h6') if 'digest' in h.get_text(' ',strip=True).lower()][:8],'explicit_panel_candidates':len(candidates)}}

def page_request(session,ticker):
    url='https://elite.finviz.com/quote.ashx?t='+quote(ticker,safe='')
    started=now()
    finviz_session._pace()
    try:r=session.get(url,timeout=30)
    except requests.RequestException as e:return {'status':'request_failed','error_type':type(e).__name__,'source_url':url,'request_started_at':started,'captured_at':now()}
    result={'source_url':url,'request_started_at':started,'captured_at':now(),'http_status':r.status_code}
    if r.status_code==429:result['status']='rate_limited';return result
    if r.status_code in (401,403):result['status']='access_denied';return result
    if r.status_code!=200:result['status']='http_error';return result
    result.update(status='page_received',html=r.text,response_sha256=hashlib.sha256(r.content).hexdigest());return result

class BrowserPanels:
    """Render the actual page and click its matching headline; never guess an API.

    Cookies stay in memory. Only the extracted public narrative and source clocks
    are archived; browser storage, account cookies and whole HTML are not saved.
    """
    def __init__(self,session):
        from playwright.sync_api import sync_playwright
        self.driver=sync_playwright().start();self.browser=self.driver.chromium.launch(headless=True)
        self.context=self.browser.new_context(user_agent=finviz_session.UA['User-Agent'])
        cookies=[{'name':x.name,'value':x.value,'domain':x.domain,'path':x.path or '/','secure':x.secure} for x in session.cookies if x.domain.endswith('finviz.com')]
        if cookies:self.context.add_cookies(cookies)
        self.page=self.context.new_page();self.blocked=None
        self.page.on('response',self._response)
    def _response(self,response):
        if urlparse(response.url).hostname in {'finviz.com','elite.finviz.com'} and response.request.resource_type in {'document','xhr','fetch'} and response.status in {401,403,429}:
            self.blocked='rate_limited' if response.status==429 else 'access_denied'
    def close(self):
        self.context.close();self.browser.close();self.driver.stop()
    def capture(self,ticker,summary):
        self.blocked=None;url='https://elite.finviz.com/quote.ashx?t='+quote(ticker,safe='');started=now()
        result={'source_url':url,'request_started_at':started,'capture_method':'authenticated_browser_expansion'}
        try:
            finviz_session._pace();response=self.page.goto(url,wait_until='domcontentloaded',timeout=30000)
            result['http_status']=response.status if response else None
            self.page.wait_for_timeout(700)
            html=self.page.content();parsed=parse_full_panel(html,ticker,summary)
            if parsed['status'] not in FULL_OK and nonempty(summary):
                headline=self.page.get_by_text(summary,exact=False)
                if headline.count():
                    headline.last.click(timeout=6000)
                    self.page.wait_for_timeout(1200)
                    parsed=parse_full_panel(self.page.content(),ticker,summary)
                    if parsed['status'] not in FULL_OK:
                        dialogs=self.page.locator('[role="dialog"], dialog').filter(has_text=summary)
                        if dialogs.count():
                            dialog=dialogs.last
                            if dialog.is_visible():
                                # Proven context: the matching digest headline opened this dialog.
                                parsed=parse_full_panel('<section id="daily-digest">'+dialog.inner_html()+'</section>',ticker,summary)
                                if parsed['status'] in FULL_OK:parsed['source_field']='expanded_matching_headline_dialog'
            result.update(parsed);result['rendered_dom_sha256']=digest_hash(self.page.content())
            if self.blocked:result={'status':self.blocked,**{k:v for k,v in result.items() if k not in {'status','full_text','full_text_sha256'}}}
        except Exception as e:result.update(status=self.blocked or 'browser_capture_failed',error_type=type(e).__name__)
        result['captured_at']=now();return result

def capture_one(session,row,renderer=None,browser_only=False):
    received=renderer.capture(row['ticker'],row.get('export_text')) if browser_only and renderer else page_request(session,row['ticker']);html=received.pop('html','')
    result={**row,**received}
    if received['status']=='page_received':result.update(parse_full_panel(html,row['ticker'],row['export_text']))
    if renderer and not browser_only and result['status']=='full_source_structure_unverified':result.update(renderer.capture(row['ticker'],row.get('export_text')))
    result['full_status']=result.pop('status')
    result['observed_phase']=phase_at(result['captured_at']);result['source_event_date']=None
    return result

def live_export(session):
    configured=(os.environ.get('FINVIZ_EXPORT') or '').strip()
    urls=[]
    if configured:
        parsed=urlparse(configured)
        if parsed.scheme!='https' or parsed.hostname not in {'elite.finviz.com','finviz.com'}:raise ValueError('configured_export_host_not_finviz_https')
        urls.append((configured,'configured_live_export'))
    if finviz_session.authed(session):urls.append(('https://elite.finviz.com/export.ashx?v=151','elite_custom_export'))
    for url,label in urls:
        finviz_session._pace()
        try:r=session.get(url,timeout=90)
        except requests.RequestException:continue
        if r.status_code in (401,403,429):raise ValueError('export_access_denied_or_rate_limited')
        if r.status_code!=200:continue
        try:rows=validate_csv(r.content)
        except (ValueError,UnicodeError):continue
        return r.content,rows,{'origin':'live_http','endpoint_label':label,'captured_at':now(),'sha256':hashlib.sha256(r.content).hexdigest()}
    raise ValueError('no_live_export_with_daily_digest_column')

def latest_membership(root,date):
    paths=sorted(p for p in (root/'data/universe').glob('*_membership.csv') if p.name[:10]<=date)
    if not paths:return {},None
    p=paths[-1]
    with p.open() as handle:
        rows={r['Ticker']:r for r in csv.DictReader(handle) if r.get('sector') in SECTORS and r.get('industry')!='Exchange Traded Fund'}
    return rows,str(p.relative_to(root))

def export_record(row,capture_id,export_meta):
    text=clean_text(row.get('Daily Digest'));valid=nonempty(text)
    return {'ticker':row['Ticker'],'company':row.get('Company'),'sector':row['Sector'],'industry':row['Industry'],'capture_id':capture_id,'export_text':text if valid else None,'export_text_sha256':digest_hash(text) if valid else None,'export_status':'short_summary_received' if valid else 'blank_in_export_unresolved','full_status':'not_attempted','export_captured_at':export_meta.get('captured_at'),'export_origin':export_meta.get('origin'),'price_snapshot':row.get('Price'),'change_snapshot':row.get('Change'),'price_window_status':'export snapshot only; not event-aligned','source_event_date':None}

def prepare(args):
    started=now();date=args.date or started[:10]
    if not re.fullmatch(r'\d{4}-\d{2}-\d{2}',date):raise ValueError('invalid_date')
    if date!=started[:10]:raise ValueError('historical_date_cannot_be_fetched_live')
    tag=re.sub(r'[^A-Za-z0-9_-]','',args.capture_id)
    if not tag:raise ValueError('capture_id_required')
    dest=args.root/'data/finviz_learning'/date/tag
    if dest.exists():raise ValueError('capture_path_already_exists')
    dest.mkdir(parents=True)
    manifest={'schema_version':1,'capture_id':date+'/'+tag,'session_date':date,'started_at':started,'requested_phase':args.phase,'observed_start_phase':phase_at(started),'mode':args.mode,'shards':args.shards,'source_commit':os.environ.get('GITHUB_SHA'),'run_url':os.environ.get('GITHUB_SERVER_URL','https://github.com')+'/'+os.environ.get('GITHUB_REPOSITORY','SRoyaltyy/fullscan')+'/actions/runs/'+os.environ.get('GITHUB_RUN_ID','local'),'full_source_verified_in_this_run':False,'expected_universe':'live export stock labels union latest saved membership; unresolved absences fail coverage'}
    membership,mpath=latest_membership(args.root,date);manifest['membership_used']=mpath
    session=finviz_session.session();rows=[];export_meta={};error=None
    try:
        raw,allrows,export_meta=live_export(session);rows=stock_rows(allrows)
        if not rows:raise ValueError('live_export_has_no_stock_rows')
        atomic(dest/'export.csv.gz',gzip.compress(raw,mtime=0));manifest['all_export_rows']=len(allrows)
    except ValueError as e:error=str(e)
    manifest['export']=export_meta;manifest['prepare_error']=error
    records={r['Ticker']:export_record(r,manifest['capture_id'],export_meta) for r in rows}
    for ticker,m in membership.items():
        if ticker not in records:records[ticker]={'ticker':ticker,'company':None,'sector':m['sector'],'industry':m['industry'],'capture_id':manifest['capture_id'],'export_text':None,'export_text_sha256':None,'export_status':'missing_from_live_export' if not error else 'live_export_failed','full_status':'not_attempted','export_origin':None,'export_captured_at':None}
    manifest['expected_tickers']=sorted(records);manifest['expected_count']=len(records)
    manifest['missing_expected_from_live_export']=sorted(set(membership)-{r['Ticker'] for r in rows})
    probe=[];renderer=None
    if args.mode=='full' and not error and finviz_session.authed(session):
        try:renderer=BrowserPanels(session)
        except Exception as e:manifest['browser_setup_error']=type(e).__name__
        available=[t for t,r in records.items() if r.get('export_text')]
        selected=[t for t in ['RXO','NVDA','AMD'] if t in available]
        selected+=sorted(set(available)-set(selected))[:max(0,5-len(selected))]
        for ticker in selected:
            result=capture_one(session,records[ticker],renderer);records[ticker]=result
            probe.append({'ticker':ticker,'status':result['full_status'],'source_url':result.get('source_url'),'source_field':result.get('source_field'),'diagnostics':result.get('diagnostics'),'http_status':result.get('http_status')})
            if result['full_status'] in {'rate_limited','access_denied'}:break
        manifest['full_source_verified_in_this_run']=bool(probe and all(p['status'] in FULL_OK for p in probe) and any(p['status']=='full_panel_captured' for p in probe))
        manifest['full_backend']='browser' if any(r.get('capture_method')=='authenticated_browser_expansion' for r in records.values()) else 'http_panel'
    elif args.mode=='full' and not error:manifest['prepare_error']='elite_auth_required_for_full_pages'
    if renderer:renderer.close()
    manifest['probe']=probe
    if args.mode=='full' and not manifest['full_source_verified_in_this_run']:
        for r in records.values():
            if r['full_status']=='not_attempted':r['full_status']='not_attempted_source_preflight_failed'
    write_rows(dest/'baseline.jsonl.gz',list(records.values()));write_json(dest/'manifest.json',manifest)
    relative=str(dest.relative_to(args.root));write_json(args.root/'capture_location.json',{'path':relative})
    if os.environ.get('GITHUB_OUTPUT'):
        with open(os.environ['GITHUB_OUTPUT'],'a') as out:
            out.write('path='+relative+'\nfull_ready='+str(manifest['full_source_verified_in_this_run']).lower()+'\nmatrix='+json.dumps({'shard':list(range(args.shards))})+'\n')
    print(json.dumps({'capture_path':relative,'expected':len(records),'export_error':manifest['prepare_error'],'full_ready':manifest['full_source_verified_in_this_run']}))

def shard(args):
    dest=args.root/args.capture_path;manifest=json.loads((dest/'manifest.json').read_text());records=read_rows(dest/'baseline.jsonl.gz')
    if not manifest['full_source_verified_in_this_run']:raise ValueError('unverified_source_fanout_forbidden')
    if not 0<=args.shard<manifest['shards']:raise ValueError('invalid_shard')
    selected=[r for i,r in enumerate(sorted(records,key=lambda r:r['ticker'])) if i%manifest['shards']==args.shard]
    session=finviz_session.session();deadline=time.monotonic()+args.budget_minutes*60;stopped=None;renderer=None
    if not finviz_session.authed(session):stopped='auth_failed'
    if not stopped and manifest.get('full_backend')=='browser':
        try:renderer=BrowserPanels(session)
        except Exception:stopped='browser_setup_failed'
    path=dest/'shards'/f'{args.shard:02}.jsonl.gz'
    write_rows(path,selected) # preserves a checkpoint if a later process is killed
    for index,row in enumerate(selected):
        if row['full_status'] in FULL_OK:continue
        if time.monotonic()>=deadline:stopped=stopped or 'time_budget_exhausted'
        if stopped:selected[index]={**row,'full_status':'not_attempted_'+stopped};continue
        result=capture_one(session,row,renderer,browser_only=manifest.get('full_backend')=='browser');selected[index]=result
        if result['full_status'] in {'rate_limited','access_denied','login_or_empty_page'}:stopped=result['full_status']
        if index%20==0:write_rows(path,selected);print(json.dumps({'shard':args.shard,'processed':index+1,'total':len(selected)}),flush=True)
    write_rows(path,selected)
    if renderer:renderer.close()
    print(json.dumps({'shard':args.shard,'statuses':dict(Counter(r['full_status'] for r in selected))}))

def aggregate(args):
    dest=args.root/args.capture_path;manifest=json.loads((dest/'manifest.json').read_text());baseline=read_rows(dest/'baseline.jsonl.gz');records={r['ticker']:r for r in baseline};found=[]
    for p in sorted((dest/'shards').glob('*.jsonl.gz')) if (dest/'shards').exists() else []:
        found.append(p.stem)
        for row in read_rows(p):
            if row['capture_id']!=manifest['capture_id'] or row['ticker'] not in records:raise ValueError('foreign_shard_record')
            records[row['ticker']]=row
    rows=sorted(records.values(),key=lambda r:r['ticker'])
    for row in rows:
        if args.mode=='full' and row['full_status']=='not_attempted':row['full_status']='capture_shard_missing'
    counts=Counter(r['full_status'] for r in rows);export_counts=Counter(r['export_status'] for r in rows)
    bulk_ok=bool(rows and manifest.get('export',{}).get('origin')=='live_http' and not manifest['missing_expected_from_live_export'] and not manifest['prepare_error'])
    full_ok=bool(bulk_ok and all(r['full_status'] in FULL_OK for r in rows))
    stamps=[r.get('captured_at') or r.get('export_captured_at') for r in rows if r.get('captured_at') or r.get('export_captured_at')]
    phase_ok=args.phase=='auto' or bool(stamps and all(phase_at(s)==args.phase for s in stamps))
    complete=(full_ok if args.mode=='full' else bulk_ok) and phase_ok
    coverage={'capture_id':manifest['capture_id'],'mode':args.mode,'expected_tickers':len(rows),'export_statuses':dict(export_counts),'full_statuses':dict(counts),'bulk_export_complete':bulk_ok,'full_text_complete':full_ok,'requested_phase':args.phase,'phase_window_valid':phase_ok,'capture_time_min':min(stamps) if stamps else None,'capture_time_max':max(stamps) if stamps else None,'complete_for_requested_mode':complete,'status':'complete' if complete else 'partial_or_failed','source_time_status':'publication time unknown unless explicitly recorded; live fetch is not proof of new news','found_shards':found,'prepare_error':manifest['prepare_error'],'expected_universe':manifest['expected_universe']}
    write_rows(dest/'records.jsonl.gz',rows);write_json(dest/'coverage.json',coverage)
    lines=['# Finviz learning capture','',f"- Capture: `{manifest['capture_id']}`.",f"- Mode: **{args.mode}**; result: **{coverage['status']}**.",f"- Expected tickers: **{len(rows)}**.",f"- Export states: `{dict(export_counts)}`.",f"- Full-text states: `{dict(counts)}`.",f"- Full-text complete: **{full_ok}**.",f"- Requested phase: {args.phase}; valid: {phase_ok}.",'- Each record retains its ticker, original text, hashes, capture clock and source status. No text-prefix deduplication or ranking.', '- A failed preflight keeps the export and diagnostics and prevents thousands of speculative page requests.','- Full panel selectors need the first authenticated live run to validate Finviz’s current page structure.','- All capture versions remain under their unique run/attempt folder. Export mode never certifies full narratives.']
    atomic(dest/'report.md',('\n'.join(lines)+'\n').encode());print(json.dumps(coverage));return complete

def main():
    p=argparse.ArgumentParser();p.add_argument('--root',type=Path,default=Path('.'));sub=p.add_subparsers(dest='command',required=True)
    prepare_p=sub.add_parser('prepare');prepare_p.add_argument('--date',default='');prepare_p.add_argument('--capture-id',required=True);prepare_p.add_argument('--mode',choices=['full','export'],default='full');prepare_p.add_argument('--phase',choices=['auto','preopen','intraday','postclose'],default='auto');prepare_p.add_argument('--shards',type=int,choices=[8,16,32],default=16)
    shard_p=sub.add_parser('shard');shard_p.add_argument('--capture-path',required=True);shard_p.add_argument('--shard',type=int,required=True);shard_p.add_argument('--budget-minutes',type=int,default=60)
    aggregate_p=sub.add_parser('aggregate');aggregate_p.add_argument('--capture-path',required=True);aggregate_p.add_argument('--mode',choices=['full','export'],required=True);aggregate_p.add_argument('--phase',choices=['auto','preopen','intraday','postclose'],default='auto')
    a=p.parse_args()
    if a.command=='prepare':prepare(a)
    elif a.command=='shard':shard(a)
    else:
        ok=aggregate(a)
        if not ok:raise SystemExit(2)
if __name__=='__main__':main()
