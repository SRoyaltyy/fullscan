"""Runtime benchmark runner: title-only input, never loads judge/reference labels."""
import argparse,hashlib,json,os,pathlib,time,signal
from datetime import datetime,timezone
from src.news_impact.semantic_headlines import SemanticHeadlineClient,rate_limit_only

class DeadlineExceeded(BaseException):
    """Bypasses provider Exception catches so the whole headline is bounded."""

def deadline_handler(signum,frame):
    raise DeadlineExceeded()

def no_available_credentials(outputs):
    return bool(outputs) and all(
        out['status']=='unavailable' and bool(out.get('attempts')) and
        all(a['status'] in ('credential_unavailable','no_allowlisted_classification_model') for a in out['attempts'])
        for out in outputs)

def restore_checkpoint(previous,current,rows):
    """Resume only the same immutable candidate/input, preserving first execution."""
    for key in ('input_sha256','runtime_sha256','configuration','catalog','requested'):
        if key not in previous or json.dumps(previous[key],sort_keys=True)!=json.dumps(current[key],sort_keys=True):
            raise ValueError('Resume checkpoint mismatch: '+key)
    if not isinstance(previous.get('generated_at'),str):raise ValueError('Missing checkpoint start time')
    expected={r['n']:r['title'] for r in rows};seen=set()
    if not isinstance(previous.get('outputs'),list):raise ValueError('Missing checkpoint outputs')
    for row in previous['outputs']:
        n=row.get('n')
        if type(n) is not int or n in seen or n not in expected or row.get('title')!=expected[n]:
            raise ValueError('Checkpoint row identity mismatch')
        seen.add(n);out=row.get('program_output')
        if not isinstance(out,dict) or out.get('status') not in ('analyzed','unavailable'):
            raise ValueError('Invalid checkpoint row status')
        if out['status']=='analyzed' and (out.get('model_output') is None or not isinstance(out.get('analysis_output'),dict)):
            raise ValueError('Analyzed checkpoint row has no complete analysis')
    restored=dict(previous)
    restored['resume_history']=previous.get('resume_history',[])+[{
        'resumed_at':datetime.now(timezone.utc).isoformat(),
        'previous_stop_reason':previous.get('stop_reason'),
        'preserved_analyzed':sum(r['program_output']['status']=='analyzed' for r in previous['outputs'])}]
    restored.pop('stop_reason',None)
    return restored

def pending_rows(rows,outputs):
    done={r['n'] for r in outputs if r['program_output']['status']=='analyzed'}
    return [r for r in rows if r['n'] not in done]

def resumable_drafts(rows,outputs):
    expected={r['n'] for r in rows};drafts={}
    for row in outputs:
        out=row['program_output'];draft=out.get('draft_analysis')
        if isinstance(draft,dict) and draft.get('status')!='analyzed':
            draft=draft.get(row['n'],draft.get(str(row['n'])))
        if row['n'] in expected and out['status']=='unavailable' and isinstance(draft,dict) and draft.get('status')=='analyzed':
            drafts[row['n']]=draft
    return drafts

def batch_rate_limited(outputs):
    return any(out['status']=='unavailable' and any(
        rate_limit_only(record) for record in (out,out.get('fact_review'),out.get('sector_review')))
        for out in outputs)

def write_checkpoint(path,result):
    temporary=path.with_name(path.name+'.tmp')
    temporary.write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
    temporary.replace(path)

def main():
    p=argparse.ArgumentParser();p.add_argument('--input',type=pathlib.Path,required=True);p.add_argument('--output',type=pathlib.Path,required=True);p.add_argument('--limit',type=int,default=20);p.add_argument('--providers',default='');p.add_argument('--models',default='');p.add_argument('--budget-seconds',type=int,default=1000);p.add_argument('--headline-timeout',type=int,default=180);p.add_argument('--no-review',action='store_true');p.add_argument('--sector-review',action='store_true');p.add_argument('--action-review',action='store_true');p.add_argument('--sector-roles',action='store_true');p.add_argument('--batch-size',type=int,default=10);p.add_argument('--batch-interval-seconds',type=int,default=0);p.add_argument('--require-catalog',action='store_true');p.add_argument('--resume',action='store_true');args=p.parse_args()
    if args.action_review and not args.no_review:p.error('--action-review requires --no-review')
    if args.sector_roles and not args.no_review:p.error('--sector-roles requires --no-review')
    if args.headline_timeout<=0 or args.budget_seconds<=0:p.error('Budgets must be positive')
    if not 1<=args.batch_size<=10:p.error('Batch size must be 1..10')
    if not 0<=args.batch_interval_seconds<=60:p.error('Batch interval must be 0..60 seconds')
    if not hasattr(signal,'SIGALRM'):p.error('Bounded runner requires POSIX SIGALRM')
    signal.signal(signal.SIGALRM,deadline_handler)
    selected=json.loads(args.input.read_text());client=SemanticHeadlineClient(args.providers.split(',') if args.providers else None,review=not args.no_review,sector_review=args.sector_review,action_review=args.action_review,sector_roles=args.sector_roles,model_allowlist=args.models.split(',') if args.models else None)
    if args.require_catalog and not client.catalog.metadata['available']:p.error('Required company catalog is missing/empty')
    selected=selected[:args.limit]
    if any(type(r['n']) is not int for r in selected) or len({r['n'] for r in selected})!=len(selected):p.error('Duplicate/non-integer input ids')
    titles=[r['title'].strip().casefold() for r in selected]
    if len(set(titles))!=len(titles):p.error('Duplicate headlines in requested sample')
    result={'generated_at':datetime.now(timezone.utc).isoformat(),'scope':'title_only_live_models; zero reference labels or deterministic classification supplied','input_sha256':hashlib.sha256(args.input.read_bytes()).hexdigest(),'requested':min(args.limit,len(selected)),'outputs':[]}
    root=pathlib.Path(__file__).resolve().parent.parent
    sources=('src/lane_route.py','src/news_impact/semantic_headlines.py','src/news_impact/semantic_ontology.py','src/news_impact/semantic_catalog.py','src/news_impact/sector_batch_review.py','src/news_impact/schema.py','src/news_impact/semantic_adapter.py','scripts/run_semantic_headlines.py')
    result['runtime_sha256']={p:hashlib.sha256((root/p).read_bytes()).hexdigest() for p in sources}
    result['configuration']={'providers':client.provider_order,'models':client.model_allowlist,'review':client.review,'sector_review':client.sector_review,'action_review':client.action_review,'sector_roles':client.sector_roles,'headline_timeout':args.headline_timeout,'batch_size':args.batch_size,'batch_interval_seconds':args.batch_interval_seconds}
    if 'openclaw' in client.provider_order:
        raw_timeout=(os.environ.get('OPENCLAW_LANE_TIMEOUT') or '').strip()
        timeout=int(raw_timeout) if raw_timeout.isdigit() else 300
        result['configuration']['openclaw_transport']={'agent':(os.environ.get('OPENCLAW_AGENT') or 'openclaw/default').strip(),'timeout_seconds':max(30,timeout)}
    result['catalog']=client.catalog.metadata
    args.output.parent.mkdir(parents=True,exist_ok=True)
    if args.resume and args.output.exists():result=restore_checkpoint(json.loads(args.output.read_text()),result,selected)
    pending=pending_rows(selected,result['outputs'])
    start=time.monotonic()
    def save():
        result['completed']=len(result['outputs'])
        result['analyzed']=sum(r['program_output']['status']=='analyzed' for r in result['outputs'])
        result['updated_at']=datetime.now(timezone.utc).isoformat()
        write_checkpoint(args.output,result)
    save()
    for offset in range(0,len(pending),args.batch_size):
        rows=pending[offset:offset+args.batch_size]
        if offset and args.batch_interval_seconds:time.sleep(min(args.batch_interval_seconds,max(0,args.budget_seconds-(time.monotonic()-start))))
        remaining=args.budget_seconds-(time.monotonic()-start)
        if remaining<=0:result['stop_reason']='elapsed_budget';break
        title_start=time.monotonic()
        signal.setitimer(signal.ITIMER_REAL,min(args.headline_timeout,remaining))
        drafts=resumable_drafts(rows,result['outputs'])
        try:outs=client.analyze_many(rows,resume_drafts=drafts) if args.batch_size>1 or client.sector_review else [client.analyze(rows[0]['title'])]
        except DeadlineExceeded:
            out={'status':'unavailable','model_output':None,'provider':None,'model':None,'attempts':[{'status':'headline_deadline_exceeded','phase':client.phase}],'draft_analysis':client.last_draft,'elapsed_seconds':round(time.monotonic()-title_start,2)}
            # A timeout during subset recovery must not erase rows that have
            # already passed their own schema/evidence checks (and audit, if
            # required). Never turn an unreviewed draft into reviewed success.
            outs=[client.batch_progress.get(row['n'],out) for row in rows] if args.batch_size>1 else [out]
        finally:signal.setitimer(signal.ITIMER_REAL,0)
        by_n={r['n']:r for r in result['outputs']}
        for row,out in zip(rows,outs,strict=True):
            by_n[row['n']]={'n':row['n'],'title':row['title'],'program_output':out}
            print(json.dumps({'n':row['n'],'status':out['status'],'provider':out['provider'],'model':out['model'],'elapsed_seconds':out['elapsed_seconds']}),flush=True)
        result['outputs']=[by_n[r['n']] for r in selected if r['n'] in by_n]
        save()
        if batch_rate_limited(outs):
            result['stop_reason']='rate_limited';break
        if no_available_credentials(outs):
            result['stop_reason']='no_available_credentials_or_models';break
    result['completed']=len(result['outputs']);result['analyzed']=sum(r['program_output']['status']=='analyzed' for r in result['outputs']);save()

if __name__=='__main__':main()
