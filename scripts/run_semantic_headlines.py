"""Runtime benchmark runner: title-only input, never loads judge/reference labels."""
import argparse,hashlib,json,pathlib,time,signal
from datetime import datetime,timezone
from src.news_impact.semantic_headlines import SemanticHeadlineClient

class DeadlineExceeded(BaseException):
    """Bypasses provider Exception catches so the whole headline is bounded."""

def deadline_handler(signum,frame):
    raise DeadlineExceeded()

def main():
    p=argparse.ArgumentParser();p.add_argument('--input',type=pathlib.Path,required=True);p.add_argument('--output',type=pathlib.Path,required=True);p.add_argument('--limit',type=int,default=20);p.add_argument('--providers',default='');p.add_argument('--models',default='');p.add_argument('--budget-seconds',type=int,default=1000);p.add_argument('--headline-timeout',type=int,default=180);p.add_argument('--no-review',action='store_true');p.add_argument('--batch-size',type=int,default=10);p.add_argument('--batch-interval-seconds',type=int,default=0);p.add_argument('--require-catalog',action='store_true');args=p.parse_args()
    if args.headline_timeout<=0 or args.budget_seconds<=0:p.error('Budgets must be positive')
    if not 1<=args.batch_size<=10:p.error('Batch size must be 1..10')
    if not 0<=args.batch_interval_seconds<=60:p.error('Batch interval must be 0..60 seconds')
    if not hasattr(signal,'SIGALRM'):p.error('Bounded runner requires POSIX SIGALRM')
    signal.signal(signal.SIGALRM,deadline_handler)
    selected=json.loads(args.input.read_text());client=SemanticHeadlineClient(args.providers.split(',') if args.providers else None,review=not args.no_review,model_allowlist=args.models.split(',') if args.models else None)
    if args.require_catalog and not client.catalog.metadata['available']:p.error('Required company catalog is missing/empty')
    titles=[r['title'].strip().casefold() for r in selected[:args.limit]]
    if len(set(titles))!=len(titles):p.error('Duplicate headlines in requested sample')
    result={'generated_at':datetime.now(timezone.utc).isoformat(),'scope':'title_only_live_models; zero reference labels or deterministic classification supplied','input_sha256':hashlib.sha256(args.input.read_bytes()).hexdigest(),'requested':min(args.limit,len(selected)),'outputs':[]}
    root=pathlib.Path(__file__).resolve().parent.parent
    sources=('src/lane_route.py','src/news_impact/semantic_headlines.py','src/news_impact/semantic_ontology.py','src/news_impact/semantic_catalog.py','src/news_impact/schema.py','src/news_impact/semantic_adapter.py','scripts/run_semantic_headlines.py')
    result['runtime_sha256']={p:hashlib.sha256((root/p).read_bytes()).hexdigest() for p in sources}
    result['configuration']={'providers':client.provider_order,'models':client.model_allowlist,'review':client.review,'headline_timeout':args.headline_timeout,'batch_size':args.batch_size,'batch_interval_seconds':args.batch_interval_seconds}
    result['catalog']=client.catalog.metadata
    args.output.parent.mkdir(parents=True,exist_ok=True);start=time.monotonic()
    def save():args.output.write_text(json.dumps(result,ensure_ascii=False,indent=2))
    save()
    for offset in range(0,min(args.limit,len(selected)),args.batch_size):
        rows=selected[offset:min(offset+args.batch_size,args.limit)]
        if offset and args.batch_interval_seconds:time.sleep(min(args.batch_interval_seconds,max(0,args.budget_seconds-(time.monotonic()-start))))
        remaining=args.budget_seconds-(time.monotonic()-start)
        if remaining<=0:result['stop_reason']='elapsed_budget';break
        title_start=time.monotonic()
        signal.setitimer(signal.ITIMER_REAL,min(args.headline_timeout,remaining))
        try:outs=client.analyze_many(rows) if args.batch_size>1 else [client.analyze(rows[0]['title'])]
        except DeadlineExceeded:
            out={'status':'unavailable','model_output':None,'provider':None,'model':None,'attempts':[{'status':'headline_deadline_exceeded','phase':client.phase}],'draft_analysis':client.last_draft,'elapsed_seconds':round(time.monotonic()-title_start,2)}
            # A timeout during subset recovery must not erase rows that have
            # already passed their own schema/evidence checks (and audit, if
            # required). Never turn an unreviewed draft into reviewed success.
            outs=[client.batch_progress.get(row['n'],out) for row in rows] if args.batch_size>1 else [out]
        finally:signal.setitimer(signal.ITIMER_REAL,0)
        for row,out in zip(rows,outs,strict=True):
            result['outputs'].append({'n':row['n'],'title':row['title'],'program_output':out})
            print(json.dumps({'n':row['n'],'status':out['status'],'provider':out['provider'],'model':out['model'],'elapsed_seconds':out['elapsed_seconds']}),flush=True)
        save()
        if all(out['status']=='unavailable' and not any(a['status'] not in ('credential_unavailable','no_allowlisted_classification_model') for a in out['attempts']) for out in outs):
            result['stop_reason']='no_available_credentials_or_models';break
    result['completed']=len(result['outputs']);result['analyzed']=sum(r['program_output']['status']=='analyzed' for r in result['outputs']);save()

if __name__=='__main__':main()
