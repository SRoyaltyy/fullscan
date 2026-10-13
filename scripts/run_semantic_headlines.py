"""Runtime benchmark runner: title-only input, never loads judge/reference labels."""
import argparse,hashlib,json,pathlib,time,signal
from datetime import datetime,timezone
from src.news_impact.semantic_headlines import SemanticHeadlineClient

class DeadlineExceeded(BaseException):
    """Bypasses provider Exception catches so the whole headline is bounded."""

def deadline_handler(signum,frame):
    raise DeadlineExceeded()

def main():
    p=argparse.ArgumentParser();p.add_argument('--input',type=pathlib.Path,required=True);p.add_argument('--output',type=pathlib.Path,required=True);p.add_argument('--limit',type=int,default=20);p.add_argument('--providers',default='');p.add_argument('--budget-seconds',type=int,default=1000);p.add_argument('--headline-timeout',type=int,default=180);p.add_argument('--no-review',action='store_true');args=p.parse_args()
    if args.headline_timeout<=0 or args.budget_seconds<=0:p.error('Budgets must be positive')
    if not hasattr(signal,'SIGALRM'):p.error('Bounded runner requires POSIX SIGALRM')
    signal.signal(signal.SIGALRM,deadline_handler)
    selected=json.loads(args.input.read_text());client=SemanticHeadlineClient(args.providers.split(',') if args.providers else None,review=not args.no_review)
    titles=[r['title'].strip().casefold() for r in selected[:args.limit]]
    if len(set(titles))!=len(titles):p.error('Duplicate headlines in requested sample')
    result={'generated_at':datetime.now(timezone.utc).isoformat(),'scope':'title_only_live_models; zero reference labels or deterministic classification supplied','input_sha256':hashlib.sha256(args.input.read_bytes()).hexdigest(),'requested':min(args.limit,len(selected)),'outputs':[]}
    args.output.parent.mkdir(parents=True,exist_ok=True);start=time.monotonic()
    def save():args.output.write_text(json.dumps(result,ensure_ascii=False,indent=2))
    save()
    for row in selected[:args.limit]:
        remaining=args.budget_seconds-(time.monotonic()-start)
        if remaining<=0:result['stop_reason']='elapsed_budget';break
        title_start=time.monotonic()
        signal.setitimer(signal.ITIMER_REAL,min(args.headline_timeout,remaining))
        try:out=client.analyze(row['title'])
        except DeadlineExceeded:
            out={'status':'unavailable','model_output':None,'provider':None,'model':None,'attempts':[{'status':'headline_deadline_exceeded'}],'elapsed_seconds':round(time.monotonic()-title_start,2)}
        finally:signal.setitimer(signal.ITIMER_REAL,0)
        result['outputs'].append({'n':row['n'],'title':row['title'],'program_output':out});save()
        print(json.dumps({'n':row['n'],'status':out['status'],'provider':out['provider'],'model':out['model'],'elapsed_seconds':out['elapsed_seconds']}),flush=True)
        if out['status']=='unavailable' and not any(a['status'] not in ('credential_unavailable','no_allowlisted_classification_model') for a in out['attempts']):
            result['stop_reason']='no_available_credentials_or_models';break
    result['completed']=len(result['outputs']);result['analyzed']=sum(r['program_output']['status']=='analyzed' for r in result['outputs']);save()

if __name__=='__main__':main()
