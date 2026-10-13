"""Runtime benchmark runner: title-only input, never loads judge/reference labels."""
import argparse,hashlib,json,pathlib,time
from datetime import datetime,timezone
from src.news_impact.semantic_headlines import SemanticHeadlineClient

def main():
    p=argparse.ArgumentParser();p.add_argument('--input',type=pathlib.Path,required=True);p.add_argument('--output',type=pathlib.Path,required=True);p.add_argument('--limit',type=int,default=20);p.add_argument('--providers',default='');p.add_argument('--budget-seconds',type=int,default=1000);args=p.parse_args()
    selected=json.loads(args.input.read_text());client=SemanticHeadlineClient(args.providers.split(',') if args.providers else None)
    result={'generated_at':datetime.now(timezone.utc).isoformat(),'scope':'title_only_live_models; zero reference labels or deterministic classification supplied','input_sha256':hashlib.sha256(args.input.read_bytes()).hexdigest(),'requested':min(args.limit,len(selected)),'outputs':[]}
    args.output.parent.mkdir(parents=True,exist_ok=True);start=time.monotonic()
    def save():args.output.write_text(json.dumps(result,ensure_ascii=False,indent=2))
    save()
    for row in selected[:args.limit]:
        if time.monotonic()-start>args.budget_seconds:result['stop_reason']='elapsed_budget';break
        out=client.analyze(row['title']);result['outputs'].append({'n':row['n'],'title':row['title'],'program_output':out});save()
        print(json.dumps({'n':row['n'],'status':out['status'],'provider':out['provider'],'model':out['model'],'elapsed_seconds':out['elapsed_seconds']}),flush=True)
        if out['status']=='unavailable' and not any(a['status'] not in ('credential_unavailable','no_allowlisted_classification_model') for a in out['attempts']):
            result['stop_reason']='no_available_credentials_or_models';break
    result['completed']=len(result['outputs']);result['analyzed']=sum(r['program_output']['status']=='analyzed' for r in result['outputs']);save()

if __name__=='__main__':main()
