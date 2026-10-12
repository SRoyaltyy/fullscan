"""Run the vault's evidence-grounded context stage on retained intake documents."""
from __future__ import annotations
import argparse,json,os,subprocess,sys
from datetime import datetime,timezone
from pathlib import Path

def run_context(root,date,vault,model=None,limit=None,document_ids=None,asof=None,semantic_model=None,semantic_budget=4):
    root=Path(root);vault=Path(vault)
    source=root/'data/news_intake'/date/'documents.json'
    documents=json.loads(source.read_text(encoding='utf-8'))
    if document_ids:
        wanted=set(document_ids);documents=[d for d in documents if d['id'] in wanted]
    if limit is not None: documents=documents[:limit]
    folder=root/'data/news_context'/date;folder.mkdir(parents=True,exist_ok=True)
    for d in documents:
        d['input_provenance']='retained_fullscan_document:'+str(source.relative_to(root))
        if d.get('legacy_generated'):
            d['body']='';d['extraction_status']='headline_only_generated_digest_excluded'
    inputs=folder/'selected_inputs.json'
    inputs.write_text(json.dumps(documents,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    output=folder/'context.json'
    args=[sys.executable,str(vault/'scripts/news_context_pipeline.py'),'--documents',str(inputs),
        '--vault',str(vault),'--seed',str(vault/'data/News_Context_Exposure_Seed_2026-10-08.json'),
        '--lane-prompts',str(root/'src/news_impact/prompts.py'),
        '--database',str(root/'data/news_context/context.sqlite'),
        '--asof',asof or datetime.now(timezone.utc).isoformat(),'--output',str(output)]
    if model:args+=['--model',model]
    if semantic_model:args+=['--semantic-model',semantic_model,'--semantic-budget',str(semantic_budget)]
    subprocess.run(args,check=True,stdout=subprocess.PIPE)
    result=json.loads(output.read_text(encoding='utf-8'))
    summary={k:v for k,v in result.items() if k not in ('results','model_calls')}
    summary.update({'input_file':str(source.relative_to(root)),'output_file':str(output.relative_to(root)),
                    'selection_limit':limit,'selected_ids':document_ids,
                    'model_calls':len(result['model_calls'])})
    dashboard=root/'dashboard/news-context';dashboard.mkdir(parents=True,exist_ok=True)
    (dashboard/'latest.json').write_text(json.dumps(summary,indent=2)+'\n',encoding='utf-8')
    # No existing classifier, final Lane decision or trade state is overwritten.
    return summary

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--root',type=Path,default=Path(__file__).resolve().parents[1])
    p.add_argument('--date',required=True);p.add_argument('--vault',type=Path,required=True)
    p.add_argument('--model');p.add_argument('--limit',type=int);p.add_argument('--ids',nargs='+')
    p.add_argument('--asof')
    p.add_argument('--semantic-model',help='Experimental local routing candidate backend; not full impact reasoning')
    p.add_argument('--semantic-budget',type=int,default=4)
    a=p.parse_args()
    if a.limit is not None and a.limit<1:p.error('--limit must be positive')
    if a.semantic_budget<0:p.error('--semantic-budget must be nonnegative')
    print(json.dumps(run_context(a.root,a.date,a.vault,a.model,a.limit,a.ids,a.asof,a.semantic_model,a.semantic_budget),indent=2))

if __name__=='__main__':main()
