"""Offline development comparison; no model calls, no acceptance credit."""
import argparse,hashlib,json,pathlib
from evaluate_semantic_acceptance import compare
from src.news_impact.bulk_screen import PUBLIC_TYPES,concrete

def grade(root):
    path=root/'bulk_200.json'
    if not path.exists():return {'status':'200_article_request_not_run','acceptance_eligible':False}
    report=json.loads(path.read_text(encoding='utf-8'))
    prepared=json.loads((root/'inputs/batch_200.json').read_text(encoding='utf-8'))
    by_n={r['n']:r for r in report['outputs']};scores=[]
    for batch in (1,2):
        inp=json.loads(pathlib.Path(f'tests/fixtures/semantic_frozen_20261014/batch_{batch:02d}.json').read_text(encoding='utf-8'))
        ref=json.loads(pathlib.Path(f'docs/benchmarks/references_20261014/batch_{batch:02d}.json').read_text(encoding='utf-8'))
        original=pathlib.Path(f'tests/fixtures/semantic_frozen_20261014/batch_{batch:02d}.json')
        assert hashlib.sha256(original.read_bytes()).hexdigest()==ref['input_sha256']
        outputs=[]
        for row in prepared:
            if row['origin_batch']!=batch:continue
            result=by_n.get(row['n']);raw=result.get('result') if result and result['status']=='analyzed' else None
            if raw:
                acts=[{'public_authority':None if a['actor_type']=='unknown' else a['actor_type'] in PUBLIC_TYPES,'concrete_public_action':concrete([a])} for a in raw['actions']]
                program={'status':'analyzed','model_output':{'actions':acts,'primary_lane_class':raw['lane'],'sectors':raw['sectors']}}
            else:program={'status':'unavailable','model_output':None}
            outputs.append({'n':row['origin_n'],'title':row['title'],'program_output':program})
        score=compare(inp,ref,{'outputs':outputs});scores.append(score)
    tp=sum(s['tp'] for s in scores);fp=sum(s['fp'] for s in scores);fn=sum(s['fn_or_unavailable'] for s in scores)
    metrics={k:sum(s['metrics'][k] for s in scores)/2 for k in ('action','lane','sectors')}
    metrics.update(action_precision=tp/(tp+fp) if tp+fp else 0,action_recall=tp/(tp+fn) if tp+fn else 0)
    return {'scope':'200-article development comparison; see request/cache counters for actual new capacity evidence','single_prompt_200_observed':len(report.get('batches',[]))==1 and report['batches'][0].get('articles_submitted')==200 and report.get('http_requests')==1,'acceptance_eligible':False,'metrics':metrics,'pass':all(v>=.9 for v in metrics.values()),'tp':tp,'fp':fp,'fn_or_unavailable':fn,'availability':sum(s['availability'] for s in scores)/2,'subsets':scores,'capacity':{k:v for k,v in report.items() if k not in ('outputs',)}}

def main():
    p=argparse.ArgumentParser();p.add_argument('--root',type=pathlib.Path,required=True);a=p.parse_args()
    result=grade(a.root);(a.root/'development_comparison.json').write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps({k:v for k,v in result.items() if k not in ('subsets','capacity')}))
if __name__=='__main__':main()
