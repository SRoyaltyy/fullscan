"""Offline judge comparison; never imported by the analysis program.

This reports acceptance, not a repair loop. References must be frozen before
execution. Missing reference/outputs, wrong hashes or duplicates cannot pass.
"""
import argparse,hashlib,json,pathlib

def derive(output):
    if not output or output.get('status')!='analyzed' or output.get('model_output') is None:return None
    m=output.get('analysis_output') or output['model_output']
    action=any(x['concrete_public_action'] is True for x in m['actions'])
    if not action and any(x['concrete_public_action'] is None and x['public_authority'] is not False for x in m['actions']):action=None
    return {'action':action,'lane':m['primary_lane_class'],'sectors':sorted({s['sector'] for s in m['sectors']})}

def compare(inputs,reference,raw):
    if len(inputs)!=100:raise ValueError('An acceptance batch requires exactly 100 titles')
    expected={r['n']:r for r in reference['records']}
    actual={r['n']:r for r in raw['outputs']}
    if len(expected)!=100 or len(reference['records'])!=100:raise ValueError('Reference must cover 100 unique ordinals')
    if len(actual)!=len(raw['outputs']):raise ValueError('Duplicate program ordinals')
    rows=[];tp=fp=fn=0
    for item in inputs:
        g=expected[item['n']];o=actual.get(item['n'])
        if g['title']!=item['title'] or (o and o['title']!=item['title']):raise ValueError('Title/reference identity mismatch')
        d=derive(o['program_output'] if o else None)
        action_ok=d is not None and d['action'] is g['action']
        lane_ok=d is not None and d['lane'] in g['acceptable_lane_classes']
        sectors_ok=d is not None and any(set(d['sectors'])==set(s) for s in g['acceptable_sector_sets'])
        if g['action'] is True:
            if d is not None and d['action'] is True:tp+=1
            else:fn+=1
        elif g['action'] is False and d is not None and d['action'] is True:fp+=1
        rows.append({'n':item['n'],'title':item['title'],'program':d,'reference':g,'checks':{'action':action_ok,'lane':lane_ok,'sectors':sectors_ok}})
    metrics={k:sum(r['checks'][k] for r in rows)/100 for k in ('action','lane','sectors')}
    metrics['action_precision']=tp/(tp+fp) if tp+fp else None
    metrics['action_recall']=tp/(tp+fn) if tp+fn else None
    passed=all(metrics[k] is not None and metrics[k]>=.9 for k in metrics)
    return {'pass':passed,'metrics':metrics,'tp':tp,'fp':fp,'fn_or_unavailable':fn,'availability':sum(r['program'] is not None for r in rows)/100,'samples':rows}

def main():
    p=argparse.ArgumentParser();p.add_argument('--inputs',type=pathlib.Path,required=True);p.add_argument('--references',type=pathlib.Path,required=True);p.add_argument('--outputs',type=pathlib.Path,required=True);p.add_argument('--report',type=pathlib.Path,required=True);args=p.parse_args()
    batches=[];titles=set();streak=0;best=0;runtime=None;configuration=None
    for b in range(1,11):
        stem=f'batch_{b:02d}';ip=args.inputs/(stem+'.json');rp=args.references/(stem+'.json');op=args.outputs/(stem+'.json')
        if not ip.exists() or not rp.exists() or not op.exists():batches.append({'batch':b,'pass':False,'status':'missing_input_reference_or_output'});streak=0;continue
        inp=json.loads(ip.read_text());ref=json.loads(rp.read_text());raw=json.loads(op.read_text())
        if not raw.get('runtime_sha256') or not raw.get('configuration'):raise ValueError('Missing runtime fingerprints/configuration; cannot establish a frozen acceptance candidate')
        if runtime is None:runtime=raw['runtime_sha256'];configuration=raw['configuration']
        if raw['runtime_sha256']!=runtime or raw['configuration']!=configuration:raise ValueError('Candidate changed across acceptance batches: restart streak')
        digest=hashlib.sha256(ip.read_bytes()).hexdigest()
        if raw['input_sha256']!=digest or ref['input_sha256']!=digest:raise ValueError('Frozen input hash mismatch')
        if not ref.get('frozen_before_program_execution'):raise ValueError('Unfrozen reference cannot establish acceptance')
        if ref['frozen_at']>=raw['generated_at']:raise ValueError('Reference freeze did not precede execution')
        for row in inp:
            key=row['title'].strip().casefold()
            if key in titles:raise ValueError('Repeated headline across acceptance batches')
            titles.add(key)
        result=compare(inp,ref,raw);result.update(batch=b,status='evaluated',reference_sha256=hashlib.sha256(rp.read_bytes()).hexdigest(),raw_sha256=hashlib.sha256(op.read_bytes()).hexdigest());batches.append(result)
        streak=streak+1 if result['pass'] else 0;best=max(best,streak)
    report={'target':'>=90% action, Lane, exact sectors, action precision and recall in each of 10 disjoint 100-title batches','acceptance_pass':best==10,'best_consecutive_passes':best,'distinct_titles_evaluated':len(titles),'batches':batches}
    args.report.parent.mkdir(parents=True,exist_ok=True);args.report.write_text(json.dumps(report,ensure_ascii=False,indent=2));print(json.dumps({k:v for k,v in report.items() if k!='batches'}))
    for b in batches:print(json.dumps({k:v for k,v in b.items() if k!='samples'}))

if __name__=='__main__':main()
