"""Draw fresh blind batches from the same headline archive used by the webpage.

Run before teacher grading. Gold labels must be supplied before live evaluation.
Known trainer, gold and result files permanently exclude previously exposed items.
"""
import argparse,json,random
from pathlib import Path
from .jev_acceptance import identity,protocol_sha,sha,RUBRIC
from .jev_gate import tokens,jaccard

def draw(root, seed, n_rounds=5):
    exposed=set(); oldtokens=set()
    def collect(value):
        if isinstance(value,dict):
            if value.get('title'):
                exposed.add(identity(value));oldtokens.add(tokens(value['title']))
            for child in value.values():collect(child)
        elif isinstance(value,list):
            for child in value:collect(child)
    for folder in ('00_grounding','src','dashboard/jev-train','validation'):
        for p in (root/folder).rglob('*.json'):
            try:collect(json.loads(p.read_text()))
            except (ValueError,UnicodeDecodeError):pass
    for p in (root/'01_daily/news').glob('*jev_keep*.json'):
        collect(json.loads(p.read_text()))
    candidates={}
    for p in sorted((root/'01_daily/news').glob('*_parsed.json')):
        for row in json.loads(p.read_text()).get('all_items',[]):
            if not row.get('title'):continue
            item={k:row[k] for k in ('title','source','published_at','url','summary','description','snippet','content') if row.get(k)}
            key=identity(item);t=tokens(item['title'])
            if key in exposed or any(jaccard(t,old)>=.8 for old in oldtokens):continue
            candidates.setdefault(key,item)
    candidates=list(candidates.values());random.Random(seed).shuffle(candidates)
    selected=[];seen=[]
    for item in candidates:
        t=tokens(item['title'])
        if any(jaccard(t,old)>=.8 for old in seen):continue
        selected.append(item);seen.append(t)
        if len(selected)==100*n_rounds:break
    if len(selected)!=100*n_rounds:raise ValueError('Insufficient fresh items; collect more articles instead of recycling tests')
    return {'schema':1,'seed':seed,'rubric':RUBRIC,'historical_seen':sorted(exposed),
            'evidence_scope':'Archive headlines and any supplied excerpts; check body coverage before claiming article-level equivalence',
            'rounds':[{'round':i+1,'protocol_sha256':protocol_sha(),'rubric_sha256':sha(RUBRIC),
                       'jev_model':'jev-1.13.0','teacher_model':'','teacher_blind':False,
                       'items':selected[i*100:(i+1)*100]} for i in range(n_rounds)]}
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--seed',type=int,required=True);p.add_argument('--output',required=True)
    args=p.parse_args();result=draw(Path(__file__).resolve().parent.parent,args.seed)
    Path(args.output).parent.mkdir(parents=True,exist_ok=True)
    Path(args.output).write_text(json.dumps(result,indent=2,ensure_ascii=False)+'\n')
