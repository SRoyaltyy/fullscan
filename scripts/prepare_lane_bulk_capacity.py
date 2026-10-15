"""Disjoint 100/200 capacity samples from already exposed development inputs only."""
import json,pathlib
from src.news_impact.bulk_screen import identity_keys

def main():
    root=pathlib.Path('.');out=root/'bulk_capacity_inputs';out.mkdir(exist_ok=True)
    used=set();two=[];one=[]
    for batch in (1,2):
        name=f'tests/fixtures/semantic_frozen_20261014/batch_{batch:02d}.json'
        for row in json.loads((root/name).read_text(encoding='utf-8')):
            keys=set(identity_keys(row))
            if keys&used:raise ValueError('Development 200 input is not unique')
            used.update(keys);two.append(row|{'n':len(two)+1,'origin_input':name,'origin_n':row['n'],'origin_batch':batch})
    for name in ('batch_02','batch_recall_fresh_01','batch_recall_fresh_02'):
        path=f'tests/fixtures/semantic_acceptance/{name}.json'
        for row in json.loads((root/path).read_text(encoding='utf-8')):
            keys=set(identity_keys(row))
            if keys&used:continue
            used.update(keys);one.append(row|{'n':len(one)+1,'origin_input':path,'origin_n':row['n']})
            if len(one)==100:break
        if len(one)==100:break
    if (len(one),len(two))!=(100,200):raise ValueError('Insufficient distinct development articles; do not use unopened holdouts')
    for count,data in ((100,one),(200,two)):(out/f'batch_{count}.json').write_text(json.dumps(data,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    print(json.dumps({'batch_100':len(one),'batch_200':len(two),'cross_batch_duplicates':0,'future_holdouts_used':0}))

if __name__=='__main__':main()
