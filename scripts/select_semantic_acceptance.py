"""Random sample selection only: no classifier or reference imports.

Archive files must first be verified as public at the pinned archive commit.
Reference assessments are authored separately, frozen before live execution.
"""
import argparse,hashlib,html,json,pathlib,random,re,ast
from difflib import SequenceMatcher

def normal(title):
    return re.sub(r'[^\w]+',' ',html.unescape(title).lower().rsplit(' - ',1)[0]).strip()

def similar(a,b):
    short,long=sorted((a,b),key=len)
    tokens=set(short.split())
    if len(tokens)>=7 and (short in long or len(tokens & set(long.split()))/len(tokens)>=.9):return True
    matcher=SequenceMatcher(None,a,b,autojunk=False)
    return matcher.real_quick_ratio()>=.85 and matcher.quick_ratio()>=.85 and matcher.ratio()>=.85

def main():
    p=argparse.ArgumentParser();p.add_argument('--archive',type=pathlib.Path,required=True);p.add_argument('--exclude',type=pathlib.Path,action='append',default=[]);p.add_argument('--controls',type=pathlib.Path);p.add_argument('--output',type=pathlib.Path,required=True);p.add_argument('--seed',type=int,default=202610131041);p.add_argument('--batches',type=int,default=10);args=p.parse_args()
    old=[];old_urls=set()
    for path in args.exclude:
        data=json.loads(path.read_text());rows=data.get('selected',data.get('records',[])) if isinstance(data,dict) else data
        old.extend(normal(r['title']) for r in rows);old_urls.update(r['source_url'] for r in rows if r.get('source_url'))
    if args.controls:
        node=next(n for n in ast.parse(args.controls.read_text()).body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='CASES' for t in n.targets))
        old.extend(normal(r[0]) for r in ast.literal_eval(node.value))
    pool={};visited=set()
    for path in sorted(args.archive.glob('*.md')):
        digest=hashlib.sha256(path.read_bytes()).hexdigest()
        for line_no,line in enumerate(path.read_text().splitlines(),1):
            m=re.match(r'^- \[(.*?)\]\((.*?)\) — ([^;]+); ([^;]+); (.*)$',line)
            if m:title,url,published=m[1],m[2],m[4]
            else:
                m=re.match(r'^- \[([^\]]+)\] (.+?) \(([^()]*)\)$',line)
                if not m:continue
                title,url,published=m[2],'',''
            norm=normal(title)
            if norm in visited:continue
            visited.add(norm)
            if (url and url in old_urls) or any(similar(norm,x) for x in old):continue
            pool[norm]={'title':title,'source_report':path.name,'source_line':line_no,'source_sha256':digest,'source_url':url,'source_published_at':published}
    candidates=sorted(pool.items());random.Random(args.seed).shuffle(candidates)
    selected=[];seen=[];urls=set()
    for norm,row in candidates:
        if (row['source_url'] and row['source_url'] in urls) or any(similar(norm,x) for x in seen):continue
        selected.append(row);seen.append(norm)
        if row['source_url']:urls.add(row['source_url'])
        if len(selected)==args.batches*100:break
    if len(selected)!=args.batches*100:raise ValueError(f'Only {len(selected)} sufficiently distinct titles; do not relax novelty to fill batches')
    args.output.mkdir(parents=True,exist_ok=True)
    manifest={'seed':args.seed,'archive_commit':'a62a3cc16b0a7303a7c4fa1b855566f75f23bd33','method':'Uniform shuffled retained archive-title pool; no action/sector/event classifier used; exact URL and near-title exclusion globally and against previous tests','pool_size':len(pool),'selected':selected,'exclusion_sha256':{str(x):hashlib.sha256(x.read_bytes()).hexdigest() for x in args.exclude}}
    (args.output/'selection.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2))
    for b in range(args.batches):
        rows=[{'n':i+1,'title':r['title']} for i,r in enumerate(selected[b*100:(b+1)*100])]
        (args.output/f'batch_{b+1:02d}.json').write_text(json.dumps(rows,ensure_ascii=False,indent=2))
    print(json.dumps({'pool_size':len(pool),'selected':len(selected),'batches':args.batches}))

if __name__=='__main__':main()
