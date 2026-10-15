"""Reproducible pre-execution selection repair; never reads classifier outputs/labels.

Run from repository root with the pinned raw inputs and archive present.
Outputs a selection manifest and corrected inputs only, not reference answers.
"""
import hashlib, html, json, pathlib, random, re, unicodedata

ROOT=pathlib.Path('.')
STOP=set('the a an and or of to in on at for from with by as is are was were be been this that after before new says said report reports update latest'.split())
PIN='a62a3cc16b0a7303a7c4fa1b855566f75f23bd33'
SEED=20261016

def normalize(title):
    text=unicodedata.normalize('NFKC',html.unescape(title)).casefold().rsplit(' - ',1)[0]
    text=re.sub(r'\d+(?:[.,]\d+)*','number',text)
    words=re.findall(r'\w+',text)
    return ' '.join(words),set(words)-STOP

def url(row):return row.get('url') or row.get('source_url')

def signature(row):return (*normalize(row['title']),url(row))

def collision(a,b):
    na,ta,ua=a;nb,tb,ub=b
    if ua and ub and ua==ub:return 'same_url'
    if na==nb:return 'normalized_title'
    if not ta or not tb:return None
    overlap=len(ta&tb);small=min(len(ta),len(tb));large=max(len(ta),len(tb))
    if overlap/len(ta|tb)>=.65:return 'token_jaccard_ge_0.65'
    if small>=4 and overlap/small>=.85 and overlap/large>=.6:return 'token_containment_ge_0.85_and_0.60'
    return None

def read(path):return json.loads((ROOT/path).read_text(encoding='utf-8'))
def digest(path):return hashlib.sha256((ROOT/path).read_bytes()).hexdigest()
def write(path,data):
    path=ROOT/path;path.parent.mkdir(parents=True,exist_ok=True)
    path.write_bytes((json.dumps(data,ensure_ascii=False,indent=2)+'\n').encode('utf-8'))

def main():
    controls=['tests/fixtures/semantic_headline_inputs.json']+[f'tests/fixtures/semantic_acceptance/{x}.json' for x in ('batch_02','batch_recall_fresh_01','batch_recall_fresh_02')]+[f'tests/fixtures/semantic_frozen_20261014/batch_{b:02d}.json' for b in (1,2)]
    prior=[]
    for path in controls:
        for row in read(path):prior.append(({'path':path,'n':row['n'],'title':row['title']},signature(row)))
    reserves={b:read(f'tests/fixtures/semantic_reserve_20261014/batch_{b:02d}.json') for b in range(1,13)}
    # Active frozen bytes are authoritative when a batch already has references.
    active={b:read(f'tests/fixtures/semantic_frozen_20261014/batch_{b:02d}.json') if b<=8 else reserves[b] for b in range(3,13)}
    retained=[];retired=[]
    for b,rows in active.items():
        assert len(rows)==100 and len({r['n'] for r in rows})==100
        for row in rows:
            sig=signature(row)
            hit=next(((meta,collision(sig,old)) for meta,old in prior+retained if collision(sig,old)),None)
            if hit:retired.append({'batch':b,'n':row['n'],'original':row,'collides_with':hit[0],'reason':hit[1]})
            else:retained.append(({'batch':b,'n':row['n'],'title':row['title']},sig))
    blocked=[signature(r) for rows in reserves.values() for r in rows]+[s for _,s in prior]+[signature(r) for rows in active.values() for r in rows]
    pool=[];sources={}
    for day in range(2,14):
        path=f'01_daily/news/2026-10-{day:02d}_intake.md';sources[path]=digest(path)
        for line_no,line in enumerate((ROOT/path).read_text(encoding='utf-8').splitlines(),1):
            m=re.match(r'^- \[(.*?)\]\((.*?)\) — ([^;]+); ([^;]+); (.*)$',line)
            if m:pool.append({'title':m[1],'source_report':path,'source_line':line_no,'source_sha256':sources[path],'source_url':m[2],'published':m[4]})
    random.Random(SEED).shuffle(pool)
    candidates=iter(pool)
    for item in retired:
        for row in candidates:
            sig=signature(row)
            if not any(collision(sig,old) for old in blocked):break
        else:raise RuntimeError('Novel source pool exhausted; do not relax exclusion thresholds')
        row={'n':item['n'],**row};item['replacement']=row;blocked.append(sig)
        rows=active[item['batch']];rows[next(i for i,r in enumerate(rows) if r['n']==item['n'])]=row
    # Proof over final 1,000 rows, including all replacements, against all prior inputs.
    checked=list(prior)
    for b,rows in active.items():
        for row in rows:
            sig=signature(row)
            assert not any(collision(sig,old) for _,old in checked),(b,row['n'])
            checked.append(({'batch':b,'n':row['n']},sig))
    out='tests/fixtures/semantic_novelty_repair_20261015'
    for b,rows in active.items():write(f'{out}/batch_{b:02d}.json',rows)
    manifest={'version':'pre-execution-novelty-repair-v1','source_commit':PIN,'selection_input_commit':'07a5f2db9691d60dc786e318e6763e9f5912326f','original_reserve_sha256':{str(b):digest(f'tests/fixtures/semantic_reserve_20261014/batch_{b:02d}.json') for b in reserves},'original_active_frozen_sha256':{str(b):digest(f'tests/fixtures/semantic_frozen_20261014/batch_{b:02d}.json') for b in range(3,9)},'seed':SEED,'scope':'Selection only. No classifier outputs, reference labels or answer correctness read. Corrected inputs must be independently frozen before execution. Original selections remain in their original paths and git history.','normalization':'NFKC; HTML unescape; casefold; remove final publisher suffix using rsplit; numeric normalization; Unicode word tokens; fixed stop words','stop_words':sorted(STOP),'collision_rules':{'same_url':True,'normalized_title':True,'token_jaccard':.65,'min_tokens_for_containment':4,'intersection_over_min':.85,'intersection_over_max':.6},'archive_sources':sources,'prior_inputs':{p:digest(p) for p in controls},'prior_rows_including_repetitions':len(prior),'final_fresh_rows':1000,'final_collisions':0,'retirements':retired,'corrected_input_sha256':{str(b):digest(f'{out}/batch_{b:02d}.json') for b in active}}
    write('docs/benchmarks/novelty_repair_20261015.json',manifest)
    print(json.dumps({'retired':len(retired),'proof_collisions':0,'replacements':[{'batch':r['batch'],'n':r['n'],'title':r['replacement']['title']} for r in retired]},ensure_ascii=True,indent=2))

if __name__=='__main__':main()
