import sys,pathlib,subprocess,types,json,hashlib
root=pathlib.Path.cwd();sys.path.insert(0,str(root))
from src.filing_relationships import load_parser,extract_sections
from src.stock_evidence_graph import build,key

def old_module(path,name):
 code=subprocess.check_output(['git','show','b05eb1b13d01f81f33a30c772c6f092541cf8fd0:'+path],text=True)
 module=types.ModuleType(name);module.__package__='src';module.__file__=str(root/path)
 exec(compile(code,str(root/path), 'exec'),module.__dict__);return module
old=old_module('src/filing_relationships.py','src._old_filing_relationships')
keywords=old_module('src/stock_keywords.py','src._old_stock_keywords')
newnlp=load_parser();oldnlp=old.load_parser()
sample=json.load(open('run_scratch/cross-source-diagnostic-5.json'));gold=json.load(open('eval/scanner-failures/cross-source-diagnostic-labels.json'))
assert gold['source_sample_sha256']==hashlib.sha256(pathlib.Path('run_scratch/cross-source-diagnostic-5.json').read_bytes()).hexdigest()

def baseline_relations(g,term):
 names={key(x) for x in term['accepted_names']};result=set()
 for f in g['facts']:
  if key(f['object']) in names:result.add((f['predicate'],f['status']))
 for d in g['specific_records']['records']:
  if key(d['entity']) not in names and not names.intersection(key(x) for x in d.get('aliases',[])):continue
  direct=d.get('direct_obligation_asserted',False);neg=d.get('applicability')=='explicitly_out_of_scope'
  pred='subject_to' if direct else 'mentions_legal_reference' if d['category']=='law' else 'mentions_institution'
  result.add((pred,'negated' if neg else 'asserted' if direct else 'reported'))
 return result

def candidate_relations(e,term):
 names={key(x) for x in term['accepted_names']}
 ids={o['entity_id'] for o in e['occurrences'] if key(o['text']) in names and o['entity_id']}
 ids.update(x['id'] for x in e['entities'] if key(x['name']) in names)
 return {(a['predicate'],a['assertion']) for a in e['assertions'] if a['entity_id'] in ids}

results=[]
for label in gold['cases']:
 c=next(c for c in sample['cases'] if c['ticker']==label['ticker']);sections=[{**p['source'],'text':p['text']} for p in c['paragraphs']]
 b=old.extract_sections(sections,ticker=c['ticker'],name=c['name'],nlp=oldnlp)
 n=extract_sections(sections,ticker=c['ticker'],name=c['name'],nlp=newnlp)
 bundle=keywords.compile_keywords(b,as_of='2026-10-24')
 baseline_names={key(r['keyword']) for r in bundle['records'] if r.get('scanner',{}).get('eligible')}
 candidate_names={key(e['name']) for e in n['stock_evidence']['entities']}
 # Verified-term input supplies exact text spans, not semantic types or expected roles.
 verified=[]
 for term in label['terms']:
  for w in term['witnesses']:
   sid=n['sources'][w['paragraph']]['id'];verified.append({'source_id':sid,**{k:w[k] for k in ('start','end','text')}})
 isolated=build(n,verified_terms=verified)
 case={'ticker':c['ticker'],'split':c['split'],'gold_terms':len(label['terms']),'terms':[]}
 alias_to_gold={key(a):t['name'] for t in label['terms'] for a in t['accepted_names']}
 for version,names,rel in [('baseline',baseline_names,lambda t:baseline_relations(b,t)),('candidate',candidate_names,lambda t:candidate_relations(n['stock_evidence'],t))]:
  recovered={alias_to_gold[x] for x in names if x in alias_to_gold}
  extras=sorted(names-set(alias_to_gold))
  correct=sum(rel(t)==set(tuple(x) for x in t['relationships']) for t in label['terms'] if t['name'] in recovered)
  isolated_correct=sum((baseline_relations(b,t) if version=='baseline' else candidate_relations(isolated,t))==set(tuple(x) for x in t['relationships']) for t in label['terms'])
  case[version]={'tp':len(recovered),'fp':len(extras),'fn':len(label['terms'])-len(recovered),
   'isolated_classification_correct':isolated_correct,'end_to_end_correct':correct,'extra_terms':extras,'missed_terms':[t['name'] for t in label['terms'] if t['name'] not in recovered]}
 for t in label['terms']:
  case['terms'].append({'name':t['name'],'expected':t['relationships'],'baseline':sorted(baseline_relations(b,t)),
                        'candidate':sorted(candidate_relations(n['stock_evidence'],t)),
                        'isolated_candidate':sorted(candidate_relations(isolated,t))})
 results.append(case)
summary={}
for split in ('development','holdout','all'):
 cases=[c for c in results if split=='all' or c['split']==split];summary[split]={}
 for version in ('baseline','candidate'):
  counts={k:sum(c[version][k] for c in cases) for k in ('tp','fp','fn','isolated_classification_correct','end_to_end_correct')};total=sum(c['gold_terms'] for c in cases)
  counts.update(gold_terms=total,issuers=len(cases),precision=counts['tp']/(counts['tp']+counts['fp']) if counts['tp']+counts['fp'] else 0,
    recall=counts['tp']/total if total else None,isolated_classification_accuracy=counts['isolated_classification_correct']/total if total else None,end_to_end_accuracy=counts['end_to_end_correct']/total if total else None)
  summary[split][version]=counts
out={'scope':gold['scope'],'annotation':gold['annotation'],'baseline_revision':'b05eb1b13d01f81f33a30c772c6f092541cf8fd0','candidate_commit':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),'summary':summary,'cases':results,'limitations':['Assistant-reviewed, definition-enriched witness diagnostic. Not full-filing recall, independent human review or population estimates.','ARM pair repeats the same passage at different original source positions; it does not validate complementary distant context.','NU had zero eligible witness paragraphs and remains unevaluable; it is not a pass.','Entity-level exact names/explicit aliases consolidated per issuer. Classification requires exact supported predicate/assertion sets.']}
pathlib.Path('run_scratch/cross-source-diagnostic-results.json').write_text(json.dumps(out,indent=2,ensure_ascii=False)+'\n');print(json.dumps(summary,indent=2))
