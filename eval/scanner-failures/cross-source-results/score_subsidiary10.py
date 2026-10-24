import json,subprocess,types,sys,hashlib
from pathlib import Path
sys.path.insert(0,str(Path.cwd()))
from src.filing_relationships import extract_sections,load_parser
from src.stock_evidence_graph import build,key
root=Path('eval/scanner-failures/cross-source-results');sample=json.loads((root/'subsidiary-source-sample-10.json').read_text());labels=json.loads((root/'subsidiary-source-labels-10.json').read_text())
assert hashlib.sha256((root/'subsidiary-source-sample-10.json').read_bytes()).hexdigest()==labels['source_sha256']
code=subprocess.check_output(['git','show','b05eb1b13d01f81f33a30c772c6f092541cf8fd0:src/filing_relationships.py'],text=True)
old=types.ModuleType('src._old_filing_relationships');old.__package__='src';old.__file__=str(Path('src/filing_relationships.py').resolve());exec(compile(code,old.__file__,'exec'),old.__dict__)
nlp=load_parser();oldnlp=old.load_parser();cases=[]
for c,label in zip(sample,labels['cases']):
 if not c['documents']:cases.append({'ticker':c['ticker'],'status':label['status']});continue
 sections=[dict(d['source'],text=d['text'],source='PR642',kind='subsidiary') for d in c['documents']]
 b=old.extract_sections(sections,ticker=c['ticker'],name=c['name'],nlp=oldnlp)
 n=extract_sections(sections,ticker=c['ticker'],name=c['name'],nlp=nlp);e=n['stock_evidence']
 bn={key(f['object']) for f in b['facts'] if f['predicate']=='owns_subsidiary'}
 ids={a['entity_id'] for a in e['assertions'] if a['predicate']=='listed_subsidiary'}
 nn={key(x['name']) for x in e['entities'] if x['id'] in ids}
 gold={key(x['name']) for x in label['terms']}
 verified=[dict(w,source_id=n['sources'][i]['id']) for t in label['terms'] for w in t['witnesses'] for i,d in enumerate(c['documents']) if d['source']['id']==w['source_id']]
 isolated=build(n,verified_terms=verified)
 isolated_ids={a['entity_id'] for a in isolated['assertions'] if a['predicate']=='listed_subsidiary' and a['assertion']=='reported' and a.get('direct_ownership_established') is False}
 isolated_names={key(x['name']) for x in isolated['entities'] if x['id'] in isolated_ids}
 row={'ticker':c['ticker'],'reference_names':len(gold),'documents':len(sections),'characters':sum(len(s['text']) for s in sections)}
 for v,names in [('baseline',bn),('candidate',nn)]:row[v]={'tp':len(names&gold),'fp':len(names-gold),'fn':len(gold-names),'predictions':sorted(names),'missed':sorted(gold-names),'extras':sorted(names-gold)}
 row['candidate_isolated_classification_correct']=len(isolated_names&gold)
 row['candidate_assertions']=e['assertions'];cases.append(row)
summary={}
for v in ('baseline','candidate'):
 counts={k:sum(c[v][k] for c in cases if v in c) for k in ('tp','fp','fn')};den=counts['tp']+counts['fp'];ref=counts['tp']+counts['fn'];summary[v]=dict(counts,precision=counts['tp']/den if den else None,recall=counts['tp']/ref if ref else None)
summary['candidate_isolated_classification_correct']=sum(c.get('candidate_isolated_classification_correct',0) for c in cases)
out={'scope':labels['scope'],'annotation':labels['annotation'],'labels_sha256':hashlib.sha256((root/'subsidiary-source-labels-10.json').read_bytes()).hexdigest(),'baseline_revision':'b05eb1b13d01f81f33a30c772c6f092541cf8fd0','candidate_methodology_sha256':labels['methodology_sha256'],'summary':summary,'cases':cases,'limitations':['Three available documents across four randomly reserved issuers; MIRM missing EX-21 remains unavailable, not passed.','Assistant reviewed subset; not independent human market-wide accuracy.','Baseline measures owns_subsidiary channel, candidate measures explicit listed_subsidiary channel. No overall extraction precision or full-stock recall is claimed.','Classification credit requires reported group relationship and direct_ownership_established false; no current/direct ownership inferred.']}
(root/'subsidiary-results-10.json').write_text(json.dumps(out,indent=2,ensure_ascii=False)+'\n');print(json.dumps(summary,indent=2))
