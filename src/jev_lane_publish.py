"""Publish all old and Lane benchmark rows, distinguishing rubric and role."""
import csv,io,json
from pathlib import Path
from .jev_lane_contract import RUBRIC,CONTRACT_VERSION
from .jev_acceptance import summarize
root=Path(__file__).resolve().parents[1];site=root/'dashboard/jev-train'
old=json.loads((root/'validation/jev_acceptance_report.json').read_text())
f=root/'validation/jev_lane_report.json'
lane=json.loads(f.read_text()) if f.exists() else dict(rounds=[],historical_seen=[])
summary=summarize(lane['rounds'],lane.get('historical_seen',[]),rubric=RUBRIC)
for status,batch in zip(summary['rounds'],lane['rounds']):status.update(dataset_role=batch.get('dataset_role','acceptance'),protocol_sha256=batch['protocol_sha256'])
summary.update(evidence_scope='Headline-only; prior-event novelty and article bodies unverified. Old-rubric passes do not count.',rubric=RUBRIC,contract_version=CONTRACT_VERSION,jev_model=lane['rounds'][-1]['jev_model'] if lane['rounds'] else 'jev-1.13.0',teacher_model='GPT-6 frontier assistant / Lane contract')
(site/'acceptance.json').write_text(json.dumps(summary,indent=2)+'\n')
rows=[];blind=[]
reports=[('historical',old),('lane',lane)]
reports += [(f'development-{i}',json.loads(path.read_text())) for i,path in enumerate(sorted((root/'validation').glob('jev_lane_development_*_report.json')),1)]
for group,report in reports:
 for ordinal,b in enumerate(report['rounds'],1):
  for i,r in enumerate(b['items'],1):
   key=f'{"R" if group=="historical" else "L" if group=="lane" else "D"+group.split("-")[-1]}{ordinal:02d}-{i:03d}'
   evidence={k:r.get(k,'') for k in ('title','source','published_at','url','summary','description','snippet','content')}
   blind.append(dict(id=key,rubric_group='historical' if group=='historical' else 'lane',**evidence))
   rows.append(dict(id=key,round=f'{group}-{ordinal}',rubric_group='historical' if group=='historical' else 'lane',dataset_role=b.get('dataset_role','acceptance'),**evidence,frontier_grade=r['gold'],jev_grade=r.get('decision','error'),match=r['gold']==r.get('decision'),jev_reason=r.get('reason',r.get('error_type','')),teacher_model=b.get('teacher_model',''),jev_model=b.get('jev_model',''),protocol_sha256=b.get('protocol_sha256',''),signals=r.get('signals',{}),gold_frozen_at=b.get('gold_frozen_at','')))
(site/'grade-records.json').write_text(json.dumps(dict(rubrics={'historical':old['rubric'],'lane':RUBRIC},evidence_scope=summary['evidence_scope'],records=rows),ensure_ascii=False))
(site/'blind-regrade.json').write_text(json.dumps(dict(instructions='Freeze labels before opening comparisons. Grade using the item rubric_group. Exposed historical/development regrades cannot count as fresh acceptance.',rubrics={'historical':old['rubric'],'lane':RUBRIC},items=blind),ensure_ascii=False))
s=io.StringIO();writer=csv.DictWriter(s,fieldnames=list(rows[0]));writer.writeheader();writer.writerows(rows);(site/'grade-records.csv').write_text(s.getvalue())
escape=lambda x:str(x).replace('|','\\|').replace('\n',' ')
lines=['# All JEV grading records','','Historical and Lane rubrics are separate. Development and failures retained.','','| ID | Role | Exact headline | Source | Date | Frontier | JEV | Match | URL |','|---|---|---|---|---|---|---|---|---|']
lines.extend('| '+' | '.join(escape(r[k]) for k in ('id','dataset_role','title','source','published_at','frontier_grade','jev_grade','match','url'))+' |' for r in rows)
(site/'grade-records.md').write_text('\n'.join(lines)+'\n')
print(json.dumps(summary,indent=2));print('Published rows',len(rows))
