"""Export recorded labels and separate blind evidence for public audits."""
import json,csv,io
from pathlib import Path
root=Path(__file__).resolve().parents[1]; p=root/'dashboard/jev-train'
d=json.loads((root/'validation/jev_acceptance_report.json').read_text())
rows=[]; blind=[]
for b in d['rounds']:
 for i,r in enumerate(b['items'],1):
  key=f"R{b['round']:02d}-{i:03d}"
  evidence={k:r.get(k,'') for k in ('title','source','published_at','url')}
  blind.append(dict(id=key,**evidence))
  rows.append(dict(id=key,round=b['round'],**evidence,frontier_grade=r['gold'],jev_grade=r['decision'],match=r['gold']==r['decision'],jev_reason=r.get('reason',''),teacher_model=b.get('teacher_model',''),jev_model=b.get('jev_model',''),protocol_sha256=b.get('protocol_sha256','')))
(p/'grade-records.json').write_text(json.dumps(dict(evidence_scope=d['evidence_scope'],rubric=d['rubric'],records=rows),ensure_ascii=False))
(p/'blind-regrade.json').write_text(json.dumps(dict(instructions='Grade every item keep or drop using the rubric. Freeze your labels before opening comparison records. These historical items are exposed audit data, not fresh acceptance rounds.',rubric=d['rubric'],evidence_scope=d['evidence_scope'],items=blind),ensure_ascii=False))
s=io.StringIO(); w=csv.DictWriter(s,fieldnames=list(rows[0]));w.writeheader();w.writerows(rows);(p/'grade-records.csv').write_text(s.getvalue())
escape=lambda x:str(x).replace('|','\\|').replace('\n',' ')
lines=['# Frontier vs JEV: all recorded articles','','Headline-level grading; historical audit records, including failures.','','| ID | Exact headline | Source | Published | Frontier | JEV | Match | Article URL |','|---|---|---|---|---|---|---|---|']
lines.extend('| '+' | '.join(escape(r[k]) for k in ('id','title','source','published_at','frontier_grade','jev_grade','match','url'))+' |' for r in rows)
(p/'grade-records.md').write_text('\n'.join(lines)+'\n')
