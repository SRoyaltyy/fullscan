"""Print aggregate/diagnostic results without publishing article or filing text."""
import argparse,json
from pathlib import Path

p=argparse.ArgumentParser();p.add_argument('paths',type=Path,nargs='+');a=p.parse_args()
for path in a.paths:
 d=json.loads(path.read_text(encoding='utf-8'))
 print(json.dumps({'file':path.name,'summary':d.get('summary'),
  'strict_metrics':{k:v for k,v in d.get('strict_metrics',{}).items() if k!='details'},
  'failures':[{k:r.get(k) for k in ('case_id','verdict','failure_classes','missing','unexpected')}
              for r in d.get('cases',[]) if r.get('verdict') in ('FAIL','TARGET_FAIL')]}),flush=True)
