"""Compact public inference outputs; no labels, titles or article bodies."""
import argparse,json
from pathlib import Path

def main():
 p=argparse.ArgumentParser();p.add_argument('root',type=Path);a=p.parse_args()
 for sub in ('','fresh','holdout','holdout4','holdout5','holdout6'):
  for engine in ('evidence','legacy','evidence-full-identity'):
   source=a.root/sub/(engine+'-headline-links.json')
   if not source.exists():continue
   payload=json.loads(source.read_text(encoding='utf-8'))
   compact=[{'id':str(h['id']),'stocks':[{'ticker':str(r['ticker']),'pass':True} for r in h.get('stocks',[]) if r.get('pass')],
    'decision':h.get('decision',h.get('status',''))} for h in payload['links']]
   target=source.with_name(engine+'-compact-links.json')
   target.write_text(json.dumps({'links':compact},ensure_ascii=True),encoding='utf-8')
   print(json.dumps({'file':str(target),'cases':len(compact)}))

if __name__=='__main__':main()
