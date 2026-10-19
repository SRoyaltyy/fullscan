"""Export reproducible, deduplicated real scanner inputs for blind labeling."""
import argparse
import gzip
import hashlib
import json
import random
import re
import shutil
from pathlib import Path


def norm(text):return re.sub(r'\W+',' ',text or '').strip().lower()


def main():
 p=argparse.ArgumentParser()
 p.add_argument('--news',type=Path,required=True)
 p.add_argument('--descriptions',type=Path,required=True)
 p.add_argument('--out',type=Path,required=True)
 p.add_argument('--budget',type=int,default=200)
 p.add_argument('--seed',type=int,default=19019)
 a=p.parse_args();a.out.mkdir(parents=True,exist_ok=True)
 payload=json.loads(a.news.read_text(encoding='utf-8'))
 rows=payload if isinstance(payload,list) else payload.get('documents') or payload.get('rows') or []
 unique={}
 for row in rows:
  key=norm(row.get('title',''))
  if key and key not in unique:unique[key]=row
 bodies=[];heads=[]
 for row in unique.values():
  title=str(row.get('title') or '')
  body=str(row.get('body') or row.get('text') or '')
  clean=norm(body)
  title_norm=norm(title)
  # Length alone accepts headlines repeated twice as a "body".
  remainder=clean.replace(title_norm,'').strip() if title_norm else clean
  (bodies if len(remainder.split())>=40 else heads).append(row)
 rng=random.Random(a.seed);rng.shuffle(bodies);rng.shuffle(heads)
 n=min(len(bodies),a.budget//2)
 picked=bodies[:n]+heads[:a.budget-n]
 rng.shuffle(picked)
 (a.out/'news-sample.json').write_text(json.dumps(picked,ensure_ascii=True),encoding='utf-8')
 with a.descriptions.open('rb') as src,gzip.open(a.out/'finviz_with_descriptions.csv.gz','wb') as dst:shutil.copyfileobj(src,dst)
 report={'seed':a.seed,'pool':len(rows),'deduplicated_pool':len(unique),'genuine_body_pool':len(bodies),
   'sample':len(picked),'with_body':n,'news_sha256':hashlib.sha256(a.news.read_bytes()).hexdigest(),
   'descriptions_sha256':hashlib.sha256(a.descriptions.read_bytes()).hexdigest(),
   'labels_provided':False,'program_outputs_included':False}
 (a.out/'manifest.json').write_text(json.dumps(report,indent=2),encoding='utf-8')
 print(json.dumps(report,indent=2))


if __name__=='__main__':main()
