"""Subscription-only, single-prompt news batches with a persistent no-resend ledger."""
import argparse,hashlib,json,pathlib,sqlite3,time
from datetime import datetime,timezone
from src.news_impact.bulk_screen import VERSION,identity_keys,instructions,request_once
from src.news_impact.semantic_catalog import CompanyCatalog

def now():return datetime.now(timezone.utc).isoformat()

class Ledger:
    def __init__(self,path,fingerprint):
        path=pathlib.Path(path);path.parent.mkdir(parents=True,exist_ok=True)
        self.db=sqlite3.connect(path,timeout=30)
        self.db.executescript('CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY,value TEXT NOT NULL); CREATE TABLE IF NOT EXISTS articles (id INTEGER PRIMARY KEY,title TEXT NOT NULL,status TEXT NOT NULL,result TEXT,updated_at TEXT NOT NULL); CREATE TABLE IF NOT EXISTS aliases (key TEXT PRIMARY KEY,article_id INTEGER NOT NULL REFERENCES articles(id));')
        try:
            with self.db:
                self.db.execute("INSERT OR IGNORE INTO settings VALUES ('fingerprint',?)",(fingerprint,))
                saved=self.db.execute("SELECT value FROM settings WHERE key='fingerprint'").fetchone()[0]
                if saved!=fingerprint:raise ValueError('Ledger model/method/catalog mismatch; refusing silent reprocessing. Use a separately named ledger for an intentional new experiment.')
        except BaseException:
            self.db.close();raise
    def claim(self,row):
        keys=identity_keys(row)
        # Claims commit before any HTTP request, including across concurrent processes.
        self.db.execute('BEGIN IMMEDIATE')
        try:
            ids={r[0] for r in self.db.execute('SELECT article_id FROM aliases WHERE key IN ('+','.join('?' for _ in keys)+')',keys)}
            if ids:
                aid=min(ids);fresh=False
            else:
                aid=self.db.execute('INSERT INTO articles(title,status,updated_at) VALUES (?,?,?)',(row['title'],'inflight',now())).lastrowid;fresh=True
            for key in keys:self.db.execute('INSERT OR IGNORE INTO aliases VALUES (?,?)',(key,aid))
            self.db.commit();return aid,fresh
        except BaseException:self.db.rollback();raise
    def finish(self,aid,result,error=None):
        with self.db:self.db.execute('UPDATE articles SET status=?,result=?,updated_at=? WHERE id=?',('analyzed' if result is not None else 'unavailable',json.dumps(result if result is not None else {'error':error},ensure_ascii=False),now(),aid))
    def get(self,aid):
        status,raw=self.db.execute('SELECT status,result FROM articles WHERE id=?',(aid,)).fetchone()
        return status,json.loads(raw) if raw else None
    def close(self):self.db.close()

def run(rows,ledger,catalog,batch_size=100,model='xai/grok-4.3',timeout=1200,max_new=200):
    if not 1<=batch_size<=200 or max_new<0:raise ValueError('Batch must be 1..200 and max_new nonnegative')
    ids=[r['n'] for r in rows]
    if any(type(n) is not int for n in ids) or len(set(ids))!=len(ids):raise ValueError('Input IDs must be unique integers')
    if any(not isinstance(r.get('title'),str) or not r['title'].strip() for r in rows):raise ValueError('Each input needs a nonempty title')
    selected=[];reports=[];stopped=None;started=time.monotonic()
    # Claim only the next batch. A quota stop must leave unsubmitted articles unclaimed.
    cursor=0;submitted=0
    while cursor<len(rows):
        pending=[]
        while cursor<len(rows) and len(pending)<batch_size:
            if submitted+len(pending)>=max_new:stopped='max_new_articles';break
            row=rows[cursor];cursor+=1;aid,fresh=ledger.claim(row)
            selected.append((row,aid,not fresh))
            if fresh:pending.append((row,aid))
        if pending:
            batch=[r for r,_ in pending]
            try:good,errors,meta=request_once(batch,catalog,model,timeout)
            except Exception as exc:
                good={};errors={r['n']:'transport_or_response_exception:'+type(exc).__name__ for r in batch}
                meta={'http_requests':1,'http_status':None,'valid_rows':0,'articles_submitted':len(batch),'exception_type':type(exc).__name__}
            for row,aid in pending:ledger.finish(aid,good.get(row['n']),errors.get(row['n']))
            reports.append(meta);submitted+=len(batch)
            if meta.get('http_status')!=200 or len(good)!=len(batch):
                stopped='rate_limited' if meta.get('http_status')==429 else 'incomplete_or_failed_batch'
                break
        if stopped:break
    output=[]
    for row,aid,cached in selected:
        status,result=ledger.get(aid)
        # Rebind only the presentation ID; the original raw decision remains in the ledger.
        output.append({'n':row['n'],'title':row['title'],'ledger_id':aid,'reused':cached,'status':status,'result':(result|{'n':row['n']}) if result and status=='analyzed' else result})
    for row in rows[cursor:]:output.append({'n':row['n'],'title':row['title'],'ledger_id':None,'reused':False,'status':'not_submitted','result':None})
    return {'version':VERSION,'generated_at':now(),'scope':'title_only; one compact Lane/action/sector prompt per batch; subscription gateway only','model_requested':model,'input_articles':len(rows),'submitted_unique_articles':submitted,'reused_input_rows':sum(r['reused'] for r in output),'not_visited':len(rows)-cursor,'http_requests':sum(r['http_requests'] for r in reports),'elapsed_seconds':round(time.monotonic()-started,3),'stop_reason':stopped,'weekly_allowance_remaining':None,'batches':reports,'outputs':output}

def point_form(report):
    lines=['# Lane concrete-action screen','',f"Model requested: {report['model_requested']}; unique submitted: {report['submitted_unique_articles']}; requests: {report['http_requests']}.",'','Concrete action means the existing public-institutional action definition. Unavailable/uncertain are not negative answers.','']
    for row in report['outputs']:
        result=row['result'];label='UNAVAILABLE';lane='unknown';why='Previously attempted or unresolved; not automatically resubmitted.'
        if row['status']=='not_submitted':why='Not submitted because the run stopped or reached its new-article budget.'
        if row['status']=='analyzed':
            label={True:'YES',False:'NO',None:'UNCERTAIN'}[result['concrete_public_action']];lane=result['lane'] or 'unresolved';why=result['why']
        why=' '.join(why.split()).replace('`',"'")
        lines.append(f"- **{row['n']}. {label}** — {lane}: {why}"+(' (cached)' if row['reused'] else ''))
    return '\n'.join(lines)+'\n'

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--input',type=pathlib.Path,required=True);p.add_argument('--output',type=pathlib.Path,required=True);p.add_argument('--ledger',type=pathlib.Path,required=True)
    p.add_argument('--batch-size',type=int,choices=(100,200),default=100);p.add_argument('--max-new',type=int,default=200)
    p.add_argument('--model',choices=('xai/grok-4.3','xai/grok-4.6'),default='xai/grok-4.3');p.add_argument('--timeout',type=int,default=1200)
    a=p.parse_args()
    if a.timeout<=0 or a.max_new<0:p.error('timeout must be positive and max-new nonnegative')
    catalog=CompanyCatalog()
    if not catalog.metadata['available']:p.error('Required frozen company catalog is unavailable')
    rows=json.loads(a.input.read_text(encoding='utf-8'))
    fingerprint=hashlib.sha256(json.dumps({'version':VERSION,'model':a.model,'instructions':instructions(),'catalog':catalog.metadata,'runner_sha256':hashlib.sha256(pathlib.Path(__file__).read_bytes()).hexdigest(),'source_sha256':hashlib.sha256((pathlib.Path(__file__).resolve().parent.parent/'src/news_impact/bulk_screen.py').read_bytes()).hexdigest()},sort_keys=True).encode()).hexdigest()
    ledger=Ledger(a.ledger,fingerprint)
    try:report=run(rows,ledger,catalog,a.batch_size,a.model,a.timeout,a.max_new)
    finally:ledger.close()
    report['method_fingerprint']=fingerprint
    a.output.parent.mkdir(parents=True,exist_ok=True)
    a.output.write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
    a.output.with_suffix('.md').write_text(point_form(report),encoding='utf-8')
    print(json.dumps({k:v for k,v in report.items() if k not in ('outputs','batches')},ensure_ascii=False))

if __name__=='__main__':main()
