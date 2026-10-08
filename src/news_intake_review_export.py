"""Export requested retained articles as small, exact, connector-readable files.

Selection lives in a frozen request, not in classifier results. No source fetching,
generated summaries, or analysis. The existing intake workflow commits data/news_intake.
"""
from __future__ import annotations
import hashlib,json,re
from pathlib import Path

def export_review_request(root):
    root=Path(root)
    request_path=root/'config/news_review_request.json'
    if not request_path.exists():return {'status':'no_request'}
    request=json.loads(request_path.read_text(encoding='utf-8'))
    session=request['session_id']
    if not re.fullmatch(r'[a-zA-Z0-9_-]{1,80}',session):raise ValueError('Invalid review session')
    folder=root/'data/news_intake/review_exports'/session
    folder.mkdir(parents=True,exist_ok=True)
    cache={};rows=[]
    for selected in request['cases']:
        date=selected['archive_date'];doc_id=selected['document_id']
        if not re.fullmatch(r'\d{4}-\d{2}-\d{2}',date) or not re.fullmatch(r'[a-zA-Z0-9_-]{1,100}',doc_id):
            raise ValueError('Invalid archive path or document ID')
        if date not in cache:
            path=root/'data/news_intake'/date/'documents.json'
            cache[date]=({d['id']:d for d in json.loads(path.read_text(encoding='utf-8'))} if path.exists() else {})
        doc=cache[date].get(doc_id)
        row=dict(selected)
        if doc is None:row['status']='missing_retained_document'
        else:
            raw=json.dumps(doc,ensure_ascii=False,indent=2)+'\n'
            row.update(status='exported_exact',sha256=hashlib.sha256(raw.encode()).hexdigest(),
                       bytes=len(raw.encode()),has_body=bool(doc.get('body')),
                       legacy_generated=bool(doc.get('legacy_generated')),
                       extraction_status=doc.get('extraction_status'),file=doc_id+'.json')
            # No silent truncation. Oversized input needs a separate chunking strategy.
            if len(raw.encode())>900000:row.update(status='oversized_not_exported',file=None)
            else:(folder/(doc_id+'.json')).write_text(raw,encoding='utf-8')
        rows.append(row)
    manifest={'request_sha256':hashlib.sha256(request_path.read_bytes()).hexdigest(),
              'session_id':session,'selection':request.get('selection'),
              'source':'retained_documents_no_new_fetch_or_summary','cases':rows}
    (folder/'manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    return {'session_id':session,'exported':sum(r['status']=='exported_exact' for r in rows),'requested':len(rows)}
