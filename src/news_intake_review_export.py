"""Export requested retained articles as small, exact, connector-readable files.

Selection lives in a frozen request, not in classifier results. No source fetching,
generated summaries, or analysis. The existing intake workflow commits data/news_intake.
"""
from __future__ import annotations
import hashlib,json,re,time
from pathlib import Path

def export_review_request(root,fetch_page=None,extract_text=None):
    root=Path(root)
    request_path=root/'config/news_review_request.json'
    if not request_path.exists():return {'status':'no_request'}
    request=json.loads(request_path.read_text(encoding='utf-8'))
    session=request['session_id']
    if not re.fullmatch(r'[a-zA-Z0-9_-]{1,80}',session):raise ValueError('Invalid review session')
    folder=root/'data/news_intake/review_exports'/session
    folder.mkdir(parents=True,exist_ok=True)
    cache={};rows=[];budget=min(max(int(request.get('fulltext_budget',0)),0),8);attempted=0
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
            doc=dict(doc)
            existing=folder/(doc_id+'.json')
            if existing.exists():
                enriched=json.loads(existing.read_text(encoding='utf-8'))
                if enriched.get('url')==doc.get('url') and enriched.get('title')==doc.get('title'):doc=enriched
            enrichment=doc.get('review_enrichment',{})
            retry=(time.time()-enrichment.get('attempted_epoch',0)>=1800 and enrichment.get('attempts',0)<3)
            if (fetch_page and extract_text and request.get('resolve_full_text') and attempted<budget
                and doc.get('extraction_status')!='page_text' and retry):
                attempted+=1
                enrichment={'attempted_epoch':time.time(),'attempts':enrichment.get('attempts',0)+1,
                            'status':'attempted','network_requests':[],'historical_body_availability_verified':False}
                try:
                    from .news_intake_google_resolver import resolve_google_url
                    url,requests=resolve_google_url(doc['url'],fetch_page,audit=enrichment['network_requests'])
                    enrichment['network_requests']=requests+[{'method':'GET','url':url}]
                    data,mime=fetch_page(url)
                    if 'html' not in mime:raise ValueError('Publisher response is not HTML')
                    from .news_article_retrieval import extract_page,publication_metadata
                    page=data.decode('utf-8','replace');body,method=extract_page(page)
                    doc.update(publication_metadata(page))
                    title_words={w for w in re.findall(r'\w+',doc['title'].lower()) if len(w)>3}
                    body_words=set(re.findall(r'\w+',body.lower()))
                    overlap=len(title_words&body_words)/max(len(title_words),1)
                    if len(body)<600 or overlap<0.65:raise ValueError('Thin or mismatched publisher text')
                    if re.search(r'\b(?:verify you are human|captcha|enable javascript and cookies)\b',body,re.I):
                        raise ValueError('Access challenge; no bypass attempted')
                    doc['retained_feed_body']=doc.get('body','');doc['body']=body
                    doc['extraction_status']='page_text';doc['extracted_url']=url
                    enrichment.update(status='page_text_checked',method=method,title_token_overlap=round(overlap,3),
                                      note='Structured body preferred; HTML boundaries remain candidates requiring review')
                except Exception as exc:enrichment.update(status='failed',error=str(exc)[:300])
                doc['review_enrichment']=enrichment
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
              'source':'retained_documents_with_explicit_optional_publisher_enrichment',
              'enrichment_attempts':attempted,'cases':rows}
    (folder/'manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    return {'session_id':session,'exported':sum(r['status']=='exported_exact' for r in rows),'requested':len(rows),'enrichment_attempts':attempted}
