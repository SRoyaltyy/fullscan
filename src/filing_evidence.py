"""Read every dated text view from PR642, without text/document-count caps.

This is a consumer of the full archive, not a replacement for its byte/hash
verifier. The optional SQLite reader is explicitly marked as a capped cache.
"""
from __future__ import annotations

import gzip
import hashlib
import json
import re
import sqlite3
from pathlib import Path


def _safe(root: Path, key: str) -> Path:
    path = (root / key).resolve()
    if not path.is_relative_to(root.resolve()):
        raise ValueError('archive key escapes object root')
    return path


def _json(path: Path):
    raw = path.read_bytes()
    if raw.startswith(b'\x1f\x8b'):
        raw = gzip.decompress(raw)
    return json.loads(raw)


def section_views(text: str, form: str, filename: str = '') -> list[dict]:
    """Keep all sections. Route 10-K/20-F/40-F and current-report views."""
    if Path(filename).suffix.lower() in ('.xsd','.xml','.json','.css','.js'):
        return [{'section':filename,'kind':'machine_data','text':text}]
    if re.search(r'ex(?:hibit)?[-_ ]?21\b',filename,re.I):
        return [{'section':'Exhibit 21','kind':'subsidiary','text':text}]
    form = form.upper().split('/')[0]
    if form in ('8-K','6-K'):
        # Current filings are event updates, not rewritten business identity.
        return [{'section':filename or form,'kind':'update','text':text}]
    if form in ('S-1','F-1','S-4','F-4'):
        # Registration statements supply the business source for new issuers.
        # The profile compiler routes their named business/risk headings.
        return [{'section':filename or form,'kind':'business','text':text}]
    matches = list(re.finditer(r'(?im)^\s*(?:PART\s+[IVX]+\s*[-.:]?\s*)?(ITEM\s+(\d+[A-Z]?))\s*[.:-]?\s*([^\n]{0,140})',text))
    if not matches:
        # 40-F often incorporates its AIF. Attached readable reports remain
        # available instead of silently rejecting the entire issuer.
        return [{'section':filename or form,'kind':'business' if form in ('40-F','20-F','10-K') else 'update','text':text}]
    views = []
    if matches and text[:matches[0].start()].strip():
        views.append({'section':'preamble','kind':'identity_source','text':text[:matches[0].start()]})
    for i,m in enumerate(matches):
        end = matches[i+1].start() if i+1 < len(matches) else len(text)
        body = text[m.start():end]
        # Retain short sections too. A heading is not operating evidence,
        # but dropping its text would break full-document source coverage.
        number = m.group(2).upper()
        label = m.group(0).strip()
        kind = 'financial'
        if form == '10-K':
            kind = {'1':'business','1A':'risk','2':'geography','3':'risk','7':'update','7A':'risk','10':'people','12':'subsidiary'}.get(number,'financial')
        elif form == '20-F':
            kind = {'3':'risk','4':'business','4A':'business','5':'update','6':'people','7':'subsidiary','8':'risk','10':'risk','11':'risk'}.get(number,'financial')
        elif form == '10-Q':
            kind = 'risk' if 'risk' in label.lower() else 'update' if number in ('2','1A') else 'financial'
        elif form == '40-F':
            kind = 'business'
        views.append({'section':label,'kind':kind,'text':body})
    return views


def load_filing_sections(objects: Path, *, as_of: str | None = None, strict: bool = False, ciks=None) -> tuple[dict,dict]:
    objects = Path(objects).resolve()
    root = objects / 'company-filings' / 'full-v1'
    if not root.is_dir():
        root = objects
    output = {}
    report = {'source':'PR642-full-text','companies':0,'filings':0,'documents':0,'characters':0,'errors':[],
              'as_of':as_of,'full_source_archive':True}
    for company_path in sorted(root.glob('*/company.json')):
        if ciks is not None and company_path.parent.name not in ciks:
            continue
        company = _json(company_path)
        chunks = []
        filings = sorted(company.get('filings') or [],key=lambda r:(str(r.get('filing_date','')),str(r.get('accession',''))))
        expected = company.get('expected_filings')
        if expected is not None and len(filings) < int(expected):
            report['errors'].append({'cik':company.get('cik'),'error':'missing expected filing records','expected':expected,'available':len(filings)})
        if company.get('failures'):
            report['errors'].append({'cik':company.get('cik'),'error':'company archive records failures','count':len(company['failures'])})
        for filing in filings:
            date = str(filing.get('filing_date') or '')
            if as_of and not date:
                report['errors'].append({'cik':company.get('cik'),'accession':filing.get('accession'),'error':'undated filing cannot satisfy as-of cutoff'})
                continue
            if as_of and date > as_of:
                continue
            accession = str(filing.get('accession') or '')
            try:
                if not filing.get('text_complete'):
                    raise ValueError('text_complete is false')
                manifest = _json(_safe(objects,filing['manifest_key'])) if filing.get('manifest_key') else _json(company_path.parent / accession / 'manifest.json')
                if manifest.get('truncated') or manifest.get('text_complete') is False:
                    raise ValueError('manifest text incomplete/truncated')
                key = manifest.get('texts',{}).get('key')
                if not key:
                    raise ValueError('missing full text view')
                view_path = _safe(objects,key)
                view_bytes = view_path.read_bytes()
                stored = manifest.get('texts',{}).get('object',{})
                if stored.get('sha256') and hashlib.sha256(view_bytes).hexdigest() != stored['sha256']:
                    raise ValueError('stored text object checksum mismatch')
                decoded = gzip.decompress(view_bytes) if view_bytes.startswith(b'\x1f\x8b') else view_bytes
                expected_sha = manifest.get('texts',{}).get('sha256')
                if expected_sha and hashlib.sha256(decoded).hexdigest() != expected_sha:
                    raise ValueError('full text checksum mismatch')
                views = json.loads(decoded)
                missing = [f.get('filename') for f in manifest.get('files',[]) if f.get('text_characters',0)>0 and f.get('filename') not in views]
                if missing:
                    raise ValueError('text views missing: '+','.join(missing[:10]))
                report['filings'] += 1
                file_metadata={f.get('filename'):f for f in manifest.get('files',[])}
                for filename, document in views.items():
                    text = str(document.get('text') or '') if isinstance(document,dict) else str(document or '')
                    if not text.strip():
                        continue
                    report['documents'] += 1
                    report['characters'] += len(text)
                    for section in section_views(text,str(filing.get('form') or manifest.get('form') or ''),filename):
                        section.update({'source':'PR642','filing_date':date,'accession':accession,'form':filing.get('form',''),
                            'document':filename,'complete':True,
                            'document_type':file_metadata.get(filename,{}).get('type',''),
                            'document_description':file_metadata.get(filename,{}).get('description',''),
                            'accepted_at':filing.get('accepted_at_raw',''), 'report_date':filing.get('report_date',''),
                            'url':f'https://www.sec.gov/Archives/edgar/data/{int(company["cik"])}/{accession.replace("-", "")}/{filename}'})
                        chunks.append(section)
            except (OSError,ValueError,KeyError,TypeError) as exc:
                report['errors'].append({'cik':company.get('cik'),'accession':accession,'error':str(exc)})
        for ticker in company.get('tickers') or []:
            output[str(ticker).upper()] = chunks
        report['companies'] += 1
    report['complete'] = bool(report['companies']) and not report['errors']
    if strict and not report['complete']:
        raise ValueError('filing evidence incomplete: '+json.dumps(report['errors'][:5]))
    return output,report


def load_section_cache(path: Path, *, as_of: str | None = None, exclude_accessions: set[str] | None = None, progress=None) -> tuple[list[dict],dict]:
    """Compatibility/import route. Never label this older capped source complete."""
    c = sqlite3.connect('file:'+str(Path(path).resolve()).replace('\\','/')+'?mode=ro',uri=True)
    c.row_factory = sqlite3.Row
    companies=list(c.execute('SELECT * FROM companies ORDER BY ticker'))
    by_cik={row['cik']:[] for row in companies}
    chars = 0
    excluded = exclude_accessions or set()
    # Do not SQL-sort compressed BLOBs: that can spill the entire archive to
    # a multi-gigabyte temporary sort. Sort lightweight chunk records later.
    sql = '''SELECT d.*,s.item,s.text_gz,s.n_chars FROM documents d JOIN sections s USING(accession)'''
    consumed=0
    for item in c.execute(sql):
            if item['cik'] not in by_cik:
                continue
            if item['accession'] in excluded or (as_of and item['filing_date'] > as_of):
                continue
            form = item['form'].upper().split('/')[0]
            section = item['item'].lower()
            kind = None
            if form == '10-K':
                kind = {'1':'business','1a':'risk','2':'geography','3':'risk','7':'update','7a':'risk'}.get(section)
            elif form in ('20-F','40-F'):
                kind = {'business':'business','4':'business','risk':'risk','mda':'update'}.get(section)
            elif form == '10-Q':
                kind = {'1-2':'update','2-1a':'risk'}.get(section)
            elif form in ('8-K','6-K'):
                kind = 'update' if section not in ('notes','body') else None
            elif form in ('S-1','F-1','S-4','F-4'):
                kind = 'business'
            if kind is None:
                continue
            text = gzip.decompress(item['text_gz']).decode('utf-8',errors='replace')
            chars += len(text)
            by_cik[item['cik']].append({'source':'section-cache','form':item['form'],'section':section,'kind':kind,'text':text,
                'filing_date':item['filing_date'],'accession':item['accession'],'complete':False})
            consumed+=1
            if progress and consumed%2000==0:progress({'sections':consumed,'characters':chars})
    rows=[]
    for company in companies:
        by_cik[company['cik']].sort(key=lambda r:(r['filing_date'],r['accession'],r['section']))
        rows.append({'ticker':company['ticker'],'name':company['name'],'industry':company['industry'] or '',
            'sector':company['sector'] or '', 'description':'','filing_sections':by_cik[company['cik']]})
    c.close()
    return rows,{'source':'legacy-section-cache','companies':len(rows),'characters':chars,'as_of':as_of,
        'complete':False,'full_source_archive':False,'warning':'Selected/capped historical sections; not full PR642 coverage'}

