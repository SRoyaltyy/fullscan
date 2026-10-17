"""Complete SEC submission archive, independent of the excerpt SQLite store."""
from __future__ import annotations

import base64
import gzip
import hashlib
import json
import os
import re
import threading
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath

from .edgar import Client, archive_url, blocked_reason, EdgarBlocked
from .full_extract import EXTRACTOR_VERSION, extract_document, file_bytes, ocr_image_only_html, sha256, split_submission
from .select import (ANNUAL_FORMS, ANNUAL_AMENDMENTS, FUND_ANNUAL_FORMS,
                     FUND_ANNUAL_AMENDMENTS, REGISTRATION_FORMS,
                     REGISTRATION_AMENDMENTS, QUARTERLY_FORMS, CURRENT_FORMS,
                     FOREIGN_FORMS, canonical_form)

ARCHIVE_VERSION = 'full-submission-1'
PREFIX = 'company-filings/full-v1'


def timestamp():
    return datetime.now(timezone.utc).isoformat()


def json_bytes(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True).encode('utf-8')


def object_key(key):
    path = PurePosixPath(key)
    if path.is_absolute() or '..' in path.parts or '\\' in key:
        raise ValueError('Unsafe archive object key')
    return key


class LocalObjects:
    durable = False
    def __init__(self, root):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
    def put(self, key, data):
        path = self.root / object_key(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(path.name + '.partial')
        temporary.write_bytes(data)
        temporary.replace(path)
        if path.stat().st_size != len(data):
            raise ValueError('Incomplete local archive write')
        return {'sha256': sha256(data), 'stored_bytes': len(data)}
    def get(self, key):
        return (self.root / object_key(key)).read_bytes()
    def maybe_json(self, key):
        try: return json.loads(self.get(key))
        except FileNotFoundError: return None


def r2_configured():
    return all(os.environ.get(k, '').strip() for k in ('R2_ENDPOINT_URL', 'R2_ACCESS_KEY_ID', 'R2_SECRET_ACCESS_KEY', 'R2_BUCKET'))


class R2Objects:
    durable = True
    def __init__(self):
        if not r2_configured():
            raise ValueError('R2 storage is not configured')
        import boto3
        from botocore.config import Config
        self.client = boto3.client('s3', endpoint_url=os.environ['R2_ENDPOINT_URL'],
            aws_access_key_id=os.environ['R2_ACCESS_KEY_ID'],
            aws_secret_access_key=os.environ['R2_SECRET_ACCESS_KEY'], region_name='auto',
            config=Config(signature_version='s3v4', request_checksum_calculation='when_required',
                          response_checksum_validation='when_required'))
        self.bucket = os.environ['R2_BUCKET']
        self.client.head_bucket(Bucket=self.bucket)
    def put(self, key, data):
        digest = sha256(data)
        self.client.put_object(Bucket=self.bucket, Key=object_key(key), Body=data,
            ContentMD5=base64.b64encode(hashlib.md5(data).digest()).decode(),
            Metadata={'sha256': digest})
        head = self.client.head_object(Bucket=self.bucket, Key=key)
        if head['ContentLength'] != len(data) or head.get('Metadata', {}).get('sha256') != digest:
            raise ValueError('R2 object verification failed')
        return {'sha256': digest, 'stored_bytes': len(data)}
    def get(self, key):
        response = self.client.get_object(Bucket=self.bucket, Key=object_key(key))
        try: return response['Body'].read()
        finally: response['Body'].close()
    def maybe_json(self, key):
        from botocore.exceptions import ClientError
        try: return json.loads(self.get(key))
        except ClientError as exc:
            if str(exc.response.get('Error', {}).get('Code')) in {'NoSuchKey', '404', 'NotFound'}:
                return None
            raise


class WholeClient(Client):
    """The full archive deliberately does not use Client.document's 25MB cap."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.blocked = threading.Event()
    def _get(self, url):
        if self.blocked.is_set(): raise EdgarBlocked('SEC access refused earlier in this run')
        try: return super()._get(url)
        except EdgarBlocked:
            self.blocked.set()
            raise
    def whole_url(self, url):
        response = self._get(url)
        raw = response.content
        reason = blocked_reason(raw[:25000].decode('utf-8', 'replace'))
        if reason: raise EdgarBlocked(reason)
        size = response.headers.get('Content-Length')
        if size and not response.headers.get('Content-Encoding') and int(size) != len(raw):
            raise ValueError('Incomplete HTTP response body')
        return raw
    def submission(self, cik, accession):
        return self.whole_url(archive_url(cik, accession, accession + '.txt'))


def columnar_rows(block):
    forms = block.get('form') or []
    def at(name, i):
        values = block.get(name) or []
        return values[i] if i < len(values) else ''
    return [{'form': canonical_form(form), 'native_form': form,
             'accession': at('accessionNumber', i), 'filing_date': at('filingDate', i),
             'report_date': at('reportDate', i), 'accepted_at_raw': at('acceptanceDateTime', i),
             'primary': at('primaryDocument', i), 'description': at('primaryDocDescription', i),
             'items': at('items', i)} for i, form in enumerate(forms)]


def select_whole_filings(rows):
    """Latest base plus every later 10-Q, 8-K and 6-K, without item/byte caps."""
    ordered = sorted(rows, key=lambda r: (r.get('filing_date', ''), r.get('accession', '')), reverse=True)
    base, amendments, reason = None, set(), ''
    for originals, amends, label in ((ANNUAL_FORMS, ANNUAL_AMENDMENTS, 'annual'),
                                    (FUND_ANNUAL_FORMS, FUND_ANNUAL_AMENDMENTS, 'fund_annual'),
                                    (REGISTRATION_FORMS, REGISTRATION_AMENDMENTS, 'registration')):
        base = next((r for r in ordered if r['form'] in originals), None)
        if base is None:
            base = next((r for r in ordered if r['form'] in amends), None)
        if base:
            amendments, reason = amends, label
            break
    if base is None:
        candidates = [r for r in ordered if r['form'] in QUARTERLY_FORMS | FOREIGN_FORMS]
        if not candidates: return [], 'no_required_filing'
        # With no annual or registration, retain the available quarterly/foreign
        # narrative set. Do not apply the old eight-6-K ceiling.
        chosen = candidates + [r for r in ordered if r['form'] in CURRENT_FORMS]
        reason = 'available_quarterly_or_foreign_reports_no_base'
    else:
        anchor = base.get('filing_date') or ''
        period = base.get('report_date') or ''
        chosen = [base]
        for row in ordered:
            if row['accession'] == base['accession']: continue
            if row['form'] in amendments and row.get('filing_date', '') >= anchor and row.get('report_date', '') == period:
                chosen.append(row)
            elif row['form'] in QUARTERLY_FORMS | CURRENT_FORMS | FOREIGN_FORMS and row.get('filing_date', '') >= anchor:
                chosen.append(row)
    by_id = {r['accession']: r for r in chosen if r.get('accession')}
    return sorted(by_id.values(), key=lambda r: (r.get('filing_date', ''), r['accession'])), reason


def filing_inventory(client, cik):
    payload = client.submissions(cik)
    rows = columnar_rows((payload.get('filings') or {}).get('recent') or {})
    histories = []
    if not any(row['form'] in ANNUAL_FORMS | ANNUAL_AMENDMENTS for row in rows):
        for history in (payload.get('filings') or {}).get('files') or []:
            name = history.get('name')
            if not name: continue
            block = client.submissions_file(name)
            histories.append({'name': name, 'sha256': sha256(json_bytes(block))})
            rows.extend(columnar_rows(block if block.get('form') else (block.get('filings') or {}).get('recent') or {}))
            if any(row['form'] in ANNUAL_FORMS | ANNUAL_AMENDMENTS for row in rows): break
    chosen, reason = select_whole_filings(rows)
    return chosen, {'retrieved_at': timestamp(), 'selection': reason, 'submissions_sha256': sha256(json_bytes(payload)),
                    'submission_history_files': histories, 'issuer_name': payload.get('name'),
                    'available_rows': len(rows), 'expected_accessions': [r['accession'] for r in chosen]}


def _manifest_key(cik, accession):
    return f'{PREFIX}/{cik}/{accession}/manifest.json'


def _verify_existing(objects, manifest):
    if manifest.get('archive_version') != ARCHIVE_VERSION or manifest.get('extractor_version') != EXTRACTOR_VERSION:
        return False
    raw = gzip.decompress(objects.get(manifest['source']['key']))
    if len(raw) != manifest['source']['byte_count'] or sha256(raw) != manifest['source']['sha256']:
        raise ValueError('Archived original checksum mismatch')
    texts_raw = gzip.decompress(objects.get(manifest['texts']['key']))
    if sha256(texts_raw) != manifest['texts']['sha256']:
        raise ValueError('Archived text checksum mismatch')
    texts = json.loads(texts_raw)
    if set(texts) != {r['filename'] for r in manifest['files']}:
        raise ValueError('Archived document inventory mismatch')
    for document in manifest['files']:
        file_bytes(raw, document)
        text = texts[document['filename']]['text']
        if len(text) != document['text_characters'] or sha256(text.encode()) != document['text_sha256']:
            raise ValueError('Archived full text checksum mismatch')
    return True


def archive_filing(client, objects, cik, filing):
    accession = filing['accession']
    key = _manifest_key(cik, accession)
    previous = objects.maybe_json(key)
    if previous and _verify_existing(objects, previous):
        if previous.get('text_complete'): return previous, True
        raw = gzip.decompress(objects.get(previous['source']['key']))
    else:
        download_key = f'{PREFIX}/{cik}/{accession}/download.json'
        downloaded = objects.maybe_json(download_key)
        if downloaded:
            raw = gzip.decompress(objects.get(downloaded['source']['key']))
            if sha256(raw) != downloaded['source']['sha256'] or len(raw) != downloaded['source']['byte_count']:
                raise ValueError('Downloaded original checksum mismatch')
        else:
            raw = client.submission(cik, accession)
    # Preserve the authoritative original even if a new attachment encoding or
    # document format defeats the derived reader. This is not a completion flag.
    raw_sha = sha256(raw)
    raw_key = f'{PREFIX}/{cik}/{accession}/source/{raw_sha}.sgml.gz'
    source_storage = objects.put(raw_key, gzip.compress(raw, mtime=0))
    objects.put(f'{PREFIX}/{cik}/{accession}/download.json', json_bytes({
        'filing': filing, 'retrieved_at': timestamp(),
        'source': {'key': raw_key, 'sha256': raw_sha, 'byte_count': len(raw)},
        'source_url': archive_url(cik, accession, accession + '.txt')}))
    files = split_submission(raw)
    primary = PurePosixPath(filing.get('primary') or '').name
    if not primary or primary not in {item['filename'] for item in files}:
        raise ValueError('Primary document absent from complete submission')
    texts = {}
    for document in files:
        body = file_bytes(raw, document)
        text = extract_document(body, document['filename'], document['type'])
        if text['status']=='extraction_failed' and not text['text']:
            visual = ocr_image_only_html(body,raw,files)
            if visual:text=visual
        texts[document['filename']] = text
        document.update(text_status=text['status'], text_characters=len(text['text']),
                        text_sha256=sha256(text['text'].encode('utf-8')))
    texts_raw = json_bytes(texts)
    text_sha = sha256(texts_raw)
    text_key = f'{PREFIX}/{cik}/{accession}/text/{EXTRACTOR_VERSION}/{text_sha}.json.gz'
    text_storage = objects.put(text_key, gzip.compress(texts_raw, mtime=0))
    header_end = raw.upper().find(b'<DOCUMENT>')
    header = raw[:header_end if header_end >= 0 else 0]
    acceptance = re.search(rb'<ACCEPTANCE-DATETIME>(\d+)', header, re.I)
    text_failures = [item['filename'] for item in files if item['text_status'] not in {'complete', 'binary_original_preserved'}]
    manifest = {'archive_version': ARCHIVE_VERSION, 'extractor_version': EXTRACTOR_VERSION,
        'cik': cik, 'accession': accession, 'form': filing['form'], 'native_form': filing.get('native_form', filing['form']),
        'filing_date': filing.get('filing_date') or None, 'report_date': filing.get('report_date') or None,
        'accepted_at_raw': filing.get('accepted_at_raw') or (acceptance.group(1).decode() if acceptance else None),
        'retrieved_at': timestamp(), 'source_url': archive_url(cik, accession, accession + '.txt'),
        'primary_document': primary, 'source': {'key': raw_key, 'byte_count': len(raw), 'sha256': raw_sha, **{'object': source_storage}},
        'texts': {'key': text_key, 'sha256': text_sha, 'object': text_storage},
        'files': files, 'raw_complete': True, 'text_complete': not text_failures,
        'text_failures': text_failures, 'truncated': False, 'durable': objects.durable,
        'binary_assets': [d['filename'] for d in files if d['text_status'] == 'binary_original_preserved']}
    # A manifest is committed only after both uncapped objects are written.
    objects.put(key, json_bytes(manifest))
    _verify_existing(objects, manifest)
    return manifest, False


def archive_company(company, client, objects):
    cik = company['cik']
    chosen, inventory = filing_inventory(client, cik)
    result = {'cik': cik, 'tickers': company['tickers'], 'archive_version': ARCHIVE_VERSION,
        'updated_at': timestamp(), 'inventory': inventory, 'expected_filings': len(chosen),
        'raw_complete_filings': 0, 'text_complete_filings': 0, 'files': 0, 'original_bytes': 0,
        'cached_filings': 0, 'filings': [], 'failures': [], 'durable': objects.durable}
    key = f'{PREFIX}/{cik}/company.json'
    if not chosen:
        result['failures'].append({'stage': 'inventory', 'error_type': 'NoRequiredFiling'})
    for filing in chosen:
        try:
            manifest, reused = archive_filing(client, objects, cik, filing)
            result['raw_complete_filings'] += int(manifest['raw_complete'])
            result['text_complete_filings'] += int(manifest['text_complete'])
            result['files'] += len(manifest['files'])
            result['original_bytes'] += manifest['source']['byte_count']
            result['cached_filings'] += int(reused)
            result['filings'].append({'accession': filing['accession'], 'form': filing['form'],
                'filing_date': manifest['filing_date'], 'report_date': manifest['report_date'],
                'manifest_key': _manifest_key(cik, filing['accession']), 'raw_complete': manifest['raw_complete'],
                'text_complete': manifest['text_complete'], 'text_failures': manifest['text_failures']})
        except Exception as exc:
            result['failures'].append({'accession': filing['accession'], 'form': filing['form'],
                'filing_date': filing.get('filing_date'), 'source_url': archive_url(cik, filing['accession'], filing['accession'] + '.txt'),
                'error_type': type(exc).__name__,
                'error_detail': str(exc)[:500] if isinstance(exc, ValueError) else None})
            if isinstance(exc, EdgarBlocked): break
        result['raw_complete'] = bool(chosen) and result['raw_complete_filings'] == len(chosen) and not result['failures']
        result['text_complete'] = result['raw_complete'] and result['text_complete_filings'] == len(chosen)
        objects.put(key, json_bytes(result))
    result.setdefault('raw_complete', False)
    result.setdefault('text_complete', False)
    objects.put(key, json_bytes(result))
    return result


def read_filing(objects, cik, accession):
    manifest = objects.maybe_json(_manifest_key(cik, accession))
    if not manifest: raise KeyError(accession)
    _verify_existing(objects, manifest)
    return manifest, json.loads(gzip.decompress(objects.get(manifest['texts']['key'])))


def read_original(objects, cik, accession, filename=None):
    manifest = objects.maybe_json(_manifest_key(cik, accession))
    if not manifest: raise KeyError(accession)
    raw = gzip.decompress(objects.get(manifest['source']['key']))
    if sha256(raw) != manifest['source']['sha256']: raise ValueError('Archived original checksum mismatch')
    if filename is None: return raw
    document = next((r for r in manifest['files'] if r['filename'] == filename), None)
    if document is None: raise KeyError(filename)
    return file_bytes(raw, document)
