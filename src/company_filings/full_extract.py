"""Lossless submission splitting and uncapped, whole-document text extraction.

The original SEC submission bytes are authoritative. Text is a derived view;
tables and all visible document sections are retained, and extraction failures
remain explicit. This module never calls the excerpt splitter.
"""
from __future__ import annotations

import binascii
import hashlib
import os
import re
import subprocess
import tempfile
import urllib.parse
from pathlib import Path
from pathlib import PurePosixPath

from lxml import html as lhtml

EXTRACTOR_VERSION = 'full-document-1'
_DOCUMENT = re.compile(rb'(?ms)^<DOCUMENT>[ \t]*\r?\n(.*?)^</DOCUMENT>[ \t]*\r?$', re.I)
_NON_TEXT = {'.gif', '.jpg', '.jpeg', '.png', '.bmp', '.ico', '.zip', '.xls', '.xlsx', '.ppt', '.pptx'}


def sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _field(header: bytes, name: bytes) -> str:
    found = re.search(rb'(?im)^<' + name + rb'>[ \t]*([^\r\n<]*)', header)
    return found.group(1).decode('utf-8', 'replace').strip() if found else ''


def _filename(value: str) -> str:
    # SEC display links may be paths, but an embedded submission file must have
    # a single safe filename. Refuse ambiguous archive paths rather than overwrite.
    if not value or value in {'.', '..'} or '/' in value or '\\' in value or '\x00' in value:
        raise ValueError('Invalid embedded filing filename')
    return value


def decode_payload(body: bytes) -> tuple[bytes, str]:
    candidate = body.strip(b'\r\n')
    for wrapper in (b'PDF', b'GRAPHIC', b'XML', b'ZIP', b'EXCEL'):
        opening, closing = b'<' + wrapper + b'>', b'</' + wrapper + b'>'
        if candidate.upper().startswith(opening) and candidate.upper().endswith(closing):
            candidate = candidate[len(opening):-len(closing)].strip(b'\r\n')
            break
    lines = candidate.splitlines()
    if lines and re.match(rb'^begin [0-7]{3} ', lines[0]):
        if not any(line.strip() == b'end' for line in lines[1:]):
            raise ValueError('Incomplete uuencoded filing attachment')
        decoded = bytearray()
        for line in lines[1:]:
            if line.strip() == b'end':
                break
            if line in (b'', b'`', b' '):
                continue
            try:
                value = binascii.a2b_uu(line)
            except binascii.Error:
                # Some SEC encoders leave nonzero padding bits/characters after
                # the declared byte count. Decode exactly that count, as the
                # CPython uu reader does, without losing any declared payload.
                declared = (line[0] - 32) & 63
                required = 1 + (declared * 4 + 2) // 3
                if len(line) < required:
                    raise ValueError('Truncated uuencoded attachment line')
                value = binascii.a2b_uu(line[:required])
                if len(value) != declared:
                    raise ValueError('Invalid uuencoded attachment byte count')
            decoded.extend(value)
        return bytes(decoded), 'uuencoded'
    return candidate, 'plain'


def split_submission(raw: bytes) -> list[dict]:
    """Index every embedded document, including exhibits, PDFs and XBRL files."""
    if not raw.rstrip().upper().endswith(b'</SEC-DOCUMENT>'):
        raise ValueError('Incomplete SEC submission: terminal marker missing')
    matches = list(_DOCUMENT.finditer(raw))
    starts = len(re.findall(rb'(?im)^<DOCUMENT>[ \t]*\r?$', raw))
    if not matches or starts != len(matches):
        raise ValueError('Incomplete SEC submission document boundaries')
    result, names = [], set()
    for match in matches:
        chunk = match.group(1)
        opening = re.search(rb'(?im)^<TEXT>[ \t]*(?:\r?\n)?', chunk)
        closings = list(re.finditer(rb'(?im)^</TEXT>[ \t]*\r?$', chunk))
        if not opening or not closings or closings[-1].start() < opening.end():
            raise ValueError('Incomplete embedded document TEXT boundaries')
        header = chunk[:opening.start()]
        filename = _filename(_field(header, b'FILENAME'))
        if filename in names:
            raise ValueError('Duplicate embedded filing filename')
        names.add(filename)
        start = match.start(1) + opening.end()
        end = match.start(1) + closings[-1].start()
        decoded, encoding = decode_payload(raw[start:end])
        result.append({'filename': filename, 'type': _field(header, b'TYPE'),
                       'sequence': _field(header, b'SEQUENCE'), 'description': _field(header, b'DESCRIPTION'),
                       'start': start, 'end': end, 'encoding': encoding,
                       'byte_count': len(decoded), 'sha256': sha256(decoded)})
    return result


def file_bytes(raw: bytes, document: dict) -> bytes:
    value, _ = decode_payload(raw[document['start']:document['end']])
    if sha256(value) != document['sha256'] or len(value) != document['byte_count']:
        raise ValueError('Embedded filing checksum mismatch')
    return value


def full_html_text(raw: bytes) -> str:
    """Read all visible text and table cells; never drop financial tables."""
    parser = lhtml.HTMLParser(recover=True, huge_tree=True)
    document = lhtml.fromstring(raw, parser=parser)
    for node in list(document.iter()):
        if not isinstance(node.tag, str):
            continue
        local = node.tag.rsplit('}', 1)[-1].rsplit(':', 1)[-1].lower()
        style = (node.get('style') or '').replace(' ', '').lower()
        hidden = local in {'script', 'style', 'noscript', 'header', 'hidden', 'references'} and (':' in node.tag or local in {'script', 'style', 'noscript'})
        if hidden or 'display:none' in style or 'visibility:hidden' in style:
            node.drop_tree()
    parts = []
    blocks = {'p', 'div', 'li', 'tr', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'br', 'section', 'table'}
    def walk(node):
        local = node.tag.rsplit('}', 1)[-1].rsplit(':', 1)[-1].lower() if isinstance(node.tag, str) else ''
        if local in blocks: parts.append('\n')
        if local in {'td', 'th'}: parts.append('\t')
        if node.text: parts.append(node.text)
        for child in node:
            walk(child)
            if child.tail: parts.append(child.tail)
        if local in blocks: parts.append('\n')
    walk(document)
    text = ''.join(parts).replace('\xa0', ' ')
    text = re.sub(r'[ \t]+', ' ', text)
    # A short line is not discarded: preserving words matters more than guessing
    # paragraph boundaries. Chunking belongs to a later, non-destructive reader.
    text = re.sub(r'\n[ \t]*\n+', '\n\n', text)
    return text.strip()


def extract_document(raw: bytes, filename: str, kind: str = '') -> dict:
    suffix = PurePosixPath(filename).suffix.lower()
    if raw.startswith(b'%PDF') or suffix == '.pdf':
        try:
            import fitz
            with fitz.open(stream=raw, filetype='pdf') as pdf:
                texts, empty, ocr_pages = [], [], []
                for i, page in enumerate(pdf):
                    text = page.get_text('text', sort=True)
                    visible = bool(page.get_images() or page.get_drawings())
                    if not text.strip() and visible and os.environ.get('FILINGS_OCR_ENABLED') == '1':
                        try:
                            text = page.get_text('text', textpage=page.get_textpage_ocr(language='eng', dpi=150, full=True))
                            ocr_pages.append(i + 1)
                        except Exception:
                            pass
                    if not text.strip() and visible: empty.append(i + 1)
                    texts.append(text)
                text = '\n\n'.join(f'[Page {i+1}]\n{value}' for i, value in enumerate(texts))
                return {'status': 'needs_ocr' if empty else 'complete', 'text': text,
                        'pages': len(texts), 'pages_without_text': empty, 'ocr_pages': ocr_pages,
                        'method': 'pymupdf_all_pages'}
        except Exception as exc:
            return {'status': 'extraction_failed', 'error_type': type(exc).__name__, 'text': '', 'method': 'pdf'}
    if suffix in _NON_TEXT or kind.upper() in {'GRAPHIC', 'ZIP', 'EXCEL'}:
        return {'status': 'binary_original_preserved', 'text': '', 'method': 'binary'}
    if not raw.strip():
        return {'status': 'empty_document', 'text': '', 'method': 'empty'}
    try:
        if suffix in {'.htm', '.html'} or re.search(rb'<(?:html|!doctype|body|div|p|ix:)', raw[:4096], re.I):
            text = full_html_text(raw)
            method = 'whole_visible_html_including_tables'
        else:
            try: text = raw.decode('utf-8')
            except UnicodeDecodeError: text = raw.decode('cp1252', 'replace')
            method = 'whole_text'
        return {'status': 'complete' if text.strip() else 'extraction_failed', 'text': text, 'method': method}
    except Exception as exc:
        return {'status': 'extraction_failed', 'error_type': type(exc).__name__, 'text': '', 'method': 'html'}


def image_text(raw: bytes) -> dict:
    """OCR a whole original image; originals remain authoritative."""
    if os.environ.get('FILINGS_OCR_ENABLED') != '1':
        return {'status':'needs_ocr','text':'','error_type':'OCRNotEnabled'}
    try:
        with tempfile.TemporaryDirectory(prefix='filing-image-') as directory:
            path=Path(directory)/'image.bin';path.write_bytes(raw)
            environment=dict(os.environ);environment.setdefault('OMP_THREAD_LIMIT','1')
            result=subprocess.run(['tesseract',str(path),'stdout','-l',os.environ.get('FILINGS_OCR_LANGUAGES','eng'),'--psm','3'],
                stdout=subprocess.PIPE,stderr=subprocess.PIPE,check=False,timeout=180,env=environment)
            if result.returncode:return {'status':'needs_ocr','text':'','error_type':'TesseractFailed'}
            return {'status':'complete','text':result.stdout.decode('utf-8','replace')}
    except Exception as exc:
        return {'status':'needs_ocr','text':'','error_type':type(exc).__name__}


def ocr_image_only_html(body: bytes, original: bytes, files: list[dict]) -> dict | None:
    """Read every embedded slide when an HTML exhibit consists only of images."""
    try:document=lhtml.fromstring(body,parser=lhtml.HTMLParser(recover=True,huge_tree=True))
    except Exception:return None
    references=[node.get('src') for node in document.iter('img') if node.get('src')]
    if not references:return None
    indexed={item['filename']:item for item in files};pages=[];parts=[]
    for ordinal,reference in enumerate(references,1):
        url=urllib.parse.urlsplit(reference)
        name=PurePosixPath(urllib.parse.unquote(url.path)).name
        if url.scheme or url.netloc or name not in indexed:
            pages.append({'ordinal':ordinal,'reference':reference,'status':'needs_ocr','error_type':'ImageNotEmbedded'})
            continue
        item=indexed[name];result=image_text(file_bytes(original,item))
        pages.append({'ordinal':ordinal,'filename':name,'sha256':item['sha256'],
                      'status':result['status'],'characters':len(result['text']),
                      'error_type':result.get('error_type')})
        parts.append(f'[Image {ordinal}: {name}]\n'+result['text'])
    return {'status':'complete' if all(p['status']=='complete' for p in pages) else 'needs_ocr',
            'text':'\n\n'.join(parts),'images':pages,'method':'tesseract_every_image_in_image_only_html',
            'ocr_language':os.environ.get('FILINGS_OCR_LANGUAGES','eng')}
