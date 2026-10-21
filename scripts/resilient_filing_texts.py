"""Recover full filing text from an artifact ZIP using resumable streaming.

The archive stays outside git. GitHub authorization is used only by the existing
source reader to obtain a fresh signed URL; storage requests have no token.
"""
import argparse
import hashlib
import http.client
import importlib.util
import json
import re
import tempfile
import time
import urllib.error
import urllib.request
import zipfile
from pathlib import Path


def download_zip(signed_url, target, *, attempts=5, sleep=time.sleep, expected_sha256=None):
    """Resume interrupted bytes; reject inconsistent ranges or identity changes."""
    target = Path(target)
    partial = target.with_suffix(target.suffix+'.part')
    total = None
    def finish():
        if expected_sha256:
            with partial.open('rb') as stream:
                digest = hashlib.file_digest(stream, 'sha256').hexdigest()
            if digest != expected_sha256:
                raise ValueError('artifact ZIP checksum mismatch')
        partial.replace(target)
        return target
    for attempt in range(attempts):
        start = partial.stat().st_size if partial.exists() else 0
        if total is not None and start == total:
            return finish()
        headers = {'Accept-Encoding': 'identity'}
        if start:
            headers['Range'] = f'bytes={start}-'
        try:
            request = urllib.request.Request(signed_url(), headers=headers)
            with urllib.request.urlopen(request, timeout=60) as response:
                if start:
                    match = re.fullmatch(r'bytes (\d+)-(\d+)/(\d+)', response.headers.get('Content-Range', ''))
                    if response.status != 206 or not match:
                        raise ValueError('archive server did not honor resume range')
                    first, last, size = map(int, match.groups())
                    if first != start or last != size-1:
                        raise ValueError('archive resume range mismatch')
                else:
                    if response.status != 200:
                        raise ValueError('unexpected archive download status')
                    size = int(response.headers['Content-Length'])
                if size <= start or (total is not None and size != total):
                    raise ValueError('archive size changed during download')
                total = size
                with partial.open('ab' if start else 'wb') as stream:
                    while True:
                        block = response.read(1024*1024)
                        if not block:
                            break
                        stream.write(block)
                        if stream.tell() > total:
                            raise ValueError('archive response exceeds its declared length')
            if partial.stat().st_size != total:
                raise http.client.IncompleteRead(b'')
            return finish()
        except urllib.error.HTTPError as exc:
            # An expired storage URL can be renewed through the authorized API.
            if exc.code not in (401, 403, 408, 429, 500, 502, 503, 504):
                raise RuntimeError('archive download returned HTTP '+str(exc.code)) from None
        except (urllib.error.URLError, TimeoutError, http.client.IncompleteRead, ConnectionError):
            pass
        if attempt+1 < attempts:
            print(f'Archive download interrupted; resuming attempt {attempt+2}/{attempts}', flush=True)
            sleep(min(2**attempt, 16))
    raise RuntimeError('archive download exhausted bounded retries')


def load_reader(path):
    spec = importlib.util.spec_from_file_location('verified_filing_reader', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repository', required=True)
    parser.add_argument('--artifact-id', type=int, required=True)
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--reader', type=Path, default=Path(__file__).with_name('materialize_filing_texts.py'))
    parser.add_argument('--as-of')
    parser.add_argument('--ticker', action='append')
    args = parser.parse_args()
    reader = load_reader(args.reader)
    args.out.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='filing-zip-', dir=args.out.parent) as temp:
        archive = download_zip(lambda: reader.signed_artifact_url(args.repository, args.artifact_id), Path(temp)/'source.zip')
        with zipfile.ZipFile(archive) as source:
            report = reader.materialize(source, args.out, as_of=args.as_of, tickers=args.ticker)
        report.update(artifact_id=args.artifact_id, artifact_bytes=archive.stat().st_size,
                      download_strategy='resumable whole ZIP; selectively retain verified text objects')
    marker = args.out/'text-materialization-report.json'
    pending = marker.with_suffix('.tmp')
    pending.write_text(json.dumps(report, indent=2), encoding='utf-8')
    pending.replace(marker)
    print(json.dumps(report), flush=True)


if __name__ == '__main__':
    main()
