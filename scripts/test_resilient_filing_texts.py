import hashlib
import http.client
import io
import tempfile
import unittest
import urllib.error
from pathlib import Path
from unittest.mock import patch
from scripts.resilient_filing_texts import download_zip


class Response(io.BytesIO):
    def __init__(self, data, status=200, headers=None, fail=False):
        super().__init__(data)
        self.status = status
        self.headers = headers or {'Content-Length': str(len(data))}
        self.fail = fail
    def read(self, n=-1):
        if self.fail and self.tell():
            raise http.client.IncompleteRead(b'')
        return super().read(2 if self.fail else n)


class DownloadControls(unittest.TestCase):
    def run_download(self, responses, **kwargs):
        seen = []
        def open_request(request, **ignored):
            seen.append(dict(request.header_items()))
            response = responses.pop(0)
            if isinstance(response, Exception):
                raise response
            return response
        with tempfile.TemporaryDirectory() as tmp, patch('urllib.request.urlopen', open_request):
            result = download_zip(lambda:'https://example.blob.core.windows.net/archive', Path(tmp)/'source.zip', sleep=lambda n:None, **kwargs)
            return result.read_bytes(), seen

    def test_interrupted_download_resumes_without_duplicate_bytes_or_credentials(self):
        raw, requests = self.run_download([Response(b'abcdef', fail=True), Response(b'cdef', 206, {'Content-Range':'bytes 2-5/6'})])
        self.assertEqual(raw, b'abcdef')
        self.assertEqual(requests[1]['Range'], 'bytes=2-')
        self.assertTrue(all('Authorization' not in r for r in requests))

    def test_expired_storage_url_is_renewed(self):
        expired = urllib.error.HTTPError('redacted',403,'expired',{},None)
        raw, _ = self.run_download([expired, Response(b'abc')])
        self.assertEqual(raw,b'abc')

    def test_ignored_resume_range_is_rejected(self):
        with self.assertRaisesRegex(ValueError,'resume range'):
            self.run_download([Response(b'abcdef',fail=True), Response(b'abcdef')])

    def test_changed_archive_size_is_rejected(self):
        with self.assertRaisesRegex(ValueError,'size changed'):
            self.run_download([Response(b'abcdef',fail=True),Response(b'cdefg',206,{'Content-Range':'bytes 2-6/7'})])

    def test_missing_archive_is_not_retried(self):
        with self.assertRaisesRegex(RuntimeError,'HTTP 404'):
            self.run_download([urllib.error.HTTPError('redacted',404,'missing',{},None)])

    def test_bounded_retries_do_not_mark_a_partial_file_complete(self):
        with self.assertRaisesRegex(RuntimeError,'bounded retries'):
            self.run_download([urllib.error.URLError('network')]*2,attempts=2)

    def test_wrong_artifact_hash_is_rejected(self):
        with self.assertRaisesRegex(ValueError,'checksum'):
            self.run_download([Response(b'abc')],expected_sha256=hashlib.sha256(b'xyz').hexdigest())


if __name__ == '__main__':
    unittest.main()
