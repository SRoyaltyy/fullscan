import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch, MagicMock
from urllib.error import HTTPError
from scripts.materialize_completed_profiles import materialize, fetch_url
from src.test_completed_profile_store import fixture


class MaterializationControls(unittest.TestCase):
    def test_transient_503_retries_then_returns_complete_bytes(self):
        response=MagicMock();response.__enter__.return_value.read.return_value=b'complete'
        with patch('urllib.request.urlopen',side_effect=[HTTPError('url',503,'timeout',{},None),response]) as request, patch('time.sleep') as sleep:
            self.assertEqual(fetch_url('url'),b'complete')
            self.assertEqual(request.call_count,2)
            sleep.assert_called_once_with(1)

    def test_retry_is_bounded_and_permanent_errors_fail_immediately(self):
        for status,attempts in ((503,4),(404,1)):
            with self.subTest(status=status),patch('urllib.request.urlopen',side_effect=HTTPError('url',status,'error',{},None)) as request,patch('time.sleep'):
                with self.assertRaises(HTTPError):fetch_url('url')
                self.assertEqual(request.call_count,attempts)

    def test_no_market_index_is_committed_until_all_issuer_assets_validate(self):
        manifest,_,raw=fixture()
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            with self.assertRaisesRegex(ValueError,'checksum'):
                materialize(manifest,root,fetch=lambda url:b'corrupt')
            self.assertFalse((root/'index.json').exists())
            materialize(manifest,root,fetch=lambda url:raw)
            self.assertEqual(json.loads((root/'index.json').read_text())['profiles'],manifest['profiles'])
            # Valid saved assets resume without a second download.
            materialize(manifest,root,fetch=lambda url:self.fail('unexpected network'))


if __name__=='__main__':unittest.main()
