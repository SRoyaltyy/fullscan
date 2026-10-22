import json
from pathlib import Path
import tempfile
import unittest
from scripts.materialize_completed_profiles import materialize
from src.test_completed_profile_store import fixture


class MaterializationControls(unittest.TestCase):
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
