import json
import os
import shutil
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch
from scripts.recover_filing_profiles import select_shards, merge_without_duplicate_bytes


class RecoveryControls(unittest.TestCase):
    def profile(self, folder, complete=True):
        folder.mkdir(parents=True)
        (folder/'index.json').write_text(json.dumps({'complete':complete}))

    def test_completed_work_reused_and_recovery_wins_only_its_shard(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)
            downloads=root/'downloaded'
            self.profile(downloads/'deep-filing-profiles-0')
            self.profile(downloads/'deep-filing-profiles-1')
            self.profile(downloads/'deep-filing-profiles-recovered-1')
            select_shards(downloads,root/'selected',[0,1])
            self.assertTrue((root/'selected/0/index.json').is_file())
            self.assertTrue((root/'selected/1/index.json').is_file())
            self.assertTrue((downloads/'deep-filing-profiles-1/index.json').is_file())
            self.assertFalse((downloads/'deep-filing-profiles-recovered-1').exists())

    def test_incomplete_or_missing_shard_cannot_be_merged(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)
            self.profile(root/'downloaded/deep-filing-profiles-0',False)
            with self.assertRaisesRegex(ValueError,'incomplete'):
                select_shards(root/'downloaded',root/'selected',[0])
            with self.assertRaisesRegex(ValueError,'missing'):
                select_shards(root/'downloaded',root/'other',[1])

    def test_merger_keeps_validation_and_uses_hard_links(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)
            src=root/'source';src.write_bytes(b'verified profile')
            dst=root/'destination'
            original=shutil.copyfile
            calls=[]
            def merger(selected,out,**kwargs):
                calls.append(kwargs)
                shutil.copyfile(src,dst)
                return {'complete':True}
            module=types.ModuleType('scripts.build_filing_profiles');module.merge=merger
            with patch.dict('sys.modules',{'scripts.build_filing_profiles':module}):
                result=merge_without_duplicate_bytes(root,root,root,companies=2,filings=3)
            self.assertTrue(result['complete'])
            self.assertEqual(calls,[{'expected_companies':2,'expected_filings':3}])
            self.assertEqual(os.stat(src).st_ino,os.stat(dst).st_ino)
            self.assertIs(shutil.copyfile,original)


if __name__=='__main__':
    unittest.main()
