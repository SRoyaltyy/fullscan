import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from scripts.publish_scanner_snapshot import validate, publish, ARTIFACTS


class SnapshotPublishTests(unittest.TestCase):
    def fixture(self, root, source='source'):
        root.mkdir(exist_ok=True)
        snapshot = {'snapshot_id': 'snapshot', 'source_revision': source, 'input_mode': 'headline',
                    'filing_coverage': {'scope': 'dated_identity_overlay', 'complete': True, 'errors': []}}
        payloads = {'meta.json': {'articles': 1, 'snapshot': snapshot},
            'results.jsonl': {'id': 'one', 'snapshot_id': 'snapshot'},
            'articles.json': [{'id': 'one'}], 'ticker-index.json': {},
            'profiles.json': {'OWN': {'used_for_scoring': True}}}
        for name, value in payloads.items():
            (root/name).write_text(json.dumps(value)+('\n' if name.endswith('jsonl') else ''))
        return snapshot

    def test_valid_complete_snapshot(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.fixture(root)
            self.assertEqual(validate(root, 'source')['articles'], 1)

    def test_different_commit_and_missing_filing_coverage_fail(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.fixture(root)
            with self.assertRaisesRegex(ValueError, 'scoring commit'):
                validate(root, 'wrong')
            meta=json.loads((root/'meta.json').read_text())
            meta['snapshot']['filing_coverage']['complete']=False
            (root/'meta.json').write_text(json.dumps(meta))
            with self.assertRaisesRegex(ValueError, 'filing identity'):
                validate(root, 'source')

    def test_mixed_results_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            self.fixture(root)
            (root/'results.jsonl').write_text(json.dumps({'id':'one','snapshot_id':'other'}))
            with self.assertRaisesRegex(ValueError, 'mixed'):
                validate(root,'source')

    def test_main_writer_preserves_unrelated_files_and_refuses_new_parser(self):
        def git(cwd,*args):
            return subprocess.check_output(['git',*args],cwd=cwd,text=True,stderr=subprocess.DEVNULL).strip()
        with tempfile.TemporaryDirectory() as directory:
            base=Path(directory)
            remote,work=base/'remote.git',base/'work'
            git(base,'init','--bare',str(remote)); work.mkdir()
            git(work,'init','-b','main');git(work,'config','user.name','test');git(work,'config','user.email','test@example.test')
            (work/'src').mkdir();(work/'src/evidence_matcher.py').write_text('original parser')
            (work/'unrelated.txt').write_text('keep me')
            git(work,'add','.');git(work,'commit','-m','base');source=git(work,'rev-parse','HEAD')
            git(work,'remote','add','origin',str(remote));git(work,'push','origin','main')
            out=base/'snapshot';self.fixture(out,source)
            old=Path.cwd()
            try:
                os.chdir(work)
                landed=publish(out,source)
                self.assertEqual(git(work,'show',landed+':unrelated.txt'),'keep me')
                changed=set(git(work,'diff','--name-only',source,landed).splitlines())
                self.assertEqual(changed,{'dashboard/scanner-backtest/'+name for name in ARTIFACTS})
                # Advance the parser on main. The old scoring checkout must
                # not overwrite that version's page with an older snapshot.
                git(work,'merge','--ff-only',landed)
                (work/'src/evidence_matcher.py').write_text('new parser')
                git(work,'add','.');git(work,'commit','-m','parser update');git(work,'push','origin','main')
                with self.assertRaisesRegex(ValueError, 'inputs changed'):
                    publish(out,source)
            finally:
                os.chdir(old)


if __name__=='__main__':
    unittest.main()
