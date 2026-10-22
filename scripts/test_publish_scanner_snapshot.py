import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from scripts.publish_scanner_snapshot import validate, publish, publish_api, ARTIFACTS
from scripts.publish_scanner_snapshot import artifact_names


class SnapshotPublishTests(unittest.TestCase):
    def test_sparse_publisher_preserves_unrelated_missing_blobs_without_fetching_them(self):
        def git(cwd,*args):
            return subprocess.check_output(['git',*args],cwd=cwd,text=True,stderr=subprocess.DEVNULL).strip()
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);remote=root/'remote.git';work=root/'work';clone=root/'sparse'
            git(root,'init','--bare',str(remote));git(remote,'config','uploadpack.allowFilter','true')
            work.mkdir();git(work,'init','-b','main');git(work,'config','user.name','test');git(work,'config','user.email','test@example.test')
            (work/'unrelated.bin').write_text('unrelated large blob '*10000)
            git(work,'add','.');git(work,'commit','-m','upstream');source=git(work,'rev-parse','HEAD')
            git(work,'remote','add','origin',str(remote));git(work,'push','origin','main')
            git(root,'clone','--filter=blob:none','--no-checkout','--branch','main',remote.as_uri(),str(clone))
            before=git(clone,'rev-list','--objects','--missing=print','HEAD')
            self.assertTrue(any(line.startswith('?') for line in before.splitlines()),before)
            out=root/'snapshot';self.fixture(out,source)
            old=Path.cwd()
            try:
                os.chdir(clone)
                with patch.dict(os.environ,{'GIT_NO_LAZY_FETCH':'1'}):landed=publish(out,source)
            finally:os.chdir(old)
            after=git(clone,'rev-list','--objects','--missing=print',landed)
            self.assertEqual({line for line in before.splitlines() if line.startswith('?')},
                             {line for line in after.splitlines() if line.startswith('?')})
            upstream_blob=git(work,'rev-parse',source+':unrelated.bin')
            self.assertIn(upstream_blob,git(clone,'ls-tree',landed,'unrelated.bin'))
            self.assertEqual(git(remote,'rev-parse','main'),landed)

    def test_page_can_use_immutable_repository_assets_when_other_site_content_fills_pages(self):
        from scripts.publish_scanner_snapshot import stage_page
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);source=root/'snapshot';self.fixture(source)
            (source/'index.html').write_text('page')
            asset=source/'scoring-profiles/aa.json.gz';asset.parent.mkdir();asset.write_bytes(b'x'*1000)
            commit='a'*40
            total=sum((source/name).stat().st_size for name in (*ARTIFACTS,'index.html'))
            result=stage_page(source,root/'page',snapshot_commit=commit,outside_bytes=0,site_limit=total+10)
            self.assertTrue(result['external_assets'])
            self.assertFalse((root/'page/scoring-profiles').exists())
            meta=json.loads((root/'page/meta.json').read_text())
            self.assertIn('/'+commit+'/',meta['asset_base_url'])
            self.assertEqual((source/'scoring-profiles/aa.json.gz').stat().st_size,1000)
            result=stage_page(source,root/'local',snapshot_commit=commit,outside_bytes=0)
            self.assertFalse(result['external_assets'])
            self.assertTrue((root/'local/scoring-profiles/aa.json.gz').is_file())

    def test_budget_failure_prevents_publication_without_truncating(self):
        from scripts.publish_scanner_snapshot import validate_budget
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);self.fixture(root)
            before=(root/'meta.json').read_bytes()
            with self.assertRaisesRegex(ValueError,'budget'):
                validate_budget(root,total_limit=1)
            with self.assertRaisesRegex(ValueError,'budget'):
                validate_budget(root,file_limit=1)
            self.assertEqual(before,(root/'meta.json').read_bytes())
    def test_browser_preview_cannot_be_mislabeled_as_an_offline_comparison(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);self.fixture(root)
            meta=json.loads((root/'meta.json').read_text())
            meta['snapshot']['keyword_comparison_mode']='browser_preview'
            (root/'meta.json').write_text(json.dumps(meta))
            rows=[json.loads(line) for line in (root/'results.jsonl').read_text().splitlines()]
            for row in rows:row['keyword_comparison_mode']='browser_preview'
            (root/'results.jsonl').write_text('\n'.join(json.dumps(row) for row in rows))
            validate(root,'source')
            rows[0]['keyword_comparison']={'matches':[]}
            (root/'results.jsonl').write_text('\n'.join(json.dumps(row) for row in rows))
            with self.assertRaisesRegex(ValueError,'offline keyword comparison'):validate(root,'source')

    def test_api_publisher_updates_only_five_paths_without_force(self):
        calls=[]
        def api(method,path,payload=None):
            calls.append((method,path,payload))
            if '/git/blobs' in path: return {'sha':'blob'}
            if '/compare/' in path: return {'files':[{'filename':'unrelated.txt'}]}
            if method=='GET' and '/git/ref/' in path: return {'object':{'sha':'current'}}
            if method=='GET' and '/git/commits/' in path:return {'tree':{'sha':'tree'}}
            if '/git/trees' in path:return {'sha':'new-tree'}
            if method=='POST' and '/git/commits' in path:return {'sha':'new-commit'}
            if method=='PATCH':return {'object':{'sha':'new-commit'}}
            raise AssertionError((method,path))
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);self.fixture(root)
            with patch('scripts.publish_scanner_snapshot.github_api',side_effect=api):
                self.assertEqual(publish_api(root,'source','owner/repo'),'new-commit')
        tree=next(payload for method,path,payload in calls if method=='POST' and '/git/trees' in path)
        self.assertEqual({row['path'] for row in tree['tree']},{'dashboard/scanner-backtest/'+n for n in ARTIFACTS})
        self.assertEqual(calls[-1][2],{'sha':'new-commit','force':False})

    def test_api_publisher_refuses_changed_parser(self):
        def api(method,path,payload=None):
            if '/git/blobs' in path:return {'sha':'blob'}
            if '/git/ref/' in path:return {'object':{'sha':'current'}}
            if '/compare/' in path:return {'files':[{'filename':'src/evidence_matcher.py'}]}
            raise AssertionError('unexpected write after changed-source check')
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);self.fixture(root)
            with patch('scripts.publish_scanner_snapshot.github_api',side_effect=api):
                with self.assertRaisesRegex(ValueError,'inputs changed'):
                    publish_api(root,'source','owner/repo')

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

    def test_lazy_profile_assets_are_included_and_their_hashes_are_checked(self):
        import hashlib
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.fixture(root)
            asset = root/'scoring-profiles/example.json.gz'
            asset.parent.mkdir()
            asset.write_bytes(b'profile bytes')
            entry = {'used_for_scoring': True, 'profile_path': 'scoring-profiles/example.json.gz',
                     'profile_sha256': hashlib.sha256(asset.read_bytes()).hexdigest()}
            (root/'profiles.json').write_text(json.dumps({'OWN': entry}))
            self.assertIn(entry['profile_path'], artifact_names(root))
            validate(root, 'source')
            asset.write_bytes(b'wrong profile')
            with self.assertRaisesRegex(ValueError, 'checksum'):
                validate(root, 'source')

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


