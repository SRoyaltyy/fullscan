import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

from scripts.preserve_completed_parser_view import PREFIX, preserve


class PreserveCompletedView(unittest.TestCase):
    def test_routine_dashboard_rebuild_keeps_saved_view_and_both_navigation_links(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            def git(*args):
                return subprocess.check_output(['git', '-C', str(root), *args], stderr=subprocess.DEVNULL).decode().strip()
            git('init')
            target = root/PREFIX
            target.mkdir(parents=True)
            manifest = {'schema': 'completed-parser-review-1', 'complete': True,
                        'asset_base_url': 'https://raw.githubusercontent.com/SRoyaltyy/fullscan/'+'a'*40+'/',
                        'profiles': {'AAPL': {}}, 'published': {'tickers': 1, 'relationships': 42}}
            original = json.dumps(manifest).encode()
            (target/'manifest.json').write_bytes(original)
            (target/'index.html').write_text('original viewer')
            git('add', '.')
            git('-c', 'user.name=test', '-c', 'user.email=test@example.com', 'commit', '-m', 'prior published view')
            ref = git('rev-parse', 'HEAD')
            (target/'manifest.json').unlink()
            (target/'index.html').write_text('stale main viewer')
            for prefix in ('dashboard/scanner-backtest', 'scanner-backtest'):
                page = root/prefix/'index.html'
                page.parent.mkdir(parents=True, exist_ok=True)
                page.write_text('<h1>Scanner backtest</h1><p>new parser version</p>')
            self.assertTrue(preserve(root, ref))
            self.assertTrue(preserve(root, ref))
            for prefix in ('dashboard/scanner-backtest', 'scanner-backtest'):
                page = root/prefix/'index.html'
                self.assertEqual(page.read_text().count('id="completed-stock-results"'), 1)
                self.assertIn('new parser version', page.read_text())
                self.assertEqual((page.parent/'completed/manifest.json').read_bytes(), original)
                self.assertEqual((page.parent/'completed/index.html').read_text(), 'original viewer')

    def test_initial_publication_without_saved_view_is_unchanged(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            subprocess.run(['git', 'init', str(root)], check=True, capture_output=True)
            (root/'index.html').write_text('first site')
            subprocess.run(['git', '-C', str(root), 'add', '.'], check=True)
            subprocess.run(['git', '-C', str(root), '-c', 'user.name=test', '-c', 'user.email=test@example.com', 'commit', '-m', 'first'], check=True, capture_output=True)
            self.assertFalse(preserve(root, 'HEAD'))
            self.assertEqual((root/'index.html').read_text(), 'first site')

    def test_actual_dashboard_publisher_preserves_separately_published_data(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            remote, work = root/'remote.git', root/'work'
            subprocess.run(['git', 'init', '--bare', str(remote)], check=True, capture_output=True)
            work.mkdir()
            def git(*args):
                return subprocess.check_output(['git', '-C', str(work), *args], stderr=subprocess.DEVNULL).decode().strip()
            git('init')
            git('config', 'user.name', 'test'); git('config', 'user.email', 'test@example.com')
            target = work/PREFIX
            target.mkdir(parents=True)
            manifest = {'schema': 'completed-parser-review-1', 'complete': True,
                        'asset_base_url': 'https://raw.githubusercontent.com/SRoyaltyy/fullscan/'+'a'*40+'/',
                        'profiles': {'MSFT': {}}, 'published': {'tickers': 1, 'relationships': 42}}
            original = json.dumps(manifest).encode()
            (target/'manifest.json').write_bytes(original)
            (target/'index.html').write_text('published viewer')
            scanner = work/'dashboard/scanner-backtest'
            snapshot = json.dumps({'snapshot': {'source_revision': 'b'*40, 'snapshot_id': 'c'*64},
                'keyword_projection': {'source_revision': 'd'*40}, 'asset_base_url': 'immutable-assets/'})
            (scanner/'meta.json').write_text(snapshot)
            (scanner/'index.html').write_text('<h1>Scanner backtest</h1><p>published keyword model</p>')
            (scanner/'profiles.json').write_text('published profile hashes')
            git('add', '.'); git('commit', '-m', 'saved stock output')
            git('push', str(remote), 'HEAD:gh-pages')
            (target/'manifest.json').unlink()
            (target/'index.html').write_text('stale main viewer')
            (work/'dashboard/index.html').write_text('<h1>Dashboard</h1>')
            (work/'dashboard/scanner-backtest/index.html').write_text('<h1>Scanner backtest</h1><p>latest article model</p>')
            (scanner/'meta.json').write_text('stale metadata')
            (scanner/'profiles.json').unlink()
            (scanner/'stale-only.json').write_text('must not mix with the published snapshot')
            scripts = work/'scripts'; scripts.mkdir()
            source = Path(__file__).parent
            for name in ('publish_dashboard.sh', 'preserve_completed_parser_view.py'):
                (scripts/name).write_bytes((source/name).read_bytes())
            env = {**os.environ, 'GITHUB_WORKSPACE': str(work), 'GITHUB_TOKEN': 'local-test-only',
                   'GITHUB_REPOSITORY': 'test/fixture', 'PAGES_PUSH_ATTEMPTS': '1',
                   'GIT_CONFIG_COUNT': '1', 'GIT_CONFIG_KEY_0': f'url.{remote.as_uri()}.insteadOf',
                   'GIT_CONFIG_VALUE_0': 'https://x-access-token:local-test-only@github.com/test/fixture.git'}
            result = subprocess.run(['bash', str(scripts/'publish_dashboard.sh')], cwd=work, env=env, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stdout+result.stderr)
            self.assertIn('[pages] published', result.stdout)
            git('fetch', str(remote), 'gh-pages')
            self.assertEqual(git('show', 'FETCH_HEAD:'+PREFIX+'manifest.json').encode(), original)
            for prefix in ('dashboard/scanner-backtest', 'scanner-backtest'):
                page = git('show', 'FETCH_HEAD:'+prefix+'/index.html')
                self.assertIn('published keyword model', page)
                self.assertNotIn('latest article model', page)
                self.assertIn('id="completed-stock-results"', page)
                self.assertEqual(git('show', 'FETCH_HEAD:'+prefix+'/meta.json'), snapshot)
                self.assertEqual(git('show', 'FETCH_HEAD:'+prefix+'/profiles.json'), 'published profile hashes')
                self.assertNotIn(prefix+'/stale-only.json', git('ls-tree', '-r', '--name-only', 'FETCH_HEAD'))

    def test_keeps_local_scanner_when_published_index_is_not_a_scanner_page(self):
        """gh-pages once had a stock-comparison page at scanner-backtest/index.html.
        Restoring that HTML made the completed-parser link step raise ValueError and
        blocked Deploy. Prefer the local scanner page; keep published meta/assets.
        """
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            remote, work = root/'remote.git', root/'work'
            subprocess.run(['git', 'init', '--bare', str(remote)], check=True, capture_output=True)
            work.mkdir()
            def git(*args):
                return subprocess.check_output(['git', '-C', str(work), *args], stderr=subprocess.DEVNULL).decode().strip()
            git('init')
            git('config', 'user.name', 'test'); git('config', 'user.email', 'test@example.com')
            target = work/PREFIX
            target.mkdir(parents=True)
            manifest = {'schema': 'completed-parser-review-1', 'complete': True,
                        'asset_base_url': 'https://raw.githubusercontent.com/SRoyaltyy/fullscan/'+'a'*40+'/',
                        'profiles': {'MSFT': {}}, 'published': {'tickers': 1, 'relationships': 42}}
            original = json.dumps(manifest).encode()
            (target/'manifest.json').write_bytes(original)
            (target/'index.html').write_text('published viewer')
            scanner = work/'dashboard/scanner-backtest'
            scanner.mkdir(parents=True, exist_ok=True)
            snapshot = json.dumps({'snapshot': {'source_revision': 'b'*40, 'snapshot_id': 'c'*64},
                'keyword_projection': {'source_revision': 'd'*40}, 'asset_base_url': 'immutable-assets/'})
            (scanner/'meta.json').write_text(snapshot)
            # Corrupted published "scanner" index — comparison UI, not scanner backtest.
            (scanner/'index.html').write_text('<h1>Stock extraction · old vs new</h1><p>comparison only</p>')
            (scanner/'profiles.json').write_text('published profile hashes')
            git('add', '.'); git('commit', '-m', 'broken scanner index on gh-pages')
            git('push', str(remote), 'HEAD:gh-pages')
            (target/'manifest.json').unlink()
            (target/'index.html').write_text('stale main viewer')
            (work/'dashboard/index.html').write_text('<h1>Dashboard</h1>')
            local_scanner = (
                '<h1>Scanner backtest</h1>'
                '<p id="completed-stock-results">prelinked</p>'
                '<p>latest article model</p>'
            )
            (work/'dashboard/scanner-backtest/index.html').write_text(local_scanner)
            (scanner/'meta.json').write_text('stale metadata')
            (scanner/'profiles.json').unlink()
            scripts = work/'scripts'; scripts.mkdir()
            source = Path(__file__).parent
            for name in ('publish_dashboard.sh', 'preserve_completed_parser_view.py'):
                (scripts/name).write_bytes((source/name).read_bytes())
            env = {**os.environ, 'GITHUB_WORKSPACE': str(work), 'GITHUB_TOKEN': 'local-test-only',
                   'GITHUB_REPOSITORY': 'test/fixture', 'PAGES_PUSH_ATTEMPTS': '1',
                   'GIT_CONFIG_COUNT': '1', 'GIT_CONFIG_KEY_0': f'url.{remote.as_uri()}.insteadOf',
                   'GIT_CONFIG_VALUE_0': 'https://x-access-token:local-test-only@github.com/test/fixture.git'}
            result = subprocess.run(['bash', str(scripts/'publish_dashboard.sh')], cwd=work, env=env, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stdout+result.stderr)
            self.assertIn('[pages] published', result.stdout)
            git('fetch', str(remote), 'gh-pages')
            self.assertEqual(git('show', 'FETCH_HEAD:'+PREFIX+'manifest.json').encode(), original)
            for prefix in ('dashboard/scanner-backtest', 'scanner-backtest'):
                page = git('show', 'FETCH_HEAD:'+prefix+'/index.html')
                self.assertIn('Scanner backtest', page)
                self.assertIn('latest article model', page)
                self.assertNotIn('Stock extraction', page)
                self.assertIn('id="completed-stock-results"', page)
                self.assertEqual(git('show', 'FETCH_HEAD:'+prefix+'/meta.json'), snapshot)
                self.assertEqual(git('show', 'FETCH_HEAD:'+prefix+'/profiles.json'), 'published profile hashes')




if __name__ == '__main__':
    unittest.main()
