"""Code changes must alter provenance; new result/news files must not."""
import sys
import tempfile
import unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.lane_source_identity import source_identity

class SourceIdentity(unittest.TestCase):
    def test_only_public_producing_inputs_affect_identity(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            for name in ('src/lane_route.py', 'scripts/lane_public_articles.py', '.github/workflows/lane_public_articles.yml'):
                path = root / name; path.parent.mkdir(parents=True, exist_ok=True); path.write_text('synthetic producing code')
            first = source_identity(root)
            for name in ('data/news_intake/2026-10-11/documents.json', 'data/lane_public_analysis/results.json', 'private_company_profiles.json'):
                path = root / name; path.parent.mkdir(parents=True, exist_ok=True); path.write_text('Not a producing source file')
            self.assertEqual(first, source_identity(root))
            (root / 'src/lane_route.py').write_text('Changed program')
            self.assertNotEqual(first['sha256'], source_identity(root)['sha256'])
            (root / 'src/new_dependency.py').write_text('New dependency')
            self.assertIn('src/new_dependency.py', {x['path'] for x in source_identity(root)['files']})
            (root / '00_grounding').mkdir()
            (root / '00_grounding/news_impact_axioms.json').write_text('Changed axiom inputs')
            self.assertNotEqual(first['sha256'], source_identity(root)['sha256'])

    def test_incomplete_snapshot_is_not_a_verified_program(self):
        with tempfile.TemporaryDirectory() as folder:
            with self.assertRaises(ValueError): source_identity(folder)

if __name__ == '__main__': unittest.main()
