"""Identify public producing code without hashing changing news/results data."""
import hashlib
import json
from pathlib import Path

FIXED = ('scripts/lane_public_articles.py', '.github/workflows/lane_public_articles.yml',
         '00_grounding/news_impact_axioms.json')

def source_identity(root):
    root = Path(root)
    if not (root / 'src/lane_route.py').is_file() or not (root / FIXED[0]).is_file() or not (root / FIXED[1]).is_file():
        raise ValueError('Public producing code snapshot is incomplete')
    paths = {p.relative_to(root).as_posix() for p in (root / 'src').rglob('*.py')}
    paths.update(FIXED)
    files = [{'path': path, 'sha256': hashlib.sha256((root / path).read_bytes()).hexdigest()
              if (root / path).is_file() else None} for path in sorted(paths)]
    encoded = json.dumps(files, sort_keys=True, separators=(',', ':')).encode()
    return {'schema_version': 1, 'sha256': hashlib.sha256(encoded).hexdigest(), 'files': files}
