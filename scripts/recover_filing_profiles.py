"""Select one verified profile set per shard, retaining completed work."""
import argparse
import errno
import json
import os
import shutil
import sys
from pathlib import Path
from unittest.mock import patch


def select_shards(downloads, selected, expected):
    downloads, selected = Path(downloads), Path(selected)
    selected.mkdir(parents=True, exist_ok=True)
    for shard in expected:
        candidates = [downloads/f'deep-filing-profiles-recovered-{shard}', downloads/f'deep-filing-profiles-{shard}']
        source = next((p for p in candidates if (p/'index.json').is_file()), None)
        if source is None:
            raise ValueError('missing source shard: '+str(shard))
        index = json.loads((source/'index.json').read_text())
        if not index.get('complete'):
            raise ValueError('incomplete source shard: '+str(shard))
        source.rename(selected/str(shard))


def merge_without_duplicate_bytes(parser_root, selected, out, *, companies, filings):
    sys.path.insert(0, str(Path(parser_root).resolve()))
    from scripts.build_filing_profiles import merge
    original = shutil.copyfile
    def linked_copy(src, dst):
        try:
            os.link(src, dst)
            return dst
        except OSError as exc:
            if exc.errno != errno.EXDEV:
                raise
            return original(src, dst)
    # The original merger still checks every graph hash, paragraph coverage,
    # date, schema and issuer/filing count. Hard links avoid a second 10 GB copy.
    with patch.object(shutil, 'copyfile', linked_copy):
        return merge(selected, out, expected_companies=companies, expected_filings=filings)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--downloads', type=Path, required=True)
    p.add_argument('--selected', type=Path, required=True)
    p.add_argument('--out', type=Path, required=True)
    p.add_argument('--parser-root', type=Path, required=True)
    p.add_argument('--shards', required=True)
    p.add_argument('--companies', type=int, required=True)
    p.add_argument('--filings', type=int, required=True)
    a = p.parse_args()
    select_shards(a.downloads, a.selected, json.loads(a.shards))
    result = merge_without_duplicate_bytes(a.parser_root, a.selected, a.out, companies=a.companies, filings=a.filings)
    print(json.dumps({k:v for k,v in result.items() if k != 'profiles'}), flush=True)


if __name__ == '__main__':
    main()
