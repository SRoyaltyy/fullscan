"""Carry the separately published saved parser view across dashboard rebuilds."""
import argparse
import json
import re
import subprocess
from pathlib import Path


PREFIX = 'dashboard/scanner-backtest/completed/'


def preserve(site, ref='FETCH_HEAD'):
    site = Path(site)
    def git(*args):
        return subprocess.check_output(['git', '-C', str(site), *args])
    names = git('ls-tree', '-r', '--name-only', ref, '--', PREFIX).decode().splitlines()
    if PREFIX+'manifest.json' not in names:
        return False  # Initial full-stock publication has not happened yet.
    manifest = git('show', ref+':'+PREFIX+'manifest.json')
    data = json.loads(manifest)
    if (not data.get('complete') or data.get('schema') != 'completed-parser-review-1'
            or not re.fullmatch(r'https://raw\.githubusercontent\.com/SRoyaltyy/fullscan/[a-f0-9]{40}/', data.get('asset_base_url', ''))
            or data.get('published', {}).get('tickers') != len(data.get('profiles', {}))):
        raise ValueError('Published completed parser view is invalid; retain previous site')
    viewer = git('show', ref+':'+PREFIX+'index.html')
    for parent in ('dashboard/scanner-backtest', 'scanner-backtest'):
        page = site/parent/'index.html'
        if not page.is_file():
            continue
        target = page.parent/'completed'
        target.mkdir(parents=True, exist_ok=True)
        (target/'manifest.json').write_bytes(manifest)
        (target/'index.html').write_bytes(viewer)
        html = page.read_text()
        if 'id="completed-stock-results"' not in html:
            title = '<h1>Scanner backtest</h1>'
            if title not in html:
                raise ValueError('Cannot link saved parser view; retain previous site')
            p = data['published']
            link = ('<p id="completed-stock-results"><a href="completed/">Browse the completed '
                    f'{p["tickers"]:,}-stock extraction — {p["relationships"]:,} saved relationships '
                    'with source paragraphs</a></p>')
            page.write_text(html.replace(title, title+'\n'+link, 1))
    return True


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--site', type=Path, required=True)
    parser.add_argument('--ref', default='FETCH_HEAD')
    args = parser.parse_args()
    print('Preserved completed parser view' if preserve(args.site, args.ref) else 'No completed parser view published yet')
