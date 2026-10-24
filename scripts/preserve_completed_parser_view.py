"""Carry the separately published saved parser view across dashboard rebuilds."""
import argparse
import json
import re
import shutil
import subprocess
import tarfile
from pathlib import Path


PREFIX = 'dashboard/scanner-backtest/completed/'


def preserve(site, ref='FETCH_HEAD'):
    site = Path(site)
    def git(*args):
        return subprocess.check_output(['git', '-C', str(site), *args])
    # Scanner snapshots have their own validated publisher. A routine dashboard
    # rebuild must retain that whole publication, including its HTML/provenance
    # and external asset URLs, rather than mixing it with an older checkout.
    scanner = 'dashboard/scanner-backtest'
    metadata = git('ls-tree', '--name-only', ref, '--', scanner+'/meta.json').decode().strip()
    restored = False
    if metadata:
        data = json.loads(git('show', ref+':'+metadata))
        snapshot = data.get('snapshot', {})
        if not (re.fullmatch(r'[a-f0-9]{40}', snapshot.get('source_revision', ''))
                and re.fullmatch(r'[a-f0-9]{64}', snapshot.get('snapshot_id', ''))):
            raise ValueError('Published scanner provenance is invalid; retain previous site')
        target = site/scanner
        # Keep a local scanner page that is still a real scanner backtest when
        # the published index was overwritten (e.g. by a stock-comparison page).
        # Restoring a non-scanner index made the completed-parser link step raise
        # ValueError and blocked every Deploy. Prefer published when it is valid.
        SCANNER_TITLE = '<h1>Scanner backtest</h1>'
        local_index = target/'index.html'
        keep_local_index = None
        if local_index.is_file():
            local_html = local_index.read_text(encoding='utf-8', errors='replace')
            if SCANNER_TITLE in local_html:
                keep_local_index = local_html
        if target.exists():
            shutil.rmtree(target)
        process = subprocess.Popen(['git', '-C', str(site), 'archive', ref, scanner], stdout=subprocess.PIPE)
        try:
            with tarfile.open(fileobj=process.stdout, mode='r|') as archive:
                for member in archive:
                    path = Path(member.name)
                    if path.is_absolute() or '..' in path.parts:
                        raise ValueError('Invalid scanner archive path')
                    if not path.is_relative_to(scanner):
                        if member.isdir() and Path(scanner).is_relative_to(path):
                            continue  # git archive includes ancestor directories.
                        raise ValueError('Invalid scanner archive path')
                    destination = site/path
                    if member.isdir():
                        destination.mkdir(parents=True, exist_ok=True)
                    elif member.isfile():
                        destination.parent.mkdir(parents=True, exist_ok=True)
                        with archive.extractfile(member) as source, destination.open('wb') as output:
                            shutil.copyfileobj(source, output)
                    else:
                        raise ValueError('Unsupported scanner archive entry')
            if process.wait() != 0:
                raise ValueError('Cannot preserve published scanner assets')
        finally:
            if process.poll() is None:
                process.kill(); process.wait()
        published_index = target/'index.html'
        published_html = ''
        if published_index.is_file():
            published_html = published_index.read_text(encoding='utf-8', errors='replace')
        if keep_local_index is not None and SCANNER_TITLE not in published_html:
            # Published index is not a scanner page — put the local one back so
            # the completed-parser link can still inject. meta/profiles/assets
            # still come from the published archive above.
            published_index.parent.mkdir(parents=True, exist_ok=True)
            published_index.write_text(keep_local_index, encoding='utf-8')
        alias = site/'scanner-backtest'
        if alias.exists():
            shutil.rmtree(alias)
        shutil.copytree(target, alias)
        restored = True
    names = git('ls-tree', '-r', '--name-only', ref, '--', PREFIX).decode().splitlines()
    if PREFIX+'manifest.json' not in names:
        return restored  # Initial full-stock publication has not happened yet.
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
