"""Required-input contract and explicit publication trigger; no broker calls."""
from __future__ import annotations
import argparse
import hashlib
import json
import os
from pathlib import Path
import urllib.request
from datetime import datetime
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parent.parent
ET = ZoneInfo('America/New_York')


def evaluate(date):
    from . import stock_book_diag as diag
    spec = next(x for x in diag.workflow_specs(date, as_of=True) if x['key'] == 'stock_book')
    required = [x for x in spec['files'] if x['role'] == 'input' or x['key'] in ('join', 'peers')]
    missing, hashes = [], {}
    for item in required:
        check = diag._check_file(item, date)
        path = ROOT / item['rel']
        if check.status != 'OK':
            missing.append({'path': item['rel'], 'reason': check.reason or check.status})
        elif path.is_file():
            hashes[item['rel']] = hashlib.sha256(path.read_bytes()).hexdigest()
    panel = ROOT / 'data/factor_mine/panel.json'
    if not panel.is_file():
        missing.append({'path': 'data/factor_mine/panel.json', 'reason': 'missing historical panel'})
    else:
        hashes['data/factor_mine/panel.json'] = hashlib.sha256(panel.read_bytes()).hexdigest()
    fingerprint = hashlib.sha256(json.dumps(hashes, sort_keys=True).encode()).hexdigest()
    return {'date': date, 'ready': bool(required) and not missing,
            'fingerprint': fingerprint, 'inputs': hashes, 'blockers': missing}


def dispatch(date):
    """GITHUB_TOKEN pushes do not trigger push workflows; dispatch explicitly."""
    token = os.environ.get('GITHUB_TOKEN')
    if not token:
        raise RuntimeError('GITHUB_TOKEN missing; cannot notify decision publisher')
    repo = os.environ.get('GITHUB_REPOSITORY', 'SRoyaltyy/fullscan')
    body = json.dumps({'ref': 'main', 'inputs': {'run_date': date}}).encode()
    req = urllib.request.Request(
        f'https://api.github.com/repos/{repo}/actions/workflows/publish_strategy_tickets.yml/dispatches',
        data=body, headers={'Authorization': f'Bearer {token}', 'Accept': 'application/vnd.github+json'},
        method='POST')
    with urllib.request.urlopen(req, timeout=15) as response:
        return response.status == 204


def notify_changed(paths):
    # Inspect changed paths, not this writer's incomplete local checkout.
    # The receiving workflow evaluates the combined main tree.
    date = datetime.now(ET).date().isoformat()
    prefixes = ('01_daily/general/', '01_daily/sectors/', '01_daily/news/',
                '01_daily/weather/', '01_daily/map_heat/', 'data/join/',
                'data/peers/', 'data/ab_checklist/')
    relevant = any((date in p and p.startswith(prefixes)) or p == 'data/factor_mine/panel.json'
                   for p in paths)
    return dispatch(date) if relevant else False


def publish(date):
    from . import stock_book, publish_live_boards
    started = datetime.now(ET)
    before = evaluate(date)
    if not before['ready']:
        print(json.dumps(before), flush=True)
        return 3
    # Run the ranker now; do not wait for optional research/LLM job completion.
    df, meta = stock_book.build(date, as_of=True)
    stock_book.write_report(df, meta, top_n=int(meta.get('top_n') or 25))
    out = publish_live_boards.publish(date, write=True, extras=False)
    path = ROOT / 'data/day_board' / f'{date}_strategy_tickets.json'
    payload = json.loads(path.read_text()) if path.exists() else {}
    proof = payload.get('decision_readiness') or {}
    hot = (payload.get('strategies') or {}).get('union_hot_n4_h1') or {}
    after = evaluate(date)
    completed = datetime.fromisoformat(proof.get('completed_at') or '1970-01-01T00:00:00+00:00')
    fresh = (completed.tzinfo is not None and completed >= started and
             proof.get('fingerprint') == before['fingerprint'] == after['fingerprint'])
    if (out.get('error') or out.get('strategy_error') or not proof.get('ready') or
            not after['ready'] or not fresh or hot.get('status') not in ('ok', 'sit')):
        raise RuntimeError('decision publication incomplete; refusing a success status')
    from .book_suggestions import refresh_factor_live_poller
    refresh_factor_live_poller()
    return 0


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', default=datetime.now(ET).date().isoformat())
    parser.add_argument('--publish', action='store_true')
    parser.add_argument('--notify', nargs='*')
    args = parser.parse_args()
    if args.notify is not None:
        notify_changed(args.notify)
        return 0
    if args.publish:
        return publish(args.date)
    result = evaluate(args.date)
    print(json.dumps(result, indent=2))
    return 0 if result['ready'] else 3


if __name__ == '__main__':
    raise SystemExit(main())
