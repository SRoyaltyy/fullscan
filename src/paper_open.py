"""Prepare sandbox orders before 09:30; submit only within a bounded bell window.

No feature building, dependency installation or Pages deployment on the send path.
Acknowledgment is recorded separately from submission; neither is a fill promise.
"""
from __future__ import annotations
import argparse
from datetime import datetime, timedelta
import json
import math
import os
from pathlib import Path
import time
import urllib.request
from zoneinfo import ZoneInfo

from . import webull_exec as we
from .open_0930_clock import is_session_day
ET = ZoneInfo('America/New_York')
ROOT = Path(__file__).resolve().parent.parent


def now():
    return datetime.now(ET)


def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + '.tmp')
    with tmp.open('w') as f:
        json.dump(value, f, indent=2, allow_nan=False)
        f.flush()
        os.fsync(f.fileno())
    tmp.replace(path)
    fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def validate_payload(payload, date, clock):
    target = clock.replace(hour=9, minute=30, second=0, microsecond=0)
    if payload.get('date') != date or date != clock.date().isoformat():
        raise ValueError('wrong-session decision')
    proof = payload.get('decision_readiness') or {}
    if proof.get('ready') is not True or not proof.get('fingerprint'):
        raise ValueError('required inputs are not validated')
    completed = datetime.fromisoformat(proof.get('completed_at', ''))
    if completed.tzinfo is None or completed > min(clock, target):
        raise ValueError('decision completed after decision clock')
    rec = (payload.get('strategies') or {}).get(we.HOT4) or {}
    if rec.get('date') != date or rec.get('status') not in ('ok', 'sit'):
        raise ValueError('hot4 missing, stale or incomplete')
    score = float(rec.get('s'))
    if not math.isfinite(score):
        raise ValueError('unknown market regime')
    if payload.get('look', {}).get('stale'):
        raise ValueError('stale factor look')
    return rec


def load_published(date, timeout=3):
    # Read committed full payload, not the first arbitrary cached dashboard copy.
    repo = os.environ.get('GITHUB_REPOSITORY', 'SRoyaltyy/fullscan')
    url = (f'https://raw.githubusercontent.com/{repo}/main/data/day_board/'
           f'{date}_strategy_tickets.json?open_clock={time.time_ns()}')
    req = urllib.request.Request(url, headers={'Cache-Control': 'no-cache'})
    with urllib.request.urlopen(req, timeout=timeout) as response:
        return json.load(response)


def make_plan(payload, snap, clock):
    date = clock.date().isoformat()
    rec = validate_payload(payload, date, clock)
    if not snap.connected:
        raise ValueError(snap.error or 'broker disconnected')
    card = we.plan_hot4_for_broker(date, snap, payload=payload)
    if card.get('stale') or card.get('look_error'):
        raise ValueError('stale or failed hot4 plan')
    bad = [x for x in card.get('skipped', []) if x.get('kind') in ('cash', 'no_price')]
    if bad:
        raise ValueError('cannot fund/price planned entries: ' + ', '.join(x['ticker'] for x in bad))
    return {'date': date, 'prepared_at': clock.isoformat(), 'fingerprint':
            payload['decision_readiness']['fingerprint'], 'card': card,
            'cash': snap.cash, 'n_positions': len(snap.positions)}


def release(plan, api, clock, journal, *, submit, max_late=2):
    """Durable before-send intent: an ambiguous send is never blindly retried."""
    current = clock()
    target = current.replace(hour=9, minute=30, second=0, microsecond=0)
    lag = (current-target).total_seconds()
    if plan['date'] != current.date().isoformat() or not is_session_day(current) or not 0 <= lag <= max_late:
        raise ValueError('outside 09:30 submission window; refusing late entry')
    prepared = datetime.fromisoformat(plan['prepared_at'])
    if not 0 <= (current-prepared).total_seconds() <= 90:
        raise ValueError('preflight snapshot expired')
    journal = Path(journal)
    if journal.exists():
        raise ValueError('session already attempted; reconcile broker before any retry')
    result = {**plan, 'target_at': target.isoformat(), 'submit': submit,
              'status': 'releasing' if submit else 'dry_run', 'sent': [],
              'fill_status': 'not_observed', 'host': we.PAPER_HOST}
    if api.host != we.PAPER_HOST:
        raise ValueError('paper-open refuses any non-sandbox host')
    # Exclusive creation plus a host lock in the caller protects local restarts.
    journal.parent.mkdir(parents=True, exist_ok=True)
    with journal.open('x') as f:
        json.dump(result, f)
    tickets = plan['card'].get('tickets', [])
    sent_at = clock()
    for ticket in tickets:
        result['sent'].append({'ticker': ticket['ticker'], 'side': ticket['side'], 'shares': ticket['shares'],
            'client_order_id': we.client_order_id(plan['date'], ticket['side'], ticket['ticker']),
            'intent_at': sent_at.isoformat(),
            'status': 'intent' if submit else 'dry_run'})
    atomic_json(journal, result)
    if submit and tickets:
        sent_at = clock()
        for row in result['sent']:
            row.update(submission_started_at=sent_at.isoformat(),
                       lateness_ms=(sent_at-target).total_seconds()*1000)
        if not 0 <= (sent_at-target).total_seconds() <= max_late:
            result['status'] = 'failed'
            for row in result['sent']:
                row.update(status='missed_deadline', ok=False)
        else:
            try:
                # One broker batch: later names do not wait behind earlier network RTTs.
                replies = api.place_batch(tickets)
                for row in result['sent']:
                    got = replies.get(row['client_order_id'], {})
                    row.update(got)
                    row['status'] = 'acknowledged' if got.get('ok') else 'rejected_or_unknown'
                if any(not row.get('ok') for row in result['sent']):
                    result['status'] = 'failed'
            except Exception:
                result['status'] = 'failed'
                for row in result['sent']:
                    row.update(status='unknown', ok=False, error='submission outcome unknown; reconcile broker')
    atomic_json(journal, result)
    if result['status'] == 'releasing':
        result['status'] = 'acknowledged' if result['sent'] else 'no_trade'
    atomic_json(journal, result)
    return result


def run(*, submit=False, clock=now, sleep=time.sleep, loader=load_published, api=None, state_dir=None):
    import fcntl
    current = clock()
    date = current.date().isoformat()
    target = current.replace(hour=9, minute=30, second=0, microsecond=0)
    if not is_session_day(current) or current.hour < 8:
        return 0
    state = Path(state_dir or os.environ.get('PAPER_OPEN_STATE', ROOT / 'data/paper_open'))
    state.mkdir(parents=True, exist_ok=True)
    with (state / 'owner.lock').open('a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError('another paper-open process owns this host')
        status_path = state / f'{date}_status.json'
        journal = state / f'{date}_{"submit" if submit else "dry_run"}.json'
        if journal.exists():
            # The second DST fallback schedule/restart must preserve the first
            # attempt's evidence rather than replace it with a late-start error.
            prior = json.loads(journal.read_text())
            print('[paper-open] session already attempted; no resend', flush=True)
            return 0 if prior.get('status') in ('acknowledged', 'no_trade', 'dry_run') else 2
        if current >= target:
            atomic_json(status_path, {'date': date, 'status': 'missed_deadline', 'observed_at': current.isoformat()})
            return 2
        api = api or we.PaperAPI('paper')
        if not api.connect():
            atomic_json(status_path, {'date': date, 'status': 'broker_unavailable', 'error': api.err})
            return 2
        plan = None
        error = 'waiting for inputs'
        # Complete all network preparation at least five seconds before the bell.
        while clock() < target - timedelta(seconds=5):
            try:
                payload = loader(date)
                snap = api.snapshot()
                plan = make_plan(payload, snap, clock())
                atomic_json(status_path, {**plan, 'status': 'armed'})
                error = ''
            except Exception as exc:
                plan = None  # never keep a previously valid plan after a failed refresh
                error = str(exc)
                atomic_json(status_path, {'date': date, 'status': 'blocked', 'error': error})
            remaining = (target-clock()).total_seconds()
            if remaining > 5:
                sleep(min(15, max(.05, remaining-5)))
        if plan is None:
            atomic_json(status_path, {'date': date, 'status': 'not_ready_at_open', 'error': error})
            return 2
        while clock() < target:
            sleep(min(.1, max(0, (target-clock()).total_seconds())))
        try:
            result = release(plan, api, clock, journal, submit=submit)
        except Exception as exc:
            atomic_json(status_path, {'date': date, 'status': 'blocked', 'error': str(exc)})
            return 2
        atomic_json(status_path, result)
        we.write_last(result)
        return 2 if result['status'] == 'failed' else 0


def owner_enabled(owner):
    repo = os.environ.get('GITHUB_REPOSITORY', 'SRoyaltyy/fullscan')
    req = urllib.request.Request(
        f'https://raw.githubusercontent.com/{repo}/main/00_grounding/paper_open_owner.json?t={time.time_ns()}',
        headers={'Cache-Control': 'no-cache'})
    with urllib.request.urlopen(req, timeout=10) as response:
        config = json.load(response)
    return config.get('owner') == owner


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--submit', action='store_true')
    p.add_argument('--owner', choices=('actions', 'ecs'), default='actions')
    args = p.parse_args()
    if not owner_enabled(args.owner):
        print(f'[paper-open] {args.owner} is not the configured automatic owner; skip')
        return 0
    return run(submit=args.submit)


if __name__ == '__main__':
    raise SystemExit(main())
