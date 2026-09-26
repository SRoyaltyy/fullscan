"""Prepare sandbox orders; submit the paper batch when decisions are ready.

Ready-publish may place standing MARKET/CORE/DAY orders before 09:30 ET.
Webull paper keeps those SUBMITTED (filled_qty=0) until RTH, then fills at
the open — proven by STANDTEST-20260918-1789726292. The 09:30 wait path is
a warm fallback only. Journal + stable client_order_id prevent a re-fire
from double-placing. Acknowledgment is not a fill promise.

Serial BUY legs are clamped to sandbox cash still free after earlier
acks in the same batch. Hot4 plans are sized with a slip haircut so a
pre-open snapshot that has not moved yet still leaves the last leg
fundable when the open prints above the plan px.

No feature building, dependency installation or Pages deployment on the
send path. Paper host only. Submit refuses when published HOT4 buys
or sells diverge from the Factor Mine cash-start recipe for that date.

00_grounding/paper_flatten.json names one ET date. On that date the
paper host cancels open orders and sells every lot (MARKET/CORE/DAY).
No buys, and no HOT4 plan. Any other date is unchanged.
"""
from __future__ import annotations
import argparse
from datetime import datetime, timedelta
import json
import math
import os
from pathlib import Path
import time
import urllib.error
import urllib.request
from zoneinfo import ZoneInfo

from . import webull_exec as we
from .open_0930_clock import is_session_day
ET = ZoneInfo('America/New_York')
ROOT = Path(__file__).resolve().parent.parent
STANDING_OPEN_HOUR = 4   # CORE session; STANDTEST accepted 06:20 ET
STANDING_CLOSE_HOUR = 16
OK_STATUSES = ('acknowledged', 'no_trade', 'dry_run')
FLATTEN_FLAG = ROOT / '00_grounding' / 'paper_flatten.json'
FLATTEN_MODE = 'flatten_account'


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


def validate_payload(payload, date, clock, *, allow_after_bell=False):
    target = clock.replace(hour=9, minute=30, second=0, microsecond=0)
    if payload.get('date') != date or date != clock.date().isoformat():
        raise ValueError('wrong-session decision')
    proof = payload.get('decision_readiness') or {}
    if proof.get('ready') is not True or not proof.get('fingerprint'):
        raise ValueError('required inputs are not validated')
    completed = datetime.fromisoformat(proof.get('completed_at', ''))
    limit = clock if allow_after_bell else min(clock, target)
    if completed.tzinfo is None or completed > limit:
        raise ValueError('decision completed after decision clock')
    rec = (payload.get('strategies') or {}).get(we.HOT4) or {}
    if rec.get('date') != date or rec.get('status') not in ('ok', 'sit'):
        raise ValueError('hot4 missing, stale or incomplete')
    score = float(rec.get('s'))
    if not math.isfinite(score):
        raise ValueError('unknown market regime')
    if payload.get('look', {}).get('stale'):
        raise ValueError('stale factor look')
    from . import strategy_tickets as st
    st.assert_hot4_wire(
        date, rec.get('buy') or [], sells=rec.get('sell') or [])
    return rec


def load_published(date, timeout=3):
    # Read committed full payload, not the first arbitrary cached dashboard copy.
    repo = os.environ.get('GITHUB_REPOSITORY', 'SRoyaltyy/fullscan')
    url = (f'https://raw.githubusercontent.com/{repo}/main/data/day_board/'
           f'{date}_strategy_tickets.json?open_clock={time.time_ns()}')
    req = urllib.request.Request(url, headers={'Cache-Control': 'no-cache'})
    with urllib.request.urlopen(req, timeout=timeout) as response:
        return json.load(response)


def load_local(date, root=None):
    """Workspace tickets written by the same ready-publish job."""
    root = Path(root or ROOT)
    dated = root / 'data' / 'day_board' / f'{date}_strategy_tickets.json'
    slim = root / 'data' / 'day_board' / 'today_strategies.json'
    path = dated if dated.is_file() else slim
    if not path.is_file():
        raise FileNotFoundError(f'no local strategy tickets for {date}')
    return json.loads(path.read_text())


def journal_name(date, submit):
    return f'{date}_{"submit" if submit else "dry_run"}.json'


def remote_session_journal(date, submit=True, timeout=3):
    """Committed journal on main — fallback runners do not share a disk."""
    repo = os.environ.get('GITHUB_REPOSITORY', 'SRoyaltyy/fullscan')
    url = (f'https://raw.githubusercontent.com/{repo}/main/data/paper_open/'
           f'{journal_name(date, submit)}?t={time.time_ns()}')
    req = urllib.request.Request(url, headers={'Cache-Control': 'no-cache'})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            return json.load(response)
    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, ValueError, json.JSONDecodeError):
        return None


def existing_attempt(journal, date, submit):
    journal = Path(journal)
    if journal.exists():
        return json.loads(journal.read_text())
    if os.environ.get('PAPER_OPEN_CHECK_REMOTE', '1') != '1':
        return None
    return remote_session_journal(date, submit)


def make_plan(payload, snap, clock, *, allow_after_bell=False):
    date = clock.date().isoformat()
    rec = validate_payload(payload, date, clock, allow_after_bell=allow_after_bell)
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


def _place_batch(result, api, tickets):
    try:
        # Serial single places (sandbox rejects multi-order combo_type).
        # place_batch may shrink a later BUY to cash still free, or skip it.
        replies = api.place_batch(tickets)
        for row in result['sent']:
            got = replies.get(row['client_order_id'], {})
            row.update(got)
            if got.get('skipped'):
                row['status'] = 'skipped_cash'
                row['ok'] = True
            else:
                row['status'] = 'acknowledged' if got.get('ok') else 'rejected_or_unknown'
        if any(not row.get('ok') for row in result['sent']):
            result['status'] = 'failed'
        elif result['sent'] and all(row.get('skipped') for row in result['sent']):
            result['status'] = 'no_trade'
    except Exception:
        result['status'] = 'failed'
        for row in result['sent']:
            row.update(status='unknown', ok=False, error='submission outcome unknown; reconcile broker')


def release(plan, api, clock, journal, *, submit, max_late=2, standing=False):
    """Durable before-send intent: an ambiguous send is never blindly retried."""
    current = clock()
    target = current.replace(hour=9, minute=30, second=0, microsecond=0)
    if plan['date'] != current.date().isoformat() or not is_session_day(current):
        raise ValueError('outside session; refusing')
    if standing:
        if not STANDING_OPEN_HOUR <= current.hour < STANDING_CLOSE_HOUR:
            raise ValueError('outside standing CORE/DAY window (04:00–16:00 ET)')
    else:
        lag = (current-target).total_seconds()
        if not 0 <= lag <= max_late:
            raise ValueError('outside 09:30 submission window; refusing late entry')
        prepared = datetime.fromisoformat(plan['prepared_at'])
        if not 0 <= (current-prepared).total_seconds() <= 90:
            raise ValueError('preflight snapshot expired')
    journal = Path(journal)
    if journal.exists():
        raise ValueError('session already attempted; reconcile broker before any retry')
    result = {**plan, 'target_at': target.isoformat(), 'submit': submit,
              'standing': standing, 'status': 'releasing' if submit else 'dry_run',
              'sent': [], 'fill_status': 'not_observed', 'host': we.PAPER_HOST}
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
        if standing:
            _place_batch(result, api, tickets)
        elif not 0 <= (sent_at-target).total_seconds() <= max_late:
            result['status'] = 'failed'
            for row in result['sent']:
                row.update(status='missed_deadline', ok=False)
        else:
            _place_batch(result, api, tickets)
    atomic_json(journal, result)
    if result['status'] == 'releasing':
        result['status'] = 'acknowledged' if result['sent'] else 'no_trade'
    atomic_json(journal, result)
    return result


def flatten_gate(clock_dt):
    """None when this ET date is not the flag date.

    ('flatten', doc) on the flagged session. ('block', reason) when the
    flag file is unreadable or the date matches a mode we will not trade.
    Callers must not fall through to HOT4 buys on block.
    """
    try:
        raw = FLATTEN_FLAG.read_text()
    except FileNotFoundError:
        return None
    try:
        doc = json.loads(raw)
    except Exception as exc:
        return ('block', f'flatten flag unreadable: {exc}')
    if not isinstance(doc, dict):
        return ('block', 'flatten flag is not an object')
    if clock_dt.tzinfo is None:
        clock_dt = clock_dt.replace(tzinfo=ET)
    else:
        clock_dt = clock_dt.astimezone(ET)
    if str(doc.get('date') or '') != clock_dt.date().isoformat():
        return None
    if str(doc.get('mode') or '') != FLATTEN_MODE:
        return ('block', 'flatten flag date matches but mode is not flatten_account')
    return ('flatten', doc)


def _whole_shares(raw):
    if isinstance(raw, dict):
        raw = raw.get('shares')
    try:
        shares = float(raw)
    except (TypeError, ValueError):
        return 0
    if not math.isfinite(shares) or shares < 1:
        return 0
    return int(shares)


def _flatten_sell_tickets(date, snap):
    """One whole-share SELL per lot. Lots under 1 share are skipped. No buys."""
    tickets = []
    positions = getattr(snap, 'positions', None) or {}
    for ticker in sorted(positions):
        shares = _whole_shares(positions[ticker])
        if shares < 1:
            continue
        name = str(ticker or '').upper().strip()
        if not name:
            continue
        tickets.append({
            'ticker': name,
            'side': 'SELL',
            'shares': shares,
            'date': date,
            'order_type': 'MARKET',
            'support_trading_session': 'CORE',
            'time_in_force': 'DAY',
        })
    return tickets


def _abort_flatten(status_path, date, error, **extra):
    print(f'[paper-open] FLATTEN ABORT — no orders sent: {error}', flush=True)
    atomic_json(status_path, {
        'date': date, 'status': 'blocked', 'mode': FLATTEN_MODE,
        'error': str(error), 'standing': True, **extra,
    })
    return 2


def _open_order_rows(api):
    rows = api.list_open_orders()
    if rows is None:
        raise RuntimeError('open-order list returned nothing')
    if not isinstance(rows, list):
        raise RuntimeError('open-order list returned an unexpected payload')
    return rows


def _flatten_session(*, date, clock, submit, api, status_path, journal):
    """Cancel every open paper order, then sell every lot. Never buy."""
    if not submit:
        print('[paper-open] FLATTEN due; dry-run sends nothing', flush=True)
        atomic_json(status_path, {
            'date': date, 'status': 'dry_run', 'mode': FLATTEN_MODE, 'submit': False,
        })
        return 0
    api = api or we.PaperAPI('paper')
    if getattr(api, 'host', None) != we.PAPER_HOST:
        return _abort_flatten(status_path, date, 'paper-open refuses any non-sandbox host')
    if not api.connect():
        err = getattr(api, 'err', None) or 'broker disconnected'
        return _abort_flatten(status_path, date, err)
    if getattr(api, 'host', None) != we.PAPER_HOST:
        return _abort_flatten(status_path, date, 'paper-open refuses any non-sandbox host')
    try:
        rows = _open_order_rows(api)
    except Exception as exc:
        return _abort_flatten(status_path, date, f'open-order list failed: {exc}')
    pending = []
    seen = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        oid = str(row.get('order_id') or row.get('orderId') or '').strip()
        if not oid:
            return _abort_flatten(
                status_path, date, 'open order missing order_id; no orders sent')
        if oid in seen:
            continue
        seen.add(oid)
        pending.append((oid, row))
    cancels = []
    for oid, row in pending:
        try:
            got = api.cancel_order(oid)
        except Exception as exc:
            cancels.append({'order_id': oid, 'ok': False, 'error': str(exc)[:400]})
            return _abort_flatten(
                status_path, date, f'cancel {oid} failed: {exc}', cancels=cancels)
        ok = isinstance(got, dict) and bool(got.get('ok'))
        rec = {
            'order_id': str((got or {}).get('order_id') or oid) if isinstance(got, dict) else oid,
            'client_order_id': str(row.get('client_order_id') or row.get('clientOrderId') or ''),
            'symbol': row.get('symbol') or row.get('ticker'),
            'ok': ok,
        }
        if isinstance(got, dict) and got.get('error'):
            rec['error'] = str(got.get('error'))[:400]
        cancels.append(rec)
        if not ok:
            return _abort_flatten(
                status_path, date,
                f'cancel {oid} failed: {rec.get("error") or "not acknowledged"}',
                cancels=cancels)
    try:
        snap = api.snapshot()
    except Exception as exc:
        return _abort_flatten(
            status_path, date, f'snapshot failed: {exc}', cancels=cancels)
    if snap is None or not getattr(snap, 'connected', False):
        err = getattr(snap, 'error', None) if snap is not None else 'no snapshot'
        return _abort_flatten(
            status_path, date, f'snapshot failed: {err or "broker snapshot failed"}',
            cancels=cancels)
    tickets = _flatten_sell_tickets(date, snap)
    for ticket in tickets:
        if str(ticket.get('side') or '').upper() != 'SELL' or int(ticket.get('shares') or 0) < 1:
            return _abort_flatten(
                status_path, date, 'flatten refused a non-sell ticket', cancels=cancels)
    current = clock()
    plan = {
        'date': date,
        'prepared_at': current.isoformat(),
        'mode': FLATTEN_MODE,
        'fingerprint': FLATTEN_MODE,
        'card': {
            'tickets': tickets,
            'skipped': [],
            'order_type': 'MARKET',
            'support_trading_session': 'CORE',
            'time_in_force': 'DAY',
        },
        'cash': getattr(snap, 'cash', None),
        'n_positions': len(getattr(snap, 'positions', None) or {}),
        'cancels': cancels,
    }
    try:
        result = release(plan, api, clock, journal, submit=True, standing=True)
    except Exception as exc:
        return _abort_flatten(status_path, date, str(exc), cancels=cancels)
    if any(str(row.get('side') or '').upper() == 'BUY' for row in result.get('sent') or []):
        print('[paper-open] FLATTEN ABORT — buy recorded after send; reconcile broker',
              flush=True)
        atomic_json(status_path, result)
        return 2
    cancel_ids = ', '.join(c['order_id'] for c in cancels) or 'none'
    sell_ids = ', '.join(
        f"{row.get('ticker')}:{row.get('order_id') or row.get('status')}"
        for row in result.get('sent') or []) or 'none'
    print(f'[paper-open] FLATTEN {date}: status={result["status"]} '
          f'cancelled=[{cancel_ids}] sells=[{sell_ids}]', flush=True)
    atomic_json(status_path, result)
    we.write_last(result)
    return 2 if result['status'] == 'failed' else 0


def _maybe_flatten(current, *, date, clock, submit, api, status_path, journal):
    """Run the one-day flatten, or return None to keep today's path."""
    gate = flatten_gate(current)
    if gate is None:
        return None
    kind, info = gate
    if kind != 'flatten':
        return _abort_flatten(status_path, date, info)
    return _flatten_session(
        date=date, clock=clock, submit=submit, api=api,
        status_path=status_path, journal=journal)


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
        journal = state / journal_name(date, submit)
        prior = existing_attempt(journal, date, submit)
        if prior is not None:
            # Ready-publish, the second DST fallback, or a restart must
            # preserve the first attempt rather than replace it.
            print('[paper-open] session already attempted; no resend', flush=True)
            return 0 if prior.get('status') in OK_STATUSES else 2
        # Flagged ET date only. Any other date keeps the bell path below.
        flattened = _maybe_flatten(
            current, date=date, clock=clock, submit=submit, api=api,
            status_path=status_path, journal=journal)
        if flattened is not None:
            return flattened
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
        prior = existing_attempt(journal, date, submit)
        if prior is not None:
            print('[paper-open] session already attempted; no resend', flush=True)
            atomic_json(status_path, prior)
            return 0 if prior.get('status') in OK_STATUSES else 2
        try:
            result = release(plan, api, clock, journal, submit=submit)
        except Exception as exc:
            atomic_json(status_path, {'date': date, 'status': 'blocked', 'error': str(exc)})
            return 2
        atomic_json(status_path, result)
        we.write_last(result)
        return 2 if result['status'] == 'failed' else 0


def submit_ready(*, submit=True, clock=now, loader=None, api=None, state_dir=None,
                 payload=None):
    """Same-workflow ready publish: place standing paper now. No bell wait."""
    import fcntl
    current = clock()
    date = current.date().isoformat()
    if not is_session_day(current):
        print('[paper-open] not a session day; skip ready submit', flush=True)
        return 0
    if not STANDING_OPEN_HOUR <= current.hour < STANDING_CLOSE_HOUR:
        print('[paper-open] outside standing CORE/DAY window (04:00–16:00 ET); skip',
              flush=True)
        return 0
    state = Path(state_dir or os.environ.get('PAPER_OPEN_STATE', ROOT / 'data/paper_open'))
    state.mkdir(parents=True, exist_ok=True)
    with (state / 'owner.lock').open('a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError('another paper-open process owns this host')
        status_path = state / f'{date}_status.json'
        journal = state / journal_name(date, submit)
        prior = existing_attempt(journal, date, submit)
        if prior is not None:
            print('[paper-open] session already attempted; no resend', flush=True)
            return 0 if prior.get('status') in OK_STATUSES else 2
        # Flagged ET date only. Any other date keeps the HOT4 standing path.
        flattened = _maybe_flatten(
            current, date=date, clock=clock, submit=submit, api=api,
            status_path=status_path, journal=journal)
        if flattened is not None:
            return flattened
        api = api or we.PaperAPI('paper')
        if not api.connect():
            atomic_json(status_path, {'date': date, 'status': 'broker_unavailable',
                                     'error': api.err, 'standing': True})
            return 2
        try:
            body = payload if payload is not None else (loader or load_local)(date)
            snap = api.snapshot()
            plan = make_plan(body, snap, clock(), allow_after_bell=True)
            atomic_json(status_path, {**plan, 'status': 'armed', 'standing': True})
        except Exception as exc:
            atomic_json(status_path, {'date': date, 'status': 'blocked',
                                     'error': str(exc), 'standing': True})
            return 2
        try:
            result = release(plan, api, clock, journal, submit=submit, standing=True)
        except Exception as exc:
            atomic_json(status_path, {'date': date, 'status': 'blocked',
                                     'error': str(exc), 'standing': True})
            return 2
        atomic_json(status_path, result)
        we.write_last(result)
        return 2 if result['status'] == 'failed' else 0


def load_owner_record():
    override = os.environ.get('PAPER_OPEN_OWNER_FILE')
    if override:
        return json.loads(Path(override).read_text())
    repo = os.environ.get('GITHUB_REPOSITORY', 'SRoyaltyy/fullscan')
    req = urllib.request.Request(
        f'https://raw.githubusercontent.com/{repo}/main/00_grounding/paper_open_owner.json?t={time.time_ns()}',
        headers={'Cache-Control': 'no-cache'})
    with urllib.request.urlopen(req, timeout=10) as response:
        return json.load(response)


def owner_enabled(owner, config=None):
    config = load_owner_record() if config is None else config
    return config.get('owner') == owner


def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument('--submit', action='store_true')
    p.add_argument('--ready', action='store_true',
                   help='submit immediately after ready publish; standing MARKET/CORE/DAY OK before 09:30')
    p.add_argument('--owner', choices=('actions', 'ecs'), default='actions')
    args = p.parse_args(argv)
    if not owner_enabled(args.owner):
        print(f'[paper-open] {args.owner} is not the configured automatic owner; skip')
        return 0
    if args.ready:
        return submit_ready(submit=args.submit)
    return run(submit=args.submit)


if __name__ == '__main__':
    raise SystemExit(main())
