"""Prepare sandbox orders; submit the paper batch when decisions are ready.

The seal path may place standing MARKET/CORE/DAY orders before 09:30 ET.
Webull paper keeps those SUBMITTED (filled_qty=0) until RTH, then fills at
the open — proven by STANDTEST-20260918-1789726292. At 09:30:00 or later
a standing submit places nothing and records ``missed_deadline``. The bell
path is the ECS backstop only: it may release inside the 0–2s window when
it armed before the bell. A start at or after 09:30 records
``missed_deadline`` and places nothing. A ticket republish or a stock-book
rebuild does not submit.

The day's ``<date>_status.json`` is append-only once it holds
``missed_deadline`` or a submitted record. A later run adds an ``events``
entry and leaves that first record intact.

The send list is that session's sealed tickets only. Sandbox positions
are not compared to the sealed book to fail the day or to add a
catch-up order. A sealed sell the account does not hold is
``drift-skipped`` and is not sent. Drift is appended to
``data/paper_open/drift_log.jsonl``.

Before any submit, the sandbox open and filled book is queried. Orders
already there (derived client_order_id, or the same symbol and side) are
not sent again. A failed query places nothing.
The id is ``h1-{date}-{ticker}-{buy|sell}`` and does not depend on a
saved file. Acknowledgment is not a fill promise.

Serial BUY legs are clamped to sandbox cash still free after earlier
acks in the same batch. Hot4 plans are sized with a slip haircut so a
pre-open snapshot that has not moved yet still leaves the last leg
fundable when the open prints above the plan px.

No feature building, dependency installation or Pages deployment on the
send path. Paper host only. The send list is the sealed h1 new-buy
set for that session (same book ``webull_exec`` uses): picks the open
fill would buy, not every name on the plan card. A Factor Mine HOT4
rebuild is not the order list. A carry-name buy, or paper cash that
cannot fund the sealed buy notionals, fails closed.

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

from . import paper_drift, webull_exec as we
from .open_0930_clock import is_session_day
ET = ZoneInfo('America/New_York')
ROOT = Path(__file__).resolve().parent.parent
STANDING_OPEN_HOUR = 4   # CORE session; STANDTEST accepted 06:20 ET
STANDING_CLOSE_HOUR = 16
OK_STATUSES = ('acknowledged', 'no_trade', 'dry_run', 'already_submitted')
# First record stays. Later runs append to ``events`` instead of replacing it.
# dry_run / armed / blocked stay replaceable so the morning loop can still
# move from armed to the real submit, and a dry-run cannot lock the day.
LOCKED_STATUSES = frozenset({
    'missed_deadline',
    'acknowledged',
    'already_submitted',
    'failed',
    'no_trade',
    'releasing',
})
SEAL_SENDER = 'seal'
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


def read_status(path):
    path = Path(path)
    if not path.is_file():
        return None
    try:
        doc = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return None
    return doc if isinstance(doc, dict) else None


def status_locked(doc) -> bool:
    """True when this file is the day's missed or submitted record."""
    if not isinstance(doc, dict):
        return False
    if doc.get('status') in LOCKED_STATUSES:
        return True
    if doc.get('submit') is True and isinstance(doc.get('sent'), list) and doc.get('sent'):
        return True
    return False


def _status_event(record, source):
    event = {
        'at': (record.get('observed_at') or record.get('prepared_at')
               or record.get('at') or datetime.now(ET).isoformat()),
        'status': record.get('status'),
        'submit': bool(record.get('submit')),
        'standing': bool(record.get('standing')),
        'source': source,
    }
    note = record.get('note') or record.get('error')
    if note:
        event['note'] = str(note)[:400]
    return event


def write_status(path, record, *, source='paper_open'):
    """Create the first record, or append an event once it is locked.

    A missed_deadline or submitted record is never replaced. The new
    attempt is an entry on ``events``. Armed, blocked, and dry-run
    notes are still replaced by the first real outcome.
    """
    path = Path(path)
    existing = read_status(path)
    if status_locked(existing):
        merged = dict(existing)
        events = [row for row in (merged.get('events') or []) if isinstance(row, dict)]
        events.append(_status_event(record, source))
        merged['events'] = events
        atomic_json(path, merged)
        return merged
    atomic_json(path, record)
    return record


def _flat_hot4_sit(rec) -> bool:
    """HOT4 is sitting and has no buy or sell leg to place.

    A no-same-day panel sit omits ``s`` (there is no regime score on the
    sleeve). That is a no-trade, not an unknown regime.
    """
    if not isinstance(rec, dict) or rec.get('status') != 'sit':
        return False
    return not (rec.get('buy') or []) and not (rec.get('sell') or [])


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
    # The published HOT4 row is a readiness signal only. Its buy and sell
    # names are not the order list — the sealed h1 plan is.
    if rec:
        if rec.get('date') != date or rec.get('status') not in ('ok', 'sit'):
            raise ValueError('hot4 missing, stale or incomplete')
        # status ok, and a sit that still has orders, keep requiring a finite s.
        if not _flat_hot4_sit(rec):
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


def _empty_sit_plan(payload, snap, clock):
    """No-trade plan. Does not size, and does not read the regime file."""
    date = clock.date().isoformat()
    positions = getattr(snap, 'positions', None) or {}
    return {
        'date': date,
        'prepared_at': clock.isoformat(),
        'fingerprint': (payload.get('decision_readiness') or {}).get('fingerprint'),
        'sit': True,
        'card': {
            'date': date,
            'want_date': date,
            'policy': we.HOT4,
            'tickets': [],
            'skipped': [],
            'stale': False,
            'look_error': '',
            'score': None,
            'hard_red': False,
            'why': f'{we.HOT4} flat sit; no buys or sells',
            'order_type': 'MARKET',
        },
        'cash': getattr(snap, 'cash', None),
        'n_positions': len(positions),
    }


def make_plan(payload, snap, clock, *, allow_after_bell=False):
    date = clock.date().isoformat()
    rec = validate_payload(payload, date, clock, allow_after_bell=allow_after_bell)
    del rec  # sealed plan, not the published HOT4 buy/sell lists
    if not snap.connected:
        raise ValueError(snap.error or 'broker disconnected')
    card = we.plan_hot4_for_broker(date, snap, payload=payload)
    if card.get('stale') or card.get('look_error'):
        raise ValueError(card.get('look_error') or 'stale or failed sealed h1 plan')
    bad = [x for x in card.get('skipped', []) if x.get('kind') in ('cash', 'no_price')]
    if bad:
        raise ValueError('cannot fund/price planned entries: ' + ', '.join(x['ticker'] for x in bad))
    return {'date': date, 'prepared_at': clock.isoformat(), 'fingerprint':
            payload['decision_readiness']['fingerprint'], 'card': card,
            'cash': snap.cash, 'n_positions': len(snap.positions)}


def _account_positions(api):
    """Sandbox lots, or None when the snapshot cannot be read.

    A failed read is not treated as an empty account, so a sealed sell
    is not drift-skipped just because the query failed. The caller does
    not fail the day on that miss.
    """
    snap_fn = getattr(api, 'snapshot', None)
    if not callable(snap_fn):
        return None
    try:
        snap = snap_fn()
    except Exception:
        return None
    if snap is None or not getattr(snap, 'connected', False):
        return None
    return getattr(snap, 'positions', None) or {}


def _load_session_orders(api, date):
    """Open and filled sandbox orders. Missing or failed query raises."""
    if getattr(api, 'host', None) != we.PAPER_HOST:
        raise RuntimeError('paper-open refuses any non-sandbox host')
    fn = getattr(api, 'list_session_orders', None)
    if not callable(fn):
        raise RuntimeError('sandbox order query is not available')
    rows = fn(date)
    if not isinstance(rows, list):
        raise RuntimeError('sandbox order query returned no order list')
    return rows


def _place_batch(result, api, tickets):
    try:
        # Serial single places (sandbox rejects multi-order combo_type).
        # place_batch may shrink a later BUY to cash still free, or skip it.
        # Rows already on the book stay already_submitted and are not in tickets.
        replies = api.place_batch(tickets)
        fresh = []
        for row in result['sent']:
            if row.get('status') in ('already_submitted', paper_drift.DRIFT_SKIPPED):
                continue
            fresh.append(row)
            got = replies.get(row['client_order_id'], {})
            row.update(got)
            if got.get('skipped'):
                row['status'] = 'skipped_cash'
                row['ok'] = True
            else:
                row['status'] = 'acknowledged' if got.get('ok') else 'rejected_or_unknown'
        if any(not row.get('ok') for row in fresh):
            result['status'] = 'failed'
        elif fresh and all(row.get('skipped') for row in fresh):
            result['status'] = 'no_trade'
        elif not fresh and result['sent']:
            result['status'] = 'already_submitted'
    except Exception:
        result['status'] = 'failed'
        for row in result['sent']:
            if row.get('status') == 'already_submitted':
                continue
            row.update(status='unknown', ok=False, error='submission outcome unknown; reconcile broker')


def release(plan, api, clock, journal, *, submit, max_late=2, standing=False,
            reconcile=True):
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
    # Standing orders rest for the open. At the bell they would fill the
    # live print. Do not create a submit journal for that refusal: the
    # 2026-10-06 morning miss had no journal, and a later standing run
    # must not look like a fresh attempt that replaces it.
    if standing and current >= target:
        if getattr(api, 'host', None) != we.PAPER_HOST:
            raise ValueError('paper-open refuses any non-sandbox host')
        return {**plan, 'target_at': target.isoformat(), 'submit': False,
                'standing': True, 'status': 'missed_deadline', 'sent': [],
                'fill_status': 'not_observed', 'host': we.PAPER_HOST,
                'observed_at': current.isoformat()}
    result = {**plan, 'target_at': target.isoformat(), 'submit': submit,
              'standing': standing, 'status': 'releasing' if submit else 'dry_run',
              'sent': [], 'fill_status': 'not_observed', 'host': we.PAPER_HOST}
    if api.host != we.PAPER_HOST:
        raise ValueError('paper-open refuses any non-sandbox host')
    tickets = [
        ticket for ticket in (plan['card'].get('tickets') or [])
        if isinstance(ticket, dict)
    ]
    # Flatten already sells the lots the account holds. Drift handling is
    # only for the sealed h1 list, and it must not add another snapshot
    # or drop those sells.
    if plan.get('mode') == FLATTEN_MODE:
        sendable, drift_skipped, foreign, drift_warning = tickets, [], [], None
    else:
        positions = _account_positions(api)
        sendable, drift_skipped, foreign, drift_warning = paper_drift.partition_tickets(
            tickets, positions, plan['date'], positions_known=positions is not None)
    if drift_warning is not None:
        drift_warning['at'] = current.isoformat()
        drift_warning['source'] = 'standing' if standing else 'bell'
        paper_drift.append_drift(drift_warning, Path(journal).parent / 'drift_log.jsonl')
    found = []
    missing = sendable
    # Broker book before any journal. A crash after the place, and before
    # this file existed, must not send those orders again. A failed query
    # places nothing and does not lock the session.
    if submit and sendable and reconcile:
        try:
            rows = _load_session_orders(api, plan['date'])
        except Exception as exc:
            result['status'] = 'query_failed'
            result['error'] = str(exc)[:400]
            result['found'] = []
            result['sent'] = []
            return result
        found, missing = we.match_sealed_orders(sendable, rows, plan['date'])
        result['found'] = found
    # Exclusive creation plus a host lock in the caller protects local restarts.
    journal.parent.mkdir(parents=True, exist_ok=True)
    with journal.open('x') as f:
        json.dump(result, f)
    found_ids = {row['client_order_id']: row for row in found}
    sent_at = clock()
    for ticket in sendable:
        coid = we.client_order_id(plan['date'], ticket['side'], ticket['ticker'])
        row = {'ticker': ticket['ticker'], 'side': ticket['side'], 'shares': ticket['shares'],
            'client_order_id': coid,
            'intent_at': sent_at.isoformat(),
            'status': 'intent' if submit else 'dry_run'}
        hit = found_ids.get(coid)
        if hit:
            row['status'] = 'already_submitted'
            row['ok'] = True
            row['match'] = hit.get('match')
            row['order_id'] = hit.get('order_id') or ''
        result['sent'].append(row)
    for row in drift_skipped + foreign:
        coid = we.client_order_id(
            plan['date'], row.get('side') or '', row.get('ticker') or '')
        result['sent'].append({
            **row,
            'client_order_id': coid,
            'intent_at': sent_at.isoformat(),
            'status': paper_drift.DRIFT_SKIPPED,
            'ok': False,
        })
    atomic_json(journal, result)
    if submit and missing:
        sent_at = clock()
        for row in result['sent']:
            if row.get('status') in ('already_submitted', paper_drift.DRIFT_SKIPPED):
                continue
            row.update(submission_started_at=sent_at.isoformat(),
                       lateness_ms=(sent_at-target).total_seconds()*1000)
        if standing:
            if sent_at >= target:
                result['status'] = 'missed_deadline'
                result['submit'] = False
                for row in result['sent']:
                    if row.get('status') in (
                            'already_submitted', paper_drift.DRIFT_SKIPPED):
                        continue
                    row.update(status='missed_deadline', ok=False)
            else:
                _place_batch(result, api, missing)
        elif not 0 <= (sent_at-target).total_seconds() <= max_late:
            result['status'] = 'failed'
            for row in result['sent']:
                if row.get('status') in (
                        'already_submitted', paper_drift.DRIFT_SKIPPED):
                    continue
                row.update(status='missed_deadline', ok=False)
        else:
            _place_batch(result, api, missing)
    elif submit and sendable:
        result['status'] = 'already_submitted'
    atomic_json(journal, result)
    if result['status'] == 'releasing':
        placed = [
            row for row in result['sent']
            if row.get('status') != paper_drift.DRIFT_SKIPPED
        ]
        result['status'] = 'acknowledged' if placed else 'no_trade'
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
    write_status(status_path, {
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
        write_status(status_path, {
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
        result = release(
            plan, api, clock, journal, submit=True, standing=True, reconcile=False)
    except Exception as exc:
        return _abort_flatten(status_path, date, str(exc), cancels=cancels)
    if any(str(row.get('side') or '').upper() == 'BUY' for row in result.get('sent') or []):
        print('[paper-open] FLATTEN ABORT — buy recorded after send; reconcile broker',
              flush=True)
        write_status(status_path, result)
        return 2
    cancel_ids = ', '.join(c['order_id'] for c in cancels) or 'none'
    sell_ids = ', '.join(
        f"{row.get('ticker')}:{row.get('order_id') or row.get('status')}"
        for row in result.get('sent') or []) or 'none'
    print(f'[paper-open] FLATTEN {date}: status={result["status"]} '
          f'cancelled=[{cancel_ids}] sells=[{sell_ids}]', flush=True)
    write_status(status_path, result)
    we.write_last(result)
    return 2 if result['status'] == 'failed' else 0


def _refuse_locked(status_path, existing, current, *, source, standing=False):
    """Append a no-send event. The first missed or submitted record stays."""
    late = we.at_or_after_open_deadline(current)
    write_status(status_path, {
        'date': existing.get('date') or current.date().isoformat(),
        'status': 'missed_deadline' if late else 'refused_existing_record',
        'observed_at': current.isoformat(),
        'submit': False,
        'standing': standing,
        'note': 'day record kept; no order sent',
    }, source=source)
    print('[paper-open] day record kept; no order sent', flush=True)
    return 0 if existing.get('status') in OK_STATUSES else 2


def _record_missed(status_path, date, current, *, source, standing=False):
    write_status(status_path, {
        'date': date,
        'status': 'missed_deadline',
        'observed_at': current.isoformat(),
        'submit': False,
        'standing': standing,
    }, source=source)
    print('[paper-open] at or after 09:30 ET; missed_deadline, no order sent',
          flush=True)
    return 2


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
            existing = read_status(status_path)
            if status_locked(existing):
                _refuse_locked(status_path, existing, current, source='run')
            return 0 if prior.get('status') in OK_STATUSES else 2
        existing = read_status(status_path)
        if status_locked(existing):
            return _refuse_locked(status_path, existing, current, source='run')
        # A start at or after the bell does not wait and does not sell a
        # flatten. The backstop is the path that armed before 09:30.
        if current >= target:
            return _record_missed(status_path, date, current, source='run')
        # Flagged ET date only. Any other date keeps the bell path below.
        flattened = _maybe_flatten(
            current, date=date, clock=clock, submit=submit, api=api,
            status_path=status_path, journal=journal)
        if flattened is not None:
            return flattened
        api = api or we.PaperAPI('paper')
        if not api.connect():
            write_status(status_path, {'date': date, 'status': 'broker_unavailable', 'error': api.err})
            return 2
        plan = None
        error = 'waiting for inputs'
        # Complete all network preparation at least five seconds before the bell.
        while clock() < target - timedelta(seconds=5):
            try:
                payload = loader(date)
                snap = api.snapshot()
                plan = make_plan(payload, snap, clock())
                write_status(status_path, {**plan, 'status': 'armed'})
                error = ''
            except Exception as exc:
                plan = None  # never keep a previously valid plan after a failed refresh
                error = str(exc)
                write_status(status_path, {'date': date, 'status': 'blocked', 'error': error})
            remaining = (target-clock()).total_seconds()
            if remaining > 5:
                sleep(min(15, max(.05, remaining-5)))
        if plan is None:
            write_status(status_path, {'date': date, 'status': 'not_ready_at_open', 'error': error})
            return 2
        while clock() < target:
            sleep(min(.1, max(0, (target-clock()).total_seconds())))
        prior = existing_attempt(journal, date, submit)
        if prior is not None:
            print('[paper-open] session already attempted; no resend', flush=True)
            write_status(status_path, prior)
            return 0 if prior.get('status') in OK_STATUSES else 2
        try:
            result = release(plan, api, clock, journal, submit=submit)
        except Exception as exc:
            write_status(status_path, {'date': date, 'status': 'blocked', 'error': str(exc)})
            return 2
        write_status(status_path, result)
        we.write_last(result)
        return 2 if result['status'] in ('failed', 'query_failed', 'missed_deadline') else 0


def submit_ready(*, submit=True, clock=now, loader=None, api=None, state_dir=None,
                 payload=None):
    """Seal path: standing paper before 09:30 ET. No bell wait.

    At or after 09:30 this records ``missed_deadline`` and places nothing.
    It does not replace a missed or submitted status already on disk.
    """
    import fcntl
    current = clock()
    date = current.date().isoformat()
    if not is_session_day(current):
        print('[paper-open] not a session day; skip ready submit', flush=True)
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
            existing = read_status(status_path)
            if status_locked(existing):
                _refuse_locked(status_path, existing, current,
                               source='submit_ready', standing=True)
            return 0 if prior.get('status') in OK_STATUSES else 2
        existing = read_status(status_path)
        if status_locked(existing):
            return _refuse_locked(status_path, existing, current,
                                  source='submit_ready', standing=True)
        # 2026-10-06 10:17 ET: a standing --ready after the open placed
        # the live print. That clock submits nothing.
        if we.at_or_after_open_deadline(current):
            return _record_missed(status_path, date, current,
                                  source='submit_ready', standing=True)
        if not STANDING_OPEN_HOUR <= current.hour < STANDING_CLOSE_HOUR:
            print('[paper-open] outside standing CORE/DAY window (04:00–16:00 ET); skip',
                  flush=True)
            return 0
        # Flagged ET date only. Any other date keeps the HOT4 standing path.
        flattened = _maybe_flatten(
            current, date=date, clock=clock, submit=submit, api=api,
            status_path=status_path, journal=journal)
        if flattened is not None:
            return flattened
        api = api or we.PaperAPI('paper')
        if not api.connect():
            write_status(status_path, {'date': date, 'status': 'broker_unavailable',
                                     'error': api.err, 'standing': True})
            return 2
        try:
            body = payload if payload is not None else (loader or load_local)(date)
            snap = api.snapshot()
            now = clock()
            # A Factor Mine flat sit does not replace the sealed h1 plan.
            plan = make_plan(body, snap, now, allow_after_bell=True)
            write_status(status_path, {**plan, 'status': 'armed', 'standing': True})
        except Exception as exc:
            write_status(status_path, {'date': date, 'status': 'blocked',
                                     'error': str(exc), 'standing': True})
            return 2
        try:
            result = release(plan, api, clock, journal, submit=submit, standing=True)
        except Exception as exc:
            write_status(status_path, {'date': date, 'status': 'blocked',
                                     'error': str(exc), 'standing': True})
            return 2
        write_status(status_path, result)
        we.write_last(result)
        return 2 if result['status'] in ('failed', 'query_failed', 'missed_deadline') else 0


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
                   help='seal path only: standing MARKET/CORE/DAY before 09:30 ET')
    p.add_argument('--owner', choices=('actions', 'ecs'), default='actions')
    args = p.parse_args(argv)
    if not owner_enabled(args.owner):
        print(f'[paper-open] {args.owner} is not the configured automatic owner; skip')
        return 0
    sender = os.environ.get('PAPER_OPEN_SENDER') or ''
    if args.ready:
        # Ticket republish and stock-book rebuild call the same flags.
        # Only the h1-seal workflow sets PAPER_OPEN_SENDER=seal.
        if sender != SEAL_SENDER:
            print('[paper-open] --ready refused; only the h1 seal sender '
                  'may place standing orders', flush=True)
            return 2
        return submit_ready(submit=args.submit)
    # The bell path places only for the ECS backstop, or for the seal
    # workflow once 09:30 has passed (that call records missed_deadline).
    if args.submit and sender not in (SEAL_SENDER, 'backstop'):
        print('[paper-open] --submit refused; only the h1 seal and the '
              'ECS backstop may place orders', flush=True)
        return 2
    return run(submit=args.submit)


if __name__ == '__main__':
    raise SystemExit(main())
