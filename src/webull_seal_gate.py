"""Webull paper send gate and ticket-readiness wait (stdlib only).

Why: 2026-10-09..10-16 no sandbox order went out. On every sealed h1 day
the seal-triggered send ran before the decision ticket was ready, failed
closed ("required inputs are not validated"), and nothing ran again
before 09:25 ET. On 10-13 the h1 forward run that pushed the seal ended
in failure, so the success-only workflow_run never fired at all.

This module decides whether a run may be the day's sender and, if so,
waits for the published ticket to turn ready. It never places orders.
The send itself stays ``python -m src.paper_open --ready --submit`` with
all its fail-closed checks (sealed-h1 hash, ticket/book match, journal,
client_order_id, 09:30 clock).

Go only when ALL hold:
  * NYSE session day, ET clock in [07:00, 09:25)
  * a sealed h1 ``plan`` line for today is in the h1 log on main
  * no submit journal for today (local checkout and raw main)
  * today's paper_open status is not a locked record
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ET = ZoneInfo('America/New_York')
ROOT = Path(__file__).resolve().parent.parent
H1_LOG = Path('research/hot_n4_clean_v4/forward_h1/h1_log.jsonl')
WINDOW_OPEN = 700
SEND_CUTOFF = 925  # last minute a standing send may start is 09:24
# Must equal paper_open.LOCKED_STATUSES (test asserts it).
LOCKED_STATUSES = frozenset({
    'missed_deadline', 'acknowledged', 'already_submitted',
    'failed', 'no_trade', 'releasing',
})


def _hm(now: datetime) -> int:
    return now.hour * 100 + now.minute


def is_session_day(now: datetime) -> bool:
    if now.weekday() >= 5:
        return False
    try:
        from src.skip_if_good import is_nyse_holiday
        return not is_nyse_holiday(now.date())
    except Exception:  # pragma: no cover - holiday table unavailable
        return True


def sealed_plan_today(date: str, root: Path = ROOT) -> bool:
    path = root / H1_LOG
    if not path.is_file():
        return False
    for line in path.read_text().splitlines():
        try:
            row = json.loads(line)
        except ValueError:
            continue
        if row.get('kind') == 'plan' and row.get('date') == date:
            return True
    return False


def _raw(path: str, timeout: float = 5):
    repo = os.environ.get('GITHUB_REPOSITORY', 'SRoyaltyy/fullscan')
    url = f'https://raw.githubusercontent.com/{repo}/main/{path}?t={time.time_ns()}'
    req = urllib.request.Request(url, headers={'Cache-Control': 'no-cache'})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.load(resp)
    except (urllib.error.URLError, TimeoutError, ValueError, OSError):
        return None


def status_locked(doc) -> bool:
    if not isinstance(doc, dict):
        return False
    if doc.get('status') in LOCKED_STATUSES:
        return True
    return bool(doc.get('submit') is True and isinstance(doc.get('sent'), list) and doc.get('sent'))


def already_attempted(date: str, root: Path = ROOT, remote=_raw) -> str | None:
    journal = root / 'data/paper_open' / f'{date}_submit.json'
    if journal.exists():
        return 'submit journal exists in checkout'
    status = root / 'data/paper_open' / f'{date}_status.json'
    if status.exists():
        try:
            if status_locked(json.loads(status.read_text())):
                return 'status already locked in checkout'
        except ValueError:
            pass
    if remote is not None:
        if remote(f'data/paper_open/{date}_submit.json') is not None:
            return 'submit journal exists on main'
        if status_locked(remote(f'data/paper_open/{date}_status.json')):
            return 'status already locked on main'
    return None


def gate(now: datetime, root: Path = ROOT, remote=_raw) -> tuple[bool, str]:
    date = now.date().isoformat()
    if not is_session_day(now):
        return False, 'not a session day'
    hm = _hm(now)
    if hm < WINDOW_OPEN:
        return False, 'before 07:00 ET'
    if hm >= SEND_CUTOFF:
        return False, 'at or after 09:25 ET; no standing send'
    if not sealed_plan_today(date, root):
        return False, f'no sealed h1 plan for {date} on main'
    prior = already_attempted(date, root, remote)
    if prior:
        return False, prior
    return True, f'sealed h1 for {date}, no attempt yet, before 09:25 ET'


def ticket_ready(doc, date: str) -> tuple[bool, str]:
    if not isinstance(doc, dict):
        return False, 'ticket not published'
    if doc.get('date') != date:
        return False, f"ticket is for {doc.get('date')}"
    proof = doc.get('decision_readiness') or {}
    if proof.get('ready') is not True or not proof.get('fingerprint'):
        return False, 'ticket not ready (required inputs are not validated)'
    return True, f"ticket ready, completed {proof.get('completed_at')}"


def wait_ready(*, clock=lambda: datetime.now(ET), sleep=time.sleep, root: Path = ROOT,
               remote=_raw, interval: float = 120.0, log=print) -> tuple[bool, str]:
    """Poll main until the ticket is ready. Stop at 09:25 ET or if someone sent."""
    while True:
        now = clock()
        ok, why = gate(now, root, remote)
        if not ok:
            return False, why
        date = now.date().isoformat()
        ready, rwhy = ticket_ready(remote(f'data/day_board/{date}_strategy_tickets.json'), date)
        log(f'[seal-gate] {now:%H:%M:%S} ET: {rwhy}', flush=True)
        if ready:
            return True, rwhy
        sleep(interval)


def _out(key: str, value: str) -> None:
    path = os.environ.get('GITHUB_OUTPUT')
    if path:
        with open(path, 'a') as fh:
            fh.write(f'{key}={value}\n')


def main(argv=None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument('cmd', choices=('check', 'wait'))
    p.add_argument('--interval', type=float, default=120.0)
    args = p.parse_args(argv)
    if args.cmd == 'check':
        ok, why = gate(datetime.now(ET))
    else:
        ok, why = wait_ready(interval=args.interval)
    print(f'[seal-gate] {args.cmd}: {"GO" if ok else "no-go"}: {why}', flush=True)
    _out('go', 'true' if ok else 'false')
    _out('why', why.replace('\n', ' '))
    return 0


if __name__ == '__main__':
    sys.exit(main())
