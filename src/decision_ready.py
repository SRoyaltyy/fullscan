"""Required-input contract and explicit publication trigger; no broker calls.

Publish writes morning tickets via ``strategy_tickets`` / ``publish_live_boards``.
Webull HOT4 uses the Factor Mine cash-start recipe (``pick_day`` on the
session panel) for buys and continuous-book list-drop sells, not the
oppset_union + Clock-B morning scan. This module does not size Webull
or flatten_robust lots.

A locked dated ticket file stays untouched. When the evening body is in
``<date>_strategy_tickets_draft.json``, that publication is complete and
``publish`` returns 0. A modified locked file, a failed draft write, or
a real strategy error still refuses a success status.
"""
from __future__ import annotations
import argparse
import hashlib
import json
import os
import subprocess
from pathlib import Path
import urllib.request
from datetime import datetime
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parent.parent
ET = ZoneInfo('America/New_York')


def _require_main_proof() -> bool:
    flag = (os.environ.get("FULLSCAN_REQUIRE_MAIN") or "").strip().lower()
    if flag in ("1", "true", "yes", "on"):
        return True
    return (os.environ.get("GITHUB_ACTIONS") or "").strip().lower() == "true"


def blob_sha256_on_main(rel: str) -> str | bool | None:
    """SHA-256 of ``origin/main:<rel>``.

    False — ref exists and the path does not.
    None — git cannot answer (no repo, no origin/main).
    """
    rel = str(rel or "").replace("\\", "/").lstrip("/")
    if not rel:
        return None
    try:
        ref = subprocess.run(
            ["git", "rev-parse", "--verify", "origin/main"],
            cwd=str(ROOT), capture_output=True, timeout=15, check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if ref.returncode != 0:
        return None
    try:
        show = subprocess.run(
            ["git", "show", f"origin/main:{rel}"],
            cwd=str(ROOT), capture_output=True, timeout=20, check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if show.returncode != 0:
        err = (show.stderr or b"").decode("utf-8", "replace").lower()
        if "does not exist" in err or "exists on disk" in err:
            return False
        return None
    return hashlib.sha256(show.stdout).hexdigest()


def apply_main_gate(proof: dict) -> dict:
    """ready=true only when every hashed input is that same blob on main.

    A local ``data/peers/<date>_peer_rs.csv`` that never landed must not
    stamp the morning tickets ready.
    """
    if not isinstance(proof, dict):
        return proof
    inputs = proof.get("inputs") or {}
    if not isinstance(inputs, dict) or not inputs:
        return proof
    blockers = list(proof.get("blockers") or [])
    extra = []
    for rel, digest in inputs.items():
        on_main = blob_sha256_on_main(str(rel))
        if on_main is None:
            if _require_main_proof():
                extra.append({"path": rel, "reason": "not_on_main"})
            continue
        if on_main is False or on_main != digest:
            extra.append({"path": rel, "reason": "not_on_main"})
    if not extra:
        return proof
    out = dict(proof)
    out["ready"] = False
    out["blockers"] = blockers + extra
    out["main_missing"] = [row["path"] for row in extra]
    return out


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
    before = apply_main_gate(evaluate(date))
    if not before['ready']:
        print(json.dumps(before), flush=True)
        return 3
    # Run the ranker now; do not wait for optional research/LLM job completion.
    df, meta = stock_book.build(date, as_of=True)
    stock_book.write_report(df, meta, top_n=int(meta.get('top_n') or 25))
    out = publish_live_boards.publish(date, write=True, extras=False)
    path = ROOT / 'data/day_board' / f'{date}_strategy_tickets.json'
    draft = ROOT / 'data/day_board' / f'{date}_strategy_tickets_draft.json'
    locked_draft = out.get('ticket_lock') == 'draft'
    if locked_draft:
        if not draft.is_file():
            raise RuntimeError(
                'dated tickets locked and the draft write failed; '
                'refusing a success status')
        draft_text = draft.read_text()
        if path.is_file() and path.read_text() == draft_text:
            raise RuntimeError(
                'locked dated ticket file was modified; '
                'refusing a success status')
        payload = json.loads(draft_text)
    else:
        payload = json.loads(path.read_text()) if path.exists() else {}
    proof = payload.get('decision_readiness') or {}
    hot = (payload.get('strategies') or {}).get('union_hot_n4_h1') or {}
    after = apply_main_gate(evaluate(date))
    completed = datetime.fromisoformat(proof.get('completed_at') or '1970-01-01T00:00:00+00:00')
    fresh = (completed.tzinfo is not None and completed >= started and
             proof.get('fingerprint') == before['fingerprint'] == after['fingerprint'])
    if (out.get('error') or out.get('strategy_error') or not proof.get('ready') or
            not after['ready'] or not fresh or hot.get('status') not in ('ok', 'sit')):
        raise RuntimeError('decision publication incomplete; refusing a success status')
    if locked_draft:
        print(
            '[decision] WARN: dated tickets locked; evening body is in '
            f'{draft.name}. Publication complete.',
            flush=True,
        )
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
