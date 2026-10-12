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

# Decision-adjacent inputs the tickets read that the stock_book spec does
# not list. IRONCLAD C.10: live tickets and the record use the same input
# set, so a change in either file must change the fingerprint — otherwise
# the publish gate can call two different decisions "same inputs".
# Absence is hashed too: a file that appears later is an input change,
# not a no-op. Absence never blocks `ready`; it only fingerprints.
ABSENT = "absent"
EXTRA_INPUTS = (
    "excel_bot/suggestions/suggestions.csv",  # excel_strats()
    "data/sleeve_merge/today.json",           # flatten_strat()
)


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
    stamp the morning tickets ready. Absent-marked extra inputs are not
    main-gated: nothing can land a file that does not exist yet. Once the
    file exists its sha joins the fingerprint and the gate applies.
    """
    if not isinstance(proof, dict):
        return proof
    inputs = proof.get("inputs") or {}
    if not isinstance(inputs, dict) or not inputs:
        return proof
    blockers = list(proof.get("blockers") or [])
    extra = []
    for rel, digest in inputs.items():
        if digest == ABSENT:
            continue
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
        if getattr(check, 'role', item['role']) == 'optional':
            # _check_file drops an optional-era sector board that is not OK
            # to optional. It never blocks the tickets. Not hashed, the same
            # as before when it was missing.
            continue
        if check.status != 'OK':
            missing.append({'path': item['rel'], 'reason': check.reason or check.status})
        elif path.is_file():
            hashes[item['rel']] = hashlib.sha256(path.read_bytes()).hexdigest()
    panel = ROOT / 'data/factor_mine/panel.json'
    if not panel.is_file():
        missing.append({'path': 'data/factor_mine/panel.json', 'reason': 'missing historical panel'})
    else:
        hashes['data/factor_mine/panel.json'] = hashlib.sha256(panel.read_bytes()).hexdigest()
    for rel in EXTRA_INPUTS:
        path = ROOT / rel
        if path.is_file():
            hashes[rel] = hashlib.sha256(path.read_bytes()).hexdigest()
        else:
            hashes[rel] = ABSENT
    fingerprint = hashlib.sha256(json.dumps(hashes, sort_keys=True).encode()).hexdigest()
    return {'date': date, 'ready': bool(required) and not missing,
            'fingerprint': fingerprint, 'inputs': hashes, 'blockers': missing}


def landed_ticket_fingerprint(date: str) -> str | None:
    """Fingerprint already written on the dated ticket, if that file exists."""
    path = ROOT / "data" / "day_board" / f"{date}_strategy_tickets.json"
    if not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    proof = payload.get("decision_readiness") if isinstance(payload, dict) else None
    if not isinstance(proof, dict):
        return None
    fp = proof.get("fingerprint")
    return str(fp) if fp else None


def inputs_match_landed_ticket(date: str) -> bool:
    """True when today's ready input hash is already the landed ticket.

    A matching hash does not need another publish. A checker error returns
    False so the dispatch still happens and the workflow can decide.
    """
    landed = landed_ticket_fingerprint(date)
    if not landed:
        return False
    try:
        current = evaluate(date)
    except Exception as exc:
        print(f"[decision] fingerprint check failed ({exc}); dispatching", flush=True)
        return False
    fp = str(current.get("fingerprint") or "")
    if current.get("ready") and fp == landed:
        print(
            f"[decision] skip dispatch {date}: inputs match landed ticket {fp[:12]}",
            flush=True,
        )
        return True
    return False


def gate_decision(date: str) -> tuple[bool, str]:
    """Whether a publish run should start, plus the input fingerprint.

    Not-ready and already-landed hashes return False so the caller does
    not join the cancel-in-progress group. A checker error returns True
    so the publish job still runs its own readiness check.
    """
    try:
        proof = apply_main_gate(evaluate(date))
    except Exception as exc:
        print(f"[decision] gate failed ({exc}); publish job will re-check", flush=True)
        return True, ""
    fp = str(proof.get("fingerprint") or "")
    if not proof.get("ready"):
        print(f"[decision] not ready {date}; skip publish", flush=True)
        return False, fp
    landed = landed_ticket_fingerprint(date)
    if landed and landed == fp:
        print(
            f"[decision] skip publish {date}: inputs match landed ticket {landed[:12]}",
            flush=True,
        )
        return False, fp
    print(f"[decision] publish {date}: fingerprint {fp[:12]}", flush=True)
    return True, fp


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
    exact = ('data/factor_mine/panel.json',
             'excel_bot/suggestions/suggestions.csv',
             'data/sleeve_merge/today.json')
    relevant = any((date in p and p.startswith(prefixes)) or p in exact
                   for p in paths)
    if not relevant:
        return False
    # Churn fix (2026-10-07: 34 ticket-publish runs before 07:00 ET, most
    # cancelled). Inside a Pre-Open packet every sector/news/map land fired
    # its own dispatch; the packet's workflow_run trigger publishes once
    # when the stage completes, so stay quiet here.
    if (os.environ.get('PREOPEN_IN_PACKET') or '').strip() == '1':
        print('[decision] in Pre-Open packet — stage-end workflow_run publishes', flush=True)
        return False
    if inputs_match_landed_ticket(date):
        return False
    if publish_already_pending():
        print('[decision] publish already queued — it reads the newer tree', flush=True)
        return False
    return dispatch(date)


def publish_already_pending():
    """True when a publish_strategy_tickets run is already queued/waiting."""
    token = os.environ.get('GITHUB_TOKEN')
    if not token:
        return False
    repo = os.environ.get('GITHUB_REPOSITORY', 'SRoyaltyy/fullscan')
    for status in ('queued', 'waiting', 'pending'):
        req = urllib.request.Request(
            f'https://api.github.com/repos/{repo}/actions/workflows/'
            f'publish_strategy_tickets.yml/runs?status={status}&per_page=1',
            headers={'Authorization': f'Bearer {token}',
                     'Accept': 'application/vnd.github+json'})
        try:
            with urllib.request.urlopen(req, timeout=10) as r:
                if json.loads(r.read() or b'{}').get('total_count', 0):
                    return True
        except Exception:
            return False
    return False


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
    parser.add_argument(
        '--gate', action='store_true',
        help='Print publish=true/false. Exit 0 either way.',
    )
    args = parser.parse_args()
    if args.notify is not None:
        notify_changed(args.notify)
        return 0
    if args.gate:
        should_publish, fingerprint = gate_decision(args.date)
        print(f"publish={'true' if should_publish else 'false'}")
        print(f"fingerprint={fingerprint}")
        return 0
    if args.publish:
        return publish(args.date)
    result = evaluate(args.date)
    print(json.dumps(result, indent=2))
    return 0 if result['ready'] else 3


if __name__ == '__main__':
    raise SystemExit(main())
