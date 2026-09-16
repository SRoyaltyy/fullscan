"""Day-board required-files gate for Deploy dashboard.

Pages publish is ready when the session day-board JSON has required
roles OK for stock_book + publish, and factor-mine today.json plus
strategy tickets exist for that date.

Does NOT wait on catalyst dossiers, sector LLM, or OpenClaw.
PARTIAL because catalyst FAIL still means "files present → publish."

Fallback: if Stock Book skipped / book_ok=False but tickets +
dashboard/factor-mine/today.json already exist for the date, still OK.

CLI: python -m src.pages_publish_gate [--date YYYY-MM-DD]
Exit 0 = fire Deploy dashboard. Exit 1 = not yet.
"""
from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
ROOT = Path(__file__).resolve().parent.parent

# Cyrus's Pages-publish process list. Catalyst / preopen LLM are not here.
PAGES_PROCESS_KEYS = ("stock_book", "publish")
IGNORE_PROCESS_KEYS = frozenset({"catalyst"})


def _today() -> str:
    return datetime.now(ET).date().isoformat()


def _load_json(path: Path) -> dict:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def _session_of(data: dict) -> str:
    return str(
        data.get("clock_legal_for")
        or data.get("session_open")
        or data.get("date")
        or ""
    )


def _board_dir(root: Path) -> Path:
    return root / "data" / "day_board"


def _fm_today(root: Path) -> Path:
    return root / "dashboard" / "factor-mine" / "today.json"


def resolve_date(date: str = "", *, root: Path | None = None) -> str:
    if date:
        return date
    latest = _load_json(_board_dir(root or ROOT) / "latest.json")
    return str(latest.get("date") or "") or _today()


def load_board(date: str, *, root: Path | None = None) -> dict:
    board_dir = _board_dir(root or ROOT)
    board = _load_json(board_dir / f"{date}.json")
    if board:
        return board
    return _load_json(board_dir / "latest.json")


def required_files(process: dict) -> list[dict]:
    """Required file rows that actually block Pages (skip pages_live)."""
    out = []
    for row in process.get("files") or []:
        if not isinstance(row, dict):
            continue
        if row.get("role") != "required":
            continue
        if row.get("status") == "SKIP":
            continue
        out.append(row)
    return out


def process_required_ok(process: dict) -> tuple[bool, int, int, list[str]]:
    rows = required_files(process)
    missing = [
        f"{row.get('key') or row.get('path')}:{row.get('status') or 'MISSING'}"
        for row in rows if row.get("status") != "OK"
    ]
    n_ok = sum(1 for row in rows if row.get("status") == "OK")
    return (not missing, n_ok, len(rows), missing)


def factor_mine_today_ok(date: str, *, root: Path | None = None) -> tuple[bool, str]:
    path = _fm_today(root or ROOT)
    if not path.is_file():
        return False, "dashboard/factor-mine/today.json missing"
    data = _load_json(path)
    got = _session_of(data)
    if got != date:
        return False, f"dashboard/factor-mine/today.json date={got!r} ≠ {date}"
    return True, f"dashboard/factor-mine/today.json date={date}"


def tickets_ok(date: str, *, root: Path | None = None) -> tuple[bool, str]:
    root = root or ROOT
    paths = [
        root / "data" / "day_board" / "today_strategies.json",
        root / "data" / "day_board" / f"{date}_strategy_tickets.json",
        root / "dashboard" / "factor-mine" / "strategy_tickets.json",
        root / "dashboard" / "factor-mine" / "today_strategies.json",
    ]
    for path in paths:
        if not path.is_file():
            continue
        data = _load_json(path)
        got = _session_of(data)
        if got == date:
            rel = path.relative_to(root).as_posix()
            return True, f"{rel} date={date}"
    return False, "strategy tickets missing or wrong session"


def evaluate(date: str = "", *, root: Path | None = None) -> dict:
    """Return a verdict dict. ``ready`` is the deploy trigger."""
    root = Path(root) if root is not None else ROOT
    date = resolve_date(date, root=root)
    board = load_board(date, root=root)
    processes = {
        str(p.get("key") or ""): p
        for p in (board.get("processes") or [])
        if isinstance(p, dict)
    }
    overall = str(board.get("overall") or "")
    cat = processes.get("catalyst") or {}
    process_bits: dict[str, dict] = {}
    required_ready = True
    for key in PAGES_PROCESS_KEYS:
        proc = processes.get(key) or {}
        ok, n_ok, n_req, missing = process_required_ok(proc)
        process_bits[key] = {
            "ok": ok, "n_ok": n_ok, "n_req": n_req, "missing": missing,
            "status": proc.get("status") or "MISSING",
        }
        if not ok:
            required_ready = False
    fm_ok, fm_note = factor_mine_today_ok(date, root=root)
    tk_ok, tk_note = tickets_ok(date, root=root)
    fallback = (not required_ready) and fm_ok and tk_ok
    ready = (required_ready and fm_ok and tk_ok) or fallback
    reason = []
    if required_ready and fm_ok and tk_ok:
        reason.append(
            "required stock_book+publish OK + factor-mine today.json + tickets"
        )
    elif fallback:
        reason.append(
            "fallback: tickets + factor-mine today.json present "
            "(stock_book/publish required not all OK)"
        )
    else:
        for key, bit in process_bits.items():
            if not bit["ok"]:
                reason.append(
                    f"{key} required {bit['n_ok']}/{bit['n_req']} "
                    f"missing={bit['missing']}"
                )
        if not fm_ok:
            reason.append(fm_note)
        if not tk_ok:
            reason.append(tk_note)
    return {
        "date": date,
        "ready": ready,
        "fallback": fallback,
        "overall": overall,
        "ranker_ready": board.get("ranker_ready"),
        "catalyst_status": cat.get("status") or "",
        "ignored": sorted(IGNORE_PROCESS_KEYS),
        "processes": process_bits,
        "factor_mine_today": fm_ok,
        "factor_mine_today_note": fm_note,
        "tickets": tk_ok,
        "tickets_note": tk_note,
        "reason": "; ".join(reason) or "not ready",
    }


def format_verdict(v: dict) -> str:
    bits = []
    for key in PAGES_PROCESS_KEYS:
        p = (v.get("processes") or {}).get(key) or {}
        bits.append(f"{key}={p.get('n_ok', 0)}/{p.get('n_req', 0)}")
    mode = "READY" if v.get("ready") else "BLOCKED"
    if v.get("fallback") and v.get("ready"):
        mode = "READY-FALLBACK"
    return (
        f"[pages-gate] {v.get('date')} {mode} "
        f"{' '.join(bits)} "
        f"factor-mine today.json={'OK' if v.get('factor_mine_today') else 'NO'} "
        f"tickets={'OK' if v.get('tickets') else 'NO'} "
        f"overall={v.get('overall') or '?'} "
        f"catalyst={v.get('catalyst_status') or 'n/a'} (ignored) "
        f"— {v.get('reason')}"
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default="")
    args = ap.parse_args()
    verdict = evaluate(args.date)
    print(format_verdict(verdict), flush=True)
    raise SystemExit(0 if verdict["ready"] else 1)


if __name__ == "__main__":
    main()
