"""Day-board required-files gate for Deploy dashboard.

Pages publish is ready when the session day-board JSON has required
roles OK for stock_book + publish, and factor-mine today.json plus
strategy tickets exist for that date.

Does NOT wait on catalyst dossiers, sector LLM, or OpenClaw.
PARTIAL because catalyst FAIL still means "files present → publish."

Fallback: if Stock Book skipped / book_ok=False but tickets +
dashboard/factor-mine/today.json already exist for the date, still OK.

When --date is omitted, the session is the latest completed NYSE
session. Before 09:30 ET on a trading day, and on a weekend or
full-day holiday, that is the previous trading session
(src.skip_if_good calendar). An explicit --date is used as given.

CLI: python -m src.pages_publish_gate [--date YYYY-MM-DD]
Exit 0 = publish. Exit 1 = do not publish. Callers must treat exit 1
as a failed deploy. A skipped publish is not a green no-op.
"""
from __future__ import annotations

import argparse
import json
import re
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
ROOT = Path(__file__).resolve().parent.parent

# Cyrus's Pages-publish process list. Catalyst / preopen LLM are not here.
PAGES_PROCESS_KEYS = ("stock_book", "publish")
IGNORE_PROCESS_KEYS = frozenset({"catalyst"})
# Regular-session open. Before this bell the day has not started, so
# the latest completed session is still the previous NYSE session.
OPEN_H, OPEN_M = 9, 30
_ASOF_META = re.compile(
    r'<meta name="data-asof" content="(20\d{2}-\d{2}-\d{2})">'
)
_ASOF_JSON = re.compile(r'"data_asof"\s*:\s*"(20\d{2}-\d{2}-\d{2})"')
_DATES_ARRAY = re.compile(r'"dates"\s*:\s*\[(.*?)\]', re.S)
_DAY = re.compile(r"20\d{2}-\d{2}-\d{2}")


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


def latest_completed_session(now: datetime | None = None) -> str:
    """Latest NYSE session that has already opened, else the previous one.

    Before 09:30 ET on a session day the open has not printed, so the
    previous trading session is the one with a finished day board.
    Weekends and full-day holidays roll back the same way. At or after
    09:30 ET on a session day, that day is the session. The calendar is
    ``skip_if_good`` (weekends + NYSE full-day holidays).
    """
    from .skip_if_good import _prev_weekday, _session_date

    if now is None:
        now = datetime.now(ET)
    elif now.tzinfo is None:
        now = now.replace(tzinfo=ET)
    else:
        now = now.astimezone(ET)
    bell = now.replace(hour=OPEN_H, minute=OPEN_M, second=0, microsecond=0)
    if _session_date(now.date()) and now >= bell:
        return now.date().isoformat()
    return _prev_weekday(now.date().isoformat())


def data_asof_from_html(text: str) -> str:
    """Data date stamped on the main page, else the last embedded curve date."""
    match = _ASOF_META.search(text or "")
    if match:
        return match.group(1)
    match = _ASOF_JSON.search(text or "")
    if match:
        return match.group(1)
    match = _DATES_ARRAY.search(text or "")
    if not match:
        return ""
    days = _DAY.findall(match.group(1))
    return days[-1] if days else ""


def stamp_data_asof(path: Path, asof: str) -> None:
    """Stamp ``dashboard/index.html`` with the curve's data date.

    ``src/paper_trade.py`` is an engine pin, so the append renderer calls
    this immediately after ``write_dashboard`` instead of editing that file.
    The stamp is the newest curve session, not the wall clock.
    """
    day = str(asof or "")[:10]
    if not _DAY.fullmatch(day):
        raise ValueError(f"data-asof must be YYYY-MM-DD, got {asof!r}")
    text = path.read_text(encoding="utf-8")
    tag = f'<meta name="data-asof" content="{day}">'
    if 'name="data-asof"' in text:
        text = _ASOF_META.sub(tag, text, count=1)
    else:
        needle = '<meta charset="utf-8">'
        if needle in text:
            text = text.replace(needle, needle + "\n" + tag, 1)
        else:
            text = tag + "\n" + text
    token = '"generated":'
    if '"data_asof":' in text:
        text = _ASOF_JSON.sub(f'"data_asof":"{day}"', text, count=1)
    elif token in text:
        text = text.replace(token, f'"data_asof":"{day}",{token}', 1)
    path.write_text(text, encoding="utf-8")


def main_page_asof(root: Path | None = None) -> str:
    path = (root or ROOT) / "dashboard" / "index.html"
    if not path.is_file():
        return ""
    # The stamp and the curve dates sit in the head / payload preamble.
    # Later trade rows are not the page's data-asof.
    with path.open(encoding="utf-8", errors="replace") as handle:
        return data_asof_from_html(handle.read(262144))


_MISSED_NOTE = re.compile(
    r'<p\s+id="missed-note"[^>]*>(.*?)</p>',
    re.I | re.S,
)


def missed_sessions(root: Path | None = None) -> dict:
    """Sessions explicitly recorded as missed. Other statuses are ignored."""
    path = (root or ROOT) / "data" / "paper" / "missed.json"
    data = _load_json(path)
    raw = data.get("sessions") if isinstance(data.get("sessions"), dict) else {}
    out = {}
    for day, row in raw.items():
        if not isinstance(row, dict):
            continue
        if str(row.get("status") or "") != "missed":
            continue
        stamp = str(day)[:10]
        if _DAY.fullmatch(stamp):
            out[stamp] = row
    return out


def page_shows_missed_note(day: str, *, root: Path | None = None,
                           text: str | None = None) -> bool:
    """True when the rendered main page shows a missed note for ``day``."""
    if text is None:
        path = (root or ROOT) / "dashboard" / "index.html"
        if not path.is_file():
            return False
        text = path.read_text(encoding="utf-8", errors="replace")
    session = str(day or "")[:10]
    if not _DAY.fullmatch(session):
        return False
    match = _MISSED_NOTE.search(text or "")
    if not match:
        return False
    note = match.group(1)
    return session in note and "missed" in note.lower()


def recorded_miss_covers(asof: str, need: str, *,
                         root: Path | None = None,
                         text: str | None = None) -> bool:
    """One session behind, and only when that session is a recorded miss.

    ``asof`` must be the previous NYSE session before ``need``. The miss
    has to be ``status=missed`` in ``data/paper/missed.json``, and the
    rendered page has to show that note. Any other gap stays stale.
    """
    from .skip_if_good import _prev_weekday

    have = str(asof or "")[:10]
    want = str(need or "")[:10]
    if not _DAY.fullmatch(have) or not _DAY.fullmatch(want):
        return False
    if have >= want:
        return False
    if _prev_weekday(want) != have:
        return False
    if want not in missed_sessions(root):
        return False
    return page_shows_missed_note(want, root=root, text=text)


def stamp_missed_note(path: Path, day: str) -> None:
    """Show ``day missed`` on the main page. Does not add a curve row."""
    session = str(day or "")[:10]
    if not _DAY.fullmatch(session):
        raise ValueError(f"missed note must be YYYY-MM-DD, got {day!r}")
    text = path.read_text(encoding="utf-8")
    note = (
        f'<p id="missed-note">{session} missed — stock book was not on '
        f"main before 09:30 ET, so this session is not on the paper curve.</p>"
    )
    if _MISSED_NOTE.search(text):
        text = _MISSED_NOTE.sub(note, text, count=1)
    elif "<body>" in text:
        text = text.replace("<body>", "<body>" + note, 1)
    else:
        text = note + "\n" + text
    path.write_text(text, encoding="utf-8")


def stamp_recorded_misses(path: Path, root: Path | None = None) -> None:
    """Put every recorded missed session back on a rewritten main page.

    The paper dashboard is rendered from the equity curve. A missed session
    is not a curve row, so a later paper land must restore the note from
    ``data/paper/missed.json`` or the one-session gate cannot see it.
    """
    days = sorted(missed_sessions(root))
    if not days or not path.is_file():
        return
    shown = " ".join(f"{day} missed" for day in days)
    note = (
        f'<p id="missed-note">{shown} — stock book was not on main before '
        f"09:30 ET, so these sessions are not on the paper curve.</p>"
    )
    text = path.read_text(encoding="utf-8")
    if _MISSED_NOTE.search(text):
        text = _MISSED_NOTE.sub(note, text, count=1)
    elif "<body>" in text:
        text = text.replace("<body>", "<body>" + note, 1)
    else:
        text = note + "\n" + text
    path.write_text(text, encoding="utf-8")


def main_page_fresh(date: str, *, root: Path | None = None,
                    now: datetime | None = None) -> bool:
    """False when the main page data-asof is older than ``date``.

    Deploy omits ``--date``, so ``date`` is ``latest_completed_session()``.
    A stamp older than that session fails the publish. An explicit older
    ``--date`` is fresh when the page already covers that session.

    The one exception is a page exactly one session behind, when that
    missing session is recorded ``status=missed`` and the rendered page
    shows the missed note. A wider gap, a missing record, a non-missed
    status, or a page that does not show the note still fails.
    """
    asof = main_page_asof(root)
    need = str(date or "").strip()[:10] or latest_completed_session(now)
    if not asof or not need:
        return False
    if asof >= need:
        return True
    return recorded_miss_covers(asof, need, root=root)


def resolve_date(date: str = "", *, root: Path | None = None,
                 now: datetime | None = None) -> str:
    """Explicit ``date`` wins. Empty uses ``latest_completed_session``.

    ``root`` stays in the signature for callers that pass a fixture.
    The empty-date choice does not read ``latest.json`` or calendar today.
    """
    picked = str(date or "").strip()
    if picked:
        return picked
    return latest_completed_session(now)


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


def preopen_today(now: datetime | None = None) -> str:
    """Today's ET date if it is a session day before 09:30 ET, else ""."""
    from .skip_if_good import _session_date

    if now is None:
        now = datetime.now(ET)
    elif now.tzinfo is None:
        now = now.replace(tzinfo=ET)
    else:
        now = now.astimezone(ET)
    bell = now.replace(hour=OPEN_H, minute=OPEN_M, second=0, microsecond=0)
    if _session_date(now.date()) and now < bell:
        return now.date().isoformat()
    return ""


def evaluate(date: str = "", *, root: Path | None = None,
             now: datetime | None = None) -> dict:
    """Return a verdict dict. ``ready`` is the deploy trigger.

    Empty ``date`` before 09:30 ET on a session day: if today's own day
    board, factor-mine today.json and tickets are already rolled to
    today and pass the gate, publish today (the morning pipeline has
    moved on, so the previous session's files no longer exist at the
    top level). Otherwise fall back to the previous session (#504).
    """
    root = Path(root) if root is not None else ROOT
    if not str(date or "").strip():
        today = preopen_today(now)
        if today and (_board_dir(root) / f"{today}.json").is_file():
            v = _evaluate_date(today, root=root)
            if v["ready"]:
                v["reason"] = "pre-open: today's session ready; " + v["reason"]
                return v
    date = resolve_date(date, root=root, now=now)
    return _evaluate_date(date, root=root)


def _evaluate_date(date: str, *, root: Path) -> dict:
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
    page_asof = main_page_asof(root)
    page_ok = main_page_fresh(date, root=root)
    fallback = (not required_ready) and fm_ok and tk_ok
    ready = ((required_ready and fm_ok and tk_ok) or fallback) and page_ok
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
    if not page_ok:
        reason.append(
            f"main page data-asof {page_asof or 'missing'} is older than {date} "
            "(latest completed session when --date is empty)"
        )
    elif page_asof and page_asof < date and recorded_miss_covers(
            page_asof, date, root=root):
        reason.append(
            f"main page is one session behind; {date} is recorded missed "
            "and the page shows that note"
        )
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
        "main_page_asof": page_asof,
        "main_page_fresh": page_ok,
        "reason": "; ".join(reason) or "not ready",
    }


def apply_newer_main_page(artifact: Path, main: Path) -> str:
    """Copy ``main`` over ``artifact`` when its data-asof is strictly newer.

    An equal stamp keeps the artifact. Returns ``main`` or ``artifact``.
    When the artifact path is ``.../dashboard/index.html``, the root
    ``index.html`` beside that dashboard directory is updated too.
    """
    def asof(path: Path) -> str:
        if not path.is_file():
            return ""
        return data_asof_from_html(
            path.read_text(encoding="utf-8", errors="replace")[:262144]
        )

    art_asof = asof(artifact)
    main_asof = asof(main)
    if not (main.is_file() and main_asof and (not art_asof or main_asof > art_asof)):
        return "artifact"
    blob = main.read_bytes()
    artifact.parent.mkdir(parents=True, exist_ok=True)
    artifact.write_bytes(blob)
    if artifact.parent.name == "dashboard":
        (artifact.parent.parent / "index.html").write_bytes(blob)
    return "main"


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
        f"main={v.get('main_page_asof') or 'STALE'} "
        f"overall={v.get('overall') or '?'} "
        f"catalyst={v.get('catalyst_status') or 'n/a'} (ignored) "
        f"— {v.get('reason')}"
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default="")
    ap.add_argument(
        "--apply-newer-main-page",
        nargs=2,
        metavar=("ARTIFACT", "MAIN"),
        help="Copy MAIN over ARTIFACT when MAIN data-asof is newer.",
    )
    args = ap.parse_args()
    if args.apply_newer_main_page:
        artifact, main_page = (Path(p) for p in args.apply_newer_main_page)
        choice = apply_newer_main_page(artifact, main_page)
        print(f"page={choice}", flush=True)
        return
    verdict = evaluate(args.date)
    line = format_verdict(verdict)
    print(line, flush=True)
    if verdict["ready"]:
        raise SystemExit(0)
    print(
        f"[pages-gate] FAIL: not publishing {verdict.get('date')}. "
        f"{verdict.get('reason')}",
        flush=True,
    )
    raise SystemExit(1)


if __name__ == "__main__":
    main()
