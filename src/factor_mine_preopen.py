"""Factor Mine pre-open sleeves: HOT4 h1 and holdup on 09:30-knowable inputs.

``union_hot_n4_h1`` and ``union_hot_n4_holdup`` pick from
``data/factor_mine/panel.json``. That panel is built after the close
(about 17:00 ET), so their lists are an evening list and are not
knowable at 09:30. Since 2026-09-28 their send file is empty
(``no_same_day_panel``) and they sit in cash.

This wrapper runs two NEW sleeves with the parent's recipe (selection,
hold, s_boost, exits) on inputs committed before 09:30 ET only:

* ``union_hot_n4_h1_preopen``     parent ``union_hot_n4_h1``
* ``union_hot_n4_holdup_preopen`` parent ``union_hot_n4_holdup``

Two steps, both append-only:

1. ``seal`` (morning, before 09:30 ET on session D). Reads, at the last
   commit before 09:30 ET on D (``git show <sha>:<path>``, never the
   working tree), and nothing else:
     - ``data/day_board/<D>_strategy_tickets.json``; its parent buy list
       is the candidate list only when ``look.source == "look"``;
     - ``01_daily/weather/<D>_weather.json`` for the morning S gate
       (``signals.general_score``). S <= -3 means no buys. Missing S
       means no buys;
     - the prior session's carried state for the sleeve.
   The evening candidate file and the predict markdown are not inputs.
   Any input that has no commit before 09:30 ET on D is not used and
   the reason is logged in the seal. With no usable ticket list, with
   no weather S, or with a prior state that was not committed before
   the open, the sleeve sits (no buys; held lots still follow the exit
   rules). Writes ``data/factor_mine/preopen/seals/<D>.json`` once.
   The seal refuses at or after 09:30 ET.

2. ``book`` (after the close). For each closed session from START that
   has no state yet, in order, it reads only that day's seal. A seal
   that was not committed before 09:30 ET on D is treated as a sit. The
   day is stepped with the pinned ``factor_mine_book.simulate_book``
   from the prior state, written once to
   ``data/factor_mine/state/<sleeve>/<D>.json``, and its row is
   appended to ``03_scoreboard/factor_mine/<sleeve>.md`` under the
   Factor Mine past-day lock (``src/past_day_lock.py``). The dashboard
   file ``dashboard/factor-mine/preopen.json`` gains that day only.
   Rows on or before the last day already in the file stay as written.
   The book fetches 09:30/16:00 bars for the names that day actually
   holds or buys. It does not wait on the factor-mine remine. If the
   closed session is still missing when the step ends, the process
   exits non-zero.

Each sleeve starts with $10k at the 2026-10-08 open. There is no
backfill and no day before START. A missed day is not re-filled from a
later seal: the book step sits that day.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import subprocess
from datetime import datetime, time as dtime
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
ET = ZoneInfo("America/New_York")
START = "2026-10-08"
OPEN_ET = dtime(9, 30)
HARD_RED = -3.0
SLEEVES: dict[str, str] = {
    "union_hot_n4_h1_preopen": "union_hot_n4_h1",
    "union_hot_n4_holdup_preopen": "union_hot_n4_holdup",
}
SEAL_DIR = ROOT / "data" / "factor_mine" / "preopen" / "seals"
SUMMARY_PATH = ROOT / "data" / "factor_mine" / "preopen" / "summary.json"
DASH_JSON = ROOT / "dashboard" / "factor-mine" / "preopen.json"
STATE_DIR = ROOT / "data" / "factor_mine" / "state"
SCORE_DIR = ROOT / "03_scoreboard" / "factor_mine"
NEW_LABEL = "starts 10-08 open, no past days"
PARENT_LABEL = ("picked after the close (evening list) — "
                "not knowable at 09:30")
H1_NOT_IRONCLAD = ("Factor Mine recipe union_hot_n4_h1, not the separate "
                   "IRONCLAD h1 book (research/hot_n4_clean_v4/forward_h1)")


class PreopenRefused(SystemExit):
    """A step that would break the pre-open or append-only rule."""

    def __init__(self, message: str):
        super().__init__(message)


def tickets_rel(day: str) -> str:
    return f"data/day_board/{day}_strategy_tickets.json"


def weather_rel(day: str) -> str:
    return f"01_daily/weather/{day}_weather.json"


def state_rel(name: str, day: str) -> str:
    return f"data/factor_mine/state/{name}/{day}.json"


def seal_rel(day: str) -> str:
    return f"data/factor_mine/preopen/seals/{day}.json"


def open_at(day: str) -> datetime:
    y, m, d = (int(x) for x in day.split("-"))
    return datetime(y, m, d, OPEN_ET.hour, OPEN_ET.minute, tzinfo=ET)


def sha256_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


# ---------------------------------------------------------------- git clock

def _git(args: list[str], root: Path) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["git", *args], cwd=str(root), capture_output=True, check=False,
    )


class GitClock:
    """Committed bytes of a path as of the last commit before an instant."""

    def __init__(self, root: Path | None = None):
        self.root = Path(root or ROOT)

    def shallow(self) -> bool:
        out = _git(["rev-parse", "--is-shallow-repository"], self.root)
        return out.stdout.decode().strip() == "true"

    def before(self, rel: str, cutoff: datetime) -> dict:
        """``{ok, commit, committed_at, sha256, raw, reason}``.

        The newest commit on HEAD that touched ``rel`` with a committer
        time strictly before ``cutoff``. Bytes come from that commit.
        """
        out = {"path": rel, "ok": False, "commit": None,
               "committed_at": None, "sha256": None, "raw": None,
               "reason": ""}
        if self.shallow():
            out["reason"] = "shallow clone: commit times not provable"
            return out
        log = _git(["log", "--format=%H %cI", "HEAD", "--", rel], self.root)
        if log.returncode != 0:
            out["reason"] = "git log failed"
            return out
        lines = [ln for ln in log.stdout.decode().splitlines() if ln.strip()]
        if not lines:
            out["reason"] = "never committed"
            return out
        for line in lines:  # newest first
            sha, stamp = line.split(" ", 1)
            when = datetime.fromisoformat(stamp.strip())
            if when < cutoff:
                show = _git(["show", f"{sha}:{rel}"], self.root)
                if show.returncode != 0:
                    out["commit"] = sha
                    out["committed_at"] = when.astimezone(ET).isoformat()
                    out["reason"] = "deleted in its last pre-open commit"
                    return out
                raw = show.stdout
                out.update(ok=True, commit=sha, raw=raw,
                           committed_at=when.astimezone(ET).isoformat(),
                           sha256=sha256_bytes(raw))
                return out
        first = lines[-1].split(" ", 1)[1].strip()
        out["reason"] = (
            "first commit "
            f"{datetime.fromisoformat(first).astimezone(ET).isoformat()} "
            "is not before 09:30 ET"
        )
        return out


def _public(info: dict) -> dict:
    return {k: v for k, v in info.items() if k != "raw"}


def _json(info: dict):
    try:
        return json.loads(info["raw"].decode("utf-8"))
    except (TypeError, ValueError, AttributeError, UnicodeDecodeError):
        return None


# ---------------------------------------------------------------- recipes

def parent_recipe(parent: str) -> dict:
    from . import factor_mine as fm
    for rec in fm.build_recipes():
        if rec.get("name") == parent:
            return dict(rec)
    raise PreopenRefused(f"preopen: parent recipe {parent} not found")


def sleeve_recipe(name: str) -> dict:
    rec = parent_recipe(SLEEVES[name])
    rec["name"] = name
    rec["note"] = (f"{rec.get('note') or ''} | pre-open inputs only; "
                   f"{NEW_LABEL}").strip(" |")
    return rec


def sessions(start: str, end: str) -> list[str]:
    from .webull_sim import nyse_sessions_through
    return nyse_sessions_through(start, end)


def prior_session(day: str) -> str | None:
    from .skip_if_good import _session_date
    from datetime import date, timedelta
    cur = date.fromisoformat(day) - timedelta(days=1)
    while not _session_date(cur):
        cur -= timedelta(days=1)
    out = cur.isoformat()
    return out if out >= START else None


# ---------------------------------------------------------------- seal

def ticket_rows(doc: dict | None, parent: str) -> tuple[list[dict], str]:
    """Parent buy list from the pre-open ticket, in ticket order."""
    if not isinstance(doc, dict):
        return [], "tickets unreadable"
    look = doc.get("look") or {}
    src = str(look.get("source") or "")
    if src != "look":
        return [], f"tickets look.source={src or 'missing'} (need look)"
    rec = (doc.get("strategies") or {}).get(parent) or {}
    rows = []
    for item in rec.get("buy") or []:
        t = str((item or {}).get("ticker") or "").upper()
        if t:
            rows.append({"ticker": t, "src": item.get("src") or "tickets"})
    return rows, "" if rows else "tickets look list empty"


def weather_s(doc: dict | None):
    if not isinstance(doc, dict):
        return None
    v = (doc.get("signals") or {}).get("general_score")
    try:
        v = float(v)
    except (TypeError, ValueError):
        return None
    return v if math.isfinite(v) else None


def build_seal(day: str, *, clock: GitClock | None = None) -> dict:
    clock = clock or GitClock()
    cut = open_at(day)
    tick = clock.before(tickets_rel(day), cut)
    wx = clock.before(weather_rel(day), cut)
    inputs = [_public(tick), _public(wx)]
    s = weather_s(_json(wx)) if wx["ok"] else None
    s_source = "weather" if s is not None else ""
    log: list[str] = []
    if tick["ok"]:
        log.append(f"tickets commit {tick['commit']} at {tick['committed_at']} "
                   "is before 09:30 ET")
    else:
        log.append(f"tickets not used: {tick['reason']}")
    if wx["ok"]:
        log.append(f"weather commit {wx['commit']} at {wx['committed_at']} "
                   "is before 09:30 ET")
        if s is None:
            log.append("weather has no general_score")
    else:
        log.append(f"weather not used: {wx['reason']}")
    if s is None:
        log.append("no morning weather S committed before 09:30 ET")
    sleeves = {}
    for name, parent in SLEEVES.items():
        rec = sleeve_recipe(name)
        reasons: list[str] = []
        prev = prior_session(day)
        prior = None
        if prev:
            prior = clock.before(state_rel(name, prev), cut)
            inputs.append(_public(prior))
            if prior["ok"]:
                log.append(
                    f"{name} prior state {prev} commit {prior['commit']} at "
                    f"{prior['committed_at']} is before 09:30 ET")
            else:
                reasons.append(f"prior state {prev} not committed before "
                               f"09:30 ET: {prior['reason']}")
        rows: list[dict] = []
        if tick["ok"]:
            rows, why = ticket_rows(_json(tick), parent)
            if why:
                reasons.append(why)
        picks = [r["ticker"] for r in rows][: int(rec.get("top_n") or 4)]
        source = "tickets_look" if picks else "none"
        sit = False
        if s is None:
            sit = True
            reasons.append("no pre-open weather S: no buys")
        elif s <= HARD_RED:
            sit = True
            reasons.append(f"S={s:+.2f} <= -3: no buys")
        if prior is not None and not prior["ok"]:
            sit = True
        if not picks:
            sit = True
            reasons.append("no pre-open candidate list")
        sleeves[name] = {
            "parent": parent,
            "picks": [] if sit else picks,
            "pick_source": source,
            "would_pick": picks,
            "sit": sit,
            "reasons": reasons,
        }
    doc = {
        "date": day,
        "start": START,
        "inputs_rule": ("tickets look.source=look, weather general_score, "
                        "prior state; last commit before 09:30 ET only"),
        "open_cutoff_et": cut.isoformat(),
        "s": s,
        "s_source": s_source,
        "inputs": sorted(inputs, key=lambda r: r["path"]),
        "log": log,
        "sleeves": sleeves,
    }
    doc["sha256"] = sha256_bytes(_dumps({k: v for k, v in doc.items()
                                         if k != "sha256"}).encode())
    return doc


def _dumps(doc) -> str:
    return json.dumps(doc, sort_keys=True, indent=2, default=str) + "\n"


def write_once(path: Path, text: str) -> bool:
    """True when written. Same bytes are a no-op. Other bytes refuse."""
    if path.is_file():
        if path.read_text(encoding="utf-8") == text:
            return False
        raise PreopenRefused(f"preopen: {path} already written; refusing to rewrite")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return True


def seal(day: str, *, now: datetime | None = None,
         clock: GitClock | None = None, seal_dir: Path | None = None) -> Path | None:
    now = now or datetime.now(ET)
    if day < START:
        print(f"[preopen] {day} is before START {START}; no past days", flush=True)
        return None
    if now >= open_at(day):
        print(f"[preopen] {now.isoformat()} is at or after 09:30 ET on {day}; "
              "nothing sealed (a missed seal stays missed)", flush=True)
        return None
    from .skip_if_good import _session_date
    from datetime import date
    if not _session_date(date.fromisoformat(day)):
        print(f"[preopen] {day} is not an NYSE session", flush=True)
        return None
    path = Path(seal_dir or SEAL_DIR) / f"{day}.json"
    if path.is_file():
        print(f"[preopen] seal {day} already on disk; left as is", flush=True)
        return path
    doc = build_seal(day, clock=clock)
    write_once(path, _dumps(doc))
    for name, rec in doc["sleeves"].items():
        print(f"[preopen] seal {day} {name} picks={rec['picks']} "
              f"sit={rec['sit']} reasons={rec['reasons']}", flush=True)
    return path


# ---------------------------------------------------------------- book

def seal_for_book(day: str, *, clock: GitClock | None = None) -> tuple[dict | None, str]:
    """The seal as committed before 09:30 ET on ``day``, else a reason."""
    clock = clock or GitClock()
    info = clock.before(seal_rel(day), open_at(day))
    if not info["ok"]:
        return None, f"seal not committed before 09:30 ET: {info['reason']}"
    doc = _json(info)
    if not isinstance(doc, dict) or doc.get("date") != day:
        return None, "seal unreadable"
    return doc, ""


def synthetic_rows(day: str, picks: list[str]) -> list[dict]:
    """Seal order as rank: the first pick gets the highest hot score."""
    n = len(picks)
    return [{
        "ticker": t, "date": day, "sources": ["preopen_seal"],
        "ohlc_hot_score": float(n - i), "alarm": False, "e_pol": None,
    } for i, t in enumerate(picks)]


def missing_bars(day: str, tickers: list[str], bars) -> list[str]:
    from . import factor_mine as fm
    out = []
    for t in tickers:
        bar = fm._bar(t, day, bars)
        if fm._finite(bar.get("open")) is None or fm._finite(bar.get("close")) is None:
            out.append(t)
    return out


def step_day(name: str, day: str, seal_doc: dict | None, seal_why: str, *,
             prior: dict | None, bars=None, fees=None) -> dict:
    from . import factor_mine as fm
    from . import factor_mine_book as fmb
    from . import factor_mine_sequential as fms
    rec = sleeve_recipe(name)
    entry = ((seal_doc or {}).get("sleeves") or {}).get(name) or {}
    reasons = list(entry.get("reasons") or [])
    picks = [str(t) for t in (entry.get("picks") or [])]
    if seal_doc is None:
        picks, reasons = [], [seal_why]
    s = (seal_doc or {}).get("s")
    s_val = float("nan") if s is None else float(s)
    if s_val == s_val and s_val <= HARD_RED:
        picks = []
    held = sorted(((prior or {}).get("state") or {}).get("pos") or {})
    gap = missing_bars(day, sorted(set(picks) | set(held)), bars)
    if gap:
        raise PreopenRefused(
            f"preopen: {name} {day} no 09:30/16:00 bar for {gap}; not written, "
            "retry after the tape lands")
    cal = sessions(START, day)
    panel = fms.one_day_panel(day, synthetic_rows(day, picks), cal)
    regime = {day: {"predict_score": s_val}}
    book = fmb.simulate_book(
        panel, rec, bars=bars, fees=fees if fees is not None else fm.pt_fees(),
        regime=regime,
        resume=None if not prior else prior.get("state"),
    )
    doc = fms.record_from_book(name, day, book)
    doc["mean"] = fms.session_mean(doc.get("equity"), prior)
    doc["parent"] = SLEEVES[name]
    doc["s"] = s
    doc["sit"] = not picks
    doc["reasons"] = reasons
    doc["picks"] = picks
    doc["seal_sha256"] = (seal_doc or {}).get("sha256")
    doc["label"] = NEW_LABEL
    return doc


def render_md(name: str, records: list[dict]) -> str:
    parent = SLEEVES[name]
    lines = [
        f"# {name}",
        "",
        f"_{NEW_LABEL}. Parent recipe `{parent}` (same selection and exits); "
        "candidates only from inputs committed before 09:30 ET._",
        "",
    ]
    if parent == "union_hot_n4_h1":
        lines += [f"_{H1_NOT_IRONCLAD}._", ""]
    last = records[-1] if records else {}
    lines += [
        f"Equity: {last.get('equity')} | days: {len(records)}",
        "",
        "| date | S | picks | buys | sells | cash | equity | mean% | sit / reason |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for r in records:
        buys = ", ".join(f"{b.get('ticker')} {b.get('shares')}@{b.get('price')}"
                         for b in r.get("buys") or []) or "-"
        sells = ", ".join(f"{b.get('ticker')} {b.get('shares')}@{b.get('price')}"
                          for b in r.get("sells") or []) or "-"
        why = "; ".join(r.get("reasons") or []) or ("sit" if r.get("sit") else "-")
        why = why.replace("|", "/")
        lines.append(
            f"| {r['date']} | {r.get('s')} | {', '.join(r.get('picks') or []) or '-'} "
            f"| {buys} | {sells} | {r.get('cash')} | {r.get('equity')} "
            f"| {r.get('mean')} | {why} |"
        )
    return "\n".join(lines) + "\n"


def _summary_days(records: list[dict]) -> list[dict]:
    return [{
        "date": r.get("date"), "s": r.get("s"), "picks": r.get("picks"),
        "buys": [b.get("ticker") for b in r.get("buys") or []],
        "sells": [b.get("ticker") for b in r.get("sells") or []],
        "equity": r.get("equity"), "mean": r.get("mean"),
        "sit": r.get("sit"), "reasons": r.get("reasons"),
    } for r in records]


def _day_rows(doc: dict | None, name: str) -> list[dict]:
    sleeve = ((doc or {}).get("sleeves") or {}).get(name) or {}
    rows = sleeve.get("days") or []
    return [row for row in rows if isinstance(row, dict)]


def fill_forward(previous: dict | None, fresh: dict) -> dict:
    """Keep sealed day rows. Append only days after the last one on disk.

    A rebuilt row whose date is on or before that day must match the
    stored row. Anything else refuses, including a day inserted before
    the last updated day.
    """
    if not previous:
        return fresh
    merged = json.loads(json.dumps(fresh))
    prev_sleeves = previous.get("sleeves") or {}
    for name in prev_sleeves:
        if name not in (merged.get("sleeves") or {}):
            raise PreopenRefused(
                f"preopen: {name} dropped; sealed days are not rewritten")
    for name, sleeve in (merged.get("sleeves") or {}).items():
        prev_rows = _day_rows(previous, name)
        if not prev_rows:
            continue
        last = str(prev_rows[-1].get("date") or "")
        fresh_rows = _day_rows(merged, name)
        sealed = [row for row in fresh_rows if str(row.get("date") or "") <= last]
        added = [row for row in fresh_rows if str(row.get("date") or "") > last]
        if sealed != prev_rows:
            raise PreopenRefused(
                f"preopen: {name} would rewrite a sealed day on or before {last}")
        sleeve["days"] = list(prev_rows) + added
    return merged


def last_dashboard_day(doc: dict, name: str | None = None) -> str:
    sleeves = doc.get("sleeves") or {}
    chosen = [name] if name else list(sleeves)
    found = []
    for sleeve_name in chosen:
        for row in _day_rows(doc, sleeve_name):
            day = str(row.get("date") or "")
            if day:
                found.append(day)
    return max(found) if found else ""


def assert_current(end: str, path: Path) -> str:
    """Fail when the dashboard does not include the closed session."""
    if not path.is_file():
        raise PreopenRefused(
            f"preopen: {path} missing; closed session {end} was skipped")
    try:
        doc = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise PreopenRefused(
            f"preopen: {path} unreadable; closed session {end} was skipped"
        ) from exc
    sleeves = doc.get("sleeves") or {}
    if not sleeves:
        raise PreopenRefused(
            f"preopen: {path} has no sleeves; closed session {end} was skipped")
    behind = []
    for name in sleeves:
        got = last_dashboard_day(doc, name)
        if got < end:
            behind.append(f"{name} ends {got or 'empty'}")
    if behind:
        raise PreopenRefused(
            "preopen: weekday book skipped closed session "
            f"{end}: " + "; ".join(behind))
    return last_dashboard_day(doc)


def tape_tickers(days: list[str], *, clock: GitClock, state_dir: Path) -> list[str]:
    """Names whose 09:30 and 16:00 prints the missing days will mark."""
    from . import factor_mine_sequential as fms
    names: list[str] = []
    for day in days:
        seal_doc, _why = seal_for_book(day, clock=clock)
        for name in SLEEVES:
            prev = prior_session(day)
            prior = fms.read_state(name, prev, state_dir) if prev else None
            held = ((prior or {}).get("state") or {}).get("pos") or {}
            entry = ((seal_doc or {}).get("sleeves") or {}).get(name) or {}
            names.extend(str(t) for t in held)
            names.extend(str(t) for t in (entry.get("picks") or []))
    return sorted({t.upper() for t in names if t})


def ensure_session_tape(end: str, *, clock: GitClock, state_dir: Path) -> None:
    """Append missing 09:30/16:00 bars for the open book. Keep stored bars.

    The factor-mine remine is what normally commits ``ohlc.parquet``.
    When that job fails, the book used to exit on an empty tape and the
    dashboard stayed on the previous session. This fetches only the
    names this book will mark. A still-missing print fails the run.
    """
    from . import factor_mine as fm
    from . import factor_mine_sequential as fms
    from . import price_store as ps
    from . import ticker_lookback as tl
    missing = []
    for day in sessions(START, end):
        if not fm.session_has_closed(day):
            continue
        if any(fms.read_state(name, day, state_dir) is None for name in SLEEVES):
            missing.append(day)
    if not missing:
        return
    tickers = tape_tickers(missing, clock=clock, state_dir=state_dir)
    if not tickers:
        return
    try:
        ps.ensure_through(missing[-1], tickers=tickers, strict=True)
    except PreopenRefused:
        raise
    except Exception as exc:
        raise PreopenRefused(
            f"preopen: no 09:30/16:00 tape through {missing[-1]} "
            f"for {tickers}; weekday session skipped: {exc}"
        ) from exc
    tl.reset_price_caches()


def chain(name: str, end: str, state_dir: Path | None = None) -> list[dict]:
    from . import factor_mine_sequential as fms
    out = []
    for day in sessions(START, end):
        doc = fms.read_state(name, day, state_dir or STATE_DIR)
        if doc is None:
            break
        out.append(doc)
    return out


def book(end: str | None = None, *, bars=None, fees=None,
         clock: GitClock | None = None, state_dir: Path | None = None,
         score_dir: Path | None = None, lock: bool = True,
         summary_path: Path | None = None) -> dict[str, list[dict]]:
    """Step every closed session from START with no state yet, in order."""
    from . import factor_mine as fm
    from . import factor_mine_sequential as fms
    from . import past_day_lock as pdl
    from .skip_if_good import last_closed_session
    end = end or last_closed_session()
    state_dir = Path(state_dir or STATE_DIR)
    score_dir = Path(score_dir or SCORE_DIR)
    days = [d for d in sessions(START, end) if fm.session_has_closed(d)]
    if bars is None:
        ensure_session_tape(end, clock=clock or GitClock(), state_dir=state_dir)
    out: dict[str, list[dict]] = {}
    for name in SLEEVES:
        for day in days:
            if fms.read_state(name, day, state_dir) is not None:
                continue
            prev = prior_session(day)
            prior = fms.read_state(name, prev, state_dir) if prev else None
            if prev and prior is None:
                raise PreopenRefused(
                    f"preopen: {name} {day} has no prior state {prev}; "
                    "the chain is sequential and is not backfilled")
            seal_doc, why = seal_for_book(day, clock=clock)
            doc = step_day(name, day, seal_doc, why, prior=prior,
                           bars=bars, fees=fees)
            fms.write_state(name, day, doc, state_dir)
            print(f"[preopen] book {name} {day} equity={doc.get('equity')} "
                  f"sit={doc['sit']} buys={[b['ticker'] for b in doc['buys']]} "
                  f"sells={[b['ticker'] for b in doc['sells']]}", flush=True)
        records = chain(name, end, state_dir)
        out[name] = records
        if not records:
            continue
        text = render_md(name, records)
        live = lock and score_dir.resolve() == pdl.FACTOR_MINE_DIR.resolve()
        wm = pdl.guard_factor_mine_dir(score_dir, [(name, text)]) if live else ""
        score_dir.mkdir(parents=True, exist_ok=True)
        (score_dir / f"{name}.md").write_text(text, encoding="utf-8")
        if live:
            pdl.seal_factor_mine_dir(score_dir, [(name, text)], watermark=wm)
    write_summary(out, summary_path)
    assert_current(end, Path(summary_path) if summary_path else DASH_JSON)
    return out


def write_summary(records: dict[str, list[dict]], path: Path | None = None) -> dict:
    doc = {
        "start": START,
        "labels": {
            **{n: NEW_LABEL for n in SLEEVES},
            **{p: PARENT_LABEL for p in SLEEVES.values()},
            "union_hot_n4_h1__not_ironclad": H1_NOT_IRONCLAD,
        },
        "sleeves": {},
    }
    for name, recs in records.items():
        doc["sleeves"][name] = {
            "parent": SLEEVES[name],
            "label": NEW_LABEL,
            "days": _summary_days(recs),
        }
    targets = [Path(path)] if path else [SUMMARY_PATH, DASH_JSON]
    merged = doc
    for dest in targets:
        previous = None
        if dest.is_file():
            try:
                previous = json.loads(dest.read_text(encoding="utf-8"))
            except json.JSONDecodeError as exc:
                raise PreopenRefused(
                    f"preopen: {dest} is unreadable; sealed days were not rewritten"
                ) from exc
        merged = fill_forward(previous, doc)
        text = _dumps(merged)
        if dest.is_file() and dest.read_text(encoding="utf-8") == text:
            continue
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(text, encoding="utf-8")
    return merged


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("step", choices=["seal", "book", "check"])
    ap.add_argument("--date", default="")
    args = ap.parse_args(argv)
    if args.step == "seal":
        day = args.date or datetime.now(ET).date().isoformat()
        seal(day)
        return 0
    if args.step == "check":
        from .skip_if_good import last_closed_session
        end = args.date or last_closed_session()
        got = assert_current(end, DASH_JSON)
        print(f"[preopen] dashboard covers {got} through closed session {end}",
              flush=True)
        return 0
    book(args.date or None)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
