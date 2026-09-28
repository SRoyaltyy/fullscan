"""Seal a plan before the open, or a fill after the close.

The pre-open run writes one plan for session D. It uses the ranked picks,
the planned sells, and the excluded names, and no print from D. The
post-close run writes the fill for that plan at D's open, plus close
records, and does not edit the plan. A rerun of a sealed plan or fill does
nothing. Missing inputs append nothing.

HOLDUP_MODE=plan is the pre-open run. HOLDUP_MODE=open_fill is the 09:35 ET
open-fill. HOLDUP_MODE=fill is the post-close run. FORWARD_BOOK selects
holdup (default) or h1.
"""
from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.hot_n4_clean_v4.forward.book import current_book  # noqa: E402
from research.hot_n4_clean_v4.forward.ledger import (  # noqa: E402
    append_records,
    book_dates,
    canonical_bytes,
    kind_on,
    load,
    open_plan,
    plan_on,
    session_dates,
)
from research.hot_n4_clean_v4.forward.openfill import decide_fill, decide_open_fill  # noqa: E402
from research.hot_n4_clean_v4.forward.opens import collect_opens  # noqa: E402
from research.hot_n4_clean_v4.forward.planfill import (  # noqa: E402
    book_state,
    book_state_before,
    build_plan,
    hide_session,
)
from research.hot_n4_clean_v4.forward.prices import fetch_yahoo, overlay_forward  # noqa: E402
from research.hot_n4_clean_v4.forward.render import write_page  # noqa: E402
from research.hot_n4_clean_v4.protocol import (  # noqa: E402
    ENGINE_SHA256,
    FEES_PATH,
    FEES_SHA256,
    SESSIONS,
    file_sha256,
)
from research.hot_n4_clean_v4.run_study import (  # noqa: E402
    Halt,
    build_candidates,
    classify,
    halt_legs,
    load_bars,
    load_fees,
    nyse_sessions,
    open_px,
    prev_session,
    scan_legs,
)
from src.skip_if_good import is_nyse_holiday  # noqa: E402

GIT_REF = os.environ.get("HOLDUP_GIT_REF", "HEAD")
# Sessions before this date stay on the frozen monthly gzip. From this
# session the gzip has no trade_date row, so the plan reads theme-radar
# data/snapshots/<previous session>.csv.
SNAPSHOT_FROM = "2026-09-29"
ET = ZoneInfo("America/New_York")


def _skips() -> Path:
    return current_book().folder / "skips.jsonl"
OPEN_UTC = "13:30:00"
PANEL_COLS = [
    "trade_date", "snapshot_date", "Ticker", "Industry",
    "Market Cap", "Average Volume", "Volume", "Price",
]
SNAPSHOT_COLS = ["Ticker", "Industry", "Market Cap", "Average Volume", "Volume", "Price"]


def _fail(message: str) -> None:
    print(f"REFUSING: {message}", file=sys.stderr)


def next_session(day: str) -> str:
    cursor = date.fromisoformat(day) + timedelta(days=1)
    for _ in range(14):
        if cursor.weekday() < 5 and not is_nyse_holiday(cursor):
            return cursor.isoformat()
        cursor += timedelta(days=1)
    raise RuntimeError(f"no next session after {day}")


def _git(repo: Path, args: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["git", "-C", str(repo), *args],
        capture_output=True,
    )


def session_open_utc(session: str) -> datetime:
    """09:30 America/New_York on ``session``, as UTC. EDT is 13:30 UTC; EST is 14:30 UTC."""
    local = datetime.fromisoformat(f"{session}T09:30:00").replace(tzinfo=ET)
    return local.astimezone(timezone.utc)


def plan_clock(session: str, now: datetime | None = None) -> str | None:
    """Refuse a plan at or after 09:30 ET. Earlier than that returns None."""
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    open_at = session_open_utc(session)
    if now >= open_at:
        stamp = open_at.strftime("%Y-%m-%dT%H:%M:%SZ")
        return f"{session} is at or after 09:30 ET ({stamp}); refusing to seal a plan"
    return None


def plan_target(records: list[dict], requested: str) -> tuple[str | None, str | None]:
    """Next book session, or the requested date when it is that session."""
    dates = book_dates(records)
    target = next_session(dates[-1])
    if requested and requested != target:
        return None, f"session_date {requested} is not the next session {target}"
    return target, None


def blob_before(repo: Path, rel: str, limit: datetime, ref: str | None = None) -> tuple[str, str, bytes] | None:
    """Return (commit, committer iso, bytes) of ``rel`` last committed strictly before ``limit``."""
    if limit.tzinfo is None:
        limit = limit.replace(tzinfo=timezone.utc)
    cutoff = limit.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    spec = [ref] if ref else []
    listed = _git(repo, ["log", *spec, "-1", f"--before={cutoff}", "--format=%H %cI", "--", rel])
    if listed.returncode != 0 or not listed.stdout.strip():
        return None
    commit, stamp = listed.stdout.decode().split()
    when = datetime.fromisoformat(stamp)
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    if when.astimezone(timezone.utc) >= limit.astimezone(timezone.utc):
        return None
    blob = _git(repo, ["cat-file", "-p", f"{commit}:{rel}"])
    if blob.returncode != 0:
        return None
    return commit, stamp, blob.stdout


def blob_before_open(repo: Path, rel: str, session: str, ref: str | None = None) -> tuple[str, str, bytes] | None:
    """Return (commit, committer iso, bytes) of ``rel`` last committed before 13:30 UTC."""
    limit = datetime.fromisoformat(f"{session}T{OPEN_UTC}+00:00")
    return blob_before(repo, rel, limit, ref)


def _score_text(kind: str, raw: bytes) -> float | None:
    if kind == "predict":
        import re
        match = re.search(
            r"Prediction:\s*(UP|DOWN|FLAT).*?total score\s*(-?[\d.]+)",
            raw.decode("utf-8", errors="replace"),
        )
        if not match:
            return None
        return float(match.group(2))
    doc = json.loads(raw)
    raw_score = (doc.get("signals") or {}).get("general_score")
    try:
        value = float(raw_score)
    except (TypeError, ValueError):
        return None
    return value


def morning_score(session: str) -> tuple[dict | None, str | None]:
    """Predict wins. The blob must already have been on GitHub before 13:30 UTC."""
    for kind, rel in (
        ("predict", f"01_daily/general/{session}_predict.md"),
        ("weather", f"01_daily/weather/{session}_weather.json"),
    ):
        found = blob_before_open(ROOT, rel, session, GIT_REF)
        if found is None:
            continue
        commit, stamp, raw = found
        try:
            score = _score_text(kind, raw)
        except (json.JSONDecodeError, UnicodeError):
            return None, f"{rel} was on GitHub before 13:30 UTC but is not readable"
        blob = _git(ROOT, ["rev-parse", f"{commit}:{rel}"])
        blob_sha = blob.stdout.decode().strip() if blob.returncode == 0 else ""
        return {
            "blob_sha": blob_sha,
            "commit": commit,
            "kind": kind,
            "morning_s": score,
            "path": rel,
            "server_time_utc": stamp,
            "status": "BEFORE_1330",
        }, None
    return None, (
        f"no predict or weather file for {session} was on {GIT_REF} before 13:30 UTC"
    )


def _panel_repos() -> list[Path]:
    found = []
    env = os.environ.get("THEME_RADAR_DIR")
    if env:
        found.append(Path(env))
    found.append(ROOT / "vendor" / "theme-radar")
    found.append(Path("/tmp/theme-radar"))
    return [path for path in found if (path / ".git").exists()]


def frozen_frame(session: str):
    """Finviz rows for ``session`` from a theme-radar blob committed before the open."""
    import io

    import pandas as pd

    month = session[:7]
    rel = f"research/lever_panel/finviz_panel_asof0930_{month}.csv.gz"
    repos = _panel_repos()
    if not repos:
        return None, "theme-radar checkout is missing, so the frozen Finviz row is not available"
    saw_repo = False
    for repo in repos:
        found = blob_before_open(repo, rel, session)
        if found is None:
            continue
        saw_repo = True
        _commit, _stamp, raw = found
        try:
            frame = pd.read_csv(io.BytesIO(raw), usecols=PANEL_COLS, compression="gzip")
        except (ValueError, OSError) as exc:
            return None, f"frozen panel {rel} could not be read ({exc})"
        frame["trade_date"] = frame["trade_date"].astype(str).str[:10]
        frame["snapshot_date"] = frame["snapshot_date"].astype(str).str[:10]
        frame["Ticker"] = frame["Ticker"].astype(str).str.strip().str.upper()
        for col in ("Market Cap", "Average Volume", "Volume", "Price"):
            frame[col] = pd.to_numeric(frame[col], errors="coerce")
        day = frame.loc[frame["trade_date"] == session].copy()
        if day.empty:
            return None, (
                f"frozen Finviz row for {session} was not in {rel} "
                f"on a theme-radar commit before 13:30 UTC"
            )
        try:
            expect = prev_session(session)
        except RuntimeError as exc:
            return None, str(exc)
        bad = day.loc[day["snapshot_date"] != expect]
        if len(bad):
            return None, (
                f"{session} snapshot_date is not {expect}; the row is not repaired"
            )
        return day, None
    if not saw_repo:
        return None, (
            f"{rel} has no theme-radar commit before 13:30 UTC on {session}"
        )
    return None, f"frozen Finviz row for {session} is missing"


def _snapshot_provenance(
    rel: str, snap: str, status: str, commit: str | None = None,
    stamp: str | None = None, raw: bytes | None = None, reason: str | None = None,
) -> dict:
    body = {
        "commit": commit,
        "commit_time": stamp,
        "file_sha256": hashlib.sha256(raw).hexdigest() if raw is not None else None,
        "path": rel,
        "snapshot_date": snap,
        "status": status,
    }
    if reason:
        body["reason"] = reason
    return body


def _snapshot_frame(raw: bytes, session: str, snap: str):
    """Map a theme-radar snapshot CSV onto the frozen panel columns.

    Earnings Date is kept when the export has it. The pinned gzip never had
    that column, so a file without it leaves the earnings sources empty, the
    same way the rebuild did. The Finviz Open column is not a fill price.
    """
    import io

    import pandas as pd

    try:
        frame = pd.read_csv(io.BytesIO(raw))
    except (ValueError, OSError) as exc:
        return None, f"snapshot could not be read ({exc})"
    missing = [col for col in SNAPSHOT_COLS if col not in frame.columns]
    if missing:
        return None, f"snapshot is missing {', '.join(missing)}"
    keep = list(SNAPSHOT_COLS)
    if "Earnings Date" in frame.columns:
        keep.append("Earnings Date")
    frame = frame.loc[:, keep].copy()
    frame["Ticker"] = frame["Ticker"].astype(str).str.strip().str.upper()
    frame["trade_date"] = session
    frame["snapshot_date"] = snap
    for col in ("Market Cap", "Average Volume", "Volume", "Price"):
        frame[col] = pd.to_numeric(frame[col], errors="coerce")
    return frame, None


def snapshot_for(session: str) -> tuple[object, dict]:
    """Finviz inputs for session D >= 2026-09-29.

    The file is data/snapshots/<previous trading day>.csv as of the latest
    theme-radar commit strictly before 09:30 ET on D. A missing file, a commit
    that is not before the open, or an unreadable export returns no frame.
    The caller then seals a plan with no Finviz names, the same as 2026-08-28.
    """
    try:
        snap = prev_session(session)
    except RuntimeError as exc:
        return None, _snapshot_provenance(
            "", "", "MISSING", reason=str(exc),
        )
    rel = f"data/snapshots/{snap}.csv"
    limit = session_open_utc(session)
    repos = _panel_repos()
    if not repos:
        return None, _snapshot_provenance(
            rel, snap, "MISSING",
            reason="theme-radar checkout is missing, so the snapshot is not available",
        )
    for repo in repos:
        found = blob_before(repo, rel, limit)
        if found is None:
            continue
        commit, stamp, raw = found
        frame, why = _snapshot_frame(raw, session, snap)
        if why:
            return None, _snapshot_provenance(
                rel, snap, "MISSING", commit=commit, stamp=stamp, raw=raw, reason=why,
            )
        return frame, _snapshot_provenance(
            rel, snap, "BEFORE_OPEN", commit=commit, stamp=stamp, raw=raw,
        )
    return None, _snapshot_provenance(
        rel, snap, "MISSING",
        reason=(
            f"{rel} has no theme-radar commit before 09:30 ET on {session}; "
            "Finviz names are skipped and no new buy is filled"
        ),
    )


def _latest_bar(stored: dict) -> str:
    latest = ""
    for blob in stored.values():
        dates = blob.get("date") or []
        if dates and dates[-1] > latest:
            latest = dates[-1]
    return latest


def _history_ok(records: list[dict], bars: dict) -> str | None:
    stored = bars["stored"]
    for record in records:
        kind = record["kind"]
        if kind in ("session", "fill", "open_fill"):
            rows = [(record["date"], row["ticker"], row["fill"]) for row in record.get("buys") or []]
            rows += [(record["date"], row["ticker"], row["fill"]) for row in record.get("sells") or []]
        elif kind == "mark":
            rows = [(record["date"], row["ticker"], row["fill"]) for row in record.get("added_buys") or []]
            rows += [(record["date"], row["ticker"], row["fill"]) for row in record.get("added_sells") or []]
        elif kind == "close":
            rows = [(record["date"], record["ticker"], record["fill"])]
        else:
            continue
        for day, ticker, fill in rows:
            op = open_px(stored, ticker, day)
            if op is None:
                # The 09:35 open is sealed from the live Yahoo print. The
                # daily bar is stored only once it is final, so an open-fill
                # may not be in the file yet. A stored open that differs
                # still fails below.
                if kind == "open_fill":
                    continue
                return f"stored open for {ticker} on {day} no longer matches the sealed fill"
            if float(op) != float(fill):
                return f"stored open for {ticker} on {day} no longer matches the sealed fill"
    return None


def _engine_ok() -> str | None:
    for path, digest in ENGINE_SHA256.items():
        if file_sha256(ROOT / path) != digest:
            return f"engine bytes changed {path}; the v4 protocol is not unchanged"
    if file_sha256(ROOT / FEES_PATH) != FEES_SHA256:
        return "fee schedule bytes changed"
    return None


def _index() -> dict[str, int]:
    calendar = nyse_sessions("2026-01-01", "2026-12-31")
    return {day: i for i, day in enumerate(calendar)}


def _log_skip(session: str, reason: str, records: list[dict]) -> None:
    print(f"append nothing: {session}: {reason}")
    last = None
    skips = _skips()
    if skips.is_file() and skips.stat().st_size:
        last = json.loads(skips.read_text(encoding="utf-8").splitlines()[-1])
    if last and last.get("date") == session and last.get("reason") == reason:
        write_page(records, {"date": session, "latest_skip": reason, "recipe": current_book().recipe})
        return
    body = {
        "at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "date": session,
        "reason": reason,
        "recipe": current_book().recipe,
    }
    with skips.open("ab") as handle:
        handle.write(canonical_bytes(body))
    write_page(records, {"date": session, "latest_skip": reason, "recipe": current_book().recipe})


def _halt_known(session: str, bars, held: set[str]) -> None:
    stored = bars["stored"]
    iwm = [leg for leg in scan_legs(stored, "IWM", session) if classify(leg) != "split"]
    if iwm:
        raise Halt(
            f"IWM {session} unexplained leg {iwm[0]['leg']} on {iwm[0]['bar_date']}"
        )
    for ticker in sorted(held):
        bad = halt_legs(stored, ticker, session)
        if bad:
            raise Halt(
                f"held {ticker} {session} unexplained {bad[0]['leg']} on {bad[0]['bar_date']}"
            )


def _payload(session: str, bars, held: set[str], score: dict, frame) -> dict:
    _halt_known(session, bars, held)
    candidates, excluded, _no_fill = build_candidates(
        bars["feat"], bars["stored"], frame, session, held,
    )
    return {
        "bar_cutoff": prev_session(session),
        "candidates": candidates,
        "day_card_sha256": None,
        "excluded_unexplained_legs": excluded,
        "morning_s": score["morning_s"],
        "session": session,
    }


def _gap_payload(session: str, bars, held: set[str], score: dict) -> dict:
    """No Finviz row. No new names. Held lots still follow the list-drop rule."""
    _halt_known(session, bars, held)
    return {
        "bar_cutoff": prev_session(session),
        "candidates": [],
        "day_card_sha256": None,
        "excluded_unexplained_legs": [],
        "morning_s": score["morning_s"],
        "session": session,
    }


def _ready(records: list[dict]) -> tuple[dict | None, int]:
    dates = session_dates(records)
    if dates[:len(SESSIONS)] != list(SESSIONS):
        _fail("seeded sessions are not the locked 2026-08-13..2026-09-25 prefix")
        return None, 1
    engine = _engine_ok()
    if engine:
        _fail(engine)
        return None, 1
    return records, 0


def _bars(records: list[dict], target: str) -> tuple[dict | None, int]:
    try:
        bars = overlay_forward(load_bars())
    except Exception as exc:  # noqa: BLE001 — a missing price file is an input gap
        _log_skip(target, f"price store could not be read ({exc})", records)
        return None, 0
    drifted = _history_ok(records, bars)
    if drifted:
        _fail(drifted)
        return None, 1
    return bars, 0


def _has_bar(bars: dict, session: str) -> bool:
    for blob in bars["stored"].values():
        dates = blob.get("date") or []
        if session in dates:
            return True
    return False


def _status(session: str, pending: str | None, skip: str | None, phase: str) -> dict:
    return {
        "date": session,
        "latest_skip": skip,
        "pending": pending,
        "phase": phase,
        "recipe": current_book().recipe,
    }


def plan_main() -> int:
    """Seal session D from inputs available before 13:30 UTC. No prices from D."""
    try:
        records = load()
    except RuntimeError as exc:
        _fail(f"existing record hash does not match the ledger ({exc})")
        return 1
    records, code = _ready(records)
    if records is None:
        return code
    pending = open_plan(records)
    if pending is not None:
        print(f"plan already sealed {pending['date']}")
        write_page(records, _status(pending["date"], pending["date"], None, "plan"))
        return 0
    requested = os.environ.get("PLAN_SESSION", "").strip()
    target, why = plan_target(records, requested)
    if why or target is None:
        _fail(why or "no session")
        return 1
    late = plan_clock(target)
    if late:
        _fail(late)
        return 1
    print(f"plan {target}", flush=True)
    index = _index()
    if target not in index:
        _log_skip(target, f"{target} is outside the v4 session calendar through 2026-12-31", records)
        return 0
    bars, code = _bars(records, target)
    if bars is None:
        return code
    score, why = morning_score(target)
    if why:
        _log_skip(target, why, records)
        return 0
    finviz = None
    if target >= SNAPSHOT_FROM:
        frame, finviz = snapshot_for(target)
    else:
        frame, why = frozen_frame(target)
        if why:
            _log_skip(target, why, records)
            return 0
    state = book_state(records)
    held = set(state["pos"])
    hidden = hide_session(bars, target)
    try:
        if frame is None:
            payload = _gap_payload(target, hidden, held, score)
        else:
            payload = _payload(target, hidden, held, score, frame)
        plan = build_plan(payload, state, index)
    except Halt as exc:
        _fail(str(exc))
        return 1
    except RuntimeError as exc:
        _log_skip(target, str(exc), records)
        return 0
    plan["s_source"] = {
        "blob_sha": score["blob_sha"],
        "kind": score["kind"],
        "path": score["path"],
        "server_time_utc": score["server_time_utc"],
        "status": score["status"],
    }
    if finviz is not None:
        plan["finviz_source"] = finviz
    try:
        current = load()
    except RuntimeError as exc:
        _fail(f"existing record hash does not match the ledger ({exc})")
        return 1
    if open_plan(current) is not None or any(
        row["kind"] == "plan" and row["date"] == target for row in current
    ):
        print(f"plan already sealed {target}")
        return 0
    append_records([plan])
    written = load()
    write_page(written, _status(target, target, None, "plan"))
    print(
        f"sealed plan {target} picks {len(plan['picks'])} "
        f"planned sells {len(plan['planned_sells'])}",
        flush=True,
    )
    return 0


def _today() -> str:
    raw = os.environ.get("FORWARD_SESSION")
    if raw:
        return raw
    return datetime.now(timezone.utc).date().isoformat()


def _already(records: list[dict], session: str) -> bool:
    return any(
        row["kind"] in ("open_fill", "fill", "mark") and row["date"] == session
        for row in records
    )


def open_fill_main() -> int:
    """Fill today's sealed plan at the official open. No close P&L yet.

    No plan for today appends nothing. A plan already filled appends nothing.
    No trustworthy open appends nothing and leaves the post-close fill to
    write the trades. 13:35 UTC is 09:35 ET only during EDT (UTC-4). During
    EST (from 2026-11-01, UTC-5) the same cron is 08:35 ET, before the open,
    and this run then finds no open.
    """
    try:
        records = load()
    except RuntimeError as exc:
        _fail(f"existing record hash does not match the ledger ({exc})")
        return 1
    records, code = _ready(records)
    if records is None:
        return code
    target = _today()
    plan = plan_on(records, target)
    if plan is None:
        print(f"no plan for {target}; append nothing")
        return 0
    if _already(records, target):
        print(f"open fill already sealed {target}")
        phase = "fill" if kind_on(records, target, "fill") or kind_on(records, target, "mark") else "open_fill"
        write_page(records, _status(target, None if phase == "fill" else target, None, phase))
        return 0
    print(f"open fill {target}", flush=True)
    index = _index()
    if target not in index:
        _log_skip(target, f"{target} is outside the v4 session calendar through 2026-12-31", records)
        return 0
    bars, code = _bars(records, target)
    if bars is None:
        return code
    held = set(book_state_before(records, target)["pos"])
    names = sorted(
        held
        | {"IWM"}
        | {row["ticker"] for row in plan.get("picks") or []}
        | {row["ticker"] for row in plan.get("planned_sells") or []}
    )
    opens = collect_opens(names, target, bars["stored"], fetch_yahoo)
    fees = load_fees()
    try:
        bodies, why = decide_open_fill(records, target, opens, bars, fees, index)
    except Halt as exc:
        _fail(str(exc))
        return 1
    except RuntimeError as exc:
        _log_skip(target, str(exc), records)
        return 0
    if not bodies:
        if why:
            _log_skip(target, why, records)
        else:
            print(f"append nothing {target}")
        return 0
    try:
        current = load()
    except RuntimeError as exc:
        _fail(f"existing record hash does not match the ledger ({exc})")
        return 1
    if plan_on(current, target) is None or _already(current, target):
        print(f"open fill already sealed {target}")
        return 0
    append_records(bodies)
    written = load()
    write_page(written, _status(target, target, None, "open_fill"))
    opened = bodies[0]
    print(
        f"sealed open fill {target} buys {len(opened['buys'])} "
        f"sells {len(opened['sells'])} pnl pending",
        flush=True,
    )
    return 0


def fill_main() -> int:
    """Add closes and the close mark. Do not repeat an open-fill already sealed.

    When the 09:35 run wrote nothing, this writes the whole fill and its
    closes, which is the path the book used before open-fill existed.
    """
    try:
        records = load()
    except RuntimeError as exc:
        _fail(f"existing record hash does not match the ledger ({exc})")
        return 1
    records, code = _ready(records)
    if records is None:
        return code
    pending = open_plan(records)
    if pending is None:
        print("no unfilled plan")
        return 0
    target = pending["date"]
    print(f"fill {target}", flush=True)
    index = _index()
    if target not in index:
        _log_skip(target, f"{target} is outside the v4 session calendar through 2026-12-31", records)
        return 0
    bars, code = _bars(records, target)
    if bars is None:
        return code
    if not _has_bar(bars, target):
        latest = _latest_bar(bars["stored"])
        _log_skip(
            target,
            f"price store latest bar is {latest or 'empty'}; {target} open is not in the file yet",
            records,
        )
        return 0
    fees = load_fees()
    try:
        bodies, why = decide_fill(records, bars, fees, index)
    except (Halt, RuntimeError) as exc:
        _fail(str(exc))
        return 1
    if not bodies:
        if why:
            _log_skip(target, why, records)
        return 0
    try:
        current = load()
    except RuntimeError as exc:
        _fail(f"existing record hash does not match the ledger ({exc})")
        return 1
    if open_plan(current) is None:
        print(f"fill already sealed {target}")
        return 0
    append_records(bodies)
    written = load()
    write_page(written, _status(target, None, None, bodies[0]["kind"]))
    if bodies[0]["kind"] == "mark":
        print(
            f"sealed mark {target} added buys {len(bodies[0]['added_buys'])} "
            f"added sells {len(bodies[0]['added_sells'])} closes {len(bodies) - 1}",
            flush=True,
        )
    else:
        print(
            f"sealed fill {target} buys {len(bodies[0]['buys'])} "
            f"sells {len(bodies[0]['sells'])} closes {len(bodies) - 1}",
            flush=True,
        )
    return 0


def main() -> int:
    mode = os.environ.get("HOLDUP_MODE", "plan")
    if mode == "fill":
        return fill_main()
    if mode == "open_fill":
        return open_fill_main()
    if mode != "plan":
        _fail(f"HOLDUP_MODE {mode}")
        return 1
    return plan_main()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except RuntimeError as exc:
        _fail(str(exc))
        raise SystemExit(1)
