"""Seal a plan before the open, or a fill after the close.

The pre-open run writes one plan for session D. It uses the ranked picks,
the planned sells, and the excluded names, and no print from D. The
post-close run writes the fill for that plan at D's open, plus close
records, and does not edit the plan. A rerun of a sealed plan or fill does
nothing. Missing inputs append nothing.

HOLDUP_MODE=plan is the pre-open run. HOLDUP_MODE=fill is the post-close run.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.hot_n4_clean_v4.forward.ledger import (  # noqa: E402
    HERE,
    RECIPE,
    append_records,
    book_dates,
    canonical_bytes,
    load,
    open_plan,
    session_dates,
)
from research.hot_n4_clean_v4.forward.planfill import (  # noqa: E402
    book_state,
    build_plan,
    fill_book,
    hide_session,
)
from research.hot_n4_clean_v4.forward.prices import overlay_forward  # noqa: E402
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

SKIPS = HERE / "skips.jsonl"
GIT_REF = os.environ.get("HOLDUP_GIT_REF", "HEAD")
OPEN_UTC = "13:30:00"
PANEL_COLS = [
    "trade_date", "snapshot_date", "Ticker", "Industry",
    "Market Cap", "Average Volume", "Volume", "Price",
]


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


def blob_before_open(repo: Path, rel: str, session: str, ref: str | None = None) -> tuple[str, str, bytes] | None:
    """Return (commit, committer iso, bytes) of ``rel`` last committed before 13:30 UTC."""
    cutoff = f"{session}T{OPEN_UTC}Z"
    spec = [ref] if ref else []
    listed = _git(repo, ["log", *spec, "-1", f"--before={cutoff}", "--format=%H %cI", "--", rel])
    if listed.returncode != 0 or not listed.stdout.strip():
        return None
    commit, stamp = listed.stdout.decode().split()
    when = datetime.fromisoformat(stamp)
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    limit = datetime.fromisoformat(f"{session}T{OPEN_UTC}+00:00")
    if when.astimezone(timezone.utc) >= limit:
        return None
    blob = _git(repo, ["cat-file", "-p", f"{commit}:{rel}"])
    if blob.returncode != 0:
        return None
    return commit, stamp, blob.stdout


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
        if record["kind"] == "session":
            rows = [(record["date"], row["ticker"], row["fill"]) for row in record["buys"]]
            rows += [(record["date"], row["ticker"], row["fill"]) for row in record["sells"]]
        else:
            rows = [(record["date"], record["ticker"], record["fill"])]
        for day, ticker, fill in rows:
            op = open_px(stored, ticker, day)
            if op is None or float(op) != float(fill):
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
    if SKIPS.is_file() and SKIPS.stat().st_size:
        last = json.loads(SKIPS.read_text(encoding="utf-8").splitlines()[-1])
    if last and last.get("date") == session and last.get("reason") == reason:
        write_page(records, {"date": session, "latest_skip": reason, "recipe": RECIPE})
        return
    body = {
        "at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "date": session,
        "reason": reason,
        "recipe": RECIPE,
    }
    with SKIPS.open("ab") as handle:
        handle.write(canonical_bytes(body))
    write_page(records, {"date": session, "latest_skip": reason, "recipe": RECIPE})


def _payload(session: str, bars, held: set[str], score: dict, frame) -> dict:
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
    candidates, excluded, _no_fill = build_candidates(
        bars["feat"], stored, frame, session, held,
    )
    return {
        "bar_cutoff": prev_session(session),
        "candidates": candidates,
        "day_card_sha256": None,
        "excluded_unexplained_legs": excluded,
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
        "recipe": RECIPE,
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
    dates = book_dates(records)
    target = next_session(dates[-1])
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
    frame, why = frozen_frame(target)
    if why:
        _log_skip(target, why, records)
        return 0
    state = book_state(records)
    held = set(state["pos"])
    try:
        payload = _payload(target, hide_session(bars, target), held, score, frame)
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


def fill_main() -> int:
    """Fill a sealed plan at D's open. Does not edit the plan."""
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
    state = book_state(records)
    fees = load_fees()
    try:
        fill, closes, _state = fill_book(pending, state, bars, fees, index)
    except (Halt, RuntimeError) as exc:
        _fail(str(exc))
        return 1
    try:
        current = load()
    except RuntimeError as exc:
        _fail(f"existing record hash does not match the ledger ({exc})")
        return 1
    if open_plan(current) is None:
        print(f"fill already sealed {target}")
        return 0
    append_records([fill, *closes])
    written = load()
    write_page(written, _status(target, None, None, "fill"))
    print(
        f"sealed fill {target} buys {len(fill['buys'])} "
        f"sells {len(fill['sells'])} closes {len(closes)}",
        flush=True,
    )
    return 0


def main() -> int:
    mode = os.environ.get("HOLDUP_MODE", "plan")
    if mode == "fill":
        return fill_main()
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
