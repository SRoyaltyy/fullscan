"""Append the next missing holdup session. Never rewrite a sealed record.

The job is meant to run on a weekday after the pre-open inputs are on
GitHub and before the US cash open. It writes one session, plus close
records whose fills are that session's official open. A rerun of a session
that is already sealed does nothing. Missing inputs append nothing.
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
    canonical_bytes,
    load,
    session_dates,
)
from research.hot_n4_clean_v4.forward.render import write_page  # noqa: E402
from research.hot_n4_clean_v4.forward.step import state_from_session, step  # noqa: E402
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


def main() -> int:
    try:
        records = load()
    except RuntimeError as exc:
        _fail(f"existing record hash does not match the ledger ({exc})")
        return 1
    dates = session_dates(records)
    if dates[:len(SESSIONS)] != list(SESSIONS):
        _fail("seeded sessions are not the locked 2026-08-13..2026-09-25 prefix")
        return 1
    engine = _engine_ok()
    if engine:
        _fail(engine)
        return 1
    target = next_session(dates[-1])
    print(f"next missing session {target}", flush=True)
    try:
        bars = load_bars()
    except Exception as exc:  # noqa: BLE001 — a missing price file is an input gap
        _log_skip(target, f"price store could not be read ({exc})", records)
        return 0
    drifted = _history_ok(records, bars)
    if drifted:
        _fail(drifted)
        return 1
    latest = _latest_bar(bars["stored"])
    if latest < target:
        _log_skip(
            target,
            f"price store latest bar is {latest or 'empty'}; {target} open is not in the file yet",
            records,
        )
        return 0
    index = _index()
    if target not in index:
        _log_skip(target, f"{target} is outside the v4 session calendar through 2026-12-31", records)
        return 0
    score, why = morning_score(target)
    if why:
        _log_skip(target, why, records)
        return 0
    frame, why = frozen_frame(target)
    if why:
        _log_skip(target, why, records)
        return 0
    last = [row for row in records if row["kind"] == "session"][-1]
    state = state_from_session(last)
    held = set(state["pos"])
    try:
        payload = _payload(target, bars, held, score, frame)
    except Halt as exc:
        _fail(str(exc))
        return 1
    except RuntimeError as exc:
        _log_skip(target, str(exc), records)
        return 0
    fees = load_fees()
    session_body, closes, _state = step(payload, state, bars, fees, index)
    session_body["s_source"] = {
        "blob_sha": score["blob_sha"],
        "kind": score["kind"],
        "path": score["path"],
        "server_time_utc": score["server_time_utc"],
        "status": score["status"],
    }
    # Re-read before the write so a sealed session is left untouched.
    try:
        current = load()
    except RuntimeError as exc:
        _fail(f"existing record hash does not match the ledger ({exc})")
        return 1
    if target in session_dates(current):
        print(f"already appended {target}")
        return 0
    append_records([session_body, *closes])
    written = load()
    write_page(written, {
        "appended": target,
        "latest_skip": None,
        "recipe": RECIPE,
    })
    print(
        f"appended {target} buys {len(session_body['buys'])} "
        f"sells {len(session_body['sells'])} closes {len(closes)}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except RuntimeError as exc:
        _fail(str(exc))
        raise SystemExit(1)
