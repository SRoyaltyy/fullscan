"""Score the 110 labelled Factor Mine recipes on the clean tape.

Same fingerprinted-prereg discipline as the labelled study. The only
changed input is the price tape: the PR #362 rebuild, with split
artefacts repaired and unrepairable names absent. Price features use
bars dated strictly before the session. The session open is the fill.
The session close is the mark only.
"""
from __future__ import annotations

import hashlib
import json
import subprocess
from pathlib import Path

from src.lever_search_bars import feature_bars  # noqa: F401  (re-exported contract)
from src.lever_search_clean_append import manifest_line
from src.lever_search_clean_protocol import (
    CHECK_CALENDAR,
    CLEAN_BAR_BLOB_SHA,
    CLEAN_BAR_COMMIT,
    CLEAN_BAR_PATH,
    CLEAN_BAR_SHA256,
    DESIGNED_AFTER_END,
    DESIGNED_AFTER_START,
    LUCK_N,
    RETURNS,
    RUN_WINDOW,
    SESSIONS,
    STUDY,
    STUDY_LABEL,
    check_day_list,
    load_manifest,
    role_present,
    search_day_list,
)
from src.lever_search_labelled_score import (
    _ab_tone,
    _export_events,
    _git_blob,
    _pin_ok,
    assemble_row,
    walk_recipe,
)
from src.lever_search_proof import build_group3_recipes, required_roles
from src.lever_search_score import (
    CAPITAL,
    Store,
    _news_map,
    best_removed,
    compound,
    luck_p,
)
from src.paper_trade import load_fees

ROOT = Path(__file__).resolve().parents[1]


class CleanStore(Store):
    """The clean tape. Loaded through duckdb; the pandas engine is not needed.

    Same Store interface and the same feature_bars guard. The tape's date
    column is already a string.
    """

    def __init__(self) -> None:
        import duckdb

        path = ROOT / CLEAN_BAR_PATH
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        if digest != CLEAN_BAR_SHA256:
            raise SystemExit(f"clean ohlc.parquet sha256 {digest} != pin {CLEAN_BAR_SHA256}")
        rows = duckdb.sql(
            "SELECT CAST(date AS VARCHAR) AS date, CAST(ticker AS VARCHAR) AS ticker, "
            "open, high, low, close, volume "
            f"FROM read_parquet('{path.as_posix()}') ORDER BY ticker, date"
        ).fetchall()
        tapes: dict[str, dict] = {}
        dates: set[str] = set()
        for date, ticker, open_, high, low, close, volume in rows:
            day = str(date)
            dates.add(day)
            tape = tapes.setdefault(str(ticker), {
                "date": [], "open": [], "high": [], "low": [], "close": [], "volume": [],
            })
            tape["date"].append(day)
            tape["open"].append(float(open_))
            tape["high"].append(float(high))
            tape["low"].append(float(low))
            tape["close"].append(float(close))
            tape["volume"].append(float(volume) if volume is not None else 0.0)
        self.dates = tuple(sorted(dates))
        self.tapes = tapes
        self._feat = {}


def build_session(store: Store, index: dict, session: str,
                  caches: dict) -> tuple[list[dict], list[dict]]:
    """The labelled build_session with the clean tape pinned instead."""
    from src.finviz_events import asof_snapshot

    panel_meta = index.get((session, "panel"))
    if not panel_meta or int(panel_meta.get("n_rows") or 0) < 1:
        raise SystemExit(
            f"{session}: no earlier panel.json version labelled this day. "
            "The per-day source files would be the candidate list. "
            "This run stops because the prereg lists an earliest version for every session."
        )
    parsed = caches["panel"].get(panel_meta["blob_sha"])
    if parsed is None:
        parsed = json.loads(_git_blob(panel_meta["blob_sha"]))
        caches["panel"][panel_meta["blob_sha"]] = parsed
    raw_rows = [
        row for row in (parsed.get("rows") or [])
        if isinstance(row, dict) and row.get("date") == session
    ]
    if len(raw_rows) != int(panel_meta["n_rows"]):
        raise SystemExit(f"{session}: panel n_rows {len(raw_rows)} != {panel_meta['n_rows']}")
    tickers = {str(row.get("ticker") or "").strip().upper() for row in raw_rows}

    news = None
    actions = index.get((session, "actions"))
    if actions:
        news = caches["news"].get(actions["blob_sha"])
        if news is None:
            news = _news_map(_git_blob(actions["blob_sha"]))
            caches["news"][actions["blob_sha"]] = news
    ab = None
    left = index.get((session, "ab_checklist"))
    right = index.get((session, "ab_enriched"))
    if left and right:
        key = (left["blob_sha"], right["blob_sha"])
        ab = caches["ab"].get(key)
        if ab is None:
            ab = _ab_tone(_git_blob(left["blob_sha"]), _git_blob(right["blob_sha"]))
            caches["ab"][key] = ab
    export_index = None
    export = index.get((session, "export"))
    if export:
        export_index = caches["export"].get(export["blob_sha"])
        if export_index is None:
            export_index = _export_events(_git_blob(export["blob_sha"]), tickers)
            caches["export"][export["blob_sha"]] = export_index

    # SPY is absent from the clean tape. rs_week is empty for every row.
    spy_ret5 = None
    rows = []
    for panel in raw_rows:
        ticker = str(panel.get("ticker") or "").strip().upper()
        feat = store.feature(ticker, session)
        snap = None
        if export_index is not None:
            snap = asof_snapshot(export_index.get(ticker) or [], session)
        rows.append(assemble_row(panel, feat, news, ab, snap, spy_ret5))

    blobs = [{
        "path": CLEAN_BAR_PATH,
        "blob_sha": CLEAN_BAR_BLOB_SHA,
        "commit": CLEAN_BAR_COMMIT,
    }]
    for role in ("panel", "stock_book", "actions", "ab_checklist", "ab_enriched",
                 "export", "catalyst", "join"):
        meta = index.get((session, role))
        if meta:
            blobs.append({
                "path": meta["path"],
                "blob_sha": meta["blob_sha"],
                "commit": meta["commit"],
            })
    blobs = sorted(blobs, key=lambda item: (item["path"], item["blob_sha"]))
    return rows, blobs


def score_all() -> dict:
    _manifest, index = load_manifest()
    seen: set[tuple[str, str]] = set()
    for row in index.values():
        key = (row["commit"], row["path"])
        if key in seen:
            continue
        seen.add(key)
        _pin_ok(row)
    bar_blob = subprocess.check_output(
        ["git", "rev-parse", f"{CLEAN_BAR_COMMIT}:{CLEAN_BAR_PATH}"],
        cwd=ROOT,
        text=True,
    ).strip()
    if bar_blob != CLEAN_BAR_BLOB_SHA:
        raise SystemExit("pinned clean bar blob mismatch")
    print("loading clean bars", flush=True)
    store = CleanStore()
    print(f"tickers {len(store.tapes)} dates {len(store.dates)}", flush=True)
    recipes = build_group3_recipes()
    if len(recipes) != 110:
        raise SystemExit(f"recipe count {len(recipes)}")
    rows_by_date: dict[str, list[dict]] = {}
    blobs_by_date: dict[str, list[dict]] = {}
    caches: dict = {"panel": {}, "news": {}, "ab": {}, "export": {}}
    for session in SESSIONS:
        print(f"build {session}", flush=True)
        rows, blobs = build_session(store, index, session, caches)
        rows_by_date[session] = rows
        blobs_by_date[session] = blobs
        print(f"  rows {len(rows)} blobs {len(blobs)}", flush=True)
    fees = load_fees()
    books = []
    sessions = list(SESSIONS)
    for recipe in recipes:
        need = required_roles(recipe)
        ok = {
            day: all(role_present(index, day, role) for role in need)
            for day in sessions
        }
        frozen = set(search_day_list(recipe, index))
        for day in RUN_WINDOW:
            if ok[day] != (day in frozen):
                raise SystemExit(f"search day drift {recipe['name']} {day}")
        print(f"walk {recipe['name']}", flush=True)
        book = walk_recipe(recipe, sessions, rows_by_date, store, ok, fees)
        books.append((recipe, book, ok))
    return {
        "sessions": sessions,
        "blobs_by_date": blobs_by_date,
        "books": books,
        "index": index,
    }


def _round(value, digits: int):
    if value is None:
        return None
    return round(float(value), digits)


def write_record(scored: dict) -> dict:
    RETURNS.mkdir(parents=True, exist_ok=True)
    sessions = scored["sessions"]
    books = scored["books"]
    index = scored["index"]
    by_session = {
        day: {"session": day, "study": STUDY, "recipes": {}}
        for day in sessions
    }
    summaries = []
    check_calendar = set(CHECK_CALENDAR)
    for recipe, book, ok in books:
        name = recipe["name"]
        check = [day for day in search_day_list(recipe, index) if day in check_calendar]
        if list(check_day_list(recipe, index)) != check:
            raise SystemExit(f"check day drift {name}")
        rets_f = []
        rets_15 = []
        after_f = []
        after_15 = []
        equity_start = {}
        prev_f = CAPITAL
        for day in book["days"]:
            session = day["session"]
            equity_start[session] = prev_f
            equity_start[session + "|end"] = day["equity_futubull"]
            prev_f = day["equity_futubull"]
            by_session[session]["recipes"][name] = {
                "sat_out": day["sat_out"],
                "picks": day["picks"],
                "fills": day["fills"],
                "ret_futubull": _round(day["ret_futubull"], 8),
                "ret_flat_15bp": _round(day["ret_flat_15bp"], 8),
                "under_3_share": None if day["under_3_share"] is None else _round(day["under_3_share"], 6),
            }
            if session in check:
                rets_f.append(day["ret_futubull"])
                rets_15.append(day["ret_flat_15bp"])
            if DESIGNED_AFTER_START <= session <= DESIGNED_AFTER_END:
                after_f.append(day["ret_futubull"])
                after_15.append(day["ret_flat_15bp"])
        ticker, removed = best_removed(check, book["ticker_pnl"], book["first_entry"], equity_start)
        cum_f = compound(rets_f) if rets_f else 0.0
        cum_15 = compound(rets_15) if rets_15 else 0.0
        mean_f = sum(rets_f) / len(rets_f) if rets_f else 0.0
        mean_15 = sum(rets_15) / len(rets_15) if rets_15 else 0.0
        p_f = luck_p(rets_f, LUCK_N)
        p_15 = luck_p(rets_15, LUCK_N)
        line_a = cum_f >= 0.20
        line_b = removed is not None and removed > 0.0
        line_c = compound(after_f) >= 0.0 if after_f else False
        line_d = p_f < 0.05
        n_check = len(check)
        if line_a and line_b and line_c and line_d and n_check < 10:
            label = "too few days to judge"
        elif line_a and line_b and line_c and line_d:
            label = "something good"
        else:
            label = "not something good"
        summaries.append({
            "name": name,
            "side": recipe.get("side") or "long",
            "n_check_days": n_check,
            "n_search_days": sum(1 for day in RUN_WINDOW if ok.get(day)),
            "cum_futubull": cum_f,
            "cum_flat_15bp": cum_15,
            "mean_futubull": mean_f,
            "mean_flat_15bp": mean_15,
            "best_ticker": ticker,
            "best_removed": removed,
            "from_0914_futubull": compound(after_f) if after_f else None,
            "from_0914_flat_15bp": compound(after_15) if after_15 else None,
            "under_3_share": (book["n_under_3"] / book["n_fills"]) if book["n_fills"] else None,
            "n_fills": book["n_fills"],
            "luck_p_futubull": p_f,
            "luck_p_flat_15bp": p_15,
            "line_a": line_a,
            "line_b": line_b,
            "line_c": line_c,
            "line_d": line_d,
            "label": label,
            "full_span_futubull": compound([day["ret_futubull"] for day in book["days"]]),
        })
    lines = []
    for session in sessions:
        payload = by_session[session]
        payload["recipes"] = {name: payload["recipes"][name] for name in sorted(payload["recipes"])}
        raw = (json.dumps(payload, separators=(",", ":"), sort_keys=True) + "\n").encode("utf-8")
        (RETURNS / f"{session}.json").write_bytes(raw)
        lines.append(manifest_line({
            "file": f"{session}.json",
            "input_blobs": scored["blobs_by_date"][session],
            "session": session,
            "sha256": hashlib.sha256(raw).hexdigest(),
        }))
    (RETURNS / "manifest.jsonl").write_text("\n".join(lines) + "\n", encoding="utf-8")
    proven = [row["name"] for row in summaries if row["label"] == "something good"]
    verdict = "nothing proven yet" if not proven else "something good: " + ", ".join(proven)
    report = render_report(summaries, sessions, verdict)
    (RETURNS / "REPORT.md").write_text(report, encoding="utf-8")
    return {"summaries": summaries, "verdict": verdict, "sessions": sessions}


def render_report(summaries: list[dict], sessions: list[str], verdict: str) -> str:
    ranked = sorted(
        summaries,
        key=lambda row: (-row["cum_futubull"], -row["full_span_futubull"], row["name"]),
    )
    longs = [row for row in ranked if row["side"] == "long"]

    def n_pass(key: str) -> int:
        return sum(1 for row in summaries if row[key])

    def pct(value) -> str:
        if value is None:
            return ""
        return f"{100.0 * value:.2f}%"

    def under(row: dict) -> str:
        if row["under_3_share"] is None:
            return ""
        return f"{100.0 * row['under_3_share']:.1f}%"

    def line(row: dict, *, rank: int | None = None) -> str:
        cells = []
        if rank is not None:
            cells.append(str(rank))
        cells.extend([
            f"`{row['name']}`",
            row["side"],
            row["label"],
            str(row["n_check_days"]),
            pct(row["cum_futubull"]),
            pct(row["mean_futubull"]),
            pct(row["cum_flat_15bp"]),
            pct(row["mean_flat_15bp"]),
            pct(row["best_removed"]),
            row["best_ticker"] or "",
            pct(row["from_0914_futubull"]),
            under(row),
            f"{row['luck_p_futubull']:.4g}",
        ])
        return "| " + " | ".join(cells) + " |"

    proven_n = sum(
        1 for row in summaries
        if row["line_a"] and row["line_b"] and row["line_c"] and row["line_d"] and row["n_check_days"] >= 10
    )
    lines = [
        "# Group 3 clean-tape score",
        "",
        f"study: {STUDY}",
        "",
        f"Study label: `{STUDY_LABEL}`.",
        "",
        "This is the clean-tape re-score of the 110 Factor Mine recipes. "
        "The labelled study's preregistration was not edited, and this study's "
        "preregistration is the fingerprinted protocol for this run. "
        "No locked ledger was rewritten. `rebuild_match` does not strike.",
        "",
        "The only changed input is the price tape: "
        f"`{CLEAN_BAR_PATH}`, blob `{CLEAN_BAR_BLOB_SHA}`. "
        "Split artefacts are repaired, and unrepairable names are absent. "
        "Feature bars are dated strictly before the session. "
        "Every recipe trades at the 09:30 open, so the session open is the fill and the gap. "
        "The session close is the mark only. The earliest panel row's open and close are not read.",
        "",
        "The candidate list is the earliest `panel.json` blob that contains rows labelled that day. "
        "Actions, AB, and the Finviz export are the earliest blobs, the same pins as the "
        "labelled study. Stock book, join, and catalyst are presence pins.",
        "",
        f"Sessions written: {len(sessions)} ({sessions[0]} through {sessions[-1]}).",
        f"Luck-test denominator: {LUCK_N}. "
        "p is a one-sided t-test that the mean check-day return is above zero, multiplied by 9,500.",
        "",
        f"Line (a) cumulative Futubull check-day return >= 20%: {n_pass('line_a')}",
        f"Line (b) best stock removed still positive: {n_pass('line_b')}",
        f"Line (c) Futubull compound 2026-09-14 through 2026-09-25 >= 0: {n_pass('line_c')}",
        f"Line (d) Futubull luck p < 0.05 on 9,500: {n_pass('line_d')}",
        f"All four lines: {sum(1 for row in summaries if row['line_a'] and row['line_b'] and row['line_c'] and row['line_d'])}",
        f"Proven (all four lines and at least 10 check days): {proven_n}",
        "",
        f"Verdict: `{verdict}`",
        "",
        "## Top 10",
        "",
        "| rank | recipe | side | label | check days | Futubull cum | Futubull mean | 15bp cum | 15bp mean | best removed | best stock | from 09-14 | under $3 | luck p |",
        "| ---: | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: |",
    ]
    for rank, row in enumerate(ranked[:10], start=1):
        lines.append(line(row, rank=rank))
    lines += [
        "",
        "## Top 5 long",
        "",
        "| rank | recipe | side | label | check days | Futubull cum | Futubull mean | 15bp cum | 15bp mean | best removed | best stock | from 09-14 | under $3 | luck p |",
        "| ---: | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: |",
    ]
    for rank, row in enumerate(longs[:5], start=1):
        lines.append(line(row, rank=rank))
    lines += [
        "",
        "## All 110",
        "",
        "| recipe | side | label | search | check | a | b | c | d | Futubull cum | Futubull mean | 15bp cum | 15bp mean | best removed | best stock | from 09-14 | under $3 | luck p |",
        "| --- | --- | --- | ---: | ---: | --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: |",
    ]
    for row in ranked:
        lines.append(
            "| `{name}` | {side} | {label} | {search} | {check} | {a} | {b} | {c} | {d} | {cum} | {mean} | {c15} | {m15} | {removed} | {ticker} | {after} | {under} | {p} |".format(
                name=row["name"],
                side=row["side"],
                label=row["label"],
                search=row["n_search_days"],
                check=row["n_check_days"],
                a="yes" if row["line_a"] else "no",
                b="yes" if row["line_b"] else "no",
                c="yes" if row["line_c"] else "no",
                d="yes" if row["line_d"] else "no",
                cum=pct(row["cum_futubull"]),
                mean=pct(row["mean_futubull"]),
                c15=pct(row["cum_flat_15bp"]),
                m15=pct(row["mean_flat_15bp"]),
                removed=pct(row["best_removed"]),
                ticker=row["best_ticker"] or "",
                after=pct(row["from_0914_futubull"]),
                under=under(row),
                p=f"{row['luck_p_futubull']:.4g}",
            )
        )
    lines.append("")
    return "\n".join(lines)


def main() -> None:
    scored = score_all()
    out = write_record(scored)
    print(out["verdict"], flush=True)


if __name__ == "__main__":
    main()
