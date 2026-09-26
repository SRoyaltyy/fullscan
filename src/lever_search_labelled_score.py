"""Score the 110 labelled Factor Mine recipes.

Reads the fingerprinted prereg and the earliest-commit manifest.
Does not edit either, and does not strike for rebuild_match.
Price features use bars dated strictly before the session.
The session open is the fill. The session close is the mark only.
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import math
import subprocess
from pathlib import Path

from src.factor_mine import matches, pick_day, should_exit
from src.finviz_events import asof_snapshot, events_from_export_fields
from src.lever_search_bars import BAR_BLOB_SHA, BAR_COMMIT, BAR_PATH
from src.lever_search_labelled_append import manifest_line
from src.lever_search_labelled_protocol import (
    CHECK_CALENDAR,
    DESIGNED_AFTER_END,
    DESIGNED_AFTER_START,
    RETURNS,
    RUN_WINDOW,
    SESSIONS,
    STUDY,
    STUDY_LABEL,
    LUCK_N,
    load_manifest,
    role_present,
    search_day_list,
    check_day_list,
)
from src.lever_search_proof import build_group3_recipes, required_roles
from src.lever_search_score import (
    BORROW_DAY,
    CAPITAL,
    Store,
    _ab_map,
    _news_map,
    _polarity,
    _vol_tone,
    best_removed,
    compound,
    fee_15,
    luck_p,
)
from src.paper_trade import load_fees, order_fees

ROOT = Path(__file__).resolve().parents[1]


def _git_blob(sha: str) -> bytes:
    return subprocess.check_output(["git", "cat-file", "-p", sha], cwd=ROOT)


def _pin_ok(row: dict) -> None:
    commit = row["commit"]
    path = row["path"]
    got = subprocess.check_output(
        ["git", "rev-parse", f"{commit}:{path}"],
        cwd=ROOT,
        text=True,
    ).strip()
    if got != row["blob_sha"]:
        raise SystemExit(f"pin blob mismatch {path} {commit}")


def _export_events(blob: bytes, wanted: set[str]) -> dict[str, list[dict]]:
    text = blob.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    names = reader.fieldnames or []
    if "Ticker" not in names:
        return {}
    earn = "Earnings Date" if "Earnings Date" in names else None
    div = "Dividend Ex Date" if "Dividend Ex Date" in names else None
    out: dict[str, list[dict]] = {}
    for rec in reader:
        ticker = str(rec.get("Ticker") or "").strip().upper()
        if ticker not in wanted:
            continue
        rows = events_from_export_fields(
            ticker,
            rec.get(earn) if earn else None,
            rec.get(div) if div else None,
        )
        if rows:
            out[ticker] = rows
    return out


def assemble_row(panel: dict, feat: dict, news: dict[str, str] | None,
                 ab: dict[str, str] | None, snap: dict | None,
                 spy_ret5: float | None) -> dict:
    """One candidate. Panel open and close are not copied."""
    ticker = str(panel.get("ticker") or "").strip().upper()
    boxes = {str(key): value for key, value in dict(panel.get("boxes") or {}).items()}
    boxes["vol"] = _vol_tone(feat)
    boxes["news"] = news.get(ticker, "missing") if news is not None else "missing"
    boxes["ab"] = ab.get(ticker, "missing") if ab is not None else "missing"
    ret5 = feat.get("ret_5")
    rs = None
    if spy_ret5 is not None and ret5 is not None:
        rs = float(ret5) - spy_ret5
    if snap is None:
        earn = False
        days_e = None
        days_r = None
        flag_e = 0
        flag_r = 0
    else:
        earn = bool(snap.get("earn_react"))
        days_e = snap.get("days_since_E")
        days_r = snap.get("days_since_R")
        flag_e = snap.get("flag_E")
        flag_r = snap.get("flag_R")
    good = sum(1 for key, value in boxes.items() if key != "yday" and value == "good")
    bad = sum(1 for key, value in boxes.items() if key != "yday" and value == "bad")
    sources = [str(item) for item in (panel.get("sources") or [])]
    row = {
        "date": panel.get("date"),
        "ticker": ticker,
        "sources": sources,
        "src_rank": panel.get("src_rank"),
        "boxes": boxes,
        "blue": bool(panel.get("blue")),
        "alarm": bool(panel.get("alarm")),
        "zero_red": bool(panel.get("zero_red")),
        "cond_good": good,
        "cond_bad": bad,
        "ohlc_ret_1": feat.get("ret_1"),
        "ohlc_ret_5": ret5,
        "ohlc_ret_10": feat.get("ret_10"),
        "ohlc_rvol": feat.get("rvol"),
        "ohlc_hot_score": feat.get("hot_score"),
        "ohlc_break_10": bool(feat.get("break_10")),
        "last_green": bool(feat.get("last_green")),
        "last_red": bool(feat.get("last_red")),
        "candle_score": feat.get("candle_score") or 0.0,
        "candle_capture": bool(feat.get("candle_capture")),
        "erd_earn_react": earn,
        "erd_days_since_E": days_e,
        "erd_days_since_R": days_r,
        "erd_flag_E": flag_e,
        "erd_flag_R": flag_r,
        "rs_week": rs,
    }
    if "open" in row or "close" in row:
        raise SystemExit("panel open/close leaked into a candidate row")
    return row


def _ab_tone(checklist: bytes, enriched: bytes) -> dict[str, str]:
    scores = _ab_map(enriched)
    common = set(_ab_map(checklist)) & set(scores)
    return {
        ticker: _polarity(math.tanh(scores[ticker] / 8.0))
        for ticker in common
    }


def build_session(store: Store, index: dict, session: str,
                  caches: dict) -> tuple[list[dict], list[dict]]:
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

    spy = store.feature("SPY", session)
    spy_ret5 = float(spy["ret_5"]) if spy.get("ok") and spy.get("ret_5") is not None else None
    rows = []
    for panel in raw_rows:
        ticker = str(panel.get("ticker") or "").strip().upper()
        feat = store.feature(ticker, session)
        snap = None
        if export_index is not None:
            snap = asof_snapshot(export_index.get(ticker) or [], session)
        rows.append(assemble_row(panel, feat, news, ab, snap, spy_ret5))

    blobs = [{"path": BAR_PATH, "blob_sha": BAR_BLOB_SHA, "commit": BAR_COMMIT}]
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


def _mark(cash: float, pos: dict, session: str, store: Store, side: str) -> float:
    stock = 0.0
    for lot in pos.values():
        px = store.session_close(lot["ticker"], session)
        if px is None:
            px = lot.get("last_px")
        if px is None:
            continue
        notional = lot["shares"] * float(px)
        stock += notional if side == "long" else -notional
    return cash + stock


def _open_stock(pos: dict, session: str, store: Store, side: str) -> float:
    stock = 0.0
    for lot in pos.values():
        px = store.session_open(lot["ticker"], session)
        if px is None:
            px = lot.get("last_px")
        if px is None:
            continue
        notional = lot["shares"] * float(px)
        stock += notional if side == "long" else -notional
    return stock


def walk_recipe(recipe: dict, sessions: list[str], rows_by_date: dict[str, list[dict]],
                store: Store, search_ok: dict[str, bool], fees: dict) -> dict:
    """One $10k book. Share counts follow Futubull fees. 15 bp reprices those fills."""
    cash_f = CAPITAL
    cash_15 = CAPITAL
    pos: dict[str, dict] = {}
    side = recipe.get("side") or "long"
    hold = int(recipe["hold"])
    days = []
    ticker_pnl: dict[str, dict[str, float]] = {}
    first_entry: dict[str, str] = {}
    prev_eq_f = CAPITAL
    prev_eq_15 = CAPITAL
    under = 0
    fills_n = 0
    for session in sessions:
        sat = not search_ok.get(session, False)
        day_rows = rows_by_date.get(session) or []
        chosen = [] if sat else pick_day(day_rows, recipe)
        if chosen and not all(matches(row, recipe) for row in chosen):
            raise SystemExit(f"pick_day leaked a non-match {recipe['name']} {session}")
        by_ticker = {row["ticker"]: row for row in day_rows}
        sold = []
        bought = []
        for ticker in list(pos):
            lot = pos[ticker]
            held = sessions.index(session) - sessions.index(lot["entry"])
            row = by_ticker.get(ticker) or {}
            early = should_exit(row, recipe.get("exit_when"))
            if not (early or held >= hold):
                continue
            px = store.session_open(ticker, session)
            if px is None:
                continue
            shares = lot["shares"]
            fee_f = order_fees(shares, px, "sell" if side == "long" else "buy", fees)
            fee_b = fee_15(shares, px)
            prev_px = float(lot.get("prev_mark", lot["entry_px"]))
            if side == "long":
                cash_f += shares * px - fee_f
                cash_15 += shares * px - fee_b
                pnl = shares * (px - prev_px) - fee_f
            else:
                cash_f -= shares * px + fee_f
                cash_15 -= shares * px + fee_b
                pnl = shares * (prev_px - px) - fee_f
            ticker_pnl.setdefault(ticker, {})
            ticker_pnl[ticker][session] = ticker_pnl[ticker].get(session, 0.0) + pnl
            pos.pop(ticker)
            price = round(float(px), 4)
            sold.append({
                "ticker": ticker,
                "side": "SELL" if side == "long" else "COVER",
                "shares": shares,
                "price": price,
            })
            if price < 3.0:
                under += 1
            fills_n += 1
        new = [row for row in chosen if row["ticker"] not in pos]
        if new and (cash_f > 0 or side == "short"):
            room = cash_f if side == "long" else max(
                0.0, (cash_f + _open_stock(pos, session, store, side)) * 0.5
            )
            per = room / len(new) if new else 0.0
            for row in new:
                ticker = row["ticker"]
                px = store.session_open(ticker, session)
                if px is None:
                    continue
                shares = int(per // px) if px else 0
                if shares < 1:
                    continue
                fee_f = order_fees(shares, px, "buy" if side == "long" else "sell", fees)
                fee_b = fee_15(shares, px)
                if side == "long":
                    cost = shares * px + fee_f
                    if cost > cash_f + 1e-6:
                        shares = int((cash_f - fee_f) // px) if px else 0
                        if shares < 1:
                            continue
                        fee_f = order_fees(shares, px, "buy", fees)
                        fee_b = fee_15(shares, px)
                        cost = shares * px + fee_f
                    cash_f -= cost
                    cash_15 -= shares * px + fee_b
                    lot = {
                        "ticker": ticker, "shares": shares, "entry_px": px, "entry": session,
                        "fee_in_f": fee_f, "notional": shares * px, "last_px": px,
                    }
                else:
                    notional = shares * px
                    fee_f = order_fees(shares, px, "sell", fees)
                    cash_f += notional - fee_f
                    cash_15 += notional - fee_b
                    lot = {
                        "ticker": ticker, "shares": shares, "entry_px": px, "entry": session,
                        "fee_in_f": fee_f, "notional": notional, "last_px": px,
                    }
                pos[ticker] = lot
                first_entry.setdefault(ticker, session)
                price = round(float(px), 4)
                bought.append({
                    "ticker": ticker,
                    "side": "BUY" if side == "long" else "SHORT",
                    "shares": shares,
                    "price": price,
                })
                if price < 3.0:
                    under += 1
                fills_n += 1
        for lot in list(pos.values()):
            close = store.session_close(lot["ticker"], session)
            if close is not None:
                lot["last_px"] = close
            prev_px = float(lot.get("prev_mark", lot["entry_px"]))
            mark_px = float(lot.get("last_px") or prev_px)
            borrow = lot["shares"] * mark_px * BORROW_DAY if side == "short" else 0.0
            if borrow:
                cash_f -= borrow
                cash_15 -= borrow
            if side == "long":
                mtm = lot["shares"] * (mark_px - prev_px)
            else:
                mtm = lot["shares"] * (prev_px - mark_px) - borrow
            if lot["entry"] == session:
                mtm -= lot["fee_in_f"]
            ticker_pnl.setdefault(lot["ticker"], {})
            ticker_pnl[lot["ticker"]][session] = ticker_pnl[lot["ticker"]].get(session, 0.0) + mtm
            lot["prev_mark"] = mark_px
        eq_f = _mark(cash_f, pos, session, store, side)
        eq_15 = _mark(cash_15, pos, session, store, side)
        ret_f = (eq_f / prev_eq_f - 1.0) if prev_eq_f else 0.0
        ret_15 = (eq_15 / prev_eq_15 - 1.0) if prev_eq_15 else 0.0
        day_fills = sold + bought
        day_under = sum(1 for fill in day_fills if fill["price"] < 3.0)
        days.append({
            "session": session,
            "sat_out": sat,
            "picks": [row["ticker"] for row in chosen],
            "fills": day_fills,
            "ret_futubull": ret_f,
            "ret_flat_15bp": ret_15,
            "equity_futubull": eq_f,
            "equity_flat_15bp": eq_15,
            "under_3_share": (day_under / len(day_fills)) if day_fills else None,
            "n_fills": len(day_fills),
        })
        prev_eq_f = eq_f
        prev_eq_15 = eq_15
    return {
        "days": days,
        "ticker_pnl": ticker_pnl,
        "first_entry": first_entry,
        "n_fills": fills_n,
        "n_under_3": under,
    }


def _round(value, digits: int):
    if value is None:
        return None
    return round(float(value), digits)


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
        ["git", "rev-parse", f"{BAR_COMMIT}:{BAR_PATH}"],
        cwd=ROOT,
        text=True,
    ).strip()
    if bar_blob != BAR_BLOB_SHA:
        raise SystemExit("pinned bar blob mismatch")
    print("loading bars", flush=True)
    store = Store()
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
        "# Group 3 labelled score",
        "",
        f"study: {STUDY}",
        "",
        f"Study label: `{STUDY_LABEL}`.",
        "",
        "This is the labelled score of the 110 Factor Mine recipes. "
        "The preregistration was not edited. No locked ledger was rewritten. "
        "`rebuild_match` does not strike.",
        "",
        "Price features are the frozen engine on `data/prices/ohlc.parquet`, "
        f"blob `{BAR_BLOB_SHA}`. "
        "Feature bars are dated strictly before the session. "
        "Every recipe trades at the 09:30 open, so the session open is the fill and the gap. "
        "The session close is the mark only. The earliest panel row's open and close are not read.",
        "",
        "The candidate list is the earliest `panel.json` blob that contains rows labelled that day. "
        "An earlier version exists for every session in the window, so the per-day source-file "
        "candidate list is not used. Actions, AB, and the Finviz export are the earliest blobs. "
        "Stock book, join, and catalyst are presence pins.",
        "",
        f"Sessions written: {len(sessions)} ({sessions[0]} through {sessions[-1]}).",
        f"Luck-test denominator: {LUCK_N}. "
        "p is a one-sided t-test that the mean check-day return is above zero, multiplied by 9,390.",
        "",
        f"Line (a) cumulative Futubull check-day return >= 20%: {n_pass('line_a')}",
        f"Line (b) best stock removed still positive: {n_pass('line_b')}",
        f"Line (c) Futubull compound 2026-09-14 through 2026-09-25 >= 0: {n_pass('line_c')}",
        f"Line (d) Futubull luck p < 0.05 on 9,390: {n_pass('line_d')}",
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
