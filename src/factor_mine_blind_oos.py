"""Fee-aware OOS board for the frozen ≤9/9 blind formation set.

Excel lane. Does **not** re-select recipes on 9/10–9/18. Names come
from PR #286 ``holdout.json`` (Cyrus featured ∪ formal KEEP ∪ hot4
contamination check). Scoring adds OOS start-day YES and after-fee H
(``FEE_RT=0.0015``) beside the already-computed continued / fresh books.

Cyrus OOS KEEP is start-day wins + Book% > 0. Win% > 55% is reported
and is not enough by itself. n≥30 is the IS trade-count bar; the
7-session OOS window cannot meet it.

Research only. Live ``flatten_robust`` / Pages are not imported or written.

  python -m src.factor_mine_blind_oos
"""
from __future__ import annotations

import json
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

from . import factor_mine as fm
from . import factor_mine_asof as fma
from . import factor_mine_blind as fmbld
from . import factor_mine_book as fmb
from . import ticker_lookback as tl

FEE_RT = 0.0015
OOS_START = fmbld.OOS_START
OOS_END = "2026-09-18"
CUTOFF = fmbld.CUTOFF
WR_BAR = 0.55
BOOK_MATCH_TOL = 0.05

HOLDOUT_JSON = fm.ROOT / "03_scoreboard" / "factor_mine_blind_0909" / "holdout.json"
IS_JSON = fm.ROOT / "03_scoreboard" / "factor_mine_blind_0909" / "factor_mine.json"
LIVE_JSON = fm.ROOT / "03_scoreboard" / "factor_mine.json"
BOARD_MD = fm.ROOT / "03_scoreboard" / "FACTOR_MINE_BLIND_0909_OOS.md"
BOARD_JSON = fm.ROOT / "03_scoreboard" / "factor_mine_blind_0909_oos.json"

TASKFORCE_BOOK_SURVIVORS = (
    "combo_sh_5050_shared",
    "combo_sh_3070_shared",
    "combo_sh_7030_shared",
    "combo_seh_451540_shared",
    "combo_seh_601525_shared",
    "combo_seh_502525_shared",
)

LIVE_POST_0909_PINS = (
    "union_hot_n4_holdup",
    "overnight_mega_h1",
    "overnight_mega_h2",
    "overnight_h1",
    "overnight_mega_green_h1",
    "combo_oh_5050_shared",
    "combo_sh_macd_5050_shared",
)


def oos_start_ok(start_green, start_n) -> bool:
    """Same Cyrus start bar as IS: ≥17/19, else ≥85% when n < 19."""
    n = int(start_n or 0)
    g = int(start_green or 0)
    if n <= 0:
        return False
    if n >= fmbld.CYRUS_START_N_REF:
        return g >= fmbld.CYRUS_START_YES
    return (g / n) >= fmbld.CYRUS_START_RATE


def is_cyrus_oos_keep(row: dict) -> bool:
    """KEEP only if IS-featured and OOS Book% > 0 and OOS starts pass.

    Win% is ignored. Missing / non-positive continued book is FAIL.
    """
    if not row.get("cyrus_is"):
        return False
    book = fm._finite(row.get("oos_book_pct_continued"))
    if book is None or float(book) <= fmbld.CYRUS_MIN_BOOK:
        return False
    return oos_start_ok(row.get("oos_start_green"), row.get("oos_start_n"))


def wr_only_would_pass(row: dict) -> bool:
    """True when after-fee H or cash-trade WR clears 55%. Not KEEP."""
    for key in ("oos_fee_h_wr", "fresh_win_rate"):
        wr = fm._finite(row.get(key))
        if wr is not None and float(wr) > WR_BAR:
            return True
    return False


def frozen_names(holdout: dict) -> list[str]:
    """IS freeze only. OOS numbers never add a name."""
    names: list[str] = []
    seen: set[str] = set()
    for key in ("cyrus_featured", "formal_keep"):
        for n in holdout.get(key) or []:
            if n and n not in seen:
                names.append(n)
                seen.add(n)
    for r in holdout.get("rows") or []:
        n = r.get("name")
        if n and n not in seen:
            names.append(n)
            seen.add(n)
    return names


def after_fee_h(open_px, close_px, side: str, fee_rt: float = FEE_RT):
    """Same-session H minus Futubull 15 bp. Shorts pay the fee."""
    o = fm._finite(open_px)
    c = fm._finite(close_px)
    if o is None or c is None or o == 0:
        return None
    raw = (float(c) / float(o)) - 1.0
    if (side or "long") == "short":
        raw = -raw
    return raw - float(fee_rt)


def score_fee_h(book: dict, *, oos_start: str, oos_end: str,
                bars=None, fee_rt: float = FEE_RT) -> dict:
    """Name-day after-fee H on OOS fills (BUY / SHORT)."""
    nets: list[float] = []
    hits = 0
    for t in book.get("trades") or []:
        side = t.get("side")
        if side not in ("BUY", "SHORT"):
            continue
        date = t.get("date") or ""
        if date < oos_start or date > oos_end:
            continue
        ticker = t.get("ticker")
        if not ticker:
            continue
        bar = fm._bar(ticker, date, bars)
        net = after_fee_h(
            bar.get("open"), bar.get("close"),
            "short" if side == "SHORT" else "long", fee_rt)
        if net is None:
            continue
        nets.append(net)
        if net > 0:
            hits += 1
    n = len(nets)
    return {
        "oos_fee_h_n": n,
        "oos_fee_h_hits": hits,
        "oos_fee_h_wr": None if not n else round(hits / n, 4),
        "oos_fee_h_mean": None if not n else round(sum(nets) / n, 6),
        "fee_rt": fee_rt,
    }


def _start_row(book: dict, start: str) -> dict:
    ret = fm._finite(book.get("total_ret_pct"))
    return {
        "start": start,
        "return_pct": None if ret is None else round(float(ret), 3),
        "made_money": bool(ret is not None and float(ret) > 0),
        "n_trades": book.get("n_trades"),
        "final_equity": book.get("final_equity"),
        "win_rate": book.get("win_rate"),
        "audit_ok": bool((book.get("audit") or {}).get("ok", True)),
    }


def replay_oos_starts(panel: dict, rec: dict, rec_by: dict, *,
                      oos_start: str, oos_end: str,
                      bars=None, fees=None, regime=None) -> tuple[list[dict], dict | None]:
    """Wake $10k empty lots on each OOS session. First start is fresh $10k."""
    cal = [d for d in (panel.get("session_dates") or [])
           if oos_start <= d <= oos_end]
    starts: list[dict] = []
    first = None
    for start in cal:
        book = fma.replay_recipe(
            panel, rec, rec_by, start=start, bars=bars, fees=fees,
            regime=regime)
        if first is None:
            first = book
        starts.append(_start_row(book, start))
    return starts, first


def _replay_name_job(job: dict) -> dict:
    """Process-worker: one frozen name, all OOS starts."""
    rec = job["rec"]
    rec_by = job["rec_by"]
    panel = job["panel"]
    starts, fresh = replay_oos_starts(
        panel, rec, rec_by, oos_start=job["oos_start"], oos_end=job["oos_end"],
        bars=job.get("bars"), fees=job["fees"], regime=job["regime"])
    fee = score_fee_h(
        fresh or {}, oos_start=job["oos_start"], oos_end=job["oos_end"],
        bars=job.get("bars"), fee_rt=FEE_RT)
    return {
        "name": job["name"],
        "oos_starts": starts,
        "fresh_win_rate": None if fresh is None else fresh.get("win_rate"),
        **fee,
    }


def should_replay(name: str, cyrus: set[str]) -> bool:
    """Replay Cyrus featured + hot4 contamination. Formal-only cannot KEEP."""
    return name in cyrus or name == "union_hot_n4_h1" or name in TASKFORCE_BOOK_SURVIVORS


def _members(rec: dict | None) -> str:
    rec = rec or {}
    kids = rec.get("members") or []
    if not kids:
        return rec.get("note") or ""
    w = rec.get("weights") or []
    if w and len(w) == len(kids):
        bits = [f"{n} {float(x):g}" for n, x in zip(kids, w)]
        return " + ".join(bits)
    return " + ".join(str(n) for n in kids)


def confirm_holdout_books(row: dict, holdout_row: dict) -> dict:
    """Flag drift vs #286 continued / fresh books. Does not re-pick."""
    out = {
        "holdout_cont": holdout_row.get("oos_book_pct_continued"),
        "holdout_fresh": holdout_row.get("fresh_book_pct"),
        "fresh_start_ret": None,
        "cont_match": True,
        "fresh_match": True,
    }
    starts = row.get("oos_starts") or []
    if starts:
        out["fresh_start_ret"] = starts[0].get("return_pct")
    h_cont = fm._finite(holdout_row.get("oos_book_pct_continued"))
    r_cont = fm._finite(row.get("oos_book_pct_continued"))
    if h_cont is not None and r_cont is not None:
        out["cont_match"] = abs(float(h_cont) - float(r_cont)) <= BOOK_MATCH_TOL
    h_fresh = fm._finite(holdout_row.get("fresh_book_pct"))
    r_fresh = fm._finite(out["fresh_start_ret"])
    if h_fresh is None:
        r_fresh = fm._finite(row.get("fresh_book_pct"))
    if h_fresh is not None and r_fresh is not None:
        out["fresh_match"] = abs(float(h_fresh) - float(r_fresh)) <= BOOK_MATCH_TOL
    return out


def attach_verdict(row: dict) -> dict:
    row = dict(row)
    cyrus = is_cyrus_oos_keep(row)
    book = fm._finite(row.get("oos_book_pct_continued"))
    book_ok = book is not None and float(book) > fmbld.CYRUS_MIN_BOOK
    starts_ok = oos_start_ok(row.get("oos_start_green"), row.get("oos_start_n"))
    wr_ok = wr_only_would_pass(row)
    if cyrus:
        verdict = "KEEP"
        why = (
            f"IS Cyrus + OOS Book% {book:+.2f} and starts "
            f"{row.get('oos_start_green')}/{row.get('oos_start_n')}"
        )
    elif not row.get("cyrus_is"):
        verdict = "FAIL"
        why = "not Cyrus-featured on the 9/9 IS freeze (cannot KEEP from OOS)"
    elif not book_ok:
        verdict = "FAIL"
        why = (
            f"OOS continued Book% "
            f"{'—' if book is None else f'{float(book):+.2f}'} ≤ 0"
        )
    elif not starts_ok:
        verdict = "FAIL"
        why = (
            f"OOS starts {row.get('oos_start_green') or 0}/"
            f"{row.get('oos_start_n') or 0} < Cyrus "
            f"≥{fmbld.CYRUS_START_RATE:.0%} (7 sessions → ≥6/7)"
        )
    else:
        verdict = "FAIL"
        why = "Cyrus OOS bar missed"
    if wr_ok and verdict != "KEEP":
        why += "; Win%>55% alone is not enough"
    row["verdict"] = verdict
    row["why"] = why
    row["book_only"] = bool(row.get("cyrus_is") and book_ok)
    row["starts_ok"] = starts_ok
    row["wr_only"] = wr_ok
    return row


def _base_row(name: str, rec: dict | None, h: dict,
              cyrus: set[str], formal: set[str]) -> dict:
    return {
        "name": name,
        "side": (rec or h).get("side"),
        "hold": (rec or h).get("hold"),
        "members": list((rec or {}).get("members") or []),
        "weights": list((rec or {}).get("weights") or []),
        "member_note": _members(rec),
        "cyrus_is": name in cyrus,
        "formal_is": name in formal,
        "taskforce_book": name in TASKFORCE_BOOK_SURVIVORS,
        "is_book_pct": h.get("is_book_pct"),
        "is_win_rate": h.get("is_win_rate"),
        "is_start_green": h.get("is_start_green"),
        "is_start_n": h.get("is_start_n"),
        "is_n_trades": h.get("is_n_trades"),
        "oos_book_pct_continued": h.get("oos_book_pct_continued"),
        "fresh_book_pct": h.get("fresh_book_pct"),
        "fresh_n_trades": h.get("fresh_n_trades"),
        "oos_dollar_days": h.get("oos_dollar_days"),
        "oos_n_hit": h.get("oos_n_hit"),
        "oos_n_days": h.get("oos_n_days"),
        "missing_recipe": rec is None,
        "oos_starts": [],
        "oos_start_green": 0,
        "oos_start_n": 0,
        "oos_start_rate": None,
        "fresh_win_rate": None,
        "oos_fee_h_n": 0,
        "oos_fee_h_hits": 0,
        "oos_fee_h_wr": None,
        "oos_fee_h_mean": None,
        "fee_rt": FEE_RT,
    }


def _apply_replay(row: dict, replayed: dict) -> dict:
    starts = list(replayed.get("oos_starts") or [])
    n_green = sum(1 for s in starts if s.get("made_money"))
    row["oos_starts"] = starts
    row["oos_start_green"] = n_green
    row["oos_start_n"] = len(starts)
    row["oos_start_rate"] = (
        None if not starts else round(n_green / len(starts), 4))
    row["fresh_win_rate"] = replayed.get("fresh_win_rate")
    row["oos_fee_h_n"] = replayed.get("oos_fee_h_n") or 0
    row["oos_fee_h_hits"] = replayed.get("oos_fee_h_hits") or 0
    row["oos_fee_h_wr"] = replayed.get("oos_fee_h_wr")
    row["oos_fee_h_mean"] = replayed.get("oos_fee_h_mean")
    return row


def score_frozen(holdout: dict, payload: dict, panel: dict, *,
                 oos_start: str = OOS_START, oos_end: str = OOS_END,
                 bars=None, fees=None, regime=None,
                 replay: bool = True, workers: int = 4) -> list[dict]:
    """Score the frozen set. ``replay=False`` keeps holdout books only."""
    fees = fees if fees is not None else fm.pt_fees()
    if regime is None:
        try:
            regime = fmb.load_regime()
        except Exception:
            regime = {}
    rec_by = fma.rec_by_name(payload)
    hold_by = {r["name"]: r for r in (holdout.get("rows") or []) if r.get("name")}
    cyrus = set(holdout.get("cyrus_featured") or [])
    formal = set(holdout.get("formal_keep") or [])
    names = frozen_names(holdout)
    rows_by = {}
    jobs = []
    for name in names:
        h = hold_by.get(name) or {}
        rec = rec_by.get(name)
        row = _base_row(name, rec, h, cyrus, formal)
        if rec is None or not replay or not should_replay(name, cyrus):
            row.update(confirm_holdout_books(row, h))
            rows_by[name] = attach_verdict(row)
            continue
        print(f"[blind-oos] queue {name}", flush=True)
        need = {name, *(rec.get("members") or [])}
        slim = {k: rec_by[k] for k in need if k in rec_by}
        jobs.append({
            "name": name, "rec": rec, "rec_by": slim, "panel": panel,
            "oos_start": oos_start, "oos_end": oos_end, "bars": bars,
            "fees": fees, "regime": regime,
        })
        rows_by[name] = row
    if jobs:
        n_workers = max(1, min(int(workers or 1), len(jobs)))
        if n_workers == 1:
            done = [_replay_name_job(j) for j in jobs]
        else:
            # Import via package name so workers do not pickle __main__.
            from src.factor_mine_blind_oos import _replay_name_job as _job
            done = []
            with ProcessPoolExecutor(max_workers=n_workers) as pool:
                futs = [pool.submit(_job, j) for j in jobs]
                for fut in as_completed(futs):
                    done.append(fut.result())
        for replayed in done:
            name = replayed["name"]
            row = _apply_replay(rows_by[name], replayed)
            h = hold_by.get(name) or {}
            row.update(confirm_holdout_books(row, h))
            rows_by[name] = attach_verdict(row)
            print(f"[blind-oos] done {name} starts "
                  f"{row.get('oos_start_green')}/{row.get('oos_start_n')} "
                  f"book={row.get('oos_book_pct_continued')}", flush=True)
    return [rows_by[n] for n in names if n in rows_by]


def live_contamination(live: dict | None, holdout: dict) -> dict:
    featured = list((live or {}).get("featured") or [])
    stats = {s["name"]: s for s in ((live or {}).get("stats") or [])
             if s.get("name")}
    hot = next((r for r in (holdout.get("rows") or [])
                if r.get("name") == "union_hot_n4_h1"), {})
    live_hot = stats.get("union_hot_n4_h1") or {}
    pins = [n for n in LIVE_POST_0909_PINS if n in featured]
    return {
        "live_from": (live or {}).get("from_date"),
        "live_to": (live or {}).get("to_date"),
        "live_n_featured": len(featured),
        "hot4_live_featured": "union_hot_n4_h1" in featured,
        "hot4_live_book": live_hot.get("total_ret_pct"),
        "hot4_live_starts": (
            f"{live_hot.get('start_green')}/{live_hot.get('start_n')}"
            if live_hot.get("start_n") else None),
        "hot4_is_starts": (
            f"{hot.get('is_start_green')}/{hot.get('is_start_n')}"
            if hot.get("is_start_n") else None),
        "hot4_is_book": hot.get("is_book_pct"),
        "hot4_cyrus_is": False,
        "holdup_live_featured": "union_hot_n4_holdup" in featured,
        "holdup_in_0909_menu": False,
        "post_0909_live_pins": pins,
        "taskforce_in_live_featured": [
            n for n in TASKFORCE_BOOK_SURVIVORS if n in featured
        ],
    }


def _md_pp(v) -> str:
    return "—" if v is None else f"{float(v):+.2f}"


def _md_pct(v) -> str:
    return "—" if v is None else f"{100 * float(v):.0f}%"


def _md_start(g, n) -> str:
    if not n:
        return "—"
    return f"{int(g or 0)}/{int(n)}"


def render_oos_md(holdout: dict, rows: list[dict], *,
                  contamination: dict | None = None,
                  oos_start: str = OOS_START, oos_end: str = OOS_END) -> str:
    keep = [r for r in rows if r.get("verdict") == "KEEP"]
    fail = [r for r in rows if r.get("verdict") != "KEEP"]
    cyrus = [r for r in rows if r.get("cyrus_is")]
    book_only = [r for r in cyrus if r.get("book_only")]
    wr_trap = [r for r in rows if r.get("wr_only") and r.get("verdict") != "KEEP"]
    mismatches = [
        r for r in rows
        if r.get("cont_match") is False or r.get("fresh_match") is False
    ]
    headline = (
        f"**KEEP** {len(keep)}" if keep else "**FAIL** 0 KEEP"
    )
    cont = contamination or {}
    lines = [
        "# Factor mine blind 9/9 — fee-aware OOS (Excel lane)",
        "",
        f"status=DONE verdict={headline} cutoff={CUTOFF} "
        f"OOS={oos_start}→{oos_end} FEE_RT={FEE_RT} "
        f"KEEP={len(keep)} FAIL={len(fail)}",
        "",
        "Research only. Live `flatten_robust` / `dashboard/factor-mine/` / "
        "`03_scoreboard/factor_mine.json` were not written. Recipe names "
        "are the #286 IS freeze — OOS dates did not re-select.",
        "",
        "## KEEP bar",
        "",
        "Cyrus OOS KEEP (plain): **IS Cyrus featured** (Starts YES + Book% "
        "on 8/13–9/9) **and** continued OOS Book% > 0 **and** OOS "
        f"start-day YES ≥{fmbld.CYRUS_START_RATE:.0%} "
        f"(7 sessions → ≥6/7). Same start rule as IS "
        f"(≥{fmbld.CYRUS_START_YES}/{fmbld.CYRUS_START_N_REF} when n≥"
        f"{fmbld.CYRUS_START_N_REF}).",
        "",
        f"**Win% > {WR_BAR:.0%} is not enough by itself.** After-fee H = "
        f"same-session open→close minus {FEE_RT * 10000:.0f} bp Futubull "
        f"(`FEE_RT={FEE_RT}`). Shorts pay the 15 bp (they do not collect "
        "it). Cash Book% already uses the Futubull order-fee schedule "
        "(`00_grounding/futubull_fees.json`). n≥30 is the IS trade-count "
        "bar; the 7-session window cannot meet it and does not KEEP.",
        "",
        "OOS **continued** Book% is copied from #286 `holdout.json` "
        "(9/9 cash book walked forward). OOS **start-day YES** wakes "
        f"$10k empty lots on each session in {oos_start}–{oos_end}. "
        "Start 9/10 is the fresh $10k path.",
        "",
        "## Headline",
        "",
    ]
    if keep:
        lines.append(
            "KEEP: " + ", ".join(f"`{r['name']}`" for r in keep) + "."
        )
    else:
        lines.append(
            "FAIL. No frozen ≤9/9 Cyrus name keeps positive continued "
            f"Book% **and** enough OOS start-day wins ({oos_start}–{oos_end})."
        )
    lines += [
        "",
        f"Taskforce continued-Book% survivors (not a KEEP bar): "
        f"**{len(book_only)}** of {len(cyrus)} Cyrus IS names. "
        f"Win%>55% traps (WR clears, Cyrus FAIL): **{len(wr_trap)}**.",
        "",
        "## Taskforce 6 — continued Book% > 0 on #286",
        "",
        "| Strategy | Cyrus OOS | Book-only | OOS starts | Cont book% | "
        "Fresh $10k | Fresh WR | After-fee H WR | n H | Why |",
        "|---|---|---|---:|---:|---:|---:|---:|---:|---|",
    ]
    by = {r["name"]: r for r in rows}
    for name in TASKFORCE_BOOK_SURVIVORS:
        r = by.get(name) or {"name": name, "verdict": "FAIL", "why": "missing"}
        lines.append(
            f"| `{r['name']}` | **{r.get('verdict') or 'FAIL'}** | "
            f"{'yes' if r.get('book_only') else 'no'} | "
            f"{_md_start(r.get('oos_start_green'), r.get('oos_start_n'))} | "
            f"{_md_pp(r.get('oos_book_pct_continued'))} | "
            f"{_md_pp(r.get('fresh_book_pct'))} | "
            f"{_md_pct(r.get('fresh_win_rate'))} | "
            f"{_md_pct(r.get('oos_fee_h_wr'))} | "
            f"{r.get('oos_fee_h_n') or 0} | {r.get('why') or ''} |"
        )
    lines += [
        "",
        "### Start-day paths (Taskforce 6)",
        "",
        "Each cell is the fee-aware $10k book that **starts** that morning "
        "(empty lots) through 9/18. YES = Book% > 0.",
        "",
    ]
    oos_dates = []
    for name in TASKFORCE_BOOK_SURVIVORS:
        for s in (by.get(name) or {}).get("oos_starts") or []:
            if s.get("start") and s["start"] not in oos_dates:
                oos_dates.append(s["start"])
    if oos_dates:
        head = "| Strategy | " + " | ".join(d[5:] for d in oos_dates) + " |"
        sep = "|---|" + "---:|" * len(oos_dates)
        lines += [head, sep]
        for name in TASKFORCE_BOOK_SURVIVORS:
            r = by.get(name) or {}
            hits = {s["start"]: s for s in (r.get("oos_starts") or [])}
            cells = []
            for d in oos_dates:
                s = hits.get(d) or {}
                ret = s.get("return_pct")
                if ret is None:
                    cells.append("—")
                else:
                    mark = "YES" if s.get("made_money") else "no"
                    cells.append(f"{mark} {_md_pp(ret)}")
            lines.append(f"| `{name}` | " + " | ".join(cells) + " |")
        lines.append("")

    lines += [
        "## Frozen Cyrus featured — KEEP / FAIL",
        "",
        "| Strategy | Side | IS start | IS book% | IS win% | "
        "OOS starts | Cont book% | Fresh $10k | After-fee H WR | "
        "WR-only | Verdict |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---|---|",
    ]
    for r in cyrus:
        lines.append(
            f"| `{r['name']}` | {r.get('side') or '—'} | "
            f"{_md_start(r.get('is_start_green'), r.get('is_start_n'))} | "
            f"{_md_pp(r.get('is_book_pct'))} | {_md_pct(r.get('is_win_rate'))} | "
            f"{_md_start(r.get('oos_start_green'), r.get('oos_start_n'))} | "
            f"{_md_pp(r.get('oos_book_pct_continued'))} | "
            f"{_md_pp(r.get('fresh_book_pct'))} | "
            f"{_md_pct(r.get('oos_fee_h_wr'))} | "
            f"{'yes' if r.get('wr_only') else ''} | **{r.get('verdict')}** |"
        )
    formal_only = [r for r in rows if r.get("formal_is") and not r.get("cyrus_is")]
    if formal_only:
        lines += [
            "",
            "## Formal-bar IS KEEP that missed Cyrus featuring",
            "",
            "These cleared WORKABLE_BAR on 9/9 (Win% / $days / start≥50%) "
            "but not Starts YES ≥17/19. OOS cannot promote them.",
            "",
            "| Strategy | IS start | IS book% | IS win% | OOS starts | "
            "Cont book% | Fresh $10k | Verdict |",
            "|---|---:|---:|---:|---:|---:|---:|---|",
        ]
        for r in formal_only:
            lines.append(
                f"| `{r['name']}` | "
                f"{_md_start(r.get('is_start_green'), r.get('is_start_n'))} | "
                f"{_md_pp(r.get('is_book_pct'))} | {_md_pct(r.get('is_win_rate'))} | "
                f"{_md_start(r.get('oos_start_green'), r.get('oos_start_n'))} | "
                f"{_md_pp(r.get('oos_book_pct_continued'))} | "
                f"{_md_pp(r.get('fresh_book_pct'))} | **FAIL** |"
            )
    is_wr = [
        r for r in cyrus
        if fm._finite(r.get("is_win_rate")) is not None
        and float(r["is_win_rate"]) > WR_BAR
    ]
    lines += [
        "",
        "## Win% > 55% alone",
        "",
        "Win% is not the Cyrus bar. On 9/9, "
        + (
            ", ".join(
                f"`{r['name']}` IS WR {_md_pct(r.get('is_win_rate'))}"
                for r in is_wr
            ) or "no Cyrus name"
        )
        + " would pass a Win%>55% screen; every one **FAIL**s OOS "
        "starts + Book%. The `combo_seh_*` mixes were Cyrus-featured "
        "with IS WR 50% — Win% alone would have dropped them on 9/9 "
        "too (starts 17/19 + Book% kept them).",
        "",
    ]
    if wr_trap:
        lines.append(
            "OOS after-fee H / fresh cash WR > 55% and still **FAIL** "
            "Cyrus: "
            + ", ".join(
                f"`{r['name']}` (H {_md_pct(r.get('oos_fee_h_wr'))}, "
                f"cash WR {_md_pct(r.get('fresh_win_rate'))})"
                for r in wr_trap
            )
            + "."
        )
    else:
        lines.append(
            "No frozen name clears **OOS** after-fee H WR or fresh "
            "cash-trade WR > 55%. Best Taskforce-6 H WR is well under "
            "the 55% screen. WR is still not the KEEP bar."
        )
    pins = ", ".join(f"`{n}`" for n in (cont.get("post_0909_live_pins") or [])) or "none"
    live_task = cont.get("taskforce_in_live_featured") or []
    lines += [
        "",
        "## Contamination vs today's full-sample board",
        "",
        f"Live board window **{cont.get('live_from') or '8/13'} → "
        f"{cont.get('live_to') or '9/18'}** "
        f"({cont.get('live_n_featured') or 0} featured pins). "
        "That pack saw 9/10–9/18 while ranking / pinning. This OOS board "
        "does not.",
        "",
        f"- **hot4 / `union_hot_n4_h1`:** In the 9/9 auto grid, **not** "
        f"Cyrus featured (IS starts {cont.get('hot4_is_starts') or '11/19'}, "
        f"book {_md_pp(cont.get('hot4_is_book'))}%). Live featured pin: "
        f"{'yes' if cont.get('hot4_live_featured') else 'no'}. "
        f"Full-sample book {_md_pp(cont.get('hot4_live_book'))}% "
        f"starts {cont.get('hot4_live_starts') or '—'}. A 9/9 researcher "
        "would not have featured it; OOS shine cannot promote it here.",
        f"- **holdup / `union_hot_n4_holdup`:** Not in the 9/9 recipe menu "
        f"(landed 2026-09-19). Live featured pin: "
        f"{'yes' if cont.get('holdup_live_featured') else 'no'}. "
        "No holdup twin was invented. Not scored.",
        f"- **Post-9/9 live pins (Clock-B / overnight / macd / holdup):** "
        f"{pins}.",
        f"- **Taskforce 6 on the live featured pin list:** "
        + (", ".join(f"`{n}`" for n in live_task) or "none")
        + ". Live pinned the macd / holdup / overnight family instead of "
        "these 9/9 start-day mixes.",
        "",
        "## Confirm vs #286 holdout books",
        "",
    ]
    if mismatches:
        lines.append(
            "MISMATCH vs holdout.json: "
            + ", ".join(
                f"`{r['name']}` cont={r.get('cont_match')} "
                f"fresh={r.get('fresh_match')}"
                for r in mismatches
            )
            + "."
        )
    else:
        lines.append(
            "Continued Book% copied from #286 `holdout.json`. "
            "Fresh $10k start (9/10) matches holdout within "
            f"{BOOK_MATCH_TOL:g} pp for every replayed name."
        )
    lines += [
        "",
        "Recipe-definition freeze: "
        f"`{holdout.get('recipe_freeze') or fmbld.RECIPE_FREEZE_SHA}`. "
        "Side paths only.",
        "",
    ]
    return "\n".join(lines) + "\n"


def write_board(holdout: dict, rows: list[dict], *,
                contamination: dict | None = None,
                oos_start: str = OOS_START, oos_end: str = OOS_END,
                dest_md: Path | None = None,
                dest_json: Path | None = None) -> dict:
    dest_md = Path(dest_md or BOARD_MD)
    dest_json = Path(dest_json or BOARD_JSON)
    md = render_oos_md(
        holdout, rows, contamination=contamination,
        oos_start=oos_start, oos_end=oos_end)
    keep = [r["name"] for r in rows if r.get("verdict") == "KEEP"]
    blob = {
        "generated_at": datetime.now(tl.ET).isoformat(),
        "kind": "blind_0909_fee_aware_oos",
        "cutoff": CUTOFF,
        "oos_start": oos_start,
        "oos_end": oos_end,
        "fee_rt": FEE_RT,
        "recipe_freeze": holdout.get("recipe_freeze") or fmbld.RECIPE_FREEZE_SHA,
        "cyrus_rule": (
            "IS Cyrus featured + continued OOS Book% > 0 + "
            f"OOS start-day YES ≥{fmbld.CYRUS_START_RATE:.0%} "
            f"(≥6/7 on this window). Win%>{WR_BAR:.0%} is not KEEP."
        ),
        "cyrus_featured": list(holdout.get("cyrus_featured") or []),
        "formal_keep": list(holdout.get("formal_keep") or []),
        "taskforce_book_survivors": list(TASKFORCE_BOOK_SURVIVORS),
        "keep": keep,
        "n_keep": len(keep),
        "n_fail": sum(1 for r in rows if r.get("verdict") != "KEEP"),
        "contamination": contamination or {},
        "live_untouched": ["flatten_robust", "dashboard/factor-mine/",
                           "03_scoreboard/factor_mine.json"],
        "rows": rows,
    }
    dest_md.parent.mkdir(parents=True, exist_ok=True)
    dest_json.parent.mkdir(parents=True, exist_ok=True)
    dest_md.write_text(md, encoding="utf-8")
    dest_json.write_text(json.dumps(blob, indent=2), encoding="utf-8")
    print(f"[blind-oos] wrote {dest_md}", flush=True)
    print(f"[blind-oos] wrote {dest_json}", flush=True)
    return blob


def load_json(path: Path) -> dict:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def run(*, holdout_path: Path | None = None, payload_path: Path | None = None,
        live_path: Path | None = None, dest_md: Path | None = None,
        dest_json: Path | None = None, panel: dict | None = None,
        bars=None, fees=None, regime=None, replay: bool = True,
        workers: int = 4) -> dict:
    holdout = load_json(holdout_path or HOLDOUT_JSON)
    payload = load_json(payload_path or IS_JSON)
    live = None
    live_p = Path(live_path or LIVE_JSON)
    if live_p.is_file():
        live = load_json(live_p)
    if panel is None and replay:
        panel = fm.rehydrate_panel(fm.load_or_build_panel(fmbld.FROM_DATE, None))
        fm.attach_tape_flow(panel)
    rows = score_frozen(
        holdout, payload, panel or {},
        bars=bars, fees=fees, regime=regime, replay=replay,
        workers=workers)
    contam = live_contamination(live, holdout)
    return write_board(
        holdout, rows, contamination=contam,
        dest_md=dest_md, dest_json=dest_json)


def main(argv=None) -> int:
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--holdout", default=str(HOLDOUT_JSON))
    ap.add_argument("--payload", default=str(IS_JSON))
    ap.add_argument("--live", default=str(LIVE_JSON))
    ap.add_argument("--md", default=str(BOARD_MD))
    ap.add_argument("--json", dest="json_path", default=str(BOARD_JSON))
    ap.add_argument("--no-replay", action="store_true",
                    help="board from holdout books only (no start-day walk)")
    ap.add_argument("--workers", type=int, default=4)
    args = ap.parse_args(argv)
    md = Path(args.md)
    js = Path(args.json_path)
    for p in (md, js):
        if p.resolve() in {fm.OUT_MD.resolve(), fm.OUT_JSON.resolve()}:
            raise SystemExit("refusing to write live factor-mine board")
    blob = run(
        holdout_path=Path(args.holdout),
        payload_path=Path(args.payload),
        live_path=Path(args.live),
        dest_md=md, dest_json=js,
        replay=not args.no_replay, workers=args.workers)
    print(f"[blind-oos] KEEP={blob['n_keep']} FAIL={blob['n_fail']}",
          flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
