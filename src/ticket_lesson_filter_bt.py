"""Leak-free backtest of the ticket lesson FILTER.

Window: 2026-08-13 → latest session in the factor-mine panel.
Universe: proposed NEW entries reconstructed from dated strategy
tickets when they exist, else panel pick_day + stock-book files.
Features as-of that morning only. No invented fills.

Sleeves:
  A  take-all proposed unique (date, ticker, side)
  B  apply filters (blocked removed)
Primary: mean hold-horizon P&L, hit rate, blocked-sleeve P&L (must be ≤ 0).
Walk-forward: fit 08-13 → 09-04, freeze, score 09-05 → latest.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

from src import factor_mine as fm
from src import ticket_lesson_filter as tlf

ROOT = Path(__file__).resolve().parent.parent
PANEL_PATH = ROOT / "data" / "factor_mine" / "panel.json"
DAY = ROOT / "data" / "day_board"
BOOK = ROOT / "data" / "stock_book"
OUT_MD = ROOT / "03_scoreboard" / "TICKET_FILTER_BT.md"
OUT_JSON = ROOT / "03_scoreboard" / "ticket_filter_bt.json"
FIT_END = "2026-09-04"
OOS_START = "2026-09-05"
HOLD_MAP = {
    "1d": 1, "3d": 3, "1w": 5, "2w": 10, "1m": 21,
}
RWT_DATE = "2026-09-11"
RWT = "RWT"


def _tick(v) -> str:
    return str(v or "").strip().upper()


def _num(v):
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _load_json(path: Path):
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _hold_for(name: str, family: str, rec_by: dict) -> int:
    rec = rec_by.get(name) or {}
    if rec.get("hold"):
        try:
            return max(int(rec["hold"]), 1)
        except (TypeError, ValueError):
            pass
    m = re.search(r"_h(\d+)$", name or "")
    if m:
        return max(int(m.group(1)), 1)
    for key, n in HOLD_MAP.items():
        if name == f"stock_book_{key}" or name == f"{key}_top":
            return n
    if family == "flatten" or name.startswith("flatten"):
        return 3
    return 1


def _session_bars(ticker: str) -> list[dict]:
    try:
        from . import candle_factor as cf
        return list(cf._ticker_bars().get(_tick(ticker)) or [])
    except Exception:
        return []


def fwd_marks(ticker: str, date: str, hold: int, side: str,
              cal: list[str]) -> dict:
    """09:30 open → horizon close, plus 1d / 5d. None when the exit is missing."""
    out = {"ret_h": None, "ret_1d": None, "ret_5d": None,
           "entry": None, "exit": None, "exit_date": None, "marked": False}
    t = _tick(ticker)
    if not t or date not in cal:
        return out
    bars = {b["date"]: b for b in _session_bars(t)}
    entry_bar = bars.get(date) or {}
    entry = _num(entry_bar.get("open")) or _num(entry_bar.get("close"))
    if entry is None or entry <= 0:
        return out
    out["entry"] = entry

    def _ret(exit_date: str | None, use_close: bool) -> float | None:
        if not exit_date:
            return None
        xb = bars.get(exit_date) or {}
        px = _num(xb.get("close") if use_close else xb.get("open"))
        if px is None:
            px = _num(xb.get("open")) or _num(xb.get("close"))
        if px is None or px <= 0:
            return None
        raw = 100.0 * (px / entry - 1.0)
        if side == "short":
            raw = -raw
        return round(raw, 4)

    i = cal.index(date)
    h = max(int(hold or 1), 1)
    if i + h - 1 < len(cal):
        out["exit_date"] = cal[i + h - 1]
        out["ret_h"] = _ret(out["exit_date"], True)
        out["marked"] = out["ret_h"] is not None
    if i + 1 - 1 < len(cal):
        out["ret_1d"] = _ret(cal[i], True)  # same-session open→close
    if i + 5 - 1 < len(cal):
        out["ret_5d"] = _ret(cal[i + 4], True)
    return out


def _panel() -> dict:
    raw = _load_json(PANEL_PATH) or {}
    return fm.rehydrate_panel(raw)


def _stock_book_tickets(date: str) -> list[dict]:
    sug = _load_json(BOOK / f"{date}_suggestions.json")
    if sug is None:
        book = _load_json(BOOK / f"{date}_stock_book.json")
        if book is None:
            return []
        try:
            from src import book_suggestions as bs
            sug = bs.suggestions_from_book({**book, "date": date})
        except Exception:
            return []
    out = []
    horizons = (sug or {}).get("horizons") or {}
    for h in ("1d", "3d", "1w", "2w", "1m"):
        block = horizons.get(h) or {}
        hold = HOLD_MAP[h]
        for row in block.get("buy") or []:
            t = _tick(row.get("ticker") if isinstance(row, dict) else row)
            if t:
                out.append({
                    "date": date, "ticker": t, "side": "long",
                    "strategy": f"stock_book_{h}", "family": "stock_book",
                    "hold": hold, "src": "stock_book",
                })
        for row in block.get("sell") or []:
            t = _tick(row.get("ticker") if isinstance(row, dict) else row)
            if t:
                out.append({
                    "date": date, "ticker": t, "side": "short",
                    "strategy": f"stock_book_{h}", "family": "stock_book",
                    "hold": hold, "src": "stock_book_sell",
                })
    return out


def _from_dated_tickets(date: str, rec_by: dict) -> list[dict] | None:
    payload = _load_json(DAY / f"{date}_strategy_tickets.json")
    if not payload:
        return None
    out = []
    for name, rec in (payload.get("strategies") or {}).items():
        if not isinstance(rec, dict):
            continue
        family = str(rec.get("family") or "")
        hold = _hold_for(name, family, rec_by)
        for row in rec.get("buy") or []:
            if not isinstance(row, dict) or not row.get("ticker"):
                continue
            side = str(row.get("kid_side") or row.get("side") or rec.get("side")
                       or "long").lower()
            if side not in ("long", "short"):
                side = "long"
            out.append({
                "date": date, "ticker": _tick(row["ticker"]), "side": side,
                "strategy": name, "family": family, "hold": hold,
                "src": row.get("src") or "dated_tickets",
                "hard_red": bool(rec.get("hard_red") or rec.get("sit")),
            })
        if family == "flatten":
            continue
        for row in rec.get("sell") or []:
            if not isinstance(row, dict) or not row.get("ticker"):
                continue
            out.append({
                "date": date, "ticker": _tick(row["ticker"]), "side": "short",
                "strategy": name, "family": family, "hold": hold,
                "src": row.get("src") or "dated_tickets_sell",
                "hard_red": bool(rec.get("hard_red") or rec.get("sit")),
            })
    return out


def _from_panel(date: str, panel: dict, recs: list[dict],
                rec_by: dict) -> list[dict]:
    rows = (panel.get("by_date") or {}).get(date) or []
    out = []
    hard = False
    try:
        from src import factor_mine_book as fmb
        s = fmb.morning_s(fmb.load_regime(), date)
        hard = s is not None and float(s) <= float(fmb.HARD_RED)
    except Exception:
        hard = False
    specs = {}
    try:
        from src import factor_mine_combo as fmc
        specs = {sp["name"]: sp for sp in fmc.combo_specs()}
    except Exception:
        specs = {}
    from src import strategy_tickets as st
    for rec in recs:
        name = rec["name"]
        is_combo = (rec.get("universe") == "combo" or rec.get("members")
                    or name.startswith("combo_"))
        try:
            if is_combo:
                spec = specs.get(name) or {}
                members = list(rec.get("members") or spec.get("members") or [])
                picked = st._combo_would_buy(rows, rec_by, members, spec, fm)
            else:
                picked = [
                    {"ticker": r["ticker"], "src": ",".join(r.get("sources") or []),
                     "kid_side": rec.get("side") or "long"}
                    for r in fm.pick_day(rows, rec) if r.get("ticker")
                ]
        except Exception:
            continue
        hold = _hold_for(name, "factor_mine", rec_by)
        for row in picked:
            t = _tick(row.get("ticker"))
            if not t:
                continue
            side = str(row.get("kid_side") or rec.get("side") or "long").lower()
            if side not in ("long", "short"):
                side = "long"
            out.append({
                "date": date, "ticker": t, "side": side,
                "strategy": name, "family": "factor_mine", "hold": hold,
                "src": row.get("src") or "panel_pick",
                "hard_red": hard,
            })
    out.extend(_stock_book_tickets(date))
    return out


def collect_proposed(panel: dict) -> tuple[list[dict], dict]:
    cal = list(panel.get("session_dates") or [])
    recs = list(fm.build_recipes())
    have = {r["name"] for r in recs}
    try:
        from src import factor_mine_combo as fmc
        for spec in fmc.combo_specs():
            if spec["name"] not in have:
                recs.append(fmc.combo_recipe(spec))
                have.add(spec["name"])
    except Exception:
        pass
    rec_by = {r["name"]: r for r in recs}
    coverage = {"dated_tickets": [], "reconstructed": [], "missing": []}
    all_rows: list[dict] = []
    by_ticker = {}
    for r in panel.get("rows") or []:
        d, t = r.get("date"), _tick(r.get("ticker"))
        if d and t:
            by_ticker[(d, t)] = r
    for date in cal:
        dated = _from_dated_tickets(date, rec_by)
        if dated is not None:
            rows = dated
            coverage["dated_tickets"].append(date)
        else:
            rows = _from_panel(date, panel, recs, rec_by)
            if rows:
                coverage["reconstructed"].append(date)
            else:
                coverage["missing"].append(date)
        for row in rows:
            prow = by_ticker.get((date, row["ticker"]))
            feat = tlf.prior_features(row["ticker"], date, prow)
            feat["hard_red"] = bool(row.get("hard_red"))
            row["features"] = feat
            marks = fwd_marks(row["ticker"], date, row["hold"], row["side"], cal)
            row.update(marks)
            all_rows.append(row)
    return all_rows, coverage


def unique_entries(rows: list[dict]) -> list[dict]:
    """One row per (date, ticker, side). Keep shortest hold, any hard-red."""
    best: dict[tuple, dict] = {}
    for row in rows:
        key = (row["date"], row["ticker"], row["side"])
        cur = best.get(key)
        if cur is None or int(row.get("hold") or 99) < int(cur.get("hold") or 99):
            item = dict(row)
            item["strategies"] = [row.get("strategy")]
            best[key] = item
        else:
            best[key].setdefault("strategies", []).append(row.get("strategy"))
            if row.get("hard_red"):
                best[key]["hard_red"] = True
    return sorted(best.values(), key=lambda r: (r["date"], r["ticker"], r["side"]))


def decide(row: dict, registry: dict) -> dict:
    extra = {"hard_red": bool(row.get("hard_red")), "new_entry": True}
    return tlf.evaluate(row["side"], row.get("features") or {},
                        registry=registry, extra=extra)


def _stats(rows: list[dict], key: str = "ret_h") -> dict:
    xs = [_num(r.get(key)) for r in rows if _num(r.get(key)) is not None]
    n = len(xs)
    if not n:
        return {"n": 0, "n_marked": 0, "mean": None, "hit": None, "sum": None}
    hits = sum(1 for x in xs if x > 0)
    return {
        "n": len(rows),
        "n_marked": n,
        "mean": round(sum(xs) / n, 4),
        "hit": round(hits / n, 4),
        "sum": round(sum(xs), 4),
    }


def sleeve_stats(rows: list[dict]) -> dict:
    return {
        "hold": _stats(rows, "ret_h"),
        "d1": _stats(rows, "ret_1d"),
        "d5": _stats(rows, "ret_5d"),
        "n": len(rows),
        "n_long": sum(1 for r in rows if r.get("side") == "long"),
        "n_short": sum(1 for r in rows if r.get("side") == "short"),
    }


def split_filter(rows: list[dict], registry: dict) -> tuple[list[dict], list[dict]]:
    kept, blocked = [], []
    for row in rows:
        dec = decide(row, registry)
        item = dict(row)
        item["decision"] = dec
        if dec.get("action") in ("block", "pause"):
            blocked.append(item)
        else:
            kept.append(item)
    return kept, blocked


def _window(rows: list[dict], start: str | None, end: str | None) -> list[dict]:
    out = []
    for r in rows:
        d = r["date"]
        if start and d < start:
            continue
        if end and d > end:
            continue
        out.append(r)
    return out


def _fmt(st: dict) -> str:
    h = st.get("hold") or {}
    if not h.get("n_marked"):
        return f"n={st.get('n', 0)} unmarked"
    return (
        f"n={st.get('n', 0)} marked={h['n_marked']} "
        f"meanH={h['mean']:+.3f}% hit={100 * (h['hit'] or 0):.1f}% "
        f"d1={((st.get('d1') or {}).get('mean'))} "
        f"d5={((st.get('d5') or {}).get('mean'))}"
    )


def evaluate_registry(unique: list[dict], registry: dict) -> dict:
    kept, blocked = split_filter(unique, registry)
    is_all = _window(unique, None, FIT_END)
    oos_all = _window(unique, OOS_START, None)
    is_k, is_b = split_filter(is_all, registry)
    oos_k, oos_b = split_filter(oos_all, registry)
    rwt = [r for r in unique if r["ticker"] == RWT and r["date"] == RWT_DATE
           and r["side"] == "short"]
    rwt_dec = decide(rwt[0], registry) if rwt else None
    crash_shorts = [
        r for r in blocked
        if r.get("side") == "short"
        and (r.get("features") or {}).get("crash")
        and _num((r.get("features") or {}).get("rsi")) is not None
        and _num((r.get("features") or {}).get("rsi")) <= 25
    ]
    return {
        "all": {
            "A": sleeve_stats(unique),
            "B": sleeve_stats(kept),
            "blocked": sleeve_stats(blocked),
        },
        "is": {
            "A": sleeve_stats(is_all),
            "B": sleeve_stats(is_k),
            "blocked": sleeve_stats(is_b),
        },
        "oos": {
            "A": sleeve_stats(oos_all),
            "B": sleeve_stats(oos_k),
            "blocked": sleeve_stats(oos_b),
        },
        "n_blocked": len(blocked),
        "blocked_rows": [
            {
                "date": r["date"], "ticker": r["ticker"], "side": r["side"],
                "hold": r.get("hold"), "ret_h": r.get("ret_h"),
                "ret_1d": r.get("ret_1d"),
                "rsi": (r.get("features") or {}).get("rsi"),
                "ret_1": (r.get("features") or {}).get("ret_1"),
                "lesson_id": (r.get("decision") or {}).get("lesson_id"),
                "reason": (r.get("decision") or {}).get("reason"),
                "strategies": r.get("strategies") or [r.get("strategy")],
            }
            for r in blocked
        ],
        "rwt": {
            "present": bool(rwt),
            "blocked": bool(rwt_dec and rwt_dec.get("action") in ("block", "pause")),
            "decision": rwt_dec,
            "features": (rwt[0].get("features") if rwt else None),
            "ret_h": rwt[0].get("ret_h") if rwt else None,
            "ret_1d": rwt[0].get("ret_1d") if rwt else None,
            "ret_5d": rwt[0].get("ret_5d") if rwt else None,
        },
        "crash_shorts_blocked": len(crash_shorts),
    }


def _mean(st: dict):
    return (st.get("hold") or {}).get("mean")


def contract_ok(score: dict) -> tuple[bool, list[str]]:
    notes = []
    ok = True
    if not score["rwt"]["present"]:
        notes.append("RWT 9/11 short not in universe")
        ok = False
    elif not score["rwt"]["blocked"]:
        notes.append("RWT 9/11 short not blocked")
        ok = False
    else:
        notes.append("RWT 9/11 short blocked")
    for split, label in (("is", "IS"), ("oos", "OOS"), ("all", "full")):
        st = score[split]["blocked"]
        mu = _mean(st)
        nmk = (st.get("hold") or {}).get("n_marked") or 0
        if nmk == 0:
            notes.append(f"{label} blocked sleeve unmarked — cannot score expectancy")
            if split == "oos":
                ok = False
            continue
        if mu is None:
            notes.append(f"{label} blocked mean missing")
            if split != "all":
                ok = False
        elif mu > 0:
            notes.append(f"{label} blocked-sleeve mean {mu:+.3f}% > 0 — drop/loosen")
            if split != "all":
                ok = False
        else:
            notes.append(f"{label} blocked-sleeve mean {mu:+.3f}% ≤ 0 (n_marked={nmk})")
    oos_a = _mean(score["oos"]["A"])
    oos_tr = _mean(score["oos"]["B"])
    if oos_a is not None and oos_tr is not None:
        if oos_tr + 0.05 < oos_a:
            notes.append(
                f"OOS traded {oos_tr:+.3f}% < baseline {oos_a:+.3f}% by >5bp"
            )
            ok = False
        else:
            notes.append(
                f"OOS traded {oos_tr:+.3f}% vs baseline {oos_a:+.3f}%"
            )
    return ok, notes


def seed_registry() -> dict:
    return tlf.load_registry()


def f3_registry() -> dict:
    base = json.loads(json.dumps(seed_registry()))
    base["filters"] = list(base.get("filters") or []) + [{
        "id": "hard_red_rsi30_short",
        "lesson_id": "hard_red_rsi30_short",
        "side": "short",
        "action": "block",
        "enabled": True,
        "require": {"rsi_max": 30.0, "hard_red": True},
        "audit": "hard-red & rsi<30",
    }]
    return base


def run() -> dict:
    tlf.reset_caches()
    panel = _panel()
    cal = list(panel.get("session_dates") or [])
    rows, coverage = collect_proposed(panel)
    unique = unique_entries(rows)
    reg = seed_registry()
    score = evaluate_registry(unique, reg)
    f3 = evaluate_registry(unique, f3_registry())
    f3_n = f3["n_blocked"] - score["n_blocked"]
    f3_block = [r for r in f3["blocked_rows"]
                if r.get("lesson_id") == "hard_red_rsi30_short"]
    f3_mean = None
    if f3_block:
        xs = [_num(r.get("ret_h")) for r in f3_block if _num(r.get("ret_h")) is not None]
        f3_mean = round(sum(xs) / len(xs), 4) if xs else None
    drop_f3 = True
    f3_why = (
        f"n_extra={f3_n} mean_h={f3_mean} — n tiny / do not fish a third predicate"
    )
    ok, notes = contract_ok(score)
    shipped = [f.get("id") for f in (reg.get("filters") or [])
               if f.get("enabled") is not False]
    dropped = [d for d in (reg.get("dropped") or []) if isinstance(d, dict)]
    if not any(d.get("id") == "hard_red_rsi30_short" for d in dropped):
        dropped.append({
            "id": "hard_red_rsi30_short",
            "why": f3_why,
        })
    else:
        for d in dropped:
            if d.get("id") == "hard_red_rsi30_short":
                d["n_extra_blocked"] = f3_n
                d["blocked_mean_h"] = f3_mean
                d["why"] = (d.get("why") or "") + f" ({f3_why})"
    payload = {
        "window": {"from": cal[0] if cal else None, "to": cal[-1] if cal else None,
                   "n_sessions": len(cal), "fit_end": FIT_END, "oos_start": OOS_START},
        "coverage": coverage,
        "n_proposed_rows": len(rows),
        "n_unique": len(unique),
        "score": score,
        "contract_ok": ok,
        "contract_notes": notes,
        "shipped": shipped,
        "dropped": dropped,
        "holes": _holes(unique, coverage, cal),
        "f3_eval": {"n_extra": f3_n, "mean_h": f3_mean, "dropped": drop_f3},
    }
    return payload


def _holes(unique: list[dict], coverage: dict, cal: list[str]) -> list[str]:
    holes = []
    n = len(unique)
    miss_rsi = sum(1 for r in unique if (r.get("features") or {}).get("rsi") is None)
    miss_ret = sum(1 for r in unique if (r.get("features") or {}).get("ret_1") is None)
    miss_news = sum(1 for r in unique if (r.get("features") or {}).get("news") is None)
    miss_mark = sum(1 for r in unique if r.get("ret_h") is None)
    if coverage.get("reconstructed"):
        holes.append(
            "dated strategy_tickets missing "
            f"{coverage['reconstructed'][0]}→{coverage['reconstructed'][-1]} "
            "— reconstructed from panel pick_day + stock-book files"
        )
    if coverage.get("missing"):
        holes.append(f"no proposed tickets: {coverage['missing']}")
    holes.append(f"rsi missing {miss_rsi}/{n}; ret_1 missing {miss_ret}/{n}; "
                 f"news/camera missing {miss_news}/{n}")
    holes.append(f"hold-horizon unmarked {miss_mark}/{n} (exit not in archive)")
    holes.append("macro / tape_anchor / channel1 not attached on ticket rows — unused in v1")
    holes.append(
        "data/prices/ohlc.parquet asof 2026-09-11 — 09-14/09-15 official "
        "opens/closes are missing, so RWT hold>1 and OOS 5d marks are unmarked. "
        "Do not invent the 3.83/3.94 bounce."
    )
    if "2026-09-12" not in cal and "2026-09-13" not in cal:
        holes.append("09-12/09-13 are weekend — not sessions")
    return holes


def render_md(payload: dict) -> str:
    sc = payload["score"]
    w = payload["window"]
    rwt = sc["rwt"]
    feat = rwt.get("features") or {}
    lines = [
        "# Ticket lesson filter — backtest",
        "",
        f"Window **{w['from']} → {w['to']}** · {w['n_sessions']} sessions · "
        f"fit `{w['fit_end']}` / OOS `{w['oos_start']}`→latest.",
        "",
        "Universe = unique proposed NEW entries `(date, ticker, side)` from "
        "dated `strategy_tickets` when present, else factor-mine `pick_day` + "
        "stock-book files. Features are prior-bar only. Marks are 09:30 open → "
        "horizon close (short flips the sign). No invented fills.",
        "",
        f"Proposed rows {payload['n_proposed_rows']} · unique {payload['n_unique']}.",
        "",
        "## Contract",
        "",
    ]
    for n in payload["contract_notes"]:
        lines.append(f"- {n}")
    lines += [
        "",
        f"**Contract {'PASS' if payload['contract_ok'] else 'FAIL'}** · "
        f"shipped `{', '.join(payload['shipped']) or 'none'}`",
        "",
        "Iteration: first seed treated any wide bar as an air-pocket, so "
        "QMLS-class *up* days and F2 melt-up longs made the blocked sleeve "
        "positive in-sample. Crash now requires prior 1d ≤ −8% or a down gap. "
        "F2/F3 dropped. Live filter is F1 only.",
        "",
        "## Sleeves (unique tickets)",
        "",
        "| Split | A take-all | B filtered | Blocked |",
        "|---|---|---|---|",
    ]
    for split, label in (("all", "Full"), ("is", "IS 08-13→09-04"), ("oos", "OOS 09-05→latest")):
        a, b, bl = sc[split]["A"], sc[split]["B"], sc[split]["blocked"]
        lines.append(
            f"| {label} | {_fmt(a)} | {_fmt(b)} | {_fmt(bl)} |"
        )
    lines += [
        "",
        "## RWT 2026-09-11 short",
        "",
    ]
    if rwt.get("present"):
        lines.append(
            f"- present={rwt['present']} blocked={rwt['blocked']} "
            f"rsi={feat.get('rsi')} 1d={feat.get('ret_1')} "
            f"rvol={feat.get('rvol')} crash={feat.get('crash')} "
            f"ret_h={rwt.get('ret_h')} ret_1d={rwt.get('ret_1d')} "
            f"ret_5d={rwt.get('ret_5d')}"
        )
        if rwt.get("decision"):
            lines.append(f"- audit: `{rwt['decision'].get('reason')}`")
    else:
        lines.append("- RWT 9/11 short was **not** in the reconstructed universe.")
    lines += [
        "",
        "## Blocked blotter",
        "",
        "| Date | Ticker | Side | RSI | 1d | H% | Lesson |",
        "|---|---|---|---:|---:|---:|---|",
    ]
    for r in sc.get("blocked_rows") or []:
        lines.append(
            f"| {r['date']} | {r['ticker']} | {r['side']} | "
            f"{r.get('rsi')} | {r.get('ret_1')} | {r.get('ret_h')} | "
            f"{r.get('lesson_id')} |"
        )
    if not sc.get("blocked_rows"):
        lines.append("| — | — | — | — | — | — | none |")
    lines += [
        "",
        "## Filters shipped vs dropped",
        "",
        f"- shipped: {payload['shipped']}",
        f"- dropped: {payload['dropped']}",
        f"- F3 eval: {payload.get('f3_eval')}",
        "",
        "## Coverage holes",
        "",
    ]
    for h in payload.get("holes") or []:
        lines.append(f"- {h}")
    dated = payload["coverage"].get("dated_tickets") or []
    recon = payload["coverage"].get("reconstructed") or []
    lines += [
        "",
        f"- dated tickets: {dated}",
        f"- reconstructed: {recon}",
        "",
        "North star remains ~2%/day after fees. These numbers are archive "
        "marks, not a live claim.",
        "",
    ]
    return "\n".join(lines) + "\n"


def write(payload: dict | None = None) -> list[Path]:
    payload = payload or run()
    OUT_MD.parent.mkdir(parents=True, exist_ok=True)
    OUT_MD.write_text(render_md(payload), encoding="utf-8")
    slim = json.loads(json.dumps(payload, default=str))
    OUT_JSON.write_text(json.dumps(slim, indent=2), encoding="utf-8")
    return [OUT_MD, OUT_JSON]


def main(argv=None) -> int:
    payload = run()
    write(payload)
    print(json.dumps({
        "contract_ok": payload["contract_ok"],
        "notes": payload["contract_notes"],
        "n_unique": payload["n_unique"],
        "n_blocked": payload["score"]["n_blocked"],
        "rwt_blocked": payload["score"]["rwt"]["blocked"],
        "oos_A": payload["score"]["oos"]["A"]["hold"],
        "oos_B": payload["score"]["oos"]["B"]["hold"],
        "oos_blocked": payload["score"]["oos"]["blocked"]["hold"],
        "wrote": [str(OUT_MD), str(OUT_JSON)],
    }, indent=2))
    return 0 if payload["contract_ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
