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
            extra = tlf.recipe_extra(row.get("strategy"), row.get("src"))
            row["recipe_require"] = extra.get("recipe_require") or {}
            row["recipe_name"] = extra.get("strategy") or row.get("strategy")
            feat["strategy"] = row["recipe_name"]
            feat["recipe_require"] = row["recipe_require"]
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


def _row_recipe_extra(row: dict) -> dict:
    extra = {"hard_red": bool(row.get("hard_red")), "new_entry": True}
    names = list(row.get("strategies") or [])
    if row.get("strategy"):
        names.append(row["strategy"])
    if row.get("src"):
        names.append(str(row["src"]).split(",")[0])
    picked_req = row.get("recipe_require") or {}
    picked_name = row.get("recipe_name") or row.get("strategy")
    for name in names:
        kid = tlf.recipe_index().get(name) or {}
        gate = tlf.advertised_ob_short(kid.get("require"), name)
        if gate:
            picked_req = kid.get("require") or {}
            picked_name = name
            break
    extra.update(tlf.recipe_extra(picked_name, row.get("src"), picked_req))
    return extra


def decide(row: dict, registry: dict) -> dict:
    return tlf.evaluate(row["side"], row.get("features") or {},
                        registry=registry, extra=_row_recipe_extra(row))


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
    miss_cam = sum(1 for r in unique if (r.get("features") or {}).get("camera_net") is None)
    miss_sec = sum(1 for r in unique if (r.get("features") or {}).get("sector") is None)
    miss_mark = sum(1 for r in unique if r.get("ret_h") is None)
    if coverage.get("reconstructed"):
        holes.append(
            "dated strategy_tickets missing "
            f"{coverage['reconstructed'][0]}→{coverage['reconstructed'][-1]} "
            "— reconstructed from panel pick_day + stock-book files"
        )
    if coverage.get("missing"):
        holes.append(f"no proposed tickets: {coverage['missing']}")
    holes.append(
        f"rsi missing {miss_rsi}/{n}; ret_1 missing {miss_ret}/{n}; "
        f"news missing {miss_news}/{n}; camera_net missing {miss_cam}/{n}; "
        f"sector missing {miss_sec}/{n}"
    )
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


OUT_V2_MD = ROOT / "03_scoreboard" / "TICKET_FILTER_V2_BT.md"
OUT_V2_JSON = ROOT / "03_scoreboard" / "ticket_filter_v2_bt.json"
MIN_STRAT_N = 8

F1_SPEC = {
    "id": "oversold_crash_pause",
    "lesson_id": "oversold_crash_pause",
    "side": "short",
    "action": "block",
    "enabled": True,
    "require": {"rsi_max": 25.0, "crash": True},
    "crash": {"ret_1_max": -8.0, "range_pct_min": 8.0, "gap_pct_max": -8.0},
    "audit": "rsi<=25 & crash",
}
C_SPECS = {
    "short_vs_own_recipe": {
        "id": "short_vs_own_recipe",
        "lesson_id": "short_vs_own_recipe",
        "side": "short",
        "action": "block",
        "enabled": True,
        "require": {"recipe_ob_or_macd_dn": True, "rsi_max": 49.99},
        "audit": "recipe OB/MACD-dn gate & rsi<50",
    },
    "short_vs_green_cameras": {
        "id": "short_vs_green_cameras",
        "lesson_id": "short_vs_green_cameras",
        "side": "short",
        "action": "block",
        "enabled": True,
        "require": {"camera_net_min": 2, "unless_news_bad": True},
        "audit": "camera_net>=+2 unless news bad",
    },
    "short_vs_green_cameras_net4": {
        "id": "short_vs_green_cameras",
        "lesson_id": "short_vs_green_cameras",
        "side": "short",
        "action": "block",
        "enabled": True,
        "require": {"camera_net_min": 4, "unless_news_bad": True},
        "audit": "camera_net>=+4 unless news bad (IS-tuned)",
    },
    "long_vs_hard_red_news": {
        "id": "long_vs_hard_red_news",
        "lesson_id": "long_vs_hard_red_news",
        "side": "long",
        "action": "block",
        "enabled": True,
        "require": {"news_bad": True, "no_camera_support": True},
        "audit": "news bad & no camera support",
    },
    "short_vs_sector_or_tape": {
        "id": "short_vs_sector_or_tape",
        "lesson_id": "short_vs_sector_or_tape",
        "side": "short",
        "action": "block",
        "enabled": True,
        "require": {"crash": True, "sector_good": True},
        "crash": {"ret_1_max": -8.0, "gap_pct_max": -8.0},
        "audit": "oversold-crash & sector good",
    },
}
F3_SPEC = {
    "id": "hard_red_rsi30_short",
    "lesson_id": "hard_red_rsi30_short",
    "side": "short",
    "action": "block",
    "enabled": True,
    "require": {"rsi_max": 30.0, "hard_red": True},
    "audit": "hard-red & rsi<30",
}
F2_TIGHT = {
    "id": "overbought_meltup_pause",
    "lesson_id": "overbought_meltup_pause",
    "side": "long",
    "action": "block",
    "enabled": True,
    "require": {"rsi_min": 75.0, "ret_1_min": 8.0, "no_camera_support": True,
                "no_news_support": True},
    "audit": "rsi>=75 & 1d>=+8 & no camera & no news support",
}


def _reg(*specs) -> dict:
    return {"filters": [json.loads(json.dumps(s)) for s in specs]}


def per_strat_means(rows: list[dict], registry: dict | None,
                    names: list[str] | None = None) -> dict[str, dict]:
    """meanH of kept trades per strategy. registry=None → take-all."""
    by: dict[str, list] = {}
    for r in rows:
        by.setdefault(r["strategy"], []).append(r)
    want = names if names is not None else sorted(by)
    out = {}
    for name in want:
        rs = by.get(name) or []
        if registry is None:
            kept = rs
            blocked: list = []
        else:
            kept, blocked = split_filter(rs, registry)
        st = _stats(kept, "ret_h")
        out[name] = {
            "n": len(rs),
            "n_marked": st["n_marked"],
            "meanH": st["mean"],
            "n_blocked": len(blocked),
            "blocked_meanH": _stats(blocked, "ret_h")["mean"],
        }
    return out


def ew_avg(per: dict[str, dict], names: list[str]) -> dict:
    xs = []
    empty = 0
    for n in names:
        mu = (per.get(n) or {}).get("meanH")
        if mu is None:
            empty += 1
            continue
        xs.append(mu)
    if not xs:
        return {"n_strats": len(names), "n_in_avg": 0, "n_empty": empty,
                "mean": None, "mean_empty0": 0.0 if names else None}
    filled = []
    for n in names:
        mu = (per.get(n) or {}).get("meanH")
        filled.append(0.0 if mu is None else mu)
    return {
        "n_strats": len(names),
        "n_in_avg": len(xs),
        "n_empty": empty,
        "mean": round(sum(xs) / len(xs), 4),
        "mean_empty0": round(sum(filled) / len(filled), 4),
    }


def strat_set(rows: list[dict], min_n: int = MIN_STRAT_N) -> list[str]:
    by: dict[str, list] = {}
    for r in rows:
        by.setdefault(r["strategy"], []).append(r)
    names = []
    for name, rs in by.items():
        if _stats(rs, "ret_h")["n_marked"] >= min_n:
            names.append(name)
    return sorted(names)


def rule_blocked_ok(rows: list[dict], registry: dict) -> tuple[bool, dict]:
    _kept, blocked = split_filter(rows, registry)
    st = _stats(blocked, "ret_h")
    mu = st["mean"]
    ok = st["n_marked"] == 0 or (mu is not None and mu <= 0)
    return ok, {"n": len(blocked), "n_marked": st["n_marked"], "meanH": mu}


def apply_mute(rows: list[dict], registry: dict,
               mute_families: set[str]) -> tuple[list[dict], list[dict]]:
    """One-day mute of a family when a contradiction already fires that day."""
    kept, blocked = split_filter(rows, registry)
    fire_days = {(r["date"], r["family"]) for r in blocked
                 if r.get("family") in mute_families}
    if not fire_days:
        return kept, blocked
    extra = []
    still = []
    for r in kept:
        if (r["date"], r.get("family")) in fire_days:
            item = dict(r)
            item["decision"] = {
                "action": "block", "lesson_id": "recipe_mute",
                "filter_id": "recipe_mute",
                "reason": f"{r.get('strategy')} {r['date']} MUTE",
            }
            extra.append(item)
        else:
            still.append(r)
    return still, blocked + extra


def run_v2() -> dict:
    tlf.reset_caches()
    panel = _panel()
    cal = list(panel.get("session_dates") or [])
    rows, coverage = collect_proposed(panel)
    unique = unique_entries(rows)
    is_rows = _window(rows, None, FIT_END)
    oos_rows = _window(rows, OOS_START, None)
    is_u = _window(unique, None, FIT_END)
    oos_u = _window(unique, OOS_START, None)
    names = strat_set(is_rows, MIN_STRAT_N)
    # also include OOS-only strats that clear N on full window
    names_full = strat_set(rows, MIN_STRAT_N)
    names_oos_set = strat_set(oos_rows, max(3, MIN_STRAT_N // 2))

    baseline = _reg()
    f1 = _reg(F1_SPEC)
    candidates = [("C1", C_SPECS["short_vs_own_recipe"]),
                  ("C2", C_SPECS["short_vs_green_cameras"]),
                  ("C2t", C_SPECS["short_vs_green_cameras_net4"]),
                  ("C3", C_SPECS["long_vs_hard_red_news"]),
                  ("C4", C_SPECS["short_vs_sector_or_tape"]),
                  ("F3", F3_SPEC),
                  ("F2t", F2_TIGHT)]
    # tighter C2: require camera_support AND net>=2
    c2b = json.loads(json.dumps(C_SPECS["short_vs_green_cameras"]))
    c2b["id"] = "short_vs_green_cameras_and"
    c2b["require"] = {**c2b["require"], "camera_support_and_net": True,
                      "camera_net_min": 2}
    candidates.append(("C2b", c2b))
    c2t_and = json.loads(json.dumps(C_SPECS["short_vs_green_cameras_net4"]))
    c2t_and["id"] = "short_vs_green_cameras_net4_and"
    c2t_and["require"] = {**c2t_and["require"], "camera_support_and_net": True}
    candidates.append(("C2t_and", c2t_and))

    ablations = []
    keep = [F1_SPEC]
    dropped = []
    for tag, spec in candidates:
        trial = _reg(F1_SPEC, spec)
        only = _reg(spec)
        is_ok, is_bl = rule_blocked_ok(is_u, only)
        oos_ok, oos_bl = rule_blocked_ok(oos_u, only)
        f1_oos = ew_avg(per_strat_means(oos_rows, f1, names_full), names_full)
        tr_oos = ew_avg(per_strat_means(oos_rows, trial, names_full), names_full)
        f1_m = f1_oos.get("mean")
        tr_m = tr_oos.get("mean")
        helps = (tr_m is not None and f1_m is not None and tr_m + 1e-12 >= f1_m)
        ship = bool(is_ok and oos_ok and helps and (is_bl["n_marked"] or oos_bl["n_marked"]))
        # empty rule (n=0 both) is not meat
        if (is_bl["n_marked"] or 0) + (oos_bl["n_marked"] or 0) == 0:
            ship = False
            why = "no hits — not meat"
        elif not is_ok:
            ship = False
            why = f"IS blocked meanH {is_bl['meanH']} > 0"
        elif not oos_ok:
            ship = False
            why = f"OOS blocked meanH {oos_bl['meanH']} > 0"
        elif not helps:
            ship = False
            why = f"OOS EW {tr_m} < F1 {f1_m}"
        else:
            why = (f"IS blocked {is_bl['meanH']} OOS blocked {oos_bl['meanH']} "
                   f"OOS EW {tr_m} ≥ F1 {f1_m}")
        drop_id = spec["id"]
        if tag == "C2":
            drop_id = "short_vs_green_cameras@net2"
        rec = {
            "tag": tag, "id": spec["id"], "ship": ship, "why": why,
            "is_blocked": is_bl, "oos_blocked": oos_bl,
            "oos_ew_f1": f1_m, "oos_ew_trial": tr_m,
        }
        ablations.append(rec)
        if ship:
            keep.append(spec)
        else:
            dropped.append({"id": drop_id, "why": why, "tag": tag})

    # One live rule per lesson_id — C2t_and is the same unique sleeve as C2t.
    seen_lessons: set[str] = set()
    deduped = []
    for spec in keep:
        lid = str(spec.get("lesson_id") or spec.get("id"))
        if lid in seen_lessons:
            dropped.append({
                "id": spec.get("id"),
                "why": f"duplicate sleeve of shipped {lid}",
            })
            for rec in ablations:
                if rec.get("id") == spec.get("id") and rec.get("ship"):
                    rec["ship"] = False
                    rec["why"] = f"duplicate sleeve of shipped {lid}"
            continue
        seen_lessons.add(lid)
        deduped.append(spec)
    keep = deduped

    v2 = _reg(*keep)
    # Layer 3: mute families whose IS blocked-by-v2 days had meanH <= 0
    is_k, is_b = split_filter(is_rows, v2)
    fam_skip: dict[str, list] = {}
    for r in is_b:
        fam_skip.setdefault(r.get("family") or "", []).append(r)
    mute_fams = set()
    for fam, rs in fam_skip.items():
        if not fam:
            continue
        mu = _stats(rs, "ret_h")["mean"]
        nmk = _stats(rs, "ret_h")["n_marked"]
        if nmk >= 3 and mu is not None and mu <= 0:
            mute_fams.add(fam)
    mute_is_k, mute_is_b = apply_mute(is_rows, v2, mute_fams)
    mute_oos_k, mute_oos_b = apply_mute(oos_rows, v2, mute_fams)
    extra_is = [r for r in mute_is_b
                if (r.get("decision") or {}).get("lesson_id") == "recipe_mute"]
    extra_oos = [r for r in mute_oos_b
                 if (r.get("decision") or {}).get("lesson_id") == "recipe_mute"]
    extra_is_st = _stats(extra_is, "ret_h")
    extra_oos_st = _stats(extra_oos, "ret_h")
    extra_is_ok = extra_is_st["n_marked"] == 0 or (
        extra_is_st["mean"] is not None and extra_is_st["mean"] <= 0)
    extra_oos_ok = extra_oos_st["n_marked"] == 0 or (
        extra_oos_st["mean"] is not None and extra_oos_st["mean"] <= 0)
    mute_ew = ew_avg(
        {n: {"meanH": _stats([r for r in mute_oos_k if r["strategy"] == n], "ret_h")["mean"]}
         for n in names_full},
        names_full,
    )
    v2_oos_ew = ew_avg(per_strat_means(oos_rows, v2, names_full), names_full)
    same_n = mute_ew.get("n_in_avg") == v2_oos_ew.get("n_in_avg")
    mute_helps = (
        extra_is_ok and extra_oos_ok and same_n and mute_fams
        and mute_ew.get("mean") is not None and v2_oos_ew.get("mean") is not None
        and mute_ew["mean"] > v2_oos_ew["mean"] + 1e-12
    )
    if not extra_is_ok:
        mute_why = (
            f"mute-extra IS meanH {extra_is_st['mean']} > 0 "
            f"(n={extra_is_st['n_marked']}; mostly family-wide longs) — drop Layer 3"
        )
    elif not extra_oos_ok:
        mute_why = f"mute-extra OOS meanH {extra_oos_st['mean']} > 0 — drop Layer 3"
    elif not same_n:
        mute_why = (
            f"OOS n_in_avg {mute_ew.get('n_in_avg')} vs v2 "
            f"{v2_oos_ew.get('n_in_avg')} — empty-sleeve bias, drop Layer 3"
        )
    elif not mute_fams:
        mute_why = "no mute families"
    elif not mute_helps:
        mute_why = (
            "OOS EW unchanged — drop Layer 3"
            if mute_ew.get("mean") == v2_oos_ew.get("mean") else
            f"OOS EW {mute_ew.get('mean')} < v2 {v2_oos_ew.get('mean')}"
        )
    else:
        mute_why = "OOS EW strictly improved on the same sleeve set"
    mute_info = {
        "families": sorted(mute_fams),
        "oos_ew": mute_ew.get("mean"),
        "oos_n_in_avg": mute_ew.get("n_in_avg"),
        "v2_oos_ew": v2_oos_ew.get("mean"),
        "v2_oos_n_in_avg": v2_oos_ew.get("n_in_avg"),
        "extra_is": extra_is_st,
        "extra_oos": extra_oos_st,
        "ship": bool(mute_helps),
        "why": mute_why,
    }

    sleeves = {
        "baseline": baseline,
        "f1": f1,
        "v2": v2,
    }
    unique_scores = {k: evaluate_registry(unique, reg) for k, reg in sleeves.items()}
    # per-strat tables on FULL window (and OOS)
    per = {
        split: {
            k: per_strat_means(rs, None if k == "baseline" else sleeves[k], names_full)
            for k in ("baseline", "f1", "v2")
        }
        for split, rs in (("all", rows), ("is", is_rows), ("oos", oos_rows))
    }
    ews = {
        split: {k: ew_avg(per[split][k], names_full) for k in ("baseline", "f1", "v2")}
        for split in ("all", "is", "oos")
    }

    rwt = unique_scores["v2"]["rwt"]
    oos_ew_ok = (
        ews["oos"]["v2"]["mean"] is not None
        and ews["oos"]["f1"]["mean"] is not None
        and ews["oos"]["baseline"]["mean"] is not None
        and ews["oos"]["v2"]["mean"] + 1e-12 >= ews["oos"]["f1"]["mean"]
        and ews["oos"]["v2"]["mean"] + 1e-12 >= ews["oos"]["baseline"]["mean"]
    )
    contract = {
        "rwt_blocked": bool(rwt.get("blocked")),
        "oos_ew_ge_f1_and_baseline": oos_ew_ok,
        "ok": bool(rwt.get("blocked") and oos_ew_ok),
        "notes": [
            f"RWT 9/11 blocked={rwt.get('blocked')}",
            f"OOS EW baseline={ews['oos']['baseline']['mean']} "
            f"F1={ews['oos']['f1']['mean']} v2={ews['oos']['v2']['mean']}",
            f"N={MIN_STRAT_N} strats in EW (IS-or-full marked≥N): {len(names_full)}",
        ],
    }
    table = []
    for name in names_full:
        b = per["all"]["baseline"][name]
        f = per["all"]["f1"][name]
        v = per["all"]["v2"][name]
        delta = None
        if v["meanH"] is not None and b["meanH"] is not None:
            delta = round(v["meanH"] - b["meanH"], 4)
        table.append({
            "strategy": name,
            "n": b["n"],
            "n_marked": b["n_marked"],
            "baseline_meanH": b["meanH"],
            "f1_meanH": f["meanH"],
            "v2_meanH": v["meanH"],
            "delta_v2_base": delta,
            "oos_baseline": (per["oos"]["baseline"].get(name) or {}).get("meanH"),
            "oos_f1": (per["oos"]["f1"].get(name) or {}).get("meanH"),
            "oos_v2": (per["oos"]["v2"].get(name) or {}).get("meanH"),
        })

    holes = _holes(unique, coverage, cal)
    payload = {
        "window": {"from": cal[0] if cal else None, "to": cal[-1] if cal else None,
                   "n_sessions": len(cal), "fit_end": FIT_END, "oos_start": OOS_START,
                   "min_strat_n": MIN_STRAT_N},
        "coverage": coverage,
        "n_proposed_rows": len(rows),
        "n_unique": len(unique),
        "n_strats_ew": len(names_full),
        "strat_names": names_full,
        "unique": {k: unique_scores[k] for k in ("baseline", "f1", "v2")},
        "ew": ews,
        "table": table,
        "ablations": ablations,
        "shipped": [s["id"] for s in keep],
        "dropped": dropped,
        "mute": mute_info,
        "rwt": rwt,
        "contract": contract,
        "holes": holes,
        "names_is": names,
        "names_oos_loose": names_oos_set,
    }
    return payload


def render_v2(payload: dict) -> str:
    w = payload["window"]
    ew = payload["ew"]
    lines = [
        "# Ticket filter v2 — contradiction gates + cross-strat backtest",
        "",
        f"Window **{w['from']} → {w['to']}** · {w['n_sessions']} sessions · "
        f"fit `{w['fit_end']}` / OOS `{w['oos_start']}`→latest.",
        "",
        f"Per-strategy EW set = strategies with **≥{w['min_strat_n']}** "
        f"marked hold-horizon trades on the full window (N chosen so tiny "
        f"sleeves do not dominate). {payload['n_strats_ew']} strategies qualify. "
        f"Proposed rows {payload['n_proposed_rows']} · unique "
        f"{payload['n_unique']}.",
        "",
        "## Contract",
        "",
    ]
    mute = payload.get("mute") or {}
    for n in payload["contract"]["notes"]:
        lines.append(f"- {n}")
    lines += [
        f"- mute Layer 3: ship={mute.get('ship')} — {mute.get('why')}",
        "",
        f"**Contract {'PASS' if payload['contract']['ok'] else 'FAIL'}** · "
        f"shipped `{', '.join(payload['shipped'])}`",
        "",
        "## Equal-weight average across strats (meanH %)",
        "",
        "| Split | baseline | F1-only | v2 | n_in_avg / n_strats |",
        "|---|---:|---:|---:|---|",
    ]
    for split, label in (("all", "Full"), ("is", "IS"), ("oos", "OOS")):
        e = ew[split]
        lines.append(
            f"| {label} | {e['baseline']['mean']} | {e['f1']['mean']} | "
            f"{e['v2']['mean']} | {e['v2']['n_in_avg']}/{e['v2']['n_strats']} "
            f"(empty {e['v2']['n_empty']}) |"
        )
    lines += [
        "",
        "EW uses defined means only (emptied sleeves excluded). "
        "`mean_empty0` treats empty as 0: "
        f"OOS baseline={ew['oos']['baseline'].get('mean_empty0')} "
        f"F1={ew['oos']['f1'].get('mean_empty0')} "
        f"v2={ew['oos']['v2'].get('mean_empty0')}.",
        "",
        "## Unique (date, ticker, side) — same as #252",
        "",
        "| Split | baseline | F1 | v2 | v2 blocked |",
        "|---|---|---|---|---|",
    ]
    for split, label in (("all", "Full"), ("is", "IS"), ("oos", "OOS")):
        u = payload["unique"]
        lines.append(
            f"| {label} | {_fmt(u['baseline'][split]['A'])} | "
            f"{_fmt(u['f1'][split]['B'])} | {_fmt(u['v2'][split]['B'])} | "
            f"{_fmt(u['v2'][split]['blocked'])} |"
        )
    rwt = payload.get("rwt") or {}
    feat = rwt.get("features") or {}
    lines += [
        "",
        "## RWT 2026-09-11 short",
        "",
        f"- blocked={rwt.get('blocked')} rsi={feat.get('rsi')} "
        f"1d={feat.get('ret_1')} ret_h={rwt.get('ret_h')}",
        f"- audit: `{(rwt.get('decision') or {}).get('reason')}`",
        "",
        "## Ablations (add one rule on top of F1)",
        "",
        "| Tag | Ship | IS blocked meanH | OOS blocked meanH | OOS EW trial vs F1 | Why |",
        "|---|---|---:|---:|---|---|",
    ]
    for a in payload.get("ablations") or []:
        lines.append(
            f"| {a['tag']} `{a['id']}` | {a['ship']} | "
            f"{(a['is_blocked'] or {}).get('meanH')} n={(a['is_blocked'] or {}).get('n_marked')} | "
            f"{(a['oos_blocked'] or {}).get('meanH')} n={(a['oos_blocked'] or {}).get('n_marked')} | "
            f"{a.get('oos_ew_trial')} vs {a.get('oos_ew_f1')} | {a['why']} |"
        )
    lines += [
        "",
        "## Per-strategy meanH (full window, n_marked ≥ N)",
        "",
        "| Strategy | n | n_marked | baseline | F1 | v2 | Δ v2−base | OOS v2 |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    ew_row = payload["ew"]["all"]
    lines.append(
        f"| **EW avg** | {payload['n_strats_ew']} | — | "
        f"{ew_row['baseline']['mean']} | {ew_row['f1']['mean']} | "
        f"{ew_row['v2']['mean']} | "
        f"{round((ew_row['v2']['mean'] or 0) - (ew_row['baseline']['mean'] or 0), 4)} | "
        f"{payload['ew']['oos']['v2']['mean']} |"
    )
    for r in payload.get("table") or []:
        lines.append(
            f"| `{r['strategy']}` | {r['n']} | {r['n_marked']} | "
            f"{r['baseline_meanH']} | {r['f1_meanH']} | {r['v2_meanH']} | "
            f"{r['delta_v2_base']} | {r['oos_v2']} |"
        )
    lines += [
        "",
        "## Blocked blotter (v2 unique)",
        "",
        "| Date | Ticker | Side | RSI | 1d | H% | Lesson |",
        "|---|---|---|---:|---:|---:|---|",
    ]
    for r in (payload["unique"]["v2"].get("blocked_rows") or []):
        lines.append(
            f"| {r['date']} | {r['ticker']} | {r['side']} | "
            f"{r.get('rsi')} | {r.get('ret_1')} | {r.get('ret_h')} | "
            f"{r.get('lesson_id')} |"
        )
    if not payload["unique"]["v2"].get("blocked_rows"):
        lines.append("| — | — | — | — | — | — | none |")
    mute = payload.get("mute") or {}
    lines += [
        "",
        "## Shipped vs dropped",
        "",
        f"- **shipped:** {payload['shipped']}",
    ]
    for d in payload.get("dropped") or []:
        lines.append(f"- dropped `{d.get('tag') or ''} {d.get('id')}`: {d.get('why')}")
    lines += [
        f"- Layer 3 mute **off**: {mute.get('why')}",
        f"  extra IS meanH={((mute.get('extra_is') or {}).get('mean'))} "
        f"n={((mute.get('extra_is') or {}).get('n_marked'))}; "
        f"OOS n_in_avg mute={mute.get('oos_n_in_avg')} vs v2={mute.get('v2_oos_n_in_avg')}.",
        "",
        "## Coverage holes",
        "",
    ]
    for h in payload.get("holes") or []:
        lines.append(f"- {h}")
    lines += [
        "",
        "These are archive marks, not a 60% claim. North star ~2%/day after fees.",
        "",
    ]
    return "\n".join(lines) + "\n"


def sync_live_registry(payload: dict) -> Path:
    """Write 00_grounding/ticket_filters.json from the walk-forward keep set."""
    shipped_ids = list(payload.get("shipped") or [])
    by_id = {F1_SPEC["id"]: F1_SPEC}
    by_id["short_vs_green_cameras"] = C_SPECS["short_vs_green_cameras_net4"]
    for spec in C_SPECS.values():
        by_id.setdefault(spec["id"], spec)
    filters = []
    for fid in shipped_ids:
        spec = json.loads(json.dumps(by_id.get(fid) or {}))
        if not spec:
            continue
        spec["enabled"] = True
        filters.append(spec)
    # document specified C* that stayed off
    off = [
        C_SPECS["short_vs_own_recipe"],
        C_SPECS["long_vs_hard_red_news"],
        C_SPECS["short_vs_sector_or_tape"],
        F2_TIGHT,
        F3_SPEC,
    ]
    have = {f["id"] for f in filters}
    for spec in off:
        if spec["id"] in have:
            continue
        item = json.loads(json.dumps(spec))
        item["enabled"] = False
        filters.append(item)
    dropped = list(payload.get("dropped") or [])
    mute = payload.get("mute") or {}
    if not mute.get("ship"):
        dropped.append({
            "id": "recipe_mute",
            "why": mute.get("why") or "Layer 3 off",
        })
    live = {
        "version": 2,
        "asof": "2026-09-16",
        "note": (
            "Ticket-level lesson FILTER. One key per (situation, side, action). "
            "Predicates are tape-checkable from files already in the repo as-of "
            "the buy date. Missing features = pass, never veto. Do not dump "
            "markdown lessons into if-statements."
        ),
        "walkforward": {
            "fit": ["2026-08-13", "2026-09-04"],
            "oos": ["2026-09-05", "latest"],
        },
        "filters": filters,
        "dropped": dropped,
    }
    path = ROOT / "00_grounding" / "ticket_filters.json"
    path.write_text(json.dumps(live, indent=2) + "\n", encoding="utf-8")
    tlf.reset_caches()
    return path


def write_v2(payload: dict | None = None) -> list[Path]:
    payload = payload or run_v2()
    OUT_V2_MD.parent.mkdir(parents=True, exist_ok=True)
    OUT_V2_MD.write_text(render_v2(payload), encoding="utf-8")
    OUT_V2_JSON.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    live = sync_live_registry(payload)
    return [OUT_V2_MD, OUT_V2_JSON, live]


def main(argv=None) -> int:
    import sys
    args = list(sys.argv[1:] if argv is None else argv)
    if "--v2" in args or "v2" in args:
        payload = run_v2()
        write_v2(payload)
        print(json.dumps({
            "mode": "v2",
            "contract": payload["contract"],
            "shipped": payload["shipped"],
            "dropped": payload["dropped"],
            "mute": payload["mute"],
            "ew_oos": payload["ew"]["oos"],
            "rwt_blocked": payload["rwt"].get("blocked"),
            "wrote": [str(OUT_V2_MD), str(OUT_V2_JSON)],
        }, indent=2, default=str))
        return 0 if payload["contract"]["ok"] else 1
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
