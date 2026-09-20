"""Time-cut overfit remine: freeze KEEP on a cutoff, score the rest of tape.

In-sample selection uses only sessions ≤ cutoff (default 2026-09-09).
Those frozen recipes — plus WORKABLE_ALWAYS extras — are then replayed
on the standing tape through last closed. OOS days never choose KEEP.

Research only. Writes side paths; does not touch live Pages.
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from . import factor_mine as fm
from . import factor_mine_book as fmb
from . import factor_mine_combo as fmc
from . import ticker_lookback as tl

DEFAULT_CUTOFF = "2026-09-09"
DEFAULT_OOS_START = "2026-09-10"
FOCUS = (
    "union_hot_n4_h1",
    "union_hot_n4_holdup",
    "combo_sh_5050_shared",
    "combo_sh_macd_5050_shared",
    "combo_sh_3070_shared",
    "combo_sh_7030_shared",
    "combo_oh_5050_shared",
    "flatten_h5",
    "overnight_mega_h1",
    "overnight_mega_h2",
    "overnight_h1",
)


def _num(v, default=None):
    x = fm._finite(v)
    return default if x is None else float(x)


def equity_on(book: dict, date: str):
    """Last marked equity on or before ``date``."""
    hit = None
    for row in book.get("daily") or []:
        d = row.get("date")
        if not d or d > date:
            continue
        eq = fm._finite(row.get("equity"))
        if eq is not None:
            hit = float(eq)
    return hit


def oos_day_rate(book: dict, oos_start: str) -> tuple[float | None, int, int]:
    days = [
        d for d in (book.get("daily") or [])
        if d.get("date") and d["date"] >= oos_start and d.get("mean") is not None
    ]
    if not days:
        return None, 0, 0
    n_hit = sum(1 for d in days if d.get("made_money"))
    return round(n_hit / len(days), 4), n_hit, len(days)


def report_names(payload: dict) -> list[str]:
    """KEEP that passed the bar + ALWAYS extras (even if they failed)."""
    stats = list(payload.get("stats") or [])
    recipes = list(payload.get("recipes") or [])
    by = {s.get("name"): s for s in stats if s.get("name")}
    passed = {
        s["name"] for s in stats
        if s.get("name") and fm.is_workable_stat(s)
    }
    names = list(fm.WORKABLE_ALWAYS)
    for n in sorted(passed):
        if n not in names:
            names.append(n)
    # Combo members that only rode along for the dash prune stay out
    # unless they passed the bar themselves.
    for n in FOCUS:
        if n in by and n not in names:
            names.append(n)
    return names


def rec_by_name(payload: dict) -> dict[str, dict]:
    out = {}
    for rec in payload.get("recipes") or []:
        if rec.get("name"):
            out[rec["name"]] = rec
    if not out:
        out = {r["name"]: r for r in fm.build_recipes()}
    for rec in fm.build_recipes():
        out.setdefault(rec["name"], rec)
    return out


def replay_recipe(panel: dict, rec: dict, rec_by: dict, *,
                  start: str | None = None, bars=None, fees=None,
                  regime=None) -> dict:
    if rec.get("universe") == "combo" or rec.get("members"):
        members = []
        for n in rec.get("members") or []:
            kid = rec_by.get(n)
            if kid is None:
                raise KeyError(f"missing combo member {n}")
            members.append(kid)
        weights = rec.get("weights") or [1] * len(members)
        if (rec.get("pool") or "shared") == "split":
            return fmc.simulate_split(
                panel, members, weights, bars=bars, fees=fees,
                regime=regime, start=start, name=rec["name"])
        return fmc.simulate_shared(
            panel, members, weights, bars=bars, fees=fees, regime=regime,
            start=start, net=rec.get("net") or "priority", name=rec["name"])
    return fmb.simulate_book(
        panel, rec, bars=bars, fees=fees, regime=regime, start=start)


def score_holdout(payload: dict, full_panel: dict, *,
                  cutoff: str = DEFAULT_CUTOFF,
                  oos_start: str | None = None,
                  names: list[str] | None = None,
                  bars=None, fees=None, regime=None) -> list[dict]:
    """Replay frozen names on the full tape. Does not re-pick KEEP."""
    oos_start = oos_start or _next_session(full_panel, cutoff)
    rec_by = rec_by_name(payload)
    stats_by = {s["name"]: s for s in (payload.get("stats") or []) if s.get("name")}
    fees = fees if fees is not None else fm.pt_fees()
    regime = regime if regime is not None else fmb.load_regime()
    panel = fm.rehydrate_panel(full_panel)
    rows = []
    for name in (names if names is not None else report_names(payload)):
        rec = rec_by.get(name)
        is_stat = stats_by.get(name) or {}
        passed = bool(is_stat) and fm.is_workable_stat(is_stat)
        always = name in fm.WORKABLE_ALWAYS
        row = {
            "name": name,
            "side": (rec or is_stat).get("side") if rec or is_stat else None,
            "hold": (rec or is_stat).get("hold") if rec or is_stat else None,
            "always": always,
            "selected_is": passed,
            "is_book_pct": is_stat.get("total_ret_pct"),
            "is_win_rate": is_stat.get("win_rate"),
            "is_dollar_days": is_stat.get("profitable_day_rate"),
            "is_start_rate": is_stat.get("start_rate"),
            "is_n_trades": is_stat.get("book_n_trades") or is_stat.get("n_trades"),
            "is_start_green": is_stat.get("start_green"),
            "is_start_n": is_stat.get("start_n"),
            "missing_recipe": rec is None,
        }
        if rec is None:
            rows.append(row)
            continue
        print(f"[asof] replay {name}", flush=True)
        full = replay_recipe(
            panel, rec, rec_by, bars=bars, fees=fees, regime=regime)
        eq_is = equity_on(full, cutoff)
        eq_end = fm._finite(full.get("final_equity"))
        cont = None
        if eq_is and eq_is != 0 and eq_end is not None:
            cont = round(100.0 * (float(eq_end) / eq_is - 1.0), 3)
        oos_days, oos_hit, oos_n = oos_day_rate(full, oos_start)
        fresh = replay_recipe(
            panel, rec, rec_by, start=oos_start, bars=bars, fees=fees,
            regime=regime)
        fresh_days, _, fresh_n = oos_day_rate(fresh, oos_start)
        row.update({
            "oos_book_pct_continued": cont,
            "oos_dollar_days": oos_days,
            "oos_n_days": oos_n,
            "oos_n_hit": oos_hit,
            "oos_eq_is": None if eq_is is None else round(float(eq_is), 2),
            "oos_eq_end": None if eq_end is None else round(float(eq_end), 2),
            "full_book_pct": full.get("total_ret_pct"),
            "fresh_book_pct": fresh.get("total_ret_pct"),
            "fresh_dollar_days": fresh_days,
            "fresh_n_days": fresh_n,
            "fresh_n_trades": fresh.get("n_trades"),
            "audit_ok": bool((full.get("audit") or {}).get("ok", True)),
        })
        rows.append(row)
    return rows


def _next_session(panel: dict, cutoff: str) -> str:
    cal = list(panel.get("session_dates") or [])
    later = [d for d in cal if d > cutoff]
    return later[0] if later else cutoff


def _md_pct(v) -> str:
    return "—" if v is None else f"{100 * float(v):.0f}%"


def _md_pp(v) -> str:
    return "—" if v is None else f"{float(v):+.2f}"


def _verdict_lines(rows: list[dict], cutoff: str, oos_end: str) -> list[str]:
    by = {r["name"]: r for r in rows}
    passed = [r for r in rows if r.get("selected_is")]
    always_fail = [
        r for r in rows
        if r.get("always") and not r.get("selected_is")
    ]
    still = []
    faded = []
    for r in passed:
        cont = r.get("oos_book_pct_continued")
        days = r.get("oos_dollar_days")
        if cont is None:
            faded.append(r)
            continue
        ok_book = float(cont) > 0
        ok_days = days is None or float(days) >= 0.40
        if ok_book and ok_days:
            still.append(r)
        else:
            faded.append(r)

    def names(xs):
        return ", ".join(f"`{r['name']}`" for r in xs) or "none"

    focus_bits = []
    for n in FOCUS:
        r = by.get(n)
        if not r:
            focus_bits.append(f"- `{n}`: not in the 9/9 payload (recipe missing).")
            continue
        sel = "would KEEP" if r.get("selected_is") else "would NOT pass the 9/9 bar"
        if r.get("always") and not r.get("selected_is"):
            sel += " (ALWAYS extra — still reported)"
        cont = _md_pp(r.get("oos_book_pct_continued"))
        fresh = _md_pp(r.get("fresh_book_pct"))
        days = _md_pct(r.get("oos_dollar_days"))
        focus_bits.append(
            f"- `{n}`: {sel}. IS book {_md_pp(r.get('is_book_pct'))}% · "
            f"OOS continued {cont}% · fresh $10k {fresh}% · OOS $days {days}."
        )

    hot = by.get("union_hot_n4_h1") or {}
    holdup = by.get("union_hot_n4_holdup") or {}
    sh = [by[n] for n in (
        "combo_sh_5050_shared", "combo_sh_macd_5050_shared",
        "combo_sh_3070_shared", "combo_sh_7030_shared",
    ) if n in by]
    sh_keep = [r for r in sh if r.get("selected_is")]
    sh_work = [
        r for r in sh_keep
        if (r.get("oos_book_pct_continued") is not None
            and float(r["oos_book_pct_continued"]) > 0)
    ]
    if sh_keep and sh_work and len(sh_work) == len(sh_keep):
        sh_ans = (
            "The sh combos that the 9/9 bar would KEEP still print a "
            "positive continued book on 9/10–end — they are not only a "
            "9/10–9/18 artifact."
        )
    elif sh_keep and not sh_work:
        sh_ans = (
            "The sh combos that looked KEEP-able on 9/9 data fade after "
            "the cut — the live-board shine is 9/10–9/18 overfit."
        )
    elif sh_keep:
        sh_ans = (
            "Sh combos are mixed after the cut: "
            + ", ".join(
                f"`{r['name']}` continued {_md_pp(r.get('oos_book_pct_continued'))}%"
                for r in sh_keep
            )
            + "."
        )
    else:
        sh_ans = (
            "No `combo_sh_*` name passes the 9/9 KEEP bar. The live-board "
            "sh winners were not selectable without 9/10–9/18."
        )

    def _survives(r: dict) -> bool:
        if not r.get("selected_is"):
            return False
        cont = r.get("oos_book_pct_continued")
        return cont is not None and float(cont) > 0

    hot_ans = (
        "`union_hot_n4_h1` survives a clean remine."
        if _survives(hot) else
        "`union_hot_n4_h1` does not survive a clean remine "
        "(fails the 9/9 bar or the OOS continued book)."
    )
    hold_ans = (
        "`union_hot_n4_holdup` survives a clean remine."
        if _survives(holdup) else
        "`union_hot_n4_holdup` does not survive a clean remine "
        "(fails the 9/9 bar or the OOS continued book)."
    )

    return [
        "## Verdict",
        "",
        f"Cutoff **{cutoff}** inclusive. Holdout **{DEFAULT_OOS_START} → {oos_end}**.",
        f"Recipes that the 9/9 bar would KEEP: **{len(passed)}**. "
        f"ALWAYS extras that fail the 9/9 bar: **{len(always_fail)}**.",
        "",
        f"Still working after the cut (KEEP on 9/9 **and** continued OOS book% > 0 "
        f"with $days still in the conversation): {names(still)}.",
        "",
        f"Only looked good because of 9/10–{oos_end[-5:] if oos_end else 'end'} "
        f"(KEEP on 9/9, then faded): {names(faded)}.",
        "",
        hot_ans,
        hold_ans,
        sh_ans,
        "",
        "This is a **full-grid remine** on the 8/13–9/9 slice (auto + auto-tweak "
        "+ featured unions/combos/holdup/overnight/shorts/Clock-B), not a slice "
        "of the already-pruned live 25. Excel fee-KEEP prove is a separate path "
        "and is not in this recipe set.",
        "",
        "### Focus names",
        "",
        *focus_bits,
        "",
    ]


def render_asof_md(payload: dict, rows: list[dict], *,
                   cutoff: str, oos_start: str, oos_end: str) -> str:
    n_mined = int(payload.get("n_mined") or payload.get("n_recipes") or 0)
    bar = fm.WORKABLE_BAR
    lines = [
        f"# Factor mine as-of {cutoff} — overfit remine",
        "",
        f"In-sample: **{payload.get('from_date')} → {cutoff}** "
        f"({payload.get('n_sessions')} sessions, {payload.get('n_rows')} rows). "
        f"Out-of-sample: **{oos_start} → {oos_end}**.",
        "",
        f"Recipe grid: auto slice + auto-tweak neighbors + featured "
        f"unions / combos / holdup / overnight / shorts / Clock-B catalogue. "
        f"Mined **{n_mined}** sleeves on 9/9 data only. "
        f"KEEP selection never saw {oos_start}–{oos_end}.",
        "",
        "Same WORKABLE_BAR as live: "
        f"min_trades {bar['min_trades']}, min_win {bar['min_win']}, "
        f"min_book_pct {bar['min_book_pct']}, min_start {bar['min_start']}, "
        f"min_dollar_days {bar['min_dollar_days']}.",
        "",
        "OOS **continued** walks the 9/9 cash book forward (lots + leftover). "
        "OOS **fresh $10k** wakes the same frozen recipe on "
        f"{oos_start} with empty lots.",
        "",
        "Live `dashboard/factor-mine/` and `03_scoreboard/factor_mine.json` "
        "are the full-window 8/13–9/18 pack and were not written.",
        "",
        "## KEEP on 9/9 data + OOS holdout",
        "",
        "| Strategy | Side | Selected 9/9 | ALWAYS | "
        "IS win% | IS $days | IS start | IS n | IS book% | "
        "OOS cont book% | OOS $days | Fresh $10k | Fresh $days |",
        "|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for r in rows:
        sel = "YES" if r.get("selected_is") else "no"
        alw = "yes" if r.get("always") else ""
        start = "—"
        if r.get("is_start_n"):
            start = f"{r.get('is_start_green') or 0}/{r.get('is_start_n')}"
        lines.append(
            f"| `{r['name']}` | {r.get('side') or '—'} | {sel} | {alw} | "
            f"{_md_pct(r.get('is_win_rate'))} | {_md_pct(r.get('is_dollar_days'))} | "
            f"{start} | {r.get('is_n_trades') or 0} | {_md_pp(r.get('is_book_pct'))} | "
            f"{_md_pp(r.get('oos_book_pct_continued'))} | {_md_pct(r.get('oos_dollar_days'))} | "
            f"{_md_pp(r.get('fresh_book_pct'))} | {_md_pct(r.get('fresh_dollar_days'))} |"
        )
    lines += ["", *_verdict_lines(rows, cutoff, oos_end)]
    return "\n".join(lines) + "\n"


def write_holdout_report(payload: dict, full_panel: dict, *,
                         cutoff: str | None = None,
                         oos_start: str | None = None,
                         out_root: Path | None = None,
                         asof_md: Path | None = None,
                         bars=None, fees=None, regime=None) -> dict:
    cutoff = cutoff or payload.get("to_date") or DEFAULT_CUTOFF
    full_panel = fm.rehydrate_panel(full_panel)
    oos_start = oos_start or _next_session(full_panel, cutoff)
    cal = list(full_panel.get("session_dates") or [])
    oos_end = cal[-1] if cal else oos_start
    rows = score_holdout(
        payload, full_panel, cutoff=cutoff, oos_start=oos_start,
        bars=bars, fees=fees, regime=regime)
    md = render_asof_md(
        payload, rows, cutoff=cutoff, oos_start=oos_start, oos_end=oos_end)
    blob = {
        "generated_at": datetime.now(tl.ET).isoformat(),
        "cutoff": cutoff,
        "oos_start": oos_start,
        "oos_end": oos_end,
        "bar": dict(fm.WORKABLE_BAR),
        "always": list(fm.WORKABLE_ALWAYS),
        "n_report": len(rows),
        "n_selected_is": sum(1 for r in rows if r.get("selected_is")),
        "rows": rows,
    }
    if out_root:
        out_root = Path(out_root)
        out_root.mkdir(parents=True, exist_ok=True)
        (out_root / "holdout.json").write_text(
            json.dumps(blob, indent=2), encoding="utf-8")
        (out_root / "FACTOR_MINE_HOLDOUT.md").write_text(md, encoding="utf-8")
    dest_md = Path(asof_md) if asof_md else None
    if dest_md:
        dest_md.parent.mkdir(parents=True, exist_ok=True)
        dest_md.write_text(md, encoding="utf-8")
        print(f"[asof] wrote {dest_md}", flush=True)
    return blob
