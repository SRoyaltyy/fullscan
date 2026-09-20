"""Factor-mine Clock-B long KEEP — fee-aware prove.

Research only. Live flatten_robust / cash book are not imported or written.

Scores the three remine-board longs that printed cash Book% after
oppset-union remine 35458265920. Cash Book% is not KEEP.

Recipe definitions come from ``src/clock_b_tells.py`` (#279 wire).
Scoring is the CATALOGUE_COMBO_KEEP / excel_factor_mine fee path:
time-split holdout, after-fee H, ≥30 prove fires, WR strictly > 55%.
Futubull FEE_RT=0.0015. Lift-only is never KEEP. Thin n is thin-n
(not KEEP).

Aisle = restored multi-src morning panel ∪ Theme Radar Clock-B
oppset. Recipes that require opp fire only on oppset membership.
Clock-B atoms are 09:30-knowable (join_morning + finviz_asof T−1 /
panel prior tape). Same-day Gap / minute Performance are never features.

  python3 excel_bot/engine/factor_clk_long_keep.py
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
REPO = os.path.dirname(ROOT)
sys.path.insert(0, HERE)
sys.path.insert(0, REPO)

from excel_clock_gate import gate_payload  # noqa: E402
from excel_factor_mine import (  # noqa: E402
    cutoff_from_dates,
    keep_verdict,
    score_hits,
    split_rows,
    walk_folds,
)
from join_post_813 import FEE_RT  # noqa: E402
from j_winrate import FEE_CAVEAT, MIN_FIRES, WIN_BAR  # noqa: E402
from catalogue_combo_keep import (  # noqa: E402
    FORBIDDEN_FEATURE_FIELDS,
    HOLD_FRAC,
    OPPSET_PATH,
    OPPSET_SOURCE,
    PANEL_PATH,
    SCOREBOARD,
    aisle_rows,
    leak_check,
    list_finviz_dates,
    load_finviz_labels,
    load_oppset_flagged,
    load_panel,
    prior_finviz_date,
)
from src import clock_b_tells as cbt  # noqa: E402

BOARD_MD = os.path.join(SCOREBOARD, "FACTOR_CLK_LONG_KEEP.md")
BOARD_JSON = os.path.join(SCOREBOARD, "factor_clk_long_keep.json")
REMINE = "35458265920"

# Feature keys clock_b_tells evaluators may read. H/I/Gap/Change/RelVol
# and minute Performance stay labels / leaks, never flags.
FEATURE_KEYS = frozenset({
    "ohlc_ret_1", "ohlc_ret_5", "ohlc_ret_10", "ohlc_rvol",
    "ohlc_hot_score", "ohlc_nr7", "ohlc_break_10",
    "last_green", "last_red", "candle_capture",
    "macd_up", "macd_down", "rsi_ob", "rsi_os", "rsi",
    "flow_in", "alarm", "boxes", "rs_week",
    "news_box", "news_prior", "erd_earn_react",
    "erd_flag_E", "erd_days_since_E", "erd_flag_R", "erd_days_since_R",
    "e_pol", "fv_inst", "ins_buy", "form4_buy",
    "oppset", "sources",
})

# Cash Book% on the remine board — context only. Not a KEEP input.
RECIPES = (
    {
        "id": 1,
        "name": "union_clk_mom_break_peer_opp_h1",
        "clk": "clk_mom_break_peer",
        "need_opp": True,
        "title": "Clock-B #1 ∩ Theme Radar T−1 oppset",
        "thesis": "continuation on flagged opportunity-set names",
        "have": "panel ohlc_ret_5 / last_green / macd_up / ohlc_break_10 / "
                "candle_capture / boxes.peer|sector / rs_week; Theme Radar "
                "T−1 oppset membership",
        "cash_book_pct": 13.82,
        "cash_starts": "19/26",
        "note": "Cash Book% +13.82% on remine 35458265920 is not KEEP. "
                "Longs also forbid clk_ext_veto and alarm (#279 wire).",
    },
    {
        "id": 2,
        "name": "union_clk_nr7_mom_h1",
        "clk": "clk_nr7_mom",
        "need_opp": False,
        "title": "Clock-B #10 NR7 compression + moderate momentum",
        "thesis": "coil then continuation",
        "have": "panel ohlc_nr7 / ohlc_ret_5 / last_green / macd_up",
        "cash_book_pct": 12.48,
        "cash_starts": "26/26",
        "note": "Cash Book% +12.48% with 26/26 starts is not KEEP. "
                "May be thin on holdout fires. Does not require oppset.",
    },
    {
        "id": 3,
        "name": "union_clk_hold_vs_sector_opp_h1",
        "clk": "clk_hold_vs_sector",
        "need_opp": True,
        "title": "Clock-B #6 ∩ Theme Radar T−1 oppset",
        "thesis": "stock holds while sector camera is red, on oppset",
        "have": "boxes.sector bad + ohlc_ret_1 / last_green; Theme Radar "
                "T−1 oppset membership",
        "cash_book_pct": 8.41,
        "cash_starts": "26/26",
        "note": "Cash Book% +8.41% on remine 35458265920 is not KEEP.",
    },
)


def _tick(v):
    return str(v or "").strip().upper()


def _finite(x):
    if x is None:
        return None
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    if v != v or v in (float("inf"), float("-inf")):
        return None
    return v


def on_oppset(row):
    """Theme Radar T−1 flagged membership. Stamp or aisle source."""
    if row.get("oppset"):
        return True
    srcs = {str(s).strip().lower() for s in (row.get("sources") or [])}
    return bool(srcs & {"oppset", "oppset_clock_b"})


def feature_row(row):
    """Clock-B inputs only. Drop labels and same-day tape leaks."""
    out = {}
    for k, v in (row or {}).items():
        if k in FORBIDDEN_FEATURE_FIELDS or k in cbt.LEAK_FIELDS:
            continue
        if k in ("H", "I", "net", "open", "close", "high", "low"):
            continue
        out[k] = v
    leak = set(out) & FORBIDDEN_FEATURE_FIELDS
    if leak:
        raise ValueError(f"LEAK abort: feature row stored {sorted(leak)}")
    leak2 = set(out) & cbt.LEAK_FIELDS
    if leak2:
        raise ValueError(f"LEAK abort: feature row stored Clock-B leaks {sorted(leak2)}")
    return out


def assert_atoms_legal():
    leak = FEATURE_KEYS & FORBIDDEN_FEATURE_FIELDS
    if leak:
        raise ValueError(f"LEAK abort: feature keys include {sorted(leak)}")
    leak2 = FEATURE_KEYS & cbt.LEAK_FIELDS
    if leak2:
        raise ValueError(f"LEAK abort: feature keys include Clock-B leaks {sorted(leak2)}")
    return True


def recipe_defs_from_wire():
    """#279 clock_b_recipes require/forbid for the three longs."""
    veto = {"clk_ext_veto": True, "alarm": True}

    def rec(name, **kw):
        return {
            "name": name,
            "universe": "union",
            "hold": 1,
            "side": "long",
            "require": dict(kw.get("require") or {}),
            "forbid": dict(kw.get("forbid") or veto),
        }

    return {
        "union_clk_mom_break_peer_opp_h1": rec(
            "union_clk_mom_break_peer_opp_h1",
            require={"clk_mom_break_peer": True, "oppset": True},
        ),
        "union_clk_nr7_mom_h1": rec(
            "union_clk_nr7_mom_h1",
            require={"clk_nr7_mom": True},
        ),
        "union_clk_hold_vs_sector_opp_h1": rec(
            "union_clk_hold_vs_sector_opp_h1",
            require={"clk_hold_vs_sector": True, "oppset": True},
        ),
    }


def recipe_fires(row, spec):
    """#279 long gate: Clock-B combo ∧ (oppset if required) ∧ not veto/alarm.

    Evaluates clock_b_tells directly. Live lookback modules stay out.
    """
    feats = row.get("feats") or {}
    if spec.get("need_opp") and not row.get("oppset"):
        return False
    if feats.get("alarm") or row.get("alarm"):
        return False
    if cbt.combo_true(feats, "clk_ext_veto"):
        return False
    return bool(cbt.combo_true(feats, spec["clk"]))


def board_verdict(n, wr):
    """KEEP / FAIL / thin-n. Thin n is never KEEP."""
    v, why = keep_verdict(n, wr)
    if v == "KEEP":
        return "KEEP", why
    if not n:
        return "FAIL", why
    if int(n) < MIN_FIRES:
        return "thin-n", why
    return "FAIL", why


def name_days_from_aisle(panel, export_dir=None, oppset_by_date=None):
    """Open-knowable aisle name-days. H is a label from same-day OHLC."""
    raw, aisle_dates, skipped, stats = aisle_rows(panel, oppset_by_date)
    fv_dates = list_finviz_dates(export_dir)
    label_cache = {}

    def _labels(iso):
        if iso not in label_cache:
            label_cache[iso] = load_finviz_labels(iso, export_dir)
        return label_cache[iso]

    rows = []
    n_label_fv = 0
    for r in raw:
        iso = str(r.get("date") or "")[:10]
        tk = _tick(r.get("ticker"))
        o, c = _finite(r.get("open")), _finite(r.get("close"))
        if (not o or not c or o <= 0) and iso and tk:
            lab = _labels(iso).get(tk) or {}
            o = o or lab.get("open")
            c = c or lab.get("close")
            if lab.get("open") and lab.get("close"):
                n_label_fv += 1
        if not o or not c or o <= 0:
            continue
        stamped = str(
            r.get("finviz_asof") or r.get("news_export_date")
            or r.get("prior_date") or ""
        )[:10]
        if stamped == iso:
            raise ValueError(f"LEAK abort: Finviz date {stamped} is same-row as {iso}")
        t1 = prior_finviz_date(iso, fv_dates, r)
        if t1 == iso:
            raise ValueError(f"LEAK abort: Finviz date {t1} is same-row as {iso}")
        feats = feature_row(r)
        H = (c - o) / o
        rows.append({
            "date": iso,
            "ticker": tk,
            "H": H,
            "net": H - FEE_RT,
            "feats": feats,
            "oppset": on_oppset(r),
            "alarm": bool(r.get("alarm")),
            "sources": list(r.get("sources") or []),
        })
    stats["n_label_finviz_t"] = n_label_fv
    stats["n_scored"] = len(rows)
    return rows, aisle_dates, skipped, stats


def recipe_hits(rows, spec):
    hits = []
    for r in rows:
        if r.get("net") is None:
            continue
        if recipe_fires(r, spec):
            hits.append({
                "net": r["net"],
                "date": r.get("date"),
                "ticker": r.get("ticker"),
            })
    return hits


def score_recipe(rows, spec):
    hits = recipe_hits(rows, spec)
    scored = score_hits(hits)
    v, why = board_verdict(scored.get("n") or 0, scored.get("wr"))
    scored["verdict"] = v
    scored["why"] = why
    return scored


def score_space(disc, hold):
    folds = walk_folds([r["date"] for r in disc])
    fold_sets = []
    for lo, hi, chunk in folds:
        s = set(chunk)
        fold_sets.append((lo, hi, [r for r in disc if r["date"] in s]))
    out = []
    for spec in RECIPES:
        d = score_recipe(disc, spec)
        h = score_recipe(hold, spec)
        rec = {
            "id": spec["id"],
            "name": spec["name"],
            "clk": spec["clk"],
            "need_opp": spec["need_opp"],
            "title": spec["title"],
            "thesis": spec["thesis"],
            "have": spec["have"],
            "note": spec["note"],
            "side": "long",
            "cash_book_pct": spec["cash_book_pct"],
            "cash_starts": spec["cash_starts"],
            "disc": d,
            "hold": h,
            "verdict": h["verdict"],
            "why": h["why"],
        }
        walk = []
        n_ok = 0
        for lo, hi, frows in fold_sets:
            fs = score_recipe(frows, spec)
            walk.append({
                "lo": lo, "hi": hi, "n": fs["n"], "wr": fs["wr"],
                "verdict": fs["verdict"],
            })
            if fs["n"] and fs["wr"] is not None and fs["wr"] > 0.50:
                n_ok += 1
        rec["walk"] = walk
        rec["walk_folds_wr_gt_50"] = n_ok
        rec["n_walk_folds"] = len(fold_sets)
        out.append(rec)
    return out


def _pct(wr):
    if wr is None:
        return "—"
    return f"{100 * wr:.1f}%"


def _mean_net(rec):
    v = (rec or {}).get("mean_net")
    if v is None:
        return "—"
    return f"{v:+.4f}"


def headline_from(scored, cutoff, baseline=None):
    keeps = [r for r in scored if r.get("verdict") == "KEEP"]
    thins = [r for r in scored if r.get("verdict") == "thin-n"]
    fails = [r for r in scored if r.get("verdict") == "FAIL"]
    if keeps:
        bits = [f"`{r['name']}` {r['why']}" for r in keeps]
        return {
            "verdict": "KEEP",
            "n_keep": len(keeps),
            "text": (
                f"KEEP. {len(keeps)} of 3 Clock-B longs cleared "
                f"≥{MIN_FIRES} prove fires and >{100 * WIN_BAR:.0f}% "
                f"after-fee H (cutoff {cutoff}): " + "; ".join(bits) + ". "
                "Cash Book% is not KEEP."
            ),
        }
    material = [r for r in scored if (r.get("hold") or {}).get("n", 0) >= MIN_FIRES]
    ranked = material or [r for r in scored if (r.get("hold") or {}).get("n")]
    best = None
    if ranked:
        best = max(ranked, key=lambda r: (
            ((r.get("hold") or {}).get("wr") or 0),
            (r.get("hold") or {}).get("n") or 0,
        ))
    extra = ""
    if best:
        h = best["hold"]
        extra = (
            f" Best near-miss: `{best['name']}` prove n={h.get('n')} "
            f"after-fee WR {_pct(h.get('wr'))}."
        )
    if baseline and baseline.get("n"):
        extra += (
            f" Aisle baseline prove n={baseline['n']} after-fee WR "
            f"{_pct(baseline.get('wr'))}."
        )
    text = (
        f"FAIL vs goal (b). 0 of 3 Clock-B longs cleared the Cyrus fee-KEEP "
        f"bar on holdout (cutoff {cutoff}). {len(fails)} FAIL, "
        f"{len(thins)} thin-n.{extra} Cash Book% is not KEEP. Lift-only "
        f"is not KEEP. Touch-rate already improved +3.3pp on Taskforce "
        f"(goal a partial)."
    )
    return {
        "verdict": "FAIL",
        "n_keep": 0,
        "text": text,
        "best": None if not best else {
            "name": best["name"],
            "n": (best.get("hold") or {}).get("n"),
            "wr": (best.get("hold") or {}).get("wr"),
        },
    }


def write_board(payload, path=None):
    path = path or BOARD_MD
    gp = payload["gate"]
    hl = payload["headline"]
    scored = payload["scored"]
    base = payload.get("baseline_hold") or {}
    lines = [
        "# Factor-mine Clock-B long KEEP — fee-aware prove",
        "",
        f"status={payload.get('status', 'DONE')} verdict=**{hl['verdict']}** "
        f"TIME-SPLIT cutoff={payload.get('cutoff')} "
        f"aisle_days={payload.get('n_aisle_dates')} "
        f"name-days={payload.get('n_rows')} "
        f"KEEP={hl.get('n_keep', 0)} thin-n={payload.get('n_thin', 0)} "
        f"FAIL={payload.get('n_fail', 0)}",
        "",
        "Research only. Live `flatten_robust` is not imported and is not written.",
        "Cash Book% on the remine board is **not** KEEP.",
        "",
        "## Headline",
        "",
        hl["text"],
        "",
        "## KEEP bar",
        "",
        f"Cyrus KEEP: **≥{MIN_FIRES} prove fires** and **after-fee H win rate "
        f"> {100 * WIN_BAR:.0f}%**. After-fee H = open-to-close minus "
        f"{FEE_RT * 10000:.0f} bp Futubull (`FEE_RT={FEE_RT}`). A fire is "
        "an aisle name-day (multi-src panel ∪ Clock-B oppset) where the "
        "#279 recipe gate is true at the 09:30 open. "
        "**Lift-only is never KEEP.** Thin n that prints >55% is **thin-n** "
        "(not KEEP). Discovery cannot KEEP. Cash Book% cannot KEEP. "
        f"{FEE_CAVEAT}",
        "",
        "## Goal",
        "",
        "- (a) Touch-rate: already improved +3.3pp on Taskforce — partial, "
        "out of scope here.",
        "- (b) Tangible factor-mine selection improvement: a Clock-B long "
        "that clears this fee-KEEP bar. **0 KEEP = FAIL vs goal (b).**",
        "",
        "## Clock lock",
        "",
        f"- Gate: `{gp['gate']}`",
        f"- Same-row leak abort: `{', '.join(gp['same_row_leak_abort'])}`",
        f"- Leak check: **{payload['leak']}**",
        "- Features: `#279` `clock_b_tells` atoms on `join_morning` (T) + "
        "`finviz_asof` (T−1) / panel prior tape. Open is the fill, not a "
        "feature. Same-day Gap / Change / RelVol / minute Performance* are "
        "never Clock B and never flags. H/I are labels only.",
        f"- Split: TIME-SPLIT last {HOLD_FRAC:.0%} of aisle "
        f"session dates (cutoff `{payload.get('cutoff')}`). Discovery "
        "feature date is strictly before cutoff.",
        f"- Aisle: restored multi-src morning panel "
        f"(`lookback={payload.get('lookback')}`) ∪ Theme Radar Clock-B "
        f"flagged oppset (`{payload.get('oppset_source') or OPPSET_SOURCE}`). "
        "Recipes that require opp fire only on oppset membership. Oppset "
        "gap/RelVol flags are T−1 membership only (VOL/CROWD aisle, not "
        "direction atoms). Flatten-only / starved days stay out unless "
        "the oppset covers that morning.",
        "- Live: `flatten_robust` not imported, not written. The miner "
        "module is not loaded (avoids live lookback).",
        f"- Remine input: GitHub Actions `{REMINE}` "
        f"(FULLSCAN_OPPSET_UNION=1, from=2026-08-13, rebuild_panel=true).",
        "",
        "## Aisle",
        "",
        f"Panel `{payload.get('panel_to')}` n_rows={payload.get('panel_n')} "
        f"lookback=`{payload.get('lookback')}`. "
        f"Oppset `{payload.get('oppset_source') or OPPSET_SOURCE}` "
        f"flagged={payload.get('n_oppset_flagged', '—')}. "
        f"Aisle mix: panel={payload.get('n_aisle_panel', '—')} "
        f"overlap={payload.get('n_aisle_overlap', '—')} "
        f"oppset_only={payload.get('n_aisle_oppset_only', '—')} "
        f"scored={payload.get('n_rows')}. "
        f"Oppset-only labels use same-day Finviz Open→Price "
        f"(n={payload.get('n_label_finviz_t', 0)}); not features. "
        f"Aisle days: {', '.join(payload.get('aisle_dates') or []) or '—'}.",
        "",
    ]
    skipped = payload.get("skipped_days") or {}
    if skipped:
        lines.append("Excluded days (not the restored aisle):")
        lines.append("")
        for iso, rec in skipped.items():
            lines.append(
                f"- `{iso}` n={rec.get('n')} sources={rec.get('sources')} "
                f"— {rec.get('why')}"
            )
        lines.append("")
    lines += [
        f"Holdout baseline (every aisle name-day, long): n={base.get('n', 0)} "
        f"after-fee WR {_pct(base.get('wr'))} mean_net="
        f"{_mean_net(base)}.",
        "",
        "## Cash Book% (not KEEP)",
        "",
        "These printed on the remine cash/recipe board. They are leftover "
        "top-8 books with hard-red sit — a different metric. They cannot "
        "KEEP a name-day fee prove.",
        "",
        "| recipe | remine Book% | starts | fee-KEEP |",
        "|---|---:|---|---|",
    ]
    for r in scored:
        lines.append(
            f"| `{r['name']}` | +{r.get('cash_book_pct'):.2f}% | "
            f"{r.get('cash_starts')} | **{r.get('verdict')}** |"
        )
    lines += [
        "",
        "## Per-recipe prove",
        "",
        "| # | recipe | Clock-B | opp | prove n | after-fee WR | verdict | notes |",
        "|--:|---|---|---|---:|---:|---|---|",
    ]
    for r in scored:
        h = r.get("hold") or {}
        lines.append(
            f"| {r['id']} | `{r['name']}` | `{r['clk']}` | "
            f"{'yes' if r.get('need_opp') else 'no'} | "
            f"{h.get('n', 0)} | {_pct(h.get('wr'))} | "
            f"**{r.get('verdict')}** | {r.get('why')} |"
        )
    lines += [
        "",
        "## Cards",
        "",
    ]
    for r in scored:
        h, d = r.get("hold") or {}, r.get("disc") or {}
        lines += [
            f"### {r['id']}. `{r['name']}`",
            "",
            f"**{r.get('verdict')}** — {r.get('why')}",
            "",
            f"- Thesis: {r['thesis']}. Side `long`. Hold 1.",
            f"- Gate: `{r['clk']}=True`"
            + (", `oppset=True`" if r.get("need_opp") else "")
            + "; forbid `clk_ext_veto` + `alarm`.",
            f"- HAVE / calculable: {r.get('have') or '—'}",
            f"- Remine cash Book% +{r.get('cash_book_pct'):.2f}% "
            f"(starts {r.get('cash_starts')}) — not KEEP.",
            f"- Discovery n={d.get('n', 0)} after-fee WR {_pct(d.get('wr'))} "
            f"mean_net={_mean_net(d)} (not KEEP).",
            f"- Prove n={h.get('n', 0)} after-fee WR {_pct(h.get('wr'))} "
            f"mean_net={_mean_net(h)} n_pos={h.get('n_pos', 0)}.",
        ]
        if r.get("note"):
            lines.append(f"- {r['note']}")
        lines.append("")
    lines += [
        "## Discovery (not KEEP)",
        "",
        "Discovery ranks honesty only. A discovery >55% print is not a call.",
        "",
        "| recipe | disc n | disc after-fee WR |",
        "|---|---:|---:|",
    ]
    for r in scored:
        d = r.get("disc") or {}
        lines.append(f"| `{r['name']}` | {d.get('n', 0)} | {_pct(d.get('wr'))} |")
    lines += [
        "",
        "## Walk-forward (discovery folds, not KEEP)",
        "",
        "| recipe | fold1 n / WR | fold2 n / WR | fold3 n / WR |",
        "|---|---|---|---|",
    ]
    for r in scored:
        bits = []
        for w in r.get("walk") or []:
            bits.append(f"{w['n']} / {_pct(w['wr'])}")
        while len(bits) < 3:
            bits.append("—")
        lines.append(f"| `{r['name']}` | {bits[0]} | {bits[1]} | {bits[2]} |")
    lines += [
        "",
        "## Explicit verdict",
        "",
    ]
    keep_l = [r for r in scored if r.get("verdict") == "KEEP"]
    fail_l = [r for r in scored if r.get("verdict") == "FAIL"]
    thin_l = [r for r in scored if r.get("verdict") == "thin-n"]
    lines.append(
        "**KEEP:** " + (", ".join(f"`{r['name']}`" for r in keep_l) or "none")
    )
    lines.append(
        "**FAIL:** " + (", ".join(f"`{r['name']}`" for r in fail_l) or "none")
    )
    lines.append(
        "**thin-n:** " + (", ".join(f"`{r['name']}`" for r in thin_l) or "none")
    )
    if not keep_l:
        lines += [
            "",
            "**Goal (b):** FAIL. No long cleared ≥30 prove fires and "
            ">55% after-fee H.",
        ]
    lines += [
        "",
        "## Explicitly not live",
        "",
        "No recipe is wired into `flatten_robust` or cash/paper. A KEEP here "
        "would be a research card, not a ship. Do not train ML on "
        "flatten-only starved days.",
        "",
        "## Source",
        "",
        "`src/clock_b_tells.py` (#279 recipe wire) · "
        "`excel_clock_gate.py` / `CLOCK_MAP.md` · `j_winrate.py` "
        f"(`WIN_BAR`, `MIN_FIRES`, `FEE_RT={FEE_RT}`) · "
        f"Clock-B oppset `{OPPSET_SOURCE}` · "
        f"oppset-union remine `{REMINE}` · "
        "restored `data/factor_mine/panel.json`. Research only.",
        "",
    ]
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    open(path, "w", encoding="utf-8").write("\n".join(lines))
    return path


def slim(r):
    def sl(s):
        if not s:
            return s
        return {
            "n": s.get("n"), "n_pos": s.get("n_pos"), "wr": s.get("wr"),
            "mean_net": s.get("mean_net"), "verdict": s.get("verdict"),
            "why": s.get("why"),
        }
    return {
        "id": r.get("id"), "name": r.get("name"), "clk": r.get("clk"),
        "need_opp": r.get("need_opp"), "title": r.get("title"),
        "thesis": r.get("thesis"), "have": r.get("have"),
        "note": r.get("note"), "side": r.get("side"),
        "cash_book_pct": r.get("cash_book_pct"),
        "cash_starts": r.get("cash_starts"),
        "disc": sl(r.get("disc")), "hold": sl(r.get("hold")),
        "verdict": r.get("verdict"), "why": r.get("why"),
        "walk": r.get("walk"),
        "walk_folds_wr_gt_50": r.get("walk_folds_wr_gt_50"),
        "n_walk_folds": r.get("n_walk_folds"),
    }


def run(panel_path=None, export_dir=None, oppset_path=None,
        out_md=None, out_json=None):
    assert_atoms_legal()
    clocks, leak = leak_check()
    panel = load_panel(panel_path)
    oppset = load_oppset_flagged(oppset_path or OPPSET_PATH)
    rows, aisle_dates, skipped, aisle_stats = name_days_from_aisle(
        panel, export_dir, oppset,
    )
    dates = sorted({r["date"] for r in rows})
    cutoff = cutoff_from_dates(dates, hold_frac=HOLD_FRAC, locked=None)
    disc, hold = split_rows(rows, cutoff)
    baseline_hold = score_hits(hold)
    baseline_disc = score_hits(disc)
    scored = score_space(disc, hold)
    hl = headline_from(scored, cutoff, baseline_hold)
    n_thin = sum(1 for r in scored if r.get("verdict") == "thin-n")
    n_fail = sum(1 for r in scored if r.get("verdict") == "FAIL")
    payload = {
        "status": "DONE",
        "generated": str(date.today()),
        "leak": leak,
        "live_untouched": "flatten_robust",
        "gate": gate_payload(),
        "fire_bar": {
            "win": f"after-fee H > 0 on prove name-days, strictly > {WIN_BAR}",
            "min_fires": MIN_FIRES,
            "fee_rt": FEE_RT,
            "fee_caveat": FEE_CAVEAT,
            "lift_never_keep": True,
            "cash_book_never_keep": True,
        },
        "goal": {
            "a": "touch-rate +3.3pp on Taskforce (partial, out of scope)",
            "b": "tangible factor-mine selection improvement via fee-KEEP long",
            "b_verdict": hl["verdict"] if hl.get("n_keep") else "FAIL",
        },
        "cutoff": cutoff,
        "split_kind": "time",
        "hold_frac": HOLD_FRAC,
        "lookback": panel.get("lookback"),
        "panel_to": panel.get("to_date"),
        "panel_n": panel.get("n_rows"),
        "aisle_dates": aisle_dates,
        "n_aisle_dates": len(aisle_dates),
        "skipped_days": skipped,
        "oppset_source": (aisle_stats or {}).get("oppset_source") or OPPSET_SOURCE,
        "n_oppset_flagged": sum(len(v) for v in oppset.values()),
        "n_aisle_panel": (aisle_stats or {}).get("n_panel"),
        "n_aisle_overlap": (aisle_stats or {}).get("n_overlap"),
        "n_aisle_oppset_only": (aisle_stats or {}).get("n_oppset_only"),
        "n_label_finviz_t": (aisle_stats or {}).get("n_label_finviz_t"),
        "n_tickers": len({r["ticker"] for r in rows}),
        "n_rows": len(rows),
        "n_disc": len(disc),
        "n_hold": len(hold),
        "n_keep": hl["n_keep"],
        "n_thin": n_thin,
        "n_fail": n_fail,
        "headline": hl,
        "remine": REMINE,
        "recipes_wire": "src/clock_b_tells.py#clock_b_recipes",
        "baseline_disc": {
            "n": baseline_disc.get("n"), "wr": baseline_disc.get("wr"),
            "mean_net": baseline_disc.get("mean_net"),
        },
        "baseline_hold": {
            "n": baseline_hold.get("n"), "wr": baseline_hold.get("wr"),
            "mean_net": baseline_hold.get("mean_net"),
            "verdict": baseline_hold.get("verdict"),
            "why": baseline_hold.get("why"),
        },
        "scored": [slim(r) for r in scored],
        "clocks_generated": clocks.get("generated"),
        "feature_keys": sorted(FEATURE_KEYS),
        "forbidden": sorted(FORBIDDEN_FEATURE_FIELDS),
    }
    md = write_board(payload, out_md or BOARD_MD)
    js = out_json or BOARD_JSON
    os.makedirs(os.path.dirname(js) or ".", exist_ok=True)
    json.dump(payload, open(js, "w"), indent=2, default=str)
    print("wrote", md, js, flush=True)
    print("headline", hl["verdict"], hl["text"], flush=True)
    return payload


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--panel", default="")
    ap.add_argument("--exports", default="")
    ap.add_argument("--oppset", default="")
    ap.add_argument("--out-md", default=BOARD_MD)
    ap.add_argument("--out-json", default=BOARD_JSON)
    args = ap.parse_args()
    run(panel_path=args.panel or None,
        export_dir=args.exports or None,
        oppset_path=args.oppset or None,
        out_md=args.out_md, out_json=args.out_json)


if __name__ == "__main__":
    main()
