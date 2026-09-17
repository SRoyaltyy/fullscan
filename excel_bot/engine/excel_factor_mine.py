"""Excel factor-mine Phase B — fee-aware combo prove.

Research only. Live flatten_robust is not imported or written.

Clock: excel_clock_gate / CLOCK_MAP / OPEN_SAME_ROW_LABELS.
Atoms are 09:30-knowable only (FILL_OPEN / VALUE_OPEN_44 lag0;
H/I close lag≥1; DF/BB/BQ same-row abort). Do not invent clocks.

KEEP bar (Cyrus): prove (holdout) n ≥ 30 AND after-fee H win rate > 55%.
Lift-only is never KEEP. Thin n that prints >55% is FAIL, not a wink.

  python3 excel_bot/engine/excel_factor_mine.py
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from collections import defaultdict
from datetime import date
from itertools import combinations

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
REPO = os.path.dirname(ROOT)
sys.path.insert(0, HERE)

from excel_clock_gate import (  # noqa: E402
    SAME_ROW_LEAK_ABORT,
    assert_excel_clock_gate,
    assert_feature_legal,
    gate_payload,
)
from excel_deep_corr_mine import (  # noqa: E402
    AVGVOL_MIN,
    HOLD_FRAC,
    MCAP_MIN_M,
    load_finviz_filter,
)
from excel_open_features import (  # noqa: E402
    assert_lag_atoms,
    feature_flags,
    open_features,
)
from join_post_813 import FEE_RT, is_session  # noqa: E402
from j_winrate import (  # noqa: E402
    FEE_CAVEAT,
    MIN_FIRES,
    WIN_BAR,
    fire_winrate,
    hit_rate,
)

SCOREBOARD = os.path.join(REPO, "03_scoreboard")
OHLC_PATH = os.path.join(REPO, "data", "prices", "ohlc.parquet")
BOARD_MD = os.path.join(SCOREBOARD, "EXCEL_FACTOR_MINE.md")
BOARD_JSON = os.path.join(SCOREBOARD, "excel_factor_mine.json")
CKPT_JSON = os.path.join(SCOREBOARD, "excel_factor_mine_ckpt.json")
DEEP_JSON = os.path.join(SCOREBOARD, "excel_deep_corr_mine.json")
DEEP_CUTOFF = "2026-07-06"
J_FRESH_DAYS = 5
PRIOR_VOL_LIQ = 1_000_000
WALK_FOLDS = 3
MIN_PRIOR_BARS = 8

# Clock-legal atoms. (col, lag) must pass assert_feature_legal.
# DF/DG/DH/BB/BQ/BU are lag t−1+ only. H/I are never features.
ATOM_SPEC = {
    "J_ge0": ("J", 0, "value"),
    "J_lt0": ("J", 0, "value"),
    "J_le-1": ("J", 0, "value"),
    "ER_m1": ("ER", 0, "value"),
    "ER_p1": ("ER", 0, "value"),
    "AH_ge1": ("AH", 0, "value"),
    "FQ": ("FQ", 0, "value"),
    "JB": ("JB", 0, "value"),
    "JC": ("JC", 0, "value"),
    "FR_ge1": ("FR", 0, "value"),
    "EP_ge03": ("EP", 0, "value"),
    "EN_ge2": ("EN", 0, "value"),
    "EN_le0": ("EN", 0, "value"),
    "el_neg": ("DF", 1, "value"),
    "el_pos": ("DF", 1, "value"),
    "prior_bearish": ("DF", 1, "value"),
    "prior_bullish": ("DF", 1, "value"),
    "prior_doji": ("DF", 1, "value"),
    "prior_hammer": ("DF", 1, "value"),
    "prior_hanging": ("DF", 1, "value"),
    "prior_shooting": ("DF", 1, "value"),
    "prior_bear_engulf": ("DG", 1, "value"),
    "prior_bull_engulf": ("DG", 1, "value"),
    "prior_morning": ("DH", 1, "value"),
    "prior_evening": ("DH", 1, "value"),
    "BB_l1": ("BB", 1, "value"),
    "BQ_l1_neg": ("BQ", 1, "value"),
    "BQ_l1_pos": ("BQ", 1, "value"),
    "BU_l1_neg": ("BU", 1, "value"),
    "BU_l1_pos": ("BU", 1, "value"),
}

# Deep-corr holdout survivors that are OHLC-reconstructable + open-gate letters.
CORE_SEEDS = (
    "J_ge0", "ER_m1", "prior_bear_engulf", "el_neg",
    "prior_bearish", "prior_hanging", "AH_ge1", "FQ",
)
OPEN_SEEDS = CORE_SEEDS + (
    "J_lt0", "J_le-1", "ER_p1", "JC", "JB", "FR_ge1", "EP_ge03", "EN_ge2",
    "prior_bullish", "prior_doji", "prior_hammer", "prior_bull_engulf",
    "prior_morning", "prior_evening", "BQ_l1_neg", "BU_l1_neg",
    "el_pos", "BB_l1",
)
# Excel fill-paint atoms from the lift board — need grids, not invented here.
GRID_ONLY_ATOMS = (
    "fill0_M_green", "fill0_A_red", "fill0_B_red", "fill0_L_red",
    "N_last20_r>=6", "N_last10_r>=3", "N_last20_r>=3",
    "M_last10_g>=3", "M_last5_g>r", "M_last10_g>r",
    "H_last5_r>=3", "I_last5_r>=3", "J_last10_g>=8",
    "in_A_red_region", "in_A_green_region",
)

_ATOM_ORDER = {name: i for i, name in enumerate(OPEN_SEEDS)}


def combo_atoms(name):
    return tuple(a for a in name.split("|") if a)


def combo_name(atoms):
    xs = list(atoms)
    xs.sort(key=lambda a: (_ATOM_ORDER.get(a, 99), a))
    return "|".join(xs)


def assert_atoms_legal(atoms=None):
    """Refuse any atom that peeks same-row close / H/I / invented clocks."""
    specs = ATOM_SPEC if atoms is None else {a: ATOM_SPEC[a] for a in atoms}
    for name, (col, lag, kind) in specs.items():
        if col in ("H", "I"):
            raise ValueError(f"atom {name} uses label {col} as a feature")
        assert_feature_legal(kind, col, lag)
        if col in SAME_ROW_LEAK_ABORT and (not lag or lag < 1):
            raise ValueError(f"LEAK abort: atom {name} same-row {col}")
    return True


def keep_verdict(n, wr):
    """KEEP only on the Cyrus fee bar. Lift is not an argument.

    ≥30 prove fires, after-fee H win rate strictly > 55%. Thin n is FAIL.
    """
    if not n or wr is None:
        return "FAIL", f"no fires (n={n or 0})"
    n = int(n)
    wr = float(wr)
    pct = f"{100 * wr:.1f}%"
    if n < MIN_FIRES:
        return "FAIL", f"thin n={n} (bar ≥{MIN_FIRES}), after-fee WR {pct}"
    if wr <= WIN_BAR:
        return "FAIL", f"n={n}, after-fee WR {pct} ≤ {100 * WIN_BAR:.0f}%"
    return "KEEP", f"n={n}, after-fee WR {pct} (>{100 * WIN_BAR:.0f}%)"


def _try_pyarrow():
    try:
        import pyarrow.parquet as pq  # noqa: F401
        return True
    except ImportError:
        return False


def load_ohlc_bars(path=None):
    """iso-sorted bars per ticker. Session weekdays only."""
    import pyarrow.parquet as pq
    path = path or OHLC_PATH
    by = defaultdict(list)
    pf = pq.ParquetFile(path)
    for i in range(pf.num_row_groups):
        tbl = pf.read_row_group(
            i, columns=["date", "ticker", "open", "high", "low", "close", "volume"],
        )
        for d, t, o, h, l, c, v in zip(
            tbl.column("date").to_pylist(),
            tbl.column("ticker").to_pylist(),
            tbl.column("open").to_pylist(),
            tbl.column("high").to_pylist(),
            tbl.column("low").to_pylist(),
            tbl.column("close").to_pylist(),
            tbl.column("volume").to_pylist(),
        ):
            iso = (d.date() if hasattr(d, "date") else d).isoformat()
            if not is_session(iso) or not o or not c or o <= 0:
                continue
            if h is None or l is None:
                continue
            by[str(t).strip().upper()].append(
                (iso, float(o), float(h), float(l), float(c), float(v or 0))
            )
    for t in by:
        by[t].sort()
    return by


def _fresh(prior_iso, iso):
    if not prior_iso:
        return False
    return (date.fromisoformat(iso) - date.fromisoformat(prior_iso)).days <= J_FRESH_DAYS


def name_days_from_hist(hist, allow=None):
    """Open-knowable name-days. Never reads today's H/L/C as features."""
    rows = []
    n_tk = 0
    for t, bars in hist.items():
        if allow is not None and t not in allow:
            continue
        prior = []
        for i, (iso, o, h, l, c, v) in enumerate(bars):
            if i == 0:
                prior.append({"o": o, "h": h, "l": l, "c": c, "v": v})
                continue
            pdate, _po, _ph, _pl, pc, pv = bars[i - 1]
            if not _fresh(pdate, iso) or len(prior) < MIN_PRIOR_BARS:
                prior.append({"o": o, "h": h, "l": l, "c": c, "v": v})
                continue
            xl = open_features(prior[-20:], o)
            if xl.get("same_row_df") or xl.get("same_row_bb") or xl.get("same_row_bq"):
                raise ValueError("LEAK abort: same-row DF/BB/BQ on an open path")
            H = (c - o) / o
            I = ((c / pc) - 1.0) if pc else None
            rows.append({
                "date": iso, "ticker": t,
                "net": H - FEE_RT,
                "i_net": None if I is None else I - FEE_RT,
                "H": H, "I": I, "prior_vol": pv,
                "flags": feature_flags(xl),
            })
            prior.append({"o": o, "h": h, "l": l, "c": c, "v": v})
        n_tk += 1
        if n_tk % 400 == 0:
            print(f"  name-days {n_tk}/{len(hist)} tickers, rows={len(rows)}", flush=True)
    return rows


def time_slot(feat_date, cutoff):
    """Fail-closed: unparseable / missing date is neither disc nor hold."""
    if not cutoff or not feat_date:
        return None
    if feat_date >= cutoff:
        return "hold"
    return "disc"


def split_rows(rows, cutoff):
    disc, hold = [], []
    for r in rows:
        slot = time_slot(r.get("date") or "", cutoff)
        if slot == "disc":
            disc.append(r)
        elif slot == "hold":
            hold.append(r)
    return disc, hold


def cutoff_from_dates(dates, hold_frac=HOLD_FRAC, locked=DEEP_CUTOFF):
    dates = sorted({d for d in dates if d})
    if not dates:
        return None
    if locked and locked in dates:
        return locked
    if len(dates) == 1:
        return dates[0]
    idx = int(round(len(dates) * (1.0 - hold_frac)))
    idx = min(max(idx, 1), len(dates) - 1)
    return dates[idx]


def walk_folds(dates, n_folds=WALK_FOLDS):
    """Chronological discovery folds. Holdout is never in these dates."""
    dates = sorted({d for d in dates if d})
    if not dates or n_folds < 2:
        return []
    n = len(dates)
    out = []
    for i in range(n_folds):
        lo = int(round(i * n / n_folds))
        hi = int(round((i + 1) * n / n_folds))
        chunk = dates[lo:hi]
        if chunk:
            out.append((chunk[0], chunk[-1], chunk))
    return out


def combo_hits(rows, atoms):
    if not atoms:
        return [r for r in rows if r.get("net") is not None]
    hits = []
    for r in rows:
        if r.get("net") is None:
            continue
        fl = r.get("flags") or {}
        if all(fl.get(a) for a in atoms):
            hits.append(r)
    return hits


def score_hits(hits):
    h = hit_rate(hits, "net")
    i = hit_rate(hits, "i_net")
    n = h["n"]
    wr = h["hit"]
    verdict, why = keep_verdict(n, wr)
    nets = [r["net"] for r in hits if r.get("net") is not None]
    mean_net = (sum(nets) / len(nets)) if nets else None
    return {
        "n": n,
        "n_pos": h["n_pos"],
        "wr": wr,
        "mean_net": mean_net,
        "hit_i": i,
        "verdict": verdict,
        "why": why,
        "bar": WIN_BAR,
        "min_fires": MIN_FIRES,
        "fee_rt": FEE_RT,
    }


def score_combo(rows, atoms):
    return score_hits(combo_hits(rows, atoms))


def day_book_score(rows, atoms, kind="unranked", n=8):
    """Presence overlay vs same-day no-rule book. Context — not the KEEP bar."""
    by = defaultdict(list)
    for r in rows:
        if r.get("date") and r.get("net") is not None:
            by[r["date"]].append(r)
    base, rule = [], []
    for iso in sorted(by):
        day = by[iso]
        if kind == "vol_top8":
            pool = sorted(day, key=lambda x: x.get("prior_vol") or 0, reverse=True)[:n]
        else:
            pool = list(day)
        match = [r for r in (day if kind != "vol_top8" else
                             sorted(day, key=lambda x: x.get("prior_vol") or 0, reverse=True))
                 if all((r.get("flags") or {}).get(a) for a in atoms)]
        if kind == "vol_top8":
            match = match[:n]
        base.extend(pool)
        rule.extend(match)
    return fire_winrate(base, rule)


def expand_combos(seeds, core):
    """Systematic singles → pairs → core triples. Not one lucky combo."""
    seeds = [s for s in seeds if s in ATOM_SPEC]
    core = [s for s in core if s in ATOM_SPEC]
    out = []
    for s in seeds:
        out.append((s,))
    for a, b in combinations(seeds, 2):
        out.append((a, b))
    for trip in combinations(core, 3):
        out.append(trip)
    # de-dupe by canonical name
    seen, uniq = set(), []
    for atoms in out:
        name = combo_name(atoms)
        if name in seen:
            continue
        seen.add(name)
        uniq.append(combo_atoms(name))
    return uniq


def deep_surviving_rules(path=None):
    path = path or DEEP_JSON
    if not os.path.isfile(path):
        return []
    try:
        raw = json.load(open(path, encoding="utf-8"))
    except Exception:
        return []
    rules = []
    for r in (raw.get("top_combos") or []) + (raw.get("top_singles") or []):
        if r.get("sign_ok") and r.get("rule"):
            rules.append(r["rule"])
    return rules


def score_space(disc, hold, combos):
    """Score every combo on discovery (expand) and holdout (KEEP)."""
    rows = []
    folds = walk_folds([r["date"] for r in disc])
    fold_sets = []
    for lo, hi, chunk in folds:
        s = set(chunk)
        fold_sets.append((lo, hi, s, [r for r in disc if r["date"] in s]))
    for i, atoms in enumerate(combos):
        name = combo_name(atoms)
        d = score_combo(disc, atoms)
        h = score_combo(hold, atoms)
        # KEEP is prove/holdout only. Discovery cannot KEEP the family.
        fam, why = h["verdict"], h["why"]
        walk = []
        n_walk_ok = 0
        for lo, hi, _s, frows in fold_sets:
            fs = score_combo(frows, atoms)
            walk.append({
                "lo": lo, "hi": hi, "n": fs["n"], "wr": fs["wr"],
                "verdict": fs["verdict"],
            })
            if fs["n"] and fs["wr"] is not None and fs["wr"] > 0.50:
                n_walk_ok += 1
        rows.append({
            "rule": name,
            "kind": "single" if len(atoms) == 1 else f"combo{len(atoms)}",
            "atoms": list(atoms),
            "disc": d,
            "hold": h,
            "verdict": fam,
            "why": why,
            "walk": walk,
            "walk_folds_wr_gt_50": n_walk_ok,
            "n_walk_folds": len(fold_sets),
        })
        if (i + 1) % 50 == 0:
            print(f"  scored {i + 1}/{len(combos)}", flush=True)
    rows.sort(key=lambda r: (
        0 if r["verdict"] == "KEEP" else 1,
        -(r["hold"]["wr"] or 0),
        -(r["hold"]["n"] or 0),
    ))
    return rows


def attach_day_books(scored, hold, names):
    want = set(names)
    for r in scored:
        if r["rule"] not in want:
            continue
        atoms = tuple(r["atoms"])
        r["day_book_unranked"] = fire_slim(day_book_score(hold, atoms, "unranked"))
        r["day_book_vol_top8"] = fire_slim(day_book_score(hold, atoms, "vol_top8"))
    return scored


def fire_slim(wr):
    if not wr:
        return wr
    return {
        "n_fires": wr.get("n_fires"),
        "n_wins": wr.get("n_wins"),
        "n_ties": wr.get("n_ties"),
        "n_losses": wr.get("n_losses"),
        "win_rate": wr.get("win_rate"),
        "verdict": wr.get("verdict"),
        "clears_55": wr.get("clears_55"),
    }


def _pct(wr):
    if wr is None:
        return "—"
    return f"{100 * wr:.1f}%"


def _card_line(r):
    h = r["hold"]
    d = r["disc"]
    return (
        f"**{r['verdict']}** `{r['rule']}` — prove {h['why']}. "
        f"Discovery n={d['n']} after-fee WR {_pct(d['wr'])} "
        f"(not KEEP). Lift is not a bar."
    )


def write_board(payload, path=None):
    path = path or BOARD_MD
    gp = payload["gate"]
    headline = payload["headline"]
    scored = payload["scored"]
    keeps = [r for r in scored if r.get("verdict") == "KEEP"]
    hypos = payload.get("hypothesis") or []
    lines = [
        "# Excel factor-mine Phase B — fee-aware combo prove",
        "",
        f"status={payload.get('status', 'DONE')} verdict=**{headline['verdict']}** "
        f"TIME-SPLIT cutoff={payload.get('cutoff')} "
        f"tickers={payload.get('n_tickers')} name-days={payload.get('n_rows')} "
        f"univ=mcap>${MCAP_MIN_M:.0f}M & vol>{AVGVOL_MIN:.0f} "
        f"combos_scored={len(scored)} KEEP={len(keeps)}",
        "",
        "Research only. Live `flatten_robust` is not imported and is not written.",
        "",
        "## Headline",
        "",
        headline["text"],
        "",
        "## KEEP bar",
        "",
        f"Cyrus KEEP: **≥{MIN_FIRES} prove fires** and **after-fee H win rate "
        f"> {100 * WIN_BAR:.0f}%**. After-fee H = open-to-close minus "
        f"{FEE_RT * 10000:.0f} bp Futubull (`FEE_RT={FEE_RT}`). "
        "A fire is a name-day where every atom is true at the 09:30 open. "
        "**Lift-only is never KEEP.** Thin n that prints >55% is FAIL. "
        "Discovery may rank and expand combinations; it cannot KEEP. "
        f"{FEE_CAVEAT}",
        "",
        "## Clock lock",
        "",
        f"- Gate: `{gp['gate']}`",
        f"- Fill OPEN same-row: `{', '.join(gp['fill_open'])}`",
        f"- Value OPEN same-row (44): `{', '.join(gp['value_mine_open'])}`",
        f"- Lag-only close: `{', '.join(gp['lag_only_close'])}`",
        f"- Same-row leak abort: `{', '.join(gp['same_row_leak_abort'])}`",
        f"- Open-44 tally subs: `{', '.join(gp['open_44_tally_subs'])}`",
        f"- Leak check: **{payload['leak']}**",
        f"- Split: TIME-SPLIT last {HOLD_FRAC:.0%} of session dates "
        f"(locked cutoff `{payload.get('cutoff')}` when present on tape). "
        "Discovery feature date is strictly before cutoff. Same-day H/I "
        "labels on a discovery date do not cross the cutoff.",
        "- Live: `flatten_robust` not imported, not written.",
        "",
        "## What was scored",
        "",
        "Open-knowable singles from holdout-surviving deep-corr atoms that "
        "Yahoo OHLC can reconstruct (`J_ge0`, `ER_m1`, `prior_bear_engulf`, "
        "`el_neg`, `prior_bearish`, `prior_hanging`) plus open-gate letters "
        f"({', '.join(a for a in OPEN_SEEDS if a not in CORE_SEEDS)}). "
        "Expansion is systematic: every single, every pair among "
        f"{len(OPEN_SEEDS)} seeds, every triple among {len(CORE_SEEDS)} "
        "core seeds. Fill-paint atoms from the lift board "
        f"({', '.join(GRID_ONLY_ATOMS[:6])}, …) need Excel grids and are "
        "**not scored** on this cut — they are not invented from Yahoo.",
        "",
        "## Hypothesis cards",
        "",
    ]
    if hypos:
        for r in hypos:
            h, d = r["hold"], r["disc"]
            d_net = "—" if d.get("mean_net") is None else f"{d['mean_net']:+.4f}"
            h_net = "—" if h.get("mean_net") is None else f"{h['mean_net']:+.4f}"
            lines += [
                f"### `{r['rule']}`",
                "",
                f"{r['verdict']} — {h['why']}.",
                f"Discovery n={d['n']} after-fee WR {_pct(d['wr'])} "
                f"mean net={d_net}. Prove mean net={h_net}.",
                "Deep-corr lift on I-green is discarded as a KEEP input.",
                "",
            ]
    else:
        lines += ["No hypothesis rows scored.", ""]
    lines += [
        "## KEEP cards",
        "",
    ]
    if keeps:
        for r in keeps:
            lines.append(f"- {_card_line(r)}")
        lines.append("")
    else:
        lines.append("**None.** No combo cleared ≥30 prove fires and >55% after-fee WR.")
        lines.append("")
    lines += [
        "## Prove (holdout) — after-fee H",
        "",
        "| rule | kind | prove n | prove after-fee WR | prove verdict | disc n | disc WR | walk folds >50% |",
        "|---|---|---:|---:|---|---:|---:|---:|",
    ]
    show = [r for r in scored if r["hold"]["n"] >= 10][:60]
    if not show:
        show = scored[:40]
    for r in show:
        h, d = r["hold"], r["disc"]
        lines.append(
            f"| `{r['rule']}` | {r['kind']} | {h['n']} | {_pct(h['wr'])} | "
            f"**{r['verdict']}** | {d['n']} | {_pct(d['wr'])} | "
            f"{r.get('walk_folds_wr_gt_50')}/{r.get('n_walk_folds')} |"
        )
    lines += [
        "",
        "## Discovery (not KEEP)",
        "",
        "Discovery is for expansion and honesty only. A discovery >55% print "
        "is not a call.",
        "",
        "| rule | disc n | disc after-fee WR | disc mean net |",
        "|---|---:|---:|---:|",
    ]
    disc_rank = sorted(scored, key=lambda r: (-(r["disc"]["wr"] or 0), -r["disc"]["n"]))
    for r in [x for x in disc_rank if x["disc"]["n"] >= 30][:25]:
        d = r["disc"]
        mn = "—" if d.get("mean_net") is None else f"{d['mean_net']:+.4f}"
        lines.append(f"| `{r['rule']}` | {d['n']} | {_pct(d['wr'])} | {mn} |")
    lines += [
        "",
        "## Walk-forward (discovery folds, not KEEP)",
        "",
        "Three chronological folds **inside discovery**. Holdout dates are "
        "never in these folds. Used to see whether a disc print was one lucky slice.",
        "",
        "| rule | fold1 n / WR | fold2 n / WR | fold3 n / WR |",
        "|---|---|---|---|",
    ]
    for r in (hypos + [x for x in scored if x["verdict"] == "KEEP"])[:20]:
        bits = []
        for w in r.get("walk") or []:
            bits.append(f"{w['n']} / {_pct(w['wr'])}")
        while len(bits) < 3:
            bits.append("—")
        lines.append(f"| `{r['rule']}` | {bits[0]} | {bits[1]} | {bits[2]} |")
    lines += [
        "",
        "## Day-book overlay (j_winrate, context)",
        "",
        "Same constants (`WIN_BAR=0.55`, `MIN_FIRES=30`). Fire = the presence "
        "book ticker set differs from the same-day no-rule set. Win = rule "
        "book mean after-fee H beats that no-rule book. This does **not** "
        "rescue a name-day FAIL. Reported for hypothesis + KEEP rows only.",
        "",
        "| rule | unranked fire | vol_top8 fire |",
        "|---|---|---|",
    ]
    book_rows = [r for r in scored if r.get("day_book_unranked")]
    if not book_rows:
        lines.append("| — | not scored | not scored |")
    for r in book_rows:
        u, v = r.get("day_book_unranked") or {}, r.get("day_book_vol_top8") or {}
        def _fs(w):
            if not w or w.get("win_rate") is None:
                return f"n={w.get('n_fires', 0) if w else 0}"
            return (
                f"{w.get('verdict')} {_pct(w['win_rate'])} "
                f"({w.get('n_wins')}/{w.get('n_fires')})"
            )
        lines.append(f"| `{r['rule']}` | {_fs(u)} | {_fs(v)} |")
    lines += [
        "",
        "## Fill-paint atoms not scored",
        "",
        "Deep-corr lift survivors that need Excel fill grids (not reconstructed "
        "from Yahoo OHLC on this cut): "
        + ", ".join(f"`{a}`" for a in GRID_ONLY_ATOMS)
        + ". Do not invent fill clocks.",
        "",
        "## Explicitly not live",
        "",
        "No combo is wired into `flatten_robust` or cash/paper. A KEEP here is "
        "a research card, not a ship.",
        "",
        "## Source",
        "",
        "`excel_clock_gate.py` / `CLOCK_MAP.md` / `OPEN_SAME_ROW_LABELS.md` · "
        "`excel_open_features.py` · `j_winrate.py` (`WIN_BAR`, `MIN_FIRES`, "
        f"`FEE_RT={FEE_RT}`) · deep-corr board `EXCEL_DEEP_CORR_MINE.md` "
        "(lift only; finished FAIL on this bar). Research only.",
        "",
    ]
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    open(path, "w", encoding="utf-8").write("\n".join(lines))
    return path


def slim_score(r):
    def sl(s):
        if not s:
            return s
        return {
            "n": s.get("n"), "n_pos": s.get("n_pos"), "wr": s.get("wr"),
            "mean_net": s.get("mean_net"), "verdict": s.get("verdict"),
            "why": s.get("why"),
        }
    return {
        "rule": r["rule"], "kind": r["kind"], "atoms": r["atoms"],
        "disc": sl(r.get("disc")), "hold": sl(r.get("hold")),
        "verdict": r.get("verdict"), "why": r.get("why"),
        "walk": r.get("walk"),
        "walk_folds_wr_gt_50": r.get("walk_folds_wr_gt_50"),
        "n_walk_folds": r.get("n_walk_folds"),
        "day_book_unranked": r.get("day_book_unranked"),
        "day_book_vol_top8": r.get("day_book_vol_top8"),
    }


def headline_from(scored, cutoff):
    keeps = [r for r in scored if r.get("verdict") == "KEEP"]
    if keeps:
        bits = [f"`{r['rule']}` {r['hold']['why']}" for r in keeps[:8]]
        return {
            "verdict": "KEEP",
            "n_keep": len(keeps),
            "text": (
                f"KEEP. {len(keeps)} combo(s) cleared ≥{MIN_FIRES} prove fires "
                f"and >{100 * WIN_BAR:.0f}% after-fee H on holdout "
                f"(cutoff {cutoff}): " + "; ".join(bits) + "."
            ),
        }
    ranked = [r for r in scored if r["hold"]["n"]]
    best = max(ranked, key=lambda r: ((r["hold"]["wr"] or 0), r["hold"]["n"])) if ranked else None
    if not best:
        text = (
            f"FAIL. No combo produced prove fires. cutoff={cutoff}. "
            "Lift-only is not KEEP."
        )
        return {"verdict": "FAIL", "n_keep": 0, "text": text, "best": None}
    h = best["hold"]
    text = (
        f"FAIL. No open-knowable combo cleared the Cyrus KEEP bar on holdout "
        f"(cutoff {cutoff}). Best prove after-fee WR: `{best['rule']}` "
        f"n={h['n']} after-fee WR {_pct(h['wr'])}. Lift-only is not KEEP."
    )
    return {
        "verdict": "FAIL", "n_keep": 0, "text": text,
        "best": {"rule": best["rule"], "n": h["n"], "wr": h["wr"]},
    }


def run(limit=0, ohlc_path=None, out_md=None, out_json=None):
    clocks = assert_excel_clock_gate()
    assert_lag_atoms()
    assert_atoms_legal()
    leak = "PASS"
    try:
        for col in SAME_ROW_LEAK_ABORT:
            assert_feature_legal("value", col, 0)
        leak = "FAIL — same-row DF/BB/BQ did not abort"
    except ValueError:
        leak = "PASS"

    finviz_path = next(
        (p for p in (
            os.path.join(ROOT, "data", "finviz_with_descriptions.csv"),
            os.path.join(REPO, "data", "finviz_with_descriptions.csv"),
            os.path.join(REPO, "excel_bot", "data", "finviz_with_descriptions.csv"),
        ) if os.path.isfile(p)),
        "",
    )
    allow = load_finviz_filter(finviz_path) if finviz_path else {}
    print(f"[univ] finviz {len(allow)} names from {finviz_path}", flush=True)

    ohlc_path = ohlc_path or OHLC_PATH
    if not (_try_pyarrow() and os.path.isfile(ohlc_path)):
        payload = {
            "status": "DONE",
            "generated": str(date.today()),
            "leak": leak,
            "live_untouched": "flatten_robust",
            "gate": gate_payload(),
            "n_tickers": 0, "n_rows": 0, "cutoff": None,
            "scored": [], "hypothesis": [],
            "headline": {
                "verdict": "FAIL", "n_keep": 0,
                "text": "FAIL. ohlc.parquet / pyarrow missing — no prove fires.",
            },
            "clocks_generated": clocks.get("generated"),
        }
        write_board(payload, out_md or BOARD_MD)
        json.dump(payload, open(out_json or BOARD_JSON, "w"), indent=2, default=str)
        print("ohlc/pyarrow missing — FAIL board written", flush=True)
        return payload

    print("loading ohlc.parquet …", flush=True)
    hist = load_ohlc_bars(ohlc_path)
    if limit:
        keys = sorted(hist)[: int(limit)]
        hist = {k: hist[k] for k in keys}
    print(f"tickers in parquet {len(hist)}", flush=True)
    print("building open-knowable name-days …", flush=True)
    rows = name_days_from_hist(hist, allow if allow else None)
    print(f"name-days {len(rows)}", flush=True)
    dates = sorted({r["date"] for r in rows})
    cutoff = cutoff_from_dates(dates)
    print(f"[split] TIME-SPLIT cutoff={cutoff} dates={len(dates)}", flush=True)
    disc, hold = split_rows(rows, cutoff)
    print(f"[split] disc={len(disc)} hold={len(hold)}", flush=True)

    combos = expand_combos(OPEN_SEEDS, CORE_SEEDS)
    print(f"[space] {len(combos)} singles+pairs+core-triples", flush=True)
    scored = score_space(disc, hold, combos)

    hypo_names = [
        "J_ge0", "ER_m1", "prior_bear_engulf",
        "J_ge0|ER_m1", "J_ge0|prior_bear_engulf",
        "J_ge0|el_neg", "J_ge0|prior_bearish", "J_ge0|prior_hanging",
        "ER_m1|prior_bear_engulf",
        "ER_m1|J_ge0|prior_bear_engulf",
    ]
    # canonical names
    hypo_names = [combo_name(combo_atoms(n)) for n in hypo_names]
    by = {r["rule"]: r for r in scored}
    hypos = [by[n] for n in hypo_names if n in by]
    keep_names = [r["rule"] for r in scored if r["verdict"] == "KEEP"]
    attach_day_books(scored, hold, hypo_names + keep_names)

    headline = headline_from(scored, cutoff)
    liq_hold = [r for r in hold if (r.get("prior_vol") or 0) >= PRIOR_VOL_LIQ]
    liq_best = None
    if liq_hold:
        liq_scores = []
        for n in hypo_names:
            if n not in by:
                continue
            s = score_combo(liq_hold, tuple(by[n]["atoms"]))
            liq_scores.append({"rule": n, **s})
        if liq_scores:
            liq_best = max(liq_scores, key=lambda r: ((r.get("wr") or 0), r["n"]))

    payload = {
        "status": "DONE",
        "generated": str(date.today()),
        "tip_sha": None,
        "leak": leak,
        "live_untouched": "flatten_robust",
        "gate": gate_payload(),
        "fire_bar": {
            "win": f"after-fee H > 0 on prove name-days, strictly > {WIN_BAR}",
            "min_fires": MIN_FIRES,
            "fee_rt": FEE_RT,
            "fee_caveat": FEE_CAVEAT,
            "lift_never_keep": True,
        },
        "cutoff": cutoff,
        "split_kind": "time",
        "hold_frac": HOLD_FRAC,
        "n_tickers": len({r["ticker"] for r in rows}),
        "n_rows": len(rows),
        "n_disc": len(disc),
        "n_hold": len(hold),
        "n_combos": len(scored),
        "n_keep": headline["n_keep"],
        "headline": headline,
        "hypothesis": [slim_score(r) for r in hypos],
        "scored": [slim_score(r) for r in scored],
        "liq_hold_n": len(liq_hold),
        "liq_best_hypo": liq_best,
        "deep_surviving": deep_surviving_rules(),
        "grid_only_unscored": list(GRID_ONLY_ATOMS),
        "seeds": list(OPEN_SEEDS),
        "core_seeds": list(CORE_SEEDS),
        "clocks_generated": clocks.get("generated"),
    }
    md = write_board(payload, out_md or BOARD_MD)
    js = out_json or BOARD_JSON
    os.makedirs(os.path.dirname(js) or ".", exist_ok=True)
    json.dump(payload, open(js, "w"), indent=2, default=str)
    ckpt = {
        "phase": "done", "split_kind": "time", "cutoff": cutoff,
        "done": True, "n_rows": len(rows), "n_keep": headline["n_keep"],
        "verdict": headline["verdict"],
    }
    json.dump(ckpt, open(CKPT_JSON, "w"), indent=2)
    print("wrote", md, js, flush=True)
    print("headline", headline["verdict"], headline["text"], flush=True)
    return payload


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--ohlc", default="")
    ap.add_argument("--out-md", default=BOARD_MD)
    ap.add_argument("--out-json", default=BOARD_JSON)
    args = ap.parse_args()
    run(limit=args.limit, ohlc_path=args.ohlc or None,
        out_md=args.out_md, out_json=args.out_json)


if __name__ == "__main__":
    main()
