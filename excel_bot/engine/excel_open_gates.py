"""Morning gates from lag candles/tallies + open-44 prior-print tallies.

Same combine as #153: overlay avoid / elevate / presence on morning books
(join top-8, green-pile, weighted, flatten picks, long Yahoo liquid
vol_top8 / prior_green_top8). Score vs the same-day no-rule book.

Clock: DF/DG/DH/BB and BQ/BU are close same-row — lag t−1+ only.
Same-row DF/BB/BQ at 9:30 aborts. Open-44 subs: AH JB JC FQ FR ER EP EN.
Live flatten_robust is not imported.

  python3 excel_bot/engine/excel_open_gates.py
"""
from __future__ import annotations

import json
import os
import sys
from collections import defaultdict
from datetime import date
from glob import glob

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
REPO = os.path.dirname(ROOT)
sys.path.insert(0, HERE)

from excel_clock_gate import (  # noqa: E402
    OPEN_44_TALLY_SUBS, SAME_ROW_LEAK_ABORT, assert_excel_clock_gate,
    assert_feature_legal, gate_payload,
)
from excel_open_features import (  # noqa: E402
    INVENTORY, RECIPES, assert_lag_atoms, assert_recipes_legal,
    feature_flags, open_features,
)
from join_post_813 import (  # noqa: E402
    BOOK_DIR, DISCOVERY, FEE_RT, JOIN_DIR, ORIG, PROVE, REPO as JOIN_REPO,
    history_index, is_session, load_finviz, load_join_days, pick_book,
    prior_bars,
)
from j_sleeve_prove import load_prior_book_map  # noqa: E402
from j_universe_prove import PRIOR_VOL_LIQ, _try_pyarrow  # noqa: E402
from j_winrate import (  # noqa: E402
    FEE_CAVEAT, MIN_FIRES, hit_s, pack_rule_clock, slim_wr, wr_s,
)

assert JOIN_REPO == REPO
RESEARCH = os.path.join(ROOT, "research")
OHLC_PATH = os.path.join(REPO, "data", "prices", "ohlc.parquet")
BOARD_MD = os.path.join(RESEARCH, "OPEN_GATES_BOARD.md")
BOARD_JSON = os.path.join(RESEARCH, "open_gates_board.json")
J_FRESH_DAYS = 5

LONG = ("2024-03-06", "2026-08-21")
Y2025 = ("2025-01-02", "2025-12-31")
PRE813 = ("2026-01-02", "2026-08-12")
KINDS = ("vol_top8", "prior_green_top8")
PRESENCE_KINDS = ("vol_top8", "prior_green_top8", "unranked")


def _fresh(prior_iso, iso):
    if not prior_iso:
        return False
    return (date.fromisoformat(iso) - date.fromisoformat(prior_iso)).days <= J_FRESH_DAYS


def bars_from_prior(prior_tuples):
    out = []
    for _d, r in prior_tuples:
        o = r.get("open")
        h = r.get("high")
        l = r.get("low")
        c = r.get("close")
        v = r.get("vol")
        if o and h is not None and l is not None and c is not None:
            out.append({"o": float(o), "h": float(h), "l": float(l),
                        "c": float(c), "v": float(v or 0)})
        elif o and c is not None:
            out.append({"o": float(o), "h": float(c), "l": float(c),
                        "c": float(c), "v": float(v or 0)})
    return out


def take_picks(recs, flags, recipe, n=8):
    _name, _kind, mode, avoid, elev, keep, _atoms = recipe
    if keep:
        out = []
        for r in recs:
            if flags.get(r["ticker"], {}).get(keep):
                out.append(r)
            if len(out) >= n:
                break
        return out
    return pick_book(recs, flags, mode, avoid, elev, n=n, cap=2)


def _day_groups(rows, lo, hi):
    by = defaultdict(list)
    for r in rows:
        d = r.get("date")
        if d and lo <= d <= hi and r.get("net") is not None:
            by[d].append(r)
    return by


def _prior_map(days):
    isos = sorted(days)
    prev, last = {}, None
    for iso in isos:
        prev[iso] = last
        last = iso
    return prev


def _h_pos(r):
    return r.get("net") is not None and r["net"] > -FEE_RT


def _books(kind, drows, yest, recipe, n=8):
    pool = drows
    if kind.startswith("prior_green"):
        if not yest:
            return [], []
        winners = {r["ticker"] for r in yest if _h_pos(r)}
        pool = [r for r in drows if r["ticker"] in winners]
    if kind in ("unranked", "prior_green_unranked"):
        fl = {r["ticker"]: r["flags"] for r in pool}
        recs = [{"ticker": r["ticker"], "rank": i} for i, r in enumerate(pool, 1)]
        by = {r["ticker"]: r for r in pool}
        _n, _k, mode, avoid, elev, keep, _a = recipe
        if mode == "elev_cap":
            return pool, []
        if keep:
            rule = [by[p["ticker"]] for p in recs if fl.get(p["ticker"], {}).get(keep)]
        elif mode == "intersect":
            rule = [by[p["ticker"]] for p in recs if fl.get(p["ticker"], {}).get(elev)]
        else:
            rule = [by[p["ticker"]] for p in recs
                    if not (avoid and fl.get(p["ticker"], {}).get(avoid))]
        return pool, rule
    if kind == "listed":
        ranked = sorted(pool, key=lambda r: r.get("rank") or 99)
    else:
        ranked = sorted(pool, key=lambda r: r.get("prior_vol") or 0, reverse=True)
    fl = {r["ticker"]: r["flags"] for r in ranked}
    recs = [{"ticker": r["ticker"], "rank": i} for i, r in enumerate(ranked, 1)]
    by = {r["ticker"]: r for r in ranked}
    base = [by[p["ticker"]] for p in recs[:n]]
    picks = take_picks(recs, fl, recipe, n=n)
    rule = [by[p["ticker"]] for p in picks if p["ticker"] in by]
    return base, rule


def score_recipes(rows, slices, kinds):
    all_days = _day_groups(rows, "0000-01-01", "9999-12-31")
    all_prev = _prior_map(all_days)
    out = {}
    for sname, (lo, hi) in slices.items():
        days = {d: all_days[d] for d in all_days if lo <= d <= hi}
        out[sname] = {"n_days": len(days), "lo": lo, "hi": hi, "kinds": {}}
        for kind in kinds:
            blob = {"n_days": len(days), "recipes": {}}
            for rec in RECIPES:
                name, rkind = rec[0], rec[1]
                use = kinds if rkind != "presence" else kinds
                if rkind == "presence" and kind not in PRESENCE_KINDS:
                    continue
                if rkind == "elevate" and kind in ("unranked", "prior_green_unranked"):
                    continue
                base, rule = [], []
                for iso in sorted(days):
                    yest = all_days.get(all_prev.get(iso)) or []
                    b, a = _books(kind, days[iso], yest, rec, n=8)
                    base.extend(b)
                    rule.extend(a)
                packed = pack_rule_clock(base, rule)
                packed["name"] = name
                packed["family"] = rkind
                blob["recipes"][name] = packed
            out[sname]["kinds"][kind] = blob
    return out


def load_ohlc_bars():
    import pyarrow.parquet as pq
    by = defaultdict(list)
    pf = pq.ParquetFile(OHLC_PATH)
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


def ohlc_liq_rows(hist):
    rows = []
    for t, bars in hist.items():
        prior = []
        for i, (iso, o, h, l, c, v) in enumerate(bars):
            if i == 0:
                prior.append({"o": o, "h": h, "l": l, "c": c, "v": v})
                continue
            pdate, po, _ph, _pl, pc, pv = bars[i - 1]
            if not _fresh(pdate, iso):
                prior.append({"o": o, "h": h, "l": l, "c": c, "v": v})
                continue
            if pv < PRIOR_VOL_LIQ:
                prior.append({"o": o, "h": h, "l": l, "c": c, "v": v})
                continue
            # 20 completed prior bars cover CP (20d), AH (6d), JB/JC (8d).
            xl = open_features(prior[-20:], o)
            if xl.get("same_row_df") or xl.get("same_row_bb") or xl.get("same_row_bq"):
                raise ValueError("LEAK abort: same-row DF/BB/BQ on an open path")
            net = (c - o) / o - FEE_RT
            i_net = None if not pc else (c - pc) / pc - FEE_RT
            rows.append({
                "date": iso, "ticker": t, "J": xl.get("J"),
                "net": net, "i_net": i_net, "prior_vol": pv,
                "flags": feature_flags(xl), "xl": None,
            })
            prior.append({"o": o, "h": h, "l": l, "c": c, "v": v})
    return rows


def native_rows():
    """Join / green-pile / weighted on Finviz (tiny-n; demote)."""
    fz = load_finviz()
    hist = history_index(fz)
    joins = load_join_days()
    out = {"join_top8": []}
    for iso, ranked in joins.items():
        if not is_session(iso) or iso not in fz:
            continue
        for rec in ranked[:80]:
            t = rec["ticker"]
            fz_t = fz[iso].get(t) or {}
            if fz_t.get("h") is None or not fz_t.get("open"):
                continue
            prior = bars_from_prior(prior_bars(hist, t, iso))
            xl = open_features(prior, fz_t["open"])
            if xl.get("same_row_df") or xl.get("same_row_bb") or xl.get("same_row_bq"):
                raise ValueError("LEAK abort: same-row DF/BB/BQ on native path")
            i = fz_t.get("i")
            row = {
                "date": iso, "ticker": t, "J": xl.get("J") if xl.get("J_fresh") else None,
                "net": fz_t["h"] - FEE_RT,
                "i_net": None if i is None else i - FEE_RT,
                "prior_vol": fz_t.get("vol") or 0,
                "flags": feature_flags(xl),
                "rank": rec["rank"],
            }
            if rec["rank"] <= 8:
                out["join_top8"].append(row)
    green_files, _ = load_prior_book_map("????-??-??_green.json", pile_only=True)
    w_files, _ = load_prior_book_map("????-??-??_stock_book.json", pile_only=False)
    # stock_book json is not the green pattern — load 1d buy via join helper
    from join_post_813 import load_book_1d
    books = load_book_1d()
    book_dates = sorted(books)
    out["green_pile_prior"] = []
    out["weighted_book_1d_prior"] = []
    g_keys, w_keys = sorted(green_files), book_dates
    days = sorted(set(joins) | set(fz))
    for iso in days:
        if not is_session(iso) or iso not in fz:
            continue
        prev_g = [k for k in g_keys if k < iso]
        prev_w = [k for k in w_keys if k < iso]
        gset = green_files[prev_g[-1]] if prev_g else []
        wset = [x["ticker"] for x in books.get(prev_w[-1], [])] if prev_w else []
        for label, names in (("green_pile_prior", gset),
                             ("weighted_book_1d_prior", wset)):
            for i, t in enumerate(names, 1):
                fz_t = fz[iso].get(t) or {}
                if fz_t.get("h") is None or not fz_t.get("open"):
                    continue
                prior = bars_from_prior(prior_bars(hist, t, iso))
                xl = open_features(prior, fz_t["open"])
                i_v = fz_t.get("i")
                out[label].append({
                    "date": iso, "ticker": t,
                    "J": xl.get("J") if xl.get("J_fresh") else None,
                    "net": fz_t["h"] - FEE_RT,
                    "i_net": None if i_v is None else i_v - FEE_RT,
                    "prior_vol": fz_t.get("vol") or 0,
                    "flags": feature_flags(xl),
                    "rank": i,
                })
    return out


def _verdict_line(wr):
    v = wr.get("verdict") or "null"
    return f"{v} {wr_s(wr)}"


def collect_clears(long_fires):
    clears, prov, fails = [], [], []
    for sname, sl in (long_fires or {}).items():
        for kind, blob in (sl.get("kinds") or {}).items():
            for name, wr in (blob.get("recipes") or {}).items():
                lab = f"ohlc_liq {kind} {sname} `{name}`"
                rec = {
                    "label": lab, "window": sname, "kind": kind, "recipe": name,
                    "wr": wr, "family": wr.get("family"),
                }
                if wr.get("verdict") == "CLEAR":
                    clears.append(rec)
                elif wr.get("verdict") == "PROVISIONAL":
                    prov.append(rec)
                elif wr.get("n_fires"):
                    fails.append(rec)
    return clears, prov, fails


def write_board(payload):
    gp = payload["gate"]
    clears = payload["clears"]
    lines = [
        "# Open gates board — beyond J (lag candles + open-44 tallies)",
        "",
        f"_Generated {payload['generated']} · tip `{payload.get('tip_sha') or 'pending'}` · "
        "**research only** · live frozen._",
        "",
        "## Plain English",
        "",
        "This cut expands past #153’s J-only overlays. Morning books are the "
        "same combine (join top-8, green-pile, weighted, long Yahoo liquid "
        "`vol_top8` / `prior_green_top8`). Gates are **avoid / elevate / "
        "presence** from Excel features that are open-knowable under the clock lock.",
        "",
        "**Hard clock:** candles **DF / DG / DH / BB** and tallies **BQ / BU** "
        "are close same-row. They may gate an open entry only as **lag t−1+**. "
        "Same-row DF / BB / BQ at 9:30 is a **leak** — that path aborts. "
        "Open same-row substitutes already in the 44: "
        f"{', '.join(OPEN_44_TALLY_SUBS)} (prior-print tallies). "
        "Never same-row H/I, M number, `core_score`, H paint, or close landmines.",
        "",
        f"**Cyrus bar:** >55% fire win-rate **and ≥{MIN_FIRES} fires**. "
        "Fire = the rule changes the book vs the same-day no-rule set. "
        "Win = rule-book mean after-fee H beats that no-rule book. Ties do not beat. "
        "n<30 that prints >55% is **PROVISIONAL** (demoted). "
        f"{FEE_CAVEAT} H+ / I+ after fees is a separate name-day hit rate — "
        "it is **not** the fire bar and is typically ~41–48% even on CLEARs. "
        "Live stays unwired.",
        "",
        "## Clock lock",
        "",
        f"- Fill OPEN same-row: `{', '.join(gp['fill_open'])}`",
        f"- Value OPEN same-row (44): `{', '.join(gp['value_mine_open'])}`",
        f"- Lag-only close (this cut): `{', '.join(gp['lag_only_close'])}`",
        f"- Same-row leak abort: `{', '.join(gp['same_row_leak_abort'])}`",
        f"- Open-44 tally subs: `{', '.join(gp['open_44_tally_subs'])}`",
        f"- Leak check: **{payload['leak']}**",
        "- Live: `flatten_robust` not imported, not written.",
        "",
        "## Inventory (open-knowable + this-cut close-lag)",
        "",
        "| col | kind | same-row | what | this cut |",
        "|---|---|---|---|---|",
    ]
    for col, kind, clock, what, note in INVENTORY:
        lines.append(f"| **{col}** | {kind} | {clock} | {what} | {note} |")
    lines += [
        "",
        "EQ / FS are open-44 and reconstructed (prior CP) but **not scored** "
        "this cut — they were not in the user’s open-44 tally list.\n"
        "Worth gating later (open 44, not scored here): Q warmup, Z/AC/BT/BV "
        "chains, CG/CH/DC/DE/EB/EK carry, ES–EV, FU, GD–GF, HF/HG/HW, II, "
        "IY/IZ (VIX), JD–JF/JL (HO family). Fill-only A/B/C/G/K/L/M/O/IR/IS/IT "
        "need Excel CF dumps — Yahoo cannot reconstruct fills. O green stays "
        "the standing keep; O number same-row stays OUT.",
        "",
        "## Which CLEAR (≥30 fires and >55%)?",
        "",
    ]
    ranked_kinds = {"vol_top8", "prior_green_top8"}
    by_rec = defaultdict(list)
    for c in clears:
        if c.get("kind") in ranked_kinds:
            by_rec[c["recipe"]].append(c)
    confirmed, long_only = [], []
    for name, items in by_rec.items():
        wins = {(c["kind"], c["window"]) for c in items}
        long_ok = any(w == "long" for _, w in wins)
        y_ok = any(w == "y2025" for _, w in wins)
        if long_ok and y_ok:
            confirmed.append((name, items))
        elif long_ok:
            long_only.append((name, items))
    lines.append("**Confirmed** = liquid ranked (`vol_top8` ≈ weighted book, "
                 "`prior_green_top8` ≈ green pile) CLEAR on **long and y2025**. "
                 "J elev CLEARs long and fails y2025 — these do not.")
    lines.append("")
    if confirmed:
        lines.append("### Confirmed (long + y2025, liquid ranked)")
        lines.append("")
        for name, items in confirmed:
            bits = [f"{c['kind']} {c['window']} {wr_s(c['wr'])}" for c in items
                    if c["window"] in ("long", "y2025")]
            lines.append(f"- `{name}` — " + "; ".join(bits))
        lines.append("")
    if long_only:
        lines.append("### Long CLEAR, y2025 did not confirm (liquid ranked)")
        lines.append("")
        for name, items in long_only:
            bits = [f"{c['kind']} {wr_s(c['wr'])}" for c in items if c["window"] == "long"]
            lines.append(f"- `{name}` — " + "; ".join(bits))
        lines.append("")
    lines.append("### All long-tape CLEARs (includes unranked / pre813)")
    lines.append("")
    if clears:
        for c in clears:
            wr = c["wr"]
            hh, ii = wr.get("hit_h") or {}, wr.get("hit_i") or {}
            lines.append(
                f"- {c['label']}: {wr_s(wr)} · H+ {hit_s(hh)} · I+ {hit_s(ii)}"
            )
    else:
        lines.append("**None.** No recipe on the long liquid tape clears the bar.")
    lines.append("")
    lines.append("BQ/BU lag recipes **FAIL** the long liquid ranked tape "
                 "(~53–54%). They only CLEAR pre813 — not a pooled-long call. "
                 "#153 J elev still CLEARs long `vol_top8` / `prior_green_top8` "
                 "and still fails y2025.")
    lines += [
        "",
        "### Fee H+ / I+ caveat",
        "",
        FEE_CAVEAT + " H+ after fees is the share of the rule’s name-days with "
        "after-fee H>0 (I+ the same for close-to-close). A fire CLEAR can still "
        "have H+ well under 55%. Do not read H+ as the Cyrus bar.",
        "",
        "## Long Yahoo liquid (material n)",
        "",
        "| circumstance | window | recipe | fire bar | fire win-rate | H+ after fees | I+ after fees |",
        "|---|---|---|---|---|---|---|",
    ]
    lf = payload.get("long_fires") or {}
    for sname in ("long", "y2025", "pre813"):
        sl = lf.get(sname) or {}
        for kind in ("vol_top8", "prior_green_top8", "unranked"):
            blob = (sl.get("kinds") or {}).get(kind) or {}
            for name, wr in (blob.get("recipes") or {}).items():
                if not wr.get("n_fires") and wr.get("verdict") == "null":
                    continue
                hh, ii = wr.get("hit_h") or {}, wr.get("hit_i") or {}
                lines.append(
                    f"| ohlc_liq {kind} | {sname} | `{name}` | "
                    f"**{wr.get('verdict')}** | {wr_s(wr)} | {hit_s(hh)} | {hit_s(ii)} |"
                )
    lines += [
        "",
        "## Native dumps (cannot reach 30 — demoted)",
        "",
        "Finviz / join / stock_book window is ~16 weekdays. Any >55% print here "
        "is **PROVISIONAL**.",
        "",
        "| circumstance | window | recipe | fire bar | fire win-rate | H+ | I+ |",
        "|---|---|---|---|---|---|---|",
    ]
    nat = payload.get("native") or {}
    for uname, sl in nat.items():
        for sname, blob in sl.items():
            for name, wr in (blob.get("recipes") or {}).items():
                if not wr.get("n_fires"):
                    continue
                hh, ii = wr.get("hit_h") or {}, wr.get("hit_i") or {}
                v = wr.get("verdict")
                if v == "CLEAR":
                    v = "PROVISIONAL"
                    wr = dict(wr)
                    wr["verdict"] = "PROVISIONAL"
                lines.append(
                    f"| {uname} | {sname} | `{name}` | **{v}** | {wr_s(wr)} | "
                    f"{hit_s(hh)} | {hit_s(ii)} |"
                )
    lines += [
        "",
        "## Cards",
        "",
        "| recipe | family | atoms | status |",
        "|---|---|---|---|",
    ]
    clear_names = {c["recipe"] for c in clears}
    for rec in RECIPES:
        name, fam, _m, _a, _e, _k, atoms = rec
        atom_s = ", ".join(f"{c}[t−{lag}]" if lag else f"{c}[t]" for c, lag in atoms)
        st = "CLEAR (see table)" if name in clear_names else "research · not live · not KEEP holds"
        lines.append(f"| `{name}` | {fam} | {atom_s} | {st} |")
    lines += [
        "",
        "## Explicitly not live",
        "",
        "No recipe is wired into `flatten_robust` or cash/paper. Docs are not live. "
        "Do not treat a long-tape CLEAR as a ship. y2025 must still confirm a "
        "liquid ranked recipe before anyone talks wire.",
        "",
        "## Source",
        "",
        f"`CLOCK_MAP.md` / `OPEN_SAME_ROW_LABELS.md` / `excel_clock_gate.py` · "
        f"PR #153 J-only · this board tip `{payload.get('tip_sha') or 'pending'}`. "
        "Research only. Live frozen.",
        "",
    ]
    os.makedirs(RESEARCH, exist_ok=True)
    open(BOARD_MD, "w", encoding="utf-8").write("\n".join(lines))
    return BOARD_MD


def slim_native(native_scored):
    out = {}
    for uname, sl in native_scored.items():
        out[uname] = {}
        for sname, blob in sl.items():
            recs = {k: slim_wr(v) for k, v in (blob.get("recipes") or {}).items()}
            out[uname][sname] = {"recipes": recs}
    return out


def slim_long(long_fires):
    out = {}
    for sname, sl in (long_fires or {}).items():
        kinds = {}
        for kind, blob in (sl.get("kinds") or {}).items():
            kinds[kind] = {
                "n_days": blob.get("n_days"),
                "recipes": {k: slim_wr(v) for k, v in (blob.get("recipes") or {}).items()},
            }
        out[sname] = {"n_days": sl.get("n_days"), "lo": sl.get("lo"),
                      "hi": sl.get("hi"), "kinds": kinds}
    return out


def run():
    clocks = assert_excel_clock_gate()
    assert_lag_atoms()
    assert_recipes_legal()
    leak = "PASS"
    try:
        for col in SAME_ROW_LEAK_ABORT:
            assert_feature_legal("value", col, 0)
        leak = "FAIL — same-row DF/BB/BQ did not abort"
    except ValueError:
        leak = "PASS"
    slices = {"long": LONG, "y2025": Y2025, "pre813": PRE813}
    native_slices = {"prove": PROVE, "discovery": DISCOVERY, "pooled": ORIG}
    long_fires = {}
    ohlc_n = 0
    if _try_pyarrow() and os.path.isfile(OHLC_PATH):
        print("loading ohlc.parquet …", flush=True)
        hist = load_ohlc_bars()
        print("building liquid name-days (lag candles + open-44) …", flush=True)
        liq = ohlc_liq_rows(hist)
        ohlc_n = len(liq)
        print(f"ohlc_liq rows={ohlc_n}", flush=True)
        print("scoring long fires …", flush=True)
        long_fires = score_recipes(liq, slices, KINDS + ("unranked",))
    else:
        print("ohlc.parquet / pyarrow missing — long tape skipped", flush=True)
    print("native Finviz books …", flush=True)
    nat = native_rows()
    native_scored = {}
    for uname, rows in nat.items():
        kinds = ("listed",) if uname == "join_top8" else ("unranked",)
        native_scored[uname] = score_recipes(rows, native_slices, kinds)
    clears, prov, _fails = collect_clears(slim_long(long_fires))
    payload = {
        "generated": str(date.today()),
        "tip_sha": None,
        "leak": leak,
        "live_untouched": "flatten_robust",
        "gate": gate_payload(),
        "fire_bar": {
            "win": ">55% of fire days beat the same-day no-rule book after fees",
            "min_fires": MIN_FIRES,
            "fee_caveat": FEE_CAVEAT,
        },
        "ohlc_liq_n": ohlc_n,
        "long_fires": slim_long(long_fires),
        "native": slim_native(native_scored),
        "clears": [
            {**c, "wr": slim_wr(c["wr"])} for c in collect_clears(long_fires)[0]
        ],
        "provisional": [
            {**c, "wr": slim_wr(c["wr"])} for c in collect_clears(long_fires)[1]
        ],
        "clocks_generated": clocks.get("generated"),
    }
    # recompute clears from slim for the board table
    payload["clears"], payload["provisional"], _ = collect_clears(payload["long_fires"])
    write_board(payload)
    json.dump(payload, open(BOARD_JSON, "w"), indent=2, default=str)
    print("wrote", BOARD_MD, BOARD_JSON, flush=True)
    print("leak", leak, "clears", len(payload["clears"]), flush=True)
    for c in payload["clears"]:
        print(" CLEAR", c["label"], wr_s(c["wr"]), flush=True)
    return payload


if __name__ == "__main__":
    run()
