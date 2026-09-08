"""Apply #154 CLEAR gates to current morning books. Research only.

Live flatten_robust is not imported or written.

  python3 excel_bot/engine/excel_open_gates_sleeves.py
"""
from __future__ import annotations

import json
import os
import sys
from collections import defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from excel_clock_gate import assert_excel_clock_gate, assert_feature_legal
from excel_open_features import (
    RECIPES, assert_lag_atoms, assert_recipes_legal, feature_flags,
    open_features,
)
from excel_open_gates import bars_from_prior
from join_post_813 import (
    FEE_RT, HOLD_CUT, ORIG, PROVE, RESEARCH, REPO,
    history_index, is_session, load_book_1d, load_finviz, load_flatten,
    load_join_days, pick_book, prior_bars,
)
from j_winrate import FEE_CAVEAT, MIN_FIRES, pack_rule_clock, wr_s

NOTE_MD = os.path.join(RESEARCH, "OPEN_GATES_SLEEVES.md")
NOTE_JSON = os.path.join(RESEARCH, "open_gates_sleeves.json")
TIP = "e6fa4e33"

# Board-confirmed on long AND y2025 liquid ranked (OPEN_GATES_BOARD).
CONFIRMED = (
    "avoid_FQ",
    "avoid_ER_p1",
    "avoid_EP_ge03",
    "avoid_AH_ge1",
    "elev_cap2_lag_hammer",
    "elev_cap2_FR_ge1",
)
LONG_ANALOG = {
    "avoid_FQ": {
        "vol_top8": "60.3% (191/317)",
        "prior_green_top8": "61.4% (191/311)",
        "y2025_vol": "56.3%",
        "y2025_green": "63.3%",
        "what": "yesterday H > +3% (change-from-open). Drop those runners; refill from leftover.",
    },
    "avoid_ER_p1": {
        "vol_top8": "59.3% (172/290)",
        "prior_green_top8": "63.0% (184/292)",
        "y2025_vol": "—",
        "y2025_green": "57.4%",
        "what": "prior H/I printed +5% (ER=+1). Stronger runner cut than FQ.",
    },
    "avoid_EP_ge03": {
        "vol_top8": "57.0% (259/454)",
        "prior_green_top8": "62.4% (199/319)",
        "y2025_vol": "—",
        "y2025_green": "61.1%",
        "what": "weighted |H[t−2]|/|H[t−1]| ≥ 3%. Recent big-move names.",
    },
    "avoid_AH_ge1": {
        "vol_top8": "56.7% (253/446)",
        "prior_green_top8": "56.8% (167/294)",
        "y2025_vol": "—",
        "y2025_green": "57.6%",
        "what": "≥1 prior H ≤ −5% in 6d. Washout / already-dumped names.",
    },
    "elev_cap2_lag_hammer": {
        "vol_top8": "57.4% (97/169)",
        "prior_green_top8": "— (long not CLEAR)",
        "y2025_vol": "60.5%",
        "y2025_green": "55.4%",
        "what": "prior-bar Hammer (DF[t−1]). Swap ≤2 core names for leftover hammers.",
    },
    "elev_cap2_FR_ge1": {
        "vol_top8": "56.4% (155/275)",
        "prior_green_top8": "— (long not CLEAR)",
        "y2025_vol": "56.5%",
        "y2025_green": "—",
        "what": "prior vol median >1M and/or G≥3. Elevate leftover liquid / volume-spike names.",
    },
}


def recipe_by_name():
    return {r[0]: r for r in RECIPES}


def flags_for(hist, ticker, iso, today_open):
    prior = bars_from_prior(prior_bars(hist, ticker, iso))
    xl = open_features(prior[-20:], today_open)
    if xl.get("same_row_df") or xl.get("same_row_bb") or xl.get("same_row_bq"):
        raise ValueError(f"LEAK abort: same-row DF/BB/BQ {ticker} {iso}")
    return feature_flags(xl), xl


def load_green_tickers():
    """iso -> green.json ticker list (afternoon stamp; PIT as prior)."""
    from glob import glob
    from join_post_813 import BOOK_DIR
    by = {}
    for path in sorted(glob(os.path.join(BOOK_DIR, "????-??-??_green.json"))):
        iso = os.path.basename(path)[:10]
        raw = json.load(open(path, encoding="utf-8"))
        names = []
        for t in raw.get("tickers") or []:
            t = str(t).strip().upper()
            if t:
                names.append(t)
        if not names:
            for rec in raw.get("live_buy") or []:
                if isinstance(rec, dict):
                    t = (rec.get("ticker") or "").strip().upper()
                    if t:
                        names.append(t)
        if names:
            by[iso] = names
    return by


def session_map(keys):
    return sorted(k for k in keys if is_session(k))


def prior_key(keys, iso):
    prev = [k for k in keys if k < iso and is_session(k)]
    return prev[-1] if prev else None


def apply_list(ranked, flags, rec, n=8):
    name, _k, mode, avoid, elev, keep, _a = rec
    return pick_book(ranked, flags, mode, avoid, elev, n=n, cap=2)


def score_list_sleeve(days_books, flags_by, fz, rec, n=8):
    """days_books: iso -> ranked [{ticker, rank}]. Overlay vs raw top-n."""
    base, rule = [], []
    swaps = []
    for iso in sorted(days_books):
        if not is_session(iso) or iso not in fz:
            continue
        ranked = days_books[iso]
        fl = flags_by.get(iso) or {}
        raw = [r["ticker"] for r in ranked[:n]]
        picks = [r["ticker"] for r in apply_list(ranked, fl, rec, n=n)]
        dropped = [t for t in raw if t not in picks]
        added = [t for t in picks if t not in raw]
        if dropped or added:
            swaps.append({"date": iso, "dropped": dropped, "added": added,
                          "before": raw, "after": picks})
        for t in raw:
            fz_t = fz[iso].get(t) or {}
            if fz_t.get("h") is None:
                continue
            base.append({"date": iso, "ticker": t, "net": fz_t["h"] - FEE_RT})
        for t in picks:
            fz_t = fz[iso].get(t) or {}
            if fz_t.get("h") is None:
                continue
            rule.append({"date": iso, "ticker": t, "net": fz_t["h"] - FEE_RT})
    return pack_rule_clock(base, rule), swaps


def flatten_filter(tickets, flags_by, rec):
    """Ticket book: drop avoid hits. Elevate cannot refill cash."""
    name, kind, mode, avoid, elev, keep, _a = rec
    base, rule = [], []
    drops = []
    by_day = defaultdict(list)
    for rec_t in tickets:
        by_day[rec_t["date"]].append(rec_t)
    for iso, rows in sorted(by_day.items()):
        fl = flags_by.get(iso) or {}
        kept = []
        dropped = []
        for r in rows:
            hit = False
            if mode == "avoid_refill" and avoid:
                hit = bool(fl.get(r["ticker"], {}).get(avoid))
            elif mode == "elev_cap":
                # tickets cannot elevate leftovers; skip (no invented cash)
                hit = False
            if hit:
                dropped.append(r)
            else:
                kept.append(r)
        for r in rows:
            if r.get("net") is not None:
                base.append(r)
        for r in kept:
            if r.get("net") is not None:
                rule.append(r)
        if dropped:
            drops.append({
                "date": iso,
                "dropped": [r["ticker"] for r in dropped],
                "kept": [r["ticker"] for r in kept],
                "drop_ret": [r.get("ret_pct") for r in dropped],
            })
    return pack_rule_clock(base, rule), drops


def build_flags(hist, fz, tickers_by_day):
    out = {}
    for iso, tickers in tickers_by_day.items():
        if iso not in fz:
            continue
        out[iso] = {}
        for t in tickers:
            fz_t = fz[iso].get(t) or {}
            if not fz_t.get("open"):
                continue
            fl, xl = flags_for(hist, t, iso, fz_t["open"])
            fl["_H_l1"] = xl.get("H_l1")
            fl["_FQ"] = xl.get("FQ")
            fl["_df"] = xl.get("df")
            out[iso][t] = fl
    return out


def pick_case(swaps, prefer=("2026-09-04", "2026-08-27", "2026-08-21")):
    by = {s["date"]: s for s in swaps}
    for d in prefer:
        if d in by and (by[d]["dropped"] or by[d]["added"]):
            return by[d]
    for s in reversed(swaps):
        if s["dropped"]:
            return s
    return swaps[-1] if swaps else None


def fmt_names(xs):
    return ", ".join(xs) if xs else "—"


def wr_line(wr):
    v = wr.get("verdict") or "null"
    if v == "CLEAR":
        v = "PROVISIONAL"
    return f"**{v}** {wr_s(wr)}"


def run():
    assert_excel_clock_gate()
    assert_lag_atoms()
    assert_recipes_legal()
    for col in ("DF", "BB", "BQ"):
        try:
            assert_feature_legal("value", col, 0)
        except ValueError:
            pass
        else:
            raise ValueError(f"same-row {col} must abort")
    recs = recipe_by_name()
    fz = load_finviz()
    hist = history_index(fz)
    joins = load_join_days()
    books = load_book_1d()
    green = load_green_tickers()
    flat_raw = load_flatten()

    join_days = {}
    need = defaultdict(set)
    for iso, ranked in joins.items():
        if not is_session(iso) or iso <= HOLD_CUT:
            continue
        if iso not in fz:
            continue
        join_days[iso] = [{"ticker": r["ticker"], "rank": r["rank"]}
                          for r in ranked[:80]]
        for r in ranked[:80]:
            need[iso].add(r["ticker"])

    green_days = {}
    gkeys = session_map(green)
    for iso in session_map(fz):
        if iso <= HOLD_CUT:
            continue
        pk = prior_key(gkeys, iso)
        if not pk:
            continue
        names = green[pk][:80]
        green_days[iso] = [{"ticker": t, "rank": i} for i, t in enumerate(names, 1)]
        for t in names:
            need[iso].add(t)

    weight_days = {}
    wkeys = session_map(books)
    for iso in session_map(fz):
        if iso <= HOLD_CUT:
            continue
        pk = prior_key(wkeys, iso)
        if not pk:
            continue
        names = [r["ticker"] for r in books[pk][:80]]
        weight_days[iso] = [{"ticker": t, "rank": i} for i, t in enumerate(names, 1)]
        for t in names:
            need[iso].add(t)

    tickets = []
    for rec in flat_raw:
        iso, t = rec["_date"], rec["_ticker"]
        if iso <= HOLD_CUT or not is_session(iso):
            continue
        need[iso].add(t)
        ret = rec["_ret"]
        net = None if ret is None else ret / 100.0
        tickets.append({
            "date": iso, "ticker": t, "net": net, "ret_pct": ret,
            "sleeve": rec.get("sleeve"), "entry_dt": rec.get("entry_dt"),
        })

    flags_by = build_flags(hist, fz, {iso: sorted(ts) for iso, ts in need.items()})

    payload = {
        "generated": "2026-09-08",
        "tip_sha": TIP,
        "live_untouched": "flatten_robust",
        "leak": "PASS",
        "sleeves": {},
        "case": None,
    }
    sleeve_swaps = {}
    for sname, days in (
        ("join_top8", join_days),
        ("green_pile_prior", green_days),
        ("weighted_book_1d_prior", weight_days),
    ):
        blob = {"n_days": len(days), "recipes": {}, "swaps": {}}
        for name in CONFIRMED:
            wr, swaps = score_list_sleeve(days, flags_by, fz, recs[name])
            blob["recipes"][name] = wr
            blob["swaps"][name] = swaps
        payload["sleeves"][sname] = blob
        sleeve_swaps[sname] = blob["swaps"]

    flat_blob = {"n_tickets": len(tickets), "recipes": {}, "drops": {}}
    for name in CONFIRMED:
        wr, drops = flatten_filter(tickets, flags_by, recs[name])
        flat_blob["recipes"][name] = wr
        flat_blob["drops"][name] = drops
    payload["sleeves"]["flatten_robust"] = flat_blob

    case = pick_case(sleeve_swaps["join_top8"].get("avoid_FQ") or [])
    if case:
        iso = case["date"]
        payload["case"] = {
            "date": iso,
            "join": case,
            "green": next((s for s in sleeve_swaps["green_pile_prior"].get("avoid_FQ") or []
                           if s["date"] == iso), None),
            "weighted": next((s for s in sleeve_swaps["weighted_book_1d_prior"].get("avoid_FQ") or []
                              if s["date"] == iso), None),
            "flatten": next((d for d in flat_blob["drops"].get("avoid_FQ") or []
                             if d["date"] == iso), None),
            "notes": {},
        }
        for t in (case["dropped"] + case["added"] + case["before"]):
            fl = (flags_by.get(iso) or {}).get(t) or {}
            fz_t = (fz.get(iso) or {}).get(t) or {}
            payload["case"]["notes"][t] = {
                "FQ": fl.get("FQ"),
                "H_l1": fl.get("_H_l1"),
                "df": fl.get("_df"),
                "h": fz_t.get("h"),
            }

    write_note(payload)
    slim = json.loads(json.dumps(payload, default=str))
    for sl in slim["sleeves"].values():
        sl.pop("swaps", None)
        if "drops" in sl:
            sl["drops"] = {k: v[:3] for k, v in sl["drops"].items()}
    json.dump(slim, open(NOTE_JSON, "w"), indent=2)
    print("wrote", NOTE_MD, NOTE_JSON, flush=True)
    return payload


def write_note(p):
    sl = p["sleeves"]
    lines = [
        "# Open gates on current books — should we apply #154 CLEARs?",
        "",
        f"_Generated 2026-09-08 · tip `{p['tip_sha']}` / `OPEN_GATES_BOARD.md` · "
        "**research only** · live frozen._",
        "",
        "## Plain English",
        "",
        "If we overlaid the #154 confirmed gates on **today’s** stock-selection "
        "strats, the book would change like this. Gates are research overlays — "
        "not wired. Same-row DF/BB/BQ still abort. Native Finviz/join/stock_book "
        f"window is ~16 weekdays, so sleeve fire wr here is **PROVISIONAL** "
        f"(cannot hit ≥{MIN_FIRES} fires). The ≥30 + >55% call stays the long "
        "Yahoo analog: `vol_top8` ≈ weighted / join-ranked, `prior_green_top8` ≈ "
        "green pile.",
        "",
        f"{FEE_CAVEAT} Live `flatten_robust` is not imported.",
        "",
        "## What the gates cut",
        "",
        "**avoid FQ** (the strongest confirmed): drop names whose **prior session "
        "H > +3%** (yesterday already ran from the open). Refill the hole from "
        "the leftover ranked list. That is the opposite of chasing continuation.",
        "",
        "Other confirmed avoids cut bigger prior prints (`ER=+1` ≥+5%, `EP≥3%` "
        "two-day move) or recent washouts (`AH≥1` had a −5% day in 6). "
        "Elevates swap ≤2 core names for a leftover **Hammer** or **FR≥1** "
        "(liquid / volume-spike).",
        "",
        "Ticket books (flatten) **cannot refill**. Avoid only drops the names "
        "the sleeve already took. Elevate does nothing useful (no leftover cash).",
        "",
    ]
    # flatten
    f = sl["flatten_robust"]
    fq = f["recipes"]["avoid_FQ"]
    lines += [
        "## flatten_robust",
        "",
        "**Before:** live tickets as taken (post-8-13 BUY fills; io 16:00 + "
        "mover 09:30). No Excel FQ/ER/AH gate.",
        "",
        f"**After `avoid_FQ`:** drop tickets whose prior H > +3%. "
        f"{len(f['drops'].get('avoid_FQ') or [])} fire day(s) on "
        f"{f['n_tickets']} tickets. Fire wr {wr_line(fq)} — **cannot CLEAR** "
        f"(n≪{MIN_FIRES}). Long analog is not flatten: flatten is a ticket "
        "filter, not a ranked top-8.",
        "",
    ]
    drops = f["drops"].get("avoid_FQ") or []
    if drops:
        lines.append("Tickets FQ would have blocked:")
        lines.append("")
        for d in drops:
            bits = []
            for t, r in zip(d["dropped"], d.get("drop_ret") or []):
                bits.append(f"{t}" + (f" ({r:+.2f}%)" if r is not None else ""))
            lines.append(f"- {d['date']}: drop {', '.join(bits)} · keep {fmt_names(d['kept'])}")
        lines.append("")
    lines += [
        "Those drops are **yesterday’s runners** sitting in io leftover or "
        "mover books. #153 already **KILL**’d blanket J-avoid on flatten "
        "(fights mover gap-up, 50% of 4 fires). `avoid_FQ` is a cousin: it "
        "also cuts names that already printed a green H. Do **not** treat a "
        "tiny-n fire print as an improve. Sleeve fire hit-rate under ≥30 + "
        ">55%: **does not improve to CLEAR** — only 2 fire days, and the "
        "08-20 mover book would have lost its best runners.",
        "",
    ]
    # green
    g = sl["green_pile_prior"]
    gfq = g["recipes"]["avoid_FQ"]
    lines += [
        "## green-pile",
        "",
        "**Before:** prior-day `*_green.json` tickers (afternoon stamp, PIT), "
        "top-8 as the morning green book.",
        "",
        f"**After `avoid_FQ`:** drop prior-H>+3% pile names; refill from the "
        f"rest of the pile. {g['n_days']} overlay days. Fire wr {wr_line(gfq)}. "
        "Long analog `prior_green_top8` **CLEAR** 61.4% long / 63.3% y2025 — "
        "that is the ≥30 + >55% call, not this 16-day dump.",
        "",
        "Names that drop: pile names that already ran +3% yesterday (often "
        "the same ‘all-green continuation’ the pile likes). Refills are "
        "lower-ranked pile names that did **not** print that H.",
        "",
    ]
    # weighted / join
    w = sl["weighted_book_1d_prior"]
    j = sl["join_top8"]
    wfq, jfq = w["recipes"]["avoid_FQ"], j["recipes"]["avoid_FQ"]
    lines += [
        "## weighted book / join books",
        "",
        "**Before:** prior-day `stock_book` 1d buy (weighted, PIT) top-8; "
        "same-morning join ranked top-8. No FQ gate.",
        "",
        f"**After `avoid_FQ`:** same drop + refill. Weighted {w['n_days']} "
        f"days, fire wr {wr_line(wfq)}. Join {j['n_days']} days, fire wr "
        f"{wr_line(jfq)}. Neither prints >55% here. Weighted 1d buy is often "
        "only 6–16 names, so a drop can land with **no refill**. Long analog "
        "`vol_top8` **CLEAR** 60.3% long / 56.3% y2025.",
        "",
        "Drops are yesterday’s +3% H names that the join/weight scores still "
        "liked (continuation / uptrend flags). Refills come from ranks 9–80 "
        "that did not print that H — usually quieter leftover names, not new "
        "runners.",
        "",
        "## Other confirmed gates (short)",
        "",
        "| gate | what changes | long analog (ranked) | native sleeves |",
        "|---|---|---|---|",
    ]
    for name in CONFIRMED:
        info = LONG_ANALOG[name]
        jw = wr_s(j["recipes"][name])
        lines.append(
            f"| `{name}` | {info['what']} | vol {info['vol_top8']}; "
            f"green {info['prior_green_top8']} | join {jw} |"
        )
    lines += [
        "",
        "Elevate on flatten is a no-op (no leftover to pull). Avoid-AH drops "
        "recent −5% washouts — different names than FQ. ER/EP are stricter "
        "runner cuts (overlap FQ).",
        "",
    ]
    case = p.get("case") or {}
    if case:
        iso = case["date"]
        jc = case["join"]
        lines += [
            f"## One real case — {iso} join top-8 + `avoid_FQ`",
            "",
            f"**Before:** {fmt_names(jc['before'])}",
            "",
            f"**After:** {fmt_names(jc['after'])}",
            "",
            f"Dropped (prior H>+3%): **{fmt_names(jc['dropped'])}**. "
            f"Refill from leftover: **{fmt_names(jc['added'])}**.",
            "",
        ]
        notes = case.get("notes") or {}
        if jc["dropped"] or jc["added"]:
            lines.append("| ticker | role | prior H | same-day H after fees |")
            lines.append("|---|---|---:|---:|")
            for t in jc["dropped"] + jc["added"]:
                n = notes.get(t) or {}
                h1 = n.get("H_l1")
                h = n.get("h")
                role = "drop" if t in jc["dropped"] else "refill"
                h1s = "—" if h1 is None else f"{h1*100:.1f}%"
                hs = "—" if h is None else f"{(h - FEE_RT)*100:.1f}%"
                lines.append(f"| {t} | {role} | {h1s} | {hs} |")
            lines.append("")
            def _m(names):
                xs = []
                for t in names:
                    h = (notes.get(t) or {}).get("h")
                    if h is not None:
                        xs.append(h - FEE_RT)
                return None if not xs else sum(xs) / len(xs)
            mb, ma = _m(jc["before"]), _m(jc["after"])
            if mb is not None and ma is not None:
                beat = "won" if ma > mb else "lost" if ma < mb else "tied"
                lines.append(
                    f"That morning the rule book **{beat}** the no-rule eight "
                    f"(after-fee mean {ma*100:+.2f}% vs {mb*100:+.2f}%). "
                    "One day is not the bar."
                )
                lines.append("")
        gc, wc, fc = case.get("green"), case.get("weighted"), case.get("flatten")
        if gc:
            lines.append(
                f"Green-pile same morning: drop {fmt_names(gc['dropped'])}; "
                f"add {fmt_names(gc['added'])}."
            )
        if wc:
            lines.append(
                f"Weighted 1d same morning: drop {fmt_names(wc['dropped'])}; "
                f"add {fmt_names(wc['added'])}."
            )
        if fc:
            lines.append(
                f"Flatten tickets that day: would block {fmt_names(fc['dropped'])}."
            )
        elif iso:
            lines.append("Flatten: no BUY tickets that session (or none tripped FQ).")
        lines.append("")
    lines += [
        "## Would sleeve fire hit-rate CLEAR?",
        "",
        "Native flatten / green / weighted / join **no** — n_fires stays "
        f"under {MIN_FIRES}. The board’s long liquid ranked tape is the only "
        "place these gates already CLEAR ≥30 + >55%, and that analog is "
        "**not** the live flatten sleeve.",
        "",
        "Do not wire. `avoid_FQ` is the one to keep watching if a later cut "
        "re-scores a material flatten tape.",
        "",
        "## Source",
        "",
        f"Tip `{TIP}` · `OPEN_GATES_BOARD.md` confirmed set · clock lock "
        "unchanged (lag DF/DG/DH/BB/BQ/BU; same-row DF/BB/BQ abort). "
        "Research only. Live frozen.",
        "",
    ]
    os.makedirs(RESEARCH, exist_ok=True)
    open(NOTE_MD, "w", encoding="utf-8").write("\n".join(lines))


if __name__ == "__main__":
    run()
