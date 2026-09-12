"""Open-J avoid/elevate on every dashboard morning-pick sleeve.

Question: after fees, does avoid (drop J≥0) or elev_cap2 (J≤−1%) beat
that sleeve alone? Leak bar unchanged — open J only. Live frozen.

List sleeves (green / weighted / unweighted books) refill from the
ranked leftover. Ticket sleeves (fills) filter the names they actually
took; elev is drop ≤2 J≥0 / day (no invented cash).

  python3 excel_bot/engine/j_sleeve_prove.py
"""
from __future__ import annotations

import csv
import json
import os
import re
import sys
from collections import defaultdict
from glob import glob

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from join_post_813 import (  # noqa: E402
    BEAT_PP, BOOK_DIR, DISCOVERY, FEE_RT, HOLD_CUT, ORIG, PROVE, REPO,
    excel_features, is_session, load_book_1d,
    load_flatten, mean, pick_book, prior_date, score_book,
)
from j_winrate import (  # noqa: E402
    FEE_CAVEAT, MIN_FIRES, hit_s, iter_long_fire_rows, pack_rule_clock, wr_s,
)

SLEEVE_JSON = os.path.join(os.path.dirname(HERE), "research", "j_sleeve_prove.json")
CATALOG = os.path.join(REPO, "data", "strategy_board", "catalog.json")
FM_DIR = os.path.join(REPO, "03_scoreboard", "factor_mine")
FLA_JSON = os.path.join(REPO, "03_scoreboard", "flatten_lookback_action.json")
PAPER_RT = os.path.join(REPO, "data", "paper", "roundtrips.csv")
COMBINE = os.path.join(REPO, "data", "sleeve_combine", "bt_trades.csv")
BOOK_PAPER = os.path.join(REPO, "data", "book_paper", "trades.csv")
MOVER_PAPER = os.path.join(REPO, "data", "mover_paper", "trades.csv")
MIN_N = 15
MIN_PROVE_N = 12
FEATURED = (
    "flatten_robust", "flatten_h5", "flatten_live_h5",
    "green_pile_prior", "green_book_prior",
    "weighted_book_1d_prior", "unweighted_book_prior",
)

BUY_RE = re.compile(
    r"^\|\s*(\d{4}-\d{2}-\d{2})\s+09:30 ET\s+\|\s+\*\*BUY\*\*\s+\|\s+`([^`]+)`"
)
SIDE_RE = re.compile(r"Side \*\*(long|short)\*\*", re.I)


def _pp(x):
    return None if x is None else f"{x:+.2f} pp"


def _pct(x):
    return None if x is None else f"{x * 100:+.2f}%"


def _ghost_s(g):
    if not g:
        return "—"
    if isinstance(g, str):
        return g
    return f"{g.get('name', '—')}/{g.get('month', '—')}/{g.get('day', '—')}"


def attach_name_day(iso, ticker, hist, fz, panel_index, sleeve_net=None):
    """Open J + after-fee H. Never uses same-row H as a feature."""
    t = (ticker or "").strip().upper()
    panel = panel_index.get((iso, t)) if panel_index else None
    fz_t = (fz.get(iso) or {}).get(t) or {}
    today_open = fz_t.get("open")
    if panel and panel.get("open"):
        today_open = panel["open"]
    xl = excel_features(hist, t, iso, today_open)
    j = xl["J"] if xl.get("J_fresh") else None
    h = None
    if panel and panel.get("h") is not None:
        h = panel["h"]
    elif fz_t.get("h") is not None:
        h = fz_t["h"]
    h_net = None if h is None else h - FEE_RT
    if panel and panel.get("net") is not None:
        h_net = panel["net"]
    i = None
    if panel and panel.get("i") is not None:
        i = panel["i"]
    elif fz_t.get("i") is not None:
        i = fz_t["i"]
    i_net = None if i is None else i - FEE_RT
    if panel and panel.get("i_net") is not None:
        i_net = panel["i_net"]
    flags = {
        "J_fresh": bool(xl.get("J_fresh")),
        "J_ge0": j is not None and j >= 0,
        "J_lt0": j is not None and j < 0,
        "J_le-1": j is not None and j <= -0.01,
    }
    return {
        "date": iso, "ticker": t, "J": j, "xl": xl, "flags": flags,
        "net": h_net, "h_net": h_net, "i": i, "i_net": i_net,
        "sleeve_net": sleeve_net,
        "session": is_session(iso),
    }


def parse_factor_mine_md(path):
    """Morning BUY fills from a factor-mine scoreboard card."""
    name = os.path.splitext(os.path.basename(path))[0]
    side = "long"
    buys = []
    try:
        text = open(path, encoding="utf-8", errors="replace").read()
    except OSError:
        return {"name": name, "side": side, "buys": buys}
    m = SIDE_RE.search(text)
    if m:
        side = m.group(1).lower()
    for line in text.splitlines():
        bm = BUY_RE.match(line)
        if not bm:
            continue
        buys.append({"date": bm.group(1), "ticker": bm.group(2).strip().upper()})
    return {"name": name, "side": side, "buys": buys}


def load_catalog():
    if not os.path.isfile(CATALOG):
        return []
    raw = json.load(open(CATALOG, encoding="utf-8"))
    return raw.get("rows") or []


def load_flatten_wishlist():
    by = {}
    if not os.path.isfile(FLA_JSON):
        return by
    raw = json.load(open(FLA_JSON, encoding="utf-8"))
    for rec in raw.get("daily") or []:
        iso = rec.get("date")
        names = [str(t).strip().upper() for t in (rec.get("tickers") or []) if t]
        if iso and names:
            by[iso] = names
    return by


def _live_buy_tickers(path, pile_only=False):
    try:
        raw = json.load(open(path, encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    names = []
    for rec in raw.get("live_buy") or []:
        if not isinstance(rec, dict):
            continue
        t = (rec.get("ticker") or "").strip().upper()
        if not t:
            continue
        if pile_only and rec.get("in_pile") is False:
            continue
        names.append(t)
    return names


def load_prior_book_map(pattern, pile_only=False):
    """iso -> prior-day ranked live_buy (afternoon stamp is PIT as prior)."""
    files = {}
    for path in sorted(glob(os.path.join(BOOK_DIR, pattern))):
        iso = os.path.basename(path)[:10]
        names = _live_buy_tickers(path, pile_only=pile_only)
        if names:
            files[iso] = names
    out = {}
    keys = sorted(files)
    # map onto later session dates via caller
    return files, keys


def ranked_rows(names):
    return [{"ticker": t, "rank": i} for i, t in enumerate(names, 1)]


def recipe_verdict(vs_pp, n, ghost_pass, h_mean):
    if n < MIN_N:
        return "null"
    if vs_pp is None:
        return "null"
    edge = vs_pp >= BEAT_PP and (h_mean is None or h_mean > 0)
    if edge and ghost_pass:
        return "KEEP"
    if edge:
        return "CONDITIONAL"
    return "KILL"


def _best_bar(slice_dict):
    """Best recipe bar that has enough n. Thin recipes do not promote."""
    def rank(v):
        return {"KEEP": 3, "CONDITIONAL": 2, "KILL": 1, "null": 0}.get(v, 0)

    keys = ("avoid_J_ge0", "elev_cap2_J_le-1")
    best = "null"
    for rec in ((slice_dict or {}).get(k) for k in keys):
        if not rec or (rec.get("n") or 0) < MIN_N:
            continue
        if rank(rec.get("family_bar")) > rank(best):
            best = rec.get("family_bar")
    return best


def family_verdict(pooled, prove, clock_ok=True, sleeve_disagree=False):
    """KEEP only if prove clears. Else CONDITIONAL / KILL / null."""
    def rank(v):
        return {"KEEP": 3, "CONDITIONAL": 2, "KILL": 1, "null": 0}.get(v, 0)

    p_best = _best_bar(prove)
    o_best = _best_bar(pooled)
    if p_best == "null" and o_best == "null":
        out = "null"
    elif p_best == "KEEP":
        out = "KEEP"
    elif p_best in ("CONDITIONAL",) or o_best in ("KEEP", "CONDITIONAL"):
        out = "CONDITIONAL"
    else:
        out = "KILL"
    if out == "KEEP" and (not clock_ok or sleeve_disagree):
        out = "CONDITIONAL"
    return out


def _pack_recipe(name, kind, trades, baseline, spy):
    rec = score_book(name, kind, trades, baseline, spy)
    rec["holdout_mean"] = mean([tr["net"] for tr in trades if tr.get("net") is not None])
    sl = [tr["sleeve_net"] for tr in trades if tr.get("sleeve_net") is not None]
    rec["sleeve_mean"] = mean(sl)
    rec["n_sleeve"] = len(sl)
    if baseline and baseline.get("sleeve_mean") is not None and rec["sleeve_mean"] is not None:
        rec["vs_sleeve_native_pp"] = (rec["sleeve_mean"] - baseline["sleeve_mean"]) * 100
    else:
        rec["vs_sleeve_native_pp"] = None
    g = rec.get("ghost") or {}
    rec["family_bar"] = recipe_verdict(
        rec.get("vs_fullscan_pp"), rec.get("n") or 0, bool(g.get("pass")),
        rec.get("holdout_mean"),
    )
    return rec


def elev_drop2(rows):
    """Ticket analogue: drop ≤2 J≥0 per day (largest J first)."""
    by = defaultdict(list)
    for r in rows:
        by[r["date"]].append(r)
    out = []
    for _iso, day in by.items():
        jpos = sorted(
            [r for r in day if r["flags"].get("J_ge0")],
            key=lambda r: (r.get("J") or 0), reverse=True,
        )
        keep = [r for r in day if not r["flags"].get("J_ge0")]
        out.extend(keep + jpos[2:])
    return out


def score_ticket_rows(name, family, rows, spy, note="", clock_ok=True):
    """Filter overlay on actual morning fills."""
    sess = [r for r in rows if r.get("session") and r.get("net") is not None
            and r["date"] > HOLD_CUT]
    if not sess:
        return _empty_sleeve(name, family, note or "no morning fills with H",
                             clock_ok=clock_ok)

    def slice_rows(lo, hi):
        return [r for r in sess if lo <= r["date"] <= hi]

    slices = {}
    for sname, (lo, hi) in (("discovery", DISCOVERY), ("prove", PROVE),
                            ("pooled", ORIG)):
        sub = slice_rows(lo, hi)
        base_tr = sub
        dummy = {"holdout_mean": None, "sleeve_mean": mean(
            [r["sleeve_net"] for r in base_tr if r.get("sleeve_net") is not None]
        )}
        base = _pack_recipe(f"{name}_all", "baseline", base_tr, None, spy)
        dummy["holdout_mean"] = base["holdout_mean"]
        dummy["sleeve_mean"] = base.get("sleeve_mean")
        # Standing avoid: drop J≥0. No-J stays (rule cannot fire).
        avoid = [r for r in sub if not r["flags"].get("J_ge0")]
        elev = elev_drop2(sub)
        a = _pack_recipe("avoid_J_ge0", "avoid", avoid, dummy, spy)
        e = _pack_recipe("elev_cap2_J_le-1", "elevate", elev, dummy, spy)
        a["winrate"] = pack_rule_clock(sub, avoid)
        e["winrate"] = pack_rule_clock(sub, elev)
        slices[sname] = {
            "lo": lo, "hi": hi,
            "baseline": base, "avoid_J_ge0": a, "elev_cap2_J_le-1": e,
        }

    sleeve_disagree = False
    pool = slices["pooled"]
    if (pool["avoid_J_ge0"].get("vs_sleeve_native_pp") is not None
            and pool["avoid_J_ge0"]["vs_sleeve_native_pp"] <= -BEAT_PP
            and (pool["avoid_J_ge0"].get("vs_fullscan_pp") or 0) >= BEAT_PP):
        sleeve_disagree = True
    fam = family_verdict(slices["pooled"], slices["prove"],
                         clock_ok=clock_ok, sleeve_disagree=sleeve_disagree)
    if len(sess) < MIN_N:
        fam = "null"
    return {
        "name": name, "family": family, "kind": "ticket",
        "verdict": fam, "clock_ok": clock_ok, "note": note,
        "n_all": len(sess), "n_dates": len({r["date"] for r in sess}),
        "sleeve_disagree": sleeve_disagree,
        "slices": slices,
    }


def score_list_days(name, family, day_lists, hist, fz, panel_index, spy,
                    note="", clock_ok=True, top_n=8):
    """Ranked morning list: avoid refill + elev_cap2 vs list-alone top-n."""
    rows_by_slice = {k: {"base": [], "avoid": [], "elev": []}
                     for k in ("discovery", "prove", "pooled")}
    n_days = 0
    for iso, names in sorted(day_lists.items()):
        if not is_session(iso) or iso <= HOLD_CUT or iso not in fz:
            continue
        ranked = ranked_rows(names)
        fl = {}
        for t in names:
            rec = attach_name_day(iso, t, hist, fz, panel_index)
            fl[t] = rec["flags"]
        picks_raw = pick_book(ranked, fl, "raw", n=top_n)
        picks_av = pick_book(ranked, fl, "avoid_refill", avoid_key="J_ge0", n=top_n)
        picks_el = pick_book(ranked, fl, "elev_cap", avoid_key="J_ge0",
                             elev_key="J_le-1", n=top_n)

        def as_tr(picks):
            out = []
            for r in picks:
                rec = attach_name_day(iso, r["ticker"], hist, fz, panel_index)
                if rec.get("net") is not None:
                    out.append(rec)
            return out

        raw_tr, av_tr, el_tr = as_tr(picks_raw), as_tr(picks_av), as_tr(picks_el)
        if not raw_tr:
            continue
        n_days += 1
        for sname, (lo, hi) in (("discovery", DISCOVERY), ("prove", PROVE),
                                ("pooled", ORIG)):
            if lo <= iso <= hi:
                rows_by_slice[sname]["base"].extend(raw_tr)
                rows_by_slice[sname]["avoid"].extend(av_tr)
                rows_by_slice[sname]["elev"].extend(el_tr)

    if n_days == 0:
        return _empty_sleeve(name, family, note or "no list days with H",
                             clock_ok=clock_ok)

    slices = {}
    for sname, (lo, hi) in (("discovery", DISCOVERY), ("prove", PROVE),
                            ("pooled", ORIG)):
        blob = rows_by_slice[sname]
        dummy = {"holdout_mean": None}
        base = _pack_recipe(f"{name}_all", "baseline", blob["base"], None, spy)
        dummy["holdout_mean"] = base["holdout_mean"]
        a = _pack_recipe("avoid_J_ge0", "avoid", blob["avoid"], dummy, spy)
        e = _pack_recipe("elev_cap2_J_le-1", "elevate", blob["elev"], dummy, spy)
        a["winrate"] = pack_rule_clock(blob["base"], blob["avoid"])
        e["winrate"] = pack_rule_clock(blob["base"], blob["elev"])
        slices[sname] = {
            "lo": lo, "hi": hi,
            "baseline": base, "avoid_J_ge0": a, "elev_cap2_J_le-1": e,
        }
    fam = family_verdict(slices["pooled"], slices["prove"], clock_ok=clock_ok)
    n_all = slices["pooled"]["baseline"]["n"]
    if n_all < MIN_N:
        fam = "null"
    return {
        "name": name, "family": family, "kind": "list",
        "verdict": fam, "clock_ok": clock_ok, "note": note,
        "n_all": n_all, "n_dates": n_days,
        "sleeve_disagree": False,
        "slices": slices,
    }


def _empty_sleeve(name, family, note, clock_ok=True, verdict="null"):
    return {
        "name": name, "family": family, "kind": "empty",
        "verdict": verdict, "clock_ok": clock_ok, "note": note,
        "n_all": 0, "n_dates": 0, "sleeve_disagree": False, "slices": {},
    }


def _dt_date(s):
    return (s or "")[:10]


def load_csv_tickets(path, date_field, ticker_field="ticker", ret_field="ret_pct",
                     extra=None):
    rows = []
    if not os.path.isfile(path):
        return rows
    with open(path, encoding="utf-8", errors="replace") as fh:
        for rec in csv.DictReader(fh):
            iso = _dt_date(rec.get(date_field) or rec.get("entry_date")
                           or rec.get("buy_date") or rec.get("entry_dt"))
            t = (rec.get(ticker_field) or rec.get("Ticker") or "").strip().upper()
            if not iso or not t:
                continue
            item = {"date": iso, "ticker": t, "raw": rec}
            if ret_field:
                try:
                    v = rec.get(ret_field)
                    item["sleeve_net"] = None if v in (None, "") else float(v) / 100.0
                except ValueError:
                    item["sleeve_net"] = None
            if extra:
                extra(item, rec)
            rows.append(item)
    return rows


def paper_sleeve_net(rec):
    try:
        px = float(rec.get("buy_px") or 0)
        sh = float(rec.get("shares") or 0)
        pnl = rec.get("realized_pnl")
        if not px or not sh or pnl in (None, ""):
            return None
        return float(pnl) / (px * sh)
    except (TypeError, ValueError, ZeroDivisionError):
        return None


def attach_ticket_list(raw_rows, hist, fz, panel_index):
    out = []
    for rec in raw_rows:
        iso, t = rec["date"], rec["ticker"]
        if not is_session(iso):
            continue
        row = attach_name_day(iso, t, hist, fz, panel_index,
                              sleeve_net=rec.get("sleeve_net"))
        row["entry_clock"] = rec.get("entry_clock")
        out.append(row)
    return out


def prior_lists_on_sessions(files, session_days):
    """Map each session to the latest prior book file."""
    keys = sorted(files)
    out = {}
    for iso in session_days:
        prev = prior_date(keys, iso)
        if prev and files.get(prev):
            out[iso] = files[prev]
    return out


def combine_match(catalog_name, rec):
    hold = (rec.get("hold") or "").lower()
    source = (rec.get("source") or "").lower()
    n = catalog_name.lower()
    want_hold = None
    for hz in ("1d", "3d", "1w", "2w"):
        if f" {hz} " in f" {n} " or n.endswith(hz) or f"_{hz}_" in n or n.endswith(f"_{hz}"):
            # catalog names like "Combine 3d io_boost"
            if hz in n:
                want_hold = hz
                break
    if want_hold and hold != want_hold:
        return False
    if "io_only" in n and source != "io":
        return False
    if "mover_only" in n and source != "mover":
        return False
    return True


def run(panel=None, flags_by_day=None, spy=None, fz=None, hist=None,
        panel_index=None):
    if spy is None:
        spy = {}
    if panel_index is None:
        panel_index = {(r["date"], r["ticker"]): r for r in (panel or [])}

    catalog = load_catalog()
    session_days = sorted({iso for iso in (fz or {}) if is_session(iso)})

    sleeves = []
    seen = set()

    def add(rec):
        if rec["name"] in seen:
            return
        seen.add(rec["name"])
        sleeves.append(rec)

    # --- flatten_robust live tickets ---
    flat_raw = []
    for rec in load_flatten():
        flat_raw.append({
            "date": rec["_date"], "ticker": rec["_ticker"],
            "sleeve_net": None if rec["_ret"] is None else rec["_ret"] / 100.0,
            "entry_clock": rec.get("entry_dt") or rec.get("entry_date"),
        })
    flat_rows = attach_ticket_list(flat_raw, hist, fz, panel_index)
    add(score_ticket_rows(
        "flatten_robust", "sleeve merge", flat_rows, spy,
        note="live tickets; io often 16:00 / 3d. Blanket J-avoid fights mover gap-up.",
        clock_ok=False,
    ))

    # --- factor mine BUY fills ---
    for path in sorted(glob(os.path.join(FM_DIR, "*.md"))):
        parsed = parse_factor_mine_md(path)
        if parsed["side"] == "short":
            add(_empty_sleeve(
                parsed["name"], "factor mine",
                "short sleeve — J avoid/elevate is a long overlay; not scored",
            ))
            continue
        raw = [{"date": b["date"], "ticker": b["ticker"]} for b in parsed["buys"]]
        rows = attach_ticket_list(raw, hist, fz, panel_index)
        note = "factor-mine 09:30 BUY fills vs same-day H after fees"
        if parsed["name"].startswith("flatten"):
            note += "; wishlist exists for context, overlay is on fills (cash book)"
        add(score_ticket_rows(parsed["name"], "factor mine", rows, spy, note=note,
                              clock_ok=True))

    # --- green / weighted / unweighted (prior-day PIT lists) ---
    green_files, _ = load_prior_book_map("????-??-??_green.json", pile_only=False)
    pile_files, _ = load_prior_book_map("????-??-??_green.json", pile_only=True)
    un_files, _ = load_prior_book_map("????-??-??_unweighted.json", pile_only=False)
    w_books = load_book_1d()
    w_files = {iso: [x["ticker"] for x in rows] for iso, rows in w_books.items()}

    add(score_list_days(
        "green_book_prior", "stock book",
        prior_lists_on_sessions(green_files, session_days),
        hist, fz, panel_index, spy,
        note="prior-day green.json live_buy (PIT). Afternoon stamp is not a same-day open feature.",
    ))
    pile_map = prior_lists_on_sessions(pile_files, session_days)
    # days with empty pile fall back to full green live_buy
    green_prior = prior_lists_on_sessions(green_files, session_days)
    for iso, names in list(pile_map.items()):
        if not names and iso in green_prior:
            pile_map[iso] = green_prior[iso]
    add(score_list_days(
        "green_pile_prior", "stock book", pile_map,
        hist, fz, panel_index, spy,
        note="prior-day green live_buy with in_pile=true (PIT).",
    ))
    add(score_list_days(
        "weighted_book_1d_prior", "stock book",
        prior_lists_on_sessions(w_files, session_days),
        hist, fz, panel_index, spy,
        note="prior-day stock_book 1d buy (weighted). Same clock as book_1d_prior universe.",
    ))
    add(score_list_days(
        "unweighted_book_prior", "stock book",
        prior_lists_on_sessions(un_files, session_days),
        hist, fz, panel_index, spy,
        note="prior-day unweighted.json live_buy (PIT).",
    ))

    # --- .io paper roundtrips ---
    paper_by = defaultdict(list)
    if os.path.isfile(PAPER_RT):
        with open(PAPER_RT, encoding="utf-8", errors="replace") as fh:
            for rec in csv.DictReader(fh):
                iso = _dt_date(rec.get("buy_date"))
                t = (rec.get("ticker") or "").strip().upper()
                sl = (rec.get("sleeve") or "").strip()
                if not iso or not t or not sl:
                    continue
                paper_by[sl].append({
                    "date": iso, "ticker": t,
                    "sleeve_net": paper_sleeve_net(rec),
                })
    for sl, raw in sorted(paper_by.items()):
        rows = attach_ticket_list(raw, hist, fz, panel_index)
        add(score_ticket_rows(
            sl, ".io paper", rows, spy,
            note=".io paper buy date; fill may be close (not 09:30).",
            clock_ok=False,
        ))

    # --- sleeve combine on-disk fills + catalog splits ---
    comb_raw = []
    if os.path.isfile(COMBINE):
        with open(COMBINE, encoding="utf-8", errors="replace") as fh:
            for rec in csv.DictReader(fh):
                iso = _dt_date(rec.get("entry_dt"))
                t = (rec.get("ticker") or "").strip().upper()
                if not iso or not t:
                    continue
                try:
                    ret = float(rec["ret_pct"]) / 100.0 if rec.get("ret_pct") not in (None, "") else None
                except ValueError:
                    ret = None
                comb_raw.append({
                    "date": iso, "ticker": t, "sleeve_net": ret,
                    "hold": rec.get("hold"), "source": rec.get("source"),
                    "entry_clock": rec.get("entry_dt"),
                })
    comb_rows = attach_ticket_list(comb_raw, hist, fz, panel_index)
    add(score_ticket_rows(
        "sleeve_combine_bt", "sleeve combine", comb_rows, spy,
        note="on-disk bt_trades.csv (io often 16:00). Other catalog combines share these fills.",
        clock_ok=False,
    ))
    for src, label in (("io", "sleeve_combine_io"), ("mover", "sleeve_combine_mover")):
        raw = [r for r in comb_raw if (r.get("source") or "").lower() == src]
        rows = attach_ticket_list(raw, hist, fz, panel_index)
        add(score_ticket_rows(
            label, "sleeve combine", rows, spy,
            note=f"bt_trades source={src}",
            clock_ok=src == "mover",
        ))

    # --- book paper / mover paper ---
    def _load_ret_csv(path, family, name, clock_ok, note, date_key="entry_dt"):
        raw = []
        if not os.path.isfile(path):
            add(_empty_sleeve(name, family, f"missing {path}"))
            return
        with open(path, encoding="utf-8", errors="replace") as fh:
            for rec in csv.DictReader(fh):
                iso = _dt_date(rec.get(date_key) or rec.get("entry_date"))
                t = (rec.get("ticker") or "").strip().upper()
                if not iso or not t:
                    continue
                if (rec.get("side") or "BUY").upper() not in ("BUY", ""):
                    continue
                try:
                    ret = float(rec["ret_pct"]) / 100.0 if rec.get("ret_pct") not in (None, "") else None
                except ValueError:
                    ret = None
                raw.append({"date": iso, "ticker": t, "sleeve_net": ret,
                            "entry_clock": rec.get(date_key)})
        rows = attach_ticket_list(raw, hist, fz, panel_index)
        add(score_ticket_rows(name, family, rows, spy, note=note, clock_ok=clock_ok))

    _load_ret_csv(BOOK_PAPER, "book paper", "book_paper_1w", False,
                  "book paper: catalog says close entry / 1w hold")
    _load_ret_csv(MOVER_PAPER, "mover stitch", "mover_paper_live", True,
                  "mover paper 09:30 fills")

    # --- remaining catalog rows without fills ---
    paper_alias = {f"io_{k}": k for k in paper_by}
    for row in catalog:
        cid, nm, fam = row.get("id") or "", row.get("name") or "", row.get("family") or ""
        if nm in seen or cid in seen:
            continue
        if fam == "excel":
            add(_empty_sleeve(nm, fam, "excel / strategies/ frozen — not scored"))
            continue
        if fam == "sleeve merge" and nm != "flatten_robust":
            add(_empty_sleeve(
                nm, fam,
                "sleeve_merge/trades.csv is flatten_robust only; sweep variants have no per-trade rows",
            ))
            continue
        if fam == "sleeve combine":
            matched = [r for r in comb_raw if combine_match(nm, r)]
            if not matched:
                add(_empty_sleeve(nm, fam, "no matching rows in bt_trades.csv"))
                continue
            rows = attach_ticket_list(matched, hist, fz, panel_index)
            add(score_ticket_rows(
                nm, fam, rows, spy,
                note="filtered from shared bt_trades.csv",
                clock_ok=False,
            ))
            continue
        if cid in paper_alias or nm in paper_by:
            continue
        if fam == ".io paper" and nm in ("SPY (benchmark)", ".io SPY (benchmark)"):
            add(_empty_sleeve(nm, fam, "benchmark, not a name-picking sleeve"))
            continue
        if fam in ("mover stitch", "mover") and nm != "mover_paper_live":
            add(_empty_sleeve(nm, fam, "no separate per-trade dump (see mover_paper_live)"))
            continue
        add(_empty_sleeve(nm, fam, "no parseable morning fills on disk"))

    featured = [s for s in sleeves if s["name"] in FEATURED]
    counts = defaultdict(int)
    for s in sleeves:
        counts[s["verdict"]] += 1
    plain = _plain_sleeves(sleeves, featured, counts)
    payload = {
        "generated": __import__("datetime").date.today().isoformat(),
        "j_formula": "J = (today Open − prior weekday Open) / prior Open",
        "leak": "PASS (unchanged gate — open J only)",
        "live_untouched": "flatten_robust",
        "question": "Does avoid_J_ge0 / elev_cap2_J_le-1 help after fees vs that sleeve alone?",
        "primary_label": "same-day H − 15 bp vs the sleeve's own morning picks",
        "min_n": MIN_N,
        "plain": plain,
        "counts": dict(counts),
        "featured": [s["name"] for s in featured],
        "sleeves": sleeves,
        "n_sleeves": len(sleeves),
    }
    return payload


def _plain_sleeves(sleeves, featured, counts):
    bits = [
        f"Dashboard sleeves (open J only): {counts.get('KEEP', 0)} KEEP / "
        f"{counts.get('KILL', 0)} KILL / {counts.get('CONDITIONAL', 0)} CONDITIONAL / "
        f"{counts.get('null', 0)} null of {len(sleeves)} scored rows. "
        "Question is after-fee H vs that sleeve alone. Live flatten_robust stays frozen."
    ]
    by = {s["name"]: s for s in sleeves}
    for name in FEATURED:
        s = by.get(name)
        if not s:
            continue
        bits.append(line_for_sleeve(s))
    return " ".join(bits)


def hold_slice(s):
    """Prove if avoid n is usable; else pooled (cash books buy little later)."""
    sl = s.get("slices") or {}
    prove = sl.get("prove") or {}
    a = (prove.get("avoid_J_ge0") or {})
    e = (prove.get("elev_cap2_J_le-1") or {})
    if max(a.get("n") or 0, e.get("n") or 0) >= MIN_PROVE_N:
        return "prove", prove
    pooled = sl.get("pooled") or prove
    return "pooled", pooled


def line_for_sleeve(s):
    """One plain-English line: NAME: VERDICT — avoid …; elev …."""
    win, hold = hold_slice(s)
    a = hold.get("avoid_J_ge0") or {}
    e = hold.get("elev_cap2_J_le-1") or {}
    aw, ew = a.get("winrate") or {}, e.get("winrate") or {}
    return (
        f"{s['name']}: **{s['verdict']}** — {win} avoid "
        f"{_pp(a.get('vs_fullscan_pp')) or '—'} n={a.get('n', 0)} "
        f"fire {wr_s(aw)}; "
        f"elev {_pp(e.get('vs_fullscan_pp')) or '—'} n={e.get('n', 0)} "
        f"fire {wr_s(ew)}. "
        f"{s.get('note') or ''}"
    ).strip()


def slim_wr(wr):
    if not wr:
        return None
    return {
        "n_fires": wr.get("n_fires"), "n_wins": wr.get("n_wins"),
        "n_ties": wr.get("n_ties"), "n_losses": wr.get("n_losses"),
        "win_rate": wr.get("win_rate"), "verdict": wr.get("verdict"),
        "clears_55": wr.get("clears_55"), "prints_55": wr.get("prints_55"),
        "hit_h": wr.get("hit_h"), "hit_i": wr.get("hit_i"),
    }


def slim_sleeve(s):
    def slim_rec(r):
        if not r:
            return r
        g = r.get("ghost") or {}
        return {
            "name": r.get("name"), "n": r.get("n"),
            "holdout_mean": r.get("holdout_mean"),
            "vs_fullscan_pp": r.get("vs_fullscan_pp"),
            "sleeve_mean": r.get("sleeve_mean"),
            "vs_sleeve_native_pp": r.get("vs_sleeve_native_pp"),
            "ghost": _ghost_s(g),
            "verdict": r.get("verdict"),
            "family_bar": r.get("family_bar"),
            "winrate": slim_wr(r.get("winrate")),
        }

    slices = {}
    for sname, sl in (s.get("slices") or {}).items():
        slices[sname] = {
            "lo": sl.get("lo"), "hi": sl.get("hi"),
            "baseline": slim_rec(sl.get("baseline")),
            "avoid_J_ge0": slim_rec(sl.get("avoid_J_ge0")),
            "elev_cap2_J_le-1": slim_rec(sl.get("elev_cap2_J_le-1")),
        }
    return {
        "name": s["name"], "family": s["family"], "kind": s.get("kind"),
        "verdict": s["verdict"], "n_all": s.get("n_all"),
        "n_dates": s.get("n_dates"), "clock_ok": s.get("clock_ok"),
        "sleeve_disagree": s.get("sleeve_disagree"),
        "note": s.get("note"), "line": line_for_sleeve(s),
        "slices": slices,
    }


def compact(payload):
    out = {k: v for k, v in payload.items() if k != "sleeves"}
    out["sleeves"] = [slim_sleeve(s) for s in payload.get("sleeves") or []]
    return out


def render_sleeve_tables(sleeves, featured_only=False):
    rows = sleeves
    if featured_only:
        want = set(FEATURED)
        rows = [s for s in sleeves if s["name"] in want]
    L = [
        "| sleeve | family | verdict | n | avoid vs | avoid fire>55 | elev vs | elev fire>55 | H+ | I+ |",
        "|---|---|---|---:|---:|---|---:|---|---|---|",
    ]
    for s in rows:
        _win, hold = hold_slice(s)
        a = hold.get("avoid_J_ge0") or {}
        e = hold.get("elev_cap2_J_le-1") or {}
        aw, ew = a.get("winrate") or {}, e.get("winrate") or {}
        L.append(
            f"| `{s['name']}` | {s.get('family','')} | **{s['verdict']}** | "
            f"{a.get('n', s.get('n_all') or 0)} | "
            f"{_pp(a.get('vs_fullscan_pp')) or '—'} | "
            f"{wr_s(aw)} {aw.get('verdict','')} | "
            f"{_pp(e.get('vs_fullscan_pp')) or '—'} | "
            f"{wr_s(ew)} {ew.get('verdict','')} | "
            f"{hit_s(aw.get('hit_h'))} | {hit_s(aw.get('hit_i'))} |"
        )
    return L


NAMED_CIRCUMSTANCES = FEATURED + (
    "1d_top", "1d_size", "3d_top", "3d_size",
    "union_h1", "union_e_fresh_h1", "union_e_green_h3",
    "union_coil_green_h1", "union_w_hot_candle_h1",
    "union_hot_n12_h1", "book_paper_1w", "mover_paper_live",
    "sleeve_combine_bt",
)


def _wr_row(label, recipe, window):
    wr = (recipe or {}).get("winrate") or {}
    return (
        f"| {label} | {window} | `{recipe.get('name','')}` | "
        f"**{wr.get('verdict', '—')}** | {wr_s(wr)} | "
        f"{hit_s(wr.get('hit_h'))} | {hit_s(wr.get('hit_i'))} | "
        f"{_pp(recipe.get('vs_fullscan_pp')) or '—'} |"
    )


def render_winrate_md(payload, sleeves):
    """Cyrus bar: >55% of fire days the rule book beats the same-day no-rule book."""
    L = [
        "### Win-rate bar (Cyrus: >55% of fires, n≥30)",
        "",
        "**Fire** = a morning the rule changes the book (ticker set ≠ no-rule set). "
        "**Win** = that day’s rule-book mean after-fee H beats the same-day no-rule book. "
        "Ties do not beat. **CLEAR** needs win-rate **>55% and ≥30 fires** on the "
        "prove/pooled window used for the call. n=8 is **not** proven. "
        "A print that is >55% but n_fires<30 is **PROVISIONAL** (demoted) — "
        "including the prior weighted-book avoid 6/8 and green-pile elev 5/8. "
        f"{FEE_CAVEAT} "
        "Separately: % of the rule’s name-days with after-fee H>0 and I>0. "
        "Native Finviz/join/stock_book dumps are ~16 weekdays and cannot reach "
        "30 fires alone; longer Yahoo OHLC day-books (2024-03→2026-08-21) are "
        "the material-n tape. Live = wired into the cash/paper bot — docs are "
        "not live. Live stays frozen.",
        "",
        "| circumstance | window | recipe | fire bar | fire win-rate | H+ after fees | I+ after fees | mean vs |",
        "|---|---|---|---|---|---|---|---|",
    ]
    clears = []
    prints = []
    windows = payload.get("windows") or {}
    for wname in ("prove", "discovery", "pooled_sessions"):
        w = windows.get(wname)
        if not w:
            continue
        for rec_name in ("avoid_J_ge0", "elev_cap2_J_le-1"):
            rec = w.get(rec_name) or {}
            wr = rec.get("winrate") or {}
            label = "join top-8"
            L.append(_wr_row(label, rec, wname))
            if wr.get("clears_55"):
                clears.append(f"{label} {wname} `{rec_name}` {wr_s(wr)}")
            elif wr.get("prints_55"):
                prints.append(f"{label} {wname} `{rec_name}` {wr_s(wr)}")
    # Long Yahoo tape first — this is the only path that can hit ≥30 fires.
    long_windows = ("long", "y2025", "pre813")
    for label, wname, rec_name, rec in iter_long_fire_rows(payload.get("long_fires")):
        if wname not in long_windows:
            continue
        wr = rec.get("winrate") or rec
        L.append(_wr_row(label, rec, wname))
        if wr.get("clears_55"):
            clears.append(f"{label} {wname} `{rec_name}` {wr_s(wr)}")
        elif wr.get("prints_55"):
            prints.append(f"{label} {wname} `{rec_name}` {wr_s(wr)}")
    want = set(NAMED_CIRCUMSTANCES)
    extra = []
    for s in sleeves or []:
        if s.get("name") in want:
            extra.append(s)
            continue
        if s.get("verdict") != "CONDITIONAL":
            continue
        sl = (s.get("slices") or {}).get("pooled") or {}
        a = sl.get("avoid_J_ge0") or {}
        if (a.get("n") or 0) >= MIN_N and (a.get("vs_fullscan_pp") or 0) >= BEAT_PP:
            extra.append(s)
    seen = set()
    for s in extra:
        if s["name"] in seen:
            continue
        seen.add(s["name"])
        win, hold = hold_slice(s)
        for rec_name in ("avoid_J_ge0", "elev_cap2_J_le-1"):
            rec = hold.get(rec_name) or {}
            wr = rec.get("winrate") or {}
            rec = dict(rec)
            rec.setdefault("name", rec_name)
            L.append(_wr_row(s["name"], rec, win))
            if wr.get("clears_55"):
                clears.append(f"{s['name']} {win} `{rec_name}` {wr_s(wr)}")
            elif wr.get("prints_55"):
                prints.append(f"{s['name']} {win} `{rec_name}` {wr_s(wr)}")
    L += [
        "",
        f"**Clears >55% with ≥{MIN_FIRES} fires:** "
        + ("; ".join(clears) if clears else "none."),
        "",
        f"**Provisional >55% but n_fires<{MIN_FIRES} (demoted, not a call):** "
        + ("; ".join(prints) if prints else "none."),
        "",
        "Native weighted-book avoid (6/8) and green-pile elev (5/8) stay "
        "**PROVISIONAL** — Finviz/join/stock_book dumps are ~16 weekdays. "
        "Long analogs: `ohlc_liq vol_top8` ≈ weighted book; "
        "`ohlc_liq prior_green_top8` ≈ green pile. "
        "y2025 does **not** confirm those liquid ranked recipes (47–54%). "
        "Unranked `ohlc_all` / `mem_20260426` avoid CLEARs are the "
        "already-demoted microcap lottery (H+ ~41%, ghost FAIL on mean edge). "
        f"{FEE_CAVEAT} Do not wire.",
        "",
    ]
    return L, clears, prints


def write_keep_cards(payload, sl):
    """Refresh JOIN_J_KEEP_CARDS.md (universes + sleeves). Live frozen."""
    tip = payload.get("tip_sha") or "local"
    uni = payload.get("universes") or {}
    sleeves = sl.get("sleeves") or payload.get("sleeves") or []
    leak = payload.get("leak") or {}
    L = [
        "# Research cards — Excel J × fullscan join (post-8-13)",
        "",
        f"_Generated {payload.get('generated')} · tip `{tip}` / "
        "`JOIN_POST_813.md` · **research cards only** · live frozen._",
        "",
        "## Plain English",
        "",
        payload.get("plain") or "",
        "",
        "Clock: **J value is open** (today Open vs prior weekday session Open). "
        "Never same-row H/I or H-fill paint. Live `flatten_robust` is not changed.",
        "",
        "## J clock / leak",
        "",
        f"**Verdict: {leak.get('verdict', 'PASS')}.** The J number used by both "
        "recipes is open-knowable at that day’s 9:30 open.",
        "",
        "| check | result |",
        "|---|---|",
        "| Excel map | `CLOCK_MAP.md`: J value-open = C[t] vs C[t−1]. C = Open / IT. In `value_mine_open` (44). |",
        "| Dump formula | `J = (Finviz Open[t] − Finviz Open[prior weekday]) / prior Open` |",
        "| Same-row H/I | labels only — never features. J ≠ H and J ≠ I on checked name-days. |",
        "| M number / H paint / `core_score` | not used |",
        "| High / Low / Close / Price | loaded as labels / reconstruction; **not** in J |",
        "| File clock | Finviz CSVs are EOD; Open column is still the 09:30 print |",
        "| Weekend / Sunday | Sat/Sun Finviz Opens skipped; Sunday join dumps held out |",
        "| Stale | 08-13 vs 04-26 Open unused. 08-26 missing Finviz → 08-27 J uses 08-25 Open (hole, not future). |",
        "",
        "pick_book reads only J flags + join rank. See `JOIN_POST_813.md` leak section.",
        "",
        "## Win-rate bar (Cyrus: >55% of fires, n≥30)",
        "",
    ]
    wr_md, _, _ = render_winrate_md(payload, sleeves)
    L += wr_md[2:]  # skip the ### heading, we already have ##
    L += [
        "",
        "## Universes (same open J, beat same-universe baseline)",
        "",
        "| universe | n (holdout) | avoid vs | ghost | family |",
        "|---|---:|---:|---|---|",
    ]
    order = [
        "join_top8", "join_top15", "join_top80", "join_full", "membership",
        "membership_liq", "book_1d_prior", "book_3d_prior", "ohlc_all",
        "ohlc_liq", "mem_20260426",
    ]
    seen = set()
    for uname in order + [k for k in uni if k not in order]:
        u = uni.get(uname)
        if not u or uname in seen:
            continue
        seen.add(uname)
        sls = u.get("slices") or {}
        hold = sls.get("prove") or sls.get("pre813") or sls.get("long") or {}
        a = hold.get("avoid_J_ge0") or {}
        L.append(
            f"| `{uname}` | {a.get('n', '—')} | {_pp(a.get('vs_fullscan_pp')) or '—'} | "
            f"{a.get('ghost') if isinstance(a.get('ghost'), str) else _ghost_s(a.get('ghost'))} | "
            f"**{u.get('verdict')}** |"
        )
    L += [
        "",
        "Yahoo OHLC cannot invent join ranks before 2026-08-12. Prices parquet "
        "ends 2026-08-21. No earlier join+J holdout exists. Universe family "
        "stays as scored — this sleeve add-on does not drop that work.",
        "",
        "## Dashboard sleeves (J vs sleeve-alone)",
        "",
        payload.get("sleeve_plain") or sl.get("plain") or "",
        "",
        "KEEP = prove window ≥20 bp after fees vs that sleeve’s own morning "
        "picks, ghost pass, H>0. CONDITIONAL = edge only in pooled/discovery, "
        "ghost fail, or clock/sleeve-P&L disagreement. KILL = no 20 bp edge. "
        "null = n<15, shorts, excel/`strategies/` frozen, or no fills.",
        "",
        "### Featured",
        "",
    ]
    L += render_sleeve_tables(sleeves, featured_only=True)
    L += [
        "",
        "### Plain English per featured sleeve",
        "",
    ]
    by = {s["name"]: s for s in sleeves}
    for name in FEATURED:
        s = by.get(name)
        if s:
            L.append(f"- {s.get('line') or line_for_sleeve(s)}")
    L += [
        "",
        "### All STRATEGY_BOARD / dashboard sleeves",
        "",
    ]
    L += render_sleeve_tables(sleeves, featured_only=False)
    L += [
        "",
        "## Case studies (prove window)",
        "",
        "### Avoid — 2026-08-27 FIGR → EMBJ (`avoid_J_ge0`)",
        "",
        "Join top-8 that morning: MNDY, RELY, **FIGR**, ECO, NVDA, SKHY, HPE, CRDO.",
        "",
        "| | FIGR (dropped) | EMBJ (refilled) |",
        "|---|---|---|",
        "| join rank | 3 | 9 |",
        "| J at open | **+3.82%** (Open 40.50 vs 2026-08-25 Open 39.01) | **−2.09%** (Open 75.78 vs 08-25 Open 77.40) |",
        "| without J | stays in the eight | stays out (rank 9) |",
        "| with J | dropped (J≥0) | refilled (J<0) |",
        "| H after 15 bp | **−8.59%** (raw −8.44%) | **−0.82%** (raw −0.67%) |",
        "| I after 15 bp | **−10.02%** (raw −9.87%) | **+0.71%** (raw +0.86%) |",
        "",
        "08-26 has join but no Finviz, so this J is vs Tuesday Open, not Wednesday. Still prior, not a close peek.",
        "",
        "### Elevate — 2026-09-04 HRMY → AVAH (`elev_cap2_J_le-1`)",
        "",
        "Join top-8 that morning: **HRMY**, HALO, CDNA, WAY, PLMR, ONC, KKR, NU.",
        "",
        "| | HRMY (dropped) | AVAH (elevated) |",
        "|---|---|---|",
        "| join rank | 1 | 13 |",
        "| J at open | **+3.92%** (Open 42.93 vs 2026-09-03 Open 41.31) | **−2.29%** (Open 13.22 vs 09-03 Open 13.53) |",
        "| without J | stays #1 in the eight | stays out (rank 13) |",
        "| with J | swapped out (J≥0, one of ≤2) | swapped in (J≤−1% from ranks 9–80) |",
        "| H after 15 bp | **−2.64%** (raw −2.49%) | **+3.03%** (raw +3.18%) |",
        "| I after 15 bp | **−2.48%** (raw −2.33%) | **+3.18%** (raw +3.33%) |",
        "",
        "These two name-days illustrate the rule. They are not a new holdout. Family stays **CONDITIONAL**.",
        "",
        "## Card 1 — avoid J≥0",
        "",
        "| field | value |",
        "|---|---|",
        "| rule | drop morning picks with J≥0; refill from J<0 when the book is ranked |",
        "| entry | open |",
        "| label | same-day H after fees |",
        "| code | `avoid_J_ge0` |",
        "| join top-8 prove | CONDITIONAL (under 20 bp + ghost fail) |",
        "| broader universes | DEMOTE |",
        "| dashboard sleeves | see table — no live wire |",
        "| status | research card · not live · not KEEP holds |",
        "",
        "## Card 2 — elevate J≤−1% cap 2",
        "",
        "| field | value |",
        "|---|---|",
        "| rule | swap ≤2 J≥0 names for J≤−1% from ranks n+1–80 (list) or drop ≤2 J≥0 / day (tickets) |",
        "| entry | open |",
        "| label | same-day H after fees |",
        "| code | `elev_cap2_J_le-1` |",
        "| join top-8 prove | CONDITIONAL |",
        "| broader universes | DEMOTE |",
        "| dashboard sleeves | see table — no live wire |",
        "| status | research card · not live · not KEEP holds |",
        "",
        "## Explicitly not carded / caveats",
        "",
        "| item | note |",
        "|---|---|",
        "| Sunday join dumps 08-30 / 09-06 | not a 1d session; held out of prove |",
        "| 08-13 J | stale (prior Open 04-26); not used |",
        "| flatten_robust overlay | blanket KEEP recipes hurt sleeve P&L; io-only fair test is not a live change |",
        "| excel / `strategies/` | frozen — not scored |",
        "| shorts | J overlay is long-only; null |",
        "| sleeve_merge sweep variants | no per-trade rows; null |",
        "",
        "## Source",
        "",
        f"`JOIN_POST_813.md` tip `{tip}` · PR #153. Gate: "
        "`OPEN_SAME_ROW_LABELS.md` / `CLOCK_MAP.md`. Research only. Live frozen.",
        "",
    ]
    cards = os.path.join(os.path.dirname(HERE), "research", "JOIN_J_KEEP_CARDS.md")
    open(cards, "w", encoding="utf-8").write("\n".join(L))
    return cards


if __name__ == "__main__":
    from join_post_813 import build_panel, history_index, load_asof, load_finviz, load_join_days
    print("loading dumps …", flush=True)
    fz = load_finviz()
    hist = history_index(fz)
    joins = load_join_days()
    books = load_book_1d()
    asof = load_asof()
    spy = {}
    panel, flags = build_panel(joins, fz, hist, books, asof, spy,
                               start=HOLD_CUT, sessions_only=True)
    p = run(panel=panel, flags_by_day=flags, spy=spy, fz=fz, hist=hist)
    c = compact(p)
    os.makedirs(os.path.dirname(SLEEVE_JSON), exist_ok=True)
    json.dump(c, open(SLEEVE_JSON, "w"), indent=2, default=str)
    print("wrote", SLEEVE_JSON, flush=True)
    print(p["plain"], flush=True)
    for s in c["sleeves"]:
        if s["name"] in FEATURED or s["verdict"] in ("KEEP", "CONDITIONAL"):
            print(f"  {s['verdict']:12} {s['name']}", flush=True)
