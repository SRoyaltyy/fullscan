"""Day-book fire win-rate for open J overlays. Research only.

Primary (Cyrus): among days the rule *fires* (book ticker set ≠ no-rule
set that morning), % where the rule book's mean after-fee H beats the
same-day no-rule book. >55% is a win. Ties do not beat.

Secondary: % of the rule's name-days with after-fee H>0 (and I>0).

Live = wired into the cash/paper bot. This module does not wire.
"""
from __future__ import annotations

from collections import defaultdict

from join_post_813 import FEE_RT, mean

WIN_BAR = 0.55  # strictly greater than 55%
MIN_FIRES = 30  # n=8 is not proven; CLEAR needs a material fire count
FEE_CAVEAT = (
    "After-fee H is the equal-weight day-mean minus 15 bp Futubull, "
    "not dollar-weighted."
)


def _verdict(wr, n):
    if not n:
        return "null"
    if wr is not None and wr > WIN_BAR and n >= MIN_FIRES:
        return "CLEAR"
    if wr is not None and wr > WIN_BAR:
        return "PROVISIONAL"  # looked good at tiny n — not a call
    return "FAIL"


def books_by_day(rows):
    by = defaultdict(list)
    for r in rows:
        if r.get("net") is not None and r.get("date") and r.get("ticker"):
            by[r["date"]].append(r)
    return by


def day_mean(rows, key="net"):
    xs = [r.get(key) for r in rows if r.get(key) is not None]
    return mean(xs)


def hit_rate(rows, key="net"):
    xs = [r.get(key) for r in rows if r.get(key) is not None]
    if not xs:
        return {"n": 0, "hit": None, "n_pos": 0}
    n_pos = sum(1 for x in xs if x > 0)
    return {"n": len(xs), "hit": n_pos / len(xs), "n_pos": n_pos}


def fire_winrate(base_rows, rule_rows, label_key="net"):
    """Same-day book vs no-rule book. Fire = ticker set changed."""
    base = books_by_day(base_rows)
    rule = books_by_day(rule_rows)
    wins = ties = losses = 0
    for iso in sorted(set(base) | set(rule)):
        b, r = base.get(iso) or [], rule.get(iso) or []
        bt = {x["ticker"] for x in b}
        rt = {x["ticker"] for x in r}
        if bt == rt:
            continue
        bm, rm = day_mean(b, label_key), day_mean(r, label_key)
        if bm is None or rm is None:
            continue
        if rm > bm:
            wins += 1
        elif rm < bm:
            losses += 1
        else:
            ties += 1
    n = wins + losses + ties
    wr = (wins / n) if n else None
    wr_ex = (wins / (wins + losses)) if (wins + losses) else None
    verdict = _verdict(wr, n)
    return {
        "n_fires": n,
        "n_wins": wins,
        "n_ties": ties,
        "n_losses": losses,
        "win_rate": wr,
        "win_rate_ex_tie": wr_ex,
        "clears_55": verdict == "CLEAR",
        "prints_55": verdict == "PROVISIONAL",
        "bar": WIN_BAR,
        "min_fires": MIN_FIRES,
        "label": label_key,
        "verdict": verdict,
    }


def attach_hits(rows):
    return {
        "h": hit_rate(rows, "net"),
        "i": hit_rate(rows, "i_net"),
    }


def pack_rule_clock(base_rows, rule_rows):
    wr = fire_winrate(base_rows, rule_rows, "net")
    hits = attach_hits(rule_rows)
    wr["hit_h"] = hits["h"]
    wr["hit_i"] = hits["i"]
    return wr


def wr_s(wr):
    if not wr or wr.get("win_rate") is None:
        return f"n_fires={wr.get('n_fires', 0) if wr else 0}"
    return (
        f"{wr['win_rate']*100:.1f}% ({wr['n_wins']}/{wr['n_fires']} fires"
        f"{'' if not wr.get('n_ties') else f', {wr['n_ties']} tie'})"
    )


def hit_s(h):
    if not h or h.get("hit") is None:
        return "—"
    return f"{h['hit']*100:.1f}% n={h['n']}"


def _j_flags(j):
    return {
        "J_ge0": j is not None and j >= 0,
        "J_lt0": j is not None and j < 0,
        "J_le-1": j is not None and j <= -0.01,
    }


def _day_groups(rows, lo, hi):
    by = defaultdict(list)
    for r in rows:
        d = r.get("date")
        if d and lo <= d <= hi and r.get("net") is not None:
            by[d].append(r)
    return by


def _ranked_books(day_rows, n):
    """Open-fair rank: prior-session volume (already on the name-day)."""
    from join_post_813 import pick_book
    ranked = sorted(day_rows, key=lambda r: r.get("prior_vol") or 0, reverse=True)
    fl, recs, by_t = {}, [], {}
    for i, r in enumerate(ranked, 1):
        t = r["ticker"]
        fl[t] = _j_flags(r.get("J"))
        recs.append({"ticker": t, "rank": i})
        by_t[t] = r

    def take(mode, avoid=None, elev=None, special=None):
        if special == "avoid_J_ge0":
            picks = []
            for rec in recs:
                if not fl.get(rec["ticker"], {}).get("J_lt0"):
                    continue
                picks.append(rec)
                if len(picks) >= n:
                    break
        else:
            picks = pick_book(recs, fl, mode, avoid, elev, n=n, cap=2)
        return [by_t[p["ticker"]] for p in picks if p["ticker"] in by_t]

    return (
        take("raw"),
        take("raw", special="avoid_J_ge0"),
        take("elev_cap", "J_ge0", "J_le-1"),
    )


def _unranked_books(day_rows):
    base = list(day_rows)
    avoid = [r for r in day_rows if r.get("J") is not None and r["J"] < 0]
    return base, avoid, None


def _h_pos(r):
    """Raw H>0. net = H − 15 bp, so H>0 iff net > −FEE_RT."""
    return r.get("net") is not None and r["net"] > -FEE_RT


def _prior_map(days):
    isos = sorted(days)
    prev, last = {}, None
    for iso in isos:
        prev[iso] = last
        last = iso
    return prev


def _green_today(today_rows, yest_rows):
    """Names that printed H>0 on the prior session (green-pile analog)."""
    winners = {r["ticker"] for r in yest_rows if _h_pos(r)}
    return [r for r in today_rows if r["ticker"] in winners]


def _books_for_kind(kind, drows, yest_rows):
    if kind == "unranked":
        return _unranked_books(drows)
    if kind == "vol_top8":
        return _ranked_books(drows, 8)
    if kind == "vol_top80":
        return _ranked_books(drows, 80)
    if kind.startswith("prior_green"):
        if not yest_rows:
            return [], [], None
        green = _green_today(drows, yest_rows)
        if kind == "prior_green_unranked":
            return _unranked_books(green)
        if kind == "prior_green_top8":
            return _ranked_books(green, 8)
        if kind == "prior_green_top80":
            return _ranked_books(green, 80)
    raise ValueError(f"unknown long-fire kind {kind}")


def _score_kinds_on_rows(rows, slices, kinds):
    all_days = _day_groups(rows, "0000-01-01", "9999-12-31")
    all_prev = _prior_map(all_days)
    out = {}
    for sname, (lo, hi) in slices.items():
        days = {d: all_days[d] for d in all_days if lo <= d <= hi}
        out[sname] = {"n_days": len(days), "lo": lo, "hi": hi, "kinds": {}}
        for kind in kinds:
            base, av, el = [], [], []
            for iso in sorted(days):
                yest = all_days.get(all_prev.get(iso)) or []
                b, a, e = _books_for_kind(kind, days[iso], yest)
                base.extend(b)
                av.extend(a)
                if e is not None:
                    el.extend(e)
            ranked = kind not in ("unranked", "prior_green_unranked")
            blob = {
                "n_days": len(days),
                "n_base": len(base),
                "avoid_J_ge0": pack_rule_clock(base, av),
            }
            if ranked:
                blob["elev_cap2_J_le-1"] = pack_rule_clock(base, el)
            else:
                blob["elev_cap2_J_le-1"] = {
                    "verdict": "null", "n_fires": 0,
                    "note": "elev_cap2 needs a ranked book; unranked is avoid-only",
                }
            out[sname]["kinds"][kind] = blob
    return out


RANKED_KINDS = (
    "unranked",
    "vol_top8",
    "vol_top80",
    "prior_green_unranked",
    "prior_green_top8",
    "prior_green_top80",
)
# vol_top8 ≈ weighted-book analog (open-fair volume rank).
# prior_green_* ≈ green-pile analog (yesterday H>0, then the standing recipes).


def score_long_universe_fires(universes, slices, kinds_by_universe=None):
    """Day-book fire win-rate on long name-day tapes (Yahoo OHLC).

    universes: {name: [rows with date, ticker, J, net, prior_vol]}.
    unranked: baseline = all names that day; avoid = J<0 only.
    vol_top8 / vol_top80: ranked by prior-session volume (open-fair).
    prior_green_*: yesterday H>0 names, then unranked / vol-rank recipes.
    """
    out = {}
    for uname, rows in universes.items():
        kinds = RANKED_KINDS
        if kinds_by_universe and kinds_by_universe.get(uname):
            kinds = kinds_by_universe[uname]
        out[uname] = _score_kinds_on_rows(rows, slices, kinds)
    return out


def slim_wr(wr):
    if not wr or not isinstance(wr, dict):
        return wr
    return {
        "n_fires": wr.get("n_fires"), "n_wins": wr.get("n_wins"),
        "n_ties": wr.get("n_ties"), "n_losses": wr.get("n_losses"),
        "win_rate": wr.get("win_rate"), "verdict": wr.get("verdict"),
        "clears_55": wr.get("clears_55"), "prints_55": wr.get("prints_55"),
        "hit_h": wr.get("hit_h"), "hit_i": wr.get("hit_i"),
        "min_fires": wr.get("min_fires", MIN_FIRES),
        "bar": wr.get("bar", WIN_BAR),
        "note": wr.get("note"),
    }


def slim_long_fires(long_fires):
    if not long_fires:
        return long_fires
    out = {}
    for uname, slices in long_fires.items():
        out[uname] = {}
        for sname, sl in (slices or {}).items():
            kinds = {}
            for kind, blob in (sl.get("kinds") or {}).items():
                kinds[kind] = {
                    "n_days": blob.get("n_days"),
                    "n_base": blob.get("n_base"),
                    "avoid_J_ge0": slim_wr(blob.get("avoid_J_ge0")),
                    "elev_cap2_J_le-1": slim_wr(blob.get("elev_cap2_J_le-1")),
                }
            out[uname][sname] = {
                "n_days": sl.get("n_days"), "lo": sl.get("lo"),
                "hi": sl.get("hi"), "kinds": kinds,
            }
    return out


def iter_long_fire_rows(long_fires):
    """Yield (label, window, recipe_name, recipe_dict) for the win-rate table."""
    for uname, slices in (long_fires or {}).items():
        for sname, sl in (slices or {}).items():
            for kind, blob in (sl.get("kinds") or {}).items():
                for rec_name in ("avoid_J_ge0", "elev_cap2_J_le-1"):
                    rec = blob.get(rec_name) or {}
                    if rec.get("verdict") == "null" and not rec.get("n_fires"):
                        continue
                    packed = dict(rec)
                    packed["name"] = rec_name
                    packed["winrate"] = rec if "n_fires" in rec else rec
                    yield f"{uname} {kind}", sname, rec_name, packed
