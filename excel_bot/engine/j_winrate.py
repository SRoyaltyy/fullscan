"""Day-book fire win-rate for open J overlays. Research only.

Primary (Cyrus): among days the rule *fires* (book ticker set ≠ no-rule
set that morning), % where the rule book's mean after-fee H beats the
same-day no-rule book. >55% is a win. Ties do not beat.

Secondary: % of the rule's name-days with after-fee H>0 (and I>0).

Live = wired into the cash/paper bot. This module does not wire.
"""
from __future__ import annotations

from collections import defaultdict

from join_post_813 import mean

WIN_BAR = 0.55  # strictly greater than 55%
MIN_FIRES = 8   # enough days to call CLEAR; thinner prints still report


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
    clears = bool(wr is not None and wr > WIN_BAR and n >= MIN_FIRES)
    prints = bool(wr is not None and wr > WIN_BAR)
    return {
        "n_fires": n,
        "n_wins": wins,
        "n_ties": ties,
        "n_losses": losses,
        "win_rate": wr,
        "win_rate_ex_tie": wr_ex,
        "clears_55": clears,
        "prints_55": prints,
        "bar": WIN_BAR,
        "min_fires": MIN_FIRES,
        "label": label_key,
        "verdict": (
            "CLEAR" if clears else
            ("PRINT" if prints else ("null" if not n else "FAIL"))
        ),
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
