"""Leak-free clocks for the Excel-replica A–O fill grid.

Timing verdict (engine/timing_test.py, NOTES.md, rulebook §5):

  Open-knowable fills : A B C G J K L M O
  Close-knowable fills: D E F H I N

core_score = A..J includes close-knowable D,E,F,H,I → close clock only.
row_score  = A..O includes the same → close clock only.
A-keyed defs may enter at the confirmation OPEN (the only open-entry
the rulebook allows). Any feature that reads a close-knowable fill
must enter at confirmation CLOSE.

Formula-value caveat (not the same as fill clocks):
  G = F_t / F_{t-1}     (same-day volume)  → value is close-knowable
  K = (D-C)/C           (same-day high)    → value is close-knowable
  M = -(E-C)/C          (same-day low)     → value is close-knowable
Fill mining still follows the perturbation test. Value-gate mining
clocks those three as close.
"""
from __future__ import annotations

VISIBLE = "ABCDEFGHIJKLMNO"
OPEN_LETTERS = "ABCGJKLMO"
CLOSE_LETTERS = "DEFHIN"
OPEN_IDX = tuple(VISIBLE.index(c) for c in OPEN_LETTERS)
CLOSE_IDX = tuple(VISIBLE.index(c) for c in CLOSE_LETTERS)
# A,B,C,G,J — open-knowable slice of the old core_score (A..J)
OPEN_CORE_IDX = tuple(VISIBLE.index(c) for c in "ABCGJ")
SLEEVE_HOLDS = (1, 2, 3, 5, 8)

# Cost models — every result row must label which one was used.
COST_MCAP_HI = 0.001   # round-trip, mcap >= $300M
COST_MCAP_LO = 0.003   # round-trip, mcap <  $300M
COST_FUTU_LONG = 0.0015   # ~$14 RT on a $10k Futu order + 5 bps/side
COST_FUTU_SHORT = 0.0020  # + borrow cushion

# Ship bar (research keep). Fail any one → not a keeper.
SHIP = {
    "disc_n": 300,
    "hold_n": 100,
    "disc_t": 3.0,
    "hold_t": 2.0,
    "n_tickers": 50,
    "n_dates": 20,
    "max_trade_frac": 0.25,   # one trade cannot be >25% of gross wins
    "min_tape_n": 40,         # each tape half
}


def col_idx(letter: str) -> int:
    return VISIBLE.index(letter.upper())


def feature_clock(feature_cols) -> str:
    """close if any close-knowable fill is read; else open."""
    for i in feature_cols:
        if i in CLOSE_IDX:
            return "close"
    return "open"


def assert_clock_legal(feature_cols, clock: str) -> None:
    inferred = feature_clock(feature_cols)
    if clock == "open" and inferred != "open":
        raise ValueError(
            f"open entry illegal: feature_cols={list(feature_cols)} "
            f"read close-knowable fills"
        )


def mcap_cost(mcap_millions) -> float:
    if mcap_millions is not None and mcap_millions < 300:
        return COST_MCAP_LO
    return COST_MCAP_HI


def futu_cost(side: int) -> float:
    return COST_FUTU_LONG if side == 1 else COST_FUTU_SHORT


def annotate_days(days):
    """Add fill families/scores + leak-free value states. Mutates copies."""
    from signals import classify_fill

    out = []
    prev_c = prev_o = prev_v = None
    for d in days:
        fams, scores = [], []
        for fill in d.get("fills") or []:
            fam, sc = classify_fill(fill)
            fams.append(fam)
            scores.append(sc)
        n = len(scores)
        o, c = d.get("open"), d.get("close")
        gap = ((o - prev_c) / prev_c) if (o and prev_c) else None
        j_ret = ((o - prev_o) / prev_o) if (o and prev_o) else None
        h_ret = ((c - o) / o) if (o and c) else None
        i_ret = ((c - prev_c) / prev_c) if (c and prev_c) else None
        rec = {
            **d,
            "fams": fams,
            "scores": scores,
            "a_score": scores[0] if n else 0.0,
            "row_score": sum(scores),
            "core_score": sum(scores[:10]),
            "open_score": sum(scores[i] for i in OPEN_IDX if i < n),
            "open_core": sum(scores[i] for i in OPEN_CORE_IDX if i < n),
            "close_score": sum(scores[i] for i in CLOSE_IDX if i < n),
            "gap": gap,          # open-knowable value
            "j_ret": j_ret,      # column J value; open-knowable
            "h_ret": h_ret,      # column H value; close-knowable
            "i_ret": i_ret,      # column I value; close-knowable
        }
        out.append(rec)
        prev_c, prev_o, prev_v = c, o, d.get("volume")
    return out


def entry_px(day, clock: str):
    if clock == "open":
        return day.get("open")
    return day.get("close")


def hold_exit_idx(entry_idx: int, hold_n: int, clock: str, last: int) -> int:
    """Sleeve-native hold: N sessions on the book.

    open clock : enter 09:30 of day ei, exit close of ei+(N-1)
                 hold1 = same-day close
    close clock: enter close of ei, exit close of ei+N
                 hold1 = next session close (current mine.py)
    """
    if clock == "open":
        return min(entry_idx + hold_n - 1, last)
    return min(entry_idx + hold_n, last)


def simulate_clock(days, cluster, clock: str, exit_rule: str):
    """Signed raw return (no costs). None if unevaluable.

    TP / trail still fill on later highs/lows — those are after entry,
    so they do not leak. Entry price respects `clock`.
    """
    ei = cluster.get("entry_idx")
    xi = cluster.get("exit_idx")
    side = cluster["side"]
    if ei is None or ei >= len(days):
        return None
    entry = entry_px(days[ei], clock)
    if not entry:
        return None
    last = (xi if xi is not None and xi < len(days) else len(days) - 1)
    if last < ei:
        return None
    closes = [d.get("close") for d in days]
    highs = [d.get("high") for d in days]
    lows = [d.get("low") for d in days]

    def raw(px):
        return (px - entry) / entry * side

    if exit_rule == "flip":
        if xi is None or xi >= len(days) or not closes[xi]:
            return None
        return raw(closes[xi])

    if exit_rule.startswith("hold"):
        n = int(exit_rule[4:])
        k = hold_exit_idx(ei, n, clock, last)
        if clock == "close" and k <= ei:
            return None
        if clock == "open" and k < ei:
            return None
        # sleeve hold must actually have the Nth bar (no silent clip)
        need = ei + (n - 1 if clock == "open" else n)
        if need > last or not closes[k]:
            return None
        return raw(closes[k])

    if exit_rule.startswith("tp"):
        x = int(exit_rule[2:]) / 100.0
        tgt = entry * (1 + x * side)
        start = ei if clock == "open" else ei + 1
        for k in range(start, last + 1):
            px = highs[k] if side == 1 else lows[k]
            if px is None:
                continue
            if (side == 1 and px >= tgt) or (side == -1 and px <= tgt):
                return raw(tgt)
        if not closes[last]:
            return None
        return raw(closes[last])

    return None
