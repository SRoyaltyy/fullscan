"""Statistical mine over the WHOLE Excel emulator sheet A..JL.

Every letter is a feature source. Same-row close atoms enter as lag-1.
Runtime is controlled by statistics, not by dropping columns:
  1. binary descriptors per letter (fill lag, value bins, 10-row counts, text)
  2. univariate 2x2 vs I>0: support, lift, chi-square, mutual info
  3. Benjamini-Hochberg FDR on discovery p-values
  4. holdout must keep lift > 1
  5. Apriori pairs among FDR survivors
"""
from __future__ import annotations

import argparse
import json
import math
import os
import sys
from collections import defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from excel_clock_gate import FILL_OPEN, VALUE_OPEN_44, assert_excel_clock_gate, gate_payload  # noqa: E402
from excel_deep_corr_mine import (  # noqa: E402
    AVGVOL_MIN, MCAP_MIN_M, _f, _fwd, find_grids, load_finviz_filter, load_grids,
)
from signals import classify_fill  # noqa: E402

FDR_Q = 0.10
MIN_SUPPORT = 0.02
MIN_DISC_N = 200
MIN_HOLD_N = 80
PAIR_MAX_SEEDS = 80


def col_letters(end="JL"):
    out = []
    s = "A"
    n = 0
    while _col_idx(s) <= _col_idx(end):
        out.append(s)
        s = _next_col(s)
        n += 1
        if n > 400:
            break
    return out


def _col_idx(s):
    n = 0
    for ch in s:
        n = n * 26 + (ord(ch.upper()) - 64)
    return n


def _next_col(s):
    chars = list(s)
    i = len(chars) - 1
    while i >= 0:
        if chars[i] != "Z":
            chars[i] = chr(ord(chars[i]) + 1)
            return "".join(chars)
        chars[i] = "A"
        i -= 1
    return "A" + "".join(chars)


ALL_LETTERS = tuple(col_letters("JL"))


def _fam(hexv):
    try:
        fam, _ = classify_fill(str(hexv).lstrip("#"))
        return fam
    except Exception:
        return "none"


def _fills_map(day):
    raw = day.get("fills")
    out = {}
    if isinstance(raw, dict):
        for k, v in raw.items():
            out[str(k).upper()] = v
        return out
    if isinstance(raw, list):
        letters = list("ABCDEFGHIJKLMNO")
        for i, v in enumerate(raw[:15]):
            out[letters[i]] = v
    return out


def _vals_map(day):
    raw = day.get("values") or day.get("cols") or {}
    if not isinstance(raw, dict):
        return {}
    return {str(k).upper(): v for k, v in raw.items()}


def _chi2_p(a, b, c, d):
    n = a + b + c + d
    if n <= 0:
        return 0.0, 1.0
    r1, r2 = a + b, c + d
    c1, c2 = a + c, b + d
    if r1 * r2 * c1 * c2 == 0:
        return 0.0, 1.0
    chi = 0.0
    for obs, ex in ((a, r1 * c1 / n), (b, r1 * c2 / n), (c, r2 * c1 / n), (d, r2 * c2 / n)):
        if ex <= 0:
            continue
        chi += (obs - ex) ** 2 / ex
    p = math.erfc(math.sqrt(max(chi, 0.0) / 2.0))
    return chi, max(min(p, 1.0), 0.0)


def _mi(a, b, c, d):
    n = a + b + c + d
    if n <= 0:
        return 0.0
    mi = 0.0
    for x, rx, cx in ((a, a + b, a + c), (b, a + b, b + d), (c, c + d, a + c), (d, c + d, b + d)):
        if x <= 0 or rx <= 0 or cx <= 0:
            continue
        mi += (x / n) * math.log((x * n) / (rx * cx) + 1e-15)
    return mi


def bh_keep(rows, q=FDR_Q):
    scored = [(i, r["p"]) for i, r in enumerate(rows) if r.get("p") is not None]
    m = len(scored)
    if m == 0:
        return set()
    scored.sort(key=lambda t: t[1])
    cutoff = -1
    for rank, (i, p) in enumerate(scored, 1):
        if p <= q * rank / m:
            cutoff = rank
    return {rows[scored[k][0]]["rule"] for k in range(cutoff)}
