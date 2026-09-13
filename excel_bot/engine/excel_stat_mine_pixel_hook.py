"""Run the statistical mine with A..JO pixel descriptors + crash resume.

python excel_bot/engine/excel_stat_mine_pixel_hook.py --grids grids_deep
"""
from __future__ import annotations

import math
from collections import defaultdict

import excel_stat_mine as m
import excel_stat_mine_ckpt as ckpt
from excel_pixel_desc import ALL_LETTERS
from excel_pixel_shade import features_with_shade


def discovery_quantiles_5(ticker_days, split_map):
    bags = defaultdict(list)
    for tkr, raw in ticker_days.items():
        if split_map.get(tkr) == "holdout":
            continue
        days = m.normalize_days(raw)
        for t in range(1, len(days)):
            for let, v in days[t - 1]["vals"].items():
                x = m._f(v)
                if x is not None and math.isfinite(x):
                    bags[let].append(x)
    q = {}
    for let, xs in bags.items():
        if len(xs) < 50:
            continue
        xs = sorted(xs)
        def pct(p, xs=xs):
            return xs[int(p * (len(xs) - 1))]
        q[let] = (pct(0.10), pct(0.25), pct(0.50), pct(0.75), pct(0.90))
    return q


m.ALL_LETTERS = ALL_LETTERS
m.features_at = features_with_shade
m.discovery_quantiles = discovery_quantiles_5
ckpt.install()


if __name__ == "__main__":
    ckpt.main()
