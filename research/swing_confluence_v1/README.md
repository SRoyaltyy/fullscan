# swing_confluence_v1 (research only, add-only)

Asks one thing: when 3 or more of v4's five per-stock signal families (volume surge, BB squeeze/breakout, divergence, MACD/RSI cross,
green/red extreme) fire within a trailing 5-bar window, is the next move **bigger** than the same-day universe base rate? This is about
size, not direction. It checks |log return| at 1d/5d/21d and forward realized vol at 5d/21d. Universe: v4's top 500 by 20-day $ volume, price >= $5.
Data runs 2020-01 → 2026-10, split into 2020–22 and 2023–26.

* Timing: signals and controls come from bar D (prior close). They predict for N = D+1, and outcomes run D→D+h. DoltHub IV must be dated <= D (next-day only).
  Earnings come from scheduled dates, with after-close reports shifted to the next session.
* Controls ("ctl") are fitted walk-forward: yearly windows, OLS trained only on rows dated before window start minus h sessions, all on date-demeaned values.
  They are 20/60-day RV, \|r_D\|, $ volume, earnings in window, recent earnings and VIX. Extra specs add the day-D volume ratio, IV/IV rank/IV−RV, or drop every earnings window.
* Inference: t-stats use non-overlapping h-session blocks of daily means, and Holm runs over all 105 tests. Leak test: `test_leak.py` plus CI workflow `swing_confluence_v1.yml`.
* Paper straddle: payoff |S_{D+h}/S_D−1| minus 0.798·IV·√(h/252). It has no spread or fees, so it is an upper bound.

## Result (results.json)
The raw effect is real: 3+ signals mean about +6.8% bigger 1-day |moves| (t 13.5, both halves). Most of it is already-jumpy stocks,
earnings and the day-D volume spike. After base controls, 1d is still +2.4% (Holm p<0.001, both halves), but it **disappears once day-D
volume is added (−0.6%)** and isn't significant with IV−RV controls on the IV subset (+1.1%, Holm p=1). 5d/21d controlled results are flat in every spec.
The straddle against DoltHub IV loses money at every horizon in absolute terms. 3+ signals beat same-day names by only +0.06%/day at 1d, before costs.
**Verdict: it does not beat the base rate in any tradable way.**
