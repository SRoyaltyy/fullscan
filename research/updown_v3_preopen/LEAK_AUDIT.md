VERDICT: PASS (no leak found) — and no horizon beats a coin flip or the base rate

# Leak audit: updown_v3_preopen

Built offline on the box on 2026-10-20 (HKT), the same way as `updown_o2c_v2` (see `dashboard/updown/LEAK_AUDIT.md`).
New strategy name; v1/v2 files and every ENGINE_SHA256-pinned file are untouched. Research only, nothing goes to Webull.

## What it predicts
For each prediction day N and ticker: P(close at session N+h-1 > close at N-1) for h = 1, 5, 21, 63 sessions.
The reference price is the **prior close**, the last price known before the open, so nothing from day N's open is needed.

## 1. Feature timing: PASS
- `features.py` computes every quantity "as of the close of bar D" and then maps bar D to the **next** session N (`searchsorted(..., 'right')`).
  A bar is only used if D is exactly the previous session (no stale carry). Row column `feat_asof` = D.
- Market columns (`mkt_*`, `breadth1`) are cross-sectional stats of already-lagged columns. Ranks `r_*` are same-day ranks of lagged columns.
- Universe (price ≥ $5, top 1500 by 20-day dollar volume) is chosen from N-1 data.
- No feature uses day N's open, high, low, close or volume (`test_no_same_day_open_features`).
- **Inputs not used:** the pre-open Finviz scrape (~32 sessions of history) and the daily predict/news files (a few months, gaps, provider changes)
  are too short to walk-forward honestly. They are listed in `SOURCES.json` with `used_in_model: false`.

## 2. Leak test (pytest): PASS, 7/7
`research/updown_v3_preopen/test_leak.py`, run by `.github/workflows/updown_v3_preopen.yml`:
- every feature row's input timestamp (close of `feat_asof`, 16:00 ET) is before 09:25 ET on the prediction day;
- scrambling every bar dated ≥ N (incl. day N's open) leaves day-N features bit-identical (3 cut points);
- the published `feature_asof.csv` (one row per published day) has max input date < prediction date, i.e. before 09:25 ET;
- every source used in `SOURCES.json` is available on the prior session (or same day before 09:25 ET).

## 3. Walk-forward: PASS
- Refit yearly 2020–2025, then quarterly in 2026 (windows in `backtest.json` → `windows`).
- Each model trains only on rows whose label was realized before the window's first test day:
  train rows dated ≤ session(first test day − h). `sessions_gap` in the log is h+1 for every window.
- Published variant **B**: stock-level features only (no market/day-of-week columns), plus a Platt shrink fitted on the
  last 250 training sessions by a model that never saw them (purged by h). Variant A (all features, no shrink) is kept in
  `backtest.json` → `variant_A` for the record. Both are reported; neither beats the baselines, so there is no selection gain to hide.

## 4. Results (walk-forward OOS, 2020-01-02 .. last realized day)
Brier per date, averaged. Coin flip = 0.2500. Base rate = share of ups in the training years. t on non-overlapping days (every h-th date).

| horizon | rows | days (indep.) | Brier B | base | t vs coin | t vs base | hit B | always-up hit | AUC (t) | Brier A |
|---|---|---|---|---|---|---|---|---|---|---|
| 1d | 2,561,988 | 1708 (1708) | 0.2502 | 0.2501 | −1.07 | −0.86 | 50.6% | 50.4% | 0.511 (6.0) | 0.2594 |
| 5d | 2,555,972 | 1704 (341) | 0.2503 | 0.2499 | −0.01 | −0.34 | 51.1% | 51.5% | 0.512 (3.4) | 0.2635 |
| 21d | 2,531,944 | 1688 (81) | 0.2545 | 0.2502 | −1.06 | −1.09 | 52.0% | 52.0% | 0.512 (0.8) | 0.2680 |
| 63d | 2,468,965 | 1646 (27) | 0.2590 | 0.2504 | −0.72 | −0.81 | 52.8% | 53.4% | 0.520 (1.7) | 0.2614 |

**Plain reading:** the model's chances are no better than saying 50/50 (or the usual up-share) for any horizon, so the page
shows "not yet meaningful" on all four. It ranks stocks very slightly better than random at 1d/5d (AUC ≈ 0.51), but that is
too small to turn into a useful probability. 63d has only 27 independent periods, so it is reported but weak by construction.

## 5. Caveats
- **Survivorship:** the ticker list holds only names still trading in 2026.
- **Bars:** `/workspace/updown/bars_all.parquet` (≤ 2026-10-05) + yfinance top-up 2026-10-06..10-19 (`fetch_topup.py`).
  Seam check on 2026-09-15..10-05 overlap: median close ratio 1.000, 99.7% within 0.5%; tickers off by > 2% keep only the old source.
- **2026-10-20** (newest day) was computed at build time ~06:20 ET from closes through 10-19, before 09:25 ET, but it was not
  locked/fingerprinted. Locked forward days start with the separate daily lock job (later PR).
