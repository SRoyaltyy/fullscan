# updown_v4_preopen (research)

Per-ticker pre-open chance of "up" over 1 / 5 / 21 / 63 trading days from the prior close, built like v3 (#655/#660),
plus five feature blocks, a with/without ablation, an event study and a signal-confluence study.
Page: `dashboard/updown-v4/` (https://sroyaltyy.github.io/fullscan/dashboard/updown-v4/).
**Research only, not a trading signal. Nothing here goes to Webull.** Does not touch v1/v2/v3, any pinned file, or sealed rows.

## What is new vs v3
Base = v3's published variant B (stock-level v3 features, HistGradientBoosting + Platt shrink, same yearly/quarterly
windows and h-session purge; code imported read-only from `research/updown_v3_preopen`). Universe: price >= $5 and
top **500** by 20-day dollar volume as of N-1 (v3 used 1500; reduced to fit the box). All 7 models are run on this same universe.

| block | columns (all as of the bar dated N-1, i.e. prior close) |
|---|---|
| ta | RSI14, MFI14, MACD/signal/hist (÷ close), Bollinger %B and width (20, 2σ) |
| opt | free CBOE indexes: VIX, 5-day VIX change, VIX9D/VIX, VIX/VIX3M, SKEW, VVIX (market-level; no free per-ticker option history) |
| hmm | 3-state Gaussian HMM on (median stock return, its 20d vol); refit at every window start on dates before it; **filtered** (forward-only) probabilities; states ordered by vol = calm/trend/stress |
| div | price at new 14/20-day closing high (low) while RSI, MFI or MACD stays below (above) its prior max (min); net count |
| gr | share of up days and log(up-volume/down-volume) over 5/10/20 days |

## Options data sources (researched 2026-10-21; nothing paid for)
See `SOURCES.json`. Free with years of history: CBOE index CSVs (VIX 1990-, VIX3M 2009-, VIX9D 2011-, SKEW 1990-, VVIX 2006-), **used**.
CBOE equity put/call CSV is free but stops 2019-10-04, **excluded**. DoltHub options (free, per-ticker IV, 2019- only, undocumented capture time), **not ingested**.
yfinance: live snapshot only. Paid: ORATS ($599 archive + $99/mo; API $199/mo), HistoricalData.net ($590 full / $99-330 partial),
Massive/Polygon ($0 = 2y bars without IV; $29/$79/$199 per month; no IV history), Cboe DataShop (~$8.5k).
So block 2 is market-level implied vol/skew only; per-ticker IV / put-call skew / option volume was skipped (no free source qualified).

## Scoring
Brier per date, averaged; t-stats on every h-th date (non-overlapping labels). Holm penalty: with-vs-without across 5 blocks × 4 horizons;
vs coin (0.25) and vs base rate across 7 models × 4 horizons. Results in `backtest.json`; events in `events.json`; confluence in `confluence.json`.

## Leak audit
`test_leak.py` (CI `.github/workflows/updown_v4_preopen.yml`): scrambling every bar and every CBOE row dated >= N leaves all block
features for N unchanged; HMM probabilities before t are unchanged when data after t changes (filtered, no backward pass) and the fit
uses only data before the window start; labels/event outcomes (`fwd*`, `ev_*`, `y_*`) are never features; published `feature_asof.csv` < N;
every used source is final on N-1 (CBOE by ~16:15 ET). Event and confluence thresholds are fixed a priori (nothing fitted) and reported for 2020-01-01 onward only
(the walk-forward test span), with a 2020-22 vs 2023-26 split as a replication check.

## Rebuild (box, low priority, outside 03:30-09:40 ET)
`nice -n 19 python -m research.updown_v4_preopen.build bars.parquet cboe_dir out && ... confluence_build bars.parquet cboe_dir out && ... export out research/updown_v4_preopen dashboard/updown-v4`
(needs scikit-learn, scipy, hmmlearn). Latest prediction day 2026-10-20 (Yahoo had no final 10-20 bar at build time).

Append-only: `lock.py` fingerprints every prediction day (p_* columns) into `LOCKS.json`; a new day is locked only before 09:25 ET, and CI (`lock check`) fails if any locked day changes. Past rows are never rewritten.
