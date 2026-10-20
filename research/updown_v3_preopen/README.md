# updown_v3_preopen (research)

Per-ticker pre-open chance of "up" over 1 / 5 / 21 / 63 trading days, measured from the prior close.
Page: `dashboard/updown-v3/` (https://sroyaltyy.github.io/fullscan/dashboard/updown-v3/). Leak audit: `LEAK_AUDIT.md`.
**Research only, not a trading signal. Nothing here goes to Webull.** New strategy name; does not touch v1/v2 or any pinned file.

Files
- `features.py` (features, all from bars dated before the prediction day), `walkforward.py` (purged walk-forward + scoring),
  `build.py` / `build_b.py` / `export.py` (offline one-off build), `auc.py`, `fetch_topup.py`.
- `test_leak.py`: pytest leak test (CI: `.github/workflows/updown_v3_preopen.yml`).
- `history/YYYY.csv.gz`: every published day × ticker: `p_<h>` = walk-forward out-of-sample P(up), `y_<h>` = realized (blank = not yet known).
- `feature_asof.csv`: per day, the oldest/newest bar date used by any feature. `backtest.json`: windows, results (variant B published, A kept).
- `SOURCES.json`: inputs and when they are available.

Rebuild (box, low priority): `nice -n 19 python -m research.updown_v3_preopen.build out && ... build_b out && ... export out research/updown_v3_preopen dashboard/updown-v3`
(the build scripts read `/workspace/updown/bars_all.parquet`; needs scikit-learn).

Append-only from here: the daily lock job (separate PR) will add locked forward days; past history rows are not rewritten.
