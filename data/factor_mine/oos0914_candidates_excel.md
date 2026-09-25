# oos0914 candidates — Excel
Author: Excel · Submitted: 2026-09-26 00:20 HKT (file written 00:26 HKT) · All 5 submitted, none dropped.
No data dated 2026-09-14 or later may be loaded. Every setting is fixed here, or chosen by time-ordered CV inside the training window only.

## Shared setup
- **Training universe:** multi-year Yahoo split-adjusted daily history ending with the 2026-09-11 session. A stock-day is included when:
  - the previous day's move was at least 5% up or down, OR its RelVol was at least 2, AND
  - its previous close was at least $1, AND
  - its 20-day average dollar volume was at least $1M.
- **Live universe:** that session's frozen morning candidate list (`data/factor_mine/candidates/<date>.json`, later `send_inputs/<date>.json`).
- **Features:** all known before 09:30 ET on day D, from bars through D-1 only, then z-scored across stocks within each day:
  - ret1, ret5, ret20 (close to close)
  - gap1, the D-1 open over the D-2 close, minus 1
  - range1, the D-1 (high minus low) over the D-2 close
  - relvol and log(relvol), where relvol is the D-1 volume over the 20-day average volume
  - atr14_pct, the 14-day ATR over the D-1 close
  - dist_hi20, the D-1 close over the 20-day high, minus 1
  - dist_lo20, the D-1 close over the 20-day low, minus 1
  - log price (D-1 close)
  - log of the 20-day average dollar volume
  - ret1 multiplied by log(relvol)
  - Missing values are filled with the day's cross-sectional median, and z-scores are capped at ±5.
- **Target:** day D's open-to-close return minus 0.0015 (15bp).
- **Trading:**
  - Buy the top 4 by score at day D's open, equal weight, and hold 1 session (exit at D's close under the build's standard exit).
  - Use Futubull fees.
  - Apply the live wire's skip rules (don't buy what's already held, and the hard-red skip).
  - Break ties by ticker, ascending.
- **Weights:** frozen at the end of training (2026-09-11), with no retraining during the test.
- **Seed:** 7 everywhere.

## Candidates
1. **oos0914_ridge_px**: ridge regression on the features to predict the target. alpha is chosen from {0.1, 1, 10, 100, 1000} by 5-fold time-ordered (expanding window) CV on training only, maximising the mean daily top-4 target.
2. **oos0914_logit_px**: L2 logistic regression predicting whether the target is above +0.01. C is chosen from {0.01, 0.1, 1, 10} by the same CV. Score is the predicted probability.
3. **oos0914_gbm_px**: gradient-boosted regression trees (sklearn HistGradientBoostingRegressor or an equivalent) with max_depth 3, 300 iterations, learning_rate 0.05, min_samples_leaf 200 and random_state 7, predicting the target. No tuning.
4. **oos0914_ens_px**: the mean of the within-day percentile ranks of candidate 1's and candidate 3's scores.
5. **oos0914_ridge_px_exmega**: identical to candidate 1, with the same fitted weights, except that a stock whose ret1 is above +40% or below -40% is dropped before picking the top 4.

## Luck test (Excel, after the 03:00 freeze)
- Best-of-N on the 08-13 to 09-11 training window, where N is every candidate on the frozen list from every author.
- Each candidate's mean is removed, then 5,000 stationary block bootstraps are run (mean block length 3, seed 7).
- p is the share of runs where the luck-only best total matched or beat the top candidate's actual total.
- Reported both after Futubull fees and at 15bp, and posted before any result from 09-14 on.
- The 09-14 to 09-25 results count as designed after the fact. The clean test starts with locked days from 2026-09-28.
