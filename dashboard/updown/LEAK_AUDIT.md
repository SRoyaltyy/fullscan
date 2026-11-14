VERDICT: PASS

# Leak audit: updown open→close model (v1 audited, v2 published)

Audit run on 2026-10-06 (HKT) on the box in /workspace/updown. Scripts and logs are in `audit/`.

**What PASS means:** the page publishes `updown_o2c_v2`. The one real leak found (in v1) is fixed in v2. Every check below
passes for v2, but two caveats remain. Neither puts future data into a prediction, but both change how the results should be read:
- **Open price:** the open→close edge depends on today's official open price. That price cannot be traded as printed, and without it the after-fee edge disappears.
- **Survivorship:** the stock list contains only companies that still exist in 2026.
v1's numbers were not rewritten. v1 would have been a FAIL.

## Headline numbers

All runs use the same `evaluate.py`:
- Metrics are computed per date, then averaged.
- L/S net = top-decile minus bottom-decile open→close return, minus 0.4% (0.1% per side × 4).
- t-stats are across dates.
- Dev = 2020-01-02..2026-07-02 (walk-forward, yearly retrain). Holdout = 2026-07-06..2026-10-05.

| run | dates | tickers/day | AUC (t) | hit | always-down | hit − always-down (t) | log-loss gain vs base (t) | L/S net/day (t) |
|---|---|---|---|---|---|---|---|---|
| v1 dev | 1633 | 3289 | 0.5439 (27.5) | 52.53% | 51.71% | +0.82pt (2.1) | 0.0023 (10.9) | +0.18% (5.8) |
| v1 holdout | 65 | 4077 | 0.5471 (7.5) | 54.79% | 55.66% | −0.87pt (−0.9) | 0.0034 (5.8) | +0.37% (3.2) |
| **v2 dev (published)** | 1633 | 3246 | 0.5416 (26.3) | 52.32% | 51.59% | +0.73pt (1.9) | 0.0020 (10.1) | +0.11% (3.7) |
| **v2 holdout (published)** | 65 | 4050 | 0.5476 (7.8) | 54.66% | 55.64% | −0.97pt (−1.0) | 0.0033 (5.8) | +0.39% (3.6) |
| v2 with NO same-day open info, dev | 1633 | 3246 | 0.5208 (12.9) | 51.33% | 51.59% | −0.26pt (−0.7) | 0.0004 (2.4) | **−0.20% (−6.3)** |
| v2 with NO same-day open info, holdout | 65 | 4050 | 0.5243 (3.1) | 54.23% | 55.64% | −1.40pt (−1.6) | 0.0011 (2.4) | **+0.07% (0.6)** |
| v2 holdout, survivors only (alive 10-05) | 65 | 4045 | 0.5476 | 54.66% | – | −0.98pt | – | +0.39% (3.6) |
| v2 holdout 08-13..10-05, all rows | 37 | 4067 | 0.5471 | 55.46% | – | −2.65pt | – | +0.40% (3.6) |
| v2 same dates, point-in-time Finviz list only | 32 | 4036 | 0.5474 | 55.38% | – | −2.94pt | – | +0.41% (3.5) |

**Plain reading:**
- After the fix, the open-based model still ranks stocks better than chance on dev and on the holdout (AUC ≈ 0.54–0.55).
- Its 0/1 hit rate does not beat "always down" on the holdout.
- **Fully clean version.** With features only through yesterday's close and the target still today's open→close, the ranking drops to AUC ≈ 0.52.
  - On dev, the after-fee long-short is clearly negative.
  - On the holdout it is not significant.
  - So there is no tradable after-fee edge without today's open.
- **The open-based edge is not tradable as printed.** Its entry price is the same official open that one of its inputs (the gap) is computed from. See check 7.

---

## 1. Feature timing: PASS

**Code** (features.py):
- Prior-day quantities use `shift(1)` within each ticker: line 7.
- `gap = open_N / close_{N-1} - 1` uses day N's open only: line 9.
- `ret1` is the lagged log return: lines 10–12. `ret{3..250}` run from close N-1 back: lines 13–14.
- `o2c_prev` and `c2o_prev`: lines 15–16. `range_prev` and `clv_prev`: lines 17–18.
- Volatility comes from the lagged return series: lines 19–20.
- Dollar volume and relative volume come from the lagged series: lines 21–26.
- `pos20` and `dd250` come from lagged highs and lows: lines 28–32.
- `green10`, `o2c_mean10` and `c2o_mean10` are shifted: lines 33–37.
- Date-level features (lines 50–57):
  - `mkt_ret1`, `mkt_ret5` and `breadth1` are medians or shares of lagged returns.
  - `mkt_gap` is the cross-sectional median of today's gap, which is knowable once the opens print.
  - `r_*` are same-day ranks of these knowable columns.
- No feature reads day N's high, low, close, volume or VWAP.

**Numeric tests:**
- a) **Truncation test** (`audit/t1_trunc2.py`, which execs the real features.py source):
  - For 7 dates (2020-03-16, 2021-06-15, 2022-11-01, 2024-02-08, 2025-08-20, 2026-07-15, 2026-09-30), every bar after N was deleted.
  - Day N's high, low, close and volume were replaced with random garbage.
  - Features were rebuilt and compared with the production panel: **965,720 cells, 0 mismatches**, and an identical universe on every date.
- b) **Independent re-implementation** (`audit/t1_pairs.py`): hand-written numpy, not features.py, working from raw bars truncated at N-1 plus open N.
  - **200 random (ticker, date) pairs × 28 per-ticker features = 5,600 cells, 0 mismatches.**
  - Cross-sectional features (medians, breadth, ranks, gap_rel) were rebuilt from that independent code for 8 full dates.
  - Result: **219,216 cells, 0 mismatches**, and an identical universe on all 8 dates.

## 2. Calibration and de-meaning: PASS

**Code** (wf.py, unchanged in wf2.py):
- The twin model is fit on training dates up to 250+h sessions before the cut.
- Platt scaling is fit on the last 250 training sessions (`calset ⊂ tr`): lines 62–70.
- At test time, the day's mean is the mean of the model's own predicted logits for that day's names: lines 80–82.
- No realized outcome and no realized cross-sectional mean enters any prediction.

**Numeric tests:**
- **Scramble test:** in window 2025, the test rows' realized returns and labels were replaced with random noise before predicting. Max |Δp| = **0.0** over 924,572 rows.
- **Calibration dates:** per-window logs (`audit/wlog/*.json`) show calibration data always ends on or before the training max date, and the twin model's data ends before calibration starts (`audit/t3_purge.log`).
- **Reproduction:** wf2.py reproduces v1's wf.py output exactly (max |Δp| = 0.0, 924,572 rows), so these checks apply to v1 too.

## 3. Walk-forward, purge and holdout use: PASS (honesty note)

- **Purge:** for every prediction row in v2 dev, v2 holdout and the no-open variant, the training max date is at least **2 sessions** before the prediction date. That covers a horizon of 1 session plus a 1-session purge.
  - Every row is covered by exactly one window, with no duplicates (`audit/t3_purge.log`).
  - Example: holdout train_max 2026-07-01, first test date 2026-07-06.
- **Fit on training data only:** LightGBM needs no scaling. The LR baseline's clip and scaler are fit on training data only (wf.py lines 89–93). No feature selection is fit on data.
- **Choices made on dev:**
  - Hyperparameters (300 trees, lr 0.03, 63 leaves, min_child 2000) were fixed in Try 1 and never tuned.
  - Feature set, calibration and horizon were chosen on dev. ITERATIONS.md "Decision (made on dev only, before any holdout read)" comes before the single holdout section.
- **Honest caveats:**
  - Try 9 (all-inputs ablation) ran inside the holdout window after the holdout was read. It changed nothing in the model.
  - **v2 was built after v1's holdout had been seen**, so the v2 holdout is a second look, not pristine.
  - The v2 change was dictated by the audit checklist (remove the price-level leak). It was not chosen for holdout results, and nothing was tuned on the holdout.
  - The v2 holdout AUC (0.5476) is essentially v1's (0.5471).

## 4. Price adjustment: v1 FAIL → fixed in v2: PASS

**The v1 leak:**
- `lprice = log(prev_close)` (features.py line 27) and the universe filter `prev_close >= 1` (line 47) use the split-adjusted price **level**.
- Yahoo rescales all past prices for later splits. The data has **2,088 reverse splits and 488 forward splits** (Yahoo split events, `audit/splits.parquet`).
- So the level carries future information.

**Effect** (`audit/t4_level.py`):
- **72,892 scored v1 rows (1.29%)** were in the universe only because a later reverse split inflated the adjusted price. Their actual prior close was below $1.
- 10.1% of rows have some later split.
- The adjusted level flags names that will **later** reverse-split, with per-date AUC **0.62**.
- Those rows average **−0.41%/day** open→close, with an up-rate of 42.2%. Rows with no later split: −0.02%, up-rate 48.5%.

**v2 fix** (features_v2.py: same features.py source with two edits; wf2.py `--fset v2`):
- The $1 filter uses the actual prior close: adjusted close × the product of later split ratios. Check: NVDA's 2020-01-02 adjusted close of 5.998 × 40 = 239.9, the actual price.
- `lprice` is dropped. Every remaining feature is a ratio, return, rank or count.

**v2 universe change:** 76,611 panel rows removed (actual price below $1) and 289 added (forward splitters).

**Effect on metrics:**
- Dev L/S net falls from **+0.18% (t 5.8) to +0.11% (t 3.7)**, and dev AUC from 0.5439 to 0.5416.
- The holdout is unchanged (0.5476 AUC, +0.39%), which is expected: few splits after 2026-10 are known yet.

**Dividends:** not used. `close` equals Yahoo's unadjusted Close (split-adjusted only) exactly, with median difference 0.0, for T, VZ, MO, XOM, KO, PFE, O and IBM (`audit/t4_div.py`). The open→close target is unaffected by dividend adjustment in any case, because one factor scales both prices of a day.

## 5. Universe and survivorship: LIMITATION, disclosed (not a prediction leak)

**Facts:**
- **All 6,307 tickers in the data have bars in 2026.** No company that died in 2018–2025 is present. Only 49 tickers stop between 2026-08-12 and 2026-10-02.
- The ticker list is listed common stocks per Nasdaq Trader 2026-09-25 (4,158) plus a late "delisted" leg (2,149) taken from Aug–Sep 2026 Finviz exports.
- So the dev universe was chosen using information from later dates: survival to 2026.
- This cannot be fixed with the available data, because Yahoo does not serve most dead tickers.
- **It does not put future information into any individual prediction.** Each p_up still uses only that stock's own past bars and today's open.

**Effect:**
- **Per-year AUC:** 2020 0.540, 2021 0.547, 2022 0.538, 2023 0.542, 2024 0.544, 2025 0.542, 2026H1 0.536, holdout 0.548.
  - There is no decay as the survival requirement shortens.
  - Per-year L/S net varies from −0.00% to +0.23%.
- **Holdout** (names only need to survive ≤3 months; the 313 rows of names that died are included):
  - Survivors-only gives the same metrics (AUC 0.5476 vs 0.5476).
  - Restricting each day to the point-in-time Finviz list (32 clean days from 2026-08-13) keeps 99.1% of rows. AUC moves 0.5471 → 0.5474 and L/S net +0.40% → +0.41%.
- **Top-up:** the yfinance top-up has no effect on the universe. A name listing after 2026-08-12 cannot reach the 60-prior-bar rule before 10-05.
- **Remaining risk:** dev-period after-fee L/S levels may be biased, in an unknown direction, because dying names are missing.

## 6. Target: PASS

- **Definition:** `y_o2c = close_N / open_N − 1` on the same adjusted series and row (features.py line 41). Up means > 0.
- **No feature reads close N:** the truncation test randomized close N and changed no feature.
- **Overlap:** the only one is the shared **open_N**. `gap` uses open_N as its numerator and the target uses it as its denominator. See check 7.
- **Detection:** the deliberate-leak placebo shows the pipeline would expose any use of close N (check 8).

## 7. Open-price realism: NOT TRADABLE AS PRINTED (not a leak under the 09:30 rule)

- **Signal share:** today's official open is used in 5 features (`gap`, `gap_z`, `r_gap`, `mkt_gap`, `gap_rel`). The gap alone has per-date AUC **0.530 (dev) / 0.535 (holdout)** and rank IC −0.054 / −0.062.
- **Same price as entry:** the target's entry price is that same official open.
- **Why it can't be traded as printed:**
  - To be filled at the official open you must place an opening-auction order before the open prints. Nasdaq's cutoff for market-on-open orders is 09:28 ET.
  - By the time you know the gap, the open is gone, and any later entry pays the spread and the move since the open.
  - Part of the gap signal is also mechanical bid/ask bounce of the opening print. An open printed at the ask looks like an up-gap and then "reverts" to the mid without any tradable move.
  - Also, not every stock has opened at exactly 09:30.
- **Rerun with no same-day open information** (all 5 gap features removed; everything else through the N−1 close only):
  - Dev AUC 0.5208 (t 12.9), L/S net **−0.20%/day (t −6.3)**.
  - Holdout AUC 0.5243 (t 3.1), L/S net **+0.07%/day (t 0.6)**.
  - Hit rate is below always-down on both.
- **Conclusion:** about half of the AUC above 0.5 comes from the open print: (0.5416 − 0.5208) / 0.0416 = 0.50 on dev and 0.49 on the holdout. **All** of the after-fee edge does.
- **Plainly:** the open-based numbers on this page are a statistical description, not an achievable trading result. The fully clean version has no after-fee edge.

## 8. Placebo tests: PASS

| test | dates | AUC (t) | reading |
|---|---|---|---|
| Train labels shuffled within each date; retrained (windows 2022, 2024, 2026H1) | 628 | **0.4977 (−1.1)** | ≈ 0.50 as expected. The same windows unshuffled give 0.5399 (t 15.5). L/S net −0.40%, which is just the fees. |
| All features lagged one extra day (v2), dev | 1633 | **0.5209 (12.8)** | Drops but does not collapse. It ≈ the no-open version (0.5208), because the extra lag removes today's open. It does not rise. |
| All features lagged one extra day (v2), holdout | 65 | 0.5162 (2.0) | Same reading. |
| Deliberate leak: add the same-day close (close_N / close_{N−1} − 1) (windows 2023, 2025) | 500 | **0.9999** | Implausible, so it is caught at once. The same windows without it give 0.5420. |

## 9. Dashboard data: PASS

`audit/t9_dash.py` decodes every file in `dashboard/updown/data/` (v2) and compares it with `preds/o2c_v2_dev.parquet` and `preds/o2c_v2_holdout.parquet`:

- **Row match:** **5,563,657 rows on both sides, 0 unmatched.** Max |p_dash − p_pred| = 5.0e-5, which is the 4-decimal storage rounding, and 0 rows exceed it.
- **Random sample:** **1,500 random rows across all 7 years** (2020: 189, 2021: 217, 2022: 236, 2023: 215, 2024: 211, 2025: 229, 2026: 203). Max |Δp| = 5.0e-5 and max |Δ realized| = 5.0e-5.
- **Out-of-sample only:** all predictions come from the walk-forward windows in check 3. The dev file is the concatenation of out-of-sample windows, the holdout comes from one frozen model, and 0 period labels are wrong. No in-sample prediction exists in the data.
- **Realized values feed nothing predictive:**
  - In index.html, p_up, Call (p > 0.5) and Decile (the within-day rank of p) are computed from p only.
  - Realized o2c feeds only the "Realized o2c", "Realized dir" and "Hit" columns and the per-day realized stats (realized up-rate, hit, AUC, decile spread). These are labeled as realized.
  - Mean p_up in index.json equals the mean of the stored predictions (max difference 0.0).
- **Display note:** 15 rows with |realized o2c| > 327.67% are clipped in the display (int16 storage). Their predictions are unaffected.

## What changed

- New model **updown_o2c_v2**: actual-price $1 filter, no price-level feature. Code: `features_v2.py`, `wf2.py`.
- Predictions: `preds/o2c_v2_dev.parquet`, `preds/o2c_v2_holdout.parquet`.
- Dashboard data regenerated from v2. The page title, notes and README were updated, with the open-price and survivorship warnings on the page.
- v1 files and v1 numbers in ITERATIONS.md were left as they were. The audit is logged in ITERATIONS.md under "Leak audit".

## Plain-English summary

- **Leak found and fixed:** the first version accidentally used a price that Yahoo rewrites after the fact. That revealed which stocks would later do a reverse split, and those stocks tend to fall. Fixing it trimmed the dev after-fee edge from +0.18% to +0.11% a day.
- **Timing is clean:** no other leak was found. Feature timing, calibration, the walk-forward gaps and the published data all check out, and the placebos behave as they should.
- **Two caveats remain:**
  - Nearly all of the after-fee edge comes from knowing today's official opening price, which you cannot trade at once you know it. Using only information up to yesterday's close, the model still ranks slightly better than chance (AUC ≈ 0.52) but makes no money after costs.
  - The historical stock list contains only companies that still exist today.
- **How to treat the page:** as research, not as a trading signal.
