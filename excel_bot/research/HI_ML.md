# Full-sheet ML → H / I (multi-year, clock-clean)

_Generated 2026-09-07 · live `flatten_robust` frozen. Yahoo/rows A–F seed the whole A–JL DAG. No Excel STOCKHISTORY cache. Not a forecast wire._

## Plain English

Full-sheet ML on clock-clean A–JL values (Yahoo A–F DAG, as far back as the book / parquet go) does not beat the overnight gap. Clean **null**. Predicting I at the open is gap algebra: I = overnight + scaled H, and overnight is knowable at 9:30 (C today vs B yesterday). Ridge and LightGBM IC on I match the gap (or lose). Predicting H (the rest of the day) dies on both tapes and a five-name ghost. Hand gates did not hide a nonlinear join. Research only; live flatten_robust stays frozen.

**Panel path (not per-ticker chat):** batch Yahoo/rows → name-day panel → train once → time holdout → next fold. Panel: **3548** names · **762084** name-days · 2025-09-23 → 2026-08-10. Open-entry features **208** (locked 44 + open-derived + lags of every reconstructed letter, never same-row H/I). Walk-forward folds fold_q1, fold_q2, fold_q3; primary cut **2026-04-01**. Futubull 0.15% long off the top-20% recipe and the buy-everyone book. fold_q1 ridge → H 1d holdout +0.63% (n=42468) vs book +0.62 pp IC ρ=+0.089 / r=+0.008 (**KILL**); fold_q1 lgb → H 1d holdout +1.22% (n=42468) vs book +1.21 pp IC ρ=+0.103 / r=+0.019 (**KILL**); fold_q1 ridge → I 1d holdout +2.24% (n=42468) vs book +2.39 pp IC ρ=+0.340 / r=+0.211 (**KILL**); fold_q1 lgb → I 1d holdout +2.44% (n=42468) vs book +2.58 pp IC ρ=+0.361 / r=+0.245 (**KILL**); fold_q2 ridge → H 1d holdout +0.45% (n=43694) vs book +0.48 pp IC ρ=+0.093 / r=+0.047 (**KILL**); fold_q2 lgb → H 1d holdout +0.45% (n=43694) vs book +0.48 pp IC ρ=+0.095 / r=+0.059 (**KILL**); fold_q2 ridge → I 1d holdout +3.01% (n=43694) vs book +2.84 pp IC ρ=+0.341 / r=+0.125 (**KILL**); fold_q2 lgb → I 1d holdout +3.25% (n=43697) vs book +3.08 pp IC ρ=+0.361 / r=+0.167 (**KILL**); fold_q3 ridge → H 1d holdout -0.06% (n=18270) vs book +0.15 pp IC ρ=+0.035 / r=+0.018 (**KILL**); fold_q3 lgb → H 1d holdout +0.02% (n=18646) vs book +0.24 pp IC ρ=+0.058 / r=+0.019 (**KILL**); fold_q3 ridge → I 1d holdout +8.02% (n=18270) vs book +7.09 pp IC ρ=+0.385 / r=+0.074 (**KILL**); fold_q3 lgb → I 1d holdout +8.27% (n=18277) vs book +7.34 pp IC ρ=+0.422 / r=+0.181 (**KILL**).

**Family verdict: null**

### Path (not per-ticker chat)

**batch Yahoo/rows → name-day panel → train once → time holdout → next fold**

One Yahoo A–F rebuild builds the **name-day panel**. Models train once across that panel, then the next expanding-window fold re-uses the same matrix. Iterative = rebuild → train → holdout → next fold. There is no interactive one-stock Excel loop and no per-ticker chat fit.

### Walk-forward folds

| fold | train < | holdout | model | label | holdout pnl | vs book | vs gap | IC | gap IC | verdict | why |
|---|---|---|---|---|---|---|---|---|---|---|---|
| fold_q1 | 2026-01-01 | 2026-04-01 | lgb | H | +1.22% (n=42468) | +1.21 pp | — | ρ=+0.103 / r=+0.019 | ρ=-0.091 / r=-0.048 | **KILL** | tape_thin,q3_missing,ticker_ghost |
| fold_q1 | 2026-01-01 | 2026-04-01 | ridge | H | +0.63% (n=42468) | +0.62 pp | — | ρ=+0.089 / r=+0.008 | ρ=-0.091 / r=-0.048 | **KILL** | hold_t,tape_thin,spy_regime,q3_missing,ticker_ghost |
| fold_q1 | 2026-01-01 | 2026-04-01 | lgb | I | +2.44% (n=42468) | +2.58 pp | — | ρ=+0.361 / r=+0.245 | ρ=+0.361 / r=+0.597 | **KILL** | tape_thin,q3_missing,gap_algebra |
| fold_q1 | 2026-01-01 | 2026-04-01 | ridge | I | +2.24% (n=42468) | +2.39 pp | — | ρ=+0.340 / r=+0.211 | ρ=+0.361 / r=+0.597 | **KILL** | tape_thin,q3_missing,gap_algebra |
| fold_q2 | 2026-04-01 | 2026-07-01 | lgb | H | +0.45% (n=43694) | +0.48 pp | — | ρ=+0.095 / r=+0.059 | ρ=-0.094 / r=-0.008 | **KILL** | q3_missing,ticker_ghost |
| fold_q2 | 2026-04-01 | 2026-07-01 | ridge | H | +0.45% (n=43694) | +0.48 pp | — | ρ=+0.093 / r=+0.047 | ρ=-0.094 / r=-0.008 | **KILL** | disc_t,spy_regime,q3_missing,ticker_ghost |
| fold_q2 | 2026-04-01 | 2026-07-01 | lgb | I | +3.25% (n=43697) | +3.08 pp | — | ρ=+0.361 / r=+0.167 | ρ=+0.365 / r=+0.905 | **KILL** | q3_missing,gap_algebra |
| fold_q2 | 2026-04-01 | 2026-07-01 | ridge | I | +3.01% (n=43694) | +2.84 pp | — | ρ=+0.341 / r=+0.125 | ρ=+0.365 / r=+0.905 | **KILL** | q3_missing,gap_algebra |
| fold_q3 | 2026-07-01 | tape end | lgb | H | +0.02% (n=18646) | +0.24 pp | — | ρ=+0.058 / r=+0.019 | ρ=-0.064 / r=-0.004 | **KILL** | disc_t,hold_t,spy_regime,no_edge_vs_uncond,month_split,ticker_ghost |
| fold_q3 | 2026-07-01 | tape end | ridge | H | -0.06% (n=18270) | +0.15 pp | — | ρ=+0.035 / r=+0.018 | ρ=-0.064 / r=-0.004 | **KILL** | disc_t,hold_t,hold_sign,tape_split,spy_regime,q3_sign,q3_split,no_edge_vs_uncond,month_split,ticker_ghost,no_edge_vs_book |
| fold_q3 | 2026-07-01 | tape end | lgb | I | +8.27% (n=18277) | +7.34 pp | — | ρ=+0.422 / r=+0.181 | ρ=+0.426 / r=+0.993 | **KILL** | gap_algebra |
| fold_q3 | 2026-07-01 | tape end | ridge | I | +8.02% (n=18270) | +7.09 pp | — | ρ=+0.385 / r=+0.074 | ρ=+0.426 / r=+0.993 | **KILL** | gap_algebra |

### What was scored

Cyrus asked for supervised ML: anything clock-clean from rows above (and open-knowable same-row only if fair) trains to predict **H** (intraday % close vs open) and **I** (daily % vs yesterday) below. The feature matrix is the **entire workbook A–JL** that the sheet can compute from Yahoo A–F — every reconstructable letter’s value, plus lags — not the locked-44 pair+lag subset. Fills that need ColorEngine (the 275-col dump is not on this VM) are out of this pass; their values are the DAG. Text EQ and external VIX (IY) are skipped.

Excel gate (enforced in `assert_ml_gate`):

- Never same-row H or I (nor transforms). Labels only. Yesterday’s H/I are fair.
- Never same-row `core_score` / anything that reads D/E/F/H/I on the same row at the open.
- Open-entry same-row: locked 44 values + fills A,B,C,G,J,K,L,M,O (fills not dumped here) + overnight gap (C today vs B yesterday).
- Open-entry same-row out: B/G/K/M/O/L numbers, D/E/F/N, unknown→close.
- Close-entry: other close cols OK same-row; still never H/I.

Labels are built from Yahoo A–F the same way the sheet does: H = (B−C)/C, I = (B−B[t−1])/B[t−1], stacked I is the k-day compound. Horizons 1d / 2d / 3d / 1w / 2w. Train is **chronological** on the name-day panel. Expanding-window folds: `fold_q1` train < 2026-01-01, hold [2026-01-01, 2026-04-01); `fold_q2` train < 2026-04-01, hold [2026-04-01, 2026-07-01); `fold_q3` train < 2026-07-01, hold ≥ 2026-07-01. Primary cut **2026-04-01**. Name-holdout IC is reported as a ghost check, not the keep bar.

Models: ordinary least squares, ridge (α by train CV), LightGBM (80 trees, depth 4). Recipe = long the top quintile of the score. Edge is versus buy-everyone after the same 15 bp fee.

### Open-entry 1d (primary fold)

| model | label | holdout | vs book | IC Spearman/Pearson | gap IC | Q1 | top-5 | July | tapes ↑/↓ | verdict | why |
|---|---|---|---|---|---|---|---|---|---|---|---|
| lgb | H | +0.45% (n=43694) | +0.48 pp | ρ=+0.095 / r=+0.059 | ρ=-0.094 / r=-0.008 | +0.89% (n=90472) | 39% | 0% | +1.51% (n=51402) / +0.01% (n=60102) | **KILL** | q3_missing,ticker_ghost |
| ridge | H | +0.45% (n=43694) | +0.48 pp | ρ=+0.093 / r=+0.047 | ρ=-0.094 / r=-0.008 | +0.78% (n=90454) | 43% | 0% | +1.36% (n=55229) / -0.07% (n=55815) | **KILL** | disc_t,spy_regime,q3_missing,ticker_ghost |
| lgb | I | +3.25% (n=43697) | +3.08 pp | ρ=+0.361 / r=+0.167 | ρ=+0.365 / r=+0.905 | +2.15% (n=90667) | 6% | 0% | +2.94% (n=82579) / +1.38% (n=36414) | **KILL** | q3_missing,gap_algebra |
| ridge | I | +3.01% (n=43694) | +2.84 pp | ρ=+0.341 / r=+0.125 | ρ=+0.365 / r=+0.905 | +1.95% (n=90454) | 7% | 0% | +2.76% (n=82024) / +1.15% (n=36538) | **KILL** | q3_missing,gap_algebra |
| lgb | I_sum | +3.25% (n=43697) | +3.08 pp | ρ=+0.361 / r=+0.167 | ρ=+0.365 / r=+0.905 | +2.15% (n=90667) | 6% | 0% | +2.94% (n=82579) / +1.38% (n=36414) | **KILL** | q3_missing,gap_algebra |
| ridge | I_sum | +3.01% (n=43694) | +2.84 pp | ρ=+0.341 / r=+0.125 | ρ=+0.365 / r=+0.905 | +1.95% (n=90454) | 7% | 0% | +2.76% (n=82024) / +1.15% (n=36538) | **KILL** | q3_missing,gap_algebra |

Code names (after the English): `ml_open_lgb_H_1d`, `ml_open_ridge_H_1d`, `ml_open_lgb_I_1d`, `ml_open_ridge_I_1d`, `ml_open_lgb_I_sum_1d`, `ml_open_ridge_I_sum_1d`.

### Horizons (open ridge + LightGBM)

| model | label | 1d | 2d | 3d | 1w | 2w |
|---|---|---|---|---|---|---|
| ridge | H | **KILL** +0.45% (n=43694) | **KILL** -0.02% (n=61963) | **KILL** -0.06% (n=61963) | **KILL** -0.03% (n=61963) | **KILL** -0.16% (n=61963) |
| ridge | I | **KILL** +3.01% (n=43694) | **KILL** +0.68% (n=61963) | **KILL** +0.95% (n=61963) | **KILL** +0.71% (n=61963) | **KILL** +0.84% (n=61963) |
| ridge | I_sum | **KILL** +3.01% (n=43694) | **KILL** +5.02% (n=61963) | **KILL** +5.04% (n=61963) | **KILL** +5.77% (n=61963) | **KILL** +11.28% (n=61963) |
| lgb | H | **KILL** +0.45% (n=43694) | **KILL** -0.03% (n=61963) | **KILL** -0.10% (n=175598) | **KILL** -0.09% (n=68643) | **KILL** -0.14% (n=109969) |
| lgb | I | **KILL** +3.25% (n=43697) | **KILL** +1.47% (n=62543) | **KILL** +1.23% (n=76968) | **KILL** +1.41% (n=63102) | **KILL** +1.27% (n=61964) |
| lgb | I_sum | **KILL** +3.25% (n=43697) | **KILL** +5.34% (n=61994) | **KILL** +5.47% (n=62370) | **KILL** +7.70% (n=62685) | **KILL** +17.57% (n=68023) |

### Close-entry 1d (same-row close cols OK; still never H/I)

| model | label | holdout | vs book | IC | verdict | why |
|---|---|---|---|---|---|---|
| ridge | H | +4.87% (n=43694) | +4.90 pp | ρ=+0.828 / r=+0.430 | **KILL** | q3_missing,same_print_close |
| lgb | H | +4.89% (n=43694) | +4.92 pp | ρ=+0.825 / r=+0.458 | **KILL** | q3_missing,same_print_close |
| ridge | I | +5.94% (n=43694) | +5.77 pp | ρ=+0.822 / r=+0.214 | **KILL** | q3_missing,same_print_close |
| lgb | I | +6.12% (n=43694) | +5.95 pp | ρ=+0.836 / r=+0.239 | **KILL** | q3_missing,same_print_close |

### Soft regimes (open ridge 1d, holdout)

Heat is the open-knowable prior-5 mean of I (terciles from **train** dates). Tape is SPY up / down / flat.

| model | label | heat cold | mid | hot | spy↑ | spy↓ | spy flat |
|---|---|---|---|---|---|---|---|
| ridge | H | +0.80% (n=48824) | +0.27% (n=36425) | +0.84% (n=48899) | +1.36% (n=55229) | -0.07% (n=55815) | +0.82% (n=23104) |
| ridge | I | +2.64% (n=52754) | +1.77% (n=31649) | +2.26% (n=49745) | +2.76% (n=82024) | +1.15% (n=36538) | +2.57% (n=15586) |
| ridge | I_sum | +2.64% (n=52754) | +1.77% (n=31649) | +2.26% (n=49745) | +2.76% (n=82024) | +1.15% (n=36538) | +2.57% (n=15586) |

### Top drivers (plain English)

Weights from the open-entry **ridge** on 1d I (absolute coefficient) and LightGBM gain. Lags of H/I are yesterday’s prints, not today’s labels.

**Ridge → 1d I**

1. today's open — today (open-knowable) (`C_l0`)
2. close — close-entry only at lag 0 — 1 session(s) ago (`B_l1`)
3. high — close-entry only at lag 0 — 1 session(s) ago (`D_l1`)
4. open-to-open % — today (open-knowable) (`J_l0`)
5. typ — 2 session(s) ago (`typ_l2`)
6. intraday % (close vs open) — label; lags are yesterday+ — 1 session(s) ago (`H_l1`)
7. today's open — 2 session(s) ago (`C_l2`)
8. close — close-entry only at lag 0 — 5 session(s) ago (`B_l5`)

**LightGBM → 1d I**

1. overnight gap (today's open vs yesterday's close) — today (open-knowable) (`overnight_l0`)
2. open-to-open % — today (open-knowable) (`J_l0`)
3. daily % (close vs yesterday) — label; lags are yesterday+ — 1 session(s) ago (`I_l1`)
4. intraday % (close vs open) — label; lags are yesterday+ — 1 session(s) ago (`H_l1`)
5. volume vs 50-day average — 1 session(s) ago (`BN_l1`)
6. volume — close-entry only at lag 0 — 1 session(s) ago (`F_l1`)
7. weighted |prior H| (sheet EP) — today (open-knowable) (`EP_l0`)
8. volume vs yesterday — 1 session(s) ago (`G_l1`)

**Ridge → 1d H**

1. open-to-open % — today (open-knowable) (`J_l0`)
2. low — close-entry only at lag 0 — 1 session(s) ago (`E_l1`)
3. intraday % (close vs open) — label; lags are yesterday+ — 1 session(s) ago (`H_l1`)
4. 50-day average close — 1 session(s) ago (`CJ_l1`)
5. close — close-entry only at lag 0 — 1 session(s) ago (`B_l1`)
6. high — close-entry only at lag 0 — 1 session(s) ago (`D_l1`)
7. high — close-entry only at lag 0 — 2 session(s) ago (`D_l2`)
8. Gm5 — 2 session(s) ago (`Gm5_l2`)

**LightGBM → 1d H**

1. overnight gap (today's open vs yesterday's close) — today (open-knowable) (`overnight_l0`)
2. overnight gap (today's open vs yesterday's close) — 1 session(s) ago (`overnight_l1`)
3. open-to-open % — today (open-knowable) (`J_l0`)
4. volume vs 50-day average — 1 session(s) ago (`BN_l1`)
5. volume — close-entry only at lag 0 — 1 session(s) ago (`F_l1`)
6. Mm5 — 2 session(s) ago (`Mm5_l2`)
7. Mm5 — 1 session(s) ago (`Mm5_l1`)
8. low wick vs open — 2 session(s) ago (`M_l2`)

### Head-to-head: overnight-gap baseline vs ML

Manager lock: gap-like open IC is **not** KEEP until holdout beats a simple overnight-gap recipe on the **same labels**, **same 0.15% fee**, and **same ghost bar**. Recipe = long the top 20% of overnight (C[t] vs B[t−1]). ML must beat that holdout by ≥20 bp.

| fold | label | gap holdout | best ML holdout | ML − gap | gap IC | best ML IC | lock |
|---|---|---|---|---|---|---|---|
| fold_q1 | H | — | +1.22% (n=42468) | — | -0.091 | +0.103 | **lose_to_gap** |
| fold_q1 | I | — | +2.44% (n=42468) | — | +0.361 | +0.361 | **lose_to_gap** |
| fold_q2 | H | — | +0.45% (n=43694) | — | -0.094 | +0.095 | **lose_to_gap** |
| fold_q2 | I | — | +3.25% (n=43697) | — | +0.365 | +0.361 | **lose_to_gap** |
| fold_q3 | H | — | +0.02% (n=18646) | — | -0.064 | +0.058 | **lose_to_gap** |
| fold_q3 | I | — | +8.27% (n=18277) | — | +0.426 | +0.422 | **lose_to_gap** |

### Coverage (A–JL, not a 44-col subset)

- Workbook letters A–JL: **275**.
- Reconstructed by sheet formula from Yahoo A–F: **36** named letters plus rolling AVERAGE/STDEV stand-ins for the many COUNTIF / AVERAGE / STDEV columns (EK, HF, CJ, CK, CL, AH, FR, …).
- Not in this matrix: ColorEngine fills (need the 275-col dump), weekly AP–AU spill, external VIX IY, unparsed `#REF!` / FILTER letters (FS, FU, EB, …). Those letters’ **lags of values we do have** still enter. Same-row unknown→close stays out at the open.
- Open-entry feature count: **208**. Close-entry: **247**.

### Cuts and bars

- Panel path: **batch Yahoo/rows → name-day panel → train once → time holdout → next fold**.
- Walk-forward: fold_q1 train < 2026-01-01 / hold 2026-04-01; fold_q2 train < 2026-04-01 / hold 2026-07-01; fold_q3 train < 2026-07-01 / hold tape end.
- Primary chronological train < **2026-04-01** · holdout ≥ 2026-04-01 (first pass combined Apr–Aug; folds split that window).
- Name-holdout IC is extra (existing `holdout_split.json`).
- Long top 20% of the score vs buy-everyone, Futubull 0.15% off both.
- KEEP needs holdout n≥100, ≥50 tickers, edge vs book ≥20 bp, Q1 not red, top-5 names ≤25%, July share ≤40%, both SPY tapes, no day lottery, not thin.
- Manager lock: open-entry KEEP also needs holdout edge vs the overnight-gap recipe ≥20 bp (same labels, same 0.15% fee, same ghost bar). Gap-like IC alone is not KEEP.
- Q1 cut **2026-04-01**. Half **2026-05-01**. Q3 **2026-07-01**.

### What this does not change

- Live `flatten_robust` is frozen. No card. No push.
- Standing light+O ± AH/FR stays **DEMOTE** under soft regimes (`HI_SOFT_REGIME.md`).
- Open locked-44 pair+lag stays the accepted null.
- Same-row H and I are never features.
- Finviz BLOCKED.

Board: KEEP 0 · KILL 108 · THIN 0. Family **null**.

Research only. Multi-year Yahoo / one book.

