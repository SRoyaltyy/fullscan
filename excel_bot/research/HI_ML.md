# Full-sheet ML → H / I (multi-year, clock-clean)

_Generated 2026-09-07 · live `flatten_robust` frozen. Yahoo/rows A–F seed the whole A–JL DAG. No Excel STOCKHISTORY cache. Not a forecast wire._

## Plain English

Full-sheet ML on clock-clean A–JL values (Yahoo A–F DAG, as far back as the book / parquet go) does not beat the overnight gap. Clean **null**. Predicting I at the open is gap algebra: I = overnight + scaled H, and overnight is knowable at 9:30 (C today vs B yesterday). Ridge and LightGBM IC on I match the gap (or lose). Predicting H (the rest of the day) dies on both tapes and a five-name ghost. Hand gates did not hide a nonlinear join. Research only; live flatten_robust stays frozen.

**Panel path (not per-ticker chat):** batch Yahoo/rows → name-day panel → train once → time holdout → next fold. Panel: **3548** names · **762084** name-days · 2025-09-23 → 2026-08-10. Open-entry features **208** (locked 44 + open-derived + lags of every reconstructed letter, never same-row H/I). Walk-forward folds fold_q1, fold_q2, fold_q3; primary cut **2026-04-01**. Futubull 0.15% long off the top-20% recipe and the buy-everyone book. cut ridge → H 1d holdout +0.36% (n=61963) vs book +0.44 pp IC ρ=+0.085 / r=+0.045 (**KILL**); cut lgb → H 1d holdout +0.41% (n=61964) vs book +0.49 pp IC ρ=+0.098 / r=+0.064 (**KILL**); cut ridge → I 1d holdout +4.47% (n=61963) vs book +4.07 pp IC ρ=+0.359 / r=+0.081 (**KILL**); cut lgb → I 1d holdout +4.72% (n=62001) vs book +4.33 pp IC ρ=+0.380 / r=+0.123 (**KILL**).

**Family verdict: null**

### Path (not per-ticker chat)

**batch Yahoo/rows → name-day panel → train once → time holdout → next fold**

One Yahoo A–F rebuild builds the **name-day panel**. Models train once across that panel, then the next expanding-window fold re-uses the same matrix. Iterative = rebuild → train → holdout → next fold. There is no interactive one-stock Excel loop and no per-ticker chat fit.

### Walk-forward folds

| fold | train < | holdout | model | label | holdout pnl | vs book | IC | gap IC | verdict | why |
|---|---|---|---|---|---|---|---|---|---|---|
| *(folds not in this file — first pass is the combined Apr–Aug holdout below)* | | | | | | | | | | |

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
| lgb | H | +0.41% (n=61964) | +0.49 pp | ρ=+0.098 / r=+0.064 | ρ=-0.085 / r=-0.003 | +0.89% (n=90454) | 37% | 0% | +1.45% (n=58022) / -0.04% (n=68962) | **KILL** | spy_regime,month_split,ticker_ghost |
| linear | H | +0.32% (n=61963) | +0.41 pp | ρ=+0.080 / r=+0.047 | ρ=-0.085 / r=-0.003 | +0.77% (n=90454) | 44% | 0% | +1.27% (n=62186) / -0.12% (n=64045) | **KILL** | disc_t,spy_regime,month_split,ticker_ghost |
| ridge | H | +0.36% (n=61963) | +0.44 pp | ρ=+0.085 / r=+0.045 | ρ=-0.085 / r=-0.003 | +0.78% (n=90454) | 42% | 0% | +1.29% (n=61845) / -0.10% (n=64295) | **KILL** | disc_t,spy_regime,month_split,ticker_ghost |
| lgb | I | +4.72% (n=62001) | +4.33 pp | ρ=+0.380 / r=+0.123 | ρ=+0.383 / r=+0.984 | +2.15% (n=90460) | 16% | 14% | +3.68% (n=91168) / +2.20% (n=41772) | **KILL** | gap_algebra |
| linear | I | +4.46% (n=61963) | +4.06 pp | ρ=+0.359 / r=+0.081 | ρ=+0.383 / r=+0.984 | +1.95% (n=90454) | 17% | 14% | +3.48% (n=90663) / +1.93% (n=42219) | **KILL** | gap_algebra |
| ridge | I | +4.47% (n=61963) | +4.07 pp | ρ=+0.359 / r=+0.081 | ρ=+0.383 / r=+0.984 | +1.95% (n=90454) | 17% | 14% | +3.49% (n=90715) / +1.94% (n=42183) | **KILL** | gap_algebra |
| lgb | I_sum | +4.72% (n=62001) | +4.33 pp | ρ=+0.380 / r=+0.123 | ρ=+0.383 / r=+0.984 | +2.15% (n=90460) | 16% | 14% | +3.68% (n=91168) / +2.20% (n=41772) | **KILL** | gap_algebra |
| linear | I_sum | +4.46% (n=61963) | +4.06 pp | ρ=+0.359 / r=+0.081 | ρ=+0.383 / r=+0.984 | +1.95% (n=90454) | 17% | 14% | +3.48% (n=90663) / +1.93% (n=42219) | **KILL** | gap_algebra |
| ridge | I_sum | +4.47% (n=61963) | +4.07 pp | ρ=+0.359 / r=+0.081 | ρ=+0.383 / r=+0.984 | +1.95% (n=90454) | 17% | 14% | +3.49% (n=90715) / +1.94% (n=42183) | **KILL** | gap_algebra |

Code names (after the English): `ml_open_lgb_H_1d`, `ml_open_linear_H_1d`, `ml_open_ridge_H_1d`, `ml_open_lgb_I_1d`, `ml_open_linear_I_1d`, `ml_open_ridge_I_1d`, `ml_open_lgb_I_sum_1d`, `ml_open_linear_I_sum_1d`.

### Horizons (open ridge + LightGBM)

| model | label | 1d | 2d | 3d | 1w | 2w |
|---|---|---|---|---|---|---|
| ridge | H | **KILL** +0.36% (n=61963) | **KILL** -0.02% (n=61963) | **KILL** -0.06% (n=61963) | **KILL** -0.03% (n=61963) | **KILL** -0.16% (n=61963) |
| ridge | I | **KILL** +4.47% (n=61963) | **KILL** +0.68% (n=61963) | **KILL** +0.95% (n=61963) | **KILL** +0.71% (n=61963) | **KILL** +0.84% (n=61963) |
| ridge | I_sum | **KILL** +4.47% (n=61963) | **KILL** +5.02% (n=61963) | **KILL** +5.04% (n=61963) | **KILL** +5.77% (n=61963) | **KILL** +11.28% (n=61963) |
| lgb | H | **KILL** +0.41% (n=61964) | **KILL** -0.03% (n=61963) | **KILL** -0.10% (n=175598) | **KILL** -0.09% (n=68643) | **KILL** -0.14% (n=109969) |
| lgb | I | **KILL** +4.72% (n=62001) | **KILL** +1.47% (n=62543) | **KILL** +1.23% (n=76968) | **KILL** +1.41% (n=63102) | **KILL** +1.27% (n=61964) |
| lgb | I_sum | **KILL** +4.72% (n=62001) | **KILL** +5.34% (n=61994) | **KILL** +5.47% (n=62370) | **KILL** +7.70% (n=62685) | **KILL** +17.57% (n=68023) |

### Close-entry 1d (same-row close cols OK; still never H/I)

| model | label | holdout | vs book | IC | verdict | why |
|---|---|---|---|---|---|---|
| linear | H | +4.74% (n=61963) | +4.82 pp | ρ=+0.831 / r=+0.467 | **KILL** | same_print_close |
| ridge | H | +4.74% (n=61963) | +4.82 pp | ρ=+0.831 / r=+0.466 | **KILL** | same_print_close |
| lgb | H | +4.75% (n=61963) | +4.84 pp | ρ=+0.828 / r=+0.498 | **KILL** | same_print_close |
| linear | I | +6.81% (n=61963) | +6.42 pp | ρ=+0.828 / r=+0.084 | **KILL** | same_print_close |
| ridge | I | +6.82% (n=61963) | +6.42 pp | ρ=+0.828 / r=+0.084 | **KILL** | same_print_close |
| lgb | I | +7.13% (n=61963) | +6.73 pp | ρ=+0.840 / r=+0.096 | **KILL** | same_print_close |

### Soft regimes (open ridge 1d, holdout)

Heat is the open-knowable prior-5 mean of I (terciles from **train** dates). Tape is SPY up / down / flat.

| model | label | heat cold | mid | hot | spy↑ | spy↓ | spy flat |
|---|---|---|---|---|---|---|---|
| ridge | H | +0.73% (n=55222) | +0.25% (n=41084) | +0.75% (n=56111) | +1.29% (n=61845) | -0.10% (n=64295) | +0.74% (n=26277) |
| ridge | I | +4.28% (n=60412) | +1.80% (n=35872) | +2.32% (n=56133) | +3.49% (n=90715) | +1.94% (n=42183) | +2.85% (n=19519) |
| ridge | I_sum | +4.28% (n=60412) | +1.80% (n=35872) | +2.32% (n=56133) | +3.49% (n=90715) | +1.94% (n=42183) | +2.85% (n=19519) |

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
3. intraday % (close vs open) — label; lags are yesterday+ — 1 session(s) ago (`H_l1`)
4. daily % (close vs yesterday) — label; lags are yesterday+ — 1 session(s) ago (`I_l1`)
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
4. volume — close-entry only at lag 0 — 1 session(s) ago (`F_l1`)
5. volume vs 50-day average — 1 session(s) ago (`BN_l1`)
6. Mm5 — 1 session(s) ago (`Mm5_l1`)
7. overnight gap (today's open vs yesterday's close) — 5 session(s) ago (`overnight_l5`)
8. range — 2 session(s) ago (`range_l2`)

### Gap algebra vs ML

At the open we already know the overnight gap (C[t] vs B[t−1]). Excel I is overnight plus a scaled H. If ML IC on I is no better than overnight→I, the sheet is a DAG on A–F and there is nothing past gap algebra. That is a clean null.

| label | overnight IC | best open ML IC | ML − gap |
|---|---|---|---|
| H | -0.085 | +0.098 | +0.183 |
| I | +0.383 | +0.380 | -0.003 |

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
- Q1 cut **2026-04-01**. Half **2026-05-01**. Q3 **2026-07-01**.

### What this does not change

- Live `flatten_robust` is frozen. No card. No push.
- Standing light+O ± AH/FR stays **DEMOTE** under soft regimes (`HI_SOFT_REGIME.md`).
- Open locked-44 pair+lag stays the accepted null.
- Same-row H and I are never features.
- Finviz BLOCKED.

Board: KEEP 0 · KILL 90 · THIN 0. Family **null**.

Research only. Multi-year Yahoo / one book.

