# Soft-regime prove — standing light+O ± AH/FR

_Generated 2026-09-07 · live `flatten_robust` frozen. Yahoo/rows A–F seed only. No merge. Nine HI_KEEP_CARDS slots only. Not a forecast wire._

## Plain English

The standing light+O ± AH/FR family dies in a majority of soft-regime cells. DEMOTE the keep. The Excel A–JL / H/I mine is exhausted on this tape. Research only; live flatten_robust stays frozen.

Slots: KEEP 2 · DEMOTE 6 · REGIME-CONDITIONAL 1 of 9. Dumps **3559** (3561 rebuilt / 3603 locked; parquet through 2026-08-21). Discovery terciles of the prior-5-session mean of I: cold ≤ -0.45%, hot ≥ 0.48%. Heat is open-knowable (yesterday and older I only). Tape is SPY up / down / flat (|day| < 15 bp). Futubull 0.15% long is taken off the recipe and the buy-everyone book in the same cell, so the edge is after fees.

Same-day **H** still carries a majority of heat×tape cells on light+O (7 KEEP / 1 KILL / 1 THIN) and on light+O ∧ AH (5 / 2 / 2). Same-day **I** and 2d stacked I die in most cells — usually a **five-name ghost**, not a missing mean. FR on H is a 4–4 split (REGIME-CONDITIONAL). Cold×flat is THIN on every recipe (n=17–28) and is not papered over.

When the sheet’s recent daily-% heat is **cold** and SPY is **up**, morning five-cell light + green O still beat the book by +1.14 pp on same-day H (n=178, both discovery +2.90% (n=251) and holdout +2.42% (n=178)).

**Family verdict: DEMOTE**

### What was scored

The standing research keep is morning **five-cell light + green O**, optionally **+AH** or **+FR** (not both). Labels are Excel **H** (intraday % close vs open) and **I** (daily % close vs yesterday). Horizons that already KEEP: same-day H, same-day I, and 1–2 day stacked I. This beat does not invent features and does not remine A–JL.

Soft regimes, without a perfect green/red coder:

- **Sheet heat:** open-knowable prior 5-session mean of I (need ≥3 prints). Terciles from **discovery** names only, then applied to holdout. Cold / mid / hot.
- **SPY tape:** up / down / flat from `spy_tape.json` (|SPY day| < 15 bp is flat). `load_spy_regimes` accepts list or dict.

Each recipe is scored **inside** each heat × tape cell versus buy-everyone on the same cell, after Futubull 0.15% long. A cell KEEPs when holdout beats that book by ≥20 bp, Q1 is not red, and name / month / day lottery stay under the HI ghost bars. THIN (holdout n < 40, or few tickers/dates) is called out, not papered over. Slot KEEP needs a strict majority of the 9 cells.

### Slot majority

| recipe | label | horizon | KEEP cells | KILL | THIN | verdict |
|---|---|---|---:|---:|---:|---|
| five-cell light + green O | same-day H | 1d | 7 | 1 | 1 | **KEEP** |
| five-cell light + green O | same-day I | 1d | 3 | 5 | 1 | **DEMOTE** |
| five-cell light + green O | stacked I | 2d | 2 | 6 | 1 | **DEMOTE** |
| light+O ∧ AH≥1 | same-day H | 1d | 5 | 2 | 2 | **KEEP** |
| light+O ∧ AH≥1 | same-day I | 1d | 2 | 5 | 2 | **DEMOTE** |
| light+O ∧ AH≥1 | stacked I | 2d | 2 | 5 | 2 | **DEMOTE** |
| light+O ∧ FR≥1 | same-day H | 1d | 4 | 4 | 1 | **REGIME-CONDITIONAL** |
| light+O ∧ FR≥1 | same-day I | 1d | 2 | 6 | 1 | **DEMOTE** |
| light+O ∧ FR≥1 | stacked I | 2d | 2 | 6 | 1 | **DEMOTE** |

### Regime cells (holdout vs book)

Plain English first: a KEEP cell means the morning recipe still pays after fees **in that heat and on that SPY tape**, versus buying every name in the same bucket.

| recipe | label | hz | heat | SPY | holdout | vs book | Q1 | top-5 | July | n tickers | verdict | why |
|---|---|---|---|---|---|---|---|---|---|---:|---|---|
| five-cell light + green O | same-day H | 1d | cold | up | +2.42% (n=178) | +1.14 pp | +5.08% (n=55) | 14% | 8% | 372 | **KEEP** | — |
| five-cell light + green O | same-day H | 1d | cold | down | +2.39% (n=123) | +3.16 pp | +2.64% (n=125) | 21% | 11% | 269 | **KEEP** | — |
| five-cell light + green O | same-day H | 1d | cold | flat | +3.67% (n=28) | +4.03 pp | +2.56% (n=17) | 39% | 6% | 80 | **THIN** | thin,lottery_day,q1_thin,ticker_ghost |
| five-cell light + green O | same-day H | 1d | mid | up | +1.56% (n=362) | +1.41 pp | +2.28% (n=74) | 9% | 13% | 743 | **KEEP** | — |
| five-cell light + green O | same-day H | 1d | mid | down | +1.21% (n=254) | +1.69 pp | +2.06% (n=207) | 16% | 0% | 564 | **KEEP** | — |
| five-cell light + green O | same-day H | 1d | mid | flat | +1.26% (n=101) | +1.35 pp | +1.18% (n=53) | 25% | 7% | 267 | **KILL** | lottery_day |
| five-cell light + green O | same-day H | 1d | hot | up | +2.30% (n=1428) | +1.98 pp | +3.90% (n=287) | 11% | 8% | 2101 | **KEEP** | — |
| five-cell light + green O | same-day H | 1d | hot | down | +1.72% (n=1067) | +1.65 pp | +2.13% (n=760) | 11% | 9% | 1802 | **KEEP** | — |
| five-cell light + green O | same-day H | 1d | hot | flat | +1.76% (n=424) | +1.87 pp | +4.58% (n=126) | 19% | 16% | 953 | **KEEP** | — |
| five-cell light + green O | same-day I | 1d | cold | up | +3.00% (n=178) | +1.52 pp | +5.55% (n=55) | 19% | 7% | 372 | **KEEP** | — |
| five-cell light + green O | same-day I | 1d | cold | down | +2.02% (n=123) | +2.08 pp | +1.56% (n=125) | 32% | 19% | 269 | **KILL** | ticker_ghost |
| five-cell light + green O | same-day I | 1d | cold | flat | +2.42% (n=28) | +2.19 pp | +1.53% (n=17) | 47% | 12% | 80 | **THIN** | thin,q1_thin,ticker_ghost |
| five-cell light + green O | same-day I | 1d | mid | up | +1.91% (n=362) | +1.51 pp | +2.32% (n=74) | 10% | 13% | 743 | **KEEP** | — |
| five-cell light + green O | same-day I | 1d | mid | down | +0.76% (n=254) | +1.44 pp | +1.10% (n=207) | 29% | 0% | 564 | **KILL** | ticker_ghost |
| five-cell light + green O | same-day I | 1d | mid | flat | +0.97% (n=101) | +1.09 pp | +1.55% (n=53) | 30% | 10% | 267 | **KILL** | ticker_ghost |
| five-cell light + green O | same-day I | 1d | hot | up | +2.46% (n=1428) | +1.93 pp | +4.17% (n=287) | 14% | 7% | 2101 | **KEEP** | — |
| five-cell light + green O | same-day I | 1d | hot | down | +1.02% (n=1067) | +2.00 pp | +1.37% (n=760) | 31% | 6% | 1802 | **KILL** | ticker_ghost |
| five-cell light + green O | same-day I | 1d | hot | flat | +1.34% (n=424) | +1.47 pp | +4.37% (n=126) | 33% | 23% | 953 | **KILL** | ticker_ghost |
| five-cell light + green O | stacked I | 2d | cold | up | +1.12% (n=178) | -0.65 pp | +4.13% (n=55) | 30% | 1% | 372 | **KILL** | ticker_ghost,no_edge_vs_book |
| five-cell light + green O | stacked I | 2d | cold | down | +3.59% (n=123) | +2.58 pp | +1.09% (n=125) | 58% | 17% | 269 | **KILL** | ticker_ghost |
| five-cell light + green O | stacked I | 2d | cold | flat | +3.85% (n=28) | +3.56 pp | +1.93% (n=17) | 54% | 14% | 80 | **THIN** | thin,lottery_day,q1_thin,ticker_ghost |
| five-cell light + green O | stacked I | 2d | mid | up | +2.18% (n=362) | +1.80 pp | +2.12% (n=74) | 13% | 13% | 743 | **KEEP** | — |
| five-cell light + green O | stacked I | 2d | mid | down | +1.39% (n=254) | +1.98 pp | +0.76% (n=207) | 36% | 0% | 564 | **KILL** | ticker_ghost |
| five-cell light + green O | stacked I | 2d | mid | flat | +1.34% (n=101) | +1.34 pp | -0.29% (n=53) | 37% | 12% | 267 | **KILL** | lottery_day,q1_sign,ticker_ghost |
| five-cell light + green O | stacked I | 2d | hot | up | +2.39% (n=1428) | +1.89 pp | +3.75% (n=287) | 9% | 5% | 2101 | **KEEP** | — |
| five-cell light + green O | stacked I | 2d | hot | down | +0.61% (n=1067) | +1.29 pp | +1.67% (n=760) | 26% | 0% | 1802 | **KILL** | ticker_ghost |
| five-cell light + green O | stacked I | 2d | hot | flat | +1.39% (n=424) | +1.41 pp | +1.14% (n=126) | 39% | 23% | 953 | **KILL** | ticker_ghost |
| light+O ∧ AH≥1 | same-day H | 1d | cold | up | +2.40% (n=131) | +1.13 pp | +5.65% (n=42) | 17% | 9% | 280 | **KEEP** | — |
| light+O ∧ AH≥1 | same-day H | 1d | cold | down | +1.89% (n=87) | +2.67 pp | +3.07% (n=83) | 23% | 12% | 182 | **KEEP** | — |
| light+O ∧ AH≥1 | same-day H | 1d | cold | flat | +4.33% (n=17) | +4.69 pp | +2.86% (n=8) | 54% | 3% | 51 | **THIN** | thin,lottery_day,q1_thin,ticker_ghost |
| light+O ∧ AH≥1 | same-day H | 1d | mid | up | +2.86% (n=113) | +2.72 pp | +4.49% (n=22) | 16% | 17% | 241 | **KEEP** | — |
| light+O ∧ AH≥1 | same-day H | 1d | mid | down | +2.68% (n=66) | +3.16 pp | +3.36% (n=69) | 29% | 0% | 171 | **KILL** | ticker_ghost |
| light+O ∧ AH≥1 | same-day H | 1d | mid | flat | +4.40% (n=27) | +4.48 pp | +3.85% (n=13) | 34% | 13% | 73 | **THIN** | thin,lottery_day,q1_thin,ticker_ghost |
| light+O ∧ AH≥1 | same-day H | 1d | hot | up | +3.88% (n=436) | +3.56 pp | +4.71% (n=98) | 21% | 8% | 797 | **KEEP** | — |
| light+O ∧ AH≥1 | same-day H | 1d | hot | down | +2.97% (n=315) | +2.90 pp | +3.79% (n=217) | 15% | 8% | 638 | **KEEP** | — |
| light+O ∧ AH≥1 | same-day H | 1d | hot | flat | +3.34% (n=90) | +3.45 pp | +11.45% (n=31) | 34% | 25% | 249 | **KILL** | ticker_ghost |
| light+O ∧ AH≥1 | same-day I | 1d | cold | up | +3.04% (n=131) | +1.55 pp | +6.60% (n=42) | 23% | 7% | 280 | **KEEP** | — |
| light+O ∧ AH≥1 | same-day I | 1d | cold | down | +1.49% (n=87) | +1.56 pp | +1.82% (n=83) | 41% | 21% | 182 | **KILL** | ticker_ghost |
| light+O ∧ AH≥1 | same-day I | 1d | cold | flat | +2.51% (n=17) | +2.28 pp | +1.07% (n=8) | 63% | 8% | 51 | **THIN** | thin,q1_thin,ticker_ghost |
| light+O ∧ AH≥1 | same-day I | 1d | mid | up | +3.35% (n=113) | +2.94 pp | +3.90% (n=22) | 20% | 15% | 241 | **KEEP** | — |
| light+O ∧ AH≥1 | same-day I | 1d | mid | down | +1.51% (n=66) | +2.19 pp | +2.04% (n=69) | 54% | 0% | 171 | **KILL** | ticker_ghost |
| light+O ∧ AH≥1 | same-day I | 1d | mid | flat | +3.89% (n=27) | +4.01 pp | +4.06% (n=13) | 38% | 12% | 73 | **THIN** | thin,lottery_day,q1_thin,ticker_ghost |
| light+O ∧ AH≥1 | same-day I | 1d | hot | up | +3.80% (n=436) | +3.27 pp | +5.04% (n=98) | 27% | 6% | 797 | **KILL** | ticker_ghost |
| light+O ∧ AH≥1 | same-day I | 1d | hot | down | +1.59% (n=315) | +2.57 pp | +3.20% (n=217) | 35% | 0% | 638 | **KILL** | ticker_ghost |
| light+O ∧ AH≥1 | same-day I | 1d | hot | flat | +2.83% (n=90) | +2.97 pp | +13.32% (n=31) | 52% | 37% | 249 | **KILL** | ticker_ghost |
| light+O ∧ AH≥1 | stacked I | 2d | cold | up | +1.01% (n=131) | -0.76 pp | +5.74% (n=42) | 35% | 3% | 280 | **KILL** | ticker_ghost,no_edge_vs_book |
| light+O ∧ AH≥1 | stacked I | 2d | cold | down | +3.70% (n=87) | +2.69 pp | +1.27% (n=83) | 81% | 17% | 182 | **KILL** | ticker_ghost |
| light+O ∧ AH≥1 | stacked I | 2d | cold | flat | +5.58% (n=17) | +5.28 pp | +2.16% (n=8) | 62% | 6% | 51 | **THIN** | thin,lottery_day,q1_thin,ticker_ghost |
| light+O ∧ AH≥1 | stacked I | 2d | mid | up | +3.90% (n=113) | +3.52 pp | +4.14% (n=22) | 24% | 14% | 241 | **KEEP** | — |
| light+O ∧ AH≥1 | stacked I | 2d | mid | down | +3.04% (n=66) | +3.62 pp | +0.66% (n=69) | 85% | 0% | 171 | **KILL** | lottery_day,ticker_ghost |
| light+O ∧ AH≥1 | stacked I | 2d | mid | flat | +5.13% (n=27) | +5.13 pp | -1.06% (n=13) | 59% | 18% | 73 | **THIN** | thin,q1_thin,ticker_ghost |
| light+O ∧ AH≥1 | stacked I | 2d | hot | up | +3.44% (n=436) | +2.94 pp | +4.09% (n=98) | 20% | 3% | 797 | **KEEP** | — |
| light+O ∧ AH≥1 | stacked I | 2d | hot | down | +0.61% (n=315) | +1.29 pp | +3.00% (n=217) | 46% | 0% | 638 | **KILL** | ticker_ghost |
| light+O ∧ AH≥1 | stacked I | 2d | hot | flat | +1.85% (n=90) | +1.86 pp | +8.01% (n=31) | 64% | 42% | 249 | **KILL** | lottery_day,month_lottery,ticker_ghost |
| light+O ∧ FR≥1 | same-day H | 1d | cold | up | +2.26% (n=104) | +0.99 pp | +5.70% (n=31) | 23% | 6% | 221 | **KEEP** | — |
| light+O ∧ FR≥1 | same-day H | 1d | cold | down | +1.96% (n=73) | +2.73 pp | +2.27% (n=73) | 35% | 15% | 161 | **KILL** | ticker_ghost |
| light+O ∧ FR≥1 | same-day H | 1d | cold | flat | +3.93% (n=15) | +4.29 pp | +1.08% (n=10) | 54% | 3% | 50 | **THIN** | thin,lottery_day,q1_thin,ticker_ghost |
| light+O ∧ FR≥1 | same-day H | 1d | mid | up | +1.99% (n=179) | +1.84 pp | +2.72% (n=37) | 14% | 14% | 361 | **KEEP** | — |
| light+O ∧ FR≥1 | same-day H | 1d | mid | down | +1.57% (n=115) | +2.06 pp | +2.09% (n=114) | 26% | 0% | 262 | **KILL** | ticker_ghost |
| light+O ∧ FR≥1 | same-day H | 1d | mid | flat | +1.53% (n=54) | +1.62 pp | +1.28% (n=26) | 40% | 7% | 125 | **KILL** | lottery_day,ticker_ghost |
| light+O ∧ FR≥1 | same-day H | 1d | hot | up | +2.65% (n=813) | +2.34 pp | +4.42% (n=170) | 16% | 7% | 1343 | **KEEP** | — |
| light+O ∧ FR≥1 | same-day H | 1d | hot | down | +2.11% (n=618) | +2.05 pp | +2.25% (n=449) | 16% | 8% | 1115 | **KEEP** | — |
| light+O ∧ FR≥1 | same-day H | 1d | hot | flat | +2.05% (n=229) | +2.16 pp | +6.18% (n=71) | 26% | 20% | 533 | **KILL** | ticker_ghost |
| light+O ∧ FR≥1 | same-day I | 1d | cold | up | +2.96% (n=104) | +1.48 pp | +6.64% (n=31) | 28% | 5% | 221 | **KILL** | ticker_ghost |
| light+O ∧ FR≥1 | same-day I | 1d | cold | down | +1.78% (n=73) | +1.84 pp | +1.17% (n=73) | 51% | 30% | 161 | **KILL** | ticker_ghost |
| light+O ∧ FR≥1 | same-day I | 1d | cold | flat | +2.48% (n=15) | +2.25 pp | +0.22% (n=10) | 60% | 13% | 50 | **THIN** | thin,lottery_day,q1_thin,ticker_ghost |
| light+O ∧ FR≥1 | same-day I | 1d | mid | up | +2.38% (n=179) | +1.98 pp | +2.70% (n=37) | 17% | 13% | 361 | **KEEP** | — |
| light+O ∧ FR≥1 | same-day I | 1d | mid | down | +0.63% (n=115) | +1.31 pp | +0.99% (n=114) | 72% | 1% | 262 | **KILL** | ticker_ghost |
| light+O ∧ FR≥1 | same-day I | 1d | mid | flat | +1.07% (n=54) | +1.18 pp | +1.87% (n=26) | 45% | 3% | 125 | **KILL** | ticker_ghost |
| light+O ∧ FR≥1 | same-day I | 1d | hot | up | +2.63% (n=813) | +2.09 pp | +4.69% (n=170) | 22% | 5% | 1343 | **KEEP** | — |
| light+O ∧ FR≥1 | same-day I | 1d | hot | down | +1.24% (n=618) | +2.22 pp | +1.52% (n=449) | 45% | 5% | 1115 | **KILL** | ticker_ghost |
| light+O ∧ FR≥1 | same-day I | 1d | hot | flat | +1.51% (n=229) | +1.65 pp | +6.29% (n=71) | 45% | 29% | 533 | **KILL** | ticker_ghost |
| light+O ∧ FR≥1 | stacked I | 2d | cold | up | +0.91% (n=104) | -0.86 pp | +5.60% (n=31) | 37% | 0% | 221 | **KILL** | ticker_ghost,no_edge_vs_book |
| light+O ∧ FR≥1 | stacked I | 2d | cold | down | +3.51% (n=73) | +2.49 pp | +1.42% (n=73) | 79% | 17% | 161 | **KILL** | ticker_ghost |
| light+O ∧ FR≥1 | stacked I | 2d | cold | flat | +5.27% (n=15) | +4.97 pp | +1.38% (n=10) | 59% | 23% | 50 | **THIN** | thin,lottery_day,q1_thin,ticker_ghost |
| light+O ∧ FR≥1 | stacked I | 2d | mid | up | +2.50% (n=179) | +2.12 pp | +2.32% (n=37) | 22% | 13% | 361 | **KEEP** | — |
| light+O ∧ FR≥1 | stacked I | 2d | mid | down | +2.11% (n=115) | +2.70 pp | +1.44% (n=114) | 73% | 0% | 262 | **KILL** | ticker_ghost |
| light+O ∧ FR≥1 | stacked I | 2d | mid | flat | +1.00% (n=54) | +1.00 pp | +0.33% (n=26) | 52% | 7% | 125 | **KILL** | ticker_ghost |
| light+O ∧ FR≥1 | stacked I | 2d | hot | up | +2.39% (n=813) | +1.90 pp | +4.42% (n=170) | 15% | 2% | 1343 | **KEEP** | — |
| light+O ∧ FR≥1 | stacked I | 2d | hot | down | +0.78% (n=618) | +1.46 pp | +1.60% (n=449) | 38% | 0% | 1115 | **KILL** | ticker_ghost |
| light+O ∧ FR≥1 | stacked I | 2d | hot | flat | +1.20% (n=229) | +1.22 pp | +2.31% (n=71) | 62% | 35% | 533 | **KILL** | lottery_day,ticker_ghost |

### Cuts and bars

- Heat window: prior 5 I prints, min 3.
- Discovery terciles: cold ≤ -0.454% · hot ≥ 0.480%.
- SPY flat: |day| < 15 bp.
- Cell THIN: holdout n < 40 or tickers < 15 or dates < 8.
- KEEP cell: holdout > 0, edge vs same-cell book ≥ 20 bp, Q1 not red (n≥20), top-5 names ≤ 25% of P&L, July share of winning-month P&L ≤ 40%, fattest day ≤ 25%.
- Futubull 0.15% long off recipe and book. Same fee model as the open harden; HI_HORIZON published raw H/I — the vs-book edge is unchanged by a constant fee.
- Q1 cut **2026-04-01**. Half cut **2026-05-01**. Q3 cut **2026-07-01**.

### What this does not change

- Live `flatten_robust` is frozen. No card. No push.
- AH∧FR stacked stays KILL (name ghost / hold1-without-hold2).
- 3d / 1w / 2w stay KILL. F-green is not a lagged forecast.
- Open locked-44 pair+lag stays the accepted null.
- Continuous H/I from Yahoo A–F stay the clean null for number cards.
- No new number-mine rabbit holes.

Dumps **3559**. Heat cuts from **281732** discovery trailing means. Family **DEMOTE**.

Research only. One 2026 regime.

