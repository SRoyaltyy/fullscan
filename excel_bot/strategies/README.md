# Strategy Dossier — 2026-07-28 (universe 68% built, 3,603 grids)

All strategies: entry at cluster-confirmation CLOSE (leak-free), net of
0.1%/0.3% round-trip costs, causality-tested code path. Discovery = 60%
frozen tickers; holdout = sealed 40%. PASS = same sign, holdout t >= 2.

| Card | Side | Exit | Cohort | Disc n | Disc avg | Disc t | Hold n | Hold avg | Hold t | Hold win | Tickers |
|---|---|---|---|---|---|---|---|---|---|---|---|
| L1_long_green_tp8_lowvol | long_green | tp8 | volM:low(<3%) | 2635 | +1.27% | 12.53 | 1869 | +1.22% | 10.02 | +51.31% | 391 |
| L2_long_green_tp3_lowvol | long_green | tp3 | volM:low(<3%) | 2635 | +0.70% | 11.16 | 1869 | +0.65% | 8.23 | +65.06% | 391 |
| L3_long_green_hold2_midcap | long_green | hold2 | mid(1-10B):all | 4045 | +0.62% | 7.36 | 2689 | +0.55% | 5.69 | +55.89% | 438 |
| L4_long_green_hold8_bbailike | long_green | hold8 | mid:BBAI-like(hi-beta,unprof) | 526 | +2.80% | 4.05 | 268 | +1.94% | 2.35 | +46.27% | 36 |
| L5_long_green_hold2_midhibeta | long_green | hold2 | mid:beta>1.5 | 542 | +1.33% | 4.92 | 349 | +1.03% | 2.51 | +57.02% | 74 |
| S1_short_red_1day_optionable | short_red | hold1 | opt:Yes | 9815 | +0.52% | 13.15 | 6623 | +0.50% | 10.47 | +54.88% | 966 |
| S2_short_red_1day_hivol | short_red | hold1 | volM:high(>8%) | 4455 | +1.22% | 9.90 | 2901 | +1.12% | 8.44 | +57.95% | 344 |

Caveats on every card: single 6.5-month regime (deep 5y validation
pending), overlapping trades inflate t, survivorship bias (current
listings only), short-borrow fees not modeled for short_red.
L4 has only 36 holdout tickers — below the 50-ticker viability bar; treat
as exploratory. Universe build paused at 68% (quota); ~550 tickers
backfillable later.