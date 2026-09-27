# hot_n4_clean_v4 scores

Primary path: Futubull fees plus 0.5% per side, keep-held, 1% dollar-volume cap, fills at the open.
0% and 1% reprice those shares. Flat 15bp is 7.5bp per side on the actual open and no slip.
The day ledger is not rewritten by this file.

## Returns

| recipe | window | 0% slip | 0.5% slip | 1% slip | flat 15bp | trades | buys | win rate | Wilson low | Wilson high | unfilled |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `union_hot_n4_h1__w0` | before | 22.88% | 9.46% | -3.96% | 32.59% | 53 | 57 | 43.40% | 30.95% | 56.73% | 0 |
| `union_hot_n4_h1__w0` | after | 24.59% | 21.55% | 17.67% | 26.27% | 27 | 27 | 48.15% | 30.74% | 66.01% | 0 |
| `union_hot_n4_h1__w0` | overall | 53.10% | 33.05% | 13.01% | 67.42% | 80 | 84 | 45.00% | 34.58% | 55.88% | 0 |
| `union_hot_n4_holdup__w0` | before | 19.66% | 9.00% | -1.66% | 27.85% | 48 | 52 | 47.92% | 34.47% | 61.67% | 4 |
| `union_hot_n4_holdup__w0` | after | 21.18% | 18.48% | 15.20% | 22.70% | 23 | 23 | 52.17% | 32.96% | 70.76% | 2 |
| `union_hot_n4_holdup__w0` | overall | 45.00% | 29.14% | 13.28% | 56.87% | 71 | 75 | 49.30% | 38.00% | 60.66% | 6 |

## Without the largest closed trades

The rerun makes those tickers ineligible from the first session. The original day cards stay as written.

| recipe | window | removed | primary | flat 15bp |
| --- | --- | --- | ---: | ---: |
| `union_hot_n4_h1__w0` | before | top 1: USDE | -1.08% | 21.55% |
| `union_hot_n4_h1__w0` | before | top 3: USDE, CYPH, CAPR | -13.75% | 8.02% |
| `union_hot_n4_h1__w0` | before | top 5: USDE, CYPH, CAPR, ASST, EROC | -24.07% | -3.30% |
| `union_hot_n4_h1__w0` | after | top 1: INDP | 11.37% | 18.01% |
| `union_hot_n4_h1__w0` | after | top 3: INDP, CMRC, CYPH | 9.63% | 16.95% |
| `union_hot_n4_h1__w0` | after | top 5: INDP, CMRC, CYPH, BNC, HLP | 5.69% | 13.35% |
| `union_hot_n4_h1__w0` | overall | top 1: INDP | 6.01% | 39.56% |
| `union_hot_n4_h1__w0` | overall | top 3: INDP, CYPH, USDE | -11.25% | 20.82% |
| `union_hot_n4_h1__w0` | overall | top 5: INDP, CYPH, USDE, CAPR, ASST | -24.16% | 5.64% |
| `union_hot_n4_holdup__w0` | before | top 1: CYPH | 3.71% | 21.87% |
| `union_hot_n4_holdup__w0` | before | top 3: CYPH, PURR, DFDV | -9.36% | 7.75% |
| `union_hot_n4_holdup__w0` | before | top 5: CYPH, PURR, DFDV, USDE, ASST | -15.40% | 1.35% |
| `union_hot_n4_holdup__w0` | after | top 1: INDP | 15.26% | 19.72% |
| `union_hot_n4_holdup__w0` | after | top 3: INDP, SDGR, CMRC | 15.80% | 20.41% |
| `union_hot_n4_holdup__w0` | after | top 5: INDP, SDGR, CMRC, CYPH, BNC | 21.43% | 24.08% |
| `union_hot_n4_holdup__w0` | overall | top 1: INDP | 9.25% | 35.97% |
| `union_hot_n4_holdup__w0` | overall | top 3: INDP, CYPH, SDGR | 5.76% | 31.03% |
| `union_hot_n4_holdup__w0` | overall | top 5: INDP, CYPH, SDGR, PURR, DFDV | -7.59% | 15.76% |

## Best-stock share, up days, joint

Only the after window can reject. A before or overall joint under 0.5 is not a rejection.

| recipe | window | best | share | up days | winning-day share | trades per session | joint | after reject | rule 21 |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- | --- |
| `union_hot_n4_h1__w0` | before | USDE | -146.26% | 10 of 21 | 47.62% | 2.524 | 43.40% | n/a | not met |
| `union_hot_n4_h1__w0` | after | INDP | 250.19% | 4 of 10 | 40.00% | 2.700 | 40.00% | no | not met |
| `union_hot_n4_h1__w0` | overall | INDP | 753.79% | 14 of 31 | 45.16% | 2.581 | 45.00% | n/a | not met |
| `union_hot_n4_holdup__w0` | before | CYPH | -119.02% | 11 of 21 | 52.38% | 2.286 | 47.92% | n/a | not met |
| `union_hot_n4_holdup__w0` | after | INDP | 128.99% | 4 of 9 | 44.44% | 2.300 | 44.44% | no | not met |
| `union_hot_n4_holdup__w0` | overall | INDP | 204.99% | 15 of 30 | 50.00% | 2.290 | 49.30% | n/a | not met |

## Positive from X of Y

- `union_hot_n4_h1__w0`: 2 of 3 (Mondays through 2026-09-11, closed-trade P&L).
- `union_hot_n4_holdup__w0`: 1 of 3 (Mondays through 2026-09-11, closed-trade P&L).

## IWM and RANDOM4

IWM is bought at the first open of that window after the Futubull entry fee only, with no 0.5% slip and no dollar cap. A session with no pinned IWM bar leaves the mark at the last stored close. The after-window figure is a fresh buy, not the continuous book. RANDOM4 is 1,000 draws, seed 20260813 plus the draw index, 4 names from that morning's candidate list, on the continuous book.

| recipe | window | IWM | IWM 15bp | RANDOM4 | RANDOM4 15bp |
| --- | --- | ---: | ---: | ---: | ---: |
| `union_hot_n4_h1__w0` | before | -3.02% | -3.01% | -23.77% | 3.00% |
| `union_hot_n4_h1__w0` | after | 0.00% | 0.00% | -12.77% | 1.09% |
| `union_hot_n4_h1__w0` | overall | -3.02% | -3.01% | -33.39% | 4.16% |
| `union_hot_n4_holdup__w0` | before | -3.02% | -3.01% | -12.06% | 9.89% |
| `union_hot_n4_holdup__w0` | after | 0.00% | 0.00% | -3.17% | 4.60% |
| `union_hot_n4_holdup__w0` | overall | -3.02% | -3.01% | -14.91% | 14.84% |

## Names removed before ranking

| session | n_excluded | tickers |
| --- | ---: | --- |
| 2026-08-13 | 0 |  |
| 2026-08-14 | 0 |  |
| 2026-08-17 | 0 |  |
| 2026-08-18 | 0 |  |
| 2026-08-19 | 1 | XHG |
| 2026-08-20 | 0 |  |
| 2026-08-21 | 1 | XHG |
| 2026-08-24 | 1 | XHG |
| 2026-08-25 | 1 | XHG |
| 2026-08-26 | 1 | XHG |
| 2026-08-27 | 1 | XHG |
| 2026-08-28 | 0 |  |
| 2026-08-31 | 0 |  |
| 2026-09-01 | 0 |  |
| 2026-09-02 | 0 |  |
| 2026-09-03 | 2 | JLHL, SLBT |
| 2026-09-04 | 0 |  |
| 2026-09-08 | 0 |  |
| 2026-09-09 | 0 |  |
| 2026-09-10 | 1 | XHG |
| 2026-09-11 | 1 | SLBT |
| 2026-09-14 | 0 |  |
| 2026-09-15 | 0 |  |
| 2026-09-16 | 0 |  |
| 2026-09-17 | 0 |  |
| 2026-09-18 | 0 |  |
| 2026-09-21 | 0 |  |
| 2026-09-22 | 0 |  |
| 2026-09-23 | 0 |  |
| 2026-09-24 | 0 |  |
| 2026-09-25 | 0 |  |

Exclusions match `EXCLUSIONS_PLAN.csv`.


## Published v4, same recipe id

Copied from `research/factor_mine_recipe_search_v4/returns/RESULTS.json` and `FORWARD.json`. A blank in those files is `not published` here. Not recomputed.

| recipe | window | v4 compound | v4 flat 15bp | v4 ex-best | v4 win rate | v4 up share | v4 trades | v4 RANDOM4 | v4 IWM | v4 joint | v4 status |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `union_hot_n4_h1__w0` | before | 38.91% | 48.77% | 22.09% | 52.46% | 66.67% | 61 | -10.98% | -4.87% | 52.46% | not published |
| `union_hot_n4_h1__w0` | after | 36.02% | 37.23% | 46.72% | 53.12% | 50.00% | 32 | not published | not published | 50.00% | not_rejected |
| `union_hot_n4_h1__w0` | slip 0% / 1%, Wilson, dollar-volume cap, skipped count, positive-from-X-of-Y, ex top 3, ex top 5 | not published | not published | not published | not published | not published | not published | not published | not published | not published | not published |
| `union_hot_n4_holdup__w0` | before | 54.62% | 63.61% | 35.35% | 55.36% | 65.00% | 56 | -6.37% | -4.87% | 55.36% | not published |
| `union_hot_n4_holdup__w0` | after | 38.82% | 39.49% | 72.01% | 53.33% | 60.00% | 30 | not published | not published | 53.33% | not_rejected |
| `union_hot_n4_holdup__w0` | slip 0% / 1%, Wilson, dollar-volume cap, skipped count, positive-from-X-of-Y, ex top 3, ex top 5 | not published | not published | not published | not published | not published | not published | not published | not published | not published | not published |

## Cap studies

The cap studies do not publish `union_hot_n4_h1__w0` or `union_hot_n4_holdup__w0`.
The rows below are the published width-4 cap-none ids, copied with that file's own column names.

### concentration_cap_v1

**Tune, 2026-08-13 through 2026-09-11**

| id | passer | worst joint | return | 15bp | ex top 1 | ex top 3 | ex top 5 | best share | tickers | trades | win rate | RANDOM4 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `union_hot_n4_h1__w0__n4__cnone` | False |  | 38.91% | 48.77% | 22.09% | -1.17% | -12.13% | 43.24% | 50 | 61 | 52.46% | -10.98% |
| `union_hot_n4_holdup__w0__n4__cnone` | False |  | 54.62% | 63.61% | 35.35% | 10.52% | -3.58% | 35.28% | 48 | 56 | 55.36% | -6.37% |

**P2, 2026-09-14 through 2026-09-25**

| id | frozen | rejected | return | 15bp | ex top 1 | ex top 3 | ex top 5 | best share | tickers | trades | win rate | RANDOM4 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `union_hot_n4_h1__w0__n4__cnone` | False | False | 36.02% | 37.23% | 5.62% | -6.19% | -11.02% | 84.41% | 31 | 32 | 53.12% | -7.27% |
| `union_hot_n4_holdup__w0__n4__cnone` | False | False | 38.82% | 39.49% | 11.25% | 0.16% | -3.05% | 71.03% | 29 | 30 | 53.33% | -2.27% |

### concentration_cap_v2

**Tune, 2026-08-13 through 2026-09-11**

| id | passer | worst joint | return | 15bp | ex top 1 | ex top 3 | ex top 5 | best share | tickers | trades | win rate | RANDOM4 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `union_hot_n4_h1__w0__n4__cnone` | False |  | 38.91% | 48.77% | 22.09% | -1.17% | -12.13% | 43.24% | 50 | 61 | 52.46% | -10.98% |
| `union_hot_n4_holdup__w0__n4__cnone` | False |  | 54.62% | 63.61% | 35.35% | 10.52% | -3.58% | 35.28% | 48 | 56 | 55.36% | -6.37% |

**P2, 2026-09-14 through 2026-09-25**

| id | frozen | rejected | return | 15bp | ex top 1 | ex top 3 | ex top 5 | best share | tickers | trades | win rate | RANDOM4 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `union_hot_n4_h1__w0__n4__cnone` | False | False | 36.02% | 37.23% | 5.62% | -6.19% | -11.02% | 84.41% | 31 | 32 | 53.12% | -7.27% |
| `union_hot_n4_holdup__w0__n4__cnone` | False | False | 38.82% | 39.49% | 11.25% | 0.16% | -3.05% | 71.03% | 29 | 30 | 53.33% | -2.27% |

### concentration_cap_v3

**Tune, 2026-08-13 through 2026-09-11**

| id | passer | worst joint | R | 15bp | R_-1 | R_-3 | R_-5 | dependence | gross share | tickers | trades | win rate | RANDOM4 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `union_hot_n4_h1__w0__n4__cnone` | False |  | 38.91% | 48.77% | 22.09% | -1.17% | -12.13% | 43.24% | 24.03% | 50 | 61 | 52.46% | -10.98% |
| `union_hot_n4_holdup__w0__n4__cnone` | False |  | 54.62% | 63.61% | 35.35% | 10.52% | -3.58% | 35.28% | 23.59% | 48 | 56 | 55.36% | -6.37% |

**P2, 2026-09-14 through 2026-09-25**

| id | frozen | rejected | R | 15bp | R_-1 | R_-3 | R_-5 | dependence | gross share | tickers | trades | win rate | RANDOM4 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `union_hot_n4_h1__w0__n4__cnone` | False | False | 36.02% | 37.23% | 5.62% | -6.19% | -11.02% | 84.41% | 59.01% | 31 | 32 | 53.12% | -7.27% |
| `union_hot_n4_holdup__w0__n4__cnone` | False | False | 38.82% | 39.49% | 11.25% | 0.16% | -3.05% | 71.03% | 58.83% | 29 | 30 | 53.33% | -2.27% |

Those cells are not this study's result.
