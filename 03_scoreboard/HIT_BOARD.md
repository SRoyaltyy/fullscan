# HIT Board — general + sectors (all dates)

Generated: **2026-09-04T11:48:03.664836-04:00**

Source: `03_scoreboard/scoreboard.json`.

**HIT% rule:** only runs with a real `predicted_direction` count. Days with no predict file are **pipeline blanks** — listed separately, not counted as model MISS.

## Overall HIT% (model calls only)

| Book | Direction HIT% | hits / graded | Mag HIT% | n mag |
|------|----------------|---------------|----------|-------|
| **General (SPX-style)** | **52.4%** | 11/21 | 47.6% | 21 |
| **All sector calls** | **43.8%** | 57/130 | 28.5% | 130 |

### Pipeline blanks (general) — excluded from HIT%

- No `predicted_direction`: **2026-08-02, 2026-08-08, 2026-08-09, 2026-08-15, 2026-08-16, 2026-08-22, 2026-08-25, 2026-08-27, 2026-08-29, 2026-08-30** (n=10)
- Of those, legacy scoreboard still marked direction_hit=false: **2026-08-02, 2026-08-08, 2026-08-09** — ops failure, not model error
- If blanks were counted as MISS (old method): **45.8%** (11/24)

## General market — by date

| Date | Pred dir | Mag | Score | Actual % | Actual dir | Dir | Mag |
|------|----------|-----|-------|----------|------------|-----|-----|
| 2026-07-31 | down | mild | -4.4 | 0.7 | up | MISS | HIT |
| 2026-08-02 | — | — | — | 0.7 | up | NO_PRED | NO_PRED |
| 2026-08-03 | up | mild | 3.75 | 1.48 | up | HIT | MISS |
| 2026-08-04 | up | mild | 6.25 | 1.79 | up | HIT | MISS |
| 2026-08-05 | down | flat | -1.575 | -0.17 | down | HIT | HIT |
| 2026-08-06 | down | flat | -2.0 | -0.18 | down | HIT | HIT |
| 2026-08-07 | up | mild | 5.75 | 0.62 | up | HIT | HIT |
| 2026-08-08 | — | — | — | 0.62 | up | NO_PRED | NO_PRED |
| 2026-08-09 | — | — | — | 0.62 | up | NO_PRED | NO_PRED |
| 2026-08-10 | down | mild | -3.6 | -0.06 | flat | MISS | MISS |
| 2026-08-11 | down | flat | -2.475 | -0.32 | down | HIT | MISS |
| 2026-08-12 | up | flat | 2.25 | 0.26 | up | HIT | HIT |
| 2026-08-13 | up | notable | 8.525 | 0.65 | up | HIT | MISS |
| 2026-08-14 | up | mild | 5.5 | -0.17 | down | MISS | MISS |
| 2026-08-15 | — | — | — | -0.17 | down | NO_PRED | NO_PRED |
| 2026-08-16 | — | — | — | -0.17 | down | NO_PRED | NO_PRED |
| 2026-08-17 | up | flat | 2.25 | -0.52 | down | MISS | MISS |
| 2026-08-18 | down | mild | -6.2 | -0.69 | down | HIT | HIT |
| 2026-08-19 | down | notable | -7.2 | 0.21 | up | MISS | MISS |
| 2026-08-20 | up | flat | 1.125 | -0.87 | down | MISS | MISS |
| 2026-08-21 | up | mild | 3.25 | 0.43 | up | HIT | HIT |
| 2026-08-22 | — | — | — | 0.43 | up | NO_PRED | NO_PRED |
| 2026-08-23 | flat | flat | 0.0 | 0.43 | up | MISS | MISS |
| 2026-08-24 | down | mild | -5.175 | — | — | — | — |
| 2026-08-25 | — | — | — | 0.32 | up | NO_PRED | NO_PRED |
| 2026-08-26 | up | flat | 2.025 | — | — | — | — |
| 2026-08-27 | — | — | — | 0.72 | up | NO_PRED | NO_PRED |
| 2026-08-28 | flat | flat | 0.75 | -0.25 | down | MISS | HIT |
| 2026-08-29 | — | — | — | 0.72 | up | NO_PRED | NO_PRED |
| 2026-08-30 | — | — | — | 0.72 | up | NO_PRED | NO_PRED |
| 2026-08-31 | down | mild | -5.85 | — | — | — | — |
| 2026-09-01 | down | mild | -6.3 | -0.71 | down | HIT | HIT |
| 2026-09-02 | down | mild | -3.825 | 0.46 | up | MISS | HIT |
| 2026-09-03 | flat | flat | -0.9 | 1.06 | up | MISS | MISS |
| 2026-09-04 | up | flat | 2.25 | — | — | — | — |

## Sectors — HIT% by date (model calls only)

| Date | n sectors | Dir HIT% | hits/graded | Mag HIT% |
|------|-----------|----------|-------------|----------|
| 2026-07-31 | 0 | **—** | 0/0 | — |
| 2026-08-02 | 0 | **—** | 0/0 | — |
| 2026-08-03 | 0 | **—** | 0/0 | — |
| 2026-08-04 | 0 | **—** | 0/0 | — |
| 2026-08-05 | 0 | **—** | 0/0 | — |
| 2026-08-06 | 0 | **—** | 0/0 | — |
| 2026-08-07 | 0 | **—** | 0/0 | — |
| 2026-08-08 | 11 | **—** | 0/0 | — |
| 2026-08-09 | 0 | **—** | 0/0 | — |
| 2026-08-10 | 11 | **72.7%** | 8/11 | 36.4% |
| 2026-08-11 | 11 | **27.3%** | 3/11 | 18.2% |
| 2026-08-12 | 11 | **63.6%** | 7/11 | 36.4% |
| 2026-08-13 | 11 | **63.6%** | 7/11 | 27.3% |
| 2026-08-14 | 11 | **72.7%** | 8/11 | 18.2% |
| 2026-08-15 | 0 | **—** | 0/0 | — |
| 2026-08-16 | 0 | **—** | 0/0 | — |
| 2026-08-17 | 11 | **36.4%** | 4/11 | 18.2% |
| 2026-08-18 | 11 | **72.7%** | 8/11 | 45.5% |
| 2026-08-19 | 0 | **—** | 0/0 | — |
| 2026-08-20 | 0 | **—** | 0/0 | — |
| 2026-08-21 | 11 | **27.3%** | 3/11 | 27.3% |
| 2026-08-22 | 0 | **—** | 0/0 | — |
| 2026-08-23 | 0 | **—** | 0/0 | — |
| 2026-08-24 | 11 | **—** | 0/0 | — |
| 2026-08-25 | 10 | **—** | 0/0 | — |
| 2026-08-26 | 10 | **20.0%** | 2/10 | 40.0% |
| 2026-08-27 | 10 | **30.0%** | 3/10 | 20.0% |
| 2026-08-28 | 11 | **36.4%** | 4/11 | 36.4% |
| 2026-08-29 | 0 | **—** | 0/0 | — |
| 2026-08-30 | 0 | **—** | 0/0 | — |
| 2026-08-31 | 10 | **—** | 0/0 | — |
| 2026-09-01 | 11 | **—** | 0/0 | — |
| 2026-09-02 | 9 | **—** | 0/0 | — |
| 2026-09-03 | 11 | **0.0%** | 0/11 | 18.2% |
| 2026-09-04 | 11 | **—** | 0/0 | — |

## Sectors — HIT% by sector (across dates)

| Sector | ETF | Dir HIT% | hits/graded | Mag HIT% |
|--------|-----|----------|-------------|----------|
| Basic Materials | XLB | **50.0%** | 6/12 | 50.0% |
| Communication Services | XLC | **25.0%** | 3/12 | 16.7% |
| Consumer Cyclical | XLY | **58.3%** | 7/12 | 25.0% |
| Consumer Defensive | XLP | **41.7%** | 5/12 | 41.7% |
| Energy | XLE | **50.0%** | 6/12 | 33.3% |
| Financial | XLF | **33.3%** | 4/12 | 8.3% |
| Healthcare | XLV | **70.0%** | 7/10 | 10.0% |
| Industrials | XLI | **25.0%** | 3/12 | 8.3% |
| Real Estate | XLRE | **50.0%** | 6/12 | 33.3% |
| Technology | XLK | **41.7%** | 5/12 | 50.0% |
| Utilities | XLU | **41.7%** | 5/12 | 33.3% |

## Sector matrix (dir hit) — last 10 dates

HIT / MISS / NO_PRED / — . Actual % when graded.

| Sector | 2026-08-26 | 2026-08-27 | 2026-08-28 | 2026-08-29 | 2026-08-30 | 2026-08-31 | 2026-09-01 | 2026-09-02 | 2026-09-03 | 2026-09-04 |
|--------|------|------|------|------|------|------|------|------|------|------|
| Basic Materials | HIT (+0.2%) | MISS (-0.8%) | MISS (-0.1%) | NO_PRED | NO_PRED | down | down | flat | MISS (-0.6%) | down |
| Communication Services | MISS (-0.5%) | MISS (-1.1%) | MISS (+1.4%) | NO_PRED | NO_PRED | NO_PRED | down | flat | MISS (+0.9%) | flat |
| Consumer Cyclical | HIT (-0.7%) | MISS (-1.1%) | MISS (+1.1%) | NO_PRED | NO_PRED | down | down | down | MISS (+1.4%) | down |
| Consumer Defensive | MISS (-0.3%) | HIT (-1.4%) | MISS (+0.4%) | NO_PRED | NO_PRED | up | flat | up | MISS (-0.3%) | down |
| Energy | MISS (+0.6%) | HIT (-0.2%) | MISS (+0.6%) | NO_PRED | NO_PRED | up | up | up | MISS (-0.7%) | flat |
| Financial | MISS (-0.1%) | MISS (-0.7%) | MISS (+0.4%) | NO_PRED | NO_PRED | up | down | flat | MISS (+1.6%) | up |
| Healthcare | NO_PRED | NO_PRED | HIT (-0.2%) | NO_PRED | NO_PRED | down | down | flat | MISS (+0.2%) | up |
| Industrials | MISS (+1.1%) | MISS (-0.9%) | HIT (-0.9%) | NO_PRED | NO_PRED | down | down | flat | MISS (+1.0%) | down |
| Real Estate | MISS (-0.6%) | MISS (-1.0%) | HIT (-0.4%) | NO_PRED | NO_PRED | down | down | down | MISS (+1.2%) | flat |
| Technology | MISS (+0.6%) | HIT (+3.2%) | MISS (-1.5%) | NO_PRED | NO_PRED | down | down | NO_PRED | MISS (+1.3%) | flat |
| Utilities | MISS (+0.5%) | MISS (-0.8%) | HIT (-1.0%) | NO_PRED | NO_PRED | down | down | NO_PRED | MISS (+0.8%) | flat |

## Files

- This board: `03_scoreboard/HIT_BOARD.md`
- JSON: `03_scoreboard/hit_board.json`
- Per-day sector snapshot: `01_daily/sectors/<date>/_BOARD.md`
