# HIT Board — general + sectors (all dates)

Generated: **2026-08-20T18:57:59.142940-04:00**

Source: `03_scoreboard/scoreboard.json`.

**HIT% rule:** only runs with a real `predicted_direction` count. Days with no predict file are **pipeline blanks** — listed separately, not counted as model MISS.

## Overall HIT% (model calls only)

| Book | Direction HIT% | hits / graded | Mag HIT% | n mag |
|------|----------------|---------------|----------|-------|
| **General (SPX-style)** | **60.0%** | 9/15 | 40.0% | 15 |
| **All sector calls** | **58.4%** | 45/77 | 28.6% | 77 |

### Pipeline blanks (general) — excluded from HIT%

- No `predicted_direction`: **2026-08-02, 2026-08-08, 2026-08-09, 2026-08-15, 2026-08-16** (n=5)
- Of those, legacy scoreboard still marked direction_hit=false: **2026-08-02, 2026-08-08, 2026-08-09** — ops failure, not model error
- If blanks were counted as MISS (old method): **50.0%** (9/18)

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

## Sectors — HIT% by sector (across dates)

| Sector | ETF | Dir HIT% | hits/graded | Mag HIT% |
|--------|-----|----------|-------------|----------|
| Basic Materials | XLB | **57.1%** | 4/7 | 28.6% |
| Communication Services | XLC | **42.9%** | 3/7 | 28.6% |
| Consumer Cyclical | XLY | **85.7%** | 6/7 | 28.6% |
| Consumer Defensive | XLP | **57.1%** | 4/7 | 42.9% |
| Energy | XLE | **71.4%** | 5/7 | 42.9% |
| Financial | XLF | **42.9%** | 3/7 | 0.0% |
| Healthcare | XLV | **71.4%** | 5/7 | 0.0% |
| Industrials | XLI | **28.6%** | 2/7 | 14.3% |
| Real Estate | XLRE | **71.4%** | 5/7 | 28.6% |
| Technology | XLK | **57.1%** | 4/7 | 57.1% |
| Utilities | XLU | **57.1%** | 4/7 | 42.9% |

## Sector matrix (dir hit) — last 10 dates

HIT / MISS / NO_PRED / — . Actual % when graded.

| Sector | 2026-08-11 | 2026-08-12 | 2026-08-13 | 2026-08-14 | 2026-08-15 | 2026-08-16 | 2026-08-17 | 2026-08-18 | 2026-08-19 | 2026-08-20 |
|--------|------|------|------|------|------|------|------|------|------|------|
| Basic Materials | HIT (+0.1%) | MISS (-1.2%) | HIT (-0.5%) | MISS (+0.4%) | NO_PRED | NO_PRED | MISS (-0.6%) | HIT (-0.9%) | NO_PRED | NO_PRED |
| Communication Services | MISS (-0.5%) | HIT (-0.9%) | MISS (+2.1%) | HIT (+0.4%) | NO_PRED | NO_PRED | MISS (-1.9%) | HIT (-0.3%) | NO_PRED | NO_PRED |
| Consumer Cyclical | MISS (-0.4%) | HIT (-1.1%) | HIT (+0.5%) | HIT (-0.2%) | NO_PRED | NO_PRED | HIT (-1.2%) | HIT (-0.3%) | NO_PRED | NO_PRED |
| Consumer Defensive | MISS (-0.3%) | MISS (+0.5%) | HIT (+1.1%) | HIT (+0.1%) | NO_PRED | NO_PRED | MISS (-1.6%) | HIT (+1.1%) | NO_PRED | NO_PRED |
| Energy | MISS (+1.2%) | HIT (+0.2%) | MISS (+0.0%) | HIT (+1.4%) | NO_PRED | NO_PRED | HIT (+1.1%) | HIT (+1.8%) | NO_PRED | NO_PRED |
| Financial | MISS (-0.0%) | HIT (+0.2%) | HIT (+0.6%) | MISS (-0.2%) | NO_PRED | NO_PRED | MISS (-1.0%) | MISS (+0.5%) | NO_PRED | NO_PRED |
| Healthcare | MISS (-0.3%) | HIT (+0.3%) | MISS (-0.0%) | HIT (-0.6%) | NO_PRED | NO_PRED | HIT (-0.2%) | HIT (+1.6%) | NO_PRED | NO_PRED |
| Industrials | HIT (+0.6%) | MISS (+0.1%) | MISS (-0.0%) | HIT (+0.4%) | NO_PRED | NO_PRED | MISS (-0.1%) | MISS (-1.5%) | NO_PRED | NO_PRED |
| Real Estate | HIT (-0.7%) | MISS (+0.9%) | HIT (+1.4%) | HIT (+0.3%) | NO_PRED | NO_PRED | MISS (-1.0%) | HIT (-0.4%) | NO_PRED | NO_PRED |
| Technology | MISS (-0.1%) | HIT (+1.5%) | HIT (+1.0%) | MISS (-0.4%) | NO_PRED | NO_PRED | HIT (+0.2%) | HIT (-2.5%) | NO_PRED | NO_PRED |
| Utilities | MISS (+1.2%) | HIT (+0.5%) | HIT (+0.5%) | HIT (+0.6%) | NO_PRED | NO_PRED | MISS (-0.3%) | MISS (-0.4%) | NO_PRED | NO_PRED |

## Files

- This board: `03_scoreboard/HIT_BOARD.md`
- JSON: `03_scoreboard/hit_board.json`
- Per-day sector snapshot: `01_daily/sectors/<date>/_BOARD.md`
