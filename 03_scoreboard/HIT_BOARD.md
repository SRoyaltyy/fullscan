# HIT Board — general + sectors (all dates)

Generated: **2026-08-18T18:53:47.535750-04:00**

Source: `03_scoreboard/scoreboard.json`.

**HIT% rule:** only runs with a real `predicted_direction` count. Days with no predict file are **pipeline blanks** — listed separately, not counted as model MISS.

## Overall HIT% (model calls only)

| Book | Direction HIT% | hits / graded | Mag HIT% | n mag |
|------|----------------|---------------|----------|-------|
| **General (SPX-style)** | **69.2%** | 9/13 | 46.2% | 13 |
| **All sector calls** | **56.1%** | 37/66 | 25.8% | 66 |

### Pipeline blanks (general) — excluded from HIT%

- No `predicted_direction`: **2026-08-02, 2026-08-08, 2026-08-09, 2026-08-15, 2026-08-16** (n=5)
- Of those, legacy scoreboard still marked direction_hit=false: **2026-08-02, 2026-08-08, 2026-08-09** — ops failure, not model error
- If blanks were counted as MISS (old method): **56.2%** (9/16)

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
| 2026-08-18 | 11 | **—** | 0/0 | — |

## Sectors — HIT% by sector (across dates)

| Sector | ETF | Dir HIT% | hits/graded | Mag HIT% |
|--------|-----|----------|-------------|----------|
| Basic Materials | XLB | **50.0%** | 3/6 | 16.7% |
| Communication Services | XLC | **33.3%** | 2/6 | 33.3% |
| Consumer Cyclical | XLY | **83.3%** | 5/6 | 33.3% |
| Consumer Defensive | XLP | **50.0%** | 3/6 | 33.3% |
| Energy | XLE | **66.7%** | 4/6 | 33.3% |
| Financial | XLF | **50.0%** | 3/6 | 0.0% |
| Healthcare | XLV | **66.7%** | 4/6 | 0.0% |
| Industrials | XLI | **33.3%** | 2/6 | 16.7% |
| Real Estate | XLRE | **66.7%** | 4/6 | 16.7% |
| Technology | XLK | **50.0%** | 3/6 | 50.0% |
| Utilities | XLU | **66.7%** | 4/6 | 50.0% |

## Sector matrix (dir hit) — last 10 dates

HIT / MISS / NO_PRED / — . Actual % when graded.

| Sector | 2026-08-09 | 2026-08-10 | 2026-08-11 | 2026-08-12 | 2026-08-13 | 2026-08-14 | 2026-08-15 | 2026-08-16 | 2026-08-17 | 2026-08-18 |
|--------|------|------|------|------|------|------|------|------|------|------|
| Basic Materials | NO_PRED | HIT (+0.6%) | HIT (+0.1%) | MISS (-1.2%) | HIT (-0.5%) | MISS (+0.4%) | NO_PRED | NO_PRED | MISS (-0.6%) | down |
| Communication Services | NO_PRED | MISS (+0.5%) | MISS (-0.5%) | HIT (-0.9%) | MISS (+2.1%) | HIT (+0.4%) | NO_PRED | NO_PRED | MISS (-1.9%) | down |
| Consumer Cyclical | NO_PRED | HIT (-0.2%) | MISS (-0.4%) | HIT (-1.1%) | HIT (+0.5%) | HIT (-0.2%) | NO_PRED | NO_PRED | HIT (-1.2%) | down |
| Consumer Defensive | NO_PRED | HIT (-0.2%) | MISS (-0.3%) | MISS (+0.5%) | HIT (+1.1%) | HIT (+0.1%) | NO_PRED | NO_PRED | MISS (-1.6%) | up |
| Energy | NO_PRED | HIT (+4.7%) | MISS (+1.2%) | HIT (+0.2%) | MISS (+0.0%) | HIT (+1.4%) | NO_PRED | NO_PRED | HIT (+1.1%) | up |
| Financial | NO_PRED | HIT (+0.4%) | MISS (-0.0%) | HIT (+0.2%) | HIT (+0.6%) | MISS (-0.2%) | NO_PRED | NO_PRED | MISS (-1.0%) | down |
| Healthcare | NO_PRED | HIT (+1.7%) | MISS (-0.3%) | HIT (+0.3%) | MISS (-0.0%) | HIT (-0.6%) | NO_PRED | NO_PRED | HIT (-0.2%) | up |
| Industrials | NO_PRED | MISS (-0.3%) | HIT (+0.6%) | MISS (+0.1%) | MISS (-0.0%) | HIT (+0.4%) | NO_PRED | NO_PRED | MISS (-0.1%) | up |
| Real Estate | NO_PRED | HIT (-1.3%) | HIT (-0.7%) | MISS (+0.9%) | HIT (+1.4%) | HIT (+0.3%) | NO_PRED | NO_PRED | MISS (-1.0%) | down |
| Technology | NO_PRED | MISS (-0.9%) | MISS (-0.1%) | HIT (+1.5%) | HIT (+1.0%) | MISS (-0.4%) | NO_PRED | NO_PRED | HIT (+0.2%) | down |
| Utilities | NO_PRED | HIT (-1.1%) | MISS (+1.2%) | HIT (+0.5%) | HIT (+0.5%) | HIT (+0.6%) | NO_PRED | NO_PRED | MISS (-0.3%) | up |

## Files

- This board: `03_scoreboard/HIT_BOARD.md`
- JSON: `03_scoreboard/hit_board.json`
- Per-day sector snapshot: `01_daily/sectors/<date>/_BOARD.md`
