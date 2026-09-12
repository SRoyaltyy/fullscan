# Replay harness — 2026-09-12T07:45:06

Re-scores every graded run from its stored LLM components plus the earliest **pre-09:30** Channel 1 snapshot of that day. Skill multipliers are walk-forward (built only from earlier runs). Days whose only snapshot was fetched after the open are scored with no tape, as the live engine would have to. `dir hit (tape days)` restricts to days where an anchor existed.

## General market — 36 graded runs, tape available on 21

| Policy | n | direction hit | magnitude hit | dir hit (tape days) | 1st half | 2nd half | flat calls (hits) |
|---|---:|---:|---:|---:|---:|---:|---:|
| legacy engine (as shipped) | 26 | **53.8%** | 46.2% | 57.9% (19) | 69.2% | 38.5% | 4 (0) |
| v2 engine (anchor + skill-weighted LLM) | 28 | **71.4%** | 57.1% | 81.0% (21) | 69.2% | 73.3% | 0 (0) |
| v2 rules, LLM only (no tape) | 26 | **69.2%** | 50.0% | 78.9% (19) | 76.9% | 61.5% | 0 (0) |
| tape anchor only (no LLM) | 21 | **61.9%** | 61.9% | 61.9% (21) | 50.0% | 72.7% | 2 (0) |
| baseline: always up | 36 | **55.6%** | 66.7% | 47.6% (21) | 50.0% | 61.1% | 0 (0) |
| baseline: always down | 36 | **41.7%** | 66.7% | 52.4% (21) | 44.4% | 38.9% | 0 (0) |
| baseline: same as yesterday | 36 | **52.8%** | 44.4% | 38.1% (21) | 66.7% | 38.9% | 2 (0) |

## Sectors — 183 graded runs, tape available on 152

| Policy | n | direction hit | magnitude hit | dir hit (tape days) | 1st half | 2nd half | flat calls (hits) |
|---|---:|---:|---:|---:|---:|---:|---:|
| legacy engine (as shipped) | 174 | **48.3%** | 31.6% | 44.8% (143) | 54.5% | 41.9% | 21 (1) |
| v2 engine (anchor + skill-weighted LLM) | 183 | **60.7%** | 47.5% | 55.9% (152) | 60.2% | 61.1% | 0 (0) |
| v2 rules, LLM only (no tape) | 174 | **55.7%** | 46.6% | 49.7% (143) | 56.8% | 54.7% | 1 (0) |
| tape anchor only (no LLM) | 152 | **45.4%** | 47.4% | 45.4% (152) | 51.9% | 38.7% | 20 (1) |
| baseline: always up | 183 | **42.6%** | 49.2% | 46.7% (152) | 50.0% | 35.8% | 0 (0) |
| baseline: always down | 183 | **51.4%** | 49.2% | 47.4% (152) | 43.2% | 58.9% | 0 (0) |
| baseline: same as yesterday | 183 | **44.3%** | 33.9% | 42.8% (152) | 39.8% | 48.4% | 15 (1) |

### Direction hit by sector

| Sector | legacy | v2 | tape only |
|---|---:|---:|---:|
| Basic Materials | 56.2% (16) | 64.7% (17) | 42.9% (14) |
| Communication Services | 33.3% (15) | 52.9% (17) | 50.0% (14) |
| Consumer Cyclical | 64.7% (17) | 70.6% (17) | 42.9% (14) |
| Consumer Defensive | 47.1% (17) | 52.9% (17) | 35.7% (14) |
| Energy | 47.1% (17) | 58.8% (17) | 50.0% (14) |
| Financial | 43.8% (16) | 52.9% (17) | 28.6% (14) |
| Healthcare | 64.3% (14) | 73.3% (15) | 25.0% (12) |
| Industrials | 31.2% (16) | 64.7% (17) | 57.1% (14) |
| Real Estate | 50.0% (16) | 58.8% (17) | 35.7% (14) |
| Technology | 43.8% (16) | 62.5% (16) | 78.6% (14) |
| Utilities | 50.0% (14) | 56.2% (16) | 50.0% (14) |

## Factor skill (sign of component vs actual direction, all graded runs)

| Factor | n | sign hit | multiplier now |
|---|---:|---:|---:|
| B0_ASIA | 13 | 0.385 | 0.0 |
| B0_EUROPE | 8 | 1.0 | 1.25 |
| B1_CATALYSTS | 21 | 0.762 | 1.25 |
| B2_BONDS | 24 | 0.333 | 0.0 |
| B3_FEDPATH | 23 | 0.478 | 0.5 |
| B4_VIX | 8 | 0.5 | 0.5 |
| B5_SENTIMENT | 21 | 0.333 | 0.0 |
| B6_FUTURES | 15 | 0.733 | 1.25 |
| B7_OIL_DOLLAR | 21 | 0.667 | 1.25 |
| S0_SHARED_MACRO (all sectors) | 114 | 0.596 | 1.0 |
| S1_SECTOR_FACTORS (all sectors) | 141 | 0.553 | 1.0 |
| S2_BREADTH (all sectors) | 116 | 0.586 | 1.0 |
| S3_FLOWS_POSITIONING (all sectors) | 87 | 0.471 | 0.5 |
| S4_ETF_TAPE (all sectors) | 122 | 0.623 | 1.0 |

## Principle check — 09:30 open gap sign vs close-to-close direction (no snapshots involved)

| Ticker | n | gap-sign hit | always up | always down | same as yesterday | flat share |
|---|---:|---:|---:|---:|---:|---:|
| SPY | 290 | **0.628** | 0.493 | 0.39 | 0.379 | 0.117 |
| XLB | 286 | **0.622** | 0.479 | 0.423 | 0.444 | 0.098 |
| XLC | 286 | **0.605** | 0.476 | 0.451 | 0.434 | 0.073 |
| XLY | 286 | **0.685** | 0.486 | 0.448 | 0.42 | 0.066 |
| XLP | 290 | **0.638** | 0.459 | 0.455 | 0.362 | 0.086 |
| XLE | 290 | **0.672** | 0.541 | 0.403 | 0.452 | 0.055 |
| XLF | 290 | **0.659** | 0.497 | 0.424 | 0.428 | 0.079 |
| XLV | 290 | **0.621** | 0.448 | 0.455 | 0.397 | 0.097 |
| XLI | 285 | **0.653** | 0.477 | 0.442 | 0.382 | 0.081 |
| XLRE | 287 | **0.624** | 0.491 | 0.401 | 0.401 | 0.108 |
| XLK | 290 | **0.707** | 0.572 | 0.372 | 0.452 | 0.055 |
| XLU | 290 | **0.566** | 0.497 | 0.417 | 0.486 | 0.086 |

The 05:55 futures/Europe read is a noisier version of the 09:30 gap, so the live anchor should land a little under these numbers; the leak-free snapshot rows above are the honest live estimate.

## Per-run rows (v2 engine)

| Date | Topic | pred | actual | % | dir | mag |
|---|---|---|---|---:|---|---|
| 2026-07-31 | general | down/mild | up/mild | +0.70 | ❌ | ✅ |
| 2026-08-03 | general | up/mild | up/notable | +1.48 | ✅ | ❌ |
| 2026-08-04 | general | up/mild | up/notable | +1.79 | ✅ | ❌ |
| 2026-08-05 | general | down/mild | down/flat | -0.17 | ✅ | ❌ |
| 2026-08-06 | general | up/mild | down/flat | -0.18 | ❌ | ❌ |
| 2026-08-07 | general | up/mild | up/mild | +0.62 | ✅ | ✅ |
| 2026-08-10 | general | down/mild | flat/flat | -0.06 | ❌ | ❌ |
| 2026-08-11 | general | down/mild | down/mild | -0.32 | ✅ | ✅ |
| 2026-08-12 | general | up/mild | up/flat | +0.26 | ✅ | ❌ |
| 2026-08-13 | general | up/mild | up/mild | +0.65 | ✅ | ✅ |
| 2026-08-14 | general | up/mild | down/flat | -0.17 | ❌ | ❌ |
| 2026-08-17 | general | down/mild | down/mild | -0.52 | ✅ | ✅ |
| 2026-08-18 | general | down/mild | down/mild | -0.69 | ✅ | ✅ |
| 2026-08-19 | general | down/mild | up/flat | +0.21 | ❌ | ❌ |
| 2026-08-20 | general | down/mild | down/mild | -0.87 | ✅ | ✅ |
| 2026-08-21 | general | up/mild | up/mild | +0.43 | ✅ | ✅ |
| 2026-08-23 | general | down/mild | up/mild | +0.43 | ❌ | ✅ |
| 2026-08-25 | general | up/mild | up/mild | +0.32 | ✅ | ✅ |
| 2026-08-27 | general | up/mild | up/mild | +0.72 | ✅ | ✅ |
| 2026-08-28 | general | down/mild | down/flat | -0.25 | ✅ | ❌ |
| 2026-09-01 | general | down/mild | down/mild | -0.71 | ✅ | ✅ |
| 2026-09-02 | general | down/mild | up/mild | +0.46 | ❌ | ✅ |
| 2026-09-03 | general | up/mild | up/notable | +1.06 | ✅ | ❌ |
| 2026-09-04 | general | up/mild | down/mild | -0.38 | ❌ | ✅ |
| 2026-09-08 | general | down/notable | down/mild | -0.58 | ✅ | ❌ |
| 2026-09-09 | general | down/notable | down/mild | -0.48 | ✅ | ❌ |
| 2026-09-10 | general | down/mild | down/mild | -0.58 | ✅ | ✅ |
| 2026-09-11 | general | up/mild | up/mild | +0.86 | ✅ | ✅ |
