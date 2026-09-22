# Improvement tracker — 2026-09-22T18:16:39-04:00

Rolling direction/magnitude hit of the *shipped* prediction vs three naive baselines computed on the same graded runs (always up, always down, same direction as the previous graded session of that topic). `edge` = engine direction hit minus the best baseline over the same window. Sessions are dated by the predicted session; sectors are pooled (11 per day).

## General market (SPX) — 33 graded runs over 33 sessions

**Read:** improving; beating best baseline by +10% (last 10 sessions)

### Eras

| Era | n | engine dir | engine mag | always up | always down | same as yesterday | edge vs best baseline |
|---|---:|---:|---:|---:|---:|---:|---:|
| all graded | 33 | **58%** | 52% | 46% | 48% | 50% | **+8%** |
| legacy engine era | 26 | **54%** | 46% | 46% | 50% | 48% | **+4%** |
| v2 engine live (since 2026-09-14) | 7 | **71%** | 71% | 43% | 43% | 57% | **+14%** |

Walk-forward replay estimate for v2 on the same history: **71%** direction / 57% magnitude (n=28, `REPLAY_HARNESS.md` 2026-09-12). The live v2 curve above should converge toward this as sessions accumulate; if it sits well below it for 20+ sessions, the anchor inputs or the LLM components changed.

### Cumulative since v2 went live

| # | Session | topic | cum dir | cum mag |
|---:|---|---|---:|---:|
| 1 | 2026-09-14 | general | 100% | 100% |
| 2 | 2026-09-15 | general | 100% | 100% |
| 3 | 2026-09-16 | general | 67% | 100% |
| 4 | 2026-09-17 | general | 75% | 100% |
| 5 | 2026-09-18 | general | 80% | 80% |
| 6 | 2026-09-21 | general | 83% | 83% |
| 7 | 2026-09-22 | general | 71% | 71% |

### Session curve (last 30 sessions)

| Session | engine | n | session dir | dir (10) | mag (10) | up (10) | down (10) | yest (10) | edge (10) | dir (20) | edge (20) |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-05 | legacy | 1 | 100% | **75%** | 50% | 75% | 25% | 67% | +0% | 75% | +0% |
| 2026-08-06 | legacy | 1 | 100% | **80%** | 60% | 60% | 40% | 75% | +5% | 80% | +5% |
| 2026-08-07 | legacy | 1 | 100% | **83%** | 67% | 67% | 33% | 60% | +17% | 83% | +17% |
| 2026-08-10 | legacy | 1 | 0% | **71%** | 57% | 57% | 29% | 50% | +14% | 71% | +14% |
| 2026-08-11 | legacy | 1 | 100% | **75%** | 50% | 50% | 38% | 43% | +25% | 75% | +25% |
| 2026-08-12 | legacy | 1 | 100% | **78%** | 56% | 56% | 33% | 38% | +22% | 78% | +22% |
| 2026-08-13 | legacy | 1 | 100% | **80%** | 50% | 60% | 30% | 44% | +20% | 80% | +20% |
| 2026-08-14 | legacy | 1 | 0% | **80%** | 40% | 50% | 40% | 40% | +30% | 73% | +18% |
| 2026-08-17 | legacy | 1 | 0% | **70%** | 40% | 40% | 50% | 40% | +20% | 67% | +17% |
| 2026-08-18 | legacy | 1 | 100% | **70%** | 50% | 30% | 60% | 40% | +10% | 69% | +19% |
| 2026-08-19 | legacy | 1 | 0% | **60%** | 40% | 40% | 50% | 40% | +10% | 64% | +14% |
| 2026-08-20 | legacy | 1 | 0% | **50%** | 30% | 40% | 50% | 30% | +0% | 60% | +13% |
| 2026-08-21 | legacy | 1 | 100% | **50%** | 30% | 40% | 50% | 30% | +0% | 62% | +12% |
| 2026-08-23 | legacy | 1 | 0% | **50%** | 30% | 50% | 50% | 40% | +0% | 59% | +6% |
| 2026-08-28 | legacy | 1 | 0% | **40%** | 40% | 50% | 50% | 40% | -10% | 56% | +6% |
| 2026-09-01 | legacy | 1 | 100% | **40%** | 40% | 40% | 60% | 50% | -20% | 58% | +10% |
| 2026-09-02 | legacy | 1 | 0% | **30%** | 50% | 40% | 60% | 40% | -30% | 55% | +5% |
| 2026-09-03 | legacy | 1 | 0% | **30%** | 50% | 50% | 50% | 50% | -20% | 55% | +5% |
| 2026-09-04 | legacy | 1 | 0% | **30%** | 50% | 50% | 50% | 40% | -20% | 50% | +0% |
| 2026-09-08 | legacy | 1 | 100% | **30%** | 50% | 50% | 50% | 40% | -20% | 50% | -5% |
| 2026-09-09 | legacy | 1 | 100% | **40%** | 60% | 40% | 60% | 50% | -20% | 50% | -5% |
| 2026-09-10 | legacy | 1 | 100% | **50%** | 60% | 40% | 60% | 60% | -10% | 50% | -5% |
| 2026-09-11 | legacy | 1 | 0% | **40%** | 50% | 40% | 60% | 60% | -20% | 45% | -10% |
| 2026-09-14 | v2 | 1 | 100% | **50%** | 60% | 30% | 70% | 50% | -20% | 50% | -10% |
| 2026-09-15 | v2 | 1 | 100% | **60%** | 60% | 30% | 70% | 60% | -10% | 50% | -10% |
| 2026-09-16 | v2 | 1 | 0% | **50%** | 60% | 30% | 70% | 60% | -20% | 45% | -20% |
| 2026-09-17 | v2 | 1 | 100% | **60%** | 60% | 30% | 70% | 60% | -10% | 45% | -20% |
| 2026-09-18 | v2 | 1 | 100% | **70%** | 60% | 30% | 70% | 60% | +0% | 50% | -10% |
| 2026-09-21 | v2 | 1 | 100% | **80%** | 70% | 40% | 60% | 70% | +10% | 55% | +0% |
| 2026-09-22 | v2 | 1 | 0% | **70%** | 60% | 40% | 50% | 60% | +10% | 50% | +0% |

## Sectors (11 ETFs pooled) — 257 graded runs over 24 sessions

**Read:** improving; not beating best baseline by -8% (last 10 sessions)

### Eras

| Era | n | engine dir | engine mag | always up | always down | same as yesterday | edge vs best baseline |
|---|---:|---:|---:|---:|---:|---:|---:|
| all graded | 257 | **44%** | 32% | 41% | 51% | 44% | **-7%** |
| legacy engine era | 183 | **46%** | 31% | 43% | 51% | 46% | **-6%** |
| v2 engine live (since 2026-09-14) | 74 | **40%** | 35% | 36% | 51% | 39% | **-11%** |

Walk-forward replay estimate for v2 on the same history: **61%** direction / 48% magnitude (n=183, `REPLAY_HARNESS.md` 2026-09-12). The live v2 curve above should converge toward this as sessions accumulate; if it sits well below it for 20+ sessions, the anchor inputs or the LLM components changed.

### Cumulative since v2 went live

| # | Session | topic | cum dir | cum mag |
|---:|---|---|---:|---:|
| 35 | 2026-09-17 | sector:Energy | 46% | 37% |
| 36 | 2026-09-17 | sector:Financial | 47% | 39% |
| 37 | 2026-09-17 | sector:Healthcare | 49% | 40% |
| 38 | 2026-09-17 | sector:Industrials | 47% | 42% |
| 39 | 2026-09-17 | sector:Real Estate | 46% | 41% |
| 40 | 2026-09-17 | sector:Technology | 45% | 40% |
| 41 | 2026-09-17 | sector:Utilities | 44% | 39% |
| 42 | 2026-09-18 | sector:Basic Materials | 43% | 38% |
| 43 | 2026-09-18 | sector:Communication Services | 42% | 37% |
| 44 | 2026-09-18 | sector:Consumer Cyclical | 41% | 36% |
| 45 | 2026-09-18 | sector:Consumer Defensive | 40% | 38% |
| 46 | 2026-09-18 | sector:Energy | 41% | 37% |
| 47 | 2026-09-18 | sector:Financial | 43% | 38% |
| 48 | 2026-09-18 | sector:Healthcare | 42% | 38% |
| 49 | 2026-09-18 | sector:Industrials | 41% | 37% |
| 50 | 2026-09-18 | sector:Real Estate | 40% | 36% |
| 51 | 2026-09-18 | sector:Technology | 39% | 35% |
| 52 | 2026-09-18 | sector:Utilities | 38% | 35% |
| 53 | 2026-09-21 | sector:Basic Materials | 38% | 34% |
| 54 | 2026-09-21 | sector:Communication Services | 39% | 33% |
| 55 | 2026-09-21 | sector:Consumer Cyclical | 38% | 33% |
| 56 | 2026-09-21 | sector:Consumer Defensive | 38% | 32% |
| 57 | 2026-09-21 | sector:Energy | 39% | 32% |
| 58 | 2026-09-21 | sector:Financial | 40% | 33% |
| 59 | 2026-09-21 | sector:Healthcare | 41% | 34% |
| 60 | 2026-09-21 | sector:Industrials | 40% | 35% |
| 61 | 2026-09-21 | sector:Real Estate | 39% | 36% |
| 62 | 2026-09-21 | sector:Technology | 39% | 36% |
| 63 | 2026-09-21 | sector:Utilities | 40% | 35% |
| 64 | 2026-09-22 | sector:Basic Materials | 39% | 34% |
| 65 | 2026-09-22 | sector:Communication Services | 38% | 34% |
| 66 | 2026-09-22 | sector:Consumer Cyclical | 38% | 33% |
| 67 | 2026-09-22 | sector:Consumer Defensive | 39% | 34% |
| 68 | 2026-09-22 | sector:Energy | 40% | 34% |
| 69 | 2026-09-22 | sector:Financial | 41% | 33% |
| 70 | 2026-09-22 | sector:Healthcare | 40% | 34% |
| 71 | 2026-09-22 | sector:Industrials | 39% | 34% |
| 72 | 2026-09-22 | sector:Real Estate | 40% | 33% |
| 73 | 2026-09-22 | sector:Technology | 40% | 34% |
| 74 | 2026-09-22 | sector:Utilities | 40% | 35% |

### Session curve (last 30 sessions)

| Session | engine | n | session dir | dir (10) | mag (10) | up (10) | down (10) | yest (10) | edge (10) | dir (20) | edge (20) |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-10 | legacy | 11 | 73% | **73%** | 36% | 46% | 55% | — | +18% | 73% | +18% |
| 2026-08-11 | legacy | 11 | 27% | **50%** | 27% | 41% | 55% | 55% | -4% | 50% | -4% |
| 2026-08-12 | legacy | 11 | 64% | **55%** | 30% | 48% | 46% | 46% | +6% | 55% | +6% |
| 2026-08-13 | legacy | 11 | 64% | **57%** | 30% | 52% | 36% | 52% | +4% | 57% | +4% |
| 2026-08-14 | legacy | 11 | 73% | **60%** | 27% | 55% | 36% | 48% | +6% | 60% | +6% |
| 2026-08-17 | legacy | 11 | 36% | **56%** | 26% | 48% | 44% | 46% | +8% | 56% | +8% |
| 2026-08-18 | legacy | 11 | 73% | **58%** | 29% | 47% | 47% | 48% | +10% | 58% | +10% |
| 2026-08-21 | legacy | 11 | 27% | **55%** | 28% | 50% | 43% | 47% | +4% | 55% | +4% |
| 2026-08-26 | legacy | 10 | 20% | **51%** | 30% | 50% | 43% | 45% | +1% | 51% | +1% |
| 2026-08-27 | legacy | 10 | 30% | **49%** | 29% | 46% | 47% | 45% | +2% | 49% | +2% |
| 2026-08-28 | legacy | 11 | 36% | **45%** | 29% | 46% | 46% | 44% | -1% | 48% | +1% |
| 2026-09-03 | legacy | 11 | 0% | **43%** | 29% | 50% | 44% | 41% | -7% | 44% | -5% |
| 2026-09-04 | legacy | 11 | 27% | **39%** | 29% | 46% | 48% | 43% | -9% | 43% | -5% |
| 2026-09-08 | legacy | 9 | 44% | **37%** | 28% | 42% | 55% | 42% | -18% | 43% | -7% |
| 2026-09-09 | legacy | 11 | 73% | **37%** | 30% | 36% | 59% | 46% | -23% | 45% | -7% |
| 2026-09-10 | legacy | 11 | 64% | **40%** | 36% | 35% | 59% | 49% | -20% | 46% | -8% |
| 2026-09-11 | legacy | 11 | 46% | **37%** | 33% | 40% | 55% | 45% | -18% | 46% | -6% |
| 2026-09-14 | v2 | 10 | 70% | **41%** | 34% | 34% | 60% | 44% | -19% | 47% | -5% |
| 2026-09-15 | v2 | 9 | 56% | **44%** | 35% | 32% | 62% | 44% | -18% | 48% | -6% |
| 2026-09-16 | v2 | 11 | 18% | **43%** | 35% | 31% | 60% | 45% | -17% | 46% | -8% |
| 2026-09-17 | v2 | 11 | 36% | **43%** | 36% | 35% | 56% | 44% | -13% | 44% | -7% |
| 2026-09-18 | v2 | 11 | 18% | **45%** | 36% | 30% | 61% | 45% | -16% | 44% | -8% |
| 2026-09-21 | v2 | 11 | 46% | **47%** | 36% | 32% | 57% | 46% | -10% | 43% | -10% |
| 2026-09-22 | v2 | 11 | 46% | **47%** | 37% | 36% | 54% | 44% | -8% | 42% | -13% |

### Per sector (all graded)

| Sector | n | dir | mag | always up | always down | yest | edge |
|---|---:|---:|---:|---:|---:|---:|---:|
| Basic Materials | 24 | 42% | 38% | 38% | 58% | 39% | -17% |
| Communication Services | 23 | 26% | 30% | 39% | 61% | 50% | -35% |
| Consumer Cyclical | 24 | 50% | 17% | 29% | 62% | 44% | -12% |
| Consumer Defensive | 24 | 46% | 38% | 42% | 54% | 48% | -8% |
| Energy | 24 | 58% | 33% | 54% | 42% | 35% | +4% |
| Financial | 24 | 54% | 38% | 33% | 46% | 48% | +6% |
| Healthcare | 22 | 50% | 32% | 41% | 46% | 38% | +4% |
| Industrials | 24 | 33% | 29% | 46% | 42% | 48% | -14% |
| Real Estate | 24 | 46% | 29% | 29% | 62% | 39% | -17% |
| Technology | 22 | 36% | 41% | 64% | 32% | 43% | -27% |
| Utilities | 22 | 46% | 32% | 36% | 59% | 57% | -14% |

