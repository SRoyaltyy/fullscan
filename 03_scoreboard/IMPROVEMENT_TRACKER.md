# Improvement tracker — 2026-09-23T16:59:28-04:00

Rolling direction/magnitude hit of the *shipped* prediction vs three naive baselines computed on the same graded runs (always up, always down, same direction as the previous graded session of that topic). `edge` = engine direction hit minus the best baseline over the same window. Sessions are dated by the predicted session; sectors are pooled (11 per day).

## General market (SPX) — 34 graded runs over 34 sessions

**Read:** improving; beating best baseline by +10% (last 10 sessions)

### Eras

| Era | n | engine dir | engine mag | always up | always down | same as yesterday | edge vs best baseline |
|---|---:|---:|---:|---:|---:|---:|---:|
| all graded | 34 | **56%** | 53% | 44% | 50% | 48% | **+6%** |
| legacy engine era | 26 | **54%** | 46% | 46% | 50% | 48% | **+4%** |
| v2 engine live (since 2026-09-14) | 8 | **62%** | 75% | 38% | 50% | 50% | **+12%** |

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
| 8 | 2026-09-23 | general | 62% | 75% |

### Session curve (last 30 sessions)

| Session | engine | n | session dir | dir (10) | mag (10) | up (10) | down (10) | yest (10) | edge (10) | dir (20) | edge (20) |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
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
| 2026-09-23 | v2 | 1 | 0% | **60%** | 60% | 40% | 50% | 50% | +10% | 50% | -5% |

## Sectors (11 ETFs pooled) — 268 graded runs over 25 sessions

**Read:** improving; not beating best baseline by -9% (last 10 sessions)

### Eras

| Era | n | engine dir | engine mag | always up | always down | same as yesterday | edge vs best baseline |
|---|---:|---:|---:|---:|---:|---:|---:|
| all graded | 268 | **44%** | 32% | 41% | 52% | 45% | **-8%** |
| legacy engine era | 183 | **46%** | 31% | 43% | 51% | 46% | **-6%** |
| v2 engine live (since 2026-09-14) | 85 | **39%** | 35% | 36% | 52% | 42% | **-13%** |

Walk-forward replay estimate for v2 on the same history: **61%** direction / 48% magnitude (n=183, `REPLAY_HARNESS.md` 2026-09-12). The live v2 curve above should converge toward this as sessions accumulate; if it sits well below it for 20+ sessions, the anchor inputs or the LLM components changed.

### Cumulative since v2 went live

| # | Session | topic | cum dir | cum mag |
|---:|---|---|---:|---:|
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
| 75 | 2026-09-23 | sector:Basic Materials | 40% | 35% |
| 76 | 2026-09-23 | sector:Communication Services | 40% | 34% |
| 77 | 2026-09-23 | sector:Consumer Cyclical | 39% | 34% |
| 78 | 2026-09-23 | sector:Consumer Defensive | 40% | 35% |
| 79 | 2026-09-23 | sector:Energy | 39% | 35% |
| 80 | 2026-09-23 | sector:Financial | 39% | 35% |
| 81 | 2026-09-23 | sector:Healthcare | 38% | 35% |
| 82 | 2026-09-23 | sector:Industrials | 39% | 35% |
| 83 | 2026-09-23 | sector:Real Estate | 40% | 35% |
| 84 | 2026-09-23 | sector:Technology | 39% | 36% |
| 85 | 2026-09-23 | sector:Utilities | 39% | 35% |

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
| 2026-09-23 | v2 | 11 | 27% | **42%** | 37% | 38% | 51% | 43% | -9% | 39% | -16% |

### Per sector (all graded)

| Sector | n | dir | mag | always up | always down | yest | edge |
|---|---:|---:|---:|---:|---:|---:|---:|
| Basic Materials | 25 | 40% | 36% | 40% | 56% | 42% | -16% |
| Communication Services | 24 | 25% | 29% | 38% | 62% | 52% | -38% |
| Consumer Cyclical | 25 | 48% | 16% | 28% | 64% | 42% | -16% |
| Consumer Defensive | 25 | 48% | 40% | 44% | 52% | 50% | -4% |
| Energy | 25 | 56% | 36% | 56% | 40% | 33% | +0% |
| Financial | 25 | 52% | 36% | 32% | 48% | 50% | +2% |
| Healthcare | 23 | 48% | 30% | 39% | 48% | 36% | +0% |
| Industrials | 25 | 36% | 32% | 44% | 40% | 46% | -10% |
| Real Estate | 25 | 48% | 28% | 28% | 64% | 42% | -16% |
| Technology | 23 | 35% | 44% | 65% | 30% | 46% | -30% |
| Utilities | 23 | 44% | 30% | 35% | 61% | 59% | -17% |

