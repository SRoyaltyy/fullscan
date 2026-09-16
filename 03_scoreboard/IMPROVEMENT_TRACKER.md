# Improvement tracker — 2026-09-16T18:55:52-04:00

Rolling direction/magnitude hit of the *shipped* prediction vs three naive baselines computed on the same graded runs (always up, always down, same direction as the previous graded session of that topic). `edge` = engine direction hit minus the best baseline over the same window. Sessions are dated by the predicted session; sectors are pooled (11 per day).

## General market (SPX) — 29 graded runs over 29 sessions

**Read:** improving; not beating best baseline by -20% (last 10 sessions)

### Eras

| Era | n | engine dir | engine mag | always up | always down | same as yesterday | edge vs best baseline |
|---|---:|---:|---:|---:|---:|---:|---:|
| all graded | 29 | **55%** | 52% | 41% | 55% | 50% | **+0%** |
| legacy engine era | 26 | **54%** | 46% | 46% | 50% | 48% | **+4%** |
| v2 engine live (since 2026-09-14) | 3 | **67%** | 100% | 0% | 100% | 67% | **-33%** |

Walk-forward replay estimate for v2 on the same history: **71%** direction / 57% magnitude (n=28, `REPLAY_HARNESS.md` 2026-09-12). The live v2 curve above should converge toward this as sessions accumulate; if it sits well below it for 20+ sessions, the anchor inputs or the LLM components changed.

### Cumulative since v2 went live

| # | Session | topic | cum dir | cum mag |
|---:|---|---|---:|---:|
| 1 | 2026-09-14 | general | 100% | 100% |
| 2 | 2026-09-15 | general | 100% | 100% |
| 3 | 2026-09-16 | general | 67% | 100% |

### Session curve (last 30 sessions)

| Session | engine | n | session dir | dir (10) | mag (10) | up (10) | down (10) | yest (10) | edge (10) | dir (20) | edge (20) |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-07-31 | legacy | 1 | 0% | **0%** | 100% | 100% | 0% | — | -100% | 0% | -100% |
| 2026-08-03 | legacy | 1 | 100% | **50%** | 50% | 100% | 0% | 100% | -50% | 50% | -50% |
| 2026-08-04 | legacy | 1 | 100% | **67%** | 33% | 100% | 0% | 100% | -33% | 67% | -33% |
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

## Sectors (11 ETFs pooled) — 213 graded runs over 20 sessions

**Read:** slipping; not beating best baseline by -17% (last 10 sessions)

### Eras

| Era | n | engine dir | engine mag | always up | always down | same as yesterday | edge vs best baseline |
|---|---:|---:|---:|---:|---:|---:|---:|
| all graded | 213 | **46%** | 32% | 39% | 54% | 45% | **-8%** |
| legacy engine era | 183 | **46%** | 31% | 43% | 51% | 46% | **-6%** |
| v2 engine live (since 2026-09-14) | 30 | **47%** | 37% | 17% | 67% | 37% | **-20%** |

Walk-forward replay estimate for v2 on the same history: **61%** direction / 48% magnitude (n=183, `REPLAY_HARNESS.md` 2026-09-12). The live v2 curve above should converge toward this as sessions accumulate; if it sits well below it for 20+ sessions, the anchor inputs or the LLM components changed.

### Cumulative since v2 went live

| # | Session | topic | cum dir | cum mag |
|---:|---|---|---:|---:|
| 1 | 2026-09-14 | sector:Basic Materials | 100% | 0% |
| 2 | 2026-09-14 | sector:Consumer Cyclical | 50% | 0% |
| 3 | 2026-09-14 | sector:Consumer Defensive | 67% | 0% |
| 4 | 2026-09-14 | sector:Energy | 50% | 25% |
| 5 | 2026-09-14 | sector:Financial | 60% | 40% |
| 6 | 2026-09-14 | sector:Healthcare | 50% | 33% |
| 7 | 2026-09-14 | sector:Industrials | 57% | 43% |
| 8 | 2026-09-14 | sector:Real Estate | 62% | 50% |
| 9 | 2026-09-14 | sector:Technology | 67% | 44% |
| 10 | 2026-09-14 | sector:Utilities | 70% | 40% |
| 11 | 2026-09-15 | sector:Basic Materials | 64% | 46% |
| 12 | 2026-09-15 | sector:Communication Services | 58% | 50% |
| 13 | 2026-09-15 | sector:Consumer Cyclical | 62% | 46% |
| 14 | 2026-09-15 | sector:Consumer Defensive | 57% | 43% |
| 15 | 2026-09-15 | sector:Energy | 60% | 40% |
| 16 | 2026-09-15 | sector:Financial | 62% | 44% |
| 17 | 2026-09-15 | sector:Healthcare | 59% | 41% |
| 18 | 2026-09-15 | sector:Industrials | 61% | 44% |
| 19 | 2026-09-15 | sector:Real Estate | 63% | 42% |
| 20 | 2026-09-16 | sector:Basic Materials | 60% | 40% |
| 21 | 2026-09-16 | sector:Communication Services | 57% | 43% |
| 22 | 2026-09-16 | sector:Consumer Cyclical | 55% | 41% |
| 23 | 2026-09-16 | sector:Consumer Defensive | 52% | 39% |
| 24 | 2026-09-16 | sector:Energy | 54% | 38% |
| 25 | 2026-09-16 | sector:Financial | 52% | 36% |
| 26 | 2026-09-16 | sector:Healthcare | 50% | 35% |
| 27 | 2026-09-16 | sector:Industrials | 52% | 37% |
| 28 | 2026-09-16 | sector:Real Estate | 50% | 36% |
| 29 | 2026-09-16 | sector:Technology | 48% | 38% |
| 30 | 2026-09-16 | sector:Utilities | 47% | 37% |

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

### Per sector (all graded)

| Sector | n | dir | mag | always up | always down | yest | edge |
|---|---:|---:|---:|---:|---:|---:|---:|
| Basic Materials | 20 | 50% | 45% | 35% | 60% | 42% | -10% |
| Communication Services | 19 | 26% | 32% | 42% | 58% | 50% | -32% |
| Consumer Cyclical | 20 | 60% | 20% | 25% | 70% | 53% | -10% |
| Consumer Defensive | 20 | 45% | 35% | 40% | 55% | 53% | -10% |
| Energy | 20 | 50% | 35% | 60% | 35% | 32% | -10% |
| Financial | 20 | 45% | 30% | 40% | 50% | 47% | -5% |
| Healthcare | 18 | 50% | 22% | 33% | 50% | 41% | +0% |
| Industrials | 20 | 40% | 25% | 35% | 50% | 42% | -10% |
| Real Estate | 20 | 50% | 30% | 25% | 65% | 47% | -15% |
| Technology | 18 | 44% | 44% | 56% | 39% | 29% | -11% |
| Utilities | 18 | 44% | 33% | 39% | 56% | 59% | -14% |

