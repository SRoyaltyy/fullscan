# Improvement tracker — 2026-09-25T16:45:08-04:00

Rolling direction/magnitude hit of the *shipped* prediction vs three naive baselines computed on the same graded runs (always up, always down, same direction as the previous graded session of that topic). `edge` = engine direction hit minus the best baseline over the same window. Sessions are dated by the predicted session; sectors are pooled (11 per day).

## General market (SPX) — 36 graded runs over 36 sessions

**Read:** improving; beating best baseline by +20% (last 10 sessions)

### Eras

| Era | n | engine dir | engine mag | always up | always down | same as yesterday | edge vs best baseline |
|---|---:|---:|---:|---:|---:|---:|---:|
| all graded | 36 | **56%** | 53% | 44% | 47% | 46% | **+8%** |
| legacy engine era | 26 | **54%** | 46% | 46% | 50% | 48% | **+4%** |
| v2 engine live (since 2026-09-14) | 10 | **60%** | 70% | 40% | 40% | 40% | **+20%** |

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
| 9 | 2026-09-24 | general | 56% | 67% |
| 10 | 2026-09-25 | general | 60% | 70% |

### Session curve (last 30 sessions)

| Session | engine | n | session dir | dir (10) | mag (10) | up (10) | down (10) | yest (10) | edge (10) | dir (20) | edge (20) |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
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
| 2026-09-24 | v2 | 1 | 0% | **50%** | 60% | 40% | 40% | 40% | +10% | 50% | +0% |
| 2026-09-25 | v2 | 1 | 100% | **60%** | 70% | 40% | 40% | 40% | +20% | 50% | +0% |

## Sectors (11 ETFs pooled) — 289 graded runs over 27 sessions

**Read:** improving; not beating best baseline by -8% (last 10 sessions)

### Eras

| Era | n | engine dir | engine mag | always up | always down | same as yesterday | edge vs best baseline |
|---|---:|---:|---:|---:|---:|---:|---:|
| all graded | 289 | **44%** | 34% | 42% | 51% | 44% | **-7%** |
| legacy engine era | 183 | **46%** | 31% | 43% | 51% | 46% | **-6%** |
| v2 engine live (since 2026-09-14) | 106 | **42%** | 40% | 40% | 50% | 41% | **-8%** |

Walk-forward replay estimate for v2 on the same history: **61%** direction / 48% magnitude (n=183, `REPLAY_HARNESS.md` 2026-09-12). The live v2 curve above should converge toward this as sessions accumulate; if it sits well below it for 20+ sessions, the anchor inputs or the LLM components changed.

### Cumulative since v2 went live

| # | Session | topic | cum dir | cum mag |
|---:|---|---|---:|---:|
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
| 86 | 2026-09-24 | sector:Basic Materials | 40% | 35% |
| 87 | 2026-09-24 | sector:Communication Services | 39% | 36% |
| 88 | 2026-09-24 | sector:Consumer Cyclical | 40% | 35% |
| 89 | 2026-09-24 | sector:Energy | 40% | 36% |
| 90 | 2026-09-24 | sector:Financial | 40% | 36% |
| 91 | 2026-09-24 | sector:Healthcare | 40% | 36% |
| 92 | 2026-09-24 | sector:Industrials | 40% | 37% |
| 93 | 2026-09-24 | sector:Real Estate | 41% | 38% |
| 94 | 2026-09-24 | sector:Technology | 42% | 37% |
| 95 | 2026-09-24 | sector:Utilities | 42% | 38% |
| 96 | 2026-09-25 | sector:Basic Materials | 42% | 38% |
| 97 | 2026-09-25 | sector:Communication Services | 41% | 38% |
| 98 | 2026-09-25 | sector:Consumer Cyclical | 41% | 38% |
| 99 | 2026-09-25 | sector:Consumer Defensive | 41% | 38% |
| 100 | 2026-09-25 | sector:Energy | 42% | 39% |
| 101 | 2026-09-25 | sector:Financial | 42% | 39% |
| 102 | 2026-09-25 | sector:Healthcare | 42% | 39% |
| 103 | 2026-09-25 | sector:Industrials | 42% | 40% |
| 104 | 2026-09-25 | sector:Real Estate | 42% | 39% |
| 105 | 2026-09-25 | sector:Technology | 42% | 39% |
| 106 | 2026-09-25 | sector:Utilities | 42% | 40% |

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
| 2026-09-24 | v2 | 10 | 70% | **42%** | 36% | 41% | 49% | 41% | -7% | 41% | -13% |
| 2026-09-25 | v2 | 11 | 36% | **42%** | 40% | 40% | 50% | 41% | -8% | 39% | -13% |

### Per sector (all graded)

| Sector | n | dir | mag | always up | always down | yest | edge |
|---|---:|---:|---:|---:|---:|---:|---:|
| Basic Materials | 27 | 41% | 37% | 41% | 56% | 38% | -15% |
| Communication Services | 26 | 23% | 31% | 38% | 62% | 48% | -38% |
| Consumer Cyclical | 27 | 48% | 15% | 30% | 63% | 42% | -15% |
| Consumer Defensive | 26 | 50% | 42% | 46% | 50% | 52% | -2% |
| Energy | 27 | 59% | 41% | 56% | 41% | 35% | +4% |
| Financial | 27 | 48% | 33% | 33% | 44% | 46% | +2% |
| Healthcare | 25 | 48% | 36% | 44% | 44% | 38% | +4% |
| Industrials | 27 | 37% | 37% | 44% | 41% | 42% | -7% |
| Real Estate | 27 | 52% | 30% | 26% | 67% | 46% | -15% |
| Technology | 25 | 36% | 40% | 64% | 32% | 42% | -28% |
| Utilities | 25 | 44% | 36% | 36% | 60% | 58% | -16% |

