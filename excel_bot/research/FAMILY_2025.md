# Family check on latest + prev tiles

_Generated 2026-09-07. Research only._

Window **2025-08-05 → 2026-09-04**. Tickers **3449**. Liquid days **607571**. 2025 days **201630**.

| family | slice | mean after fees | n | t | holdout book |
|---|---|---:|---:|---:|---:|
| CE yesterday = 1 (fresh 24d+43d low) → same-day H | holdout 2026 | +1.25% | 3907 | 13.3 | -0.11% |
| CE yesterday = 1 (fresh 24d+43d low) → same-day H | holdout 2025 | +0.69% | 1539 | 7.0 | -0.22% |
| CE yesterday = 1 (fresh 24d+43d low) → same-day H | holdout all | +1.09% | 5446 | 14.9 | -0.14% |
| CE yesterday = 1 (fresh 24d+43d low) → same-day H | discovery 2025 | +0.54% | 2372 | 5.7 | -0.23% |
| CF yesterday = 1 (24d low) → same-day H | holdout 2026 | -0.07% | 13769 | -1.7 | -0.11% |
| CF yesterday = 1 (24d low) → same-day H | holdout 2025 | -0.31% | 4243 | -4.0 | -0.22% |
| CF yesterday = 1 (24d low) → same-day H | holdout all | -0.13% | 18012 | -3.4 | -0.14% |
| CF yesterday = 1 (24d low) → same-day H | discovery 2025 | -0.38% | 6565 | -6.4 | -0.23% |
| five-cell light + green O → same-day H | holdout 2026 | -0.09% | 22831 | -2.9 | -0.11% |
| five-cell light + green O → same-day H | holdout 2025 | -0.41% | 8666 | -7.6 | -0.22% |
| five-cell light + green O → same-day H | holdout all | -0.18% | 31497 | -6.6 | -0.14% |
| five-cell light + green O → same-day H | discovery 2025 | -0.36% | 12577 | -7.4 | -0.23% |
| CE yesterday = 1 → buy open, sell ~1w later | holdout 2026 | +1.18% | 3907 | 7.3 | +0.03% |
| CE yesterday = 1 → buy open, sell ~1w later | holdout 2025 | +0.84% | 1539 | 3.6 | -0.03% |
| CE yesterday = 1 → buy open, sell ~1w later | holdout all | +1.09% | 5446 | 8.1 | +0.01% |
| CE yesterday = 1 → buy open, sell ~1w later | discovery 2025 | +0.42% | 2372 | 1.9 | -0.15% |

## Ship bar (same harden as the full mine)

| family | label | verdict | holdout | why |
|---|---|---|---:|---|
| CE yesterday = 1 (fresh 24d+43d low) → same-day H | y_h1 | **KEEP** | +1.09% (n=5446) | — |
| CF yesterday = 1 (24d low) → same-day H | y_h1 | **KILL** | -0.13% (n=18012) | disc_t, hold_t, disc_sign, hold_sign, tape, early, late, q1, y2025, y2026, no_edge, y2025_no_edge |
| five-cell light + green O → same-day H | y_h1 | **KILL** | -0.18% (n=31497) | disc_t, hold_t, disc_sign, hold_sign, tape, early, late, q1, y2025, y2026, no_edge, y2025_no_edge |
| CE yesterday = 1 → buy open, sell ~1w later | y_from_open_1w | **KEEP** | +1.09% (n=5446) | — |

CE must beat the book in **2025 and 2026**, both ticker halves, both SPY tapes, fees, no lottery. Light+O is this ColorEngine paint.

Research only. No cards.
