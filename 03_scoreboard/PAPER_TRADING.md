# Paper trading — Futubull-fee simulation

As of **2026-08-30** · $10,000 starting capital per sleeve · fees per `00_grounding/futubull_fees.json`

Latest book **2026-08-30** ranker: **weighted** (green pile liquid=0, need 8).

Sleeves: `{horizon}_top` = top-N overall buys, `{horizon}_size` = top 3 per size bucket. Fill at signal-day close. Sell only after min-hold (1d=1, 3d=3, 1w=5, 2w=10, 1m=21 **trading sessions** — weekends and NYSE holidays do not count) AND the name has left the book.

| Sleeve | Equity | Return | Cash | Open pos | Trades | Fees paid | Realized P/L | Unrealized P/L | Closed win | Open win |
|--------|--------|--------|------|----------|--------|-----------|--------------|----------------|------------|----------|
| 1d_top | $10,116.36 | +1.16% | $71.07 | 10 | 104 | $238.21 | $+137.71 | $-21.35 | 42.6% | 0.0% |
| 1d_size | $10,776.05 | +7.76% | $530.47 | 9 | 131 | $376.11 | $+795.74 | $-19.69 | 45.9% | 0.0% |
| 3d_top | $10,079.30 | +0.79% | $156.36 | 14 | 96 | $220.94 | $+89.25 | $-9.96 | 43.9% | 7.1% |
| 3d_size | $11,083.30 | +10.83% | $532.74 | 13 | 87 | $202.59 | $+1,114.86 | $-31.56 | 48.6% | 0.0% |
| 1w_top | $10,835.41 | +8.35% | $29.13 | 14 | 68 | $162.10 | $+862.86 | $-27.45 | 37.0% | 7.1% |
| 1w_size | $10,895.16 | +8.95% | $763.00 | 13 | 77 | $164.20 | $+926.47 | $-31.31 | 50.0% | 0.0% |
| 2w_top | $11,050.99 | +10.51% | $21.27 | 25 | 31 | $82.87 | $+304.12 | $+746.87 | 33.3% | 36.0% |
| 2w_size | $10,927.35 | +9.27% | $153.21 | 22 | 44 | $91.57 | $+938.90 | $-11.56 | 63.6% | 22.7% |
| 1m_top | $10,444.18 | +4.44% | $118.94 | 20 | 20 | $35.38 | $+0.00 | $+444.18 | — | 50.0% |
| 1m_size | $11,075.13 | +10.75% | $24.26 | 22 | 22 | $34.03 | $+0.00 | $+1,075.13 | — | 68.2% |

Equity curves + positions: `dashboard/index.html`
