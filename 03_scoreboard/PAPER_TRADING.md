# Paper trading — Futubull-fee simulation

As of **2026-11-26** · $10,000 starting capital per sleeve · fees per `00_grounding/futubull_fees.json`

Latest book **2026-11-26** ranker: **weighted** (green pile liquid=0, need 8).

Sleeves: `{horizon}_top` = top-N overall buys, `{horizon}_size` = top 3 per size bucket. Fill at signal-day close. Sell only after min-hold (1d=1, 3d=3, 1w=5, 2w=10, 1m=21 **trading sessions** — weekends and NYSE holidays do not count) AND the name has left the book.

| Sleeve | Equity | Return | Cash | Open pos | Trades | Fees paid | Realized P/L | Unrealized P/L | Closed win | Open win |
|--------|--------|--------|------|----------|--------|-----------|--------------|----------------|------------|----------|
| 1d_top | $8,458.79 | -15.41% | $84.20 | 8 | 700 | $1,505.64 | $-1,569.63 | $+28.41 | 35.0% | 62.5% |
| 1d_size | $8,940.43 | -10.60% | $137.44 | 8 | 626 | $1,480.97 | $-1,092.14 | $+32.57 | 37.5% | 62.5% |
| 3d_top | $8,935.74 | -10.64% | $38.82 | 16 | 700 | $1,308.15 | $-1,176.24 | $+111.98 | 33.9% | 68.8% |
| 3d_size | $9,285.80 | -7.14% | $100.10 | 12 | 644 | $1,236.53 | $-792.32 | $+78.12 | 34.2% | 75.0% |
| 1w_top | $9,322.71 | -6.77% | $106.21 | 17 | 567 | $1,032.41 | $-809.09 | $+131.80 | 28.4% | 88.2% |
| 1w_size | $9,128.36 | -8.72% | $137.96 | 13 | 561 | $991.41 | $-1,025.47 | $+153.84 | 31.0% | 84.6% |
| 2w_top | $10,078.25 | +0.78% | $108.30 | 22 | 380 | $554.45 | $-263.19 | $+341.45 | 37.4% | 77.3% |
| 2w_size | $10,464.69 | +4.65% | $59.52 | 20 | 392 | $571.39 | $-98.59 | $+563.28 | 36.0% | 80.0% |
| 1m_top | $9,320.52 | -6.79% | $42.12 | 23 | 265 | $277.29 | $-833.17 | $+153.69 | 38.8% | 34.8% |
| 1m_size | $10,265.86 | +2.66% | $33.18 | 19 | 227 | $322.49 | $-63.68 | $+329.54 | 39.4% | 52.6% |

Equity curves + positions: `dashboard/index.html`
