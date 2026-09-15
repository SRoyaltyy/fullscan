# Paper trading — Futubull-fee simulation

As of **2026-09-15** · $10,000 starting capital per sleeve · fees per `00_grounding/futubull_fees.json`

Latest book **2026-09-15** ranker: **weighted** (green pile liquid=0, need 8).

Sleeves: `{horizon}_top` = top-N overall buys, `{horizon}_size` = top 3 per size bucket. Fill at signal-day close. Sell only after min-hold (1d=1, 3d=3, 1w=5, 2w=10, 1m=21 **trading sessions** — weekends and NYSE holidays do not count) AND the name has left the book.

| Sleeve | Equity | Return | Cash | Open pos | Trades | Fees paid | Realized P/L | Unrealized P/L | Closed win | Open win |
|--------|--------|--------|------|----------|--------|-----------|--------------|----------------|------------|----------|
| 1d_top | $9,619.15 | -3.81% | $2,360.55 | 7 | 263 | $575.22 | $-361.40 | $-19.44 | 34.4% | 0.0% |
| 1d_size | $10,030.02 | +0.30% | $1,948.20 | 5 | 243 | $628.55 | $+47.77 | $-17.75 | 36.1% | 0.0% |
| 3d_top | $9,584.77 | -4.15% | $1,113.25 | 27 | 261 | $547.78 | $-378.41 | $-36.82 | 31.6% | 3.7% |
| 3d_size | $10,302.88 | +3.03% | $217.21 | 28 | 258 | $550.60 | $+353.01 | $-50.13 | 27.8% | 3.6% |
| 1w_top | $10,153.11 | +1.53% | $772.04 | 41 | 209 | $413.52 | $+217.71 | $-64.59 | 25.0% | 2.4% |
| 1w_size | $10,319.34 | +3.19% | $466.92 | 44 | 214 | $416.77 | $+399.23 | $-79.90 | 27.1% | 2.3% |
| 2w_top | $10,239.88 | +2.40% | $301.51 | 41 | 93 | $176.34 | $+282.70 | $-42.82 | 50.0% | 2.4% |
| 2w_size | $10,977.75 | +9.78% | $316.32 | 43 | 117 | $186.71 | $+1,044.26 | $-66.52 | 48.6% | 2.3% |
| 1m_top | $9,671.46 | -3.29% | $224.15 | 47 | 63 | $98.37 | $-289.99 | $-38.55 | 37.5% | 2.1% |
| 1m_size | $10,687.52 | +6.88% | $208.52 | 38 | 68 | $121.86 | $+744.36 | $-56.84 | 53.3% | 2.6% |

Equity curves + positions: `dashboard/index.html`
