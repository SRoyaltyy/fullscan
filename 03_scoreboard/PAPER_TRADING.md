# Paper trading — Futubull-fee simulation

As of **2026-11-06** · $10,000 starting capital per sleeve · fees per `00_grounding/futubull_fees.json`

Latest book **2026-11-06** ranker: **weighted** (green pile liquid=0, need 8).

Sleeves: `{horizon}_top` = top-N overall buys, `{horizon}_size` = top 3 per size bucket. Fill at signal-day close. Sell only after min-hold (1d=1, 3d=3, 1w=5, 2w=10, 1m=21 **trading sessions** — weekends and NYSE holidays do not count) AND the name has left the book.

| Sleeve | Equity | Return | Cash | Open pos | Trades | Fees paid | Realized P/L | Unrealized P/L | Closed win | Open win |
|--------|--------|--------|------|----------|--------|-----------|--------------|----------------|------------|----------|
| 1d_top | $9,362.42 | -6.38% | $725.95 | 8 | 536 | $1,134.27 | $-621.04 | $-16.54 | 33.0% | 0.0% |
| 1d_size | $10,024.32 | +0.24% | $400.32 | 6 | 486 | $1,129.38 | $+37.00 | $-12.69 | 37.9% | 0.0% |
| 3d_top | $9,371.64 | -6.28% | $152.56 | 14 | 546 | $1,099.15 | $-483.77 | $-144.60 | 33.5% | 14.3% |
| 3d_size | $9,986.00 | -0.14% | $89.78 | 13 | 515 | $1,063.69 | $+189.21 | $-203.21 | 34.3% | 15.4% |
| 1w_top | $9,621.91 | -3.78% | $82.59 | 13 | 479 | $905.16 | $-228.93 | $-149.17 | 26.2% | 15.4% |
| 1w_size | $9,339.58 | -6.60% | $90.16 | 13 | 467 | $874.92 | $-469.63 | $-190.78 | 28.6% | 15.4% |
| 2w_top | $9,854.58 | -1.45% | $118.49 | 14 | 330 | $496.58 | $-37.00 | $-108.42 | 39.9% | 14.3% |
| 2w_size | $10,037.08 | +0.37% | $140.77 | 13 | 343 | $529.12 | $+235.31 | $-198.23 | 38.2% | 15.4% |
| 1m_top | $9,064.78 | -9.35% | $121.36 | 14 | 256 | $276.48 | $-833.17 | $-102.05 | 38.8% | 14.3% |
| 1m_size | $9,738.67 | -2.61% | $82.07 | 10 | 218 | $321.98 | $-63.68 | $-197.66 | 39.4% | 20.0% |

Equity curves + positions: `dashboard/index.html`
