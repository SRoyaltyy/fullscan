# Paper trading — Futubull-fee simulation

As of **2026-11-27** · $10,000 starting capital per sleeve · fees per `00_grounding/futubull_fees.json`

Latest book **2026-11-27** ranker: **weighted** (green pile liquid=0, need 8).

Sleeves: `{horizon}_top` = top-N overall buys, `{horizon}_size` = top 3 per size bucket. Fill at signal-day close. Sell only after min-hold (1d=1, 3d=3, 1w=5, 2w=10, 1m=21 **trading sessions** — weekends and NYSE holidays do not count) AND the name has left the book.

| Sleeve | Equity | Return | Cash | Open pos | Trades | Fees paid | Realized P/L | Unrealized P/L | Closed win | Open win |
|--------|--------|--------|------|----------|--------|-----------|--------------|----------------|------------|----------|
| 1d_top | $8,450.32 | -15.50% | $298.49 | 8 | 704 | $1,514.11 | $-1,575.71 | $+26.03 | 35.1% | 50.0% |
| 1d_size | $8,929.83 | -10.70% | $200.41 | 7 | 631 | $1,491.57 | $-1,091.24 | $+21.06 | 37.8% | 42.9% |
| 3d_top | $8,935.74 | -10.64% | $38.82 | 16 | 700 | $1,308.15 | $-1,176.24 | $+111.98 | 33.9% | 68.8% |
| 3d_size | $9,285.46 | -7.15% | $67.90 | 13 | 645 | $1,236.86 | $-792.32 | $+77.78 | 34.2% | 69.2% |
| 1w_top | $9,319.54 | -6.80% | $25.84 | 17 | 573 | $1,035.58 | $-806.50 | $+126.04 | 28.8% | 70.6% |
| 1w_size | $9,126.59 | -8.73% | $5.24 | 13 | 563 | $993.18 | $-1,024.60 | $+151.20 | 31.3% | 76.9% |
| 2w_top | $10,078.25 | +0.78% | $108.30 | 22 | 380 | $554.45 | $-263.19 | $+341.45 | 37.4% | 77.3% |
| 2w_size | $10,464.64 | +4.65% | $54.16 | 21 | 393 | $571.45 | $-98.59 | $+563.23 | 36.0% | 76.2% |
| 1m_top | $9,320.52 | -6.79% | $42.12 | 23 | 265 | $277.29 | $-833.17 | $+153.69 | 38.8% | 34.8% |
| 1m_size | $10,265.86 | +2.66% | $33.18 | 19 | 227 | $322.49 | $-63.68 | $+329.54 | 39.4% | 52.6% |

Equity curves + positions: `dashboard/index.html`
