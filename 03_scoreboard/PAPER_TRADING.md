# Paper trading — Futubull-fee simulation

As of **2026-09-03** · $10,000 starting capital per sleeve · fees per `00_grounding/futubull_fees.json`

Latest book **2026-09-03** ranker: **green pile** (188 liquid names). Paper buys the pile 15, not the old weighted 15.

Sleeves: `{horizon}_top` = top-N overall buys, `{horizon}_size` = top 3 per size bucket. Fill at signal-day close. Sell only after min-hold (1d=1, 3d=3, 1w=5, 2w=10, 1m=21 **trading sessions** — weekends and NYSE holidays do not count) AND the name has left the book.

| Sleeve | Equity | Return | Cash | Open pos | Trades | Fees paid | Realized P/L | Unrealized P/L | Closed win | Open win |
|--------|--------|--------|------|----------|--------|-----------|--------------|----------------|------------|----------|
| 1d_top | $9,931.82 | -0.68% | $5,011.99 | 8 | 176 | $386.91 | $-40.22 | $-27.97 | 44.0% | 0.0% |
| 1d_size | $10,540.47 | +5.40% | $5,554.48 | 6 | 170 | $460.29 | $+554.52 | $-14.05 | 45.1% | 0.0% |
| 3d_top | $10,036.19 | +0.36% | $4,765.91 | 20 | 142 | $308.62 | $+81.30 | $-45.10 | 44.3% | 0.0% |
| 3d_size | $10,900.77 | +9.01% | $5,312.20 | 18 | 130 | $286.45 | $+934.50 | $-33.73 | 42.9% | 0.0% |
| 1w_top | $10,599.14 | +5.99% | $176.36 | 22 | 96 | $206.15 | $+727.89 | $-128.76 | 37.8% | 27.3% |
| 1w_size | $10,785.23 | +7.85% | $122.67 | 24 | 102 | $208.72 | $+906.70 | $-121.46 | 48.7% | 25.0% |
| 2w_top | $10,717.49 | +7.17% | $75.57 | 14 | 46 | $108.83 | $+818.07 | $-100.58 | 62.5% | 42.9% |
| 2w_size | $11,284.55 | +12.85% | $72.00 | 22 | 62 | $95.26 | $+943.81 | $+340.74 | 65.0% | 40.9% |
| 1m_top | $10,017.63 | +0.18% | $54.91 | 24 | 24 | $36.14 | $+0.00 | $+17.63 | — | 50.0% |
| 1m_size | $10,937.22 | +9.37% | $24.26 | 22 | 22 | $34.03 | $+0.00 | $+937.22 | — | 63.6% |

Equity curves + positions: `dashboard/index.html`
