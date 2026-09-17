# Paper trading — Futubull-fee simulation

As of **2026-09-17** · $10,000 starting capital per sleeve · fees per `00_grounding/futubull_fees.json`

Latest book **2026-09-17** ranker: **green pile** (181 liquid names). Paper buys the pile 15, not the old weighted 15.

Sleeves: `{horizon}_top` = top-N overall buys, `{horizon}_size` = top 3 per size bucket. Fill at signal-day close. Sell only after min-hold (1d=1, 3d=3, 1w=5, 2w=10, 1m=21 **trading sessions** — weekends and NYSE holidays do not count) AND the name has left the book.

| Sleeve | Equity | Return | Cash | Open pos | Trades | Fees paid | Realized P/L | Unrealized P/L | Closed win | Open win |
|--------|--------|--------|------|----------|--------|-----------|--------------|----------------|------------|----------|
| 1d_top | $9,613.20 | -3.87% | $2,324.50 | 7 | 269 | $582.27 | $-367.07 | $-19.73 | 33.6% | 0.0% |
| 1d_size | $10,022.61 | +0.23% | $1,790.27 | 6 | 250 | $635.96 | $+41.88 | $-19.27 | 35.2% | 0.0% |
| 3d_top | $9,558.89 | -4.41% | $2,107.43 | 25 | 269 | $559.61 | $-397.10 | $-44.02 | 31.1% | 0.0% |
| 3d_size | $10,233.03 | +2.33% | $1,869.73 | 23 | 265 | $564.84 | $+276.73 | $-43.70 | 28.9% | 0.0% |
| 1w_top | $10,123.38 | +1.23% | $1,692.22 | 38 | 214 | $421.70 | $+191.38 | $-68.00 | 25.0% | 0.0% |
| 1w_size | $10,262.80 | +2.63% | $1,437.57 | 38 | 220 | $428.26 | $+323.35 | $-60.55 | 25.3% | 2.6% |
| 2w_top | $10,219.58 | +2.20% | $420.98 | 41 | 93 | $175.85 | $+282.70 | $-63.12 | 50.0% | 2.4% |
| 2w_size | $10,956.08 | +9.56% | $318.47 | 43 | 117 | $186.69 | $+1,044.26 | $-88.19 | 48.6% | 2.3% |
| 1m_top | $9,636.46 | -3.64% | $428.71 | 46 | 64 | $98.40 | $-291.21 | $-72.33 | 33.3% | 2.2% |
| 1m_size | $10,649.14 | +6.49% | $194.06 | 39 | 69 | $122.01 | $+744.36 | $-95.22 | 53.3% | 2.6% |

Equity curves + positions: `dashboard/index.html`
