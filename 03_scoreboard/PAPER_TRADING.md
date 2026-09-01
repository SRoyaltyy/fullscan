# Paper trading — Futubull-fee simulation

As of **2026-09-01** · $10,000 starting capital per sleeve · fees per `00_grounding/futubull_fees.json`

Latest book **2026-09-01** ranker: **green pile** (50 liquid names). Paper buys the pile 15, not the old weighted 15.

Sleeves: `{horizon}_top` = top-N overall buys, `{horizon}_size` = top 3 per size bucket. Fill at signal-day close. Sell only after min-hold (1d=1, 3d=3, 1w=5, 2w=10, 1m=21 **trading sessions** — weekends and NYSE holidays do not count) AND the name has left the book.

| Sleeve | Equity | Return | Cash | Open pos | Trades | Fees paid | Realized P/L | Unrealized P/L | Closed win | Open win |
|--------|--------|--------|------|----------|--------|-----------|--------------|----------------|------------|----------|
| 1d_top | $10,073.93 | +0.74% | $290.39 | 10 | 128 | $286.38 | $+92.68 | $-18.74 | 45.8% | 0.0% |
| 1d_size | $10,564.21 | +5.64% | $81.16 | 6 | 136 | $384.73 | $+568.81 | $-4.60 | 50.8% | 16.7% |
| 3d_top | $10,013.18 | +0.13% | $122.39 | 8 | 106 | $241.06 | $-7.04 | $+20.21 | 42.9% | 50.0% |
| 3d_size | $10,825.11 | +8.25% | $80.78 | 10 | 98 | $227.44 | $+802.33 | $+22.78 | 45.5% | 40.0% |
| 1w_top | $10,839.42 | +8.39% | $115.87 | 15 | 75 | $172.01 | $+864.29 | $-24.87 | 36.7% | 40.0% |
| 1w_size | $10,770.43 | +7.70% | $90.35 | 16 | 80 | $164.66 | $+899.67 | $-129.24 | 50.0% | 31.2% |
| 2w_top | $10,861.07 | +8.61% | $91.96 | 17 | 43 | $109.50 | $+856.17 | $+4.90 | 76.9% | 47.1% |
| 2w_size | $10,774.10 | +7.74% | $108.83 | 19 | 53 | $94.42 | $+943.94 | $-169.84 | 64.7% | 21.1% |
| 1m_top | $9,924.79 | -0.75% | $53.18 | 23 | 23 | $36.05 | $+0.00 | $-75.21 | — | 34.8% |
| 1m_size | $10,431.79 | +4.32% | $24.26 | 22 | 22 | $34.03 | $+0.00 | $+431.79 | — | 36.4% |

Equity curves + positions: `dashboard/index.html`
