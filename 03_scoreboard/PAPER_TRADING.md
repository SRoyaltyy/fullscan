# Paper trading — Futubull-fee simulation

As of **2026-09-09** · $10,000 starting capital per sleeve · fees per `00_grounding/futubull_fees.json`

Latest book **2026-09-09** ranker: **weighted** (green pile liquid=0, need 8).

Sleeves: `{horizon}_top` = top-N overall buys, `{horizon}_size` = top 3 per size bucket. Fill at signal-day close. Sell only after min-hold (1d=1, 3d=3, 1w=5, 2w=10, 1m=21 **trading sessions** — weekends and NYSE holidays do not count) AND the name has left the book.

| Sleeve | Equity | Return | Cash | Open pos | Trades | Fees paid | Realized P/L | Unrealized P/L | Closed win | Open win |
|--------|--------|--------|------|----------|--------|-----------|--------------|----------------|------------|----------|
| 1d_top | $9,811.70 | -1.88% | $4,403.17 | 3 | 235 | $510.50 | $-181.45 | $-6.85 | 36.2% | 0.0% |
| 1d_size | $10,246.19 | +2.46% | $4,314.70 | 3 | 219 | $566.50 | $+253.29 | $-7.10 | 37.0% | 0.0% |
| 3d_top | $9,829.20 | -1.71% | $1,397.94 | 27 | 205 | $420.53 | $-139.37 | $-31.43 | 34.8% | 3.7% |
| 3d_size | $10,686.26 | +6.86% | $1,194.21 | 26 | 188 | $385.67 | $+709.98 | $-23.72 | 34.6% | 3.8% |
| 1w_top | $10,412.28 | +4.12% | $3,303.23 | 34 | 150 | $295.79 | $+468.08 | $-55.80 | 31.0% | 2.9% |
| 1w_size | $10,543.33 | +5.43% | $2,837.10 | 30 | 156 | $303.90 | $+600.33 | $-57.00 | 34.9% | 0.0% |
| 2w_top | $10,741.99 | +7.42% | $638.25 | 21 | 67 | $128.42 | $+762.07 | $-20.08 | 52.2% | 4.8% |
| 2w_size | $11,026.26 | +10.26% | $3,960.74 | 29 | 87 | $152.97 | $+1,064.16 | $-37.89 | 62.1% | 0.0% |
| 1m_top | $9,963.51 | -0.36% | $39.76 | 27 | 27 | $36.19 | $+0.00 | $-36.49 | — | 0.0% |
| 1m_size | $9,965.94 | -0.34% | $23.00 | 23 | 23 | $34.05 | $+0.00 | $-34.06 | — | 0.0% |

Equity curves + positions: `dashboard/index.html`
