# Paper trading — Futubull-fee simulation

As of **2026-08-18** · $10,000 starting capital per sleeve · fees per `00_grounding/futubull_fees.json`

Sleeves: `{horizon}_top` = top-N overall buys, `{horizon}_size` = top 3 per size bucket. Entries/exits at signal-day close; hold while the book keeps recommending.

| Sleeve | Equity | Return | Cash | Open pos | Trades | Fees paid | Realized P/L | Win rate |
|--------|--------|--------|------|----------|--------|-----------|--------------|----------|
| 1d_top | $10,248.33 | +2.48% | $301.47 | 10 | 22 | $68.44 | $+68.05 | 16.7% |
| 1d_size | $10,151.49 | +1.51% | $152.38 | 9 | 57 | $175.04 | $+94.70 | 50.0% |
| 3d_top | $10,137.89 | +1.38% | $374.10 | 10 | 30 | $92.70 | $+87.24 | 40.0% |
| 3d_size | $9,954.88 | -0.45% | $129.23 | 9 | 61 | $221.05 | $-30.47 | 42.3% |
| 1w_top | $10,014.98 | +0.15% | $1,172.43 | 10 | 36 | $108.25 | $+19.10 | 46.2% |
| 1w_size | $10,212.42 | +2.12% | $419.51 | 9 | 63 | $183.77 | $+236.56 | 55.6% |
| 2w_top | $9,965.66 | -0.34% | $1,761.61 | 10 | 36 | $107.43 | $-14.29 | 53.8% |
| 2w_size | $10,075.01 | +0.75% | $336.43 | 9 | 61 | $189.64 | $+83.45 | 50.0% |
| 1m_top | $9,770.72 | -2.29% | $1,498.76 | 10 | 36 | $95.40 | $-209.20 | 30.8% |
| 1m_size | $9,693.57 | -3.06% | $379.64 | 9 | 59 | $222.18 | $-293.26 | 36.0% |

Equity curves + positions: `dashboard/index.html`
