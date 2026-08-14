# Paper trading — Futubull-fee simulation

As of **2026-08-14** · $10,000 starting capital per sleeve · fees per `00_grounding/futubull_fees.json`

Sleeves: `{horizon}_top` = top-N overall buys, `{horizon}_size` = top 3 per size bucket. Entries/exits at signal-day close; hold while the book keeps recommending.

| Sleeve | Equity | Return | Cash | Open pos | Trades | Fees paid | Realized P/L | Win rate |
|--------|--------|--------|------|----------|--------|-----------|--------------|----------|
| 1d_top | $10,100.07 | +1.00% | $929.70 | 10 | 14 | $52.32 | $+120.27 | 50.0% |
| 1d_size | $10,152.23 | +1.52% | $269.87 | 9 | 25 | $80.64 | $+145.21 | 62.5% |
| 3d_top | $10,116.27 | +1.16% | $8,495.97 | 2 | 6 | $36.12 | $+120.27 | 50.0% |
| 3d_size | $10,037.21 | +0.37% | $350.23 | 9 | 27 | $103.72 | $+76.15 | 44.4% |
| 1w_top | $10,118.28 | +1.18% | $9,390.30 | 1 | 5 | $34.11 | $+120.27 | 50.0% |
| 1w_size | $10,108.33 | +1.08% | $324.66 | 9 | 27 | $87.12 | $+130.20 | 55.6% |
| 2w_top | $10,147.57 | +1.48% | $9,136.79 | 1 | 3 | $30.19 | $+149.90 | 100.0% |
| 2w_size | $10,173.94 | +1.74% | $695.69 | 9 | 27 | $94.10 | $+202.93 | 55.6% |
| 1m_top | $9,962.05 | -0.38% | $8,965.53 | 1 | 3 | $12.58 | $-29.63 | 0.0% |
| 1m_size | $10,056.55 | +0.57% | $24.77 | 9 | 25 | $107.03 | $+110.21 | 50.0% |

Equity curves + positions: `dashboard/index.html`
