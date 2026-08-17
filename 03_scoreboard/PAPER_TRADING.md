# Paper trading — Futubull-fee simulation

As of **2026-08-17** · $10,000 starting capital per sleeve · fees per `00_grounding/futubull_fees.json`

Sleeves: `{horizon}_top` = top-N overall buys, `{horizon}_size` = top 3 per size bucket. Entries/exits at signal-day close; hold while the book keeps recommending.

| Sleeve | Equity | Return | Cash | Open pos | Trades | Fees paid | Realized P/L | Win rate |
|--------|--------|--------|------|----------|--------|-----------|--------------|----------|
| 1d_top | $10,123.38 | +1.23% | $84.10 | 10 | 22 | $70.08 | $+145.23 | 16.7% |
| 1d_size | $10,196.29 | +1.96% | $288.47 | 9 | 41 | $120.87 | $+184.33 | 31.2% |
| 3d_top | $10,129.41 | +1.29% | $47.04 | 10 | 18 | $64.05 | $+153.30 | 25.0% |
| 3d_size | $10,045.23 | +0.45% | $257.99 | 9 | 45 | $167.59 | $+69.58 | 22.2% |
| 1w_top | $10,132.58 | +1.33% | $100.55 | 10 | 16 | $60.88 | $+157.33 | 33.3% |
| 1w_size | $10,141.41 | +1.41% | $173.72 | 9 | 45 | $133.23 | $+165.91 | 27.8% |
| 2w_top | $10,153.91 | +1.54% | $127.65 | 10 | 14 | $57.18 | $+178.54 | 50.0% |
| 2w_size | $10,167.47 | +1.67% | $55.81 | 9 | 45 | $148.64 | $+192.01 | 27.8% |
| 1m_top | $9,936.97 | -0.63% | $212.95 | 10 | 14 | $45.40 | $-38.39 | 0.0% |
| 1m_size | $10,032.12 | +0.32% | $92.83 | 9 | 43 | $178.28 | $+55.93 | 23.5% |

Equity curves + positions: `dashboard/index.html`
