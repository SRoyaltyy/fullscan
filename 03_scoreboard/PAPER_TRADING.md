# Paper trading — Futubull-fee simulation

As of **2026-08-13** · $10,000 starting capital per sleeve · fees per `00_grounding/futubull_fees.json`

Sleeves: `{horizon}_top` = top-N overall buys, `{horizon}_size` = top 3 per size bucket. Entries/exits at signal-day close; hold while the book keeps recommending.

| Sleeve | Equity | Return | Cash | Open pos | Trades | Fees paid | Realized P/L | Win rate |
|--------|--------|--------|------|----------|--------|-----------|--------------|----------|
| 1d_top | $9,979.75 | -0.20% | $801.13 | 10 | 10 | $20.25 | $+0.00 | — |
| 1d_size | $9,975.19 | -0.25% | $41.34 | 9 | 9 | $24.81 | $+0.00 | — |
| 3d_top | $9,951.34 | -0.49% | $333.28 | 10 | 10 | $48.66 | $+0.00 | — |
| 3d_size | $9,967.74 | -0.32% | $56.14 | 9 | 9 | $32.26 | $+0.00 | — |
| 1w_top | $9,953.19 | -0.47% | $330.40 | 10 | 10 | $46.81 | $+0.00 | — |
| 1w_size | $9,969.17 | -0.31% | $48.14 | 9 | 9 | $30.83 | $+0.00 | — |
| 2w_top | $9,972.45 | -0.28% | $387.65 | 10 | 10 | $27.55 | $+0.00 | — |
| 2w_size | $9,980.91 | -0.19% | $112.43 | 9 | 9 | $19.09 | $+0.00 | — |
| 1m_top | $9,964.89 | -0.35% | $83.27 | 10 | 10 | $35.11 | $+0.00 | — |
| 1m_size | $9,979.53 | -0.20% | $300.46 | 9 | 9 | $20.47 | $+0.00 | — |

Equity curves + positions: `dashboard/index.html`
