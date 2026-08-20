# Paper trading — Futubull-fee simulation

As of **2026-08-18** · $10,000 starting capital per sleeve · fees per `00_grounding/futubull_fees.json` · **4 trading session(s)**

All returns are **after Futubull fees**. `Daily after-fee` = total return / N sessions (simple average, not CAGR). `Last day` = close-to-close on the equity curve. Owner north star is ~2%/day after fees — that is a target, not a result. No sleeve is claimed to hit 2%.

Sleeves: `{horizon}_top` = top-N overall buys, `{horizon}_size` = top 3 per size bucket. Entries/exits at signal-day close; hold while the book keeps recommending.

| Sleeve | Equity | Total | Daily after-fee | Last day | vs +2%/day | Fees | Trades | Win rate |
|--------|--------|-------|-----------------|----------|------------|------|--------|----------|
| 1d_top | $10,274.69 | +2.75% | +0.69% | +0.74% | -1.31pp | $68.44 | 22 | 16.7% |
| 1w_size | $10,165.45 | +1.65% | +0.41% | -1.01% | -1.59pp | $183.51 | 63 | 44.4% |
| 1d_size | $10,135.01 | +1.35% | +0.34% | -0.91% | -1.66pp | $181.67 | 59 | 48.0% |
| 3d_top | $10,112.29 | +1.12% | +0.28% | +0.16% | -1.72pp | $92.59 | 30 | 30.0% |
| 2w_size | $10,102.58 | +1.03% | +0.26% | -1.39% | -1.74pp | $198.67 | 63 | 48.1% |
| 3d_size | $9,956.02 | -0.44% | -0.11% | -0.89% | -2.11pp | $225.18 | 63 | 44.4% |
| 1w_top | $9,956.19 | -0.44% | -0.11% | -1.53% | -2.11pp | $112.16 | 38 | 35.7% |
| 2w_top | $9,955.97 | -0.44% | -0.11% | -1.34% | -2.11pp | $107.45 | 36 | 30.8% |
| 1m_size | $9,774.07 | -2.26% | -0.56% | -1.52% | -2.56pp | $226.35 | 61 | 38.5% |
| 1m_top | $9,753.55 | -2.46% | -0.62% | -1.94% | -2.62pp | $95.34 | 36 | 30.8% |

Live page: `dashboard/index.html` (equity + per-sleeve daily after-fee vs +2%/day + buy/sell blotter + existing stock-book backtest).
