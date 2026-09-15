# Paper trading — Futubull-fee simulation

As of **2026-09-15** · $10,000 starting capital per sleeve · fees per `00_grounding/futubull_fees.json`

Latest book **2026-09-15** ranker: **weighted** (green pile liquid=0, need 8).

Sleeves: `{horizon}_top` = top-N overall buys, `{horizon}_size` = top 3 per size bucket. Fill at signal-day close. Sell only after min-hold (1d=1, 3d=3, 1w=5, 2w=10, 1m=21 **trading sessions** — weekends and NYSE holidays do not count) AND the name has left the book.

| Sleeve | Equity | Return | Cash | Open pos | Trades | Fees paid | Realized P/L | Unrealized P/L | Closed win | Open win |
|--------|--------|--------|------|----------|--------|-----------|--------------|----------------|------------|----------|
| 1d_top | $9,588.77 | -4.11% | $7,285.07 | 4 | 274 | $605.61 | $-402.97 | $-8.27 | 32.6% | 0.0% |
| 1d_size | $10,002.77 | +0.03% | $7,555.21 | 2 | 252 | $655.80 | $+7.09 | $-4.32 | 34.4% | 0.0% |
| 3d_top | $9,536.36 | -4.64% | $3,518.02 | 22 | 278 | $581.84 | $-422.66 | $-40.99 | 28.9% | 0.0% |
| 3d_size | $10,258.66 | +2.59% | $4,356.57 | 19 | 277 | $590.50 | $+294.93 | $-36.27 | 24.8% | 0.0% |
| 1w_top | $10,130.43 | +1.30% | $2,216.52 | 37 | 225 | $442.57 | $+196.90 | $-66.48 | 24.5% | 0.0% |
| 1w_size | $10,285.94 | +2.86% | $2,095.00 | 34 | 230 | $450.28 | $+352.10 | $-66.17 | 24.5% | 0.0% |
| 2w_top | $10,246.52 | +2.47% | $398.21 | 41 | 95 | $176.16 | $+281.87 | $-35.35 | 48.1% | 24.4% |
| 2w_size | $10,969.32 | +9.69% | $299.94 | 42 | 120 | $187.22 | $+1,041.59 | $-72.26 | 46.2% | 19.0% |
| 1m_top | $9,657.89 | -3.42% | $404.07 | 47 | 69 | $99.50 | $-290.53 | $-51.58 | 36.4% | 14.9% |
| 1m_size | $10,680.38 | +6.80% | $211.20 | 37 | 69 | $121.91 | $+743.81 | $-63.43 | 50.0% | 16.2% |

Equity curves + positions: `dashboard/index.html`
