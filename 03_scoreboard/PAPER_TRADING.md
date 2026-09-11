# Paper trading — Futubull-fee simulation

As of **2026-09-11** · $10,000 starting capital per sleeve · fees per `00_grounding/futubull_fees.json`

Latest book **2026-09-11** ranker: **weighted** (green pile liquid=0, need 8).

Sleeves: `{horizon}_top` = top-N overall buys, `{horizon}_size` = top 3 per size bucket. Fill at signal-day close. Sell only after min-hold (1d=1, 3d=3, 1w=5, 2w=10, 1m=21 **trading sessions** — weekends and NYSE holidays do not count) AND the name has left the book.

| Sleeve | Equity | Return | Cash | Open pos | Trades | Fees paid | Realized P/L | Unrealized P/L | Closed win | Open win |
|--------|--------|--------|------|----------|--------|-----------|--------------|----------------|------------|----------|
| 1d_top | $9,782.86 | -2.17% | $3,318.22 | 4 | 236 | $511.60 | $-208.28 | $-8.86 | 35.3% | 0.0% |
| 1d_size | $10,178.90 | +1.79% | $3,350.81 | 4 | 220 | $567.30 | $+188.00 | $-9.10 | 37.0% | 0.0% |
| 3d_top | $9,668.65 | -3.31% | $2,352.24 | 25 | 213 | $450.81 | $-285.85 | $-45.50 | 34.0% | 0.0% |
| 3d_size | $10,494.09 | +4.94% | $2,321.93 | 23 | 203 | $440.53 | $+538.42 | $-44.32 | 32.2% | 0.0% |
| 1w_top | $10,349.83 | +3.50% | $2,485.52 | 39 | 163 | $326.80 | $+420.47 | $-70.63 | 27.4% | 0.0% |
| 1w_size | $10,511.09 | +5.11% | $2,199.51 | 36 | 168 | $327.77 | $+582.18 | $-71.09 | 31.8% | 0.0% |
| 2w_top | $10,742.10 | +7.42% | $632.98 | 21 | 67 | $128.47 | $+762.07 | $-19.98 | 52.2% | 0.0% |
| 2w_size | $11,026.90 | +10.27% | $3,933.05 | 29 | 87 | $152.64 | $+1,064.16 | $-37.25 | 62.1% | 0.0% |
| 1m_top | $9,963.99 | -0.36% | $58.50 | 28 | 28 | $36.01 | $+0.00 | $-36.01 | — | 0.0% |
| 1m_size | $9,965.95 | -0.34% | $23.00 | 23 | 23 | $34.05 | $+0.00 | $-34.05 | — | 0.0% |

Equity curves + positions: `dashboard/index.html`
