# Paper trading — Futubull-fee simulation

As of **2026-08-27** · $10,000 starting capital per sleeve · fees per `00_grounding/futubull_fees.json`

Sleeves: `{horizon}_top` = top-N overall buys, `{horizon}_size` = top 3 per size bucket. Fill at signal-day close. Sell only after min-hold (1d=1, 3d=3, 1w=5, 2w=10, 1m=21 **trading sessions** — weekends and NYSE holidays do not count) AND the name has left the book.

| Sleeve | Equity | Return | Cash | Open pos | Trades | Fees paid | Realized P/L | Unrealized P/L | Closed win | Open win |
|--------|--------|--------|------|----------|--------|-----------|--------------|----------------|------------|----------|
| 1d_top | $10,140.23 | +1.40% | $274.09 | 10 | 90 | $208.52 | $+66.15 | $+74.08 | 42.5% | 20.0% |
| 1d_size | $10,867.90 | +8.68% | $96.27 | 9 | 115 | $337.91 | $+886.70 | $-18.80 | 49.1% | 11.1% |
| 3d_top | $10,154.41 | +1.54% | $153.13 | 10 | 88 | $205.20 | $+116.63 | $+37.78 | 38.5% | 30.0% |
| 3d_size | $11,128.98 | +11.29% | $101.77 | 9 | 79 | $188.84 | $+1,144.82 | $-15.84 | 42.9% | 22.2% |
| 1w_top | $10,816.78 | +8.17% | $234.82 | 17 | 57 | $141.36 | $+679.43 | $+137.35 | 40.0% | 29.4% |
| 1w_size | $10,885.45 | +8.85% | $71.96 | 14 | 64 | $137.38 | $+878.11 | $+7.34 | 56.0% | 14.3% |
| 2w_top | $11,011.80 | +10.12% | $164.14 | 22 | 24 | $68.83 | $+416.54 | $+595.26 | 100.0% | 36.4% |
| 2w_size | $10,952.88 | +9.53% | $130.33 | 20 | 38 | $90.09 | $+972.64 | $-19.76 | 88.9% | 30.0% |
| 1m_top | $10,366.27 | +3.66% | $145.36 | 18 | 18 | $35.11 | $+0.00 | $+366.27 | — | 50.0% |
| 1m_size | $10,928.21 | +9.28% | $24.29 | 22 | 22 | $34.03 | $+0.00 | $+928.21 | — | 54.5% |

Equity curves + positions: `dashboard/index.html`
