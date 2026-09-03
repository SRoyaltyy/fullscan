# Mover paper trading

_Generated 2026-09-03T07:35:35 — calls 2026-08-13 → 2026-09-03_

Trades the **gated** mover calls (featured preset + SPY down-streak ≥ 3 SELL veto), top 10 per side by conviction, 5% of equity per trade, 1-session hold (09:30 open → next session close), Futubull fees, shorts screened below $5 with a 1%/yr borrow charge. Trades that don't fit the cash / margin available are **skipped and logged** — this is a cash-accounted book, not a theoretical fill-everything tally.

## Headline

| Start capital | Final equity | Return | Max DD | Trades | Skipped | Win rate |
|---:|---:|---:|---:|---:|---:|---:|
| $100,000 | $98,752.02 | **-1.25%** | 2.24% | 117 | 3589 | 50.4% |

| Side | Trades | Win rate | P&L |
|---|---:|---:|---:|
| BUY (long) | 26 | 38.5% | $-543.35 |
| SELL (short) | 91 | 53.8% | $-734.01 |

Full detail: `data/mover_paper/trades.csv`, `skipped.csv`, `equity_curve.csv`. Dashboard: `dashboard/mover-paper/index.html`.

## Last 20 filled trades

| Date | Ticker | Side | Shares | Entry 09:30 | Exit close | P&L | Ret | Why |
|---|---|---|---:|---:|---:|---:|---:|---|
| 2026-09-02 | `CBT` | SELL | 61 | $81.47 | $82.84 | $-88.10 | -1.77% | fade: 🚨+heat🔴 |
| 2026-09-02 | `CLSK` | SELL | 444 | $11.32 | $11.33 | $-16.15 | -0.32% | fade: 🚨+heat🔴 |
| 2026-09-02 | `DAR` | BUY | 75 | $66.33 | $67.62 | $92.27 | 1.85% | lane=probable |
| 2026-09-02 | `GIS` | BUY | 121 | $41.56 | $40.58 | $-123.34 | -2.45% | lane=probable |
| 2026-09-02 | `MKC` | BUY | 92 | $54.50 | $53.80 | $-68.99 | -1.38% | lane=probable |
| 2026-09-02 | `SJM` | BUY | 38 | $132.21 | $131.44 | $-33.51 | -0.67% | lane=probable |
| 2026-09-02 | `FRPT` | BUY | 71 | $70.07 | $70.76 | $44.54 | 0.9% | lane=probable |
| 2026-09-02 | `NOMD` | BUY | 430 | $11.69 | $11.59 | $-54.21 | -1.08% | lane=probable |
| 2026-09-02 | `NU` | BUY | 349 | $14.43 | $15.40 | $329.43 | 6.54% | lane=probable |
| 2026-08-18 | `EOG` | SELL | 33 | $148.04 | $148.96 | $-34.72 | -0.71% | fade: first crack [force-closed] |
| 2026-08-24 | `AA` | SELL | 96 | $51.50 | $51.05 | $38.45 | 0.78% | fade: first crack [force-closed] |
| 2026-08-24 | `AG` | SELL | 232 | $21.47 | $21.20 | $56.44 | 1.13% | fade: first crack [force-closed] |
| 2026-08-24 | `AGI` | SELL | 130 | $38.15 | $36.13 | $257.64 | 5.19% | fade: first crack [force-closed] |
| 2026-08-24 | `AJG` | SELL | 18 | $265.00 | $264.46 | $5.46 | 0.11% | fade: first crack [force-closed] |
| 2026-08-24 | `APAM` | SELL | 118 | $42.00 | $41.17 | $93.06 | 1.88% | fade: first crack [force-closed] |
| 2026-08-24 | `ARCT` | SELL | 376 | $13.26 | $16.74 | $-1,318.42 | -26.44% | fade: first crack [force-closed] |
| 2026-08-24 | `ATRC` | SELL | 100 | $49.53 | $52.59 | $-310.77 | -6.27% | fade: first crack [force-closed] |
| 2026-08-24 | `AUGO` | SELL | 55 | $89.87 | $84.60 | $285.36 | 5.77% | fade: first crack [force-closed] |
| 2026-08-24 | `AUPH` | SELL | 300 | $16.60 | $16.29 | $85.03 | 1.71% | fade: first crack [force-closed] |
| 2026-08-24 | `BNTX` | SELL | 43 | $114.11 | $103.81 | $438.48 | 8.94% | fade: first crack [force-closed] |

