# Factor mine action — `union_cond_n4_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 4 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · top 4 by cond

Cash book **-0.18%** ($9,982) · signal-only (no cash/fees) was +5.57%. Starts YES **5/17**. Fills 46 · skips 82 · realized $-28.82.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `cond` · **top_n** 4.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $18.11.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `BTSG` | 41 | — | $59.80 | +0.00 | $60.23 | +17.63 | +17.63 | +0.00 | +17.63 |
| 2026-08-13 | `HIMS` | 84 | — | $29.74 | +0.00 | $28.77 | -81.48 | -81.48 | +0.00 | -81.48 |
| 2026-08-13 | `INO` | 3086 | — | $0.81 | +0.00 | $0.90 | +277.74 | +277.74 | +0.00 | +277.74 |
| 2026-08-13 | `IREN` | 54 | — | $45.98 | +0.00 | $44.76 | -65.88 | -65.88 | +0.00 | -65.88 |
| 2026-08-14 | `BTSG` | 41 | $60.23 | $59.65 | -23.78 | $61.71 | +84.46 | +60.68 | -6.15 | +78.31 |
| 2026-08-14 | `HIMS` | 84 | $28.77 | $29.15 | +31.92 | $28.15 | -84.00 | -52.08 | -49.56 | -133.56 |
| 2026-08-14 | `INO` | 3086 | $0.90 | $0.93 | +92.58 | $1.09 | +493.76 | +586.34 | +370.32 | +864.08 |
| 2026-08-14 | `IREN` | 54 | $44.76 | $44.09 | -36.18 | $44.06 | -1.62 | -37.80 | -102.06 | -103.68 |
| 2026-08-14 | `BTBT` | 4 | — | $1.50 | +0.00 | $1.57 | +0.28 | +0.28 | +0.00 | +0.28 |
| 2026-08-17 | `BTSG` | 41 | $61.71 | $61.69 | -0.82 | $60.38 | -53.71 | -54.53 | +77.49 | +23.78 |
| 2026-08-17 | `HIMS` | 84 | $28.15 | $28.14 | -0.84 | $28.61 | +39.48 | +38.64 | -134.40 | -94.92 |
| 2026-08-17 | `INO` | 3086 | $1.09 | $1.07 | -61.72 | $1.15 | +246.88 | +185.16 | +802.36 | +1049.24 |
| 2026-08-17 | `IREN` | 54 | $44.06 | $45.23 | +63.18 | $44.90 | -17.82 | +45.36 | -40.50 | -58.32 |
| 2026-08-17 | `BTBT` | 4 | $1.57 | $1.52 | -0.20 | $1.60 | +0.32 | +0.12 | +0.08 | +0.40 |
| 2026-08-17 | `INV` | 3 | — | $1.62 | +0.00 | $1.39 | -0.71 | -0.71 | +0.00 | -0.71 |
| 2026-08-17 | `XHG` | 1 | — | $4.19 | +0.00 | $3.91 | -0.28 | -0.28 | +0.00 | -0.28 |
| 2026-08-18 | `BTSG` | 41 | $60.38 | $60.00 | -15.58 | — | +0.00 | -15.58 | +8.20 | — |
| 2026-08-18 | `HIMS` | 84 | $28.61 | $27.85 | -63.84 | — | +0.00 | -63.84 | -158.76 | — |
| 2026-08-18 | `INO` | 3086 | $1.15 | $1.14 | -30.86 | — | +0.00 | -30.86 | +1018.38 | — |
| 2026-08-18 | `IREN` | 54 | $44.90 | $43.56 | -72.36 | — | +0.00 | -72.36 | -130.68 | — |
| 2026-08-18 | `BTBT` | 4 | $1.60 | $1.54 | -0.24 | $1.45 | -0.36 | -0.60 | +0.16 | -0.20 |
| 2026-08-18 | `INV` | 3 | $1.39 | $1.32 | -0.18 | $1.32 | +0.00 | -0.18 | -0.89 | -0.89 |
| 2026-08-18 | `XHG` | 1 | $3.91 | $3.94 | +0.03 | $4.28 | +0.34 | +0.37 | -0.25 | +0.09 |
| 2026-08-19 | `BTBT` | 4 | $1.45 | $1.42 | -0.12 | — | +0.00 | -0.12 | -0.32 | — |
| 2026-08-19 | `INV` | 3 | $1.32 | $1.39 | +0.19 | $1.54 | +0.45 | +0.64 | -0.69 | -0.24 |
| 2026-08-19 | `XHG` | 1 | $4.28 | $4.32 | +0.04 | $4.33 | +0.01 | +0.05 | +0.13 | +0.14 |
| 2026-08-20 | `INV` | 3 | $1.54 | $1.55 | +0.03 | — | +0.00 | +0.03 | -0.21 | — |
| 2026-08-20 | `XHG` | 1 | $4.33 | $4.10 | -0.23 | — | +0.00 | -0.23 | -0.09 | — |
| 2026-08-20 | `AG` | 129 | — | $20.55 | +0.00 | $21.19 | +82.56 | +82.56 | +0.00 | +82.56 |
| 2026-08-20 | `BHP` | 29 | — | $91.01 | +0.00 | $93.63 | +75.98 | +75.98 | +0.00 | +75.98 |
| 2026-08-20 | `CDE` | 128 | — | $20.65 | +0.00 | $21.11 | +58.88 | +58.88 | +0.00 | +58.88 |
| 2026-08-20 | `HDSN` | 461 | — | $5.77 | +0.00 | $5.57 | -92.20 | -92.20 | +0.00 | -92.20 |
| 2026-08-21 | `AG` | 129 | $21.19 | $21.90 | +91.59 | $21.09 | -104.49 | -12.90 | +174.15 | +69.66 |
| 2026-08-21 | `BHP` | 29 | $93.63 | $95.72 | +60.61 | $97.03 | +37.99 | +98.60 | +136.59 | +174.58 |
| 2026-08-21 | `CDE` | 128 | $21.11 | $21.75 | +81.92 | $20.97 | -99.84 | -17.92 | +140.80 | +40.96 |
| 2026-08-21 | `HDSN` | 461 | $5.57 | $5.67 | +46.10 | $5.63 | -18.44 | +27.66 | -46.10 | -64.54 |
| 2026-08-24 | `AG` | 129 | $21.09 | $21.47 | +49.02 | $20.57 | -116.10 | -67.08 | +118.68 | +2.58 |
| 2026-08-24 | `BHP` | 29 | $97.03 | $97.34 | +8.99 | $96.66 | -19.72 | -10.73 | +183.57 | +163.85 |
| 2026-08-24 | `CDE` | 128 | $20.97 | $21.26 | +37.12 | $20.49 | -98.56 | -61.44 | +78.08 | -20.48 |
| 2026-08-24 | `HDSN` | 461 | $5.63 | $5.69 | +27.66 | $5.57 | -55.32 | -27.66 | -36.88 | -92.20 |
| 2026-08-25 | `AG` | 129 | $20.57 | $20.73 | +20.64 | — | +0.00 | +20.64 | +23.22 | — |
| 2026-08-25 | `BHP` | 29 | $96.66 | $95.95 | -20.59 | — | +0.00 | -20.59 | +143.26 | — |
| 2026-08-25 | `CDE` | 128 | $20.49 | $20.85 | +46.08 | — | +0.00 | +46.08 | +25.60 | — |
| 2026-08-25 | `HDSN` | 461 | $5.57 | $5.53 | -18.44 | — | +0.00 | -18.44 | -110.64 | — |
| 2026-08-25 | `AU` | 22 | — | $119.46 | +0.00 | $118.55 | -20.02 | -20.02 | +0.00 | -20.02 |
| 2026-08-25 | `ERO` | 70 | — | $38.00 | +0.00 | $38.55 | +38.50 | +38.50 | +0.00 | +38.50 |
| 2026-08-25 | `FCX` | 34 | — | $77.90 | +0.00 | $77.49 | -13.94 | -13.94 | +0.00 | -13.94 |
| 2026-08-25 | `CNH` | 228 | — | $11.72 | +0.00 | $11.80 | +18.24 | +18.24 | +0.00 | +18.24 |
| 2026-08-26 | `AU` | 22 | $118.55 | $118.55 | +0.00 | $118.55 | +0.00 | +0.00 | -20.02 | -20.02 |
| 2026-08-26 | `ERO` | 70 | $38.55 | $38.55 | +0.00 | $38.55 | +0.00 | +0.00 | +38.50 | +38.50 |
| 2026-08-26 | `FCX` | 34 | $77.49 | $77.49 | +0.00 | $77.49 | +0.00 | +0.00 | -13.94 | -13.94 |
| 2026-08-26 | `CNH` | 228 | $11.80 | $11.80 | +0.00 | $11.80 | +0.00 | +0.00 | +18.24 | +18.24 |
| 2026-08-27 | `AU` | 22 | $118.55 | $119.80 | +27.50 | $118.11 | -37.18 | -9.68 | +7.48 | -29.70 |
| 2026-08-27 | `ERO` | 70 | $38.55 | $40.51 | +137.20 | $39.24 | -88.90 | +48.30 | +175.70 | +86.80 |
| 2026-08-27 | `FCX` | 34 | $77.49 | $79.34 | +62.90 | $79.00 | -11.56 | +51.34 | +48.96 | +37.40 |
| 2026-08-27 | `CNH` | 228 | $11.80 | $11.54 | -59.28 | $11.62 | +18.24 | -41.04 | -41.04 | -22.80 |
| 2026-08-27 | `GGB` | 4 | — | $4.42 | +0.00 | $4.46 | +0.16 | +0.16 | +0.00 | +0.16 |
| 2026-08-28 | `AU` | 22 | $118.11 | $117.41 | -15.40 | — | +0.00 | -15.40 | -45.10 | — |
| 2026-08-28 | `ERO` | 70 | $39.24 | $39.20 | -2.80 | — | +0.00 | -2.80 | +84.00 | — |
| 2026-08-28 | `FCX` | 34 | $79.00 | $78.83 | -5.78 | — | +0.00 | -5.78 | +31.62 | — |
| 2026-08-28 | `CNH` | 228 | $11.62 | $11.62 | +0.00 | — | +0.00 | +0.00 | -22.80 | — |
| 2026-08-28 | `GGB` | 4 | $4.46 | $4.57 | +0.44 | $4.70 | +0.52 | +0.96 | +0.60 | +1.12 |
| 2026-08-28 | `KEYS` | 8 | — | $323.82 | +0.00 | $325.82 | +16.00 | +16.00 | +0.00 | +16.00 |
| 2026-08-28 | `SMTC` | 17 | — | $149.40 | +0.00 | $142.43 | -118.49 | -118.49 | +0.00 | -118.49 |
| 2026-08-28 | `CIEN` | 6 | — | $411.53 | +0.00 | $399.85 | -70.08 | -70.08 | +0.00 | -70.08 |
| 2026-08-28 | `MPWR` | 2 | — | $1319.75 | +0.00 | $1311.08 | -17.34 | -17.34 | +0.00 | -17.34 |
| 2026-08-31 | `GGB` | 4 | $4.70 | $4.55 | -0.60 | $4.55 | +0.00 | -0.60 | +0.52 | +0.52 |
| 2026-08-31 | `KEYS` | 8 | $325.82 | $324.14 | -13.44 | $319.02 | -40.96 | -54.40 | +2.56 | -38.40 |
| 2026-08-31 | `SMTC` | 17 | $142.43 | $133.04 | -159.63 | $132.54 | -8.50 | -168.13 | -278.12 | -286.62 |
| 2026-08-31 | `CIEN` | 6 | $399.85 | $373.68 | -157.02 | $379.87 | +37.14 | -119.88 | -227.10 | -189.96 |
| 2026-08-31 | `MPWR` | 2 | $1311.08 | $1288.35 | -45.46 | $1270.00 | -36.70 | -82.16 | -62.80 | -99.50 |
| 2026-09-01 | `GGB` | 4 | $4.55 | $4.61 | +0.24 | — | +0.00 | +0.24 | +0.76 | — |
| 2026-09-01 | `KEYS` | 8 | $319.02 | $323.71 | +37.52 | $322.70 | -8.08 | +29.44 | -0.88 | -8.96 |
| 2026-09-01 | `SMTC` | 17 | $132.54 | $131.65 | -15.13 | $129.50 | -36.55 | -51.68 | -301.75 | -338.30 |
| 2026-09-01 | `CIEN` | 6 | $379.87 | $383.85 | +23.88 | $378.12 | -34.38 | -10.50 | -166.08 | -200.46 |
| 2026-09-01 | `MPWR` | 2 | $1270.00 | $1279.37 | +18.74 | $1253.54 | -51.66 | -32.92 | -80.76 | -132.42 |
| 2026-09-02 | `KEYS` | 8 | $322.70 | $321.47 | -9.84 | — | +0.00 | -9.84 | -18.80 | — |
| 2026-09-02 | `SMTC` | 17 | $129.50 | $127.63 | -31.79 | — | +0.00 | -31.79 | -370.09 | — |
| 2026-09-02 | `CIEN` | 6 | $378.12 | $376.89 | -7.38 | — | +0.00 | -7.38 | -207.84 | — |
| 2026-09-02 | `MPWR` | 2 | $1253.54 | $1245.11 | -16.86 | — | +0.00 | -16.86 | -149.28 | — |
| 2026-09-03 | `ARCT` | 151 | — | $16.46 | +0.00 | $16.74 | +42.28 | +42.28 | +0.00 | +42.28 |
| 2026-09-03 | `BMEA` | 1384 | — | $1.80 | +0.00 | $1.93 | +179.92 | +179.92 | +0.00 | +179.92 |
| 2026-09-03 | `CRDL` | 1154 | — | $2.16 | +0.00 | $2.17 | +11.54 | +11.54 | +0.00 | +11.54 |
| 2026-09-03 | `HRMY` | 59 | — | $41.31 | +0.00 | $42.86 | +91.45 | +91.45 | +0.00 | +91.45 |
| 2026-09-04 | `ARCT` | 151 | $16.74 | $16.77 | +4.53 | $15.56 | -182.71 | -178.18 | +46.81 | -135.90 |
| 2026-09-04 | `BMEA` | 1384 | $1.93 | $1.93 | +0.00 | $1.91 | -27.68 | -27.68 | +179.92 | +152.24 |
| 2026-09-04 | `CRDL` | 1154 | $2.17 | $2.18 | +11.54 | $2.16 | -23.08 | -11.54 | +23.08 | +0.00 |
| 2026-09-04 | `HRMY` | 59 | $42.86 | $42.93 | +4.13 | $41.86 | -63.13 | -59.00 | +95.58 | +32.45 |
| 2026-09-04 | `CABA` | 1 | — | $3.63 | +0.00 | $3.48 | -0.15 | -0.15 | +0.00 | -0.15 |
| 2026-09-04 | `ALEC` | 2 | — | $2.70 | +0.00 | $2.51 | -0.38 | -0.38 | +0.00 | -0.38 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +148.01 | BTSG, HIMS, INO, IREN | — | $26.70 | $10,107.25 | BTSG×41, HIMS×84, INO×3086, IREN×54 |
| 2026-08-14 | +5.50 | $26.70 | BTSG×41, HIMS×84, INO×3086, IREN×54 | $10,171.79 | +64.54 | +492.88 | BTBT | — | $20.63 | $10,664.60 | BTSG×41, HIMS×84, INO×3086, IREN×54, BTBT×4 |
| 2026-08-17 | +2.25 | $20.63 | BTSG×41, HIMS×84, INO×3086, IREN×54, BTBT×4 | $10,664.20 | -0.40 | +214.16 | INV, XHG | — | $11.47 | $10,878.26 | BTSG×41, HIMS×84, INO×3086, IREN×54, BTBT×4, INV×3, XHG×1 |
| 2026-08-18 | -6.20 | $11.47 | BTSG×41, HIMS×84, INO×3086, IREN×54, BTBT×4, INV×3, XHG×1 | $10,695.23 | -183.03 | -0.02 | — | BTSG, HIMS, INO, IREN | $10,634.21 | $10,648.26 | BTBT×4, INV×3, XHG×1 |
| 2026-08-19 | -7.20 | $10,634.21 | BTBT×4, INV×3, XHG×1 | $10,648.38 | +0.12 | +0.46 | — | BTBT | $10,639.80 | $10,648.75 | INV×3, XHG×1 |
| 2026-08-20 | +1.12 | $10,639.80 | INV×3, XHG×1 | $10,648.55 | -0.20 | +125.22 | AG, BHP, CDE, HDSN | INV, XHG | $42.22 | $10,760.85 | AG×129, BHP×29, CDE×128, HDSN×461 |
| 2026-08-21 | +3.25 | $42.22 | AG×129, BHP×29, CDE×128, HDSN×461 | $11,041.07 | +280.22 | -184.78 | — | — | $42.22 | $10,856.29 | AG×129, BHP×29, CDE×128, HDSN×461 |
| 2026-08-24 | -5.17 | $42.22 | AG×129, BHP×29, CDE×128, HDSN×461 | $10,979.08 | +122.79 | -289.70 | — | — | $42.22 | $10,689.38 | AG×129, BHP×29, CDE×128, HDSN×461 |
| 2026-08-25 | +1.80 | $42.22 | AG×129, BHP×29, CDE×128, HDSN×461 | $10,717.07 | +27.69 | +22.78 | AU, ERO, FCX, CNH | AG, BHP, CDE, HDSN | $85.91 | $10,717.57 | AU×22, ERO×70, FCX×34, CNH×228 |
| 2026-08-26 | +2.02 | $85.91 | AU×22, ERO×70, FCX×34, CNH×228 | $10,717.57 | +0.00 | +0.00 | — | — | $85.91 | $10,717.57 | AU×22, ERO×70, FCX×34, CNH×228 |
| 2026-08-27 | — | $85.91 | AU×22, ERO×70, FCX×34, CNH×228 | $10,885.89 | +168.32 | -119.24 | GGB | — | $68.05 | $10,766.47 | AU×22, ERO×70, FCX×34, CNH×228, GGB×4 |
| 2026-08-28 | +0.75 | $68.05 | AU×22, ERO×70, FCX×34, CNH×228, GGB×4 | $10,742.93 | -23.54 | -189.39 | KEYS, SMTC, CIEN, MPWR | AU, ERO, FCX, CNH | $468.10 | $10,536.03 | GGB×4, KEYS×8, SMTC×17, CIEN×6, MPWR×2 |
| 2026-08-31 | -5.85 | $468.10 | GGB×4, KEYS×8, SMTC×17, CIEN×6, MPWR×2 | $10,159.88 | -376.15 | -49.02 | — | — | $468.10 | $10,110.86 | GGB×4, KEYS×8, SMTC×17, CIEN×6, MPWR×2 |
| 2026-09-01 | -6.30 | $468.10 | GGB×4, KEYS×8, SMTC×17, CIEN×6, MPWR×2 | $10,176.11 | +65.25 | -130.67 | — | GGB | $486.33 | $10,045.23 | KEYS×8, SMTC×17, CIEN×6, MPWR×2 |
| 2026-09-02 | -3.83 | $486.33 | KEYS×8, SMTC×17, CIEN×6, MPWR×2 | $9,979.36 | -65.87 | +0.00 | — | KEYS, SMTC, CIEN, MPWR | $9,971.18 | $9,971.18 | — |
| 2026-09-03 | -0.90 | $9,971.18 | — | $9,971.18 | +0.00 | +325.19 | ARCT, BMEA, CRDL, HRMY | — | $27.24 | $10,259.02 | ARCT×151, BMEA×1384, CRDL×1154, HRMY×59 |
| 2026-09-04 | — | $27.24 | ARCT×151, BMEA×1384, CRDL×1154, HRMY×59 | $10,279.22 | +20.20 | -297.13 | CABA, ALEC | — | $18.11 | $9,981.99 | ARCT×151, BMEA×1384, CRDL×1154, HRMY×59, CABA×1, ALEC×2 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 41 | $59.80 | $2.11 | — | $7,546.09 | — | top 4 by cond; rank cond; list flatten; ⚪; ret5=-5.3; leftover $2500.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 84 | $29.74 | $2.24 | — | $5,045.69 | — | top 4 by cond; rank cond; list flatten; ⚪; ret5=-5.3; leftover $2500.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 3086 | $0.81 | $34.25 | — | $2,511.77 | — | top 4 by cond; rank cond; list flatten; ⚪; ret5=+13.2; leftover $2500.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 54 | $45.98 | $2.15 | — | $26.70 | — | top 4 by cond; rank cond; list flatten; ⚪; ret5=+12.3; leftover $2500.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $26.70 | ▲ close $10,107.25 vs 09:30 $10,000.00 (session +148.01) | 16:00 close · cash $26.70 · equity $10,107.25 vs 09:30 $10,000.00 (+107.25; session marks +148.01) · 4 name(s) marked open→close (per-name table). BTSG×41 09:30 $59.80 → close $60.23 +17.63; HIMS×84 09:30 $29.74 → close $28.77 -81.48; INO×3086 09:30 $0.81 → close $0.90 +277.74; IREN×54 09:30 $45.98 → close $44.76 -65.88 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $26.70 | ▲ 09:30 equity $10,171.79 vs yday $10,107.25 (+64.54) | 09:30 open · cash $26.70 (unchanged overnight, no fees) · equity $10,171.79 vs prior close $10,107.25 (+64.54) · 4 name(s) re-marked at the open (per-name table). BTSG×41 yday $60.23 → 09:30 $59.65 -23.78; HIMS×84 yday $28.77 → 09:30 $29.15 +31.92; INO×3086 yday $0.90 → 09:30 $0.93 +92.58; IREN×54 yday $44.76 → 09:30 $44.09 -36.18 | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 4 | $1.50 | $0.07 | — | $20.63 | — | top 4 by cond; rank cond; list flatten; 🔵; ⚪; ret5=+9.2; leftover $6.67 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20.63 | ▲ close $10,664.60 vs 09:30 $10,171.79 (session +492.88) | 16:00 close · cash $20.63 · equity $10,664.60 vs 09:30 $10,171.79 (+492.81; session marks +492.88) · 5 name(s) marked open→close (per-name table). BTSG×41 09:30 $59.65 → close $61.71 +84.46; HIMS×84 09:30 $29.15 → close $28.15 -84.00; INO×3086 09:30 $0.93 → close $1.09 +493.76; IREN×54 09:30 $44.09 → close $44.06 -1.62; BTBT×4 09:30 $1.50 → close $1.57 +0.28 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20.63 | ▼ 09:30 equity $10,664.20 vs yday $10,664.60 (-0.40) | 09:30 open · cash $20.63 (unchanged overnight, no fees) · equity $10,664.20 vs prior close $10,664.60 (-0.40) · 5 name(s) re-marked at the open (per-name table). BTSG×41 yday $61.71 → 09:30 $61.69 -0.82; HIMS×84 yday $28.15 → 09:30 $28.14 -0.84; INO×3086 yday $1.09 → 09:30 $1.07 -61.72; IREN×54 yday $44.06 → 09:30 $45.23 +63.18; BTBT×4 yday $1.57 → 09:30 $1.52 -0.20 | — |
| 2026-08-17 09:30 ET | **BUY** | `INV` | 3 | $1.62 | $0.06 | — | $15.71 | — | top 4 by cond; rank cond; list yday_mover; 🔵; ⚪; ret5=-53.0; leftover $5.16 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 1 | $4.19 | $0.04 | — | $11.47 | — | top 4 by cond; rank cond; list yday_mover; ⚪; ret5=+291.8; leftover $5.16 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11.47 | ▲ close $10,878.26 vs 09:30 $10,664.20 (session +214.16) | 16:00 close · cash $11.47 · equity $10,878.26 vs 09:30 $10,664.20 (+214.06; session marks +214.16) · 7 name(s) marked open→close (per-name table). BTSG×41 09:30 $61.69 → close $60.38 -53.71; HIMS×84 09:30 $28.14 → close $28.61 +39.48; INO×3086 09:30 $1.07 → close $1.15 +246.88; IREN×54 09:30 $45.23 → close $44.90 -17.82; BTBT×4 09:30 $1.52 → close $1.60 +0.32; INV×3 09:30 $1.62 → close $1.39 -0.71; XHG×1 09:30 $4.19 → close $3.91 -0.28 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11.47 | ▼ 09:30 equity $10,695.23 vs yday $10,878.26 (-183.03) | 09:30 open · cash $11.47 (unchanged overnight, no fees) · equity $10,695.23 vs prior close $10,878.26 (-183.03) · 7 name(s) re-marked at the open (per-name table). BTSG×41 yday $60.38 → 09:30 $60.00 -15.58; HIMS×84 yday $28.61 → 09:30 $27.85 -63.84; INO×3086 yday $1.15 → 09:30 $1.14 -30.86; IREN×54 yday $44.90 → 09:30 $43.56 -72.36; BTBT×4 yday $1.60 → 09:30 $1.54 -0.24; INV×3 yday $1.39 → 09:30 $1.32 -0.18; XHG×1 yday $3.91 → 09:30 $3.94 +0.03 | — |
| 2026-08-18 09:30 ET | **SELL** | `BTSG` | 41 | $60.00 | $2.14 | $+3.94 | $2,469.33 | ▲ +3.94 after sell → book $10,693.09; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `HIMS` | 84 | $27.85 | $2.27 | $-163.28 | $4,806.46 | ▼ -163.28 after sell → book $10,690.81; vs 09:30 mark -2.28 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 3086 | $1.14 | $40.35 | $+943.78 | $8,284.15 | ▲ +943.78 after sell → book $10,650.46; vs 09:30 mark -40.35 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `IREN` | 54 | $43.56 | $2.18 | $-135.01 | $10,634.21 | ▼ -135.01 after sell → book $10,648.28; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,634.21 | ▼ close $10,648.26 vs 09:30 $10,695.23 (session -0.02) | 16:00 close · cash $10,634.21 · equity $10,648.26 vs 09:30 $10,695.23 (-46.97; session marks -0.02) · 3 name(s) marked open→close (per-name table). BTBT×4 09:30 $1.54 → close $1.45 -0.36; INV×3 09:30 $1.32 → close $1.32 +0.00; XHG×1 09:30 $3.94 → close $4.28 +0.34 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,634.21 | ▲ 09:30 equity $10,648.38 vs yday $10,648.26 (+0.12) | 09:30 open · cash $10,634.21 (unchanged overnight, no fees) · equity $10,648.38 vs prior close $10,648.26 (+0.12) · 3 name(s) re-marked at the open (per-name table). BTBT×4 yday $1.45 → 09:30 $1.42 -0.12; INV×3 yday $1.32 → 09:30 $1.39 +0.19; XHG×1 yday $4.28 → 09:30 $4.32 +0.04 | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 4 | $1.42 | $0.09 | $-0.48 | $10,639.80 | ▼ -0.48 after sell → book $10,648.29; vs 09:30 mark -0.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,639.80 | ▲ close $10,648.75 vs 09:30 $10,648.38 (session +0.46) | 16:00 close · cash $10,639.80 · equity $10,648.75 vs 09:30 $10,648.38 (+0.37; session marks +0.46) · 2 name(s) marked open→close (per-name table). INV×3 09:30 $1.39 → close $1.54 +0.45; XHG×1 09:30 $4.32 → close $4.33 +0.01 | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,639.80 | ▼ 09:30 equity $10,648.55 vs yday $10,648.75 (-0.20) | 09:30 open · cash $10,639.80 (unchanged overnight, no fees) · equity $10,648.55 vs prior close $10,648.75 (-0.20) · 2 name(s) re-marked at the open (per-name table). INV×3 yday $1.54 → 09:30 $1.55 +0.03; XHG×1 yday $4.33 → 09:30 $4.10 -0.23 | — |
| 2026-08-20 09:30 ET | **SELL** | `INV` | 3 | $1.55 | $0.08 | $-0.34 | $10,644.37 | ▼ -0.34 after sell → book $10,648.47; vs 09:30 mark -0.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `XHG` | 1 | $4.10 | $0.06 | $-0.20 | $10,648.41 | ▼ -0.20 after sell → book $10,648.41; vs 09:30 mark -0.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 129 | $20.55 | $2.38 | — | $7,995.08 | — | top 4 by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $2662.10 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 29 | $91.01 | $2.08 | — | $5,353.71 | — | top 4 by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $2662.10 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 128 | $20.65 | $2.37 | — | $2,708.14 | — | top 4 by cond; rank cond; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $2662.10 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 461 | $5.77 | $5.95 | — | $42.22 | — | top 4 by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $2662.10 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $42.22 | ▲ close $10,760.85 vs 09:30 $10,648.55 (session +125.22) | 16:00 close · cash $42.22 · equity $10,760.85 vs 09:30 $10,648.55 (+112.30; session marks +125.22) · 4 name(s) marked open→close (per-name table). AG×129 09:30 $20.55 → close $21.19 +82.56; BHP×29 09:30 $91.01 → close $93.63 +75.98; CDE×128 09:30 $20.65 → close $21.11 +58.88; HDSN×461 09:30 $5.77 → close $5.57 -92.20 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $42.22 | ▲ 09:30 equity $11,041.07 vs yday $10,760.85 (+280.22) | 09:30 open · cash $42.22 (unchanged overnight, no fees) · equity $11,041.07 vs prior close $10,760.85 (+280.22) · 4 name(s) re-marked at the open (per-name table). AG×129 yday $21.19 → 09:30 $21.90 +91.59; BHP×29 yday $93.63 → 09:30 $95.72 +60.61; CDE×128 yday $21.11 → 09:30 $21.75 +81.92; HDSN×461 yday $5.57 → 09:30 $5.67 +46.10 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $42.22 | ▼ close $10,856.29 vs 09:30 $11,041.07 (session -184.78) | 16:00 close · cash $42.22 · equity $10,856.29 vs 09:30 $11,041.07 (-184.78; session marks -184.78) · 4 name(s) marked open→close (per-name table). AG×129 09:30 $21.90 → close $21.09 -104.49; BHP×29 09:30 $95.72 → close $97.03 +37.99; CDE×128 09:30 $21.75 → close $20.97 -99.84; HDSN×461 09:30 $5.67 → close $5.63 -18.44 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $42.22 | ▲ 09:30 equity $10,979.08 vs yday $10,856.29 (+122.79) | 09:30 open · cash $42.22 (unchanged overnight, no fees) · equity $10,979.08 vs prior close $10,856.29 (+122.79) · 4 name(s) re-marked at the open (per-name table). AG×129 yday $21.09 → 09:30 $21.47 +49.02; BHP×29 yday $97.03 → 09:30 $97.34 +8.99; CDE×128 yday $20.97 → 09:30 $21.26 +37.12; HDSN×461 yday $5.63 → 09:30 $5.69 +27.66 | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $42.22 | ▼ close $10,689.38 vs 09:30 $10,979.08 (session -289.70) | 16:00 close · cash $42.22 · equity $10,689.38 vs 09:30 $10,979.08 (-289.70; session marks -289.70) · 4 name(s) marked open→close (per-name table). AG×129 09:30 $21.47 → close $20.57 -116.10; BHP×29 09:30 $97.34 → close $96.66 -19.72; CDE×128 09:30 $21.26 → close $20.49 -98.56; HDSN×461 09:30 $5.69 → close $5.57 -55.32 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $42.22 | ▲ 09:30 equity $10,717.07 vs yday $10,689.38 (+27.69) | 09:30 open · cash $42.22 (unchanged overnight, no fees) · equity $10,717.07 vs prior close $10,689.38 (+27.69) · 4 name(s) re-marked at the open (per-name table). AG×129 yday $20.57 → 09:30 $20.73 +20.64; BHP×29 yday $96.66 → 09:30 $95.95 -20.59; CDE×128 yday $20.49 → 09:30 $20.85 +46.08; HDSN×461 yday $5.57 → 09:30 $5.53 -18.44 | — |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 129 | $20.73 | $2.42 | $+18.42 | $2,713.97 | ▲ +18.42 after sell → book $10,714.65; vs 09:30 mark -2.42 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 29 | $95.95 | $2.11 | $+139.07 | $5,494.41 | ▲ +139.07 after sell → book $10,712.54; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CDE` | 128 | $20.85 | $2.42 | $+20.81 | $8,160.80 | ▲ +20.81 after sell → book $10,710.13; vs 09:30 mark -2.41 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 461 | $5.53 | $6.04 | $-122.63 | $10,704.08 | ▼ -122.63 after sell → book $10,704.08; vs 09:30 mark -6.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 22 | $119.46 | $2.06 | — | $8,073.91 | — | top 4 by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+25.9; leftover $2676.02 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ERO` | 70 | $38.00 | $2.20 | — | $5,411.71 | — | top 4 by cond; rank cond; list ohlc_hot,mover_buy; ⚪; ret5=+16.6; leftover $2676.02 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 34 | $77.90 | $2.09 | — | $2,761.02 | — | top 4 by cond; rank cond; list ohlc_hot,mover_buy; ⚪; ret5=+15.3; leftover $2676.02 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CNH` | 228 | $11.72 | $2.94 | — | $85.91 | — | top 4 by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+13.7; leftover $2676.02 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $85.91 | ▲ close $10,717.57 vs 09:30 $10,717.07 (session +22.78) | 16:00 close · cash $85.91 · equity $10,717.57 vs 09:30 $10,717.07 (+0.50; session marks +22.78) · 4 name(s) marked open→close (per-name table). AU×22 09:30 $119.46 → close $118.55 -20.02; ERO×70 09:30 $38.00 → close $38.55 +38.50; FCX×34 09:30 $77.90 → close $77.49 -13.94; CNH×228 09:30 $11.72 → close $11.80 +18.24 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $85.91 | ▲ 09:30 equity $10,717.57 vs yday $10,717.57 (+0.00) | 09:30 open · cash $85.91 (unchanged overnight, no fees) · equity $10,717.57 vs prior close $10,717.57 (+0.00) · 4 name(s) re-marked at the open (per-name table). AU×22 yday $118.55 → 09:30 $118.55 +0.00; ERO×70 yday $38.55 → 09:30 $38.55 +0.00; FCX×34 yday $77.49 → 09:30 $77.49 +0.00; CNH×228 yday $11.80 → 09:30 $11.80 +0.00 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $85.91 | ▲ close $10,717.57 vs 09:30 $10,717.57 (session +0.00) | 16:00 close · cash $85.91 · equity $10,717.57 vs 09:30 $10,717.57 (+0.00; session marks +0.00) · 4 name(s) marked open→close (per-name table). AU×22 09:30 $118.55 → close $118.55 +0.00; ERO×70 09:30 $38.55 → close $38.55 +0.00; FCX×34 09:30 $77.49 → close $77.49 +0.00; CNH×228 09:30 $11.80 → close $11.80 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $85.91 | ▲ 09:30 equity $10,885.89 vs yday $10,717.57 (+168.32) | 09:30 open · cash $85.91 (unchanged overnight, no fees) · equity $10,885.89 vs prior close $10,717.57 (+168.32) · 4 name(s) re-marked at the open (per-name table). AU×22 yday $118.55 → 09:30 $119.80 +27.50; ERO×70 yday $38.55 → 09:30 $40.51 +137.20; FCX×34 yday $77.49 → 09:30 $79.34 +62.90; CNH×228 yday $11.80 → 09:30 $11.54 -59.28 | — |
| 2026-08-27 09:30 ET | **BUY** | `GGB` | 4 | $4.42 | $0.19 | — | $68.05 | — | top 4 by cond; rank cond; list mover_buy; 🔵; ret5=-8.6; leftover $21.48 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $68.05 | ▼ close $10,766.47 vs 09:30 $10,885.89 (session -119.24) | 16:00 close · cash $68.05 · equity $10,766.47 vs 09:30 $10,885.89 (-119.42; session marks -119.24) · 5 name(s) marked open→close (per-name table). AU×22 09:30 $119.80 → close $118.11 -37.18; ERO×70 09:30 $40.51 → close $39.24 -88.90; FCX×34 09:30 $79.34 → close $79.00 -11.56; CNH×228 09:30 $11.54 → close $11.62 +18.24; GGB×4 09:30 $4.42 → close $4.46 +0.16 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $68.05 | ▼ 09:30 equity $10,742.93 vs yday $10,766.47 (-23.54) | 09:30 open · cash $68.05 (unchanged overnight, no fees) · equity $10,742.93 vs prior close $10,766.47 (-23.54) · 5 name(s) re-marked at the open (per-name table). AU×22 yday $118.11 → 09:30 $117.41 -15.40; ERO×70 yday $39.24 → 09:30 $39.20 -2.80; FCX×34 yday $79.00 → 09:30 $78.83 -5.78; CNH×228 yday $11.62 → 09:30 $11.62 +0.00; GGB×4 yday $4.46 → 09:30 $4.57 +0.44 | — |
| 2026-08-28 09:30 ET | **SELL** | `AU` | 22 | $117.41 | $2.09 | $-49.24 | $2,648.98 | ▼ -49.24 after sell → book $10,740.84; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ERO` | 70 | $39.20 | $2.23 | $+79.57 | $5,390.74 | ▲ +79.57 after sell → book $10,738.60; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `FCX` | 34 | $78.83 | $2.12 | $+27.40 | $8,068.84 | ▲ +27.40 after sell → book $10,736.48; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 catal🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `CNH` | 228 | $11.62 | $3.00 | $-28.74 | $10,715.20 | ▼ -28.74 after sell → book $10,733.48; vs 09:30 mark -3.00 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 8 | $323.82 | $2.01 | — | $8,122.63 | — | top 4 by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=-11.7; leftover $2678.80 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 17 | $149.40 | $2.04 | — | $5,580.79 | — | top 4 by cond; rank cond; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=-11.6; leftover $2678.80 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 6 | $411.53 | $2.01 | — | $3,109.60 | — | top 4 by cond; rank cond; list mover_buy; 🔵; ret5=-7.7; leftover $2678.80 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `MPWR` | 2 | $1319.75 | $2.00 | — | $468.10 | — | top 4 by cond; rank cond; list mover_buy; 🔵; ret5=-6.1; leftover $2678.80 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $468.10 | ▼ close $10,536.03 vs 09:30 $10,742.93 (session -189.39) | 16:00 close · cash $468.10 · equity $10,536.03 vs 09:30 $10,742.93 (-206.90; session marks -189.39) · 5 name(s) marked open→close (per-name table). GGB×4 09:30 $4.57 → close $4.70 +0.52; KEYS×8 09:30 $323.82 → close $325.82 +16.00; SMTC×17 09:30 $149.40 → close $142.43 -118.49; CIEN×6 09:30 $411.53 → close $399.85 -70.08; MPWR×2 09:30 $1319.75 → close $1311.08 -17.34 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $468.10 | ▼ 09:30 equity $10,159.88 vs yday $10,536.03 (-376.15) | 09:30 open · cash $468.10 (unchanged overnight, no fees) · equity $10,159.88 vs prior close $10,536.03 (-376.15) · 5 name(s) re-marked at the open (per-name table). GGB×4 yday $4.70 → 09:30 $4.55 -0.60; KEYS×8 yday $325.82 → 09:30 $324.14 -13.44; SMTC×17 yday $142.43 → 09:30 $133.04 -159.63; CIEN×6 yday $399.85 → 09:30 $373.68 -157.02; MPWR×2 yday $1311.08 → 09:30 $1288.35 -45.46 | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $468.10 | ▼ close $10,110.86 vs 09:30 $10,159.88 (session -49.02) | 16:00 close · cash $468.10 · equity $10,110.86 vs 09:30 $10,159.88 (-49.02; session marks -49.02) · 5 name(s) marked open→close (per-name table). GGB×4 09:30 $4.55 → close $4.55 +0.00; KEYS×8 09:30 $324.14 → close $319.02 -40.96; SMTC×17 09:30 $133.04 → close $132.54 -8.50; CIEN×6 09:30 $373.68 → close $379.87 +37.14; MPWR×2 09:30 $1288.35 → close $1270.00 -36.70 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $468.10 | ▲ 09:30 equity $10,176.11 vs yday $10,110.86 (+65.25) | 09:30 open · cash $468.10 (unchanged overnight, no fees) · equity $10,176.11 vs prior close $10,110.86 (+65.25) · 5 name(s) re-marked at the open (per-name table). GGB×4 yday $4.55 → 09:30 $4.61 +0.24; KEYS×8 yday $319.02 → 09:30 $323.71 +37.52; SMTC×17 yday $132.54 → 09:30 $131.65 -15.13; CIEN×6 yday $379.87 → 09:30 $383.85 +23.88; MPWR×2 yday $1270.00 → 09:30 $1279.37 +18.74 | — |
| 2026-09-01 09:30 ET | **SELL** | `GGB` | 4 | $4.61 | $0.22 | $+0.35 | $486.33 | ▲ +0.35 after sell → book $10,175.90; vs 09:30 mark -0.21 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $486.33 | ▼ close $10,045.23 vs 09:30 $10,176.11 (session -130.67) | 16:00 close · cash $486.33 · equity $10,045.23 vs 09:30 $10,176.11 (-130.88; session marks -130.67) · 4 name(s) marked open→close (per-name table). KEYS×8 09:30 $323.71 → close $322.70 -8.08; SMTC×17 09:30 $131.65 → close $129.50 -36.55; CIEN×6 09:30 $383.85 → close $378.12 -34.38; MPWR×2 09:30 $1279.37 → close $1253.54 -51.66 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $486.33 | ▼ 09:30 equity $9,979.36 vs yday $10,045.23 (-65.87) | 09:30 open · cash $486.33 (unchanged overnight, no fees) · equity $9,979.36 vs prior close $10,045.23 (-65.87) · 4 name(s) re-marked at the open (per-name table). KEYS×8 yday $322.70 → 09:30 $321.47 -9.84; SMTC×17 yday $129.50 → 09:30 $127.63 -31.79; CIEN×6 yday $378.12 → 09:30 $376.89 -7.38; MPWR×2 yday $1253.54 → 09:30 $1245.11 -16.86 | — |
| 2026-09-02 09:30 ET | **SELL** | `KEYS` | 8 | $321.47 | $2.04 | $-22.86 | $3,056.04 | ▼ -22.86 after sell → book $9,977.31; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SMTC` | 17 | $127.63 | $2.07 | $-374.20 | $5,223.68 | ▼ -374.20 after sell → book $9,975.24; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CIEN` | 6 | $376.89 | $2.04 | $-211.88 | $7,482.99 | ▼ -211.88 after sell → book $9,973.21; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `MPWR` | 2 | $1245.11 | $2.03 | $-153.30 | $9,971.18 | ▼ -153.30 after sell → book $9,971.18; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,971.18 | ▲ close $9,971.18 vs 09:30 $9,979.36 (session +0.00) | 16:00 close · cash $9,971.18 · no lots left · equity $9,971.18. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,971.18 | ▲ 09:30 equity $9,971.18 vs yday $9,971.18 (+0.00) | 09:30 open · cash $9,971.18 · no holdings · equity $9,971.18 vs prior close $9,971.18 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 151 | $16.46 | $2.44 | — | $7,483.28 | — | top 4 by cond; rank cond; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+63.4; leftover $2492.80 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `BMEA` | 1384 | $1.80 | $17.85 | — | $4,974.22 | — | top 4 by cond; rank cond; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $2492.80 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 1154 | $2.16 | $14.89 | — | $2,466.70 | — | top 4 by cond; rank cond; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.3; leftover $2492.80 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 59 | $41.31 | $2.17 | — | $27.24 | — | top 4 by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $2492.80 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $27.24 | ▲ close $10,259.02 vs 09:30 $9,971.18 (session +325.19) | 16:00 close · cash $27.24 · equity $10,259.02 vs 09:30 $9,971.18 (+287.84; session marks +325.19) · 4 name(s) marked open→close (per-name table). ARCT×151 09:30 $16.46 → close $16.74 +42.28; BMEA×1384 09:30 $1.80 → close $1.93 +179.92; CRDL×1154 09:30 $2.16 → close $2.17 +11.54; HRMY×59 09:30 $41.31 → close $42.86 +91.45 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $27.24 | ▲ 09:30 equity $10,279.22 vs yday $10,259.02 (+20.20) | 09:30 open · cash $27.24 (unchanged overnight, no fees) · equity $10,279.22 vs prior close $10,259.02 (+20.20) · 4 name(s) re-marked at the open (per-name table). ARCT×151 yday $16.74 → 09:30 $16.77 +4.53; BMEA×1384 yday $1.93 → 09:30 $1.93 +0.00; CRDL×1154 yday $2.17 → 09:30 $2.18 +11.54; HRMY×59 yday $42.86 → 09:30 $42.93 +4.13 | — |
| 2026-09-04 09:30 ET | **BUY** | `CABA` | 1 | $3.63 | $0.04 | — | $23.57 | — | top 4 by cond; rank cond; list flatten; 🔵; ⚪; ret5=+13.8; leftover $6.81 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 2 | $2.70 | $0.06 | — | $18.11 | — | top 4 by cond; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.4; leftover $6.81 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18.11 | ▼ close $9,981.99 vs 09:30 $10,279.22 (session -297.13) | 16:00 close · cash $18.11 · equity $9,981.99 vs 09:30 $10,279.22 (-297.23; session marks -297.13) · 6 name(s) marked open→close (per-name table). ARCT×151 09:30 $16.77 → close $15.56 -182.71; BMEA×1384 09:30 $1.93 → close $1.91 -27.68; CRDL×1154 09:30 $2.18 → close $2.16 -23.08; HRMY×59 09:30 $42.93 → close $41.86 -63.13; CABA×1 09:30 $3.63 → close $3.48 -0.15; ALEC×2 09:30 $2.70 → close $2.51 -0.38 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `BTSG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `HIMS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `INO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `IREN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `ARX` | cash | leftover split 6.67 < 1 share @ 19.57 |
| 2026-08-14 | `BETR` | cash | leftover split 6.67 < 1 share @ 14.80 |
| 2026-08-14 | `FIGR` | cash | leftover split 6.67 < 1 share @ 32.12 |
| 2026-08-17 | `BTSG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `HIMS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `INO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `IREN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ABX` | cash | leftover split 5.16 < 1 share @ 9.12 |
| 2026-08-17 | `NU` | cash | leftover split 5.16 < 1 share @ 15.40 |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `INV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `XHG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AURA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `INV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `XHG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ADI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BHP` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BSBR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 10.56 < 1 share @ 119.43 |
| 2026-08-21 | `AUPH` | cash | leftover split 10.56 < 1 share @ 17.20 |
| 2026-08-21 | `AEM` | cash | leftover split 10.56 < 1 share @ 216.30 |
| 2026-08-21 | `ARCT` | cash | leftover split 10.56 < 1 share @ 11.13 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ERO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ALM` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-26 | `AU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `CNH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `AEM` | no_price | no 09:30 open |
| 2026-08-26 | `HOOD` | no_price | no 09:30 open |
| 2026-08-27 | `AU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ERO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `FCX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CNH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ACMR` | cash | leftover split 21.48 < 1 share @ 80.97 |
| 2026-08-27 | `MT` | cash | leftover split 21.48 < 1 share @ 75.12 |
| 2026-08-27 | `MU` | cash | leftover split 21.48 < 1 share @ 925.74 |
| 2026-08-28 | `GGB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `GGB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `KEYS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CIEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `MPWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TYL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ACIW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `KEYS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CIEN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `MPWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `NOV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NVDA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `RES` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `ALAB` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ACB` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ADM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ALVO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `HRMY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ATRC` | cash | leftover split 6.81 < 1 share @ 52.88 |
| 2026-09-04 | `MLYS` | cash | leftover split 6.81 < 1 share @ 29.15 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ARCT` | 151 | 2026-09-03 @ $16.46 | top 4 by cond; rank cond; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+63.4; leftover $2492.80 |
| `BMEA` | 1384 | 2026-09-03 @ $1.80 | top 4 by cond; rank cond; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $2492.80 |
| `CRDL` | 1154 | 2026-09-03 @ $2.16 | top 4 by cond; rank cond; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.3; leftover $2492.80 |
| `HRMY` | 59 | 2026-09-03 @ $41.31 | top 4 by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $2492.80 |
| `CABA` | 1 | 2026-09-04 @ $3.63 | top 4 by cond; rank cond; list flatten; 🔵; ⚪; ret5=+13.8; leftover $6.81 |
| `ALEC` | 2 | 2026-09-04 @ $2.70 | top 4 by cond; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.4; leftover $6.81 |
