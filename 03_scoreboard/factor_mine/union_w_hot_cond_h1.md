# Factor mine action — `union_w_hot_cond_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `w_hot_cond` · size `leftover` · sell `list` · S-boost `none` · rank by w_hot_cond

Cash book **-4.34%** ($9,566) · signal-only (no cash/fees) was +0.19%. Starts YES **4/17**. Fills 140 · skips 54 · realized $-208.59.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `w_hot_cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $33.22.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `IREN` | 27 | — | $45.98 | +0.00 | $44.76 | -32.94 | -32.94 | +0.00 | -32.94 |
| 2026-08-13 | `TNDM` | 53 | — | $23.33 | +0.00 | $23.13 | -10.60 | -10.60 | +0.00 | -10.60 |
| 2026-08-13 | `TPG` | 24 | — | $50.62 | +0.00 | $54.62 | +95.92 | +95.92 | +0.00 | +95.92 |
| 2026-08-13 | `INO` | 1543 | — | $0.81 | +0.00 | $0.90 | +138.87 | +138.87 | +0.00 | +138.87 |
| 2026-08-13 | `HIMS` | 42 | — | $29.74 | +0.00 | $28.77 | -40.74 | -40.74 | +0.00 | -40.74 |
| 2026-08-13 | `SLS` | 106 | — | $11.70 | +0.00 | $12.36 | +69.96 | +69.96 | +0.00 | +69.96 |
| 2026-08-13 | `VOR` | 56 | — | $22.01 | +0.00 | $23.29 | +71.68 | +71.68 | +0.00 | +71.68 |
| 2026-08-13 | `BTSG` | 20 | — | $59.80 | +0.00 | $60.23 | +8.60 | +8.60 | +0.00 | +8.60 |
| 2026-08-14 | `IREN` | 27 | $44.76 | $44.09 | -18.09 | — | +0.00 | -18.09 | -51.03 | — |
| 2026-08-14 | `TNDM` | 53 | $23.13 | $22.92 | -11.13 | — | +0.00 | -11.13 | -21.73 | — |
| 2026-08-14 | `TPG` | 24 | $54.62 | $55.29 | +16.08 | — | +0.00 | +16.08 | +112.00 | — |
| 2026-08-14 | `INO` | 1543 | $0.90 | $0.93 | +46.29 | — | +0.00 | +46.29 | +185.16 | — |
| 2026-08-14 | `HIMS` | 42 | $28.77 | $29.15 | +15.96 | — | +0.00 | +15.96 | -24.78 | — |
| 2026-08-14 | `SLS` | 106 | $12.36 | $12.40 | +4.24 | — | +0.00 | +4.24 | +74.20 | — |
| 2026-08-14 | `VOR` | 56 | $23.29 | $23.33 | +2.24 | — | +0.00 | +2.24 | +73.92 | — |
| 2026-08-14 | `BTSG` | 20 | $60.23 | $59.65 | -11.60 | — | +0.00 | -11.60 | -3.00 | — |
| 2026-08-14 | `QMCO` | 52 | — | $24.68 | +0.00 | $26.11 | +74.36 | +74.36 | +0.00 | +74.36 |
| 2026-08-14 | `ARX` | 65 | — | $19.57 | +0.00 | $19.58 | +0.65 | +0.65 | +0.00 | +0.65 |
| 2026-08-14 | `ZENA` | 583 | — | $2.20 | +0.00 | $2.14 | -34.98 | -34.98 | +0.00 | -34.98 |
| 2026-08-14 | `AIRO` | 115 | — | $11.12 | +0.00 | $9.57 | -178.25 | -178.25 | +0.00 | -178.25 |
| 2026-08-14 | `LIFE` | 36 | — | $35.04 | +0.00 | $34.02 | -36.72 | -36.72 | +0.00 | -36.72 |
| 2026-08-14 | `LUNR` | 67 | — | $19.17 | +0.00 | $19.01 | -10.72 | -10.72 | +0.00 | -10.72 |
| 2026-08-14 | `TBBB` | 26 | — | $48.82 | +0.00 | $47.79 | -26.78 | -26.78 | +0.00 | -26.78 |
| 2026-08-14 | `BZAI` | 1677 | — | $0.77 | +0.00 | $0.59 | -290.12 | -290.12 | +0.00 | -290.12 |
| 2026-08-17 | `QMCO` | 52 | $26.11 | $24.83 | -66.56 | — | +0.00 | -66.56 | +7.80 | — |
| 2026-08-17 | `ARX` | 65 | $19.58 | $19.57 | -0.65 | — | +0.00 | -0.65 | +0.00 | — |
| 2026-08-17 | `ZENA` | 583 | $2.14 | $2.08 | -32.07 | — | +0.00 | -32.07 | -67.05 | — |
| 2026-08-17 | `AIRO` | 115 | $9.57 | $9.57 | +0.00 | — | +0.00 | +0.00 | -178.25 | — |
| 2026-08-17 | `LIFE` | 36 | $34.02 | $34.03 | +0.36 | — | +0.00 | +0.36 | -36.36 | — |
| 2026-08-17 | `LUNR` | 67 | $19.01 | $20.25 | +83.08 | — | +0.00 | +83.08 | +72.36 | — |
| 2026-08-17 | `TBBB` | 26 | $47.79 | $47.39 | -10.40 | — | +0.00 | -10.40 | -37.18 | — |
| 2026-08-17 | `BZAI` | 1677 | $0.59 | $0.55 | -68.76 | — | +0.00 | -68.76 | -358.88 | — |
| 2026-08-17 | `XHG` | 286 | — | $4.19 | +0.00 | $3.91 | -80.08 | -80.08 | +0.00 | -80.08 |
| 2026-08-17 | `CAPR` | 174 | — | $6.87 | +0.00 | $7.45 | +100.92 | +100.92 | +0.00 | +100.92 |
| 2026-08-17 | `STDN` | 88 | — | $13.64 | +0.00 | $13.31 | -29.04 | -29.04 | +0.00 | -29.04 |
| 2026-08-17 | `HTFL` | 29 | — | $41.23 | +0.00 | $41.94 | +20.59 | +20.59 | +0.00 | +20.59 |
| 2026-08-17 | `UMAC` | 36 | — | $32.55 | +0.00 | $30.15 | -86.40 | -86.40 | +0.00 | -86.40 |
| 2026-08-17 | `ALOY` | 81 | — | $14.66 | +0.00 | $13.86 | -65.20 | -65.20 | +0.00 | -65.20 |
| 2026-08-17 | `NPWR` | 625 | — | $1.92 | +0.00 | $1.73 | -118.75 | -118.75 | +0.00 | -118.75 |
| 2026-08-17 | `LPTH` | 80 | — | $14.94 | +0.00 | $14.80 | -11.20 | -11.20 | +0.00 | -11.20 |
| 2026-08-18 | `XHG` | 286 | $3.91 | $3.94 | +8.58 | — | +0.00 | +8.58 | -71.50 | — |
| 2026-08-18 | `CAPR` | 174 | $7.45 | $7.50 | +8.70 | $7.08 | -73.08 | -64.38 | +109.62 | +36.54 |
| 2026-08-18 | `STDN` | 88 | $13.31 | $13.31 | +0.00 | — | +0.00 | +0.00 | -29.04 | — |
| 2026-08-18 | `HTFL` | 29 | $41.94 | $41.50 | -12.76 | — | +0.00 | -12.76 | +7.83 | — |
| 2026-08-18 | `UMAC` | 36 | $30.15 | $28.59 | -56.16 | — | +0.00 | -56.16 | -142.56 | — |
| 2026-08-18 | `ALOY` | 81 | $13.86 | $13.19 | -53.87 | — | +0.00 | -53.87 | -119.07 | — |
| 2026-08-18 | `NPWR` | 625 | $1.73 | $1.70 | -18.75 | — | +0.00 | -18.75 | -137.50 | — |
| 2026-08-18 | `LPTH` | 80 | $14.80 | $14.01 | -63.20 | — | +0.00 | -63.20 | -74.40 | — |
| 2026-08-19 | `CAPR` | 174 | $7.08 | $7.19 | +19.14 | — | +0.00 | +19.14 | +55.68 | — |
| 2026-08-20 | `MRNA` | 7 | — | $150.14 | +0.00 | $133.32 | -117.74 | -117.74 | +0.00 | -117.74 |
| 2026-08-20 | `CYPH` | 983 | — | $1.15 | +0.00 | $1.19 | +39.32 | +39.32 | +0.00 | +39.32 |
| 2026-08-20 | `ABCL` | 95 | — | $11.81 | +0.00 | $11.57 | -23.27 | -23.27 | +0.00 | -23.27 |
| 2026-08-20 | `SENS` | 126 | — | $8.91 | +0.00 | $8.82 | -11.34 | -11.34 | +0.00 | -11.34 |
| 2026-08-20 | `AUTL` | 457 | — | $2.47 | +0.00 | $2.46 | -4.57 | -4.57 | +0.00 | -4.57 |
| 2026-08-20 | `TEM` | 18 | — | $61.83 | +0.00 | $66.65 | +86.76 | +86.76 | +0.00 | +86.76 |
| 2026-08-20 | `WPM` | 7 | — | $144.54 | +0.00 | $150.25 | +39.97 | +39.97 | +0.00 | +39.97 |
| 2026-08-20 | `IAG` | 57 | — | $19.63 | +0.00 | $20.50 | +49.59 | +49.59 | +0.00 | +49.59 |
| 2026-08-21 | `MRNA` | 7 | $133.32 | $133.11 | -1.47 | $145.13 | +84.14 | +82.67 | -119.21 | -35.07 |
| 2026-08-21 | `CYPH` | 983 | $1.19 | $1.32 | +127.79 | $1.42 | +98.30 | +226.09 | +167.11 | +265.41 |
| 2026-08-21 | `ABCL` | 95 | $11.57 | $11.57 | +0.00 | — | +0.00 | +0.00 | -23.27 | — |
| 2026-08-21 | `SENS` | 126 | $8.82 | $9.24 | +52.92 | — | +0.00 | +52.92 | +41.58 | — |
| 2026-08-21 | `AUTL` | 457 | $2.46 | $2.47 | +4.57 | — | +0.00 | +4.57 | +0.00 | — |
| 2026-08-21 | `TEM` | 18 | $66.65 | $65.60 | -18.90 | $72.69 | +127.62 | +108.72 | +67.86 | +195.48 |
| 2026-08-21 | `WPM` | 7 | $150.25 | $154.70 | +31.15 | — | +0.00 | +31.15 | +71.12 | — |
| 2026-08-21 | `IAG` | 57 | $20.50 | $21.17 | +38.19 | — | +0.00 | +38.19 | +87.78 | — |
| 2026-08-21 | `XHG` | 262 | — | $4.49 | +0.00 | $4.41 | -20.96 | -20.96 | +0.00 | -20.96 |
| 2026-08-21 | `ARCT` | 105 | — | $11.13 | +0.00 | $13.45 | +243.60 | +243.60 | +0.00 | +243.60 |
| 2026-08-21 | `IOVA` | 129 | — | $9.08 | +0.00 | $8.29 | -101.91 | -101.91 | +0.00 | -101.91 |
| 2026-08-21 | `CAPR` | 172 | — | $6.81 | +0.00 | $6.29 | -89.44 | -89.44 | +0.00 | -89.44 |
| 2026-08-21 | `AU` | 9 | — | $119.43 | +0.00 | $121.22 | +16.11 | +16.11 | +0.00 | +16.11 |
| 2026-08-24 | `MRNA` | 7 | $145.13 | $142.70 | -17.01 | — | +0.00 | -17.01 | -52.08 | — |
| 2026-08-24 | `CYPH` | 983 | $1.42 | $1.83 | +403.03 | — | +0.00 | +403.03 | +668.44 | — |
| 2026-08-24 | `TEM` | 18 | $72.69 | $70.07 | -47.16 | — | +0.00 | -47.16 | +148.32 | — |
| 2026-08-24 | `XHG` | 262 | $4.41 | $4.24 | -44.54 | — | +0.00 | -44.54 | -65.50 | — |
| 2026-08-24 | `ARCT` | 105 | $13.45 | $13.26 | -19.95 | — | +0.00 | -19.95 | +223.65 | — |
| 2026-08-24 | `IOVA` | 129 | $8.29 | $8.05 | -30.96 | — | +0.00 | -30.96 | -132.87 | — |
| 2026-08-24 | `CAPR` | 172 | $6.29 | $8.01 | +295.84 | — | +0.00 | +295.84 | +206.40 | — |
| 2026-08-24 | `AU` | 9 | $121.22 | $120.50 | -6.48 | — | +0.00 | -6.48 | +9.63 | — |
| 2026-08-25 | `CYPH` | 745 | — | $1.70 | +0.00 | $1.64 | -44.70 | -44.70 | +0.00 | -44.70 |
| 2026-08-25 | `XHG` | 315 | — | $4.02 | +0.00 | $4.05 | +9.45 | +9.45 | +0.00 | +9.45 |
| 2026-08-25 | `AU` | 10 | — | $119.46 | +0.00 | $118.55 | -9.10 | -9.10 | +0.00 | -9.10 |
| 2026-08-25 | `ERO` | 33 | — | $38.00 | +0.00 | $38.55 | +18.15 | +18.15 | +0.00 | +18.15 |
| 2026-08-25 | `ASST` | 60 | — | $20.90 | +0.00 | $20.20 | -42.00 | -42.00 | +0.00 | -42.00 |
| 2026-08-25 | `HMY` | 55 | — | $22.65 | +0.00 | $22.50 | -8.25 | -8.25 | +0.00 | -8.25 |
| 2026-08-25 | `FCX` | 16 | — | $77.90 | +0.00 | $77.49 | -6.56 | -6.56 | +0.00 | -6.56 |
| 2026-08-25 | `WPM` | 7 | — | $160.00 | +0.00 | $158.25 | -12.25 | -12.25 | +0.00 | -12.25 |
| 2026-08-26 | `CYPH` | 745 | $1.64 | $1.64 | +0.00 | $1.64 | +0.00 | +0.00 | -44.70 | -44.70 |
| 2026-08-26 | `XHG` | 315 | $4.05 | $4.05 | +0.00 | $4.05 | +0.00 | +0.00 | +9.45 | +9.45 |
| 2026-08-26 | `AU` | 10 | $118.55 | $118.55 | +0.00 | $118.55 | +0.00 | +0.00 | -9.10 | -9.10 |
| 2026-08-26 | `ERO` | 33 | $38.55 | $38.55 | +0.00 | $38.55 | +0.00 | +0.00 | +18.15 | +18.15 |
| 2026-08-26 | `ASST` | 60 | $20.20 | $20.20 | +0.00 | $20.20 | +0.00 | +0.00 | -42.00 | -42.00 |
| 2026-08-26 | `HMY` | 55 | $22.50 | $22.50 | +0.00 | $22.50 | +0.00 | +0.00 | -8.25 | -8.25 |
| 2026-08-26 | `FCX` | 16 | $77.49 | $77.49 | +0.00 | $77.49 | +0.00 | +0.00 | -6.56 | -6.56 |
| 2026-08-26 | `WPM` | 7 | $158.25 | $158.25 | +0.00 | $158.25 | +0.00 | +0.00 | -12.25 | -12.25 |
| 2026-08-27 | `CYPH` | 745 | $1.64 | $1.60 | -29.80 | — | +0.00 | -29.80 | -74.50 | — |
| 2026-08-27 | `XHG` | 315 | $4.05 | $3.81 | -75.60 | — | +0.00 | -75.60 | -66.15 | — |
| 2026-08-27 | `AU` | 10 | $118.55 | $119.80 | +12.50 | — | +0.00 | +12.50 | +3.40 | — |
| 2026-08-27 | `ERO` | 33 | $38.55 | $40.51 | +64.68 | — | +0.00 | +64.68 | +82.83 | — |
| 2026-08-27 | `ASST` | 60 | $20.20 | $20.72 | +31.20 | — | +0.00 | +31.20 | -10.80 | — |
| 2026-08-27 | `HMY` | 55 | $22.50 | $22.39 | -6.05 | — | +0.00 | -6.05 | -14.30 | — |
| 2026-08-27 | `FCX` | 16 | $77.49 | $79.34 | +29.60 | — | +0.00 | +29.60 | +23.04 | — |
| 2026-08-27 | `WPM` | 7 | $158.25 | $160.93 | +18.76 | — | +0.00 | +18.76 | +6.51 | — |
| 2026-08-27 | `MOS` | 50 | — | $24.84 | +0.00 | $24.16 | -34.00 | -34.00 | +0.00 | -34.00 |
| 2026-08-27 | `SLI` | 484 | — | $2.59 | +0.00 | $2.61 | +9.68 | +9.68 | +0.00 | +9.68 |
| 2026-08-27 | `DLO` | 80 | — | $15.60 | +0.00 | $15.36 | -19.20 | -19.20 | +0.00 | -19.20 |
| 2026-08-27 | `TX` | 22 | — | $55.20 | +0.00 | $55.13 | -1.54 | -1.54 | +0.00 | -1.54 |
| 2026-08-27 | `MRVL` | 5 | — | $240.00 | +0.00 | $245.11 | +25.55 | +25.55 | +0.00 | +25.55 |
| 2026-08-27 | `MU` | 1 | — | $925.74 | +0.00 | $938.40 | +12.66 | +12.66 | +0.00 | +12.66 |
| 2026-08-27 | `PLTR` | 7 | — | $170.60 | +0.00 | $177.50 | +48.30 | +48.30 | +0.00 | +48.30 |
| 2026-08-27 | `MT` | 16 | — | $75.12 | +0.00 | $74.53 | -9.44 | -9.44 | +0.00 | -9.44 |
| 2026-08-28 | `MOS` | 50 | $24.16 | $24.00 | -8.00 | — | +0.00 | -8.00 | -42.00 | — |
| 2026-08-28 | `SLI` | 484 | $2.61 | $2.60 | -4.84 | — | +0.00 | -4.84 | +4.84 | — |
| 2026-08-28 | `DLO` | 80 | $15.36 | $15.33 | -2.40 | — | +0.00 | -2.40 | -21.60 | — |
| 2026-08-28 | `TX` | 22 | $55.13 | $55.25 | +2.64 | — | +0.00 | +2.64 | +1.10 | — |
| 2026-08-28 | `MRVL` | 5 | $245.11 | $253.44 | +41.65 | — | +0.00 | +41.65 | +67.20 | — |
| 2026-08-28 | `MU` | 1 | $938.40 | $967.01 | +28.61 | — | +0.00 | +28.61 | +41.27 | — |
| 2026-08-28 | `PLTR` | 7 | $177.50 | $178.75 | +8.75 | — | +0.00 | +8.75 | +57.05 | — |
| 2026-08-28 | `MT` | 16 | $74.53 | $74.54 | +0.16 | — | +0.00 | +0.16 | -9.28 | — |
| 2026-08-28 | `ERO` | 32 | — | $39.20 | +0.00 | $39.82 | +19.84 | +19.84 | +0.00 | +19.84 |
| 2026-08-28 | `FIGR` | 33 | — | $37.42 | +0.00 | $38.02 | +19.80 | +19.80 | +0.00 | +19.80 |
| 2026-08-28 | `BKKT` | 148 | — | $8.50 | +0.00 | $8.42 | -11.84 | -11.84 | +0.00 | -11.84 |
| 2026-08-28 | `FCX` | 16 | — | $78.83 | +0.00 | $78.42 | -6.56 | -6.56 | +0.00 | -6.56 |
| 2026-08-28 | `QMCO` | 53 | — | $23.50 | +0.00 | $23.56 | +3.18 | +3.18 | +0.00 | +3.18 |
| 2026-08-28 | `TIGR` | 229 | — | $5.49 | +0.00 | $5.06 | -98.47 | -98.47 | +0.00 | -98.47 |
| 2026-08-28 | `NIQ` | 67 | — | $18.79 | +0.00 | $19.07 | +18.76 | +18.76 | +0.00 | +18.76 |
| 2026-08-28 | `VIRT` | 19 | — | $65.42 | +0.00 | $67.04 | +30.78 | +30.78 | +0.00 | +30.78 |
| 2026-08-31 | `ERO` | 32 | $39.82 | $38.60 | -39.04 | — | +0.00 | -39.04 | -19.20 | — |
| 2026-08-31 | `FIGR` | 33 | $38.02 | $35.50 | -83.16 | — | +0.00 | -83.16 | -63.36 | — |
| 2026-08-31 | `BKKT` | 148 | $8.42 | $7.58 | -124.32 | — | +0.00 | -124.32 | -136.16 | — |
| 2026-08-31 | `FCX` | 16 | $78.42 | $76.10 | -37.12 | — | +0.00 | -37.12 | -43.68 | — |
| 2026-08-31 | `QMCO` | 53 | $23.56 | $21.70 | -98.58 | — | +0.00 | -98.58 | -95.40 | — |
| 2026-08-31 | `TIGR` | 229 | $5.06 | $4.96 | -22.90 | $5.01 | +11.45 | -11.45 | -121.37 | -109.92 |
| 2026-08-31 | `NIQ` | 67 | $19.07 | $19.20 | +8.71 | — | +0.00 | +8.71 | +27.47 | — |
| 2026-08-31 | `VIRT` | 19 | $67.04 | $66.39 | -12.35 | — | +0.00 | -12.35 | +18.43 | — |
| 2026-09-01 | `TIGR` | 229 | $5.01 | $5.02 | +2.29 | — | +0.00 | +2.29 | -107.63 | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `MRNA` | 7 | — | $151.40 | +0.00 | $150.81 | -4.13 | -4.13 | +0.00 | -4.13 |
| 2026-09-03 | `ARCT` | 73 | — | $16.46 | +0.00 | $16.74 | +20.44 | +20.44 | +0.00 | +20.44 |
| 2026-09-03 | `XHG` | 337 | — | $3.57 | +0.00 | $3.32 | -84.25 | -84.25 | +0.00 | -84.25 |
| 2026-09-03 | `CAN` | 4016 | — | $0.30 | +0.00 | $0.31 | +40.16 | +40.16 | +0.00 | +40.16 |
| 2026-09-03 | `NVAX` | 117 | — | $10.27 | +0.00 | $10.32 | +5.85 | +5.85 | +0.00 | +5.85 |
| 2026-09-03 | `INO` | 899 | — | $1.34 | +0.00 | $1.36 | +17.98 | +17.98 | +0.00 | +17.98 |
| 2026-09-03 | `RVTY` | 9 | — | $125.94 | +0.00 | $130.94 | +45.00 | +45.00 | +0.00 | +45.00 |
| 2026-09-03 | `ZYME` | 40 | — | $30.00 | +0.00 | $31.05 | +42.00 | +42.00 | +0.00 | +42.00 |
| 2026-09-04 | `MRNA` | 7 | $150.81 | $145.95 | -34.02 | — | +0.00 | -34.02 | -38.15 | — |
| 2026-09-04 | `ARCT` | 73 | $16.74 | $16.77 | +2.19 | — | +0.00 | +2.19 | +22.63 | — |
| 2026-09-04 | `XHG` | 337 | $3.32 | $3.38 | +20.22 | $3.43 | +16.85 | +37.07 | -64.03 | -47.18 |
| 2026-09-04 | `CAN` | 4016 | $0.31 | $0.34 | +120.48 | — | +0.00 | +120.48 | +160.64 | — |
| 2026-09-04 | `NVAX` | 117 | $10.32 | $10.41 | +10.53 | — | +0.00 | +10.53 | +16.38 | — |
| 2026-09-04 | `INO` | 899 | $1.36 | $1.37 | +8.99 | $1.36 | -8.99 | +0.00 | +26.97 | +17.98 |
| 2026-09-04 | `RVTY` | 9 | $130.94 | $132.45 | +13.59 | — | +0.00 | +13.59 | +58.59 | — |
| 2026-09-04 | `ZYME` | 40 | $31.05 | $31.34 | +11.60 | $29.90 | -57.60 | -46.00 | +53.60 | -4.00 |
| 2026-09-04 | `OABI` | 242 | — | $5.08 | +0.00 | $4.75 | -79.86 | -79.86 | +0.00 | -79.86 |
| 2026-09-04 | `TRLV` | 103 | — | $11.89 | +0.00 | $11.99 | +10.30 | +10.30 | +0.00 | +10.30 |
| 2026-09-04 | `ALEC` | 456 | — | $2.70 | +0.00 | $2.51 | -86.64 | -86.64 | +0.00 | -86.64 |
| 2026-09-04 | `OMER` | 64 | — | $18.99 | +0.00 | $19.11 | +7.68 | +7.68 | +0.00 | +7.68 |
| 2026-09-04 | `ATRC` | 23 | — | $52.88 | +0.00 | $52.46 | -9.66 | -9.66 | +0.00 | -9.66 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +300.75 | IREN, TNDM, TPG, INO, HIMS, SLS, VOR, BTSG | — | $107.38 | $10,268.71 | IREN×27, TNDM×53, TPG×24, INO×1543, HIMS×42, SLS×106, VOR×56, BTSG×20 |
| 2026-08-14 | +5.50 | $107.38 | IREN×27, TNDM×53, TPG×24, INO×1543, HIMS×42, SLS×106, VOR×56, BTSG×20 | $10,312.70 | +43.99 | -502.56 | QMCO, ARX, ZENA, AIRO, LIFE, LUNR, TBBB, BZAI | IREN, TNDM, TPG, INO, HIMS, SLS, VOR, BTSG | $23.43 | $9,737.41 | QMCO×52, ARX×65, ZENA×583, AIRO×115, LIFE×36, LUNR×67, TBBB×26, BZAI×1677 |
| 2026-08-17 | +2.25 | $23.43 | QMCO×52, ARX×65, ZENA×583, AIRO×115, LIFE×36, LUNR×67, TBBB×26, BZAI×1677 | $9,642.42 | -94.99 | -269.16 | XHG, CAPR, STDN, HTFL, UMAC, ALOY, NPWR, LPTH | QMCO, ARX, ZENA, AIRO, LIFE, LUNR, TBBB, BZAI | $37.73 | $9,312.74 | XHG×286, CAPR×174, STDN×88, HTFL×29, UMAC×36, ALOY×81, NPWR×625, LPTH×80 |
| 2026-08-18 | -6.20 | $37.73 | XHG×286, CAPR×174, STDN×88, HTFL×29, UMAC×36, ALOY×81, NPWR×625, LPTH×80 | $9,125.28 | -187.46 | -73.08 | — | XHG, STDN, HTFL, UMAC, ALOY, NPWR, LPTH | $7,797.36 | $9,029.28 | CAPR×174 |
| 2026-08-19 | -7.20 | $7,797.36 | CAPR×174 | $9,048.42 | +19.14 | +0.00 | — | CAPR | $9,045.87 | $9,045.87 | — |
| 2026-08-20 | +1.12 | $9,045.87 | — | $9,045.87 | -0.00 | +58.72 | MRNA, CYPH, ABCL, SENS, AUTL, TEM, WPM, IAG | — | $215.49 | $9,073.14 | MRNA×7, CYPH×983, ABCL×95, SENS×126, AUTL×457, TEM×18, WPM×7, IAG×57 |
| 2026-08-21 | +3.25 | $215.49 | MRNA×7, CYPH×983, ABCL×95, SENS×126, AUTL×457, TEM×18, WPM×7, IAG×57 | $9,307.39 | +234.25 | +357.46 | XHG, ARCT, IOVA, CAPR, AU | ABCL, SENS, AUTL, WPM, IAG | $107.24 | $9,637.37 | MRNA×7, CYPH×983, TEM×18, XHG×262, ARCT×105, IOVA×129, CAPR×172, AU×9 |
| 2026-08-24 | -5.17 | $107.24 | MRNA×7, CYPH×983, TEM×18, XHG×262, ARCT×105, IOVA×129, CAPR×172, AU×9 | $10,170.14 | +532.77 | +0.00 | — | MRNA, CYPH, TEM, XHG, ARCT, IOVA, CAPR, AU | $10,140.43 | $10,140.43 | — |
| 2026-08-25 | +1.80 | $10,140.43 | — | $10,140.43 | -0.00 | -95.26 | CYPH, XHG, AU, ERO, ASST, HMY, FCX, WPM | — | $266.72 | $10,019.01 | CYPH×745, XHG×315, AU×10, ERO×33, ASST×60, HMY×55, FCX×16, WPM×7 |
| 2026-08-26 | +2.02 | $266.72 | CYPH×745, XHG×315, AU×10, ERO×33, ASST×60, HMY×55, FCX×16, WPM×7 | $10,019.01 | -0.00 | +0.00 | — | — | $266.72 | $10,019.01 | CYPH×745, XHG×315, AU×10, ERO×33, ASST×60, HMY×55, FCX×16, WPM×7 |
| 2026-08-27 | — | $266.72 | CYPH×745, XHG×315, AU×10, ERO×33, ASST×60, HMY×55, FCX×16, WPM×7 | $10,064.30 | +45.29 | +32.01 | MOS, SLI, DLO, TX, MRVL, MU, PLTR, MT | CYPH, XHG, AU, ERO, ASST, HMY, FCX, WPM | $537.29 | $10,049.12 | MOS×50, SLI×484, DLO×80, TX×22, MRVL×5, MU×1, PLTR×7, MT×16 |
| 2026-08-28 | +0.75 | $537.29 | MOS×50, SLI×484, DLO×80, TX×22, MRVL×5, MU×1, PLTR×7, MT×16 | $10,115.69 | +66.57 | -24.51 | ERO, FIGR, BKKT, FCX, QMCO, TIGR, NIQ, VIRT | MOS, SLI, DLO, TX, MRVL, MU, PLTR, MT | $63.59 | $10,052.24 | ERO×32, FIGR×33, BKKT×148, FCX×16, QMCO×53, TIGR×229, NIQ×67, VIRT×19 |
| 2026-08-31 | -5.85 | $63.59 | ERO×32, FIGR×33, BKKT×148, FCX×16, QMCO×53, TIGR×229, NIQ×67, VIRT×19 | $9,643.48 | -408.76 | +11.45 | — | ERO, FIGR, BKKT, FCX, QMCO, NIQ, VIRT | $8,492.45 | $9,639.74 | TIGR×229 |
| 2026-09-01 | -6.30 | $8,492.45 | TIGR×229 | $9,642.03 | +2.29 | +0.00 | — | TIGR | $9,639.03 | $9,639.03 | — |
| 2026-09-02 | -3.83 | $9,639.03 | — | $9,639.03 | -0.00 | +0.00 | — | — | $9,639.03 | $9,639.03 | — |
| 2026-09-03 | -0.90 | $9,639.03 | — | $9,639.03 | -0.00 | +83.05 | MRNA, ARCT, XHG, CAN, NVAX, INO, RVTY, ZYME | — | $179.32 | $9,671.35 | MRNA×7, ARCT×73, XHG×337, CAN×4016, NVAX×117, INO×899, RVTY×9, ZYME×40 |
| 2026-09-04 | — | $179.32 | MRNA×7, ARCT×73, XHG×337, CAN×4016, NVAX×117, INO×899, RVTY×9, ZYME×40 | $9,824.93 | +153.58 | -207.92 | OABI, TRLV, ALEC, OMER, ATRC | MRNA, ARCT, CAN, NVAX, RVTY | $33.22 | $9,566.42 | XHG×337, INO×899, ZYME×40, OABI×242, TRLV×103, ALEC×456, OMER×64, ATRC×23 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $8,756.47 | — | rank by w_hot_cond; rank w_hot_cond; list flatten; ⚪; ret5=+12.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $7,517.83 | — | rank by w_hot_cond; rank w_hot_cond; list flatten; ⚪; ret5=+19.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $6,300.81 | — | rank by w_hot_cond; rank w_hot_cond; list flatten; ⚪; ret5=+6.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $5,033.85 | — | rank by w_hot_cond; rank w_hot_cond; list flatten; ⚪; ret5=+13.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $3,782.66 | — | rank by w_hot_cond; rank w_hot_cond; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $2,540.15 | — | rank by w_hot_cond; rank w_hot_cond; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 56 | $22.01 | $2.16 | — | $1,305.43 | — | rank by w_hot_cond; rank w_hot_cond; list flatten; ⚪; ret5=+0.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $107.38 | — | rank by w_hot_cond; rank w_hot_cond; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $107.38 | ▲ close $10,268.71 vs 09:30 $10,000.00 (session +300.75) | 16:00 close · cash $107.38 · equity $10,268.71 vs 09:30 $10,000.00 (+268.71; session marks +300.75) · 8 name(s) marked open→close (per-name table). IREN×27 09:30 $45.98 → close $44.76 -32.94; TNDM×53 09:30 $23.33 → close $23.13 -10.60; TPG×24 09:30 $50.62 → close $54.62 +95.92; INO×1543 09:30 $0.81 → close $0.90 +138.87; HIMS×42 09:30 $29.74 → close $28.77 -40.74; SLS×106 09:30 $11.70 → close $12.36 +69.96; VOR×56 09:30 $22.01 → close $23.29 +71.68; BTSG×20 09:30 $59.80 → close $60.23 +8.60 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $107.38 | ▲ 09:30 equity $10,312.70 vs yday $10,268.71 (+43.99) | 09:30 open · cash $107.38 (unchanged overnight, no fees) · equity $10,312.70 vs prior close $10,268.71 (+43.99) · 8 name(s) re-marked at the open (per-name table). IREN×27 yday $44.76 → 09:30 $44.09 -18.09; TNDM×53 yday $23.13 → 09:30 $22.92 -11.13; TPG×24 yday $54.62 → 09:30 $55.29 +16.08; INO×1543 yday $0.90 → 09:30 $0.93 +46.29; HIMS×42 yday $28.77 → 09:30 $29.15 +15.96; SLS×106 yday $12.36 → 09:30 $12.40 +4.24; VOR×56 yday $23.29 → 09:30 $23.33 +2.24; BTSG×20 yday $60.23 → 09:30 $59.65 -11.60 | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 27 | $44.09 | $2.09 | $-55.19 | $1,295.72 | ▼ -55.19 after sell → book $10,310.61; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 53 | $22.92 | $2.17 | $-26.05 | $2,508.31 | ▼ -26.05 after sell → book $10,308.44; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 24 | $55.29 | $2.08 | $+107.86 | $3,833.19 | ▲ +107.86 after sell → book $10,306.36; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 1543 | $0.93 | $19.25 | $+148.79 | $5,248.93 | ▲ +148.79 after sell → book $10,287.11; vs 09:30 mark -19.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `HIMS` | 42 | $29.15 | $2.14 | $-29.03 | $6,471.10 | ▼ -29.03 after sell → book $10,284.98; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `SLS` | 106 | $12.40 | $2.34 | $+69.56 | $7,783.16 | ▲ +69.56 after sell → book $10,282.64; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `VOR` | 56 | $23.33 | $2.18 | $+69.58 | $9,087.46 | ▲ +69.58 after sell → book $10,280.46; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 20 | $59.65 | $2.07 | $-7.12 | $10,278.39 | ▼ -7.12 after sell → book $10,278.39; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `QMCO` | 52 | $24.68 | $2.15 | — | $8,992.89 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer; 🔵; ⚪; ret5=+111.3; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 65 | $19.57 | $2.19 | — | $7,718.65 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ZENA` | 583 | $2.20 | $7.52 | — | $6,428.53 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer; 🔵; ⚪; ret5=+54.3; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 115 | $11.12 | $2.33 | — | $5,147.40 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LIFE` | 36 | $35.04 | $2.10 | — | $3,883.86 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ⚪; ret5=+16.7; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LUNR` | 67 | $19.17 | $2.19 | — | $2,597.28 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🔴 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `TBBB` | 26 | $48.82 | $2.07 | — | $1,325.89 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.0; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BZAI` | 1677 | $0.77 | $17.88 | — | $23.43 | — | rank by w_hot_cond; rank w_hot_cond; list earn_react; 🔵; ⚪; ret5=+20.4; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $23.43 | ▼ close $9,737.41 vs 09:30 $10,312.70 (session -502.56) | 16:00 close · cash $23.43 · equity $9,737.41 vs 09:30 $10,312.70 (-575.29; session marks -502.56) · 8 name(s) marked open→close (per-name table). QMCO×52 09:30 $24.68 → close $26.11 +74.36; ARX×65 09:30 $19.57 → close $19.58 +0.65; ZENA×583 09:30 $2.20 → close $2.14 -34.98; AIRO×115 09:30 $11.12 → close $9.57 -178.25; LIFE×36 09:30 $35.04 → close $34.02 -36.72; LUNR×67 09:30 $19.17 → close $19.01 -10.72; TBBB×26 09:30 $48.82 → close $47.79 -26.78; BZAI×1677 09:30 $0.77 → close $0.59 -290.12 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $23.43 | ▼ 09:30 equity $9,642.42 vs yday $9,737.41 (-94.99) | 09:30 open · cash $23.43 (unchanged overnight, no fees) · equity $9,642.42 vs prior close $9,737.41 (-94.99) · 8 name(s) re-marked at the open (per-name table). QMCO×52 yday $26.11 → 09:30 $24.83 -66.56; ARX×65 yday $19.58 → 09:30 $19.57 -0.65; ZENA×583 yday $2.14 → 09:30 $2.08 -32.07; AIRO×115 yday $9.57 → 09:30 $9.57 +0.00; LIFE×36 yday $34.02 → 09:30 $34.03 +0.36; LUNR×67 yday $19.01 → 09:30 $20.25 +83.08; TBBB×26 yday $47.79 → 09:30 $47.39 -10.40; BZAI×1677 yday $0.59 → 09:30 $0.55 -68.76 | — |
| 2026-08-17 09:30 ET | **SELL** | `QMCO` | 52 | $24.83 | $2.17 | $+3.49 | $1,312.42 | ▲ +3.49 after sell → book $9,640.25; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 65 | $19.57 | $2.21 | $-4.39 | $2,582.27 | ▼ -4.39 after sell → book $9,638.05; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ZENA` | 583 | $2.08 | $7.63 | $-82.19 | $3,790.19 | ▼ -82.19 after sell → book $9,630.42; vs 09:30 mark -7.63 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 115 | $9.57 | $2.36 | $-182.95 | $4,888.38 | ▼ -182.95 after sell → book $9,628.05; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LIFE` | 36 | $34.03 | $2.12 | $-40.58 | $6,111.34 | ▼ -40.58 after sell → book $9,625.94; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LUNR` | 67 | $20.25 | $2.21 | $+67.96 | $7,465.88 | ▲ +67.96 after sell → book $9,623.72; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `TBBB` | 26 | $47.39 | $2.09 | $-41.34 | $8,695.93 | ▼ -41.34 after sell → book $9,621.64; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BZAI` | 1677 | $0.55 | $14.58 | $-391.33 | $9,607.06 | ▼ -391.33 after sell → book $9,607.06; vs 09:30 mark -14.58 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 286 | $4.19 | $3.69 | — | $8,405.03 | — | rank by w_hot_cond; rank w_hot_cond; list yday_mover; ⚪; ret5=+291.8; leftover $1200.88 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 174 | $6.87 | $2.51 | — | $7,207.14 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; ret5=+62.6; leftover $1200.88 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `STDN` | 88 | $13.64 | $2.25 | — | $6,004.56 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer; 🔵; ⚪; ret5=+57.7; leftover $1200.88 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 29 | $41.23 | $2.08 | — | $4,806.82 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; ret5=+46.0; leftover $1200.88 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `UMAC` | 36 | $32.55 | $2.10 | — | $3,632.92 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; ⚪; ret5=+30.4; leftover $1200.88 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ALOY` | 81 | $14.66 | $2.23 | — | $2,443.23 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.0; leftover $1200.88 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `NPWR` | 625 | $1.92 | $8.06 | — | $1,235.16 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $1200.88 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `LPTH` | 80 | $14.94 | $2.23 | — | $37.73 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover,ohlc_hot; ⚪; ret5=+16.2; leftover $1200.88 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟢 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $37.73 | ▼ close $9,312.74 vs 09:30 $9,642.42 (session -269.16) | 16:00 close · cash $37.73 · equity $9,312.74 vs 09:30 $9,642.42 (-329.68; session marks -269.16) · 8 name(s) marked open→close (per-name table). XHG×286 09:30 $4.19 → close $3.91 -80.08; CAPR×174 09:30 $6.87 → close $7.45 +100.92; STDN×88 09:30 $13.64 → close $13.31 -29.04; HTFL×29 09:30 $41.23 → close $41.94 +20.59; UMAC×36 09:30 $32.55 → close $30.15 -86.40; ALOY×81 09:30 $14.66 → close $13.86 -65.20; NPWR×625 09:30 $1.92 → close $1.73 -118.75; LPTH×80 09:30 $14.94 → close $14.80 -11.20 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $37.73 | ▼ 09:30 equity $9,125.28 vs yday $9,312.74 (-187.46) | 09:30 open · cash $37.73 (unchanged overnight, no fees) · equity $9,125.28 vs prior close $9,312.74 (-187.46) · 8 name(s) re-marked at the open (per-name table). XHG×286 yday $3.91 → 09:30 $3.94 +8.58; CAPR×174 yday $7.45 → 09:30 $7.50 +8.70; STDN×88 yday $13.31 → 09:30 $13.31 +0.00; HTFL×29 yday $41.94 → 09:30 $41.50 -12.76; UMAC×36 yday $30.15 → 09:30 $28.59 -56.16; ALOY×81 yday $13.86 → 09:30 $13.19 -53.87; NPWR×625 yday $1.73 → 09:30 $1.70 -18.75; LPTH×80 yday $14.80 → 09:30 $14.01 -63.20 | — |
| 2026-08-18 09:30 ET | **SELL** | `XHG` | 286 | $3.94 | $3.75 | $-78.94 | $1,160.83 | ▼ -78.94 after sell → book $9,121.54; vs 09:30 mark -3.74 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `STDN` | 88 | $13.31 | $2.28 | $-33.57 | $2,329.83 | ▼ -33.57 after sell → book $9,119.26; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🔴 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 29 | $41.50 | $2.10 | $+3.66 | $3,531.23 | ▲ +3.66 after sell → book $9,117.16; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `UMAC` | 36 | $28.59 | $2.12 | $-146.78 | $4,558.35 | ▼ -146.78 after sell → book $9,115.04; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALOY` | 81 | $13.19 | $2.26 | $-123.56 | $5,624.49 | ▼ -123.56 after sell → book $9,112.79; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `NPWR` | 625 | $1.70 | $8.18 | $-153.74 | $6,678.81 | ▼ -153.74 after sell → book $9,104.61; vs 09:30 mark -8.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `LPTH` | 80 | $14.01 | $2.25 | $-78.88 | $7,797.36 | ▼ -78.88 after sell → book $9,102.36; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,797.36 | ▼ close $9,029.28 vs 09:30 $9,125.28 (session -73.08) | 16:00 close · cash $7,797.36 · equity $9,029.28 vs 09:30 $9,125.28 (-96.00; session marks -73.08) · 1 name(s) marked open→close (per-name table). CAPR×174 09:30 $7.50 → close $7.08 -73.08 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,797.36 | ▲ 09:30 equity $9,048.42 vs yday $9,029.28 (+19.14) | 09:30 open · cash $7,797.36 (unchanged overnight, no fees) · equity $9,048.42 vs prior close $9,029.28 (+19.14) · 1 name(s) re-marked at the open (per-name table). CAPR×174 yday $7.08 → 09:30 $7.19 +19.14 | — |
| 2026-08-19 09:30 ET | **SELL** | `CAPR` | 174 | $7.19 | $2.55 | $+50.62 | $9,045.87 | ▲ +50.62 after sell → book $9,045.87; vs 09:30 mark -2.55 | dropped from list after 2 sess (min 1) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,045.87 | ▲ close $9,045.87 vs 09:30 $9,048.42 (session +0.00) | 16:00 close · cash $9,045.87 · no lots left · equity $9,045.87. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,045.87 | ▲ 09:30 equity $9,045.87 vs yday $9,045.87 (-0.00) | 09:30 open · cash $9,045.87 · no holdings · equity $9,045.87 vs prior close $9,045.87 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 7 | $150.14 | $2.01 | — | $7,992.88 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1130.73 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 983 | $1.15 | $12.68 | — | $6,849.74 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; leftover $1130.73 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABCL` | 95 | $11.81 | $2.27 | — | $5,725.04 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1130.73 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SENS` | 126 | $8.91 | $2.37 | — | $4,600.02 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ⚪; ret5=+14.9; leftover $1130.73 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 457 | $2.47 | $5.90 | — | $3,465.33 | — | rank by w_hot_cond; rank w_hot_cond; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $1130.73 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `TEM` | 18 | $61.83 | $2.04 | — | $2,350.35 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+12.2; leftover $1130.73 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 7 | $144.54 | $2.01 | — | $1,336.56 | — | rank by w_hot_cond; rank w_hot_cond; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1130.73 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 57 | $19.63 | $2.16 | — | $215.49 | — | rank by w_hot_cond; rank w_hot_cond; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1130.73 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $215.49 | ▲ close $9,073.14 vs 09:30 $9,045.87 (session +58.72) | 16:00 close · cash $215.49 · equity $9,073.14 vs 09:30 $9,045.87 (+27.27; session marks +58.72) · 8 name(s) marked open→close (per-name table). MRNA×7 09:30 $150.14 → close $133.32 -117.74; CYPH×983 09:30 $1.15 → close $1.19 +39.32; ABCL×95 09:30 $11.81 → close $11.57 -23.27; SENS×126 09:30 $8.91 → close $8.82 -11.34; AUTL×457 09:30 $2.47 → close $2.46 -4.57; TEM×18 09:30 $61.83 → close $66.65 +86.76; WPM×7 09:30 $144.54 → close $150.25 +39.97; IAG×57 09:30 $19.63 → close $20.50 +49.59 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $215.49 | ▲ 09:30 equity $9,307.39 vs yday $9,073.14 (+234.25) | 09:30 open · cash $215.49 (unchanged overnight, no fees) · equity $9,307.39 vs prior close $9,073.14 (+234.25) · 8 name(s) re-marked at the open (per-name table). MRNA×7 yday $133.32 → 09:30 $133.11 -1.47; CYPH×983 yday $1.19 → 09:30 $1.32 +127.79; ABCL×95 yday $11.57 → 09:30 $11.57 +0.00; SENS×126 yday $8.82 → 09:30 $9.24 +52.92; AUTL×457 yday $2.46 → 09:30 $2.47 +4.57; TEM×18 yday $66.65 → 09:30 $65.60 -18.90; WPM×7 yday $150.25 → 09:30 $154.70 +31.15; IAG×57 yday $20.50 → 09:30 $21.17 +38.19 | — |
| 2026-08-21 09:30 ET | **SELL** | `ABCL` | 95 | $11.57 | $2.30 | $-27.85 | $1,312.33 | ▼ -27.85 after sell → book $9,305.08; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `SENS` | 126 | $9.24 | $2.40 | $+36.81 | $2,474.18 | ▲ +36.81 after sell → book $9,302.69; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `AUTL` | 457 | $2.47 | $5.98 | $-11.88 | $3,596.98 | ▼ -11.88 after sell → book $9,296.70; vs 09:30 mark -5.99 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 7 | $154.70 | $2.03 | $+67.08 | $4,677.85 | ▲ +67.08 after sell → book $9,294.67; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 57 | $21.17 | $2.18 | $+83.44 | $5,882.36 | ▲ +83.44 after sell → book $9,292.49; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `XHG` | 262 | $4.49 | $3.38 | — | $4,702.60 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ret5=+12.7; leftover $1176.47 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 105 | $11.13 | $2.31 | — | $3,531.65 | — | rank by w_hot_cond; rank w_hot_cond; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1176.47 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `IOVA` | 129 | $9.08 | $2.38 | — | $2,357.95 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer; 🔵; ⚪; ret5=+37.9; leftover $1176.47 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CAPR` | 172 | $6.81 | $2.51 | — | $1,184.12 | — | rank by w_hot_cond; rank w_hot_cond; list yday_mover; 🔵; ret5=+62.5; leftover $1176.47 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 9 | $119.43 | $2.02 | — | $107.24 | — | rank by w_hot_cond; rank w_hot_cond; list flatten,mover_buy; 🔵; ⚪; ret5=+20.4; leftover $1176.47 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $107.24 | ▲ close $9,637.37 vs 09:30 $9,307.39 (session +357.46) | 16:00 close · cash $107.24 · equity $9,637.37 vs 09:30 $9,307.39 (+329.98; session marks +357.46) · 8 name(s) marked open→close (per-name table). MRNA×7 09:30 $133.11 → close $145.13 +84.14; CYPH×983 09:30 $1.32 → close $1.42 +98.30; TEM×18 09:30 $65.60 → close $72.69 +127.62; XHG×262 09:30 $4.49 → close $4.41 -20.96; ARCT×105 09:30 $11.13 → close $13.45 +243.60; IOVA×129 09:30 $9.08 → close $8.29 -101.91; CAPR×172 09:30 $6.81 → close $6.29 -89.44; AU×9 09:30 $119.43 → close $121.22 +16.11 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $107.24 | ▲ 09:30 equity $10,170.14 vs yday $9,637.37 (+532.77) | 09:30 open · cash $107.24 (unchanged overnight, no fees) · equity $10,170.14 vs prior close $9,637.37 (+532.77) · 8 name(s) re-marked at the open (per-name table). MRNA×7 yday $145.13 → 09:30 $142.70 -17.01; CYPH×983 yday $1.42 → 09:30 $1.83 +403.03; TEM×18 yday $72.69 → 09:30 $70.07 -47.16; XHG×262 yday $4.41 → 09:30 $4.24 -44.54; ARCT×105 yday $13.45 → 09:30 $13.26 -19.95; IOVA×129 yday $8.29 → 09:30 $8.05 -30.96; CAPR×172 yday $6.29 → 09:30 $8.01 +295.84; AU×9 yday $121.22 → 09:30 $120.50 -6.48 | — |
| 2026-08-24 09:30 ET | **SELL** | `MRNA` | 7 | $142.70 | $2.03 | $-56.12 | $1,104.11 | ▼ -56.12 after sell → book $10,168.11; vs 09:30 mark -2.03 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 983 | $1.83 | $12.86 | $+642.90 | $2,890.14 | ▲ +642.90 after sell → book $10,155.25; vs 09:30 mark -12.86 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `TEM` | 18 | $70.07 | $2.06 | $+144.21 | $4,149.33 | ▲ +144.21 after sell → book $10,153.18; vs 09:30 mark -2.07 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `XHG` | 262 | $4.24 | $3.43 | $-72.31 | $5,256.78 | ▼ -72.31 after sell → book $10,149.75; vs 09:30 mark -3.43 | dropped from list after 1 sess (min 1) | join🟡 sector🟡 gen🔴 news🟡 digest🟢 ab🔴 peer🟢 vol🔴 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 105 | $13.26 | $2.33 | $+219.01 | $6,646.75 | ▲ +219.01 after sell → book $10,147.42; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `IOVA` | 129 | $8.05 | $2.41 | $-137.66 | $7,682.79 | ▼ -137.66 after sell → book $10,145.01; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CAPR` | 172 | $8.01 | $2.55 | $+201.35 | $9,057.96 | ▲ +201.35 after sell → book $10,142.46; vs 09:30 mark -2.55 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 9 | $120.50 | $2.04 | $+5.58 | $10,140.43 | ▲ +5.58 after sell → book $10,140.43; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,140.43 | ▲ close $10,140.43 vs 09:30 $10,170.14 (session +0.00) | 16:00 close · cash $10,140.43 · no lots left · equity $10,140.43. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,140.43 | ▲ 09:30 equity $10,140.43 vs yday $10,140.43 (-0.00) | 09:30 open · cash $10,140.43 · no holdings · equity $10,140.43 vs prior close $10,140.43 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 745 | $1.70 | $9.61 | — | $8,864.32 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+120.5; leftover $1267.55 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `XHG` | 315 | $4.02 | $4.06 | — | $7,593.95 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ret5=+16.1; leftover $1267.55 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 10 | $119.46 | $2.02 | — | $6,397.33 | — | rank by w_hot_cond; rank w_hot_cond; list mover_buy; 🔵; ⚪; ret5=+25.9; leftover $1267.55 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ERO` | 33 | $38.00 | $2.09 | — | $5,141.24 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot,mover_buy; ⚪; ret5=+16.6; leftover $1267.55 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 60 | $20.90 | $2.17 | — | $3,885.07 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer; 🔵; ret5=+47.9; leftover $1267.55 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HMY` | 55 | $22.65 | $2.15 | — | $2,637.17 | — | rank by w_hot_cond; rank w_hot_cond; list mover_buy; ⚪; ret5=+21.1; leftover $1267.55 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 16 | $77.90 | $2.04 | — | $1,388.73 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot,mover_buy; ⚪; ret5=+15.3; leftover $1267.55 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `WPM` | 7 | $160.00 | $2.01 | — | $266.72 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot,mover_buy; ⚪; ret5=+17.6; leftover $1267.55 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $266.72 | ▼ close $10,019.01 vs 09:30 $10,140.43 (session -95.26) | 16:00 close · cash $266.72 · equity $10,019.01 vs 09:30 $10,140.43 (-121.42; session marks -95.26) · 8 name(s) marked open→close (per-name table). CYPH×745 09:30 $1.70 → close $1.64 -44.70; XHG×315 09:30 $4.02 → close $4.05 +9.45; AU×10 09:30 $119.46 → close $118.55 -9.10; ERO×33 09:30 $38.00 → close $38.55 +18.15; ASST×60 09:30 $20.90 → close $20.20 -42.00; HMY×55 09:30 $22.65 → close $22.50 -8.25; FCX×16 09:30 $77.90 → close $77.49 -6.56; WPM×7 09:30 $160.00 → close $158.25 -12.25 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $266.72 | ▲ 09:30 equity $10,019.01 vs yday $10,019.01 (-0.00) | 09:30 open · cash $266.72 (unchanged overnight, no fees) · equity $10,019.01 vs prior close $10,019.01 (-0.00) · 8 name(s) re-marked at the open (per-name table). CYPH×745 yday $1.64 → 09:30 $1.64 +0.00; XHG×315 yday $4.05 → 09:30 $4.05 +0.00; AU×10 yday $118.55 → 09:30 $118.55 +0.00; ERO×33 yday $38.55 → 09:30 $38.55 +0.00; ASST×60 yday $20.20 → 09:30 $20.20 +0.00; HMY×55 yday $22.50 → 09:30 $22.50 +0.00; FCX×16 yday $77.49 → 09:30 $77.49 +0.00; WPM×7 yday $158.25 → 09:30 $158.25 +0.00 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $266.72 | ▲ close $10,019.01 vs 09:30 $10,019.01 (session +0.00) | 16:00 close · cash $266.72 · equity $10,019.01 vs 09:30 $10,019.01 (-0.00; session marks +0.00) · 8 name(s) marked open→close (per-name table). CYPH×745 09:30 $1.64 → close $1.64 +0.00; XHG×315 09:30 $4.05 → close $4.05 +0.00; AU×10 09:30 $118.55 → close $118.55 +0.00; ERO×33 09:30 $38.55 → close $38.55 +0.00; ASST×60 09:30 $20.20 → close $20.20 +0.00; HMY×55 09:30 $22.50 → close $22.50 +0.00; FCX×16 09:30 $77.49 → close $77.49 +0.00; WPM×7 09:30 $158.25 → close $158.25 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $266.72 | ▲ 09:30 equity $10,064.30 vs yday $10,019.01 (+45.29) | 09:30 open · cash $266.72 (unchanged overnight, no fees) · equity $10,064.30 vs prior close $10,019.01 (+45.29) · 8 name(s) re-marked at the open (per-name table). CYPH×745 yday $1.64 → 09:30 $1.60 -29.80; XHG×315 yday $4.05 → 09:30 $3.81 -75.60; AU×10 yday $118.55 → 09:30 $119.80 +12.50; ERO×33 yday $38.55 → 09:30 $40.51 +64.68; ASST×60 yday $20.20 → 09:30 $20.72 +31.20; HMY×55 yday $22.50 → 09:30 $22.39 -6.05; FCX×16 yday $77.49 → 09:30 $79.34 +29.60; WPM×7 yday $158.25 → 09:30 $160.93 +18.76 | — |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 745 | $1.60 | $9.74 | $-93.85 | $1,448.98 | ▼ -93.85 after sell → book $10,054.56; vs 09:30 mark -9.74 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `XHG` | 315 | $3.81 | $4.13 | $-74.34 | $2,645.00 | ▼ -74.34 after sell → book $10,050.43; vs 09:30 mark -4.13 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `AU` | 10 | $119.80 | $2.04 | $-0.66 | $3,840.96 | ▼ -0.66 after sell → book $10,048.39; vs 09:30 mark -2.04 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ERO` | 33 | $40.51 | $2.11 | $+78.63 | $5,175.68 | ▲ +78.63 after sell → book $10,046.28; vs 09:30 mark -2.11 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ASST` | 60 | $20.72 | $2.19 | $-15.16 | $6,416.69 | ▼ -15.16 after sell → book $10,044.09; vs 09:30 mark -2.19 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `HMY` | 55 | $22.39 | $2.17 | $-18.63 | $7,645.96 | ▼ -18.63 after sell → book $10,041.91; vs 09:30 mark -2.18 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FCX` | 16 | $79.34 | $2.06 | $+18.94 | $8,913.35 | ▲ +18.94 after sell → book $10,039.86; vs 09:30 mark -2.05 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `WPM` | 7 | $160.93 | $2.03 | $+2.47 | $10,037.83 | ▲ +2.47 after sell → book $10,037.83; vs 09:30 mark -2.03 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `MOS` | 50 | $24.84 | $2.14 | — | $8,793.69 | — | rank by w_hot_cond; rank w_hot_cond; list flatten; ret5=+13.0; leftover $1254.73 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 484 | $2.59 | $6.24 | — | $7,533.88 | — | rank by w_hot_cond; rank w_hot_cond; list flatten; ret5=+4.2; leftover $1254.73 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `DLO` | 80 | $15.60 | $2.23 | — | $6,283.65 | — | rank by w_hot_cond; rank w_hot_cond; list mover_buy; 🔵; ret5=+7.1; leftover $1254.73 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `TX` | 22 | $55.20 | $2.06 | — | $5,067.20 | — | rank by w_hot_cond; rank w_hot_cond; list mover_buy; 🔵; ret5=+3.0; leftover $1254.73 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MRVL` | 5 | $240.00 | $2.00 | — | $3,865.19 | — | rank by w_hot_cond; rank w_hot_cond; list mover_buy; 🔵; ret5=+6.8; leftover $1254.73 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $925.74 | $1.99 | — | $2,937.46 | — | rank by w_hot_cond; rank w_hot_cond; list mover_buy; 🔵; ret5=-0.5; leftover $1254.73 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `PLTR` | 7 | $170.60 | $2.01 | — | $1,741.25 | — | rank by w_hot_cond; rank w_hot_cond; list mover_buy; 🔵; ret5=+3.4; leftover $1254.73 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MT` | 16 | $75.12 | $2.04 | — | $537.29 | — | rank by w_hot_cond; rank w_hot_cond; list mover_buy; 🔵; ret5=-2.2; leftover $1254.73 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $537.29 | ▲ close $10,049.12 vs 09:30 $10,064.30 (session +32.01) | 16:00 close · cash $537.29 · equity $10,049.12 vs 09:30 $10,064.30 (-15.18; session marks +32.01) · 8 name(s) marked open→close (per-name table). MOS×50 09:30 $24.84 → close $24.16 -34.00; SLI×484 09:30 $2.59 → close $2.61 +9.68; DLO×80 09:30 $15.60 → close $15.36 -19.20; TX×22 09:30 $55.20 → close $55.13 -1.54; MRVL×5 09:30 $240.00 → close $245.11 +25.55; MU×1 09:30 $925.74 → close $938.40 +12.66; PLTR×7 09:30 $170.60 → close $177.50 +48.30; MT×16 09:30 $75.12 → close $74.53 -9.44 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $537.29 | ▲ 09:30 equity $10,115.69 vs yday $10,049.12 (+66.57) | 09:30 open · cash $537.29 (unchanged overnight, no fees) · equity $10,115.69 vs prior close $10,049.12 (+66.57) · 8 name(s) re-marked at the open (per-name table). MOS×50 yday $24.16 → 09:30 $24.00 -8.00; SLI×484 yday $2.61 → 09:30 $2.60 -4.84; DLO×80 yday $15.36 → 09:30 $15.33 -2.40; TX×22 yday $55.13 → 09:30 $55.25 +2.64; MRVL×5 yday $245.11 → 09:30 $253.44 +41.65; MU×1 yday $938.40 → 09:30 $967.01 +28.61; PLTR×7 yday $177.50 → 09:30 $178.75 +8.75; MT×16 yday $74.53 → 09:30 $74.54 +0.16 | — |
| 2026-08-28 09:30 ET | **SELL** | `MOS` | 50 | $24.00 | $2.16 | $-46.30 | $1,735.13 | ▼ -46.30 after sell → book $10,113.53; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `SLI` | 484 | $2.60 | $6.33 | $-7.74 | $2,987.19 | ▼ -7.74 after sell → book $10,107.19; vs 09:30 mark -6.34 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `DLO` | 80 | $15.33 | $2.25 | $-26.08 | $4,211.34 | ▼ -26.08 after sell → book $10,104.94; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `TX` | 22 | $55.25 | $2.08 | $-3.03 | $5,424.77 | ▼ -3.03 after sell → book $10,102.87; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MRVL` | 5 | $253.44 | $2.03 | $+63.17 | $6,689.94 | ▲ +63.17 after sell → book $10,100.84; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $967.01 | $2.01 | $+37.26 | $7,654.94 | ▲ +37.26 after sell → book $10,098.83; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `PLTR` | 7 | $178.75 | $2.03 | $+53.01 | $8,904.16 | ▲ +53.01 after sell → book $10,096.80; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MT` | 16 | $74.54 | $2.06 | $-13.38 | $10,094.74 | ▼ -13.38 after sell → book $10,094.74; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `ERO` | 32 | $39.20 | $2.09 | — | $8,838.25 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; ret5=+16.6; leftover $1261.84 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `FIGR` | 33 | $37.42 | $2.09 | — | $7,601.30 | — | rank by w_hot_cond; rank w_hot_cond; list yday_mover; ret5=+24.4; leftover $1261.84 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🟡 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `BKKT` | 148 | $8.50 | $2.43 | — | $6,340.87 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; ret5=+12.3; leftover $1261.84 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `FCX` | 16 | $78.83 | $2.04 | — | $5,077.55 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; ret5=+15.3; leftover $1261.84 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 catal🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `QMCO` | 53 | $23.50 | $2.15 | — | $3,829.90 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ret5=-14.8; leftover $1261.84 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟡 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TIGR` | 229 | $5.49 | $2.95 | — | $2,569.74 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; ret5=+15.9; leftover $1261.84 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `NIQ` | 67 | $18.79 | $2.19 | — | $1,308.62 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; ret5=+7.6; leftover $1261.84 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `VIRT` | 19 | $65.42 | $2.05 | — | $63.59 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; ret5=+13.2; leftover $1261.84 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $63.59 | ▼ close $10,052.24 vs 09:30 $10,115.69 (session -24.51) | 16:00 close · cash $63.59 · equity $10,052.24 vs 09:30 $10,115.69 (-63.45; session marks -24.51) · 8 name(s) marked open→close (per-name table). ERO×32 09:30 $39.20 → close $39.82 +19.84; FIGR×33 09:30 $37.42 → close $38.02 +19.80; BKKT×148 09:30 $8.50 → close $8.42 -11.84; FCX×16 09:30 $78.83 → close $78.42 -6.56; QMCO×53 09:30 $23.50 → close $23.56 +3.18; TIGR×229 09:30 $5.49 → close $5.06 -98.47; NIQ×67 09:30 $18.79 → close $19.07 +18.76; VIRT×19 09:30 $65.42 → close $67.04 +30.78 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $63.59 | ▼ 09:30 equity $9,643.48 vs yday $10,052.24 (-408.76) | 09:30 open · cash $63.59 (unchanged overnight, no fees) · equity $9,643.48 vs prior close $10,052.24 (-408.76) · 8 name(s) re-marked at the open (per-name table). ERO×32 yday $39.82 → 09:30 $38.60 -39.04; FIGR×33 yday $38.02 → 09:30 $35.50 -83.16; BKKT×148 yday $8.42 → 09:30 $7.58 -124.32; FCX×16 yday $78.42 → 09:30 $76.10 -37.12; QMCO×53 yday $23.56 → 09:30 $21.70 -98.58; TIGR×229 yday $5.06 → 09:30 $4.96 -22.90; NIQ×67 yday $19.07 → 09:30 $19.20 +8.71; VIRT×19 yday $67.04 → 09:30 $66.39 -12.35 | — |
| 2026-08-31 09:30 ET | **SELL** | `ERO` | 32 | $38.60 | $2.11 | $-23.39 | $1,296.68 | ▼ -23.39 after sell → book $9,641.37; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-31 09:30 ET | **SELL** | `FIGR` | 33 | $35.50 | $2.11 | $-67.56 | $2,466.08 | ▼ -67.56 after sell → book $9,639.27; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BKKT` | 148 | $7.58 | $2.47 | $-141.06 | $3,585.45 | ▼ -141.06 after sell → book $9,636.80; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `FCX` | 16 | $76.10 | $2.06 | $-47.78 | $4,800.99 | ▼ -47.78 after sell → book $9,634.74; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `QMCO` | 53 | $21.70 | $2.17 | $-99.72 | $5,948.92 | ▼ -99.72 after sell → book $9,632.57; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `NIQ` | 67 | $19.20 | $2.21 | $+23.07 | $7,233.11 | ▲ +23.07 after sell → book $9,630.36; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🔴 digest🟢 judge🟡 ab🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `VIRT` | 19 | $66.39 | $2.07 | $+14.32 | $8,492.45 | ▲ +14.32 after sell → book $9,628.29; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,492.45 | ▲ close $9,639.74 vs 09:30 $9,643.48 (session +11.45) | 16:00 close · cash $8,492.45 · equity $9,639.74 vs 09:30 $9,643.48 (-3.74; session marks +11.45) · 1 name(s) marked open→close (per-name table). TIGR×229 09:30 $4.96 → close $5.01 +11.45 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,492.45 | ▲ 09:30 equity $9,642.03 vs yday $9,639.74 (+2.29) | 09:30 open · cash $8,492.45 (unchanged overnight, no fees) · equity $9,642.03 vs prior close $9,639.74 (+2.29) · 1 name(s) re-marked at the open (per-name table). TIGR×229 yday $5.01 → 09:30 $5.02 +2.29 | — |
| 2026-09-01 09:30 ET | **SELL** | `TIGR` | 229 | $5.02 | $3.00 | $-113.59 | $9,639.03 | ▼ -113.59 after sell → book $9,639.03; vs 09:30 mark -3.00 | dropped from list after 2 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,639.03 | ▲ close $9,639.03 vs 09:30 $9,642.03 (session +0.00) | 16:00 close · cash $9,639.03 · no lots left · equity $9,639.03. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,639.03 | ▲ 09:30 equity $9,639.03 vs yday $9,639.03 (-0.00) | 09:30 open · cash $9,639.03 · no holdings · equity $9,639.03 vs prior close $9,639.03 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,639.03 | ▲ close $9,639.03 vs 09:30 $9,639.03 (session +0.00) | 16:00 close · cash $9,639.03 · no lots left · equity $9,639.03. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,639.03 | ▲ 09:30 equity $9,639.03 vs yday $9,639.03 (-0.00) | 09:30 open · cash $9,639.03 · no holdings · equity $9,639.03 vs prior close $9,639.03 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 7 | $151.40 | $2.01 | — | $8,577.22 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ret5=+129.2; leftover $1204.88 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 73 | $16.46 | $2.21 | — | $7,373.43 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+63.4; leftover $1204.88 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `XHG` | 337 | $3.57 | $4.35 | — | $6,165.99 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; ret5=+16.1; leftover $1204.88 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CAN` | 4016 | $0.30 | $24.10 | — | $4,937.09 | — | rank by w_hot_cond; rank w_hot_cond; list yday_mover; 🔵; ret5=+54.3; leftover $1204.88 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `NVAX` | 117 | $10.27 | $2.34 | — | $3,733.16 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+11.1; leftover $1204.88 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `INO` | 899 | $1.34 | $11.60 | — | $2,516.91 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ⚪; ret5=+8.3; leftover $1204.88 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $125.94 | $2.02 | — | $1,381.43 | — | rank by w_hot_cond; rank w_hot_cond; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1204.88 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ZYME` | 40 | $30.00 | $2.11 | — | $179.32 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; ⚪; ret5=+14.1; leftover $1204.88 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟡 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $179.32 | ▲ close $9,671.35 vs 09:30 $9,639.03 (session +83.05) | 16:00 close · cash $179.32 · equity $9,671.35 vs 09:30 $9,639.03 (+32.32; session marks +83.05) · 8 name(s) marked open→close (per-name table). MRNA×7 09:30 $151.40 → close $150.81 -4.13; ARCT×73 09:30 $16.46 → close $16.74 +20.44; XHG×337 09:30 $3.57 → close $3.32 -84.25; CAN×4016 09:30 $0.30 → close $0.31 +40.16; NVAX×117 09:30 $10.27 → close $10.32 +5.85; INO×899 09:30 $1.34 → close $1.36 +17.98; RVTY×9 09:30 $125.94 → close $130.94 +45.00; ZYME×40 09:30 $30.00 → close $31.05 +42.00 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $179.32 | ▲ 09:30 equity $9,824.93 vs yday $9,671.35 (+153.58) | 09:30 open · cash $179.32 (unchanged overnight, no fees) · equity $9,824.93 vs prior close $9,671.35 (+153.58) · 8 name(s) re-marked at the open (per-name table). MRNA×7 yday $150.81 → 09:30 $145.95 -34.02; ARCT×73 yday $16.74 → 09:30 $16.77 +2.19; XHG×337 yday $3.32 → 09:30 $3.38 +20.22; CAN×4016 yday $0.31 → 09:30 $0.34 +120.48; NVAX×117 yday $10.32 → 09:30 $10.41 +10.53; INO×899 yday $1.36 → 09:30 $1.37 +8.99; RVTY×9 yday $130.94 → 09:30 $132.45 +13.59; ZYME×40 yday $31.05 → 09:30 $31.34 +11.60 | — |
| 2026-09-04 09:30 ET | **SELL** | `MRNA` | 7 | $145.95 | $2.03 | $-42.19 | $1,198.94 | ▼ -42.19 after sell → book $9,822.90; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 73 | $16.77 | $2.23 | $+18.19 | $2,420.92 | ▲ +18.19 after sell → book $9,820.67; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CAN` | 4016 | $0.34 | $26.38 | $+110.16 | $3,759.98 | ▲ +110.16 after sell → book $9,794.29; vs 09:30 mark -26.38 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `NVAX` | 117 | $10.41 | $2.37 | $+11.67 | $4,975.58 | ▲ +11.67 after sell → book $9,791.92; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 9 | $132.45 | $2.04 | $+54.54 | $6,165.59 | ▲ +54.54 after sell → book $9,789.88; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 242 | $5.08 | $3.12 | — | $4,933.11 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+28.1; leftover $1233.12 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `TRLV` | 103 | $11.89 | $2.30 | — | $3,706.14 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ⚪; ret5=+15.0; leftover $1233.12 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 456 | $2.70 | $5.88 | — | $2,469.06 | — | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.4; leftover $1233.12 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OMER` | 64 | $18.99 | $2.18 | — | $1,251.51 | — | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ret5=+12.1; leftover $1233.12 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ATRC` | 23 | $52.88 | $2.06 | — | $33.22 | — | rank by w_hot_cond; rank w_hot_cond; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1233.12 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $33.22 | ▼ close $9,566.42 vs 09:30 $9,824.93 (session -207.92) | 16:00 close · cash $33.22 · equity $9,566.42 vs 09:30 $9,824.93 (-258.51; session marks -207.92) · 8 name(s) marked open→close (per-name table). XHG×337 09:30 $3.38 → close $3.43 +16.85; INO×899 09:30 $1.37 → close $1.36 -8.99; ZYME×40 09:30 $31.34 → close $29.90 -57.60; OABI×242 09:30 $5.08 → close $4.75 -79.86; TRLV×103 09:30 $11.89 → close $11.99 +10.30; ALEC×456 09:30 $2.70 → close $2.51 -86.64; OMER×64 09:30 $18.99 → close $19.11 +7.68; ATRC×23 09:30 $52.88 → close $52.46 -9.66 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `OABI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AVAH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OCUL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AXTI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FIGR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `IOVA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ARX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `IVVD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WEAV` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KURA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SENS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `REAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `ERO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ALM` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HMY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `KGC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `GFI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SCCO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-26 | `CYPH` | no_price | no 09:30 open — carry |
| 2026-08-26 | `AU` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ASST` | no_price | no 09:30 open — carry |
| 2026-08-26 | `HMY` | no_price | no 09:30 open — carry |
| 2026-08-26 | `USDE` | no_price | no 09:30 open |
| 2026-08-26 | `HOOD` | no_price | no 09:30 open |
| 2026-08-26 | `AEM` | no_price | no 09:30 open |
| 2026-08-26 | `SCCO` | no_price | no 09:30 open |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `XHG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PURR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CAN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CVI` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `DK` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NVAX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OMER` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `VFF` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OBE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `INO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TRGP` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `USDE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `XHG` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TXG` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ZYME` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `DK` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `OMER` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `NVAX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ALVO` | hard_red | hard-red S=-3.83 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `XHG` | 337 | 2026-09-03 @ $3.57 | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; ret5=+16.1; leftover $1204.88 |
| `INO` | 899 | 2026-09-03 @ $1.34 | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ⚪; ret5=+8.3; leftover $1204.88 |
| `ZYME` | 40 | 2026-09-03 @ $30.00 | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; ⚪; ret5=+14.1; leftover $1204.88 |
| `OABI` | 242 | 2026-09-04 @ $5.08 | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+28.1; leftover $1233.12 |
| `TRLV` | 103 | 2026-09-04 @ $11.89 | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ⚪; ret5=+15.0; leftover $1233.12 |
| `ALEC` | 456 | 2026-09-04 @ $2.70 | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.4; leftover $1233.12 |
| `OMER` | 64 | 2026-09-04 @ $18.99 | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ret5=+12.1; leftover $1233.12 |
| `ATRC` | 23 | 2026-09-04 @ $52.88 | rank by w_hot_cond; rank w_hot_cond; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1233.12 |
