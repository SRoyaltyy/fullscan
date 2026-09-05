# Factor mine action — `union_hot_score_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `hot_score` · size `leftover` · sell `list` · S-boost `none` · rank by hot_score

Cash book **-0.85%** ($9,915) · signal-only (no cash/fees) was -0.76%. Starts YES **5/17**. Fills 138 · skips 56 · realized $+136.47.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `hot_score` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $1.48.

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
| 2026-08-14 | `BZAI` | 1677 | — | $0.77 | +0.00 | $0.59 | -290.12 | -290.12 | +0.00 | -290.12 |
| 2026-08-14 | `VOYG` | 28 | — | $44.49 | +0.00 | $42.98 | -42.28 | -42.28 | +0.00 | -42.28 |
| 2026-08-14 | `LUNR` | 67 | — | $19.17 | +0.00 | $19.01 | -10.72 | -10.72 | +0.00 | -10.72 |
| 2026-08-17 | `QMCO` | 52 | $26.11 | $24.83 | -66.56 | — | +0.00 | -66.56 | +7.80 | — |
| 2026-08-17 | `ARX` | 65 | $19.58 | $19.57 | -0.65 | — | +0.00 | -0.65 | +0.00 | — |
| 2026-08-17 | `ZENA` | 583 | $2.14 | $2.08 | -32.07 | — | +0.00 | -32.07 | -67.05 | — |
| 2026-08-17 | `AIRO` | 115 | $9.57 | $9.57 | +0.00 | — | +0.00 | +0.00 | -178.25 | — |
| 2026-08-17 | `LIFE` | 36 | $34.02 | $34.03 | +0.36 | — | +0.00 | +0.36 | -36.36 | — |
| 2026-08-17 | `BZAI` | 1677 | $0.59 | $0.55 | -68.76 | — | +0.00 | -68.76 | -358.88 | — |
| 2026-08-17 | `VOYG` | 28 | $42.98 | $42.12 | -24.08 | — | +0.00 | -24.08 | -66.36 | — |
| 2026-08-17 | `LUNR` | 67 | $19.01 | $20.25 | +83.08 | — | +0.00 | +83.08 | +72.36 | — |
| 2026-08-17 | `XHG` | 285 | — | $4.19 | +0.00 | $3.91 | -79.80 | -79.80 | +0.00 | -79.80 |
| 2026-08-17 | `CAPR` | 174 | — | $6.87 | +0.00 | $7.45 | +100.92 | +100.92 | +0.00 | +100.92 |
| 2026-08-17 | `STDN` | 87 | — | $13.64 | +0.00 | $13.31 | -28.71 | -28.71 | +0.00 | -28.71 |
| 2026-08-17 | `HTFL` | 29 | — | $41.23 | +0.00 | $41.94 | +20.59 | +20.59 | +0.00 | +20.59 |
| 2026-08-17 | `UMAC` | 36 | — | $32.55 | +0.00 | $30.15 | -86.40 | -86.40 | +0.00 | -86.40 |
| 2026-08-17 | `SMJF` | 118 | — | $10.10 | +0.00 | $10.45 | +41.30 | +41.30 | +0.00 | +41.30 |
| 2026-08-17 | `ALOY` | 81 | — | $14.66 | +0.00 | $13.86 | -65.20 | -65.20 | +0.00 | -65.20 |
| 2026-08-17 | `NPWR` | 623 | — | $1.92 | +0.00 | $1.73 | -118.37 | -118.37 | +0.00 | -118.37 |
| 2026-08-18 | `XHG` | 285 | $3.91 | $3.94 | +8.55 | — | +0.00 | +8.55 | -71.25 | — |
| 2026-08-18 | `CAPR` | 174 | $7.45 | $7.50 | +8.70 | $7.08 | -73.08 | -64.38 | +109.62 | +36.54 |
| 2026-08-18 | `STDN` | 87 | $13.31 | $13.31 | +0.00 | — | +0.00 | +0.00 | -28.71 | — |
| 2026-08-18 | `HTFL` | 29 | $41.94 | $41.50 | -12.76 | — | +0.00 | -12.76 | +7.83 | — |
| 2026-08-18 | `UMAC` | 36 | $30.15 | $28.59 | -56.16 | — | +0.00 | -56.16 | -142.56 | — |
| 2026-08-18 | `SMJF` | 118 | $10.45 | $10.45 | +0.00 | — | +0.00 | +0.00 | +41.30 | — |
| 2026-08-18 | `ALOY` | 81 | $13.86 | $13.19 | -53.87 | — | +0.00 | -53.87 | -119.07 | — |
| 2026-08-18 | `NPWR` | 623 | $1.73 | $1.70 | -18.69 | — | +0.00 | -18.69 | -137.06 | — |
| 2026-08-19 | `CAPR` | 174 | $7.08 | $7.19 | +19.14 | — | +0.00 | +19.14 | +55.68 | — |
| 2026-08-20 | `MRNA` | 7 | — | $150.14 | +0.00 | $133.32 | -117.74 | -117.74 | +0.00 | -117.74 |
| 2026-08-20 | `CYPH` | 992 | — | $1.15 | +0.00 | $1.19 | +39.68 | +39.68 | +0.00 | +39.68 |
| 2026-08-20 | `ABCL` | 96 | — | $11.81 | +0.00 | $11.57 | -23.52 | -23.52 | +0.00 | -23.52 |
| 2026-08-20 | `AZI` | 833 | — | $1.37 | +0.00 | $1.44 | +58.31 | +58.31 | +0.00 | +58.31 |
| 2026-08-20 | `SENS` | 128 | — | $8.91 | +0.00 | $8.82 | -11.52 | -11.52 | +0.00 | -11.52 |
| 2026-08-20 | `ALEC` | 475 | — | $2.40 | +0.00 | $2.26 | -66.50 | -66.50 | +0.00 | -66.50 |
| 2026-08-20 | `BTGO` | 172 | — | $6.61 | +0.00 | $6.60 | -0.86 | -0.86 | +0.00 | -0.86 |
| 2026-08-20 | `AUTL` | 462 | — | $2.47 | +0.00 | $2.46 | -4.62 | -4.62 | +0.00 | -4.62 |
| 2026-08-21 | `MRNA` | 7 | $133.32 | $133.11 | -1.47 | $145.13 | +84.14 | +82.67 | -119.21 | -35.07 |
| 2026-08-21 | `CYPH` | 992 | $1.19 | $1.32 | +128.96 | $1.42 | +99.20 | +228.16 | +168.64 | +267.84 |
| 2026-08-21 | `ABCL` | 96 | $11.57 | $11.57 | +0.00 | — | +0.00 | +0.00 | -23.52 | — |
| 2026-08-21 | `AZI` | 833 | $1.44 | $1.46 | +16.66 | — | +0.00 | +16.66 | +74.97 | — |
| 2026-08-21 | `SENS` | 128 | $8.82 | $9.24 | +53.76 | — | +0.00 | +53.76 | +42.24 | — |
| 2026-08-21 | `ALEC` | 475 | $2.26 | $2.28 | +9.50 | — | +0.00 | +9.50 | -57.00 | — |
| 2026-08-21 | `BTGO` | 172 | $6.60 | $6.95 | +60.20 | — | +0.00 | +60.20 | +59.34 | — |
| 2026-08-21 | `AUTL` | 462 | $2.46 | $2.47 | +4.62 | — | +0.00 | +4.62 | +0.00 | — |
| 2026-08-21 | `XHG` | 258 | — | $4.49 | +0.00 | $4.41 | -20.64 | -20.64 | +0.00 | -20.64 |
| 2026-08-21 | `CAPR` | 170 | — | $6.81 | +0.00 | $6.29 | -88.40 | -88.40 | +0.00 | -88.40 |
| 2026-08-21 | `ARCT` | 104 | — | $11.13 | +0.00 | $13.45 | +241.28 | +241.28 | +0.00 | +241.28 |
| 2026-08-21 | `IOVA` | 127 | — | $9.08 | +0.00 | $8.29 | -100.33 | -100.33 | +0.00 | -100.33 |
| 2026-08-21 | `CAN` | 3946 | — | $0.29 | +0.00 | $0.35 | +240.71 | +240.71 | +0.00 | +240.71 |
| 2026-08-21 | `TEM` | 17 | — | $65.60 | +0.00 | $72.69 | +120.53 | +120.53 | +0.00 | +120.53 |
| 2026-08-24 | `MRNA` | 7 | $145.13 | $142.70 | -17.01 | — | +0.00 | -17.01 | -52.08 | — |
| 2026-08-24 | `CYPH` | 992 | $1.42 | $1.83 | +406.72 | — | +0.00 | +406.72 | +674.56 | — |
| 2026-08-24 | `XHG` | 258 | $4.41 | $4.24 | -43.86 | — | +0.00 | -43.86 | -64.50 | — |
| 2026-08-24 | `CAPR` | 170 | $6.29 | $8.01 | +292.40 | — | +0.00 | +292.40 | +204.00 | — |
| 2026-08-24 | `ARCT` | 104 | $13.45 | $13.26 | -19.76 | — | +0.00 | -19.76 | +221.52 | — |
| 2026-08-24 | `IOVA` | 127 | $8.29 | $8.05 | -30.48 | — | +0.00 | -30.48 | -130.81 | — |
| 2026-08-24 | `CAN` | 3946 | $0.35 | $0.38 | +98.65 | — | +0.00 | +98.65 | +339.36 | — |
| 2026-08-24 | `TEM` | 17 | $72.69 | $70.07 | -44.54 | — | +0.00 | -44.54 | +75.99 | — |
| 2026-08-25 | `CYPH` | 759 | — | $1.70 | +0.00 | $1.64 | -45.54 | -45.54 | +0.00 | -45.54 |
| 2026-08-25 | `XHG` | 321 | — | $4.02 | +0.00 | $4.05 | +9.63 | +9.63 | +0.00 | +9.63 |
| 2026-08-25 | `ASST` | 61 | — | $20.90 | +0.00 | $20.20 | -42.70 | -42.70 | +0.00 | -42.70 |
| 2026-08-25 | `AU` | 10 | — | $119.46 | +0.00 | $118.55 | -9.10 | -9.10 | +0.00 | -9.10 |
| 2026-08-25 | `RUM` | 137 | — | $9.36 | +0.00 | $9.35 | -1.37 | -1.37 | +0.00 | -1.37 |
| 2026-08-25 | `BMNR` | 52 | — | $24.73 | +0.00 | $24.21 | -27.04 | -27.04 | +0.00 | -27.04 |
| 2026-08-25 | `NIQ` | 66 | — | $19.56 | +0.00 | $19.46 | -6.60 | -6.60 | +0.00 | -6.60 |
| 2026-08-25 | `DEFT` | 2017 | — | $0.64 | +0.00 | $0.62 | -40.34 | -40.34 | +0.00 | -40.34 |
| 2026-08-26 | `CYPH` | 759 | $1.64 | $1.64 | +0.00 | $1.64 | +0.00 | +0.00 | -45.54 | -45.54 |
| 2026-08-26 | `XHG` | 321 | $4.05 | $4.05 | +0.00 | $4.05 | +0.00 | +0.00 | +9.63 | +9.63 |
| 2026-08-26 | `ASST` | 61 | $20.20 | $20.20 | +0.00 | $20.20 | +0.00 | +0.00 | -42.70 | -42.70 |
| 2026-08-26 | `AU` | 10 | $118.55 | $118.55 | +0.00 | $118.55 | +0.00 | +0.00 | -9.10 | -9.10 |
| 2026-08-26 | `RUM` | 137 | $9.35 | $9.35 | +0.00 | $9.35 | +0.00 | +0.00 | -1.37 | -1.37 |
| 2026-08-26 | `BMNR` | 52 | $24.21 | $24.21 | +0.00 | $24.21 | +0.00 | +0.00 | -27.04 | -27.04 |
| 2026-08-26 | `NIQ` | 66 | $19.46 | $19.46 | +0.00 | $19.46 | +0.00 | +0.00 | -6.60 | -6.60 |
| 2026-08-26 | `DEFT` | 2017 | $0.62 | $0.62 | +0.00 | $0.62 | +0.00 | +0.00 | -40.34 | -40.34 |
| 2026-08-27 | `CYPH` | 759 | $1.64 | $1.60 | -30.36 | — | +0.00 | -30.36 | -75.90 | — |
| 2026-08-27 | `XHG` | 321 | $4.05 | $3.81 | -77.04 | — | +0.00 | -77.04 | -67.41 | — |
| 2026-08-27 | `ASST` | 61 | $20.20 | $20.72 | +31.72 | — | +0.00 | +31.72 | -10.98 | — |
| 2026-08-27 | `AU` | 10 | $118.55 | $119.80 | +12.50 | — | +0.00 | +12.50 | +3.40 | — |
| 2026-08-27 | `RUM` | 137 | $9.35 | $10.07 | +98.64 | — | +0.00 | +98.64 | +97.27 | — |
| 2026-08-27 | `BMNR` | 52 | $24.21 | $24.24 | +1.56 | — | +0.00 | +1.56 | -25.48 | — |
| 2026-08-27 | `NIQ` | 66 | $19.46 | $19.20 | -17.16 | — | +0.00 | -17.16 | -23.76 | — |
| 2026-08-27 | `DEFT` | 2017 | $0.62 | $0.60 | -40.34 | — | +0.00 | -40.34 | -80.68 | — |
| 2026-08-27 | `MOS` | 50 | — | $24.84 | +0.00 | $24.16 | -34.00 | -34.00 | +0.00 | -34.00 |
| 2026-08-27 | `DLO` | 80 | — | $15.60 | +0.00 | $15.36 | -19.20 | -19.20 | +0.00 | -19.20 |
| 2026-08-27 | `SLI` | 485 | — | $2.59 | +0.00 | $2.61 | +9.70 | +9.70 | +0.00 | +9.70 |
| 2026-08-27 | `MRVL` | 5 | — | $240.00 | +0.00 | $245.11 | +25.55 | +25.55 | +0.00 | +25.55 |
| 2026-08-27 | `CRK` | 89 | — | $14.09 | +0.00 | $14.50 | +36.49 | +36.49 | +0.00 | +36.49 |
| 2026-08-27 | `PLTR` | 7 | — | $170.60 | +0.00 | $177.50 | +48.30 | +48.30 | +0.00 | +48.30 |
| 2026-08-27 | `RRC` | 30 | — | $40.72 | +0.00 | $41.55 | +24.90 | +24.90 | +0.00 | +24.90 |
| 2026-08-27 | `GEN` | 43 | — | $28.89 | +0.00 | $29.64 | +32.25 | +32.25 | +0.00 | +32.25 |
| 2026-08-28 | `MOS` | 50 | $24.16 | $24.00 | -8.00 | — | +0.00 | -8.00 | -42.00 | — |
| 2026-08-28 | `DLO` | 80 | $15.36 | $15.33 | -2.40 | — | +0.00 | -2.40 | -21.60 | — |
| 2026-08-28 | `SLI` | 485 | $2.61 | $2.60 | -4.85 | — | +0.00 | -4.85 | +4.85 | — |
| 2026-08-28 | `MRVL` | 5 | $245.11 | $253.44 | +41.65 | — | +0.00 | +41.65 | +67.20 | — |
| 2026-08-28 | `CRK` | 89 | $14.50 | $14.42 | -7.12 | — | +0.00 | -7.12 | +29.37 | — |
| 2026-08-28 | `PLTR` | 7 | $177.50 | $178.75 | +8.75 | — | +0.00 | +8.75 | +57.05 | — |
| 2026-08-28 | `RRC` | 30 | $41.55 | $41.44 | -3.30 | — | +0.00 | -3.30 | +21.60 | — |
| 2026-08-28 | `GEN` | 43 | $29.64 | $29.83 | +8.17 | — | +0.00 | +8.17 | +40.42 | — |
| 2026-08-28 | `FIGR` | 33 | — | $37.42 | +0.00 | $38.02 | +19.80 | +19.80 | +0.00 | +19.80 |
| 2026-08-28 | `NIQ` | 67 | — | $18.79 | +0.00 | $19.07 | +18.76 | +18.76 | +0.00 | +18.76 |
| 2026-08-28 | `ERO` | 32 | — | $39.20 | +0.00 | $39.82 | +19.84 | +19.84 | +0.00 | +19.84 |
| 2026-08-28 | `TRLV` | 111 | — | $11.38 | +0.00 | $11.03 | -38.85 | -38.85 | +0.00 | -38.85 |
| 2026-08-28 | `CVI` | 31 | — | $40.04 | +0.00 | $39.76 | -8.68 | -8.68 | +0.00 | -8.68 |
| 2026-08-28 | `VIRT` | 19 | — | $65.42 | +0.00 | $67.04 | +30.78 | +30.78 | +0.00 | +30.78 |
| 2026-08-28 | `TXG` | 19 | — | $64.10 | +0.00 | $64.85 | +14.25 | +14.25 | +0.00 | +14.25 |
| 2026-08-28 | `GUTS` | 1718 | — | $0.74 | +0.00 | $0.74 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-31 | `FIGR` | 33 | $38.02 | $35.50 | -83.16 | — | +0.00 | -83.16 | -63.36 | — |
| 2026-08-31 | `NIQ` | 67 | $19.07 | $19.20 | +8.71 | $19.20 | +0.00 | +8.71 | +27.47 | +27.47 |
| 2026-08-31 | `ERO` | 32 | $39.82 | $38.60 | -39.04 | — | +0.00 | -39.04 | -19.20 | — |
| 2026-08-31 | `TRLV` | 111 | $11.03 | $12.41 | +153.18 | — | +0.00 | +153.18 | +114.33 | — |
| 2026-08-31 | `CVI` | 31 | $39.76 | $41.76 | +62.00 | — | +0.00 | +62.00 | +53.32 | — |
| 2026-08-31 | `VIRT` | 19 | $67.04 | $66.39 | -12.35 | — | +0.00 | -12.35 | +18.43 | — |
| 2026-08-31 | `TXG` | 19 | $64.85 | $60.90 | -75.05 | — | +0.00 | -75.05 | -60.80 | — |
| 2026-08-31 | `GUTS` | 1718 | $0.74 | $0.67 | -120.26 | — | +0.00 | -120.26 | -120.26 | — |
| 2026-09-01 | `NIQ` | 67 | $19.20 | $19.06 | -9.38 | — | +0.00 | -9.38 | +18.09 | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `MRNA` | 8 | — | $151.40 | +0.00 | $150.81 | -4.72 | -4.72 | +0.00 | -4.72 |
| 2026-09-03 | `XHG` | 351 | — | $3.57 | +0.00 | $3.32 | -87.75 | -87.75 | +0.00 | -87.75 |
| 2026-09-03 | `ARCT` | 76 | — | $16.46 | +0.00 | $16.74 | +21.28 | +21.28 | +0.00 | +21.28 |
| 2026-09-03 | `CAN` | 4187 | — | $0.30 | +0.00 | $0.31 | +41.87 | +41.87 | +0.00 | +41.87 |
| 2026-09-03 | `NIQ` | 67 | — | $18.60 | +0.00 | $18.35 | -16.75 | -16.75 | +0.00 | -16.75 |
| 2026-09-03 | `DEFT` | 1875 | — | $0.67 | +0.00 | $0.65 | -37.50 | -37.50 | +0.00 | -37.50 |
| 2026-09-03 | `OMER` | 66 | — | $18.97 | +0.00 | $18.86 | -7.26 | -7.26 | +0.00 | -7.26 |
| 2026-09-03 | `ERO` | 35 | — | $35.62 | +0.00 | $34.76 | -30.10 | -30.10 | +0.00 | -30.10 |
| 2026-09-04 | `MRNA` | 8 | $150.81 | $145.95 | -38.88 | — | +0.00 | -38.88 | -43.60 | — |
| 2026-09-04 | `XHG` | 351 | $3.32 | $3.38 | +21.06 | $3.43 | +17.55 | +38.61 | -66.69 | -49.14 |
| 2026-09-04 | `ARCT` | 76 | $16.74 | $16.77 | +2.28 | — | +0.00 | +2.28 | +23.56 | — |
| 2026-09-04 | `CAN` | 4187 | $0.31 | $0.34 | +125.61 | — | +0.00 | +125.61 | +167.48 | — |
| 2026-09-04 | `NIQ` | 67 | $18.35 | $18.66 | +20.77 | $18.82 | +10.72 | +31.49 | +4.02 | +14.74 |
| 2026-09-04 | `DEFT` | 1875 | $0.65 | $0.65 | +0.00 | $0.68 | +56.25 | +56.25 | -37.50 | +18.75 |
| 2026-09-04 | `OMER` | 66 | $18.86 | $18.99 | +8.58 | $19.11 | +7.92 | +16.50 | +1.32 | +9.24 |
| 2026-09-04 | `ERO` | 35 | $34.76 | $35.82 | +37.10 | $35.32 | -17.50 | +19.60 | +7.00 | -10.50 |
| 2026-09-04 | `HQ` | 75 | — | $17.06 | +0.00 | $15.79 | -95.25 | -95.25 | +0.00 | -95.25 |
| 2026-09-04 | `OABI` | 252 | — | $5.08 | +0.00 | $4.75 | -83.16 | -83.16 | +0.00 | -83.16 |
| 2026-09-04 | `TRLV` | 108 | — | $11.89 | +0.00 | $11.99 | +10.80 | +10.80 | +0.00 | +10.80 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +300.75 | IREN, TNDM, TPG, INO, HIMS, SLS, VOR, BTSG | — | $107.38 | $10,268.71 | IREN×27, TNDM×53, TPG×24, INO×1543, HIMS×42, SLS×106, VOR×56, BTSG×20 |
| 2026-08-14 | +5.50 | $107.38 | IREN×27, TNDM×53, TPG×24, INO×1543, HIMS×42, SLS×106, VOR×56, BTSG×20 | $10,312.70 | +43.99 | -518.06 | QMCO, ARX, ZENA, AIRO, LIFE, BZAI, VOYG, LUNR | IREN, TNDM, TPG, INO, HIMS, SLS, VOR, BTSG | $47.02 | $9,721.90 | QMCO×52, ARX×65, ZENA×583, AIRO×115, LIFE×36, BZAI×1677, VOYG×28, LUNR×67 |
| 2026-08-17 | +2.25 | $47.02 | QMCO×52, ARX×65, ZENA×583, AIRO×115, LIFE×36, BZAI×1677, VOYG×28, LUNR×67 | $9,613.23 | -108.67 | -215.67 | XHG, CAPR, STDN, HTFL, UMAC, SMJF, ALOY, NPWR | QMCO, ARX, ZENA, AIRO, LIFE, BZAI, VOYG, LUNR | $33.54 | $9,336.96 | XHG×285, CAPR×174, STDN×87, HTFL×29, UMAC×36, SMJF×118, ALOY×81, NPWR×623 |
| 2026-08-18 | -6.20 | $33.54 | XHG×285, CAPR×174, STDN×87, HTFL×29, UMAC×36, SMJF×118, ALOY×81, NPWR×623 | $9,212.74 | -124.22 | -73.08 | — | XHG, STDN, HTFL, UMAC, SMJF, ALOY, NPWR | $7,884.73 | $9,116.65 | CAPR×174 |
| 2026-08-19 | -7.20 | $7,884.73 | CAPR×174 | $9,135.79 | +19.14 | +0.00 | — | CAPR | $9,133.24 | $9,133.24 | — |
| 2026-08-20 | +1.12 | $9,133.24 | — | $9,133.24 | +0.00 | -126.77 | MRNA, CYPH, ABCL, AZI, SENS, ALEC, BTGO, AUTL | — | $63.54 | $8,961.68 | MRNA×7, CYPH×992, ABCL×96, AZI×833, SENS×128, ALEC×475, BTGO×172, AUTL×462 |
| 2026-08-21 | +3.25 | $63.54 | MRNA×7, CYPH×992, ABCL×96, AZI×833, SENS×128, ALEC×475, BTGO×172, AUTL×462 | $9,233.91 | +272.23 | +576.49 | XHG, CAPR, ARCT, IOVA, CAN, TEM | ABCL, AZI, SENS, ALEC, BTGO, AUTL | $24.18 | $9,744.00 | MRNA×7, CYPH×992, XHG×258, CAPR×170, ARCT×104, IOVA×127, CAN×3946, TEM×17 |
| 2026-08-24 | -5.17 | $24.18 | MRNA×7, CYPH×992, XHG×258, CAPR×170, ARCT×104, IOVA×127, CAN×3946, TEM×17 | $10,386.12 | +642.12 | +0.00 | — | MRNA, CYPH, XHG, CAPR, ARCT, IOVA, CAN, TEM | $10,330.90 | $10,330.90 | — |
| 2026-08-25 | +1.80 | $10,330.90 | — | $10,330.90 | -0.00 | -163.06 | CYPH, XHG, ASST, AU, RUM, BMNR, NIQ, DEFT | — | $86.74 | $10,124.02 | CYPH×759, XHG×321, ASST×61, AU×10, RUM×137, BMNR×52, NIQ×66, DEFT×2017 |
| 2026-08-26 | +2.02 | $86.74 | CYPH×759, XHG×321, ASST×61, AU×10, RUM×137, BMNR×52, NIQ×66, DEFT×2017 | $10,124.02 | -0.00 | +0.00 | — | — | $86.74 | $10,124.02 | CYPH×759, XHG×321, ASST×61, AU×10, RUM×137, BMNR×52, NIQ×66, DEFT×2017 |
| 2026-08-27 | — | $86.74 | CYPH×759, XHG×321, ASST×61, AU×10, RUM×137, BMNR×52, NIQ×66, DEFT×2017 | $10,103.54 | -20.48 | +123.99 | MOS, DLO, SLI, MRVL, CRK, PLTR, RRC, GEN | CYPH, XHG, ASST, AU, RUM, BMNR, NIQ, DEFT | $180.54 | $10,162.76 | MOS×50, DLO×80, SLI×485, MRVL×5, CRK×89, PLTR×7, RRC×30, GEN×43 |
| 2026-08-28 | +0.75 | $180.54 | MOS×50, DLO×80, SLI×485, MRVL×5, CRK×89, PLTR×7, RRC×30, GEN×43 | $10,195.66 | +32.90 | +55.90 | FIGR, NIQ, ERO, TRLV, CVI, VIRT, TXG, GUTS | MOS, DLO, SLI, MRVL, CRK, PLTR, RRC, GEN | $156.78 | $10,197.49 | FIGR×33, NIQ×67, ERO×32, TRLV×111, CVI×31, VIRT×19, TXG×19, GUTS×1718 |
| 2026-08-31 | -5.85 | $156.78 | FIGR×33, NIQ×67, ERO×32, TRLV×111, CVI×31, VIRT×19, TXG×19, GUTS×1718 | $10,091.52 | -105.97 | +0.00 | — | FIGR, ERO, TRLV, CVI, VIRT, TXG, GUTS | $8,775.35 | $10,061.75 | NIQ×67 |
| 2026-09-01 | -6.30 | $8,775.35 | NIQ×67 | $10,052.37 | -9.38 | +0.00 | — | NIQ | $10,050.16 | $10,050.16 | — |
| 2026-09-02 | -3.83 | $10,050.16 | — | $10,050.16 | +0.00 | +0.00 | — | — | $10,050.16 | $10,050.16 | — |
| 2026-09-03 | -0.90 | $10,050.16 | — | $10,050.16 | +0.00 | -120.93 | MRNA, XHG, ARCT, CAN, NIQ, DEFT, OMER, ERO | — | $19.12 | $9,870.69 | MRNA×8, XHG×351, ARCT×76, CAN×4187, NIQ×67, DEFT×1875, OMER×66, ERO×35 |
| 2026-09-04 | — | $19.12 | MRNA×8, XHG×351, ARCT×76, CAN×4187, NIQ×67, DEFT×1875, OMER×66, ERO×35 | $10,047.21 | +176.52 | -92.67 | HQ, OABI, TRLV | MRNA, ARCT, CAN | $1.48 | $9,914.98 | XHG×351, NIQ×67, DEFT×1875, OMER×66, ERO×35, HQ×75, OABI×252, TRLV×108 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $8,756.47 | — | rank by hot_score; rank hot_score; list flatten; ⚪; ret5=+12.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $7,517.83 | — | rank by hot_score; rank hot_score; list flatten; ⚪; ret5=+19.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $6,300.81 | — | rank by hot_score; rank hot_score; list flatten; ⚪; ret5=+6.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $5,033.85 | — | rank by hot_score; rank hot_score; list flatten; ⚪; ret5=+13.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $3,782.66 | — | rank by hot_score; rank hot_score; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $2,540.15 | — | rank by hot_score; rank hot_score; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 56 | $22.01 | $2.16 | — | $1,305.43 | — | rank by hot_score; rank hot_score; list flatten; ⚪; ret5=+0.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $107.38 | — | rank by hot_score; rank hot_score; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
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
| 2026-08-14 09:30 ET | **BUY** | `QMCO` | 52 | $24.68 | $2.15 | — | $8,992.89 | — | rank by hot_score; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+111.3; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 65 | $19.57 | $2.19 | — | $7,718.65 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ZENA` | 583 | $2.20 | $7.52 | — | $6,428.53 | — | rank by hot_score; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+54.3; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 115 | $11.12 | $2.33 | — | $5,147.40 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LIFE` | 36 | $35.04 | $2.10 | — | $3,883.86 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+16.7; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BZAI` | 1677 | $0.77 | $17.88 | — | $2,581.40 | — | rank by hot_score; rank hot_score; list earn_react; 🔵; ⚪; ret5=+20.4; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `VOYG` | 28 | $44.49 | $2.07 | — | $1,333.60 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ret5=+15.6; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LUNR` | 67 | $19.17 | $2.19 | — | $47.02 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🔴 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $47.02 | ▼ close $9,721.90 vs 09:30 $10,312.70 (session -518.06) | 16:00 close · cash $47.02 · equity $9,721.90 vs 09:30 $10,312.70 (-590.80; session marks -518.06) · 8 name(s) marked open→close (per-name table). QMCO×52 09:30 $24.68 → close $26.11 +74.36; ARX×65 09:30 $19.57 → close $19.58 +0.65; ZENA×583 09:30 $2.20 → close $2.14 -34.98; AIRO×115 09:30 $11.12 → close $9.57 -178.25; LIFE×36 09:30 $35.04 → close $34.02 -36.72; BZAI×1677 09:30 $0.77 → close $0.59 -290.12; VOYG×28 09:30 $44.49 → close $42.98 -42.28; LUNR×67 09:30 $19.17 → close $19.01 -10.72 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $47.02 | ▼ 09:30 equity $9,613.23 vs yday $9,721.90 (-108.67) | 09:30 open · cash $47.02 (unchanged overnight, no fees) · equity $9,613.23 vs prior close $9,721.90 (-108.67) · 8 name(s) re-marked at the open (per-name table). QMCO×52 yday $26.11 → 09:30 $24.83 -66.56; ARX×65 yday $19.58 → 09:30 $19.57 -0.65; ZENA×583 yday $2.14 → 09:30 $2.08 -32.07; AIRO×115 yday $9.57 → 09:30 $9.57 +0.00; LIFE×36 yday $34.02 → 09:30 $34.03 +0.36; BZAI×1677 yday $0.59 → 09:30 $0.55 -68.76; VOYG×28 yday $42.98 → 09:30 $42.12 -24.08; LUNR×67 yday $19.01 → 09:30 $20.25 +83.08 | — |
| 2026-08-17 09:30 ET | **SELL** | `QMCO` | 52 | $24.83 | $2.17 | $+3.49 | $1,336.02 | ▲ +3.49 after sell → book $9,611.07; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 65 | $19.57 | $2.21 | $-4.39 | $2,605.86 | ▼ -4.39 after sell → book $9,608.86; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ZENA` | 583 | $2.08 | $7.63 | $-82.19 | $3,813.79 | ▼ -82.19 after sell → book $9,601.23; vs 09:30 mark -7.63 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 115 | $9.57 | $2.36 | $-182.95 | $4,911.97 | ▼ -182.95 after sell → book $9,598.87; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LIFE` | 36 | $34.03 | $2.12 | $-40.58 | $6,134.94 | ▼ -40.58 after sell → book $9,596.75; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BZAI` | 1677 | $0.55 | $14.58 | $-391.33 | $7,046.06 | ▼ -391.33 after sell → book $9,582.17; vs 09:30 mark -14.58 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VOYG` | 28 | $42.12 | $2.09 | $-70.53 | $8,223.33 | ▼ -70.53 after sell → book $9,580.08; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `LUNR` | 67 | $20.25 | $2.21 | $+67.96 | $9,577.87 | ▲ +67.96 after sell → book $9,577.87; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 285 | $4.19 | $3.68 | — | $8,380.04 | — | rank by hot_score; rank hot_score; list yday_mover; ⚪; ret5=+291.8; leftover $1197.23 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 174 | $6.87 | $2.51 | — | $7,182.15 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; ret5=+62.6; leftover $1197.23 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `STDN` | 87 | $13.64 | $2.25 | — | $5,993.22 | — | rank by hot_score; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+57.7; leftover $1197.23 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 29 | $41.23 | $2.08 | — | $4,795.47 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; ret5=+46.0; leftover $1197.23 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `UMAC` | 36 | $32.55 | $2.10 | — | $3,621.57 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; ⚪; ret5=+30.4; leftover $1197.23 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `SMJF` | 118 | $10.10 | $2.34 | — | $2,427.43 | — | rank by hot_score; rank hot_score; list mover_buy; ret5=+22.8; leftover $1197.23 | join🔴 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ALOY` | 81 | $14.66 | $2.23 | — | $1,237.74 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.0; leftover $1197.23 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `NPWR` | 623 | $1.92 | $8.04 | — | $33.54 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $1197.23 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $33.54 | ▼ close $9,336.96 vs 09:30 $9,613.23 (session -215.67) | 16:00 close · cash $33.54 · equity $9,336.96 vs 09:30 $9,613.23 (-276.27; session marks -215.67) · 8 name(s) marked open→close (per-name table). XHG×285 09:30 $4.19 → close $3.91 -79.80; CAPR×174 09:30 $6.87 → close $7.45 +100.92; STDN×87 09:30 $13.64 → close $13.31 -28.71; HTFL×29 09:30 $41.23 → close $41.94 +20.59; UMAC×36 09:30 $32.55 → close $30.15 -86.40; SMJF×118 09:30 $10.10 → close $10.45 +41.30; ALOY×81 09:30 $14.66 → close $13.86 -65.20; NPWR×623 09:30 $1.92 → close $1.73 -118.37 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $33.54 | ▼ 09:30 equity $9,212.74 vs yday $9,336.96 (-124.22) | 09:30 open · cash $33.54 (unchanged overnight, no fees) · equity $9,212.74 vs prior close $9,336.96 (-124.22) · 8 name(s) re-marked at the open (per-name table). XHG×285 yday $3.91 → 09:30 $3.94 +8.55; CAPR×174 yday $7.45 → 09:30 $7.50 +8.70; STDN×87 yday $13.31 → 09:30 $13.31 +0.00; HTFL×29 yday $41.94 → 09:30 $41.50 -12.76; UMAC×36 yday $30.15 → 09:30 $28.59 -56.16; SMJF×118 yday $10.45 → 09:30 $10.45 +0.00; ALOY×81 yday $13.86 → 09:30 $13.19 -53.87; NPWR×623 yday $1.73 → 09:30 $1.70 -18.69 | — |
| 2026-08-18 09:30 ET | **SELL** | `XHG` | 285 | $3.94 | $3.73 | $-78.66 | $1,152.71 | ▼ -78.66 after sell → book $9,209.01; vs 09:30 mark -3.73 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `STDN` | 87 | $13.31 | $2.28 | $-33.24 | $2,308.40 | ▼ -33.24 after sell → book $9,206.73; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🔴 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 29 | $41.50 | $2.10 | $+3.66 | $3,509.80 | ▲ +3.66 after sell → book $9,204.63; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `UMAC` | 36 | $28.59 | $2.12 | $-146.78 | $4,536.93 | ▼ -146.78 after sell → book $9,202.51; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `SMJF` | 118 | $10.45 | $2.37 | $+36.58 | $5,767.65 | ▲ +36.58 after sell → book $9,200.14; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALOY` | 81 | $13.19 | $2.26 | $-123.56 | $6,833.79 | ▼ -123.56 after sell → book $9,197.89; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `NPWR` | 623 | $1.70 | $8.15 | $-153.25 | $7,884.73 | ▼ -153.25 after sell → book $9,189.73; vs 09:30 mark -8.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,884.73 | ▼ close $9,116.65 vs 09:30 $9,212.74 (session -73.08) | 16:00 close · cash $7,884.73 · equity $9,116.65 vs 09:30 $9,212.74 (-96.09; session marks -73.08) · 1 name(s) marked open→close (per-name table). CAPR×174 09:30 $7.50 → close $7.08 -73.08 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,884.73 | ▲ 09:30 equity $9,135.79 vs yday $9,116.65 (+19.14) | 09:30 open · cash $7,884.73 (unchanged overnight, no fees) · equity $9,135.79 vs prior close $9,116.65 (+19.14) · 1 name(s) re-marked at the open (per-name table). CAPR×174 yday $7.08 → 09:30 $7.19 +19.14 | — |
| 2026-08-19 09:30 ET | **SELL** | `CAPR` | 174 | $7.19 | $2.55 | $+50.62 | $9,133.24 | ▲ +50.62 after sell → book $9,133.24; vs 09:30 mark -2.55 | dropped from list after 2 sess (min 1) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,133.24 | ▲ close $9,133.24 vs 09:30 $9,135.79 (session +0.00) | 16:00 close · cash $9,133.24 · no lots left · equity $9,133.24. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,133.24 | ▲ 09:30 equity $9,133.24 vs yday $9,133.24 (+0.00) | 09:30 open · cash $9,133.24 · no holdings · equity $9,133.24 vs prior close $9,133.24 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 7 | $150.14 | $2.01 | — | $8,080.25 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1141.66 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 992 | $1.15 | $12.80 | — | $6,926.66 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; leftover $1141.66 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABCL` | 96 | $11.81 | $2.28 | — | $5,790.14 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1141.66 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AZI` | 833 | $1.37 | $10.75 | — | $4,638.18 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.7; leftover $1141.66 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SENS` | 128 | $8.91 | $2.37 | — | $3,495.33 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+14.9; leftover $1141.66 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ALEC` | 475 | $2.40 | $6.13 | — | $2,349.20 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ret5=+13.0; leftover $1141.66 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BTGO` | 172 | $6.61 | $2.51 | — | $1,210.64 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+25.3; leftover $1141.66 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 462 | $2.47 | $5.96 | — | $63.54 | — | rank by hot_score; rank hot_score; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $1141.66 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $63.54 | ▼ close $8,961.68 vs 09:30 $9,133.24 (session -126.77) | 16:00 close · cash $63.54 · equity $8,961.68 vs 09:30 $9,133.24 (-171.56; session marks -126.77) · 8 name(s) marked open→close (per-name table). MRNA×7 09:30 $150.14 → close $133.32 -117.74; CYPH×992 09:30 $1.15 → close $1.19 +39.68; ABCL×96 09:30 $11.81 → close $11.57 -23.52; AZI×833 09:30 $1.37 → close $1.44 +58.31; SENS×128 09:30 $8.91 → close $8.82 -11.52; ALEC×475 09:30 $2.40 → close $2.26 -66.50; BTGO×172 09:30 $6.61 → close $6.60 -0.86; AUTL×462 09:30 $2.47 → close $2.46 -4.62 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $63.54 | ▲ 09:30 equity $9,233.91 vs yday $8,961.68 (+272.23) | 09:30 open · cash $63.54 (unchanged overnight, no fees) · equity $9,233.91 vs prior close $8,961.68 (+272.23) · 8 name(s) re-marked at the open (per-name table). MRNA×7 yday $133.32 → 09:30 $133.11 -1.47; CYPH×992 yday $1.19 → 09:30 $1.32 +128.96; ABCL×96 yday $11.57 → 09:30 $11.57 +0.00; AZI×833 yday $1.44 → 09:30 $1.46 +16.66; SENS×128 yday $8.82 → 09:30 $9.24 +53.76; ALEC×475 yday $2.26 → 09:30 $2.28 +9.50; BTGO×172 yday $6.60 → 09:30 $6.95 +60.20; AUTL×462 yday $2.46 → 09:30 $2.47 +4.62 | — |
| 2026-08-21 09:30 ET | **SELL** | `ABCL` | 96 | $11.57 | $2.30 | $-28.10 | $1,171.95 | ▼ -28.10 after sell → book $9,231.60; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AZI` | 833 | $1.46 | $10.89 | $+53.33 | $2,377.24 | ▲ +53.33 after sell → book $9,220.71; vs 09:30 mark -10.89 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `SENS` | 128 | $9.24 | $2.41 | $+37.46 | $3,557.55 | ▲ +37.46 after sell → book $9,218.30; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `ALEC` | 475 | $2.28 | $6.22 | $-69.34 | $4,634.34 | ▼ -69.34 after sell → book $9,212.09; vs 09:30 mark -6.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTGO` | 172 | $6.95 | $2.54 | $+54.29 | $5,827.19 | ▲ +54.29 after sell → book $9,209.54; vs 09:30 mark -2.55 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AUTL` | 462 | $2.47 | $6.05 | $-12.01 | $6,962.28 | ▼ -12.01 after sell → book $9,203.49; vs 09:30 mark -6.05 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `XHG` | 258 | $4.49 | $3.33 | — | $5,800.54 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ret5=+12.7; leftover $1160.38 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CAPR` | 170 | $6.81 | $2.50 | — | $4,640.34 | — | rank by hot_score; rank hot_score; list yday_mover; 🔵; ret5=+62.5; leftover $1160.38 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 104 | $11.13 | $2.30 | — | $3,480.51 | — | rank by hot_score; rank hot_score; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1160.38 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `IOVA` | 127 | $9.08 | $2.37 | — | $2,324.98 | — | rank by hot_score; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+37.9; leftover $1160.38 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CAN` | 3946 | $0.29 | $23.44 | — | $1,141.42 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.4; leftover $1160.38 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `TEM` | 17 | $65.60 | $2.04 | — | $24.18 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+22.8; leftover $1160.38 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $24.18 | ▲ close $9,744.00 vs 09:30 $9,233.91 (session +576.49) | 16:00 close · cash $24.18 · equity $9,744.00 vs 09:30 $9,233.91 (+510.09; session marks +576.49) · 8 name(s) marked open→close (per-name table). MRNA×7 09:30 $133.11 → close $145.13 +84.14; CYPH×992 09:30 $1.32 → close $1.42 +99.20; XHG×258 09:30 $4.49 → close $4.41 -20.64; CAPR×170 09:30 $6.81 → close $6.29 -88.40; ARCT×104 09:30 $11.13 → close $13.45 +241.28; IOVA×127 09:30 $9.08 → close $8.29 -100.33; CAN×3946 09:30 $0.29 → close $0.35 +240.71; TEM×17 09:30 $65.60 → close $72.69 +120.53 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $24.18 | ▲ 09:30 equity $10,386.12 vs yday $9,744.00 (+642.12) | 09:30 open · cash $24.18 (unchanged overnight, no fees) · equity $10,386.12 vs prior close $9,744.00 (+642.12) · 8 name(s) re-marked at the open (per-name table). MRNA×7 yday $145.13 → 09:30 $142.70 -17.01; CYPH×992 yday $1.42 → 09:30 $1.83 +406.72; XHG×258 yday $4.41 → 09:30 $4.24 -43.86; CAPR×170 yday $6.29 → 09:30 $8.01 +292.40; ARCT×104 yday $13.45 → 09:30 $13.26 -19.76; IOVA×127 yday $8.29 → 09:30 $8.05 -30.48; CAN×3946 yday $0.35 → 09:30 $0.38 +98.65; TEM×17 yday $72.69 → 09:30 $70.07 -44.54 | — |
| 2026-08-24 09:30 ET | **SELL** | `MRNA` | 7 | $142.70 | $2.03 | $-56.12 | $1,021.05 | ▼ -56.12 after sell → book $10,384.09; vs 09:30 mark -2.03 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 992 | $1.83 | $12.98 | $+648.79 | $2,823.43 | ▲ +648.79 after sell → book $10,371.11; vs 09:30 mark -12.98 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `XHG` | 258 | $4.24 | $3.38 | $-71.21 | $3,913.97 | ▼ -71.21 after sell → book $10,367.73; vs 09:30 mark -3.38 | dropped from list after 1 sess (min 1) | join🟡 sector🟡 gen🔴 news🟡 digest🟢 ab🔴 peer🟢 vol🔴 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `CAPR` | 170 | $8.01 | $2.54 | $+198.96 | $5,273.13 | ▲ +198.96 after sell → book $10,365.19; vs 09:30 mark -2.54 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 104 | $13.26 | $2.33 | $+216.89 | $6,649.84 | ▲ +216.89 after sell → book $10,362.86; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `IOVA` | 127 | $8.05 | $2.40 | $-135.58 | $7,669.79 | ▼ -135.58 after sell → book $10,360.46; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CAN` | 3946 | $0.38 | $27.50 | $+288.42 | $9,141.77 | ▲ +288.42 after sell → book $10,332.96; vs 09:30 mark -27.50 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `TEM` | 17 | $70.07 | $2.06 | $+71.89 | $10,330.90 | ▲ +71.89 after sell → book $10,330.90; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,330.90 | ▲ close $10,330.90 vs 09:30 $10,386.12 (session +0.00) | 16:00 close · cash $10,330.90 · no lots left · equity $10,330.90. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,330.90 | ▲ 09:30 equity $10,330.90 vs yday $10,330.90 (-0.00) | 09:30 open · cash $10,330.90 · no holdings · equity $10,330.90 vs prior close $10,330.90 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 759 | $1.70 | $9.79 | — | $9,030.81 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+120.5; leftover $1291.36 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `XHG` | 321 | $4.02 | $4.14 | — | $7,736.25 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ret5=+16.1; leftover $1291.36 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 61 | $20.90 | $2.17 | — | $6,459.17 | — | rank by hot_score; rank hot_score; list yday_gainer; 🔵; ret5=+47.9; leftover $1291.36 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 10 | $119.46 | $2.02 | — | $5,262.55 | — | rank by hot_score; rank hot_score; list mover_buy; 🔵; ⚪; ret5=+25.9; leftover $1291.36 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 137 | $9.36 | $2.40 | — | $3,977.83 | — | rank by hot_score; rank hot_score; list yday_gainer; 🔵; ret5=+21.3; leftover $1291.36 | join🔴 sector🟡 gen🟡 news🟢 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMNR` | 52 | $24.73 | $2.15 | — | $2,689.73 | — | rank by hot_score; rank hot_score; list yday_gainer; ret5=+26.3; leftover $1291.36 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NIQ` | 66 | $19.56 | $2.19 | — | $1,396.58 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ret5=+7.6; leftover $1291.36 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `DEFT` | 2017 | $0.64 | $18.96 | — | $86.74 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover,ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1291.36 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $86.74 | ▼ close $10,124.02 vs 09:30 $10,330.90 (session -163.06) | 16:00 close · cash $86.74 · equity $10,124.02 vs 09:30 $10,330.90 (-206.88; session marks -163.06) · 8 name(s) marked open→close (per-name table). CYPH×759 09:30 $1.70 → close $1.64 -45.54; XHG×321 09:30 $4.02 → close $4.05 +9.63; ASST×61 09:30 $20.90 → close $20.20 -42.70; AU×10 09:30 $119.46 → close $118.55 -9.10; RUM×137 09:30 $9.36 → close $9.35 -1.37; BMNR×52 09:30 $24.73 → close $24.21 -27.04; NIQ×66 09:30 $19.56 → close $19.46 -6.60; DEFT×2017 09:30 $0.64 → close $0.62 -40.34 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $86.74 | ▲ 09:30 equity $10,124.02 vs yday $10,124.02 (-0.00) | 09:30 open · cash $86.74 (unchanged overnight, no fees) · equity $10,124.02 vs prior close $10,124.02 (-0.00) · 8 name(s) re-marked at the open (per-name table). CYPH×759 yday $1.64 → 09:30 $1.64 +0.00; XHG×321 yday $4.05 → 09:30 $4.05 +0.00; ASST×61 yday $20.20 → 09:30 $20.20 +0.00; AU×10 yday $118.55 → 09:30 $118.55 +0.00; RUM×137 yday $9.35 → 09:30 $9.35 +0.00; BMNR×52 yday $24.21 → 09:30 $24.21 +0.00; NIQ×66 yday $19.46 → 09:30 $19.46 +0.00; DEFT×2017 yday $0.62 → 09:30 $0.62 +0.00 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $86.74 | ▲ close $10,124.02 vs 09:30 $10,124.02 (session +0.00) | 16:00 close · cash $86.74 · equity $10,124.02 vs 09:30 $10,124.02 (-0.00; session marks +0.00) · 8 name(s) marked open→close (per-name table). CYPH×759 09:30 $1.64 → close $1.64 +0.00; XHG×321 09:30 $4.05 → close $4.05 +0.00; ASST×61 09:30 $20.20 → close $20.20 +0.00; AU×10 09:30 $118.55 → close $118.55 +0.00; RUM×137 09:30 $9.35 → close $9.35 +0.00; BMNR×52 09:30 $24.21 → close $24.21 +0.00; NIQ×66 09:30 $19.46 → close $19.46 +0.00; DEFT×2017 09:30 $0.62 → close $0.62 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $86.74 | ▼ 09:30 equity $10,103.54 vs yday $10,124.02 (-20.48) | 09:30 open · cash $86.74 (unchanged overnight, no fees) · equity $10,103.54 vs prior close $10,124.02 (-20.48) · 8 name(s) re-marked at the open (per-name table). CYPH×759 yday $1.64 → 09:30 $1.60 -30.36; XHG×321 yday $4.05 → 09:30 $3.81 -77.04; ASST×61 yday $20.20 → 09:30 $20.72 +31.72; AU×10 yday $118.55 → 09:30 $119.80 +12.50; RUM×137 yday $9.35 → 09:30 $10.07 +98.64; BMNR×52 yday $24.21 → 09:30 $24.24 +1.56; NIQ×66 yday $19.46 → 09:30 $19.20 -17.16; DEFT×2017 yday $0.62 → 09:30 $0.60 -40.34 | — |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 759 | $1.60 | $9.93 | $-95.62 | $1,291.21 | ▼ -95.62 after sell → book $10,093.61; vs 09:30 mark -9.93 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `XHG` | 321 | $3.81 | $4.20 | $-75.76 | $2,510.02 | ▼ -75.76 after sell → book $10,089.41; vs 09:30 mark -4.20 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ASST` | 61 | $20.72 | $2.19 | $-15.35 | $3,771.74 | ▼ -15.35 after sell → book $10,087.21; vs 09:30 mark -2.20 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `AU` | 10 | $119.80 | $2.04 | $-0.66 | $4,967.70 | ▼ -0.66 after sell → book $10,085.17; vs 09:30 mark -2.04 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `RUM` | 137 | $10.07 | $2.43 | $+92.43 | $6,344.86 | ▲ +92.43 after sell → book $10,082.74; vs 09:30 mark -2.43 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BMNR` | 52 | $24.24 | $2.17 | $-29.79 | $7,603.17 | ▼ -29.79 after sell → book $10,080.57; vs 09:30 mark -2.17 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `NIQ` | 66 | $19.20 | $2.21 | $-28.16 | $8,868.16 | ▼ -28.16 after sell → book $10,078.36; vs 09:30 mark -2.21 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `DEFT` | 2017 | $0.60 | $18.50 | $-118.14 | $10,059.87 | ▼ -118.14 after sell → book $10,059.87; vs 09:30 mark -18.49 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `MOS` | 50 | $24.84 | $2.14 | — | $8,815.73 | — | rank by hot_score; rank hot_score; list flatten; ret5=+13.0; leftover $1257.48 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-27 09:30 ET | **BUY** | `DLO` | 80 | $15.60 | $2.23 | — | $7,565.50 | — | rank by hot_score; rank hot_score; list mover_buy; 🔵; ret5=+7.1; leftover $1257.48 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 485 | $2.59 | $6.26 | — | $6,303.09 | — | rank by hot_score; rank hot_score; list flatten; ret5=+4.2; leftover $1257.48 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MRVL` | 5 | $240.00 | $2.00 | — | $5,101.09 | — | rank by hot_score; rank hot_score; list mover_buy; 🔵; ret5=+6.8; leftover $1257.48 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 89 | $14.09 | $2.26 | — | $3,844.82 | — | rank by hot_score; rank hot_score; list flatten; ret5=+1.1; leftover $1257.48 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `PLTR` | 7 | $170.60 | $2.01 | — | $2,648.61 | — | rank by hot_score; rank hot_score; list mover_buy; 🔵; ret5=+3.4; leftover $1257.48 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 30 | $40.72 | $2.08 | — | $1,424.93 | — | rank by hot_score; rank hot_score; list flatten; ret5=+1.8; leftover $1257.48 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GEN` | 43 | $28.89 | $2.12 | — | $180.54 | — | rank by hot_score; rank hot_score; list mover_buy; 🔵; ret5=+1.6; leftover $1257.48 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $180.54 | ▲ close $10,162.76 vs 09:30 $10,103.54 (session +123.99) | 16:00 close · cash $180.54 · equity $10,162.76 vs 09:30 $10,103.54 (+59.22; session marks +123.99) · 8 name(s) marked open→close (per-name table). MOS×50 09:30 $24.84 → close $24.16 -34.00; DLO×80 09:30 $15.60 → close $15.36 -19.20; SLI×485 09:30 $2.59 → close $2.61 +9.70; MRVL×5 09:30 $240.00 → close $245.11 +25.55; CRK×89 09:30 $14.09 → close $14.50 +36.49; PLTR×7 09:30 $170.60 → close $177.50 +48.30; RRC×30 09:30 $40.72 → close $41.55 +24.90; GEN×43 09:30 $28.89 → close $29.64 +32.25 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $180.54 | ▲ 09:30 equity $10,195.66 vs yday $10,162.76 (+32.90) | 09:30 open · cash $180.54 (unchanged overnight, no fees) · equity $10,195.66 vs prior close $10,162.76 (+32.90) · 8 name(s) re-marked at the open (per-name table). MOS×50 yday $24.16 → 09:30 $24.00 -8.00; DLO×80 yday $15.36 → 09:30 $15.33 -2.40; SLI×485 yday $2.61 → 09:30 $2.60 -4.85; MRVL×5 yday $245.11 → 09:30 $253.44 +41.65; CRK×89 yday $14.50 → 09:30 $14.42 -7.12; PLTR×7 yday $177.50 → 09:30 $178.75 +8.75; RRC×30 yday $41.55 → 09:30 $41.44 -3.30; GEN×43 yday $29.64 → 09:30 $29.83 +8.17 | — |
| 2026-08-28 09:30 ET | **SELL** | `MOS` | 50 | $24.00 | $2.16 | $-46.30 | $1,378.38 | ▼ -46.30 after sell → book $10,193.50; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `DLO` | 80 | $15.33 | $2.25 | $-26.08 | $2,602.53 | ▼ -26.08 after sell → book $10,191.24; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `SLI` | 485 | $2.60 | $6.35 | $-7.75 | $3,857.18 | ▼ -7.75 after sell → book $10,184.90; vs 09:30 mark -6.34 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `MRVL` | 5 | $253.44 | $2.03 | $+63.17 | $5,122.35 | ▲ +63.17 after sell → book $10,182.87; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `CRK` | 89 | $14.42 | $2.28 | $+24.83 | $6,403.45 | ▲ +24.83 after sell → book $10,180.59; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `PLTR` | 7 | $178.75 | $2.03 | $+53.01 | $7,652.67 | ▲ +53.01 after sell → book $10,178.56; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `RRC` | 30 | $41.44 | $2.10 | $+17.42 | $8,893.77 | ▲ +17.42 after sell → book $10,176.46; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `GEN` | 43 | $29.83 | $2.14 | $+36.16 | $10,174.32 | ▲ +36.16 after sell → book $10,174.32; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `FIGR` | 33 | $37.42 | $2.09 | — | $8,937.37 | — | rank by hot_score; rank hot_score; list yday_mover; ret5=+24.4; leftover $1271.79 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🟡 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `NIQ` | 67 | $18.79 | $2.19 | — | $7,676.25 | — | rank by hot_score; rank hot_score; list ohlc_hot; ret5=+7.6; leftover $1271.79 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ERO` | 32 | $39.20 | $2.09 | — | $6,419.76 | — | rank by hot_score; rank hot_score; list ohlc_hot; ret5=+16.6; leftover $1271.79 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `TRLV` | 111 | $11.38 | $2.32 | — | $5,154.26 | — | rank by hot_score; rank hot_score; list ohlc_hot; ret5=+15.0; leftover $1271.79 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CVI` | 31 | $40.04 | $2.08 | — | $3,910.94 | — | rank by hot_score; rank hot_score; list ohlc_hot; ret5=+12.1; leftover $1271.79 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `VIRT` | 19 | $65.42 | $2.05 | — | $2,665.91 | — | rank by hot_score; rank hot_score; list ohlc_hot; ret5=+13.2; leftover $1271.79 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TXG` | 19 | $64.10 | $2.05 | — | $1,445.96 | — | rank by hot_score; rank hot_score; list ohlc_hot; ret5=+16.1; leftover $1271.79 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GUTS` | 1718 | $0.74 | $17.87 | — | $156.78 | — | rank by hot_score; rank hot_score; list ohlc_hot; ret5=+14.7; leftover $1271.79 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $156.78 | ▲ close $10,197.49 vs 09:30 $10,195.66 (session +55.90) | 16:00 close · cash $156.78 · equity $10,197.49 vs 09:30 $10,195.66 (+1.83; session marks +55.90) · 8 name(s) marked open→close (per-name table). FIGR×33 09:30 $37.42 → close $38.02 +19.80; NIQ×67 09:30 $18.79 → close $19.07 +18.76; ERO×32 09:30 $39.20 → close $39.82 +19.84; TRLV×111 09:30 $11.38 → close $11.03 -38.85; CVI×31 09:30 $40.04 → close $39.76 -8.68; VIRT×19 09:30 $65.42 → close $67.04 +30.78; TXG×19 09:30 $64.10 → close $64.85 +14.25; GUTS×1718 09:30 $0.74 → close $0.74 +0.00 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $156.78 | ▼ 09:30 equity $10,091.52 vs yday $10,197.49 (-105.97) | 09:30 open · cash $156.78 (unchanged overnight, no fees) · equity $10,091.52 vs prior close $10,197.49 (-105.97) · 8 name(s) re-marked at the open (per-name table). FIGR×33 yday $38.02 → 09:30 $35.50 -83.16; NIQ×67 yday $19.07 → 09:30 $19.20 +8.71; ERO×32 yday $39.82 → 09:30 $38.60 -39.04; TRLV×111 yday $11.03 → 09:30 $12.41 +153.18; CVI×31 yday $39.76 → 09:30 $41.76 +62.00; VIRT×19 yday $67.04 → 09:30 $66.39 -12.35; TXG×19 yday $64.85 → 09:30 $60.90 -75.05; GUTS×1718 yday $0.74 → 09:30 $0.67 -120.26 | — |
| 2026-08-31 09:30 ET | **SELL** | `FIGR` | 33 | $35.50 | $2.11 | $-67.56 | $1,326.17 | ▼ -67.56 after sell → book $10,089.41; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ERO` | 32 | $38.60 | $2.11 | $-23.39 | $2,559.26 | ▼ -23.39 after sell → book $10,087.30; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-31 09:30 ET | **SELL** | `TRLV` | 111 | $12.41 | $2.35 | $+109.65 | $3,934.42 | ▲ +109.65 after sell → book $10,084.95; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `CVI` | 31 | $41.76 | $2.10 | $+49.13 | $5,226.88 | ▲ +49.13 after sell → book $10,082.85; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `VIRT` | 19 | $66.39 | $2.07 | $+14.32 | $6,486.22 | ▲ +14.32 after sell → book $10,080.78; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `TXG` | 19 | $60.90 | $2.07 | $-64.91 | $7,641.25 | ▼ -64.91 after sell → book $10,078.71; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `GUTS` | 1718 | $0.67 | $16.96 | $-155.09 | $8,775.35 | ▼ -155.09 after sell → book $10,061.75; vs 09:30 mark -16.96 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,775.35 | ▲ close $10,061.75 vs 09:30 $10,091.52 (session +0.00) | 16:00 close · cash $8,775.35 · equity $10,061.75 vs 09:30 $10,091.52 (-29.77; session marks +0.00) · 1 name(s) marked open→close (per-name table). NIQ×67 09:30 $19.20 → close $19.20 +0.00 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,775.35 | ▼ 09:30 equity $10,052.37 vs yday $10,061.75 (-9.38) | 09:30 open · cash $8,775.35 (unchanged overnight, no fees) · equity $10,052.37 vs prior close $10,061.75 (-9.38) · 1 name(s) re-marked at the open (per-name table). NIQ×67 yday $19.20 → 09:30 $19.06 -9.38 | — |
| 2026-09-01 09:30 ET | **SELL** | `NIQ` | 67 | $19.06 | $2.21 | $+13.69 | $10,050.16 | ▲ +13.69 after sell → book $10,050.16; vs 09:30 mark -2.21 | dropped from list after 2 sess (min 1) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,050.16 | ▲ close $10,050.16 vs 09:30 $10,052.37 (session +0.00) | 16:00 close · cash $10,050.16 · no lots left · equity $10,050.16. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,050.16 | ▲ 09:30 equity $10,050.16 vs yday $10,050.16 (+0.00) | 09:30 open · cash $10,050.16 · no holdings · equity $10,050.16 vs prior close $10,050.16 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,050.16 | ▲ close $10,050.16 vs 09:30 $10,050.16 (session +0.00) | 16:00 close · cash $10,050.16 · no lots left · equity $10,050.16. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,050.16 | ▲ 09:30 equity $10,050.16 vs yday $10,050.16 (+0.00) | 09:30 open · cash $10,050.16 · no holdings · equity $10,050.16 vs prior close $10,050.16 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 8 | $151.40 | $2.01 | — | $8,836.95 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+129.2; leftover $1256.27 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `XHG` | 351 | $3.57 | $4.53 | — | $7,579.35 | — | rank by hot_score; rank hot_score; list ohlc_hot; ret5=+16.1; leftover $1256.27 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 76 | $16.46 | $2.22 | — | $6,326.17 | — | rank by hot_score; rank hot_score; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+63.4; leftover $1256.27 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CAN` | 4187 | $0.30 | $25.12 | — | $5,044.95 | — | rank by hot_score; rank hot_score; list yday_mover; 🔵; ret5=+54.3; leftover $1256.27 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `NIQ` | 67 | $18.60 | $2.19 | — | $3,796.56 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ret5=+7.6; leftover $1256.27 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟡 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DEFT` | 1875 | $0.67 | $18.19 | — | $2,522.12 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.6; leftover $1256.27 | join🟡 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `OMER` | 66 | $18.97 | $2.19 | — | $1,267.91 | — | rank by hot_score; rank hot_score; list ohlc_hot; ret5=+12.1; leftover $1256.27 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ERO` | 35 | $35.62 | $2.10 | — | $19.12 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ret5=+16.6; leftover $1256.27 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🔴 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19.12 | ▼ close $9,870.69 vs 09:30 $10,050.16 (session -120.93) | 16:00 close · cash $19.12 · equity $9,870.69 vs 09:30 $10,050.16 (-179.47; session marks -120.93) · 8 name(s) marked open→close (per-name table). MRNA×8 09:30 $151.40 → close $150.81 -4.72; XHG×351 09:30 $3.57 → close $3.32 -87.75; ARCT×76 09:30 $16.46 → close $16.74 +21.28; CAN×4187 09:30 $0.30 → close $0.31 +41.87; NIQ×67 09:30 $18.60 → close $18.35 -16.75; DEFT×1875 09:30 $0.67 → close $0.65 -37.50; OMER×66 09:30 $18.97 → close $18.86 -7.26; ERO×35 09:30 $35.62 → close $34.76 -30.10 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19.12 | ▲ 09:30 equity $10,047.21 vs yday $9,870.69 (+176.52) | 09:30 open · cash $19.12 (unchanged overnight, no fees) · equity $10,047.21 vs prior close $9,870.69 (+176.52) · 8 name(s) re-marked at the open (per-name table). MRNA×8 yday $150.81 → 09:30 $145.95 -38.88; XHG×351 yday $3.32 → 09:30 $3.38 +21.06; ARCT×76 yday $16.74 → 09:30 $16.77 +2.28; CAN×4187 yday $0.31 → 09:30 $0.34 +125.61; NIQ×67 yday $18.35 → 09:30 $18.66 +20.77; DEFT×1875 yday $0.65 → 09:30 $0.65 +0.00; OMER×66 yday $18.86 → 09:30 $18.99 +8.58; ERO×35 yday $34.76 → 09:30 $35.82 +37.10 | — |
| 2026-09-04 09:30 ET | **SELL** | `MRNA` | 8 | $145.95 | $2.03 | $-47.65 | $1,184.68 | ▼ -47.65 after sell → book $10,045.17; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 76 | $16.77 | $2.24 | $+19.10 | $2,456.96 | ▲ +19.10 after sell → book $10,042.93; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CAN` | 4187 | $0.34 | $27.50 | $+114.85 | $3,853.04 | ▲ +114.85 after sell → book $10,015.43; vs 09:30 mark -27.50 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `HQ` | 75 | $17.06 | $2.21 | — | $2,571.32 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; ret5=+17.3; leftover $1284.35 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 252 | $5.08 | $3.25 | — | $1,287.91 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+28.1; leftover $1284.35 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `TRLV` | 108 | $11.89 | $2.31 | — | $1.48 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+15.0; leftover $1284.35 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.48 | ▼ close $9,914.98 vs 09:30 $10,047.21 (session -92.67) | 16:00 close · cash $1.48 · equity $9,914.98 vs 09:30 $10,047.21 (-132.23; session marks -92.67) · 8 name(s) marked open→close (per-name table). XHG×351 09:30 $3.38 → close $3.43 +17.55; NIQ×67 09:30 $18.66 → close $18.82 +10.72; DEFT×1875 09:30 $0.65 → close $0.68 +56.25; OMER×66 09:30 $18.99 → close $19.11 +7.92; ERO×35 09:30 $35.82 → close $35.32 -17.50; HQ×75 09:30 $17.06 → close $15.79 -95.25; OABI×252 09:30 $5.08 → close $4.75 -83.16; TRLV×108 09:30 $11.89 → close $11.99 +10.80 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `OABI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AVAH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AXTI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FIGR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `IOVA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ALM` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ARX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `IVVD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WEAV` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KURA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SENS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `REAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `ALM` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OMER` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HMY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ERO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TRLV` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TMC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `KGC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-26 | `CYPH` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ASST` | no_price | no 09:30 open — carry |
| 2026-08-26 | `AU` | no_price | no 09:30 open — carry |
| 2026-08-26 | `RUM` | no_price | no 09:30 open — carry |
| 2026-08-26 | `BMNR` | no_price | no 09:30 open — carry |
| 2026-08-26 | `USDE` | no_price | no 09:30 open |
| 2026-08-26 | `SENS` | no_price | no 09:30 open |
| 2026-08-26 | `OMER` | no_price | no 09:30 open |
| 2026-08-26 | `ERO` | no_price | no 09:30 open |
| 2026-08-26 | `TRLV` | no_price | no 09:30 open |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `XHG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CAN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PURR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MSTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `DEFT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OMER` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TRLV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TXG` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `GUTS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `INO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `UEC` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `USDE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `XHG` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `OMER` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TRLV` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TXG` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GUTS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `WPM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ZYME` | hard_red | hard-red S=-3.83 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `XHG` | 351 | 2026-09-03 @ $3.57 | rank by hot_score; rank hot_score; list ohlc_hot; ret5=+16.1; leftover $1256.27 |
| `NIQ` | 67 | 2026-09-03 @ $18.60 | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ret5=+7.6; leftover $1256.27 |
| `DEFT` | 1875 | 2026-09-03 @ $0.67 | rank by hot_score; rank hot_score; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.6; leftover $1256.27 |
| `OMER` | 66 | 2026-09-03 @ $18.97 | rank by hot_score; rank hot_score; list ohlc_hot; ret5=+12.1; leftover $1256.27 |
| `ERO` | 35 | 2026-09-03 @ $35.62 | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ret5=+16.6; leftover $1256.27 |
| `HQ` | 75 | 2026-09-04 @ $17.06 | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; ret5=+17.3; leftover $1284.35 |
| `OABI` | 252 | 2026-09-04 @ $5.08 | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+28.1; leftover $1284.35 |
| `TRLV` | 108 | 2026-09-04 @ $11.89 | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+15.0; leftover $1284.35 |
