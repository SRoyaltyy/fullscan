# Factor mine action — `union_w_hot_candle_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `w_hot_candle` · size `leftover` · sell `list` · S-boost `none` · rank by w_hot_candle

Cash book **+0.71%** ($10,071) · signal-only (no cash/fees) was +0.69%. Starts YES **8/17**. Fills 138 · skips 56 · realized $+232.24.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `w_hot_candle` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $30.59.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `TNDM` | 53 | — | $23.33 | +0.00 | $23.13 | -10.60 | -10.60 | +0.00 | -10.60 |
| 2026-08-13 | `IREN` | 27 | — | $45.98 | +0.00 | $44.76 | -32.94 | -32.94 | +0.00 | -32.94 |
| 2026-08-13 | `TPG` | 24 | — | $50.62 | +0.00 | $54.62 | +95.92 | +95.92 | +0.00 | +95.92 |
| 2026-08-13 | `HIMS` | 42 | — | $29.74 | +0.00 | $28.77 | -40.74 | -40.74 | +0.00 | -40.74 |
| 2026-08-13 | `INO` | 1543 | — | $0.81 | +0.00 | $0.90 | +138.87 | +138.87 | +0.00 | +138.87 |
| 2026-08-13 | `VOR` | 56 | — | $22.01 | +0.00 | $23.29 | +71.68 | +71.68 | +0.00 | +71.68 |
| 2026-08-13 | `SLS` | 106 | — | $11.70 | +0.00 | $12.36 | +69.96 | +69.96 | +0.00 | +69.96 |
| 2026-08-13 | `BTSG` | 20 | — | $59.80 | +0.00 | $60.23 | +8.60 | +8.60 | +0.00 | +8.60 |
| 2026-08-14 | `TNDM` | 53 | $23.13 | $22.92 | -11.13 | — | +0.00 | -11.13 | -21.73 | — |
| 2026-08-14 | `IREN` | 27 | $44.76 | $44.09 | -18.09 | — | +0.00 | -18.09 | -51.03 | — |
| 2026-08-14 | `TPG` | 24 | $54.62 | $55.29 | +16.08 | — | +0.00 | +16.08 | +112.00 | — |
| 2026-08-14 | `HIMS` | 42 | $28.77 | $29.15 | +15.96 | — | +0.00 | +15.96 | -24.78 | — |
| 2026-08-14 | `INO` | 1543 | $0.90 | $0.93 | +46.29 | — | +0.00 | +46.29 | +185.16 | — |
| 2026-08-14 | `VOR` | 56 | $23.29 | $23.33 | +2.24 | — | +0.00 | +2.24 | +73.92 | — |
| 2026-08-14 | `SLS` | 106 | $12.36 | $12.40 | +4.24 | — | +0.00 | +4.24 | +74.20 | — |
| 2026-08-14 | `BTSG` | 20 | $60.23 | $59.65 | -11.60 | — | +0.00 | -11.60 | -3.00 | — |
| 2026-08-14 | `QMCO` | 52 | — | $24.68 | +0.00 | $26.11 | +74.36 | +74.36 | +0.00 | +74.36 |
| 2026-08-14 | `ZENA` | 583 | — | $2.20 | +0.00 | $2.14 | -34.98 | -34.98 | +0.00 | -34.98 |
| 2026-08-14 | `AIRO` | 115 | — | $11.12 | +0.00 | $9.57 | -178.25 | -178.25 | +0.00 | -178.25 |
| 2026-08-14 | `ARX` | 65 | — | $19.57 | +0.00 | $19.58 | +0.65 | +0.65 | +0.00 | +0.65 |
| 2026-08-14 | `LIFE` | 36 | — | $35.04 | +0.00 | $34.02 | -36.72 | -36.72 | +0.00 | -36.72 |
| 2026-08-14 | `BETA` | 50 | — | $25.21 | +0.00 | $24.86 | -17.50 | -17.50 | +0.00 | -17.50 |
| 2026-08-14 | `LUNR` | 67 | — | $19.17 | +0.00 | $19.01 | -10.72 | -10.72 | +0.00 | -10.72 |
| 2026-08-14 | `VOYG` | 28 | — | $44.49 | +0.00 | $42.98 | -42.28 | -42.28 | +0.00 | -42.28 |
| 2026-08-17 | `QMCO` | 52 | $26.11 | $24.83 | -66.56 | — | +0.00 | -66.56 | +7.80 | — |
| 2026-08-17 | `ZENA` | 583 | $2.14 | $2.08 | -32.07 | — | +0.00 | -32.07 | -67.05 | — |
| 2026-08-17 | `AIRO` | 115 | $9.57 | $9.57 | +0.00 | — | +0.00 | +0.00 | -178.25 | — |
| 2026-08-17 | `ARX` | 65 | $19.58 | $19.57 | -0.65 | — | +0.00 | -0.65 | +0.00 | — |
| 2026-08-17 | `LIFE` | 36 | $34.02 | $34.03 | +0.36 | — | +0.00 | +0.36 | -36.36 | — |
| 2026-08-17 | `BETA` | 50 | $24.86 | $24.61 | -12.50 | — | +0.00 | -12.50 | -30.00 | — |
| 2026-08-17 | `LUNR` | 67 | $19.01 | $20.25 | +83.08 | — | +0.00 | +83.08 | +72.36 | — |
| 2026-08-17 | `VOYG` | 28 | $42.98 | $42.12 | -24.08 | — | +0.00 | -24.08 | -66.36 | — |
| 2026-08-17 | `XHG` | 296 | — | $4.19 | +0.00 | $3.91 | -82.88 | -82.88 | +0.00 | -82.88 |
| 2026-08-17 | `STDN` | 91 | — | $13.64 | +0.00 | $13.31 | -30.03 | -30.03 | +0.00 | -30.03 |
| 2026-08-17 | `HTFL` | 30 | — | $41.23 | +0.00 | $41.94 | +21.30 | +21.30 | +0.00 | +21.30 |
| 2026-08-17 | `SMJF` | 122 | — | $10.10 | +0.00 | $10.45 | +42.70 | +42.70 | +0.00 | +42.70 |
| 2026-08-17 | `NPWR` | 646 | — | $1.92 | +0.00 | $1.73 | -122.74 | -122.74 | +0.00 | -122.74 |
| 2026-08-17 | `NMAX` | 113 | — | $10.97 | +0.00 | $10.36 | -68.93 | -68.93 | +0.00 | -68.93 |
| 2026-08-17 | `CAPR` | 180 | — | $6.87 | +0.00 | $7.45 | +104.40 | +104.40 | +0.00 | +104.40 |
| 2026-08-17 | `UMAC` | 38 | — | $32.55 | +0.00 | $30.15 | -91.20 | -91.20 | +0.00 | -91.20 |
| 2026-08-18 | `XHG` | 296 | $3.91 | $3.94 | +8.88 | — | +0.00 | +8.88 | -74.00 | — |
| 2026-08-18 | `STDN` | 91 | $13.31 | $13.31 | +0.00 | — | +0.00 | +0.00 | -30.03 | — |
| 2026-08-18 | `HTFL` | 30 | $41.94 | $41.50 | -13.20 | — | +0.00 | -13.20 | +8.10 | — |
| 2026-08-18 | `SMJF` | 122 | $10.45 | $10.45 | +0.00 | — | +0.00 | +0.00 | +42.70 | — |
| 2026-08-18 | `NPWR` | 646 | $1.73 | $1.70 | -19.38 | — | +0.00 | -19.38 | -142.12 | — |
| 2026-08-18 | `NMAX` | 113 | $10.36 | $10.31 | -5.65 | — | +0.00 | -5.65 | -74.58 | — |
| 2026-08-18 | `CAPR` | 180 | $7.45 | $7.50 | +9.00 | $7.08 | -75.60 | -66.60 | +113.40 | +37.80 |
| 2026-08-18 | `UMAC` | 38 | $30.15 | $28.59 | -59.28 | — | +0.00 | -59.28 | -150.48 | — |
| 2026-08-19 | `CAPR` | 180 | $7.08 | $7.19 | +19.80 | — | +0.00 | +19.80 | +57.60 | — |
| 2026-08-20 | `MRNA` | 7 | — | $150.14 | +0.00 | $133.32 | -117.74 | -117.74 | +0.00 | -117.74 |
| 2026-08-20 | `CYPH` | 1034 | — | $1.15 | +0.00 | $1.19 | +41.36 | +41.36 | +0.00 | +41.36 |
| 2026-08-20 | `ABCL` | 100 | — | $11.81 | +0.00 | $11.57 | -24.50 | -24.50 | +0.00 | -24.50 |
| 2026-08-20 | `SENS` | 133 | — | $8.91 | +0.00 | $8.82 | -11.97 | -11.97 | +0.00 | -11.97 |
| 2026-08-20 | `ALEC` | 495 | — | $2.40 | +0.00 | $2.26 | -69.30 | -69.30 | +0.00 | -69.30 |
| 2026-08-20 | `BTGO` | 180 | — | $6.61 | +0.00 | $6.60 | -0.90 | -0.90 | +0.00 | -0.90 |
| 2026-08-20 | `IMMX` | 91 | — | $12.98 | +0.00 | $13.16 | +16.38 | +16.38 | +0.00 | +16.38 |
| 2026-08-20 | `BBNX` | 59 | — | $20.00 | +0.00 | $19.48 | -30.68 | -30.68 | +0.00 | -30.68 |
| 2026-08-21 | `MRNA` | 7 | $133.32 | $133.11 | -1.47 | $145.13 | +84.14 | +82.67 | -119.21 | -35.07 |
| 2026-08-21 | `CYPH` | 1034 | $1.19 | $1.32 | +134.42 | $1.42 | +103.40 | +237.82 | +175.78 | +279.18 |
| 2026-08-21 | `ABCL` | 100 | $11.57 | $11.57 | +0.00 | — | +0.00 | +0.00 | -24.50 | — |
| 2026-08-21 | `SENS` | 133 | $8.82 | $9.24 | +55.86 | — | +0.00 | +55.86 | +43.89 | — |
| 2026-08-21 | `ALEC` | 495 | $2.26 | $2.28 | +9.90 | — | +0.00 | +9.90 | -59.40 | — |
| 2026-08-21 | `BTGO` | 180 | $6.60 | $6.95 | +63.00 | — | +0.00 | +63.00 | +62.10 | — |
| 2026-08-21 | `IMMX` | 91 | $13.16 | $13.36 | +18.20 | — | +0.00 | +18.20 | +34.58 | — |
| 2026-08-21 | `BBNX` | 59 | $19.48 | $19.50 | +1.18 | — | +0.00 | +1.18 | -29.50 | — |
| 2026-08-21 | `XHG` | 269 | — | $4.49 | +0.00 | $4.41 | -21.52 | -21.52 | +0.00 | -21.52 |
| 2026-08-21 | `ARCT` | 108 | — | $11.13 | +0.00 | $13.45 | +250.56 | +250.56 | +0.00 | +250.56 |
| 2026-08-21 | `IOVA` | 133 | — | $9.08 | +0.00 | $8.29 | -105.07 | -105.07 | +0.00 | -105.07 |
| 2026-08-21 | `DFDV` | 299 | — | $4.04 | +0.00 | $3.94 | -29.90 | -29.90 | +0.00 | -29.90 |
| 2026-08-21 | `XXI` | 188 | — | $6.42 | +0.00 | $6.49 | +13.16 | +13.16 | +0.00 | +13.16 |
| 2026-08-21 | `INO` | 972 | — | $1.23 | +0.00 | $1.18 | -48.60 | -48.60 | +0.00 | -48.60 |
| 2026-08-24 | `MRNA` | 7 | $145.13 | $142.70 | -17.01 | — | +0.00 | -17.01 | -52.08 | — |
| 2026-08-24 | `CYPH` | 1034 | $1.42 | $1.83 | +423.94 | — | +0.00 | +423.94 | +703.12 | — |
| 2026-08-24 | `XHG` | 269 | $4.41 | $4.24 | -45.73 | — | +0.00 | -45.73 | -67.25 | — |
| 2026-08-24 | `ARCT` | 108 | $13.45 | $13.26 | -20.52 | — | +0.00 | -20.52 | +230.04 | — |
| 2026-08-24 | `IOVA` | 133 | $8.29 | $8.05 | -31.92 | — | +0.00 | -31.92 | -136.99 | — |
| 2026-08-24 | `DFDV` | 299 | $3.94 | $4.15 | +62.79 | — | +0.00 | +62.79 | +32.89 | — |
| 2026-08-24 | `XXI` | 188 | $6.49 | $6.60 | +20.68 | — | +0.00 | +20.68 | +33.84 | — |
| 2026-08-24 | `INO` | 972 | $1.18 | $1.20 | +19.44 | — | +0.00 | +19.44 | -29.16 | — |
| 2026-08-25 | `CYPH` | 745 | — | $1.70 | +0.00 | $1.64 | -44.70 | -44.70 | +0.00 | -44.70 |
| 2026-08-25 | `XHG` | 315 | — | $4.02 | +0.00 | $4.05 | +9.45 | +9.45 | +0.00 | +9.45 |
| 2026-08-25 | `ASST` | 60 | — | $20.90 | +0.00 | $20.20 | -42.00 | -42.00 | +0.00 | -42.00 |
| 2026-08-25 | `AU` | 10 | — | $119.46 | +0.00 | $118.55 | -9.10 | -9.10 | +0.00 | -9.10 |
| 2026-08-25 | `RUM` | 135 | — | $9.36 | +0.00 | $9.35 | -1.35 | -1.35 | +0.00 | -1.35 |
| 2026-08-25 | `OMER` | 67 | — | $18.75 | +0.00 | $19.03 | +18.76 | +18.76 | +0.00 | +18.76 |
| 2026-08-25 | `BMNR` | 51 | — | $24.73 | +0.00 | $24.21 | -26.52 | -26.52 | +0.00 | -26.52 |
| 2026-08-25 | `TRLV` | 115 | — | $11.02 | +0.00 | $11.02 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-26 | `CYPH` | 745 | $1.64 | $1.64 | +0.00 | $1.64 | +0.00 | +0.00 | -44.70 | -44.70 |
| 2026-08-26 | `XHG` | 315 | $4.05 | $4.05 | +0.00 | $4.05 | +0.00 | +0.00 | +9.45 | +9.45 |
| 2026-08-26 | `ASST` | 60 | $20.20 | $20.20 | +0.00 | $20.20 | +0.00 | +0.00 | -42.00 | -42.00 |
| 2026-08-26 | `AU` | 10 | $118.55 | $118.55 | +0.00 | $118.55 | +0.00 | +0.00 | -9.10 | -9.10 |
| 2026-08-26 | `RUM` | 135 | $9.35 | $9.35 | +0.00 | $9.35 | +0.00 | +0.00 | -1.35 | -1.35 |
| 2026-08-26 | `OMER` | 67 | $19.03 | $19.03 | +0.00 | $19.03 | +0.00 | +0.00 | +18.76 | +18.76 |
| 2026-08-26 | `BMNR` | 51 | $24.21 | $24.21 | +0.00 | $24.21 | +0.00 | +0.00 | -26.52 | -26.52 |
| 2026-08-26 | `TRLV` | 115 | $11.02 | $11.02 | +0.00 | $11.02 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-27 | `CYPH` | 745 | $1.64 | $1.60 | -29.80 | — | +0.00 | -29.80 | -74.50 | — |
| 2026-08-27 | `XHG` | 315 | $4.05 | $3.81 | -75.60 | — | +0.00 | -75.60 | -66.15 | — |
| 2026-08-27 | `ASST` | 60 | $20.20 | $20.72 | +31.20 | — | +0.00 | +31.20 | -10.80 | — |
| 2026-08-27 | `AU` | 10 | $118.55 | $119.80 | +12.50 | — | +0.00 | +12.50 | +3.40 | — |
| 2026-08-27 | `RUM` | 135 | $9.35 | $10.07 | +97.20 | — | +0.00 | +97.20 | +95.85 | — |
| 2026-08-27 | `OMER` | 67 | $19.03 | $18.96 | -4.69 | — | +0.00 | -4.69 | +14.07 | — |
| 2026-08-27 | `BMNR` | 51 | $24.21 | $24.24 | +1.53 | — | +0.00 | +1.53 | -24.99 | — |
| 2026-08-27 | `TRLV` | 115 | $11.02 | $11.22 | +23.00 | — | +0.00 | +23.00 | +23.00 | — |
| 2026-08-27 | `MOS` | 50 | — | $24.84 | +0.00 | $24.16 | -34.00 | -34.00 | +0.00 | -34.00 |
| 2026-08-27 | `DLO` | 80 | — | $15.60 | +0.00 | $15.36 | -19.20 | -19.20 | +0.00 | -19.20 |
| 2026-08-27 | `RRC` | 30 | — | $40.72 | +0.00 | $41.55 | +24.90 | +24.90 | +0.00 | +24.90 |
| 2026-08-27 | `GEN` | 43 | — | $28.89 | +0.00 | $29.64 | +32.25 | +32.25 | +0.00 | +32.25 |
| 2026-08-27 | `SLI` | 484 | — | $2.59 | +0.00 | $2.61 | +9.68 | +9.68 | +0.00 | +9.68 |
| 2026-08-27 | `PLTR` | 7 | — | $170.60 | +0.00 | $177.50 | +48.30 | +48.30 | +0.00 | +48.30 |
| 2026-08-27 | `CRK` | 89 | — | $14.09 | +0.00 | $14.50 | +36.49 | +36.49 | +0.00 | +36.49 |
| 2026-08-27 | `PGY` | 57 | — | $21.97 | +0.00 | $22.41 | +25.08 | +25.08 | +0.00 | +25.08 |
| 2026-08-28 | `MOS` | 50 | $24.16 | $24.00 | -8.00 | — | +0.00 | -8.00 | -42.00 | — |
| 2026-08-28 | `DLO` | 80 | $15.36 | $15.33 | -2.40 | — | +0.00 | -2.40 | -21.60 | — |
| 2026-08-28 | `RRC` | 30 | $41.55 | $41.44 | -3.30 | — | +0.00 | -3.30 | +21.60 | — |
| 2026-08-28 | `GEN` | 43 | $29.64 | $29.83 | +8.17 | — | +0.00 | +8.17 | +40.42 | — |
| 2026-08-28 | `SLI` | 484 | $2.61 | $2.60 | -4.84 | — | +0.00 | -4.84 | +4.84 | — |
| 2026-08-28 | `PLTR` | 7 | $177.50 | $178.75 | +8.75 | — | +0.00 | +8.75 | +57.05 | — |
| 2026-08-28 | `CRK` | 89 | $14.50 | $14.42 | -7.12 | — | +0.00 | -7.12 | +29.37 | — |
| 2026-08-28 | `PGY` | 57 | $22.41 | $22.93 | +29.64 | — | +0.00 | +29.64 | +54.72 | — |
| 2026-08-28 | `FIGR` | 33 | — | $37.42 | +0.00 | $38.02 | +19.80 | +19.80 | +0.00 | +19.80 |
| 2026-08-28 | `TRLV` | 111 | — | $11.38 | +0.00 | $11.03 | -38.85 | -38.85 | +0.00 | -38.85 |
| 2026-08-28 | `VIRT` | 19 | — | $65.42 | +0.00 | $67.04 | +30.78 | +30.78 | +0.00 | +30.78 |
| 2026-08-28 | `ZYME` | 43 | — | $29.33 | +0.00 | $29.01 | -13.76 | -13.76 | +0.00 | -13.76 |
| 2026-08-28 | `NIQ` | 67 | — | $18.79 | +0.00 | $19.07 | +18.76 | +18.76 | +0.00 | +18.76 |
| 2026-08-28 | `AMTX` | 678 | — | $1.87 | +0.00 | $1.87 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-28 | `NVAX` | 139 | — | $9.12 | +0.00 | $9.05 | -9.73 | -9.73 | +0.00 | -9.73 |
| 2026-08-28 | `WPM` | 8 | — | $155.89 | +0.00 | $157.99 | +16.80 | +16.80 | +0.00 | +16.80 |
| 2026-08-31 | `FIGR` | 33 | $38.02 | $35.50 | -83.16 | — | +0.00 | -83.16 | -63.36 | — |
| 2026-08-31 | `TRLV` | 111 | $11.03 | $12.41 | +153.18 | — | +0.00 | +153.18 | +114.33 | — |
| 2026-08-31 | `VIRT` | 19 | $67.04 | $66.39 | -12.35 | $66.39 | +0.00 | -12.35 | +18.43 | +18.43 |
| 2026-08-31 | `ZYME` | 43 | $29.01 | $28.27 | -31.82 | — | +0.00 | -31.82 | -45.58 | — |
| 2026-08-31 | `NIQ` | 67 | $19.07 | $19.20 | +8.71 | — | +0.00 | +8.71 | +27.47 | — |
| 2026-08-31 | `AMTX` | 678 | $1.87 | $1.90 | +20.34 | — | +0.00 | +20.34 | +20.34 | — |
| 2026-08-31 | `NVAX` | 139 | $9.05 | $9.23 | +25.02 | — | +0.00 | +25.02 | +15.29 | — |
| 2026-08-31 | `WPM` | 8 | $157.99 | $152.49 | -44.00 | — | +0.00 | -44.00 | -27.20 | — |
| 2026-09-01 | `VIRT` | 19 | $66.39 | $65.64 | -14.25 | — | +0.00 | -14.25 | +4.18 | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `MRNA` | 8 | — | $151.40 | +0.00 | $150.81 | -4.72 | -4.72 | +0.00 | -4.72 |
| 2026-09-03 | `XHG` | 355 | — | $3.57 | +0.00 | $3.32 | -88.75 | -88.75 | +0.00 | -88.75 |
| 2026-09-03 | `ARCT` | 77 | — | $16.46 | +0.00 | $16.74 | +21.56 | +21.56 | +0.00 | +21.56 |
| 2026-09-03 | `CAN` | 4226 | — | $0.30 | +0.00 | $0.31 | +42.26 | +42.26 | +0.00 | +42.26 |
| 2026-09-03 | `OMER` | 66 | — | $18.97 | +0.00 | $18.86 | -7.26 | -7.26 | +0.00 | -7.26 |
| 2026-09-03 | `TRLV` | 107 | — | $11.78 | +0.00 | $11.69 | -9.63 | -9.63 | +0.00 | -9.63 |
| 2026-09-03 | `SG` | 197 | — | $6.43 | +0.00 | $6.73 | +59.10 | +59.10 | +0.00 | +59.10 |
| 2026-09-03 | `VIRT` | 19 | — | $65.64 | +0.00 | $62.69 | -56.05 | -56.05 | +0.00 | -56.05 |
| 2026-09-04 | `MRNA` | 8 | $150.81 | $145.95 | -38.88 | — | +0.00 | -38.88 | -43.60 | — |
| 2026-09-04 | `XHG` | 355 | $3.32 | $3.38 | +21.30 | $3.43 | +17.75 | +39.05 | -67.45 | -49.70 |
| 2026-09-04 | `ARCT` | 77 | $16.74 | $16.77 | +2.31 | — | +0.00 | +2.31 | +23.87 | — |
| 2026-09-04 | `CAN` | 4226 | $0.31 | $0.34 | +126.78 | — | +0.00 | +126.78 | +169.04 | — |
| 2026-09-04 | `OMER` | 66 | $18.86 | $18.99 | +8.58 | $19.11 | +7.92 | +16.50 | +1.32 | +9.24 |
| 2026-09-04 | `TRLV` | 107 | $11.69 | $11.89 | +21.40 | $11.99 | +10.70 | +32.10 | +11.77 | +22.47 |
| 2026-09-04 | `SG` | 197 | $6.73 | $6.75 | +3.94 | $6.68 | -13.79 | -9.85 | +63.04 | +49.25 |
| 2026-09-04 | `VIRT` | 19 | $62.69 | $63.37 | +12.92 | $64.19 | +15.58 | +28.50 | -43.13 | -27.55 |
| 2026-09-04 | `HQ` | 76 | — | $17.06 | +0.00 | $15.79 | -96.52 | -96.52 | +0.00 | -96.52 |
| 2026-09-04 | `ZYME` | 41 | — | $31.34 | +0.00 | $29.90 | -59.04 | -59.04 | +0.00 | -59.04 |
| 2026-09-04 | `NIQ` | 70 | — | $18.66 | +0.00 | $18.82 | +11.20 | +11.20 | +0.00 | +11.20 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +300.75 | TNDM, IREN, TPG, HIMS, INO, VOR, SLS, BTSG | — | $107.38 | $10,268.71 | TNDM×53, IREN×27, TPG×24, HIMS×42, INO×1543, VOR×56, SLS×106, BTSG×20 |
| 2026-08-14 | +5.50 | $107.38 | TNDM×53, IREN×27, TPG×24, HIMS×42, INO×1543, VOR×56, SLS×106, BTSG×20 | $10,312.70 | +43.99 | -245.44 | QMCO, ZENA, AIRO, ARX, LIFE, BETA, LUNR, VOYG | TNDM, IREN, TPG, HIMS, INO, VOR, SLS, BTSG | $86.84 | $10,010.26 | QMCO×52, ZENA×583, AIRO×115, ARX×65, LIFE×36, BETA×50, LUNR×67, VOYG×28 |
| 2026-08-17 | +2.25 | $86.84 | QMCO×52, ZENA×583, AIRO×115, ARX×65, LIFE×36, BETA×50, LUNR×67, VOYG×28 | $9,957.85 | -52.41 | -227.38 | XHG, STDN, HTFL, SMJF, NPWR, NMAX, CAPR, UMAC | QMCO, ZENA, AIRO, ARX, LIFE, BETA, LUNR, VOYG | $5.07 | $9,681.70 | XHG×296, STDN×91, HTFL×30, SMJF×122, NPWR×646, NMAX×113, CAPR×180, UMAC×38 |
| 2026-08-18 | -6.20 | $5.07 | XHG×296, STDN×91, HTFL×30, SMJF×122, NPWR×646, NMAX×113, CAPR×180, UMAC×38 | $9,602.07 | -79.63 | -75.60 | — | XHG, STDN, HTFL, SMJF, NPWR, NMAX, UMAC | $8,228.49 | $9,502.89 | CAPR×180 |
| 2026-08-19 | -7.20 | $8,228.49 | CAPR×180 | $9,522.69 | +19.80 | +0.00 | — | CAPR | $9,520.12 | $9,520.12 | — |
| 2026-08-20 | +1.12 | $9,520.12 | — | $9,520.12 | +0.00 | -197.35 | MRNA, CYPH, ABCL, SENS, ALEC, BTGO, IMMX, BBNX | — | $142.06 | $9,289.40 | MRNA×7, CYPH×1034, ABCL×100, SENS×133, ALEC×495, BTGO×180, IMMX×91, BBNX×59 |
| 2026-08-21 | +3.25 | $142.06 | MRNA×7, CYPH×1034, ABCL×100, SENS×133, ALEC×495, BTGO×180, IMMX×91, BBNX×59 | $9,570.49 | +281.09 | +246.17 | XHG, ARCT, IOVA, DFDV, XXI, INO | ABCL, SENS, ALEC, BTGO, IMMX, BBNX | $0.48 | $9,771.27 | MRNA×7, CYPH×1034, XHG×269, ARCT×108, IOVA×133, DFDV×299, XXI×188, INO×972 |
| 2026-08-24 | -5.17 | $0.48 | MRNA×7, CYPH×1034, XHG×269, ARCT×108, IOVA×133, DFDV×299, XXI×188, INO×972 | $10,182.94 | +411.67 | +0.00 | — | MRNA, CYPH, XHG, ARCT, IOVA, DFDV, XXI, INO | $10,139.87 | $10,139.87 | — |
| 2026-08-25 | +1.80 | $10,139.87 | — | $10,139.87 | +0.00 | -95.46 | CYPH, XHG, ASST, AU, RUM, OMER, BMNR, TRLV | — | $83.17 | $10,017.49 | CYPH×745, XHG×315, ASST×60, AU×10, RUM×135, OMER×67, BMNR×51, TRLV×115 |
| 2026-08-26 | +2.02 | $83.17 | CYPH×745, XHG×315, ASST×60, AU×10, RUM×135, OMER×67, BMNR×51, TRLV×115 | $10,017.49 | -0.00 | +0.00 | — | — | $83.17 | $10,017.49 | CYPH×745, XHG×315, ASST×60, AU×10, RUM×135, OMER×67, BMNR×51, TRLV×115 |
| 2026-08-27 | — | $83.17 | CYPH×745, XHG×315, ASST×60, AU×10, RUM×135, OMER×67, BMNR×51, TRLV×115 | $10,072.83 | +55.34 | +123.50 | MOS, DLO, RRC, GEN, SLI, PLTR, CRK, PGY | CYPH, XHG, ASST, AU, RUM, OMER, BMNR, TRLV | $116.39 | $10,147.82 | MOS×50, DLO×80, RRC×30, GEN×43, SLI×484, PLTR×7, CRK×89, PGY×57 |
| 2026-08-28 | +0.75 | $116.39 | MOS×50, DLO×80, RRC×30, GEN×43, SLI×484, PLTR×7, CRK×89, PGY×57 | $10,168.72 | +20.90 | +23.80 | FIGR, TRLV, VIRT, ZYME, NIQ, AMTX, NVAX, WPM | MOS, DLO, RRC, GEN, SLI, PLTR, CRK, PGY | $79.50 | $10,147.10 | FIGR×33, TRLV×111, VIRT×19, ZYME×43, NIQ×67, AMTX×678, NVAX×139, WPM×8 |
| 2026-08-31 | -5.85 | $79.50 | FIGR×33, TRLV×111, VIRT×19, ZYME×43, NIQ×67, AMTX×678, NVAX×139, WPM×8 | $10,183.02 | +35.92 | +0.00 | — | FIGR, TRLV, ZYME, NIQ, AMTX, NVAX, WPM | $8,899.45 | $10,160.86 | VIRT×19 |
| 2026-09-01 | -6.30 | $8,899.45 | VIRT×19 | $10,146.61 | -14.25 | +0.00 | — | VIRT | $10,144.55 | $10,144.55 | — |
| 2026-09-02 | -3.83 | $10,144.55 | — | $10,144.55 | -0.00 | +0.00 | — | — | $10,144.55 | $10,144.55 | — |
| 2026-09-03 | -0.90 | $10,144.55 | — | $10,144.55 | -0.00 | -43.49 | MRNA, XHG, ARCT, CAN, OMER, TRLV, SG, VIRT | — | $61.13 | $10,057.76 | MRNA×8, XHG×355, ARCT×77, CAN×4226, OMER×66, TRLV×107, SG×197, VIRT×19 |
| 2026-09-04 | — | $61.13 | MRNA×8, XHG×355, ARCT×77, CAN×4226, OMER×66, TRLV×107, SG×197, VIRT×19 | $10,216.11 | +158.35 | -106.20 | HQ, ZYME, NIQ | MRNA, ARCT, CAN | $30.59 | $10,071.34 | XHG×355, OMER×66, TRLV×107, SG×197, VIRT×19, HQ×76, ZYME×41, NIQ×70 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $8,761.36 | — | rank by w_hot_candle; rank w_hot_candle; list flatten; ⚪; ret5=+19.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $7,517.83 | — | rank by w_hot_candle; rank w_hot_candle; list flatten; ⚪; ret5=+12.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $6,300.81 | — | rank by w_hot_candle; rank w_hot_candle; list flatten; ⚪; ret5=+6.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $5,049.62 | — | rank by w_hot_candle; rank w_hot_candle; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $3,782.66 | — | rank by w_hot_candle; rank w_hot_candle; list flatten; ⚪; ret5=+13.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 56 | $22.01 | $2.16 | — | $2,547.94 | — | rank by w_hot_candle; rank w_hot_candle; list flatten; ⚪; ret5=+0.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $1,305.43 | — | rank by w_hot_candle; rank w_hot_candle; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $107.38 | — | rank by w_hot_candle; rank w_hot_candle; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $107.38 | ▲ close $10,268.71 vs 09:30 $10,000.00 (session +300.75) | 16:00 close · cash $107.38 · equity $10,268.71 vs 09:30 $10,000.00 (+268.71; session marks +300.75) · 8 name(s) marked open→close (per-name table). TNDM×53 09:30 $23.33 → close $23.13 -10.60; IREN×27 09:30 $45.98 → close $44.76 -32.94; TPG×24 09:30 $50.62 → close $54.62 +95.92; HIMS×42 09:30 $29.74 → close $28.77 -40.74; INO×1543 09:30 $0.81 → close $0.90 +138.87; VOR×56 09:30 $22.01 → close $23.29 +71.68; SLS×106 09:30 $11.70 → close $12.36 +69.96; BTSG×20 09:30 $59.80 → close $60.23 +8.60 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $107.38 | ▲ 09:30 equity $10,312.70 vs yday $10,268.71 (+43.99) | 09:30 open · cash $107.38 (unchanged overnight, no fees) · equity $10,312.70 vs prior close $10,268.71 (+43.99) · 8 name(s) re-marked at the open (per-name table). TNDM×53 yday $23.13 → 09:30 $22.92 -11.13; IREN×27 yday $44.76 → 09:30 $44.09 -18.09; TPG×24 yday $54.62 → 09:30 $55.29 +16.08; HIMS×42 yday $28.77 → 09:30 $29.15 +15.96; INO×1543 yday $0.90 → 09:30 $0.93 +46.29; VOR×56 yday $23.29 → 09:30 $23.33 +2.24; SLS×106 yday $12.36 → 09:30 $12.40 +4.24; BTSG×20 yday $60.23 → 09:30 $59.65 -11.60 | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 53 | $22.92 | $2.17 | $-26.05 | $1,319.97 | ▼ -26.05 after sell → book $10,310.53; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 27 | $44.09 | $2.09 | $-55.19 | $2,508.31 | ▼ -55.19 after sell → book $10,308.44; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 24 | $55.29 | $2.08 | $+107.86 | $3,833.19 | ▲ +107.86 after sell → book $10,306.36; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SELL** | `HIMS` | 42 | $29.15 | $2.14 | $-29.03 | $5,055.35 | ▼ -29.03 after sell → book $10,304.22; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 1543 | $0.93 | $19.25 | $+148.79 | $6,471.10 | ▲ +148.79 after sell → book $10,284.98; vs 09:30 mark -19.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `VOR` | 56 | $23.33 | $2.18 | $+69.58 | $7,775.40 | ▲ +69.58 after sell → book $10,282.80; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `SLS` | 106 | $12.40 | $2.34 | $+69.56 | $9,087.46 | ▲ +69.56 after sell → book $10,280.46; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 20 | $59.65 | $2.07 | $-7.12 | $10,278.39 | ▼ -7.12 after sell → book $10,278.39; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `QMCO` | 52 | $24.68 | $2.15 | — | $8,992.89 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer; 🔵; ⚪; ret5=+111.3; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ZENA` | 583 | $2.20 | $7.52 | — | $7,702.77 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer; 🔵; ⚪; ret5=+54.3; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 115 | $11.12 | $2.33 | — | $6,421.63 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 65 | $19.57 | $2.19 | — | $5,147.40 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LIFE` | 36 | $35.04 | $2.10 | — | $3,883.86 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ⚪; ret5=+16.7; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BETA` | 50 | $25.21 | $2.14 | — | $2,621.22 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ⚪; ret5=+15.5; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LUNR` | 67 | $19.17 | $2.19 | — | $1,334.64 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🔴 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `VOYG` | 28 | $44.49 | $2.07 | — | $86.84 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+15.6; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $86.84 | ▼ close $10,010.26 vs 09:30 $10,312.70 (session -245.44) | 16:00 close · cash $86.84 · equity $10,010.26 vs 09:30 $10,312.70 (-302.44; session marks -245.44) · 8 name(s) marked open→close (per-name table). QMCO×52 09:30 $24.68 → close $26.11 +74.36; ZENA×583 09:30 $2.20 → close $2.14 -34.98; AIRO×115 09:30 $11.12 → close $9.57 -178.25; ARX×65 09:30 $19.57 → close $19.58 +0.65; LIFE×36 09:30 $35.04 → close $34.02 -36.72; BETA×50 09:30 $25.21 → close $24.86 -17.50; LUNR×67 09:30 $19.17 → close $19.01 -10.72; VOYG×28 09:30 $44.49 → close $42.98 -42.28 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $86.84 | ▼ 09:30 equity $9,957.85 vs yday $10,010.26 (-52.41) | 09:30 open · cash $86.84 (unchanged overnight, no fees) · equity $9,957.85 vs prior close $10,010.26 (-52.41) · 8 name(s) re-marked at the open (per-name table). QMCO×52 yday $26.11 → 09:30 $24.83 -66.56; ZENA×583 yday $2.14 → 09:30 $2.08 -32.07; AIRO×115 yday $9.57 → 09:30 $9.57 +0.00; ARX×65 yday $19.58 → 09:30 $19.57 -0.65; LIFE×36 yday $34.02 → 09:30 $34.03 +0.36; BETA×50 yday $24.86 → 09:30 $24.61 -12.50; LUNR×67 yday $19.01 → 09:30 $20.25 +83.08; VOYG×28 yday $42.98 → 09:30 $42.12 -24.08 | — |
| 2026-08-17 09:30 ET | **SELL** | `QMCO` | 52 | $24.83 | $2.17 | $+3.49 | $1,375.84 | ▲ +3.49 after sell → book $9,955.68; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ZENA` | 583 | $2.08 | $7.63 | $-82.19 | $2,583.76 | ▼ -82.19 after sell → book $9,948.05; vs 09:30 mark -7.63 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 115 | $9.57 | $2.36 | $-182.95 | $3,681.95 | ▼ -182.95 after sell → book $9,945.69; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 65 | $19.57 | $2.21 | $-4.39 | $4,951.79 | ▼ -4.39 after sell → book $9,943.48; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LIFE` | 36 | $34.03 | $2.12 | $-40.58 | $6,174.76 | ▼ -40.58 after sell → book $9,941.37; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BETA` | 50 | $24.61 | $2.16 | $-34.30 | $7,403.10 | ▼ -34.30 after sell → book $9,939.21; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LUNR` | 67 | $20.25 | $2.21 | $+67.96 | $8,757.63 | ▲ +67.96 after sell → book $9,936.99; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `VOYG` | 28 | $42.12 | $2.09 | $-70.53 | $9,934.90 | ▼ -70.53 after sell → book $9,934.90; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 296 | $4.19 | $3.82 | — | $8,690.84 | — | rank by w_hot_candle; rank w_hot_candle; list yday_mover; ⚪; ret5=+291.8; leftover $1241.86 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `STDN` | 91 | $13.64 | $2.26 | — | $7,447.34 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer; 🔵; ⚪; ret5=+57.7; leftover $1241.86 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 30 | $41.23 | $2.08 | — | $6,208.36 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; ret5=+46.0; leftover $1241.86 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `SMJF` | 122 | $10.10 | $2.36 | — | $4,973.80 | — | rank by w_hot_candle; rank w_hot_candle; list mover_buy; ret5=+22.8; leftover $1241.86 | join🔴 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `NPWR` | 646 | $1.92 | $8.33 | — | $3,725.15 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $1241.86 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `NMAX` | 113 | $10.97 | $2.33 | — | $2,483.21 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; ⚪; ret5=+21.2; leftover $1241.86 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 180 | $6.87 | $2.53 | — | $1,244.08 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; ret5=+62.6; leftover $1241.86 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `UMAC` | 38 | $32.55 | $2.10 | — | $5.07 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; ⚪; ret5=+30.4; leftover $1241.86 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5.07 | ▼ close $9,681.70 vs 09:30 $9,957.85 (session -227.38) | 16:00 close · cash $5.07 · equity $9,681.70 vs 09:30 $9,957.85 (-276.15; session marks -227.38) · 8 name(s) marked open→close (per-name table). XHG×296 09:30 $4.19 → close $3.91 -82.88; STDN×91 09:30 $13.64 → close $13.31 -30.03; HTFL×30 09:30 $41.23 → close $41.94 +21.30; SMJF×122 09:30 $10.10 → close $10.45 +42.70; NPWR×646 09:30 $1.92 → close $1.73 -122.74; NMAX×113 09:30 $10.97 → close $10.36 -68.93; CAPR×180 09:30 $6.87 → close $7.45 +104.40; UMAC×38 09:30 $32.55 → close $30.15 -91.20 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5.07 | ▼ 09:30 equity $9,602.07 vs yday $9,681.70 (-79.63) | 09:30 open · cash $5.07 (unchanged overnight, no fees) · equity $9,602.07 vs prior close $9,681.70 (-79.63) · 8 name(s) re-marked at the open (per-name table). XHG×296 yday $3.91 → 09:30 $3.94 +8.88; STDN×91 yday $13.31 → 09:30 $13.31 +0.00; HTFL×30 yday $41.94 → 09:30 $41.50 -13.20; SMJF×122 yday $10.45 → 09:30 $10.45 +0.00; NPWR×646 yday $1.73 → 09:30 $1.70 -19.38; NMAX×113 yday $10.36 → 09:30 $10.31 -5.65; CAPR×180 yday $7.45 → 09:30 $7.50 +9.00; UMAC×38 yday $30.15 → 09:30 $28.59 -59.28 | — |
| 2026-08-18 09:30 ET | **SELL** | `XHG` | 296 | $3.94 | $3.88 | $-81.70 | $1,167.44 | ▼ -81.70 after sell → book $9,598.20; vs 09:30 mark -3.87 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `STDN` | 91 | $13.31 | $2.29 | $-34.58 | $2,376.36 | ▼ -34.58 after sell → book $9,595.91; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🔴 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 30 | $41.50 | $2.10 | $+3.92 | $3,619.26 | ▲ +3.92 after sell → book $9,593.81; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `SMJF` | 122 | $10.45 | $2.39 | $+37.96 | $4,891.77 | ▲ +37.96 after sell → book $9,591.42; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `NPWR` | 646 | $1.70 | $8.45 | $-158.90 | $5,981.52 | ▼ -158.90 after sell → book $9,582.97; vs 09:30 mark -8.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `NMAX` | 113 | $10.31 | $2.36 | $-79.27 | $7,144.19 | ▼ -79.27 after sell → book $9,580.61; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `UMAC` | 38 | $28.59 | $2.12 | $-154.71 | $8,228.49 | ▼ -154.71 after sell → book $9,578.49; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,228.49 | ▼ close $9,502.89 vs 09:30 $9,602.07 (session -75.60) | 16:00 close · cash $8,228.49 · equity $9,502.89 vs 09:30 $9,602.07 (-99.18; session marks -75.60) · 1 name(s) marked open→close (per-name table). CAPR×180 09:30 $7.50 → close $7.08 -75.60 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,228.49 | ▲ 09:30 equity $9,522.69 vs yday $9,502.89 (+19.80) | 09:30 open · cash $8,228.49 (unchanged overnight, no fees) · equity $9,522.69 vs prior close $9,502.89 (+19.80) · 1 name(s) re-marked at the open (per-name table). CAPR×180 yday $7.08 → 09:30 $7.19 +19.80 | — |
| 2026-08-19 09:30 ET | **SELL** | `CAPR` | 180 | $7.19 | $2.57 | $+52.50 | $9,520.12 | ▲ +52.50 after sell → book $9,520.12; vs 09:30 mark -2.57 | dropped from list after 2 sess (min 1) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,520.12 | ▲ close $9,520.12 vs 09:30 $9,522.69 (session +0.00) | 16:00 close · cash $9,520.12 · no lots left · equity $9,520.12. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,520.12 | ▲ 09:30 equity $9,520.12 vs yday $9,520.12 (+0.00) | 09:30 open · cash $9,520.12 · no holdings · equity $9,520.12 vs prior close $9,520.12 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 7 | $150.14 | $2.01 | — | $8,467.13 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1190.02 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 1034 | $1.15 | $13.34 | — | $7,264.69 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; leftover $1190.02 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABCL` | 100 | $11.81 | $2.29 | — | $6,080.90 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1190.02 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SENS` | 133 | $8.91 | $2.39 | — | $4,893.48 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ⚪; ret5=+14.9; leftover $1190.02 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ALEC` | 495 | $2.40 | $6.39 | — | $3,699.10 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+13.0; leftover $1190.02 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BTGO` | 180 | $6.61 | $2.53 | — | $2,507.67 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ret5=+25.3; leftover $1190.02 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IMMX` | 91 | $12.98 | $2.26 | — | $1,324.22 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1190.02 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BBNX` | 59 | $20.00 | $2.17 | — | $142.06 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ⚪; ret5=+15.5; leftover $1190.02 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $142.06 | ▼ close $9,289.40 vs 09:30 $9,520.12 (session -197.35) | 16:00 close · cash $142.06 · equity $9,289.40 vs 09:30 $9,520.12 (-230.72; session marks -197.35) · 8 name(s) marked open→close (per-name table). MRNA×7 09:30 $150.14 → close $133.32 -117.74; CYPH×1034 09:30 $1.15 → close $1.19 +41.36; ABCL×100 09:30 $11.81 → close $11.57 -24.50; SENS×133 09:30 $8.91 → close $8.82 -11.97; ALEC×495 09:30 $2.40 → close $2.26 -69.30; BTGO×180 09:30 $6.61 → close $6.60 -0.90; IMMX×91 09:30 $12.98 → close $13.16 +16.38; BBNX×59 09:30 $20.00 → close $19.48 -30.68 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $142.06 | ▲ 09:30 equity $9,570.49 vs yday $9,289.40 (+281.09) | 09:30 open · cash $142.06 (unchanged overnight, no fees) · equity $9,570.49 vs prior close $9,289.40 (+281.09) · 8 name(s) re-marked at the open (per-name table). MRNA×7 yday $133.32 → 09:30 $133.11 -1.47; CYPH×1034 yday $1.19 → 09:30 $1.32 +134.42; ABCL×100 yday $11.57 → 09:30 $11.57 +0.00; SENS×133 yday $8.82 → 09:30 $9.24 +55.86; ALEC×495 yday $2.26 → 09:30 $2.28 +9.90; BTGO×180 yday $6.60 → 09:30 $6.95 +63.00; IMMX×91 yday $13.16 → 09:30 $13.36 +18.20; BBNX×59 yday $19.48 → 09:30 $19.50 +1.18 | — |
| 2026-08-21 09:30 ET | **SELL** | `ABCL` | 100 | $11.57 | $2.32 | $-29.11 | $1,296.74 | ▼ -29.11 after sell → book $9,568.17; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `SENS` | 133 | $9.24 | $2.42 | $+39.08 | $2,523.24 | ▲ +39.08 after sell → book $9,565.75; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `ALEC` | 495 | $2.28 | $6.48 | $-72.26 | $3,645.36 | ▼ -72.26 after sell → book $9,559.27; vs 09:30 mark -6.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTGO` | 180 | $6.95 | $2.57 | $+57.00 | $4,893.79 | ▲ +57.00 after sell → book $9,556.70; vs 09:30 mark -2.57 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IMMX` | 91 | $13.36 | $2.29 | $+30.03 | $6,107.26 | ▲ +30.03 after sell → book $9,554.41; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BBNX` | 59 | $19.50 | $2.19 | $-33.85 | $7,255.58 | ▼ -33.85 after sell → book $9,552.23; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `XHG` | 269 | $4.49 | $3.47 | — | $6,044.30 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+12.7; leftover $1209.26 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 108 | $11.13 | $2.31 | — | $4,839.94 | — | rank by w_hot_candle; rank w_hot_candle; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1209.26 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `IOVA` | 133 | $9.08 | $2.39 | — | $3,629.91 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer; 🔵; ⚪; ret5=+37.9; leftover $1209.26 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `DFDV` | 299 | $4.04 | $3.86 | — | $2,418.10 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+23.6; leftover $1209.26 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `XXI` | 188 | $6.42 | $2.55 | — | $1,208.58 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer; ret5=+23.8; leftover $1209.26 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `INO` | 972 | $1.23 | $12.54 | — | $0.48 | — | rank by w_hot_candle; rank w_hot_candle; list mover_buy; 🔵; ⚪; ret5=+34.4; leftover $1209.26 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.48 | ▲ close $9,771.27 vs 09:30 $9,570.49 (session +246.17) | 16:00 close · cash $0.48 · equity $9,771.27 vs 09:30 $9,570.49 (+200.78; session marks +246.17) · 8 name(s) marked open→close (per-name table). MRNA×7 09:30 $133.11 → close $145.13 +84.14; CYPH×1034 09:30 $1.32 → close $1.42 +103.40; XHG×269 09:30 $4.49 → close $4.41 -21.52; ARCT×108 09:30 $11.13 → close $13.45 +250.56; IOVA×133 09:30 $9.08 → close $8.29 -105.07; DFDV×299 09:30 $4.04 → close $3.94 -29.90; XXI×188 09:30 $6.42 → close $6.49 +13.16; INO×972 09:30 $1.23 → close $1.18 -48.60 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.48 | ▲ 09:30 equity $10,182.94 vs yday $9,771.27 (+411.67) | 09:30 open · cash $0.48 (unchanged overnight, no fees) · equity $10,182.94 vs prior close $9,771.27 (+411.67) · 8 name(s) re-marked at the open (per-name table). MRNA×7 yday $145.13 → 09:30 $142.70 -17.01; CYPH×1034 yday $1.42 → 09:30 $1.83 +423.94; XHG×269 yday $4.41 → 09:30 $4.24 -45.73; ARCT×108 yday $13.45 → 09:30 $13.26 -20.52; IOVA×133 yday $8.29 → 09:30 $8.05 -31.92; DFDV×299 yday $3.94 → 09:30 $4.15 +62.79; XXI×188 yday $6.49 → 09:30 $6.60 +20.68; INO×972 yday $1.18 → 09:30 $1.20 +19.44 | — |
| 2026-08-24 09:30 ET | **SELL** | `MRNA` | 7 | $142.70 | $2.03 | $-56.12 | $997.35 | ▼ -56.12 after sell → book $10,180.91; vs 09:30 mark -2.03 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 1034 | $1.83 | $13.53 | $+676.26 | $2,876.05 | ▲ +676.26 after sell → book $10,167.39; vs 09:30 mark -13.52 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `XHG` | 269 | $4.24 | $3.52 | $-74.24 | $4,013.08 | ▼ -74.24 after sell → book $10,163.86; vs 09:30 mark -3.53 | dropped from list after 1 sess (min 1) | join🟡 sector🟡 gen🔴 news🟡 digest🟢 ab🔴 peer🟢 vol🔴 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 108 | $13.26 | $2.34 | $+225.38 | $5,442.82 | ▲ +225.38 after sell → book $10,161.52; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `IOVA` | 133 | $8.05 | $2.42 | $-141.80 | $6,511.05 | ▼ -141.80 after sell → book $10,159.10; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `DFDV` | 299 | $4.15 | $3.92 | $+25.12 | $7,747.98 | ▲ +25.12 after sell → book $10,155.18; vs 09:30 mark -3.92 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `XXI` | 188 | $6.60 | $2.60 | $+28.69 | $8,986.19 | ▲ +28.69 after sell → book $10,152.59; vs 09:30 mark -2.59 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `INO` | 972 | $1.20 | $12.71 | $-54.41 | $10,139.87 | ▼ -54.41 after sell → book $10,139.87; vs 09:30 mark -12.72 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟡 vol🟢 buy🟡 |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,139.87 | ▲ close $10,139.87 vs 09:30 $10,182.94 (session +0.00) | 16:00 close · cash $10,139.87 · no lots left · equity $10,139.87. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,139.87 | ▲ 09:30 equity $10,139.87 vs yday $10,139.87 (+0.00) | 09:30 open · cash $10,139.87 · no holdings · equity $10,139.87 vs prior close $10,139.87 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 745 | $1.70 | $9.61 | — | $8,863.76 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+120.5; leftover $1267.48 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `XHG` | 315 | $4.02 | $4.06 | — | $7,593.40 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+16.1; leftover $1267.48 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 60 | $20.90 | $2.17 | — | $6,337.23 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer; 🔵; ret5=+47.9; leftover $1267.48 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 10 | $119.46 | $2.02 | — | $5,140.61 | — | rank by w_hot_candle; rank w_hot_candle; list mover_buy; 🔵; ⚪; ret5=+25.9; leftover $1267.48 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 135 | $9.36 | $2.40 | — | $3,874.62 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer; 🔵; ret5=+21.3; leftover $1267.48 | join🔴 sector🟡 gen🟡 news🟢 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `OMER` | 67 | $18.75 | $2.19 | — | $2,616.17 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+12.1; leftover $1267.48 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMNR` | 51 | $24.73 | $2.14 | — | $1,352.80 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer; ret5=+26.3; leftover $1267.48 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `TRLV` | 115 | $11.02 | $2.33 | — | $83.17 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+15.0; leftover $1267.48 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $83.17 | ▼ close $10,017.49 vs 09:30 $10,139.87 (session -95.46) | 16:00 close · cash $83.17 · equity $10,017.49 vs 09:30 $10,139.87 (-122.38; session marks -95.46) · 8 name(s) marked open→close (per-name table). CYPH×745 09:30 $1.70 → close $1.64 -44.70; XHG×315 09:30 $4.02 → close $4.05 +9.45; ASST×60 09:30 $20.90 → close $20.20 -42.00; AU×10 09:30 $119.46 → close $118.55 -9.10; RUM×135 09:30 $9.36 → close $9.35 -1.35; OMER×67 09:30 $18.75 → close $19.03 +18.76; BMNR×51 09:30 $24.73 → close $24.21 -26.52; TRLV×115 09:30 $11.02 → close $11.02 +0.00 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $83.17 | ▲ 09:30 equity $10,017.49 vs yday $10,017.49 (-0.00) | 09:30 open · cash $83.17 (unchanged overnight, no fees) · equity $10,017.49 vs prior close $10,017.49 (-0.00) · 8 name(s) re-marked at the open (per-name table). CYPH×745 yday $1.64 → 09:30 $1.64 +0.00; XHG×315 yday $4.05 → 09:30 $4.05 +0.00; ASST×60 yday $20.20 → 09:30 $20.20 +0.00; AU×10 yday $118.55 → 09:30 $118.55 +0.00; RUM×135 yday $9.35 → 09:30 $9.35 +0.00; OMER×67 yday $19.03 → 09:30 $19.03 +0.00; BMNR×51 yday $24.21 → 09:30 $24.21 +0.00; TRLV×115 yday $11.02 → 09:30 $11.02 +0.00 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $83.17 | ▲ close $10,017.49 vs 09:30 $10,017.49 (session +0.00) | 16:00 close · cash $83.17 · equity $10,017.49 vs 09:30 $10,017.49 (-0.00; session marks +0.00) · 8 name(s) marked open→close (per-name table). CYPH×745 09:30 $1.64 → close $1.64 +0.00; XHG×315 09:30 $4.05 → close $4.05 +0.00; ASST×60 09:30 $20.20 → close $20.20 +0.00; AU×10 09:30 $118.55 → close $118.55 +0.00; RUM×135 09:30 $9.35 → close $9.35 +0.00; OMER×67 09:30 $19.03 → close $19.03 +0.00; BMNR×51 09:30 $24.21 → close $24.21 +0.00; TRLV×115 09:30 $11.02 → close $11.02 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $83.17 | ▲ 09:30 equity $10,072.83 vs yday $10,017.49 (+55.34) | 09:30 open · cash $83.17 (unchanged overnight, no fees) · equity $10,072.83 vs prior close $10,017.49 (+55.34) · 8 name(s) re-marked at the open (per-name table). CYPH×745 yday $1.64 → 09:30 $1.60 -29.80; XHG×315 yday $4.05 → 09:30 $3.81 -75.60; ASST×60 yday $20.20 → 09:30 $20.72 +31.20; AU×10 yday $118.55 → 09:30 $119.80 +12.50; RUM×135 yday $9.35 → 09:30 $10.07 +97.20; OMER×67 yday $19.03 → 09:30 $18.96 -4.69; BMNR×51 yday $24.21 → 09:30 $24.24 +1.53; TRLV×115 yday $11.02 → 09:30 $11.22 +23.00 | — |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 745 | $1.60 | $9.74 | $-93.85 | $1,265.42 | ▼ -93.85 after sell → book $10,063.08; vs 09:30 mark -9.75 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `XHG` | 315 | $3.81 | $4.13 | $-74.34 | $2,461.45 | ▼ -74.34 after sell → book $10,058.96; vs 09:30 mark -4.12 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ASST` | 60 | $20.72 | $2.19 | $-15.16 | $3,702.46 | ▼ -15.16 after sell → book $10,056.77; vs 09:30 mark -2.19 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `AU` | 10 | $119.80 | $2.04 | $-0.66 | $4,898.42 | ▼ -0.66 after sell → book $10,054.73; vs 09:30 mark -2.04 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `RUM` | 135 | $10.07 | $2.43 | $+91.03 | $6,255.44 | ▲ +91.03 after sell → book $10,052.30; vs 09:30 mark -2.43 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `OMER` | 67 | $18.96 | $2.21 | $+9.67 | $7,523.55 | ▲ +9.67 after sell → book $10,050.09; vs 09:30 mark -2.21 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BMNR` | 51 | $24.24 | $2.16 | $-29.30 | $8,757.62 | ▼ -29.30 after sell → book $10,047.92; vs 09:30 mark -2.17 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `TRLV` | 115 | $11.22 | $2.36 | $+18.30 | $10,045.56 | ▲ +18.30 after sell → book $10,045.56; vs 09:30 mark -2.36 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `MOS` | 50 | $24.84 | $2.14 | — | $8,801.42 | — | rank by w_hot_candle; rank w_hot_candle; list flatten; ret5=+13.0; leftover $1255.69 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-27 09:30 ET | **BUY** | `DLO` | 80 | $15.60 | $2.23 | — | $7,551.19 | — | rank by w_hot_candle; rank w_hot_candle; list mover_buy; 🔵; ret5=+7.1; leftover $1255.69 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 30 | $40.72 | $2.08 | — | $6,327.51 | — | rank by w_hot_candle; rank w_hot_candle; list flatten; ret5=+1.8; leftover $1255.69 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GEN` | 43 | $28.89 | $2.12 | — | $5,083.12 | — | rank by w_hot_candle; rank w_hot_candle; list mover_buy; 🔵; ret5=+1.6; leftover $1255.69 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 484 | $2.59 | $6.24 | — | $3,823.32 | — | rank by w_hot_candle; rank w_hot_candle; list flatten; ret5=+4.2; leftover $1255.69 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `PLTR` | 7 | $170.60 | $2.01 | — | $2,627.11 | — | rank by w_hot_candle; rank w_hot_candle; list mover_buy; 🔵; ret5=+3.4; leftover $1255.69 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 89 | $14.09 | $2.26 | — | $1,370.84 | — | rank by w_hot_candle; rank w_hot_candle; list flatten; ret5=+1.1; leftover $1255.69 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `PGY` | 57 | $21.97 | $2.16 | — | $116.39 | — | rank by w_hot_candle; rank w_hot_candle; list mover_buy; 🔵; ret5=+0.6; leftover $1255.69 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $116.39 | ▲ close $10,147.82 vs 09:30 $10,072.83 (session +123.50) | 16:00 close · cash $116.39 · equity $10,147.82 vs 09:30 $10,072.83 (+74.99; session marks +123.50) · 8 name(s) marked open→close (per-name table). MOS×50 09:30 $24.84 → close $24.16 -34.00; DLO×80 09:30 $15.60 → close $15.36 -19.20; RRC×30 09:30 $40.72 → close $41.55 +24.90; GEN×43 09:30 $28.89 → close $29.64 +32.25; SLI×484 09:30 $2.59 → close $2.61 +9.68; PLTR×7 09:30 $170.60 → close $177.50 +48.30; CRK×89 09:30 $14.09 → close $14.50 +36.49; PGY×57 09:30 $21.97 → close $22.41 +25.08 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $116.39 | ▲ 09:30 equity $10,168.72 vs yday $10,147.82 (+20.90) | 09:30 open · cash $116.39 (unchanged overnight, no fees) · equity $10,168.72 vs prior close $10,147.82 (+20.90) · 8 name(s) re-marked at the open (per-name table). MOS×50 yday $24.16 → 09:30 $24.00 -8.00; DLO×80 yday $15.36 → 09:30 $15.33 -2.40; RRC×30 yday $41.55 → 09:30 $41.44 -3.30; GEN×43 yday $29.64 → 09:30 $29.83 +8.17; SLI×484 yday $2.61 → 09:30 $2.60 -4.84; PLTR×7 yday $177.50 → 09:30 $178.75 +8.75; CRK×89 yday $14.50 → 09:30 $14.42 -7.12; PGY×57 yday $22.41 → 09:30 $22.93 +29.64 | — |
| 2026-08-28 09:30 ET | **SELL** | `MOS` | 50 | $24.00 | $2.16 | $-46.30 | $1,314.23 | ▼ -46.30 after sell → book $10,166.56; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `DLO` | 80 | $15.33 | $2.25 | $-26.08 | $2,538.37 | ▼ -26.08 after sell → book $10,164.30; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `RRC` | 30 | $41.44 | $2.10 | $+17.42 | $3,779.47 | ▲ +17.42 after sell → book $10,162.20; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `GEN` | 43 | $29.83 | $2.14 | $+36.16 | $5,060.02 | ▲ +36.16 after sell → book $10,160.06; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `SLI` | 484 | $2.60 | $6.33 | $-7.74 | $6,312.09 | ▼ -7.74 after sell → book $10,153.73; vs 09:30 mark -6.33 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `PLTR` | 7 | $178.75 | $2.03 | $+53.01 | $7,561.31 | ▲ +53.01 after sell → book $10,151.70; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRK` | 89 | $14.42 | $2.28 | $+24.83 | $8,842.41 | ▲ +24.83 after sell → book $10,149.42; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `PGY` | 57 | $22.93 | $2.18 | $+50.38 | $10,147.24 | ▲ +50.38 after sell → book $10,147.24; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `FIGR` | 33 | $37.42 | $2.09 | — | $8,910.29 | — | rank by w_hot_candle; rank w_hot_candle; list yday_mover; ret5=+24.4; leftover $1268.40 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🟡 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `TRLV` | 111 | $11.38 | $2.32 | — | $7,644.78 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+15.0; leftover $1268.40 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `VIRT` | 19 | $65.42 | $2.05 | — | $6,399.76 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+13.2; leftover $1268.40 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ZYME` | 43 | $29.33 | $2.12 | — | $5,136.45 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,ohlc_hot; ret5=+14.1; leftover $1268.40 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `NIQ` | 67 | $18.79 | $2.19 | — | $3,875.33 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+7.6; leftover $1268.40 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `AMTX` | 678 | $1.87 | $8.75 | — | $2,598.72 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+16.9; leftover $1268.40 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `NVAX` | 139 | $9.12 | $2.41 | — | $1,328.63 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+11.1; leftover $1268.40 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `WPM` | 8 | $155.89 | $2.01 | — | $79.50 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+17.6; leftover $1268.40 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $79.50 | ▲ close $10,147.10 vs 09:30 $10,168.72 (session +23.80) | 16:00 close · cash $79.50 · equity $10,147.10 vs 09:30 $10,168.72 (-21.62; session marks +23.80) · 8 name(s) marked open→close (per-name table). FIGR×33 09:30 $37.42 → close $38.02 +19.80; TRLV×111 09:30 $11.38 → close $11.03 -38.85; VIRT×19 09:30 $65.42 → close $67.04 +30.78; ZYME×43 09:30 $29.33 → close $29.01 -13.76; NIQ×67 09:30 $18.79 → close $19.07 +18.76; AMTX×678 09:30 $1.87 → close $1.87 +0.00; NVAX×139 09:30 $9.12 → close $9.05 -9.73; WPM×8 09:30 $155.89 → close $157.99 +16.80 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $79.50 | ▲ 09:30 equity $10,183.02 vs yday $10,147.10 (+35.92) | 09:30 open · cash $79.50 (unchanged overnight, no fees) · equity $10,183.02 vs prior close $10,147.10 (+35.92) · 8 name(s) re-marked at the open (per-name table). FIGR×33 yday $38.02 → 09:30 $35.50 -83.16; TRLV×111 yday $11.03 → 09:30 $12.41 +153.18; VIRT×19 yday $67.04 → 09:30 $66.39 -12.35; ZYME×43 yday $29.01 → 09:30 $28.27 -31.82; NIQ×67 yday $19.07 → 09:30 $19.20 +8.71; AMTX×678 yday $1.87 → 09:30 $1.90 +20.34; NVAX×139 yday $9.05 → 09:30 $9.23 +25.02; WPM×8 yday $157.99 → 09:30 $152.49 -44.00 | — |
| 2026-08-31 09:30 ET | **SELL** | `FIGR` | 33 | $35.50 | $2.11 | $-67.56 | $1,248.89 | ▼ -67.56 after sell → book $10,180.91; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TRLV` | 111 | $12.41 | $2.35 | $+109.65 | $2,624.05 | ▲ +109.65 after sell → book $10,178.56; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `ZYME` | 43 | $28.27 | $2.14 | $-49.84 | $3,837.52 | ▼ -49.84 after sell → book $10,176.42; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `NIQ` | 67 | $19.20 | $2.21 | $+23.07 | $5,121.71 | ▲ +23.07 after sell → book $10,174.21; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🔴 digest🟢 judge🟡 ab🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `AMTX` | 678 | $1.90 | $8.87 | $+2.72 | $6,401.04 | ▲ +2.72 after sell → book $10,165.34; vs 09:30 mark -8.87 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `NVAX` | 139 | $9.23 | $2.44 | $+10.44 | $7,681.57 | ▲ +10.44 after sell → book $10,162.90; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | join🟡 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `WPM` | 8 | $152.49 | $2.03 | $-31.25 | $8,899.45 | ▼ -31.25 after sell → book $10,160.86; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,899.45 | ▲ close $10,160.86 vs 09:30 $10,183.02 (session +0.00) | 16:00 close · cash $8,899.45 · equity $10,160.86 vs 09:30 $10,183.02 (-22.16; session marks +0.00) · 1 name(s) marked open→close (per-name table). VIRT×19 09:30 $66.39 → close $66.39 +0.00 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,899.45 | ▼ 09:30 equity $10,146.61 vs yday $10,160.86 (-14.25) | 09:30 open · cash $8,899.45 (unchanged overnight, no fees) · equity $10,146.61 vs prior close $10,160.86 (-14.25) · 1 name(s) re-marked at the open (per-name table). VIRT×19 yday $66.39 → 09:30 $65.64 -14.25 | — |
| 2026-09-01 09:30 ET | **SELL** | `VIRT` | 19 | $65.64 | $2.07 | $+0.07 | $10,144.55 | ▲ +0.07 after sell → book $10,144.55; vs 09:30 mark -2.06 | dropped from list after 2 sess (min 1) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,144.55 | ▲ close $10,144.55 vs 09:30 $10,146.61 (session +0.00) | 16:00 close · cash $10,144.55 · no lots left · equity $10,144.55. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,144.55 | ▲ 09:30 equity $10,144.55 vs yday $10,144.55 (-0.00) | 09:30 open · cash $10,144.55 · no holdings · equity $10,144.55 vs prior close $10,144.55 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,144.55 | ▲ close $10,144.55 vs 09:30 $10,144.55 (session +0.00) | 16:00 close · cash $10,144.55 · no lots left · equity $10,144.55. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,144.55 | ▲ 09:30 equity $10,144.55 vs yday $10,144.55 (-0.00) | 09:30 open · cash $10,144.55 · no holdings · equity $10,144.55 vs prior close $10,144.55 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 8 | $151.40 | $2.01 | — | $8,931.33 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; 🔵; ret5=+129.2; leftover $1268.07 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `XHG` | 355 | $3.57 | $4.58 | — | $7,659.40 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+16.1; leftover $1268.07 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 77 | $16.46 | $2.22 | — | $6,389.76 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+63.4; leftover $1268.07 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CAN` | 4226 | $0.30 | $25.36 | — | $5,096.61 | — | rank by w_hot_candle; rank w_hot_candle; list yday_mover; 🔵; ret5=+54.3; leftover $1268.07 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `OMER` | 66 | $18.97 | $2.19 | — | $3,842.40 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+12.1; leftover $1268.07 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `TRLV` | 107 | $11.78 | $2.31 | — | $2,579.63 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+15.0; leftover $1268.07 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SG` | 197 | $6.43 | $2.58 | — | $1,310.34 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+11.3; leftover $1268.07 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VIRT` | 19 | $65.64 | $2.05 | — | $61.13 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+13.2; leftover $1268.07 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $61.13 | ▼ close $10,057.76 vs 09:30 $10,144.55 (session -43.49) | 16:00 close · cash $61.13 · equity $10,057.76 vs 09:30 $10,144.55 (-86.79; session marks -43.49) · 8 name(s) marked open→close (per-name table). MRNA×8 09:30 $151.40 → close $150.81 -4.72; XHG×355 09:30 $3.57 → close $3.32 -88.75; ARCT×77 09:30 $16.46 → close $16.74 +21.56; CAN×4226 09:30 $0.30 → close $0.31 +42.26; OMER×66 09:30 $18.97 → close $18.86 -7.26; TRLV×107 09:30 $11.78 → close $11.69 -9.63; SG×197 09:30 $6.43 → close $6.73 +59.10; VIRT×19 09:30 $65.64 → close $62.69 -56.05 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $61.13 | ▲ 09:30 equity $10,216.11 vs yday $10,057.76 (+158.35) | 09:30 open · cash $61.13 (unchanged overnight, no fees) · equity $10,216.11 vs prior close $10,057.76 (+158.35) · 8 name(s) re-marked at the open (per-name table). MRNA×8 yday $150.81 → 09:30 $145.95 -38.88; XHG×355 yday $3.32 → 09:30 $3.38 +21.30; ARCT×77 yday $16.74 → 09:30 $16.77 +2.31; CAN×4226 yday $0.31 → 09:30 $0.34 +126.78; OMER×66 yday $18.86 → 09:30 $18.99 +8.58; TRLV×107 yday $11.69 → 09:30 $11.89 +21.40; SG×197 yday $6.73 → 09:30 $6.75 +3.94; VIRT×19 yday $62.69 → 09:30 $63.37 +12.92 | — |
| 2026-09-04 09:30 ET | **SELL** | `MRNA` | 8 | $145.95 | $2.03 | $-47.65 | $1,226.70 | ▼ -47.65 after sell → book $10,214.08; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 77 | $16.77 | $2.24 | $+19.40 | $2,515.74 | ▲ +19.40 after sell → book $10,211.83; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CAN` | 4226 | $0.34 | $27.76 | $+115.92 | $3,924.82 | ▲ +115.92 after sell → book $10,184.07; vs 09:30 mark -27.76 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `HQ` | 76 | $17.06 | $2.22 | — | $2,626.04 | — | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; ret5=+17.3; leftover $1308.27 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ZYME` | 41 | $31.34 | $2.11 | — | $1,338.99 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ⚪; ret5=+14.1; leftover $1308.27 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `NIQ` | 70 | $18.66 | $2.20 | — | $30.59 | — | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+7.6; leftover $1308.27 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $30.59 | ▼ close $10,071.34 vs 09:30 $10,216.11 (session -106.20) | 16:00 close · cash $30.59 · equity $10,071.34 vs 09:30 $10,216.11 (-144.77; session marks -106.20) · 8 name(s) marked open→close (per-name table). XHG×355 09:30 $3.38 → close $3.43 +17.75; OMER×66 09:30 $18.99 → close $19.11 +7.92; TRLV×107 09:30 $11.89 → close $11.99 +10.70; SG×197 09:30 $6.75 → close $6.68 -13.79; VIRT×19 09:30 $63.37 → close $64.19 +15.58; HQ×76 09:30 $17.06 → close $15.79 -96.52; ZYME×41 09:30 $31.34 → close $29.90 -59.04; NIQ×70 09:30 $18.66 → close $18.82 +11.20 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AVAH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OABI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `IOVA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CRDL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `KYTX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OVID` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `IVVD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `NMAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SENS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ARX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KURA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `HAE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `REAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `OMER` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ALM` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TRLV` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SG` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ZYME` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `KGC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HMY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-26 | `CYPH` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ASST` | no_price | no 09:30 open — carry |
| 2026-08-26 | `AU` | no_price | no 09:30 open — carry |
| 2026-08-26 | `RUM` | no_price | no 09:30 open — carry |
| 2026-08-26 | `BMNR` | no_price | no 09:30 open — carry |
| 2026-08-26 | `USDE` | no_price | no 09:30 open |
| 2026-08-26 | `SENS` | no_price | no 09:30 open |
| 2026-08-26 | `SG` | no_price | no 09:30 open |
| 2026-08-26 | `ZYME` | no_price | no 09:30 open |
| 2026-08-26 | `NIQ` | no_price | no 09:30 open |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `XHG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PURR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CAN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `OMER` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OMER` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TRLV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SG` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NVAX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CELH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TXG` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `INO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `USDE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `XHG` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `OMER` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TRLV` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `SG` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ZYME` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `NOG` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `NVAX` | hard_red | hard-red S=-3.83 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `XHG` | 355 | 2026-09-03 @ $3.57 | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+16.1; leftover $1268.07 |
| `OMER` | 66 | 2026-09-03 @ $18.97 | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+12.1; leftover $1268.07 |
| `TRLV` | 107 | 2026-09-03 @ $11.78 | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+15.0; leftover $1268.07 |
| `SG` | 197 | 2026-09-03 @ $6.43 | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; ret5=+11.3; leftover $1268.07 |
| `VIRT` | 19 | 2026-09-03 @ $65.64 | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+13.2; leftover $1268.07 |
| `HQ` | 76 | 2026-09-04 @ $17.06 | rank by w_hot_candle; rank w_hot_candle; list yday_gainer,yday_mover; ret5=+17.3; leftover $1308.27 |
| `ZYME` | 41 | 2026-09-04 @ $31.34 | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ⚪; ret5=+14.1; leftover $1308.27 |
| `NIQ` | 70 | 2026-09-04 @ $18.66 | rank by w_hot_candle; rank w_hot_candle; list ohlc_hot; 🔵; ret5=+7.6; leftover $1308.27 |
