# Factor mine action — `short_extended_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **short** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · ret_5>15

Cash book **-8.50%** ($9,150) · signal-only (no cash/fees) was +3.26%. Starts YES **0/17**. Fills 108 · skips 55 · realized $-894.12.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `ret_5_min=15.0` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $16,806.65.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `TNDM` | 214 | — | $23.33 | +0.00 | $23.13 | +42.80 | +42.80 | -0.00 | +42.80 |
| 2026-08-14 | `TNDM` | 214 | $23.13 | $22.92 | +44.94 | — | +0.00 | +44.94 | +87.74 | — |
| 2026-08-14 | `ARX` | 32 | — | $19.57 | +0.00 | $19.58 | -0.32 | -0.32 | -0.00 | -0.32 |
| 2026-08-14 | `OMER` | 36 | — | $17.35 | +0.00 | $17.19 | +5.76 | +5.76 | -0.00 | +5.76 |
| 2026-08-14 | `AIRO` | 56 | — | $11.12 | +0.00 | $9.57 | +86.80 | +86.80 | -0.00 | +86.80 |
| 2026-08-14 | `MXCT` | 453 | — | $1.39 | +0.00 | $1.32 | +31.71 | +31.71 | -0.00 | +31.71 |
| 2026-08-14 | `QMLS` | 86 | — | $7.29 | +0.00 | $7.32 | -2.58 | -2.58 | -0.00 | -2.58 |
| 2026-08-14 | `AVAH` | 52 | — | $11.91 | +0.00 | $12.32 | -21.32 | -21.32 | -0.00 | -21.32 |
| 2026-08-14 | `TBBB` | 12 | — | $48.82 | +0.00 | $47.79 | +12.36 | +12.36 | -0.00 | +12.36 |
| 2026-08-14 | `AMPY` | 127 | — | $4.94 | +0.00 | $4.78 | +20.32 | +20.32 | -0.00 | +20.32 |
| 2026-08-17 | `ARX` | 32 | $19.58 | $19.57 | +0.32 | — | +0.00 | +0.32 | -0.00 | — |
| 2026-08-17 | `OMER` | 36 | $17.19 | $17.17 | +0.72 | — | +0.00 | +0.72 | +6.48 | — |
| 2026-08-17 | `AIRO` | 56 | $9.57 | $9.57 | +0.00 | — | +0.00 | +0.00 | +86.80 | — |
| 2026-08-17 | `MXCT` | 453 | $1.32 | $1.32 | +0.00 | — | +0.00 | +0.00 | +31.71 | — |
| 2026-08-17 | `QMLS` | 86 | $7.32 | $7.24 | +6.88 | — | +0.00 | +6.88 | +4.30 | — |
| 2026-08-17 | `AVAH` | 52 | $12.32 | $12.21 | +5.72 | — | +0.00 | +5.72 | -15.60 | — |
| 2026-08-17 | `TBBB` | 12 | $47.79 | $47.39 | +4.80 | — | +0.00 | +4.80 | +17.16 | — |
| 2026-08-17 | `AMPY` | 127 | $4.78 | $4.86 | -10.16 | — | +0.00 | -10.16 | +10.16 | — |
| 2026-08-17 | `CAPR` | 92 | — | $6.87 | +0.00 | $7.45 | -53.36 | -53.36 | -0.00 | -53.36 |
| 2026-08-17 | `HTFL` | 15 | — | $41.23 | +0.00 | $41.94 | -10.65 | -10.65 | -0.00 | -10.65 |
| 2026-08-17 | `UMAC` | 19 | — | $32.55 | +0.00 | $30.15 | +45.60 | +45.60 | -0.00 | +45.60 |
| 2026-08-17 | `NPWR` | 331 | — | $1.92 | +0.00 | $1.73 | +62.89 | +62.89 | -0.00 | +62.89 |
| 2026-08-17 | `LPTH` | 42 | — | $14.94 | +0.00 | $14.80 | +5.88 | +5.88 | -0.00 | +5.88 |
| 2026-08-17 | `NMAX` | 58 | — | $10.97 | +0.00 | $10.36 | +35.38 | +35.38 | -0.00 | +35.38 |
| 2026-08-17 | `ALOY` | 43 | — | $14.66 | +0.00 | $13.86 | +34.61 | +34.61 | -0.00 | +34.61 |
| 2026-08-17 | `INO` | 594 | — | $1.07 | +0.00 | $1.15 | -47.52 | -47.52 | -0.00 | -47.52 |
| 2026-08-18 | `CAPR` | 92 | $7.45 | $7.50 | -4.60 | — | +0.00 | -4.60 | -57.96 | — |
| 2026-08-18 | `HTFL` | 15 | $41.94 | $41.50 | +6.60 | — | +0.00 | +6.60 | -4.05 | — |
| 2026-08-18 | `UMAC` | 19 | $30.15 | $28.59 | +29.64 | — | +0.00 | +29.64 | +75.24 | — |
| 2026-08-18 | `NPWR` | 331 | $1.73 | $1.70 | +9.93 | — | +0.00 | +9.93 | +72.82 | — |
| 2026-08-18 | `LPTH` | 42 | $14.80 | $14.01 | +33.18 | — | +0.00 | +33.18 | +39.06 | — |
| 2026-08-18 | `NMAX` | 58 | $10.36 | $10.31 | +2.90 | — | +0.00 | +2.90 | +38.28 | — |
| 2026-08-18 | `ALOY` | 43 | $13.86 | $13.19 | +28.60 | — | +0.00 | +28.60 | +63.21 | — |
| 2026-08-18 | `INO` | 594 | $1.15 | $1.14 | +5.94 | — | +0.00 | +5.94 | -41.58 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `MRNA` | 4 | — | $150.14 | +0.00 | $133.32 | +67.28 | +67.28 | -0.00 | +67.28 |
| 2026-08-20 | `AZI` | 470 | — | $1.37 | +0.00 | $1.44 | -32.90 | -32.90 | -0.00 | -32.90 |
| 2026-08-20 | `CYPH` | 560 | — | $1.15 | +0.00 | $1.19 | -22.40 | -22.40 | -0.00 | -22.40 |
| 2026-08-20 | `BNTX` | 5 | — | $109.06 | +0.00 | $110.89 | -9.15 | -9.15 | -0.00 | -9.15 |
| 2026-08-20 | `BTGO` | 97 | — | $6.61 | +0.00 | $6.60 | +0.49 | +0.49 | -0.00 | +0.49 |
| 2026-08-20 | `ASST` | 40 | — | $16.00 | +0.00 | $16.13 | -5.20 | -5.20 | -0.00 | -5.20 |
| 2026-08-20 | `PPC` | 21 | — | $30.65 | +0.00 | $31.24 | -12.39 | -12.39 | -0.00 | -12.39 |
| 2026-08-20 | `ABCL` | 54 | — | $11.81 | +0.00 | $11.57 | +13.23 | +13.23 | -0.00 | +13.23 |
| 2026-08-21 | `MRNA` | 4 | $133.32 | $133.11 | +0.84 | — | +0.00 | +0.84 | +68.12 | — |
| 2026-08-21 | `AZI` | 470 | $1.44 | $1.46 | -9.40 | — | +0.00 | -9.40 | -42.30 | — |
| 2026-08-21 | `CYPH` | 560 | $1.19 | $1.32 | -72.80 | $1.42 | -56.00 | -128.80 | -95.20 | -151.20 |
| 2026-08-21 | `BNTX` | 5 | $110.89 | $110.92 | -0.15 | — | +0.00 | -0.15 | -9.30 | — |
| 2026-08-21 | `BTGO` | 97 | $6.60 | $6.95 | -33.95 | — | +0.00 | -33.95 | -33.46 | — |
| 2026-08-21 | `ASST` | 40 | $16.13 | $17.66 | -61.20 | — | +0.00 | -61.20 | -66.40 | — |
| 2026-08-21 | `PPC` | 21 | $31.24 | $31.13 | +2.31 | — | +0.00 | +2.31 | -10.08 | — |
| 2026-08-21 | `ABCL` | 54 | $11.57 | $11.57 | +0.00 | — | +0.00 | +0.00 | +13.23 | — |
| 2026-08-21 | `AU` | 6 | — | $119.43 | +0.00 | $121.22 | -10.74 | -10.74 | -0.00 | -10.74 |
| 2026-08-21 | `AEM` | 3 | — | $216.30 | +0.00 | $216.06 | +0.72 | +0.72 | -0.00 | +0.72 |
| 2026-08-21 | `ARCT` | 64 | — | $11.13 | +0.00 | $13.45 | -148.48 | -148.48 | -0.00 | -148.48 |
| 2026-08-21 | `INDP` | 518 | — | $1.39 | +0.00 | $1.29 | +51.80 | +51.80 | -0.00 | +51.80 |
| 2026-08-21 | `CAN` | 2452 | — | $0.29 | +0.00 | $0.35 | -149.57 | -149.57 | -0.00 | -149.57 |
| 2026-08-21 | `DFDV` | 178 | — | $4.04 | +0.00 | $3.94 | +17.80 | +17.80 | -0.00 | +17.80 |
| 2026-08-21 | `TEM` | 10 | — | $65.60 | +0.00 | $72.69 | -70.90 | -70.90 | -0.00 | -70.90 |
| 2026-08-24 | `CYPH` | 560 | $1.42 | $1.83 | -229.60 | — | +0.00 | -229.60 | -380.80 | — |
| 2026-08-24 | `AU` | 6 | $121.22 | $120.50 | +4.32 | — | +0.00 | +4.32 | -6.42 | — |
| 2026-08-24 | `AEM` | 3 | $216.06 | $217.03 | -2.91 | — | +0.00 | -2.91 | -2.19 | — |
| 2026-08-24 | `ARCT` | 64 | $13.45 | $13.26 | +12.16 | $13.76 | -32.00 | -19.84 | -136.32 | -168.32 |
| 2026-08-24 | `INDP` | 518 | $1.29 | $1.24 | +25.90 | — | +0.00 | +25.90 | +77.70 | — |
| 2026-08-24 | `CAN` | 2452 | $0.35 | $0.38 | -61.30 | $0.37 | +24.52 | -36.78 | -210.87 | -186.35 |
| 2026-08-24 | `DFDV` | 178 | $3.94 | $4.15 | -37.38 | — | +0.00 | -37.38 | -19.58 | — |
| 2026-08-24 | `TEM` | 10 | $72.69 | $70.07 | +26.20 | — | +0.00 | +26.20 | -44.70 | — |
| 2026-08-25 | `ARCT` | 64 | $13.76 | $14.34 | -37.12 | — | +0.00 | -37.12 | -205.44 | — |
| 2026-08-25 | `CAN` | 2452 | $0.37 | $0.38 | -24.52 | — | +0.00 | -24.52 | -210.87 | — |
| 2026-08-25 | `SUJA` | 66 | — | $8.79 | +0.00 | $8.54 | +16.50 | +16.50 | -0.00 | +16.50 |
| 2026-08-25 | `CYPH` | 342 | — | $1.70 | +0.00 | $1.64 | +20.52 | +20.52 | -0.00 | +20.52 |
| 2026-08-25 | `FWDI` | 97 | — | $5.99 | +0.00 | $5.86 | +12.61 | +12.61 | -0.00 | +12.61 |
| 2026-08-25 | `DEFT` | 910 | — | $0.64 | +0.00 | $0.62 | +18.20 | +18.20 | -0.00 | +18.20 |
| 2026-08-25 | `GORO` | 165 | — | $3.53 | +0.00 | $3.56 | -4.95 | -4.95 | -0.00 | -4.95 |
| 2026-08-25 | `ASST` | 27 | — | $20.90 | +0.00 | $20.20 | +18.90 | +18.90 | -0.00 | +18.90 |
| 2026-08-25 | `BMNR` | 23 | — | $24.73 | +0.00 | $24.21 | +11.96 | +11.96 | -0.00 | +11.96 |
| 2026-08-25 | `RUM` | 62 | — | $9.36 | +0.00 | $9.35 | +0.62 | +0.62 | -0.00 | +0.62 |
| 2026-08-26 | `SUJA` | 66 | $8.54 | $8.54 | +0.00 | $8.54 | +0.00 | +0.00 | +16.50 | +16.50 |
| 2026-08-26 | `CYPH` | 342 | $1.64 | $1.64 | +0.00 | $1.64 | +0.00 | +0.00 | +20.52 | +20.52 |
| 2026-08-26 | `FWDI` | 97 | $5.86 | $5.86 | +0.00 | $5.86 | +0.00 | +0.00 | +12.61 | +12.61 |
| 2026-08-26 | `DEFT` | 910 | $0.62 | $0.62 | +0.00 | $0.62 | +0.00 | +0.00 | +18.20 | +18.20 |
| 2026-08-26 | `GORO` | 165 | $3.56 | $3.56 | +0.00 | $3.56 | +0.00 | +0.00 | -4.95 | -4.95 |
| 2026-08-26 | `ASST` | 27 | $20.20 | $20.20 | +0.00 | $20.20 | +0.00 | +0.00 | +18.90 | +18.90 |
| 2026-08-26 | `BMNR` | 23 | $24.21 | $24.21 | +0.00 | $24.21 | +0.00 | +0.00 | +11.96 | +11.96 |
| 2026-08-26 | `RUM` | 62 | $9.35 | $9.35 | +0.00 | $9.35 | +0.00 | +0.00 | +0.62 | +0.62 |
| 2026-08-27 | `SUJA` | 66 | $8.54 | $9.39 | -56.10 | — | +0.00 | -56.10 | -39.60 | — |
| 2026-08-27 | `CYPH` | 342 | $1.64 | $1.60 | +13.68 | — | +0.00 | +13.68 | +34.20 | — |
| 2026-08-27 | `FWDI` | 97 | $5.86 | $5.97 | -10.67 | — | +0.00 | -10.67 | +1.94 | — |
| 2026-08-27 | `DEFT` | 910 | $0.62 | $0.60 | +18.20 | — | +0.00 | +18.20 | +36.40 | — |
| 2026-08-27 | `GORO` | 165 | $3.56 | $3.77 | -34.65 | — | +0.00 | -34.65 | -39.60 | — |
| 2026-08-27 | `ASST` | 27 | $20.20 | $20.72 | -14.04 | — | +0.00 | -14.04 | +4.86 | — |
| 2026-08-27 | `BMNR` | 23 | $24.21 | $24.24 | -0.69 | — | +0.00 | -0.69 | +11.27 | — |
| 2026-08-27 | `RUM` | 62 | $9.35 | $10.07 | -44.64 | — | +0.00 | -44.64 | -44.02 | — |
| 2026-08-28 | `FIGR` | 15 | — | $37.42 | +0.00 | $38.02 | -9.00 | -9.00 | -0.00 | -9.00 |
| 2026-08-28 | `XHG` | 142 | — | $4.06 | +0.00 | $3.80 | +36.92 | +36.92 | -0.00 | +36.92 |
| 2026-08-28 | `DEFT` | 962 | — | $0.60 | +0.00 | $0.65 | -48.10 | -48.10 | -0.00 | -48.10 |
| 2026-08-28 | `ERO` | 14 | — | $39.20 | +0.00 | $39.82 | -8.68 | -8.68 | -0.00 | -8.68 |
| 2026-08-28 | `TRLV` | 50 | — | $11.38 | +0.00 | $11.03 | +17.50 | +17.50 | -0.00 | +17.50 |
| 2026-08-28 | `FUTU` | 4 | — | $128.00 | +0.00 | $124.57 | +13.72 | +13.72 | -0.00 | +13.72 |
| 2026-08-28 | `TXG` | 9 | — | $64.10 | +0.00 | $64.85 | -6.75 | -6.75 | -0.00 | -6.75 |
| 2026-08-28 | `WPM` | 3 | — | $155.89 | +0.00 | $157.99 | -6.30 | -6.30 | -0.00 | -6.30 |
| 2026-08-31 | `FIGR` | 15 | $38.02 | $35.50 | +37.80 | — | +0.00 | +37.80 | +28.80 | — |
| 2026-08-31 | `XHG` | 142 | $3.80 | $3.44 | +51.12 | $3.44 | +0.00 | +51.12 | +88.04 | +88.04 |
| 2026-08-31 | `DEFT` | 962 | $0.65 | $0.62 | +28.86 | — | +0.00 | +28.86 | -19.24 | — |
| 2026-08-31 | `ERO` | 14 | $39.82 | $38.60 | +17.08 | — | +0.00 | +17.08 | +8.40 | — |
| 2026-08-31 | `TRLV` | 50 | $11.03 | $12.41 | -69.00 | — | +0.00 | -69.00 | -51.50 | — |
| 2026-08-31 | `FUTU` | 4 | $124.57 | $122.82 | +7.00 | — | +0.00 | +7.00 | +20.72 | — |
| 2026-08-31 | `TXG` | 9 | $64.85 | $60.90 | +35.55 | — | +0.00 | +35.55 | +28.80 | — |
| 2026-08-31 | `WPM` | 3 | $157.99 | $152.49 | +16.50 | — | +0.00 | +16.50 | +10.20 | — |
| 2026-09-01 | `XHG` | 142 | $3.44 | $3.52 | -11.36 | $3.43 | +12.78 | +1.42 | +76.68 | +89.46 |
| 2026-09-02 | `XHG` | 142 | $3.43 | $3.48 | -7.10 | $3.51 | -4.26 | -11.36 | +82.36 | +78.10 |
| 2026-09-03 | `XHG` | 142 | $3.51 | $3.57 | -8.52 | $3.32 | +35.50 | +26.98 | +69.58 | +105.08 |
| 2026-09-03 | `DEFT` | 990 | — | $0.67 | +0.00 | $0.65 | +19.80 | +19.80 | -0.00 | +19.80 |
| 2026-09-03 | `MRNA` | 4 | — | $151.40 | +0.00 | $150.81 | +2.36 | +2.36 | -0.00 | +2.36 |
| 2026-09-03 | `ARCT` | 40 | — | $16.46 | +0.00 | $16.74 | -11.20 | -11.20 | -0.00 | -11.20 |
| 2026-09-03 | `ALEC` | 276 | — | $2.40 | +0.00 | $2.72 | -88.32 | -88.32 | -0.00 | -88.32 |
| 2026-09-03 | `CAN` | 2211 | — | $0.30 | +0.00 | $0.31 | -22.11 | -22.11 | -0.00 | -22.11 |
| 2026-09-03 | `ERO` | 18 | — | $35.62 | +0.00 | $34.76 | +15.48 | +15.48 | -0.00 | +15.48 |
| 2026-09-03 | `TRLV` | 56 | — | $11.78 | +0.00 | $11.69 | +5.04 | +5.04 | -0.00 | +5.04 |
| 2026-09-04 | `XHG` | 142 | $3.32 | $3.38 | -8.52 | $3.43 | -7.10 | -15.62 | +96.56 | +89.46 |
| 2026-09-04 | `DEFT` | 990 | $0.65 | $0.65 | +0.00 | $0.68 | -29.70 | -29.70 | +19.80 | -9.90 |
| 2026-09-04 | `MRNA` | 4 | $150.81 | $145.95 | +19.44 | — | +0.00 | +19.44 | +21.80 | — |
| 2026-09-04 | `ARCT` | 40 | $16.74 | $16.77 | -1.20 | — | +0.00 | -1.20 | -12.40 | — |
| 2026-09-04 | `ALEC` | 276 | $2.72 | $2.70 | +5.52 | $2.51 | +52.44 | +57.96 | -82.80 | -30.36 |
| 2026-09-04 | `CAN` | 2211 | $0.31 | $0.34 | -66.33 | — | +0.00 | -66.33 | -88.44 | — |
| 2026-09-04 | `ERO` | 18 | $34.76 | $35.82 | -19.08 | $35.32 | +9.00 | -10.08 | -3.60 | +5.40 |
| 2026-09-04 | `TRLV` | 56 | $11.69 | $11.89 | -11.20 | $11.99 | -5.60 | -16.80 | -6.16 | -11.76 |
| 2026-09-04 | `HQ` | 88 | — | $17.06 | +0.00 | $15.79 | +111.76 | +111.76 | -0.00 | +111.76 |
| 2026-09-04 | `OABI` | 298 | — | $5.08 | +0.00 | $4.75 | +98.34 | +98.34 | -0.00 | +98.34 |
| 2026-09-04 | `BRR` | 643 | — | $2.36 | +0.00 | $2.63 | -173.61 | -173.61 | -0.00 | -173.61 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +42.80 | TNDM | — | $14,989.65 | $10,039.83 | TNDM×214 |
| 2026-08-14 | +5.50 | $14,989.65 | TNDM×214 | $10,084.77 | +44.94 | +132.73 | ARX, OMER, AIRO, MXCT, QMLS, AVAH, TBBB, AMPY | TNDM | $15,023.36 | $10,193.38 | ARX×32, OMER×36, AIRO×56, MXCT×453, QMLS×86, AVAH×52, TBBB×12, AMPY×127 |
| 2026-08-17 | +2.25 | $15,023.36 | ARX×32, OMER×36, AIRO×56, MXCT×453, QMLS×86, AVAH×52, TBBB×12, AMPY×127 | $10,201.66 | +8.28 | +72.83 | CAPR, HTFL, UMAC, NPWR, LPTH, NMAX, ALOY, INO | ARX, OMER, AIRO, MXCT, QMLS, AVAH, TBBB, AMPY | $15,189.73 | $10,228.41 | CAPR×92, HTFL×15, UMAC×19, NPWR×331, LPTH×42, NMAX×58, ALOY×43, INO×594 |
| 2026-08-18 | -6.20 | $15,189.73 | CAPR×92, HTFL×15, UMAC×19, NPWR×331, LPTH×42, NMAX×58, ALOY×43, INO×594 | $10,340.59 | +112.18 | +0.00 | — | CAPR, HTFL, UMAC, NPWR, LPTH, NMAX, ALOY, INO | $10,315.91 | $10,315.91 | — |
| 2026-08-19 | -7.20 | $10,315.91 | — | $10,315.91 | +0.00 | +0.00 | — | — | $10,315.91 | $10,315.91 | — |
| 2026-08-20 | +1.12 | $10,315.91 | — | $10,315.91 | +0.00 | -1.04 | MRNA, AZI, CYPH, BNTX, BTGO, ASST, PPC, ABCL | — | $15,285.67 | $10,288.52 | MRNA×4, AZI×470, CYPH×560, BNTX×5, BTGO×97, ASST×40, PPC×21, ABCL×54 |
| 2026-08-21 | +3.25 | $15,285.67 | MRNA×4, AZI×470, CYPH×560, BNTX×5, BTGO×97, ASST×40, PPC×21, ABCL×54 | $10,114.17 | -174.35 | -365.37 | AU, AEM, ARCT, INDP, CAN, DFDV, TEM | MRNA, AZI, BNTX, BTGO, ASST, PPC, ABCL | $15,695.79 | $9,697.39 | CYPH×560, AU×6, AEM×3, ARCT×64, INDP×518, CAN×2452, DFDV×178, TEM×10 |
| 2026-08-24 | -5.17 | $15,695.79 | CYPH×560, AU×6, AEM×3, ARCT×64, INDP×518, CAN×2452, DFDV×178, TEM×10 | $9,434.78 | -262.61 | -7.48 | — | CYPH, AU, AEM, INDP, DFDV, TEM | $11,192.72 | $9,404.84 | ARCT×64, CAN×2452 |
| 2026-08-25 | +1.80 | $11,192.72 | ARCT×64, CAN×2452 | $9,343.20 | -61.64 | +94.36 | SUJA, CYPH, FWDI, DEFT, GORO, ASST, BMNR, RUM | ARCT, CAN | $13,918.45 | $9,391.98 | SUJA×66, CYPH×342, FWDI×97, DEFT×910, GORO×165, ASST×27, BMNR×23, RUM×62 |
| 2026-08-26 | +2.02 | $13,918.45 | SUJA×66, CYPH×342, FWDI×97, DEFT×910, GORO×165, ASST×27, BMNR×23, RUM×62 | $9,391.98 | -0.00 | +0.00 | — | — | $13,918.45 | $9,391.98 | SUJA×66, CYPH×342, FWDI×97, DEFT×910, GORO×165, ASST×27, BMNR×23, RUM×62 |
| 2026-08-27 | — | $13,918.45 | SUJA×66, CYPH×342, FWDI×97, DEFT×910, GORO×165, ASST×27, BMNR×23, RUM×62 | $9,263.07 | -128.91 | +0.00 | — | SUJA, CYPH, FWDI, DEFT, GORO, ASST, BMNR, RUM | $9,237.21 | $9,237.21 | — |
| 2026-08-28 | +0.75 | $9,237.21 | — | $9,237.21 | -0.00 | -10.69 | FIGR, XHG, DEFT, ERO, TRLV, FUTU, TXG, WPM | — | $13,602.85 | $9,202.77 | FIGR×15, XHG×142, DEFT×962, ERO×14, TRLV×50, FUTU×4, TXG×9, WPM×3 |
| 2026-08-31 | -5.85 | $13,602.85 | FIGR×15, XHG×142, DEFT×962, ERO×14, TRLV×50, FUTU×4, TXG×9, WPM×3 | $9,327.68 | +124.91 | +0.00 | — | FIGR, DEFT, ERO, TRLV, FUTU, TXG, WPM | $9,795.09 | $9,306.61 | XHG×142 |
| 2026-09-01 | -6.30 | $9,795.09 | XHG×142 | $9,295.25 | -11.36 | +12.78 | — | — | $9,795.09 | $9,308.03 | XHG×142 |
| 2026-09-02 | -3.83 | $9,795.09 | XHG×142 | $9,300.93 | -7.10 | -4.26 | — | — | $9,795.09 | $9,296.67 | XHG×142 |
| 2026-09-03 | -0.90 | $9,795.09 | XHG×142 | $9,288.15 | -8.52 | -43.45 | DEFT, MRNA, ARCT, ALEC, CAN, ERO, TRLV | — | $14,313.37 | $9,209.14 | XHG×142, DEFT×990, MRNA×4, ARCT×40, ALEC×276, CAN×2211, ERO×18, TRLV×56 |
| 2026-09-04 | — | $14,313.37 | XHG×142, DEFT×990, MRNA×4, ARCT×40, ALEC×276, CAN×2211, ERO×18, TRLV×56 | $9,127.77 | -81.37 | +55.53 | HQ, OABI, BRR | MRNA, ARCT, CAN | $16,806.65 | $9,150.32 | XHG×142, DEFT×990, ALEC×276, ERO×18, TRLV×56, HQ×88, OABI×298, BRR×643 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **SHORT** | `TNDM` | 214 | $23.33 | $2.97 | — | $14,989.65 | — | ret_5>15; gate ret_5_min=15.0; list flatten; ⚪; ret5=+19.7; leftover $5000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,989.65 | ▲ close $10,039.83 vs 09:30 $10,000.00 (session +42.80) | 16:00 close · cash $14,989.65 · equity $10,039.83 vs 09:30 $10,000.00 (+39.83; session marks +42.80) · 1 name(s) marked open→close (per-name table). TNDM×214 09:30 $23.33 → close $23.13 +42.80 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,989.65 | ▲ 09:30 equity $10,084.77 vs yday $10,039.83 (+44.94) | 09:30 open · cash $14,989.65 (unchanged overnight, no fees) · equity $10,084.77 vs prior close $10,039.83 (+44.94) · 1 name(s) re-marked at the open (per-name table). TNDM×214 yday $23.13 → 09:30 $22.92 +44.94 | — |
| 2026-08-14 09:30 ET | **COVER** | `TNDM` | 214 | $22.92 | $2.76 | $+82.01 | $10,082.01 | ▲ +82.01 after sell → book $10,082.01; vs 09:30 mark -2.76 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SHORT** | `ARX` | 32 | $19.57 | $2.12 | — | $10,706.12 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $630.13 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `OMER` | 36 | $17.35 | $2.14 | — | $11,328.59 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+31.9; leftover $630.13 | join🟢 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `AIRO` | 56 | $11.12 | $2.20 | — | $11,949.11 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $630.13 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `MXCT` | 453 | $1.39 | $5.95 | — | $12,572.84 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+25.2; leftover $630.13 | join🟢 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `QMLS` | 86 | $7.29 | $2.29 | — | $13,197.49 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.1; leftover $630.13 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `AVAH` | 52 | $11.91 | $2.18 | — | $13,814.62 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+21.3; leftover $630.13 | join🟢 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `TBBB` | 12 | $48.82 | $2.06 | — | $14,398.40 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.0; leftover $630.13 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `AMPY` | 127 | $4.94 | $2.42 | — | $15,023.36 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.4; leftover $630.13 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,023.36 | ▲ close $10,193.38 vs 09:30 $10,084.77 (session +132.73) | 16:00 close · cash $15,023.36 · equity $10,193.38 vs 09:30 $10,084.77 (+108.61; session marks +132.73) · 8 name(s) marked open→close (per-name table). ARX×32 09:30 $19.57 → close $19.58 -0.32; OMER×36 09:30 $17.35 → close $17.19 +5.76; AIRO×56 09:30 $11.12 → close $9.57 +86.80; MXCT×453 09:30 $1.39 → close $1.32 +31.71; QMLS×86 09:30 $7.29 → close $7.32 -2.58; AVAH×52 09:30 $11.91 → close $12.32 -21.32; TBBB×12 09:30 $48.82 → close $47.79 +12.36; AMPY×127 09:30 $4.94 → close $4.78 +20.32 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,023.36 | ▲ 09:30 equity $10,201.66 vs yday $10,193.38 (+8.28) | 09:30 open · cash $15,023.36 (unchanged overnight, no fees) · equity $10,201.66 vs prior close $10,193.38 (+8.28) · 8 name(s) re-marked at the open (per-name table). ARX×32 yday $19.58 → 09:30 $19.57 +0.32; OMER×36 yday $17.19 → 09:30 $17.17 +0.72; AIRO×56 yday $9.57 → 09:30 $9.57 -0.00; MXCT×453 yday $1.32 → 09:30 $1.32 -0.00; QMLS×86 yday $7.32 → 09:30 $7.24 +6.88; AVAH×52 yday $12.32 → 09:30 $12.21 +5.72; TBBB×12 yday $47.79 → 09:30 $47.39 +4.80; AMPY×127 yday $4.78 → 09:30 $4.86 -10.16 | — |
| 2026-08-17 09:30 ET | **COVER** | `ARX` | 32 | $19.57 | $2.09 | $-4.21 | $14,395.04 | ▼ -4.21 after sell → book $10,199.58; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `OMER` | 36 | $17.17 | $2.10 | $+2.25 | $13,774.82 | ▲ +2.25 after sell → book $10,197.48; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `AIRO` | 56 | $9.57 | $2.16 | $+82.45 | $13,236.74 | ▲ +82.45 after sell → book $10,195.32; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `MXCT` | 453 | $1.32 | $5.84 | $+19.92 | $12,632.94 | ▲ +19.92 after sell → book $10,189.48; vs 09:30 mark -5.84 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `QMLS` | 86 | $7.24 | $2.25 | $-0.24 | $12,008.05 | ▼ -0.24 after sell → book $10,187.23; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `AVAH` | 52 | $12.21 | $2.15 | $-19.93 | $11,370.98 | ▼ -19.93 after sell → book $10,185.08; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `TBBB` | 12 | $47.39 | $2.03 | $+13.07 | $10,800.28 | ▲ +13.07 after sell → book $10,183.06; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **COVER** | `AMPY` | 127 | $4.86 | $2.37 | $+5.37 | $10,180.69 | ▲ +5.37 after sell → book $10,180.69; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SHORT** | `CAPR` | 92 | $6.87 | $2.31 | — | $10,810.42 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+62.6; leftover $636.29 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `HTFL` | 15 | $41.23 | $2.07 | — | $11,426.80 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+46.0; leftover $636.29 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `UMAC` | 19 | $32.55 | $2.08 | — | $12,043.16 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ⚪; ret5=+30.4; leftover $636.29 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `NPWR` | 331 | $1.92 | $4.35 | — | $12,674.33 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $636.29 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `LPTH` | 42 | $14.94 | $2.15 | — | $13,299.66 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover,ohlc_hot; ⚪; ret5=+16.2; leftover $636.29 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟢 |
| 2026-08-17 09:30 ET | **SHORT** | `NMAX` | 58 | $10.97 | $2.20 | — | $13,933.72 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ⚪; ret5=+21.2; leftover $636.29 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `ALOY` | 43 | $14.66 | $2.16 | — | $14,561.94 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.0; leftover $636.29 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `INO` | 594 | $1.07 | $7.79 | — | $15,189.73 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+62.7; leftover $636.29 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,189.73 | ▲ close $10,228.41 vs 09:30 $10,201.66 (session +72.83) | 16:00 close · cash $15,189.73 · equity $10,228.41 vs 09:30 $10,201.66 (+26.75; session marks +72.83) · 8 name(s) marked open→close (per-name table). CAPR×92 09:30 $6.87 → close $7.45 -53.36; HTFL×15 09:30 $41.23 → close $41.94 -10.65; UMAC×19 09:30 $32.55 → close $30.15 +45.60; NPWR×331 09:30 $1.92 → close $1.73 +62.89; LPTH×42 09:30 $14.94 → close $14.80 +5.88; NMAX×58 09:30 $10.97 → close $10.36 +35.38; ALOY×43 09:30 $14.66 → close $13.86 +34.61; INO×594 09:30 $1.07 → close $1.15 -47.52 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,189.73 | ▲ 09:30 equity $10,340.59 vs yday $10,228.41 (+112.18) | 09:30 open · cash $15,189.73 (unchanged overnight, no fees) · equity $10,340.59 vs prior close $10,228.41 (+112.18) · 8 name(s) re-marked at the open (per-name table). CAPR×92 yday $7.45 → 09:30 $7.50 -4.60; HTFL×15 yday $41.94 → 09:30 $41.50 +6.60; UMAC×19 yday $30.15 → 09:30 $28.59 +29.64; NPWR×331 yday $1.73 → 09:30 $1.70 +9.93; LPTH×42 yday $14.80 → 09:30 $14.01 +33.18; NMAX×58 yday $10.36 → 09:30 $10.31 +2.90; ALOY×43 yday $13.86 → 09:30 $13.19 +28.60; INO×594 yday $1.15 → 09:30 $1.14 +5.94 | — |
| 2026-08-18 09:30 ET | **COVER** | `CAPR` | 92 | $7.50 | $2.27 | $-62.53 | $14,497.46 | ▼ -62.53 after sell → book $10,338.32; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **COVER** | `HTFL` | 15 | $41.50 | $2.04 | $-8.16 | $13,872.93 | ▼ -8.16 after sell → book $10,336.29; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `UMAC` | 19 | $28.59 | $2.05 | $+71.11 | $13,327.67 | ▲ +71.11 after sell → book $10,334.24; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `NPWR` | 331 | $1.70 | $4.27 | $+64.20 | $12,760.70 | ▲ +64.20 after sell → book $10,329.97; vs 09:30 mark -4.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `LPTH` | 42 | $14.01 | $2.12 | $+34.79 | $12,170.17 | ▲ +34.79 after sell → book $10,327.86; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **COVER** | `NMAX` | 58 | $10.31 | $2.16 | $+33.91 | $11,570.02 | ▲ +33.91 after sell → book $10,325.69; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **COVER** | `ALOY` | 43 | $13.19 | $2.12 | $+58.93 | $11,000.73 | ▲ +58.93 after sell → book $10,323.57; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **COVER** | `INO` | 594 | $1.14 | $7.66 | $-57.03 | $10,315.91 | ▼ -57.03 after sell → book $10,315.91; vs 09:30 mark -7.66 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,315.91 | ▲ close $10,315.91 vs 09:30 $10,340.59 (session +0.00) | 16:00 close · cash $10,315.91 · no lots left · equity $10,315.91. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,315.91 | ▲ 09:30 equity $10,315.91 vs yday $10,315.91 (+0.00) | 09:30 open · cash $10,315.91 · no holdings · equity $10,315.91 vs prior close $10,315.91 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,315.91 | ▲ close $10,315.91 vs 09:30 $10,315.91 (session +0.00) | 16:00 close · cash $10,315.91 · no lots left · equity $10,315.91. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,315.91 | ▲ 09:30 equity $10,315.91 vs yday $10,315.91 (+0.00) | 09:30 open · cash $10,315.91 · no holdings · equity $10,315.91 vs prior close $10,315.91 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **SHORT** | `MRNA` | 4 | $150.14 | $2.04 | — | $10,914.43 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $644.74 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `AZI` | 470 | $1.37 | $6.17 | — | $11,552.16 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+30.7; leftover $644.74 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `CYPH` | 560 | $1.15 | $7.34 | — | $12,188.82 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; leftover $644.74 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `BNTX` | 5 | $109.06 | $2.04 | — | $12,732.08 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+22.0; leftover $644.74 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `BTGO` | 97 | $6.61 | $2.32 | — | $13,370.44 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+25.3; leftover $644.74 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `ASST` | 40 | $16.00 | $2.15 | — | $14,008.29 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+18.9; leftover $644.74 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `PPC` | 21 | $30.65 | $2.09 | — | $14,649.85 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+16.5; leftover $644.74 | join🟡 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `ABCL` | 54 | $11.81 | $2.19 | — | $15,285.67 | — | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $644.74 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,285.67 | ▼ close $10,288.52 vs 09:30 $10,315.91 (session -1.04) | 16:00 close · cash $15,285.67 · equity $10,288.52 vs 09:30 $10,315.91 (-27.39; session marks -1.04) · 8 name(s) marked open→close (per-name table). MRNA×4 09:30 $150.14 → close $133.32 +67.28; AZI×470 09:30 $1.37 → close $1.44 -32.90; CYPH×560 09:30 $1.15 → close $1.19 -22.40; BNTX×5 09:30 $109.06 → close $110.89 -9.15; BTGO×97 09:30 $6.61 → close $6.60 +0.49; ASST×40 09:30 $16.00 → close $16.13 -5.20; PPC×21 09:30 $30.65 → close $31.24 -12.39; ABCL×54 09:30 $11.81 → close $11.57 +13.23 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,285.67 | ▼ 09:30 equity $10,114.17 vs yday $10,288.52 (-174.35) | 09:30 open · cash $15,285.67 (unchanged overnight, no fees) · equity $10,114.17 vs prior close $10,288.52 (-174.35) · 8 name(s) re-marked at the open (per-name table). MRNA×4 yday $133.32 → 09:30 $133.11 +0.84; AZI×470 yday $1.44 → 09:30 $1.46 -9.40; CYPH×560 yday $1.19 → 09:30 $1.32 -72.80; BNTX×5 yday $110.89 → 09:30 $110.92 -0.15; BTGO×97 yday $6.60 → 09:30 $6.95 -33.95; ASST×40 yday $16.13 → 09:30 $17.66 -61.20; PPC×21 yday $31.24 → 09:30 $31.13 +2.31; ABCL×54 yday $11.57 → 09:30 $11.57 -0.00 | — |
| 2026-08-21 09:30 ET | **COVER** | `MRNA` | 4 | $133.11 | $2.00 | $+64.08 | $14,751.23 | ▲ +64.08 after sell → book $10,112.17; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **COVER** | `AZI` | 470 | $1.46 | $6.06 | $-54.53 | $14,058.97 | ▼ -54.53 after sell → book $10,106.11; vs 09:30 mark -6.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `BNTX` | 5 | $110.92 | $2.00 | $-13.34 | $13,502.36 | ▼ -13.34 after sell → book $10,104.10; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `BTGO` | 97 | $6.95 | $2.28 | $-38.07 | $12,825.93 | ▼ -38.07 after sell → book $10,101.82; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `ASST` | 40 | $17.66 | $2.11 | $-70.66 | $12,117.42 | ▼ -70.66 after sell → book $10,099.71; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `PPC` | 21 | $31.13 | $2.05 | $-14.22 | $11,461.64 | ▼ -14.22 after sell → book $10,097.66; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **COVER** | `ABCL` | 54 | $11.57 | $2.15 | $+8.89 | $10,834.71 | ▲ +8.89 after sell → book $10,095.51; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SHORT** | `AU` | 6 | $119.43 | $2.05 | — | $11,549.24 | — | ret_5>15; gate ret_5_min=15.0; list flatten,mover_buy; 🔵; ⚪; ret5=+20.4; leftover $721.11 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `AEM` | 3 | $216.30 | $2.04 | — | $12,196.10 | — | ret_5>15; gate ret_5_min=15.0; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $721.11 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `ARCT` | 64 | $11.13 | $2.22 | — | $12,906.20 | — | ret_5>15; gate ret_5_min=15.0; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $721.11 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `INDP` | 518 | $1.39 | $6.80 | — | $13,619.42 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+30.2; leftover $721.11 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `CAN` | 2452 | $0.29 | $15.00 | — | $14,325.31 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+30.4; leftover $721.11 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `DFDV` | 178 | $4.04 | $2.58 | — | $15,041.84 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+23.6; leftover $721.11 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `TEM` | 10 | $65.60 | $2.06 | — | $15,695.79 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+22.8; leftover $721.11 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,695.79 | ▼ close $9,697.39 vs 09:30 $10,114.17 (session -365.37) | 16:00 close · cash $15,695.79 · equity $9,697.39 vs 09:30 $10,114.17 (-416.78; session marks -365.37) · 8 name(s) marked open→close (per-name table). CYPH×560 09:30 $1.32 → close $1.42 -56.00; AU×6 09:30 $119.43 → close $121.22 -10.74; AEM×3 09:30 $216.30 → close $216.06 +0.72; ARCT×64 09:30 $11.13 → close $13.45 -148.48; INDP×518 09:30 $1.39 → close $1.29 +51.80; CAN×2452 09:30 $0.29 → close $0.35 -149.57; DFDV×178 09:30 $4.04 → close $3.94 +17.80; TEM×10 09:30 $65.60 → close $72.69 -70.90 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,695.79 | ▼ 09:30 equity $9,434.78 vs yday $9,697.39 (-262.61) | 09:30 open · cash $15,695.79 (unchanged overnight, no fees) · equity $9,434.78 vs prior close $9,697.39 (-262.61) · 8 name(s) re-marked at the open (per-name table). CYPH×560 yday $1.42 → 09:30 $1.83 -229.60; AU×6 yday $121.22 → 09:30 $120.50 +4.32; AEM×3 yday $216.06 → 09:30 $217.03 -2.91; ARCT×64 yday $13.45 → 09:30 $13.26 +12.16; INDP×518 yday $1.29 → 09:30 $1.24 +25.90; CAN×2452 yday $0.35 → 09:30 $0.38 -61.30; DFDV×178 yday $3.94 → 09:30 $4.15 -37.38; TEM×10 yday $72.69 → 09:30 $70.07 +26.20 | — |
| 2026-08-24 09:30 ET | **COVER** | `CYPH` | 560 | $1.83 | $7.22 | $-395.37 | $14,663.76 | ▼ -395.37 after sell → book $9,427.55; vs 09:30 mark -7.23 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `AU` | 6 | $120.50 | $2.01 | $-10.48 | $13,938.75 | ▼ -10.48 after sell → book $9,425.54; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `AEM` | 3 | $217.03 | $2.00 | $-6.23 | $13,285.67 | ▼ -6.23 after sell → book $9,423.55; vs 09:30 mark -1.99 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **COVER** | `INDP` | 518 | $1.24 | $6.68 | $+64.22 | $12,636.66 | ▲ +64.22 after sell → book $9,416.86; vs 09:30 mark -6.69 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `DFDV` | 178 | $4.15 | $2.52 | $-24.69 | $11,895.44 | ▼ -24.69 after sell → book $9,414.34; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **COVER** | `TEM` | 10 | $70.07 | $2.02 | $-48.78 | $11,192.72 | ▼ -48.78 after sell → book $9,412.32; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,192.72 | ▼ close $9,404.84 vs 09:30 $9,434.78 (session -7.48) | 16:00 close · cash $11,192.72 · equity $9,404.84 vs 09:30 $9,434.78 (-29.94; session marks -7.48) · 2 name(s) marked open→close (per-name table). ARCT×64 09:30 $13.26 → close $13.76 -32.00; CAN×2452 09:30 $0.38 → close $0.37 +24.52 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,192.72 | ▼ 09:30 equity $9,343.20 vs yday $9,404.84 (-61.64) | 09:30 open · cash $11,192.72 (unchanged overnight, no fees) · equity $9,343.20 vs prior close $9,404.84 (-61.64) · 2 name(s) re-marked at the open (per-name table). ARCT×64 yday $13.76 → 09:30 $14.34 -37.12; CAN×2452 yday $0.37 → 09:30 $0.38 -24.52 | — |
| 2026-08-25 09:30 ET | **COVER** | `ARCT` | 64 | $14.34 | $2.18 | $-209.84 | $10,272.78 | ▼ -209.84 after sell → book $9,341.02; vs 09:30 mark -2.18 | dropped from list after 2 sess (min 1) | — |
| 2026-08-25 09:30 ET | **COVER** | `CAN` | 2452 | $0.38 | $16.67 | $-242.55 | $9,324.34 | ▼ -242.55 after sell → book $9,324.34; vs 09:30 mark -16.68 | dropped from list after 2 sess (min 1) | — |
| 2026-08-25 09:30 ET | **SHORT** | `SUJA` | 66 | $8.79 | $2.22 | — | $9,902.26 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.5; leftover $582.77 | join🟡 sector🟡 gen🟡 news🟡 digest🟡 ab🟡 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `CYPH` | 342 | $1.70 | $4.49 | — | $10,479.16 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+120.5; leftover $582.77 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `FWDI` | 97 | $5.99 | $2.32 | — | $11,057.87 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+20.7; leftover $582.77 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `DEFT` | 910 | $0.64 | $8.73 | — | $11,631.54 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover,ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $582.77 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `GORO` | 165 | $3.53 | $2.54 | — | $12,211.45 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+16.0; leftover $582.77 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `ASST` | 27 | $20.90 | $2.11 | — | $12,773.65 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer; 🔵; ret5=+47.9; leftover $582.77 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `BMNR` | 23 | $24.73 | $2.09 | — | $13,340.34 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer; ret5=+26.3; leftover $582.77 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `RUM` | 62 | $9.36 | $2.21 | — | $13,918.45 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer; 🔵; ret5=+21.3; leftover $582.77 | join🔴 sector🟡 gen🟡 news🟢 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,918.45 | ▲ close $9,391.98 vs 09:30 $9,343.20 (session +94.36) | 16:00 close · cash $13,918.45 · equity $9,391.98 vs 09:30 $9,343.20 (+48.78; session marks +94.36) · 8 name(s) marked open→close (per-name table). SUJA×66 09:30 $8.79 → close $8.54 +16.50; CYPH×342 09:30 $1.70 → close $1.64 +20.52; FWDI×97 09:30 $5.99 → close $5.86 +12.61; DEFT×910 09:30 $0.64 → close $0.62 +18.20; GORO×165 09:30 $3.53 → close $3.56 -4.95; ASST×27 09:30 $20.90 → close $20.20 +18.90; BMNR×23 09:30 $24.73 → close $24.21 +11.96; RUM×62 09:30 $9.36 → close $9.35 +0.62 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,918.45 | ▲ 09:30 equity $9,391.98 vs yday $9,391.98 (-0.00) | 09:30 open · cash $13,918.45 (unchanged overnight, no fees) · equity $9,391.98 vs prior close $9,391.98 (-0.00) · 8 name(s) re-marked at the open (per-name table). SUJA×66 yday $8.54 → 09:30 $8.54 -0.00; CYPH×342 yday $1.64 → 09:30 $1.64 -0.00; FWDI×97 yday $5.86 → 09:30 $5.86 -0.00; DEFT×910 yday $0.62 → 09:30 $0.62 -0.00; GORO×165 yday $3.56 → 09:30 $3.56 -0.00; ASST×27 yday $20.20 → 09:30 $20.20 -0.00; BMNR×23 yday $24.21 → 09:30 $24.21 -0.00; RUM×62 yday $9.35 → 09:30 $9.35 -0.00 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,918.45 | ▲ close $9,391.98 vs 09:30 $9,391.98 (session +0.00) | 16:00 close · cash $13,918.45 · equity $9,391.98 vs 09:30 $9,391.98 (-0.00; session marks +0.00) · 8 name(s) marked open→close (per-name table). SUJA×66 09:30 $8.54 → close $8.54 -0.00; CYPH×342 09:30 $1.64 → close $1.64 -0.00; FWDI×97 09:30 $5.86 → close $5.86 -0.00; DEFT×910 09:30 $0.62 → close $0.62 -0.00; GORO×165 09:30 $3.56 → close $3.56 -0.00; ASST×27 09:30 $20.20 → close $20.20 -0.00; BMNR×23 09:30 $24.21 → close $24.21 -0.00; RUM×62 09:30 $9.35 → close $9.35 -0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,918.45 | ▼ 09:30 equity $9,263.07 vs yday $9,391.98 (-128.91) | 09:30 open · cash $13,918.45 (unchanged overnight, no fees) · equity $9,263.07 vs prior close $9,391.98 (-128.91) · 8 name(s) re-marked at the open (per-name table). SUJA×66 yday $8.54 → 09:30 $9.39 -56.10; CYPH×342 yday $1.64 → 09:30 $1.60 +13.68; FWDI×97 yday $5.86 → 09:30 $5.97 -10.67; DEFT×910 yday $0.62 → 09:30 $0.60 +18.20; GORO×165 yday $3.56 → 09:30 $3.77 -34.65; ASST×27 yday $20.20 → 09:30 $20.72 -14.04; BMNR×23 yday $24.21 → 09:30 $24.24 -0.69; RUM×62 yday $9.35 → 09:30 $10.07 -44.64 | — |
| 2026-08-27 09:30 ET | **COVER** | `SUJA` | 66 | $9.39 | $2.19 | $-44.01 | $13,296.52 | ▼ -44.01 after sell → book $9,260.88; vs 09:30 mark -2.19 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `CYPH` | 342 | $1.60 | $4.41 | $+25.29 | $12,744.91 | ▲ +25.29 after sell → book $9,256.47; vs 09:30 mark -4.41 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `FWDI` | 97 | $5.97 | $2.28 | $-2.66 | $12,163.54 | ▼ -2.66 after sell → book $9,254.19; vs 09:30 mark -2.28 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `DEFT` | 910 | $0.60 | $8.19 | $+19.48 | $11,609.35 | ▲ +19.48 after sell → book $9,246.00; vs 09:30 mark -8.19 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `GORO` | 165 | $3.77 | $2.48 | $-44.62 | $10,984.81 | ▼ -44.62 after sell → book $9,243.51; vs 09:30 mark -2.49 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `ASST` | 27 | $20.72 | $2.07 | $+0.68 | $10,423.30 | ▲ +0.68 after sell → book $9,241.44; vs 09:30 mark -2.07 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `BMNR` | 23 | $24.24 | $2.06 | $+7.12 | $9,863.72 | ▲ +7.12 after sell → book $9,239.38; vs 09:30 mark -2.06 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **COVER** | `RUM` | 62 | $10.07 | $2.18 | $-48.41 | $9,237.21 | ▼ -48.41 after sell → book $9,237.21; vs 09:30 mark -2.17 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,237.21 | ▲ close $9,237.21 vs 09:30 $9,263.07 (session +0.00) | 16:00 close · cash $9,237.21 · no lots left · equity $9,237.21. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,237.21 | ▲ 09:30 equity $9,237.21 vs yday $9,237.21 (-0.00) | 09:30 open · cash $9,237.21 · no holdings · equity $9,237.21 vs prior close $9,237.21 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **SHORT** | `FIGR` | 15 | $37.42 | $2.07 | — | $9,796.44 | — | ret_5>15; gate ret_5_min=15.0; list yday_mover; ret5=+24.4; leftover $577.33 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🟡 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SHORT** | `XHG` | 142 | $4.06 | $2.47 | — | $10,370.49 | — | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; ret5=+16.1; leftover $577.33 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `DEFT` | 962 | $0.60 | $8.84 | — | $10,938.85 | — | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; ret5=+17.6; leftover $577.33 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🟡 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `ERO` | 14 | $39.20 | $2.07 | — | $11,485.58 | — | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; ret5=+16.6; leftover $577.33 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SHORT** | `TRLV` | 50 | $11.38 | $2.18 | — | $12,052.40 | — | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; ret5=+15.0; leftover $577.33 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `FUTU` | 4 | $128.00 | $2.04 | — | $12,562.37 | — | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; ret5=+17.5; leftover $577.33 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `TXG` | 9 | $64.10 | $2.05 | — | $13,137.22 | — | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; ret5=+16.1; leftover $577.33 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `WPM` | 3 | $155.89 | $2.03 | — | $13,602.85 | — | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; ret5=+17.6; leftover $577.33 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,602.85 | ▼ close $9,202.77 vs 09:30 $9,237.21 (session -10.69) | 16:00 close · cash $13,602.85 · equity $9,202.77 vs 09:30 $9,237.21 (-34.44; session marks -10.69) · 8 name(s) marked open→close (per-name table). FIGR×15 09:30 $37.42 → close $38.02 -9.00; XHG×142 09:30 $4.06 → close $3.80 +36.92; DEFT×962 09:30 $0.60 → close $0.65 -48.10; ERO×14 09:30 $39.20 → close $39.82 -8.68; TRLV×50 09:30 $11.38 → close $11.03 +17.50; FUTU×4 09:30 $128.00 → close $124.57 +13.72; TXG×9 09:30 $64.10 → close $64.85 -6.75; WPM×3 09:30 $155.89 → close $157.99 -6.30 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,602.85 | ▲ 09:30 equity $9,327.68 vs yday $9,202.77 (+124.91) | 09:30 open · cash $13,602.85 (unchanged overnight, no fees) · equity $9,327.68 vs prior close $9,202.77 (+124.91) · 8 name(s) re-marked at the open (per-name table). FIGR×15 yday $38.02 → 09:30 $35.50 +37.80; XHG×142 yday $3.80 → 09:30 $3.44 +51.12; DEFT×962 yday $0.65 → 09:30 $0.62 +28.86; ERO×14 yday $39.82 → 09:30 $38.60 +17.08; TRLV×50 yday $11.03 → 09:30 $12.41 -69.00; FUTU×4 yday $124.57 → 09:30 $122.82 +7.00; TXG×9 yday $64.85 → 09:30 $60.90 +35.55; WPM×3 yday $157.99 → 09:30 $152.49 +16.50 | — |
| 2026-08-31 09:30 ET | **COVER** | `FIGR` | 15 | $35.50 | $2.04 | $+24.69 | $13,068.32 | ▲ +24.69 after sell → book $9,325.65; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **COVER** | `DEFT` | 962 | $0.62 | $8.85 | $-36.93 | $12,463.03 | ▼ -36.93 after sell → book $9,316.80; vs 09:30 mark -8.85 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **COVER** | `ERO` | 14 | $38.60 | $2.03 | $+4.30 | $11,920.60 | ▲ +4.30 after sell → book $9,314.77; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-31 09:30 ET | **COVER** | `TRLV` | 50 | $12.41 | $2.14 | $-55.82 | $11,297.96 | ▼ -55.82 after sell → book $9,312.63; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **COVER** | `FUTU` | 4 | $122.82 | $2.00 | $+16.68 | $10,804.67 | ▲ +16.68 after sell → book $9,310.62; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-31 09:30 ET | **COVER** | `TXG` | 9 | $60.90 | $2.02 | $+24.73 | $10,254.56 | ▲ +24.73 after sell → book $9,308.61; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **COVER** | `WPM` | 3 | $152.49 | $2.00 | $+6.17 | $9,795.09 | ▲ +6.17 after sell → book $9,306.61; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,795.09 | ▲ close $9,306.61 vs 09:30 $9,327.68 (session +0.00) | 16:00 close · cash $9,795.09 · equity $9,306.61 vs 09:30 $9,327.68 (-21.07; session marks +0.00) · 1 name(s) marked open→close (per-name table). XHG×142 09:30 $3.44 → close $3.44 -0.00 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,795.09 | ▼ 09:30 equity $9,295.25 vs yday $9,306.61 (-11.36) | 09:30 open · cash $9,795.09 (unchanged overnight, no fees) · equity $9,295.25 vs prior close $9,306.61 (-11.36) · 1 name(s) re-marked at the open (per-name table). XHG×142 yday $3.44 → 09:30 $3.52 -11.36 | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,795.09 | ▲ close $9,308.03 vs 09:30 $9,295.25 (session +12.78) | 16:00 close · cash $9,795.09 · equity $9,308.03 vs 09:30 $9,295.25 (+12.78; session marks +12.78) · 1 name(s) marked open→close (per-name table). XHG×142 09:30 $3.52 → close $3.43 +12.78 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,795.09 | ▼ 09:30 equity $9,300.93 vs yday $9,308.03 (-7.10) | 09:30 open · cash $9,795.09 (unchanged overnight, no fees) · equity $9,300.93 vs prior close $9,308.03 (-7.10) · 1 name(s) re-marked at the open (per-name table). XHG×142 yday $3.43 → 09:30 $3.48 -7.10 | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,795.09 | ▼ close $9,296.67 vs 09:30 $9,300.93 (session -4.26) | 16:00 close · cash $9,795.09 · equity $9,296.67 vs 09:30 $9,300.93 (-4.26; session marks -4.26) · 1 name(s) marked open→close (per-name table). XHG×142 09:30 $3.48 → close $3.51 -4.26 | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,795.09 | ▼ 09:30 equity $9,288.15 vs yday $9,296.67 (-8.52) | 09:30 open · cash $9,795.09 (unchanged overnight, no fees) · equity $9,288.15 vs prior close $9,296.67 (-8.52) · 1 name(s) re-marked at the open (per-name table). XHG×142 yday $3.51 → 09:30 $3.57 -8.52 | — |
| 2026-09-03 09:30 ET | **SHORT** | `DEFT` | 990 | $0.67 | $9.80 | — | $10,448.59 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.6; leftover $663.44 | join🟡 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `MRNA` | 4 | $151.40 | $2.04 | — | $11,052.15 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+129.2; leftover $663.44 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `ARCT` | 40 | $16.46 | $2.15 | — | $11,708.41 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+63.4; leftover $663.44 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `ALEC` | 276 | $2.40 | $3.63 | — | $12,367.17 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer; 🔵; ret5=+20.4; leftover $663.44 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `CAN` | 2211 | $0.30 | $13.66 | — | $13,016.81 | — | ret_5>15; gate ret_5_min=15.0; list yday_mover; 🔵; ret5=+54.3; leftover $663.44 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `ERO` | 18 | $35.62 | $2.08 | — | $13,655.89 | — | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; 🔵; ret5=+16.6; leftover $663.44 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🔴 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `TRLV` | 56 | $11.78 | $2.20 | — | $14,313.37 | — | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; 🔵; ret5=+15.0; leftover $663.44 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,313.37 | ▼ close $9,209.14 vs 09:30 $9,288.15 (session -43.45) | 16:00 close · cash $14,313.37 · equity $9,209.14 vs 09:30 $9,288.15 (-79.01; session marks -43.45) · 8 name(s) marked open→close (per-name table). XHG×142 09:30 $3.57 → close $3.32 +35.50; DEFT×990 09:30 $0.67 → close $0.65 +19.80; MRNA×4 09:30 $151.40 → close $150.81 +2.36; ARCT×40 09:30 $16.46 → close $16.74 -11.20; ALEC×276 09:30 $2.40 → close $2.72 -88.32; CAN×2211 09:30 $0.30 → close $0.31 -22.11; ERO×18 09:30 $35.62 → close $34.76 +15.48; TRLV×56 09:30 $11.78 → close $11.69 +5.04 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,313.37 | ▼ 09:30 equity $9,127.77 vs yday $9,209.14 (-81.37) | 09:30 open · cash $14,313.37 (unchanged overnight, no fees) · equity $9,127.77 vs prior close $9,209.14 (-81.37) · 8 name(s) re-marked at the open (per-name table). XHG×142 yday $3.32 → 09:30 $3.38 -8.52; DEFT×990 yday $0.65 → 09:30 $0.65 -0.00; MRNA×4 yday $150.81 → 09:30 $145.95 +19.44; ARCT×40 yday $16.74 → 09:30 $16.77 -1.20; ALEC×276 yday $2.72 → 09:30 $2.70 +5.52; CAN×2211 yday $0.31 → 09:30 $0.34 -66.33; ERO×18 yday $34.76 → 09:30 $35.82 -19.08; TRLV×56 yday $11.69 → 09:30 $11.89 -11.20 | — |
| 2026-09-04 09:30 ET | **COVER** | `MRNA` | 4 | $145.95 | $2.00 | $+17.76 | $13,727.57 | ▲ +17.76 after sell → book $9,125.77; vs 09:30 mark -2.00 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **COVER** | `ARCT` | 40 | $16.77 | $2.11 | $-16.66 | $13,054.66 | ▼ -16.66 after sell → book $9,123.66; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **COVER** | `CAN` | 2211 | $0.34 | $14.15 | $-116.25 | $12,288.77 | ▼ -116.25 after sell → book $9,109.51; vs 09:30 mark -14.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SHORT** | `HQ` | 88 | $17.06 | $2.32 | — | $13,787.73 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+17.3; leftover $1518.25 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **SHORT** | `OABI` | 298 | $5.08 | $3.95 | — | $15,297.62 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+28.1; leftover $1518.25 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SHORT** | `BRR` | 643 | $2.36 | $8.46 | — | $16,806.65 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer; 🔵; ⚪; ret5=+28.0; leftover $1518.25 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16,806.65 | ▲ close $9,150.32 vs 09:30 $9,127.77 (session +55.53) | 16:00 close · cash $16,806.65 · equity $9,150.32 vs 09:30 $9,127.77 (+22.55; session marks +55.53) · 8 name(s) marked open→close (per-name table). XHG×142 09:30 $3.38 → close $3.43 -7.10; DEFT×990 09:30 $0.65 → close $0.68 -29.70; ALEC×276 09:30 $2.70 → close $2.51 +52.44; ERO×18 09:30 $35.82 → close $35.32 +9.00; TRLV×56 09:30 $11.89 → close $11.99 -5.60; HQ×88 09:30 $17.06 → close $15.79 +111.76; OABI×298 09:30 $5.08 → close $4.75 +98.34; BRR×643 09:30 $2.36 → close $2.63 -173.61 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AXTI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HIVE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `SNDK` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `SMTC` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FIGR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `QTRX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ALM` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WEAV` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `HAE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `IVVD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KURA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DVLT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `NMAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `USDE` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ASST` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TMC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SLS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `COIN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-26 | `CYPH` | no_price | no 09:30 open — carry |
| 2026-08-26 | `FWDI` | no_price | no 09:30 open — carry |
| 2026-08-26 | `GORO` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ASST` | no_price | no 09:30 open — carry |
| 2026-08-26 | `BMNR` | no_price | no 09:30 open — carry |
| 2026-08-26 | `RUM` | no_price | no 09:30 open — carry |
| 2026-08-26 | `INDP` | no_price | no 09:30 open |
| 2026-08-26 | `SENS` | no_price | no 09:30 open |
| 2026-08-26 | `BRR` | no_price | no 09:30 open |
| 2026-08-26 | `USDE` | no_price | no 09:30 open |
| 2026-08-26 | `XHG` | no_price | no 09:30 open |
| 2026-08-26 | `ERO` | no_price | no 09:30 open |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `DFDV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FWDI` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MSTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CAN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PURR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `KOS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SUJA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `PURR` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TRLV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `ALEC` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DEFT` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `PRQR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `USDE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `SUJA` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `DEFT` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ERO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TRLV` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FUTU` | hard_red | hard-red S=-3.83 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `XHG` | 142 | 2026-08-28 @ $4.06 | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; ret5=+16.1; leftover $577.33 |
| `DEFT` | 990 | 2026-09-03 @ $0.67 | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.6; leftover $663.44 |
| `ALEC` | 276 | 2026-09-03 @ $2.40 | ret_5>15; gate ret_5_min=15.0; list yday_gainer; 🔵; ret5=+20.4; leftover $663.44 |
| `ERO` | 18 | 2026-09-03 @ $35.62 | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; 🔵; ret5=+16.6; leftover $663.44 |
| `TRLV` | 56 | 2026-09-03 @ $11.78 | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; 🔵; ret5=+15.0; leftover $663.44 |
| `HQ` | 88 | 2026-09-04 @ $17.06 | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+17.3; leftover $1518.25 |
| `OABI` | 298 | 2026-09-04 @ $5.08 | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+28.1; leftover $1518.25 |
| `BRR` | 643 | 2026-09-04 @ $2.36 | ret_5>15; gate ret_5_min=15.0; list yday_gainer; 🔵; ⚪; ret5=+28.0; leftover $1518.25 |
