# Factor mine action — `union_white_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ white, no 🚨

Cash book **+0.95%** ($10,095) · signal-only (no cash/fees) was +3.48%. Starts YES **10/17**. Fills 132 · skips 9 · realized $+70.46.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `zero_red=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $321.23.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `BTSG` | 20 | — | $59.80 | +0.00 | $60.23 | +8.60 | +8.60 | +0.00 | +8.60 |
| 2026-08-13 | `IREN` | 27 | — | $45.98 | +0.00 | $44.76 | -32.94 | -32.94 | +0.00 | -32.94 |
| 2026-08-13 | `TPG` | 24 | — | $50.62 | +0.00 | $54.62 | +95.92 | +95.92 | +0.00 | +95.92 |
| 2026-08-13 | `TGTX` | 25 | — | $49.70 | +0.00 | $47.94 | -44.00 | -44.00 | +0.00 | -44.00 |
| 2026-08-13 | `SLS` | 106 | — | $11.70 | +0.00 | $12.36 | +69.96 | +69.96 | +0.00 | +69.96 |
| 2026-08-13 | `HIMS` | 42 | — | $29.74 | +0.00 | $28.77 | -40.74 | -40.74 | +0.00 | -40.74 |
| 2026-08-13 | `INO` | 1543 | — | $0.81 | +0.00 | $0.90 | +138.87 | +138.87 | +0.00 | +138.87 |
| 2026-08-13 | `TNDM` | 53 | — | $23.33 | +0.00 | $23.13 | -10.60 | -10.60 | +0.00 | -10.60 |
| 2026-08-14 | `BTSG` | 20 | $60.23 | $59.65 | -11.60 | — | +0.00 | -11.60 | -3.00 | — |
| 2026-08-14 | `IREN` | 27 | $44.76 | $44.09 | -18.09 | — | +0.00 | -18.09 | -51.03 | — |
| 2026-08-14 | `TPG` | 24 | $54.62 | $55.29 | +16.08 | — | +0.00 | +16.08 | +112.00 | — |
| 2026-08-14 | `TGTX` | 25 | $47.94 | $47.27 | -16.75 | — | +0.00 | -16.75 | -60.75 | — |
| 2026-08-14 | `SLS` | 106 | $12.36 | $12.40 | +4.24 | — | +0.00 | +4.24 | +74.20 | — |
| 2026-08-14 | `HIMS` | 42 | $28.77 | $29.15 | +15.96 | — | +0.00 | +15.96 | -24.78 | — |
| 2026-08-14 | `INO` | 1543 | $0.90 | $0.93 | +46.29 | — | +0.00 | +46.29 | +185.16 | — |
| 2026-08-14 | `TNDM` | 53 | $23.13 | $22.92 | -11.13 | — | +0.00 | -11.13 | -21.73 | — |
| 2026-08-14 | `DAVE` | 3 | — | $330.91 | +0.00 | $334.57 | +10.98 | +10.98 | +0.00 | +10.98 |
| 2026-08-14 | `MARA` | 140 | — | $9.01 | +0.00 | $9.20 | +26.60 | +26.60 | +0.00 | +26.60 |
| 2026-08-14 | `LDI` | 1353 | — | $0.94 | +0.00 | $0.90 | -54.12 | -54.12 | +0.00 | -54.12 |
| 2026-08-14 | `BTBT` | 845 | — | $1.50 | +0.00 | $1.57 | +59.15 | +59.15 | +0.00 | +59.15 |
| 2026-08-14 | `BETR` | 85 | — | $14.80 | +0.00 | $13.73 | -90.95 | -90.95 | +0.00 | -90.95 |
| 2026-08-14 | `ANGX` | 294 | — | $4.31 | +0.00 | $4.37 | +17.64 | +17.64 | +0.00 | +17.64 |
| 2026-08-14 | `HYLN` | 303 | — | $4.18 | +0.00 | $4.06 | -36.36 | -36.36 | +0.00 | -36.36 |
| 2026-08-14 | `WDC` | 2 | — | $503.50 | +0.00 | $508.80 | +10.60 | +10.60 | +0.00 | +10.60 |
| 2026-08-17 | `DAVE` | 3 | $334.57 | $336.94 | +7.11 | — | +0.00 | +7.11 | +18.09 | — |
| 2026-08-17 | `MARA` | 140 | $9.20 | $9.22 | +2.80 | — | +0.00 | +2.80 | +29.40 | — |
| 2026-08-17 | `LDI` | 1353 | $0.90 | $0.91 | +13.53 | — | +0.00 | +13.53 | -40.59 | — |
| 2026-08-17 | `BTBT` | 845 | $1.57 | $1.52 | -42.25 | — | +0.00 | -42.25 | +16.90 | — |
| 2026-08-17 | `BETR` | 85 | $13.73 | $13.67 | -5.10 | — | +0.00 | -5.10 | -96.05 | — |
| 2026-08-17 | `ANGX` | 294 | $4.37 | $4.60 | +67.62 | — | +0.00 | +67.62 | +85.26 | — |
| 2026-08-17 | `HYLN` | 303 | $4.06 | $4.10 | +12.12 | — | +0.00 | +12.12 | -24.24 | — |
| 2026-08-17 | `WDC` | 2 | $508.80 | $525.53 | +33.46 | — | +0.00 | +33.46 | +44.06 | — |
| 2026-08-17 | `TMC` | 311 | — | $4.05 | +0.00 | $3.77 | -87.08 | -87.08 | +0.00 | -87.08 |
| 2026-08-17 | `TGB` | 149 | — | $8.46 | +0.00 | $8.77 | +46.19 | +46.19 | +0.00 | +46.19 |
| 2026-08-17 | `DNN` | 389 | — | $3.24 | +0.00 | $3.19 | -19.45 | -19.45 | +0.00 | -19.45 |
| 2026-08-17 | `CDNL` | 31 | — | $39.85 | +0.00 | $39.23 | -19.22 | -19.22 | +0.00 | -19.22 |
| 2026-08-17 | `ABX` | 138 | — | $9.12 | +0.00 | $9.12 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-17 | `OCC` | 69 | — | $18.24 | +0.00 | $17.12 | -77.28 | -77.28 | +0.00 | -77.28 |
| 2026-08-17 | `ALM` | 77 | — | $16.20 | +0.00 | $16.36 | +12.32 | +12.32 | +0.00 | +12.32 |
| 2026-08-17 | `UMAC` | 38 | — | $32.55 | +0.00 | $30.15 | -91.20 | -91.20 | +0.00 | -91.20 |
| 2026-08-18 | `TMC` | 311 | $3.77 | $3.72 | -15.55 | — | +0.00 | -15.55 | -102.63 | — |
| 2026-08-18 | `TGB` | 149 | $8.77 | $8.55 | -32.78 | — | +0.00 | -32.78 | +13.41 | — |
| 2026-08-18 | `DNN` | 389 | $3.19 | $3.11 | -31.12 | — | +0.00 | -31.12 | -50.57 | — |
| 2026-08-18 | `CDNL` | 31 | $39.23 | $41.57 | +72.54 | — | +0.00 | +72.54 | +53.32 | — |
| 2026-08-18 | `ABX` | 138 | $9.12 | $9.03 | -12.42 | — | +0.00 | -12.42 | -12.42 | — |
| 2026-08-18 | `OCC` | 69 | $17.12 | $16.20 | -63.48 | — | +0.00 | -63.48 | -140.76 | — |
| 2026-08-18 | `ALM` | 77 | $16.36 | $15.78 | -44.66 | — | +0.00 | -44.66 | -32.34 | — |
| 2026-08-18 | `UMAC` | 38 | $30.15 | $28.59 | -59.28 | — | +0.00 | -59.28 | -150.48 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `AG` | 58 | — | $20.55 | +0.00 | $21.19 | +37.12 | +37.12 | +0.00 | +37.12 |
| 2026-08-20 | `BHP` | 13 | — | $91.01 | +0.00 | $93.63 | +34.06 | +34.06 | +0.00 | +34.06 |
| 2026-08-20 | `CDE` | 58 | — | $20.65 | +0.00 | $21.11 | +26.68 | +26.68 | +0.00 | +26.68 |
| 2026-08-20 | `HDSN` | 208 | — | $5.77 | +0.00 | $5.57 | -41.60 | -41.60 | +0.00 | -41.60 |
| 2026-08-20 | `IAG` | 61 | — | $19.63 | +0.00 | $20.50 | +53.07 | +53.07 | +0.00 | +53.07 |
| 2026-08-20 | `KGC` | 40 | — | $29.63 | +0.00 | $31.43 | +72.00 | +72.00 | +0.00 | +72.00 |
| 2026-08-20 | `NFGC` | 687 | — | $1.75 | +0.00 | $1.75 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `WPM` | 8 | — | $144.54 | +0.00 | $150.25 | +45.68 | +45.68 | +0.00 | +45.68 |
| 2026-08-21 | `AG` | 58 | $21.19 | $21.90 | +41.18 | — | +0.00 | +41.18 | +78.30 | — |
| 2026-08-21 | `BHP` | 13 | $93.63 | $95.72 | +27.17 | — | +0.00 | +27.17 | +61.23 | — |
| 2026-08-21 | `CDE` | 58 | $21.11 | $21.75 | +37.12 | — | +0.00 | +37.12 | +63.80 | — |
| 2026-08-21 | `HDSN` | 208 | $5.57 | $5.67 | +20.80 | — | +0.00 | +20.80 | -20.80 | — |
| 2026-08-21 | `IAG` | 61 | $20.50 | $21.17 | +40.87 | — | +0.00 | +40.87 | +93.94 | — |
| 2026-08-21 | `KGC` | 40 | $31.43 | $32.17 | +29.60 | — | +0.00 | +29.60 | +101.60 | — |
| 2026-08-21 | `NFGC` | 687 | $1.75 | $1.79 | +27.48 | — | +0.00 | +27.48 | +27.48 | — |
| 2026-08-21 | `WPM` | 8 | $150.25 | $154.70 | +35.60 | — | +0.00 | +35.60 | +81.28 | — |
| 2026-08-21 | `AU` | 10 | — | $119.43 | +0.00 | $121.22 | +17.90 | +17.90 | +0.00 | +17.90 |
| 2026-08-21 | `AUPH` | 73 | — | $17.20 | +0.00 | $16.65 | -40.15 | -40.15 | +0.00 | -40.15 |
| 2026-08-21 | `AEM` | 5 | — | $216.30 | +0.00 | $216.06 | -1.20 | -1.20 | +0.00 | -1.20 |
| 2026-08-21 | `ARCT` | 112 | — | $11.13 | +0.00 | $13.45 | +259.84 | +259.84 | +0.00 | +259.84 |
| 2026-08-21 | `AUTL` | 509 | — | $2.47 | +0.00 | $2.41 | -30.54 | -30.54 | +0.00 | -30.54 |
| 2026-08-21 | `CRDL` | 651 | — | $1.93 | +0.00 | $1.86 | -45.57 | -45.57 | +0.00 | -45.57 |
| 2026-08-21 | `CRSP` | 21 | — | $59.72 | +0.00 | $59.50 | -4.62 | -4.62 | +0.00 | -4.62 |
| 2026-08-21 | `CYPH` | 952 | — | $1.32 | +0.00 | $1.42 | +95.20 | +95.20 | +0.00 | +95.20 |
| 2026-08-24 | `AU` | 10 | $121.22 | $120.50 | -7.20 | — | +0.00 | -7.20 | +10.70 | — |
| 2026-08-24 | `AUPH` | 73 | $16.65 | $16.60 | -3.65 | — | +0.00 | -3.65 | -43.80 | — |
| 2026-08-24 | `AEM` | 5 | $216.06 | $217.03 | +4.85 | — | +0.00 | +4.85 | +3.65 | — |
| 2026-08-24 | `ARCT` | 112 | $13.45 | $13.26 | -21.28 | — | +0.00 | -21.28 | +238.56 | — |
| 2026-08-24 | `AUTL` | 509 | $2.41 | $2.36 | -25.45 | — | +0.00 | -25.45 | -55.99 | — |
| 2026-08-24 | `CRDL` | 651 | $1.86 | $1.87 | +6.51 | — | +0.00 | +6.51 | -39.06 | — |
| 2026-08-24 | `CRSP` | 21 | $59.50 | $58.79 | -14.91 | — | +0.00 | -14.91 | -19.53 | — |
| 2026-08-24 | `CYPH` | 952 | $1.42 | $1.83 | +390.32 | — | +0.00 | +390.32 | +485.52 | — |
| 2026-08-25 | `MOS` | 55 | — | $24.00 | +0.00 | $23.75 | -13.75 | -13.75 | +0.00 | -13.75 |
| 2026-08-25 | `CRMD` | 159 | — | $8.28 | +0.00 | $8.28 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-25 | `BMEA` | 815 | — | $1.62 | +0.00 | $1.61 | -8.15 | -8.15 | +0.00 | -8.15 |
| 2026-08-25 | `ALVO` | 252 | — | $5.22 | +0.00 | $5.25 | +7.56 | +7.56 | +0.00 | +7.56 |
| 2026-08-25 | `ZURA` | 206 | — | $6.38 | +0.00 | $6.50 | +24.72 | +24.72 | +0.00 | +24.72 |
| 2026-08-25 | `SUJA` | 150 | — | $8.79 | +0.00 | $8.54 | -37.50 | -37.50 | +0.00 | -37.50 |
| 2026-08-25 | `CYPH` | 776 | — | $1.70 | +0.00 | $1.64 | -46.56 | -46.56 | +0.00 | -46.56 |
| 2026-08-25 | `DEFT` | 2008 | — | $0.64 | +0.00 | $0.62 | -40.16 | -40.16 | +0.00 | -40.16 |
| 2026-08-26 | `MOS` | 55 | $23.75 | $23.75 | +0.00 | $23.75 | +0.00 | +0.00 | -13.75 | -13.75 |
| 2026-08-26 | `CRMD` | 159 | $8.28 | $8.28 | +0.00 | $8.28 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-26 | `BMEA` | 815 | $1.61 | $1.61 | +0.00 | $1.61 | +0.00 | +0.00 | -8.15 | -8.15 |
| 2026-08-26 | `ALVO` | 252 | $5.25 | $5.25 | +0.00 | $5.25 | +0.00 | +0.00 | +7.56 | +7.56 |
| 2026-08-26 | `ZURA` | 206 | $6.50 | $6.50 | +0.00 | $6.50 | +0.00 | +0.00 | +24.72 | +24.72 |
| 2026-08-26 | `SUJA` | 150 | $8.54 | $8.54 | +0.00 | $8.54 | +0.00 | +0.00 | -37.50 | -37.50 |
| 2026-08-26 | `CYPH` | 776 | $1.64 | $1.64 | +0.00 | $1.64 | +0.00 | +0.00 | -46.56 | -46.56 |
| 2026-08-26 | `DEFT` | 2008 | $0.62 | $0.62 | +0.00 | $0.62 | +0.00 | +0.00 | -40.16 | -40.16 |
| 2026-08-27 | `MOS` | 55 | $23.75 | $24.84 | +59.95 | — | +0.00 | +59.95 | +46.20 | — |
| 2026-08-27 | `CRMD` | 159 | $8.28 | $8.60 | +50.88 | — | +0.00 | +50.88 | +50.88 | — |
| 2026-08-27 | `BMEA` | 815 | $1.61 | $1.75 | +114.10 | — | +0.00 | +114.10 | +105.95 | — |
| 2026-08-27 | `ALVO` | 252 | $5.25 | $4.98 | -68.04 | — | +0.00 | -68.04 | -60.48 | — |
| 2026-08-27 | `ZURA` | 206 | $6.50 | $6.13 | -76.22 | — | +0.00 | -76.22 | -51.50 | — |
| 2026-08-27 | `SUJA` | 150 | $8.54 | $9.39 | +127.50 | — | +0.00 | +127.50 | +90.00 | — |
| 2026-08-27 | `CYPH` | 776 | $1.64 | $1.60 | -31.04 | — | +0.00 | -31.04 | -77.60 | — |
| 2026-08-27 | `DEFT` | 2008 | $0.62 | $0.60 | -40.16 | — | +0.00 | -40.16 | -80.32 | — |
| 2026-08-28 | `SMTC` | 8 | — | $149.40 | +0.00 | $142.43 | -55.76 | -55.76 | +0.00 | -55.76 |
| 2026-08-28 | `SIMO` | 4 | — | $272.00 | +0.00 | $255.08 | -67.68 | -67.68 | +0.00 | -67.68 |
| 2026-08-28 | `TTMI` | 10 | — | $127.07 | +0.00 | $124.73 | -23.40 | -23.40 | +0.00 | -23.40 |
| 2026-08-28 | `KEYS` | 4 | — | $323.82 | +0.00 | $325.82 | +8.00 | +8.00 | +0.00 | +8.00 |
| 2026-08-28 | `AVT` | 14 | — | $91.11 | +0.00 | $91.51 | +5.60 | +5.60 | +0.00 | +5.60 |
| 2026-08-28 | `CGNX` | 20 | — | $62.80 | +0.00 | $62.97 | +3.40 | +3.40 | +0.00 | +3.40 |
| 2026-08-28 | `COHR` | 4 | — | $303.67 | +0.00 | $295.39 | -33.12 | -33.12 | +0.00 | -33.12 |
| 2026-08-28 | `LSCC` | 10 | — | $121.13 | +0.00 | $120.47 | -6.60 | -6.60 | +0.00 | -6.60 |
| 2026-08-31 | `SMTC` | 8 | $142.43 | $133.04 | -75.12 | — | +0.00 | -75.12 | -130.88 | — |
| 2026-08-31 | `SIMO` | 4 | $255.08 | $246.79 | -33.16 | — | +0.00 | -33.16 | -100.84 | — |
| 2026-08-31 | `TTMI` | 10 | $124.73 | $117.20 | -75.30 | — | +0.00 | -75.30 | -98.70 | — |
| 2026-08-31 | `KEYS` | 4 | $325.82 | $324.14 | -6.72 | — | +0.00 | -6.72 | +1.28 | — |
| 2026-08-31 | `AVT` | 14 | $91.51 | $88.63 | -40.32 | — | +0.00 | -40.32 | -34.72 | — |
| 2026-08-31 | `CGNX` | 20 | $62.97 | $60.31 | -53.20 | — | +0.00 | -53.20 | -49.80 | — |
| 2026-08-31 | `COHR` | 4 | $295.39 | $274.13 | -85.04 | — | +0.00 | -85.04 | -118.16 | — |
| 2026-08-31 | `LSCC` | 10 | $120.47 | $116.00 | -44.70 | — | +0.00 | -44.70 | -51.30 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `ATRC` | 24 | — | $49.76 | +0.00 | $52.59 | +67.92 | +67.92 | +0.00 | +67.92 |
| 2026-09-03 | `HRMY` | 29 | — | $41.31 | +0.00 | $42.86 | +44.95 | +44.95 | +0.00 | +44.95 |
| 2026-09-03 | `CABA` | 377 | — | $3.27 | +0.00 | $3.57 | +113.10 | +113.10 | +0.00 | +113.10 |
| 2026-09-03 | `VSTM` | 160 | — | $7.70 | +0.00 | $8.02 | +51.20 | +51.20 | +0.00 | +51.20 |
| 2026-09-03 | `RVTY` | 9 | — | $125.94 | +0.00 | $130.94 | +45.00 | +45.00 | +0.00 | +45.00 |
| 2026-09-03 | `MMED` | 54 | — | $22.78 | +0.00 | $23.76 | +52.92 | +52.92 | +0.00 | +52.92 |
| 2026-09-03 | `SLN` | 83 | — | $14.70 | +0.00 | $14.79 | +7.47 | +7.47 | +0.00 | +7.47 |
| 2026-09-03 | `CRDL` | 570 | — | $2.16 | +0.00 | $2.17 | +5.70 | +5.70 | +0.00 | +5.70 |
| 2026-09-04 | `ATRC` | 24 | $52.59 | $52.88 | +6.96 | $52.46 | -10.08 | -3.12 | +74.88 | +64.80 |
| 2026-09-04 | `HRMY` | 29 | $42.86 | $42.93 | +2.03 | — | +0.00 | +2.03 | +46.98 | — |
| 2026-09-04 | `CABA` | 377 | $3.57 | $3.63 | +22.62 | $3.48 | -56.55 | -33.93 | +135.72 | +79.17 |
| 2026-09-04 | `VSTM` | 160 | $8.02 | $8.03 | +1.60 | — | +0.00 | +1.60 | +52.80 | — |
| 2026-09-04 | `RVTY` | 9 | $130.94 | $132.45 | +13.59 | — | +0.00 | +13.59 | +58.59 | — |
| 2026-09-04 | `MMED` | 54 | $23.76 | $23.88 | +6.48 | — | +0.00 | +6.48 | +59.40 | — |
| 2026-09-04 | `SLN` | 83 | $14.79 | $14.85 | +4.98 | — | +0.00 | +4.98 | +12.45 | — |
| 2026-09-04 | `CRDL` | 570 | $2.17 | $2.18 | +5.70 | — | +0.00 | +5.70 | +11.40 | — |
| 2026-09-04 | `NVAX` | 122 | — | $10.41 | +0.00 | $10.34 | -8.54 | -8.54 | +0.00 | -8.54 |
| 2026-09-04 | `BVS` | 87 | — | $14.50 | +0.00 | $14.36 | -12.18 | -12.18 | +0.00 | -12.18 |
| 2026-09-04 | `DELL` | 2 | — | $486.31 | +0.00 | $516.39 | +60.16 | +60.16 | +0.00 | +60.16 |
| 2026-09-04 | `MLYS` | 43 | — | $29.15 | +0.00 | $28.27 | -37.84 | -37.84 | +0.00 | -37.84 |
| 2026-09-04 | `IRD` | 273 | — | $4.66 | +0.00 | $4.60 | -16.38 | -16.38 | +0.00 | -16.38 |
| 2026-09-04 | `OABI` | 250 | — | $5.08 | +0.00 | $4.75 | -82.50 | -82.50 | +0.00 | -82.50 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +185.07 | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | — | $97.53 | $10,153.12 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53 |
| 2026-08-14 | +5.50 | $97.53 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53 | $10,178.12 | +25.00 | -56.46 | DAVE, MARA, LDI, BTBT, BETR, ANGX, HYLN, WDC | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | $511.85 | $10,043.46 | DAVE×3, MARA×140, LDI×1353, BTBT×845, BETR×85, ANGX×294, HYLN×303, WDC×2 |
| 2026-08-17 | +2.25 | $511.85 | DAVE×3, MARA×140, LDI×1353, BTBT×845, BETR×85, ANGX×294, HYLN×303, WDC×2 | $10,132.75 | +89.29 | -235.72 | TMC, TGB, DNN, CDNL, ABX, OCC, ALM, UMAC | DAVE, MARA, LDI, BTBT, BETR, ANGX, HYLN, WDC | $48.87 | $9,830.37 | TMC×311, TGB×149, DNN×389, CDNL×31, ABX×138, OCC×69, ALM×77, UMAC×38 |
| 2026-08-18 | -6.20 | $48.87 | TMC×311, TGB×149, DNN×389, CDNL×31, ABX×138, OCC×69, ALM×77, UMAC×38 | $9,643.62 | -186.75 | +0.00 | — | TMC, TGB, DNN, CDNL, ABX, OCC, ALM, UMAC | $9,620.85 | $9,620.85 | — |
| 2026-08-19 | -7.20 | $9,620.85 | — | $9,620.85 | +0.00 | +0.00 | — | — | $9,620.85 | $9,620.85 | — |
| 2026-08-20 | +1.12 | $9,620.85 | — | $9,620.85 | +0.00 | +227.01 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | — | $82.57 | $9,823.67 | AG×58, BHP×13, CDE×58, HDSN×208, IAG×61, KGC×40, NFGC×687, WPM×8 |
| 2026-08-21 | +3.25 | $82.57 | AG×58, BHP×13, CDE×58, HDSN×208, IAG×61, KGC×40, NFGC×687, WPM×8 | $10,083.49 | +259.82 | +250.86 | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $218.76 | $10,272.00 | AU×10, AUPH×73, AEM×5, ARCT×112, AUTL×509, CRDL×651, CRSP×21, CYPH×952 |
| 2026-08-24 | -5.17 | $218.76 | AU×10, AUPH×73, AEM×5, ARCT×112, AUTL×509, CRDL×651, CRSP×21, CYPH×952 | $10,601.19 | +329.19 | +0.00 | — | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | $10,562.83 | $10,562.83 | — |
| 2026-08-25 | +1.80 | $10,562.83 | — | $10,562.83 | +0.00 | -113.84 | MOS, CRMD, BMEA, ALVO, ZURA, SUJA, CYPH, DEFT | — | $1.10 | $10,396.62 | MOS×55, CRMD×159, BMEA×815, ALVO×252, ZURA×206, SUJA×150, CYPH×776, DEFT×2008 |
| 2026-08-26 | +2.02 | $1.10 | MOS×55, CRMD×159, BMEA×815, ALVO×252, ZURA×206, SUJA×150, CYPH×776, DEFT×2008 | $10,396.62 | +0.00 | +0.00 | — | — | $1.10 | $10,396.62 | MOS×55, CRMD×159, BMEA×815, ALVO×252, ZURA×206, SUJA×150, CYPH×776, DEFT×2008 |
| 2026-08-27 | — | $1.10 | MOS×55, CRMD×159, BMEA×815, ALVO×252, ZURA×206, SUJA×150, CYPH×776, DEFT×2008 | $10,533.59 | +136.97 | +0.00 | — | MOS, CRMD, BMEA, ALVO, ZURA, SUJA, CYPH, DEFT | $10,481.21 | $10,481.21 | — |
| 2026-08-28 | +0.75 | $10,481.21 | — | $10,481.21 | -0.00 | -169.56 | SMTC, SIMO, TTMI, KEYS, AVT, CGNX, COHR, LSCC | — | $658.37 | $10,295.51 | SMTC×8, SIMO×4, TTMI×10, KEYS×4, AVT×14, CGNX×20, COHR×4, LSCC×10 |
| 2026-08-31 | -5.85 | $658.37 | SMTC×8, SIMO×4, TTMI×10, KEYS×4, AVT×14, CGNX×20, COHR×4, LSCC×10 | $9,881.95 | -413.56 | +0.00 | — | SMTC, SIMO, TTMI, KEYS, AVT, CGNX, COHR, LSCC | $9,865.64 | $9,865.64 | — |
| 2026-09-01 | -6.30 | $9,865.64 | — | $9,865.64 | +0.00 | +0.00 | — | — | $9,865.64 | $9,865.64 | — |
| 2026-09-02 | -3.83 | $9,865.64 | — | $9,865.64 | +0.00 | +0.00 | — | — | $9,865.64 | $9,865.64 | — |
| 2026-09-03 | -0.90 | $9,865.64 | — | $9,865.64 | +0.00 | +388.26 | ATRC, HRMY, CABA, VSTM, RVTY, MMED, SLN, CRDL | — | $168.51 | $10,228.67 | ATRC×24, HRMY×29, CABA×377, VSTM×160, RVTY×9, MMED×54, SLN×83, CRDL×570 |
| 2026-09-04 | — | $168.51 | ATRC×24, HRMY×29, CABA×377, VSTM×160, RVTY×9, MMED×54, SLN×83, CRDL×570 | $10,292.63 | +63.96 | -163.91 | NVAX, BVS, DELL, MLYS, IRD, OABI | HRMY, VSTM, RVTY, MMED, SLN, CRDL | $321.23 | $10,094.72 | ATRC×24, CABA×377, NVAX×122, BVS×87, DELL×2, MLYS×43, IRD×273, OABI×250 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $8,801.95 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $7,558.42 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=+12.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $6,341.40 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=+6.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 25 | $49.70 | $2.06 | — | $5,096.84 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $3,854.33 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $2,603.13 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $1,336.17 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=+13.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $97.53 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=+19.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $97.53 | ▲ close $10,153.12 vs 09:30 $10,000.00 (session +185.07) | 16:00 close · cash $97.53 · equity $10,153.12 vs 09:30 $10,000.00 (+153.12; session marks +185.07) · 8 name(s) marked open→close (per-name table). BTSG×20 09:30 $59.80 → close $60.23 +8.60; IREN×27 09:30 $45.98 → close $44.76 -32.94; TPG×24 09:30 $50.62 → close $54.62 +95.92; TGTX×25 09:30 $49.70 → close $47.94 -44.00; SLS×106 09:30 $11.70 → close $12.36 +69.96; HIMS×42 09:30 $29.74 → close $28.77 -40.74; INO×1543 09:30 $0.81 → close $0.90 +138.87; TNDM×53 09:30 $23.33 → close $23.13 -10.60 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $97.53 | ▲ 09:30 equity $10,178.12 vs yday $10,153.12 (+25.00) | 09:30 open · cash $97.53 (unchanged overnight, no fees) · equity $10,178.12 vs prior close $10,153.12 (+25.00) · 8 name(s) re-marked at the open (per-name table). BTSG×20 yday $60.23 → 09:30 $59.65 -11.60; IREN×27 yday $44.76 → 09:30 $44.09 -18.09; TPG×24 yday $54.62 → 09:30 $55.29 +16.08; TGTX×25 yday $47.94 → 09:30 $47.27 -16.75; SLS×106 yday $12.36 → 09:30 $12.40 +4.24; HIMS×42 yday $28.77 → 09:30 $29.15 +15.96; INO×1543 yday $0.90 → 09:30 $0.93 +46.29; TNDM×53 yday $23.13 → 09:30 $22.92 -11.13 | — |
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 20 | $59.65 | $2.07 | $-7.12 | $1,288.46 | ▼ -7.12 after sell → book $10,176.05; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 27 | $44.09 | $2.09 | $-55.19 | $2,476.80 | ▼ -55.19 after sell → book $10,173.96; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 24 | $55.29 | $2.08 | $+107.86 | $3,801.68 | ▲ +107.86 after sell → book $10,171.88; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SELL** | `TGTX` | 25 | $47.27 | $2.08 | $-64.90 | $4,981.35 | ▼ -64.90 after sell → book $10,169.80; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `SLS` | 106 | $12.40 | $2.34 | $+69.56 | $6,293.41 | ▲ +69.56 after sell → book $10,167.46; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `HIMS` | 42 | $29.15 | $2.14 | $-29.03 | $7,515.57 | ▼ -29.03 after sell → book $10,165.32; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 1543 | $0.93 | $19.25 | $+148.79 | $8,931.32 | ▲ +148.79 after sell → book $10,146.08; vs 09:30 mark -19.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 53 | $22.92 | $2.17 | $-26.05 | $10,143.91 | ▼ -26.05 after sell → book $10,143.91; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `DAVE` | 3 | $330.91 | $2.00 | — | $9,149.18 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=-8.6; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 140 | $9.01 | $2.41 | — | $7,885.37 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=-13.5; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 1353 | $0.94 | $16.74 | — | $6,600.87 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+0.5; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 845 | $1.50 | $10.90 | — | $5,322.47 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BETR` | 85 | $14.80 | $2.25 | — | $4,062.23 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=-9.9; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 294 | $4.31 | $3.79 | — | $2,791.29 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 303 | $4.18 | $3.91 | — | $1,520.85 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `WDC` | 2 | $503.50 | $2.00 | — | $511.85 | — | union ∩ white, no 🚨; gate zero_red=True; list probable; 🔵; ⚪; ret5=+7.9; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $511.85 | ▼ close $10,043.46 vs 09:30 $10,178.12 (session -56.46) | 16:00 close · cash $511.85 · equity $10,043.46 vs 09:30 $10,178.12 (-134.66; session marks -56.46) · 8 name(s) marked open→close (per-name table). DAVE×3 09:30 $330.91 → close $334.57 +10.98; MARA×140 09:30 $9.01 → close $9.20 +26.60; LDI×1353 09:30 $0.94 → close $0.90 -54.12; BTBT×845 09:30 $1.50 → close $1.57 +59.15; BETR×85 09:30 $14.80 → close $13.73 -90.95; ANGX×294 09:30 $4.31 → close $4.37 +17.64; HYLN×303 09:30 $4.18 → close $4.06 -36.36; WDC×2 09:30 $503.50 → close $508.80 +10.60 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $511.85 | ▲ 09:30 equity $10,132.75 vs yday $10,043.46 (+89.29) | 09:30 open · cash $511.85 (unchanged overnight, no fees) · equity $10,132.75 vs prior close $10,043.46 (+89.29) · 8 name(s) re-marked at the open (per-name table). DAVE×3 yday $334.57 → 09:30 $336.94 +7.11; MARA×140 yday $9.20 → 09:30 $9.22 +2.80; LDI×1353 yday $0.90 → 09:30 $0.91 +13.53; BTBT×845 yday $1.57 → 09:30 $1.52 -42.25; BETR×85 yday $13.73 → 09:30 $13.67 -5.10; ANGX×294 yday $4.37 → 09:30 $4.60 +67.62; HYLN×303 yday $4.06 → 09:30 $4.10 +12.12; WDC×2 yday $508.80 → 09:30 $525.53 +33.46 | — |
| 2026-08-17 09:30 ET | **SELL** | `DAVE` | 3 | $336.94 | $2.02 | $+14.07 | $1,520.65 | ▲ +14.07 after sell → book $10,130.73; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MARA` | 140 | $9.22 | $2.44 | $+24.55 | $2,809.01 | ▲ +24.55 after sell → book $10,128.29; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LDI` | 1353 | $0.91 | $16.57 | $-73.89 | $4,019.61 | ▼ -73.89 after sell → book $10,111.72; vs 09:30 mark -16.57 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 845 | $1.52 | $11.05 | $-5.05 | $5,292.96 | ▼ -5.05 after sell → book $10,100.67; vs 09:30 mark -11.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BETR` | 85 | $13.67 | $2.27 | $-100.56 | $6,452.64 | ▼ -100.56 after sell → book $10,098.40; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 294 | $4.60 | $3.85 | $+77.62 | $7,801.19 | ▲ +77.62 after sell → book $10,094.55; vs 09:30 mark -3.85 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HYLN` | 303 | $4.10 | $3.97 | $-32.12 | $9,039.52 | ▼ -32.12 after sell → book $10,090.58; vs 09:30 mark -3.97 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `WDC` | 2 | $525.53 | $2.02 | $+40.05 | $10,088.57 | ▲ +40.05 after sell → book $10,088.57; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 311 | $4.05 | $4.01 | — | $8,825.00 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=-12.3; leftover $1261.07 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 149 | $8.46 | $2.44 | — | $7,562.03 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+0.4; leftover $1261.07 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 389 | $3.24 | $5.02 | — | $6,296.65 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=+0.3; leftover $1261.07 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CDNL` | 31 | $39.85 | $2.08 | — | $5,059.22 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer; ⚪; ret5=-38.4; leftover $1261.07 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 138 | $9.12 | $2.40 | — | $3,798.25 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $1261.07 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `OCC` | 69 | $18.24 | $2.20 | — | $2,537.49 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,ohlc_hot; ⚪; ret5=+9.5; leftover $1261.07 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ALM` | 77 | $16.20 | $2.22 | — | $1,287.87 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,ohlc_hot; 🔵; ⚪; ret5=+6.4; leftover $1261.07 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `UMAC` | 38 | $32.55 | $2.10 | — | $48.87 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,yday_mover; ⚪; ret5=+30.4; leftover $1261.07 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $48.87 | ▼ close $9,830.37 vs 09:30 $10,132.75 (session -235.72) | 16:00 close · cash $48.87 · equity $9,830.37 vs 09:30 $10,132.75 (-302.38; session marks -235.72) · 8 name(s) marked open→close (per-name table). TMC×311 09:30 $4.05 → close $3.77 -87.08; TGB×149 09:30 $8.46 → close $8.77 +46.19; DNN×389 09:30 $3.24 → close $3.19 -19.45; CDNL×31 09:30 $39.85 → close $39.23 -19.22; ABX×138 09:30 $9.12 → close $9.12 +0.00; OCC×69 09:30 $18.24 → close $17.12 -77.28; ALM×77 09:30 $16.20 → close $16.36 +12.32; UMAC×38 09:30 $32.55 → close $30.15 -91.20 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $48.87 | ▼ 09:30 equity $9,643.62 vs yday $9,830.37 (-186.75) | 09:30 open · cash $48.87 (unchanged overnight, no fees) · equity $9,643.62 vs prior close $9,830.37 (-186.75) · 8 name(s) re-marked at the open (per-name table). TMC×311 yday $3.77 → 09:30 $3.72 -15.55; TGB×149 yday $8.77 → 09:30 $8.55 -32.78; DNN×389 yday $3.19 → 09:30 $3.11 -31.12; CDNL×31 yday $39.23 → 09:30 $41.57 +72.54; ABX×138 yday $9.12 → 09:30 $9.03 -12.42; OCC×69 yday $17.12 → 09:30 $16.20 -63.48; ALM×77 yday $16.36 → 09:30 $15.78 -44.66; UMAC×38 yday $30.15 → 09:30 $28.59 -59.28 | — |
| 2026-08-18 09:30 ET | **SELL** | `TMC` | 311 | $3.72 | $4.07 | $-110.72 | $1,201.72 | ▼ -110.72 after sell → book $9,639.55; vs 09:30 mark -4.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGB` | 149 | $8.55 | $2.47 | $+8.50 | $2,473.19 | ▲ +8.50 after sell → book $9,637.07; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `DNN` | 389 | $3.11 | $5.09 | $-60.68 | $3,677.89 | ▼ -60.68 after sell → book $9,631.98; vs 09:30 mark -5.09 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 vol🟢 buy🟢 |
| 2026-08-18 09:30 ET | **SELL** | `CDNL` | 31 | $41.57 | $2.10 | $+49.13 | $4,964.46 | ▲ +49.13 after sell → book $9,629.88; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 138 | $9.03 | $2.44 | $-17.26 | $6,208.16 | ▼ -17.26 after sell → book $9,627.44; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `OCC` | 69 | $16.20 | $2.22 | $-145.18 | $7,323.74 | ▼ -145.18 after sell → book $9,625.22; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALM` | 77 | $15.78 | $2.24 | $-36.80 | $8,536.56 | ▼ -36.80 after sell → book $9,622.98; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `UMAC` | 38 | $28.59 | $2.12 | $-154.71 | $9,620.85 | ▼ -154.71 after sell → book $9,620.85; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,620.85 | ▲ close $9,620.85 vs 09:30 $9,643.62 (session +0.00) | 16:00 close · cash $9,620.85 · no lots left · equity $9,620.85. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,620.85 | ▲ 09:30 equity $9,620.85 vs yday $9,620.85 (+0.00) | 09:30 open · cash $9,620.85 · no holdings · equity $9,620.85 vs prior close $9,620.85 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,620.85 | ▲ close $9,620.85 vs 09:30 $9,620.85 (session +0.00) | 16:00 close · cash $9,620.85 · no lots left · equity $9,620.85. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,620.85 | ▲ 09:30 equity $9,620.85 vs yday $9,620.85 (+0.00) | 09:30 open · cash $9,620.85 · no holdings · equity $9,620.85 vs prior close $9,620.85 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 58 | $20.55 | $2.16 | — | $8,426.79 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1202.61 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,241.63 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1202.61 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 58 | $20.65 | $2.16 | — | $6,041.77 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1202.61 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 208 | $5.77 | $2.68 | — | $4,838.92 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1202.61 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 61 | $19.63 | $2.17 | — | $3,639.32 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1202.61 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 40 | $29.63 | $2.11 | — | $2,452.01 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1202.61 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 687 | $1.75 | $8.86 | — | $1,240.90 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1202.61 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $82.57 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1202.61 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $82.57 | ▲ close $9,823.67 vs 09:30 $9,620.85 (session +227.01) | 16:00 close · cash $82.57 · equity $9,823.67 vs 09:30 $9,620.85 (+202.82; session marks +227.01) · 8 name(s) marked open→close (per-name table). AG×58 09:30 $20.55 → close $21.19 +37.12; BHP×13 09:30 $91.01 → close $93.63 +34.06; CDE×58 09:30 $20.65 → close $21.11 +26.68; HDSN×208 09:30 $5.77 → close $5.57 -41.60; IAG×61 09:30 $19.63 → close $20.50 +53.07; KGC×40 09:30 $29.63 → close $31.43 +72.00; NFGC×687 09:30 $1.75 → close $1.75 +0.00; WPM×8 09:30 $144.54 → close $150.25 +45.68 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $82.57 | ▲ 09:30 equity $10,083.49 vs yday $9,823.67 (+259.82) | 09:30 open · cash $82.57 (unchanged overnight, no fees) · equity $10,083.49 vs prior close $9,823.67 (+259.82) · 8 name(s) re-marked at the open (per-name table). AG×58 yday $21.19 → 09:30 $21.90 +41.18; BHP×13 yday $93.63 → 09:30 $95.72 +27.17; CDE×58 yday $21.11 → 09:30 $21.75 +37.12; HDSN×208 yday $5.57 → 09:30 $5.67 +20.80; IAG×61 yday $20.50 → 09:30 $21.17 +40.87; KGC×40 yday $31.43 → 09:30 $32.17 +29.60; NFGC×687 yday $1.75 → 09:30 $1.79 +27.48; WPM×8 yday $150.25 → 09:30 $154.70 +35.60 | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 58 | $21.90 | $2.18 | $+73.95 | $1,350.58 | ▲ +73.95 after sell → book $10,081.30; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $2,592.89 | ▲ +57.15 after sell → book $10,079.25; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 58 | $21.75 | $2.18 | $+59.45 | $3,852.21 | ▲ +59.45 after sell → book $10,077.07; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 208 | $5.67 | $2.73 | $-26.21 | $5,028.84 | ▼ -26.21 after sell → book $10,074.34; vs 09:30 mark -2.73 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 61 | $21.17 | $2.19 | $+89.57 | $6,318.02 | ▲ +89.57 after sell → book $10,072.15; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 40 | $32.17 | $2.13 | $+97.36 | $7,602.69 | ▲ +97.36 after sell → book $10,070.02; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 687 | $1.79 | $8.99 | $+9.63 | $8,823.43 | ▲ +9.63 after sell → book $10,061.03; vs 09:30 mark -8.99 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 8 | $154.70 | $2.03 | $+77.23 | $10,059.00 | ▲ +77.23 after sell → book $10,059.00; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $8,862.68 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+20.4; leftover $1257.37 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 73 | $17.20 | $2.21 | — | $7,604.87 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $1257.37 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 5 | $216.30 | $2.00 | — | $6,521.36 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1257.37 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 112 | $11.13 | $2.33 | — | $5,272.48 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1257.37 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 509 | $2.47 | $6.57 | — | $4,008.68 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $1257.37 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 651 | $1.93 | $8.40 | — | $2,743.85 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $1257.37 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 21 | $59.72 | $2.05 | — | $1,487.68 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1257.37 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 952 | $1.32 | $12.28 | — | $218.76 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1257.37 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $218.76 | ▲ close $10,272.00 vs 09:30 $10,083.49 (session +250.86) | 16:00 close · cash $218.76 · equity $10,272.00 vs 09:30 $10,083.49 (+188.51; session marks +250.86) · 8 name(s) marked open→close (per-name table). AU×10 09:30 $119.43 → close $121.22 +17.90; AUPH×73 09:30 $17.20 → close $16.65 -40.15; AEM×5 09:30 $216.30 → close $216.06 -1.20; ARCT×112 09:30 $11.13 → close $13.45 +259.84; AUTL×509 09:30 $2.47 → close $2.41 -30.54; CRDL×651 09:30 $1.93 → close $1.86 -45.57; CRSP×21 09:30 $59.72 → close $59.50 -4.62; CYPH×952 09:30 $1.32 → close $1.42 +95.20 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $218.76 | ▲ 09:30 equity $10,601.19 vs yday $10,272.00 (+329.19) | 09:30 open · cash $218.76 (unchanged overnight, no fees) · equity $10,601.19 vs prior close $10,272.00 (+329.19) · 8 name(s) re-marked at the open (per-name table). AU×10 yday $121.22 → 09:30 $120.50 -7.20; AUPH×73 yday $16.65 → 09:30 $16.60 -3.65; AEM×5 yday $216.06 → 09:30 $217.03 +4.85; ARCT×112 yday $13.45 → 09:30 $13.26 -21.28; AUTL×509 yday $2.41 → 09:30 $2.36 -25.45; CRDL×651 yday $1.86 → 09:30 $1.87 +6.51; CRSP×21 yday $59.50 → 09:30 $58.79 -14.91; CYPH×952 yday $1.42 → 09:30 $1.83 +390.32 | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.50 | $2.04 | $+6.64 | $1,421.72 | ▲ +6.64 after sell → book $10,599.15; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 73 | $16.60 | $2.23 | $-48.24 | $2,631.29 | ▼ -48.24 after sell → book $10,596.92; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 5 | $217.03 | $2.02 | $-0.38 | $3,714.41 | ▼ -0.38 after sell → book $10,594.89; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 112 | $13.26 | $2.36 | $+233.88 | $5,197.18 | ▲ +233.88 after sell → book $10,592.54; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 509 | $2.36 | $6.66 | $-69.22 | $6,391.75 | ▼ -69.22 after sell → book $10,585.87; vs 09:30 mark -6.67 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRDL` | 651 | $1.87 | $8.52 | $-55.97 | $7,600.61 | ▼ -55.97 after sell → book $10,577.36; vs 09:30 mark -8.51 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 21 | $58.79 | $2.07 | $-23.66 | $8,833.13 | ▼ -23.66 after sell → book $10,575.29; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 952 | $1.83 | $12.45 | $+460.79 | $10,562.83 | ▲ +460.79 after sell → book $10,562.83; vs 09:30 mark -12.46 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,562.83 | ▲ close $10,562.83 vs 09:30 $10,601.19 (session +0.00) | 16:00 close · cash $10,562.83 · no lots left · equity $10,562.83. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,562.83 | ▲ 09:30 equity $10,562.83 vs yday $10,562.83 (+0.00) | 09:30 open · cash $10,562.83 · no holdings · equity $10,562.83 vs prior close $10,562.83 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 55 | $24.00 | $2.15 | — | $9,240.68 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; ⚪; ret5=+13.0; leftover $1320.35 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 159 | $8.28 | $2.47 | — | $7,921.69 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+8.8; leftover $1320.35 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 815 | $1.62 | $10.51 | — | $6,590.88 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $1320.35 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 252 | $5.22 | $3.25 | — | $5,272.19 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+9.2; leftover $1320.35 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 206 | $6.38 | $2.66 | — | $3,955.25 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer; 🔵; ⚪; ret5=+5.0; leftover $1320.35 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SUJA` | 150 | $8.79 | $2.44 | — | $2,634.31 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.5; leftover $1320.35 | join🟡 sector🟡 gen🟡 news🟡 digest🟡 ab🟡 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 776 | $1.70 | $10.01 | — | $1,305.10 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+120.5; leftover $1320.35 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `DEFT` | 2008 | $0.64 | $18.88 | — | $1.10 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,yday_mover,ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1320.35 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.10 | ▼ close $10,396.62 vs 09:30 $10,562.83 (session -113.84) | 16:00 close · cash $1.10 · equity $10,396.62 vs 09:30 $10,562.83 (-166.21; session marks -113.84) · 8 name(s) marked open→close (per-name table). MOS×55 09:30 $24.00 → close $23.75 -13.75; CRMD×159 09:30 $8.28 → close $8.28 +0.00; BMEA×815 09:30 $1.62 → close $1.61 -8.15; ALVO×252 09:30 $5.22 → close $5.25 +7.56; ZURA×206 09:30 $6.38 → close $6.50 +24.72; SUJA×150 09:30 $8.79 → close $8.54 -37.50; CYPH×776 09:30 $1.70 → close $1.64 -46.56; DEFT×2008 09:30 $0.64 → close $0.62 -40.16 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.10 | ▲ 09:30 equity $10,396.62 vs yday $10,396.62 (+0.00) | 09:30 open · cash $1.10 (unchanged overnight, no fees) · equity $10,396.62 vs prior close $10,396.62 (+0.00) · 8 name(s) re-marked at the open (per-name table). MOS×55 yday $23.75 → 09:30 $23.75 +0.00; CRMD×159 yday $8.28 → 09:30 $8.28 +0.00; BMEA×815 yday $1.61 → 09:30 $1.61 +0.00; ALVO×252 yday $5.25 → 09:30 $5.25 +0.00; ZURA×206 yday $6.50 → 09:30 $6.50 +0.00; SUJA×150 yday $8.54 → 09:30 $8.54 +0.00; CYPH×776 yday $1.64 → 09:30 $1.64 +0.00; DEFT×2008 yday $0.62 → 09:30 $0.62 +0.00 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.10 | ▲ close $10,396.62 vs 09:30 $10,396.62 (session +0.00) | 16:00 close · cash $1.10 · equity $10,396.62 vs 09:30 $10,396.62 (+0.00; session marks +0.00) · 8 name(s) marked open→close (per-name table). MOS×55 09:30 $23.75 → close $23.75 +0.00; CRMD×159 09:30 $8.28 → close $8.28 +0.00; BMEA×815 09:30 $1.61 → close $1.61 +0.00; ALVO×252 09:30 $5.25 → close $5.25 +0.00; ZURA×206 09:30 $6.50 → close $6.50 +0.00; SUJA×150 09:30 $8.54 → close $8.54 +0.00; CYPH×776 09:30 $1.64 → close $1.64 +0.00; DEFT×2008 09:30 $0.62 → close $0.62 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.10 | ▲ 09:30 equity $10,533.59 vs yday $10,396.62 (+136.97) | 09:30 open · cash $1.10 (unchanged overnight, no fees) · equity $10,533.59 vs prior close $10,396.62 (+136.97) · 8 name(s) re-marked at the open (per-name table). MOS×55 yday $23.75 → 09:30 $24.84 +59.95; CRMD×159 yday $8.28 → 09:30 $8.60 +50.88; BMEA×815 yday $1.61 → 09:30 $1.75 +114.10; ALVO×252 yday $5.25 → 09:30 $4.98 -68.04; ZURA×206 yday $6.50 → 09:30 $6.13 -76.22; SUJA×150 yday $8.54 → 09:30 $9.39 +127.50; CYPH×776 yday $1.64 → 09:30 $1.60 -31.04; DEFT×2008 yday $0.62 → 09:30 $0.60 -40.16 | — |
| 2026-08-27 09:30 ET | **SELL** | `MOS` | 55 | $24.84 | $2.18 | $+41.87 | $1,365.13 | ▲ +41.87 after sell → book $10,531.42; vs 09:30 mark -2.17 | dropped from list after 2 sess (min 1) | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-27 09:30 ET | **SELL** | `CRMD` | 159 | $8.60 | $2.50 | $+45.91 | $2,730.02 | ▲ +45.91 after sell → book $10,528.91; vs 09:30 mark -2.51 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BMEA` | 815 | $1.75 | $10.66 | $+84.78 | $4,145.61 | ▲ +84.78 after sell → book $10,518.25; vs 09:30 mark -10.66 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ALVO` | 252 | $4.98 | $3.30 | $-67.03 | $5,397.27 | ▼ -67.03 after sell → book $10,514.95; vs 09:30 mark -3.30 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ZURA` | 206 | $6.13 | $2.70 | $-56.86 | $6,657.35 | ▼ -56.86 after sell → book $10,512.25; vs 09:30 mark -2.70 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SUJA` | 150 | $9.39 | $2.48 | $+85.08 | $8,063.37 | ▲ +85.08 after sell → book $10,509.77; vs 09:30 mark -2.48 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 776 | $1.60 | $10.15 | $-97.76 | $9,294.82 | ▼ -97.76 after sell → book $10,499.62; vs 09:30 mark -10.15 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `DEFT` | 2008 | $0.60 | $18.42 | $-117.61 | $10,481.21 | ▼ -117.61 after sell → book $10,481.21; vs 09:30 mark -18.41 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,481.21 | ▲ close $10,481.21 vs 09:30 $10,533.59 (session +0.00) | 16:00 close · cash $10,481.21 · no lots left · equity $10,481.21. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,481.21 | ▲ 09:30 equity $10,481.21 vs yday $10,481.21 (-0.00) | 09:30 open · cash $10,481.21 · no holdings · equity $10,481.21 vs prior close $10,481.21 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 8 | $149.40 | $2.01 | — | $9,283.99 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=-11.6; leftover $1310.15 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SIMO` | 4 | $272.00 | $2.00 | — | $8,193.99 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer; ⚪; ret5=-3.9; leftover $1310.15 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TTMI` | 10 | $127.07 | $2.02 | — | $6,921.27 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,mover_buy; 🔵; ⚪; ret5=-21.0; leftover $1310.15 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 4 | $323.82 | $2.00 | — | $5,623.99 | — | union ∩ white, no 🚨; gate zero_red=True; list mover_buy; 🔵; ⚪; ret5=-11.7; leftover $1310.15 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `AVT` | 14 | $91.11 | $2.03 | — | $4,346.42 | — | union ∩ white, no 🚨; gate zero_red=True; list mover_buy; 🔵; ⚪; ret5=-7.4; leftover $1310.15 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CGNX` | 20 | $62.80 | $2.05 | — | $3,088.37 | — | union ∩ white, no 🚨; gate zero_red=True; list mover_buy; 🔵; ⚪; ret5=-7.8; leftover $1310.15 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `COHR` | 4 | $303.67 | $2.00 | — | $1,871.69 | — | union ∩ white, no 🚨; gate zero_red=True; list mover_buy; 🔵; ⚪; ret5=-11.1; leftover $1310.15 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `LSCC` | 10 | $121.13 | $2.02 | — | $658.37 | — | union ∩ white, no 🚨; gate zero_red=True; list mover_buy; 🔵; ⚪; ret5=-9.8; leftover $1310.15 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $658.37 | ▼ close $10,295.51 vs 09:30 $10,481.21 (session -169.56) | 16:00 close · cash $658.37 · equity $10,295.51 vs 09:30 $10,481.21 (-185.70; session marks -169.56) · 8 name(s) marked open→close (per-name table). SMTC×8 09:30 $149.40 → close $142.43 -55.76; SIMO×4 09:30 $272.00 → close $255.08 -67.68; TTMI×10 09:30 $127.07 → close $124.73 -23.40; KEYS×4 09:30 $323.82 → close $325.82 +8.00; AVT×14 09:30 $91.11 → close $91.51 +5.60; CGNX×20 09:30 $62.80 → close $62.97 +3.40; COHR×4 09:30 $303.67 → close $295.39 -33.12; LSCC×10 09:30 $121.13 → close $120.47 -6.60 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $658.37 | ▼ 09:30 equity $9,881.95 vs yday $10,295.51 (-413.56) | 09:30 open · cash $658.37 (unchanged overnight, no fees) · equity $9,881.95 vs prior close $10,295.51 (-413.56) · 8 name(s) re-marked at the open (per-name table). SMTC×8 yday $142.43 → 09:30 $133.04 -75.12; SIMO×4 yday $255.08 → 09:30 $246.79 -33.16; TTMI×10 yday $124.73 → 09:30 $117.20 -75.30; KEYS×4 yday $325.82 → 09:30 $324.14 -6.72; AVT×14 yday $91.51 → 09:30 $88.63 -40.32; CGNX×20 yday $62.97 → 09:30 $60.31 -53.20; COHR×4 yday $295.39 → 09:30 $274.13 -85.04; LSCC×10 yday $120.47 → 09:30 $116.00 -44.70 | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 8 | $133.04 | $2.03 | $-134.93 | $1,720.65 | ▼ -134.93 after sell → book $9,879.91; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SIMO` | 4 | $246.79 | $2.02 | $-104.86 | $2,705.79 | ▼ -104.86 after sell → book $9,877.89; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TTMI` | 10 | $117.20 | $2.04 | $-102.76 | $3,875.75 | ▼ -102.76 after sell → book $9,875.85; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 4 | $324.14 | $2.02 | $-2.74 | $5,170.29 | ▼ -2.74 after sell → book $9,873.83; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `AVT` | 14 | $88.63 | $2.05 | $-38.80 | $6,409.06 | ▼ -38.80 after sell → book $9,871.78; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CGNX` | 20 | $60.31 | $2.07 | $-53.92 | $7,613.19 | ▼ -53.92 after sell → book $9,869.71; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `COHR` | 4 | $274.13 | $2.02 | $-122.18 | $8,707.68 | ▼ -122.18 after sell → book $9,867.68; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `LSCC` | 10 | $116.00 | $2.04 | $-55.36 | $9,865.64 | ▼ -55.36 after sell → book $9,865.64; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,865.64 | ▲ close $9,865.64 vs 09:30 $9,881.95 (session +0.00) | 16:00 close · cash $9,865.64 · no lots left · equity $9,865.64. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,865.64 | ▲ 09:30 equity $9,865.64 vs yday $9,865.64 (+0.00) | 09:30 open · cash $9,865.64 · no holdings · equity $9,865.64 vs prior close $9,865.64 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,865.64 | ▲ close $9,865.64 vs 09:30 $9,865.64 (session +0.00) | 16:00 close · cash $9,865.64 · no lots left · equity $9,865.64. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,865.64 | ▲ 09:30 equity $9,865.64 vs yday $9,865.64 (+0.00) | 09:30 open · cash $9,865.64 · no holdings · equity $9,865.64 vs prior close $9,865.64 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,865.64 | ▲ close $9,865.64 vs 09:30 $9,865.64 (session +0.00) | 16:00 close · cash $9,865.64 · no lots left · equity $9,865.64. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,865.64 | ▲ 09:30 equity $9,865.64 vs yday $9,865.64 (+0.00) | 09:30 open · cash $9,865.64 · no holdings · equity $9,865.64 vs prior close $9,865.64 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 24 | $49.76 | $2.06 | — | $8,669.34 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1233.21 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 29 | $41.31 | $2.08 | — | $7,469.27 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $1233.21 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 377 | $3.27 | $4.86 | — | $6,231.62 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1233.21 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 160 | $7.70 | $2.47 | — | $4,997.15 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $1233.21 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $125.94 | $2.02 | — | $3,861.67 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1233.21 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 54 | $22.78 | $2.15 | — | $2,629.40 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1233.21 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SLN` | 83 | $14.70 | $2.24 | — | $1,407.06 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer; 🔵; ⚪; ret5=-3.3; leftover $1233.21 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 570 | $2.16 | $7.35 | — | $168.51 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.3; leftover $1233.21 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $168.51 | ▲ close $10,228.67 vs 09:30 $9,865.64 (session +388.26) | 16:00 close · cash $168.51 · equity $10,228.67 vs 09:30 $9,865.64 (+363.03; session marks +388.26) · 8 name(s) marked open→close (per-name table). ATRC×24 09:30 $49.76 → close $52.59 +67.92; HRMY×29 09:30 $41.31 → close $42.86 +44.95; CABA×377 09:30 $3.27 → close $3.57 +113.10; VSTM×160 09:30 $7.70 → close $8.02 +51.20; RVTY×9 09:30 $125.94 → close $130.94 +45.00; MMED×54 09:30 $22.78 → close $23.76 +52.92; SLN×83 09:30 $14.70 → close $14.79 +7.47; CRDL×570 09:30 $2.16 → close $2.17 +5.70 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $168.51 | ▲ 09:30 equity $10,292.63 vs yday $10,228.67 (+63.96) | 09:30 open · cash $168.51 (unchanged overnight, no fees) · equity $10,292.63 vs prior close $10,228.67 (+63.96) · 8 name(s) re-marked at the open (per-name table). ATRC×24 yday $52.59 → 09:30 $52.88 +6.96; HRMY×29 yday $42.86 → 09:30 $42.93 +2.03; CABA×377 yday $3.57 → 09:30 $3.63 +22.62; VSTM×160 yday $8.02 → 09:30 $8.03 +1.60; RVTY×9 yday $130.94 → 09:30 $132.45 +13.59; MMED×54 yday $23.76 → 09:30 $23.88 +6.48; SLN×83 yday $14.79 → 09:30 $14.85 +4.98; CRDL×570 yday $2.17 → 09:30 $2.18 +5.70 | — |
| 2026-09-04 09:30 ET | **SELL** | `HRMY` | 29 | $42.93 | $2.10 | $+42.81 | $1,411.38 | ▲ +42.81 after sell → book $10,290.53; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 160 | $8.03 | $2.51 | $+47.82 | $2,693.68 | ▲ +47.82 after sell → book $10,288.03; vs 09:30 mark -2.50 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 9 | $132.45 | $2.04 | $+54.54 | $3,883.69 | ▲ +54.54 after sell → book $10,285.99; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 54 | $23.88 | $2.17 | $+55.08 | $5,171.04 | ▲ +55.08 after sell → book $10,283.82; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `SLN` | 83 | $14.85 | $2.26 | $+7.95 | $6,401.32 | ▲ +7.95 after sell → book $10,281.55; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRDL` | 570 | $2.18 | $7.46 | $-3.41 | $7,636.47 | ▼ -3.41 after sell → book $10,274.10; vs 09:30 mark -7.45 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `NVAX` | 122 | $10.41 | $2.36 | — | $6,364.09 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $1272.74 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BVS` | 87 | $14.50 | $2.25 | — | $5,100.34 | — | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+0.8; leftover $1272.74 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 2 | $486.31 | $2.00 | — | $4,125.72 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-9.9; leftover $1272.74 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MLYS` | 43 | $29.15 | $2.12 | — | $2,870.16 | — | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.0; leftover $1272.74 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `IRD` | 273 | $4.66 | $3.52 | — | $1,594.45 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+12.2; leftover $1272.74 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 250 | $5.08 | $3.23 | — | $321.23 | — | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+28.1; leftover $1272.74 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $321.23 | ▼ close $10,094.72 vs 09:30 $10,292.63 (session -163.91) | 16:00 close · cash $321.23 · equity $10,094.72 vs 09:30 $10,292.63 (-197.91; session marks -163.91) · 8 name(s) marked open→close (per-name table). ATRC×24 09:30 $52.88 → close $52.46 -10.08; CABA×377 09:30 $3.63 → close $3.48 -56.55; NVAX×122 09:30 $10.41 → close $10.34 -8.54; BVS×87 09:30 $14.50 → close $14.36 -12.18; DELL×2 09:30 $486.31 → close $516.39 +60.16; MLYS×43 09:30 $29.15 → close $28.27 -37.84; IRD×273 09:30 $4.66 → close $4.60 -16.38; OABI×250 09:30 $5.08 → close $4.75 -82.50 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-26 | `MOS` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CRMD` | no_price | no 09:30 open — carry |
| 2026-08-26 | `BMEA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ALVO` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ZURA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `SUJA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CYPH` | no_price | no 09:30 open — carry |
| 2026-08-26 | `DEFT` | no_price | no 09:30 open — carry |
| 2026-08-26 | `USDE` | no_price | no 09:30 open |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ATRC` | 24 | 2026-09-03 @ $49.76 | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1233.21 |
| `CABA` | 377 | 2026-09-03 @ $3.27 | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1233.21 |
| `NVAX` | 122 | 2026-09-04 @ $10.41 | union ∩ white, no 🚨; gate zero_red=True; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $1272.74 |
| `BVS` | 87 | 2026-09-04 @ $14.50 | union ∩ white, no 🚨; gate zero_red=True; list flatten; 🔵; ⚪; ret5=+0.8; leftover $1272.74 |
| `DELL` | 2 | 2026-09-04 @ $486.31 | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-9.9; leftover $1272.74 |
| `MLYS` | 43 | 2026-09-04 @ $29.15 | union ∩ white, no 🚨; gate zero_red=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.0; leftover $1272.74 |
| `IRD` | 273 | 2026-09-04 @ $4.66 | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+12.2; leftover $1272.74 |
| `OABI` | 250 | 2026-09-04 @ $5.08 | union ∩ white, no 🚨; gate zero_red=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+28.1; leftover $1272.74 |
