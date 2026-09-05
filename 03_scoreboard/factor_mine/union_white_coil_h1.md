# Factor mine action — `union_white_coil_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **+2.05%** ($10,205) · signal-only (no cash/fees) was +7.42%. Starts YES **11/17**. Fills 124 · skips 8 · realized $+234.04.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `zero_red=True,ret_5_max=10.0,rvol_max=2.2` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $293.60.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `BTSG` | 27 | — | $59.80 | +0.00 | $60.23 | +11.61 | +11.61 | +0.00 | +11.61 |
| 2026-08-13 | `TPG` | 32 | — | $50.62 | +0.00 | $54.62 | +127.90 | +127.90 | +0.00 | +127.90 |
| 2026-08-13 | `TGTX` | 33 | — | $49.70 | +0.00 | $47.94 | -58.08 | -58.08 | +0.00 | -58.08 |
| 2026-08-13 | `SLS` | 142 | — | $11.70 | +0.00 | $12.36 | +93.72 | +93.72 | +0.00 | +93.72 |
| 2026-08-13 | `HIMS` | 56 | — | $29.74 | +0.00 | $28.77 | -54.32 | -54.32 | +0.00 | -54.32 |
| 2026-08-13 | `VOR` | 75 | — | $22.01 | +0.00 | $23.29 | +96.00 | +96.00 | +0.00 | +96.00 |
| 2026-08-14 | `BTSG` | 27 | $60.23 | $59.65 | -15.66 | — | +0.00 | -15.66 | -4.05 | — |
| 2026-08-14 | `TPG` | 32 | $54.62 | $55.29 | +21.44 | — | +0.00 | +21.44 | +149.34 | — |
| 2026-08-14 | `TGTX` | 33 | $47.94 | $47.27 | -22.11 | — | +0.00 | -22.11 | -80.19 | — |
| 2026-08-14 | `SLS` | 142 | $12.36 | $12.40 | +5.68 | — | +0.00 | +5.68 | +99.40 | — |
| 2026-08-14 | `HIMS` | 56 | $28.77 | $29.15 | +21.28 | — | +0.00 | +21.28 | -33.04 | — |
| 2026-08-14 | `VOR` | 75 | $23.29 | $23.33 | +3.00 | — | +0.00 | +3.00 | +99.00 | — |
| 2026-08-14 | `DAVE` | 3 | — | $330.91 | +0.00 | $334.57 | +10.98 | +10.98 | +0.00 | +10.98 |
| 2026-08-14 | `MARA` | 141 | — | $9.01 | +0.00 | $9.20 | +26.79 | +26.79 | +0.00 | +26.79 |
| 2026-08-14 | `LDI` | 1361 | — | $0.94 | +0.00 | $0.90 | -54.44 | -54.44 | +0.00 | -54.44 |
| 2026-08-14 | `BTBT` | 850 | — | $1.50 | +0.00 | $1.57 | +59.50 | +59.50 | +0.00 | +59.50 |
| 2026-08-14 | `BETR` | 86 | — | $14.80 | +0.00 | $13.73 | -92.02 | -92.02 | +0.00 | -92.02 |
| 2026-08-14 | `ANGX` | 295 | — | $4.31 | +0.00 | $4.37 | +17.70 | +17.70 | +0.00 | +17.70 |
| 2026-08-14 | `HYLN` | 305 | — | $4.18 | +0.00 | $4.06 | -36.60 | -36.60 | +0.00 | -36.60 |
| 2026-08-14 | `WDC` | 2 | — | $503.50 | +0.00 | $508.80 | +10.60 | +10.60 | +0.00 | +10.60 |
| 2026-08-17 | `DAVE` | 3 | $334.57 | $336.94 | +7.11 | — | +0.00 | +7.11 | +18.09 | — |
| 2026-08-17 | `MARA` | 141 | $9.20 | $9.22 | +2.82 | — | +0.00 | +2.82 | +29.61 | — |
| 2026-08-17 | `LDI` | 1361 | $0.90 | $0.91 | +13.61 | — | +0.00 | +13.61 | -40.83 | — |
| 2026-08-17 | `BTBT` | 850 | $1.57 | $1.52 | -42.50 | — | +0.00 | -42.50 | +17.00 | — |
| 2026-08-17 | `BETR` | 86 | $13.73 | $13.67 | -5.16 | — | +0.00 | -5.16 | -97.18 | — |
| 2026-08-17 | `ANGX` | 295 | $4.37 | $4.60 | +67.85 | — | +0.00 | +67.85 | +85.55 | — |
| 2026-08-17 | `HYLN` | 305 | $4.06 | $4.10 | +12.20 | — | +0.00 | +12.20 | -24.40 | — |
| 2026-08-17 | `WDC` | 2 | $508.80 | $525.53 | +33.46 | — | +0.00 | +33.46 | +44.06 | — |
| 2026-08-17 | `TMC` | 313 | — | $4.05 | +0.00 | $3.77 | -87.64 | -87.64 | +0.00 | -87.64 |
| 2026-08-17 | `TGB` | 149 | — | $8.46 | +0.00 | $8.77 | +46.19 | +46.19 | +0.00 | +46.19 |
| 2026-08-17 | `DNN` | 391 | — | $3.24 | +0.00 | $3.19 | -19.55 | -19.55 | +0.00 | -19.55 |
| 2026-08-17 | `CDNL` | 31 | — | $39.85 | +0.00 | $39.23 | -19.22 | -19.22 | +0.00 | -19.22 |
| 2026-08-17 | `ABX` | 139 | — | $9.12 | +0.00 | $9.12 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-17 | `OCC` | 69 | — | $18.24 | +0.00 | $17.12 | -77.28 | -77.28 | +0.00 | -77.28 |
| 2026-08-17 | `ALM` | 78 | — | $16.20 | +0.00 | $16.36 | +12.48 | +12.48 | +0.00 | +12.48 |
| 2026-08-17 | `MRLN` | 338 | — | $3.75 | +0.00 | $3.54 | -72.67 | -72.67 | +0.00 | -72.67 |
| 2026-08-18 | `TMC` | 313 | $3.77 | $3.72 | -15.65 | — | +0.00 | -15.65 | -103.29 | — |
| 2026-08-18 | `TGB` | 149 | $8.77 | $8.55 | -32.78 | — | +0.00 | -32.78 | +13.41 | — |
| 2026-08-18 | `DNN` | 391 | $3.19 | $3.11 | -31.28 | — | +0.00 | -31.28 | -50.83 | — |
| 2026-08-18 | `CDNL` | 31 | $39.23 | $41.57 | +72.54 | — | +0.00 | +72.54 | +53.32 | — |
| 2026-08-18 | `ABX` | 139 | $9.12 | $9.03 | -12.51 | — | +0.00 | -12.51 | -12.51 | — |
| 2026-08-18 | `OCC` | 69 | $17.12 | $16.20 | -63.48 | — | +0.00 | -63.48 | -140.76 | — |
| 2026-08-18 | `ALM` | 78 | $16.36 | $15.78 | -45.24 | — | +0.00 | -45.24 | -32.76 | — |
| 2026-08-18 | `MRLN` | 338 | $3.54 | $3.50 | -11.83 | — | +0.00 | -11.83 | -84.50 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `AG` | 59 | — | $20.55 | +0.00 | $21.19 | +37.76 | +37.76 | +0.00 | +37.76 |
| 2026-08-20 | `BHP` | 13 | — | $91.01 | +0.00 | $93.63 | +34.06 | +34.06 | +0.00 | +34.06 |
| 2026-08-20 | `HDSN` | 210 | — | $5.77 | +0.00 | $5.57 | -42.00 | -42.00 | +0.00 | -42.00 |
| 2026-08-20 | `IAG` | 62 | — | $19.63 | +0.00 | $20.50 | +53.94 | +53.94 | +0.00 | +53.94 |
| 2026-08-20 | `KGC` | 41 | — | $29.63 | +0.00 | $31.43 | +73.80 | +73.80 | +0.00 | +73.80 |
| 2026-08-20 | `NFGC` | 695 | — | $1.75 | +0.00 | $1.75 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `MRVI` | 164 | — | $7.38 | +0.00 | $8.26 | +144.32 | +144.32 | +0.00 | +144.32 |
| 2026-08-20 | `SCZM` | 128 | — | $9.46 | +0.00 | $9.76 | +38.40 | +38.40 | +0.00 | +38.40 |
| 2026-08-21 | `AG` | 59 | $21.19 | $21.90 | +41.89 | — | +0.00 | +41.89 | +79.65 | — |
| 2026-08-21 | `BHP` | 13 | $93.63 | $95.72 | +27.17 | — | +0.00 | +27.17 | +61.23 | — |
| 2026-08-21 | `HDSN` | 210 | $5.57 | $5.67 | +21.00 | — | +0.00 | +21.00 | -21.00 | — |
| 2026-08-21 | `IAG` | 62 | $20.50 | $21.17 | +41.54 | — | +0.00 | +41.54 | +95.48 | — |
| 2026-08-21 | `KGC` | 41 | $31.43 | $32.17 | +30.34 | — | +0.00 | +30.34 | +104.14 | — |
| 2026-08-21 | `NFGC` | 695 | $1.75 | $1.79 | +27.80 | — | +0.00 | +27.80 | +27.80 | — |
| 2026-08-21 | `MRVI` | 164 | $8.26 | $8.20 | -9.84 | $8.70 | +82.00 | +72.16 | +134.48 | +216.48 |
| 2026-08-21 | `SCZM` | 128 | $9.76 | $10.26 | +64.00 | — | +0.00 | +64.00 | +102.40 | — |
| 2026-08-21 | `CRSP` | 29 | — | $59.72 | +0.00 | $59.50 | -6.38 | -6.38 | +0.00 | -6.38 |
| 2026-08-21 | `EMBC` | 328 | — | $5.43 | +0.00 | $5.23 | -65.60 | -65.60 | +0.00 | -65.60 |
| 2026-08-21 | `TXG` | 27 | — | $64.39 | +0.00 | $65.12 | +19.71 | +19.71 | +0.00 | +19.71 |
| 2026-08-21 | `BEKE` | 99 | — | $17.93 | +0.00 | $17.75 | -18.31 | -18.31 | +0.00 | -18.31 |
| 2026-08-21 | `HITI` | 735 | — | $2.43 | +0.00 | $2.45 | +14.70 | +14.70 | +0.00 | +14.70 |
| 2026-08-24 | `MRVI` | 164 | $8.70 | $8.59 | -18.04 | — | +0.00 | -18.04 | +198.44 | — |
| 2026-08-24 | `CRSP` | 29 | $59.50 | $58.79 | -20.59 | — | +0.00 | -20.59 | -26.97 | — |
| 2026-08-24 | `EMBC` | 328 | $5.23 | $5.21 | -6.56 | — | +0.00 | -6.56 | -72.16 | — |
| 2026-08-24 | `TXG` | 27 | $65.12 | $63.07 | -55.35 | — | +0.00 | -55.35 | -35.64 | — |
| 2026-08-24 | `BEKE` | 99 | $17.75 | $18.06 | +30.69 | — | +0.00 | +30.69 | +12.38 | — |
| 2026-08-24 | `HITI` | 735 | $2.45 | $2.45 | +0.00 | — | +0.00 | +0.00 | +14.70 | — |
| 2026-08-25 | `CRMD` | 153 | — | $8.28 | +0.00 | $8.28 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-25 | `BMEA` | 786 | — | $1.62 | +0.00 | $1.61 | -7.86 | -7.86 | +0.00 | -7.86 |
| 2026-08-25 | `ZURA` | 199 | — | $6.38 | +0.00 | $6.50 | +23.88 | +23.88 | +0.00 | +23.88 |
| 2026-08-25 | `EZPW` | 36 | — | $34.48 | +0.00 | $34.69 | +7.56 | +7.56 | +0.00 | +7.56 |
| 2026-08-25 | `BZ` | 83 | — | $15.34 | +0.00 | $16.32 | +81.34 | +81.34 | +0.00 | +81.34 |
| 2026-08-25 | `VIPS` | 91 | — | $13.91 | +0.00 | $13.83 | -7.28 | -7.28 | +0.00 | -7.28 |
| 2026-08-25 | `RHI` | 28 | — | $44.52 | +0.00 | $44.48 | -1.12 | -1.12 | +0.00 | -1.12 |
| 2026-08-25 | `SUZ` | 140 | — | $9.07 | +0.00 | $9.18 | +15.40 | +15.40 | +0.00 | +15.40 |
| 2026-08-26 | `CRMD` | 153 | $8.28 | $8.28 | +0.00 | $8.28 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-26 | `BMEA` | 786 | $1.61 | $1.61 | +0.00 | $1.61 | +0.00 | +0.00 | -7.86 | -7.86 |
| 2026-08-26 | `ZURA` | 199 | $6.50 | $6.50 | +0.00 | $6.50 | +0.00 | +0.00 | +23.88 | +23.88 |
| 2026-08-26 | `EZPW` | 36 | $34.69 | $34.69 | +0.00 | $34.69 | +0.00 | +0.00 | +7.56 | +7.56 |
| 2026-08-26 | `BZ` | 83 | $16.32 | $16.32 | +0.00 | $16.32 | +0.00 | +0.00 | +81.34 | +81.34 |
| 2026-08-26 | `VIPS` | 91 | $13.83 | $13.83 | +0.00 | $13.83 | +0.00 | +0.00 | -7.28 | -7.28 |
| 2026-08-26 | `RHI` | 28 | $44.48 | $44.48 | +0.00 | $44.48 | +0.00 | +0.00 | -1.12 | -1.12 |
| 2026-08-26 | `SUZ` | 140 | $9.18 | $9.18 | +0.00 | $9.18 | +0.00 | +0.00 | +15.40 | +15.40 |
| 2026-08-27 | `CRMD` | 153 | $8.28 | $8.60 | +48.96 | — | +0.00 | +48.96 | +48.96 | — |
| 2026-08-27 | `BMEA` | 786 | $1.61 | $1.75 | +110.04 | — | +0.00 | +110.04 | +102.18 | — |
| 2026-08-27 | `ZURA` | 199 | $6.50 | $6.13 | -73.63 | — | +0.00 | -73.63 | -49.75 | — |
| 2026-08-27 | `EZPW` | 36 | $34.69 | $35.70 | +36.36 | — | +0.00 | +36.36 | +43.92 | — |
| 2026-08-27 | `BZ` | 83 | $16.32 | $16.77 | +37.35 | — | +0.00 | +37.35 | +118.69 | — |
| 2026-08-27 | `VIPS` | 91 | $13.83 | $14.00 | +15.47 | — | +0.00 | +15.47 | +8.19 | — |
| 2026-08-27 | `RHI` | 28 | $44.48 | $44.33 | -4.20 | — | +0.00 | -4.20 | -5.32 | — |
| 2026-08-27 | `SUZ` | 140 | $9.18 | $9.03 | -21.00 | — | +0.00 | -21.00 | -5.60 | — |
| 2026-08-28 | `SMTC` | 8 | — | $149.40 | +0.00 | $142.43 | -55.76 | -55.76 | +0.00 | -55.76 |
| 2026-08-28 | `TTMI` | 10 | — | $127.07 | +0.00 | $124.73 | -23.40 | -23.40 | +0.00 | -23.40 |
| 2026-08-28 | `KEYS` | 4 | — | $323.82 | +0.00 | $325.82 | +8.00 | +8.00 | +0.00 | +8.00 |
| 2026-08-28 | `AVT` | 14 | — | $91.11 | +0.00 | $91.51 | +5.60 | +5.60 | +0.00 | +5.60 |
| 2026-08-28 | `CGNX` | 20 | — | $62.80 | +0.00 | $62.97 | +3.40 | +3.40 | +0.00 | +3.40 |
| 2026-08-28 | `COHR` | 4 | — | $303.67 | +0.00 | $295.39 | -33.12 | -33.12 | +0.00 | -33.12 |
| 2026-08-28 | `LSCC` | 10 | — | $121.13 | +0.00 | $120.47 | -6.60 | -6.60 | +0.00 | -6.60 |
| 2026-08-28 | `MEI` | 75 | — | $17.32 | +0.00 | $17.78 | +34.50 | +34.50 | +0.00 | +34.50 |
| 2026-08-31 | `SMTC` | 8 | $142.43 | $133.04 | -75.12 | — | +0.00 | -75.12 | -130.88 | — |
| 2026-08-31 | `TTMI` | 10 | $124.73 | $117.20 | -75.30 | — | +0.00 | -75.30 | -98.70 | — |
| 2026-08-31 | `KEYS` | 4 | $325.82 | $324.14 | -6.72 | — | +0.00 | -6.72 | +1.28 | — |
| 2026-08-31 | `AVT` | 14 | $91.51 | $88.63 | -40.32 | — | +0.00 | -40.32 | -34.72 | — |
| 2026-08-31 | `CGNX` | 20 | $62.97 | $60.31 | -53.20 | — | +0.00 | -53.20 | -49.80 | — |
| 2026-08-31 | `COHR` | 4 | $295.39 | $274.13 | -85.04 | — | +0.00 | -85.04 | -118.16 | — |
| 2026-08-31 | `LSCC` | 10 | $120.47 | $116.00 | -44.70 | — | +0.00 | -44.70 | -51.30 | — |
| 2026-08-31 | `MEI` | 75 | $17.78 | $18.21 | +32.25 | — | +0.00 | +32.25 | +66.75 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `HRMY` | 30 | — | $41.31 | +0.00 | $42.86 | +46.50 | +46.50 | +0.00 | +46.50 |
| 2026-09-03 | `VSTM` | 161 | — | $7.70 | +0.00 | $8.02 | +51.52 | +51.52 | +0.00 | +51.52 |
| 2026-09-03 | `RVTY` | 9 | — | $125.94 | +0.00 | $130.94 | +45.00 | +45.00 | +0.00 | +45.00 |
| 2026-09-03 | `MMED` | 54 | — | $22.78 | +0.00 | $23.76 | +52.92 | +52.92 | +0.00 | +52.92 |
| 2026-09-03 | `CRDL` | 575 | — | $2.16 | +0.00 | $2.17 | +5.75 | +5.75 | +0.00 | +5.75 |
| 2026-09-03 | `BMEA` | 690 | — | $1.80 | +0.00 | $1.93 | +89.70 | +89.70 | +0.00 | +89.70 |
| 2026-09-03 | `VIR` | 106 | — | $11.63 | +0.00 | $11.50 | -13.78 | -13.78 | +0.00 | -13.78 |
| 2026-09-03 | `NEOV` | 339 | — | $3.66 | +0.00 | $3.78 | +40.68 | +40.68 | +0.00 | +40.68 |
| 2026-09-04 | `HRMY` | 30 | $42.86 | $42.93 | +2.10 | — | +0.00 | +2.10 | +48.60 | — |
| 2026-09-04 | `VSTM` | 161 | $8.02 | $8.03 | +1.61 | — | +0.00 | +1.61 | +53.13 | — |
| 2026-09-04 | `RVTY` | 9 | $130.94 | $132.45 | +13.59 | — | +0.00 | +13.59 | +58.59 | — |
| 2026-09-04 | `MMED` | 54 | $23.76 | $23.88 | +6.48 | — | +0.00 | +6.48 | +59.40 | — |
| 2026-09-04 | `CRDL` | 575 | $2.17 | $2.18 | +5.75 | — | +0.00 | +5.75 | +11.50 | — |
| 2026-09-04 | `BMEA` | 690 | $1.93 | $1.93 | +0.00 | — | +0.00 | +0.00 | +89.70 | — |
| 2026-09-04 | `VIR` | 106 | $11.50 | $11.54 | +4.24 | — | +0.00 | +4.24 | -9.54 | — |
| 2026-09-04 | `NEOV` | 339 | $3.78 | $3.77 | -3.39 | — | +0.00 | -3.39 | +37.29 | — |
| 2026-09-04 | `BVS` | 117 | — | $14.50 | +0.00 | $14.36 | -16.38 | -16.38 | +0.00 | -16.38 |
| 2026-09-04 | `DELL` | 3 | — | $486.31 | +0.00 | $516.39 | +90.24 | +90.24 | +0.00 | +90.24 |
| 2026-09-04 | `MLYS` | 58 | — | $29.15 | +0.00 | $28.27 | -51.04 | -51.04 | +0.00 | -51.04 |
| 2026-09-04 | `TARS` | 20 | — | $82.76 | +0.00 | $83.21 | +9.00 | +9.00 | +0.00 | +9.00 |
| 2026-09-04 | `LENZ` | 289 | — | $5.90 | +0.00 | $5.83 | -20.23 | -20.23 | +0.00 | -20.23 |
| 2026-09-04 | `INO` | 1245 | — | $1.37 | +0.00 | $1.36 | -12.45 | -12.45 | +0.00 | -12.45 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +216.83 | BTSG, TPG, TGTX, SLS, HIMS, VOR | — | $134.73 | $10,203.79 | BTSG×27, TPG×32, TGTX×33, SLS×142, HIMS×56, VOR×75 |
| 2026-08-14 | +5.50 | $134.73 | BTSG×27, TPG×32, TGTX×33, SLS×142, HIMS×56, VOR×75 | $10,217.42 | +13.63 | -57.49 | DAVE, MARA, LDI, BTBT, BETR, ANGX, HYLN, WDC | BTSG, TPG, TGTX, SLS, HIMS, VOR | $520.49 | $10,102.54 | DAVE×3, MARA×141, LDI×1361, BTBT×850, BETR×86, ANGX×295, HYLN×305, WDC×2 |
| 2026-08-17 | +2.25 | $520.49 | DAVE×3, MARA×141, LDI×1361, BTBT×850, BETR×86, ANGX×295, HYLN×305, WDC×2 | $10,191.93 | +89.39 | -217.69 | TMC, TGB, DNN, CDNL, ABX, OCC, ALM, MRLN | DAVE, MARA, LDI, BTBT, BETR, ANGX, HYLN, WDC | $35.03 | $9,905.06 | TMC×313, TGB×149, DNN×391, CDNL×31, ABX×139, OCC×69, ALM×78, MRLN×338 |
| 2026-08-18 | -6.20 | $35.03 | TMC×313, TGB×149, DNN×391, CDNL×31, ABX×139, OCC×69, ALM×78, MRLN×338 | $9,764.83 | -140.23 | +0.00 | — | TMC, TGB, DNN, CDNL, ABX, OCC, ALM, MRLN | $9,739.70 | $9,739.70 | — |
| 2026-08-19 | -7.20 | $9,739.70 | — | $9,739.70 | +0.00 | +0.00 | — | — | $9,739.70 | $9,739.70 | — |
| 2026-08-20 | +1.12 | $9,739.70 | — | $9,739.70 | +0.00 | +340.28 | AG, BHP, HDSN, IAG, KGC, NFGC, MRVI, SCZM | — | $38.07 | $10,054.97 | AG×59, BHP×13, HDSN×210, IAG×62, KGC×41, NFGC×695, MRVI×164, SCZM×128 |
| 2026-08-21 | +3.25 | $38.07 | AG×59, BHP×13, HDSN×210, IAG×62, KGC×41, NFGC×695, MRVI×164, SCZM×128 | $10,298.87 | +243.90 | +26.12 | CRSP, EMBC, TXG, BEKE, HITI | AG, BHP, HDSN, IAG, KGC, NFGC, SCZM | $98.04 | $10,282.02 | MRVI×164, CRSP×29, EMBC×328, TXG×27, BEKE×99, HITI×735 |
| 2026-08-24 | -5.17 | $98.04 | MRVI×164, CRSP×29, EMBC×328, TXG×27, BEKE×99, HITI×735 | $10,212.17 | -69.85 | +0.00 | — | MRVI, CRSP, EMBC, TXG, BEKE, HITI | $10,189.22 | $10,189.22 | — |
| 2026-08-25 | +1.80 | $10,189.22 | — | $10,189.22 | -0.00 | +111.92 | CRMD, BMEA, ZURA, EZPW, BZ, VIPS, RHI, SUZ | — | $56.51 | $10,274.88 | CRMD×153, BMEA×786, ZURA×199, EZPW×36, BZ×83, VIPS×91, RHI×28, SUZ×140 |
| 2026-08-26 | +2.02 | $56.51 | CRMD×153, BMEA×786, ZURA×199, EZPW×36, BZ×83, VIPS×91, RHI×28, SUZ×140 | $10,274.88 | -0.00 | +0.00 | — | — | $56.51 | $10,274.88 | CRMD×153, BMEA×786, ZURA×199, EZPW×36, BZ×83, VIPS×91, RHI×28, SUZ×140 |
| 2026-08-27 | — | $56.51 | CRMD×153, BMEA×786, ZURA×199, EZPW×36, BZ×83, VIPS×91, RHI×28, SUZ×140 | $10,424.23 | +149.35 | +0.00 | — | CRMD, BMEA, ZURA, EZPW, BZ, VIPS, RHI, SUZ | $10,397.63 | $10,397.63 | — |
| 2026-08-28 | +0.75 | $10,397.63 | — | $10,397.63 | -0.00 | -67.38 | SMTC, TTMI, KEYS, AVT, CGNX, COHR, LSCC, MEI | — | $363.57 | $10,313.89 | SMTC×8, TTMI×10, KEYS×4, AVT×14, CGNX×20, COHR×4, LSCC×10, MEI×75 |
| 2026-08-31 | -5.85 | $363.57 | SMTC×8, TTMI×10, KEYS×4, AVT×14, CGNX×20, COHR×4, LSCC×10, MEI×75 | $9,965.74 | -348.15 | +0.00 | — | SMTC, TTMI, KEYS, AVT, CGNX, COHR, LSCC, MEI | $9,949.22 | $9,949.22 | — |
| 2026-09-01 | -6.30 | $9,949.22 | — | $9,949.22 | +0.00 | +0.00 | — | — | $9,949.22 | $9,949.22 | — |
| 2026-09-02 | -3.83 | $9,949.22 | — | $9,949.22 | +0.00 | +0.00 | — | — | $9,949.22 | $9,949.22 | — |
| 2026-09-03 | -0.90 | $9,949.22 | — | $9,949.22 | +0.00 | +318.29 | HRMY, VSTM, RVTY, MMED, CRDL, BMEA, VIR, NEOV | — | $117.40 | $10,235.79 | HRMY×30, VSTM×161, RVTY×9, MMED×54, CRDL×575, BMEA×690, VIR×106, NEOV×339 |
| 2026-09-04 | — | $117.40 | HRMY×30, VSTM×161, RVTY×9, MMED×54, CRDL×575, BMEA×690, VIR×106, NEOV×339 | $10,266.17 | +30.38 | -0.86 | BVS, DELL, MLYS, TARS, LENZ, INO | HRMY, VSTM, RVTY, MMED, CRDL, BMEA, VIR, NEOV | $293.60 | $10,204.82 | BVS×117, DELL×3, MLYS×58, TARS×20, LENZ×289, INO×1245 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 27 | $59.80 | $2.07 | — | $8,383.33 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=-5.3; leftover $1666.67 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 32 | $50.62 | $2.09 | — | $6,761.30 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=+6.2; leftover $1666.67 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 33 | $49.70 | $2.09 | — | $5,119.11 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=-0.8; leftover $1666.67 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 142 | $11.70 | $2.42 | — | $3,455.30 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=-0.8; leftover $1666.67 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 56 | $29.74 | $2.16 | — | $1,787.70 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=-5.3; leftover $1666.67 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 75 | $22.01 | $2.21 | — | $134.73 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=+0.3; leftover $1666.67 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $134.73 | ▲ close $10,203.79 vs 09:30 $10,000.00 (session +216.83) | 16:00 close · cash $134.73 · equity $10,203.79 vs 09:30 $10,000.00 (+203.79; session marks +216.83) · 6 name(s) marked open→close (per-name table). BTSG×27 09:30 $59.80 → close $60.23 +11.61; TPG×32 09:30 $50.62 → close $54.62 +127.90; TGTX×33 09:30 $49.70 → close $47.94 -58.08; SLS×142 09:30 $11.70 → close $12.36 +93.72; HIMS×56 09:30 $29.74 → close $28.77 -54.32; VOR×75 09:30 $22.01 → close $23.29 +96.00 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $134.73 | ▲ 09:30 equity $10,217.42 vs yday $10,203.79 (+13.63) | 09:30 open · cash $134.73 (unchanged overnight, no fees) · equity $10,217.42 vs prior close $10,203.79 (+13.63) · 6 name(s) re-marked at the open (per-name table). BTSG×27 yday $60.23 → 09:30 $59.65 -15.66; TPG×32 yday $54.62 → 09:30 $55.29 +21.44; TGTX×33 yday $47.94 → 09:30 $47.27 -22.11; SLS×142 yday $12.36 → 09:30 $12.40 +5.68; HIMS×56 yday $28.77 → 09:30 $29.15 +21.28; VOR×75 yday $23.29 → 09:30 $23.33 +3.00 | — |
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 27 | $59.65 | $2.09 | $-8.21 | $1,743.19 | ▼ -8.21 after sell → book $10,215.33; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 32 | $55.29 | $2.11 | $+145.14 | $3,510.36 | ▲ +145.14 after sell → book $10,213.22; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SELL** | `TGTX` | 33 | $47.27 | $2.11 | $-84.39 | $5,068.16 | ▼ -84.39 after sell → book $10,211.11; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `SLS` | 142 | $12.40 | $2.45 | $+94.53 | $6,826.50 | ▲ +94.53 after sell → book $10,208.65; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `HIMS` | 56 | $29.15 | $2.18 | $-37.38 | $8,456.72 | ▼ -37.38 after sell → book $10,206.47; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `VOR` | 75 | $23.33 | $2.24 | $+94.54 | $10,204.23 | ▲ +94.54 after sell → book $10,204.23; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `DAVE` | 3 | $330.91 | $2.00 | — | $9,209.50 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=-8.6; leftover $1275.53 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 141 | $9.01 | $2.41 | — | $7,936.68 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=-13.5; leftover $1275.53 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 1361 | $0.94 | $16.84 | — | $6,644.59 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+0.5; leftover $1275.53 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 850 | $1.50 | $10.96 | — | $5,358.62 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1275.53 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BETR` | 86 | $14.80 | $2.25 | — | $4,083.57 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=-9.9; leftover $1275.53 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 295 | $4.31 | $3.81 | — | $2,808.32 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1275.53 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 305 | $4.18 | $3.93 | — | $1,529.48 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $1275.53 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `WDC` | 2 | $503.50 | $2.00 | — | $520.49 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable; 🔵; ⚪; ret5=+7.9; leftover $1275.53 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $520.49 | ▼ close $10,102.54 vs 09:30 $10,217.42 (session -57.49) | 16:00 close · cash $520.49 · equity $10,102.54 vs 09:30 $10,217.42 (-114.88; session marks -57.49) · 8 name(s) marked open→close (per-name table). DAVE×3 09:30 $330.91 → close $334.57 +10.98; MARA×141 09:30 $9.01 → close $9.20 +26.79; LDI×1361 09:30 $0.94 → close $0.90 -54.44; BTBT×850 09:30 $1.50 → close $1.57 +59.50; BETR×86 09:30 $14.80 → close $13.73 -92.02; ANGX×295 09:30 $4.31 → close $4.37 +17.70; HYLN×305 09:30 $4.18 → close $4.06 -36.60; WDC×2 09:30 $503.50 → close $508.80 +10.60 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $520.49 | ▲ 09:30 equity $10,191.93 vs yday $10,102.54 (+89.39) | 09:30 open · cash $520.49 (unchanged overnight, no fees) · equity $10,191.93 vs prior close $10,102.54 (+89.39) · 8 name(s) re-marked at the open (per-name table). DAVE×3 yday $334.57 → 09:30 $336.94 +7.11; MARA×141 yday $9.20 → 09:30 $9.22 +2.82; LDI×1361 yday $0.90 → 09:30 $0.91 +13.61; BTBT×850 yday $1.57 → 09:30 $1.52 -42.50; BETR×86 yday $13.73 → 09:30 $13.67 -5.16; ANGX×295 yday $4.37 → 09:30 $4.60 +67.85; HYLN×305 yday $4.06 → 09:30 $4.10 +12.20; WDC×2 yday $508.80 → 09:30 $525.53 +33.46 | — |
| 2026-08-17 09:30 ET | **SELL** | `DAVE` | 3 | $336.94 | $2.02 | $+14.07 | $1,529.29 | ▲ +14.07 after sell → book $10,189.92; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MARA` | 141 | $9.22 | $2.45 | $+24.75 | $2,826.86 | ▲ +24.75 after sell → book $10,187.47; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LDI` | 1361 | $0.91 | $16.66 | $-74.33 | $4,044.63 | ▼ -74.33 after sell → book $10,170.81; vs 09:30 mark -16.66 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 850 | $1.52 | $11.12 | $-5.08 | $5,325.51 | ▼ -5.08 after sell → book $10,159.69; vs 09:30 mark -11.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BETR` | 86 | $13.67 | $2.27 | $-101.70 | $6,498.86 | ▼ -101.70 after sell → book $10,157.42; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 295 | $4.60 | $3.87 | $+77.88 | $7,851.99 | ▲ +77.88 after sell → book $10,153.55; vs 09:30 mark -3.87 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HYLN` | 305 | $4.10 | $4.00 | $-32.33 | $9,098.50 | ▼ -32.33 after sell → book $10,149.56; vs 09:30 mark -3.99 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `WDC` | 2 | $525.53 | $2.02 | $+40.05 | $10,147.54 | ▲ +40.05 after sell → book $10,147.54; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 313 | $4.05 | $4.04 | — | $8,875.85 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=-12.3; leftover $1268.44 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 149 | $8.46 | $2.44 | — | $7,612.88 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+0.4; leftover $1268.44 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 391 | $3.24 | $5.04 | — | $6,340.99 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=+0.3; leftover $1268.44 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CDNL` | 31 | $39.85 | $2.08 | — | $5,103.56 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; ⚪; ret5=-38.4; leftover $1268.44 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 139 | $9.12 | $2.41 | — | $3,833.47 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $1268.44 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `OCC` | 69 | $18.24 | $2.20 | — | $2,572.71 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,ohlc_hot; ⚪; ret5=+9.5; leftover $1268.44 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ALM` | 78 | $16.20 | $2.22 | — | $1,306.89 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,ohlc_hot; 🔵; ⚪; ret5=+6.4; leftover $1268.44 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `MRLN` | 338 | $3.75 | $4.36 | — | $35.03 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_mover; ⚪; ret5=-15.4; leftover $1268.44 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $35.03 | ▼ close $9,905.06 vs 09:30 $10,191.93 (session -217.69) | 16:00 close · cash $35.03 · equity $9,905.06 vs 09:30 $10,191.93 (-286.87; session marks -217.69) · 8 name(s) marked open→close (per-name table). TMC×313 09:30 $4.05 → close $3.77 -87.64; TGB×149 09:30 $8.46 → close $8.77 +46.19; DNN×391 09:30 $3.24 → close $3.19 -19.55; CDNL×31 09:30 $39.85 → close $39.23 -19.22; ABX×139 09:30 $9.12 → close $9.12 +0.00; OCC×69 09:30 $18.24 → close $17.12 -77.28; ALM×78 09:30 $16.20 → close $16.36 +12.48; MRLN×338 09:30 $3.75 → close $3.54 -72.67 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $35.03 | ▼ 09:30 equity $9,764.83 vs yday $9,905.06 (-140.23) | 09:30 open · cash $35.03 (unchanged overnight, no fees) · equity $9,764.83 vs prior close $9,905.06 (-140.23) · 8 name(s) re-marked at the open (per-name table). TMC×313 yday $3.77 → 09:30 $3.72 -15.65; TGB×149 yday $8.77 → 09:30 $8.55 -32.78; DNN×391 yday $3.19 → 09:30 $3.11 -31.28; CDNL×31 yday $39.23 → 09:30 $41.57 +72.54; ABX×139 yday $9.12 → 09:30 $9.03 -12.51; OCC×69 yday $17.12 → 09:30 $16.20 -63.48; ALM×78 yday $16.36 → 09:30 $15.78 -45.24; MRLN×338 yday $3.54 → 09:30 $3.50 -11.83 | — |
| 2026-08-18 09:30 ET | **SELL** | `TMC` | 313 | $3.72 | $4.10 | $-111.43 | $1,195.29 | ▼ -111.43 after sell → book $9,760.73; vs 09:30 mark -4.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGB` | 149 | $8.55 | $2.47 | $+8.50 | $2,466.77 | ▲ +8.50 after sell → book $9,758.26; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `DNN` | 391 | $3.11 | $5.12 | $-60.99 | $3,677.66 | ▼ -60.99 after sell → book $9,753.14; vs 09:30 mark -5.12 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 vol🟢 buy🟢 |
| 2026-08-18 09:30 ET | **SELL** | `CDNL` | 31 | $41.57 | $2.10 | $+49.13 | $4,964.23 | ▲ +49.13 after sell → book $9,751.04; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 139 | $9.03 | $2.44 | $-17.36 | $6,216.96 | ▼ -17.36 after sell → book $9,748.60; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `OCC` | 69 | $16.20 | $2.22 | $-145.18 | $7,332.54 | ▼ -145.18 after sell → book $9,746.38; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALM` | 78 | $15.78 | $2.25 | $-37.23 | $8,561.13 | ▼ -37.23 after sell → book $9,744.13; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `MRLN` | 338 | $3.50 | $4.43 | $-93.29 | $9,739.70 | ▼ -93.29 after sell → book $9,739.70; vs 09:30 mark -4.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,739.70 | ▲ close $9,739.70 vs 09:30 $9,764.83 (session +0.00) | 16:00 close · cash $9,739.70 · no lots left · equity $9,739.70. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,739.70 | ▲ 09:30 equity $9,739.70 vs yday $9,739.70 (+0.00) | 09:30 open · cash $9,739.70 · no holdings · equity $9,739.70 vs prior close $9,739.70 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,739.70 | ▲ close $9,739.70 vs 09:30 $9,739.70 (session +0.00) | 16:00 close · cash $9,739.70 · no lots left · equity $9,739.70. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,739.70 | ▲ 09:30 equity $9,739.70 vs yday $9,739.70 (+0.00) | 09:30 open · cash $9,739.70 · no holdings · equity $9,739.70 vs prior close $9,739.70 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 59 | $20.55 | $2.17 | — | $8,525.09 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1217.46 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,339.93 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1217.46 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 210 | $5.77 | $2.71 | — | $6,125.52 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1217.46 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 62 | $19.63 | $2.18 | — | $4,906.28 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1217.46 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 41 | $29.63 | $2.11 | — | $3,689.34 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1217.46 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 695 | $1.75 | $8.97 | — | $2,464.13 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1217.46 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MRVI` | 164 | $7.38 | $2.48 | — | $1,251.32 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-8.5; leftover $1217.46 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SCZM` | 128 | $9.46 | $2.37 | — | $38.07 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; 🔵; ⚪; ret5=+7.6; leftover $1217.46 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $38.07 | ▲ close $10,054.97 vs 09:30 $9,739.70 (session +340.28) | 16:00 close · cash $38.07 · equity $10,054.97 vs 09:30 $9,739.70 (+315.27; session marks +340.28) · 8 name(s) marked open→close (per-name table). AG×59 09:30 $20.55 → close $21.19 +37.76; BHP×13 09:30 $91.01 → close $93.63 +34.06; HDSN×210 09:30 $5.77 → close $5.57 -42.00; IAG×62 09:30 $19.63 → close $20.50 +53.94; KGC×41 09:30 $29.63 → close $31.43 +73.80; NFGC×695 09:30 $1.75 → close $1.75 +0.00; MRVI×164 09:30 $7.38 → close $8.26 +144.32; SCZM×128 09:30 $9.46 → close $9.76 +38.40 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $38.07 | ▲ 09:30 equity $10,298.87 vs yday $10,054.97 (+243.90) | 09:30 open · cash $38.07 (unchanged overnight, no fees) · equity $10,298.87 vs prior close $10,054.97 (+243.90) · 8 name(s) re-marked at the open (per-name table). AG×59 yday $21.19 → 09:30 $21.90 +41.89; BHP×13 yday $93.63 → 09:30 $95.72 +27.17; HDSN×210 yday $5.57 → 09:30 $5.67 +21.00; IAG×62 yday $20.50 → 09:30 $21.17 +41.54; KGC×41 yday $31.43 → 09:30 $32.17 +30.34; NFGC×695 yday $1.75 → 09:30 $1.79 +27.80; MRVI×164 yday $8.26 → 09:30 $8.20 -9.84; SCZM×128 yday $9.76 → 09:30 $10.26 +64.00 | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 59 | $21.90 | $2.19 | $+75.30 | $1,327.98 | ▲ +75.30 after sell → book $10,296.68; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $2,570.29 | ▲ +57.15 after sell → book $10,294.63; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 210 | $5.67 | $2.75 | $-26.46 | $3,758.24 | ▼ -26.46 after sell → book $10,291.88; vs 09:30 mark -2.75 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 62 | $21.17 | $2.20 | $+91.11 | $5,068.58 | ▲ +91.11 after sell → book $10,289.68; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 41 | $32.17 | $2.13 | $+99.89 | $6,385.42 | ▲ +99.89 after sell → book $10,287.55; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 695 | $1.79 | $9.09 | $+9.74 | $7,620.38 | ▲ +9.74 after sell → book $10,278.46; vs 09:30 mark -9.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `SCZM` | 128 | $10.26 | $2.41 | $+97.62 | $8,931.25 | ▲ +97.62 after sell → book $10,276.05; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 29 | $59.72 | $2.08 | — | $7,197.30 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1786.25 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `EMBC` | 328 | $5.43 | $4.23 | — | $5,412.02 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ⚪; ret5=+7.0; leftover $1786.25 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `TXG` | 27 | $64.39 | $2.07 | — | $3,671.42 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1786.25 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BEKE` | 99 | $17.93 | $2.29 | — | $1,893.57 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list earn_react; 🔵; ⚪; ret5=+0.2; leftover $1786.25 | join🟢 sector🟢 gen🟢 news🟡 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `HITI` | 735 | $2.43 | $9.48 | — | $98.04 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=+5.6; leftover $1786.25 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $98.04 | ▲ close $10,282.02 vs 09:30 $10,298.87 (session +26.12) | 16:00 close · cash $98.04 · equity $10,282.02 vs 09:30 $10,298.87 (-16.85; session marks +26.12) · 6 name(s) marked open→close (per-name table). MRVI×164 09:30 $8.20 → close $8.70 +82.00; CRSP×29 09:30 $59.72 → close $59.50 -6.38; EMBC×328 09:30 $5.43 → close $5.23 -65.60; TXG×27 09:30 $64.39 → close $65.12 +19.71; BEKE×99 09:30 $17.93 → close $17.75 -18.31; HITI×735 09:30 $2.43 → close $2.45 +14.70 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $98.04 | ▼ 09:30 equity $10,212.17 vs yday $10,282.02 (-69.85) | 09:30 open · cash $98.04 (unchanged overnight, no fees) · equity $10,212.17 vs prior close $10,282.02 (-69.85) · 6 name(s) re-marked at the open (per-name table). MRVI×164 yday $8.70 → 09:30 $8.59 -18.04; CRSP×29 yday $59.50 → 09:30 $58.79 -20.59; EMBC×328 yday $5.23 → 09:30 $5.21 -6.56; TXG×27 yday $65.12 → 09:30 $63.07 -55.35; BEKE×99 yday $17.75 → 09:30 $18.06 +30.69; HITI×735 yday $2.45 → 09:30 $2.45 +0.00 | — |
| 2026-08-24 09:30 ET | **SELL** | `MRVI` | 164 | $8.59 | $2.52 | $+193.44 | $1,504.28 | ▲ +193.44 after sell → book $10,209.65; vs 09:30 mark -2.52 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 29 | $58.79 | $2.10 | $-31.15 | $3,207.09 | ▼ -31.15 after sell → book $10,207.55; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `EMBC` | 328 | $5.21 | $4.30 | $-80.69 | $4,911.67 | ▼ -80.69 after sell → book $10,203.25; vs 09:30 mark -4.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `TXG` | 27 | $63.07 | $2.09 | $-39.81 | $6,612.46 | ▼ -39.81 after sell → book $10,201.15; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `BEKE` | 99 | $18.06 | $2.32 | $+7.77 | $8,398.09 | ▲ +7.77 after sell → book $10,198.84; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `HITI` | 735 | $2.45 | $9.62 | $-4.40 | $10,189.22 | ▼ -4.40 after sell → book $10,189.22; vs 09:30 mark -9.62 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,189.22 | ▲ close $10,189.22 vs 09:30 $10,212.17 (session +0.00) | 16:00 close · cash $10,189.22 · no lots left · equity $10,189.22. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,189.22 | ▲ 09:30 equity $10,189.22 vs yday $10,189.22 (-0.00) | 09:30 open · cash $10,189.22 · no holdings · equity $10,189.22 vs prior close $10,189.22 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 153 | $8.28 | $2.45 | — | $8,919.93 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+8.8; leftover $1273.65 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 786 | $1.62 | $10.14 | — | $7,636.47 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $1273.65 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 199 | $6.38 | $2.59 | — | $6,364.26 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; 🔵; ⚪; ret5=+5.0; leftover $1273.65 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 36 | $34.48 | $2.10 | — | $5,120.89 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_gainer; 🔵; ⚪; ret5=+9.3; leftover $1273.65 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BZ` | 83 | $15.34 | $2.24 | — | $3,845.43 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list earn_react; 🔵; ⚪; ret5=+2.8; leftover $1273.65 | join🟢 sector🟡 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `VIPS` | 91 | $13.91 | $2.26 | — | $2,577.35 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list earn_react; 🔵; ⚪; ret5=+2.5; leftover $1273.65 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RHI` | 28 | $44.52 | $2.07 | — | $1,328.72 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=+3.5; leftover $1273.65 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SUZ` | 140 | $9.07 | $2.41 | — | $56.51 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; ⚪; ret5=+8.3; leftover $1273.65 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $56.51 | ▲ close $10,274.88 vs 09:30 $10,189.22 (session +111.92) | 16:00 close · cash $56.51 · equity $10,274.88 vs 09:30 $10,189.22 (+85.66; session marks +111.92) · 8 name(s) marked open→close (per-name table). CRMD×153 09:30 $8.28 → close $8.28 +0.00; BMEA×786 09:30 $1.62 → close $1.61 -7.86; ZURA×199 09:30 $6.38 → close $6.50 +23.88; EZPW×36 09:30 $34.48 → close $34.69 +7.56; BZ×83 09:30 $15.34 → close $16.32 +81.34; VIPS×91 09:30 $13.91 → close $13.83 -7.28; RHI×28 09:30 $44.52 → close $44.48 -1.12; SUZ×140 09:30 $9.07 → close $9.18 +15.40 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $56.51 | ▲ 09:30 equity $10,274.88 vs yday $10,274.88 (-0.00) | 09:30 open · cash $56.51 (unchanged overnight, no fees) · equity $10,274.88 vs prior close $10,274.88 (-0.00) · 8 name(s) re-marked at the open (per-name table). CRMD×153 yday $8.28 → 09:30 $8.28 +0.00; BMEA×786 yday $1.61 → 09:30 $1.61 +0.00; ZURA×199 yday $6.50 → 09:30 $6.50 +0.00; EZPW×36 yday $34.69 → 09:30 $34.69 +0.00; BZ×83 yday $16.32 → 09:30 $16.32 +0.00; VIPS×91 yday $13.83 → 09:30 $13.83 +0.00; RHI×28 yday $44.48 → 09:30 $44.48 +0.00; SUZ×140 yday $9.18 → 09:30 $9.18 +0.00 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $56.51 | ▲ close $10,274.88 vs 09:30 $10,274.88 (session +0.00) | 16:00 close · cash $56.51 · equity $10,274.88 vs 09:30 $10,274.88 (-0.00; session marks +0.00) · 8 name(s) marked open→close (per-name table). CRMD×153 09:30 $8.28 → close $8.28 +0.00; BMEA×786 09:30 $1.61 → close $1.61 +0.00; ZURA×199 09:30 $6.50 → close $6.50 +0.00; EZPW×36 09:30 $34.69 → close $34.69 +0.00; BZ×83 09:30 $16.32 → close $16.32 +0.00; VIPS×91 09:30 $13.83 → close $13.83 +0.00; RHI×28 09:30 $44.48 → close $44.48 +0.00; SUZ×140 09:30 $9.18 → close $9.18 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $56.51 | ▲ 09:30 equity $10,424.23 vs yday $10,274.88 (+149.35) | 09:30 open · cash $56.51 (unchanged overnight, no fees) · equity $10,424.23 vs prior close $10,274.88 (+149.35) · 8 name(s) re-marked at the open (per-name table). CRMD×153 yday $8.28 → 09:30 $8.60 +48.96; BMEA×786 yday $1.61 → 09:30 $1.75 +110.04; ZURA×199 yday $6.50 → 09:30 $6.13 -73.63; EZPW×36 yday $34.69 → 09:30 $35.70 +36.36; BZ×83 yday $16.32 → 09:30 $16.77 +37.35; VIPS×91 yday $13.83 → 09:30 $14.00 +15.47; RHI×28 yday $44.48 → 09:30 $44.33 -4.20; SUZ×140 yday $9.18 → 09:30 $9.03 -21.00 | — |
| 2026-08-27 09:30 ET | **SELL** | `CRMD` | 153 | $8.60 | $2.48 | $+44.03 | $1,369.82 | ▲ +44.03 after sell → book $10,421.74; vs 09:30 mark -2.49 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BMEA` | 786 | $1.75 | $10.28 | $+81.76 | $2,735.04 | ▲ +81.76 after sell → book $10,411.46; vs 09:30 mark -10.28 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ZURA` | 199 | $6.13 | $2.63 | $-54.97 | $3,952.28 | ▼ -54.97 after sell → book $10,408.83; vs 09:30 mark -2.63 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `EZPW` | 36 | $35.70 | $2.12 | $+39.70 | $5,235.37 | ▲ +39.70 after sell → book $10,406.72; vs 09:30 mark -2.11 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BZ` | 83 | $16.77 | $2.26 | $+114.19 | $6,625.01 | ▲ +114.19 after sell → book $10,404.45; vs 09:30 mark -2.27 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `VIPS` | 91 | $14.00 | $2.29 | $+3.64 | $7,896.72 | ▲ +3.64 after sell → book $10,402.16; vs 09:30 mark -2.29 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `RHI` | 28 | $44.33 | $2.09 | $-9.49 | $9,135.87 | ▼ -9.49 after sell → book $10,400.07; vs 09:30 mark -2.09 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SUZ` | 140 | $9.03 | $2.44 | $-10.45 | $10,397.63 | ▼ -10.45 after sell → book $10,397.63; vs 09:30 mark -2.44 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,397.63 | ▲ close $10,397.63 vs 09:30 $10,424.23 (session +0.00) | 16:00 close · cash $10,397.63 · no lots left · equity $10,397.63. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,397.63 | ▲ 09:30 equity $10,397.63 vs yday $10,397.63 (-0.00) | 09:30 open · cash $10,397.63 · no holdings · equity $10,397.63 vs prior close $10,397.63 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 8 | $149.40 | $2.01 | — | $9,200.41 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=-11.6; leftover $1299.70 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TTMI` | 10 | $127.07 | $2.02 | — | $7,927.69 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_gainer,mover_buy; 🔵; ⚪; ret5=-21.0; leftover $1299.70 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 4 | $323.82 | $2.00 | — | $6,630.41 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=-11.7; leftover $1299.70 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `AVT` | 14 | $91.11 | $2.03 | — | $5,352.84 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=-7.4; leftover $1299.70 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CGNX` | 20 | $62.80 | $2.05 | — | $4,094.79 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=-7.8; leftover $1299.70 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `COHR` | 4 | $303.67 | $2.00 | — | $2,878.11 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=-11.1; leftover $1299.70 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `LSCC` | 10 | $121.13 | $2.02 | — | $1,664.79 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=-9.8; leftover $1299.70 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `MEI` | 75 | $17.32 | $2.21 | — | $363.57 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list mover_buy; 🔵; ⚪; ret5=-16.7; leftover $1299.70 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $363.57 | ▼ close $10,313.89 vs 09:30 $10,397.63 (session -67.38) | 16:00 close · cash $363.57 · equity $10,313.89 vs 09:30 $10,397.63 (-83.74; session marks -67.38) · 8 name(s) marked open→close (per-name table). SMTC×8 09:30 $149.40 → close $142.43 -55.76; TTMI×10 09:30 $127.07 → close $124.73 -23.40; KEYS×4 09:30 $323.82 → close $325.82 +8.00; AVT×14 09:30 $91.11 → close $91.51 +5.60; CGNX×20 09:30 $62.80 → close $62.97 +3.40; COHR×4 09:30 $303.67 → close $295.39 -33.12; LSCC×10 09:30 $121.13 → close $120.47 -6.60; MEI×75 09:30 $17.32 → close $17.78 +34.50 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $363.57 | ▼ 09:30 equity $9,965.74 vs yday $10,313.89 (-348.15) | 09:30 open · cash $363.57 (unchanged overnight, no fees) · equity $9,965.74 vs prior close $10,313.89 (-348.15) · 8 name(s) re-marked at the open (per-name table). SMTC×8 yday $142.43 → 09:30 $133.04 -75.12; TTMI×10 yday $124.73 → 09:30 $117.20 -75.30; KEYS×4 yday $325.82 → 09:30 $324.14 -6.72; AVT×14 yday $91.51 → 09:30 $88.63 -40.32; CGNX×20 yday $62.97 → 09:30 $60.31 -53.20; COHR×4 yday $295.39 → 09:30 $274.13 -85.04; LSCC×10 yday $120.47 → 09:30 $116.00 -44.70; MEI×75 yday $17.78 → 09:30 $18.21 +32.25 | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 8 | $133.04 | $2.03 | $-134.93 | $1,425.86 | ▼ -134.93 after sell → book $9,963.71; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TTMI` | 10 | $117.20 | $2.04 | $-102.76 | $2,595.82 | ▼ -102.76 after sell → book $9,961.67; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 4 | $324.14 | $2.02 | $-2.74 | $3,890.35 | ▼ -2.74 after sell → book $9,959.64; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `AVT` | 14 | $88.63 | $2.05 | $-38.80 | $5,129.12 | ▼ -38.80 after sell → book $9,957.59; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CGNX` | 20 | $60.31 | $2.07 | $-53.92 | $6,333.25 | ▼ -53.92 after sell → book $9,955.52; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `COHR` | 4 | $274.13 | $2.02 | $-122.18 | $7,427.75 | ▼ -122.18 after sell → book $9,953.50; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `LSCC` | 10 | $116.00 | $2.04 | $-55.36 | $8,585.71 | ▼ -55.36 after sell → book $9,951.46; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MEI` | 75 | $18.21 | $2.24 | $+62.30 | $9,949.22 | ▲ +62.30 after sell → book $9,949.22; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,949.22 | ▲ close $9,949.22 vs 09:30 $9,965.74 (session +0.00) | 16:00 close · cash $9,949.22 · no lots left · equity $9,949.22. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,949.22 | ▲ 09:30 equity $9,949.22 vs yday $9,949.22 (+0.00) | 09:30 open · cash $9,949.22 · no holdings · equity $9,949.22 vs prior close $9,949.22 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,949.22 | ▲ close $9,949.22 vs 09:30 $9,949.22 (session +0.00) | 16:00 close · cash $9,949.22 · no lots left · equity $9,949.22. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,949.22 | ▲ 09:30 equity $9,949.22 vs yday $9,949.22 (+0.00) | 09:30 open · cash $9,949.22 · no holdings · equity $9,949.22 vs prior close $9,949.22 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,949.22 | ▲ close $9,949.22 vs 09:30 $9,949.22 (session +0.00) | 16:00 close · cash $9,949.22 · no lots left · equity $9,949.22. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,949.22 | ▲ 09:30 equity $9,949.22 vs yday $9,949.22 (+0.00) | 09:30 open · cash $9,949.22 · no holdings · equity $9,949.22 vs prior close $9,949.22 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 30 | $41.31 | $2.08 | — | $8,707.84 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $1243.65 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 161 | $7.70 | $2.47 | — | $7,465.67 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $1243.65 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $125.94 | $2.02 | — | $6,330.19 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1243.65 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 54 | $22.78 | $2.15 | — | $5,097.92 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1243.65 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 575 | $2.16 | $7.42 | — | $3,848.50 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.3; leftover $1243.65 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `BMEA` | 690 | $1.80 | $8.90 | — | $2,597.60 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $1243.65 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VIR` | 106 | $11.63 | $2.31 | — | $1,362.51 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_gainer; 🔵; ⚪; ret5=+5.8; leftover $1243.65 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `NEOV` | 339 | $3.66 | $4.37 | — | $117.40 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_mover; 🔵; ⚪; ret5=-8.0; leftover $1243.65 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟡 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $117.40 | ▲ close $10,235.79 vs 09:30 $9,949.22 (session +318.29) | 16:00 close · cash $117.40 · equity $10,235.79 vs 09:30 $9,949.22 (+286.57; session marks +318.29) · 8 name(s) marked open→close (per-name table). HRMY×30 09:30 $41.31 → close $42.86 +46.50; VSTM×161 09:30 $7.70 → close $8.02 +51.52; RVTY×9 09:30 $125.94 → close $130.94 +45.00; MMED×54 09:30 $22.78 → close $23.76 +52.92; CRDL×575 09:30 $2.16 → close $2.17 +5.75; BMEA×690 09:30 $1.80 → close $1.93 +89.70; VIR×106 09:30 $11.63 → close $11.50 -13.78; NEOV×339 09:30 $3.66 → close $3.78 +40.68 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $117.40 | ▲ 09:30 equity $10,266.17 vs yday $10,235.79 (+30.38) | 09:30 open · cash $117.40 (unchanged overnight, no fees) · equity $10,266.17 vs prior close $10,235.79 (+30.38) · 8 name(s) re-marked at the open (per-name table). HRMY×30 yday $42.86 → 09:30 $42.93 +2.10; VSTM×161 yday $8.02 → 09:30 $8.03 +1.61; RVTY×9 yday $130.94 → 09:30 $132.45 +13.59; MMED×54 yday $23.76 → 09:30 $23.88 +6.48; CRDL×575 yday $2.17 → 09:30 $2.18 +5.75; BMEA×690 yday $1.93 → 09:30 $1.93 +0.00; VIR×106 yday $11.50 → 09:30 $11.54 +4.24; NEOV×339 yday $3.78 → 09:30 $3.77 -3.39 | — |
| 2026-09-04 09:30 ET | **SELL** | `HRMY` | 30 | $42.93 | $2.10 | $+44.42 | $1,403.20 | ▲ +44.42 after sell → book $10,264.07; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 161 | $8.03 | $2.51 | $+48.15 | $2,693.52 | ▲ +48.15 after sell → book $10,261.56; vs 09:30 mark -2.51 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 9 | $132.45 | $2.04 | $+54.54 | $3,883.53 | ▲ +54.54 after sell → book $10,259.52; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 54 | $23.88 | $2.17 | $+55.08 | $5,170.88 | ▲ +55.08 after sell → book $10,257.35; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRDL` | 575 | $2.18 | $7.52 | $-3.44 | $6,416.86 | ▼ -3.44 after sell → book $10,249.83; vs 09:30 mark -7.52 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `BMEA` | 690 | $1.93 | $9.03 | $+71.77 | $7,739.53 | ▲ +71.77 after sell → book $10,240.80; vs 09:30 mark -9.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `VIR` | 106 | $11.54 | $2.34 | $-14.18 | $8,960.44 | ▼ -14.18 after sell → book $10,238.47; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `NEOV` | 339 | $3.77 | $4.44 | $+28.48 | $10,234.03 | ▲ +28.48 after sell → book $10,234.03; vs 09:30 mark -4.44 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `BVS` | 117 | $14.50 | $2.34 | — | $8,535.19 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+0.8; leftover $1705.67 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 3 | $486.31 | $2.00 | — | $7,074.26 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-9.9; leftover $1705.67 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MLYS` | 58 | $29.15 | $2.16 | — | $5,381.39 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.0; leftover $1705.67 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `TARS` | 20 | $82.76 | $2.05 | — | $3,724.14 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_gainer; 🔵; ⚪; ret5=+5.1; leftover $1705.67 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `LENZ` | 289 | $5.90 | $3.73 | — | $2,015.31 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_gainer; 🔵; ⚪; ret5=-1.1; leftover $1705.67 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `INO` | 1245 | $1.37 | $16.06 | — | $293.60 | — | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ⚪; ret5=+8.3; leftover $1705.67 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $293.60 | ▼ close $10,204.82 vs 09:30 $10,266.17 (session -0.86) | 16:00 close · cash $293.60 · equity $10,204.82 vs 09:30 $10,266.17 (-61.35; session marks -0.86) · 6 name(s) marked open→close (per-name table). BVS×117 09:30 $14.50 → close $14.36 -16.38; DELL×3 09:30 $486.31 → close $516.39 +90.24; MLYS×58 09:30 $29.15 → close $28.27 -51.04; TARS×20 09:30 $82.76 → close $83.21 +9.00; LENZ×289 09:30 $5.90 → close $5.83 -20.23; INO×1245 09:30 $1.37 → close $1.36 -12.45 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-26 | `CRMD` | no_price | no 09:30 open — carry |
| 2026-08-26 | `BMEA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ZURA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `EZPW` | no_price | no 09:30 open — carry |
| 2026-08-26 | `BZ` | no_price | no 09:30 open — carry |
| 2026-08-26 | `VIPS` | no_price | no 09:30 open — carry |
| 2026-08-26 | `RHI` | no_price | no 09:30 open — carry |
| 2026-08-26 | `SUZ` | no_price | no 09:30 open — carry |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `BVS` | 117 | 2026-09-04 @ $14.50 | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+0.8; leftover $1705.67 |
| `DELL` | 3 | 2026-09-04 @ $486.31 | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-9.9; leftover $1705.67 |
| `MLYS` | 58 | 2026-09-04 @ $29.15 | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.0; leftover $1705.67 |
| `TARS` | 20 | 2026-09-04 @ $82.76 | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_gainer; 🔵; ⚪; ret5=+5.1; leftover $1705.67 |
| `LENZ` | 289 | 2026-09-04 @ $5.90 | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list yday_gainer; 🔵; ⚪; ret5=-1.1; leftover $1705.67 |
| `INO` | 1245 | 2026-09-04 @ $1.37 | combo gate; gate zero_red=True,ret_5_max=10.0,rvol_max=2.2; list ohlc_hot; 🔵; ⚪; ret5=+8.3; leftover $1705.67 |
