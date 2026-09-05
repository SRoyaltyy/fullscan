# Factor mine action — `union_h1_rankw`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `rank_w` · sell `list` · S-boost `none` · rank-weighted leftover

Cash book **+13.90%** ($11,391) · signal-only (no cash/fees) was +18.57%. Starts YES **16/17**. Fills 134 · skips 53 · realized $+958.98.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `rank_w` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $121.88.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `BTSG` | 37 | — | $59.80 | +0.00 | $60.23 | +15.91 | +15.91 | +0.00 | +15.91 |
| 2026-08-13 | `IREN` | 42 | — | $45.98 | +0.00 | $44.76 | -51.24 | -51.24 | +0.00 | -51.24 |
| 2026-08-13 | `TPG` | 32 | — | $50.62 | +0.00 | $54.62 | +127.90 | +127.90 | +0.00 | +127.90 |
| 2026-08-13 | `TGTX` | 27 | — | $49.70 | +0.00 | $47.94 | -47.52 | -47.52 | +0.00 | -47.52 |
| 2026-08-13 | `SLS` | 94 | — | $11.70 | +0.00 | $12.36 | +62.04 | +62.04 | +0.00 | +62.04 |
| 2026-08-13 | `HIMS` | 28 | — | $29.74 | +0.00 | $28.77 | -27.16 | -27.16 | +0.00 | -27.16 |
| 2026-08-13 | `INO` | 685 | — | $0.81 | +0.00 | $0.90 | +61.65 | +61.65 | +0.00 | +61.65 |
| 2026-08-13 | `TNDM` | 11 | — | $23.33 | +0.00 | $23.13 | -2.20 | -2.20 | +0.00 | -2.20 |
| 2026-08-14 | `BTSG` | 37 | $60.23 | $59.65 | -21.46 | — | +0.00 | -21.46 | -5.55 | — |
| 2026-08-14 | `IREN` | 42 | $44.76 | $44.09 | -28.14 | — | +0.00 | -28.14 | -79.38 | — |
| 2026-08-14 | `TPG` | 32 | $54.62 | $55.29 | +21.44 | — | +0.00 | +21.44 | +149.34 | — |
| 2026-08-14 | `TGTX` | 27 | $47.94 | $47.27 | -18.09 | — | +0.00 | -18.09 | -65.61 | — |
| 2026-08-14 | `SLS` | 94 | $12.36 | $12.40 | +3.76 | — | +0.00 | +3.76 | +65.80 | — |
| 2026-08-14 | `HIMS` | 28 | $28.77 | $29.15 | +10.64 | — | +0.00 | +10.64 | -16.52 | — |
| 2026-08-14 | `INO` | 685 | $0.90 | $0.93 | +20.55 | — | +0.00 | +20.55 | +82.20 | — |
| 2026-08-14 | `TNDM` | 11 | $23.13 | $22.92 | -2.31 | — | +0.00 | -2.31 | -4.51 | — |
| 2026-08-14 | `TLN` | 6 | — | $359.83 | +0.00 | $362.74 | +17.46 | +17.46 | +0.00 | +17.46 |
| 2026-08-14 | `VST` | 13 | — | $146.90 | +0.00 | $148.13 | +15.99 | +15.99 | +0.00 | +15.99 |
| 2026-08-14 | `NRG` | 13 | — | $120.00 | +0.00 | $126.24 | +81.12 | +81.12 | +0.00 | +81.12 |
| 2026-08-14 | `DAVE` | 4 | — | $330.91 | +0.00 | $334.57 | +14.64 | +14.64 | +0.00 | +14.64 |
| 2026-08-14 | `SLG` | 19 | — | $57.61 | +0.00 | $56.09 | -28.88 | -28.88 | +0.00 | -28.88 |
| 2026-08-14 | `MARA` | 93 | — | $9.01 | +0.00 | $9.20 | +17.67 | +17.67 | +0.00 | +17.67 |
| 2026-08-14 | `LDI` | 597 | — | $0.94 | +0.00 | $0.90 | -23.88 | -23.88 | +0.00 | -23.88 |
| 2026-08-14 | `BTBT` | 186 | — | $1.50 | +0.00 | $1.57 | +13.02 | +13.02 | +0.00 | +13.02 |
| 2026-08-17 | `TLN` | 6 | $362.74 | $367.88 | +30.84 | — | +0.00 | +30.84 | +48.30 | — |
| 2026-08-17 | `VST` | 13 | $148.13 | $149.37 | +16.12 | — | +0.00 | +16.12 | +32.11 | — |
| 2026-08-17 | `NRG` | 13 | $126.24 | $127.40 | +15.08 | — | +0.00 | +15.08 | +96.20 | — |
| 2026-08-17 | `DAVE` | 4 | $334.57 | $336.94 | +9.48 | — | +0.00 | +9.48 | +24.12 | — |
| 2026-08-17 | `SLG` | 19 | $56.09 | $55.37 | -13.68 | — | +0.00 | -13.68 | -42.56 | — |
| 2026-08-17 | `MARA` | 93 | $9.20 | $9.22 | +1.86 | — | +0.00 | +1.86 | +19.53 | — |
| 2026-08-17 | `LDI` | 597 | $0.90 | $0.91 | +5.97 | — | +0.00 | +5.97 | -17.91 | — |
| 2026-08-17 | `BTBT` | 186 | $1.57 | $1.52 | -9.30 | — | +0.00 | -9.30 | +3.72 | — |
| 2026-08-17 | `DVN` | 49 | — | $46.18 | +0.00 | $47.57 | +68.11 | +68.11 | +0.00 | +68.11 |
| 2026-08-17 | `EOG` | 13 | — | $142.77 | +0.00 | $146.15 | +43.94 | +43.94 | +0.00 | +43.94 |
| 2026-08-17 | `FANG` | 8 | — | $202.70 | +0.00 | $206.29 | +28.72 | +28.72 | +0.00 | +28.72 |
| 2026-08-17 | `TMC` | 349 | — | $4.05 | +0.00 | $3.77 | -97.72 | -97.72 | +0.00 | -97.72 |
| 2026-08-17 | `TGB` | 133 | — | $8.46 | +0.00 | $8.77 | +41.23 | +41.23 | +0.00 | +41.23 |
| 2026-08-17 | `ELF` | 9 | — | $90.54 | +0.00 | $93.66 | +28.08 | +28.08 | +0.00 | +28.08 |
| 2026-08-17 | `DNN` | 174 | — | $3.24 | +0.00 | $3.19 | -8.70 | -8.70 | +0.00 | -8.70 |
| 2026-08-17 | `HNST` | 58 | — | $4.81 | +0.00 | $4.70 | -6.38 | -6.38 | +0.00 | -6.38 |
| 2026-08-18 | `DVN` | 49 | $47.57 | $48.00 | +21.07 | — | +0.00 | +21.07 | +89.18 | — |
| 2026-08-18 | `EOG` | 13 | $146.15 | $148.04 | +24.57 | — | +0.00 | +24.57 | +68.51 | — |
| 2026-08-18 | `FANG` | 8 | $206.29 | $208.93 | +21.12 | — | +0.00 | +21.12 | +49.84 | — |
| 2026-08-18 | `TMC` | 349 | $3.77 | $3.72 | -17.45 | — | +0.00 | -17.45 | -115.17 | — |
| 2026-08-18 | `TGB` | 133 | $8.77 | $8.55 | -29.26 | — | +0.00 | -29.26 | +11.97 | — |
| 2026-08-18 | `ELF` | 9 | $93.66 | $93.44 | -1.98 | — | +0.00 | -1.98 | +26.10 | — |
| 2026-08-18 | `DNN` | 174 | $3.19 | $3.11 | -13.92 | — | +0.00 | -13.92 | -22.62 | — |
| 2026-08-18 | `HNST` | 58 | $4.70 | $4.67 | -1.74 | — | +0.00 | -1.74 | -8.12 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `AG` | 110 | — | $20.55 | +0.00 | $21.19 | +70.40 | +70.40 | +0.00 | +70.40 |
| 2026-08-20 | `BHP` | 21 | — | $91.01 | +0.00 | $93.63 | +55.02 | +55.02 | +0.00 | +55.02 |
| 2026-08-20 | `CDE` | 82 | — | $20.65 | +0.00 | $21.11 | +37.72 | +37.72 | +0.00 | +37.72 |
| 2026-08-20 | `HDSN` | 246 | — | $5.77 | +0.00 | $5.57 | -49.20 | -49.20 | +0.00 | -49.20 |
| 2026-08-20 | `IAG` | 58 | — | $19.63 | +0.00 | $20.50 | +50.46 | +50.46 | +0.00 | +50.46 |
| 2026-08-20 | `KGC` | 28 | — | $29.63 | +0.00 | $31.43 | +50.40 | +50.40 | +0.00 | +50.40 |
| 2026-08-20 | `NFGC` | 325 | — | $1.75 | +0.00 | $1.75 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `WPM` | 1 | — | $144.54 | +0.00 | $150.25 | +5.71 | +5.71 | +0.00 | +5.71 |
| 2026-08-21 | `AG` | 110 | $21.19 | $21.90 | +78.10 | — | +0.00 | +78.10 | +148.50 | — |
| 2026-08-21 | `BHP` | 21 | $93.63 | $95.72 | +43.89 | — | +0.00 | +43.89 | +98.91 | — |
| 2026-08-21 | `CDE` | 82 | $21.11 | $21.75 | +52.48 | — | +0.00 | +52.48 | +90.20 | — |
| 2026-08-21 | `HDSN` | 246 | $5.57 | $5.67 | +24.60 | — | +0.00 | +24.60 | -24.60 | — |
| 2026-08-21 | `IAG` | 58 | $20.50 | $21.17 | +38.86 | — | +0.00 | +38.86 | +89.32 | — |
| 2026-08-21 | `KGC` | 28 | $31.43 | $32.17 | +20.72 | — | +0.00 | +20.72 | +71.12 | — |
| 2026-08-21 | `NFGC` | 325 | $1.75 | $1.79 | +13.00 | — | +0.00 | +13.00 | +13.00 | — |
| 2026-08-21 | `WPM` | 1 | $150.25 | $154.70 | +4.45 | — | +0.00 | +4.45 | +10.16 | — |
| 2026-08-21 | `AU` | 19 | — | $119.43 | +0.00 | $121.22 | +34.01 | +34.01 | +0.00 | +34.01 |
| 2026-08-21 | `AUPH` | 121 | — | $17.20 | +0.00 | $16.65 | -66.55 | -66.55 | +0.00 | -66.55 |
| 2026-08-21 | `AEM` | 8 | — | $216.30 | +0.00 | $216.06 | -1.92 | -1.92 | +0.00 | -1.92 |
| 2026-08-21 | `ARCT` | 133 | — | $11.13 | +0.00 | $13.45 | +308.56 | +308.56 | +0.00 | +308.56 |
| 2026-08-21 | `AUTL` | 482 | — | $2.47 | +0.00 | $2.41 | -28.92 | -28.92 | +0.00 | -28.92 |
| 2026-08-21 | `CRDL` | 462 | — | $1.93 | +0.00 | $1.86 | -32.34 | -32.34 | +0.00 | -32.34 |
| 2026-08-21 | `CRSP` | 9 | — | $59.72 | +0.00 | $59.50 | -1.98 | -1.98 | +0.00 | -1.98 |
| 2026-08-21 | `CYPH` | 225 | — | $1.32 | +0.00 | $1.42 | +22.50 | +22.50 | +0.00 | +22.50 |
| 2026-08-24 | `AU` | 19 | $121.22 | $120.50 | -13.68 | — | +0.00 | -13.68 | +20.33 | — |
| 2026-08-24 | `AUPH` | 121 | $16.65 | $16.60 | -6.05 | — | +0.00 | -6.05 | -72.60 | — |
| 2026-08-24 | `AEM` | 8 | $216.06 | $217.03 | +7.76 | — | +0.00 | +7.76 | +5.84 | — |
| 2026-08-24 | `ARCT` | 133 | $13.45 | $13.26 | -25.27 | — | +0.00 | -25.27 | +283.29 | — |
| 2026-08-24 | `AUTL` | 482 | $2.41 | $2.36 | -24.10 | — | +0.00 | -24.10 | -53.02 | — |
| 2026-08-24 | `CRDL` | 462 | $1.86 | $1.87 | +4.62 | — | +0.00 | +4.62 | -27.72 | — |
| 2026-08-24 | `CRSP` | 9 | $59.50 | $58.79 | -6.39 | — | +0.00 | -6.39 | -8.37 | — |
| 2026-08-24 | `CYPH` | 225 | $1.42 | $1.83 | +92.25 | — | +0.00 | +92.25 | +114.75 | — |
| 2026-08-25 | `MOS` | 101 | — | $24.00 | +0.00 | $23.75 | -25.25 | -25.25 | +0.00 | -25.25 |
| 2026-08-25 | `OCUL` | 194 | — | $10.92 | +0.00 | $10.92 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-25 | `INSP` | 29 | — | $61.47 | +0.00 | $61.47 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-25 | `CRMD` | 183 | — | $8.28 | +0.00 | $8.28 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-25 | `RZLT` | 232 | — | $5.23 | +0.00 | $5.29 | +13.92 | +13.92 | +0.00 | +13.92 |
| 2026-08-25 | `HCA` | 2 | — | $429.24 | +0.00 | $428.50 | -1.48 | -1.48 | +0.00 | -1.48 |
| 2026-08-25 | `BMEA` | 374 | — | $1.62 | +0.00 | $1.61 | -3.74 | -3.74 | +0.00 | -3.74 |
| 2026-08-25 | `NPWR` | 151 | — | $2.00 | +0.00 | $2.02 | +3.02 | +3.02 | +0.00 | +3.02 |
| 2026-08-26 | `MOS` | 101 | $23.75 | $23.75 | +0.00 | $23.75 | +0.00 | +0.00 | -25.25 | -25.25 |
| 2026-08-26 | `OCUL` | 194 | $10.92 | $10.92 | +0.00 | $10.92 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-26 | `INSP` | 29 | $61.47 | $61.47 | +0.00 | $61.47 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-26 | `CRMD` | 183 | $8.28 | $8.28 | +0.00 | $8.28 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-26 | `RZLT` | 232 | $5.29 | $5.29 | +0.00 | $5.29 | +0.00 | +0.00 | +13.92 | +13.92 |
| 2026-08-26 | `HCA` | 2 | $428.50 | $428.50 | +0.00 | $428.50 | +0.00 | +0.00 | -1.48 | -1.48 |
| 2026-08-26 | `BMEA` | 374 | $1.61 | $1.61 | +0.00 | $1.61 | +0.00 | +0.00 | -3.74 | -3.74 |
| 2026-08-26 | `NPWR` | 151 | $2.02 | $2.02 | +0.00 | $2.02 | +0.00 | +0.00 | +3.02 | +3.02 |
| 2026-08-27 | `MOS` | 101 | $23.75 | $24.84 | +110.09 | $24.16 | -68.68 | +41.41 | +84.84 | +16.16 |
| 2026-08-27 | `OCUL` | 194 | $10.92 | $10.79 | -25.22 | — | +0.00 | -25.22 | -25.22 | — |
| 2026-08-27 | `INSP` | 29 | $61.47 | $60.07 | -40.60 | — | +0.00 | -40.60 | -40.60 | — |
| 2026-08-27 | `CRMD` | 183 | $8.28 | $8.60 | +58.56 | — | +0.00 | +58.56 | +58.56 | — |
| 2026-08-27 | `RZLT` | 232 | $5.29 | $5.01 | -64.96 | — | +0.00 | -64.96 | -51.04 | — |
| 2026-08-27 | `HCA` | 2 | $428.50 | $427.50 | -2.00 | — | +0.00 | -2.00 | -3.48 | — |
| 2026-08-27 | `BMEA` | 374 | $1.61 | $1.75 | +52.36 | — | +0.00 | +52.36 | +48.62 | — |
| 2026-08-27 | `NPWR` | 151 | $2.02 | $1.93 | -13.59 | — | +0.00 | -13.59 | -10.57 | — |
| 2026-08-27 | `RRC` | 51 | — | $40.72 | +0.00 | $41.55 | +42.33 | +42.33 | +0.00 | +42.33 |
| 2026-08-27 | `CRK` | 128 | — | $14.09 | +0.00 | $14.50 | +52.48 | +52.48 | +0.00 | +52.48 |
| 2026-08-27 | `SLI` | 581 | — | $2.59 | +0.00 | $2.61 | +11.62 | +11.62 | +0.00 | +11.62 |
| 2026-08-27 | `ACMR` | 14 | — | $80.97 | +0.00 | $79.11 | -26.04 | -26.04 | +0.00 | -26.04 |
| 2026-08-27 | `GGB` | 204 | — | $4.42 | +0.00 | $4.46 | +8.16 | +8.16 | +0.00 | +8.16 |
| 2026-08-27 | `MT` | 8 | — | $75.12 | +0.00 | $74.53 | -4.72 | -4.72 | +0.00 | -4.72 |
| 2026-08-28 | `MOS` | 101 | $24.16 | $24.00 | -16.16 | $23.76 | -24.24 | -40.40 | +0.00 | -24.24 |
| 2026-08-28 | `RRC` | 51 | $41.55 | $41.44 | -5.61 | $41.64 | +10.20 | +4.59 | +36.72 | +46.92 |
| 2026-08-28 | `CRK` | 128 | $14.50 | $14.42 | -10.24 | $14.62 | +25.60 | +15.36 | +42.24 | +67.84 |
| 2026-08-28 | `SLI` | 581 | $2.61 | $2.60 | -5.81 | $2.64 | +23.24 | +17.43 | +5.81 | +29.05 |
| 2026-08-28 | `ACMR` | 14 | $79.11 | $81.65 | +35.56 | — | +0.00 | +35.56 | +9.52 | — |
| 2026-08-28 | `GGB` | 204 | $4.46 | $4.57 | +22.44 | — | +0.00 | +22.44 | +30.60 | — |
| 2026-08-28 | `MT` | 8 | $74.53 | $74.54 | +0.08 | — | +0.00 | +0.08 | -4.64 | — |
| 2026-08-28 | `ANF` | 8 | — | $144.70 | +0.00 | $145.75 | +8.40 | +8.40 | +0.00 | +8.40 |
| 2026-08-28 | `BHVN` | 54 | — | $16.95 | +0.00 | $16.12 | -44.82 | -44.82 | +0.00 | -44.82 |
| 2026-08-28 | `BZ` | 33 | — | $18.50 | +0.00 | $18.00 | -16.50 | -16.50 | +0.00 | -16.50 |
| 2026-08-28 | `CAPR` | 33 | — | $9.19 | +0.00 | $10.06 | +28.71 | +28.71 | +0.00 | +28.71 |
| 2026-08-31 | `MOS` | 101 | $23.76 | $23.75 | -1.01 | — | +0.00 | -1.01 | -25.25 | — |
| 2026-08-31 | `RRC` | 51 | $41.64 | $41.11 | -27.03 | — | +0.00 | -27.03 | +19.89 | — |
| 2026-08-31 | `CRK` | 128 | $14.62 | $14.56 | -7.68 | — | +0.00 | -7.68 | +60.16 | — |
| 2026-08-31 | `SLI` | 581 | $2.64 | $2.51 | -75.53 | — | +0.00 | -75.53 | -46.48 | — |
| 2026-08-31 | `ANF` | 8 | $145.75 | $148.67 | +23.36 | — | +0.00 | +23.36 | +31.76 | — |
| 2026-08-31 | `BHVN` | 54 | $16.12 | $15.44 | -36.72 | — | +0.00 | -36.72 | -81.54 | — |
| 2026-08-31 | `BZ` | 33 | $18.00 | $17.89 | -3.63 | — | +0.00 | -3.63 | -20.13 | — |
| 2026-08-31 | `CAPR` | 33 | $10.06 | $9.44 | -20.46 | — | +0.00 | -20.46 | +8.25 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `ATRC` | 48 | — | $49.76 | +0.00 | $52.59 | +135.84 | +135.84 | +0.00 | +135.84 |
| 2026-09-03 | `HRMY` | 50 | — | $41.31 | +0.00 | $42.86 | +77.50 | +77.50 | +0.00 | +77.50 |
| 2026-09-03 | `CABA` | 549 | — | $3.27 | +0.00 | $3.57 | +164.70 | +164.70 | +0.00 | +164.70 |
| 2026-09-03 | `VSTM` | 194 | — | $7.70 | +0.00 | $8.02 | +62.08 | +62.08 | +0.00 | +62.08 |
| 2026-09-03 | `RVTY` | 9 | — | $125.94 | +0.00 | $130.94 | +45.00 | +45.00 | +0.00 | +45.00 |
| 2026-09-03 | `GPRO` | 736 | — | $1.22 | +0.00 | $1.69 | +345.92 | +345.92 | +0.00 | +345.92 |
| 2026-09-03 | `FRVO` | 32 | — | $18.40 | +0.00 | $17.98 | -13.44 | -13.44 | +0.00 | -13.44 |
| 2026-09-03 | `CRK` | 19 | — | $15.70 | +0.00 | $15.54 | -3.04 | -3.04 | +0.00 | -3.04 |
| 2026-09-04 | `ATRC` | 48 | $52.59 | $52.88 | +13.92 | $52.46 | -20.16 | -6.24 | +149.76 | +129.60 |
| 2026-09-04 | `HRMY` | 50 | $42.86 | $42.93 | +3.50 | — | +0.00 | +3.50 | +81.00 | — |
| 2026-09-04 | `CABA` | 549 | $3.57 | $3.63 | +32.94 | $3.48 | -82.35 | -49.41 | +197.64 | +115.29 |
| 2026-09-04 | `VSTM` | 194 | $8.02 | $8.03 | +1.94 | — | +0.00 | +1.94 | +64.02 | — |
| 2026-09-04 | `RVTY` | 9 | $130.94 | $132.45 | +13.59 | — | +0.00 | +13.59 | +58.59 | — |
| 2026-09-04 | `GPRO` | 736 | $1.69 | $1.78 | +66.24 | $1.39 | -287.04 | -220.80 | +412.16 | +125.12 |
| 2026-09-04 | `FRVO` | 32 | $17.98 | $18.27 | +9.28 | — | +0.00 | +9.28 | -4.16 | — |
| 2026-09-04 | `CRK` | 19 | $15.54 | $15.45 | -1.71 | — | +0.00 | -1.71 | -4.75 | — |
| 2026-09-04 | `ASND` | 7 | — | $266.94 | +0.00 | $271.12 | +29.26 | +29.26 | +0.00 | +29.26 |
| 2026-09-04 | `OSCR` | 50 | — | $30.65 | +0.00 | $32.24 | +79.50 | +79.50 | +0.00 | +79.50 |
| 2026-09-04 | `NVAX` | 112 | — | $10.41 | +0.00 | $10.34 | -7.84 | -7.84 | +0.00 | -7.84 |
| 2026-09-04 | `BVS` | 53 | — | $14.50 | +0.00 | $14.36 | -7.42 | -7.42 | +0.00 | -7.42 |
| 2026-09-04 | `BAK` | 200 | — | $1.95 | +0.00 | $1.94 | -2.00 | -2.00 | +0.00 | -2.00 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +139.38 | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | — | $128.05 | $10,117.03 | BTSG×37, IREN×42, TPG×32, TGTX×27, SLS×94, HIMS×28, INO×685, TNDM×11 |
| 2026-08-14 | +5.50 | $128.05 | BTSG×37, IREN×42, TPG×32, TGTX×27, SLS×94, HIMS×28, INO×685, TNDM×11 | $10,103.42 | -13.61 | +107.14 | TLN, VST, NRG, DAVE, SLG, MARA, LDI, BTBT | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | $334.42 | $10,164.79 | TLN×6, VST×13, NRG×13, DAVE×4, SLG×19, MARA×93, LDI×597, BTBT×186 |
| 2026-08-17 | +2.25 | $334.42 | TLN×6, VST×13, NRG×13, DAVE×4, SLG×19, MARA×93, LDI×597, BTBT×186 | $10,221.16 | +56.37 | +97.28 | DVN, EOG, FANG, TMC, TGB, ELF, DNN, HNST | TLN, VST, NRG, DAVE, SLG, MARA, LDI, BTBT | $242.31 | $10,276.25 | DVN×49, EOG×13, FANG×8, TMC×349, TGB×133, ELF×9, DNN×174, HNST×58 |
| 2026-08-18 | -6.20 | $242.31 | DVN×49, EOG×13, FANG×8, TMC×349, TGB×133, ELF×9, DNN×174, HNST×58 | $10,278.66 | +2.41 | +0.00 | — | DVN, EOG, FANG, TMC, TGB, ELF, DNN, HNST | $10,258.63 | $10,258.63 | — |
| 2026-08-19 | -7.20 | $10,258.63 | — | $10,258.63 | +0.00 | +0.00 | — | — | $10,258.63 | $10,258.63 | — |
| 2026-08-20 | +1.12 | $10,258.63 | — | $10,258.63 | +0.00 | +220.51 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | — | $273.07 | $10,459.48 | AG×110, BHP×21, CDE×82, HDSN×246, IAG×58, KGC×28, NFGC×325, WPM×1 |
| 2026-08-21 | +3.25 | $273.07 | AG×110, BHP×21, CDE×82, HDSN×246, IAG×58, KGC×28, NFGC×325, WPM×1 | $10,735.58 | +276.10 | +233.36 | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $211.91 | $10,923.01 | AU×19, AUPH×121, AEM×8, ARCT×133, AUTL×482, CRDL×462, CRSP×9, CYPH×225 |
| 2026-08-24 | -5.17 | $211.91 | AU×19, AUPH×121, AEM×8, ARCT×133, AUTL×482, CRDL×462, CRSP×9, CYPH×225 | $10,952.15 | +29.14 | +0.00 | — | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | $10,925.88 | $10,925.88 | — |
| 2026-08-25 | +1.80 | $10,925.88 | — | $10,925.88 | +0.00 | -13.53 | MOS, OCUL, INSP, CRMD, RZLT, HCA, BMEA, NPWR | — | $84.08 | $10,890.62 | MOS×101, OCUL×194, INSP×29, CRMD×183, RZLT×232, HCA×2, BMEA×374, NPWR×151 |
| 2026-08-26 | +2.02 | $84.08 | MOS×101, OCUL×194, INSP×29, CRMD×183, RZLT×232, HCA×2, BMEA×374, NPWR×151 | $10,890.62 | -0.00 | +0.00 | — | — | $84.08 | $10,890.62 | MOS×101, OCUL×194, INSP×29, CRMD×183, RZLT×232, HCA×2, BMEA×374, NPWR×151 |
| 2026-08-27 | — | $84.08 | MOS×101, OCUL×194, INSP×29, CRMD×183, RZLT×232, HCA×2, BMEA×374, NPWR×151 | $10,965.26 | +74.64 | +15.15 | RRC, CRK, SLI, ACMR, GGB, MT | OCUL, INSP, CRMD, RZLT, HCA, BMEA, NPWR | $396.74 | $10,941.98 | MOS×101, RRC×51, CRK×128, SLI×581, ACMR×14, GGB×204, MT×8 |
| 2026-08-28 | +0.75 | $396.74 | MOS×101, RRC×51, CRK×128, SLI×581, ACMR×14, GGB×204, MT×8 | $10,962.24 | +20.26 | +10.59 | ANF, BHVN, BZ, CAPR | ACMR, GGB, MT | $66.67 | $10,957.73 | MOS×101, RRC×51, CRK×128, SLI×581, ANF×8, BHVN×54, BZ×33, CAPR×33 |
| 2026-08-31 | -5.85 | $66.67 | MOS×101, RRC×51, CRK×128, SLI×581, ANF×8, BHVN×54, BZ×33, CAPR×33 | $10,809.03 | -148.70 | +0.00 | — | MOS, RRC, CRK, SLI, ANF, BHVN, BZ, CAPR | $10,786.09 | $10,786.09 | — |
| 2026-09-01 | -6.30 | $10,786.09 | — | $10,786.09 | -0.00 | +0.00 | — | — | $10,786.09 | $10,786.09 | — |
| 2026-09-02 | -3.83 | $10,786.09 | — | $10,786.09 | -0.00 | +0.00 | — | — | $10,786.09 | $10,786.09 | — |
| 2026-09-03 | -0.90 | $10,786.09 | — | $10,786.09 | -0.00 | +814.56 | ATRC, HRMY, CABA, VSTM, RVTY, GPRO, FRVO, CRK | — | $95.03 | $11,571.08 | ATRC×48, HRMY×50, CABA×549, VSTM×194, RVTY×9, GPRO×736, FRVO×32, CRK×19 |
| 2026-09-04 | — | $95.03 | ATRC×48, HRMY×50, CABA×549, VSTM×194, RVTY×9, GPRO×736, FRVO×32, CRK×19 | $11,710.78 | +139.70 | -298.05 | ASND, OSCR, NVAX, BVS, BAK | HRMY, VSTM, RVTY, FRVO, CRK | $121.88 | $11,390.52 | ATRC×48, CABA×549, GPRO×736, ASND×7, OSCR×50, NVAX×112, BVS×53, BAK×200 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 37 | $59.80 | $2.10 | — | $7,785.30 | — | rank-weighted leftover; list flatten; ⚪; ret5=-5.3; leftover $2222.22 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 42 | $45.98 | $2.12 | — | $5,852.02 | — | rank-weighted leftover; list flatten; ⚪; ret5=+12.3; leftover $1944.44 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 32 | $50.62 | $2.09 | — | $4,229.99 | — | rank-weighted leftover; list flatten; ⚪; ret5=+6.2; leftover $1666.67 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 27 | $49.70 | $2.07 | — | $2,886.02 | — | rank-weighted leftover; list flatten; ⚪; ret5=-0.8; leftover $1388.89 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 94 | $11.70 | $2.27 | — | $1,783.95 | — | rank-weighted leftover; list flatten; ⚪; ret5=-0.8; leftover $1111.11 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 28 | $29.74 | $2.07 | — | $949.16 | — | rank-weighted leftover; list flatten; ⚪; ret5=-5.3; leftover $833.33 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 685 | $0.81 | $7.60 | — | $386.70 | — | rank-weighted leftover; list flatten; ⚪; ret5=+13.2; leftover $555.56 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 11 | $23.33 | $2.02 | — | $128.05 | — | rank-weighted leftover; list flatten; ⚪; ret5=+19.7; leftover $277.78 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $128.05 | ▲ close $10,117.03 vs 09:30 $10,000.00 (session +139.38) | 16:00 close · cash $128.05 · equity $10,117.03 vs 09:30 $10,000.00 (+117.03; session marks +139.38) · 8 name(s) marked open→close (per-name table). BTSG×37 09:30 $59.80 → close $60.23 +15.91; IREN×42 09:30 $45.98 → close $44.76 -51.24; TPG×32 09:30 $50.62 → close $54.62 +127.90; TGTX×27 09:30 $49.70 → close $47.94 -47.52; SLS×94 09:30 $11.70 → close $12.36 +62.04; HIMS×28 09:30 $29.74 → close $28.77 -27.16; INO×685 09:30 $0.81 → close $0.90 +61.65; TNDM×11 09:30 $23.33 → close $23.13 -2.20 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $128.05 | ▼ 09:30 equity $10,103.42 vs yday $10,117.03 (-13.61) | 09:30 open · cash $128.05 (unchanged overnight, no fees) · equity $10,103.42 vs prior close $10,117.03 (-13.61) · 8 name(s) re-marked at the open (per-name table). BTSG×37 yday $60.23 → 09:30 $59.65 -21.46; IREN×42 yday $44.76 → 09:30 $44.09 -28.14; TPG×32 yday $54.62 → 09:30 $55.29 +21.44; TGTX×27 yday $47.94 → 09:30 $47.27 -18.09; SLS×94 yday $12.36 → 09:30 $12.40 +3.76; HIMS×28 yday $28.77 → 09:30 $29.15 +10.64; INO×685 yday $0.90 → 09:30 $0.93 +20.55; TNDM×11 yday $23.13 → 09:30 $22.92 -2.31 | — |
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 37 | $59.65 | $2.13 | $-9.78 | $2,332.97 | ▼ -9.78 after sell → book $10,101.29; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 42 | $44.09 | $2.14 | $-83.64 | $4,182.61 | ▼ -83.64 after sell → book $10,099.15; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 32 | $55.29 | $2.11 | $+145.14 | $5,949.78 | ▲ +145.14 after sell → book $10,097.04; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SELL** | `TGTX` | 27 | $47.27 | $2.09 | $-69.77 | $7,223.98 | ▼ -69.77 after sell → book $10,094.95; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `SLS` | 94 | $12.40 | $2.30 | $+61.23 | $8,387.28 | ▲ +61.23 after sell → book $10,092.65; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `HIMS` | 28 | $29.15 | $2.09 | $-20.69 | $9,201.39 | ▼ -20.69 after sell → book $10,090.56; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 685 | $0.93 | $8.55 | $+66.05 | $9,829.89 | ▲ +66.05 after sell → book $10,082.01; vs 09:30 mark -8.55 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 11 | $22.92 | $2.04 | $-8.58 | $10,079.97 | ▼ -8.58 after sell → book $10,079.97; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 6 | $359.83 | $2.01 | — | $7,918.98 | — | rank-weighted leftover; list flatten; 🔵; ret5=+5.9; leftover $2239.99 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 13 | $146.90 | $2.03 | — | $6,007.25 | — | rank-weighted leftover; list flatten; 🔵; ret5=+3.6; leftover $1959.99 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 13 | $120.00 | $2.03 | — | $4,445.22 | — | rank-weighted leftover; list flatten; 🔵; ret5=+0.6; leftover $1679.99 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `DAVE` | 4 | $330.91 | $2.00 | — | $3,119.58 | — | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=-8.6; leftover $1400.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 19 | $57.61 | $2.05 | — | $2,022.94 | — | rank-weighted leftover; list flatten; 🔵; ret5=+5.7; leftover $1120.00 | join🟢 sector🟢 gen🟢 news🟡 judge🔴 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 93 | $9.01 | $2.27 | — | $1,182.74 | — | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=-13.5; leftover $840.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 597 | $0.94 | $7.38 | — | $615.97 | — | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+0.5; leftover $560.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 186 | $1.50 | $2.55 | — | $334.42 | — | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+9.2; leftover $280.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $334.42 | ▲ close $10,164.79 vs 09:30 $10,103.42 (session +107.14) | 16:00 close · cash $334.42 · equity $10,164.79 vs 09:30 $10,103.42 (+61.37; session marks +107.14) · 8 name(s) marked open→close (per-name table). TLN×6 09:30 $359.83 → close $362.74 +17.46; VST×13 09:30 $146.90 → close $148.13 +15.99; NRG×13 09:30 $120.00 → close $126.24 +81.12; DAVE×4 09:30 $330.91 → close $334.57 +14.64; SLG×19 09:30 $57.61 → close $56.09 -28.88; MARA×93 09:30 $9.01 → close $9.20 +17.67; LDI×597 09:30 $0.94 → close $0.90 -23.88; BTBT×186 09:30 $1.50 → close $1.57 +13.02 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $334.42 | ▲ 09:30 equity $10,221.16 vs yday $10,164.79 (+56.37) | 09:30 open · cash $334.42 (unchanged overnight, no fees) · equity $10,221.16 vs prior close $10,164.79 (+56.37) · 8 name(s) re-marked at the open (per-name table). TLN×6 yday $362.74 → 09:30 $367.88 +30.84; VST×13 yday $148.13 → 09:30 $149.37 +16.12; NRG×13 yday $126.24 → 09:30 $127.40 +15.08; DAVE×4 yday $334.57 → 09:30 $336.94 +9.48; SLG×19 yday $56.09 → 09:30 $55.37 -13.68; MARA×93 yday $9.20 → 09:30 $9.22 +1.86; LDI×597 yday $0.90 → 09:30 $0.91 +5.97; BTBT×186 yday $1.57 → 09:30 $1.52 -9.30 | — |
| 2026-08-17 09:30 ET | **SELL** | `TLN` | 6 | $367.88 | $2.04 | $+44.26 | $2,539.66 | ▲ +44.26 after sell → book $10,219.12; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VST` | 13 | $149.37 | $2.05 | $+28.03 | $4,479.42 | ▲ +28.03 after sell → book $10,217.07; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NRG` | 13 | $127.40 | $2.05 | $+92.12 | $6,133.57 | ▲ +92.12 after sell → book $10,215.02; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `DAVE` | 4 | $336.94 | $2.02 | $+20.10 | $7,479.31 | ▲ +20.10 after sell → book $10,212.99; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SLG` | 19 | $55.37 | $2.07 | $-46.67 | $8,529.27 | ▼ -46.67 after sell → book $10,210.93; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MARA` | 93 | $9.22 | $2.29 | $+14.97 | $9,384.43 | ▲ +14.97 after sell → book $10,208.63; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LDI` | 597 | $0.91 | $7.31 | $-32.61 | $9,918.60 | ▼ -32.61 after sell → book $10,201.32; vs 09:30 mark -7.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 186 | $1.52 | $2.59 | $-1.42 | $10,198.73 | ▼ -1.42 after sell → book $10,198.73; vs 09:30 mark -2.59 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 49 | $46.18 | $2.14 | — | $7,933.77 | — | rank-weighted leftover; list flatten; 🔵; ret5=+6.7; leftover $2266.38 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 13 | $142.77 | $2.03 | — | $6,075.73 | — | rank-weighted leftover; list flatten; 🔵; ret5=+5.8; leftover $1983.09 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 8 | $202.70 | $2.01 | — | $4,452.12 | — | rank-weighted leftover; list flatten; 🔵; ret5=+8.3; leftover $1699.79 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 349 | $4.05 | $4.50 | — | $3,034.17 | — | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=-12.3; leftover $1416.49 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 133 | $8.46 | $2.39 | — | $1,906.60 | — | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+0.4; leftover $1133.19 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ELF` | 9 | $90.54 | $2.02 | — | $1,089.72 | — | rank-weighted leftover; list flatten; ret5=-7.2; leftover $849.89 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 174 | $3.24 | $2.51 | — | $523.45 | — | rank-weighted leftover; list flatten; ⚪; ret5=+0.3; leftover $566.60 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HNST` | 58 | $4.81 | $2.16 | — | $242.31 | — | rank-weighted leftover; list flatten; ⚪; ret5=-11.4; leftover $283.30 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $242.31 | ▲ close $10,276.25 vs 09:30 $10,221.16 (session +97.28) | 16:00 close · cash $242.31 · equity $10,276.25 vs 09:30 $10,221.16 (+55.09; session marks +97.28) · 8 name(s) marked open→close (per-name table). DVN×49 09:30 $46.18 → close $47.57 +68.11; EOG×13 09:30 $142.77 → close $146.15 +43.94; FANG×8 09:30 $202.70 → close $206.29 +28.72; TMC×349 09:30 $4.05 → close $3.77 -97.72; TGB×133 09:30 $8.46 → close $8.77 +41.23; ELF×9 09:30 $90.54 → close $93.66 +28.08; DNN×174 09:30 $3.24 → close $3.19 -8.70; HNST×58 09:30 $4.81 → close $4.70 -6.38 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $242.31 | ▲ 09:30 equity $10,278.66 vs yday $10,276.25 (+2.41) | 09:30 open · cash $242.31 (unchanged overnight, no fees) · equity $10,278.66 vs prior close $10,276.25 (+2.41) · 8 name(s) re-marked at the open (per-name table). DVN×49 yday $47.57 → 09:30 $48.00 +21.07; EOG×13 yday $146.15 → 09:30 $148.04 +24.57; FANG×8 yday $206.29 → 09:30 $208.93 +21.12; TMC×349 yday $3.77 → 09:30 $3.72 -17.45; TGB×133 yday $8.77 → 09:30 $8.55 -29.26; ELF×9 yday $93.66 → 09:30 $93.44 -1.98; DNN×174 yday $3.19 → 09:30 $3.11 -13.92; HNST×58 yday $4.70 → 09:30 $4.67 -1.74 | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 49 | $48.00 | $2.17 | $+84.88 | $2,592.14 | ▲ +84.88 after sell → book $10,276.49; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 13 | $148.04 | $2.05 | $+64.43 | $4,514.60 | ▲ +64.43 after sell → book $10,274.43; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 8 | $208.93 | $2.04 | $+45.79 | $6,184.01 | ▲ +45.79 after sell → book $10,272.40; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TMC` | 349 | $3.72 | $4.57 | $-124.24 | $7,477.72 | ▼ -124.24 after sell → book $10,267.83; vs 09:30 mark -4.57 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGB` | 133 | $8.55 | $2.42 | $+7.16 | $8,612.45 | ▲ +7.16 after sell → book $10,265.41; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ELF` | 9 | $93.44 | $2.04 | $+22.05 | $9,451.37 | ▲ +22.05 after sell → book $10,263.37; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `DNN` | 174 | $3.11 | $2.55 | $-27.68 | $9,989.96 | ▼ -27.68 after sell → book $10,260.82; vs 09:30 mark -2.55 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 vol🟢 buy🟢 |
| 2026-08-18 09:30 ET | **SELL** | `HNST` | 58 | $4.67 | $2.18 | $-12.47 | $10,258.63 | ▼ -12.47 after sell → book $10,258.63; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,258.63 | ▲ close $10,258.63 vs 09:30 $10,278.66 (session +0.00) | 16:00 close · cash $10,258.63 · no lots left · equity $10,258.63. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,258.63 | ▲ 09:30 equity $10,258.63 vs yday $10,258.63 (+0.00) | 09:30 open · cash $10,258.63 · no holdings · equity $10,258.63 vs prior close $10,258.63 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,258.63 | ▲ close $10,258.63 vs 09:30 $10,258.63 (session +0.00) | 16:00 close · cash $10,258.63 · no lots left · equity $10,258.63. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,258.63 | ▲ 09:30 equity $10,258.63 vs yday $10,258.63 (+0.00) | 09:30 open · cash $10,258.63 · no holdings · equity $10,258.63 vs prior close $10,258.63 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 110 | $20.55 | $2.32 | — | $7,995.81 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $2279.70 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 21 | $91.01 | $2.05 | — | $6,082.55 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1994.73 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 82 | $20.65 | $2.24 | — | $4,387.02 | — | rank-weighted leftover; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1709.77 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 246 | $5.77 | $3.17 | — | $2,964.42 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1424.81 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 58 | $19.63 | $2.16 | — | $1,823.72 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1139.85 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 28 | $29.63 | $2.07 | — | $992.00 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $854.89 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 325 | $1.75 | $4.19 | — | $419.06 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $569.92 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 1 | $144.54 | $1.45 | — | $273.07 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $284.96 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $273.07 | ▲ close $10,459.48 vs 09:30 $10,258.63 (session +220.51) | 16:00 close · cash $273.07 · equity $10,459.48 vs 09:30 $10,258.63 (+200.85; session marks +220.51) · 8 name(s) marked open→close (per-name table). AG×110 09:30 $20.55 → close $21.19 +70.40; BHP×21 09:30 $91.01 → close $93.63 +55.02; CDE×82 09:30 $20.65 → close $21.11 +37.72; HDSN×246 09:30 $5.77 → close $5.57 -49.20; IAG×58 09:30 $19.63 → close $20.50 +50.46; KGC×28 09:30 $29.63 → close $31.43 +50.40; NFGC×325 09:30 $1.75 → close $1.75 +0.00; WPM×1 09:30 $144.54 → close $150.25 +5.71 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $273.07 | ▲ 09:30 equity $10,735.58 vs yday $10,459.48 (+276.10) | 09:30 open · cash $273.07 (unchanged overnight, no fees) · equity $10,735.58 vs prior close $10,459.48 (+276.10) · 8 name(s) re-marked at the open (per-name table). AG×110 yday $21.19 → 09:30 $21.90 +78.10; BHP×21 yday $93.63 → 09:30 $95.72 +43.89; CDE×82 yday $21.11 → 09:30 $21.75 +52.48; HDSN×246 yday $5.57 → 09:30 $5.67 +24.60; IAG×58 yday $20.50 → 09:30 $21.17 +38.86; KGC×28 yday $31.43 → 09:30 $32.17 +20.72; NFGC×325 yday $1.75 → 09:30 $1.79 +13.00; WPM×1 yday $150.25 → 09:30 $154.70 +4.45 | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 110 | $21.90 | $2.36 | $+143.82 | $2,679.72 | ▲ +143.82 after sell → book $10,733.23; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 21 | $95.72 | $2.08 | $+94.78 | $4,687.76 | ▲ +94.78 after sell → book $10,731.15; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 82 | $21.75 | $2.26 | $+85.70 | $6,468.99 | ▲ +85.70 after sell → book $10,728.88; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 246 | $5.67 | $3.23 | $-31.00 | $7,860.59 | ▼ -31.00 after sell → book $10,725.66; vs 09:30 mark -3.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 58 | $21.17 | $2.18 | $+84.97 | $9,086.26 | ▲ +84.97 after sell → book $10,723.47; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 28 | $32.17 | $2.09 | $+66.95 | $9,984.93 | ▲ +66.95 after sell → book $10,721.38; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 325 | $1.79 | $4.26 | $+4.55 | $10,562.42 | ▲ +4.55 after sell → book $10,717.12; vs 09:30 mark -4.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 1 | $154.70 | $1.57 | $+7.14 | $10,715.55 | ▲ +7.14 after sell → book $10,715.55; vs 09:30 mark -1.57 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 19 | $119.43 | $2.05 | — | $8,444.34 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+20.4; leftover $2381.23 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 121 | $17.20 | $2.35 | — | $6,360.78 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $2083.58 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 8 | $216.30 | $2.01 | — | $4,628.37 | — | rank-weighted leftover; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1785.93 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 133 | $11.13 | $2.39 | — | $3,145.69 | — | rank-weighted leftover; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1488.27 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 482 | $2.47 | $6.22 | — | $1,948.93 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $1190.62 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 462 | $1.93 | $5.96 | — | $1,051.31 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $892.96 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 9 | $59.72 | $2.02 | — | $511.81 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $595.31 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 225 | $1.32 | $2.90 | — | $211.91 | — | rank-weighted leftover; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $297.65 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $211.91 | ▲ close $10,923.01 vs 09:30 $10,735.58 (session +233.36) | 16:00 close · cash $211.91 · equity $10,923.01 vs 09:30 $10,735.58 (+187.43; session marks +233.36) · 8 name(s) marked open→close (per-name table). AU×19 09:30 $119.43 → close $121.22 +34.01; AUPH×121 09:30 $17.20 → close $16.65 -66.55; AEM×8 09:30 $216.30 → close $216.06 -1.92; ARCT×133 09:30 $11.13 → close $13.45 +308.56; AUTL×482 09:30 $2.47 → close $2.41 -28.92; CRDL×462 09:30 $1.93 → close $1.86 -32.34; CRSP×9 09:30 $59.72 → close $59.50 -1.98; CYPH×225 09:30 $1.32 → close $1.42 +22.50 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $211.91 | ▲ 09:30 equity $10,952.15 vs yday $10,923.01 (+29.14) | 09:30 open · cash $211.91 (unchanged overnight, no fees) · equity $10,952.15 vs prior close $10,923.01 (+29.14) · 8 name(s) re-marked at the open (per-name table). AU×19 yday $121.22 → 09:30 $120.50 -13.68; AUPH×121 yday $16.65 → 09:30 $16.60 -6.05; AEM×8 yday $216.06 → 09:30 $217.03 +7.76; ARCT×133 yday $13.45 → 09:30 $13.26 -25.27; AUTL×482 yday $2.41 → 09:30 $2.36 -24.10; CRDL×462 yday $1.86 → 09:30 $1.87 +4.62; CRSP×9 yday $59.50 → 09:30 $58.79 -6.39; CYPH×225 yday $1.42 → 09:30 $1.83 +92.25 | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 19 | $120.50 | $2.08 | $+16.21 | $2,499.34 | ▲ +16.21 after sell → book $10,950.08; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 121 | $16.60 | $2.39 | $-77.34 | $4,505.55 | ▼ -77.34 after sell → book $10,947.69; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 8 | $217.03 | $2.04 | $+1.79 | $6,239.75 | ▲ +1.79 after sell → book $10,945.65; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 133 | $13.26 | $2.43 | $+278.48 | $8,000.90 | ▲ +278.48 after sell → book $10,943.22; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 482 | $2.36 | $6.31 | $-65.55 | $9,132.12 | ▼ -65.55 after sell → book $10,936.92; vs 09:30 mark -6.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRDL` | 462 | $1.87 | $6.05 | $-39.73 | $9,990.01 | ▼ -39.73 after sell → book $10,930.87; vs 09:30 mark -6.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 9 | $58.79 | $2.04 | $-12.42 | $10,517.08 | ▼ -12.42 after sell → book $10,928.83; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 225 | $1.83 | $2.95 | $+108.90 | $10,925.88 | ▲ +108.90 after sell → book $10,925.88; vs 09:30 mark -2.95 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,925.88 | ▲ close $10,925.88 vs 09:30 $10,952.15 (session +0.00) | 16:00 close · cash $10,925.88 · no lots left · equity $10,925.88. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,925.88 | ▲ 09:30 equity $10,925.88 vs yday $10,925.88 (+0.00) | 09:30 open · cash $10,925.88 · no holdings · equity $10,925.88 vs prior close $10,925.88 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 101 | $24.00 | $2.29 | — | $8,499.59 | — | rank-weighted leftover; list flatten; ⚪; ret5=+13.0; leftover $2427.97 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 194 | $10.92 | $2.57 | — | $6,378.54 | — | rank-weighted leftover; list flatten; 🔵; ret5=+10.4; leftover $2124.48 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 29 | $61.47 | $2.08 | — | $4,593.83 | — | rank-weighted leftover; list flatten; 🔵; ret5=+9.2; leftover $1820.98 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 183 | $8.28 | $2.54 | — | $3,076.05 | — | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+8.8; leftover $1517.48 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 232 | $5.23 | $2.99 | — | $1,859.70 | — | rank-weighted leftover; list flatten; ret5=+10.7; leftover $1213.99 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 2 | $429.24 | $2.00 | — | $999.22 | — | rank-weighted leftover; list flatten; ret5=+6.1; leftover $910.49 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 374 | $1.62 | $4.82 | — | $388.52 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $606.99 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 151 | $2.00 | $2.44 | — | $84.08 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.8; leftover $303.50 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $84.08 | ▼ close $10,890.62 vs 09:30 $10,925.88 (session -13.53) | 16:00 close · cash $84.08 · equity $10,890.62 vs 09:30 $10,925.88 (-35.26; session marks -13.53) · 8 name(s) marked open→close (per-name table). MOS×101 09:30 $24.00 → close $23.75 -25.25; OCUL×194 09:30 $10.92 → close $10.92 +0.00; INSP×29 09:30 $61.47 → close $61.47 +0.00; CRMD×183 09:30 $8.28 → close $8.28 +0.00; RZLT×232 09:30 $5.23 → close $5.29 +13.92; HCA×2 09:30 $429.24 → close $428.50 -1.48; BMEA×374 09:30 $1.62 → close $1.61 -3.74; NPWR×151 09:30 $2.00 → close $2.02 +3.02 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $84.08 | ▲ 09:30 equity $10,890.62 vs yday $10,890.62 (-0.00) | 09:30 open · cash $84.08 (unchanged overnight, no fees) · equity $10,890.62 vs prior close $10,890.62 (-0.00) · 8 name(s) re-marked at the open (per-name table). MOS×101 yday $23.75 → 09:30 $23.75 +0.00; OCUL×194 yday $10.92 → 09:30 $10.92 +0.00; INSP×29 yday $61.47 → 09:30 $61.47 +0.00; CRMD×183 yday $8.28 → 09:30 $8.28 +0.00; RZLT×232 yday $5.29 → 09:30 $5.29 +0.00; HCA×2 yday $428.50 → 09:30 $428.50 +0.00; BMEA×374 yday $1.61 → 09:30 $1.61 +0.00; NPWR×151 yday $2.02 → 09:30 $2.02 +0.00 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $84.08 | ▲ close $10,890.62 vs 09:30 $10,890.62 (session +0.00) | 16:00 close · cash $84.08 · equity $10,890.62 vs 09:30 $10,890.62 (-0.00; session marks +0.00) · 8 name(s) marked open→close (per-name table). MOS×101 09:30 $23.75 → close $23.75 +0.00; OCUL×194 09:30 $10.92 → close $10.92 +0.00; INSP×29 09:30 $61.47 → close $61.47 +0.00; CRMD×183 09:30 $8.28 → close $8.28 +0.00; RZLT×232 09:30 $5.29 → close $5.29 +0.00; HCA×2 09:30 $428.50 → close $428.50 +0.00; BMEA×374 09:30 $1.61 → close $1.61 +0.00; NPWR×151 09:30 $2.02 → close $2.02 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $84.08 | ▲ 09:30 equity $10,965.26 vs yday $10,890.62 (+74.64) | 09:30 open · cash $84.08 (unchanged overnight, no fees) · equity $10,965.26 vs prior close $10,890.62 (+74.64) · 8 name(s) re-marked at the open (per-name table). MOS×101 yday $23.75 → 09:30 $24.84 +110.09; OCUL×194 yday $10.92 → 09:30 $10.79 -25.22; INSP×29 yday $61.47 → 09:30 $60.07 -40.60; CRMD×183 yday $8.28 → 09:30 $8.60 +58.56; RZLT×232 yday $5.29 → 09:30 $5.01 -64.96; HCA×2 yday $428.50 → 09:30 $427.50 -2.00; BMEA×374 yday $1.61 → 09:30 $1.75 +52.36; NPWR×151 yday $2.02 → 09:30 $1.93 -13.59 | — |
| 2026-08-27 09:30 ET | **SELL** | `OCUL` | 194 | $10.79 | $2.62 | $-30.41 | $2,174.72 | ▼ -30.41 after sell → book $10,962.64; vs 09:30 mark -2.62 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `INSP` | 29 | $60.07 | $2.10 | $-44.78 | $3,914.64 | ▼ -44.78 after sell → book $10,960.53; vs 09:30 mark -2.11 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRMD` | 183 | $8.60 | $2.58 | $+53.44 | $5,485.86 | ▲ +53.44 after sell → book $10,957.95; vs 09:30 mark -2.58 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `RZLT` | 232 | $5.01 | $3.04 | $-57.07 | $6,645.14 | ▼ -57.07 after sell → book $10,954.91; vs 09:30 mark -3.04 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `HCA` | 2 | $427.50 | $2.02 | $-7.49 | $7,498.12 | ▼ -7.49 after sell → book $10,952.89; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BMEA` | 374 | $1.75 | $4.90 | $+38.90 | $8,147.73 | ▲ +38.90 after sell → book $10,948.00; vs 09:30 mark -4.89 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `NPWR` | 151 | $1.93 | $2.48 | $-15.49 | $8,436.68 | ▼ -15.49 after sell → book $10,945.52; vs 09:30 mark -2.48 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 51 | $40.72 | $2.14 | — | $6,357.82 | — | rank-weighted leftover; list flatten; ret5=+1.8; leftover $2109.17 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 128 | $14.09 | $2.37 | — | $4,551.92 | — | rank-weighted leftover; list flatten; ret5=+1.1; leftover $1807.86 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 581 | $2.59 | $7.49 | — | $3,039.64 | — | rank-weighted leftover; list flatten; ret5=+4.2; leftover $1506.55 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 14 | $80.97 | $2.03 | — | $1,904.03 | — | rank-weighted leftover; list mover_buy; 🔵; ret5=-1.3; leftover $1205.24 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GGB` | 204 | $4.42 | $2.63 | — | $999.71 | — | rank-weighted leftover; list mover_buy; 🔵; ret5=-8.6; leftover $903.93 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MT` | 8 | $75.12 | $2.01 | — | $396.74 | — | rank-weighted leftover; list mover_buy; 🔵; ret5=-2.2; leftover $602.62 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $396.74 | ▲ close $10,941.98 vs 09:30 $10,965.26 (session +15.15) | 16:00 close · cash $396.74 · equity $10,941.98 vs 09:30 $10,965.26 (-23.28; session marks +15.15) · 7 name(s) marked open→close (per-name table). MOS×101 09:30 $24.84 → close $24.16 -68.68; RRC×51 09:30 $40.72 → close $41.55 +42.33; CRK×128 09:30 $14.09 → close $14.50 +52.48; SLI×581 09:30 $2.59 → close $2.61 +11.62; ACMR×14 09:30 $80.97 → close $79.11 -26.04; GGB×204 09:30 $4.42 → close $4.46 +8.16; MT×8 09:30 $75.12 → close $74.53 -4.72 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $396.74 | ▲ 09:30 equity $10,962.24 vs yday $10,941.98 (+20.26) | 09:30 open · cash $396.74 (unchanged overnight, no fees) · equity $10,962.24 vs prior close $10,941.98 (+20.26) · 7 name(s) re-marked at the open (per-name table). MOS×101 yday $24.16 → 09:30 $24.00 -16.16; RRC×51 yday $41.55 → 09:30 $41.44 -5.61; CRK×128 yday $14.50 → 09:30 $14.42 -10.24; SLI×581 yday $2.61 → 09:30 $2.60 -5.81; ACMR×14 yday $79.11 → 09:30 $81.65 +35.56; GGB×204 yday $4.46 → 09:30 $4.57 +22.44; MT×8 yday $74.53 → 09:30 $74.54 +0.08 | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 14 | $81.65 | $2.05 | $+5.44 | $1,537.79 | ▲ +5.44 after sell → book $10,960.19; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GGB` | 204 | $4.57 | $2.68 | $+25.29 | $2,467.39 | ▲ +25.29 after sell → book $10,957.51; vs 09:30 mark -2.68 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MT` | 8 | $74.54 | $2.03 | $-8.69 | $3,061.68 | ▼ -8.69 after sell → book $10,955.48; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 8 | $144.70 | $2.01 | — | $1,902.07 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1224.67 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BHVN` | 54 | $16.95 | $2.15 | — | $984.61 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $918.50 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 33 | $18.50 | $2.09 | — | $372.02 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $612.34 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 33 | $9.19 | $2.09 | — | $66.67 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $306.17 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $66.67 | ▲ close $10,957.73 vs 09:30 $10,962.24 (session +10.59) | 16:00 close · cash $66.67 · equity $10,957.73 vs 09:30 $10,962.24 (-4.51; session marks +10.59) · 8 name(s) marked open→close (per-name table). MOS×101 09:30 $24.00 → close $23.76 -24.24; RRC×51 09:30 $41.44 → close $41.64 +10.20; CRK×128 09:30 $14.42 → close $14.62 +25.60; SLI×581 09:30 $2.60 → close $2.64 +23.24; ANF×8 09:30 $144.70 → close $145.75 +8.40; BHVN×54 09:30 $16.95 → close $16.12 -44.82; BZ×33 09:30 $18.50 → close $18.00 -16.50; CAPR×33 09:30 $9.19 → close $10.06 +28.71 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $66.67 | ▼ 09:30 equity $10,809.03 vs yday $10,957.73 (-148.70) | 09:30 open · cash $66.67 (unchanged overnight, no fees) · equity $10,809.03 vs prior close $10,957.73 (-148.70) · 8 name(s) re-marked at the open (per-name table). MOS×101 yday $23.76 → 09:30 $23.75 -1.01; RRC×51 yday $41.64 → 09:30 $41.11 -27.03; CRK×128 yday $14.62 → 09:30 $14.56 -7.68; SLI×581 yday $2.64 → 09:30 $2.51 -75.53; ANF×8 yday $145.75 → 09:30 $148.67 +23.36; BHVN×54 yday $16.12 → 09:30 $15.44 -36.72; BZ×33 yday $18.00 → 09:30 $17.89 -3.63; CAPR×33 yday $10.06 → 09:30 $9.44 -20.46 | — |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 101 | $23.75 | $2.33 | $-29.87 | $2,463.09 | ▼ -29.87 after sell → book $10,806.70; vs 09:30 mark -2.33 | dropped from list after 4 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `RRC` | 51 | $41.11 | $2.17 | $+15.58 | $4,557.53 | ▲ +15.58 after sell → book $10,804.53; vs 09:30 mark -2.17 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CRK` | 128 | $14.56 | $2.41 | $+55.38 | $6,418.80 | ▲ +55.38 after sell → book $10,802.12; vs 09:30 mark -2.41 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SLI` | 581 | $2.51 | $7.60 | $-61.58 | $7,869.50 | ▼ -61.58 after sell → book $10,794.51; vs 09:30 mark -7.61 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 8 | $148.67 | $2.03 | $+27.71 | $9,056.83 | ▲ +27.71 after sell → book $10,792.48; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BHVN` | 54 | $15.44 | $2.17 | $-85.86 | $9,888.42 | ▼ -85.86 after sell → book $10,790.31; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BZ` | 33 | $17.89 | $2.11 | $-24.33 | $10,476.68 | ▼ -24.33 after sell → book $10,788.20; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 33 | $9.44 | $2.11 | $+4.05 | $10,786.09 | ▲ +4.05 after sell → book $10,786.09; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,786.09 | ▲ close $10,786.09 vs 09:30 $10,809.03 (session +0.00) | 16:00 close · cash $10,786.09 · no lots left · equity $10,786.09. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,786.09 | ▲ 09:30 equity $10,786.09 vs yday $10,786.09 (-0.00) | 09:30 open · cash $10,786.09 · no holdings · equity $10,786.09 vs prior close $10,786.09 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,786.09 | ▲ close $10,786.09 vs 09:30 $10,786.09 (session +0.00) | 16:00 close · cash $10,786.09 · no lots left · equity $10,786.09. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,786.09 | ▲ 09:30 equity $10,786.09 vs yday $10,786.09 (-0.00) | 09:30 open · cash $10,786.09 · no holdings · equity $10,786.09 vs prior close $10,786.09 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,786.09 | ▲ close $10,786.09 vs 09:30 $10,786.09 (session +0.00) | 16:00 close · cash $10,786.09 · no lots left · equity $10,786.09. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,786.09 | ▲ 09:30 equity $10,786.09 vs yday $10,786.09 (-0.00) | 09:30 open · cash $10,786.09 · no holdings · equity $10,786.09 vs prior close $10,786.09 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 48 | $49.76 | $2.13 | — | $8,395.48 | — | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+10.6; leftover $2396.91 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 50 | $41.31 | $2.14 | — | $6,327.84 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $2097.30 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 549 | $3.27 | $7.08 | — | $4,525.52 | — | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1797.68 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 194 | $7.70 | $2.57 | — | $3,029.15 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $1498.07 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $125.94 | $2.02 | — | $1,893.67 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1198.45 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 736 | $1.22 | $9.49 | — | $986.26 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $898.84 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRVO` | 32 | $18.40 | $2.09 | — | $395.37 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $599.23 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 19 | $15.70 | $2.05 | — | $95.03 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $299.61 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $95.03 | ▲ close $11,571.08 vs 09:30 $10,786.09 (session +814.56) | 16:00 close · cash $95.03 · equity $11,571.08 vs 09:30 $10,786.09 (+784.99; session marks +814.56) · 8 name(s) marked open→close (per-name table). ATRC×48 09:30 $49.76 → close $52.59 +135.84; HRMY×50 09:30 $41.31 → close $42.86 +77.50; CABA×549 09:30 $3.27 → close $3.57 +164.70; VSTM×194 09:30 $7.70 → close $8.02 +62.08; RVTY×9 09:30 $125.94 → close $130.94 +45.00; GPRO×736 09:30 $1.22 → close $1.69 +345.92; FRVO×32 09:30 $18.40 → close $17.98 -13.44; CRK×19 09:30 $15.70 → close $15.54 -3.04 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $95.03 | ▲ 09:30 equity $11,710.78 vs yday $11,571.08 (+139.70) | 09:30 open · cash $95.03 (unchanged overnight, no fees) · equity $11,710.78 vs prior close $11,571.08 (+139.70) · 8 name(s) re-marked at the open (per-name table). ATRC×48 yday $52.59 → 09:30 $52.88 +13.92; HRMY×50 yday $42.86 → 09:30 $42.93 +3.50; CABA×549 yday $3.57 → 09:30 $3.63 +32.94; VSTM×194 yday $8.02 → 09:30 $8.03 +1.94; RVTY×9 yday $130.94 → 09:30 $132.45 +13.59; GPRO×736 yday $1.69 → 09:30 $1.78 +66.24; FRVO×32 yday $17.98 → 09:30 $18.27 +9.28; CRK×19 yday $15.54 → 09:30 $15.45 -1.71 | — |
| 2026-09-04 09:30 ET | **SELL** | `HRMY` | 50 | $42.93 | $2.17 | $+76.69 | $2,239.36 | ▲ +76.69 after sell → book $11,708.61; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 194 | $8.03 | $2.62 | $+58.83 | $3,794.56 | ▲ +58.83 after sell → book $11,705.99; vs 09:30 mark -2.62 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 9 | $132.45 | $2.04 | $+54.54 | $4,984.58 | ▲ +54.54 after sell → book $11,703.96; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `FRVO` | 32 | $18.27 | $2.11 | $-8.35 | $5,567.11 | ▼ -8.35 after sell → book $11,701.85; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 19 | $15.45 | $2.07 | $-8.86 | $5,858.59 | ▼ -8.86 after sell → book $11,699.78; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ASND` | 7 | $266.94 | $2.01 | — | $3,988.00 | — | rank-weighted leftover; list flatten; ret5=+1.9; leftover $1952.86 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OSCR` | 50 | $30.65 | $2.14 | — | $2,453.36 | — | rank-weighted leftover; list flatten; 🔵; ret5=-2.2; leftover $1562.29 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `NVAX` | 112 | $10.41 | $2.33 | — | $1,285.12 | — | rank-weighted leftover; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $1171.72 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BVS` | 53 | $14.50 | $2.15 | — | $514.47 | — | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+0.8; leftover $781.15 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 200 | $1.95 | $2.59 | — | $121.88 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $390.57 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $121.88 | ▼ close $11,390.52 vs 09:30 $11,710.78 (session -298.05) | 16:00 close · cash $121.88 · equity $11,390.52 vs 09:30 $11,710.78 (-320.26; session marks -298.05) · 8 name(s) marked open→close (per-name table). ATRC×48 09:30 $52.88 → close $52.46 -20.16; CABA×549 09:30 $3.63 → close $3.48 -82.35; GPRO×736 09:30 $1.78 → close $1.39 -287.04; ASND×7 09:30 $266.94 → close $271.12 +29.26; OSCR×50 09:30 $30.65 → close $32.24 +79.50; NVAX×112 09:30 $10.41 → close $10.34 -7.84; BVS×53 09:30 $14.50 → close $14.36 -7.42; BAK×200 09:30 $1.95 → close $1.94 -2.00 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MUR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MLYS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OBE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CYPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `STE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SYK` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MUR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TRMD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MLYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TBPH` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OCUL` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRMD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SAFX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-26 | `BMEA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `NPWR` | no_price | no 09:30 open — carry |
| 2026-08-26 | `KURA` | no_price | no 09:30 open |
| 2026-08-26 | `AVBP` | no_price | no 09:30 open |
| 2026-08-27 | `MU` | cash | leftover split 301.31 < 1 share @ 925.74 |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `OKTA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `VEEV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MTDR` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `RES` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KOS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OIS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `FTI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KMI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `PCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HRMY` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VSTM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MGTX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBR-A` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ATRC` | 48 | 2026-09-03 @ $49.76 | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+10.6; leftover $2396.91 |
| `CABA` | 549 | 2026-09-03 @ $3.27 | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1797.68 |
| `GPRO` | 736 | 2026-09-03 @ $1.22 | rank-weighted leftover; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $898.84 |
| `ASND` | 7 | 2026-09-04 @ $266.94 | rank-weighted leftover; list flatten; ret5=+1.9; leftover $1952.86 |
| `OSCR` | 50 | 2026-09-04 @ $30.65 | rank-weighted leftover; list flatten; 🔵; ret5=-2.2; leftover $1562.29 |
| `NVAX` | 112 | 2026-09-04 @ $10.41 | rank-weighted leftover; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $1171.72 |
| `BVS` | 53 | 2026-09-04 @ $14.50 | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+0.8; leftover $781.15 |
| `BAK` | 200 | 2026-09-04 @ $1.95 | rank-weighted leftover; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $390.57 |
