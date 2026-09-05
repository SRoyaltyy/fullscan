# Factor mine action — `union_h1_half`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `half` · sell `list` · S-boost `none` · deploy half leftover

Cash book **+5.58%** ($10,557) · signal-only (no cash/fees) was +18.57%. Starts YES **16/17**. Fills 134 · skips 53 · realized $+381.95.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `half` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $4,308.94.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `BTSG` | 10 | — | $59.80 | +0.00 | $60.23 | +4.30 | +4.30 | +0.00 | +4.30 |
| 2026-08-13 | `IREN` | 13 | — | $45.98 | +0.00 | $44.76 | -15.86 | -15.86 | +0.00 | -15.86 |
| 2026-08-13 | `TPG` | 12 | — | $50.62 | +0.00 | $54.62 | +47.96 | +47.96 | +0.00 | +47.96 |
| 2026-08-13 | `TGTX` | 12 | — | $49.70 | +0.00 | $47.94 | -21.12 | -21.12 | +0.00 | -21.12 |
| 2026-08-13 | `SLS` | 53 | — | $11.70 | +0.00 | $12.36 | +34.98 | +34.98 | +0.00 | +34.98 |
| 2026-08-13 | `HIMS` | 21 | — | $29.74 | +0.00 | $28.77 | -20.37 | -20.37 | +0.00 | -20.37 |
| 2026-08-13 | `INO` | 771 | — | $0.81 | +0.00 | $0.90 | +69.39 | +69.39 | +0.00 | +69.39 |
| 2026-08-13 | `TNDM` | 26 | — | $23.33 | +0.00 | $23.13 | -5.20 | -5.20 | +0.00 | -5.20 |
| 2026-08-14 | `BTSG` | 10 | $60.23 | $59.65 | -5.80 | — | +0.00 | -5.80 | -1.50 | — |
| 2026-08-14 | `IREN` | 13 | $44.76 | $44.09 | -8.71 | — | +0.00 | -8.71 | -24.57 | — |
| 2026-08-14 | `TPG` | 12 | $54.62 | $55.29 | +8.04 | — | +0.00 | +8.04 | +56.00 | — |
| 2026-08-14 | `TGTX` | 12 | $47.94 | $47.27 | -8.04 | — | +0.00 | -8.04 | -29.16 | — |
| 2026-08-14 | `SLS` | 53 | $12.36 | $12.40 | +2.12 | — | +0.00 | +2.12 | +37.10 | — |
| 2026-08-14 | `HIMS` | 21 | $28.77 | $29.15 | +7.98 | — | +0.00 | +7.98 | -12.39 | — |
| 2026-08-14 | `INO` | 771 | $0.90 | $0.93 | +23.13 | — | +0.00 | +23.13 | +92.52 | — |
| 2026-08-14 | `TNDM` | 26 | $23.13 | $22.92 | -5.46 | — | +0.00 | -5.46 | -10.66 | — |
| 2026-08-14 | `TLN` | 1 | — | $359.83 | +0.00 | $362.74 | +2.91 | +2.91 | +0.00 | +2.91 |
| 2026-08-14 | `VST` | 4 | — | $146.90 | +0.00 | $148.13 | +4.92 | +4.92 | +0.00 | +4.92 |
| 2026-08-14 | `NRG` | 5 | — | $120.00 | +0.00 | $126.24 | +31.20 | +31.20 | +0.00 | +31.20 |
| 2026-08-14 | `DAVE` | 1 | — | $330.91 | +0.00 | $334.57 | +3.66 | +3.66 | +0.00 | +3.66 |
| 2026-08-14 | `SLG` | 10 | — | $57.61 | +0.00 | $56.09 | -15.20 | -15.20 | +0.00 | -15.20 |
| 2026-08-14 | `MARA` | 69 | — | $9.01 | +0.00 | $9.20 | +13.11 | +13.11 | +0.00 | +13.11 |
| 2026-08-14 | `LDI` | 671 | — | $0.94 | +0.00 | $0.90 | -26.84 | -26.84 | +0.00 | -26.84 |
| 2026-08-14 | `BTBT` | 419 | — | $1.50 | +0.00 | $1.57 | +29.33 | +29.33 | +0.00 | +29.33 |
| 2026-08-17 | `TLN` | 1 | $362.74 | $367.88 | +5.14 | — | +0.00 | +5.14 | +8.05 | — |
| 2026-08-17 | `VST` | 4 | $148.13 | $149.37 | +4.96 | — | +0.00 | +4.96 | +9.88 | — |
| 2026-08-17 | `NRG` | 5 | $126.24 | $127.40 | +5.80 | — | +0.00 | +5.80 | +37.00 | — |
| 2026-08-17 | `DAVE` | 1 | $334.57 | $336.94 | +2.37 | — | +0.00 | +2.37 | +6.03 | — |
| 2026-08-17 | `SLG` | 10 | $56.09 | $55.37 | -7.20 | — | +0.00 | -7.20 | -22.40 | — |
| 2026-08-17 | `MARA` | 69 | $9.20 | $9.22 | +1.38 | — | +0.00 | +1.38 | +14.49 | — |
| 2026-08-17 | `LDI` | 671 | $0.90 | $0.91 | +6.71 | — | +0.00 | +6.71 | -20.13 | — |
| 2026-08-17 | `BTBT` | 419 | $1.57 | $1.52 | -20.95 | — | +0.00 | -20.95 | +8.38 | — |
| 2026-08-17 | `DVN` | 13 | — | $46.18 | +0.00 | $47.57 | +18.07 | +18.07 | +0.00 | +18.07 |
| 2026-08-17 | `EOG` | 4 | — | $142.77 | +0.00 | $146.15 | +13.52 | +13.52 | +0.00 | +13.52 |
| 2026-08-17 | `FANG` | 3 | — | $202.70 | +0.00 | $206.29 | +10.77 | +10.77 | +0.00 | +10.77 |
| 2026-08-17 | `TMC` | 155 | — | $4.05 | +0.00 | $3.77 | -43.40 | -43.40 | +0.00 | -43.40 |
| 2026-08-17 | `TGB` | 74 | — | $8.46 | +0.00 | $8.77 | +22.94 | +22.94 | +0.00 | +22.94 |
| 2026-08-17 | `ELF` | 6 | — | $90.54 | +0.00 | $93.66 | +18.72 | +18.72 | +0.00 | +18.72 |
| 2026-08-17 | `DNN` | 193 | — | $3.24 | +0.00 | $3.19 | -9.65 | -9.65 | +0.00 | -9.65 |
| 2026-08-17 | `HNST` | 130 | — | $4.81 | +0.00 | $4.70 | -14.30 | -14.30 | +0.00 | -14.30 |
| 2026-08-18 | `DVN` | 13 | $47.57 | $48.00 | +5.59 | — | +0.00 | +5.59 | +23.66 | — |
| 2026-08-18 | `EOG` | 4 | $146.15 | $148.04 | +7.56 | — | +0.00 | +7.56 | +21.08 | — |
| 2026-08-18 | `FANG` | 3 | $206.29 | $208.93 | +7.92 | — | +0.00 | +7.92 | +18.69 | — |
| 2026-08-18 | `TMC` | 155 | $3.77 | $3.72 | -7.75 | — | +0.00 | -7.75 | -51.15 | — |
| 2026-08-18 | `TGB` | 74 | $8.77 | $8.55 | -16.28 | — | +0.00 | -16.28 | +6.66 | — |
| 2026-08-18 | `ELF` | 6 | $93.66 | $93.44 | -1.32 | — | +0.00 | -1.32 | +17.40 | — |
| 2026-08-18 | `DNN` | 193 | $3.19 | $3.11 | -15.44 | — | +0.00 | -15.44 | -25.09 | — |
| 2026-08-18 | `HNST` | 130 | $4.70 | $4.67 | -3.90 | — | +0.00 | -3.90 | -18.20 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `AG` | 30 | — | $20.55 | +0.00 | $21.19 | +19.20 | +19.20 | +0.00 | +19.20 |
| 2026-08-20 | `BHP` | 6 | — | $91.01 | +0.00 | $93.63 | +15.72 | +15.72 | +0.00 | +15.72 |
| 2026-08-20 | `CDE` | 30 | — | $20.65 | +0.00 | $21.11 | +13.80 | +13.80 | +0.00 | +13.80 |
| 2026-08-20 | `HDSN` | 108 | — | $5.77 | +0.00 | $5.57 | -21.60 | -21.60 | +0.00 | -21.60 |
| 2026-08-20 | `IAG` | 31 | — | $19.63 | +0.00 | $20.50 | +26.97 | +26.97 | +0.00 | +26.97 |
| 2026-08-20 | `KGC` | 21 | — | $29.63 | +0.00 | $31.43 | +37.80 | +37.80 | +0.00 | +37.80 |
| 2026-08-20 | `NFGC` | 357 | — | $1.75 | +0.00 | $1.75 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `WPM` | 4 | — | $144.54 | +0.00 | $150.25 | +22.84 | +22.84 | +0.00 | +22.84 |
| 2026-08-21 | `AG` | 30 | $21.19 | $21.90 | +21.30 | — | +0.00 | +21.30 | +40.50 | — |
| 2026-08-21 | `BHP` | 6 | $93.63 | $95.72 | +12.54 | — | +0.00 | +12.54 | +28.26 | — |
| 2026-08-21 | `CDE` | 30 | $21.11 | $21.75 | +19.20 | — | +0.00 | +19.20 | +33.00 | — |
| 2026-08-21 | `HDSN` | 108 | $5.57 | $5.67 | +10.80 | — | +0.00 | +10.80 | -10.80 | — |
| 2026-08-21 | `IAG` | 31 | $20.50 | $21.17 | +20.77 | — | +0.00 | +20.77 | +47.74 | — |
| 2026-08-21 | `KGC` | 21 | $31.43 | $32.17 | +15.54 | — | +0.00 | +15.54 | +53.34 | — |
| 2026-08-21 | `NFGC` | 357 | $1.75 | $1.79 | +14.28 | — | +0.00 | +14.28 | +14.28 | — |
| 2026-08-21 | `WPM` | 4 | $150.25 | $154.70 | +17.80 | — | +0.00 | +17.80 | +40.64 | — |
| 2026-08-21 | `AU` | 5 | — | $119.43 | +0.00 | $121.22 | +8.95 | +8.95 | +0.00 | +8.95 |
| 2026-08-21 | `AUPH` | 37 | — | $17.20 | +0.00 | $16.65 | -20.35 | -20.35 | +0.00 | -20.35 |
| 2026-08-21 | `AEM` | 2 | — | $216.30 | +0.00 | $216.06 | -0.48 | -0.48 | +0.00 | -0.48 |
| 2026-08-21 | `ARCT` | 57 | — | $11.13 | +0.00 | $13.45 | +132.24 | +132.24 | +0.00 | +132.24 |
| 2026-08-21 | `AUTL` | 258 | — | $2.47 | +0.00 | $2.41 | -15.48 | -15.48 | +0.00 | -15.48 |
| 2026-08-21 | `CRDL` | 330 | — | $1.93 | +0.00 | $1.86 | -23.10 | -23.10 | +0.00 | -23.10 |
| 2026-08-21 | `CRSP` | 10 | — | $59.72 | +0.00 | $59.50 | -2.20 | -2.20 | +0.00 | -2.20 |
| 2026-08-21 | `CYPH` | 483 | — | $1.32 | +0.00 | $1.42 | +48.30 | +48.30 | +0.00 | +48.30 |
| 2026-08-24 | `AU` | 5 | $121.22 | $120.50 | -3.60 | — | +0.00 | -3.60 | +5.35 | — |
| 2026-08-24 | `AUPH` | 37 | $16.65 | $16.60 | -1.85 | — | +0.00 | -1.85 | -22.20 | — |
| 2026-08-24 | `AEM` | 2 | $216.06 | $217.03 | +1.94 | — | +0.00 | +1.94 | +1.46 | — |
| 2026-08-24 | `ARCT` | 57 | $13.45 | $13.26 | -10.83 | — | +0.00 | -10.83 | +121.41 | — |
| 2026-08-24 | `AUTL` | 258 | $2.41 | $2.36 | -12.90 | — | +0.00 | -12.90 | -28.38 | — |
| 2026-08-24 | `CRDL` | 330 | $1.86 | $1.87 | +3.30 | — | +0.00 | +3.30 | -19.80 | — |
| 2026-08-24 | `CRSP` | 10 | $59.50 | $58.79 | -7.10 | — | +0.00 | -7.10 | -9.30 | — |
| 2026-08-24 | `CYPH` | 483 | $1.42 | $1.83 | +198.03 | — | +0.00 | +198.03 | +246.33 | — |
| 2026-08-25 | `MOS` | 27 | — | $24.00 | +0.00 | $23.75 | -6.75 | -6.75 | +0.00 | -6.75 |
| 2026-08-25 | `OCUL` | 59 | — | $10.92 | +0.00 | $10.92 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-25 | `INSP` | 10 | — | $61.47 | +0.00 | $61.47 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-25 | `CRMD` | 78 | — | $8.28 | +0.00 | $8.28 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-25 | `RZLT` | 125 | — | $5.23 | +0.00 | $5.29 | +7.50 | +7.50 | +0.00 | +7.50 |
| 2026-08-25 | `HCA` | 1 | — | $429.24 | +0.00 | $428.50 | -0.74 | -0.74 | +0.00 | -0.74 |
| 2026-08-25 | `BMEA` | 403 | — | $1.62 | +0.00 | $1.61 | -4.03 | -4.03 | +0.00 | -4.03 |
| 2026-08-25 | `NPWR` | 326 | — | $2.00 | +0.00 | $2.02 | +6.52 | +6.52 | +0.00 | +6.52 |
| 2026-08-26 | `MOS` | 27 | $23.75 | $23.75 | +0.00 | $23.75 | +0.00 | +0.00 | -6.75 | -6.75 |
| 2026-08-26 | `OCUL` | 59 | $10.92 | $10.92 | +0.00 | $10.92 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-26 | `INSP` | 10 | $61.47 | $61.47 | +0.00 | $61.47 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-26 | `CRMD` | 78 | $8.28 | $8.28 | +0.00 | $8.28 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-26 | `RZLT` | 125 | $5.29 | $5.29 | +0.00 | $5.29 | +0.00 | +0.00 | +7.50 | +7.50 |
| 2026-08-26 | `HCA` | 1 | $428.50 | $428.50 | +0.00 | $428.50 | +0.00 | +0.00 | -0.74 | -0.74 |
| 2026-08-26 | `BMEA` | 403 | $1.61 | $1.61 | +0.00 | $1.61 | +0.00 | +0.00 | -4.03 | -4.03 |
| 2026-08-26 | `NPWR` | 326 | $2.02 | $2.02 | +0.00 | $2.02 | +0.00 | +0.00 | +6.52 | +6.52 |
| 2026-08-27 | `MOS` | 27 | $23.75 | $24.84 | +29.43 | $24.16 | -18.36 | +11.07 | +22.68 | +4.32 |
| 2026-08-27 | `OCUL` | 59 | $10.92 | $10.79 | -7.67 | — | +0.00 | -7.67 | -7.67 | — |
| 2026-08-27 | `INSP` | 10 | $61.47 | $60.07 | -14.00 | — | +0.00 | -14.00 | -14.00 | — |
| 2026-08-27 | `CRMD` | 78 | $8.28 | $8.60 | +24.96 | — | +0.00 | +24.96 | +24.96 | — |
| 2026-08-27 | `RZLT` | 125 | $5.29 | $5.01 | -35.00 | — | +0.00 | -35.00 | -27.50 | — |
| 2026-08-27 | `HCA` | 1 | $428.50 | $427.50 | -1.00 | — | +0.00 | -1.00 | -1.74 | — |
| 2026-08-27 | `BMEA` | 403 | $1.61 | $1.75 | +56.42 | — | +0.00 | +56.42 | +52.39 | — |
| 2026-08-27 | `NPWR` | 326 | $2.02 | $1.93 | -29.34 | — | +0.00 | -29.34 | -22.82 | — |
| 2026-08-27 | `RRC` | 17 | — | $40.72 | +0.00 | $41.55 | +14.11 | +14.11 | +0.00 | +14.11 |
| 2026-08-27 | `CRK` | 49 | — | $14.09 | +0.00 | $14.50 | +20.09 | +20.09 | +0.00 | +20.09 |
| 2026-08-27 | `SLI` | 269 | — | $2.59 | +0.00 | $2.61 | +5.38 | +5.38 | +0.00 | +5.38 |
| 2026-08-27 | `ACMR` | 8 | — | $80.97 | +0.00 | $79.11 | -14.88 | -14.88 | +0.00 | -14.88 |
| 2026-08-27 | `GGB` | 157 | — | $4.42 | +0.00 | $4.46 | +6.28 | +6.28 | +0.00 | +6.28 |
| 2026-08-27 | `MT` | 9 | — | $75.12 | +0.00 | $74.53 | -5.31 | -5.31 | +0.00 | -5.31 |
| 2026-08-28 | `MOS` | 27 | $24.16 | $24.00 | -4.32 | $23.76 | -6.48 | -10.80 | +0.00 | -6.48 |
| 2026-08-28 | `RRC` | 17 | $41.55 | $41.44 | -1.87 | $41.64 | +3.40 | +1.53 | +12.24 | +15.64 |
| 2026-08-28 | `CRK` | 49 | $14.50 | $14.42 | -3.92 | $14.62 | +9.80 | +5.88 | +16.17 | +25.97 |
| 2026-08-28 | `SLI` | 269 | $2.61 | $2.60 | -2.69 | $2.64 | +10.76 | +8.07 | +2.69 | +13.45 |
| 2026-08-28 | `ACMR` | 8 | $79.11 | $81.65 | +20.32 | — | +0.00 | +20.32 | +5.44 | — |
| 2026-08-28 | `GGB` | 157 | $4.46 | $4.57 | +17.27 | — | +0.00 | +17.27 | +23.55 | — |
| 2026-08-28 | `MT` | 9 | $74.53 | $74.54 | +0.09 | — | +0.00 | +0.09 | -5.22 | — |
| 2026-08-28 | `ANF` | 6 | — | $144.70 | +0.00 | $145.75 | +6.30 | +6.30 | +0.00 | +6.30 |
| 2026-08-28 | `BHVN` | 56 | — | $16.95 | +0.00 | $16.12 | -46.48 | -46.48 | +0.00 | -46.48 |
| 2026-08-28 | `BZ` | 52 | — | $18.50 | +0.00 | $18.00 | -26.00 | -26.00 | +0.00 | -26.00 |
| 2026-08-28 | `CAPR` | 104 | — | $9.19 | +0.00 | $10.06 | +90.48 | +90.48 | +0.00 | +90.48 |
| 2026-08-31 | `MOS` | 27 | $23.76 | $23.75 | -0.27 | — | +0.00 | -0.27 | -6.75 | — |
| 2026-08-31 | `RRC` | 17 | $41.64 | $41.11 | -9.01 | — | +0.00 | -9.01 | +6.63 | — |
| 2026-08-31 | `CRK` | 49 | $14.62 | $14.56 | -2.94 | — | +0.00 | -2.94 | +23.03 | — |
| 2026-08-31 | `SLI` | 269 | $2.64 | $2.51 | -34.97 | — | +0.00 | -34.97 | -21.52 | — |
| 2026-08-31 | `ANF` | 6 | $145.75 | $148.67 | +17.52 | — | +0.00 | +17.52 | +23.82 | — |
| 2026-08-31 | `BHVN` | 56 | $16.12 | $15.44 | -38.08 | — | +0.00 | -38.08 | -84.56 | — |
| 2026-08-31 | `BZ` | 52 | $18.00 | $17.89 | -5.72 | — | +0.00 | -5.72 | -31.72 | — |
| 2026-08-31 | `CAPR` | 104 | $10.06 | $9.44 | -64.48 | — | +0.00 | -64.48 | +26.00 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `ATRC` | 12 | — | $49.76 | +0.00 | $52.59 | +33.96 | +33.96 | +0.00 | +33.96 |
| 2026-09-03 | `HRMY` | 15 | — | $41.31 | +0.00 | $42.86 | +23.25 | +23.25 | +0.00 | +23.25 |
| 2026-09-03 | `CABA` | 197 | — | $3.27 | +0.00 | $3.57 | +59.10 | +59.10 | +0.00 | +59.10 |
| 2026-09-03 | `VSTM` | 83 | — | $7.70 | +0.00 | $8.02 | +26.56 | +26.56 | +0.00 | +26.56 |
| 2026-09-03 | `RVTY` | 5 | — | $125.94 | +0.00 | $130.94 | +25.00 | +25.00 | +0.00 | +25.00 |
| 2026-09-03 | `GPRO` | 529 | — | $1.22 | +0.00 | $1.69 | +248.63 | +248.63 | +0.00 | +248.63 |
| 2026-09-03 | `FRVO` | 35 | — | $18.40 | +0.00 | $17.98 | -14.70 | -14.70 | +0.00 | -14.70 |
| 2026-09-03 | `CRK` | 41 | — | $15.70 | +0.00 | $15.54 | -6.56 | -6.56 | +0.00 | -6.56 |
| 2026-09-04 | `ATRC` | 12 | $52.59 | $52.88 | +3.48 | $52.46 | -5.04 | -1.56 | +37.44 | +32.40 |
| 2026-09-04 | `HRMY` | 15 | $42.86 | $42.93 | +1.05 | — | +0.00 | +1.05 | +24.30 | — |
| 2026-09-04 | `CABA` | 197 | $3.57 | $3.63 | +11.82 | $3.48 | -29.55 | -17.73 | +70.92 | +41.37 |
| 2026-09-04 | `VSTM` | 83 | $8.02 | $8.03 | +0.83 | — | +0.00 | +0.83 | +27.39 | — |
| 2026-09-04 | `RVTY` | 5 | $130.94 | $132.45 | +7.55 | — | +0.00 | +7.55 | +32.55 | — |
| 2026-09-04 | `GPRO` | 529 | $1.69 | $1.78 | +47.61 | $1.39 | -206.31 | -158.70 | +296.24 | +89.93 |
| 2026-09-04 | `FRVO` | 35 | $17.98 | $18.27 | +10.15 | — | +0.00 | +10.15 | -4.55 | — |
| 2026-09-04 | `CRK` | 41 | $15.54 | $15.45 | -3.69 | — | +0.00 | -3.69 | -10.25 | — |
| 2026-09-04 | `ASND` | 3 | — | $266.94 | +0.00 | $271.12 | +12.54 | +12.54 | +0.00 | +12.54 |
| 2026-09-04 | `OSCR` | 27 | — | $30.65 | +0.00 | $32.24 | +42.93 | +42.93 | +0.00 | +42.93 |
| 2026-09-04 | `NVAX` | 81 | — | $10.41 | +0.00 | $10.34 | -5.67 | -5.67 | +0.00 | -5.67 |
| 2026-09-04 | `BVS` | 58 | — | $14.50 | +0.00 | $14.36 | -8.12 | -8.12 | +0.00 | -8.12 |
| 2026-09-04 | `BAK` | 435 | — | $1.95 | +0.00 | $1.94 | -4.35 | -4.35 | +0.00 | -4.35 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +94.08 | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | — | $5,101.72 | $10,071.15 | BTSG×10, IREN×13, TPG×12, TGTX×12, SLS×53, HIMS×21, INO×771, TNDM×26 |
| 2026-08-14 | +5.50 | $5,101.72 | BTSG×10, IREN×13, TPG×12, TGTX×12, SLS×53, HIMS×21, INO×771, TNDM×26 | $10,084.41 | +13.26 | +43.09 | TLN, VST, NRG, DAVE, SLG, MARA, LDI, BTBT | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | $5,701.01 | $10,077.45 | TLN×1, VST×4, NRG×5, DAVE×1, SLG×10, MARA×69, LDI×671, BTBT×419 |
| 2026-08-17 | +2.25 | $5,701.01 | TLN×1, VST×4, NRG×5, DAVE×1, SLG×10, MARA×69, LDI×671, BTBT×419 | $10,075.66 | -1.79 | +16.67 | DVN, EOG, FANG, TMC, TGB, ELF, DNN, HNST | TLN, VST, NRG, DAVE, SLG, MARA, LDI, BTBT | $5,204.80 | $10,048.64 | DVN×13, EOG×4, FANG×3, TMC×155, TGB×74, ELF×6, DNN×193, HNST×130 |
| 2026-08-18 | -6.20 | $5,204.80 | DVN×13, EOG×4, FANG×3, TMC×155, TGB×74, ELF×6, DNN×193, HNST×130 | $10,025.02 | -23.62 | +0.00 | — | DVN, EOG, FANG, TMC, TGB, ELF, DNN, HNST | $10,007.16 | $10,007.16 | — |
| 2026-08-19 | -7.20 | $10,007.16 | — | $10,007.16 | -0.00 | +0.00 | — | — | $10,007.16 | $10,007.16 | — |
| 2026-08-20 | +1.12 | $10,007.16 | — | $10,007.16 | -0.00 | +114.73 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | — | $5,149.04 | $10,102.66 | AG×30, BHP×6, CDE×30, HDSN×108, IAG×31, KGC×21, NFGC×357, WPM×4 |
| 2026-08-21 | +3.25 | $5,149.04 | AG×30, BHP×6, CDE×30, HDSN×108, IAG×31, KGC×21, NFGC×357, WPM×4 | $10,234.89 | +132.23 | +127.88 | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $5,381.87 | $10,319.23 | AU×5, AUPH×37, AEM×2, ARCT×57, AUTL×258, CRDL×330, CRSP×10, CYPH×483 |
| 2026-08-24 | -5.17 | $5,381.87 | AU×5, AUPH×37, AEM×2, ARCT×57, AUTL×258, CRDL×330, CRSP×10, CYPH×483 | $10,486.22 | +166.99 | +0.00 | — | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | $10,461.82 | $10,461.82 | — |
| 2026-08-25 | +1.80 | $10,461.82 | — | $10,461.82 | -0.00 | +2.50 | MOS, OCUL, INSP, CRMD, RZLT, HCA, BMEA, NPWR | — | $5,498.90 | $10,442.07 | MOS×27, OCUL×59, INSP×10, CRMD×78, RZLT×125, HCA×1, BMEA×403, NPWR×326 |
| 2026-08-26 | +2.02 | $5,498.90 | MOS×27, OCUL×59, INSP×10, CRMD×78, RZLT×125, HCA×1, BMEA×403, NPWR×326 | $10,442.07 | +0.00 | +0.00 | — | — | $5,498.90 | $10,442.07 | MOS×27, OCUL×59, INSP×10, CRMD×78, RZLT×125, HCA×1, BMEA×403, NPWR×326 |
| 2026-08-27 | — | $5,498.90 | MOS×27, OCUL×59, INSP×10, CRMD×78, RZLT×125, HCA×1, BMEA×403, NPWR×326 | $10,465.87 | +23.80 | +7.31 | RRC, CRK, SLI, ACMR, GGB, MT | OCUL, INSP, CRMD, RZLT, HCA, BMEA, NPWR | $5,663.48 | $10,438.61 | MOS×27, RRC×17, CRK×49, SLI×269, ACMR×8, GGB×157, MT×9 |
| 2026-08-28 | +0.75 | $5,663.48 | MOS×27, RRC×17, CRK×49, SLI×269, ACMR×8, GGB×157, MT×9 | $10,463.49 | +24.88 | +41.78 | ANF, BHVN, BZ, CAPR | ACMR, GGB, MT | $3,954.69 | $10,490.09 | MOS×27, RRC×17, CRK×49, SLI×269, ANF×6, BHVN×56, BZ×52, CAPR×104 |
| 2026-08-31 | -5.85 | $3,954.69 | MOS×27, RRC×17, CRK×49, SLI×269, ANF×6, BHVN×56, BZ×52, CAPR×104 | $10,352.14 | -137.95 | +0.00 | — | MOS, RRC, CRK, SLI, ANF, BHVN, BZ, CAPR | $10,333.61 | $10,333.61 | — |
| 2026-09-01 | -6.30 | $10,333.61 | — | $10,333.61 | -0.00 | +0.00 | — | — | $10,333.61 | $10,333.61 | — |
| 2026-09-02 | -3.83 | $10,333.61 | — | $10,333.61 | -0.00 | +0.00 | — | — | $10,333.61 | $10,333.61 | — |
| 2026-09-03 | -0.90 | $10,333.61 | — | $10,333.61 | -0.00 | +395.24 | ATRC, HRMY, CABA, VSTM, RVTY, GPRO, FRVO, CRK | — | $5,248.85 | $10,706.93 | ATRC×12, HRMY×15, CABA×197, VSTM×83, RVTY×5, GPRO×529, FRVO×35, CRK×41 |
| 2026-09-04 | — | $5,248.85 | ATRC×12, HRMY×15, CABA×197, VSTM×83, RVTY×5, GPRO×529, FRVO×35, CRK×41 | $10,785.73 | +78.80 | -203.57 | ASND, OSCR, NVAX, BVS, BAK | HRMY, VSTM, RVTY, FRVO, CRK | $4,308.94 | $10,557.49 | ATRC×12, CABA×197, GPRO×529, ASND×3, OSCR×27, NVAX×81, BVS×58, BAK×435 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 10 | $59.80 | $2.02 | — | $9,399.98 | — | deploy half leftover; list flatten; ⚪; ret5=-5.3; leftover $625.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 13 | $45.98 | $2.03 | — | $8,800.21 | — | deploy half leftover; list flatten; ⚪; ret5=+12.3; leftover $625.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 12 | $50.62 | $2.03 | — | $8,190.71 | — | deploy half leftover; list flatten; ⚪; ret5=+6.2; leftover $625.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 12 | $49.70 | $2.03 | — | $7,592.28 | — | deploy half leftover; list flatten; ⚪; ret5=-0.8; leftover $625.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 53 | $11.70 | $2.15 | — | $6,970.03 | — | deploy half leftover; list flatten; ⚪; ret5=-0.8; leftover $625.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 21 | $29.74 | $2.05 | — | $6,343.44 | — | deploy half leftover; list flatten; ⚪; ret5=-5.3; leftover $625.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 771 | $0.81 | $8.56 | — | $5,710.37 | — | deploy half leftover; list flatten; ⚪; ret5=+13.2; leftover $625.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 26 | $23.33 | $2.07 | — | $5,101.72 | — | deploy half leftover; list flatten; ⚪; ret5=+19.7; leftover $625.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,101.72 | ▲ close $10,071.15 vs 09:30 $10,000.00 (session +94.08) | 16:00 close · cash $5,101.72 · equity $10,071.15 vs 09:30 $10,000.00 (+71.15; session marks +94.08) · 8 name(s) marked open→close (per-name table). BTSG×10 09:30 $59.80 → close $60.23 +4.30; IREN×13 09:30 $45.98 → close $44.76 -15.86; TPG×12 09:30 $50.62 → close $54.62 +47.96; TGTX×12 09:30 $49.70 → close $47.94 -21.12; SLS×53 09:30 $11.70 → close $12.36 +34.98; HIMS×21 09:30 $29.74 → close $28.77 -20.37; INO×771 09:30 $0.81 → close $0.90 +69.39; TNDM×26 09:30 $23.33 → close $23.13 -5.20 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,101.72 | ▲ 09:30 equity $10,084.41 vs yday $10,071.15 (+13.26) | 09:30 open · cash $5,101.72 (unchanged overnight, no fees) · equity $10,084.41 vs prior close $10,071.15 (+13.26) · 8 name(s) re-marked at the open (per-name table). BTSG×10 yday $60.23 → 09:30 $59.65 -5.80; IREN×13 yday $44.76 → 09:30 $44.09 -8.71; TPG×12 yday $54.62 → 09:30 $55.29 +8.04; TGTX×12 yday $47.94 → 09:30 $47.27 -8.04; SLS×53 yday $12.36 → 09:30 $12.40 +2.12; HIMS×21 yday $28.77 → 09:30 $29.15 +7.98; INO×771 yday $0.90 → 09:30 $0.93 +23.13; TNDM×26 yday $23.13 → 09:30 $22.92 -5.46 | — |
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 10 | $59.65 | $2.04 | $-5.56 | $5,696.18 | ▼ -5.56 after sell → book $10,082.37; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 13 | $44.09 | $2.05 | $-28.65 | $6,267.30 | ▼ -28.65 after sell → book $10,080.32; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 12 | $55.29 | $2.05 | $+51.93 | $6,928.74 | ▲ +51.93 after sell → book $10,078.28; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SELL** | `TGTX` | 12 | $47.27 | $2.05 | $-33.23 | $7,493.93 | ▼ -33.23 after sell → book $10,076.23; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `SLS` | 53 | $12.40 | $2.17 | $+32.78 | $8,148.96 | ▲ +32.78 after sell → book $10,074.06; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `HIMS` | 21 | $29.15 | $2.07 | $-16.52 | $8,759.04 | ▼ -16.52 after sell → book $10,071.99; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 771 | $0.93 | $9.62 | $+74.34 | $9,466.45 | ▲ +74.34 after sell → book $10,062.37; vs 09:30 mark -9.62 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 26 | $22.92 | $2.09 | $-14.82 | $10,060.28 | ▼ -14.82 after sell → book $10,060.28; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 1 | $359.83 | $1.99 | — | $9,698.46 | — | deploy half leftover; list flatten; 🔵; ret5=+5.9; leftover $628.77 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 4 | $146.90 | $2.00 | — | $9,108.86 | — | deploy half leftover; list flatten; 🔵; ret5=+3.6; leftover $628.77 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 5 | $120.00 | $2.00 | — | $8,506.85 | — | deploy half leftover; list flatten; 🔵; ret5=+0.6; leftover $628.77 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `DAVE` | 1 | $330.91 | $1.99 | — | $8,173.95 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=-8.6; leftover $628.77 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 10 | $57.61 | $2.02 | — | $7,595.83 | — | deploy half leftover; list flatten; 🔵; ret5=+5.7; leftover $628.77 | join🟢 sector🟢 gen🟢 news🟡 judge🔴 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 69 | $9.01 | $2.20 | — | $6,971.94 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=-13.5; leftover $628.77 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 671 | $0.94 | $8.30 | — | $6,334.91 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=+0.5; leftover $628.77 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 419 | $1.50 | $5.41 | — | $5,701.01 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=+9.2; leftover $628.77 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,701.01 | ▲ close $10,077.45 vs 09:30 $10,084.41 (session +43.09) | 16:00 close · cash $5,701.01 · equity $10,077.45 vs 09:30 $10,084.41 (-6.96; session marks +43.09) · 8 name(s) marked open→close (per-name table). TLN×1 09:30 $359.83 → close $362.74 +2.91; VST×4 09:30 $146.90 → close $148.13 +4.92; NRG×5 09:30 $120.00 → close $126.24 +31.20; DAVE×1 09:30 $330.91 → close $334.57 +3.66; SLG×10 09:30 $57.61 → close $56.09 -15.20; MARA×69 09:30 $9.01 → close $9.20 +13.11; LDI×671 09:30 $0.94 → close $0.90 -26.84; BTBT×419 09:30 $1.50 → close $1.57 +29.33 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,701.01 | ▼ 09:30 equity $10,075.66 vs yday $10,077.45 (-1.79) | 09:30 open · cash $5,701.01 (unchanged overnight, no fees) · equity $10,075.66 vs prior close $10,077.45 (-1.79) · 8 name(s) re-marked at the open (per-name table). TLN×1 yday $362.74 → 09:30 $367.88 +5.14; VST×4 yday $148.13 → 09:30 $149.37 +4.96; NRG×5 yday $126.24 → 09:30 $127.40 +5.80; DAVE×1 yday $334.57 → 09:30 $336.94 +2.37; SLG×10 yday $56.09 → 09:30 $55.37 -7.20; MARA×69 yday $9.20 → 09:30 $9.22 +1.38; LDI×671 yday $0.90 → 09:30 $0.91 +6.71; BTBT×419 yday $1.57 → 09:30 $1.52 -20.95 | — |
| 2026-08-17 09:30 ET | **SELL** | `TLN` | 1 | $367.88 | $2.01 | $+4.04 | $6,066.87 | ▲ +4.04 after sell → book $10,073.65; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VST` | 4 | $149.37 | $2.02 | $+5.86 | $6,662.33 | ▲ +5.86 after sell → book $10,071.63; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NRG` | 5 | $127.40 | $2.02 | $+32.97 | $7,297.31 | ▲ +32.97 after sell → book $10,069.60; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `DAVE` | 1 | $336.94 | $2.01 | $+2.02 | $7,632.23 | ▲ +2.02 after sell → book $10,067.59; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SLG` | 10 | $55.37 | $2.04 | $-26.46 | $8,183.89 | ▼ -26.46 after sell → book $10,065.55; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MARA` | 69 | $9.22 | $2.22 | $+10.07 | $8,817.86 | ▲ +10.07 after sell → book $10,063.33; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LDI` | 671 | $0.91 | $8.22 | $-36.65 | $9,418.23 | ▼ -36.65 after sell → book $10,055.11; vs 09:30 mark -8.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 419 | $1.52 | $5.48 | $-2.51 | $10,049.63 | ▼ -2.51 after sell → book $10,049.63; vs 09:30 mark -5.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 13 | $46.18 | $2.03 | — | $9,447.26 | — | deploy half leftover; list flatten; 🔵; ret5=+6.7; leftover $628.10 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 4 | $142.77 | $2.00 | — | $8,874.18 | — | deploy half leftover; list flatten; 🔵; ret5=+5.8; leftover $628.10 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 3 | $202.70 | $2.00 | — | $8,264.08 | — | deploy half leftover; list flatten; 🔵; ret5=+8.3; leftover $628.10 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 155 | $4.05 | $2.46 | — | $7,633.87 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=-12.3; leftover $628.10 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 74 | $8.46 | $2.21 | — | $7,005.62 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=+0.4; leftover $628.10 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ELF` | 6 | $90.54 | $2.01 | — | $6,460.37 | — | deploy half leftover; list flatten; ret5=-7.2; leftover $628.10 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 193 | $3.24 | $2.57 | — | $5,832.48 | — | deploy half leftover; list flatten; ⚪; ret5=+0.3; leftover $628.10 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HNST` | 130 | $4.81 | $2.38 | — | $5,204.80 | — | deploy half leftover; list flatten; ⚪; ret5=-11.4; leftover $628.10 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,204.80 | ▲ close $10,048.64 vs 09:30 $10,075.66 (session +16.67) | 16:00 close · cash $5,204.80 · equity $10,048.64 vs 09:30 $10,075.66 (-27.02; session marks +16.67) · 8 name(s) marked open→close (per-name table). DVN×13 09:30 $46.18 → close $47.57 +18.07; EOG×4 09:30 $142.77 → close $146.15 +13.52; FANG×3 09:30 $202.70 → close $206.29 +10.77; TMC×155 09:30 $4.05 → close $3.77 -43.40; TGB×74 09:30 $8.46 → close $8.77 +22.94; ELF×6 09:30 $90.54 → close $93.66 +18.72; DNN×193 09:30 $3.24 → close $3.19 -9.65; HNST×130 09:30 $4.81 → close $4.70 -14.30 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,204.80 | ▼ 09:30 equity $10,025.02 vs yday $10,048.64 (-23.62) | 09:30 open · cash $5,204.80 (unchanged overnight, no fees) · equity $10,025.02 vs prior close $10,048.64 (-23.62) · 8 name(s) re-marked at the open (per-name table). DVN×13 yday $47.57 → 09:30 $48.00 +5.59; EOG×4 yday $146.15 → 09:30 $148.04 +7.56; FANG×3 yday $206.29 → 09:30 $208.93 +7.92; TMC×155 yday $3.77 → 09:30 $3.72 -7.75; TGB×74 yday $8.77 → 09:30 $8.55 -16.28; ELF×6 yday $93.66 → 09:30 $93.44 -1.32; DNN×193 yday $3.19 → 09:30 $3.11 -15.44; HNST×130 yday $4.70 → 09:30 $4.67 -3.90 | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 13 | $48.00 | $2.05 | $+19.58 | $5,826.76 | ▲ +19.58 after sell → book $10,022.98; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 4 | $148.04 | $2.02 | $+17.06 | $6,416.89 | ▲ +17.06 after sell → book $10,020.95; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 3 | $208.93 | $2.02 | $+14.67 | $7,041.66 | ▲ +14.67 after sell → book $10,018.93; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TMC` | 155 | $3.72 | $2.49 | $-56.10 | $7,615.77 | ▼ -56.10 after sell → book $10,016.44; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGB` | 74 | $8.55 | $2.23 | $+2.21 | $8,246.24 | ▲ +2.21 after sell → book $10,014.21; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ELF` | 6 | $93.44 | $2.03 | $+13.36 | $8,804.85 | ▲ +13.36 after sell → book $10,012.18; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `DNN` | 193 | $3.11 | $2.61 | $-30.27 | $9,402.47 | ▼ -30.27 after sell → book $10,009.57; vs 09:30 mark -2.61 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 vol🟢 buy🟢 |
| 2026-08-18 09:30 ET | **SELL** | `HNST` | 130 | $4.67 | $2.41 | $-22.99 | $10,007.16 | ▼ -22.99 after sell → book $10,007.16; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,007.16 | ▲ close $10,007.16 vs 09:30 $10,025.02 (session +0.00) | 16:00 close · cash $10,007.16 · no lots left · equity $10,007.16. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,007.16 | ▲ 09:30 equity $10,007.16 vs yday $10,007.16 (-0.00) | 09:30 open · cash $10,007.16 · no holdings · equity $10,007.16 vs prior close $10,007.16 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,007.16 | ▲ close $10,007.16 vs 09:30 $10,007.16 (session +0.00) | 16:00 close · cash $10,007.16 · no lots left · equity $10,007.16. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,007.16 | ▲ 09:30 equity $10,007.16 vs yday $10,007.16 (-0.00) | 09:30 open · cash $10,007.16 · no holdings · equity $10,007.16 vs prior close $10,007.16 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 30 | $20.55 | $2.08 | — | $9,388.58 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $625.45 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 6 | $91.01 | $2.01 | — | $8,840.51 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $625.45 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 30 | $20.65 | $2.08 | — | $8,218.93 | — | deploy half leftover; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $625.45 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 108 | $5.77 | $2.31 | — | $7,593.46 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $625.45 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 31 | $19.63 | $2.08 | — | $6,982.84 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $625.45 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 21 | $29.63 | $2.05 | — | $6,358.56 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $625.45 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 357 | $1.75 | $4.61 | — | $5,729.21 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $625.45 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 4 | $144.54 | $2.00 | — | $5,149.04 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $625.45 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,149.04 | ▲ close $10,102.66 vs 09:30 $10,007.16 (session +114.73) | 16:00 close · cash $5,149.04 · equity $10,102.66 vs 09:30 $10,007.16 (+95.50; session marks +114.73) · 8 name(s) marked open→close (per-name table). AG×30 09:30 $20.55 → close $21.19 +19.20; BHP×6 09:30 $91.01 → close $93.63 +15.72; CDE×30 09:30 $20.65 → close $21.11 +13.80; HDSN×108 09:30 $5.77 → close $5.57 -21.60; IAG×31 09:30 $19.63 → close $20.50 +26.97; KGC×21 09:30 $29.63 → close $31.43 +37.80; NFGC×357 09:30 $1.75 → close $1.75 +0.00; WPM×4 09:30 $144.54 → close $150.25 +22.84 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,149.04 | ▲ 09:30 equity $10,234.89 vs yday $10,102.66 (+132.23) | 09:30 open · cash $5,149.04 (unchanged overnight, no fees) · equity $10,234.89 vs prior close $10,102.66 (+132.23) · 8 name(s) re-marked at the open (per-name table). AG×30 yday $21.19 → 09:30 $21.90 +21.30; BHP×6 yday $93.63 → 09:30 $95.72 +12.54; CDE×30 yday $21.11 → 09:30 $21.75 +19.20; HDSN×108 yday $5.57 → 09:30 $5.67 +10.80; IAG×31 yday $20.50 → 09:30 $21.17 +20.77; KGC×21 yday $31.43 → 09:30 $32.17 +15.54; NFGC×357 yday $1.75 → 09:30 $1.79 +14.28; WPM×4 yday $150.25 → 09:30 $154.70 +17.80 | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 30 | $21.90 | $2.10 | $+36.32 | $5,803.94 | ▲ +36.32 after sell → book $10,232.79; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 6 | $95.72 | $2.03 | $+24.22 | $6,376.24 | ▲ +24.22 after sell → book $10,230.77; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 30 | $21.75 | $2.10 | $+28.82 | $7,026.64 | ▲ +28.82 after sell → book $10,228.67; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 108 | $5.67 | $2.34 | $-15.46 | $7,636.65 | ▼ -15.46 after sell → book $10,226.32; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 31 | $21.17 | $2.10 | $+43.55 | $8,290.82 | ▲ +43.55 after sell → book $10,224.22; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 21 | $32.17 | $2.07 | $+49.21 | $8,964.32 | ▲ +49.21 after sell → book $10,222.15; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 357 | $1.79 | $4.67 | $+5.00 | $9,598.67 | ▲ +5.00 after sell → book $10,217.47; vs 09:30 mark -4.68 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 4 | $154.70 | $2.02 | $+36.62 | $10,215.45 | ▲ +36.62 after sell → book $10,215.45; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 5 | $119.43 | $2.00 | — | $9,616.30 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+20.4; leftover $638.47 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 37 | $17.20 | $2.10 | — | $8,977.79 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $638.47 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 2 | $216.30 | $2.00 | — | $8,543.20 | — | deploy half leftover; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $638.47 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 57 | $11.13 | $2.16 | — | $7,906.63 | — | deploy half leftover; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $638.47 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 258 | $2.47 | $3.33 | — | $7,266.04 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $638.47 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 330 | $1.93 | $4.26 | — | $6,624.88 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $638.47 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 10 | $59.72 | $2.02 | — | $6,025.66 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $638.47 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 483 | $1.32 | $6.23 | — | $5,381.87 | — | deploy half leftover; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $638.47 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,381.87 | ▲ close $10,319.23 vs 09:30 $10,234.89 (session +127.88) | 16:00 close · cash $5,381.87 · equity $10,319.23 vs 09:30 $10,234.89 (+84.34; session marks +127.88) · 8 name(s) marked open→close (per-name table). AU×5 09:30 $119.43 → close $121.22 +8.95; AUPH×37 09:30 $17.20 → close $16.65 -20.35; AEM×2 09:30 $216.30 → close $216.06 -0.48; ARCT×57 09:30 $11.13 → close $13.45 +132.24; AUTL×258 09:30 $2.47 → close $2.41 -15.48; CRDL×330 09:30 $1.93 → close $1.86 -23.10; CRSP×10 09:30 $59.72 → close $59.50 -2.20; CYPH×483 09:30 $1.32 → close $1.42 +48.30 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,381.87 | ▲ 09:30 equity $10,486.22 vs yday $10,319.23 (+166.99) | 09:30 open · cash $5,381.87 (unchanged overnight, no fees) · equity $10,486.22 vs prior close $10,319.23 (+166.99) · 8 name(s) re-marked at the open (per-name table). AU×5 yday $121.22 → 09:30 $120.50 -3.60; AUPH×37 yday $16.65 → 09:30 $16.60 -1.85; AEM×2 yday $216.06 → 09:30 $217.03 +1.94; ARCT×57 yday $13.45 → 09:30 $13.26 -10.83; AUTL×258 yday $2.41 → 09:30 $2.36 -12.90; CRDL×330 yday $1.86 → 09:30 $1.87 +3.30; CRSP×10 yday $59.50 → 09:30 $58.79 -7.10; CYPH×483 yday $1.42 → 09:30 $1.83 +198.03 | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 5 | $120.50 | $2.02 | $+1.32 | $5,982.35 | ▲ +1.32 after sell → book $10,484.20; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 37 | $16.60 | $2.12 | $-26.42 | $6,594.43 | ▼ -26.42 after sell → book $10,482.08; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 2 | $217.03 | $2.02 | $-2.55 | $7,026.47 | ▼ -2.55 after sell → book $10,480.06; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 57 | $13.26 | $2.18 | $+117.07 | $7,780.11 | ▲ +117.07 after sell → book $10,477.88; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 258 | $2.36 | $3.38 | $-35.09 | $8,385.61 | ▼ -35.09 after sell → book $10,474.50; vs 09:30 mark -3.38 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRDL` | 330 | $1.87 | $4.32 | $-28.38 | $8,998.39 | ▼ -28.38 after sell → book $10,470.18; vs 09:30 mark -4.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 10 | $58.79 | $2.04 | $-13.36 | $9,584.25 | ▼ -13.36 after sell → book $10,468.14; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 483 | $1.83 | $6.32 | $+233.78 | $10,461.82 | ▲ +233.78 after sell → book $10,461.82; vs 09:30 mark -6.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,461.82 | ▲ close $10,461.82 vs 09:30 $10,486.22 (session +0.00) | 16:00 close · cash $10,461.82 · no lots left · equity $10,461.82. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,461.82 | ▲ 09:30 equity $10,461.82 vs yday $10,461.82 (-0.00) | 09:30 open · cash $10,461.82 · no holdings · equity $10,461.82 vs prior close $10,461.82 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 27 | $24.00 | $2.07 | — | $9,811.74 | — | deploy half leftover; list flatten; ⚪; ret5=+13.0; leftover $653.86 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 59 | $10.92 | $2.17 | — | $9,165.30 | — | deploy half leftover; list flatten; 🔵; ret5=+10.4; leftover $653.86 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 10 | $61.47 | $2.02 | — | $8,548.58 | — | deploy half leftover; list flatten; 🔵; ret5=+9.2; leftover $653.86 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 78 | $8.28 | $2.22 | — | $7,900.51 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=+8.8; leftover $653.86 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 125 | $5.23 | $2.37 | — | $7,244.40 | — | deploy half leftover; list flatten; ret5=+10.7; leftover $653.86 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 1 | $429.24 | $1.99 | — | $6,813.17 | — | deploy half leftover; list flatten; ret5=+6.1; leftover $653.86 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 403 | $1.62 | $5.20 | — | $6,155.11 | — | deploy half leftover; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $653.86 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 326 | $2.00 | $4.21 | — | $5,498.90 | — | deploy half leftover; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.8; leftover $653.86 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,498.90 | ▲ close $10,442.07 vs 09:30 $10,461.82 (session +2.50) | 16:00 close · cash $5,498.90 · equity $10,442.07 vs 09:30 $10,461.82 (-19.75; session marks +2.50) · 8 name(s) marked open→close (per-name table). MOS×27 09:30 $24.00 → close $23.75 -6.75; OCUL×59 09:30 $10.92 → close $10.92 +0.00; INSP×10 09:30 $61.47 → close $61.47 +0.00; CRMD×78 09:30 $8.28 → close $8.28 +0.00; RZLT×125 09:30 $5.23 → close $5.29 +7.50; HCA×1 09:30 $429.24 → close $428.50 -0.74; BMEA×403 09:30 $1.62 → close $1.61 -4.03; NPWR×326 09:30 $2.00 → close $2.02 +6.52 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,498.90 | ▲ 09:30 equity $10,442.07 vs yday $10,442.07 (+0.00) | 09:30 open · cash $5,498.90 (unchanged overnight, no fees) · equity $10,442.07 vs prior close $10,442.07 (+0.00) · 8 name(s) re-marked at the open (per-name table). MOS×27 yday $23.75 → 09:30 $23.75 +0.00; OCUL×59 yday $10.92 → 09:30 $10.92 +0.00; INSP×10 yday $61.47 → 09:30 $61.47 +0.00; CRMD×78 yday $8.28 → 09:30 $8.28 +0.00; RZLT×125 yday $5.29 → 09:30 $5.29 +0.00; HCA×1 yday $428.50 → 09:30 $428.50 +0.00; BMEA×403 yday $1.61 → 09:30 $1.61 +0.00; NPWR×326 yday $2.02 → 09:30 $2.02 +0.00 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,498.90 | ▲ close $10,442.07 vs 09:30 $10,442.07 (session +0.00) | 16:00 close · cash $5,498.90 · equity $10,442.07 vs 09:30 $10,442.07 (+0.00; session marks +0.00) · 8 name(s) marked open→close (per-name table). MOS×27 09:30 $23.75 → close $23.75 +0.00; OCUL×59 09:30 $10.92 → close $10.92 +0.00; INSP×10 09:30 $61.47 → close $61.47 +0.00; CRMD×78 09:30 $8.28 → close $8.28 +0.00; RZLT×125 09:30 $5.29 → close $5.29 +0.00; HCA×1 09:30 $428.50 → close $428.50 +0.00; BMEA×403 09:30 $1.61 → close $1.61 +0.00; NPWR×326 09:30 $2.02 → close $2.02 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,498.90 | ▲ 09:30 equity $10,465.87 vs yday $10,442.07 (+23.80) | 09:30 open · cash $5,498.90 (unchanged overnight, no fees) · equity $10,465.87 vs prior close $10,442.07 (+23.80) · 8 name(s) re-marked at the open (per-name table). MOS×27 yday $23.75 → 09:30 $24.84 +29.43; OCUL×59 yday $10.92 → 09:30 $10.79 -7.67; INSP×10 yday $61.47 → 09:30 $60.07 -14.00; CRMD×78 yday $8.28 → 09:30 $8.60 +24.96; RZLT×125 yday $5.29 → 09:30 $5.01 -35.00; HCA×1 yday $428.50 → 09:30 $427.50 -1.00; BMEA×403 yday $1.61 → 09:30 $1.75 +56.42; NPWR×326 yday $2.02 → 09:30 $1.93 -29.34 | — |
| 2026-08-27 09:30 ET | **SELL** | `OCUL` | 59 | $10.79 | $2.19 | $-12.02 | $6,133.32 | ▼ -12.02 after sell → book $10,463.68; vs 09:30 mark -2.19 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `INSP` | 10 | $60.07 | $2.04 | $-18.06 | $6,731.98 | ▼ -18.06 after sell → book $10,461.64; vs 09:30 mark -2.04 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRMD` | 78 | $8.60 | $2.25 | $+20.49 | $7,400.54 | ▲ +20.49 after sell → book $10,459.40; vs 09:30 mark -2.24 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `RZLT` | 125 | $5.01 | $2.40 | $-32.26 | $8,024.39 | ▼ -32.26 after sell → book $10,457.00; vs 09:30 mark -2.40 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `HCA` | 1 | $427.50 | $2.01 | $-5.75 | $8,449.88 | ▼ -5.75 after sell → book $10,454.99; vs 09:30 mark -2.01 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BMEA` | 403 | $1.75 | $5.28 | $+41.92 | $9,149.85 | ▲ +41.92 after sell → book $10,449.71; vs 09:30 mark -5.28 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `NPWR` | 326 | $1.93 | $4.27 | $-31.29 | $9,774.76 | ▼ -31.29 after sell → book $10,445.44; vs 09:30 mark -4.27 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 17 | $40.72 | $2.04 | — | $9,080.48 | — | deploy half leftover; list flatten; ret5=+1.8; leftover $698.20 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 49 | $14.09 | $2.14 | — | $8,387.94 | — | deploy half leftover; list flatten; ret5=+1.1; leftover $698.20 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 269 | $2.59 | $3.47 | — | $7,687.76 | — | deploy half leftover; list flatten; ret5=+4.2; leftover $698.20 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 8 | $80.97 | $2.01 | — | $7,037.98 | — | deploy half leftover; list mover_buy; 🔵; ret5=-1.3; leftover $698.20 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GGB` | 157 | $4.42 | $2.46 | — | $6,341.58 | — | deploy half leftover; list mover_buy; 🔵; ret5=-8.6; leftover $698.20 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MT` | 9 | $75.12 | $2.02 | — | $5,663.48 | — | deploy half leftover; list mover_buy; 🔵; ret5=-2.2; leftover $698.20 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,663.48 | ▲ close $10,438.61 vs 09:30 $10,465.87 (session +7.31) | 16:00 close · cash $5,663.48 · equity $10,438.61 vs 09:30 $10,465.87 (-27.26; session marks +7.31) · 7 name(s) marked open→close (per-name table). MOS×27 09:30 $24.84 → close $24.16 -18.36; RRC×17 09:30 $40.72 → close $41.55 +14.11; CRK×49 09:30 $14.09 → close $14.50 +20.09; SLI×269 09:30 $2.59 → close $2.61 +5.38; ACMR×8 09:30 $80.97 → close $79.11 -14.88; GGB×157 09:30 $4.42 → close $4.46 +6.28; MT×9 09:30 $75.12 → close $74.53 -5.31 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,663.48 | ▲ 09:30 equity $10,463.49 vs yday $10,438.61 (+24.88) | 09:30 open · cash $5,663.48 (unchanged overnight, no fees) · equity $10,463.49 vs prior close $10,438.61 (+24.88) · 7 name(s) re-marked at the open (per-name table). MOS×27 yday $24.16 → 09:30 $24.00 -4.32; RRC×17 yday $41.55 → 09:30 $41.44 -1.87; CRK×49 yday $14.50 → 09:30 $14.42 -3.92; SLI×269 yday $2.61 → 09:30 $2.60 -2.69; ACMR×8 yday $79.11 → 09:30 $81.65 +20.32; GGB×157 yday $4.46 → 09:30 $4.57 +17.27; MT×9 yday $74.53 → 09:30 $74.54 +0.09 | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 8 | $81.65 | $2.03 | $+1.39 | $6,314.65 | ▲ +1.39 after sell → book $10,461.46; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GGB` | 157 | $4.57 | $2.50 | $+18.59 | $7,029.64 | ▲ +18.59 after sell → book $10,458.96; vs 09:30 mark -2.50 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MT` | 9 | $74.54 | $2.04 | $-9.27 | $7,698.47 | ▼ -9.27 after sell → book $10,456.93; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 6 | $144.70 | $2.01 | — | $6,828.26 | — | deploy half leftover; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $962.31 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BHVN` | 56 | $16.95 | $2.16 | — | $5,876.90 | — | deploy half leftover; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $962.31 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 52 | $18.50 | $2.15 | — | $4,912.75 | — | deploy half leftover; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $962.31 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 104 | $9.19 | $2.30 | — | $3,954.69 | — | deploy half leftover; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $962.31 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,954.69 | ▲ close $10,490.09 vs 09:30 $10,463.49 (session +41.78) | 16:00 close · cash $3,954.69 · equity $10,490.09 vs 09:30 $10,463.49 (+26.60; session marks +41.78) · 8 name(s) marked open→close (per-name table). MOS×27 09:30 $24.00 → close $23.76 -6.48; RRC×17 09:30 $41.44 → close $41.64 +3.40; CRK×49 09:30 $14.42 → close $14.62 +9.80; SLI×269 09:30 $2.60 → close $2.64 +10.76; ANF×6 09:30 $144.70 → close $145.75 +6.30; BHVN×56 09:30 $16.95 → close $16.12 -46.48; BZ×52 09:30 $18.50 → close $18.00 -26.00; CAPR×104 09:30 $9.19 → close $10.06 +90.48 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,954.69 | ▼ 09:30 equity $10,352.14 vs yday $10,490.09 (-137.95) | 09:30 open · cash $3,954.69 (unchanged overnight, no fees) · equity $10,352.14 vs prior close $10,490.09 (-137.95) · 8 name(s) re-marked at the open (per-name table). MOS×27 yday $23.76 → 09:30 $23.75 -0.27; RRC×17 yday $41.64 → 09:30 $41.11 -9.01; CRK×49 yday $14.62 → 09:30 $14.56 -2.94; SLI×269 yday $2.64 → 09:30 $2.51 -34.97; ANF×6 yday $145.75 → 09:30 $148.67 +17.52; BHVN×56 yday $16.12 → 09:30 $15.44 -38.08; BZ×52 yday $18.00 → 09:30 $17.89 -5.72; CAPR×104 yday $10.06 → 09:30 $9.44 -64.48 | — |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 27 | $23.75 | $2.09 | $-10.91 | $4,593.85 | ▼ -10.91 after sell → book $10,350.05; vs 09:30 mark -2.09 | dropped from list after 4 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `RRC` | 17 | $41.11 | $2.06 | $+2.53 | $5,290.66 | ▲ +2.53 after sell → book $10,347.99; vs 09:30 mark -2.06 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CRK` | 49 | $14.56 | $2.16 | $+18.74 | $6,001.94 | ▲ +18.74 after sell → book $10,345.83; vs 09:30 mark -2.16 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SLI` | 269 | $2.51 | $3.52 | $-28.51 | $6,673.61 | ▼ -28.51 after sell → book $10,342.31; vs 09:30 mark -3.52 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 6 | $148.67 | $2.03 | $+19.78 | $7,563.60 | ▲ +19.78 after sell → book $10,340.28; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BHVN` | 56 | $15.44 | $2.18 | $-88.90 | $8,426.06 | ▼ -88.90 after sell → book $10,338.10; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BZ` | 52 | $17.89 | $2.17 | $-36.03 | $9,354.18 | ▼ -36.03 after sell → book $10,335.94; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 104 | $9.44 | $2.33 | $+21.37 | $10,333.61 | ▲ +21.37 after sell → book $10,333.61; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,333.61 | ▲ close $10,333.61 vs 09:30 $10,352.14 (session +0.00) | 16:00 close · cash $10,333.61 · no lots left · equity $10,333.61. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,333.61 | ▲ 09:30 equity $10,333.61 vs yday $10,333.61 (-0.00) | 09:30 open · cash $10,333.61 · no holdings · equity $10,333.61 vs prior close $10,333.61 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,333.61 | ▲ close $10,333.61 vs 09:30 $10,333.61 (session +0.00) | 16:00 close · cash $10,333.61 · no lots left · equity $10,333.61. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,333.61 | ▲ 09:30 equity $10,333.61 vs yday $10,333.61 (-0.00) | 09:30 open · cash $10,333.61 · no holdings · equity $10,333.61 vs prior close $10,333.61 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,333.61 | ▲ close $10,333.61 vs 09:30 $10,333.61 (session +0.00) | 16:00 close · cash $10,333.61 · no lots left · equity $10,333.61. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,333.61 | ▲ 09:30 equity $10,333.61 vs yday $10,333.61 (-0.00) | 09:30 open · cash $10,333.61 · no holdings · equity $10,333.61 vs prior close $10,333.61 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 12 | $49.76 | $2.03 | — | $9,734.46 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=+10.6; leftover $645.85 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 15 | $41.31 | $2.04 | — | $9,112.78 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $645.85 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 197 | $3.27 | $2.58 | — | $8,466.00 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=+13.8; leftover $645.85 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 83 | $7.70 | $2.24 | — | $7,824.67 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $645.85 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 5 | $125.94 | $2.00 | — | $7,192.96 | — | deploy half leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $645.85 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 529 | $1.22 | $6.82 | — | $6,540.76 | — | deploy half leftover; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $645.85 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRVO` | 35 | $18.40 | $2.10 | — | $5,894.66 | — | deploy half leftover; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $645.85 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 41 | $15.70 | $2.11 | — | $5,248.85 | — | deploy half leftover; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $645.85 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,248.85 | ▲ close $10,706.93 vs 09:30 $10,333.61 (session +395.24) | 16:00 close · cash $5,248.85 · equity $10,706.93 vs 09:30 $10,333.61 (+373.32; session marks +395.24) · 8 name(s) marked open→close (per-name table). ATRC×12 09:30 $49.76 → close $52.59 +33.96; HRMY×15 09:30 $41.31 → close $42.86 +23.25; CABA×197 09:30 $3.27 → close $3.57 +59.10; VSTM×83 09:30 $7.70 → close $8.02 +26.56; RVTY×5 09:30 $125.94 → close $130.94 +25.00; GPRO×529 09:30 $1.22 → close $1.69 +248.63; FRVO×35 09:30 $18.40 → close $17.98 -14.70; CRK×41 09:30 $15.70 → close $15.54 -6.56 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,248.85 | ▲ 09:30 equity $10,785.73 vs yday $10,706.93 (+78.80) | 09:30 open · cash $5,248.85 (unchanged overnight, no fees) · equity $10,785.73 vs prior close $10,706.93 (+78.80) · 8 name(s) re-marked at the open (per-name table). ATRC×12 yday $52.59 → 09:30 $52.88 +3.48; HRMY×15 yday $42.86 → 09:30 $42.93 +1.05; CABA×197 yday $3.57 → 09:30 $3.63 +11.82; VSTM×83 yday $8.02 → 09:30 $8.03 +0.83; RVTY×5 yday $130.94 → 09:30 $132.45 +7.55; GPRO×529 yday $1.69 → 09:30 $1.78 +47.61; FRVO×35 yday $17.98 → 09:30 $18.27 +10.15; CRK×41 yday $15.54 → 09:30 $15.45 -3.69 | — |
| 2026-09-04 09:30 ET | **SELL** | `HRMY` | 15 | $42.93 | $2.06 | $+20.21 | $5,890.74 | ▲ +20.21 after sell → book $10,783.67; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 83 | $8.03 | $2.26 | $+22.89 | $6,554.97 | ▲ +22.89 after sell → book $10,781.41; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 5 | $132.45 | $2.02 | $+28.52 | $7,215.20 | ▲ +28.52 after sell → book $10,779.39; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `FRVO` | 35 | $18.27 | $2.12 | $-8.76 | $7,852.53 | ▼ -8.76 after sell → book $10,777.27; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 41 | $15.45 | $2.13 | $-14.50 | $8,483.85 | ▼ -14.50 after sell → book $10,775.14; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ASND` | 3 | $266.94 | $2.00 | — | $7,681.03 | — | deploy half leftover; list flatten; ret5=+1.9; leftover $848.38 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OSCR` | 27 | $30.65 | $2.07 | — | $6,851.41 | — | deploy half leftover; list flatten; 🔵; ret5=-2.2; leftover $848.38 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `NVAX` | 81 | $10.41 | $2.23 | — | $6,005.96 | — | deploy half leftover; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $848.38 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BVS` | 58 | $14.50 | $2.16 | — | $5,162.80 | — | deploy half leftover; list flatten; 🔵; ⚪; ret5=+0.8; leftover $848.38 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 435 | $1.95 | $5.61 | — | $4,308.94 | — | deploy half leftover; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $848.38 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4,308.94 | ▼ close $10,557.49 vs 09:30 $10,785.73 (session -203.57) | 16:00 close · cash $4,308.94 · equity $10,557.49 vs 09:30 $10,785.73 (-228.24; session marks -203.57) · 8 name(s) marked open→close (per-name table). ATRC×12 09:30 $52.88 → close $52.46 -5.04; CABA×197 09:30 $3.63 → close $3.48 -29.55; GPRO×529 09:30 $1.78 → close $1.39 -206.31; ASND×3 09:30 $266.94 → close $271.12 +12.54; OSCR×27 09:30 $30.65 → close $32.24 +42.93; NVAX×81 09:30 $10.41 → close $10.34 -5.67; BVS×58 09:30 $14.50 → close $14.36 -8.12; BAK×435 09:30 $1.95 → close $1.94 -4.35 | — |

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
| 2026-08-27 | `MU` | cash | leftover split 698.20 < 1 share @ 925.74 |
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
| `ATRC` | 12 | 2026-09-03 @ $49.76 | deploy half leftover; list flatten; 🔵; ⚪; ret5=+10.6; leftover $645.85 |
| `CABA` | 197 | 2026-09-03 @ $3.27 | deploy half leftover; list flatten; 🔵; ⚪; ret5=+13.8; leftover $645.85 |
| `GPRO` | 529 | 2026-09-03 @ $1.22 | deploy half leftover; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $645.85 |
| `ASND` | 3 | 2026-09-04 @ $266.94 | deploy half leftover; list flatten; ret5=+1.9; leftover $848.38 |
| `OSCR` | 27 | 2026-09-04 @ $30.65 | deploy half leftover; list flatten; 🔵; ret5=-2.2; leftover $848.38 |
| `NVAX` | 81 | 2026-09-04 @ $10.41 | deploy half leftover; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $848.38 |
| `BVS` | 58 | 2026-09-04 @ $14.50 | deploy half leftover; list flatten; 🔵; ⚪; ret5=+0.8; leftover $848.38 |
| `BAK` | 435 | 2026-09-04 @ $1.95 | deploy half leftover; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $848.38 |
