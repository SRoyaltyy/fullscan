# Factor mine action — `union_join_present_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ join_present, no 🚨

Cash book **+13.80%** ($11,380) · signal-only (no cash/fees) was +18.15%. Starts YES **16/17**. Fills 136 · skips 52 · realized $+1006.81.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `join_present=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $54.78.

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
| 2026-08-14 | `TLN` | 3 | — | $359.83 | +0.00 | $362.74 | +8.73 | +8.73 | +0.00 | +8.73 |
| 2026-08-14 | `VST` | 8 | — | $146.90 | +0.00 | $148.13 | +9.84 | +9.84 | +0.00 | +9.84 |
| 2026-08-14 | `NRG` | 10 | — | $120.00 | +0.00 | $126.24 | +62.40 | +62.40 | +0.00 | +62.40 |
| 2026-08-14 | `DAVE` | 3 | — | $330.91 | +0.00 | $334.57 | +10.98 | +10.98 | +0.00 | +10.98 |
| 2026-08-14 | `SLG` | 22 | — | $57.61 | +0.00 | $56.09 | -33.44 | -33.44 | +0.00 | -33.44 |
| 2026-08-14 | `MARA` | 140 | — | $9.01 | +0.00 | $9.20 | +26.60 | +26.60 | +0.00 | +26.60 |
| 2026-08-14 | `LDI` | 1353 | — | $0.94 | +0.00 | $0.90 | -54.12 | -54.12 | +0.00 | -54.12 |
| 2026-08-14 | `BTBT` | 845 | — | $1.50 | +0.00 | $1.57 | +59.15 | +59.15 | +0.00 | +59.15 |
| 2026-08-17 | `TLN` | 3 | $362.74 | $367.88 | +15.42 | — | +0.00 | +15.42 | +24.15 | — |
| 2026-08-17 | `VST` | 8 | $148.13 | $149.37 | +9.92 | — | +0.00 | +9.92 | +19.76 | — |
| 2026-08-17 | `NRG` | 10 | $126.24 | $127.40 | +11.60 | — | +0.00 | +11.60 | +74.00 | — |
| 2026-08-17 | `DAVE` | 3 | $334.57 | $336.94 | +7.11 | — | +0.00 | +7.11 | +18.09 | — |
| 2026-08-17 | `SLG` | 22 | $56.09 | $55.37 | -15.84 | — | +0.00 | -15.84 | -49.28 | — |
| 2026-08-17 | `MARA` | 140 | $9.20 | $9.22 | +2.80 | — | +0.00 | +2.80 | +29.40 | — |
| 2026-08-17 | `LDI` | 1353 | $0.90 | $0.91 | +13.53 | — | +0.00 | +13.53 | -40.59 | — |
| 2026-08-17 | `BTBT` | 845 | $1.57 | $1.52 | -42.25 | — | +0.00 | -42.25 | +16.90 | — |
| 2026-08-17 | `DVN` | 27 | — | $46.18 | +0.00 | $47.57 | +37.53 | +37.53 | +0.00 | +37.53 |
| 2026-08-17 | `EOG` | 8 | — | $142.77 | +0.00 | $146.15 | +27.04 | +27.04 | +0.00 | +27.04 |
| 2026-08-17 | `FANG` | 6 | — | $202.70 | +0.00 | $206.29 | +21.54 | +21.54 | +0.00 | +21.54 |
| 2026-08-17 | `TMC` | 313 | — | $4.05 | +0.00 | $3.77 | -87.64 | -87.64 | +0.00 | -87.64 |
| 2026-08-17 | `TGB` | 150 | — | $8.46 | +0.00 | $8.77 | +46.50 | +46.50 | +0.00 | +46.50 |
| 2026-08-17 | `ELF` | 14 | — | $90.54 | +0.00 | $93.66 | +43.68 | +43.68 | +0.00 | +43.68 |
| 2026-08-17 | `DNN` | 391 | — | $3.24 | +0.00 | $3.19 | -19.55 | -19.55 | +0.00 | -19.55 |
| 2026-08-17 | `NB` | 250 | — | $5.07 | +0.00 | $4.81 | -65.00 | -65.00 | +0.00 | -65.00 |
| 2026-08-18 | `DVN` | 27 | $47.57 | $48.00 | +11.61 | — | +0.00 | +11.61 | +49.14 | — |
| 2026-08-18 | `EOG` | 8 | $146.15 | $148.04 | +15.12 | — | +0.00 | +15.12 | +42.16 | — |
| 2026-08-18 | `FANG` | 6 | $206.29 | $208.93 | +15.84 | — | +0.00 | +15.84 | +37.38 | — |
| 2026-08-18 | `TMC` | 313 | $3.77 | $3.72 | -15.65 | — | +0.00 | -15.65 | -103.29 | — |
| 2026-08-18 | `TGB` | 150 | $8.77 | $8.55 | -33.00 | — | +0.00 | -33.00 | +13.50 | — |
| 2026-08-18 | `ELF` | 14 | $93.66 | $93.44 | -3.08 | — | +0.00 | -3.08 | +40.60 | — |
| 2026-08-18 | `DNN` | 391 | $3.19 | $3.11 | -31.28 | — | +0.00 | -31.28 | -50.83 | — |
| 2026-08-18 | `NB` | 250 | $4.81 | $4.66 | -37.50 | — | +0.00 | -37.50 | -102.50 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `AG` | 61 | — | $20.55 | +0.00 | $21.19 | +39.04 | +39.04 | +0.00 | +39.04 |
| 2026-08-20 | `BHP` | 13 | — | $91.01 | +0.00 | $93.63 | +34.06 | +34.06 | +0.00 | +34.06 |
| 2026-08-20 | `CDE` | 60 | — | $20.65 | +0.00 | $21.11 | +27.60 | +27.60 | +0.00 | +27.60 |
| 2026-08-20 | `HDSN` | 217 | — | $5.77 | +0.00 | $5.57 | -43.40 | -43.40 | +0.00 | -43.40 |
| 2026-08-20 | `IAG` | 63 | — | $19.63 | +0.00 | $20.50 | +54.81 | +54.81 | +0.00 | +54.81 |
| 2026-08-20 | `KGC` | 42 | — | $29.63 | +0.00 | $31.43 | +75.60 | +75.60 | +0.00 | +75.60 |
| 2026-08-20 | `NFGC` | 716 | — | $1.75 | +0.00 | $1.75 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `WPM` | 8 | — | $144.54 | +0.00 | $150.25 | +45.68 | +45.68 | +0.00 | +45.68 |
| 2026-08-21 | `AG` | 61 | $21.19 | $21.90 | +43.31 | — | +0.00 | +43.31 | +82.35 | — |
| 2026-08-21 | `BHP` | 13 | $93.63 | $95.72 | +27.17 | — | +0.00 | +27.17 | +61.23 | — |
| 2026-08-21 | `CDE` | 60 | $21.11 | $21.75 | +38.40 | — | +0.00 | +38.40 | +66.00 | — |
| 2026-08-21 | `HDSN` | 217 | $5.57 | $5.67 | +21.70 | — | +0.00 | +21.70 | -21.70 | — |
| 2026-08-21 | `IAG` | 63 | $20.50 | $21.17 | +42.21 | — | +0.00 | +42.21 | +97.02 | — |
| 2026-08-21 | `KGC` | 42 | $31.43 | $32.17 | +31.08 | — | +0.00 | +31.08 | +106.68 | — |
| 2026-08-21 | `NFGC` | 716 | $1.75 | $1.79 | +28.64 | — | +0.00 | +28.64 | +28.64 | — |
| 2026-08-21 | `WPM` | 8 | $150.25 | $154.70 | +35.60 | — | +0.00 | +35.60 | +81.28 | — |
| 2026-08-21 | `AU` | 10 | — | $119.43 | +0.00 | $121.22 | +17.90 | +17.90 | +0.00 | +17.90 |
| 2026-08-21 | `AUPH` | 76 | — | $17.20 | +0.00 | $16.65 | -41.80 | -41.80 | +0.00 | -41.80 |
| 2026-08-21 | `AEM` | 6 | — | $216.30 | +0.00 | $216.06 | -1.44 | -1.44 | +0.00 | -1.44 |
| 2026-08-21 | `ARCT` | 117 | — | $11.13 | +0.00 | $13.45 | +271.44 | +271.44 | +0.00 | +271.44 |
| 2026-08-21 | `AUTL` | 530 | — | $2.47 | +0.00 | $2.41 | -31.80 | -31.80 | +0.00 | -31.80 |
| 2026-08-21 | `CRDL` | 679 | — | $1.93 | +0.00 | $1.86 | -47.53 | -47.53 | +0.00 | -47.53 |
| 2026-08-21 | `CRSP` | 21 | — | $59.72 | +0.00 | $59.50 | -4.62 | -4.62 | +0.00 | -4.62 |
| 2026-08-21 | `CYPH` | 993 | — | $1.32 | +0.00 | $1.42 | +99.30 | +99.30 | +0.00 | +99.30 |
| 2026-08-24 | `AU` | 10 | $121.22 | $120.50 | -7.20 | — | +0.00 | -7.20 | +10.70 | — |
| 2026-08-24 | `AUPH` | 76 | $16.65 | $16.60 | -3.80 | — | +0.00 | -3.80 | -45.60 | — |
| 2026-08-24 | `AEM` | 6 | $216.06 | $217.03 | +5.82 | — | +0.00 | +5.82 | +4.38 | — |
| 2026-08-24 | `ARCT` | 117 | $13.45 | $13.26 | -22.23 | — | +0.00 | -22.23 | +249.21 | — |
| 2026-08-24 | `AUTL` | 530 | $2.41 | $2.36 | -26.50 | — | +0.00 | -26.50 | -58.30 | — |
| 2026-08-24 | `CRDL` | 679 | $1.86 | $1.87 | +6.79 | — | +0.00 | +6.79 | -40.74 | — |
| 2026-08-24 | `CRSP` | 21 | $59.50 | $58.79 | -14.91 | — | +0.00 | -14.91 | -19.53 | — |
| 2026-08-24 | `CYPH` | 993 | $1.42 | $1.83 | +407.13 | — | +0.00 | +407.13 | +506.43 | — |
| 2026-08-25 | `MOS` | 57 | — | $24.00 | +0.00 | $23.75 | -14.25 | -14.25 | +0.00 | -14.25 |
| 2026-08-25 | `OCUL` | 126 | — | $10.92 | +0.00 | $10.92 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-25 | `INSP` | 22 | — | $61.47 | +0.00 | $61.47 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-25 | `CRMD` | 166 | — | $8.28 | +0.00 | $8.28 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-25 | `RZLT` | 263 | — | $5.23 | +0.00 | $5.29 | +15.78 | +15.78 | +0.00 | +15.78 |
| 2026-08-25 | `HCA` | 3 | — | $429.24 | +0.00 | $428.50 | -2.22 | -2.22 | +0.00 | -2.22 |
| 2026-08-25 | `BMEA` | 849 | — | $1.62 | +0.00 | $1.61 | -8.49 | -8.49 | +0.00 | -8.49 |
| 2026-08-25 | `NPWR` | 688 | — | $2.00 | +0.00 | $2.02 | +13.76 | +13.76 | +0.00 | +13.76 |
| 2026-08-26 | `MOS` | 57 | $23.75 | $23.75 | +0.00 | $23.75 | +0.00 | +0.00 | -14.25 | -14.25 |
| 2026-08-26 | `OCUL` | 126 | $10.92 | $10.92 | +0.00 | $10.92 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-26 | `INSP` | 22 | $61.47 | $61.47 | +0.00 | $61.47 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-26 | `CRMD` | 166 | $8.28 | $8.28 | +0.00 | $8.28 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-26 | `RZLT` | 263 | $5.29 | $5.29 | +0.00 | $5.29 | +0.00 | +0.00 | +15.78 | +15.78 |
| 2026-08-26 | `HCA` | 3 | $428.50 | $428.50 | +0.00 | $428.50 | +0.00 | +0.00 | -2.22 | -2.22 |
| 2026-08-26 | `BMEA` | 849 | $1.61 | $1.61 | +0.00 | $1.61 | +0.00 | +0.00 | -8.49 | -8.49 |
| 2026-08-26 | `NPWR` | 688 | $2.02 | $2.02 | +0.00 | $2.02 | +0.00 | +0.00 | +13.76 | +13.76 |
| 2026-08-27 | `MOS` | 57 | $23.75 | $24.84 | +62.13 | $24.16 | -38.76 | +23.37 | +47.88 | +9.12 |
| 2026-08-27 | `OCUL` | 126 | $10.92 | $10.79 | -16.38 | — | +0.00 | -16.38 | -16.38 | — |
| 2026-08-27 | `INSP` | 22 | $61.47 | $60.07 | -30.80 | — | +0.00 | -30.80 | -30.80 | — |
| 2026-08-27 | `CRMD` | 166 | $8.28 | $8.60 | +53.12 | — | +0.00 | +53.12 | +53.12 | — |
| 2026-08-27 | `RZLT` | 263 | $5.29 | $5.01 | -73.64 | — | +0.00 | -73.64 | -57.86 | — |
| 2026-08-27 | `HCA` | 3 | $428.50 | $427.50 | -3.00 | — | +0.00 | -3.00 | -5.22 | — |
| 2026-08-27 | `BMEA` | 849 | $1.61 | $1.75 | +118.86 | — | +0.00 | +118.86 | +110.37 | — |
| 2026-08-27 | `NPWR` | 688 | $2.02 | $1.93 | -61.92 | — | +0.00 | -61.92 | -48.16 | — |
| 2026-08-27 | `RRC` | 33 | — | $40.72 | +0.00 | $41.55 | +27.39 | +27.39 | +0.00 | +27.39 |
| 2026-08-27 | `CRK` | 97 | — | $14.09 | +0.00 | $14.50 | +39.77 | +39.77 | +0.00 | +39.77 |
| 2026-08-27 | `SLI` | 528 | — | $2.59 | +0.00 | $2.61 | +10.56 | +10.56 | +0.00 | +10.56 |
| 2026-08-27 | `ACMR` | 16 | — | $80.97 | +0.00 | $79.11 | -29.76 | -29.76 | +0.00 | -29.76 |
| 2026-08-27 | `GGB` | 309 | — | $4.42 | +0.00 | $4.46 | +12.36 | +12.36 | +0.00 | +12.36 |
| 2026-08-27 | `MT` | 18 | — | $75.12 | +0.00 | $74.53 | -10.62 | -10.62 | +0.00 | -10.62 |
| 2026-08-27 | `MU` | 1 | — | $925.74 | +0.00 | $938.40 | +12.66 | +12.66 | +0.00 | +12.66 |
| 2026-08-28 | `MOS` | 57 | $24.16 | $24.00 | -9.12 | $23.76 | -13.68 | -22.80 | +0.00 | -13.68 |
| 2026-08-28 | `RRC` | 33 | $41.55 | $41.44 | -3.63 | $41.64 | +6.60 | +2.97 | +23.76 | +30.36 |
| 2026-08-28 | `CRK` | 97 | $14.50 | $14.42 | -7.76 | $14.62 | +19.40 | +11.64 | +32.01 | +51.41 |
| 2026-08-28 | `SLI` | 528 | $2.61 | $2.60 | -5.28 | $2.64 | +21.12 | +15.84 | +5.28 | +26.40 |
| 2026-08-28 | `ACMR` | 16 | $79.11 | $81.65 | +40.64 | — | +0.00 | +40.64 | +10.88 | — |
| 2026-08-28 | `GGB` | 309 | $4.46 | $4.57 | +33.99 | — | +0.00 | +33.99 | +46.35 | — |
| 2026-08-28 | `MT` | 18 | $74.53 | $74.54 | +0.18 | — | +0.00 | +0.18 | -10.44 | — |
| 2026-08-28 | `MU` | 1 | $938.40 | $967.01 | +28.61 | — | +0.00 | +28.61 | +41.27 | — |
| 2026-08-28 | `ANF` | 9 | — | $144.70 | +0.00 | $145.75 | +9.45 | +9.45 | +0.00 | +9.45 |
| 2026-08-28 | `BHVN` | 82 | — | $16.95 | +0.00 | $16.12 | -68.06 | -68.06 | +0.00 | -68.06 |
| 2026-08-28 | `BZ` | 75 | — | $18.50 | +0.00 | $18.00 | -37.50 | -37.50 | +0.00 | -37.50 |
| 2026-08-28 | `CAPR` | 151 | — | $9.19 | +0.00 | $10.06 | +131.37 | +131.37 | +0.00 | +131.37 |
| 2026-08-31 | `MOS` | 57 | $23.76 | $23.75 | -0.57 | — | +0.00 | -0.57 | -14.25 | — |
| 2026-08-31 | `RRC` | 33 | $41.64 | $41.11 | -17.49 | — | +0.00 | -17.49 | +12.87 | — |
| 2026-08-31 | `CRK` | 97 | $14.62 | $14.56 | -5.82 | — | +0.00 | -5.82 | +45.59 | — |
| 2026-08-31 | `SLI` | 528 | $2.64 | $2.51 | -68.64 | — | +0.00 | -68.64 | -42.24 | — |
| 2026-08-31 | `ANF` | 9 | $145.75 | $148.67 | +26.28 | — | +0.00 | +26.28 | +35.73 | — |
| 2026-08-31 | `BHVN` | 82 | $16.12 | $15.44 | -55.76 | — | +0.00 | -55.76 | -123.82 | — |
| 2026-08-31 | `BZ` | 75 | $18.00 | $17.89 | -8.25 | — | +0.00 | -8.25 | -45.75 | — |
| 2026-08-31 | `CAPR` | 151 | $10.06 | $9.44 | -93.62 | — | +0.00 | -93.62 | +37.75 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `ATRC` | 27 | — | $49.76 | +0.00 | $52.59 | +76.41 | +76.41 | +0.00 | +76.41 |
| 2026-09-03 | `HRMY` | 32 | — | $41.31 | +0.00 | $42.86 | +49.60 | +49.60 | +0.00 | +49.60 |
| 2026-09-03 | `CABA` | 416 | — | $3.27 | +0.00 | $3.57 | +124.80 | +124.80 | +0.00 | +124.80 |
| 2026-09-03 | `VSTM` | 176 | — | $7.70 | +0.00 | $8.02 | +56.32 | +56.32 | +0.00 | +56.32 |
| 2026-09-03 | `RVTY` | 10 | — | $125.94 | +0.00 | $130.94 | +50.00 | +50.00 | +0.00 | +50.00 |
| 2026-09-03 | `GPRO` | 1115 | — | $1.22 | +0.00 | $1.69 | +524.05 | +524.05 | +0.00 | +524.05 |
| 2026-09-03 | `FRVO` | 73 | — | $18.40 | +0.00 | $17.98 | -30.66 | -30.66 | +0.00 | -30.66 |
| 2026-09-03 | `CRK` | 86 | — | $15.70 | +0.00 | $15.54 | -13.76 | -13.76 | +0.00 | -13.76 |
| 2026-09-04 | `ATRC` | 27 | $52.59 | $52.88 | +7.83 | $52.46 | -11.34 | -3.51 | +84.24 | +72.90 |
| 2026-09-04 | `HRMY` | 32 | $42.86 | $42.93 | +2.24 | — | +0.00 | +2.24 | +51.84 | — |
| 2026-09-04 | `CABA` | 416 | $3.57 | $3.63 | +24.96 | $3.48 | -62.40 | -37.44 | +149.76 | +87.36 |
| 2026-09-04 | `VSTM` | 176 | $8.02 | $8.03 | +1.76 | — | +0.00 | +1.76 | +58.08 | — |
| 2026-09-04 | `RVTY` | 10 | $130.94 | $132.45 | +15.10 | — | +0.00 | +15.10 | +65.10 | — |
| 2026-09-04 | `GPRO` | 1115 | $1.69 | $1.78 | +100.35 | $1.39 | -434.85 | -334.50 | +624.40 | +189.55 |
| 2026-09-04 | `FRVO` | 73 | $17.98 | $18.27 | +21.17 | — | +0.00 | +21.17 | -9.49 | — |
| 2026-09-04 | `CRK` | 86 | $15.54 | $15.45 | -7.74 | — | +0.00 | -7.74 | -21.50 | — |
| 2026-09-04 | `ASND` | 5 | — | $266.94 | +0.00 | $271.12 | +20.90 | +20.90 | +0.00 | +20.90 |
| 2026-09-04 | `OSCR` | 45 | — | $30.65 | +0.00 | $32.24 | +71.55 | +71.55 | +0.00 | +71.55 |
| 2026-09-04 | `NVAX` | 132 | — | $10.41 | +0.00 | $10.34 | -9.24 | -9.24 | +0.00 | -9.24 |
| 2026-09-04 | `BVS` | 95 | — | $14.50 | +0.00 | $14.36 | -13.30 | -13.30 | +0.00 | -13.30 |
| 2026-09-04 | `BAK` | 709 | — | $1.95 | +0.00 | $1.94 | -7.09 | -7.09 | +0.00 | -7.09 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +185.07 | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | — | $97.53 | $10,153.12 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53 |
| 2026-08-14 | +5.50 | $97.53 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53 | $10,178.12 | +25.00 | +90.14 | TLN, VST, NRG, DAVE, SLG, MARA, LDI, BTBT | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | $592.27 | $10,193.91 | TLN×3, VST×8, NRG×10, DAVE×3, SLG×22, MARA×140, LDI×1353, BTBT×845 |
| 2026-08-17 | +2.25 | $592.27 | TLN×3, VST×8, NRG×10, DAVE×3, SLG×22, MARA×140, LDI×1353, BTBT×845 | $10,196.20 | +2.29 | +4.10 | DVN, EOG, FANG, TMC, TGB, ELF, DNN, NB | TLN, VST, NRG, DAVE, SLG, MARA, LDI, BTBT | $189.31 | $10,137.18 | DVN×27, EOG×8, FANG×6, TMC×313, TGB×150, ELF×14, DNN×391, NB×250 |
| 2026-08-18 | -6.20 | $189.31 | DVN×27, EOG×8, FANG×6, TMC×313, TGB×150, ELF×14, DNN×391, NB×250 | $10,059.24 | -77.94 | +0.00 | — | DVN, EOG, FANG, TMC, TGB, ELF, DNN, NB | $10,036.07 | $10,036.07 | — |
| 2026-08-19 | -7.20 | $10,036.07 | — | $10,036.07 | -0.00 | +0.00 | — | — | $10,036.07 | $10,036.07 | — |
| 2026-08-20 | +1.12 | $10,036.07 | — | $10,036.07 | -0.00 | +233.39 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | — | $193.11 | $10,244.74 | AG×61, BHP×13, CDE×60, HDSN×217, IAG×63, KGC×42, NFGC×716, WPM×8 |
| 2026-08-21 | +3.25 | $193.11 | AG×61, BHP×13, CDE×60, HDSN×217, IAG×63, KGC×42, NFGC×716, WPM×8 | $10,512.85 | +268.11 | +261.45 | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $162.83 | $10,710.24 | AU×10, AUPH×76, AEM×6, ARCT×117, AUTL×530, CRDL×679, CRSP×21, CYPH×993 |
| 2026-08-24 | -5.17 | $162.83 | AU×10, AUPH×76, AEM×6, ARCT×117, AUTL×530, CRDL×679, CRSP×21, CYPH×993 | $11,055.34 | +345.10 | +0.00 | — | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | $11,015.78 | $11,015.78 | — |
| 2026-08-25 | +1.80 | $11,015.78 | — | $11,015.78 | -0.00 | +4.58 | MOS, OCUL, INSP, CRMD, RZLT, HCA, BMEA, NPWR | — | $96.16 | $10,986.07 | MOS×57, OCUL×126, INSP×22, CRMD×166, RZLT×263, HCA×3, BMEA×849, NPWR×688 |
| 2026-08-26 | +2.02 | $96.16 | MOS×57, OCUL×126, INSP×22, CRMD×166, RZLT×263, HCA×3, BMEA×849, NPWR×688 | $10,986.07 | -0.00 | +0.00 | — | — | $96.16 | $10,986.07 | MOS×57, OCUL×126, INSP×22, CRMD×166, RZLT×263, HCA×3, BMEA×849, NPWR×688 |
| 2026-08-27 | — | $96.16 | MOS×57, OCUL×126, INSP×22, CRMD×166, RZLT×263, HCA×3, BMEA×849, NPWR×688 | $11,034.44 | +48.37 | +23.60 | RRC, CRK, SLI, ACMR, GGB, MT, MU | OCUL, INSP, CRMD, RZLT, HCA, BMEA, NPWR | $547.53 | $11,004.22 | MOS×57, RRC×33, CRK×97, SLI×528, ACMR×16, GGB×309, MT×18, MU×1 |
| 2026-08-28 | +0.75 | $547.53 | MOS×57, RRC×33, CRK×97, SLI×528, ACMR×16, GGB×309, MT×18, MU×1 | $11,081.85 | +77.63 | +68.70 | ANF, BHVN, BZ, CAPR | ACMR, GGB, MT, MU | $88.30 | $11,131.45 | MOS×57, RRC×33, CRK×97, SLI×528, ANF×9, BHVN×82, BZ×75, CAPR×151 |
| 2026-08-31 | -5.85 | $88.30 | MOS×57, RRC×33, CRK×97, SLI×528, ANF×9, BHVN×82, BZ×75, CAPR×151 | $10,907.58 | -223.87 | +0.00 | — | MOS, RRC, CRK, SLI, ANF, BHVN, BZ, CAPR | $10,885.06 | $10,885.06 | — |
| 2026-09-01 | -6.30 | $10,885.06 | — | $10,885.06 | +0.00 | +0.00 | — | — | $10,885.06 | $10,885.06 | — |
| 2026-09-02 | -3.83 | $10,885.06 | — | $10,885.06 | +0.00 | +0.00 | — | — | $10,885.06 | $10,885.06 | — |
| 2026-09-03 | -0.90 | $10,885.06 | — | $10,885.06 | +0.00 | +836.76 | ATRC, HRMY, CABA, VSTM, RVTY, GPRO, FRVO, CRK | — | $158.10 | $11,688.92 | ATRC×27, HRMY×32, CABA×416, VSTM×176, RVTY×10, GPRO×1115, FRVO×73, CRK×86 |
| 2026-09-04 | — | $158.10 | ATRC×27, HRMY×32, CABA×416, VSTM×176, RVTY×10, GPRO×1115, FRVO×73, CRK×86 | $11,854.59 | +165.67 | -445.77 | ASND, OSCR, NVAX, BVS, BAK | HRMY, VSTM, RVTY, FRVO, CRK | $54.78 | $11,379.67 | ATRC×27, CABA×416, GPRO×1115, ASND×5, OSCR×45, NVAX×132, BVS×95, BAK×709 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $8,801.95 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $7,558.42 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; ⚪; ret5=+12.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $6,341.40 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; ⚪; ret5=+6.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 25 | $49.70 | $2.06 | — | $5,096.84 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $3,854.33 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $2,603.13 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $1,336.17 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; ⚪; ret5=+13.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $97.53 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; ⚪; ret5=+19.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
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
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 3 | $359.83 | $2.00 | — | $9,062.42 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ret5=+5.9; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 8 | $146.90 | $2.01 | — | $7,885.21 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ret5=+3.6; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 10 | $120.00 | $2.02 | — | $6,683.19 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ret5=+0.6; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `DAVE` | 3 | $330.91 | $2.00 | — | $5,688.46 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ⚪; ret5=-8.6; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 22 | $57.61 | $2.06 | — | $4,418.98 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ret5=+5.7; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟡 judge🔴 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 140 | $9.01 | $2.41 | — | $3,155.17 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ⚪; ret5=-13.5; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 1353 | $0.94 | $16.74 | — | $1,870.67 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ⚪; ret5=+0.5; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 845 | $1.50 | $10.90 | — | $592.27 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $592.27 | ▲ close $10,193.91 vs 09:30 $10,178.12 (session +90.14) | 16:00 close · cash $592.27 · equity $10,193.91 vs 09:30 $10,178.12 (+15.79; session marks +90.14) · 8 name(s) marked open→close (per-name table). TLN×3 09:30 $359.83 → close $362.74 +8.73; VST×8 09:30 $146.90 → close $148.13 +9.84; NRG×10 09:30 $120.00 → close $126.24 +62.40; DAVE×3 09:30 $330.91 → close $334.57 +10.98; SLG×22 09:30 $57.61 → close $56.09 -33.44; MARA×140 09:30 $9.01 → close $9.20 +26.60; LDI×1353 09:30 $0.94 → close $0.90 -54.12; BTBT×845 09:30 $1.50 → close $1.57 +59.15 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $592.27 | ▲ 09:30 equity $10,196.20 vs yday $10,193.91 (+2.29) | 09:30 open · cash $592.27 (unchanged overnight, no fees) · equity $10,196.20 vs prior close $10,193.91 (+2.29) · 8 name(s) re-marked at the open (per-name table). TLN×3 yday $362.74 → 09:30 $367.88 +15.42; VST×8 yday $148.13 → 09:30 $149.37 +9.92; NRG×10 yday $126.24 → 09:30 $127.40 +11.60; DAVE×3 yday $334.57 → 09:30 $336.94 +7.11; SLG×22 yday $56.09 → 09:30 $55.37 -15.84; MARA×140 yday $9.20 → 09:30 $9.22 +2.80; LDI×1353 yday $0.90 → 09:30 $0.91 +13.53; BTBT×845 yday $1.57 → 09:30 $1.52 -42.25 | — |
| 2026-08-17 09:30 ET | **SELL** | `TLN` | 3 | $367.88 | $2.02 | $+20.13 | $1,693.89 | ▲ +20.13 after sell → book $10,194.18; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VST` | 8 | $149.37 | $2.03 | $+15.71 | $2,886.82 | ▲ +15.71 after sell → book $10,192.15; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NRG` | 10 | $127.40 | $2.04 | $+69.94 | $4,158.78 | ▲ +69.94 after sell → book $10,190.11; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `DAVE` | 3 | $336.94 | $2.02 | $+14.07 | $5,167.58 | ▲ +14.07 after sell → book $10,188.09; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SLG` | 22 | $55.37 | $2.08 | $-53.41 | $6,383.64 | ▼ -53.41 after sell → book $10,186.02; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MARA` | 140 | $9.22 | $2.44 | $+24.55 | $7,672.00 | ▲ +24.55 after sell → book $10,183.57; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LDI` | 1353 | $0.91 | $16.57 | $-73.89 | $8,882.61 | ▼ -73.89 after sell → book $10,167.01; vs 09:30 mark -16.56 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 845 | $1.52 | $11.05 | $-5.05 | $10,155.96 | ▼ -5.05 after sell → book $10,155.96; vs 09:30 mark -11.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 27 | $46.18 | $2.07 | — | $8,907.02 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ret5=+6.7; leftover $1269.49 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 8 | $142.77 | $2.01 | — | $7,762.85 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ret5=+5.8; leftover $1269.49 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 6 | $202.70 | $2.01 | — | $6,544.64 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ret5=+8.3; leftover $1269.49 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 313 | $4.05 | $4.04 | — | $5,272.95 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ⚪; ret5=-12.3; leftover $1269.49 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 150 | $8.46 | $2.44 | — | $4,001.51 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ⚪; ret5=+0.4; leftover $1269.49 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ELF` | 14 | $90.54 | $2.03 | — | $2,731.92 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; ret5=-7.2; leftover $1269.49 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 391 | $3.24 | $5.04 | — | $1,460.04 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; ⚪; ret5=+0.3; leftover $1269.49 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `NB` | 250 | $5.07 | $3.23 | — | $189.31 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; ret5=-4.7; leftover $1269.49 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $189.31 | ▲ close $10,137.18 vs 09:30 $10,196.20 (session +4.10) | 16:00 close · cash $189.31 · equity $10,137.18 vs 09:30 $10,196.20 (-59.02; session marks +4.10) · 8 name(s) marked open→close (per-name table). DVN×27 09:30 $46.18 → close $47.57 +37.53; EOG×8 09:30 $142.77 → close $146.15 +27.04; FANG×6 09:30 $202.70 → close $206.29 +21.54; TMC×313 09:30 $4.05 → close $3.77 -87.64; TGB×150 09:30 $8.46 → close $8.77 +46.50; ELF×14 09:30 $90.54 → close $93.66 +43.68; DNN×391 09:30 $3.24 → close $3.19 -19.55; NB×250 09:30 $5.07 → close $4.81 -65.00 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $189.31 | ▼ 09:30 equity $10,059.24 vs yday $10,137.18 (-77.94) | 09:30 open · cash $189.31 (unchanged overnight, no fees) · equity $10,059.24 vs prior close $10,137.18 (-77.94) · 8 name(s) re-marked at the open (per-name table). DVN×27 yday $47.57 → 09:30 $48.00 +11.61; EOG×8 yday $146.15 → 09:30 $148.04 +15.12; FANG×6 yday $206.29 → 09:30 $208.93 +15.84; TMC×313 yday $3.77 → 09:30 $3.72 -15.65; TGB×150 yday $8.77 → 09:30 $8.55 -33.00; ELF×14 yday $93.66 → 09:30 $93.44 -3.08; DNN×391 yday $3.19 → 09:30 $3.11 -31.28; NB×250 yday $4.81 → 09:30 $4.66 -37.50 | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 27 | $48.00 | $2.09 | $+44.98 | $1,483.22 | ▲ +44.98 after sell → book $10,057.15; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 8 | $148.04 | $2.03 | $+38.11 | $2,665.51 | ▲ +38.11 after sell → book $10,055.12; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 6 | $208.93 | $2.03 | $+33.34 | $3,917.06 | ▲ +33.34 after sell → book $10,053.09; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TMC` | 313 | $3.72 | $4.10 | $-111.43 | $5,077.32 | ▼ -111.43 after sell → book $10,048.99; vs 09:30 mark -4.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGB` | 150 | $8.55 | $2.48 | $+8.58 | $6,357.35 | ▲ +8.58 after sell → book $10,046.52; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ELF` | 14 | $93.44 | $2.05 | $+36.52 | $7,663.45 | ▲ +36.52 after sell → book $10,044.46; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `DNN` | 391 | $3.11 | $5.12 | $-60.99 | $8,874.34 | ▼ -60.99 after sell → book $10,039.34; vs 09:30 mark -5.12 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 vol🟢 buy🟢 |
| 2026-08-18 09:30 ET | **SELL** | `NB` | 250 | $4.66 | $3.28 | $-109.00 | $10,036.07 | ▼ -109.00 after sell → book $10,036.07; vs 09:30 mark -3.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,036.07 | ▲ close $10,036.07 vs 09:30 $10,059.24 (session +0.00) | 16:00 close · cash $10,036.07 · no lots left · equity $10,036.07. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,036.07 | ▲ 09:30 equity $10,036.07 vs yday $10,036.07 (-0.00) | 09:30 open · cash $10,036.07 · no holdings · equity $10,036.07 vs prior close $10,036.07 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,036.07 | ▲ close $10,036.07 vs 09:30 $10,036.07 (session +0.00) | 16:00 close · cash $10,036.07 · no lots left · equity $10,036.07. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,036.07 | ▲ 09:30 equity $10,036.07 vs yday $10,036.07 (-0.00) | 09:30 open · cash $10,036.07 · no holdings · equity $10,036.07 vs prior close $10,036.07 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 61 | $20.55 | $2.17 | — | $8,780.34 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1254.51 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,595.19 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1254.51 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 60 | $20.65 | $2.17 | — | $6,354.02 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1254.51 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 217 | $5.77 | $2.80 | — | $5,099.13 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1254.51 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 63 | $19.63 | $2.18 | — | $3,860.26 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1254.51 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 42 | $29.63 | $2.12 | — | $2,613.68 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1254.51 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 716 | $1.75 | $9.24 | — | $1,351.45 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1254.51 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $193.11 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1254.51 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $193.11 | ▲ close $10,244.74 vs 09:30 $10,036.07 (session +233.39) | 16:00 close · cash $193.11 · equity $10,244.74 vs 09:30 $10,036.07 (+208.67; session marks +233.39) · 8 name(s) marked open→close (per-name table). AG×61 09:30 $20.55 → close $21.19 +39.04; BHP×13 09:30 $91.01 → close $93.63 +34.06; CDE×60 09:30 $20.65 → close $21.11 +27.60; HDSN×217 09:30 $5.77 → close $5.57 -43.40; IAG×63 09:30 $19.63 → close $20.50 +54.81; KGC×42 09:30 $29.63 → close $31.43 +75.60; NFGC×716 09:30 $1.75 → close $1.75 +0.00; WPM×8 09:30 $144.54 → close $150.25 +45.68 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $193.11 | ▲ 09:30 equity $10,512.85 vs yday $10,244.74 (+268.11) | 09:30 open · cash $193.11 (unchanged overnight, no fees) · equity $10,512.85 vs prior close $10,244.74 (+268.11) · 8 name(s) re-marked at the open (per-name table). AG×61 yday $21.19 → 09:30 $21.90 +43.31; BHP×13 yday $93.63 → 09:30 $95.72 +27.17; CDE×60 yday $21.11 → 09:30 $21.75 +38.40; HDSN×217 yday $5.57 → 09:30 $5.67 +21.70; IAG×63 yday $20.50 → 09:30 $21.17 +42.21; KGC×42 yday $31.43 → 09:30 $32.17 +31.08; NFGC×716 yday $1.75 → 09:30 $1.79 +28.64; WPM×8 yday $150.25 → 09:30 $154.70 +35.60 | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 61 | $21.90 | $2.19 | $+77.98 | $1,526.82 | ▲ +77.98 after sell → book $10,510.66; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $2,769.13 | ▲ +57.15 after sell → book $10,508.61; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 60 | $21.75 | $2.19 | $+61.64 | $4,071.94 | ▲ +61.64 after sell → book $10,506.42; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 217 | $5.67 | $2.85 | $-27.34 | $5,299.48 | ▼ -27.34 after sell → book $10,503.57; vs 09:30 mark -2.85 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 63 | $21.17 | $2.20 | $+92.64 | $6,630.99 | ▲ +92.64 after sell → book $10,501.37; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 42 | $32.17 | $2.14 | $+102.43 | $7,980.00 | ▲ +102.43 after sell → book $10,499.24; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 716 | $1.79 | $9.37 | $+10.04 | $9,252.27 | ▲ +10.04 after sell → book $10,489.87; vs 09:30 mark -9.37 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 8 | $154.70 | $2.03 | $+77.23 | $10,487.84 | ▲ +77.23 after sell → book $10,487.84; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $9,291.52 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+20.4; leftover $1310.98 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 76 | $17.20 | $2.22 | — | $7,982.10 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $1310.98 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 6 | $216.30 | $2.01 | — | $6,682.29 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1310.98 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 117 | $11.13 | $2.34 | — | $5,377.74 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1310.98 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 530 | $2.47 | $6.84 | — | $4,061.80 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $1310.98 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 679 | $1.93 | $8.76 | — | $2,742.57 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $1310.98 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 21 | $59.72 | $2.05 | — | $1,486.40 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1310.98 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 993 | $1.32 | $12.81 | — | $162.83 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1310.98 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $162.83 | ▲ close $10,710.24 vs 09:30 $10,512.85 (session +261.45) | 16:00 close · cash $162.83 · equity $10,710.24 vs 09:30 $10,512.85 (+197.39; session marks +261.45) · 8 name(s) marked open→close (per-name table). AU×10 09:30 $119.43 → close $121.22 +17.90; AUPH×76 09:30 $17.20 → close $16.65 -41.80; AEM×6 09:30 $216.30 → close $216.06 -1.44; ARCT×117 09:30 $11.13 → close $13.45 +271.44; AUTL×530 09:30 $2.47 → close $2.41 -31.80; CRDL×679 09:30 $1.93 → close $1.86 -47.53; CRSP×21 09:30 $59.72 → close $59.50 -4.62; CYPH×993 09:30 $1.32 → close $1.42 +99.30 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $162.83 | ▲ 09:30 equity $11,055.34 vs yday $10,710.24 (+345.10) | 09:30 open · cash $162.83 (unchanged overnight, no fees) · equity $11,055.34 vs prior close $10,710.24 (+345.10) · 8 name(s) re-marked at the open (per-name table). AU×10 yday $121.22 → 09:30 $120.50 -7.20; AUPH×76 yday $16.65 → 09:30 $16.60 -3.80; AEM×6 yday $216.06 → 09:30 $217.03 +5.82; ARCT×117 yday $13.45 → 09:30 $13.26 -22.23; AUTL×530 yday $2.41 → 09:30 $2.36 -26.50; CRDL×679 yday $1.86 → 09:30 $1.87 +6.79; CRSP×21 yday $59.50 → 09:30 $58.79 -14.91; CYPH×993 yday $1.42 → 09:30 $1.83 +407.13 | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.50 | $2.04 | $+6.64 | $1,365.79 | ▲ +6.64 after sell → book $11,053.30; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 76 | $16.60 | $2.24 | $-50.06 | $2,625.15 | ▼ -50.06 after sell → book $11,051.06; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 6 | $217.03 | $2.03 | $+0.34 | $3,925.30 | ▲ +0.34 after sell → book $11,049.03; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 117 | $13.26 | $2.37 | $+244.50 | $5,474.35 | ▲ +244.50 after sell → book $11,046.66; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 530 | $2.36 | $6.93 | $-72.07 | $6,718.21 | ▼ -72.07 after sell → book $11,039.72; vs 09:30 mark -6.94 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRDL` | 679 | $1.87 | $8.88 | $-58.38 | $7,979.06 | ▼ -58.38 after sell → book $11,030.84; vs 09:30 mark -8.88 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 21 | $58.79 | $2.07 | $-23.66 | $9,211.58 | ▼ -23.66 after sell → book $11,028.77; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 993 | $1.83 | $12.99 | $+480.63 | $11,015.78 | ▲ +480.63 after sell → book $11,015.78; vs 09:30 mark -12.99 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,015.78 | ▲ close $11,015.78 vs 09:30 $11,055.34 (session +0.00) | 16:00 close · cash $11,015.78 · no lots left · equity $11,015.78. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,015.78 | ▲ 09:30 equity $11,015.78 vs yday $11,015.78 (-0.00) | 09:30 open · cash $11,015.78 · no holdings · equity $11,015.78 vs prior close $11,015.78 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 57 | $24.00 | $2.16 | — | $9,645.62 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; ⚪; ret5=+13.0; leftover $1376.97 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 126 | $10.92 | $2.37 | — | $8,267.33 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ret5=+10.4; leftover $1376.97 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 22 | $61.47 | $2.06 | — | $6,912.93 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ret5=+9.2; leftover $1376.97 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 166 | $8.28 | $2.49 | — | $5,535.97 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ⚪; ret5=+8.8; leftover $1376.97 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 263 | $5.23 | $3.39 | — | $4,157.08 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; ret5=+10.7; leftover $1376.97 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 3 | $429.24 | $2.00 | — | $2,867.36 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; ret5=+6.1; leftover $1376.97 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 849 | $1.62 | $10.95 | — | $1,481.03 | — | union ∩ join_present, no 🚨; gate join_present=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $1376.97 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 688 | $2.00 | $8.88 | — | $96.16 | — | union ∩ join_present, no 🚨; gate join_present=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.8; leftover $1376.97 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $96.16 | ▲ close $10,986.07 vs 09:30 $11,015.78 (session +4.58) | 16:00 close · cash $96.16 · equity $10,986.07 vs 09:30 $11,015.78 (-29.71; session marks +4.58) · 8 name(s) marked open→close (per-name table). MOS×57 09:30 $24.00 → close $23.75 -14.25; OCUL×126 09:30 $10.92 → close $10.92 +0.00; INSP×22 09:30 $61.47 → close $61.47 +0.00; CRMD×166 09:30 $8.28 → close $8.28 +0.00; RZLT×263 09:30 $5.23 → close $5.29 +15.78; HCA×3 09:30 $429.24 → close $428.50 -2.22; BMEA×849 09:30 $1.62 → close $1.61 -8.49; NPWR×688 09:30 $2.00 → close $2.02 +13.76 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $96.16 | ▲ 09:30 equity $10,986.07 vs yday $10,986.07 (-0.00) | 09:30 open · cash $96.16 (unchanged overnight, no fees) · equity $10,986.07 vs prior close $10,986.07 (-0.00) · 8 name(s) re-marked at the open (per-name table). MOS×57 yday $23.75 → 09:30 $23.75 +0.00; OCUL×126 yday $10.92 → 09:30 $10.92 +0.00; INSP×22 yday $61.47 → 09:30 $61.47 +0.00; CRMD×166 yday $8.28 → 09:30 $8.28 +0.00; RZLT×263 yday $5.29 → 09:30 $5.29 +0.00; HCA×3 yday $428.50 → 09:30 $428.50 +0.00; BMEA×849 yday $1.61 → 09:30 $1.61 +0.00; NPWR×688 yday $2.02 → 09:30 $2.02 +0.00 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $96.16 | ▲ close $10,986.07 vs 09:30 $10,986.07 (session +0.00) | 16:00 close · cash $96.16 · equity $10,986.07 vs 09:30 $10,986.07 (-0.00; session marks +0.00) · 8 name(s) marked open→close (per-name table). MOS×57 09:30 $23.75 → close $23.75 +0.00; OCUL×126 09:30 $10.92 → close $10.92 +0.00; INSP×22 09:30 $61.47 → close $61.47 +0.00; CRMD×166 09:30 $8.28 → close $8.28 +0.00; RZLT×263 09:30 $5.29 → close $5.29 +0.00; HCA×3 09:30 $428.50 → close $428.50 +0.00; BMEA×849 09:30 $1.61 → close $1.61 +0.00; NPWR×688 09:30 $2.02 → close $2.02 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $96.16 | ▲ 09:30 equity $11,034.44 vs yday $10,986.07 (+48.37) | 09:30 open · cash $96.16 (unchanged overnight, no fees) · equity $11,034.44 vs prior close $10,986.07 (+48.37) · 8 name(s) re-marked at the open (per-name table). MOS×57 yday $23.75 → 09:30 $24.84 +62.13; OCUL×126 yday $10.92 → 09:30 $10.79 -16.38; INSP×22 yday $61.47 → 09:30 $60.07 -30.80; CRMD×166 yday $8.28 → 09:30 $8.60 +53.12; RZLT×263 yday $5.29 → 09:30 $5.01 -73.64; HCA×3 yday $428.50 → 09:30 $427.50 -3.00; BMEA×849 yday $1.61 → 09:30 $1.75 +118.86; NPWR×688 yday $2.02 → 09:30 $1.93 -61.92 | — |
| 2026-08-27 09:30 ET | **SELL** | `OCUL` | 126 | $10.79 | $2.40 | $-21.15 | $1,453.30 | ▼ -21.15 after sell → book $11,032.04; vs 09:30 mark -2.40 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `INSP` | 22 | $60.07 | $2.08 | $-34.93 | $2,772.76 | ▼ -34.93 after sell → book $11,029.96; vs 09:30 mark -2.08 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRMD` | 166 | $8.60 | $2.53 | $+48.11 | $4,197.83 | ▲ +48.11 after sell → book $11,027.43; vs 09:30 mark -2.53 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `RZLT` | 263 | $5.01 | $3.45 | $-64.70 | $5,512.02 | ▼ -64.70 after sell → book $11,023.99; vs 09:30 mark -3.44 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `HCA` | 3 | $427.50 | $2.02 | $-9.24 | $6,792.50 | ▼ -9.24 after sell → book $11,021.97; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BMEA` | 849 | $1.75 | $11.10 | $+88.31 | $8,267.14 | ▲ +88.31 after sell → book $11,010.86; vs 09:30 mark -11.11 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `NPWR` | 688 | $1.93 | $9.00 | $-66.04 | $9,585.98 | ▼ -66.04 after sell → book $11,001.86; vs 09:30 mark -9.00 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 33 | $40.72 | $2.09 | — | $8,240.13 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; ret5=+1.8; leftover $1369.43 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 97 | $14.09 | $2.28 | — | $6,871.12 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; ret5=+1.1; leftover $1369.43 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 528 | $2.59 | $6.81 | — | $5,496.79 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; ret5=+4.2; leftover $1369.43 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 16 | $80.97 | $2.04 | — | $4,199.23 | — | union ∩ join_present, no 🚨; gate join_present=True; list mover_buy; 🔵; ret5=-1.3; leftover $1369.43 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GGB` | 309 | $4.42 | $3.99 | — | $2,829.47 | — | union ∩ join_present, no 🚨; gate join_present=True; list mover_buy; 🔵; ret5=-8.6; leftover $1369.43 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MT` | 18 | $75.12 | $2.04 | — | $1,475.26 | — | union ∩ join_present, no 🚨; gate join_present=True; list mover_buy; 🔵; ret5=-2.2; leftover $1369.43 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $925.74 | $1.99 | — | $547.53 | — | union ∩ join_present, no 🚨; gate join_present=True; list mover_buy; 🔵; ret5=-0.5; leftover $1369.43 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $547.53 | ▲ close $11,004.22 vs 09:30 $11,034.44 (session +23.60) | 16:00 close · cash $547.53 · equity $11,004.22 vs 09:30 $11,034.44 (-30.22; session marks +23.60) · 8 name(s) marked open→close (per-name table). MOS×57 09:30 $24.84 → close $24.16 -38.76; RRC×33 09:30 $40.72 → close $41.55 +27.39; CRK×97 09:30 $14.09 → close $14.50 +39.77; SLI×528 09:30 $2.59 → close $2.61 +10.56; ACMR×16 09:30 $80.97 → close $79.11 -29.76; GGB×309 09:30 $4.42 → close $4.46 +12.36; MT×18 09:30 $75.12 → close $74.53 -10.62; MU×1 09:30 $925.74 → close $938.40 +12.66 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $547.53 | ▲ 09:30 equity $11,081.85 vs yday $11,004.22 (+77.63) | 09:30 open · cash $547.53 (unchanged overnight, no fees) · equity $11,081.85 vs prior close $11,004.22 (+77.63) · 8 name(s) re-marked at the open (per-name table). MOS×57 yday $24.16 → 09:30 $24.00 -9.12; RRC×33 yday $41.55 → 09:30 $41.44 -3.63; CRK×97 yday $14.50 → 09:30 $14.42 -7.76; SLI×528 yday $2.61 → 09:30 $2.60 -5.28; ACMR×16 yday $79.11 → 09:30 $81.65 +40.64; GGB×309 yday $4.46 → 09:30 $4.57 +33.99; MT×18 yday $74.53 → 09:30 $74.54 +0.18; MU×1 yday $938.40 → 09:30 $967.01 +28.61 | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 16 | $81.65 | $2.06 | $+6.78 | $1,851.87 | ▲ +6.78 after sell → book $11,079.79; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GGB` | 309 | $4.57 | $4.05 | $+38.32 | $3,259.95 | ▲ +38.32 after sell → book $11,075.74; vs 09:30 mark -4.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MT` | 18 | $74.54 | $2.06 | $-14.55 | $4,599.61 | ▼ -14.55 after sell → book $11,073.68; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $967.01 | $2.01 | $+37.26 | $5,564.61 | ▲ +37.26 after sell → book $11,071.67; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 9 | $144.70 | $2.02 | — | $4,260.29 | — | union ∩ join_present, no 🚨; gate join_present=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1391.15 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BHVN` | 82 | $16.95 | $2.24 | — | $2,868.15 | — | union ∩ join_present, no 🚨; gate join_present=True; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $1391.15 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 75 | $18.50 | $2.21 | — | $1,478.44 | — | union ∩ join_present, no 🚨; gate join_present=True; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $1391.15 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 151 | $9.19 | $2.44 | — | $88.30 | — | union ∩ join_present, no 🚨; gate join_present=True; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $1391.15 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $88.30 | ▲ close $11,131.45 vs 09:30 $11,081.85 (session +68.70) | 16:00 close · cash $88.30 · equity $11,131.45 vs 09:30 $11,081.85 (+49.60; session marks +68.70) · 8 name(s) marked open→close (per-name table). MOS×57 09:30 $24.00 → close $23.76 -13.68; RRC×33 09:30 $41.44 → close $41.64 +6.60; CRK×97 09:30 $14.42 → close $14.62 +19.40; SLI×528 09:30 $2.60 → close $2.64 +21.12; ANF×9 09:30 $144.70 → close $145.75 +9.45; BHVN×82 09:30 $16.95 → close $16.12 -68.06; BZ×75 09:30 $18.50 → close $18.00 -37.50; CAPR×151 09:30 $9.19 → close $10.06 +131.37 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $88.30 | ▼ 09:30 equity $10,907.58 vs yday $11,131.45 (-223.87) | 09:30 open · cash $88.30 (unchanged overnight, no fees) · equity $10,907.58 vs prior close $11,131.45 (-223.87) · 8 name(s) re-marked at the open (per-name table). MOS×57 yday $23.76 → 09:30 $23.75 -0.57; RRC×33 yday $41.64 → 09:30 $41.11 -17.49; CRK×97 yday $14.62 → 09:30 $14.56 -5.82; SLI×528 yday $2.64 → 09:30 $2.51 -68.64; ANF×9 yday $145.75 → 09:30 $148.67 +26.28; BHVN×82 yday $16.12 → 09:30 $15.44 -55.76; BZ×75 yday $18.00 → 09:30 $17.89 -8.25; CAPR×151 yday $10.06 → 09:30 $9.44 -93.62 | — |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 57 | $23.75 | $2.18 | $-18.59 | $1,439.87 | ▼ -18.59 after sell → book $10,905.40; vs 09:30 mark -2.18 | dropped from list after 4 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `RRC` | 33 | $41.11 | $2.11 | $+8.67 | $2,794.39 | ▲ +8.67 after sell → book $10,903.29; vs 09:30 mark -2.11 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CRK` | 97 | $14.56 | $2.31 | $+41.00 | $4,204.40 | ▲ +41.00 after sell → book $10,900.98; vs 09:30 mark -2.31 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SLI` | 528 | $2.51 | $6.91 | $-55.96 | $5,522.78 | ▼ -55.96 after sell → book $10,894.08; vs 09:30 mark -6.90 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 9 | $148.67 | $2.04 | $+31.68 | $6,858.77 | ▲ +31.68 after sell → book $10,892.04; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BHVN` | 82 | $15.44 | $2.26 | $-128.32 | $8,122.59 | ▼ -128.32 after sell → book $10,889.78; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BZ` | 75 | $17.89 | $2.24 | $-50.20 | $9,462.10 | ▼ -50.20 after sell → book $10,887.54; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 151 | $9.44 | $2.48 | $+32.83 | $10,885.06 | ▲ +32.83 after sell → book $10,885.06; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,885.06 | ▲ close $10,885.06 vs 09:30 $10,907.58 (session +0.00) | 16:00 close · cash $10,885.06 · no lots left · equity $10,885.06. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,885.06 | ▲ 09:30 equity $10,885.06 vs yday $10,885.06 (+0.00) | 09:30 open · cash $10,885.06 · no holdings · equity $10,885.06 vs prior close $10,885.06 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,885.06 | ▲ close $10,885.06 vs 09:30 $10,885.06 (session +0.00) | 16:00 close · cash $10,885.06 · no lots left · equity $10,885.06. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,885.06 | ▲ 09:30 equity $10,885.06 vs yday $10,885.06 (+0.00) | 09:30 open · cash $10,885.06 · no holdings · equity $10,885.06 vs prior close $10,885.06 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,885.06 | ▲ close $10,885.06 vs 09:30 $10,885.06 (session +0.00) | 16:00 close · cash $10,885.06 · no lots left · equity $10,885.06. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,885.06 | ▲ 09:30 equity $10,885.06 vs yday $10,885.06 (+0.00) | 09:30 open · cash $10,885.06 · no holdings · equity $10,885.06 vs prior close $10,885.06 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 27 | $49.76 | $2.07 | — | $9,539.47 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1360.63 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 32 | $41.31 | $2.09 | — | $8,215.46 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $1360.63 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 416 | $3.27 | $5.37 | — | $6,849.78 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1360.63 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 176 | $7.70 | $2.52 | — | $5,492.06 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $1360.63 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $125.94 | $2.02 | — | $4,230.64 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1360.63 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 1115 | $1.22 | $14.38 | — | $2,855.96 | — | union ∩ join_present, no 🚨; gate join_present=True; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1360.63 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRVO` | 73 | $18.40 | $2.21 | — | $1,510.55 | — | union ∩ join_present, no 🚨; gate join_present=True; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $1360.63 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 86 | $15.70 | $2.25 | — | $158.10 | — | union ∩ join_present, no 🚨; gate join_present=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1360.63 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $158.10 | ▲ close $11,688.92 vs 09:30 $10,885.06 (session +836.76) | 16:00 close · cash $158.10 · equity $11,688.92 vs 09:30 $10,885.06 (+803.86; session marks +836.76) · 8 name(s) marked open→close (per-name table). ATRC×27 09:30 $49.76 → close $52.59 +76.41; HRMY×32 09:30 $41.31 → close $42.86 +49.60; CABA×416 09:30 $3.27 → close $3.57 +124.80; VSTM×176 09:30 $7.70 → close $8.02 +56.32; RVTY×10 09:30 $125.94 → close $130.94 +50.00; GPRO×1115 09:30 $1.22 → close $1.69 +524.05; FRVO×73 09:30 $18.40 → close $17.98 -30.66; CRK×86 09:30 $15.70 → close $15.54 -13.76 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $158.10 | ▲ 09:30 equity $11,854.59 vs yday $11,688.92 (+165.67) | 09:30 open · cash $158.10 (unchanged overnight, no fees) · equity $11,854.59 vs prior close $11,688.92 (+165.67) · 8 name(s) re-marked at the open (per-name table). ATRC×27 yday $52.59 → 09:30 $52.88 +7.83; HRMY×32 yday $42.86 → 09:30 $42.93 +2.24; CABA×416 yday $3.57 → 09:30 $3.63 +24.96; VSTM×176 yday $8.02 → 09:30 $8.03 +1.76; RVTY×10 yday $130.94 → 09:30 $132.45 +15.10; GPRO×1115 yday $1.69 → 09:30 $1.78 +100.35; FRVO×73 yday $17.98 → 09:30 $18.27 +21.17; CRK×86 yday $15.54 → 09:30 $15.45 -7.74 | — |
| 2026-09-04 09:30 ET | **SELL** | `HRMY` | 32 | $42.93 | $2.11 | $+47.65 | $1,529.75 | ▲ +47.65 after sell → book $11,852.48; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 176 | $8.03 | $2.56 | $+53.00 | $2,940.47 | ▲ +53.00 after sell → book $11,849.92; vs 09:30 mark -2.56 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 10 | $132.45 | $2.04 | $+61.04 | $4,262.93 | ▲ +61.04 after sell → book $11,847.88; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `FRVO` | 73 | $18.27 | $2.23 | $-13.93 | $5,594.41 | ▼ -13.93 after sell → book $11,845.65; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 86 | $15.45 | $2.27 | $-26.02 | $6,920.84 | ▼ -26.02 after sell → book $11,843.38; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ASND` | 5 | $266.94 | $2.00 | — | $5,584.13 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; ret5=+1.9; leftover $1384.17 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OSCR` | 45 | $30.65 | $2.12 | — | $4,202.76 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ret5=-2.2; leftover $1384.17 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `NVAX` | 132 | $10.41 | $2.39 | — | $2,826.25 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $1384.17 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BVS` | 95 | $14.50 | $2.27 | — | $1,446.48 | — | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ⚪; ret5=+0.8; leftover $1384.17 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 709 | $1.95 | $9.15 | — | $54.78 | — | union ∩ join_present, no 🚨; gate join_present=True; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $1384.17 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $54.78 | ▼ close $11,379.67 vs 09:30 $11,854.59 (session -445.77) | 16:00 close · cash $54.78 · equity $11,379.67 vs 09:30 $11,854.59 (-474.92; session marks -445.77) · 8 name(s) marked open→close (per-name table). ATRC×27 09:30 $52.88 → close $52.46 -11.34; CABA×416 09:30 $3.63 → close $3.48 -62.40; GPRO×1115 09:30 $1.78 → close $1.39 -434.85; ASND×5 09:30 $266.94 → close $271.12 +20.90; OSCR×45 09:30 $30.65 → close $32.24 +71.55; NVAX×132 09:30 $10.41 → close $10.34 -9.24; BVS×95 09:30 $14.50 → close $14.36 -13.30; BAK×709 09:30 $1.95 → close $1.94 -7.09 | — |

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
| 2026-08-19 | `MLYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `GUTS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-26 | `BMEA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `NPWR` | no_price | no 09:30 open — carry |
| 2026-08-26 | `KURA` | no_price | no 09:30 open |
| 2026-08-26 | `AVBP` | no_price | no 09:30 open |
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
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRVO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FATE` | hard_red | hard-red S=-3.83 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ATRC` | 27 | 2026-09-03 @ $49.76 | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1360.63 |
| `CABA` | 416 | 2026-09-03 @ $3.27 | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1360.63 |
| `GPRO` | 1115 | 2026-09-03 @ $1.22 | union ∩ join_present, no 🚨; gate join_present=True; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1360.63 |
| `ASND` | 5 | 2026-09-04 @ $266.94 | union ∩ join_present, no 🚨; gate join_present=True; list flatten; ret5=+1.9; leftover $1384.17 |
| `OSCR` | 45 | 2026-09-04 @ $30.65 | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ret5=-2.2; leftover $1384.17 |
| `NVAX` | 132 | 2026-09-04 @ $10.41 | union ∩ join_present, no 🚨; gate join_present=True; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $1384.17 |
| `BVS` | 95 | 2026-09-04 @ $14.50 | union ∩ join_present, no 🚨; gate join_present=True; list flatten; 🔵; ⚪; ret5=+0.8; leftover $1384.17 |
| `BAK` | 709 | 2026-09-04 @ $1.95 | union ∩ join_present, no 🚨; gate join_present=True; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $1384.17 |
