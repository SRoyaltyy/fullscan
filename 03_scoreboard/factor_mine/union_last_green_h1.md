# Factor mine action — `union_last_green_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ last_green, no 🚨

Cash book **+13.14%** ($11,314) · signal-only (no cash/fees) was +15.19%. Starts YES **16/17**. Fills 130 · skips 57 · realized $+1017.55.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `last_green=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $414.63.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `BTSG` | 33 | — | $59.80 | +0.00 | $60.23 | +14.19 | +14.19 | +0.00 | +14.19 |
| 2026-08-13 | `IREN` | 43 | — | $45.98 | +0.00 | $44.76 | -52.46 | -52.46 | +0.00 | -52.46 |
| 2026-08-13 | `TPG` | 39 | — | $50.62 | +0.00 | $54.62 | +155.88 | +155.88 | +0.00 | +155.88 |
| 2026-08-13 | `INO` | 2469 | — | $0.81 | +0.00 | $0.90 | +222.21 | +222.21 | +0.00 | +222.21 |
| 2026-08-13 | `TNDM` | 85 | — | $23.33 | +0.00 | $23.13 | -17.00 | -17.00 | +0.00 | -17.00 |
| 2026-08-14 | `BTSG` | 33 | $60.23 | $59.65 | -19.14 | — | +0.00 | -19.14 | -4.95 | — |
| 2026-08-14 | `IREN` | 43 | $44.76 | $44.09 | -28.81 | — | +0.00 | -28.81 | -81.27 | — |
| 2026-08-14 | `TPG` | 39 | $54.62 | $55.29 | +26.13 | — | +0.00 | +26.13 | +182.01 | — |
| 2026-08-14 | `INO` | 2469 | $0.90 | $0.93 | +74.07 | — | +0.00 | +74.07 | +296.28 | — |
| 2026-08-14 | `TNDM` | 85 | $23.13 | $22.92 | -17.85 | — | +0.00 | -17.85 | -34.85 | — |
| 2026-08-14 | `VST` | 8 | — | $146.90 | +0.00 | $148.13 | +9.84 | +9.84 | +0.00 | +9.84 |
| 2026-08-14 | `DAVE` | 3 | — | $330.91 | +0.00 | $334.57 | +10.98 | +10.98 | +0.00 | +10.98 |
| 2026-08-14 | `SLG` | 22 | — | $57.61 | +0.00 | $56.09 | -33.44 | -33.44 | +0.00 | -33.44 |
| 2026-08-14 | `LDI` | 1371 | — | $0.94 | +0.00 | $0.90 | -54.84 | -54.84 | +0.00 | -54.84 |
| 2026-08-14 | `BTBT` | 856 | — | $1.50 | +0.00 | $1.57 | +59.92 | +59.92 | +0.00 | +59.92 |
| 2026-08-14 | `BETR` | 86 | — | $14.80 | +0.00 | $13.73 | -92.02 | -92.02 | +0.00 | -92.02 |
| 2026-08-14 | `ANGX` | 298 | — | $4.31 | +0.00 | $4.37 | +17.88 | +17.88 | +0.00 | +17.88 |
| 2026-08-14 | `HYLN` | 307 | — | $4.18 | +0.00 | $4.06 | -36.84 | -36.84 | +0.00 | -36.84 |
| 2026-08-17 | `VST` | 8 | $148.13 | $149.37 | +9.92 | — | +0.00 | +9.92 | +19.76 | — |
| 2026-08-17 | `DAVE` | 3 | $334.57 | $336.94 | +7.11 | — | +0.00 | +7.11 | +18.09 | — |
| 2026-08-17 | `SLG` | 22 | $56.09 | $55.37 | -15.84 | — | +0.00 | -15.84 | -49.28 | — |
| 2026-08-17 | `LDI` | 1371 | $0.90 | $0.91 | +13.71 | — | +0.00 | +13.71 | -41.13 | — |
| 2026-08-17 | `BTBT` | 856 | $1.57 | $1.52 | -42.80 | — | +0.00 | -42.80 | +17.12 | — |
| 2026-08-17 | `BETR` | 86 | $13.73 | $13.67 | -5.16 | — | +0.00 | -5.16 | -97.18 | — |
| 2026-08-17 | `ANGX` | 298 | $4.37 | $4.60 | +68.54 | — | +0.00 | +68.54 | +86.42 | — |
| 2026-08-17 | `HYLN` | 307 | $4.06 | $4.10 | +12.28 | — | +0.00 | +12.28 | -24.56 | — |
| 2026-08-17 | `DVN` | 27 | — | $46.18 | +0.00 | $47.57 | +37.53 | +37.53 | +0.00 | +37.53 |
| 2026-08-17 | `EOG` | 8 | — | $142.77 | +0.00 | $146.15 | +27.04 | +27.04 | +0.00 | +27.04 |
| 2026-08-17 | `FANG` | 6 | — | $202.70 | +0.00 | $206.29 | +21.54 | +21.54 | +0.00 | +21.54 |
| 2026-08-17 | `NB` | 249 | — | $5.07 | +0.00 | $4.81 | -64.74 | -64.74 | +0.00 | -64.74 |
| 2026-08-17 | `CDNL` | 31 | — | $39.85 | +0.00 | $39.23 | -19.22 | -19.22 | +0.00 | -19.22 |
| 2026-08-17 | `ABX` | 138 | — | $9.12 | +0.00 | $9.12 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-17 | `VERA` | 40 | — | $31.30 | +0.00 | $31.63 | +13.20 | +13.20 | +0.00 | +13.20 |
| 2026-08-17 | `CELC` | 13 | — | $92.99 | +0.00 | $92.44 | -7.15 | -7.15 | +0.00 | -7.15 |
| 2026-08-18 | `DVN` | 27 | $47.57 | $48.00 | +11.61 | — | +0.00 | +11.61 | +49.14 | — |
| 2026-08-18 | `EOG` | 8 | $146.15 | $148.04 | +15.12 | — | +0.00 | +15.12 | +42.16 | — |
| 2026-08-18 | `FANG` | 6 | $206.29 | $208.93 | +15.84 | — | +0.00 | +15.84 | +37.38 | — |
| 2026-08-18 | `NB` | 249 | $4.81 | $4.66 | -37.35 | — | +0.00 | -37.35 | -102.09 | — |
| 2026-08-18 | `CDNL` | 31 | $39.23 | $41.57 | +72.54 | — | +0.00 | +72.54 | +53.32 | — |
| 2026-08-18 | `ABX` | 138 | $9.12 | $9.03 | -12.42 | — | +0.00 | -12.42 | -12.42 | — |
| 2026-08-18 | `VERA` | 40 | $31.63 | $31.31 | -12.80 | — | +0.00 | -12.80 | +0.40 | — |
| 2026-08-18 | `CELC` | 13 | $92.44 | $92.38 | -0.78 | — | +0.00 | -0.78 | -7.93 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `AG` | 61 | — | $20.55 | +0.00 | $21.19 | +39.04 | +39.04 | +0.00 | +39.04 |
| 2026-08-20 | `CDE` | 61 | — | $20.65 | +0.00 | $21.11 | +28.06 | +28.06 | +0.00 | +28.06 |
| 2026-08-20 | `HDSN` | 219 | — | $5.77 | +0.00 | $5.57 | -43.80 | -43.80 | +0.00 | -43.80 |
| 2026-08-20 | `IAG` | 64 | — | $19.63 | +0.00 | $20.50 | +55.68 | +55.68 | +0.00 | +55.68 |
| 2026-08-20 | `KGC` | 42 | — | $29.63 | +0.00 | $31.43 | +75.60 | +75.60 | +0.00 | +75.60 |
| 2026-08-20 | `NFGC` | 724 | — | $1.75 | +0.00 | $1.75 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `WPM` | 8 | — | $144.54 | +0.00 | $150.25 | +45.68 | +45.68 | +0.00 | +45.68 |
| 2026-08-20 | `ABUS` | 257 | — | $4.92 | +0.00 | $4.77 | -38.55 | -38.55 | +0.00 | -38.55 |
| 2026-08-21 | `AG` | 61 | $21.19 | $21.90 | +43.31 | — | +0.00 | +43.31 | +82.35 | — |
| 2026-08-21 | `CDE` | 61 | $21.11 | $21.75 | +39.04 | — | +0.00 | +39.04 | +67.10 | — |
| 2026-08-21 | `HDSN` | 219 | $5.57 | $5.67 | +21.90 | — | +0.00 | +21.90 | -21.90 | — |
| 2026-08-21 | `IAG` | 64 | $20.50 | $21.17 | +42.88 | — | +0.00 | +42.88 | +98.56 | — |
| 2026-08-21 | `KGC` | 42 | $31.43 | $32.17 | +31.08 | — | +0.00 | +31.08 | +106.68 | — |
| 2026-08-21 | `NFGC` | 724 | $1.75 | $1.79 | +28.96 | — | +0.00 | +28.96 | +28.96 | — |
| 2026-08-21 | `WPM` | 8 | $150.25 | $154.70 | +35.60 | — | +0.00 | +35.60 | +81.28 | — |
| 2026-08-21 | `ABUS` | 257 | $4.77 | $5.20 | +110.51 | — | +0.00 | +110.51 | +71.96 | — |
| 2026-08-21 | `AU` | 11 | — | $119.43 | +0.00 | $121.22 | +19.69 | +19.69 | +0.00 | +19.69 |
| 2026-08-21 | `AUPH` | 77 | — | $17.20 | +0.00 | $16.65 | -42.35 | -42.35 | +0.00 | -42.35 |
| 2026-08-21 | `AEM` | 6 | — | $216.30 | +0.00 | $216.06 | -1.44 | -1.44 | +0.00 | -1.44 |
| 2026-08-21 | `ARCT` | 119 | — | $11.13 | +0.00 | $13.45 | +276.08 | +276.08 | +0.00 | +276.08 |
| 2026-08-21 | `CYPH` | 1004 | — | $1.32 | +0.00 | $1.42 | +100.40 | +100.40 | +0.00 | +100.40 |
| 2026-08-21 | `BTBT` | 798 | — | $1.66 | +0.00 | $1.53 | -103.74 | -103.74 | +0.00 | -103.74 |
| 2026-08-21 | `DE` | 2 | — | $623.26 | +0.00 | $647.47 | +48.42 | +48.42 | +0.00 | +48.42 |
| 2026-08-21 | `QDEL` | 88 | — | $14.96 | +0.00 | $14.74 | -19.36 | -19.36 | +0.00 | -19.36 |
| 2026-08-24 | `AU` | 11 | $121.22 | $120.50 | -7.92 | — | +0.00 | -7.92 | +11.77 | — |
| 2026-08-24 | `AUPH` | 77 | $16.65 | $16.60 | -3.85 | — | +0.00 | -3.85 | -46.20 | — |
| 2026-08-24 | `AEM` | 6 | $216.06 | $217.03 | +5.82 | — | +0.00 | +5.82 | +4.38 | — |
| 2026-08-24 | `ARCT` | 119 | $13.45 | $13.26 | -22.61 | — | +0.00 | -22.61 | +253.47 | — |
| 2026-08-24 | `CYPH` | 1004 | $1.42 | $1.83 | +411.64 | — | +0.00 | +411.64 | +512.04 | — |
| 2026-08-24 | `BTBT` | 798 | $1.53 | $1.55 | +15.96 | — | +0.00 | +15.96 | -87.78 | — |
| 2026-08-24 | `DE` | 2 | $647.47 | $653.62 | +12.30 | — | +0.00 | +12.30 | +60.72 | — |
| 2026-08-24 | `QDEL` | 88 | $14.74 | $14.71 | -2.64 | — | +0.00 | -2.64 | -22.00 | — |
| 2026-08-25 | `MOS` | 58 | — | $24.00 | +0.00 | $23.75 | -14.50 | -14.50 | +0.00 | -14.50 |
| 2026-08-25 | `INSP` | 22 | — | $61.47 | +0.00 | $61.47 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-25 | `RZLT` | 268 | — | $5.23 | +0.00 | $5.29 | +16.08 | +16.08 | +0.00 | +16.08 |
| 2026-08-25 | `HCA` | 3 | — | $429.24 | +0.00 | $428.50 | -2.22 | -2.22 | +0.00 | -2.22 |
| 2026-08-25 | `NPWR` | 701 | — | $2.00 | +0.00 | $2.02 | +14.02 | +14.02 | +0.00 | +14.02 |
| 2026-08-25 | `ALVO` | 268 | — | $5.22 | +0.00 | $5.25 | +8.04 | +8.04 | +0.00 | +8.04 |
| 2026-08-25 | `ALIT` | 94 | — | $14.86 | +0.00 | $14.87 | +0.94 | +0.94 | +0.00 | +0.94 |
| 2026-08-25 | `ZURA` | 219 | — | $6.38 | +0.00 | $6.50 | +26.28 | +26.28 | +0.00 | +26.28 |
| 2026-08-26 | `MOS` | 58 | $23.75 | $23.75 | +0.00 | $23.75 | +0.00 | +0.00 | -14.50 | -14.50 |
| 2026-08-26 | `INSP` | 22 | $61.47 | $61.47 | +0.00 | $61.47 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-26 | `RZLT` | 268 | $5.29 | $5.29 | +0.00 | $5.29 | +0.00 | +0.00 | +16.08 | +16.08 |
| 2026-08-26 | `HCA` | 3 | $428.50 | $428.50 | +0.00 | $428.50 | +0.00 | +0.00 | -2.22 | -2.22 |
| 2026-08-26 | `NPWR` | 701 | $2.02 | $2.02 | +0.00 | $2.02 | +0.00 | +0.00 | +14.02 | +14.02 |
| 2026-08-26 | `ALVO` | 268 | $5.25 | $5.25 | +0.00 | $5.25 | +0.00 | +0.00 | +8.04 | +8.04 |
| 2026-08-26 | `ALIT` | 94 | $14.87 | $14.87 | +0.00 | $14.87 | +0.00 | +0.00 | +0.94 | +0.94 |
| 2026-08-26 | `ZURA` | 219 | $6.50 | $6.50 | +0.00 | $6.50 | +0.00 | +0.00 | +26.28 | +26.28 |
| 2026-08-27 | `MOS` | 58 | $23.75 | $24.84 | +63.22 | $24.16 | -39.44 | +23.78 | +48.72 | +9.28 |
| 2026-08-27 | `INSP` | 22 | $61.47 | $60.07 | -30.80 | — | +0.00 | -30.80 | -30.80 | — |
| 2026-08-27 | `RZLT` | 268 | $5.29 | $5.01 | -75.04 | — | +0.00 | -75.04 | -58.96 | — |
| 2026-08-27 | `HCA` | 3 | $428.50 | $427.50 | -3.00 | — | +0.00 | -3.00 | -5.22 | — |
| 2026-08-27 | `NPWR` | 701 | $2.02 | $1.93 | -63.09 | — | +0.00 | -63.09 | -49.07 | — |
| 2026-08-27 | `ALVO` | 268 | $5.25 | $4.98 | -72.36 | — | +0.00 | -72.36 | -64.32 | — |
| 2026-08-27 | `ALIT` | 94 | $14.87 | $14.85 | -1.88 | — | +0.00 | -1.88 | -0.94 | — |
| 2026-08-27 | `ZURA` | 219 | $6.50 | $6.13 | -81.03 | — | +0.00 | -81.03 | -54.75 | — |
| 2026-08-27 | `RRC` | 33 | — | $40.72 | +0.00 | $41.55 | +27.39 | +27.39 | +0.00 | +27.39 |
| 2026-08-27 | `CRK` | 96 | — | $14.09 | +0.00 | $14.50 | +39.36 | +39.36 | +0.00 | +39.36 |
| 2026-08-27 | `SLI` | 524 | — | $2.59 | +0.00 | $2.61 | +10.48 | +10.48 | +0.00 | +10.48 |
| 2026-08-27 | `ANET` | 7 | — | $190.90 | +0.00 | $202.25 | +79.45 | +79.45 | +0.00 | +79.45 |
| 2026-08-27 | `DLO` | 87 | — | $15.60 | +0.00 | $15.36 | -20.88 | -20.88 | +0.00 | -20.88 |
| 2026-08-27 | `GEN` | 47 | — | $28.89 | +0.00 | $29.64 | +35.25 | +35.25 | +0.00 | +35.25 |
| 2026-08-28 | `MOS` | 58 | $24.16 | $24.00 | -9.28 | $23.76 | -13.92 | -23.20 | +0.00 | -13.92 |
| 2026-08-28 | `RRC` | 33 | $41.55 | $41.44 | -3.63 | $41.64 | +6.60 | +2.97 | +23.76 | +30.36 |
| 2026-08-28 | `CRK` | 96 | $14.50 | $14.42 | -7.68 | $14.62 | +19.20 | +11.52 | +31.68 | +50.88 |
| 2026-08-28 | `SLI` | 524 | $2.61 | $2.60 | -5.24 | $2.64 | +20.96 | +15.72 | +5.24 | +26.20 |
| 2026-08-28 | `ANET` | 7 | $202.25 | $205.90 | +25.55 | — | +0.00 | +25.55 | +105.00 | — |
| 2026-08-28 | `DLO` | 87 | $15.36 | $15.33 | -2.61 | — | +0.00 | -2.61 | -23.49 | — |
| 2026-08-28 | `GEN` | 47 | $29.64 | $29.83 | +8.93 | — | +0.00 | +8.93 | +44.18 | — |
| 2026-08-28 | `ANF` | 9 | — | $144.70 | +0.00 | $145.75 | +9.45 | +9.45 | +0.00 | +9.45 |
| 2026-08-28 | `BHVN` | 82 | — | $16.95 | +0.00 | $16.12 | -68.06 | -68.06 | +0.00 | -68.06 |
| 2026-08-28 | `BZ` | 75 | — | $18.50 | +0.00 | $18.00 | -37.50 | -37.50 | +0.00 | -37.50 |
| 2026-08-28 | `LVWR` | 1007 | — | $1.38 | +0.00 | $1.36 | -20.14 | -20.14 | +0.00 | -20.14 |
| 2026-08-31 | `MOS` | 58 | $23.76 | $23.75 | -0.58 | — | +0.00 | -0.58 | -14.50 | — |
| 2026-08-31 | `RRC` | 33 | $41.64 | $41.11 | -17.49 | — | +0.00 | -17.49 | +12.87 | — |
| 2026-08-31 | `CRK` | 96 | $14.62 | $14.56 | -5.76 | — | +0.00 | -5.76 | +45.12 | — |
| 2026-08-31 | `SLI` | 524 | $2.64 | $2.51 | -68.12 | — | +0.00 | -68.12 | -41.92 | — |
| 2026-08-31 | `ANF` | 9 | $145.75 | $148.67 | +26.28 | — | +0.00 | +26.28 | +35.73 | — |
| 2026-08-31 | `BHVN` | 82 | $16.12 | $15.44 | -55.76 | — | +0.00 | -55.76 | -123.82 | — |
| 2026-08-31 | `BZ` | 75 | $18.00 | $17.89 | -8.25 | — | +0.00 | -8.25 | -45.75 | — |
| 2026-08-31 | `LVWR` | 1007 | $1.36 | $1.37 | +10.07 | — | +0.00 | +10.07 | -10.07 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `ATRC` | 27 | — | $49.76 | +0.00 | $52.59 | +76.41 | +76.41 | +0.00 | +76.41 |
| 2026-09-03 | `HRMY` | 32 | — | $41.31 | +0.00 | $42.86 | +49.60 | +49.60 | +0.00 | +49.60 |
| 2026-09-03 | `VSTM` | 175 | — | $7.70 | +0.00 | $8.02 | +56.00 | +56.00 | +0.00 | +56.00 |
| 2026-09-03 | `RVTY` | 10 | — | $125.94 | +0.00 | $130.94 | +50.00 | +50.00 | +0.00 | +50.00 |
| 2026-09-03 | `GPRO` | 1107 | — | $1.22 | +0.00 | $1.69 | +520.29 | +520.29 | +0.00 | +520.29 |
| 2026-09-03 | `CRK` | 86 | — | $15.70 | +0.00 | $15.54 | -13.76 | -13.76 | +0.00 | -13.76 |
| 2026-09-03 | `MMED` | 59 | — | $22.78 | +0.00 | $23.76 | +57.82 | +57.82 | +0.00 | +57.82 |
| 2026-09-03 | `SLN` | 91 | — | $14.70 | +0.00 | $14.79 | +8.19 | +8.19 | +0.00 | +8.19 |
| 2026-09-04 | `ATRC` | 27 | $52.59 | $52.88 | +7.83 | $52.46 | -11.34 | -3.51 | +84.24 | +72.90 |
| 2026-09-04 | `HRMY` | 32 | $42.86 | $42.93 | +2.24 | — | +0.00 | +2.24 | +51.84 | — |
| 2026-09-04 | `VSTM` | 175 | $8.02 | $8.03 | +1.75 | — | +0.00 | +1.75 | +57.75 | — |
| 2026-09-04 | `RVTY` | 10 | $130.94 | $132.45 | +15.10 | — | +0.00 | +15.10 | +65.10 | — |
| 2026-09-04 | `GPRO` | 1107 | $1.69 | $1.78 | +99.63 | $1.39 | -431.73 | -332.10 | +619.92 | +188.19 |
| 2026-09-04 | `CRK` | 86 | $15.54 | $15.45 | -7.74 | — | +0.00 | -7.74 | -21.50 | — |
| 2026-09-04 | `MMED` | 59 | $23.76 | $23.88 | +7.08 | — | +0.00 | +7.08 | +64.90 | — |
| 2026-09-04 | `SLN` | 91 | $14.79 | $14.85 | +5.46 | — | +0.00 | +5.46 | +13.65 | — |
| 2026-09-04 | `OSCR` | 45 | — | $30.65 | +0.00 | $32.24 | +71.55 | +71.55 | +0.00 | +71.55 |
| 2026-09-04 | `NVAX` | 132 | — | $10.41 | +0.00 | $10.34 | -9.24 | -9.24 | +0.00 | -9.24 |
| 2026-09-04 | `BVS` | 95 | — | $14.50 | +0.00 | $14.36 | -13.30 | -13.30 | +0.00 | -13.30 |
| 2026-09-04 | `BAK` | 710 | — | $1.95 | +0.00 | $1.94 | -7.10 | -7.10 | +0.00 | -7.10 |
| 2026-09-04 | `EOSE` | 387 | — | $3.57 | +0.00 | $3.50 | -27.09 | -27.09 | +0.00 | -27.09 |
| 2026-09-04 | `DELL` | 2 | — | $486.31 | +0.00 | $516.39 | +60.16 | +60.16 | +0.00 | +60.16 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +322.82 | BTSG, IREN, TPG, INO, TNDM | — | $56.25 | $10,286.85 | BTSG×33, IREN×43, TPG×39, INO×2469, TNDM×85 |
| 2026-08-14 | +5.50 | $56.25 | BTSG×33, IREN×43, TPG×39, INO×2469, TNDM×85 | $10,321.25 | +34.40 | -118.52 | VST, DAVE, SLG, LDI, BTBT, BETR, ANGX, HYLN | BTSG, IREN, TPG, INO, TNDM | $393.24 | $10,119.14 | VST×8, DAVE×3, SLG×22, LDI×1371, BTBT×856, BETR×86, ANGX×298, HYLN×307 |
| 2026-08-17 | +2.25 | $393.24 | VST×8, DAVE×3, SLG×22, LDI×1371, BTBT×856, BETR×86, ANGX×298, HYLN×307 | $10,166.90 | +47.76 | +8.20 | DVN, EOG, FANG, NB, CDNL, ABX, VERA, CELC | VST, DAVE, SLG, LDI, BTBT, BETR, ANGX, HYLN | $282.23 | $10,112.86 | DVN×27, EOG×8, FANG×6, NB×249, CDNL×31, ABX×138, VERA×40, CELC×13 |
| 2026-08-18 | -6.20 | $282.23 | DVN×27, EOG×8, FANG×6, NB×249, CDNL×31, ABX×138, VERA×40, CELC×13 | $10,164.62 | +51.76 | +0.00 | — | DVN, EOG, FANG, NB, CDNL, ABX, VERA, CELC | $10,146.49 | $10,146.49 | — |
| 2026-08-19 | -7.20 | $10,146.49 | — | $10,146.49 | -0.00 | +0.00 | — | — | $10,146.49 | $10,146.49 | — |
| 2026-08-20 | +1.12 | $10,146.49 | — | $10,146.49 | -0.00 | +161.71 | AG, CDE, HDSN, IAG, KGC, NFGC, WPM, ABUS | — | $154.98 | $10,282.06 | AG×61, CDE×61, HDSN×219, IAG×64, KGC×42, NFGC×724, WPM×8, ABUS×257 |
| 2026-08-21 | +3.25 | $154.98 | AG×61, CDE×61, HDSN×219, IAG×64, KGC×42, NFGC×724, WPM×8, ABUS×257 | $10,635.34 | +353.28 | +277.70 | AU, AUPH, AEM, ARCT, CYPH, BTBT, DE, QDEL | AG, CDE, HDSN, IAG, KGC, NFGC, WPM, ABUS | $99.41 | $10,850.47 | AU×11, AUPH×77, AEM×6, ARCT×119, CYPH×1004, BTBT×798, DE×2, QDEL×88 |
| 2026-08-24 | -5.17 | $99.41 | AU×11, AUPH×77, AEM×6, ARCT×119, CYPH×1004, BTBT×798, DE×2, QDEL×88 | $11,259.17 | +408.70 | +0.00 | — | AU, AUPH, AEM, ARCT, CYPH, BTBT, DE, QDEL | $11,222.61 | $11,222.61 | — |
| 2026-08-25 | +1.80 | $11,222.61 | — | $11,222.61 | +0.00 | +48.64 | MOS, INSP, RZLT, HCA, NPWR, ALVO, ALIT, ZURA | — | $166.62 | $11,243.98 | MOS×58, INSP×22, RZLT×268, HCA×3, NPWR×701, ALVO×268, ALIT×94, ZURA×219 |
| 2026-08-26 | +2.02 | $166.62 | MOS×58, INSP×22, RZLT×268, HCA×3, NPWR×701, ALVO×268, ALIT×94, ZURA×219 | $11,243.98 | -0.00 | +0.00 | — | — | $166.62 | $11,243.98 | MOS×58, INSP×22, RZLT×268, HCA×3, NPWR×701, ALVO×268, ALIT×94, ZURA×219 |
| 2026-08-27 | — | $166.62 | MOS×58, INSP×22, RZLT×268, HCA×3, NPWR×701, ALVO×268, ALIT×94, ZURA×219 | $10,980.00 | -263.98 | +131.61 | RRC, CRK, SLI, ANET, DLO, GEN | INSP, RZLT, HCA, NPWR, ALVO, ALIT, ZURA | $1,391.41 | $11,068.63 | MOS×58, RRC×33, CRK×96, SLI×524, ANET×7, DLO×87, GEN×47 |
| 2026-08-28 | +0.75 | $1,391.41 | MOS×58, RRC×33, CRK×96, SLI×524, ANET×7, DLO×87, GEN×47 | $11,074.67 | +6.04 | -83.41 | ANF, BHVN, BZ, LVWR | ANET, DLO, GEN | $73.15 | $10,965.34 | MOS×58, RRC×33, CRK×96, SLI×524, ANF×9, BHVN×82, BZ×75, LVWR×1007 |
| 2026-08-31 | -5.85 | $73.15 | MOS×58, RRC×33, CRK×96, SLI×524, ANF×9, BHVN×82, BZ×75, LVWR×1007 | $10,845.73 | -119.61 | +0.00 | — | MOS, RRC, CRK, SLI, ANF, BHVN, BZ, LVWR | $10,812.57 | $10,812.57 | — |
| 2026-09-01 | -6.30 | $10,812.57 | — | $10,812.57 | -0.00 | +0.00 | — | — | $10,812.57 | $10,812.57 | — |
| 2026-09-02 | -3.83 | $10,812.57 | — | $10,812.57 | -0.00 | +0.00 | — | — | $10,812.57 | $10,812.57 | — |
| 2026-09-03 | -0.90 | $10,812.57 | — | $10,812.57 | -0.00 | +804.55 | ATRC, HRMY, VSTM, RVTY, GPRO, CRK, MMED, SLN | — | $128.12 | $11,587.47 | ATRC×27, HRMY×32, VSTM×175, RVTY×10, GPRO×1107, CRK×86, MMED×59, SLN×91 |
| 2026-09-04 | — | $128.12 | ATRC×27, HRMY×32, VSTM×175, RVTY×10, GPRO×1107, CRK×86, MMED×59, SLN×91 | $11,718.82 | +131.35 | -368.09 | OSCR, NVAX, BVS, BAK, EOSE, DELL | HRMY, VSTM, RVTY, CRK, MMED, SLN | $414.63 | $11,314.34 | ATRC×27, GPRO×1107, OSCR×45, NVAX×132, BVS×95, BAK×710, EOSE×387, DELL×2 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 33 | $59.80 | $2.09 | — | $8,024.51 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ⚪; ret5=-5.3; leftover $2000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 43 | $45.98 | $2.12 | — | $6,045.25 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ⚪; ret5=+12.3; leftover $2000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 39 | $50.62 | $2.11 | — | $4,068.84 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ⚪; ret5=+6.2; leftover $2000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 2469 | $0.81 | $27.41 | — | $2,041.54 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ⚪; ret5=+13.2; leftover $2000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 85 | $23.33 | $2.25 | — | $56.25 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ⚪; ret5=+19.7; leftover $2000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $56.25 | ▲ close $10,286.85 vs 09:30 $10,000.00 (session +322.82) | 16:00 close · cash $56.25 · equity $10,286.85 vs 09:30 $10,000.00 (+286.85; session marks +322.82) · 5 name(s) marked open→close (per-name table). BTSG×33 09:30 $59.80 → close $60.23 +14.19; IREN×43 09:30 $45.98 → close $44.76 -52.46; TPG×39 09:30 $50.62 → close $54.62 +155.88; INO×2469 09:30 $0.81 → close $0.90 +222.21; TNDM×85 09:30 $23.33 → close $23.13 -17.00 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $56.25 | ▲ 09:30 equity $10,321.25 vs yday $10,286.85 (+34.40) | 09:30 open · cash $56.25 (unchanged overnight, no fees) · equity $10,321.25 vs prior close $10,286.85 (+34.40) · 5 name(s) re-marked at the open (per-name table). BTSG×33 yday $60.23 → 09:30 $59.65 -19.14; IREN×43 yday $44.76 → 09:30 $44.09 -28.81; TPG×39 yday $54.62 → 09:30 $55.29 +26.13; INO×2469 yday $0.90 → 09:30 $0.93 +74.07; TNDM×85 yday $23.13 → 09:30 $22.92 -17.85 | — |
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 33 | $59.65 | $2.11 | $-9.15 | $2,022.58 | ▼ -9.15 after sell → book $10,319.13; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 43 | $44.09 | $2.14 | $-85.53 | $3,916.31 | ▼ -85.53 after sell → book $10,316.99; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 39 | $55.29 | $2.13 | $+177.76 | $6,070.49 | ▲ +177.76 after sell → book $10,314.86; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 2469 | $0.93 | $30.80 | $+238.08 | $8,335.86 | ▲ +238.08 after sell → book $10,284.06; vs 09:30 mark -30.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 85 | $22.92 | $2.27 | $-39.37 | $10,281.78 | ▼ -39.37 after sell → book $10,281.78; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 8 | $146.90 | $2.01 | — | $9,104.57 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ret5=+3.6; leftover $1285.22 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `DAVE` | 3 | $330.91 | $2.00 | — | $8,109.84 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ⚪; ret5=-8.6; leftover $1285.22 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 22 | $57.61 | $2.06 | — | $6,840.37 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ret5=+5.7; leftover $1285.22 | join🟢 sector🟢 gen🟢 news🟡 judge🔴 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 1371 | $0.94 | $16.96 | — | $5,538.78 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ⚪; ret5=+0.5; leftover $1285.22 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 856 | $1.50 | $11.04 | — | $4,243.74 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1285.22 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BETR` | 86 | $14.80 | $2.25 | — | $2,968.69 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ⚪; ret5=-9.9; leftover $1285.22 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 298 | $4.31 | $3.84 | — | $1,680.46 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1285.22 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 307 | $4.18 | $3.96 | — | $393.24 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $1285.22 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $393.24 | ▼ close $10,119.14 vs 09:30 $10,321.25 (session -118.52) | 16:00 close · cash $393.24 · equity $10,119.14 vs 09:30 $10,321.25 (-202.11; session marks -118.52) · 8 name(s) marked open→close (per-name table). VST×8 09:30 $146.90 → close $148.13 +9.84; DAVE×3 09:30 $330.91 → close $334.57 +10.98; SLG×22 09:30 $57.61 → close $56.09 -33.44; LDI×1371 09:30 $0.94 → close $0.90 -54.84; BTBT×856 09:30 $1.50 → close $1.57 +59.92; BETR×86 09:30 $14.80 → close $13.73 -92.02; ANGX×298 09:30 $4.31 → close $4.37 +17.88; HYLN×307 09:30 $4.18 → close $4.06 -36.84 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $393.24 | ▲ 09:30 equity $10,166.90 vs yday $10,119.14 (+47.76) | 09:30 open · cash $393.24 (unchanged overnight, no fees) · equity $10,166.90 vs prior close $10,119.14 (+47.76) · 8 name(s) re-marked at the open (per-name table). VST×8 yday $148.13 → 09:30 $149.37 +9.92; DAVE×3 yday $334.57 → 09:30 $336.94 +7.11; SLG×22 yday $56.09 → 09:30 $55.37 -15.84; LDI×1371 yday $0.90 → 09:30 $0.91 +13.71; BTBT×856 yday $1.57 → 09:30 $1.52 -42.80; BETR×86 yday $13.73 → 09:30 $13.67 -5.16; ANGX×298 yday $4.37 → 09:30 $4.60 +68.54; HYLN×307 yday $4.06 → 09:30 $4.10 +12.28 | — |
| 2026-08-17 09:30 ET | **SELL** | `VST` | 8 | $149.37 | $2.03 | $+15.71 | $1,586.17 | ▲ +15.71 after sell → book $10,164.87; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `DAVE` | 3 | $336.94 | $2.02 | $+14.07 | $2,594.97 | ▲ +14.07 after sell → book $10,162.85; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SLG` | 22 | $55.37 | $2.08 | $-53.41 | $3,811.04 | ▼ -53.41 after sell → book $10,160.77; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LDI` | 1371 | $0.91 | $16.79 | $-74.87 | $5,037.75 | ▼ -74.87 after sell → book $10,143.99; vs 09:30 mark -16.78 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 856 | $1.52 | $11.19 | $-5.12 | $6,327.67 | ▼ -5.12 after sell → book $10,132.79; vs 09:30 mark -11.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BETR` | 86 | $13.67 | $2.27 | $-101.70 | $7,501.02 | ▼ -101.70 after sell → book $10,130.52; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 298 | $4.60 | $3.90 | $+78.67 | $8,867.91 | ▲ +78.67 after sell → book $10,126.61; vs 09:30 mark -3.91 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HYLN` | 307 | $4.10 | $4.02 | $-32.54 | $10,122.59 | ▼ -32.54 after sell → book $10,122.59; vs 09:30 mark -4.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 27 | $46.18 | $2.07 | — | $8,873.66 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ret5=+6.7; leftover $1265.32 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 8 | $142.77 | $2.01 | — | $7,729.49 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ret5=+5.8; leftover $1265.32 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 6 | $202.70 | $2.01 | — | $6,511.28 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ret5=+8.3; leftover $1265.32 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `NB` | 249 | $5.07 | $3.21 | — | $5,245.64 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ret5=-4.7; leftover $1265.32 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CDNL` | 31 | $39.85 | $2.08 | — | $4,008.21 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; ⚪; ret5=-38.4; leftover $1265.32 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 138 | $9.12 | $2.40 | — | $2,747.24 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $1265.32 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `VERA` | 40 | $31.30 | $2.11 | — | $1,493.13 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; ret5=-3.8; leftover $1265.32 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CELC` | 13 | $92.99 | $2.03 | — | $282.23 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; ret5=-0.8; leftover $1265.32 | join🟡 sector🔴 gen🟢 news🟢 judge🟢 vol🟡 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $282.23 | ▲ close $10,112.86 vs 09:30 $10,166.90 (session +8.20) | 16:00 close · cash $282.23 · equity $10,112.86 vs 09:30 $10,166.90 (-54.04; session marks +8.20) · 8 name(s) marked open→close (per-name table). DVN×27 09:30 $46.18 → close $47.57 +37.53; EOG×8 09:30 $142.77 → close $146.15 +27.04; FANG×6 09:30 $202.70 → close $206.29 +21.54; NB×249 09:30 $5.07 → close $4.81 -64.74; CDNL×31 09:30 $39.85 → close $39.23 -19.22; ABX×138 09:30 $9.12 → close $9.12 +0.00; VERA×40 09:30 $31.30 → close $31.63 +13.20; CELC×13 09:30 $92.99 → close $92.44 -7.15 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $282.23 | ▲ 09:30 equity $10,164.62 vs yday $10,112.86 (+51.76) | 09:30 open · cash $282.23 (unchanged overnight, no fees) · equity $10,164.62 vs prior close $10,112.86 (+51.76) · 8 name(s) re-marked at the open (per-name table). DVN×27 yday $47.57 → 09:30 $48.00 +11.61; EOG×8 yday $146.15 → 09:30 $148.04 +15.12; FANG×6 yday $206.29 → 09:30 $208.93 +15.84; NB×249 yday $4.81 → 09:30 $4.66 -37.35; CDNL×31 yday $39.23 → 09:30 $41.57 +72.54; ABX×138 yday $9.12 → 09:30 $9.03 -12.42; VERA×40 yday $31.63 → 09:30 $31.31 -12.80; CELC×13 yday $92.44 → 09:30 $92.38 -0.78 | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 27 | $48.00 | $2.09 | $+44.98 | $1,576.14 | ▲ +44.98 after sell → book $10,162.53; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 8 | $148.04 | $2.03 | $+38.11 | $2,758.43 | ▲ +38.11 after sell → book $10,160.50; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 6 | $208.93 | $2.03 | $+33.34 | $4,009.98 | ▲ +33.34 after sell → book $10,158.47; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `NB` | 249 | $4.66 | $3.26 | $-108.57 | $5,167.06 | ▼ -108.57 after sell → book $10,155.21; vs 09:30 mark -3.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CDNL` | 31 | $41.57 | $2.10 | $+49.13 | $6,453.62 | ▲ +49.13 after sell → book $10,153.10; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 138 | $9.03 | $2.44 | $-17.26 | $7,697.33 | ▼ -17.26 after sell → book $10,150.67; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `VERA` | 40 | $31.31 | $2.13 | $-3.84 | $8,947.60 | ▼ -3.84 after sell → book $10,148.54; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CELC` | 13 | $92.38 | $2.05 | $-12.01 | $10,146.49 | ▼ -12.01 after sell → book $10,146.49; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,146.49 | ▲ close $10,146.49 vs 09:30 $10,164.62 (session +0.00) | 16:00 close · cash $10,146.49 · no lots left · equity $10,146.49. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,146.49 | ▲ 09:30 equity $10,146.49 vs yday $10,146.49 (-0.00) | 09:30 open · cash $10,146.49 · no holdings · equity $10,146.49 vs prior close $10,146.49 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,146.49 | ▲ close $10,146.49 vs 09:30 $10,146.49 (session +0.00) | 16:00 close · cash $10,146.49 · no lots left · equity $10,146.49. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,146.49 | ▲ 09:30 equity $10,146.49 vs yday $10,146.49 (-0.00) | 09:30 open · cash $10,146.49 · no holdings · equity $10,146.49 vs prior close $10,146.49 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 61 | $20.55 | $2.17 | — | $8,890.76 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1268.31 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 61 | $20.65 | $2.17 | — | $7,628.94 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1268.31 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 219 | $5.77 | $2.83 | — | $6,362.49 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1268.31 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 64 | $19.63 | $2.18 | — | $5,103.98 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1268.31 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 42 | $29.63 | $2.12 | — | $3,857.41 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1268.31 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 724 | $1.75 | $9.34 | — | $2,581.07 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1268.31 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $1,422.73 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1268.31 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABUS` | 257 | $4.92 | $3.32 | — | $154.98 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1268.31 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $154.98 | ▲ close $10,282.06 vs 09:30 $10,146.49 (session +161.71) | 16:00 close · cash $154.98 · equity $10,282.06 vs 09:30 $10,146.49 (+135.57; session marks +161.71) · 8 name(s) marked open→close (per-name table). AG×61 09:30 $20.55 → close $21.19 +39.04; CDE×61 09:30 $20.65 → close $21.11 +28.06; HDSN×219 09:30 $5.77 → close $5.57 -43.80; IAG×64 09:30 $19.63 → close $20.50 +55.68; KGC×42 09:30 $29.63 → close $31.43 +75.60; NFGC×724 09:30 $1.75 → close $1.75 +0.00; WPM×8 09:30 $144.54 → close $150.25 +45.68; ABUS×257 09:30 $4.92 → close $4.77 -38.55 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $154.98 | ▲ 09:30 equity $10,635.34 vs yday $10,282.06 (+353.28) | 09:30 open · cash $154.98 (unchanged overnight, no fees) · equity $10,635.34 vs prior close $10,282.06 (+353.28) · 8 name(s) re-marked at the open (per-name table). AG×61 yday $21.19 → 09:30 $21.90 +43.31; CDE×61 yday $21.11 → 09:30 $21.75 +39.04; HDSN×219 yday $5.57 → 09:30 $5.67 +21.90; IAG×64 yday $20.50 → 09:30 $21.17 +42.88; KGC×42 yday $31.43 → 09:30 $32.17 +31.08; NFGC×724 yday $1.75 → 09:30 $1.79 +28.96; WPM×8 yday $150.25 → 09:30 $154.70 +35.60; ABUS×257 yday $4.77 → 09:30 $5.20 +110.51 | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 61 | $21.90 | $2.19 | $+77.98 | $1,488.68 | ▲ +77.98 after sell → book $10,633.14; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 61 | $21.75 | $2.19 | $+62.73 | $2,813.24 | ▲ +62.73 after sell → book $10,630.95; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 219 | $5.67 | $2.87 | $-27.60 | $4,052.10 | ▼ -27.60 after sell → book $10,628.08; vs 09:30 mark -2.87 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 64 | $21.17 | $2.20 | $+94.17 | $5,404.78 | ▲ +94.17 after sell → book $10,625.88; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 42 | $32.17 | $2.14 | $+102.43 | $6,753.78 | ▲ +102.43 after sell → book $10,623.74; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 724 | $1.79 | $9.47 | $+10.15 | $8,040.27 | ▲ +10.15 after sell → book $10,614.27; vs 09:30 mark -9.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 8 | $154.70 | $2.03 | $+77.23 | $9,275.84 | ▲ +77.23 after sell → book $10,612.24; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `ABUS` | 257 | $5.20 | $3.37 | $+65.28 | $10,608.87 | ▲ +65.28 after sell → book $10,608.87; vs 09:30 mark -3.37 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 11 | $119.43 | $2.02 | — | $9,293.11 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+20.4; leftover $1326.11 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 77 | $17.20 | $2.22 | — | $7,966.49 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $1326.11 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 6 | $216.30 | $2.01 | — | $6,666.68 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1326.11 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 119 | $11.13 | $2.35 | — | $5,339.87 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1326.11 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 1004 | $1.32 | $12.95 | — | $4,001.64 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1326.11 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 798 | $1.66 | $10.29 | — | $2,666.66 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $1326.11 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 2 | $623.26 | $2.00 | — | $1,418.15 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ret5=+1.4; leftover $1326.11 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `QDEL` | 88 | $14.96 | $2.25 | — | $99.41 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; ret5=-1.6; leftover $1326.11 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $99.41 | ▲ close $10,850.47 vs 09:30 $10,635.34 (session +277.70) | 16:00 close · cash $99.41 · equity $10,850.47 vs 09:30 $10,635.34 (+215.13; session marks +277.70) · 8 name(s) marked open→close (per-name table). AU×11 09:30 $119.43 → close $121.22 +19.69; AUPH×77 09:30 $17.20 → close $16.65 -42.35; AEM×6 09:30 $216.30 → close $216.06 -1.44; ARCT×119 09:30 $11.13 → close $13.45 +276.08; CYPH×1004 09:30 $1.32 → close $1.42 +100.40; BTBT×798 09:30 $1.66 → close $1.53 -103.74; DE×2 09:30 $623.26 → close $647.47 +48.42; QDEL×88 09:30 $14.96 → close $14.74 -19.36 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $99.41 | ▲ 09:30 equity $11,259.17 vs yday $10,850.47 (+408.70) | 09:30 open · cash $99.41 (unchanged overnight, no fees) · equity $11,259.17 vs prior close $10,850.47 (+408.70) · 8 name(s) re-marked at the open (per-name table). AU×11 yday $121.22 → 09:30 $120.50 -7.92; AUPH×77 yday $16.65 → 09:30 $16.60 -3.85; AEM×6 yday $216.06 → 09:30 $217.03 +5.82; ARCT×119 yday $13.45 → 09:30 $13.26 -22.61; CYPH×1004 yday $1.42 → 09:30 $1.83 +411.64; BTBT×798 yday $1.53 → 09:30 $1.55 +15.96; DE×2 yday $647.47 → 09:30 $653.62 +12.30; QDEL×88 yday $14.74 → 09:30 $14.71 -2.64 | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 11 | $120.50 | $2.04 | $+7.70 | $1,422.87 | ▲ +7.70 after sell → book $11,257.13; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 77 | $16.60 | $2.24 | $-50.66 | $2,698.82 | ▼ -50.66 after sell → book $11,254.88; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 6 | $217.03 | $2.03 | $+0.34 | $3,998.98 | ▲ +0.34 after sell → book $11,252.86; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 119 | $13.26 | $2.38 | $+248.74 | $5,574.54 | ▲ +248.74 after sell → book $11,250.48; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 1004 | $1.83 | $13.13 | $+485.96 | $7,398.72 | ▲ +485.96 after sell → book $11,237.34; vs 09:30 mark -13.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BTBT` | 798 | $1.55 | $10.44 | $-108.51 | $8,625.19 | ▼ -108.51 after sell → book $11,226.91; vs 09:30 mark -10.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `DE` | 2 | $653.62 | $2.02 | $+56.71 | $9,930.41 | ▲ +56.71 after sell → book $11,224.89; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `QDEL` | 88 | $14.71 | $2.28 | $-26.53 | $11,222.61 | ▼ -26.53 after sell → book $11,222.61; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,222.61 | ▲ close $11,222.61 vs 09:30 $11,259.17 (session +0.00) | 16:00 close · cash $11,222.61 · no lots left · equity $11,222.61. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,222.61 | ▲ 09:30 equity $11,222.61 vs yday $11,222.61 (+0.00) | 09:30 open · cash $11,222.61 · no holdings · equity $11,222.61 vs prior close $11,222.61 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 58 | $24.00 | $2.16 | — | $9,828.45 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ⚪; ret5=+13.0; leftover $1402.83 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 22 | $61.47 | $2.06 | — | $8,474.05 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ret5=+9.2; leftover $1402.83 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 268 | $5.23 | $3.46 | — | $7,068.95 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ret5=+10.7; leftover $1402.83 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 3 | $429.24 | $2.00 | — | $5,779.23 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ret5=+6.1; leftover $1402.83 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 701 | $2.00 | $9.04 | — | $4,368.19 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.8; leftover $1402.83 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 268 | $5.22 | $3.46 | — | $2,965.77 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+9.2; leftover $1402.83 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALIT` | 94 | $14.86 | $2.27 | — | $1,566.66 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ret5=+3.0; leftover $1402.83 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟡 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 219 | $6.38 | $2.83 | — | $166.62 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=+5.0; leftover $1402.83 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $166.62 | ▲ close $11,243.98 vs 09:30 $11,222.61 (session +48.64) | 16:00 close · cash $166.62 · equity $11,243.98 vs 09:30 $11,222.61 (+21.37; session marks +48.64) · 8 name(s) marked open→close (per-name table). MOS×58 09:30 $24.00 → close $23.75 -14.50; INSP×22 09:30 $61.47 → close $61.47 +0.00; RZLT×268 09:30 $5.23 → close $5.29 +16.08; HCA×3 09:30 $429.24 → close $428.50 -2.22; NPWR×701 09:30 $2.00 → close $2.02 +14.02; ALVO×268 09:30 $5.22 → close $5.25 +8.04; ALIT×94 09:30 $14.86 → close $14.87 +0.94; ZURA×219 09:30 $6.38 → close $6.50 +26.28 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $166.62 | ▲ 09:30 equity $11,243.98 vs yday $11,243.98 (-0.00) | 09:30 open · cash $166.62 (unchanged overnight, no fees) · equity $11,243.98 vs prior close $11,243.98 (-0.00) · 8 name(s) re-marked at the open (per-name table). MOS×58 yday $23.75 → 09:30 $23.75 +0.00; INSP×22 yday $61.47 → 09:30 $61.47 +0.00; RZLT×268 yday $5.29 → 09:30 $5.29 +0.00; HCA×3 yday $428.50 → 09:30 $428.50 +0.00; NPWR×701 yday $2.02 → 09:30 $2.02 +0.00; ALVO×268 yday $5.25 → 09:30 $5.25 +0.00; ALIT×94 yday $14.87 → 09:30 $14.87 +0.00; ZURA×219 yday $6.50 → 09:30 $6.50 +0.00 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $166.62 | ▲ close $11,243.98 vs 09:30 $11,243.98 (session +0.00) | 16:00 close · cash $166.62 · equity $11,243.98 vs 09:30 $11,243.98 (-0.00; session marks +0.00) · 8 name(s) marked open→close (per-name table). MOS×58 09:30 $23.75 → close $23.75 +0.00; INSP×22 09:30 $61.47 → close $61.47 +0.00; RZLT×268 09:30 $5.29 → close $5.29 +0.00; HCA×3 09:30 $428.50 → close $428.50 +0.00; NPWR×701 09:30 $2.02 → close $2.02 +0.00; ALVO×268 09:30 $5.25 → close $5.25 +0.00; ALIT×94 09:30 $14.87 → close $14.87 +0.00; ZURA×219 09:30 $6.50 → close $6.50 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $166.62 | ▼ 09:30 equity $10,980.00 vs yday $11,243.98 (-263.98) | 09:30 open · cash $166.62 (unchanged overnight, no fees) · equity $10,980.00 vs prior close $11,243.98 (-263.98) · 8 name(s) re-marked at the open (per-name table). MOS×58 yday $23.75 → 09:30 $24.84 +63.22; INSP×22 yday $61.47 → 09:30 $60.07 -30.80; RZLT×268 yday $5.29 → 09:30 $5.01 -75.04; HCA×3 yday $428.50 → 09:30 $427.50 -3.00; NPWR×701 yday $2.02 → 09:30 $1.93 -63.09; ALVO×268 yday $5.25 → 09:30 $4.98 -72.36; ALIT×94 yday $14.87 → 09:30 $14.85 -1.88; ZURA×219 yday $6.50 → 09:30 $6.13 -81.03 | — |
| 2026-08-27 09:30 ET | **SELL** | `INSP` | 22 | $60.07 | $2.08 | $-34.93 | $1,486.08 | ▼ -34.93 after sell → book $10,977.92; vs 09:30 mark -2.08 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `RZLT` | 268 | $5.01 | $3.51 | $-65.93 | $2,825.25 | ▼ -65.93 after sell → book $10,974.41; vs 09:30 mark -3.51 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `HCA` | 3 | $427.50 | $2.02 | $-9.24 | $4,105.73 | ▼ -9.24 after sell → book $10,972.39; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `NPWR` | 701 | $1.93 | $9.17 | $-67.28 | $5,449.49 | ▼ -67.28 after sell → book $10,963.22; vs 09:30 mark -9.17 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ALVO` | 268 | $4.98 | $3.51 | $-71.29 | $6,780.62 | ▼ -71.29 after sell → book $10,959.71; vs 09:30 mark -3.51 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ALIT` | 94 | $14.85 | $2.30 | $-5.51 | $8,174.22 | ▼ -5.51 after sell → book $10,957.41; vs 09:30 mark -2.30 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ZURA` | 219 | $6.13 | $2.87 | $-60.45 | $9,513.82 | ▼ -60.45 after sell → book $10,954.54; vs 09:30 mark -2.87 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 33 | $40.72 | $2.09 | — | $8,167.97 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ret5=+1.8; leftover $1359.12 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 96 | $14.09 | $2.28 | — | $6,813.05 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ret5=+1.1; leftover $1359.12 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 524 | $2.59 | $6.76 | — | $5,449.13 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ret5=+4.2; leftover $1359.12 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ANET` | 7 | $190.90 | $2.01 | — | $4,110.82 | — | union ∩ last_green, no 🚨; gate last_green=True; list mover_buy; 🔵; ret5=-5.1; leftover $1359.12 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `DLO` | 87 | $15.60 | $2.25 | — | $2,751.37 | — | union ∩ last_green, no 🚨; gate last_green=True; list mover_buy; 🔵; ret5=+7.1; leftover $1359.12 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GEN` | 47 | $28.89 | $2.13 | — | $1,391.41 | — | union ∩ last_green, no 🚨; gate last_green=True; list mover_buy; 🔵; ret5=+1.6; leftover $1359.12 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,391.41 | ▲ close $11,068.63 vs 09:30 $10,980.00 (session +131.61) | 16:00 close · cash $1,391.41 · equity $11,068.63 vs 09:30 $10,980.00 (+88.63; session marks +131.61) · 7 name(s) marked open→close (per-name table). MOS×58 09:30 $24.84 → close $24.16 -39.44; RRC×33 09:30 $40.72 → close $41.55 +27.39; CRK×96 09:30 $14.09 → close $14.50 +39.36; SLI×524 09:30 $2.59 → close $2.61 +10.48; ANET×7 09:30 $190.90 → close $202.25 +79.45; DLO×87 09:30 $15.60 → close $15.36 -20.88; GEN×47 09:30 $28.89 → close $29.64 +35.25 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,391.41 | ▲ 09:30 equity $11,074.67 vs yday $11,068.63 (+6.04) | 09:30 open · cash $1,391.41 (unchanged overnight, no fees) · equity $11,074.67 vs prior close $11,068.63 (+6.04) · 7 name(s) re-marked at the open (per-name table). MOS×58 yday $24.16 → 09:30 $24.00 -9.28; RRC×33 yday $41.55 → 09:30 $41.44 -3.63; CRK×96 yday $14.50 → 09:30 $14.42 -7.68; SLI×524 yday $2.61 → 09:30 $2.60 -5.24; ANET×7 yday $202.25 → 09:30 $205.90 +25.55; DLO×87 yday $15.36 → 09:30 $15.33 -2.61; GEN×47 yday $29.64 → 09:30 $29.83 +8.93 | — |
| 2026-08-28 09:30 ET | **SELL** | `ANET` | 7 | $205.90 | $2.03 | $+100.96 | $2,830.67 | ▲ +100.96 after sell → book $11,072.63; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `DLO` | 87 | $15.33 | $2.28 | $-28.02 | $4,162.11 | ▼ -28.02 after sell → book $11,070.36; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GEN` | 47 | $29.83 | $2.15 | $+39.90 | $5,561.97 | ▲ +39.90 after sell → book $11,068.21; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 9 | $144.70 | $2.02 | — | $4,257.65 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1390.49 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BHVN` | 82 | $16.95 | $2.24 | — | $2,865.51 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $1390.49 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 75 | $18.50 | $2.21 | — | $1,475.80 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $1390.49 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `LVWR` | 1007 | $1.38 | $12.99 | — | $73.15 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; ret5=+0.0; leftover $1390.49 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $73.15 | ▼ close $10,965.34 vs 09:30 $11,074.67 (session -83.41) | 16:00 close · cash $73.15 · equity $10,965.34 vs 09:30 $11,074.67 (-109.33; session marks -83.41) · 8 name(s) marked open→close (per-name table). MOS×58 09:30 $24.00 → close $23.76 -13.92; RRC×33 09:30 $41.44 → close $41.64 +6.60; CRK×96 09:30 $14.42 → close $14.62 +19.20; SLI×524 09:30 $2.60 → close $2.64 +20.96; ANF×9 09:30 $144.70 → close $145.75 +9.45; BHVN×82 09:30 $16.95 → close $16.12 -68.06; BZ×75 09:30 $18.50 → close $18.00 -37.50; LVWR×1007 09:30 $1.38 → close $1.36 -20.14 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $73.15 | ▼ 09:30 equity $10,845.73 vs yday $10,965.34 (-119.61) | 09:30 open · cash $73.15 (unchanged overnight, no fees) · equity $10,845.73 vs prior close $10,965.34 (-119.61) · 8 name(s) re-marked at the open (per-name table). MOS×58 yday $23.76 → 09:30 $23.75 -0.58; RRC×33 yday $41.64 → 09:30 $41.11 -17.49; CRK×96 yday $14.62 → 09:30 $14.56 -5.76; SLI×524 yday $2.64 → 09:30 $2.51 -68.12; ANF×9 yday $145.75 → 09:30 $148.67 +26.28; BHVN×82 yday $16.12 → 09:30 $15.44 -55.76; BZ×75 yday $18.00 → 09:30 $17.89 -8.25; LVWR×1007 yday $1.36 → 09:30 $1.37 +10.07 | — |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 58 | $23.75 | $2.19 | $-18.85 | $1,448.46 | ▼ -18.85 after sell → book $10,843.54; vs 09:30 mark -2.19 | dropped from list after 4 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `RRC` | 33 | $41.11 | $2.11 | $+8.67 | $2,802.98 | ▲ +8.67 after sell → book $10,841.43; vs 09:30 mark -2.11 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CRK` | 96 | $14.56 | $2.31 | $+40.54 | $4,198.44 | ▲ +40.54 after sell → book $10,839.13; vs 09:30 mark -2.30 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SLI` | 524 | $2.51 | $6.86 | $-55.54 | $5,506.82 | ▼ -55.54 after sell → book $10,832.27; vs 09:30 mark -6.86 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 9 | $148.67 | $2.04 | $+31.68 | $6,842.81 | ▲ +31.68 after sell → book $10,830.23; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BHVN` | 82 | $15.44 | $2.26 | $-128.32 | $8,106.63 | ▼ -128.32 after sell → book $10,827.97; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BZ` | 75 | $17.89 | $2.24 | $-50.20 | $9,446.14 | ▼ -50.20 after sell → book $10,825.73; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `LVWR` | 1007 | $1.37 | $13.17 | $-36.23 | $10,812.57 | ▼ -36.23 after sell → book $10,812.57; vs 09:30 mark -13.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,812.57 | ▲ close $10,812.57 vs 09:30 $10,845.73 (session +0.00) | 16:00 close · cash $10,812.57 · no lots left · equity $10,812.57. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,812.57 | ▲ 09:30 equity $10,812.57 vs yday $10,812.57 (-0.00) | 09:30 open · cash $10,812.57 · no holdings · equity $10,812.57 vs prior close $10,812.57 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,812.57 | ▲ close $10,812.57 vs 09:30 $10,812.57 (session +0.00) | 16:00 close · cash $10,812.57 · no lots left · equity $10,812.57. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,812.57 | ▲ 09:30 equity $10,812.57 vs yday $10,812.57 (-0.00) | 09:30 open · cash $10,812.57 · no holdings · equity $10,812.57 vs prior close $10,812.57 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,812.57 | ▲ close $10,812.57 vs 09:30 $10,812.57 (session +0.00) | 16:00 close · cash $10,812.57 · no lots left · equity $10,812.57. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,812.57 | ▲ 09:30 equity $10,812.57 vs yday $10,812.57 (-0.00) | 09:30 open · cash $10,812.57 · no holdings · equity $10,812.57 vs prior close $10,812.57 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 27 | $49.76 | $2.07 | — | $9,466.97 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1351.57 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 32 | $41.31 | $2.09 | — | $8,142.97 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $1351.57 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 175 | $7.70 | $2.52 | — | $6,792.95 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $1351.57 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $125.94 | $2.02 | — | $5,531.53 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1351.57 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 1107 | $1.22 | $14.28 | — | $4,166.71 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1351.57 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 86 | $15.70 | $2.25 | — | $2,814.27 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1351.57 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 59 | $22.78 | $2.17 | — | $1,468.08 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1351.57 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SLN` | 91 | $14.70 | $2.26 | — | $128.12 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=-3.3; leftover $1351.57 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $128.12 | ▲ close $11,587.47 vs 09:30 $10,812.57 (session +804.55) | 16:00 close · cash $128.12 · equity $11,587.47 vs 09:30 $10,812.57 (+774.90; session marks +804.55) · 8 name(s) marked open→close (per-name table). ATRC×27 09:30 $49.76 → close $52.59 +76.41; HRMY×32 09:30 $41.31 → close $42.86 +49.60; VSTM×175 09:30 $7.70 → close $8.02 +56.00; RVTY×10 09:30 $125.94 → close $130.94 +50.00; GPRO×1107 09:30 $1.22 → close $1.69 +520.29; CRK×86 09:30 $15.70 → close $15.54 -13.76; MMED×59 09:30 $22.78 → close $23.76 +57.82; SLN×91 09:30 $14.70 → close $14.79 +8.19 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $128.12 | ▲ 09:30 equity $11,718.82 vs yday $11,587.47 (+131.35) | 09:30 open · cash $128.12 (unchanged overnight, no fees) · equity $11,718.82 vs prior close $11,587.47 (+131.35) · 8 name(s) re-marked at the open (per-name table). ATRC×27 yday $52.59 → 09:30 $52.88 +7.83; HRMY×32 yday $42.86 → 09:30 $42.93 +2.24; VSTM×175 yday $8.02 → 09:30 $8.03 +1.75; RVTY×10 yday $130.94 → 09:30 $132.45 +15.10; GPRO×1107 yday $1.69 → 09:30 $1.78 +99.63; CRK×86 yday $15.54 → 09:30 $15.45 -7.74; MMED×59 yday $23.76 → 09:30 $23.88 +7.08; SLN×91 yday $14.79 → 09:30 $14.85 +5.46 | — |
| 2026-09-04 09:30 ET | **SELL** | `HRMY` | 32 | $42.93 | $2.11 | $+47.65 | $1,499.77 | ▲ +47.65 after sell → book $11,716.71; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 175 | $8.03 | $2.56 | $+52.68 | $2,902.46 | ▲ +52.68 after sell → book $11,714.15; vs 09:30 mark -2.56 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 10 | $132.45 | $2.04 | $+61.04 | $4,224.92 | ▲ +61.04 after sell → book $11,712.11; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 86 | $15.45 | $2.27 | $-26.02 | $5,551.35 | ▼ -26.02 after sell → book $11,709.84; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 59 | $23.88 | $2.19 | $+60.54 | $6,958.08 | ▲ +60.54 after sell → book $11,707.65; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `SLN` | 91 | $14.85 | $2.29 | $+9.10 | $8,307.14 | ▲ +9.10 after sell → book $11,705.36; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `OSCR` | 45 | $30.65 | $2.12 | — | $6,925.77 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ret5=-2.2; leftover $1384.52 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `NVAX` | 132 | $10.41 | $2.39 | — | $5,549.26 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $1384.52 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BVS` | 95 | $14.50 | $2.27 | — | $4,169.49 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ⚪; ret5=+0.8; leftover $1384.52 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 710 | $1.95 | $9.16 | — | $2,775.83 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $1384.52 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `EOSE` | 387 | $3.57 | $4.99 | — | $1,389.25 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.2; leftover $1384.52 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 2 | $486.31 | $2.00 | — | $414.63 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-9.9; leftover $1384.52 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $414.63 | ▼ close $11,314.34 vs 09:30 $11,718.82 (session -368.09) | 16:00 close · cash $414.63 · equity $11,314.34 vs 09:30 $11,718.82 (-404.48; session marks -368.09) · 8 name(s) marked open→close (per-name table). ATRC×27 09:30 $52.88 → close $52.46 -11.34; GPRO×1107 09:30 $1.78 → close $1.39 -431.73; OSCR×45 09:30 $30.65 → close $32.24 +71.55; NVAX×132 09:30 $10.41 → close $10.34 -9.24; BVS×95 09:30 $14.50 → close $14.36 -13.30; BAK×710 09:30 $1.95 → close $1.94 -7.10; EOSE×387 09:30 $3.57 → close $3.50 -27.09; DELL×2 09:30 $486.31 → close $516.39 +60.16 | — |

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
| 2026-08-19 | `MLYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `GUTS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `RANI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SECZ` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ULTA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNDY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-26 | `NPWR` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ALVO` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ALIT` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ZURA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `KURA` | no_price | no 09:30 open |
| 2026-08-26 | `ABX` | no_price | no 09:30 open |
| 2026-08-26 | `ITG` | no_price | no 09:30 open |
| 2026-08-26 | `SENS` | no_price | no 09:30 open |
| 2026-08-27 | `ASML` | cash | leftover split 1359.12 < 1 share @ 1746.33 |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RPD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ACDC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PANW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NMRA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NTNX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NAGE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `ELMT` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TRLV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `ALEC` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `PCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HRMY` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VSTM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TII` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `AVXL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `KMX` | hard_red | hard-red S=-3.83 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ATRC` | 27 | 2026-09-03 @ $49.76 | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1351.57 |
| `GPRO` | 1107 | 2026-09-03 @ $1.22 | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1351.57 |
| `OSCR` | 45 | 2026-09-04 @ $30.65 | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ret5=-2.2; leftover $1384.52 |
| `NVAX` | 132 | 2026-09-04 @ $10.41 | union ∩ last_green, no 🚨; gate last_green=True; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $1384.52 |
| `BVS` | 95 | 2026-09-04 @ $14.50 | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ⚪; ret5=+0.8; leftover $1384.52 |
| `BAK` | 710 | 2026-09-04 @ $1.95 | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $1384.52 |
| `EOSE` | 387 | 2026-09-04 @ $3.57 | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.2; leftover $1384.52 |
| `DELL` | 2 | 2026-09-04 @ $486.31 | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-9.9; leftover $1384.52 |
