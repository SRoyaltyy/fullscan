# Factor mine action — `union_news_present_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ news_present, no 🚨

Cash book **+13.17%** ($11,317) · signal-only (no cash/fees) was +15.48%. Starts YES **16/17**. Fills 124 · skips 52 · realized $+947.04.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `news_present=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $65.28.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `TLN` | 3 | — | $359.83 | +0.00 | $362.74 | +8.73 | +8.73 | +0.00 | +8.73 |
| 2026-08-14 | `VST` | 8 | — | $146.90 | +0.00 | $148.13 | +9.84 | +9.84 | +0.00 | +9.84 |
| 2026-08-14 | `NRG` | 10 | — | $120.00 | +0.00 | $126.24 | +62.40 | +62.40 | +0.00 | +62.40 |
| 2026-08-14 | `DAVE` | 3 | — | $330.91 | +0.00 | $334.57 | +10.98 | +10.98 | +0.00 | +10.98 |
| 2026-08-14 | `SLG` | 21 | — | $57.61 | +0.00 | $56.09 | -31.92 | -31.92 | +0.00 | -31.92 |
| 2026-08-14 | `MARA` | 138 | — | $9.01 | +0.00 | $9.20 | +26.22 | +26.22 | +0.00 | +26.22 |
| 2026-08-14 | `LDI` | 1334 | — | $0.94 | +0.00 | $0.90 | -53.36 | -53.36 | +0.00 | -53.36 |
| 2026-08-14 | `BTBT` | 833 | — | $1.50 | +0.00 | $1.57 | +58.31 | +58.31 | +0.00 | +58.31 |
| 2026-08-17 | `TLN` | 3 | $362.74 | $367.88 | +15.42 | — | +0.00 | +15.42 | +24.15 | — |
| 2026-08-17 | `VST` | 8 | $148.13 | $149.37 | +9.92 | — | +0.00 | +9.92 | +19.76 | — |
| 2026-08-17 | `NRG` | 10 | $126.24 | $127.40 | +11.60 | — | +0.00 | +11.60 | +74.00 | — |
| 2026-08-17 | `DAVE` | 3 | $334.57 | $336.94 | +7.11 | — | +0.00 | +7.11 | +18.09 | — |
| 2026-08-17 | `SLG` | 21 | $56.09 | $55.37 | -15.12 | — | +0.00 | -15.12 | -47.04 | — |
| 2026-08-17 | `MARA` | 138 | $9.20 | $9.22 | +2.76 | — | +0.00 | +2.76 | +28.98 | — |
| 2026-08-17 | `LDI` | 1334 | $0.90 | $0.91 | +13.34 | — | +0.00 | +13.34 | -40.02 | — |
| 2026-08-17 | `BTBT` | 833 | $1.57 | $1.52 | -41.65 | — | +0.00 | -41.65 | +16.66 | — |
| 2026-08-17 | `DVN` | 27 | — | $46.18 | +0.00 | $47.57 | +37.53 | +37.53 | +0.00 | +37.53 |
| 2026-08-17 | `EOG` | 8 | — | $142.77 | +0.00 | $146.15 | +27.04 | +27.04 | +0.00 | +27.04 |
| 2026-08-17 | `FANG` | 6 | — | $202.70 | +0.00 | $206.29 | +21.54 | +21.54 | +0.00 | +21.54 |
| 2026-08-17 | `TMC` | 309 | — | $4.05 | +0.00 | $3.77 | -86.52 | -86.52 | +0.00 | -86.52 |
| 2026-08-17 | `TGB` | 147 | — | $8.46 | +0.00 | $8.77 | +45.57 | +45.57 | +0.00 | +45.57 |
| 2026-08-17 | `ELF` | 13 | — | $90.54 | +0.00 | $93.66 | +40.56 | +40.56 | +0.00 | +40.56 |
| 2026-08-17 | `DNN` | 386 | — | $3.24 | +0.00 | $3.19 | -19.30 | -19.30 | +0.00 | -19.30 |
| 2026-08-17 | `NB` | 246 | — | $5.07 | +0.00 | $4.81 | -63.96 | -63.96 | +0.00 | -63.96 |
| 2026-08-18 | `DVN` | 27 | $47.57 | $48.00 | +11.61 | — | +0.00 | +11.61 | +49.14 | — |
| 2026-08-18 | `EOG` | 8 | $146.15 | $148.04 | +15.12 | — | +0.00 | +15.12 | +42.16 | — |
| 2026-08-18 | `FANG` | 6 | $206.29 | $208.93 | +15.84 | — | +0.00 | +15.84 | +37.38 | — |
| 2026-08-18 | `TMC` | 309 | $3.77 | $3.72 | -15.45 | — | +0.00 | -15.45 | -101.97 | — |
| 2026-08-18 | `TGB` | 147 | $8.77 | $8.55 | -32.34 | — | +0.00 | -32.34 | +13.23 | — |
| 2026-08-18 | `ELF` | 13 | $93.66 | $93.44 | -2.86 | — | +0.00 | -2.86 | +37.70 | — |
| 2026-08-18 | `DNN` | 386 | $3.19 | $3.11 | -30.88 | — | +0.00 | -30.88 | -50.18 | — |
| 2026-08-18 | `NB` | 246 | $4.81 | $4.66 | -36.90 | — | +0.00 | -36.90 | -100.86 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `AG` | 60 | — | $20.55 | +0.00 | $21.19 | +38.40 | +38.40 | +0.00 | +38.40 |
| 2026-08-20 | `BHP` | 13 | — | $91.01 | +0.00 | $93.63 | +34.06 | +34.06 | +0.00 | +34.06 |
| 2026-08-20 | `CDE` | 59 | — | $20.65 | +0.00 | $21.11 | +27.14 | +27.14 | +0.00 | +27.14 |
| 2026-08-20 | `HDSN` | 214 | — | $5.77 | +0.00 | $5.57 | -42.80 | -42.80 | +0.00 | -42.80 |
| 2026-08-20 | `IAG` | 63 | — | $19.63 | +0.00 | $20.50 | +54.81 | +54.81 | +0.00 | +54.81 |
| 2026-08-20 | `KGC` | 41 | — | $29.63 | +0.00 | $31.43 | +73.80 | +73.80 | +0.00 | +73.80 |
| 2026-08-20 | `NFGC` | 706 | — | $1.75 | +0.00 | $1.75 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `WPM` | 8 | — | $144.54 | +0.00 | $150.25 | +45.68 | +45.68 | +0.00 | +45.68 |
| 2026-08-21 | `AG` | 60 | $21.19 | $21.90 | +42.60 | — | +0.00 | +42.60 | +81.00 | — |
| 2026-08-21 | `BHP` | 13 | $93.63 | $95.72 | +27.17 | — | +0.00 | +27.17 | +61.23 | — |
| 2026-08-21 | `CDE` | 59 | $21.11 | $21.75 | +37.76 | — | +0.00 | +37.76 | +64.90 | — |
| 2026-08-21 | `HDSN` | 214 | $5.57 | $5.67 | +21.40 | — | +0.00 | +21.40 | -21.40 | — |
| 2026-08-21 | `IAG` | 63 | $20.50 | $21.17 | +42.21 | — | +0.00 | +42.21 | +97.02 | — |
| 2026-08-21 | `KGC` | 41 | $31.43 | $32.17 | +30.34 | — | +0.00 | +30.34 | +104.14 | — |
| 2026-08-21 | `NFGC` | 706 | $1.75 | $1.79 | +28.24 | — | +0.00 | +28.24 | +28.24 | — |
| 2026-08-21 | `WPM` | 8 | $150.25 | $154.70 | +35.60 | — | +0.00 | +35.60 | +81.28 | — |
| 2026-08-21 | `AU` | 10 | — | $119.43 | +0.00 | $121.22 | +17.90 | +17.90 | +0.00 | +17.90 |
| 2026-08-21 | `AUPH` | 75 | — | $17.20 | +0.00 | $16.65 | -41.25 | -41.25 | +0.00 | -41.25 |
| 2026-08-21 | `AEM` | 5 | — | $216.30 | +0.00 | $216.06 | -1.20 | -1.20 | +0.00 | -1.20 |
| 2026-08-21 | `ARCT` | 116 | — | $11.13 | +0.00 | $13.45 | +269.12 | +269.12 | +0.00 | +269.12 |
| 2026-08-21 | `AUTL` | 523 | — | $2.47 | +0.00 | $2.41 | -31.38 | -31.38 | +0.00 | -31.38 |
| 2026-08-21 | `CRDL` | 669 | — | $1.93 | +0.00 | $1.86 | -46.83 | -46.83 | +0.00 | -46.83 |
| 2026-08-21 | `CRSP` | 21 | — | $59.72 | +0.00 | $59.50 | -4.62 | -4.62 | +0.00 | -4.62 |
| 2026-08-21 | `CYPH` | 979 | — | $1.32 | +0.00 | $1.42 | +97.90 | +97.90 | +0.00 | +97.90 |
| 2026-08-24 | `AU` | 10 | $121.22 | $120.50 | -7.20 | — | +0.00 | -7.20 | +10.70 | — |
| 2026-08-24 | `AUPH` | 75 | $16.65 | $16.60 | -3.75 | — | +0.00 | -3.75 | -45.00 | — |
| 2026-08-24 | `AEM` | 5 | $216.06 | $217.03 | +4.85 | — | +0.00 | +4.85 | +3.65 | — |
| 2026-08-24 | `ARCT` | 116 | $13.45 | $13.26 | -22.04 | — | +0.00 | -22.04 | +247.08 | — |
| 2026-08-24 | `AUTL` | 523 | $2.41 | $2.36 | -26.15 | — | +0.00 | -26.15 | -57.53 | — |
| 2026-08-24 | `CRDL` | 669 | $1.86 | $1.87 | +6.69 | — | +0.00 | +6.69 | -40.14 | — |
| 2026-08-24 | `CRSP` | 21 | $59.50 | $58.79 | -14.91 | — | +0.00 | -14.91 | -19.53 | — |
| 2026-08-24 | `CYPH` | 979 | $1.42 | $1.83 | +401.39 | — | +0.00 | +401.39 | +499.29 | — |
| 2026-08-25 | `MOS` | 56 | — | $24.00 | +0.00 | $23.75 | -14.00 | -14.00 | +0.00 | -14.00 |
| 2026-08-25 | `OCUL` | 124 | — | $10.92 | +0.00 | $10.92 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-25 | `INSP` | 22 | — | $61.47 | +0.00 | $61.47 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-25 | `CRMD` | 164 | — | $8.28 | +0.00 | $8.28 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-25 | `RZLT` | 259 | — | $5.23 | +0.00 | $5.29 | +15.54 | +15.54 | +0.00 | +15.54 |
| 2026-08-25 | `HCA` | 3 | — | $429.24 | +0.00 | $428.50 | -2.22 | -2.22 | +0.00 | -2.22 |
| 2026-08-25 | `BMEA` | 838 | — | $1.62 | +0.00 | $1.61 | -8.38 | -8.38 | +0.00 | -8.38 |
| 2026-08-25 | `NPWR` | 678 | — | $2.00 | +0.00 | $2.02 | +13.56 | +13.56 | +0.00 | +13.56 |
| 2026-08-26 | `MOS` | 56 | $23.75 | $23.75 | +0.00 | $23.75 | +0.00 | +0.00 | -14.00 | -14.00 |
| 2026-08-26 | `OCUL` | 124 | $10.92 | $10.92 | +0.00 | $10.92 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-26 | `INSP` | 22 | $61.47 | $61.47 | +0.00 | $61.47 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-26 | `CRMD` | 164 | $8.28 | $8.28 | +0.00 | $8.28 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-26 | `RZLT` | 259 | $5.29 | $5.29 | +0.00 | $5.29 | +0.00 | +0.00 | +15.54 | +15.54 |
| 2026-08-26 | `HCA` | 3 | $428.50 | $428.50 | +0.00 | $428.50 | +0.00 | +0.00 | -2.22 | -2.22 |
| 2026-08-26 | `BMEA` | 838 | $1.61 | $1.61 | +0.00 | $1.61 | +0.00 | +0.00 | -8.38 | -8.38 |
| 2026-08-26 | `NPWR` | 678 | $2.02 | $2.02 | +0.00 | $2.02 | +0.00 | +0.00 | +13.56 | +13.56 |
| 2026-08-27 | `MOS` | 56 | $23.75 | $24.84 | +61.04 | — | +0.00 | +61.04 | +47.04 | — |
| 2026-08-27 | `OCUL` | 124 | $10.92 | $10.79 | -16.12 | — | +0.00 | -16.12 | -16.12 | — |
| 2026-08-27 | `INSP` | 22 | $61.47 | $60.07 | -30.80 | — | +0.00 | -30.80 | -30.80 | — |
| 2026-08-27 | `CRMD` | 164 | $8.28 | $8.60 | +52.48 | — | +0.00 | +52.48 | +52.48 | — |
| 2026-08-27 | `RZLT` | 259 | $5.29 | $5.01 | -72.52 | — | +0.00 | -72.52 | -56.98 | — |
| 2026-08-27 | `HCA` | 3 | $428.50 | $427.50 | -3.00 | — | +0.00 | -3.00 | -5.22 | — |
| 2026-08-27 | `BMEA` | 838 | $1.61 | $1.75 | +117.32 | — | +0.00 | +117.32 | +108.94 | — |
| 2026-08-27 | `NPWR` | 678 | $2.02 | $1.93 | -61.02 | — | +0.00 | -61.02 | -47.46 | — |
| 2026-08-27 | `RRC` | 44 | — | $40.72 | +0.00 | $41.55 | +36.52 | +36.52 | +0.00 | +36.52 |
| 2026-08-27 | `ACMR` | 22 | — | $80.97 | +0.00 | $79.11 | -40.92 | -40.92 | +0.00 | -40.92 |
| 2026-08-27 | `MU` | 1 | — | $925.74 | +0.00 | $938.40 | +12.66 | +12.66 | +0.00 | +12.66 |
| 2026-08-27 | `ASML` | 1 | — | $1746.33 | +0.00 | $1745.64 | -0.69 | -0.69 | +0.00 | -0.69 |
| 2026-08-27 | `LRCX` | 5 | — | $314.61 | +0.00 | $312.88 | -8.65 | -8.65 | +0.00 | -8.65 |
| 2026-08-27 | `NVDA` | 8 | — | $212.64 | +0.00 | $209.66 | -23.84 | -23.84 | +0.00 | -23.84 |
| 2026-08-28 | `RRC` | 44 | $41.55 | $41.44 | -4.84 | $41.64 | +8.80 | +3.96 | +31.68 | +40.48 |
| 2026-08-28 | `ACMR` | 22 | $79.11 | $81.65 | +55.88 | — | +0.00 | +55.88 | +14.96 | — |
| 2026-08-28 | `MU` | 1 | $938.40 | $967.01 | +28.61 | — | +0.00 | +28.61 | +41.27 | — |
| 2026-08-28 | `ASML` | 1 | $1745.64 | $1746.53 | +0.89 | — | +0.00 | +0.89 | +0.20 | — |
| 2026-08-28 | `LRCX` | 5 | $312.88 | $318.88 | +30.00 | — | +0.00 | +30.00 | +21.35 | — |
| 2026-08-28 | `NVDA` | 8 | $209.66 | $222.86 | +105.60 | — | +0.00 | +105.60 | +81.76 | — |
| 2026-08-28 | `CRK` | 91 | — | $14.42 | +0.00 | $14.62 | +18.20 | +18.20 | +0.00 | +18.20 |
| 2026-08-28 | `MOS` | 54 | — | $24.00 | +0.00 | $23.76 | -12.96 | -12.96 | +0.00 | -12.96 |
| 2026-08-28 | `SLI` | 505 | — | $2.60 | +0.00 | $2.64 | +20.20 | +20.20 | +0.00 | +20.20 |
| 2026-08-28 | `ANF` | 9 | — | $144.70 | +0.00 | $145.75 | +9.45 | +9.45 | +0.00 | +9.45 |
| 2026-08-28 | `BHVN` | 77 | — | $16.95 | +0.00 | $16.12 | -63.91 | -63.91 | +0.00 | -63.91 |
| 2026-08-28 | `BZ` | 70 | — | $18.50 | +0.00 | $18.00 | -35.00 | -35.00 | +0.00 | -35.00 |
| 2026-08-28 | `CAPR` | 142 | — | $9.19 | +0.00 | $10.06 | +123.54 | +123.54 | +0.00 | +123.54 |
| 2026-08-31 | `RRC` | 44 | $41.64 | $41.11 | -23.32 | — | +0.00 | -23.32 | +17.16 | — |
| 2026-08-31 | `CRK` | 91 | $14.62 | $14.56 | -5.46 | — | +0.00 | -5.46 | +12.74 | — |
| 2026-08-31 | `MOS` | 54 | $23.76 | $23.75 | -0.54 | — | +0.00 | -0.54 | -13.50 | — |
| 2026-08-31 | `SLI` | 505 | $2.64 | $2.51 | -65.65 | — | +0.00 | -65.65 | -45.45 | — |
| 2026-08-31 | `ANF` | 9 | $145.75 | $148.67 | +26.28 | — | +0.00 | +26.28 | +35.73 | — |
| 2026-08-31 | `BHVN` | 77 | $16.12 | $15.44 | -52.36 | — | +0.00 | -52.36 | -116.27 | — |
| 2026-08-31 | `BZ` | 70 | $18.00 | $17.89 | -7.70 | — | +0.00 | -7.70 | -42.70 | — |
| 2026-08-31 | `CAPR` | 142 | $10.06 | $9.44 | -88.04 | — | +0.00 | -88.04 | +35.50 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `ATRC` | 27 | — | $49.76 | +0.00 | $52.59 | +76.41 | +76.41 | +0.00 | +76.41 |
| 2026-09-03 | `HRMY` | 32 | — | $41.31 | +0.00 | $42.86 | +49.60 | +49.60 | +0.00 | +49.60 |
| 2026-09-03 | `CABA` | 413 | — | $3.27 | +0.00 | $3.57 | +123.90 | +123.90 | +0.00 | +123.90 |
| 2026-09-03 | `VSTM` | 175 | — | $7.70 | +0.00 | $8.02 | +56.00 | +56.00 | +0.00 | +56.00 |
| 2026-09-03 | `RVTY` | 10 | — | $125.94 | +0.00 | $130.94 | +50.00 | +50.00 | +0.00 | +50.00 |
| 2026-09-03 | `GPRO` | 1109 | — | $1.22 | +0.00 | $1.69 | +521.23 | +521.23 | +0.00 | +521.23 |
| 2026-09-03 | `FRVO` | 73 | — | $18.40 | +0.00 | $17.98 | -30.66 | -30.66 | +0.00 | -30.66 |
| 2026-09-03 | `CRK` | 86 | — | $15.70 | +0.00 | $15.54 | -13.76 | -13.76 | +0.00 | -13.76 |
| 2026-09-04 | `ATRC` | 27 | $52.59 | $52.88 | +7.83 | $52.46 | -11.34 | -3.51 | +84.24 | +72.90 |
| 2026-09-04 | `HRMY` | 32 | $42.86 | $42.93 | +2.24 | — | +0.00 | +2.24 | +51.84 | — |
| 2026-09-04 | `CABA` | 413 | $3.57 | $3.63 | +24.78 | $3.48 | -61.95 | -37.17 | +148.68 | +86.73 |
| 2026-09-04 | `VSTM` | 175 | $8.02 | $8.03 | +1.75 | — | +0.00 | +1.75 | +57.75 | — |
| 2026-09-04 | `RVTY` | 10 | $130.94 | $132.45 | +15.10 | — | +0.00 | +15.10 | +65.10 | — |
| 2026-09-04 | `GPRO` | 1109 | $1.69 | $1.78 | +99.81 | $1.39 | -432.51 | -332.70 | +621.04 | +188.53 |
| 2026-09-04 | `FRVO` | 73 | $17.98 | $18.27 | +21.17 | — | +0.00 | +21.17 | -9.49 | — |
| 2026-09-04 | `CRK` | 86 | $15.54 | $15.45 | -7.74 | — | +0.00 | -7.74 | -21.50 | — |
| 2026-09-04 | `ASND` | 5 | — | $266.94 | +0.00 | $271.12 | +20.90 | +20.90 | +0.00 | +20.90 |
| 2026-09-04 | `OSCR` | 44 | — | $30.65 | +0.00 | $32.24 | +69.96 | +69.96 | +0.00 | +69.96 |
| 2026-09-04 | `NVAX` | 132 | — | $10.41 | +0.00 | $10.34 | -9.24 | -9.24 | +0.00 | -9.24 |
| 2026-09-04 | `BVS` | 94 | — | $14.50 | +0.00 | $14.36 | -13.16 | -13.16 | +0.00 | -13.16 |
| 2026-09-04 | `BAK` | 705 | — | $1.95 | +0.00 | $1.94 | -7.05 | -7.05 | +0.00 | -7.05 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +91.20 | TLN, VST, NRG, DAVE, SLG, MARA, LDI, BTBT | — | $560.20 | $10,051.46 | TLN×3, VST×8, NRG×10, DAVE×3, SLG×21, MARA×138, LDI×1334, BTBT×833 |
| 2026-08-17 | +2.25 | $560.20 | TLN×3, VST×8, NRG×10, DAVE×3, SLG×21, MARA×138, LDI×1334, BTBT×833 | $10,054.84 | +3.38 | +2.46 | DVN, EOG, FANG, TMC, TGB, ELF, DNN, NB | TLN, VST, NRG, DAVE, SLG, MARA, LDI, BTBT | $217.13 | $9,994.76 | DVN×27, EOG×8, FANG×6, TMC×309, TGB×147, ELF×13, DNN×386, NB×246 |
| 2026-08-18 | -6.20 | $217.13 | DVN×27, EOG×8, FANG×6, TMC×309, TGB×147, ELF×13, DNN×386, NB×246 | $9,918.90 | -75.86 | +0.00 | — | DVN, EOG, FANG, TMC, TGB, ELF, DNN, NB | $9,895.91 | $9,895.91 | — |
| 2026-08-19 | -7.20 | $9,895.91 | — | $9,895.91 | -0.00 | +0.00 | — | — | $9,895.91 | $9,895.91 | — |
| 2026-08-20 | +1.12 | $9,895.91 | — | $9,895.91 | -0.00 | +231.09 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | — | $158.77 | $10,102.46 | AG×60, BHP×13, CDE×59, HDSN×214, IAG×63, KGC×41, NFGC×706, WPM×8 |
| 2026-08-21 | +3.25 | $158.77 | AG×60, BHP×13, CDE×59, HDSN×214, IAG×63, KGC×41, NFGC×706, WPM×8 | $10,367.78 | +265.32 | +259.64 | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $318.05 | $10,563.95 | AU×10, AUPH×75, AEM×5, ARCT×116, AUTL×523, CRDL×669, CRSP×21, CYPH×979 |
| 2026-08-24 | -5.17 | $318.05 | AU×10, AUPH×75, AEM×5, ARCT×116, AUTL×523, CRDL×669, CRSP×21, CYPH×979 | $10,902.83 | +338.88 | +0.00 | — | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | $10,863.68 | $10,863.68 | — |
| 2026-08-25 | +1.80 | $10,863.68 | — | $10,863.68 | +0.00 | +4.50 | MOS, OCUL, INSP, CRMD, RZLT, HCA, BMEA, NPWR | — | $65.54 | $10,834.23 | MOS×56, OCUL×124, INSP×22, CRMD×164, RZLT×259, HCA×3, BMEA×838, NPWR×678 |
| 2026-08-26 | +2.02 | $65.54 | MOS×56, OCUL×124, INSP×22, CRMD×164, RZLT×259, HCA×3, BMEA×838, NPWR×678 | $10,834.23 | -0.00 | +0.00 | — | — | $65.54 | $10,834.23 | MOS×56, OCUL×124, INSP×22, CRMD×164, RZLT×259, HCA×3, BMEA×838, NPWR×678 |
| 2026-08-27 | — | $65.54 | MOS×56, OCUL×124, INSP×22, CRMD×164, RZLT×259, HCA×3, BMEA×838, NPWR×678 | $10,881.61 | +47.38 | -24.92 | RRC, ACMR, MU, ASML, LRCX, NVDA | MOS, OCUL, INSP, CRMD, RZLT, HCA, BMEA, NPWR | $1,315.75 | $10,810.09 | RRC×44, ACMR×22, MU×1, ASML×1, LRCX×5, NVDA×8 |
| 2026-08-28 | +0.75 | $1,315.75 | RRC×44, ACMR×22, MU×1, ASML×1, LRCX×5, NVDA×8 | $11,026.23 | +216.14 | +68.32 | CRK, MOS, SLI, ANF, BHVN, BZ, CAPR | ACMR, MU, ASML, LRCX, NVDA | $44.26 | $11,064.59 | RRC×44, CRK×91, MOS×54, SLI×505, ANF×9, BHVN×77, BZ×70, CAPR×142 |
| 2026-08-31 | -5.85 | $44.26 | RRC×44, CRK×91, MOS×54, SLI×505, ANF×9, BHVN×77, BZ×70, CAPR×142 | $10,847.80 | -216.79 | +0.00 | — | RRC, CRK, MOS, SLI, ANF, BHVN, BZ, CAPR | $10,825.63 | $10,825.63 | — |
| 2026-09-01 | -6.30 | $10,825.63 | — | $10,825.63 | +0.00 | +0.00 | — | — | $10,825.63 | $10,825.63 | — |
| 2026-09-02 | -3.83 | $10,825.63 | — | $10,825.63 | +0.00 | +0.00 | — | — | $10,825.63 | $10,825.63 | — |
| 2026-09-03 | -0.90 | $10,825.63 | — | $10,825.63 | +0.00 | +832.72 | ATRC, HRMY, CABA, VSTM, RVTY, GPRO, FRVO, CRK | — | $123.62 | $11,625.57 | ATRC×27, HRMY×32, CABA×413, VSTM×175, RVTY×10, GPRO×1109, FRVO×73, CRK×86 |
| 2026-09-04 | — | $123.62 | ATRC×27, HRMY×32, CABA×413, VSTM×175, RVTY×10, GPRO×1109, FRVO×73, CRK×86 | $11,790.51 | +164.94 | -444.39 | ASND, OSCR, NVAX, BVS, BAK | HRMY, VSTM, RVTY, FRVO, CRK | $65.28 | $11,317.03 | ATRC×27, CABA×413, GPRO×1109, ASND×5, OSCR×44, NVAX×132, BVS×94, BAK×705 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 3 | $359.83 | $2.00 | — | $8,918.51 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ret5=+5.9; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 8 | $146.90 | $2.01 | — | $7,741.30 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ret5=+3.6; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 10 | $120.00 | $2.02 | — | $6,539.28 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ret5=+0.6; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `DAVE` | 3 | $330.91 | $2.00 | — | $5,544.55 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=-8.6; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 21 | $57.61 | $2.05 | — | $4,332.68 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ret5=+5.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🔴 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 138 | $9.01 | $2.40 | — | $3,086.90 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=-13.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 1334 | $0.94 | $16.50 | — | $1,820.44 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 833 | $1.50 | $10.75 | — | $560.20 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $560.20 | ▲ close $10,051.46 vs 09:30 $10,000.00 (session +91.20) | 16:00 close · cash $560.20 · equity $10,051.46 vs 09:30 $10,000.00 (+51.46; session marks +91.20) · 8 name(s) marked open→close (per-name table). TLN×3 09:30 $359.83 → close $362.74 +8.73; VST×8 09:30 $146.90 → close $148.13 +9.84; NRG×10 09:30 $120.00 → close $126.24 +62.40; DAVE×3 09:30 $330.91 → close $334.57 +10.98; SLG×21 09:30 $57.61 → close $56.09 -31.92; MARA×138 09:30 $9.01 → close $9.20 +26.22; LDI×1334 09:30 $0.94 → close $0.90 -53.36; BTBT×833 09:30 $1.50 → close $1.57 +58.31 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $560.20 | ▲ 09:30 equity $10,054.84 vs yday $10,051.46 (+3.38) | 09:30 open · cash $560.20 (unchanged overnight, no fees) · equity $10,054.84 vs prior close $10,051.46 (+3.38) · 8 name(s) re-marked at the open (per-name table). TLN×3 yday $362.74 → 09:30 $367.88 +15.42; VST×8 yday $148.13 → 09:30 $149.37 +9.92; NRG×10 yday $126.24 → 09:30 $127.40 +11.60; DAVE×3 yday $334.57 → 09:30 $336.94 +7.11; SLG×21 yday $56.09 → 09:30 $55.37 -15.12; MARA×138 yday $9.20 → 09:30 $9.22 +2.76; LDI×1334 yday $0.90 → 09:30 $0.91 +13.34; BTBT×833 yday $1.57 → 09:30 $1.52 -41.65 | — |
| 2026-08-17 09:30 ET | **SELL** | `TLN` | 3 | $367.88 | $2.02 | $+20.13 | $1,661.82 | ▲ +20.13 after sell → book $10,052.82; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VST` | 8 | $149.37 | $2.03 | $+15.71 | $2,854.74 | ▲ +15.71 after sell → book $10,050.79; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NRG` | 10 | $127.40 | $2.04 | $+69.94 | $4,126.70 | ▲ +69.94 after sell → book $10,048.75; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `DAVE` | 3 | $336.94 | $2.02 | $+14.07 | $5,135.50 | ▲ +14.07 after sell → book $10,046.73; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SLG` | 21 | $55.37 | $2.07 | $-51.17 | $6,296.20 | ▼ -51.17 after sell → book $10,044.66; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MARA` | 138 | $9.22 | $2.44 | $+24.14 | $7,566.12 | ▲ +24.14 after sell → book $10,042.22; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LDI` | 1334 | $0.91 | $16.33 | $-72.85 | $8,759.73 | ▼ -72.85 after sell → book $10,025.89; vs 09:30 mark -16.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 833 | $1.52 | $10.89 | $-4.98 | $10,014.99 | ▼ -4.98 after sell → book $10,014.99; vs 09:30 mark -10.90 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 27 | $46.18 | $2.07 | — | $8,766.06 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ret5=+6.7; leftover $1251.87 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 8 | $142.77 | $2.01 | — | $7,621.89 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ret5=+5.8; leftover $1251.87 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 6 | $202.70 | $2.01 | — | $6,403.68 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ret5=+8.3; leftover $1251.87 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 309 | $4.05 | $3.99 | — | $5,148.25 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=-12.3; leftover $1251.87 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 147 | $8.46 | $2.43 | — | $3,902.19 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=+0.4; leftover $1251.87 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ELF` | 13 | $90.54 | $2.03 | — | $2,723.15 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; ret5=-7.2; leftover $1251.87 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 386 | $3.24 | $4.98 | — | $1,467.53 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; ⚪; ret5=+0.3; leftover $1251.87 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `NB` | 246 | $5.07 | $3.17 | — | $217.13 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; ret5=-4.7; leftover $1251.87 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $217.13 | ▲ close $9,994.76 vs 09:30 $10,054.84 (session +2.46) | 16:00 close · cash $217.13 · equity $9,994.76 vs 09:30 $10,054.84 (-60.08; session marks +2.46) · 8 name(s) marked open→close (per-name table). DVN×27 09:30 $46.18 → close $47.57 +37.53; EOG×8 09:30 $142.77 → close $146.15 +27.04; FANG×6 09:30 $202.70 → close $206.29 +21.54; TMC×309 09:30 $4.05 → close $3.77 -86.52; TGB×147 09:30 $8.46 → close $8.77 +45.57; ELF×13 09:30 $90.54 → close $93.66 +40.56; DNN×386 09:30 $3.24 → close $3.19 -19.30; NB×246 09:30 $5.07 → close $4.81 -63.96 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $217.13 | ▼ 09:30 equity $9,918.90 vs yday $9,994.76 (-75.86) | 09:30 open · cash $217.13 (unchanged overnight, no fees) · equity $9,918.90 vs prior close $9,994.76 (-75.86) · 8 name(s) re-marked at the open (per-name table). DVN×27 yday $47.57 → 09:30 $48.00 +11.61; EOG×8 yday $146.15 → 09:30 $148.04 +15.12; FANG×6 yday $206.29 → 09:30 $208.93 +15.84; TMC×309 yday $3.77 → 09:30 $3.72 -15.45; TGB×147 yday $8.77 → 09:30 $8.55 -32.34; ELF×13 yday $93.66 → 09:30 $93.44 -2.86; DNN×386 yday $3.19 → 09:30 $3.11 -30.88; NB×246 yday $4.81 → 09:30 $4.66 -36.90 | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 27 | $48.00 | $2.09 | $+44.98 | $1,511.04 | ▲ +44.98 after sell → book $9,916.81; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 8 | $148.04 | $2.03 | $+38.11 | $2,693.33 | ▲ +38.11 after sell → book $9,914.78; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 6 | $208.93 | $2.03 | $+33.34 | $3,944.88 | ▲ +33.34 after sell → book $9,912.75; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TMC` | 309 | $3.72 | $4.05 | $-110.00 | $5,090.31 | ▼ -110.00 after sell → book $9,908.70; vs 09:30 mark -4.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGB` | 147 | $8.55 | $2.47 | $+8.33 | $6,344.70 | ▲ +8.33 after sell → book $9,906.24; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ELF` | 13 | $93.44 | $2.05 | $+33.62 | $7,557.37 | ▲ +33.62 after sell → book $9,904.19; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `DNN` | 386 | $3.11 | $5.05 | $-60.21 | $8,752.77 | ▼ -60.21 after sell → book $9,899.13; vs 09:30 mark -5.06 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 vol🟢 buy🟢 |
| 2026-08-18 09:30 ET | **SELL** | `NB` | 246 | $4.66 | $3.22 | $-107.26 | $9,895.91 | ▼ -107.26 after sell → book $9,895.91; vs 09:30 mark -3.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,895.91 | ▲ close $9,895.91 vs 09:30 $9,918.90 (session +0.00) | 16:00 close · cash $9,895.91 · no lots left · equity $9,895.91. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,895.91 | ▲ 09:30 equity $9,895.91 vs yday $9,895.91 (-0.00) | 09:30 open · cash $9,895.91 · no holdings · equity $9,895.91 vs prior close $9,895.91 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,895.91 | ▲ close $9,895.91 vs 09:30 $9,895.91 (session +0.00) | 16:00 close · cash $9,895.91 · no lots left · equity $9,895.91. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,895.91 | ▲ 09:30 equity $9,895.91 vs yday $9,895.91 (-0.00) | 09:30 open · cash $9,895.91 · no holdings · equity $9,895.91 vs prior close $9,895.91 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 60 | $20.55 | $2.17 | — | $8,660.74 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1236.99 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,475.58 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1236.99 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 59 | $20.65 | $2.17 | — | $6,255.06 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1236.99 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 214 | $5.77 | $2.76 | — | $5,017.52 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1236.99 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 63 | $19.63 | $2.18 | — | $3,778.65 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1236.99 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 41 | $29.63 | $2.11 | — | $2,561.71 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1236.99 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 706 | $1.75 | $9.11 | — | $1,317.10 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1236.99 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $158.77 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1236.99 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $158.77 | ▲ close $10,102.46 vs 09:30 $9,895.91 (session +231.09) | 16:00 close · cash $158.77 · equity $10,102.46 vs 09:30 $9,895.91 (+206.55; session marks +231.09) · 8 name(s) marked open→close (per-name table). AG×60 09:30 $20.55 → close $21.19 +38.40; BHP×13 09:30 $91.01 → close $93.63 +34.06; CDE×59 09:30 $20.65 → close $21.11 +27.14; HDSN×214 09:30 $5.77 → close $5.57 -42.80; IAG×63 09:30 $19.63 → close $20.50 +54.81; KGC×41 09:30 $29.63 → close $31.43 +73.80; NFGC×706 09:30 $1.75 → close $1.75 +0.00; WPM×8 09:30 $144.54 → close $150.25 +45.68 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $158.77 | ▲ 09:30 equity $10,367.78 vs yday $10,102.46 (+265.32) | 09:30 open · cash $158.77 (unchanged overnight, no fees) · equity $10,367.78 vs prior close $10,102.46 (+265.32) · 8 name(s) re-marked at the open (per-name table). AG×60 yday $21.19 → 09:30 $21.90 +42.60; BHP×13 yday $93.63 → 09:30 $95.72 +27.17; CDE×59 yday $21.11 → 09:30 $21.75 +37.76; HDSN×214 yday $5.57 → 09:30 $5.67 +21.40; IAG×63 yday $20.50 → 09:30 $21.17 +42.21; KGC×41 yday $31.43 → 09:30 $32.17 +30.34; NFGC×706 yday $1.75 → 09:30 $1.79 +28.24; WPM×8 yday $150.25 → 09:30 $154.70 +35.60 | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 60 | $21.90 | $2.19 | $+76.64 | $1,470.58 | ▲ +76.64 after sell → book $10,365.59; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $2,712.89 | ▲ +57.15 after sell → book $10,363.54; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 59 | $21.75 | $2.19 | $+60.55 | $3,993.95 | ▲ +60.55 after sell → book $10,361.35; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 214 | $5.67 | $2.81 | $-26.97 | $5,204.53 | ▼ -26.97 after sell → book $10,358.55; vs 09:30 mark -2.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 63 | $21.17 | $2.20 | $+92.64 | $6,536.04 | ▲ +92.64 after sell → book $10,356.35; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 41 | $32.17 | $2.13 | $+99.89 | $7,852.87 | ▲ +99.89 after sell → book $10,354.21; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 706 | $1.79 | $9.23 | $+9.90 | $9,107.38 | ▲ +9.90 after sell → book $10,344.98; vs 09:30 mark -9.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 8 | $154.70 | $2.03 | $+77.23 | $10,342.94 | ▲ +77.23 after sell → book $10,342.94; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $9,146.62 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+20.4; leftover $1292.87 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 75 | $17.20 | $2.21 | — | $7,854.41 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $1292.87 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 5 | $216.30 | $2.00 | — | $6,770.90 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1292.87 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 116 | $11.13 | $2.34 | — | $5,477.49 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1292.87 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 523 | $2.47 | $6.75 | — | $4,178.93 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $1292.87 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 669 | $1.93 | $8.63 | — | $2,879.13 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $1292.87 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 21 | $59.72 | $2.05 | — | $1,622.96 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1292.87 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 979 | $1.32 | $12.63 | — | $318.05 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1292.87 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $318.05 | ▲ close $10,563.95 vs 09:30 $10,367.78 (session +259.64) | 16:00 close · cash $318.05 · equity $10,563.95 vs 09:30 $10,367.78 (+196.17; session marks +259.64) · 8 name(s) marked open→close (per-name table). AU×10 09:30 $119.43 → close $121.22 +17.90; AUPH×75 09:30 $17.20 → close $16.65 -41.25; AEM×5 09:30 $216.30 → close $216.06 -1.20; ARCT×116 09:30 $11.13 → close $13.45 +269.12; AUTL×523 09:30 $2.47 → close $2.41 -31.38; CRDL×669 09:30 $1.93 → close $1.86 -46.83; CRSP×21 09:30 $59.72 → close $59.50 -4.62; CYPH×979 09:30 $1.32 → close $1.42 +97.90 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $318.05 | ▲ 09:30 equity $10,902.83 vs yday $10,563.95 (+338.88) | 09:30 open · cash $318.05 (unchanged overnight, no fees) · equity $10,902.83 vs prior close $10,563.95 (+338.88) · 8 name(s) re-marked at the open (per-name table). AU×10 yday $121.22 → 09:30 $120.50 -7.20; AUPH×75 yday $16.65 → 09:30 $16.60 -3.75; AEM×5 yday $216.06 → 09:30 $217.03 +4.85; ARCT×116 yday $13.45 → 09:30 $13.26 -22.04; AUTL×523 yday $2.41 → 09:30 $2.36 -26.15; CRDL×669 yday $1.86 → 09:30 $1.87 +6.69; CRSP×21 yday $59.50 → 09:30 $58.79 -14.91; CYPH×979 yday $1.42 → 09:30 $1.83 +401.39 | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.50 | $2.04 | $+6.64 | $1,521.01 | ▲ +6.64 after sell → book $10,900.79; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 75 | $16.60 | $2.24 | $-49.45 | $2,763.77 | ▼ -49.45 after sell → book $10,898.55; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 5 | $217.03 | $2.02 | $-0.38 | $3,846.89 | ▼ -0.38 after sell → book $10,896.52; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 116 | $13.26 | $2.37 | $+242.37 | $5,382.69 | ▲ +242.37 after sell → book $10,894.16; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 523 | $2.36 | $6.84 | $-71.12 | $6,610.12 | ▼ -71.12 after sell → book $10,887.31; vs 09:30 mark -6.85 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRDL` | 669 | $1.87 | $8.75 | $-57.52 | $7,852.40 | ▼ -57.52 after sell → book $10,878.56; vs 09:30 mark -8.75 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 21 | $58.79 | $2.07 | $-23.66 | $9,084.92 | ▼ -23.66 after sell → book $10,876.49; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 979 | $1.83 | $12.81 | $+473.86 | $10,863.68 | ▲ +473.86 after sell → book $10,863.68; vs 09:30 mark -12.81 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,863.68 | ▲ close $10,863.68 vs 09:30 $10,902.83 (session +0.00) | 16:00 close · cash $10,863.68 · no lots left · equity $10,863.68. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,863.68 | ▲ 09:30 equity $10,863.68 vs yday $10,863.68 (+0.00) | 09:30 open · cash $10,863.68 · no holdings · equity $10,863.68 vs prior close $10,863.68 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 56 | $24.00 | $2.16 | — | $9,517.52 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; ⚪; ret5=+13.0; leftover $1357.96 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 124 | $10.92 | $2.36 | — | $8,161.08 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ret5=+10.4; leftover $1357.96 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 22 | $61.47 | $2.06 | — | $6,806.69 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ret5=+9.2; leftover $1357.96 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 164 | $8.28 | $2.48 | — | $5,446.28 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=+8.8; leftover $1357.96 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 259 | $5.23 | $3.34 | — | $4,088.37 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; ret5=+10.7; leftover $1357.96 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 3 | $429.24 | $2.00 | — | $2,798.65 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; ret5=+6.1; leftover $1357.96 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 838 | $1.62 | $10.81 | — | $1,430.28 | — | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $1357.96 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 678 | $2.00 | $8.75 | — | $65.54 | — | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.8; leftover $1357.96 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $65.54 | ▲ close $10,834.23 vs 09:30 $10,863.68 (session +4.50) | 16:00 close · cash $65.54 · equity $10,834.23 vs 09:30 $10,863.68 (-29.45; session marks +4.50) · 8 name(s) marked open→close (per-name table). MOS×56 09:30 $24.00 → close $23.75 -14.00; OCUL×124 09:30 $10.92 → close $10.92 +0.00; INSP×22 09:30 $61.47 → close $61.47 +0.00; CRMD×164 09:30 $8.28 → close $8.28 +0.00; RZLT×259 09:30 $5.23 → close $5.29 +15.54; HCA×3 09:30 $429.24 → close $428.50 -2.22; BMEA×838 09:30 $1.62 → close $1.61 -8.38; NPWR×678 09:30 $2.00 → close $2.02 +13.56 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $65.54 | ▲ 09:30 equity $10,834.23 vs yday $10,834.23 (-0.00) | 09:30 open · cash $65.54 (unchanged overnight, no fees) · equity $10,834.23 vs prior close $10,834.23 (-0.00) · 8 name(s) re-marked at the open (per-name table). MOS×56 yday $23.75 → 09:30 $23.75 +0.00; OCUL×124 yday $10.92 → 09:30 $10.92 +0.00; INSP×22 yday $61.47 → 09:30 $61.47 +0.00; CRMD×164 yday $8.28 → 09:30 $8.28 +0.00; RZLT×259 yday $5.29 → 09:30 $5.29 +0.00; HCA×3 yday $428.50 → 09:30 $428.50 +0.00; BMEA×838 yday $1.61 → 09:30 $1.61 +0.00; NPWR×678 yday $2.02 → 09:30 $2.02 +0.00 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $65.54 | ▲ close $10,834.23 vs 09:30 $10,834.23 (session +0.00) | 16:00 close · cash $65.54 · equity $10,834.23 vs 09:30 $10,834.23 (-0.00; session marks +0.00) · 8 name(s) marked open→close (per-name table). MOS×56 09:30 $23.75 → close $23.75 +0.00; OCUL×124 09:30 $10.92 → close $10.92 +0.00; INSP×22 09:30 $61.47 → close $61.47 +0.00; CRMD×164 09:30 $8.28 → close $8.28 +0.00; RZLT×259 09:30 $5.29 → close $5.29 +0.00; HCA×3 09:30 $428.50 → close $428.50 +0.00; BMEA×838 09:30 $1.61 → close $1.61 +0.00; NPWR×678 09:30 $2.02 → close $2.02 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $65.54 | ▲ 09:30 equity $10,881.61 vs yday $10,834.23 (+47.38) | 09:30 open · cash $65.54 (unchanged overnight, no fees) · equity $10,881.61 vs prior close $10,834.23 (+47.38) · 8 name(s) re-marked at the open (per-name table). MOS×56 yday $23.75 → 09:30 $24.84 +61.04; OCUL×124 yday $10.92 → 09:30 $10.79 -16.12; INSP×22 yday $61.47 → 09:30 $60.07 -30.80; CRMD×164 yday $8.28 → 09:30 $8.60 +52.48; RZLT×259 yday $5.29 → 09:30 $5.01 -72.52; HCA×3 yday $428.50 → 09:30 $427.50 -3.00; BMEA×838 yday $1.61 → 09:30 $1.75 +117.32; NPWR×678 yday $2.02 → 09:30 $1.93 -61.02 | — |
| 2026-08-27 09:30 ET | **SELL** | `MOS` | 56 | $24.84 | $2.18 | $+42.70 | $1,454.40 | ▲ +42.70 after sell → book $10,879.43; vs 09:30 mark -2.18 | dropped from list after 2 sess (min 1) | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-27 09:30 ET | **SELL** | `OCUL` | 124 | $10.79 | $2.39 | $-20.88 | $2,789.96 | ▼ -20.88 after sell → book $10,877.03; vs 09:30 mark -2.40 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `INSP` | 22 | $60.07 | $2.08 | $-34.93 | $4,109.43 | ▼ -34.93 after sell → book $10,874.96; vs 09:30 mark -2.07 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRMD` | 164 | $8.60 | $2.52 | $+47.48 | $5,517.31 | ▲ +47.48 after sell → book $10,872.44; vs 09:30 mark -2.52 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `RZLT` | 259 | $5.01 | $3.39 | $-63.72 | $6,811.50 | ▼ -63.72 after sell → book $10,869.04; vs 09:30 mark -3.40 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `HCA` | 3 | $427.50 | $2.02 | $-9.24 | $8,091.98 | ▼ -9.24 after sell → book $10,867.02; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BMEA` | 838 | $1.75 | $10.96 | $+87.17 | $9,547.52 | ▲ +87.17 after sell → book $10,856.06; vs 09:30 mark -10.96 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `NPWR` | 678 | $1.93 | $8.87 | $-65.08 | $10,847.19 | ▼ -65.08 after sell → book $10,847.19; vs 09:30 mark -8.87 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 44 | $40.72 | $2.12 | — | $9,053.39 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; ret5=+1.8; leftover $1807.87 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 22 | $80.97 | $2.06 | — | $7,270.00 | — | union ∩ news_present, no 🚨; gate news_present=True; list mover_buy; 🔵; ret5=-1.3; leftover $1807.87 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $925.74 | $1.99 | — | $6,342.26 | — | union ∩ news_present, no 🚨; gate news_present=True; list mover_buy; 🔵; ret5=-0.5; leftover $1807.87 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ASML` | 1 | $1746.33 | $1.99 | — | $4,593.94 | — | union ∩ news_present, no 🚨; gate news_present=True; list mover_buy; 🔵; ret5=-4.4; leftover $1807.87 | join🟡 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `LRCX` | 5 | $314.61 | $2.00 | — | $3,018.88 | — | union ∩ news_present, no 🚨; gate news_present=True; list mover_buy; 🔵; ret5=-5.5; leftover $1807.87 | join🟡 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `NVDA` | 8 | $212.64 | $2.01 | — | $1,315.75 | — | union ∩ news_present, no 🚨; gate news_present=True; list mover_buy; 🔵; ret5=-4.6; leftover $1807.87 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,315.75 | ▼ close $10,810.09 vs 09:30 $10,881.61 (session -24.92) | 16:00 close · cash $1,315.75 · equity $10,810.09 vs 09:30 $10,881.61 (-71.52; session marks -24.92) · 6 name(s) marked open→close (per-name table). RRC×44 09:30 $40.72 → close $41.55 +36.52; ACMR×22 09:30 $80.97 → close $79.11 -40.92; MU×1 09:30 $925.74 → close $938.40 +12.66; ASML×1 09:30 $1746.33 → close $1745.64 -0.69; LRCX×5 09:30 $314.61 → close $312.88 -8.65; NVDA×8 09:30 $212.64 → close $209.66 -23.84 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,315.75 | ▲ 09:30 equity $11,026.23 vs yday $10,810.09 (+216.14) | 09:30 open · cash $1,315.75 (unchanged overnight, no fees) · equity $11,026.23 vs prior close $10,810.09 (+216.14) · 6 name(s) re-marked at the open (per-name table). RRC×44 yday $41.55 → 09:30 $41.44 -4.84; ACMR×22 yday $79.11 → 09:30 $81.65 +55.88; MU×1 yday $938.40 → 09:30 $967.01 +28.61; ASML×1 yday $1745.64 → 09:30 $1746.53 +0.89; LRCX×5 yday $312.88 → 09:30 $318.88 +30.00; NVDA×8 yday $209.66 → 09:30 $222.86 +105.60 | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 22 | $81.65 | $2.08 | $+10.82 | $3,109.97 | ▲ +10.82 after sell → book $11,024.15; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $967.01 | $2.01 | $+37.26 | $4,074.97 | ▲ +37.26 after sell → book $11,022.14; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ASML` | 1 | $1746.53 | $2.02 | $-3.81 | $5,819.48 | ▼ -3.81 after sell → book $11,020.12; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `LRCX` | 5 | $318.88 | $2.03 | $+17.32 | $7,411.85 | ▲ +17.32 after sell → book $11,018.09; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `NVDA` | 8 | $222.86 | $2.04 | $+77.71 | $9,192.69 | ▲ +77.71 after sell → book $11,016.05; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `CRK` | 91 | $14.42 | $2.26 | — | $7,878.21 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; ret5=+1.1; leftover $1313.24 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `MOS` | 54 | $24.00 | $2.15 | — | $6,580.06 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; ret5=+13.0; leftover $1313.24 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `SLI` | 505 | $2.60 | $6.51 | — | $5,260.54 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; ret5=+4.2; leftover $1313.24 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 9 | $144.70 | $2.02 | — | $3,956.23 | — | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1313.24 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BHVN` | 77 | $16.95 | $2.22 | — | $2,648.86 | — | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $1313.24 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 70 | $18.50 | $2.20 | — | $1,351.66 | — | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $1313.24 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 142 | $9.19 | $2.42 | — | $44.26 | — | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $1313.24 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $44.26 | ▲ close $11,064.59 vs 09:30 $11,026.23 (session +68.32) | 16:00 close · cash $44.26 · equity $11,064.59 vs 09:30 $11,026.23 (+38.36; session marks +68.32) · 8 name(s) marked open→close (per-name table). RRC×44 09:30 $41.44 → close $41.64 +8.80; CRK×91 09:30 $14.42 → close $14.62 +18.20; MOS×54 09:30 $24.00 → close $23.76 -12.96; SLI×505 09:30 $2.60 → close $2.64 +20.20; ANF×9 09:30 $144.70 → close $145.75 +9.45; BHVN×77 09:30 $16.95 → close $16.12 -63.91; BZ×70 09:30 $18.50 → close $18.00 -35.00; CAPR×142 09:30 $9.19 → close $10.06 +123.54 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $44.26 | ▼ 09:30 equity $10,847.80 vs yday $11,064.59 (-216.79) | 09:30 open · cash $44.26 (unchanged overnight, no fees) · equity $10,847.80 vs prior close $11,064.59 (-216.79) · 8 name(s) re-marked at the open (per-name table). RRC×44 yday $41.64 → 09:30 $41.11 -23.32; CRK×91 yday $14.62 → 09:30 $14.56 -5.46; MOS×54 yday $23.76 → 09:30 $23.75 -0.54; SLI×505 yday $2.64 → 09:30 $2.51 -65.65; ANF×9 yday $145.75 → 09:30 $148.67 +26.28; BHVN×77 yday $16.12 → 09:30 $15.44 -52.36; BZ×70 yday $18.00 → 09:30 $17.89 -7.70; CAPR×142 yday $10.06 → 09:30 $9.44 -88.04 | — |
| 2026-08-31 09:30 ET | **SELL** | `RRC` | 44 | $41.11 | $2.15 | $+12.89 | $1,850.95 | ▲ +12.89 after sell → book $10,845.65; vs 09:30 mark -2.15 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CRK` | 91 | $14.56 | $2.29 | $+8.19 | $3,173.63 | ▲ +8.19 after sell → book $10,843.37; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 54 | $23.75 | $2.17 | $-17.82 | $4,453.95 | ▼ -17.82 after sell → book $10,841.19; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SLI` | 505 | $2.51 | $6.61 | $-58.57 | $5,714.89 | ▼ -58.57 after sell → book $10,834.58; vs 09:30 mark -6.61 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 9 | $148.67 | $2.04 | $+31.68 | $7,050.89 | ▲ +31.68 after sell → book $10,832.55; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BHVN` | 77 | $15.44 | $2.24 | $-120.73 | $8,237.52 | ▼ -120.73 after sell → book $10,830.30; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BZ` | 70 | $17.89 | $2.22 | $-47.12 | $9,487.60 | ▼ -47.12 after sell → book $10,828.08; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 142 | $9.44 | $2.45 | $+30.63 | $10,825.63 | ▲ +30.63 after sell → book $10,825.63; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,825.63 | ▲ close $10,825.63 vs 09:30 $10,847.80 (session +0.00) | 16:00 close · cash $10,825.63 · no lots left · equity $10,825.63. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,825.63 | ▲ 09:30 equity $10,825.63 vs yday $10,825.63 (+0.00) | 09:30 open · cash $10,825.63 · no holdings · equity $10,825.63 vs prior close $10,825.63 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,825.63 | ▲ close $10,825.63 vs 09:30 $10,825.63 (session +0.00) | 16:00 close · cash $10,825.63 · no lots left · equity $10,825.63. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,825.63 | ▲ 09:30 equity $10,825.63 vs yday $10,825.63 (+0.00) | 09:30 open · cash $10,825.63 · no holdings · equity $10,825.63 vs prior close $10,825.63 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,825.63 | ▲ close $10,825.63 vs 09:30 $10,825.63 (session +0.00) | 16:00 close · cash $10,825.63 · no lots left · equity $10,825.63. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,825.63 | ▲ 09:30 equity $10,825.63 vs yday $10,825.63 (+0.00) | 09:30 open · cash $10,825.63 · no holdings · equity $10,825.63 vs prior close $10,825.63 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 27 | $49.76 | $2.07 | — | $9,480.04 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1353.20 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 32 | $41.31 | $2.09 | — | $8,156.03 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $1353.20 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 413 | $3.27 | $5.33 | — | $6,800.20 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1353.20 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 175 | $7.70 | $2.52 | — | $5,450.18 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $1353.20 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $125.94 | $2.02 | — | $4,188.76 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1353.20 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 1109 | $1.22 | $14.31 | — | $2,821.48 | — | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1353.20 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRVO` | 73 | $18.40 | $2.21 | — | $1,476.07 | — | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $1353.20 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 86 | $15.70 | $2.25 | — | $123.62 | — | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1353.20 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $123.62 | ▲ close $11,625.57 vs 09:30 $10,825.63 (session +832.72) | 16:00 close · cash $123.62 · equity $11,625.57 vs 09:30 $10,825.63 (+799.94; session marks +832.72) · 8 name(s) marked open→close (per-name table). ATRC×27 09:30 $49.76 → close $52.59 +76.41; HRMY×32 09:30 $41.31 → close $42.86 +49.60; CABA×413 09:30 $3.27 → close $3.57 +123.90; VSTM×175 09:30 $7.70 → close $8.02 +56.00; RVTY×10 09:30 $125.94 → close $130.94 +50.00; GPRO×1109 09:30 $1.22 → close $1.69 +521.23; FRVO×73 09:30 $18.40 → close $17.98 -30.66; CRK×86 09:30 $15.70 → close $15.54 -13.76 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $123.62 | ▲ 09:30 equity $11,790.51 vs yday $11,625.57 (+164.94) | 09:30 open · cash $123.62 (unchanged overnight, no fees) · equity $11,790.51 vs prior close $11,625.57 (+164.94) · 8 name(s) re-marked at the open (per-name table). ATRC×27 yday $52.59 → 09:30 $52.88 +7.83; HRMY×32 yday $42.86 → 09:30 $42.93 +2.24; CABA×413 yday $3.57 → 09:30 $3.63 +24.78; VSTM×175 yday $8.02 → 09:30 $8.03 +1.75; RVTY×10 yday $130.94 → 09:30 $132.45 +15.10; GPRO×1109 yday $1.69 → 09:30 $1.78 +99.81; FRVO×73 yday $17.98 → 09:30 $18.27 +21.17; CRK×86 yday $15.54 → 09:30 $15.45 -7.74 | — |
| 2026-09-04 09:30 ET | **SELL** | `HRMY` | 32 | $42.93 | $2.11 | $+47.65 | $1,495.27 | ▲ +47.65 after sell → book $11,788.40; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 175 | $8.03 | $2.56 | $+52.68 | $2,897.97 | ▲ +52.68 after sell → book $11,785.85; vs 09:30 mark -2.55 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 10 | $132.45 | $2.04 | $+61.04 | $4,220.43 | ▲ +61.04 after sell → book $11,783.81; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `FRVO` | 73 | $18.27 | $2.23 | $-13.93 | $5,551.90 | ▼ -13.93 after sell → book $11,781.57; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 86 | $15.45 | $2.27 | $-26.02 | $6,878.33 | ▼ -26.02 after sell → book $11,779.30; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ASND` | 5 | $266.94 | $2.00 | — | $5,541.63 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; ret5=+1.9; leftover $1375.67 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OSCR` | 44 | $30.65 | $2.12 | — | $4,190.90 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ret5=-2.2; leftover $1375.67 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `NVAX` | 132 | $10.41 | $2.39 | — | $2,814.40 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $1375.67 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BVS` | 94 | $14.50 | $2.27 | — | $1,449.13 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=+0.8; leftover $1375.67 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 705 | $1.95 | $9.09 | — | $65.28 | — | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $1375.67 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $65.28 | ▼ close $11,317.03 vs 09:30 $11,790.51 (session -444.39) | 16:00 close · cash $65.28 · equity $11,317.03 vs 09:30 $11,790.51 (-473.48; session marks -444.39) · 8 name(s) marked open→close (per-name table). ATRC×27 09:30 $52.88 → close $52.46 -11.34; CABA×413 09:30 $3.63 → close $3.48 -61.95; GPRO×1109 09:30 $1.78 → close $1.39 -432.51; ASND×5 09:30 $266.94 → close $271.12 +20.90; OSCR×44 09:30 $30.65 → close $32.24 +69.96; NVAX×132 09:30 $10.41 → close $10.34 -9.24; BVS×94 09:30 $14.50 → close $14.36 -13.16; BAK×705 09:30 $1.95 → close $1.94 -7.05 | — |

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
| `ATRC` | 27 | 2026-09-03 @ $49.76 | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1353.20 |
| `CABA` | 413 | 2026-09-03 @ $3.27 | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1353.20 |
| `GPRO` | 1109 | 2026-09-03 @ $1.22 | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1353.20 |
| `ASND` | 5 | 2026-09-04 @ $266.94 | union ∩ news_present, no 🚨; gate news_present=True; list flatten; ret5=+1.9; leftover $1375.67 |
| `OSCR` | 44 | 2026-09-04 @ $30.65 | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ret5=-2.2; leftover $1375.67 |
| `NVAX` | 132 | 2026-09-04 @ $10.41 | union ∩ news_present, no 🚨; gate news_present=True; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $1375.67 |
| `BVS` | 94 | 2026-09-04 @ $14.50 | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=+0.8; leftover $1375.67 |
| `BAK` | 705 | 2026-09-04 @ $1.95 | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $1375.67 |
