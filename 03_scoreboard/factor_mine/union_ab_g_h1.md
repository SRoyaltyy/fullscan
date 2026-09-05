# Factor mine action — `union_ab_g_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ ab_g, no 🚨

Cash book **+10.69%** ($11,069) · signal-only (no cash/fees) was +5.13%. Starts YES **16/17**. Fills 90 · skips 36 · realized $+847.04.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `ab=good` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $46.63.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-17 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-18 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `AG` | 60 | — | $20.55 | +0.00 | $21.19 | +38.40 | +38.40 | +0.00 | +38.40 |
| 2026-08-20 | `BHP` | 13 | — | $91.01 | +0.00 | $93.63 | +34.06 | +34.06 | +0.00 | +34.06 |
| 2026-08-20 | `CDE` | 60 | — | $20.65 | +0.00 | $21.11 | +27.60 | +27.60 | +0.00 | +27.60 |
| 2026-08-20 | `HDSN` | 216 | — | $5.77 | +0.00 | $5.57 | -43.20 | -43.20 | +0.00 | -43.20 |
| 2026-08-20 | `IAG` | 63 | — | $19.63 | +0.00 | $20.50 | +54.81 | +54.81 | +0.00 | +54.81 |
| 2026-08-20 | `KGC` | 42 | — | $29.63 | +0.00 | $31.43 | +75.60 | +75.60 | +0.00 | +75.60 |
| 2026-08-20 | `NFGC` | 714 | — | $1.75 | +0.00 | $1.75 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `WPM` | 8 | — | $144.54 | +0.00 | $150.25 | +45.68 | +45.68 | +0.00 | +45.68 |
| 2026-08-21 | `AG` | 60 | $21.19 | $21.90 | +42.60 | — | +0.00 | +42.60 | +81.00 | — |
| 2026-08-21 | `BHP` | 13 | $93.63 | $95.72 | +27.17 | — | +0.00 | +27.17 | +61.23 | — |
| 2026-08-21 | `CDE` | 60 | $21.11 | $21.75 | +38.40 | — | +0.00 | +38.40 | +66.00 | — |
| 2026-08-21 | `HDSN` | 216 | $5.57 | $5.67 | +21.60 | — | +0.00 | +21.60 | -21.60 | — |
| 2026-08-21 | `IAG` | 63 | $20.50 | $21.17 | +42.21 | — | +0.00 | +42.21 | +97.02 | — |
| 2026-08-21 | `KGC` | 42 | $31.43 | $32.17 | +31.08 | — | +0.00 | +31.08 | +106.68 | — |
| 2026-08-21 | `NFGC` | 714 | $1.75 | $1.79 | +28.56 | — | +0.00 | +28.56 | +28.56 | — |
| 2026-08-21 | `WPM` | 8 | $150.25 | $154.70 | +35.60 | — | +0.00 | +35.60 | +81.28 | — |
| 2026-08-21 | `AU` | 10 | — | $119.43 | +0.00 | $121.22 | +17.90 | +17.90 | +0.00 | +17.90 |
| 2026-08-21 | `AUPH` | 75 | — | $17.20 | +0.00 | $16.65 | -41.25 | -41.25 | +0.00 | -41.25 |
| 2026-08-21 | `AEM` | 6 | — | $216.30 | +0.00 | $216.06 | -1.44 | -1.44 | +0.00 | -1.44 |
| 2026-08-21 | `ARCT` | 117 | — | $11.13 | +0.00 | $13.45 | +271.44 | +271.44 | +0.00 | +271.44 |
| 2026-08-21 | `AUTL` | 528 | — | $2.47 | +0.00 | $2.41 | -31.68 | -31.68 | +0.00 | -31.68 |
| 2026-08-21 | `CRDL` | 676 | — | $1.93 | +0.00 | $1.86 | -47.32 | -47.32 | +0.00 | -47.32 |
| 2026-08-21 | `CRSP` | 21 | — | $59.72 | +0.00 | $59.50 | -4.62 | -4.62 | +0.00 | -4.62 |
| 2026-08-21 | `CYPH` | 989 | — | $1.32 | +0.00 | $1.42 | +98.90 | +98.90 | +0.00 | +98.90 |
| 2026-08-24 | `AU` | 10 | $121.22 | $120.50 | -7.20 | — | +0.00 | -7.20 | +10.70 | — |
| 2026-08-24 | `AUPH` | 75 | $16.65 | $16.60 | -3.75 | — | +0.00 | -3.75 | -45.00 | — |
| 2026-08-24 | `AEM` | 6 | $216.06 | $217.03 | +5.82 | — | +0.00 | +5.82 | +4.38 | — |
| 2026-08-24 | `ARCT` | 117 | $13.45 | $13.26 | -22.23 | — | +0.00 | -22.23 | +249.21 | — |
| 2026-08-24 | `AUTL` | 528 | $2.41 | $2.36 | -26.40 | — | +0.00 | -26.40 | -58.08 | — |
| 2026-08-24 | `CRDL` | 676 | $1.86 | $1.87 | +6.76 | — | +0.00 | +6.76 | -40.56 | — |
| 2026-08-24 | `CRSP` | 21 | $59.50 | $58.79 | -14.91 | — | +0.00 | -14.91 | -19.53 | — |
| 2026-08-24 | `CYPH` | 989 | $1.42 | $1.83 | +405.49 | — | +0.00 | +405.49 | +504.39 | — |
| 2026-08-25 | `MOS` | 57 | — | $24.00 | +0.00 | $23.75 | -14.25 | -14.25 | +0.00 | -14.25 |
| 2026-08-25 | `OCUL` | 125 | — | $10.92 | +0.00 | $10.92 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-25 | `INSP` | 22 | — | $61.47 | +0.00 | $61.47 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-25 | `CRMD` | 165 | — | $8.28 | +0.00 | $8.28 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-25 | `RZLT` | 262 | — | $5.23 | +0.00 | $5.29 | +15.72 | +15.72 | +0.00 | +15.72 |
| 2026-08-25 | `HCA` | 3 | — | $429.24 | +0.00 | $428.50 | -2.22 | -2.22 | +0.00 | -2.22 |
| 2026-08-25 | `BMEA` | 847 | — | $1.62 | +0.00 | $1.61 | -8.47 | -8.47 | +0.00 | -8.47 |
| 2026-08-25 | `ALVO` | 262 | — | $5.22 | +0.00 | $5.25 | +7.86 | +7.86 | +0.00 | +7.86 |
| 2026-08-26 | `MOS` | 57 | $23.75 | $23.75 | +0.00 | $23.75 | +0.00 | +0.00 | -14.25 | -14.25 |
| 2026-08-26 | `OCUL` | 125 | $10.92 | $10.92 | +0.00 | $10.92 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-26 | `INSP` | 22 | $61.47 | $61.47 | +0.00 | $61.47 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-26 | `CRMD` | 165 | $8.28 | $8.28 | +0.00 | $8.28 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-26 | `RZLT` | 262 | $5.29 | $5.29 | +0.00 | $5.29 | +0.00 | +0.00 | +15.72 | +15.72 |
| 2026-08-26 | `HCA` | 3 | $428.50 | $428.50 | +0.00 | $428.50 | +0.00 | +0.00 | -2.22 | -2.22 |
| 2026-08-26 | `BMEA` | 847 | $1.61 | $1.61 | +0.00 | $1.61 | +0.00 | +0.00 | -8.47 | -8.47 |
| 2026-08-26 | `ALVO` | 262 | $5.25 | $5.25 | +0.00 | $5.25 | +0.00 | +0.00 | +7.86 | +7.86 |
| 2026-08-27 | `MOS` | 57 | $23.75 | $24.84 | +62.13 | $24.16 | -38.76 | +23.37 | +47.88 | +9.12 |
| 2026-08-27 | `OCUL` | 125 | $10.92 | $10.79 | -16.25 | — | +0.00 | -16.25 | -16.25 | — |
| 2026-08-27 | `INSP` | 22 | $61.47 | $60.07 | -30.80 | — | +0.00 | -30.80 | -30.80 | — |
| 2026-08-27 | `CRMD` | 165 | $8.28 | $8.60 | +52.80 | — | +0.00 | +52.80 | +52.80 | — |
| 2026-08-27 | `RZLT` | 262 | $5.29 | $5.01 | -73.36 | — | +0.00 | -73.36 | -57.64 | — |
| 2026-08-27 | `HCA` | 3 | $428.50 | $427.50 | -3.00 | — | +0.00 | -3.00 | -5.22 | — |
| 2026-08-27 | `BMEA` | 847 | $1.61 | $1.75 | +118.58 | — | +0.00 | +118.58 | +110.11 | — |
| 2026-08-27 | `ALVO` | 262 | $5.25 | $4.98 | -70.74 | — | +0.00 | -70.74 | -62.88 | — |
| 2026-08-27 | `RRC` | 33 | — | $40.72 | +0.00 | $41.55 | +27.39 | +27.39 | +0.00 | +27.39 |
| 2026-08-27 | `CRK` | 96 | — | $14.09 | +0.00 | $14.50 | +39.36 | +39.36 | +0.00 | +39.36 |
| 2026-08-27 | `SLI` | 526 | — | $2.59 | +0.00 | $2.61 | +10.52 | +10.52 | +0.00 | +10.52 |
| 2026-08-27 | `ACMR` | 16 | — | $80.97 | +0.00 | $79.11 | -29.76 | -29.76 | +0.00 | -29.76 |
| 2026-08-27 | `GGB` | 308 | — | $4.42 | +0.00 | $4.46 | +12.32 | +12.32 | +0.00 | +12.32 |
| 2026-08-27 | `MT` | 18 | — | $75.12 | +0.00 | $74.53 | -10.62 | -10.62 | +0.00 | -10.62 |
| 2026-08-27 | `MU` | 1 | — | $925.74 | +0.00 | $938.40 | +12.66 | +12.66 | +0.00 | +12.66 |
| 2026-08-28 | `MOS` | 57 | $24.16 | $24.00 | -9.12 | $23.76 | -13.68 | -22.80 | +0.00 | -13.68 |
| 2026-08-28 | `RRC` | 33 | $41.55 | $41.44 | -3.63 | $41.64 | +6.60 | +2.97 | +23.76 | +30.36 |
| 2026-08-28 | `CRK` | 96 | $14.50 | $14.42 | -7.68 | $14.62 | +19.20 | +11.52 | +31.68 | +50.88 |
| 2026-08-28 | `SLI` | 526 | $2.61 | $2.60 | -5.26 | $2.64 | +21.04 | +15.78 | +5.26 | +26.30 |
| 2026-08-28 | `ACMR` | 16 | $79.11 | $81.65 | +40.64 | — | +0.00 | +40.64 | +10.88 | — |
| 2026-08-28 | `GGB` | 308 | $4.46 | $4.57 | +33.88 | — | +0.00 | +33.88 | +46.20 | — |
| 2026-08-28 | `MT` | 18 | $74.53 | $74.54 | +0.18 | — | +0.00 | +0.18 | -10.44 | — |
| 2026-08-28 | `MU` | 1 | $938.40 | $967.01 | +28.61 | — | +0.00 | +28.61 | +41.27 | — |
| 2026-08-28 | `ANF` | 9 | — | $144.70 | +0.00 | $145.75 | +9.45 | +9.45 | +0.00 | +9.45 |
| 2026-08-28 | `BZ` | 74 | — | $18.50 | +0.00 | $18.00 | -37.00 | -37.00 | +0.00 | -37.00 |
| 2026-08-28 | `SMTC` | 9 | — | $149.40 | +0.00 | $142.43 | -62.73 | -62.73 | +0.00 | -62.73 |
| 2026-08-28 | `GRRR` | 86 | — | $15.94 | +0.00 | $15.66 | -24.08 | -24.08 | +0.00 | -24.08 |
| 2026-08-31 | `MOS` | 57 | $23.76 | $23.75 | -0.57 | — | +0.00 | -0.57 | -14.25 | — |
| 2026-08-31 | `RRC` | 33 | $41.64 | $41.11 | -17.49 | — | +0.00 | -17.49 | +12.87 | — |
| 2026-08-31 | `CRK` | 96 | $14.62 | $14.56 | -5.76 | — | +0.00 | -5.76 | +45.12 | — |
| 2026-08-31 | `SLI` | 526 | $2.64 | $2.51 | -68.38 | — | +0.00 | -68.38 | -42.08 | — |
| 2026-08-31 | `ANF` | 9 | $145.75 | $148.67 | +26.28 | — | +0.00 | +26.28 | +35.73 | — |
| 2026-08-31 | `BZ` | 74 | $18.00 | $17.89 | -8.14 | — | +0.00 | -8.14 | -45.14 | — |
| 2026-08-31 | `SMTC` | 9 | $142.43 | $133.04 | -84.51 | — | +0.00 | -84.51 | -147.24 | — |
| 2026-08-31 | `GRRR` | 86 | $15.66 | $14.32 | -115.24 | — | +0.00 | -115.24 | -139.32 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `ATRC` | 26 | — | $49.76 | +0.00 | $52.59 | +73.58 | +73.58 | +0.00 | +73.58 |
| 2026-09-03 | `HRMY` | 32 | — | $41.31 | +0.00 | $42.86 | +49.60 | +49.60 | +0.00 | +49.60 |
| 2026-09-03 | `CABA` | 406 | — | $3.27 | +0.00 | $3.57 | +121.80 | +121.80 | +0.00 | +121.80 |
| 2026-09-03 | `VSTM` | 172 | — | $7.70 | +0.00 | $8.02 | +55.04 | +55.04 | +0.00 | +55.04 |
| 2026-09-03 | `RVTY` | 10 | — | $125.94 | +0.00 | $130.94 | +50.00 | +50.00 | +0.00 | +50.00 |
| 2026-09-03 | `CRK` | 84 | — | $15.70 | +0.00 | $15.54 | -13.44 | -13.44 | +0.00 | -13.44 |
| 2026-09-03 | `MMED` | 58 | — | $22.78 | +0.00 | $23.76 | +56.84 | +56.84 | +0.00 | +56.84 |
| 2026-09-03 | `SLN` | 90 | — | $14.70 | +0.00 | $14.79 | +8.10 | +8.10 | +0.00 | +8.10 |
| 2026-09-04 | `ATRC` | 26 | $52.59 | $52.88 | +7.54 | $52.46 | -10.92 | -3.38 | +81.12 | +70.20 |
| 2026-09-04 | `HRMY` | 32 | $42.86 | $42.93 | +2.24 | — | +0.00 | +2.24 | +51.84 | — |
| 2026-09-04 | `CABA` | 406 | $3.57 | $3.63 | +24.36 | $3.48 | -60.90 | -36.54 | +146.16 | +85.26 |
| 2026-09-04 | `VSTM` | 172 | $8.02 | $8.03 | +1.72 | — | +0.00 | +1.72 | +56.76 | — |
| 2026-09-04 | `RVTY` | 10 | $130.94 | $132.45 | +15.10 | — | +0.00 | +15.10 | +65.10 | — |
| 2026-09-04 | `CRK` | 84 | $15.54 | $15.45 | -7.56 | — | +0.00 | -7.56 | -21.00 | — |
| 2026-09-04 | `MMED` | 58 | $23.76 | $23.88 | +6.96 | — | +0.00 | +6.96 | +63.80 | — |
| 2026-09-04 | `SLN` | 90 | $14.79 | $14.85 | +5.40 | — | +0.00 | +5.40 | +13.50 | — |
| 2026-09-04 | `ASND` | 5 | — | $266.94 | +0.00 | $271.12 | +20.90 | +20.90 | +0.00 | +20.90 |
| 2026-09-04 | `OSCR` | 44 | — | $30.65 | +0.00 | $32.24 | +69.96 | +69.96 | +0.00 | +69.96 |
| 2026-09-04 | `NVAX` | 131 | — | $10.41 | +0.00 | $10.34 | -9.17 | -9.17 | +0.00 | -9.17 |
| 2026-09-04 | `BVS` | 94 | — | $14.50 | +0.00 | $14.36 | -13.16 | -13.16 | +0.00 | -13.16 |
| 2026-09-04 | `BAK` | 702 | — | $1.95 | +0.00 | $1.94 | -7.02 | -7.02 | +0.00 | -7.02 |
| 2026-09-04 | `SLBT` | 446 | — | $3.07 | +0.00 | $3.15 | +35.68 | +35.68 | +0.00 | +35.68 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-17 | +2.25 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-18 | -6.20 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-19 | -7.20 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-20 | +1.12 | $10,000.00 | — | $10,000.00 | +0.00 | +232.95 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | — | $186.91 | $10,208.28 | AG×60, BHP×13, CDE×60, HDSN×216, IAG×63, KGC×42, NFGC×714, WPM×8 |
| 2026-08-21 | +3.25 | $186.91 | AG×60, BHP×13, CDE×60, HDSN×216, IAG×63, KGC×42, NFGC×714, WPM×8 | $10,475.50 | +267.22 | +261.93 | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $158.85 | $10,673.53 | AU×10, AUPH×75, AEM×6, ARCT×117, AUTL×528, CRDL×676, CRSP×21, CYPH×989 |
| 2026-08-24 | -5.17 | $158.85 | AU×10, AUPH×75, AEM×6, ARCT×117, AUTL×528, CRDL×676, CRSP×21, CYPH×989 | $11,017.11 | +343.58 | +0.00 | — | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | $10,977.67 | $10,977.67 | — |
| 2026-08-25 | +1.80 | $10,977.67 | — | $10,977.67 | -0.00 | -1.36 | MOS, OCUL, INSP, CRMD, RZLT, HCA, BMEA, ALVO | — | $99.61 | $10,947.55 | MOS×57, OCUL×125, INSP×22, CRMD×165, RZLT×262, HCA×3, BMEA×847, ALVO×262 |
| 2026-08-26 | +2.02 | $99.61 | MOS×57, OCUL×125, INSP×22, CRMD×165, RZLT×262, HCA×3, BMEA×847, ALVO×262 | $10,947.55 | +0.00 | +0.00 | — | — | $99.61 | $10,947.55 | MOS×57, OCUL×125, INSP×22, CRMD×165, RZLT×262, HCA×3, BMEA×847, ALVO×262 |
| 2026-08-27 | — | $99.61 | MOS×57, OCUL×125, INSP×22, CRMD×165, RZLT×262, HCA×3, BMEA×847, ALVO×262 | $10,986.91 | +39.36 | +23.11 | RRC, CRK, SLI, ACMR, GGB, MT, MU | OCUL, INSP, CRMD, RZLT, HCA, BMEA, ALVO | $529.35 | $10,961.86 | MOS×57, RRC×33, CRK×96, SLI×526, ACMR×16, GGB×308, MT×18, MU×1 |
| 2026-08-28 | +0.75 | $529.35 | MOS×57, RRC×33, CRK×96, SLI×526, ACMR×16, GGB×308, MT×18, MU×1 | $11,039.48 | +77.62 | -81.20 | ANF, BZ, SMTC, GRRR | ACMR, GGB, MT, MU | $146.64 | $10,939.62 | MOS×57, RRC×33, CRK×96, SLI×526, ANF×9, BZ×74, SMTC×9, GRRR×86 |
| 2026-08-31 | -5.85 | $146.64 | MOS×57, RRC×33, CRK×96, SLI×526, ANF×9, BZ×74, SMTC×9, GRRR×86 | $10,665.81 | -273.81 | +0.00 | — | MOS, RRC, CRK, SLI, ANF, BZ, SMTC, GRRR | $10,643.74 | $10,643.74 | — |
| 2026-09-01 | -6.30 | $10,643.74 | — | $10,643.74 | +0.00 | +0.00 | — | — | $10,643.74 | $10,643.74 | — |
| 2026-09-02 | -3.83 | $10,643.74 | — | $10,643.74 | +0.00 | +0.00 | — | — | $10,643.74 | $10,643.74 | — |
| 2026-09-03 | -0.90 | $10,643.74 | — | $10,643.74 | +0.00 | +401.52 | ATRC, HRMY, CABA, VSTM, RVTY, CRK, MMED, SLN | — | $133.02 | $11,024.68 | ATRC×26, HRMY×32, CABA×406, VSTM×172, RVTY×10, CRK×84, MMED×58, SLN×90 |
| 2026-09-04 | — | $133.02 | ATRC×26, HRMY×32, CABA×406, VSTM×172, RVTY×10, CRK×84, MMED×58, SLN×90 | $11,080.44 | +55.76 | +25.37 | ASND, OSCR, NVAX, BVS, BAK, SLBT | HRMY, VSTM, RVTY, CRK, MMED, SLN | $46.63 | $11,068.79 | ATRC×26, CABA×406, ASND×5, OSCR×44, NVAX×131, BVS×94, BAK×702, SLBT×446 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 60 | $20.55 | $2.17 | — | $8,764.83 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,579.67 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 60 | $20.65 | $2.17 | — | $6,338.50 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 216 | $5.77 | $2.79 | — | $5,089.39 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 63 | $19.63 | $2.18 | — | $3,850.53 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 42 | $29.63 | $2.12 | — | $2,603.95 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 714 | $1.75 | $9.21 | — | $1,345.24 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $186.91 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1250.00 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $186.91 | ▲ close $10,208.28 vs 09:30 $10,000.00 (session +232.95) | 16:00 close · cash $186.91 · equity $10,208.28 vs 09:30 $10,000.00 (+208.28; session marks +232.95) · 8 name(s) marked open→close (per-name table). AG×60 09:30 $20.55 → close $21.19 +38.40; BHP×13 09:30 $91.01 → close $93.63 +34.06; CDE×60 09:30 $20.65 → close $21.11 +27.60; HDSN×216 09:30 $5.77 → close $5.57 -43.20; IAG×63 09:30 $19.63 → close $20.50 +54.81; KGC×42 09:30 $29.63 → close $31.43 +75.60; NFGC×714 09:30 $1.75 → close $1.75 +0.00; WPM×8 09:30 $144.54 → close $150.25 +45.68 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $186.91 | ▲ 09:30 equity $10,475.50 vs yday $10,208.28 (+267.22) | 09:30 open · cash $186.91 (unchanged overnight, no fees) · equity $10,475.50 vs prior close $10,208.28 (+267.22) · 8 name(s) re-marked at the open (per-name table). AG×60 yday $21.19 → 09:30 $21.90 +42.60; BHP×13 yday $93.63 → 09:30 $95.72 +27.17; CDE×60 yday $21.11 → 09:30 $21.75 +38.40; HDSN×216 yday $5.57 → 09:30 $5.67 +21.60; IAG×63 yday $20.50 → 09:30 $21.17 +42.21; KGC×42 yday $31.43 → 09:30 $32.17 +31.08; NFGC×714 yday $1.75 → 09:30 $1.79 +28.56; WPM×8 yday $150.25 → 09:30 $154.70 +35.60 | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 60 | $21.90 | $2.19 | $+76.64 | $1,498.71 | ▲ +76.64 after sell → book $10,473.30; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $2,741.03 | ▲ +57.15 after sell → book $10,471.26; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 60 | $21.75 | $2.19 | $+61.64 | $4,043.84 | ▲ +61.64 after sell → book $10,469.07; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 216 | $5.67 | $2.83 | $-27.22 | $5,265.72 | ▼ -27.22 after sell → book $10,466.23; vs 09:30 mark -2.84 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 63 | $21.17 | $2.20 | $+92.64 | $6,597.23 | ▲ +92.64 after sell → book $10,464.03; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 42 | $32.17 | $2.14 | $+102.43 | $7,946.24 | ▲ +102.43 after sell → book $10,461.90; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 714 | $1.79 | $9.34 | $+10.01 | $9,214.96 | ▲ +10.01 after sell → book $10,452.56; vs 09:30 mark -9.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 8 | $154.70 | $2.03 | $+77.23 | $10,450.52 | ▲ +77.23 after sell → book $10,450.52; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $9,254.20 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+20.4; leftover $1306.32 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 75 | $17.20 | $2.21 | — | $7,961.99 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $1306.32 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 6 | $216.30 | $2.01 | — | $6,662.18 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1306.32 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 117 | $11.13 | $2.34 | — | $5,357.63 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1306.32 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 528 | $2.47 | $6.81 | — | $4,046.66 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $1306.32 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 676 | $1.93 | $8.72 | — | $2,733.26 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $1306.32 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 21 | $59.72 | $2.05 | — | $1,477.08 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1306.32 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 989 | $1.32 | $12.76 | — | $158.85 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1306.32 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $158.85 | ▲ close $10,673.53 vs 09:30 $10,475.50 (session +261.93) | 16:00 close · cash $158.85 · equity $10,673.53 vs 09:30 $10,475.50 (+198.03; session marks +261.93) · 8 name(s) marked open→close (per-name table). AU×10 09:30 $119.43 → close $121.22 +17.90; AUPH×75 09:30 $17.20 → close $16.65 -41.25; AEM×6 09:30 $216.30 → close $216.06 -1.44; ARCT×117 09:30 $11.13 → close $13.45 +271.44; AUTL×528 09:30 $2.47 → close $2.41 -31.68; CRDL×676 09:30 $1.93 → close $1.86 -47.32; CRSP×21 09:30 $59.72 → close $59.50 -4.62; CYPH×989 09:30 $1.32 → close $1.42 +98.90 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $158.85 | ▲ 09:30 equity $11,017.11 vs yday $10,673.53 (+343.58) | 09:30 open · cash $158.85 (unchanged overnight, no fees) · equity $11,017.11 vs prior close $10,673.53 (+343.58) · 8 name(s) re-marked at the open (per-name table). AU×10 yday $121.22 → 09:30 $120.50 -7.20; AUPH×75 yday $16.65 → 09:30 $16.60 -3.75; AEM×6 yday $216.06 → 09:30 $217.03 +5.82; ARCT×117 yday $13.45 → 09:30 $13.26 -22.23; AUTL×528 yday $2.41 → 09:30 $2.36 -26.40; CRDL×676 yday $1.86 → 09:30 $1.87 +6.76; CRSP×21 yday $59.50 → 09:30 $58.79 -14.91; CYPH×989 yday $1.42 → 09:30 $1.83 +405.49 | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.50 | $2.04 | $+6.64 | $1,361.81 | ▲ +6.64 after sell → book $11,015.07; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 75 | $16.60 | $2.24 | $-49.45 | $2,604.57 | ▼ -49.45 after sell → book $11,012.83; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 6 | $217.03 | $2.03 | $+0.34 | $3,904.72 | ▲ +0.34 after sell → book $11,010.80; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 117 | $13.26 | $2.37 | $+244.50 | $5,453.77 | ▲ +244.50 after sell → book $11,008.43; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 528 | $2.36 | $6.91 | $-71.80 | $6,692.94 | ▼ -71.80 after sell → book $11,001.52; vs 09:30 mark -6.91 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRDL` | 676 | $1.87 | $8.84 | $-58.12 | $7,948.22 | ▼ -58.12 after sell → book $10,992.68; vs 09:30 mark -8.84 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 21 | $58.79 | $2.07 | $-23.66 | $9,180.73 | ▼ -23.66 after sell → book $10,990.60; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 989 | $1.83 | $12.94 | $+478.70 | $10,977.67 | ▲ +478.70 after sell → book $10,977.67; vs 09:30 mark -12.93 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,977.67 | ▲ close $10,977.67 vs 09:30 $11,017.11 (session +0.00) | 16:00 close · cash $10,977.67 · no lots left · equity $10,977.67. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,977.67 | ▲ 09:30 equity $10,977.67 vs yday $10,977.67 (-0.00) | 09:30 open · cash $10,977.67 · no holdings · equity $10,977.67 vs prior close $10,977.67 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 57 | $24.00 | $2.16 | — | $9,607.50 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ⚪; ret5=+13.0; leftover $1372.21 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 125 | $10.92 | $2.37 | — | $8,240.14 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ret5=+10.4; leftover $1372.21 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 22 | $61.47 | $2.06 | — | $6,885.74 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ret5=+9.2; leftover $1372.21 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 165 | $8.28 | $2.48 | — | $5,517.06 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ⚪; ret5=+8.8; leftover $1372.21 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 262 | $5.23 | $3.38 | — | $4,143.42 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ret5=+10.7; leftover $1372.21 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 3 | $429.24 | $2.00 | — | $2,853.70 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ret5=+6.1; leftover $1372.21 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 847 | $1.62 | $10.93 | — | $1,470.63 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $1372.21 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 262 | $5.22 | $3.38 | — | $99.61 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+9.2; leftover $1372.21 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $99.61 | ▼ close $10,947.55 vs 09:30 $10,977.67 (session -1.36) | 16:00 close · cash $99.61 · equity $10,947.55 vs 09:30 $10,977.67 (-30.12; session marks -1.36) · 8 name(s) marked open→close (per-name table). MOS×57 09:30 $24.00 → close $23.75 -14.25; OCUL×125 09:30 $10.92 → close $10.92 +0.00; INSP×22 09:30 $61.47 → close $61.47 +0.00; CRMD×165 09:30 $8.28 → close $8.28 +0.00; RZLT×262 09:30 $5.23 → close $5.29 +15.72; HCA×3 09:30 $429.24 → close $428.50 -2.22; BMEA×847 09:30 $1.62 → close $1.61 -8.47; ALVO×262 09:30 $5.22 → close $5.25 +7.86 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $99.61 | ▲ 09:30 equity $10,947.55 vs yday $10,947.55 (+0.00) | 09:30 open · cash $99.61 (unchanged overnight, no fees) · equity $10,947.55 vs prior close $10,947.55 (+0.00) · 8 name(s) re-marked at the open (per-name table). MOS×57 yday $23.75 → 09:30 $23.75 +0.00; OCUL×125 yday $10.92 → 09:30 $10.92 +0.00; INSP×22 yday $61.47 → 09:30 $61.47 +0.00; CRMD×165 yday $8.28 → 09:30 $8.28 +0.00; RZLT×262 yday $5.29 → 09:30 $5.29 +0.00; HCA×3 yday $428.50 → 09:30 $428.50 +0.00; BMEA×847 yday $1.61 → 09:30 $1.61 +0.00; ALVO×262 yday $5.25 → 09:30 $5.25 +0.00 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $99.61 | ▲ close $10,947.55 vs 09:30 $10,947.55 (session +0.00) | 16:00 close · cash $99.61 · equity $10,947.55 vs 09:30 $10,947.55 (+0.00; session marks +0.00) · 8 name(s) marked open→close (per-name table). MOS×57 09:30 $23.75 → close $23.75 +0.00; OCUL×125 09:30 $10.92 → close $10.92 +0.00; INSP×22 09:30 $61.47 → close $61.47 +0.00; CRMD×165 09:30 $8.28 → close $8.28 +0.00; RZLT×262 09:30 $5.29 → close $5.29 +0.00; HCA×3 09:30 $428.50 → close $428.50 +0.00; BMEA×847 09:30 $1.61 → close $1.61 +0.00; ALVO×262 09:30 $5.25 → close $5.25 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $99.61 | ▲ 09:30 equity $10,986.91 vs yday $10,947.55 (+39.36) | 09:30 open · cash $99.61 (unchanged overnight, no fees) · equity $10,986.91 vs prior close $10,947.55 (+39.36) · 8 name(s) re-marked at the open (per-name table). MOS×57 yday $23.75 → 09:30 $24.84 +62.13; OCUL×125 yday $10.92 → 09:30 $10.79 -16.25; INSP×22 yday $61.47 → 09:30 $60.07 -30.80; CRMD×165 yday $8.28 → 09:30 $8.60 +52.80; RZLT×262 yday $5.29 → 09:30 $5.01 -73.36; HCA×3 yday $428.50 → 09:30 $427.50 -3.00; BMEA×847 yday $1.61 → 09:30 $1.75 +118.58; ALVO×262 yday $5.25 → 09:30 $4.98 -70.74 | — |
| 2026-08-27 09:30 ET | **SELL** | `OCUL` | 125 | $10.79 | $2.40 | $-21.01 | $1,445.97 | ▼ -21.01 after sell → book $10,984.52; vs 09:30 mark -2.39 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `INSP` | 22 | $60.07 | $2.08 | $-34.93 | $2,765.43 | ▼ -34.93 after sell → book $10,982.44; vs 09:30 mark -2.08 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRMD` | 165 | $8.60 | $2.52 | $+47.79 | $4,181.91 | ▲ +47.79 after sell → book $10,979.92; vs 09:30 mark -2.52 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `RZLT` | 262 | $5.01 | $3.43 | $-64.45 | $5,491.09 | ▼ -64.45 after sell → book $10,976.48; vs 09:30 mark -3.44 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `HCA` | 3 | $427.50 | $2.02 | $-9.24 | $6,771.57 | ▼ -9.24 after sell → book $10,974.46; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BMEA` | 847 | $1.75 | $11.08 | $+88.10 | $8,242.75 | ▲ +88.10 after sell → book $10,963.39; vs 09:30 mark -11.07 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ALVO` | 262 | $4.98 | $3.43 | $-69.69 | $9,544.07 | ▼ -69.69 after sell → book $10,959.95; vs 09:30 mark -3.44 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 33 | $40.72 | $2.09 | — | $8,198.22 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ret5=+1.8; leftover $1363.44 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 96 | $14.09 | $2.28 | — | $6,843.30 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ret5=+1.1; leftover $1363.44 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 526 | $2.59 | $6.79 | — | $5,474.18 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ret5=+4.2; leftover $1363.44 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 16 | $80.97 | $2.04 | — | $4,176.62 | — | union ∩ ab_g, no 🚨; gate ab=good; list mover_buy; 🔵; ret5=-1.3; leftover $1363.44 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GGB` | 308 | $4.42 | $3.97 | — | $2,811.29 | — | union ∩ ab_g, no 🚨; gate ab=good; list mover_buy; 🔵; ret5=-8.6; leftover $1363.44 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MT` | 18 | $75.12 | $2.04 | — | $1,457.08 | — | union ∩ ab_g, no 🚨; gate ab=good; list mover_buy; 🔵; ret5=-2.2; leftover $1363.44 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $925.74 | $1.99 | — | $529.35 | — | union ∩ ab_g, no 🚨; gate ab=good; list mover_buy; 🔵; ret5=-0.5; leftover $1363.44 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $529.35 | ▲ close $10,961.86 vs 09:30 $10,986.91 (session +23.11) | 16:00 close · cash $529.35 · equity $10,961.86 vs 09:30 $10,986.91 (-25.05; session marks +23.11) · 8 name(s) marked open→close (per-name table). MOS×57 09:30 $24.84 → close $24.16 -38.76; RRC×33 09:30 $40.72 → close $41.55 +27.39; CRK×96 09:30 $14.09 → close $14.50 +39.36; SLI×526 09:30 $2.59 → close $2.61 +10.52; ACMR×16 09:30 $80.97 → close $79.11 -29.76; GGB×308 09:30 $4.42 → close $4.46 +12.32; MT×18 09:30 $75.12 → close $74.53 -10.62; MU×1 09:30 $925.74 → close $938.40 +12.66 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $529.35 | ▲ 09:30 equity $11,039.48 vs yday $10,961.86 (+77.62) | 09:30 open · cash $529.35 (unchanged overnight, no fees) · equity $11,039.48 vs prior close $10,961.86 (+77.62) · 8 name(s) re-marked at the open (per-name table). MOS×57 yday $24.16 → 09:30 $24.00 -9.12; RRC×33 yday $41.55 → 09:30 $41.44 -3.63; CRK×96 yday $14.50 → 09:30 $14.42 -7.68; SLI×526 yday $2.61 → 09:30 $2.60 -5.26; ACMR×16 yday $79.11 → 09:30 $81.65 +40.64; GGB×308 yday $4.46 → 09:30 $4.57 +33.88; MT×18 yday $74.53 → 09:30 $74.54 +0.18; MU×1 yday $938.40 → 09:30 $967.01 +28.61 | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 16 | $81.65 | $2.06 | $+6.78 | $1,833.69 | ▲ +6.78 after sell → book $11,037.42; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GGB` | 308 | $4.57 | $4.04 | $+38.19 | $3,237.22 | ▲ +38.19 after sell → book $11,033.39; vs 09:30 mark -4.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MT` | 18 | $74.54 | $2.06 | $-14.55 | $4,576.87 | ▼ -14.55 after sell → book $11,031.32; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $967.01 | $2.01 | $+37.26 | $5,541.87 | ▲ +37.26 after sell → book $11,029.31; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 9 | $144.70 | $2.02 | — | $4,237.55 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1385.47 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 74 | $18.50 | $2.21 | — | $2,866.34 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $1385.47 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 9 | $149.40 | $2.02 | — | $1,519.72 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=-11.6; leftover $1385.47 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 86 | $15.94 | $2.25 | — | $146.64 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-1.9; leftover $1385.47 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $146.64 | ▼ close $10,939.62 vs 09:30 $11,039.48 (session -81.20) | 16:00 close · cash $146.64 · equity $10,939.62 vs 09:30 $11,039.48 (-99.86; session marks -81.20) · 8 name(s) marked open→close (per-name table). MOS×57 09:30 $24.00 → close $23.76 -13.68; RRC×33 09:30 $41.44 → close $41.64 +6.60; CRK×96 09:30 $14.42 → close $14.62 +19.20; SLI×526 09:30 $2.60 → close $2.64 +21.04; ANF×9 09:30 $144.70 → close $145.75 +9.45; BZ×74 09:30 $18.50 → close $18.00 -37.00; SMTC×9 09:30 $149.40 → close $142.43 -62.73; GRRR×86 09:30 $15.94 → close $15.66 -24.08 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $146.64 | ▼ 09:30 equity $10,665.81 vs yday $10,939.62 (-273.81) | 09:30 open · cash $146.64 (unchanged overnight, no fees) · equity $10,665.81 vs prior close $10,939.62 (-273.81) · 8 name(s) re-marked at the open (per-name table). MOS×57 yday $23.76 → 09:30 $23.75 -0.57; RRC×33 yday $41.64 → 09:30 $41.11 -17.49; CRK×96 yday $14.62 → 09:30 $14.56 -5.76; SLI×526 yday $2.64 → 09:30 $2.51 -68.38; ANF×9 yday $145.75 → 09:30 $148.67 +26.28; BZ×74 yday $18.00 → 09:30 $17.89 -8.14; SMTC×9 yday $142.43 → 09:30 $133.04 -84.51; GRRR×86 yday $15.66 → 09:30 $14.32 -115.24 | — |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 57 | $23.75 | $2.18 | $-18.59 | $1,498.20 | ▼ -18.59 after sell → book $10,663.62; vs 09:30 mark -2.19 | dropped from list after 4 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `RRC` | 33 | $41.11 | $2.11 | $+8.67 | $2,852.72 | ▲ +8.67 after sell → book $10,661.51; vs 09:30 mark -2.11 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CRK` | 96 | $14.56 | $2.31 | $+40.54 | $4,248.18 | ▲ +40.54 after sell → book $10,659.21; vs 09:30 mark -2.30 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SLI` | 526 | $2.51 | $6.88 | $-55.75 | $5,561.56 | ▼ -55.75 after sell → book $10,652.33; vs 09:30 mark -6.88 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 9 | $148.67 | $2.04 | $+31.68 | $6,897.55 | ▲ +31.68 after sell → book $10,650.29; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BZ` | 74 | $17.89 | $2.23 | $-49.59 | $8,219.17 | ▼ -49.59 after sell → book $10,648.05; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 9 | $133.04 | $2.04 | $-151.29 | $9,414.50 | ▼ -151.29 after sell → book $10,646.02; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GRRR` | 86 | $14.32 | $2.27 | $-143.84 | $10,643.74 | ▼ -143.84 after sell → book $10,643.74; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,643.74 | ▲ close $10,643.74 vs 09:30 $10,665.81 (session +0.00) | 16:00 close · cash $10,643.74 · no lots left · equity $10,643.74. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,643.74 | ▲ 09:30 equity $10,643.74 vs yday $10,643.74 (+0.00) | 09:30 open · cash $10,643.74 · no holdings · equity $10,643.74 vs prior close $10,643.74 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,643.74 | ▲ close $10,643.74 vs 09:30 $10,643.74 (session +0.00) | 16:00 close · cash $10,643.74 · no lots left · equity $10,643.74. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,643.74 | ▲ 09:30 equity $10,643.74 vs yday $10,643.74 (+0.00) | 09:30 open · cash $10,643.74 · no holdings · equity $10,643.74 vs prior close $10,643.74 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,643.74 | ▲ close $10,643.74 vs 09:30 $10,643.74 (session +0.00) | 16:00 close · cash $10,643.74 · no lots left · equity $10,643.74. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,643.74 | ▲ 09:30 equity $10,643.74 vs yday $10,643.74 (+0.00) | 09:30 open · cash $10,643.74 · no holdings · equity $10,643.74 vs prior close $10,643.74 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 26 | $49.76 | $2.07 | — | $9,347.92 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1330.47 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 32 | $41.31 | $2.09 | — | $8,023.91 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $1330.47 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 406 | $3.27 | $5.24 | — | $6,691.05 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1330.47 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 172 | $7.70 | $2.51 | — | $5,364.15 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $1330.47 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $125.94 | $2.02 | — | $4,102.73 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1330.47 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 84 | $15.70 | $2.24 | — | $2,781.68 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1330.47 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 58 | $22.78 | $2.16 | — | $1,458.28 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1330.47 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SLN` | 90 | $14.70 | $2.26 | — | $133.02 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer; 🔵; ⚪; ret5=-3.3; leftover $1330.47 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $133.02 | ▲ close $11,024.68 vs 09:30 $10,643.74 (session +401.52) | 16:00 close · cash $133.02 · equity $11,024.68 vs 09:30 $10,643.74 (+380.94; session marks +401.52) · 8 name(s) marked open→close (per-name table). ATRC×26 09:30 $49.76 → close $52.59 +73.58; HRMY×32 09:30 $41.31 → close $42.86 +49.60; CABA×406 09:30 $3.27 → close $3.57 +121.80; VSTM×172 09:30 $7.70 → close $8.02 +55.04; RVTY×10 09:30 $125.94 → close $130.94 +50.00; CRK×84 09:30 $15.70 → close $15.54 -13.44; MMED×58 09:30 $22.78 → close $23.76 +56.84; SLN×90 09:30 $14.70 → close $14.79 +8.10 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $133.02 | ▲ 09:30 equity $11,080.44 vs yday $11,024.68 (+55.76) | 09:30 open · cash $133.02 (unchanged overnight, no fees) · equity $11,080.44 vs prior close $11,024.68 (+55.76) · 8 name(s) re-marked at the open (per-name table). ATRC×26 yday $52.59 → 09:30 $52.88 +7.54; HRMY×32 yday $42.86 → 09:30 $42.93 +2.24; CABA×406 yday $3.57 → 09:30 $3.63 +24.36; VSTM×172 yday $8.02 → 09:30 $8.03 +1.72; RVTY×10 yday $130.94 → 09:30 $132.45 +15.10; CRK×84 yday $15.54 → 09:30 $15.45 -7.56; MMED×58 yday $23.76 → 09:30 $23.88 +6.96; SLN×90 yday $14.79 → 09:30 $14.85 +5.40 | — |
| 2026-09-04 09:30 ET | **SELL** | `HRMY` | 32 | $42.93 | $2.11 | $+47.65 | $1,504.67 | ▲ +47.65 after sell → book $11,078.33; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 172 | $8.03 | $2.55 | $+51.71 | $2,883.29 | ▲ +51.71 after sell → book $11,075.79; vs 09:30 mark -2.54 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 10 | $132.45 | $2.04 | $+61.04 | $4,205.75 | ▲ +61.04 after sell → book $11,073.75; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 84 | $15.45 | $2.27 | $-25.51 | $5,501.28 | ▼ -25.51 after sell → book $11,071.48; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 58 | $23.88 | $2.19 | $+59.45 | $6,884.14 | ▲ +59.45 after sell → book $11,069.30; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `SLN` | 90 | $14.85 | $2.29 | $+8.95 | $8,218.35 | ▲ +8.95 after sell → book $11,067.01; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ASND` | 5 | $266.94 | $2.00 | — | $6,881.64 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ret5=+1.9; leftover $1369.72 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OSCR` | 44 | $30.65 | $2.12 | — | $5,530.92 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ret5=-2.2; leftover $1369.72 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `NVAX` | 131 | $10.41 | $2.38 | — | $4,164.83 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $1369.72 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BVS` | 94 | $14.50 | $2.27 | — | $2,799.56 | — | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ⚪; ret5=+0.8; leftover $1369.72 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 702 | $1.95 | $9.06 | — | $1,421.60 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $1369.72 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `SLBT` | 446 | $3.07 | $5.75 | — | $46.63 | — | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-0.4; leftover $1369.72 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $46.63 | ▲ close $11,068.79 vs 09:30 $11,080.44 (session +25.37) | 16:00 close · cash $46.63 · equity $11,068.79 vs 09:30 $11,080.44 (-11.65; session marks +25.37) · 8 name(s) marked open→close (per-name table). ATRC×26 09:30 $52.88 → close $52.46 -10.92; CABA×406 09:30 $3.63 → close $3.48 -60.90; ASND×5 09:30 $266.94 → close $271.12 +20.90; OSCR×44 09:30 $30.65 → close $32.24 +69.96; NVAX×131 09:30 $10.41 → close $10.34 -9.17; BVS×94 09:30 $14.50 → close $14.36 -13.16; BAK×702 09:30 $1.95 → close $1.94 -7.02; SLBT×446 09:30 $3.07 → close $3.15 +35.68 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ALOY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BKKT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `QSI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ELMT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-26 | `BMEA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ALVO` | no_price | no 09:30 open — carry |
| 2026-08-26 | `KURA` | no_price | no 09:30 open |
| 2026-08-26 | `AVBP` | no_price | no 09:30 open |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `OKTA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `VEEV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RPD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MTDR` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `RES` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OIS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `FTI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KMI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OKE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `PCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HRMY` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VSTM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MGTX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TII` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `KMX` | hard_red | hard-red S=-3.83 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ATRC` | 26 | 2026-09-03 @ $49.76 | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1330.47 |
| `CABA` | 406 | 2026-09-03 @ $3.27 | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1330.47 |
| `ASND` | 5 | 2026-09-04 @ $266.94 | union ∩ ab_g, no 🚨; gate ab=good; list flatten; ret5=+1.9; leftover $1369.72 |
| `OSCR` | 44 | 2026-09-04 @ $30.65 | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ret5=-2.2; leftover $1369.72 |
| `NVAX` | 131 | 2026-09-04 @ $10.41 | union ∩ ab_g, no 🚨; gate ab=good; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $1369.72 |
| `BVS` | 94 | 2026-09-04 @ $14.50 | union ∩ ab_g, no 🚨; gate ab=good; list flatten; 🔵; ⚪; ret5=+0.8; leftover $1369.72 |
| `BAK` | 702 | 2026-09-04 @ $1.95 | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $1369.72 |
| `SLBT` | 446 | 2026-09-04 @ $3.07 | union ∩ ab_g, no 🚨; gate ab=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-0.4; leftover $1369.72 |
