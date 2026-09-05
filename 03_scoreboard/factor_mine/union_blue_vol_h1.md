# Factor mine action — `union_blue_vol_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **-9.72%** ($9,028) · signal-only (no cash/fees) was -10.68%. Starts YES **3/17**. Fills 112 · skips 33 · realized $-515.49.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `vol=good,blue=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $209.17.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `BTBT` | 833 | — | $1.50 | +0.00 | $1.57 | +58.31 | +58.31 | +0.00 | +58.31 |
| 2026-08-14 | `BETR` | 84 | — | $14.80 | +0.00 | $13.73 | -89.88 | -89.88 | +0.00 | -89.88 |
| 2026-08-14 | `ANGX` | 290 | — | $4.31 | +0.00 | $4.37 | +17.40 | +17.40 | +0.00 | +17.40 |
| 2026-08-14 | `HYLN` | 299 | — | $4.18 | +0.00 | $4.06 | -35.88 | -35.88 | +0.00 | -35.88 |
| 2026-08-14 | `ADUR` | 75 | — | $16.50 | +0.00 | $16.17 | -24.75 | -24.75 | +0.00 | -24.75 |
| 2026-08-14 | `ARX` | 63 | — | $19.57 | +0.00 | $19.58 | +0.63 | +0.63 | +0.00 | +0.63 |
| 2026-08-14 | `AIRO` | 112 | — | $11.12 | +0.00 | $9.57 | -173.60 | -173.60 | +0.00 | -173.60 |
| 2026-08-14 | `NCMI` | 464 | — | $2.69 | +0.00 | $2.86 | +78.88 | +78.88 | +0.00 | +78.88 |
| 2026-08-17 | `BTBT` | 833 | $1.57 | $1.52 | -41.65 | — | +0.00 | -41.65 | +16.66 | — |
| 2026-08-17 | `BETR` | 84 | $13.73 | $13.67 | -5.04 | — | +0.00 | -5.04 | -94.92 | — |
| 2026-08-17 | `ANGX` | 290 | $4.37 | $4.60 | +66.70 | — | +0.00 | +66.70 | +84.10 | — |
| 2026-08-17 | `HYLN` | 299 | $4.06 | $4.10 | +11.96 | — | +0.00 | +11.96 | -23.92 | — |
| 2026-08-17 | `ADUR` | 75 | $16.17 | $15.73 | -33.00 | — | +0.00 | -33.00 | -57.75 | — |
| 2026-08-17 | `ARX` | 63 | $19.58 | $19.57 | -0.63 | — | +0.00 | -0.63 | +0.00 | — |
| 2026-08-17 | `AIRO` | 112 | $9.57 | $9.57 | +0.00 | — | +0.00 | +0.00 | -173.60 | — |
| 2026-08-17 | `NCMI` | 464 | $2.86 | $2.80 | -27.84 | — | +0.00 | -27.84 | +51.04 | — |
| 2026-08-17 | `TMC` | 300 | — | $4.05 | +0.00 | $3.77 | -84.00 | -84.00 | +0.00 | -84.00 |
| 2026-08-17 | `ABX` | 133 | — | $9.12 | +0.00 | $9.12 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-17 | `ALOY` | 83 | — | $14.66 | +0.00 | $13.86 | -66.81 | -66.81 | +0.00 | -66.81 |
| 2026-08-17 | `NU` | 79 | — | $15.40 | +0.00 | $14.74 | -52.14 | -52.14 | +0.00 | -52.14 |
| 2026-08-17 | `INV` | 751 | — | $1.62 | +0.00 | $1.39 | -176.49 | -176.49 | +0.00 | -176.49 |
| 2026-08-17 | `KLC` | 464 | — | $2.62 | +0.00 | $2.56 | -27.84 | -27.84 | +0.00 | -27.84 |
| 2026-08-17 | `ENHA` | 605 | — | $2.01 | +0.00 | $1.71 | -181.50 | -181.50 | +0.00 | -181.50 |
| 2026-08-17 | `MP` | 20 | — | $58.01 | +0.00 | $58.51 | +10.00 | +10.00 | +0.00 | +10.00 |
| 2026-08-18 | `TMC` | 300 | $3.77 | $3.72 | -15.00 | — | +0.00 | -15.00 | -99.00 | — |
| 2026-08-18 | `ABX` | 133 | $9.12 | $9.03 | -11.97 | — | +0.00 | -11.97 | -11.97 | — |
| 2026-08-18 | `ALOY` | 83 | $13.86 | $13.19 | -55.20 | — | +0.00 | -55.20 | -122.01 | — |
| 2026-08-18 | `NU` | 79 | $14.74 | $14.53 | -16.59 | — | +0.00 | -16.59 | -68.73 | — |
| 2026-08-18 | `INV` | 751 | $1.39 | $1.32 | -45.06 | — | +0.00 | -45.06 | -221.55 | — |
| 2026-08-18 | `KLC` | 464 | $2.56 | $2.52 | -18.56 | — | +0.00 | -18.56 | -46.40 | — |
| 2026-08-18 | `ENHA` | 605 | $1.71 | $1.70 | -6.05 | — | +0.00 | -6.05 | -187.55 | — |
| 2026-08-18 | `MP` | 20 | $58.51 | $56.35 | -43.20 | — | +0.00 | -43.20 | -33.20 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `AG` | 53 | — | $20.55 | +0.00 | $21.19 | +33.92 | +33.92 | +0.00 | +33.92 |
| 2026-08-20 | `BHP` | 12 | — | $91.01 | +0.00 | $93.63 | +31.44 | +31.44 | +0.00 | +31.44 |
| 2026-08-20 | `CDE` | 53 | — | $20.65 | +0.00 | $21.11 | +24.38 | +24.38 | +0.00 | +24.38 |
| 2026-08-20 | `HDSN` | 192 | — | $5.77 | +0.00 | $5.57 | -38.40 | -38.40 | +0.00 | -38.40 |
| 2026-08-20 | `IAG` | 56 | — | $19.63 | +0.00 | $20.50 | +48.72 | +48.72 | +0.00 | +48.72 |
| 2026-08-20 | `KGC` | 37 | — | $29.63 | +0.00 | $31.43 | +66.60 | +66.60 | +0.00 | +66.60 |
| 2026-08-20 | `NFGC` | 633 | — | $1.75 | +0.00 | $1.75 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `WPM` | 7 | — | $144.54 | +0.00 | $150.25 | +39.97 | +39.97 | +0.00 | +39.97 |
| 2026-08-21 | `AG` | 53 | $21.19 | $21.90 | +37.63 | — | +0.00 | +37.63 | +71.55 | — |
| 2026-08-21 | `BHP` | 12 | $93.63 | $95.72 | +25.08 | — | +0.00 | +25.08 | +56.52 | — |
| 2026-08-21 | `CDE` | 53 | $21.11 | $21.75 | +33.92 | — | +0.00 | +33.92 | +58.30 | — |
| 2026-08-21 | `HDSN` | 192 | $5.57 | $5.67 | +19.20 | — | +0.00 | +19.20 | -19.20 | — |
| 2026-08-21 | `IAG` | 56 | $20.50 | $21.17 | +37.52 | — | +0.00 | +37.52 | +86.24 | — |
| 2026-08-21 | `KGC` | 37 | $31.43 | $32.17 | +27.38 | — | +0.00 | +27.38 | +93.98 | — |
| 2026-08-21 | `NFGC` | 633 | $1.75 | $1.79 | +25.32 | — | +0.00 | +25.32 | +25.32 | — |
| 2026-08-21 | `WPM` | 7 | $150.25 | $154.70 | +31.15 | — | +0.00 | +31.15 | +71.12 | — |
| 2026-08-21 | `AU` | 9 | — | $119.43 | +0.00 | $121.22 | +16.11 | +16.11 | +0.00 | +16.11 |
| 2026-08-21 | `AUPH` | 67 | — | $17.20 | +0.00 | $16.65 | -36.85 | -36.85 | +0.00 | -36.85 |
| 2026-08-21 | `AEM` | 5 | — | $216.30 | +0.00 | $216.06 | -1.20 | -1.20 | +0.00 | -1.20 |
| 2026-08-21 | `ARCT` | 104 | — | $11.13 | +0.00 | $13.45 | +241.28 | +241.28 | +0.00 | +241.28 |
| 2026-08-21 | `AUTL` | 469 | — | $2.47 | +0.00 | $2.41 | -28.14 | -28.14 | +0.00 | -28.14 |
| 2026-08-21 | `CRDL` | 600 | — | $1.93 | +0.00 | $1.86 | -42.00 | -42.00 | +0.00 | -42.00 |
| 2026-08-21 | `CRSP` | 19 | — | $59.72 | +0.00 | $59.50 | -4.18 | -4.18 | +0.00 | -4.18 |
| 2026-08-21 | `CYPH` | 877 | — | $1.32 | +0.00 | $1.42 | +87.70 | +87.70 | +0.00 | +87.70 |
| 2026-08-24 | `AU` | 9 | $121.22 | $120.50 | -6.48 | — | +0.00 | -6.48 | +9.63 | — |
| 2026-08-24 | `AUPH` | 67 | $16.65 | $16.60 | -3.35 | — | +0.00 | -3.35 | -40.20 | — |
| 2026-08-24 | `AEM` | 5 | $216.06 | $217.03 | +4.85 | — | +0.00 | +4.85 | +3.65 | — |
| 2026-08-24 | `ARCT` | 104 | $13.45 | $13.26 | -19.76 | — | +0.00 | -19.76 | +221.52 | — |
| 2026-08-24 | `AUTL` | 469 | $2.41 | $2.36 | -23.45 | — | +0.00 | -23.45 | -51.59 | — |
| 2026-08-24 | `CRDL` | 600 | $1.86 | $1.87 | +6.00 | — | +0.00 | +6.00 | -36.00 | — |
| 2026-08-24 | `CRSP` | 19 | $59.50 | $58.79 | -13.49 | — | +0.00 | -13.49 | -17.67 | — |
| 2026-08-24 | `CYPH` | 877 | $1.42 | $1.83 | +359.57 | — | +0.00 | +359.57 | +447.27 | — |
| 2026-08-25 | `BMEA` | 750 | — | $1.62 | +0.00 | $1.61 | -7.50 | -7.50 | +0.00 | -7.50 |
| 2026-08-25 | `NPWR` | 608 | — | $2.00 | +0.00 | $2.02 | +12.16 | +12.16 | +0.00 | +12.16 |
| 2026-08-25 | `PUSA` | 328 | — | $3.70 | +0.00 | $3.91 | +68.88 | +68.88 | +0.00 | +68.88 |
| 2026-08-25 | `ALVO` | 233 | — | $5.22 | +0.00 | $5.25 | +6.99 | +6.99 | +0.00 | +6.99 |
| 2026-08-25 | `CAPR` | 179 | — | $6.79 | +0.00 | $7.19 | +71.60 | +71.60 | +0.00 | +71.60 |
| 2026-08-25 | `ZURA` | 190 | — | $6.38 | +0.00 | $6.50 | +22.80 | +22.80 | +0.00 | +22.80 |
| 2026-08-25 | `SUJA` | 138 | — | $8.79 | +0.00 | $8.54 | -34.50 | -34.50 | +0.00 | -34.50 |
| 2026-08-25 | `CYPH` | 699 | — | $1.70 | +0.00 | $1.64 | -41.94 | -41.94 | +0.00 | -41.94 |
| 2026-08-26 | `BMEA` | 750 | $1.61 | $1.61 | +0.00 | $1.61 | +0.00 | +0.00 | -7.50 | -7.50 |
| 2026-08-26 | `NPWR` | 608 | $2.02 | $2.02 | +0.00 | $2.02 | +0.00 | +0.00 | +12.16 | +12.16 |
| 2026-08-26 | `PUSA` | 328 | $3.91 | $3.91 | +0.00 | $3.91 | +0.00 | +0.00 | +68.88 | +68.88 |
| 2026-08-26 | `ALVO` | 233 | $5.25 | $5.25 | +0.00 | $5.25 | +0.00 | +0.00 | +6.99 | +6.99 |
| 2026-08-26 | `CAPR` | 179 | $7.19 | $7.19 | +0.00 | $7.19 | +0.00 | +0.00 | +71.60 | +71.60 |
| 2026-08-26 | `ZURA` | 190 | $6.50 | $6.50 | +0.00 | $6.50 | +0.00 | +0.00 | +22.80 | +22.80 |
| 2026-08-26 | `SUJA` | 138 | $8.54 | $8.54 | +0.00 | $8.54 | +0.00 | +0.00 | -34.50 | -34.50 |
| 2026-08-26 | `CYPH` | 699 | $1.64 | $1.64 | +0.00 | $1.64 | +0.00 | +0.00 | -41.94 | -41.94 |
| 2026-08-27 | `BMEA` | 750 | $1.61 | $1.75 | +105.00 | — | +0.00 | +105.00 | +97.50 | — |
| 2026-08-27 | `NPWR` | 608 | $2.02 | $1.93 | -54.72 | — | +0.00 | -54.72 | -42.56 | — |
| 2026-08-27 | `PUSA` | 328 | $3.91 | $3.84 | -22.96 | — | +0.00 | -22.96 | +45.92 | — |
| 2026-08-27 | `ALVO` | 233 | $5.25 | $4.98 | -62.91 | — | +0.00 | -62.91 | -55.92 | — |
| 2026-08-27 | `CAPR` | 179 | $7.19 | $8.29 | +196.90 | — | +0.00 | +196.90 | +268.50 | — |
| 2026-08-27 | `ZURA` | 190 | $6.50 | $6.13 | -70.30 | — | +0.00 | -70.30 | -47.50 | — |
| 2026-08-27 | `SUJA` | 138 | $8.54 | $9.39 | +117.30 | — | +0.00 | +117.30 | +82.80 | — |
| 2026-08-27 | `CYPH` | 699 | $1.64 | $1.60 | -27.96 | — | +0.00 | -27.96 | -69.90 | — |
| 2026-08-28 | `ANF` | 17 | — | $144.70 | +0.00 | $145.75 | +17.85 | +17.85 | +0.00 | +17.85 |
| 2026-08-28 | `SEDG` | 73 | — | $33.78 | +0.00 | $33.51 | -19.71 | -19.71 | +0.00 | -19.71 |
| 2026-08-28 | `SMTC` | 16 | — | $149.40 | +0.00 | $142.43 | -111.52 | -111.52 | +0.00 | -111.52 |
| 2026-08-28 | `URBN` | 30 | — | $82.70 | +0.00 | $78.79 | -117.30 | -117.30 | +0.00 | -117.30 |
| 2026-08-31 | `ANF` | 17 | $145.75 | $148.67 | +49.64 | — | +0.00 | +49.64 | +67.49 | — |
| 2026-08-31 | `SEDG` | 73 | $33.51 | $31.50 | -146.73 | — | +0.00 | -146.73 | -166.44 | — |
| 2026-08-31 | `SMTC` | 16 | $142.43 | $133.04 | -150.24 | — | +0.00 | -150.24 | -261.76 | — |
| 2026-08-31 | `URBN` | 30 | $78.79 | $81.09 | +69.00 | — | +0.00 | +69.00 | -48.30 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `RVTY` | 9 | — | $125.94 | +0.00 | $130.94 | +45.00 | +45.00 | +0.00 | +45.00 |
| 2026-09-03 | `CRK` | 75 | — | $15.70 | +0.00 | $15.54 | -12.00 | -12.00 | +0.00 | -12.00 |
| 2026-09-03 | `MMED` | 52 | — | $22.78 | +0.00 | $23.76 | +50.96 | +50.96 | +0.00 | +50.96 |
| 2026-09-03 | `CTMX` | 319 | — | $3.72 | +0.00 | $3.72 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-09-03 | `CRDL` | 549 | — | $2.16 | +0.00 | $2.17 | +5.49 | +5.49 | +0.00 | +5.49 |
| 2026-09-03 | `DEFT` | 1772 | — | $0.67 | +0.00 | $0.65 | -35.44 | -35.44 | +0.00 | -35.44 |
| 2026-09-03 | `MRNA` | 7 | — | $151.40 | +0.00 | $150.81 | -4.13 | -4.13 | +0.00 | -4.13 |
| 2026-09-03 | `ARCT` | 72 | — | $16.46 | +0.00 | $16.74 | +20.16 | +20.16 | +0.00 | +20.16 |
| 2026-09-04 | `RVTY` | 9 | $130.94 | $132.45 | +13.59 | — | +0.00 | +13.59 | +58.59 | — |
| 2026-09-04 | `CRK` | 75 | $15.54 | $15.45 | -6.75 | — | +0.00 | -6.75 | -18.75 | — |
| 2026-09-04 | `MMED` | 52 | $23.76 | $23.88 | +6.24 | — | +0.00 | +6.24 | +57.20 | — |
| 2026-09-04 | `CTMX` | 319 | $3.72 | $3.73 | +3.19 | — | +0.00 | +3.19 | +3.19 | — |
| 2026-09-04 | `CRDL` | 549 | $2.17 | $2.18 | +5.49 | — | +0.00 | +5.49 | +10.98 | — |
| 2026-09-04 | `DEFT` | 1772 | $0.65 | $0.65 | +0.00 | — | +0.00 | +0.00 | -35.44 | — |
| 2026-09-04 | `MRNA` | 7 | $150.81 | $145.95 | -34.02 | — | +0.00 | -34.02 | -38.15 | — |
| 2026-09-04 | `ARCT` | 72 | $16.74 | $16.77 | +2.16 | — | +0.00 | +2.16 | +22.32 | — |
| 2026-09-04 | `CABA` | 326 | — | $3.63 | +0.00 | $3.48 | -48.90 | -48.90 | +0.00 | -48.90 |
| 2026-09-04 | `GPRO` | 666 | — | $1.78 | +0.00 | $1.39 | -259.74 | -259.74 | +0.00 | -259.74 |
| 2026-09-04 | `EOSE` | 332 | — | $3.57 | +0.00 | $3.50 | -23.24 | -23.24 | +0.00 | -23.24 |
| 2026-09-04 | `DELL` | 2 | — | $486.31 | +0.00 | $516.39 | +60.16 | +60.16 | +0.00 | +60.16 |
| 2026-09-04 | `MLYS` | 40 | — | $29.15 | +0.00 | $28.27 | -35.20 | -35.20 | +0.00 | -35.20 |
| 2026-09-04 | `CCOI` | 116 | — | $10.22 | +0.00 | $9.98 | -27.84 | -27.84 | +0.00 | -27.84 |
| 2026-09-04 | `IRD` | 254 | — | $4.66 | +0.00 | $4.60 | -15.24 | -15.24 | +0.00 | -15.24 |
| 2026-09-04 | `OABI` | 233 | — | $5.08 | +0.00 | $4.75 | -76.89 | -76.89 | +0.00 | -76.89 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | -168.89 | BTBT, BETR, ANGX, HYLN, ADUR, ARX, AIRO, NCMI | — | $10.28 | $9,797.82 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, ARX×63, AIRO×112, NCMI×464 |
| 2026-08-17 | +2.25 | $10.28 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, ARX×63, AIRO×112, NCMI×464 | $9,768.32 | -29.50 | -578.78 | TMC, ABX, ALOY, NU, INV, KLC, ENHA, MP | BTBT, BETR, ANGX, HYLN, ADUR, ARX, AIRO, NCMI | $28.43 | $9,119.54 | TMC×300, ABX×133, ALOY×83, NU×79, INV×751, KLC×464, ENHA×605, MP×20 |
| 2026-08-18 | -6.20 | $28.43 | TMC×300, ABX×133, ALOY×83, NU×79, INV×751, KLC×464, ENHA×605, MP×20 | $8,907.92 | -211.62 | +0.00 | — | TMC, ABX, ALOY, NU, INV, KLC, ENHA, MP | $8,871.18 | $8,871.18 | — |
| 2026-08-19 | -7.20 | $8,871.18 | — | $8,871.18 | -0.00 | +0.00 | — | — | $8,871.18 | $8,871.18 | — |
| 2026-08-20 | +1.12 | $8,871.18 | — | $8,871.18 | -0.00 | +206.63 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | — | $149.17 | $9,054.48 | AG×53, BHP×12, CDE×53, HDSN×192, IAG×56, KGC×37, NFGC×633, WPM×7 |
| 2026-08-21 | +3.25 | $149.17 | AG×53, BHP×12, CDE×53, HDSN×192, IAG×56, KGC×37, NFGC×633, WPM×7 | $9,291.68 | +237.20 | +232.72 | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $157.37 | $9,465.13 | AU×9, AUPH×67, AEM×5, ARCT×104, AUTL×469, CRDL×600, CRSP×19, CYPH×877 |
| 2026-08-24 | -5.17 | $157.37 | AU×9, AUPH×67, AEM×5, ARCT×104, AUTL×469, CRDL×600, CRSP×19, CYPH×877 | $9,769.02 | +303.89 | +0.00 | — | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | $9,732.89 | $9,732.89 | — |
| 2026-08-25 | +1.80 | $9,732.89 | — | $9,732.89 | +0.00 | +98.49 | BMEA, NPWR, PUSA, ALVO, CAPR, ZURA, SUJA, CYPH | — | $1.84 | $9,790.12 | BMEA×750, NPWR×608, PUSA×328, ALVO×233, CAPR×179, ZURA×190, SUJA×138, CYPH×699 |
| 2026-08-26 | +2.02 | $1.84 | BMEA×750, NPWR×608, PUSA×328, ALVO×233, CAPR×179, ZURA×190, SUJA×138, CYPH×699 | $9,790.12 | -0.00 | +0.00 | — | — | $1.84 | $9,790.12 | BMEA×750, NPWR×608, PUSA×328, ALVO×233, CAPR×179, ZURA×190, SUJA×138, CYPH×699 |
| 2026-08-27 | — | $1.84 | BMEA×750, NPWR×608, PUSA×328, ALVO×233, CAPR×179, ZURA×190, SUJA×138, CYPH×699 | $9,970.47 | +180.35 | +0.00 | — | BMEA, NPWR, PUSA, ALVO, CAPR, ZURA, SUJA, CYPH | $9,928.60 | $9,928.60 | — |
| 2026-08-28 | +0.75 | $9,928.60 | — | $9,928.60 | +0.00 | -230.68 | ANF, SEDG, SMTC, URBN | — | $123.00 | $9,689.56 | ANF×17, SEDG×73, SMTC×16, URBN×30 |
| 2026-08-31 | -5.85 | $123.00 | ANF×17, SEDG×73, SMTC×16, URBN×30 | $9,511.23 | -178.33 | +0.00 | — | ANF, SEDG, SMTC, URBN | $9,502.74 | $9,502.74 | — |
| 2026-09-01 | -6.30 | $9,502.74 | — | $9,502.74 | +0.00 | +0.00 | — | — | $9,502.74 | $9,502.74 | — |
| 2026-09-02 | -3.83 | $9,502.74 | — | $9,502.74 | +0.00 | +0.00 | — | — | $9,502.74 | $9,502.74 | — |
| 2026-09-03 | -0.90 | $9,502.74 | — | $9,502.74 | +0.00 | +70.04 | RVTY, CRK, MMED, CTMX, CRDL, DEFT, MRNA, ARCT | — | $163.56 | $9,533.80 | RVTY×9, CRK×75, MMED×52, CTMX×319, CRDL×549, DEFT×1772, MRNA×7, ARCT×72 |
| 2026-09-04 | — | $163.56 | RVTY×9, CRK×75, MMED×52, CTMX×319, CRDL×549, DEFT×1772, MRNA×7, ARCT×72 | $9,523.70 | -10.10 | -426.89 | CABA, GPRO, EOSE, DELL, MLYS, CCOI, IRD, OABI | RVTY, CRK, MMED, CTMX, CRDL, DEFT, MRNA, ARCT | $209.17 | $9,027.80 | CABA×326, GPRO×666, EOSE×332, DELL×2, MLYS×40, CCOI×116, IRD×254, OABI×233 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 833 | $1.50 | $10.75 | — | $8,739.75 | — | combo gate; gate vol=good,blue=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BETR` | 84 | $14.80 | $2.24 | — | $7,494.31 | — | combo gate; gate vol=good,blue=True; list flatten; 🔵; ⚪; ret5=-9.9; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 290 | $4.31 | $3.74 | — | $6,240.67 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 299 | $4.18 | $3.86 | — | $4,986.99 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 75 | $16.50 | $2.21 | — | $3,747.28 | — | combo gate; gate vol=good,blue=True; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 63 | $19.57 | $2.18 | — | $2,512.19 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 112 | $11.12 | $2.33 | — | $1,264.42 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NCMI` | 464 | $2.69 | $5.99 | — | $10.28 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=-33.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10.28 | ▼ close $9,797.82 vs 09:30 $10,000.00 (session -168.89) | 16:00 close · cash $10.28 · equity $9,797.82 vs 09:30 $10,000.00 (-202.18; session marks -168.89) · 8 name(s) marked open→close (per-name table). BTBT×833 09:30 $1.50 → close $1.57 +58.31; BETR×84 09:30 $14.80 → close $13.73 -89.88; ANGX×290 09:30 $4.31 → close $4.37 +17.40; HYLN×299 09:30 $4.18 → close $4.06 -35.88; ADUR×75 09:30 $16.50 → close $16.17 -24.75; ARX×63 09:30 $19.57 → close $19.58 +0.63; AIRO×112 09:30 $11.12 → close $9.57 -173.60; NCMI×464 09:30 $2.69 → close $2.86 +78.88 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10.28 | ▼ 09:30 equity $9,768.32 vs yday $9,797.82 (-29.50) | 09:30 open · cash $10.28 (unchanged overnight, no fees) · equity $9,768.32 vs prior close $9,797.82 (-29.50) · 8 name(s) re-marked at the open (per-name table). BTBT×833 yday $1.57 → 09:30 $1.52 -41.65; BETR×84 yday $13.73 → 09:30 $13.67 -5.04; ANGX×290 yday $4.37 → 09:30 $4.60 +66.70; HYLN×299 yday $4.06 → 09:30 $4.10 +11.96; ADUR×75 yday $16.17 → 09:30 $15.73 -33.00; ARX×63 yday $19.58 → 09:30 $19.57 -0.63; AIRO×112 yday $9.57 → 09:30 $9.57 +0.00; NCMI×464 yday $2.86 → 09:30 $2.80 -27.84 | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 833 | $1.52 | $10.89 | $-4.98 | $1,265.54 | ▼ -4.98 after sell → book $9,757.42; vs 09:30 mark -10.90 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BETR` | 84 | $13.67 | $2.27 | $-99.43 | $2,411.56 | ▼ -99.43 after sell → book $9,755.16; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 290 | $4.60 | $3.80 | $+76.56 | $3,741.76 | ▲ +76.56 after sell → book $9,751.36; vs 09:30 mark -3.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HYLN` | 299 | $4.10 | $3.92 | $-31.69 | $4,963.74 | ▼ -31.69 after sell → book $9,747.44; vs 09:30 mark -3.92 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ADUR` | 75 | $15.73 | $2.24 | $-62.20 | $6,141.25 | ▼ -62.20 after sell → book $9,745.20; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 63 | $19.57 | $2.20 | $-4.38 | $7,371.97 | ▼ -4.38 after sell → book $9,743.01; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 112 | $9.57 | $2.35 | $-178.28 | $8,441.45 | ▼ -178.28 after sell → book $9,740.65; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NCMI` | 464 | $2.80 | $6.07 | $+38.98 | $9,734.58 | ▲ +38.98 after sell → book $9,734.58; vs 09:30 mark -6.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 300 | $4.05 | $3.87 | — | $8,515.71 | — | combo gate; gate vol=good,blue=True; list flatten; 🔵; ⚪; ret5=-12.3; leftover $1216.82 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 133 | $9.12 | $2.39 | — | $7,300.36 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $1216.82 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ALOY` | 83 | $14.66 | $2.24 | — | $6,081.34 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.0; leftover $1216.82 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `NU` | 79 | $15.40 | $2.23 | — | $4,862.51 | — | combo gate; gate vol=good,blue=True; list yday_gainer; 🔵; ⚪; ret5=+10.0; leftover $1216.82 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `INV` | 751 | $1.62 | $9.69 | — | $3,636.20 | — | combo gate; gate vol=good,blue=True; list yday_mover; 🔵; ⚪; ret5=-53.0; leftover $1216.82 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `KLC` | 464 | $2.62 | $5.99 | — | $2,414.54 | — | combo gate; gate vol=good,blue=True; list yday_mover; 🔵; ⚪; ret5=-49.7; leftover $1216.82 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ENHA` | 605 | $2.01 | $7.80 | — | $1,190.68 | — | combo gate; gate vol=good,blue=True; list yday_mover; 🔵; ⚪; ret5=-26.0; leftover $1216.82 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `MP` | 20 | $58.01 | $2.05 | — | $28.43 | — | combo gate; gate vol=good,blue=True; list ohlc_hot; 🔵; ⚪; ret5=+14.9; leftover $1216.82 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $28.43 | ▼ close $9,119.54 vs 09:30 $9,768.32 (session -578.78) | 16:00 close · cash $28.43 · equity $9,119.54 vs 09:30 $9,768.32 (-648.78; session marks -578.78) · 8 name(s) marked open→close (per-name table). TMC×300 09:30 $4.05 → close $3.77 -84.00; ABX×133 09:30 $9.12 → close $9.12 +0.00; ALOY×83 09:30 $14.66 → close $13.86 -66.81; NU×79 09:30 $15.40 → close $14.74 -52.14; INV×751 09:30 $1.62 → close $1.39 -176.49; KLC×464 09:30 $2.62 → close $2.56 -27.84; ENHA×605 09:30 $2.01 → close $1.71 -181.50; MP×20 09:30 $58.01 → close $58.51 +10.00 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $28.43 | ▼ 09:30 equity $8,907.92 vs yday $9,119.54 (-211.62) | 09:30 open · cash $28.43 (unchanged overnight, no fees) · equity $8,907.92 vs prior close $9,119.54 (-211.62) · 8 name(s) re-marked at the open (per-name table). TMC×300 yday $3.77 → 09:30 $3.72 -15.00; ABX×133 yday $9.12 → 09:30 $9.03 -11.97; ALOY×83 yday $13.86 → 09:30 $13.19 -55.20; NU×79 yday $14.74 → 09:30 $14.53 -16.59; INV×751 yday $1.39 → 09:30 $1.32 -45.06; KLC×464 yday $2.56 → 09:30 $2.52 -18.56; ENHA×605 yday $1.71 → 09:30 $1.70 -6.05; MP×20 yday $58.51 → 09:30 $56.35 -43.20 | — |
| 2026-08-18 09:30 ET | **SELL** | `TMC` | 300 | $3.72 | $3.93 | $-106.80 | $1,140.50 | ▼ -106.80 after sell → book $8,903.99; vs 09:30 mark -3.93 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 133 | $9.03 | $2.42 | $-16.78 | $2,339.07 | ▼ -16.78 after sell → book $8,901.57; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALOY` | 83 | $13.19 | $2.26 | $-126.51 | $3,431.58 | ▼ -126.51 after sell → book $8,899.31; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `NU` | 79 | $14.53 | $2.25 | $-73.21 | $4,577.20 | ▼ -73.21 after sell → book $8,897.06; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `INV` | 751 | $1.32 | $9.82 | $-241.06 | $5,562.45 | ▼ -241.06 after sell → book $8,887.23; vs 09:30 mark -9.83 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `KLC` | 464 | $2.52 | $6.07 | $-58.46 | $6,725.66 | ▼ -58.46 after sell → book $8,881.16; vs 09:30 mark -6.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ENHA` | 605 | $1.70 | $7.91 | $-203.27 | $7,746.25 | ▼ -203.27 after sell → book $8,873.25; vs 09:30 mark -7.91 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `MP` | 20 | $56.35 | $2.07 | $-37.32 | $8,871.18 | ▼ -37.32 after sell → book $8,871.18; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,871.18 | ▲ close $8,871.18 vs 09:30 $8,907.92 (session +0.00) | 16:00 close · cash $8,871.18 · no lots left · equity $8,871.18. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,871.18 | ▲ 09:30 equity $8,871.18 vs yday $8,871.18 (-0.00) | 09:30 open · cash $8,871.18 · no holdings · equity $8,871.18 vs prior close $8,871.18 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,871.18 | ▲ close $8,871.18 vs 09:30 $8,871.18 (session +0.00) | 16:00 close · cash $8,871.18 · no lots left · equity $8,871.18. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,871.18 | ▲ 09:30 equity $8,871.18 vs yday $8,871.18 (-0.00) | 09:30 open · cash $8,871.18 · no holdings · equity $8,871.18 vs prior close $8,871.18 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 53 | $20.55 | $2.15 | — | $7,779.88 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1108.90 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 12 | $91.01 | $2.03 | — | $6,685.73 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1108.90 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 53 | $20.65 | $2.15 | — | $5,589.13 | — | combo gate; gate vol=good,blue=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1108.90 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 192 | $5.77 | $2.57 | — | $4,478.73 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1108.90 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 56 | $19.63 | $2.16 | — | $3,377.29 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1108.90 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 37 | $29.63 | $2.10 | — | $2,278.88 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1108.90 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 633 | $1.75 | $8.17 | — | $1,162.96 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1108.90 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 7 | $144.54 | $2.01 | — | $149.17 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1108.90 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $149.17 | ▲ close $9,054.48 vs 09:30 $8,871.18 (session +206.63) | 16:00 close · cash $149.17 · equity $9,054.48 vs 09:30 $8,871.18 (+183.30; session marks +206.63) · 8 name(s) marked open→close (per-name table). AG×53 09:30 $20.55 → close $21.19 +33.92; BHP×12 09:30 $91.01 → close $93.63 +31.44; CDE×53 09:30 $20.65 → close $21.11 +24.38; HDSN×192 09:30 $5.77 → close $5.57 -38.40; IAG×56 09:30 $19.63 → close $20.50 +48.72; KGC×37 09:30 $29.63 → close $31.43 +66.60; NFGC×633 09:30 $1.75 → close $1.75 +0.00; WPM×7 09:30 $144.54 → close $150.25 +39.97 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $149.17 | ▲ 09:30 equity $9,291.68 vs yday $9,054.48 (+237.20) | 09:30 open · cash $149.17 (unchanged overnight, no fees) · equity $9,291.68 vs prior close $9,054.48 (+237.20) · 8 name(s) re-marked at the open (per-name table). AG×53 yday $21.19 → 09:30 $21.90 +37.63; BHP×12 yday $93.63 → 09:30 $95.72 +25.08; CDE×53 yday $21.11 → 09:30 $21.75 +33.92; HDSN×192 yday $5.57 → 09:30 $5.67 +19.20; IAG×56 yday $20.50 → 09:30 $21.17 +37.52; KGC×37 yday $31.43 → 09:30 $32.17 +27.38; NFGC×633 yday $1.75 → 09:30 $1.79 +25.32; WPM×7 yday $150.25 → 09:30 $154.70 +31.15 | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 53 | $21.90 | $2.17 | $+67.23 | $1,307.70 | ▲ +67.23 after sell → book $9,289.51; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 12 | $95.72 | $2.05 | $+52.45 | $2,454.30 | ▲ +52.45 after sell → book $9,287.47; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 53 | $21.75 | $2.17 | $+53.98 | $3,604.88 | ▲ +53.98 after sell → book $9,285.30; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 192 | $5.67 | $2.61 | $-24.37 | $4,690.91 | ▼ -24.37 after sell → book $9,282.69; vs 09:30 mark -2.61 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 56 | $21.17 | $2.18 | $+81.90 | $5,874.25 | ▲ +81.90 after sell → book $9,280.51; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 37 | $32.17 | $2.12 | $+89.76 | $7,062.42 | ▲ +89.76 after sell → book $9,278.39; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 633 | $1.79 | $8.28 | $+8.87 | $8,187.21 | ▲ +8.87 after sell → book $9,270.11; vs 09:30 mark -8.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 7 | $154.70 | $2.03 | $+67.08 | $9,268.08 | ▲ +67.08 after sell → book $9,268.08; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 9 | $119.43 | $2.02 | — | $8,191.19 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+20.4; leftover $1158.51 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 67 | $17.20 | $2.19 | — | $7,036.60 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $1158.51 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 5 | $216.30 | $2.00 | — | $5,953.09 | — | combo gate; gate vol=good,blue=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1158.51 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 104 | $11.13 | $2.30 | — | $4,793.27 | — | combo gate; gate vol=good,blue=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1158.51 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 469 | $2.47 | $6.05 | — | $3,628.79 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $1158.51 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 600 | $1.93 | $7.74 | — | $2,463.05 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $1158.51 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 19 | $59.72 | $2.05 | — | $1,326.33 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1158.51 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 877 | $1.32 | $11.31 | — | $157.37 | — | combo gate; gate vol=good,blue=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1158.51 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $157.37 | ▲ close $9,465.13 vs 09:30 $9,291.68 (session +232.72) | 16:00 close · cash $157.37 · equity $9,465.13 vs 09:30 $9,291.68 (+173.45; session marks +232.72) · 8 name(s) marked open→close (per-name table). AU×9 09:30 $119.43 → close $121.22 +16.11; AUPH×67 09:30 $17.20 → close $16.65 -36.85; AEM×5 09:30 $216.30 → close $216.06 -1.20; ARCT×104 09:30 $11.13 → close $13.45 +241.28; AUTL×469 09:30 $2.47 → close $2.41 -28.14; CRDL×600 09:30 $1.93 → close $1.86 -42.00; CRSP×19 09:30 $59.72 → close $59.50 -4.18; CYPH×877 09:30 $1.32 → close $1.42 +87.70 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $157.37 | ▲ 09:30 equity $9,769.02 vs yday $9,465.13 (+303.89) | 09:30 open · cash $157.37 (unchanged overnight, no fees) · equity $9,769.02 vs prior close $9,465.13 (+303.89) · 8 name(s) re-marked at the open (per-name table). AU×9 yday $121.22 → 09:30 $120.50 -6.48; AUPH×67 yday $16.65 → 09:30 $16.60 -3.35; AEM×5 yday $216.06 → 09:30 $217.03 +4.85; ARCT×104 yday $13.45 → 09:30 $13.26 -19.76; AUTL×469 yday $2.41 → 09:30 $2.36 -23.45; CRDL×600 yday $1.86 → 09:30 $1.87 +6.00; CRSP×19 yday $59.50 → 09:30 $58.79 -13.49; CYPH×877 yday $1.42 → 09:30 $1.83 +359.57 | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 9 | $120.50 | $2.04 | $+5.58 | $1,239.83 | ▲ +5.58 after sell → book $9,766.98; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 67 | $16.60 | $2.21 | $-44.60 | $2,349.82 | ▼ -44.60 after sell → book $9,764.77; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 5 | $217.03 | $2.02 | $-0.38 | $3,432.95 | ▼ -0.38 after sell → book $9,762.75; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 104 | $13.26 | $2.33 | $+216.89 | $4,809.66 | ▲ +216.89 after sell → book $9,760.42; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 469 | $2.36 | $6.14 | $-63.78 | $5,910.36 | ▼ -63.78 after sell → book $9,754.28; vs 09:30 mark -6.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRDL` | 600 | $1.87 | $7.85 | $-51.59 | $7,024.51 | ▼ -51.59 after sell → book $9,746.43; vs 09:30 mark -7.85 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 19 | $58.79 | $2.07 | $-21.78 | $8,139.45 | ▼ -21.78 after sell → book $9,744.36; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 877 | $1.83 | $11.47 | $+424.48 | $9,732.89 | ▲ +424.48 after sell → book $9,732.89; vs 09:30 mark -11.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,732.89 | ▲ close $9,732.89 vs 09:30 $9,769.02 (session +0.00) | 16:00 close · cash $9,732.89 · no lots left · equity $9,732.89. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,732.89 | ▲ 09:30 equity $9,732.89 vs yday $9,732.89 (+0.00) | 09:30 open · cash $9,732.89 · no holdings · equity $9,732.89 vs prior close $9,732.89 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 750 | $1.62 | $9.68 | — | $8,508.22 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $1216.61 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 608 | $2.00 | $7.84 | — | $7,284.37 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.8; leftover $1216.61 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `PUSA` | 328 | $3.70 | $4.23 | — | $6,066.54 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.8; leftover $1216.61 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 233 | $5.22 | $3.01 | — | $4,847.28 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+9.2; leftover $1216.61 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 179 | $6.79 | $2.53 | — | $3,629.34 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.4; leftover $1216.61 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 190 | $6.38 | $2.56 | — | $2,414.58 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer; 🔵; ⚪; ret5=+5.0; leftover $1216.61 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SUJA` | 138 | $8.79 | $2.40 | — | $1,199.16 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.5; leftover $1216.61 | join🟡 sector🟡 gen🟡 news🟡 digest🟡 ab🟡 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 699 | $1.70 | $9.02 | — | $1.84 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+120.5; leftover $1216.61 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.84 | ▲ close $9,790.12 vs 09:30 $9,732.89 (session +98.49) | 16:00 close · cash $1.84 · equity $9,790.12 vs 09:30 $9,732.89 (+57.23; session marks +98.49) · 8 name(s) marked open→close (per-name table). BMEA×750 09:30 $1.62 → close $1.61 -7.50; NPWR×608 09:30 $2.00 → close $2.02 +12.16; PUSA×328 09:30 $3.70 → close $3.91 +68.88; ALVO×233 09:30 $5.22 → close $5.25 +6.99; CAPR×179 09:30 $6.79 → close $7.19 +71.60; ZURA×190 09:30 $6.38 → close $6.50 +22.80; SUJA×138 09:30 $8.79 → close $8.54 -34.50; CYPH×699 09:30 $1.70 → close $1.64 -41.94 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.84 | ▲ 09:30 equity $9,790.12 vs yday $9,790.12 (-0.00) | 09:30 open · cash $1.84 (unchanged overnight, no fees) · equity $9,790.12 vs prior close $9,790.12 (-0.00) · 8 name(s) re-marked at the open (per-name table). BMEA×750 yday $1.61 → 09:30 $1.61 +0.00; NPWR×608 yday $2.02 → 09:30 $2.02 +0.00; PUSA×328 yday $3.91 → 09:30 $3.91 +0.00; ALVO×233 yday $5.25 → 09:30 $5.25 +0.00; CAPR×179 yday $7.19 → 09:30 $7.19 +0.00; ZURA×190 yday $6.50 → 09:30 $6.50 +0.00; SUJA×138 yday $8.54 → 09:30 $8.54 +0.00; CYPH×699 yday $1.64 → 09:30 $1.64 +0.00 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.84 | ▲ close $9,790.12 vs 09:30 $9,790.12 (session +0.00) | 16:00 close · cash $1.84 · equity $9,790.12 vs 09:30 $9,790.12 (-0.00; session marks +0.00) · 8 name(s) marked open→close (per-name table). BMEA×750 09:30 $1.61 → close $1.61 +0.00; NPWR×608 09:30 $2.02 → close $2.02 +0.00; PUSA×328 09:30 $3.91 → close $3.91 +0.00; ALVO×233 09:30 $5.25 → close $5.25 +0.00; CAPR×179 09:30 $7.19 → close $7.19 +0.00; ZURA×190 09:30 $6.50 → close $6.50 +0.00; SUJA×138 09:30 $8.54 → close $8.54 +0.00; CYPH×699 09:30 $1.64 → close $1.64 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.84 | ▲ 09:30 equity $9,970.47 vs yday $9,790.12 (+180.35) | 09:30 open · cash $1.84 (unchanged overnight, no fees) · equity $9,970.47 vs prior close $9,790.12 (+180.35) · 8 name(s) re-marked at the open (per-name table). BMEA×750 yday $1.61 → 09:30 $1.75 +105.00; NPWR×608 yday $2.02 → 09:30 $1.93 -54.72; PUSA×328 yday $3.91 → 09:30 $3.84 -22.96; ALVO×233 yday $5.25 → 09:30 $4.98 -62.91; CAPR×179 yday $7.19 → 09:30 $8.29 +196.90; ZURA×190 yday $6.50 → 09:30 $6.13 -70.30; SUJA×138 yday $8.54 → 09:30 $9.39 +117.30; CYPH×699 yday $1.64 → 09:30 $1.60 -27.96 | — |
| 2026-08-27 09:30 ET | **SELL** | `BMEA` | 750 | $1.75 | $9.81 | $+78.02 | $1,304.53 | ▲ +78.02 after sell → book $9,960.66; vs 09:30 mark -9.81 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `NPWR` | 608 | $1.93 | $7.95 | $-58.36 | $2,470.01 | ▼ -58.36 after sell → book $9,952.70; vs 09:30 mark -7.96 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `PUSA` | 328 | $3.84 | $4.30 | $+37.39 | $3,725.24 | ▲ +37.39 after sell → book $9,948.41; vs 09:30 mark -4.29 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ALVO` | 233 | $4.98 | $3.05 | $-61.98 | $4,882.52 | ▼ -61.98 after sell → book $9,945.35; vs 09:30 mark -3.06 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CAPR` | 179 | $8.29 | $2.57 | $+263.40 | $6,363.87 | ▲ +263.40 after sell → book $9,942.79; vs 09:30 mark -2.56 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ZURA` | 190 | $6.13 | $2.60 | $-52.66 | $7,525.96 | ▼ -52.66 after sell → book $9,940.18; vs 09:30 mark -2.61 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SUJA` | 138 | $9.39 | $2.44 | $+77.96 | $8,819.35 | ▲ +77.96 after sell → book $9,937.75; vs 09:30 mark -2.43 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 699 | $1.60 | $9.14 | $-88.06 | $9,928.60 | ▼ -88.06 after sell → book $9,928.60; vs 09:30 mark -9.15 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,928.60 | ▲ close $9,928.60 vs 09:30 $9,970.47 (session +0.00) | 16:00 close · cash $9,928.60 · no lots left · equity $9,928.60. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,928.60 | ▲ 09:30 equity $9,928.60 vs yday $9,928.60 (+0.00) | 09:30 open · cash $9,928.60 · no holdings · equity $9,928.60 vs prior close $9,928.60 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 17 | $144.70 | $2.04 | — | $7,466.66 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $2482.15 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 73 | $33.78 | $2.21 | — | $4,998.51 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.9; leftover $2482.15 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 16 | $149.40 | $2.04 | — | $2,606.08 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=-11.6; leftover $2482.15 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 30 | $82.70 | $2.08 | — | $123.00 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ret5=-4.6; leftover $2482.15 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $123.00 | ▼ close $9,689.56 vs 09:30 $9,928.60 (session -230.68) | 16:00 close · cash $123.00 · equity $9,689.56 vs 09:30 $9,928.60 (-239.04; session marks -230.68) · 4 name(s) marked open→close (per-name table). ANF×17 09:30 $144.70 → close $145.75 +17.85; SEDG×73 09:30 $33.78 → close $33.51 -19.71; SMTC×16 09:30 $149.40 → close $142.43 -111.52; URBN×30 09:30 $82.70 → close $78.79 -117.30 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $123.00 | ▼ 09:30 equity $9,511.23 vs yday $9,689.56 (-178.33) | 09:30 open · cash $123.00 (unchanged overnight, no fees) · equity $9,511.23 vs prior close $9,689.56 (-178.33) · 4 name(s) re-marked at the open (per-name table). ANF×17 yday $145.75 → 09:30 $148.67 +49.64; SEDG×73 yday $33.51 → 09:30 $31.50 -146.73; SMTC×16 yday $142.43 → 09:30 $133.04 -150.24; URBN×30 yday $78.79 → 09:30 $81.09 +69.00 | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 17 | $148.67 | $2.07 | $+63.38 | $2,648.31 | ▲ +63.38 after sell → book $9,509.15; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 73 | $31.50 | $2.24 | $-170.89 | $4,945.57 | ▼ -170.89 after sell → book $9,506.91; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 16 | $133.04 | $2.06 | $-265.86 | $7,072.15 | ▼ -265.86 after sell → book $9,504.85; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `URBN` | 30 | $81.09 | $2.11 | $-52.49 | $9,502.74 | ▼ -52.49 after sell → book $9,502.74; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,502.74 | ▲ close $9,502.74 vs 09:30 $9,511.23 (session +0.00) | 16:00 close · cash $9,502.74 · no lots left · equity $9,502.74. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,502.74 | ▲ 09:30 equity $9,502.74 vs yday $9,502.74 (+0.00) | 09:30 open · cash $9,502.74 · no holdings · equity $9,502.74 vs prior close $9,502.74 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,502.74 | ▲ close $9,502.74 vs 09:30 $9,502.74 (session +0.00) | 16:00 close · cash $9,502.74 · no lots left · equity $9,502.74. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,502.74 | ▲ 09:30 equity $9,502.74 vs yday $9,502.74 (+0.00) | 09:30 open · cash $9,502.74 · no holdings · equity $9,502.74 vs prior close $9,502.74 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,502.74 | ▲ close $9,502.74 vs 09:30 $9,502.74 (session +0.00) | 16:00 close · cash $9,502.74 · no lots left · equity $9,502.74. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,502.74 | ▲ 09:30 equity $9,502.74 vs yday $9,502.74 (+0.00) | 09:30 open · cash $9,502.74 · no holdings · equity $9,502.74 vs prior close $9,502.74 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $125.94 | $2.02 | — | $8,367.26 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1187.84 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 75 | $15.70 | $2.21 | — | $7,187.55 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1187.84 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 52 | $22.78 | $2.15 | — | $6,000.84 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1187.84 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CTMX` | 319 | $3.72 | $4.12 | — | $4,810.05 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer; 🔵; ret5=-2.4; leftover $1187.84 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 549 | $2.16 | $7.08 | — | $3,617.12 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.3; leftover $1187.84 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DEFT` | 1772 | $0.67 | $17.19 | — | $2,412.70 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.6; leftover $1187.84 | join🟡 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 7 | $151.40 | $2.01 | — | $1,350.89 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ret5=+129.2; leftover $1187.84 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 72 | $16.46 | $2.21 | — | $163.56 | — | combo gate; gate vol=good,blue=True; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+63.4; leftover $1187.84 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $163.56 | ▲ close $9,533.80 vs 09:30 $9,502.74 (session +70.04) | 16:00 close · cash $163.56 · equity $9,533.80 vs 09:30 $9,502.74 (+31.06; session marks +70.04) · 8 name(s) marked open→close (per-name table). RVTY×9 09:30 $125.94 → close $130.94 +45.00; CRK×75 09:30 $15.70 → close $15.54 -12.00; MMED×52 09:30 $22.78 → close $23.76 +50.96; CTMX×319 09:30 $3.72 → close $3.72 +0.00; CRDL×549 09:30 $2.16 → close $2.17 +5.49; DEFT×1772 09:30 $0.67 → close $0.65 -35.44; MRNA×7 09:30 $151.40 → close $150.81 -4.13; ARCT×72 09:30 $16.46 → close $16.74 +20.16 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $163.56 | ▼ 09:30 equity $9,523.70 vs yday $9,533.80 (-10.10) | 09:30 open · cash $163.56 (unchanged overnight, no fees) · equity $9,523.70 vs prior close $9,533.80 (-10.10) · 8 name(s) re-marked at the open (per-name table). RVTY×9 yday $130.94 → 09:30 $132.45 +13.59; CRK×75 yday $15.54 → 09:30 $15.45 -6.75; MMED×52 yday $23.76 → 09:30 $23.88 +6.24; CTMX×319 yday $3.72 → 09:30 $3.73 +3.19; CRDL×549 yday $2.17 → 09:30 $2.18 +5.49; DEFT×1772 yday $0.65 → 09:30 $0.65 +0.00; MRNA×7 yday $150.81 → 09:30 $145.95 -34.02; ARCT×72 yday $16.74 → 09:30 $16.77 +2.16 | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 9 | $132.45 | $2.04 | $+54.54 | $1,353.57 | ▲ +54.54 after sell → book $9,521.66; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 75 | $15.45 | $2.24 | $-23.20 | $2,510.09 | ▼ -23.20 after sell → book $9,519.43; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 52 | $23.88 | $2.17 | $+52.89 | $3,749.68 | ▲ +52.89 after sell → book $9,517.26; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CTMX` | 319 | $3.73 | $4.18 | $-5.10 | $4,935.37 | ▼ -5.10 after sell → book $9,513.08; vs 09:30 mark -4.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRDL` | 549 | $2.18 | $7.18 | $-3.29 | $6,125.01 | ▼ -3.29 after sell → book $9,505.90; vs 09:30 mark -7.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DEFT` | 1772 | $0.65 | $17.14 | $-69.77 | $7,259.67 | ▼ -69.77 after sell → book $9,488.76; vs 09:30 mark -17.14 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `MRNA` | 7 | $145.95 | $2.03 | $-42.19 | $8,279.29 | ▼ -42.19 after sell → book $9,486.73; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 72 | $16.77 | $2.23 | $+17.89 | $9,484.50 | ▲ +17.89 after sell → book $9,484.50; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CABA` | 326 | $3.63 | $4.21 | — | $8,296.92 | — | combo gate; gate vol=good,blue=True; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1185.56 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `GPRO` | 666 | $1.78 | $8.59 | — | $7,102.84 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+5.9; leftover $1185.56 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `EOSE` | 332 | $3.57 | $4.28 | — | $5,913.32 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.2; leftover $1185.56 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 2 | $486.31 | $2.00 | — | $4,938.70 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-9.9; leftover $1185.56 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MLYS` | 40 | $29.15 | $2.11 | — | $3,770.59 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.0; leftover $1185.56 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `CCOI` | 116 | $10.22 | $2.34 | — | $2,582.74 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.9; leftover $1185.56 | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `IRD` | 254 | $4.66 | $3.28 | — | $1,395.82 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+12.2; leftover $1185.56 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 233 | $5.08 | $3.01 | — | $209.17 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+28.1; leftover $1185.56 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $209.17 | ▼ close $9,027.80 vs 09:30 $9,523.70 (session -426.89) | 16:00 close · cash $209.17 · equity $9,027.80 vs 09:30 $9,523.70 (-495.90; session marks -426.89) · 8 name(s) marked open→close (per-name table). CABA×326 09:30 $3.63 → close $3.48 -48.90; GPRO×666 09:30 $1.78 → close $1.39 -259.74; EOSE×332 09:30 $3.57 → close $3.50 -23.24; DELL×2 09:30 $486.31 → close $516.39 +60.16; MLYS×40 09:30 $29.15 → close $28.27 -35.20; CCOI×116 09:30 $10.22 → close $9.98 -27.84; IRD×254 09:30 $4.66 → close $4.60 -15.24; OABI×233 09:30 $5.08 → close $4.75 -76.89 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SECZ` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `UGI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DVLT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `NMAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TRGP` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `TMC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ELMT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ERO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SBSW` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SCCO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HMY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-26 | `BMEA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `NPWR` | no_price | no 09:30 open — carry |
| 2026-08-26 | `PUSA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ALVO` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CAPR` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ZURA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `SUJA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CYPH` | no_price | no 09:30 open — carry |
| 2026-08-26 | `SLQT` | no_price | no 09:30 open |
| 2026-08-26 | `USDE` | no_price | no 09:30 open |
| 2026-08-26 | `DKS` | no_price | no 09:30 open |
| 2026-08-31 | `VEEV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SRPT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRVO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `YDDL` | hard_red | hard-red S=-3.83 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `CABA` | 326 | 2026-09-04 @ $3.63 | combo gate; gate vol=good,blue=True; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1185.56 |
| `GPRO` | 666 | 2026-09-04 @ $1.78 | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+5.9; leftover $1185.56 |
| `EOSE` | 332 | 2026-09-04 @ $3.57 | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.2; leftover $1185.56 |
| `DELL` | 2 | 2026-09-04 @ $486.31 | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-9.9; leftover $1185.56 |
| `MLYS` | 40 | 2026-09-04 @ $29.15 | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.0; leftover $1185.56 |
| `CCOI` | 116 | 2026-09-04 @ $10.22 | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.9; leftover $1185.56 |
| `IRD` | 254 | 2026-09-04 @ $4.66 | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+12.2; leftover $1185.56 |
| `OABI` | 233 | 2026-09-04 @ $5.08 | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+28.1; leftover $1185.56 |
