# Factor mine action — `union_join_vol_green_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **-2.54%** ($9,746) · signal-only (no cash/fees) was -4.08%. Starts YES **5/17**. Fills 104 · skips 21 · realized $-146.75.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `join=good,vol=good,last_green=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $363.12.

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
| 2026-08-14 | `AIRO` | 112 | — | $11.12 | +0.00 | $9.57 | -173.60 | -173.60 | +0.00 | -173.60 |
| 2026-08-14 | `NCMI` | 464 | — | $2.69 | +0.00 | $2.86 | +78.88 | +78.88 | +0.00 | +78.88 |
| 2026-08-14 | `QMLS` | 170 | — | $7.29 | +0.00 | $7.32 | +5.10 | +5.10 | +0.00 | +5.10 |
| 2026-08-17 | `BTBT` | 833 | $1.57 | $1.52 | -41.65 | — | +0.00 | -41.65 | +16.66 | — |
| 2026-08-17 | `BETR` | 84 | $13.73 | $13.67 | -5.04 | — | +0.00 | -5.04 | -94.92 | — |
| 2026-08-17 | `ANGX` | 290 | $4.37 | $4.60 | +66.70 | — | +0.00 | +66.70 | +84.10 | — |
| 2026-08-17 | `HYLN` | 299 | $4.06 | $4.10 | +11.96 | — | +0.00 | +11.96 | -23.92 | — |
| 2026-08-17 | `ADUR` | 75 | $16.17 | $15.73 | -33.00 | — | +0.00 | -33.00 | -57.75 | — |
| 2026-08-17 | `AIRO` | 112 | $9.57 | $9.57 | +0.00 | — | +0.00 | +0.00 | -173.60 | — |
| 2026-08-17 | `NCMI` | 464 | $2.86 | $2.80 | -27.84 | — | +0.00 | -27.84 | +51.04 | — |
| 2026-08-17 | `QMLS` | 170 | $7.32 | $7.24 | -13.60 | — | +0.00 | -13.60 | -8.50 | — |
| 2026-08-17 | `ABX` | 213 | — | $9.12 | +0.00 | $9.12 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-17 | `ALOY` | 132 | — | $14.66 | +0.00 | $13.86 | -106.26 | -106.26 | +0.00 | -106.26 |
| 2026-08-17 | `BORR` | 423 | — | $4.59 | +0.00 | $4.50 | -38.07 | -38.07 | +0.00 | -38.07 |
| 2026-08-17 | `XHG` | 464 | — | $4.19 | +0.00 | $3.91 | -129.92 | -129.92 | +0.00 | -129.92 |
| 2026-08-17 | `MP` | 33 | — | $58.01 | +0.00 | $58.51 | +16.50 | +16.50 | +0.00 | +16.50 |
| 2026-08-18 | `ABX` | 213 | $9.12 | $9.03 | -19.17 | — | +0.00 | -19.17 | -19.17 | — |
| 2026-08-18 | `ALOY` | 132 | $13.86 | $13.19 | -87.78 | — | +0.00 | -87.78 | -194.04 | — |
| 2026-08-18 | `BORR` | 423 | $4.50 | $4.56 | +25.38 | — | +0.00 | +25.38 | -12.69 | — |
| 2026-08-18 | `XHG` | 464 | $3.91 | $3.94 | +13.92 | — | +0.00 | +13.92 | -116.00 | — |
| 2026-08-18 | `MP` | 33 | $58.51 | $56.35 | -71.28 | — | +0.00 | -71.28 | -54.78 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `AG` | 56 | — | $20.55 | +0.00 | $21.19 | +35.84 | +35.84 | +0.00 | +35.84 |
| 2026-08-20 | `CDE` | 56 | — | $20.65 | +0.00 | $21.11 | +25.76 | +25.76 | +0.00 | +25.76 |
| 2026-08-20 | `HDSN` | 201 | — | $5.77 | +0.00 | $5.57 | -40.20 | -40.20 | +0.00 | -40.20 |
| 2026-08-20 | `IAG` | 59 | — | $19.63 | +0.00 | $20.50 | +51.33 | +51.33 | +0.00 | +51.33 |
| 2026-08-20 | `KGC` | 39 | — | $29.63 | +0.00 | $31.43 | +70.20 | +70.20 | +0.00 | +70.20 |
| 2026-08-20 | `NFGC` | 663 | — | $1.75 | +0.00 | $1.75 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `WPM` | 8 | — | $144.54 | +0.00 | $150.25 | +45.68 | +45.68 | +0.00 | +45.68 |
| 2026-08-20 | `ABUS` | 236 | — | $4.92 | +0.00 | $4.77 | -35.40 | -35.40 | +0.00 | -35.40 |
| 2026-08-21 | `AG` | 56 | $21.19 | $21.90 | +39.76 | — | +0.00 | +39.76 | +75.60 | — |
| 2026-08-21 | `CDE` | 56 | $21.11 | $21.75 | +35.84 | — | +0.00 | +35.84 | +61.60 | — |
| 2026-08-21 | `HDSN` | 201 | $5.57 | $5.67 | +20.10 | — | +0.00 | +20.10 | -20.10 | — |
| 2026-08-21 | `IAG` | 59 | $20.50 | $21.17 | +39.53 | — | +0.00 | +39.53 | +90.86 | — |
| 2026-08-21 | `KGC` | 39 | $31.43 | $32.17 | +28.86 | — | +0.00 | +28.86 | +99.06 | — |
| 2026-08-21 | `NFGC` | 663 | $1.75 | $1.79 | +26.52 | — | +0.00 | +26.52 | +26.52 | — |
| 2026-08-21 | `WPM` | 8 | $150.25 | $154.70 | +35.60 | — | +0.00 | +35.60 | +81.28 | — |
| 2026-08-21 | `ABUS` | 236 | $4.77 | $5.20 | +101.48 | — | +0.00 | +101.48 | +66.08 | — |
| 2026-08-21 | `AU` | 10 | — | $119.43 | +0.00 | $121.22 | +17.90 | +17.90 | +0.00 | +17.90 |
| 2026-08-21 | `AUPH` | 70 | — | $17.20 | +0.00 | $16.65 | -38.50 | -38.50 | +0.00 | -38.50 |
| 2026-08-21 | `AEM` | 5 | — | $216.30 | +0.00 | $216.06 | -1.20 | -1.20 | +0.00 | -1.20 |
| 2026-08-21 | `ARCT` | 109 | — | $11.13 | +0.00 | $13.45 | +252.88 | +252.88 | +0.00 | +252.88 |
| 2026-08-21 | `CYPH` | 920 | — | $1.32 | +0.00 | $1.42 | +92.00 | +92.00 | +0.00 | +92.00 |
| 2026-08-21 | `BTBT` | 732 | — | $1.66 | +0.00 | $1.53 | -95.16 | -95.16 | +0.00 | -95.16 |
| 2026-08-21 | `INDP` | 874 | — | $1.39 | +0.00 | $1.29 | -87.40 | -87.40 | +0.00 | -87.40 |
| 2026-08-21 | `TEM` | 18 | — | $65.60 | +0.00 | $72.69 | +127.62 | +127.62 | +0.00 | +127.62 |
| 2026-08-24 | `AU` | 10 | $121.22 | $120.50 | -7.20 | — | +0.00 | -7.20 | +10.70 | — |
| 2026-08-24 | `AUPH` | 70 | $16.65 | $16.60 | -3.50 | — | +0.00 | -3.50 | -42.00 | — |
| 2026-08-24 | `AEM` | 5 | $216.06 | $217.03 | +4.85 | — | +0.00 | +4.85 | +3.65 | — |
| 2026-08-24 | `ARCT` | 109 | $13.45 | $13.26 | -20.71 | — | +0.00 | -20.71 | +232.17 | — |
| 2026-08-24 | `CYPH` | 920 | $1.42 | $1.83 | +377.20 | — | +0.00 | +377.20 | +469.20 | — |
| 2026-08-24 | `BTBT` | 732 | $1.53 | $1.55 | +14.64 | — | +0.00 | +14.64 | -80.52 | — |
| 2026-08-24 | `INDP` | 874 | $1.29 | $1.24 | -43.70 | — | +0.00 | -43.70 | -131.10 | — |
| 2026-08-24 | `TEM` | 18 | $72.69 | $70.07 | -47.16 | — | +0.00 | -47.16 | +80.46 | — |
| 2026-08-25 | `ZURA` | 199 | — | $6.38 | +0.00 | $6.50 | +23.88 | +23.88 | +0.00 | +23.88 |
| 2026-08-25 | `CYPH` | 748 | — | $1.70 | +0.00 | $1.64 | -44.88 | -44.88 | +0.00 | -44.88 |
| 2026-08-25 | `DEFT` | 1987 | — | $0.64 | +0.00 | $0.62 | -39.74 | -39.74 | +0.00 | -39.74 |
| 2026-08-25 | `GORO` | 360 | — | $3.53 | +0.00 | $3.56 | +10.80 | +10.80 | +0.00 | +10.80 |
| 2026-08-25 | `EZPW` | 36 | — | $34.48 | +0.00 | $34.69 | +7.56 | +7.56 | +0.00 | +7.56 |
| 2026-08-25 | `ERO` | 33 | — | $38.00 | +0.00 | $38.55 | +18.15 | +18.15 | +0.00 | +18.15 |
| 2026-08-25 | `WPM` | 7 | — | $160.00 | +0.00 | $158.25 | -12.25 | -12.25 | +0.00 | -12.25 |
| 2026-08-25 | `FCX` | 16 | — | $77.90 | +0.00 | $77.49 | -6.56 | -6.56 | +0.00 | -6.56 |
| 2026-08-26 | `ZURA` | 199 | $6.50 | $6.50 | +0.00 | $6.50 | +0.00 | +0.00 | +23.88 | +23.88 |
| 2026-08-26 | `CYPH` | 748 | $1.64 | $1.64 | +0.00 | $1.64 | +0.00 | +0.00 | -44.88 | -44.88 |
| 2026-08-26 | `DEFT` | 1987 | $0.62 | $0.62 | +0.00 | $0.62 | +0.00 | +0.00 | -39.74 | -39.74 |
| 2026-08-26 | `GORO` | 360 | $3.56 | $3.56 | +0.00 | $3.56 | +0.00 | +0.00 | +10.80 | +10.80 |
| 2026-08-26 | `EZPW` | 36 | $34.69 | $34.69 | +0.00 | $34.69 | +0.00 | +0.00 | +7.56 | +7.56 |
| 2026-08-26 | `ERO` | 33 | $38.55 | $38.55 | +0.00 | $38.55 | +0.00 | +0.00 | +18.15 | +18.15 |
| 2026-08-26 | `WPM` | 7 | $158.25 | $158.25 | +0.00 | $158.25 | +0.00 | +0.00 | -12.25 | -12.25 |
| 2026-08-26 | `FCX` | 16 | $77.49 | $77.49 | +0.00 | $77.49 | +0.00 | +0.00 | -6.56 | -6.56 |
| 2026-08-27 | `ZURA` | 199 | $6.50 | $6.13 | -73.63 | — | +0.00 | -73.63 | -49.75 | — |
| 2026-08-27 | `CYPH` | 748 | $1.64 | $1.60 | -29.92 | — | +0.00 | -29.92 | -74.80 | — |
| 2026-08-27 | `DEFT` | 1987 | $0.62 | $0.60 | -39.74 | — | +0.00 | -39.74 | -79.48 | — |
| 2026-08-27 | `GORO` | 360 | $3.56 | $3.77 | +75.60 | — | +0.00 | +75.60 | +86.40 | — |
| 2026-08-27 | `EZPW` | 36 | $34.69 | $35.70 | +36.36 | — | +0.00 | +36.36 | +43.92 | — |
| 2026-08-27 | `ERO` | 33 | $38.55 | $40.51 | +64.68 | — | +0.00 | +64.68 | +82.83 | — |
| 2026-08-27 | `WPM` | 7 | $158.25 | $160.93 | +18.76 | — | +0.00 | +18.76 | +6.51 | — |
| 2026-08-27 | `FCX` | 16 | $77.49 | $79.34 | +29.60 | — | +0.00 | +29.60 | +23.04 | — |
| 2026-08-28 | `ANF` | 17 | — | $144.70 | +0.00 | $145.75 | +17.85 | +17.85 | +0.00 | +17.85 |
| 2026-08-28 | `BZ` | 136 | — | $18.50 | +0.00 | $18.00 | -68.00 | -68.00 | +0.00 | -68.00 |
| 2026-08-28 | `URBN` | 30 | — | $82.70 | +0.00 | $78.79 | -117.30 | -117.30 | +0.00 | -117.30 |
| 2026-08-28 | `TIGR` | 461 | — | $5.49 | +0.00 | $5.06 | -198.23 | -198.23 | +0.00 | -198.23 |
| 2026-08-31 | `ANF` | 17 | $145.75 | $148.67 | +49.64 | — | +0.00 | +49.64 | +67.49 | — |
| 2026-08-31 | `BZ` | 136 | $18.00 | $17.89 | -14.96 | — | +0.00 | -14.96 | -82.96 | — |
| 2026-08-31 | `URBN` | 30 | $78.79 | $81.09 | +69.00 | — | +0.00 | +69.00 | -48.30 | — |
| 2026-08-31 | `TIGR` | 461 | $5.06 | $4.96 | -46.10 | — | +0.00 | -46.10 | -244.33 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `RVTY` | 9 | — | $125.94 | +0.00 | $130.94 | +45.00 | +45.00 | +0.00 | +45.00 |
| 2026-09-03 | `CRK` | 77 | — | $15.70 | +0.00 | $15.54 | -12.32 | -12.32 | +0.00 | -12.32 |
| 2026-09-03 | `MMED` | 53 | — | $22.78 | +0.00 | $23.76 | +51.94 | +51.94 | +0.00 | +51.94 |
| 2026-09-03 | `MRNA` | 8 | — | $151.40 | +0.00 | $150.81 | -4.72 | -4.72 | +0.00 | -4.72 |
| 2026-09-03 | `ARCT` | 74 | — | $16.46 | +0.00 | $16.74 | +20.72 | +20.72 | +0.00 | +20.72 |
| 2026-09-03 | `NVAX` | 119 | — | $10.27 | +0.00 | $10.32 | +5.95 | +5.95 | +0.00 | +5.95 |
| 2026-09-03 | `ALMS` | 120 | — | $10.15 | +0.00 | $10.35 | +24.00 | +24.00 | +0.00 | +24.00 |
| 2026-09-03 | `OSW` | 54 | — | $22.53 | +0.00 | $21.90 | -34.02 | -34.02 | +0.00 | -34.02 |
| 2026-09-04 | `RVTY` | 9 | $130.94 | $132.45 | +13.59 | — | +0.00 | +13.59 | +58.59 | — |
| 2026-09-04 | `CRK` | 77 | $15.54 | $15.45 | -6.93 | — | +0.00 | -6.93 | -19.25 | — |
| 2026-09-04 | `MMED` | 53 | $23.76 | $23.88 | +6.36 | — | +0.00 | +6.36 | +58.30 | — |
| 2026-09-04 | `MRNA` | 8 | $150.81 | $145.95 | -38.88 | — | +0.00 | -38.88 | -43.60 | — |
| 2026-09-04 | `ARCT` | 74 | $16.74 | $16.77 | +2.22 | — | +0.00 | +2.22 | +22.94 | — |
| 2026-09-04 | `NVAX` | 119 | $10.32 | $10.41 | +10.71 | — | +0.00 | +10.71 | +16.66 | — |
| 2026-09-04 | `ALMS` | 120 | $10.35 | $10.38 | +3.60 | — | +0.00 | +3.60 | +27.60 | — |
| 2026-09-04 | `OSW` | 54 | $21.90 | $22.00 | +5.40 | — | +0.00 | +5.40 | -28.62 | — |
| 2026-09-04 | `DELL` | 3 | — | $486.31 | +0.00 | $516.39 | +90.24 | +90.24 | +0.00 | +90.24 |
| 2026-09-04 | `OABI` | 323 | — | $5.08 | +0.00 | $4.75 | -106.59 | -106.59 | +0.00 | -106.59 |
| 2026-09-04 | `ALEC` | 608 | — | $2.70 | +0.00 | $2.51 | -115.52 | -115.52 | +0.00 | -115.52 |
| 2026-09-04 | `TARS` | 19 | — | $82.76 | +0.00 | $83.21 | +8.55 | +8.55 | +0.00 | +8.55 |
| 2026-09-04 | `MDB` | 4 | — | $378.76 | +0.00 | $384.45 | +22.76 | +22.76 | +0.00 | +22.76 |
| 2026-09-04 | `TRLV` | 138 | — | $11.89 | +0.00 | $11.99 | +13.80 | +13.80 | +0.00 | +13.80 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | -164.42 | BTBT, BETR, ANGX, HYLN, ADUR, AIRO, NCMI, QMLS | — | $3.57 | $9,801.97 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, AIRO×112, NCMI×464, QMLS×170 |
| 2026-08-17 | +2.25 | $3.57 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, AIRO×112, NCMI×464, QMLS×170 | $9,759.50 | -42.47 | -257.75 | ABX, ALOY, BORR, XHG, MP | BTBT, BETR, ANGX, HYLN, ADUR, AIRO, NCMI, QMLS | $29.01 | $9,449.00 | ABX×213, ALOY×132, BORR×423, XHG×464, MP×33 |
| 2026-08-18 | -6.20 | $29.01 | ABX×213, ALOY×132, BORR×423, XHG×464, MP×33 | $9,310.07 | -138.93 | +0.00 | — | ABX, ALOY, BORR, XHG, MP | $9,291.12 | $9,291.12 | — |
| 2026-08-19 | -7.20 | $9,291.12 | — | $9,291.12 | -0.00 | +0.00 | — | — | $9,291.12 | $9,291.12 | — |
| 2026-08-20 | +1.12 | $9,291.12 | — | $9,291.12 | -0.00 | +153.21 | AG, CDE, HDSN, IAG, KGC, NFGC, WPM, ABUS | — | $7.92 | $9,419.53 | AG×56, CDE×56, HDSN×201, IAG×59, KGC×39, NFGC×663, WPM×8, ABUS×236 |
| 2026-08-21 | +3.25 | $7.92 | AG×56, CDE×56, HDSN×201, IAG×59, KGC×39, NFGC×663, WPM×8, ABUS×236 | $9,747.22 | +327.69 | +268.14 | AU, AUPH, AEM, ARCT, CYPH, BTBT, INDP, TEM | AG, CDE, HDSN, IAG, KGC, NFGC, WPM, ABUS | $160.79 | $9,947.08 | AU×10, AUPH×70, AEM×5, ARCT×109, CYPH×920, BTBT×732, INDP×874, TEM×18 |
| 2026-08-24 | -5.17 | $160.79 | AU×10, AUPH×70, AEM×5, ARCT×109, CYPH×920, BTBT×732, INDP×874, TEM×18 | $10,221.50 | +274.42 | +0.00 | — | AU, AUPH, AEM, ARCT, CYPH, BTBT, INDP, TEM | $10,177.76 | $10,177.76 | — |
| 2026-08-25 | +1.80 | $10,177.76 | — | $10,177.76 | +0.00 | -43.04 | ZURA, CYPH, DEFT, GORO, EZPW, ERO, WPM, FCX | — | $188.59 | $10,090.93 | ZURA×199, CYPH×748, DEFT×1987, GORO×360, EZPW×36, ERO×33, WPM×7, FCX×16 |
| 2026-08-26 | +2.02 | $188.59 | ZURA×199, CYPH×748, DEFT×1987, GORO×360, EZPW×36, ERO×33, WPM×7, FCX×16 | $10,090.93 | -0.00 | +0.00 | — | — | $188.59 | $10,090.93 | ZURA×199, CYPH×748, DEFT×1987, GORO×360, EZPW×36, ERO×33, WPM×7, FCX×16 |
| 2026-08-27 | — | $188.59 | ZURA×199, CYPH×748, DEFT×1987, GORO×360, EZPW×36, ERO×33, WPM×7, FCX×16 | $10,172.64 | +81.71 | +0.00 | — | ZURA, CYPH, DEFT, GORO, EZPW, ERO, WPM, FCX | $10,128.97 | $10,128.97 | — |
| 2026-08-28 | +0.75 | $10,128.97 | — | $10,128.97 | -0.00 | -365.68 | ANF, BZ, URBN, TIGR | — | $128.71 | $9,750.82 | ANF×17, BZ×136, URBN×30, TIGR×461 |
| 2026-08-31 | -5.85 | $128.71 | ANF×17, BZ×136, URBN×30, TIGR×461 | $9,808.40 | +57.58 | +0.00 | — | ANF, BZ, URBN, TIGR | $9,795.74 | $9,795.74 | — |
| 2026-09-01 | -6.30 | $9,795.74 | — | $9,795.74 | +0.00 | +0.00 | — | — | $9,795.74 | $9,795.74 | — |
| 2026-09-02 | -3.83 | $9,795.74 | — | $9,795.74 | +0.00 | +0.00 | — | — | $9,795.74 | $9,795.74 | — |
| 2026-09-03 | -0.90 | $9,795.74 | — | $9,795.74 | +0.00 | +96.55 | RVTY, CRK, MMED, MRNA, ARCT, NVAX, ALMS, OSW | — | $142.59 | $9,874.83 | RVTY×9, CRK×77, MMED×53, MRNA×8, ARCT×74, NVAX×119, ALMS×120, OSW×54 |
| 2026-09-04 | — | $142.59 | RVTY×9, CRK×77, MMED×53, MRNA×8, ARCT×74, NVAX×119, ALMS×120, OSW×54 | $9,870.90 | -3.93 | -86.76 | DELL, OABI, ALEC, TARS, MDB, TRLV | RVTY, CRK, MMED, MRNA, ARCT, NVAX, ALMS, OSW | $363.12 | $9,746.03 | DELL×3, OABI×323, ALEC×608, TARS×19, MDB×4, TRLV×138 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 833 | $1.50 | $10.75 | — | $8,739.75 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BETR` | 84 | $14.80 | $2.24 | — | $7,494.31 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten; 🔵; ⚪; ret5=-9.9; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 290 | $4.31 | $3.74 | — | $6,240.67 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 299 | $4.18 | $3.86 | — | $4,986.99 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 75 | $16.50 | $2.21 | — | $3,747.28 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 112 | $11.12 | $2.33 | — | $2,499.51 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NCMI` | 464 | $2.69 | $5.99 | — | $1,245.37 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=-33.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `QMLS` | 170 | $7.29 | $2.50 | — | $3.57 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.1; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.57 | ▼ close $9,801.97 vs 09:30 $10,000.00 (session -164.42) | 16:00 close · cash $3.57 · equity $9,801.97 vs 09:30 $10,000.00 (-198.03; session marks -164.42) · 8 name(s) marked open→close (per-name table). BTBT×833 09:30 $1.50 → close $1.57 +58.31; BETR×84 09:30 $14.80 → close $13.73 -89.88; ANGX×290 09:30 $4.31 → close $4.37 +17.40; HYLN×299 09:30 $4.18 → close $4.06 -35.88; ADUR×75 09:30 $16.50 → close $16.17 -24.75; AIRO×112 09:30 $11.12 → close $9.57 -173.60; NCMI×464 09:30 $2.69 → close $2.86 +78.88; QMLS×170 09:30 $7.29 → close $7.32 +5.10 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.57 | ▼ 09:30 equity $9,759.50 vs yday $9,801.97 (-42.47) | 09:30 open · cash $3.57 (unchanged overnight, no fees) · equity $9,759.50 vs prior close $9,801.97 (-42.47) · 8 name(s) re-marked at the open (per-name table). BTBT×833 yday $1.57 → 09:30 $1.52 -41.65; BETR×84 yday $13.73 → 09:30 $13.67 -5.04; ANGX×290 yday $4.37 → 09:30 $4.60 +66.70; HYLN×299 yday $4.06 → 09:30 $4.10 +11.96; ADUR×75 yday $16.17 → 09:30 $15.73 -33.00; AIRO×112 yday $9.57 → 09:30 $9.57 +0.00; NCMI×464 yday $2.86 → 09:30 $2.80 -27.84; QMLS×170 yday $7.32 → 09:30 $7.24 -13.60 | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 833 | $1.52 | $10.89 | $-4.98 | $1,258.83 | ▼ -4.98 after sell → book $9,748.60; vs 09:30 mark -10.90 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BETR` | 84 | $13.67 | $2.27 | $-99.43 | $2,404.85 | ▼ -99.43 after sell → book $9,746.34; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 290 | $4.60 | $3.80 | $+76.56 | $3,735.05 | ▲ +76.56 after sell → book $9,742.54; vs 09:30 mark -3.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HYLN` | 299 | $4.10 | $3.92 | $-31.69 | $4,957.03 | ▼ -31.69 after sell → book $9,738.62; vs 09:30 mark -3.92 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ADUR` | 75 | $15.73 | $2.24 | $-62.20 | $6,134.54 | ▼ -62.20 after sell → book $9,736.38; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 112 | $9.57 | $2.35 | $-178.28 | $7,204.03 | ▼ -178.28 after sell → book $9,734.03; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NCMI` | 464 | $2.80 | $6.07 | $+38.98 | $8,497.16 | ▲ +38.98 after sell → book $9,727.96; vs 09:30 mark -6.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `QMLS` | 170 | $7.24 | $2.54 | $-13.54 | $9,725.42 | ▼ -13.54 after sell → book $9,725.42; vs 09:30 mark -2.54 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 213 | $9.12 | $2.75 | — | $7,780.11 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $1945.08 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ALOY` | 132 | $14.66 | $2.39 | — | $5,842.60 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.0; leftover $1945.08 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `BORR` | 423 | $4.59 | $5.46 | — | $3,895.58 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot; ⚪; ret5=+14.8; leftover $1945.08 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 464 | $4.19 | $5.99 | — | $1,945.43 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; ⚪; ret5=+291.8; leftover $1945.08 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `MP` | 33 | $58.01 | $2.09 | — | $29.01 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ⚪; ret5=+14.9; leftover $1945.08 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $29.01 | ▼ close $9,449.00 vs 09:30 $9,759.50 (session -257.75) | 16:00 close · cash $29.01 · equity $9,449.00 vs 09:30 $9,759.50 (-310.50; session marks -257.75) · 5 name(s) marked open→close (per-name table). ABX×213 09:30 $9.12 → close $9.12 +0.00; ALOY×132 09:30 $14.66 → close $13.86 -106.26; BORR×423 09:30 $4.59 → close $4.50 -38.07; XHG×464 09:30 $4.19 → close $3.91 -129.92; MP×33 09:30 $58.01 → close $58.51 +16.50 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $29.01 | ▼ 09:30 equity $9,310.07 vs yday $9,449.00 (-138.93) | 09:30 open · cash $29.01 (unchanged overnight, no fees) · equity $9,310.07 vs prior close $9,449.00 (-138.93) · 5 name(s) re-marked at the open (per-name table). ABX×213 yday $9.12 → 09:30 $9.03 -19.17; ALOY×132 yday $13.86 → 09:30 $13.19 -87.78; BORR×423 yday $4.50 → 09:30 $4.56 +25.38; XHG×464 yday $3.91 → 09:30 $3.94 +13.92; MP×33 yday $58.51 → 09:30 $56.35 -71.28 | — |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 213 | $9.03 | $2.80 | $-24.72 | $1,949.60 | ▼ -24.72 after sell → book $9,307.27; vs 09:30 mark -2.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALOY` | 132 | $13.19 | $2.42 | $-198.85 | $3,688.26 | ▼ -198.85 after sell → book $9,304.85; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `BORR` | 423 | $4.56 | $5.54 | $-23.69 | $5,611.60 | ▼ -23.69 after sell → book $9,299.31; vs 09:30 mark -5.54 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `XHG` | 464 | $3.94 | $6.08 | $-128.06 | $7,433.68 | ▼ -128.06 after sell → book $9,293.23; vs 09:30 mark -6.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `MP` | 33 | $56.35 | $2.11 | $-58.98 | $9,291.12 | ▼ -58.98 after sell → book $9,291.12; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,291.12 | ▲ close $9,291.12 vs 09:30 $9,310.07 (session +0.00) | 16:00 close · cash $9,291.12 · no lots left · equity $9,291.12. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,291.12 | ▲ 09:30 equity $9,291.12 vs yday $9,291.12 (-0.00) | 09:30 open · cash $9,291.12 · no holdings · equity $9,291.12 vs prior close $9,291.12 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,291.12 | ▲ close $9,291.12 vs 09:30 $9,291.12 (session +0.00) | 16:00 close · cash $9,291.12 · no lots left · equity $9,291.12. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,291.12 | ▲ 09:30 equity $9,291.12 vs yday $9,291.12 (-0.00) | 09:30 open · cash $9,291.12 · no holdings · equity $9,291.12 vs prior close $9,291.12 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 56 | $20.55 | $2.16 | — | $8,138.16 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1161.39 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 56 | $20.65 | $2.16 | — | $6,979.60 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1161.39 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 201 | $5.77 | $2.60 | — | $5,817.24 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1161.39 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 59 | $19.63 | $2.17 | — | $4,656.90 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1161.39 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 39 | $29.63 | $2.11 | — | $3,499.22 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1161.39 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 663 | $1.75 | $8.55 | — | $2,330.42 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1161.39 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $1,172.08 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1161.39 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABUS` | 236 | $4.92 | $3.04 | — | $7.92 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1161.39 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7.92 | ▲ close $9,419.53 vs 09:30 $9,291.12 (session +153.21) | 16:00 close · cash $7.92 · equity $9,419.53 vs 09:30 $9,291.12 (+128.41; session marks +153.21) · 8 name(s) marked open→close (per-name table). AG×56 09:30 $20.55 → close $21.19 +35.84; CDE×56 09:30 $20.65 → close $21.11 +25.76; HDSN×201 09:30 $5.77 → close $5.57 -40.20; IAG×59 09:30 $19.63 → close $20.50 +51.33; KGC×39 09:30 $29.63 → close $31.43 +70.20; NFGC×663 09:30 $1.75 → close $1.75 +0.00; WPM×8 09:30 $144.54 → close $150.25 +45.68; ABUS×236 09:30 $4.92 → close $4.77 -35.40 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7.92 | ▲ 09:30 equity $9,747.22 vs yday $9,419.53 (+327.69) | 09:30 open · cash $7.92 (unchanged overnight, no fees) · equity $9,747.22 vs prior close $9,419.53 (+327.69) · 8 name(s) re-marked at the open (per-name table). AG×56 yday $21.19 → 09:30 $21.90 +39.76; CDE×56 yday $21.11 → 09:30 $21.75 +35.84; HDSN×201 yday $5.57 → 09:30 $5.67 +20.10; IAG×59 yday $20.50 → 09:30 $21.17 +39.53; KGC×39 yday $31.43 → 09:30 $32.17 +28.86; NFGC×663 yday $1.75 → 09:30 $1.79 +26.52; WPM×8 yday $150.25 → 09:30 $154.70 +35.60; ABUS×236 yday $4.77 → 09:30 $5.20 +101.48 | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 56 | $21.90 | $2.18 | $+71.26 | $1,232.14 | ▲ +71.26 after sell → book $9,745.04; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 56 | $21.75 | $2.18 | $+57.26 | $2,447.96 | ▲ +57.26 after sell → book $9,742.86; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 201 | $5.67 | $2.64 | $-25.34 | $3,584.99 | ▼ -25.34 after sell → book $9,740.22; vs 09:30 mark -2.64 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 59 | $21.17 | $2.19 | $+86.51 | $4,831.84 | ▲ +86.51 after sell → book $9,738.04; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 39 | $32.17 | $2.13 | $+94.83 | $6,084.34 | ▲ +94.83 after sell → book $9,735.91; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 663 | $1.79 | $8.67 | $+9.29 | $7,262.44 | ▲ +9.29 after sell → book $9,727.24; vs 09:30 mark -8.67 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 8 | $154.70 | $2.03 | $+77.23 | $8,498.00 | ▲ +77.23 after sell → book $9,725.20; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `ABUS` | 236 | $5.20 | $3.09 | $+59.94 | $9,722.11 | ▲ +59.94 after sell → book $9,722.11; vs 09:30 mark -3.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $8,525.79 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+20.4; leftover $1215.26 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 70 | $17.20 | $2.20 | — | $7,319.59 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $1215.26 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 5 | $216.30 | $2.00 | — | $6,236.08 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1215.26 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 109 | $11.13 | $2.32 | — | $5,020.60 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1215.26 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 920 | $1.32 | $11.87 | — | $3,794.33 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1215.26 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 732 | $1.66 | $9.44 | — | $2,569.77 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $1215.26 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `INDP` | 874 | $1.39 | $11.27 | — | $1,343.63 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+30.2; leftover $1215.26 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `TEM` | 18 | $65.60 | $2.04 | — | $160.79 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+22.8; leftover $1215.26 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $160.79 | ▲ close $9,947.08 vs 09:30 $9,747.22 (session +268.14) | 16:00 close · cash $160.79 · equity $9,947.08 vs 09:30 $9,747.22 (+199.86; session marks +268.14) · 8 name(s) marked open→close (per-name table). AU×10 09:30 $119.43 → close $121.22 +17.90; AUPH×70 09:30 $17.20 → close $16.65 -38.50; AEM×5 09:30 $216.30 → close $216.06 -1.20; ARCT×109 09:30 $11.13 → close $13.45 +252.88; CYPH×920 09:30 $1.32 → close $1.42 +92.00; BTBT×732 09:30 $1.66 → close $1.53 -95.16; INDP×874 09:30 $1.39 → close $1.29 -87.40; TEM×18 09:30 $65.60 → close $72.69 +127.62 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $160.79 | ▲ 09:30 equity $10,221.50 vs yday $9,947.08 (+274.42) | 09:30 open · cash $160.79 (unchanged overnight, no fees) · equity $10,221.50 vs prior close $9,947.08 (+274.42) · 8 name(s) re-marked at the open (per-name table). AU×10 yday $121.22 → 09:30 $120.50 -7.20; AUPH×70 yday $16.65 → 09:30 $16.60 -3.50; AEM×5 yday $216.06 → 09:30 $217.03 +4.85; ARCT×109 yday $13.45 → 09:30 $13.26 -20.71; CYPH×920 yday $1.42 → 09:30 $1.83 +377.20; BTBT×732 yday $1.53 → 09:30 $1.55 +14.64; INDP×874 yday $1.29 → 09:30 $1.24 -43.70; TEM×18 yday $72.69 → 09:30 $70.07 -47.16 | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.50 | $2.04 | $+6.64 | $1,363.75 | ▲ +6.64 after sell → book $10,219.46; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 70 | $16.60 | $2.22 | $-46.42 | $2,523.53 | ▼ -46.42 after sell → book $10,217.24; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 5 | $217.03 | $2.02 | $-0.38 | $3,606.65 | ▼ -0.38 after sell → book $10,215.21; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 109 | $13.26 | $2.35 | $+227.51 | $5,049.64 | ▲ +227.51 after sell → book $10,212.86; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 920 | $1.83 | $12.03 | $+445.30 | $6,721.21 | ▲ +445.30 after sell → book $10,200.83; vs 09:30 mark -12.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BTBT` | 732 | $1.55 | $9.57 | $-99.54 | $7,846.24 | ▼ -99.54 after sell → book $10,191.26; vs 09:30 mark -9.57 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `INDP` | 874 | $1.24 | $11.43 | $-153.80 | $8,918.57 | ▼ -153.80 after sell → book $10,179.83; vs 09:30 mark -11.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `TEM` | 18 | $70.07 | $2.06 | $+76.35 | $10,177.76 | ▲ +76.35 after sell → book $10,177.76; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,177.76 | ▲ close $10,177.76 vs 09:30 $10,221.50 (session +0.00) | 16:00 close · cash $10,177.76 · no lots left · equity $10,177.76. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,177.76 | ▲ 09:30 equity $10,177.76 vs yday $10,177.76 (+0.00) | 09:30 open · cash $10,177.76 · no holdings · equity $10,177.76 vs prior close $10,177.76 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 199 | $6.38 | $2.59 | — | $8,905.55 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=+5.0; leftover $1272.22 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 748 | $1.70 | $9.65 | — | $7,624.31 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+120.5; leftover $1272.22 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `DEFT` | 1987 | $0.64 | $18.68 | — | $6,333.95 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover,ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1272.22 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `GORO` | 360 | $3.53 | $4.64 | — | $5,058.50 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+16.0; leftover $1272.22 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 36 | $34.48 | $2.10 | — | $3,815.13 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ⚪; ret5=+9.3; leftover $1272.22 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ERO` | 33 | $38.00 | $2.09 | — | $2,559.04 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot,mover_buy; ⚪; ret5=+16.6; leftover $1272.22 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `WPM` | 7 | $160.00 | $2.01 | — | $1,437.03 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot,mover_buy; ⚪; ret5=+17.6; leftover $1272.22 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 16 | $77.90 | $2.04 | — | $188.59 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot,mover_buy; ⚪; ret5=+15.3; leftover $1272.22 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $188.59 | ▼ close $10,090.93 vs 09:30 $10,177.76 (session -43.04) | 16:00 close · cash $188.59 · equity $10,090.93 vs 09:30 $10,177.76 (-86.83; session marks -43.04) · 8 name(s) marked open→close (per-name table). ZURA×199 09:30 $6.38 → close $6.50 +23.88; CYPH×748 09:30 $1.70 → close $1.64 -44.88; DEFT×1987 09:30 $0.64 → close $0.62 -39.74; GORO×360 09:30 $3.53 → close $3.56 +10.80; EZPW×36 09:30 $34.48 → close $34.69 +7.56; ERO×33 09:30 $38.00 → close $38.55 +18.15; WPM×7 09:30 $160.00 → close $158.25 -12.25; FCX×16 09:30 $77.90 → close $77.49 -6.56 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $188.59 | ▲ 09:30 equity $10,090.93 vs yday $10,090.93 (-0.00) | 09:30 open · cash $188.59 (unchanged overnight, no fees) · equity $10,090.93 vs prior close $10,090.93 (-0.00) · 8 name(s) re-marked at the open (per-name table). ZURA×199 yday $6.50 → 09:30 $6.50 +0.00; CYPH×748 yday $1.64 → 09:30 $1.64 +0.00; DEFT×1987 yday $0.62 → 09:30 $0.62 +0.00; GORO×360 yday $3.56 → 09:30 $3.56 +0.00; EZPW×36 yday $34.69 → 09:30 $34.69 +0.00; ERO×33 yday $38.55 → 09:30 $38.55 +0.00; WPM×7 yday $158.25 → 09:30 $158.25 +0.00; FCX×16 yday $77.49 → 09:30 $77.49 +0.00 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $188.59 | ▲ close $10,090.93 vs 09:30 $10,090.93 (session +0.00) | 16:00 close · cash $188.59 · equity $10,090.93 vs 09:30 $10,090.93 (-0.00; session marks +0.00) · 8 name(s) marked open→close (per-name table). ZURA×199 09:30 $6.50 → close $6.50 +0.00; CYPH×748 09:30 $1.64 → close $1.64 +0.00; DEFT×1987 09:30 $0.62 → close $0.62 +0.00; GORO×360 09:30 $3.56 → close $3.56 +0.00; EZPW×36 09:30 $34.69 → close $34.69 +0.00; ERO×33 09:30 $38.55 → close $38.55 +0.00; WPM×7 09:30 $158.25 → close $158.25 +0.00; FCX×16 09:30 $77.49 → close $77.49 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $188.59 | ▲ 09:30 equity $10,172.64 vs yday $10,090.93 (+81.71) | 09:30 open · cash $188.59 (unchanged overnight, no fees) · equity $10,172.64 vs prior close $10,090.93 (+81.71) · 8 name(s) re-marked at the open (per-name table). ZURA×199 yday $6.50 → 09:30 $6.13 -73.63; CYPH×748 yday $1.64 → 09:30 $1.60 -29.92; DEFT×1987 yday $0.62 → 09:30 $0.60 -39.74; GORO×360 yday $3.56 → 09:30 $3.77 +75.60; EZPW×36 yday $34.69 → 09:30 $35.70 +36.36; ERO×33 yday $38.55 → 09:30 $40.51 +64.68; WPM×7 yday $158.25 → 09:30 $160.93 +18.76; FCX×16 yday $77.49 → 09:30 $79.34 +29.60 | — |
| 2026-08-27 09:30 ET | **SELL** | `ZURA` | 199 | $6.13 | $2.63 | $-54.97 | $1,405.83 | ▼ -54.97 after sell → book $10,170.01; vs 09:30 mark -2.63 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 748 | $1.60 | $9.78 | $-94.23 | $2,592.84 | ▼ -94.23 after sell → book $10,160.22; vs 09:30 mark -9.79 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `DEFT` | 1987 | $0.60 | $18.22 | $-116.38 | $3,766.82 | ▼ -116.38 after sell → book $10,142.00; vs 09:30 mark -18.22 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `GORO` | 360 | $3.77 | $4.71 | $+77.04 | $5,119.31 | ▲ +77.04 after sell → book $10,137.29; vs 09:30 mark -4.71 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `EZPW` | 36 | $35.70 | $2.12 | $+39.70 | $6,402.39 | ▲ +39.70 after sell → book $10,135.17; vs 09:30 mark -2.12 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ERO` | 33 | $40.51 | $2.11 | $+78.63 | $7,737.11 | ▲ +78.63 after sell → book $10,133.06; vs 09:30 mark -2.11 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `WPM` | 7 | $160.93 | $2.03 | $+2.47 | $8,861.59 | ▲ +2.47 after sell → book $10,131.03; vs 09:30 mark -2.03 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FCX` | 16 | $79.34 | $2.06 | $+18.94 | $10,128.97 | ▲ +18.94 after sell → book $10,128.97; vs 09:30 mark -2.06 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,128.97 | ▲ close $10,128.97 vs 09:30 $10,172.64 (session +0.00) | 16:00 close · cash $10,128.97 · no lots left · equity $10,128.97. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,128.97 | ▲ 09:30 equity $10,128.97 vs yday $10,128.97 (-0.00) | 09:30 open · cash $10,128.97 · no holdings · equity $10,128.97 vs prior close $10,128.97 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 17 | $144.70 | $2.04 | — | $7,667.03 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $2532.24 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 136 | $18.50 | $2.40 | — | $5,148.63 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $2532.24 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 30 | $82.70 | $2.08 | — | $2,665.55 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=-4.6; leftover $2532.24 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TIGR` | 461 | $5.49 | $5.95 | — | $128.71 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; ret5=+15.9; leftover $2532.24 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟢 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $128.71 | ▼ close $9,750.82 vs 09:30 $10,128.97 (session -365.68) | 16:00 close · cash $128.71 · equity $9,750.82 vs 09:30 $10,128.97 (-378.15; session marks -365.68) · 4 name(s) marked open→close (per-name table). ANF×17 09:30 $144.70 → close $145.75 +17.85; BZ×136 09:30 $18.50 → close $18.00 -68.00; URBN×30 09:30 $82.70 → close $78.79 -117.30; TIGR×461 09:30 $5.49 → close $5.06 -198.23 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $128.71 | ▲ 09:30 equity $9,808.40 vs yday $9,750.82 (+57.58) | 09:30 open · cash $128.71 (unchanged overnight, no fees) · equity $9,808.40 vs prior close $9,750.82 (+57.58) · 4 name(s) re-marked at the open (per-name table). ANF×17 yday $145.75 → 09:30 $148.67 +49.64; BZ×136 yday $18.00 → 09:30 $17.89 -14.96; URBN×30 yday $78.79 → 09:30 $81.09 +69.00; TIGR×461 yday $5.06 → 09:30 $4.96 -46.10 | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 17 | $148.67 | $2.07 | $+63.38 | $2,654.03 | ▲ +63.38 after sell → book $9,806.33; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BZ` | 136 | $17.89 | $2.44 | $-87.80 | $5,084.63 | ▼ -87.80 after sell → book $9,803.89; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `URBN` | 30 | $81.09 | $2.11 | $-52.49 | $7,515.22 | ▼ -52.49 after sell → book $9,801.78; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TIGR` | 461 | $4.96 | $6.04 | $-256.32 | $9,795.74 | ▼ -256.32 after sell → book $9,795.74; vs 09:30 mark -6.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,795.74 | ▲ close $9,795.74 vs 09:30 $9,808.40 (session +0.00) | 16:00 close · cash $9,795.74 · no lots left · equity $9,795.74. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,795.74 | ▲ 09:30 equity $9,795.74 vs yday $9,795.74 (+0.00) | 09:30 open · cash $9,795.74 · no holdings · equity $9,795.74 vs prior close $9,795.74 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,795.74 | ▲ close $9,795.74 vs 09:30 $9,795.74 (session +0.00) | 16:00 close · cash $9,795.74 · no lots left · equity $9,795.74. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,795.74 | ▲ 09:30 equity $9,795.74 vs yday $9,795.74 (+0.00) | 09:30 open · cash $9,795.74 · no holdings · equity $9,795.74 vs prior close $9,795.74 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,795.74 | ▲ close $9,795.74 vs 09:30 $9,795.74 (session +0.00) | 16:00 close · cash $9,795.74 · no lots left · equity $9,795.74. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,795.74 | ▲ 09:30 equity $9,795.74 vs yday $9,795.74 (+0.00) | 09:30 open · cash $9,795.74 · no holdings · equity $9,795.74 vs prior close $9,795.74 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $125.94 | $2.02 | — | $8,660.26 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1224.47 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 77 | $15.70 | $2.22 | — | $7,449.14 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1224.47 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 53 | $22.78 | $2.15 | — | $6,239.65 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1224.47 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 8 | $151.40 | $2.01 | — | $5,026.44 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+129.2; leftover $1224.47 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 74 | $16.46 | $2.21 | — | $3,806.19 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+63.4; leftover $1224.47 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `NVAX` | 119 | $10.27 | $2.35 | — | $2,581.71 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+11.1; leftover $1224.47 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ALMS` | 120 | $10.15 | $2.35 | — | $1,361.36 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; 🔵; ret5=-4.5; leftover $1224.47 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `OSW` | 54 | $22.53 | $2.15 | — | $142.59 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; 🔵; ret5=-0.9; leftover $1224.47 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $142.59 | ▲ close $9,874.83 vs 09:30 $9,795.74 (session +96.55) | 16:00 close · cash $142.59 · equity $9,874.83 vs 09:30 $9,795.74 (+79.09; session marks +96.55) · 8 name(s) marked open→close (per-name table). RVTY×9 09:30 $125.94 → close $130.94 +45.00; CRK×77 09:30 $15.70 → close $15.54 -12.32; MMED×53 09:30 $22.78 → close $23.76 +51.94; MRNA×8 09:30 $151.40 → close $150.81 -4.72; ARCT×74 09:30 $16.46 → close $16.74 +20.72; NVAX×119 09:30 $10.27 → close $10.32 +5.95; ALMS×120 09:30 $10.15 → close $10.35 +24.00; OSW×54 09:30 $22.53 → close $21.90 -34.02 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $142.59 | ▼ 09:30 equity $9,870.90 vs yday $9,874.83 (-3.93) | 09:30 open · cash $142.59 (unchanged overnight, no fees) · equity $9,870.90 vs prior close $9,874.83 (-3.93) · 8 name(s) re-marked at the open (per-name table). RVTY×9 yday $130.94 → 09:30 $132.45 +13.59; CRK×77 yday $15.54 → 09:30 $15.45 -6.93; MMED×53 yday $23.76 → 09:30 $23.88 +6.36; MRNA×8 yday $150.81 → 09:30 $145.95 -38.88; ARCT×74 yday $16.74 → 09:30 $16.77 +2.22; NVAX×119 yday $10.32 → 09:30 $10.41 +10.71; ALMS×120 yday $10.35 → 09:30 $10.38 +3.60; OSW×54 yday $21.90 → 09:30 $22.00 +5.40 | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 9 | $132.45 | $2.04 | $+54.54 | $1,332.60 | ▲ +54.54 after sell → book $9,868.86; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 77 | $15.45 | $2.24 | $-23.71 | $2,520.01 | ▼ -23.71 after sell → book $9,866.62; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 53 | $23.88 | $2.17 | $+53.98 | $3,783.48 | ▲ +53.98 after sell → book $9,864.45; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MRNA` | 8 | $145.95 | $2.03 | $-47.65 | $4,949.05 | ▼ -47.65 after sell → book $9,862.42; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 74 | $16.77 | $2.23 | $+18.49 | $6,187.79 | ▲ +18.49 after sell → book $9,860.18; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `NVAX` | 119 | $10.41 | $2.38 | $+11.94 | $7,424.20 | ▲ +11.94 after sell → book $9,857.80; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `ALMS` | 120 | $10.38 | $2.38 | $+22.87 | $8,667.42 | ▲ +22.87 after sell → book $9,855.42; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `OSW` | 54 | $22.00 | $2.17 | $-32.94 | $9,853.25 | ▼ -32.94 after sell → book $9,853.25; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 3 | $486.31 | $2.00 | — | $8,392.32 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-9.9; leftover $1642.21 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 323 | $5.08 | $4.17 | — | $6,747.32 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+28.1; leftover $1642.21 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 608 | $2.70 | $7.84 | — | $5,097.87 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.4; leftover $1642.21 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `TARS` | 19 | $82.76 | $2.05 | — | $3,523.39 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ⚪; ret5=+5.1; leftover $1642.21 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MDB` | 4 | $378.76 | $2.00 | — | $2,006.34 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; ret5=-6.4; leftover $1642.21 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `TRLV` | 138 | $11.89 | $2.40 | — | $363.12 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ⚪; ret5=+15.0; leftover $1642.21 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $363.12 | ▼ close $9,746.03 vs 09:30 $9,870.90 (session -86.76) | 16:00 close · cash $363.12 · equity $9,746.03 vs 09:30 $9,870.90 (-124.87; session marks -86.76) · 6 name(s) marked open→close (per-name table). DELL×3 09:30 $486.31 → close $516.39 +90.24; OABI×323 09:30 $5.08 → close $4.75 -106.59; ALEC×608 09:30 $2.70 → close $2.51 -115.52; TARS×19 09:30 $82.76 → close $83.21 +8.55; MDB×4 09:30 $378.76 → close $384.45 +22.76; TRLV×138 09:30 $11.89 → close $11.99 +13.80 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TMC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ELMT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ERO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SCCO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-26 | `ZURA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CYPH` | no_price | no 09:30 open — carry |
| 2026-08-26 | `DEFT` | no_price | no 09:30 open — carry |
| 2026-08-26 | `GORO` | no_price | no 09:30 open — carry |
| 2026-08-26 | `EZPW` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ERO` | no_price | no 09:30 open — carry |
| 2026-08-26 | `WPM` | no_price | no 09:30 open — carry |
| 2026-08-26 | `FCX` | no_price | no 09:30 open — carry |
| 2026-08-26 | `USDE` | no_price | no 09:30 open |
| 2026-08-26 | `DKS` | no_price | no 09:30 open |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PURR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TYL` | hard_red | hard-red S=-5.85 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `DELL` | 3 | 2026-09-04 @ $486.31 | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-9.9; leftover $1642.21 |
| `OABI` | 323 | 2026-09-04 @ $5.08 | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+28.1; leftover $1642.21 |
| `ALEC` | 608 | 2026-09-04 @ $2.70 | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.4; leftover $1642.21 |
| `TARS` | 19 | 2026-09-04 @ $82.76 | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ⚪; ret5=+5.1; leftover $1642.21 |
| `MDB` | 4 | 2026-09-04 @ $378.76 | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; ret5=-6.4; leftover $1642.21 |
| `TRLV` | 138 | 2026-09-04 @ $11.89 | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ⚪; ret5=+15.0; leftover $1642.21 |
