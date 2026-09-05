# Factor mine action — `union_join_vol_green_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **-8.86%** ($9,114) · signal-only (no cash/fees) was -10.84%. Starts YES **4/17**. Fills 69 · skips 95 · realized $-948.80.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `join=good,vol=good,last_green=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $110.32.

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
| 2026-08-17 | `BTBT` | 833 | $1.57 | $1.52 | -41.65 | $1.60 | +66.64 | +24.99 | +16.66 | +83.30 |
| 2026-08-17 | `BETR` | 84 | $13.73 | $13.67 | -5.04 | $13.54 | -10.92 | -15.96 | -94.92 | -105.84 |
| 2026-08-17 | `ANGX` | 290 | $4.37 | $4.60 | +66.70 | $4.71 | +31.90 | +98.60 | +84.10 | +116.00 |
| 2026-08-17 | `HYLN` | 299 | $4.06 | $4.10 | +11.96 | $4.09 | -2.99 | +8.97 | -23.92 | -26.91 |
| 2026-08-17 | `ADUR` | 75 | $16.17 | $15.73 | -33.00 | $15.85 | +9.00 | -24.00 | -57.75 | -48.75 |
| 2026-08-17 | `AIRO` | 112 | $9.57 | $9.57 | +0.00 | $9.41 | -17.92 | -17.92 | -173.60 | -191.52 |
| 2026-08-17 | `NCMI` | 464 | $2.86 | $2.80 | -27.84 | $2.73 | -32.48 | -60.32 | +51.04 | +18.56 |
| 2026-08-17 | `QMLS` | 170 | $7.32 | $7.24 | -13.60 | $7.14 | -17.00 | -30.60 | -8.50 | -25.50 |
| 2026-08-18 | `BTBT` | 833 | $1.60 | $1.54 | -49.98 | $1.45 | -74.97 | -124.95 | +33.32 | -41.65 |
| 2026-08-18 | `BETR` | 84 | $13.54 | $13.21 | -27.72 | $13.05 | -13.44 | -41.16 | -133.56 | -147.00 |
| 2026-08-18 | `ANGX` | 290 | $4.71 | $4.79 | +23.20 | $4.85 | +17.40 | +40.60 | +139.20 | +156.60 |
| 2026-08-18 | `HYLN` | 299 | $4.09 | $3.95 | -41.86 | $3.86 | -26.91 | -68.77 | -68.77 | -95.68 |
| 2026-08-18 | `ADUR` | 75 | $15.85 | $15.41 | -33.00 | $15.63 | +16.50 | -16.50 | -81.75 | -65.25 |
| 2026-08-18 | `AIRO` | 112 | $9.41 | $9.01 | -44.80 | $8.98 | -3.36 | -48.16 | -236.32 | -239.68 |
| 2026-08-18 | `NCMI` | 464 | $2.73 | $2.71 | -9.28 | $2.52 | -88.16 | -97.44 | +9.28 | -78.88 |
| 2026-08-18 | `QMLS` | 170 | $7.14 | $6.85 | -49.30 | $6.74 | -18.70 | -68.00 | -74.80 | -93.50 |
| 2026-08-19 | `BTBT` | 833 | $1.45 | $1.42 | -24.99 | — | +0.00 | -24.99 | -66.64 | — |
| 2026-08-19 | `BETR` | 84 | $13.05 | $13.03 | -1.68 | — | +0.00 | -1.68 | -148.68 | — |
| 2026-08-19 | `ANGX` | 290 | $4.85 | $4.79 | -17.40 | — | +0.00 | -17.40 | +139.20 | — |
| 2026-08-19 | `HYLN` | 299 | $3.86 | $3.87 | +2.99 | — | +0.00 | +2.99 | -92.69 | — |
| 2026-08-19 | `ADUR` | 75 | $15.63 | $15.65 | +1.50 | — | +0.00 | +1.50 | -63.75 | — |
| 2026-08-19 | `AIRO` | 112 | $8.98 | $9.10 | +13.44 | — | +0.00 | +13.44 | -226.24 | — |
| 2026-08-19 | `NCMI` | 464 | $2.52 | $2.56 | +18.56 | — | +0.00 | +18.56 | -60.32 | — |
| 2026-08-19 | `QMLS` | 170 | $6.74 | $6.74 | +0.00 | — | +0.00 | +0.00 | -93.50 | — |
| 2026-08-20 | `AG` | 56 | — | $20.55 | +0.00 | $21.19 | +35.84 | +35.84 | +0.00 | +35.84 |
| 2026-08-20 | `CDE` | 56 | — | $20.65 | +0.00 | $21.11 | +25.76 | +25.76 | +0.00 | +25.76 |
| 2026-08-20 | `HDSN` | 201 | — | $5.77 | +0.00 | $5.57 | -40.20 | -40.20 | +0.00 | -40.20 |
| 2026-08-20 | `IAG` | 59 | — | $19.63 | +0.00 | $20.50 | +51.33 | +51.33 | +0.00 | +51.33 |
| 2026-08-20 | `KGC` | 39 | — | $29.63 | +0.00 | $31.43 | +70.20 | +70.20 | +0.00 | +70.20 |
| 2026-08-20 | `NFGC` | 665 | — | $1.75 | +0.00 | $1.75 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `WPM` | 8 | — | $144.54 | +0.00 | $150.25 | +45.68 | +45.68 | +0.00 | +45.68 |
| 2026-08-20 | `ABUS` | 236 | — | $4.92 | +0.00 | $4.77 | -35.40 | -35.40 | +0.00 | -35.40 |
| 2026-08-21 | `AG` | 56 | $21.19 | $21.90 | +39.76 | $21.09 | -45.36 | -5.60 | +75.60 | +30.24 |
| 2026-08-21 | `CDE` | 56 | $21.11 | $21.75 | +35.84 | $20.97 | -43.68 | -7.84 | +61.60 | +17.92 |
| 2026-08-21 | `HDSN` | 201 | $5.57 | $5.67 | +20.10 | $5.63 | -8.04 | +12.06 | -20.10 | -28.14 |
| 2026-08-21 | `IAG` | 59 | $20.50 | $21.17 | +39.53 | $21.14 | -1.77 | +37.76 | +90.86 | +89.09 |
| 2026-08-21 | `KGC` | 39 | $31.43 | $32.17 | +28.86 | $32.76 | +23.01 | +51.87 | +99.06 | +122.07 |
| 2026-08-21 | `NFGC` | 665 | $1.75 | $1.79 | +26.60 | $1.84 | +33.25 | +59.85 | +26.60 | +59.85 |
| 2026-08-21 | `WPM` | 8 | $150.25 | $154.70 | +35.60 | $157.78 | +24.64 | +60.24 | +81.28 | +105.92 |
| 2026-08-21 | `ABUS` | 236 | $4.77 | $5.20 | +101.48 | $5.21 | +2.36 | +103.84 | +66.08 | +68.44 |
| 2026-08-21 | `CYPH` | 3 | — | $1.32 | +0.00 | $1.42 | +0.30 | +0.30 | +0.00 | +0.30 |
| 2026-08-21 | `BTBT` | 2 | — | $1.66 | +0.00 | $1.53 | -0.26 | -0.26 | +0.00 | -0.26 |
| 2026-08-21 | `INDP` | 2 | — | $1.39 | +0.00 | $1.29 | -0.20 | -0.20 | +0.00 | -0.20 |
| 2026-08-24 | `AG` | 56 | $21.09 | $21.47 | +21.28 | $20.57 | -50.40 | -29.12 | +51.52 | +1.12 |
| 2026-08-24 | `CDE` | 56 | $20.97 | $21.26 | +16.24 | $20.49 | -43.12 | -26.88 | +34.16 | -8.96 |
| 2026-08-24 | `HDSN` | 201 | $5.63 | $5.69 | +12.06 | $5.57 | -24.12 | -12.06 | -16.08 | -40.20 |
| 2026-08-24 | `IAG` | 59 | $21.14 | $21.44 | +17.70 | $21.36 | -4.72 | +12.98 | +106.79 | +102.07 |
| 2026-08-24 | `KGC` | 39 | $32.76 | $33.21 | +17.55 | $32.47 | -28.86 | -11.31 | +139.62 | +110.76 |
| 2026-08-24 | `NFGC` | 665 | $1.84 | $1.86 | +13.30 | $1.90 | +26.60 | +39.90 | +73.15 | +99.75 |
| 2026-08-24 | `WPM` | 8 | $157.78 | $158.96 | +9.44 | $158.00 | -7.68 | +1.76 | +115.36 | +107.68 |
| 2026-08-24 | `ABUS` | 236 | $5.21 | $5.18 | -7.08 | $5.20 | +4.72 | -2.36 | +61.36 | +66.08 |
| 2026-08-24 | `CYPH` | 3 | $1.42 | $1.83 | +1.23 | $1.64 | -0.57 | +0.66 | +1.53 | +0.96 |
| 2026-08-24 | `BTBT` | 2 | $1.53 | $1.55 | +0.04 | $1.56 | +0.02 | +0.06 | -0.22 | -0.20 |
| 2026-08-24 | `INDP` | 2 | $1.29 | $1.24 | -0.10 | $1.16 | -0.16 | -0.26 | -0.30 | -0.46 |
| 2026-08-25 | `AG` | 56 | $20.57 | $20.73 | +8.96 | — | +0.00 | +8.96 | +10.08 | — |
| 2026-08-25 | `CDE` | 56 | $20.49 | $20.85 | +20.16 | — | +0.00 | +20.16 | +11.20 | — |
| 2026-08-25 | `HDSN` | 201 | $5.57 | $5.53 | -8.04 | — | +0.00 | -8.04 | -48.24 | — |
| 2026-08-25 | `IAG` | 59 | $21.36 | $21.63 | +15.93 | — | +0.00 | +15.93 | +118.00 | — |
| 2026-08-25 | `KGC` | 39 | $32.47 | $32.76 | +11.31 | — | +0.00 | +11.31 | +122.07 | — |
| 2026-08-25 | `NFGC` | 665 | $1.90 | $1.91 | +6.65 | — | +0.00 | +6.65 | +106.40 | — |
| 2026-08-25 | `WPM` | 8 | $158.00 | $160.00 | +16.00 | $158.25 | -14.00 | +2.00 | +123.68 | +109.68 |
| 2026-08-25 | `ABUS` | 236 | $5.20 | $5.26 | +14.16 | — | +0.00 | +14.16 | +80.24 | — |
| 2026-08-25 | `CYPH` | 3 | $1.64 | $1.70 | +0.18 | $1.64 | -0.18 | +0.00 | +1.14 | +0.96 |
| 2026-08-25 | `BTBT` | 2 | $1.56 | $1.55 | -0.02 | $1.53 | -0.04 | -0.06 | -0.22 | -0.26 |
| 2026-08-25 | `INDP` | 2 | $1.16 | $1.18 | +0.04 | $1.25 | +0.14 | +0.18 | -0.42 | -0.28 |
| 2026-08-25 | `ZURA` | 222 | — | $6.38 | +0.00 | $6.50 | +26.64 | +26.64 | +0.00 | +26.64 |
| 2026-08-25 | `DEFT` | 2214 | — | $0.64 | +0.00 | $0.62 | -44.28 | -44.28 | +0.00 | -44.28 |
| 2026-08-25 | `GORO` | 401 | — | $3.53 | +0.00 | $3.56 | +12.03 | +12.03 | +0.00 | +12.03 |
| 2026-08-25 | `EZPW` | 41 | — | $34.48 | +0.00 | $34.69 | +8.61 | +8.61 | +0.00 | +8.61 |
| 2026-08-25 | `ERO` | 37 | — | $38.00 | +0.00 | $38.55 | +20.35 | +20.35 | +0.00 | +20.35 |
| 2026-08-25 | `FCX` | 17 | — | $77.90 | +0.00 | $77.49 | -6.97 | -6.97 | +0.00 | -6.97 |
| 2026-08-26 | `WPM` | 8 | $158.25 | $158.25 | +0.00 | $158.25 | +0.00 | +0.00 | +109.68 | +109.68 |
| 2026-08-26 | `CYPH` | 3 | $1.64 | $1.64 | +0.00 | $1.64 | +0.00 | +0.00 | +0.96 | +0.96 |
| 2026-08-26 | `BTBT` | 2 | $1.53 | $1.53 | +0.00 | $1.53 | +0.00 | +0.00 | -0.26 | -0.26 |
| 2026-08-26 | `INDP` | 2 | $1.25 | $1.25 | +0.00 | $1.25 | +0.00 | +0.00 | -0.28 | -0.28 |
| 2026-08-26 | `ZURA` | 222 | $6.50 | $6.50 | +0.00 | $6.50 | +0.00 | +0.00 | +26.64 | +26.64 |
| 2026-08-26 | `DEFT` | 2214 | $0.62 | $0.62 | +0.00 | $0.62 | +0.00 | +0.00 | -44.28 | -44.28 |
| 2026-08-26 | `GORO` | 401 | $3.56 | $3.56 | +0.00 | $3.56 | +0.00 | +0.00 | +12.03 | +12.03 |
| 2026-08-26 | `EZPW` | 41 | $34.69 | $34.69 | +0.00 | $34.69 | +0.00 | +0.00 | +8.61 | +8.61 |
| 2026-08-26 | `ERO` | 37 | $38.55 | $38.55 | +0.00 | $38.55 | +0.00 | +0.00 | +20.35 | +20.35 |
| 2026-08-26 | `FCX` | 17 | $77.49 | $77.49 | +0.00 | $77.49 | +0.00 | +0.00 | -6.97 | -6.97 |
| 2026-08-27 | `WPM` | 8 | $158.25 | $160.93 | +21.44 | — | +0.00 | +21.44 | +131.12 | — |
| 2026-08-27 | `CYPH` | 3 | $1.64 | $1.60 | -0.12 | — | +0.00 | -0.12 | +0.84 | — |
| 2026-08-27 | `BTBT` | 2 | $1.53 | $1.53 | +0.00 | — | +0.00 | +0.00 | -0.26 | — |
| 2026-08-27 | `INDP` | 2 | $1.25 | $1.09 | -0.32 | — | +0.00 | -0.32 | -0.60 | — |
| 2026-08-27 | `ZURA` | 222 | $6.50 | $6.13 | -82.14 | $5.99 | -31.08 | -113.22 | -55.50 | -86.58 |
| 2026-08-27 | `DEFT` | 2214 | $0.62 | $0.60 | -44.28 | $0.59 | -22.14 | -66.42 | -88.56 | -110.70 |
| 2026-08-27 | `GORO` | 401 | $3.56 | $3.77 | +84.21 | $3.56 | -84.21 | +0.00 | +96.24 | +12.03 |
| 2026-08-27 | `EZPW` | 41 | $34.69 | $35.70 | +41.41 | $33.90 | -73.80 | -32.39 | +50.02 | -23.78 |
| 2026-08-27 | `ERO` | 37 | $38.55 | $40.51 | +72.52 | $39.24 | -46.99 | +25.53 | +92.87 | +45.88 |
| 2026-08-27 | `FCX` | 17 | $77.49 | $79.34 | +31.45 | $79.00 | -5.78 | +25.67 | +24.48 | +18.70 |
| 2026-08-28 | `ZURA` | 222 | $5.99 | $6.02 | +6.66 | — | +0.00 | +6.66 | -79.92 | — |
| 2026-08-28 | `DEFT` | 2214 | $0.59 | $0.60 | +22.14 | — | +0.00 | +22.14 | -88.56 | — |
| 2026-08-28 | `GORO` | 401 | $3.56 | $3.59 | +12.03 | — | +0.00 | +12.03 | +24.06 | — |
| 2026-08-28 | `EZPW` | 41 | $33.90 | $33.50 | -16.40 | — | +0.00 | -16.40 | -40.18 | — |
| 2026-08-28 | `ERO` | 37 | $39.24 | $39.20 | -1.48 | — | +0.00 | -1.48 | +44.40 | — |
| 2026-08-28 | `FCX` | 17 | $79.00 | $78.83 | -2.89 | — | +0.00 | -2.89 | +15.81 | — |
| 2026-08-28 | `ANF` | 16 | — | $144.70 | +0.00 | $145.75 | +16.80 | +16.80 | +0.00 | +16.80 |
| 2026-08-28 | `BZ` | 129 | — | $18.50 | +0.00 | $18.00 | -64.50 | -64.50 | +0.00 | -64.50 |
| 2026-08-28 | `URBN` | 29 | — | $82.70 | +0.00 | $78.79 | -113.39 | -113.39 | +0.00 | -113.39 |
| 2026-08-28 | `TIGR` | 437 | — | $5.49 | +0.00 | $5.06 | -187.91 | -187.91 | +0.00 | -187.91 |
| 2026-08-31 | `ANF` | 16 | $145.75 | $148.67 | +46.72 | $149.28 | +9.76 | +56.48 | +63.52 | +73.28 |
| 2026-08-31 | `BZ` | 129 | $18.00 | $17.89 | -14.19 | $17.90 | +1.29 | -12.90 | -78.69 | -77.40 |
| 2026-08-31 | `URBN` | 29 | $78.79 | $81.09 | +66.70 | $81.09 | +0.00 | +66.70 | -46.69 | -46.69 |
| 2026-08-31 | `TIGR` | 437 | $5.06 | $4.96 | -43.70 | $5.01 | +21.85 | -21.85 | -231.61 | -209.76 |
| 2026-09-01 | `ANF` | 16 | $149.28 | $142.47 | -108.96 | $143.00 | +8.48 | -100.48 | -35.68 | -27.20 |
| 2026-09-01 | `BZ` | 129 | $17.90 | $17.37 | -68.37 | $17.17 | -25.80 | -94.17 | -145.77 | -171.57 |
| 2026-09-01 | `URBN` | 29 | $81.09 | $80.69 | -11.60 | $80.69 | +0.00 | -11.60 | -58.29 | -58.29 |
| 2026-09-01 | `TIGR` | 437 | $5.01 | $5.02 | +4.37 | $5.00 | -8.74 | -4.37 | -205.39 | -214.13 |
| 2026-09-02 | `ANF` | 16 | $143.00 | $142.00 | -16.00 | — | +0.00 | -16.00 | -43.20 | — |
| 2026-09-02 | `BZ` | 129 | $17.17 | $17.29 | +15.48 | — | +0.00 | +15.48 | -156.09 | — |
| 2026-09-02 | `URBN` | 29 | $80.69 | $79.12 | -45.53 | — | +0.00 | -45.53 | -103.82 | — |
| 2026-09-02 | `TIGR` | 437 | $5.00 | $4.97 | -13.11 | — | +0.00 | -13.11 | -227.24 | — |
| 2026-09-03 | `RVTY` | 8 | — | $125.94 | +0.00 | $130.94 | +40.00 | +40.00 | +0.00 | +40.00 |
| 2026-09-03 | `CRK` | 72 | — | $15.70 | +0.00 | $15.54 | -11.52 | -11.52 | +0.00 | -11.52 |
| 2026-09-03 | `MMED` | 49 | — | $22.78 | +0.00 | $23.76 | +48.02 | +48.02 | +0.00 | +48.02 |
| 2026-09-03 | `MRNA` | 7 | — | $151.40 | +0.00 | $150.81 | -4.13 | -4.13 | +0.00 | -4.13 |
| 2026-09-03 | `ARCT` | 68 | — | $16.46 | +0.00 | $16.74 | +19.04 | +19.04 | +0.00 | +19.04 |
| 2026-09-03 | `NVAX` | 110 | — | $10.27 | +0.00 | $10.32 | +5.50 | +5.50 | +0.00 | +5.50 |
| 2026-09-03 | `ALMS` | 111 | — | $10.15 | +0.00 | $10.35 | +22.20 | +22.20 | +0.00 | +22.20 |
| 2026-09-03 | `OSW` | 50 | — | $22.53 | +0.00 | $21.90 | -31.50 | -31.50 | +0.00 | -31.50 |
| 2026-09-04 | `RVTY` | 8 | $130.94 | $132.45 | +12.08 | $130.63 | -14.56 | -2.48 | +52.08 | +37.52 |
| 2026-09-04 | `CRK` | 72 | $15.54 | $15.45 | -6.48 | $14.95 | -36.00 | -42.48 | -18.00 | -54.00 |
| 2026-09-04 | `MMED` | 49 | $23.76 | $23.88 | +5.88 | $23.84 | -1.96 | +3.92 | +53.90 | +51.94 |
| 2026-09-04 | `MRNA` | 7 | $150.81 | $145.95 | -34.02 | $148.87 | +20.44 | -13.58 | -38.15 | -17.71 |
| 2026-09-04 | `ARCT` | 68 | $16.74 | $16.77 | +2.04 | $15.56 | -82.28 | -80.24 | +21.08 | -61.20 |
| 2026-09-04 | `NVAX` | 110 | $10.32 | $10.41 | +9.90 | $10.34 | -7.70 | +2.20 | +15.40 | +7.70 |
| 2026-09-04 | `ALMS` | 111 | $10.35 | $10.38 | +3.33 | $11.36 | +108.78 | +112.11 | +25.53 | +134.31 |
| 2026-09-04 | `OSW` | 50 | $21.90 | $22.00 | +5.00 | $22.27 | +13.50 | +18.50 | -26.50 | -13.00 |
| 2026-09-04 | `OABI` | 7 | — | $5.08 | +0.00 | $4.75 | -2.31 | -2.31 | +0.00 | -2.31 |
| 2026-09-04 | `ALEC` | 13 | — | $2.70 | +0.00 | $2.51 | -2.47 | -2.47 | +0.00 | -2.47 |
| 2026-09-04 | `TRLV` | 3 | — | $11.89 | +0.00 | $11.99 | +0.30 | +0.30 | +0.00 | +0.30 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | -164.42 | BTBT, BETR, ANGX, HYLN, ADUR, AIRO, NCMI, QMLS | — | $3.57 | $9,801.97 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, AIRO×112, NCMI×464, QMLS×170 |
| 2026-08-17 | +2.25 | $3.57 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, AIRO×112, NCMI×464, QMLS×170 | $9,759.50 | -42.47 | +26.23 | — | — | $3.57 | $9,785.73 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, AIRO×112, NCMI×464, QMLS×170 |
| 2026-08-18 | -6.20 | $3.57 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, AIRO×112, NCMI×464, QMLS×170 | $9,552.99 | -232.74 | -191.64 | — | — | $3.57 | $9,361.35 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, AIRO×112, NCMI×464, QMLS×170 |
| 2026-08-19 | -7.20 | $3.57 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, AIRO×112, NCMI×464, QMLS×170 | $9,353.77 | -7.58 | +0.00 | — | BTBT, BETR, ANGX, HYLN, ADUR, AIRO, NCMI, QMLS | $9,319.69 | $9,319.69 | — |
| 2026-08-20 | +1.12 | $9,319.69 | — | $9,319.69 | -0.00 | +153.21 | AG, CDE, HDSN, IAG, KGC, NFGC, WPM, ABUS | — | $32.96 | $9,448.07 | AG×56, CDE×56, HDSN×201, IAG×59, KGC×39, NFGC×665, WPM×8, ABUS×236 |
| 2026-08-21 | +3.25 | $32.96 | AG×56, CDE×56, HDSN×201, IAG×59, KGC×39, NFGC×665, WPM×8, ABUS×236 | $9,775.84 | +327.77 | -15.75 | CYPH, BTBT, INDP | — | $22.78 | $9,759.97 | AG×56, CDE×56, HDSN×201, IAG×59, KGC×39, NFGC×665, WPM×8, ABUS×236, CYPH×3, BTBT×2, INDP×2 |
| 2026-08-24 | -5.17 | $22.78 | AG×56, CDE×56, HDSN×201, IAG×59, KGC×39, NFGC×665, WPM×8, ABUS×236, CYPH×3, BTBT×2, INDP×2 | $9,861.63 | +101.66 | -128.29 | — | — | $22.78 | $9,733.34 | AG×56, CDE×56, HDSN×201, IAG×59, KGC×39, NFGC×665, WPM×8, ABUS×236, CYPH×3, BTBT×2, INDP×2 |
| 2026-08-25 | +1.80 | $22.78 | AG×56, CDE×56, HDSN×201, IAG×59, KGC×39, NFGC×665, WPM×8, ABUS×236, CYPH×3, BTBT×2, INDP×2 | $9,818.67 | +85.33 | +2.30 | ZURA, DEFT, GORO, EZPW, ERO, FCX | AG, CDE, HDSN, IAG, KGC, NFGC, ABUS | $77.07 | $9,762.76 | WPM×8, CYPH×3, BTBT×2, INDP×2, ZURA×222, DEFT×2214, GORO×401, EZPW×41, ERO×37, FCX×17 |
| 2026-08-26 | +2.02 | $77.07 | WPM×8, CYPH×3, BTBT×2, INDP×2, ZURA×222, DEFT×2214, GORO×401, EZPW×41, ERO×37, FCX×17 | $9,762.76 | +0.00 | +0.00 | — | — | $77.07 | $9,762.76 | WPM×8, CYPH×3, BTBT×2, INDP×2, ZURA×222, DEFT×2214, GORO×401, EZPW×41, ERO×37, FCX×17 |
| 2026-08-27 | — | $77.07 | WPM×8, CYPH×3, BTBT×2, INDP×2, ZURA×222, DEFT×2214, GORO×401, EZPW×41, ERO×37, FCX×17 | $9,886.93 | +124.17 | -264.00 | — | WPM, CYPH, BTBT, INDP | $1,372.34 | $9,620.72 | ZURA×222, DEFT×2214, GORO×401, EZPW×41, ERO×37, FCX×17 |
| 2026-08-28 | +0.75 | $1,372.34 | ZURA×222, DEFT×2214, GORO×401, EZPW×41, ERO×37, FCX×17 | $9,640.78 | +20.06 | -349.00 | ANF, BZ, URBN, TIGR | ZURA, DEFT, GORO, EZPW, ERO, FCX | $94.73 | $9,244.86 | ANF×16, BZ×129, URBN×29, TIGR×437 |
| 2026-08-31 | -5.85 | $94.73 | ANF×16, BZ×129, URBN×29, TIGR×437 | $9,300.39 | +55.53 | +32.90 | — | — | $94.73 | $9,333.29 | ANF×16, BZ×129, URBN×29, TIGR×437 |
| 2026-09-01 | -6.30 | $94.73 | ANF×16, BZ×129, URBN×29, TIGR×437 | $9,148.73 | -184.56 | -26.06 | — | — | $94.73 | $9,122.67 | ANF×16, BZ×129, URBN×29, TIGR×437 |
| 2026-09-02 | -3.83 | $94.73 | ANF×16, BZ×129, URBN×29, TIGR×437 | $9,063.51 | -59.16 | +0.00 | — | ANF, BZ, URBN, TIGR | $9,051.20 | $9,051.20 | — |
| 2026-09-03 | -0.90 | $9,051.20 | — | $9,051.20 | -0.00 | +87.61 | RVTY, CRK, MMED, MRNA, ARCT, NVAX, ALMS, OSW | — | $217.78 | $9,121.46 | RVTY×8, CRK×72, MMED×49, MRNA×7, ARCT×68, NVAX×110, ALMS×111, OSW×50 |
| 2026-09-04 | — | $217.78 | RVTY×8, CRK×72, MMED×49, MRNA×7, ARCT×68, NVAX×110, ALMS×111, OSW×50 | $9,119.19 | -2.27 | -4.26 | OABI, ALEC, TRLV | — | $110.32 | $9,113.80 | RVTY×8, CRK×72, MMED×49, MRNA×7, ARCT×68, NVAX×110, ALMS×111, OSW×50, OABI×7, ALEC×13, TRLV×3 |

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
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.57 | ▲ close $9,785.73 vs 09:30 $9,759.50 (session +26.23) | 16:00 close · cash $3.57 · equity $9,785.73 vs 09:30 $9,759.50 (+26.23; session marks +26.23) · 8 name(s) marked open→close (per-name table). BTBT×833 09:30 $1.52 → close $1.60 +66.64; BETR×84 09:30 $13.67 → close $13.54 -10.92; ANGX×290 09:30 $4.60 → close $4.71 +31.90; HYLN×299 09:30 $4.10 → close $4.09 -2.99; ADUR×75 09:30 $15.73 → close $15.85 +9.00; AIRO×112 09:30 $9.57 → close $9.41 -17.92; NCMI×464 09:30 $2.80 → close $2.73 -32.48; QMLS×170 09:30 $7.24 → close $7.14 -17.00 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.57 | ▼ 09:30 equity $9,552.99 vs yday $9,785.73 (-232.74) | 09:30 open · cash $3.57 (unchanged overnight, no fees) · equity $9,552.99 vs prior close $9,785.73 (-232.74) · 8 name(s) re-marked at the open (per-name table). BTBT×833 yday $1.60 → 09:30 $1.54 -49.98; BETR×84 yday $13.54 → 09:30 $13.21 -27.72; ANGX×290 yday $4.71 → 09:30 $4.79 +23.20; HYLN×299 yday $4.09 → 09:30 $3.95 -41.86; ADUR×75 yday $15.85 → 09:30 $15.41 -33.00; AIRO×112 yday $9.41 → 09:30 $9.01 -44.80; NCMI×464 yday $2.73 → 09:30 $2.71 -9.28; QMLS×170 yday $7.14 → 09:30 $6.85 -49.30 | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.57 | ▼ close $9,361.35 vs 09:30 $9,552.99 (session -191.64) | 16:00 close · cash $3.57 · equity $9,361.35 vs 09:30 $9,552.99 (-191.64; session marks -191.64) · 8 name(s) marked open→close (per-name table). BTBT×833 09:30 $1.54 → close $1.45 -74.97; BETR×84 09:30 $13.21 → close $13.05 -13.44; ANGX×290 09:30 $4.79 → close $4.85 +17.40; HYLN×299 09:30 $3.95 → close $3.86 -26.91; ADUR×75 09:30 $15.41 → close $15.63 +16.50; AIRO×112 09:30 $9.01 → close $8.98 -3.36; NCMI×464 09:30 $2.71 → close $2.52 -88.16; QMLS×170 09:30 $6.85 → close $6.74 -18.70 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.57 | ▼ 09:30 equity $9,353.77 vs yday $9,361.35 (-7.58) | 09:30 open · cash $3.57 (unchanged overnight, no fees) · equity $9,353.77 vs prior close $9,361.35 (-7.58) · 8 name(s) re-marked at the open (per-name table). BTBT×833 yday $1.45 → 09:30 $1.42 -24.99; BETR×84 yday $13.05 → 09:30 $13.03 -1.68; ANGX×290 yday $4.85 → 09:30 $4.79 -17.40; HYLN×299 yday $3.86 → 09:30 $3.87 +2.99; ADUR×75 yday $15.63 → 09:30 $15.65 +1.50; AIRO×112 yday $8.98 → 09:30 $9.10 +13.44; NCMI×464 yday $2.52 → 09:30 $2.56 +18.56; QMLS×170 yday $6.74 → 09:30 $6.74 +0.00 | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 833 | $1.42 | $10.89 | $-88.28 | $1,175.53 | ▼ -88.28 after sell → book $9,342.87; vs 09:30 mark -10.90 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BETR` | 84 | $13.03 | $2.27 | $-153.19 | $2,267.79 | ▼ -153.19 after sell → book $9,340.61; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ANGX` | 290 | $4.79 | $3.80 | $+131.66 | $3,653.09 | ▲ +131.66 after sell → book $9,336.81; vs 09:30 mark -3.80 | dropped from list after 3 sess (min 3) | join🔴 sector🟡 gen🔴 news🟢 vol🟢 buy🟡 |
| 2026-08-19 09:30 ET | **SELL** | `HYLN` | 299 | $3.87 | $3.92 | $-100.46 | $4,806.30 | ▼ -100.46 after sell → book $9,332.89; vs 09:30 mark -3.92 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ADUR` | 75 | $15.65 | $2.24 | $-68.20 | $5,977.81 | ▼ -68.20 after sell → book $9,330.65; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `AIRO` | 112 | $9.10 | $2.35 | $-230.92 | $6,994.66 | ▼ -230.92 after sell → book $9,328.30; vs 09:30 mark -2.35 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `NCMI` | 464 | $2.56 | $6.07 | $-72.38 | $8,176.43 | ▼ -72.38 after sell → book $9,322.23; vs 09:30 mark -6.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `QMLS` | 170 | $6.74 | $2.54 | $-98.54 | $9,319.69 | ▼ -98.54 after sell → book $9,319.69; vs 09:30 mark -2.54 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,319.69 | ▲ close $9,319.69 vs 09:30 $9,353.77 (session +0.00) | 16:00 close · cash $9,319.69 · no lots left · equity $9,319.69. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,319.69 | ▲ 09:30 equity $9,319.69 vs yday $9,319.69 (-0.00) | 09:30 open · cash $9,319.69 · no holdings · equity $9,319.69 vs prior close $9,319.69 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 56 | $20.55 | $2.16 | — | $8,166.73 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1164.96 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 56 | $20.65 | $2.16 | — | $7,008.17 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1164.96 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 201 | $5.77 | $2.60 | — | $5,845.80 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1164.96 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 59 | $19.63 | $2.17 | — | $4,685.47 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1164.96 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 39 | $29.63 | $2.11 | — | $3,527.79 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1164.96 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 665 | $1.75 | $8.58 | — | $2,355.46 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1164.96 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $1,197.13 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1164.96 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABUS` | 236 | $4.92 | $3.04 | — | $32.96 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1164.96 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $32.96 | ▲ close $9,448.07 vs 09:30 $9,319.69 (session +153.21) | 16:00 close · cash $32.96 · equity $9,448.07 vs 09:30 $9,319.69 (+128.38; session marks +153.21) · 8 name(s) marked open→close (per-name table). AG×56 09:30 $20.55 → close $21.19 +35.84; CDE×56 09:30 $20.65 → close $21.11 +25.76; HDSN×201 09:30 $5.77 → close $5.57 -40.20; IAG×59 09:30 $19.63 → close $20.50 +51.33; KGC×39 09:30 $29.63 → close $31.43 +70.20; NFGC×665 09:30 $1.75 → close $1.75 +0.00; WPM×8 09:30 $144.54 → close $150.25 +45.68; ABUS×236 09:30 $4.92 → close $4.77 -35.40 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $32.96 | ▲ 09:30 equity $9,775.84 vs yday $9,448.07 (+327.77) | 09:30 open · cash $32.96 (unchanged overnight, no fees) · equity $9,775.84 vs prior close $9,448.07 (+327.77) · 8 name(s) re-marked at the open (per-name table). AG×56 yday $21.19 → 09:30 $21.90 +39.76; CDE×56 yday $21.11 → 09:30 $21.75 +35.84; HDSN×201 yday $5.57 → 09:30 $5.67 +20.10; IAG×59 yday $20.50 → 09:30 $21.17 +39.53; KGC×39 yday $31.43 → 09:30 $32.17 +28.86; NFGC×665 yday $1.75 → 09:30 $1.79 +26.60; WPM×8 yday $150.25 → 09:30 $154.70 +35.60; ABUS×236 yday $4.77 → 09:30 $5.20 +101.48 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 3 | $1.32 | $0.05 | — | $28.95 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $4.12 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 2 | $1.66 | $0.04 | — | $25.60 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $4.12 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `INDP` | 2 | $1.39 | $0.03 | — | $22.78 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+30.2; leftover $4.12 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $22.78 | ▼ close $9,759.97 vs 09:30 $9,775.84 (session -15.75) | 16:00 close · cash $22.78 · equity $9,759.97 vs 09:30 $9,775.84 (-15.87; session marks -15.75) · 11 name(s) marked open→close (per-name table). AG×56 09:30 $21.90 → close $21.09 -45.36; CDE×56 09:30 $21.75 → close $20.97 -43.68; HDSN×201 09:30 $5.67 → close $5.63 -8.04; IAG×59 09:30 $21.17 → close $21.14 -1.77; KGC×39 09:30 $32.17 → close $32.76 +23.01; NFGC×665 09:30 $1.79 → close $1.84 +33.25; WPM×8 09:30 $154.70 → close $157.78 +24.64; ABUS×236 09:30 $5.20 → close $5.21 +2.36; CYPH×3 09:30 $1.32 → close $1.42 +0.30; BTBT×2 09:30 $1.66 → close $1.53 -0.26; INDP×2 09:30 $1.39 → close $1.29 -0.20 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $22.78 | ▲ 09:30 equity $9,861.63 vs yday $9,759.97 (+101.66) | 09:30 open · cash $22.78 (unchanged overnight, no fees) · equity $9,861.63 vs prior close $9,759.97 (+101.66) · 11 name(s) re-marked at the open (per-name table). AG×56 yday $21.09 → 09:30 $21.47 +21.28; CDE×56 yday $20.97 → 09:30 $21.26 +16.24; HDSN×201 yday $5.63 → 09:30 $5.69 +12.06; IAG×59 yday $21.14 → 09:30 $21.44 +17.70; KGC×39 yday $32.76 → 09:30 $33.21 +17.55; NFGC×665 yday $1.84 → 09:30 $1.86 +13.30; WPM×8 yday $157.78 → 09:30 $158.96 +9.44; ABUS×236 yday $5.21 → 09:30 $5.18 -7.08; CYPH×3 yday $1.42 → 09:30 $1.83 +1.23; BTBT×2 yday $1.53 → 09:30 $1.55 +0.04; INDP×2 yday $1.29 → 09:30 $1.24 -0.10 | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $22.78 | ▼ close $9,733.34 vs 09:30 $9,861.63 (session -128.29) | 16:00 close · cash $22.78 · equity $9,733.34 vs 09:30 $9,861.63 (-128.29; session marks -128.29) · 11 name(s) marked open→close (per-name table). AG×56 09:30 $21.47 → close $20.57 -50.40; CDE×56 09:30 $21.26 → close $20.49 -43.12; HDSN×201 09:30 $5.69 → close $5.57 -24.12; IAG×59 09:30 $21.44 → close $21.36 -4.72; KGC×39 09:30 $33.21 → close $32.47 -28.86; NFGC×665 09:30 $1.86 → close $1.90 +26.60; WPM×8 09:30 $158.96 → close $158.00 -7.68; ABUS×236 09:30 $5.18 → close $5.20 +4.72; CYPH×3 09:30 $1.83 → close $1.64 -0.57; BTBT×2 09:30 $1.55 → close $1.56 +0.02; INDP×2 09:30 $1.24 → close $1.16 -0.16 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $22.78 | ▲ 09:30 equity $9,818.67 vs yday $9,733.34 (+85.33) | 09:30 open · cash $22.78 (unchanged overnight, no fees) · equity $9,818.67 vs prior close $9,733.34 (+85.33) · 11 name(s) re-marked at the open (per-name table). AG×56 yday $20.57 → 09:30 $20.73 +8.96; CDE×56 yday $20.49 → 09:30 $20.85 +20.16; HDSN×201 yday $5.57 → 09:30 $5.53 -8.04; IAG×59 yday $21.36 → 09:30 $21.63 +15.93; KGC×39 yday $32.47 → 09:30 $32.76 +11.31; NFGC×665 yday $1.90 → 09:30 $1.91 +6.65; WPM×8 yday $158.00 → 09:30 $160.00 +16.00; ABUS×236 yday $5.20 → 09:30 $5.26 +14.16; CYPH×3 yday $1.64 → 09:30 $1.70 +0.18; BTBT×2 yday $1.56 → 09:30 $1.55 -0.02; INDP×2 yday $1.16 → 09:30 $1.18 +0.04 | — |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 56 | $20.73 | $2.18 | $+5.74 | $1,181.48 | ▲ +5.74 after sell → book $9,816.49; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CDE` | 56 | $20.85 | $2.18 | $+6.86 | $2,346.91 | ▲ +6.86 after sell → book $9,814.32; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 201 | $5.53 | $2.64 | $-53.48 | $3,455.79 | ▼ -53.48 after sell → book $9,811.67; vs 09:30 mark -2.65 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 59 | $21.63 | $2.19 | $+113.65 | $4,729.78 | ▲ +113.65 after sell → book $9,809.49; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 39 | $32.76 | $2.13 | $+117.84 | $6,005.29 | ▲ +117.84 after sell → book $9,807.36; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 665 | $1.91 | $8.70 | $+89.12 | $7,266.74 | ▲ +89.12 after sell → book $9,798.66; vs 09:30 mark -8.70 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ABUS` | 236 | $5.26 | $3.09 | $+74.10 | $8,505.01 | ▲ +74.10 after sell → book $9,795.57; vs 09:30 mark -3.09 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 222 | $6.38 | $2.86 | — | $7,085.78 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=+5.0; leftover $1417.50 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `DEFT` | 2214 | $0.64 | $20.81 | — | $5,648.01 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover,ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1417.50 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `GORO` | 401 | $3.53 | $5.17 | — | $4,227.31 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+16.0; leftover $1417.50 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 41 | $34.48 | $2.11 | — | $2,811.52 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ⚪; ret5=+9.3; leftover $1417.50 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ERO` | 37 | $38.00 | $2.10 | — | $1,403.41 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot,mover_buy; ⚪; ret5=+16.6; leftover $1417.50 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 17 | $77.90 | $2.04 | — | $77.07 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot,mover_buy; ⚪; ret5=+15.3; leftover $1417.50 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $77.07 | ▲ close $9,762.76 vs 09:30 $9,818.67 (session +2.30) | 16:00 close · cash $77.07 · equity $9,762.76 vs 09:30 $9,818.67 (-55.91; session marks +2.30) · 10 name(s) marked open→close (per-name table). WPM×8 09:30 $160.00 → close $158.25 -14.00; CYPH×3 09:30 $1.70 → close $1.64 -0.18; BTBT×2 09:30 $1.55 → close $1.53 -0.04; INDP×2 09:30 $1.18 → close $1.25 +0.14; ZURA×222 09:30 $6.38 → close $6.50 +26.64; DEFT×2214 09:30 $0.64 → close $0.62 -44.28; GORO×401 09:30 $3.53 → close $3.56 +12.03; EZPW×41 09:30 $34.48 → close $34.69 +8.61; ERO×37 09:30 $38.00 → close $38.55 +20.35; FCX×17 09:30 $77.90 → close $77.49 -6.97 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $77.07 | ▲ 09:30 equity $9,762.76 vs yday $9,762.76 (+0.00) | 09:30 open · cash $77.07 (unchanged overnight, no fees) · equity $9,762.76 vs prior close $9,762.76 (+0.00) · 10 name(s) re-marked at the open (per-name table). WPM×8 yday $158.25 → 09:30 $158.25 +0.00; CYPH×3 yday $1.64 → 09:30 $1.64 +0.00; BTBT×2 yday $1.53 → 09:30 $1.53 +0.00; INDP×2 yday $1.25 → 09:30 $1.25 +0.00; ZURA×222 yday $6.50 → 09:30 $6.50 +0.00; DEFT×2214 yday $0.62 → 09:30 $0.62 +0.00; GORO×401 yday $3.56 → 09:30 $3.56 +0.00; EZPW×41 yday $34.69 → 09:30 $34.69 +0.00; ERO×37 yday $38.55 → 09:30 $38.55 +0.00; FCX×17 yday $77.49 → 09:30 $77.49 +0.00 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $77.07 | ▲ close $9,762.76 vs 09:30 $9,762.76 (session +0.00) | 16:00 close · cash $77.07 · equity $9,762.76 vs 09:30 $9,762.76 (+0.00; session marks +0.00) · 10 name(s) marked open→close (per-name table). WPM×8 09:30 $158.25 → close $158.25 +0.00; CYPH×3 09:30 $1.64 → close $1.64 +0.00; BTBT×2 09:30 $1.53 → close $1.53 +0.00; INDP×2 09:30 $1.25 → close $1.25 +0.00; ZURA×222 09:30 $6.50 → close $6.50 +0.00; DEFT×2214 09:30 $0.62 → close $0.62 +0.00; GORO×401 09:30 $3.56 → close $3.56 +0.00; EZPW×41 09:30 $34.69 → close $34.69 +0.00; ERO×37 09:30 $38.55 → close $38.55 +0.00; FCX×17 09:30 $77.49 → close $77.49 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $77.07 | ▲ 09:30 equity $9,886.93 vs yday $9,762.76 (+124.17) | 09:30 open · cash $77.07 (unchanged overnight, no fees) · equity $9,886.93 vs prior close $9,762.76 (+124.17) · 10 name(s) re-marked at the open (per-name table). WPM×8 yday $158.25 → 09:30 $160.93 +21.44; CYPH×3 yday $1.64 → 09:30 $1.60 -0.12; BTBT×2 yday $1.53 → 09:30 $1.53 +0.00; INDP×2 yday $1.25 → 09:30 $1.09 -0.32; ZURA×222 yday $6.50 → 09:30 $6.13 -82.14; DEFT×2214 yday $0.62 → 09:30 $0.60 -44.28; GORO×401 yday $3.56 → 09:30 $3.77 +84.21; EZPW×41 yday $34.69 → 09:30 $35.70 +41.41; ERO×37 yday $38.55 → 09:30 $40.51 +72.52; FCX×17 yday $77.49 → 09:30 $79.34 +31.45 | — |
| 2026-08-27 09:30 ET | **SELL** | `WPM` | 8 | $160.93 | $2.03 | $+127.07 | $1,362.48 | ▲ +127.07 after sell → book $9,884.90; vs 09:30 mark -2.03 | dropped from list after 5 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 3 | $1.60 | $0.08 | $+0.71 | $1,367.20 | ▲ +0.71 after sell → book $9,884.82; vs 09:30 mark -0.08 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `BTBT` | 2 | $1.53 | $0.06 | $-0.36 | $1,370.21 | ▼ -0.36 after sell → book $9,884.77; vs 09:30 mark -0.05 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `INDP` | 2 | $1.09 | $0.05 | $-0.68 | $1,372.34 | ▼ -0.68 after sell → book $9,884.72; vs 09:30 mark -0.05 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,372.34 | ▼ close $9,620.72 vs 09:30 $9,886.93 (session -264.00) | 16:00 close · cash $1,372.34 · equity $9,620.72 vs 09:30 $9,886.93 (-266.21; session marks -264.00) · 6 name(s) marked open→close (per-name table). ZURA×222 09:30 $6.13 → close $5.99 -31.08; DEFT×2214 09:30 $0.60 → close $0.59 -22.14; GORO×401 09:30 $3.77 → close $3.56 -84.21; EZPW×41 09:30 $35.70 → close $33.90 -73.80; ERO×37 09:30 $40.51 → close $39.24 -46.99; FCX×17 09:30 $79.34 → close $79.00 -5.78 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,372.34 | ▲ 09:30 equity $9,640.78 vs yday $9,620.72 (+20.06) | 09:30 open · cash $1,372.34 (unchanged overnight, no fees) · equity $9,640.78 vs prior close $9,620.72 (+20.06) · 6 name(s) re-marked at the open (per-name table). ZURA×222 yday $5.99 → 09:30 $6.02 +6.66; DEFT×2214 yday $0.59 → 09:30 $0.60 +22.14; GORO×401 yday $3.56 → 09:30 $3.59 +12.03; EZPW×41 yday $33.90 → 09:30 $33.50 -16.40; ERO×37 yday $39.24 → 09:30 $39.20 -1.48; FCX×17 yday $79.00 → 09:30 $78.83 -2.89 | — |
| 2026-08-28 09:30 ET | **SELL** | `ZURA` | 222 | $6.02 | $2.91 | $-85.70 | $2,705.87 | ▼ -85.70 after sell → book $9,637.87; vs 09:30 mark -2.91 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `DEFT` | 2214 | $0.60 | $20.30 | $-129.68 | $4,013.96 | ▼ -129.68 after sell → book $9,617.56; vs 09:30 mark -20.31 | dropped from list after 3 sess (min 3) | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🟡 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `GORO` | 401 | $3.59 | $5.25 | $+13.64 | $5,448.30 | ▲ +13.64 after sell → book $9,612.31; vs 09:30 mark -5.25 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `EZPW` | 41 | $33.50 | $2.13 | $-44.43 | $6,819.67 | ▼ -44.43 after sell → book $9,610.18; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ERO` | 37 | $39.20 | $2.12 | $+40.18 | $8,267.94 | ▲ +40.18 after sell → book $9,608.05; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `FCX` | 17 | $78.83 | $2.06 | $+11.71 | $9,605.99 | ▲ +11.71 after sell → book $9,605.99; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 catal🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 16 | $144.70 | $2.04 | — | $7,288.76 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $2401.50 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 129 | $18.50 | $2.38 | — | $4,899.88 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $2401.50 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 29 | $82.70 | $2.08 | — | $2,499.50 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=-4.6; leftover $2401.50 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TIGR` | 437 | $5.49 | $5.64 | — | $94.73 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; ret5=+15.9; leftover $2401.50 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟢 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $94.73 | ▼ close $9,244.86 vs 09:30 $9,640.78 (session -349.00) | 16:00 close · cash $94.73 · equity $9,244.86 vs 09:30 $9,640.78 (-395.92; session marks -349.00) · 4 name(s) marked open→close (per-name table). ANF×16 09:30 $144.70 → close $145.75 +16.80; BZ×129 09:30 $18.50 → close $18.00 -64.50; URBN×29 09:30 $82.70 → close $78.79 -113.39; TIGR×437 09:30 $5.49 → close $5.06 -187.91 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $94.73 | ▲ 09:30 equity $9,300.39 vs yday $9,244.86 (+55.53) | 09:30 open · cash $94.73 (unchanged overnight, no fees) · equity $9,300.39 vs prior close $9,244.86 (+55.53) · 4 name(s) re-marked at the open (per-name table). ANF×16 yday $145.75 → 09:30 $148.67 +46.72; BZ×129 yday $18.00 → 09:30 $17.89 -14.19; URBN×29 yday $78.79 → 09:30 $81.09 +66.70; TIGR×437 yday $5.06 → 09:30 $4.96 -43.70 | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $94.73 | ▲ close $9,333.29 vs 09:30 $9,300.39 (session +32.90) | 16:00 close · cash $94.73 · equity $9,333.29 vs 09:30 $9,300.39 (+32.90; session marks +32.90) · 4 name(s) marked open→close (per-name table). ANF×16 09:30 $148.67 → close $149.28 +9.76; BZ×129 09:30 $17.89 → close $17.90 +1.29; URBN×29 09:30 $81.09 → close $81.09 +0.00; TIGR×437 09:30 $4.96 → close $5.01 +21.85 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $94.73 | ▼ 09:30 equity $9,148.73 vs yday $9,333.29 (-184.56) | 09:30 open · cash $94.73 (unchanged overnight, no fees) · equity $9,148.73 vs prior close $9,333.29 (-184.56) · 4 name(s) re-marked at the open (per-name table). ANF×16 yday $149.28 → 09:30 $142.47 -108.96; BZ×129 yday $17.90 → 09:30 $17.37 -68.37; URBN×29 yday $81.09 → 09:30 $80.69 -11.60; TIGR×437 yday $5.01 → 09:30 $5.02 +4.37 | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $94.73 | ▼ close $9,122.67 vs 09:30 $9,148.73 (session -26.06) | 16:00 close · cash $94.73 · equity $9,122.67 vs 09:30 $9,148.73 (-26.06; session marks -26.06) · 4 name(s) marked open→close (per-name table). ANF×16 09:30 $142.47 → close $143.00 +8.48; BZ×129 09:30 $17.37 → close $17.17 -25.80; URBN×29 09:30 $80.69 → close $80.69 +0.00; TIGR×437 09:30 $5.02 → close $5.00 -8.74 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $94.73 | ▼ 09:30 equity $9,063.51 vs yday $9,122.67 (-59.16) | 09:30 open · cash $94.73 (unchanged overnight, no fees) · equity $9,063.51 vs prior close $9,122.67 (-59.16) · 4 name(s) re-marked at the open (per-name table). ANF×16 yday $143.00 → 09:30 $142.00 -16.00; BZ×129 yday $17.17 → 09:30 $17.29 +15.48; URBN×29 yday $80.69 → 09:30 $79.12 -45.53; TIGR×437 yday $5.00 → 09:30 $4.97 -13.11 | — |
| 2026-09-02 09:30 ET | **SELL** | `ANF` | 16 | $142.00 | $2.07 | $-47.30 | $2,364.67 | ▼ -47.30 after sell → book $9,061.45; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BZ` | 129 | $17.29 | $2.42 | $-160.88 | $4,592.66 | ▼ -160.88 after sell → book $9,059.03; vs 09:30 mark -2.42 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `URBN` | 29 | $79.12 | $2.11 | $-108.00 | $6,885.04 | ▼ -108.00 after sell → book $9,056.93; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `TIGR` | 437 | $4.97 | $5.73 | $-238.60 | $9,051.20 | ▼ -238.60 after sell → book $9,051.20; vs 09:30 mark -5.73 | dropped from list after 3 sess (min 3) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,051.20 | ▲ close $9,051.20 vs 09:30 $9,063.51 (session +0.00) | 16:00 close · cash $9,051.20 · no lots left · equity $9,051.20. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,051.20 | ▲ 09:30 equity $9,051.20 vs yday $9,051.20 (-0.00) | 09:30 open · cash $9,051.20 · no holdings · equity $9,051.20 vs prior close $9,051.20 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 8 | $125.94 | $2.01 | — | $8,041.66 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1131.40 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 72 | $15.70 | $2.21 | — | $6,909.06 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1131.40 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 49 | $22.78 | $2.14 | — | $5,790.70 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1131.40 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 7 | $151.40 | $2.01 | — | $4,728.89 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+129.2; leftover $1131.40 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 68 | $16.46 | $2.19 | — | $3,607.42 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+63.4; leftover $1131.40 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `NVAX` | 110 | $10.27 | $2.32 | — | $2,475.40 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+11.1; leftover $1131.40 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ALMS` | 111 | $10.15 | $2.32 | — | $1,346.42 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; 🔵; ret5=-4.5; leftover $1131.40 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `OSW` | 50 | $22.53 | $2.14 | — | $217.78 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; 🔵; ret5=-0.9; leftover $1131.40 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $217.78 | ▲ close $9,121.46 vs 09:30 $9,051.20 (session +87.61) | 16:00 close · cash $217.78 · equity $9,121.46 vs 09:30 $9,051.20 (+70.26; session marks +87.61) · 8 name(s) marked open→close (per-name table). RVTY×8 09:30 $125.94 → close $130.94 +40.00; CRK×72 09:30 $15.70 → close $15.54 -11.52; MMED×49 09:30 $22.78 → close $23.76 +48.02; MRNA×7 09:30 $151.40 → close $150.81 -4.13; ARCT×68 09:30 $16.46 → close $16.74 +19.04; NVAX×110 09:30 $10.27 → close $10.32 +5.50; ALMS×111 09:30 $10.15 → close $10.35 +22.20; OSW×50 09:30 $22.53 → close $21.90 -31.50 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $217.78 | ▼ 09:30 equity $9,119.19 vs yday $9,121.46 (-2.27) | 09:30 open · cash $217.78 (unchanged overnight, no fees) · equity $9,119.19 vs prior close $9,121.46 (-2.27) · 8 name(s) re-marked at the open (per-name table). RVTY×8 yday $130.94 → 09:30 $132.45 +12.08; CRK×72 yday $15.54 → 09:30 $15.45 -6.48; MMED×49 yday $23.76 → 09:30 $23.88 +5.88; MRNA×7 yday $150.81 → 09:30 $145.95 -34.02; ARCT×68 yday $16.74 → 09:30 $16.77 +2.04; NVAX×110 yday $10.32 → 09:30 $10.41 +9.90; ALMS×111 yday $10.35 → 09:30 $10.38 +3.33; OSW×50 yday $21.90 → 09:30 $22.00 +5.00 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 7 | $5.08 | $0.38 | — | $181.85 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+28.1; leftover $36.30 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 13 | $2.70 | $0.39 | — | $146.36 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.4; leftover $36.30 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `TRLV` | 3 | $11.89 | $0.37 | — | $110.32 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ⚪; ret5=+15.0; leftover $36.30 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $110.32 | ▼ close $9,113.80 vs 09:30 $9,119.19 (session -4.26) | 16:00 close · cash $110.32 · equity $9,113.80 vs 09:30 $9,119.19 (-5.39; session marks -4.26) · 11 name(s) marked open→close (per-name table). RVTY×8 09:30 $132.45 → close $130.63 -14.56; CRK×72 09:30 $15.45 → close $14.95 -36.00; MMED×49 09:30 $23.88 → close $23.84 -1.96; MRNA×7 09:30 $145.95 → close $148.87 +20.44; ARCT×68 09:30 $16.77 → close $15.56 -82.28; NVAX×110 09:30 $10.41 → close $10.34 -7.70; ALMS×111 09:30 $10.38 → close $11.36 +108.78; OSW×50 09:30 $22.00 → close $22.27 +13.50; OABI×7 09:30 $5.08 → close $4.75 -2.31; ALEC×13 09:30 $2.70 → close $2.51 -2.47; TRLV×3 09:30 $11.89 → close $11.99 +0.30 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `BETR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `HYLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ADUR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `AIRO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `NCMI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `QMLS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ABX` | cash | leftover split 0.71 < 1 share @ 9.12 |
| 2026-08-17 | `ALOY` | cash | leftover split 0.71 < 1 share @ 14.66 |
| 2026-08-17 | `BORR` | cash | leftover split 0.71 < 1 share @ 4.59 |
| 2026-08-17 | `XHG` | cash | leftover split 0.71 < 1 share @ 4.19 |
| 2026-08-17 | `MP` | cash | leftover split 0.71 < 1 share @ 58.01 |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BETR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ANGX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `HYLN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ADUR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `AIRO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `NCMI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `QMLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ABUS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 4.12 < 1 share @ 119.43 |
| 2026-08-21 | `AUPH` | cash | leftover split 4.12 < 1 share @ 17.20 |
| 2026-08-21 | `AEM` | cash | leftover split 4.12 < 1 share @ 216.30 |
| 2026-08-21 | `ARCT` | cash | leftover split 4.12 < 1 share @ 11.13 |
| 2026-08-21 | `TEM` | cash | leftover split 4.12 < 1 share @ 65.60 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ABUS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `INDP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TMC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ELMT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ERO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SCCO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `INDP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `WPM` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CYPH` | no_price | no 09:30 open — carry |
| 2026-08-26 | `BTBT` | no_price | no 09:30 open — carry |
| 2026-08-26 | `INDP` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ZURA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `DEFT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `GORO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `EZPW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ERO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `FCX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `USDE` | no_price | no 09:30 open |
| 2026-08-26 | `DKS` | no_price | no 09:30 open |
| 2026-08-27 | `ZURA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `DEFT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `GORO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `EZPW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ERO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `FCX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `ANF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BZ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `URBN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `TIGR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PURR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TYL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `ANF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `URBN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `TIGR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MMED` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MRNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `NVAX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ALMS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `OSW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DELL` | cash | leftover split 36.30 < 1 share @ 486.31 |
| 2026-09-04 | `TARS` | cash | leftover split 36.30 < 1 share @ 82.76 |
| 2026-09-04 | `MDB` | cash | leftover split 36.30 < 1 share @ 378.76 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `RVTY` | 8 | 2026-09-03 @ $125.94 | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1131.40 |
| `CRK` | 72 | 2026-09-03 @ $15.70 | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1131.40 |
| `MMED` | 49 | 2026-09-03 @ $22.78 | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1131.40 |
| `MRNA` | 7 | 2026-09-03 @ $151.40 | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+129.2; leftover $1131.40 |
| `ARCT` | 68 | 2026-09-03 @ $16.46 | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+63.4; leftover $1131.40 |
| `NVAX` | 110 | 2026-09-03 @ $10.27 | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+11.1; leftover $1131.40 |
| `ALMS` | 111 | 2026-09-03 @ $10.15 | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; 🔵; ret5=-4.5; leftover $1131.40 |
| `OSW` | 50 | 2026-09-03 @ $22.53 | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; 🔵; ret5=-0.9; leftover $1131.40 |
| `OABI` | 7 | 2026-09-04 @ $5.08 | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+28.1; leftover $36.30 |
| `ALEC` | 13 | 2026-09-04 @ $2.70 | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.4; leftover $36.30 |
| `TRLV` | 3 | 2026-09-04 @ $11.89 | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ⚪; ret5=+15.0; leftover $36.30 |
