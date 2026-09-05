# Factor mine action — `union_news_vol_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **+0.89%** ($10,089) · signal-only (no cash/fees) was -1.72%. Starts YES **16/17**. Fills 60 · skips 76 · realized $-354.11.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `news=good,vol=good` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $0.54.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `ANGX` | 464 | — | $4.31 | +0.00 | $4.37 | +27.84 | +27.84 | +0.00 | +27.84 |
| 2026-08-14 | `ARX` | 102 | — | $19.57 | +0.00 | $19.58 | +1.02 | +1.02 | +0.00 | +1.02 |
| 2026-08-14 | `SNDK` | 1 | — | $1646.93 | +0.00 | $1641.11 | -5.82 | -5.82 | +0.00 | -5.82 |
| 2026-08-14 | `MH` | 147 | — | $13.55 | +0.00 | $13.10 | -66.15 | -66.15 | +0.00 | -66.15 |
| 2026-08-14 | `HLIT` | 151 | — | $13.18 | +0.00 | $13.92 | +111.74 | +111.74 | +0.00 | +111.74 |
| 2026-08-17 | `ANGX` | 464 | $4.37 | $4.60 | +106.72 | $4.71 | +51.04 | +157.76 | +134.56 | +185.60 |
| 2026-08-17 | `ARX` | 102 | $19.58 | $19.57 | -1.02 | $19.54 | -3.06 | -4.08 | +0.00 | -3.06 |
| 2026-08-17 | `SNDK` | 1 | $1641.11 | $1700.74 | +59.63 | $1786.85 | +86.11 | +145.74 | +53.81 | +139.92 |
| 2026-08-17 | `MH` | 147 | $13.10 | $13.16 | +8.82 | $12.77 | -57.33 | -48.51 | -57.33 | -114.66 |
| 2026-08-17 | `HLIT` | 151 | $13.92 | $13.84 | -12.08 | $13.43 | -61.91 | -73.99 | +99.66 | +37.75 |
| 2026-08-18 | `ANGX` | 464 | $4.71 | $4.79 | +37.12 | $4.85 | +27.84 | +64.96 | +222.72 | +250.56 |
| 2026-08-18 | `ARX` | 102 | $19.54 | $19.57 | +3.06 | $19.56 | -1.02 | +2.04 | +0.00 | -1.02 |
| 2026-08-18 | `SNDK` | 1 | $1786.85 | $1677.54 | -109.31 | $1625.78 | -51.76 | -161.07 | +30.61 | -21.15 |
| 2026-08-18 | `MH` | 147 | $12.77 | $13.00 | +33.81 | $13.12 | +17.64 | +51.45 | -80.85 | -63.21 |
| 2026-08-18 | `HLIT` | 151 | $13.43 | $12.93 | -75.50 | $12.73 | -30.20 | -105.70 | -37.75 | -67.95 |
| 2026-08-19 | `ANGX` | 464 | $4.85 | $4.79 | -27.84 | $4.60 | -88.16 | -116.00 | +222.72 | +134.56 |
| 2026-08-19 | `ARX` | 102 | $19.56 | $19.58 | +2.04 | — | +0.00 | +2.04 | +1.02 | — |
| 2026-08-19 | `SNDK` | 1 | $1625.78 | $1682.40 | +56.62 | — | +0.00 | +56.62 | +35.47 | — |
| 2026-08-19 | `MH` | 147 | $13.12 | $13.01 | -16.17 | — | +0.00 | -16.17 | -79.38 | — |
| 2026-08-19 | `HLIT` | 151 | $12.73 | $12.90 | +25.67 | — | +0.00 | +25.67 | -42.28 | — |
| 2026-08-20 | `ANGX` | 464 | $4.60 | $4.57 | -13.92 | — | +0.00 | -13.92 | +120.64 | — |
| 2026-08-20 | `BHP` | 13 | — | $91.01 | +0.00 | $93.63 | +34.06 | +34.06 | +0.00 | +34.06 |
| 2026-08-20 | `MRNA` | 8 | — | $150.14 | +0.00 | $133.32 | -134.56 | -134.56 | +0.00 | -134.56 |
| 2026-08-20 | `HUMA` | 1768 | — | $0.71 | +0.00 | $0.68 | -45.97 | -45.97 | +0.00 | -45.97 |
| 2026-08-20 | `BTGO` | 189 | — | $6.61 | +0.00 | $6.60 | -0.95 | -0.95 | +0.00 | -0.95 |
| 2026-08-20 | `ZLAB` | 47 | — | $26.57 | +0.00 | $26.02 | -25.85 | -25.85 | +0.00 | -25.85 |
| 2026-08-20 | `CRSP` | 21 | — | $58.73 | +0.00 | $58.12 | -12.81 | -12.81 | +0.00 | -12.81 |
| 2026-08-20 | `APA` | 27 | — | $44.76 | +0.00 | $44.39 | -9.99 | -9.99 | +0.00 | -9.99 |
| 2026-08-20 | `AUTL` | 506 | — | $2.47 | +0.00 | $2.46 | -5.06 | -5.06 | +0.00 | -5.06 |
| 2026-08-21 | `BHP` | 13 | $93.63 | $95.72 | +27.17 | $97.03 | +17.03 | +44.20 | +61.23 | +78.26 |
| 2026-08-21 | `MRNA` | 8 | $133.32 | $133.11 | -1.68 | $145.13 | +96.16 | +94.48 | -136.24 | -40.08 |
| 2026-08-21 | `HUMA` | 1768 | $0.68 | $0.67 | -12.38 | $0.64 | -56.58 | -68.96 | -58.34 | -114.92 |
| 2026-08-21 | `BTGO` | 189 | $6.60 | $6.95 | +66.15 | $6.84 | -20.79 | +45.36 | +65.20 | +44.41 |
| 2026-08-21 | `ZLAB` | 47 | $26.02 | $26.25 | +10.81 | $26.01 | -11.28 | -0.47 | -15.04 | -26.32 |
| 2026-08-21 | `CRSP` | 21 | $58.12 | $59.72 | +33.60 | $59.50 | -4.62 | +28.98 | +20.79 | +16.17 |
| 2026-08-21 | `APA` | 27 | $44.39 | $44.52 | +3.51 | $43.39 | -30.51 | -27.00 | -6.48 | -36.99 |
| 2026-08-21 | `AUTL` | 506 | $2.46 | $2.47 | +5.06 | $2.41 | -30.36 | -25.30 | +0.00 | -30.36 |
| 2026-08-21 | `MARA` | 2 | — | $11.70 | +0.00 | $11.26 | -0.88 | -0.88 | +0.00 | -0.88 |
| 2026-08-21 | `BTDR` | 2 | — | $11.10 | +0.00 | $11.37 | +0.55 | +0.55 | +0.00 | +0.55 |
| 2026-08-21 | `HIVE` | 7 | — | $3.24 | +0.00 | $3.03 | -1.47 | -1.47 | +0.00 | -1.47 |
| 2026-08-24 | `BHP` | 13 | $97.03 | $97.34 | +4.03 | $96.66 | -8.84 | -4.81 | +82.29 | +73.45 |
| 2026-08-24 | `MRNA` | 8 | $145.13 | $142.70 | -19.44 | $139.27 | -27.44 | -46.88 | -59.52 | -86.96 |
| 2026-08-24 | `HUMA` | 1768 | $0.64 | $0.68 | +67.18 | $0.67 | -17.68 | +49.50 | -47.74 | -65.42 |
| 2026-08-24 | `BTGO` | 189 | $6.84 | $6.87 | +5.67 | $6.97 | +18.90 | +24.57 | +50.08 | +68.98 |
| 2026-08-24 | `ZLAB` | 47 | $26.01 | $25.59 | -19.74 | $25.51 | -3.76 | -23.50 | -46.06 | -49.82 |
| 2026-08-24 | `CRSP` | 21 | $59.50 | $58.79 | -14.91 | $56.91 | -39.48 | -54.39 | +1.26 | -38.22 |
| 2026-08-24 | `APA` | 27 | $43.39 | $42.93 | -12.42 | $42.10 | -22.41 | -34.83 | -49.41 | -71.82 |
| 2026-08-24 | `AUTL` | 506 | $2.41 | $2.36 | -25.30 | $2.38 | +10.12 | -15.18 | -55.66 | -45.54 |
| 2026-08-24 | `MARA` | 2 | $11.26 | $11.18 | -0.16 | $11.44 | +0.52 | +0.36 | -1.04 | -0.52 |
| 2026-08-24 | `BTDR` | 2 | $11.37 | $11.49 | +0.24 | $11.30 | -0.38 | -0.14 | +0.79 | +0.41 |
| 2026-08-24 | `HIVE` | 7 | $3.03 | $2.98 | -0.35 | $2.94 | -0.28 | -0.63 | -1.82 | -2.10 |
| 2026-08-25 | `BHP` | 13 | $96.66 | $95.95 | -9.23 | — | +0.00 | -9.23 | +64.22 | — |
| 2026-08-25 | `MRNA` | 8 | $139.27 | $141.19 | +15.36 | — | +0.00 | +15.36 | -71.60 | — |
| 2026-08-25 | `HUMA` | 1768 | $0.67 | $0.67 | +0.00 | — | +0.00 | +0.00 | -65.42 | — |
| 2026-08-25 | `BTGO` | 189 | $6.97 | $6.89 | -15.12 | — | +0.00 | -15.12 | +53.86 | — |
| 2026-08-25 | `ZLAB` | 47 | $25.51 | $25.93 | +19.74 | — | +0.00 | +19.74 | -30.08 | — |
| 2026-08-25 | `CRSP` | 21 | $56.91 | $57.00 | +1.89 | — | +0.00 | +1.89 | -36.33 | — |
| 2026-08-25 | `APA` | 27 | $42.10 | $42.70 | +16.20 | — | +0.00 | +16.20 | -55.62 | — |
| 2026-08-25 | `AUTL` | 506 | $2.38 | $2.32 | -30.36 | — | +0.00 | -30.36 | -75.90 | — |
| 2026-08-25 | `MARA` | 2 | $11.44 | $11.28 | -0.32 | $11.29 | +0.02 | -0.30 | -0.84 | -0.82 |
| 2026-08-25 | `BTDR` | 2 | $11.30 | $11.19 | -0.22 | $11.28 | +0.18 | -0.04 | +0.19 | +0.37 |
| 2026-08-25 | `HIVE` | 7 | $2.94 | $2.82 | -0.84 | $2.89 | +0.49 | -0.35 | -2.94 | -2.45 |
| 2026-08-25 | `RUM` | 147 | — | $9.36 | +0.00 | $9.35 | -1.47 | -1.47 | +0.00 | -1.47 |
| 2026-08-25 | `EZPW` | 39 | — | $34.48 | +0.00 | $34.69 | +8.19 | +8.19 | +0.00 | +8.19 |
| 2026-08-25 | `REAX` | 57 | — | $24.00 | +0.00 | $24.00 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-25 | `BKKT` | 166 | — | $8.28 | +0.00 | $8.38 | +16.60 | +16.60 | +0.00 | +16.60 |
| 2026-08-25 | `FCX` | 17 | — | $77.90 | +0.00 | $77.49 | -6.97 | -6.97 | +0.00 | -6.97 |
| 2026-08-25 | `NVAX` | 155 | — | $8.88 | +0.00 | $8.93 | +7.75 | +7.75 | +0.00 | +7.75 |
| 2026-08-25 | `AU` | 11 | — | $119.46 | +0.00 | $118.55 | -10.01 | -10.01 | +0.00 | -10.01 |
| 2026-08-26 | `MARA` | 2 | $11.29 | $11.29 | +0.00 | $11.29 | +0.00 | +0.00 | -0.82 | -0.82 |
| 2026-08-26 | `BTDR` | 2 | $11.28 | $11.28 | +0.00 | $11.28 | +0.00 | +0.00 | +0.37 | +0.37 |
| 2026-08-26 | `HIVE` | 7 | $2.89 | $2.89 | +0.00 | $2.89 | +0.00 | +0.00 | -2.45 | -2.45 |
| 2026-08-26 | `RUM` | 147 | $9.35 | $9.35 | +0.00 | $9.35 | +0.00 | +0.00 | -1.47 | -1.47 |
| 2026-08-26 | `EZPW` | 39 | $34.69 | $34.69 | +0.00 | $34.69 | +0.00 | +0.00 | +8.19 | +8.19 |
| 2026-08-26 | `REAX` | 57 | $24.00 | $24.00 | +0.00 | $24.00 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-26 | `BKKT` | 166 | $8.38 | $8.38 | +0.00 | $8.38 | +0.00 | +0.00 | +16.60 | +16.60 |
| 2026-08-26 | `FCX` | 17 | $77.49 | $77.49 | +0.00 | $77.49 | +0.00 | +0.00 | -6.97 | -6.97 |
| 2026-08-26 | `NVAX` | 155 | $8.93 | $8.93 | +0.00 | $8.93 | +0.00 | +0.00 | +7.75 | +7.75 |
| 2026-08-26 | `AU` | 11 | $118.55 | $118.55 | +0.00 | $118.55 | +0.00 | +0.00 | -10.01 | -10.01 |
| 2026-08-27 | `MARA` | 2 | $11.29 | $11.56 | +0.54 | — | +0.00 | +0.54 | -0.28 | — |
| 2026-08-27 | `BTDR` | 2 | $11.28 | $11.05 | -0.46 | — | +0.00 | -0.46 | -0.09 | — |
| 2026-08-27 | `HIVE` | 7 | $2.89 | $2.95 | +0.42 | — | +0.00 | +0.42 | -2.03 | — |
| 2026-08-27 | `RUM` | 147 | $9.35 | $10.07 | +105.84 | $9.38 | -101.43 | +4.41 | +104.37 | +2.94 |
| 2026-08-27 | `EZPW` | 39 | $34.69 | $35.70 | +39.39 | $33.90 | -70.20 | -30.81 | +47.58 | -22.62 |
| 2026-08-27 | `REAX` | 57 | $24.00 | $26.61 | +148.77 | $26.59 | -1.14 | +147.63 | +148.77 | +147.63 |
| 2026-08-27 | `BKKT` | 166 | $8.38 | $8.38 | +0.00 | $8.23 | -24.90 | -24.90 | +16.60 | -8.30 |
| 2026-08-27 | `FCX` | 17 | $77.49 | $79.34 | +31.45 | $79.00 | -5.78 | +25.67 | +24.48 | +18.70 |
| 2026-08-27 | `NVAX` | 155 | $8.93 | $9.33 | +62.00 | $9.21 | -18.60 | +43.40 | +69.75 | +51.15 |
| 2026-08-27 | `AU` | 11 | $118.55 | $119.80 | +13.75 | $118.11 | -18.59 | -4.84 | +3.74 | -14.85 |
| 2026-08-28 | `RUM` | 147 | $9.38 | $9.51 | +19.11 | — | +0.00 | +19.11 | +22.05 | — |
| 2026-08-28 | `EZPW` | 39 | $33.90 | $33.50 | -15.60 | — | +0.00 | -15.60 | -38.22 | — |
| 2026-08-28 | `REAX` | 57 | $26.59 | $25.91 | -38.76 | — | +0.00 | -38.76 | +108.87 | — |
| 2026-08-28 | `BKKT` | 166 | $8.23 | $8.50 | +44.82 | — | +0.00 | +44.82 | +36.52 | — |
| 2026-08-28 | `FCX` | 17 | $79.00 | $78.83 | -2.89 | — | +0.00 | -2.89 | +15.81 | — |
| 2026-08-28 | `NVAX` | 155 | $9.21 | $9.12 | -13.95 | — | +0.00 | -13.95 | +37.20 | — |
| 2026-08-28 | `AU` | 11 | $118.11 | $117.41 | -7.70 | — | +0.00 | -7.70 | -22.55 | — |
| 2026-08-28 | `CAPR` | 178 | — | $9.19 | +0.00 | $10.06 | +154.86 | +154.86 | +0.00 | +154.86 |
| 2026-08-28 | `SEDG` | 48 | — | $33.78 | +0.00 | $33.51 | -12.96 | -12.96 | +0.00 | -12.96 |
| 2026-08-28 | `SMTC` | 10 | — | $149.40 | +0.00 | $142.43 | -69.70 | -69.70 | +0.00 | -69.70 |
| 2026-08-28 | `ERAS` | 84 | — | $19.30 | +0.00 | $19.49 | +15.96 | +15.96 | +0.00 | +15.96 |
| 2026-08-28 | `BBWI` | 87 | — | $18.68 | +0.00 | $18.65 | -2.61 | -2.61 | +0.00 | -2.61 |
| 2026-08-28 | `ZYME` | 55 | — | $29.33 | +0.00 | $29.01 | -17.60 | -17.60 | +0.00 | -17.60 |
| 2026-08-31 | `CAPR` | 178 | $10.06 | $9.44 | -110.36 | $9.36 | -14.24 | -124.60 | +44.50 | +30.26 |
| 2026-08-31 | `SEDG` | 48 | $33.51 | $31.50 | -96.48 | $31.27 | -11.04 | -107.52 | -109.44 | -120.48 |
| 2026-08-31 | `SMTC` | 10 | $142.43 | $133.04 | -93.90 | $132.54 | -5.00 | -98.90 | -163.60 | -168.60 |
| 2026-08-31 | `ERAS` | 84 | $19.49 | $17.90 | -133.56 | $17.90 | +0.00 | -133.56 | -117.60 | -117.60 |
| 2026-08-31 | `BBWI` | 87 | $18.65 | $19.30 | +56.55 | $19.22 | -6.96 | +49.59 | +53.94 | +46.98 |
| 2026-08-31 | `ZYME` | 55 | $29.01 | $28.27 | -40.70 | $28.27 | +0.00 | -40.70 | -58.30 | -58.30 |
| 2026-09-01 | `CAPR` | 178 | $9.36 | $10.43 | +190.46 | $10.19 | -42.72 | +147.74 | +220.72 | +178.00 |
| 2026-09-01 | `SEDG` | 48 | $31.27 | $32.22 | +45.60 | $31.80 | -20.16 | +25.44 | -74.88 | -95.04 |
| 2026-09-01 | `SMTC` | 10 | $132.54 | $131.65 | -8.90 | $129.50 | -21.50 | -30.40 | -177.50 | -199.00 |
| 2026-09-01 | `ERAS` | 84 | $17.90 | $18.00 | +8.40 | $17.70 | -25.20 | -16.80 | -109.20 | -134.40 |
| 2026-09-01 | `BBWI` | 87 | $19.22 | $19.10 | -10.44 | $19.10 | +0.00 | -10.44 | +36.54 | +36.54 |
| 2026-09-01 | `ZYME` | 55 | $28.27 | $29.32 | +57.75 | $29.33 | +0.55 | +58.30 | -0.55 | +0.00 |
| 2026-09-02 | `CAPR` | 178 | $10.19 | $10.77 | +103.24 | — | +0.00 | +103.24 | +281.24 | — |
| 2026-09-02 | `SEDG` | 48 | $31.80 | $31.87 | +3.36 | — | +0.00 | +3.36 | -91.68 | — |
| 2026-09-02 | `SMTC` | 10 | $129.50 | $127.63 | -18.70 | — | +0.00 | -18.70 | -217.70 | — |
| 2026-09-02 | `ERAS` | 84 | $17.70 | $17.58 | -10.08 | — | +0.00 | -10.08 | -144.48 | — |
| 2026-09-02 | `BBWI` | 87 | $19.10 | $18.77 | -28.71 | — | +0.00 | -28.71 | +7.83 | — |
| 2026-09-02 | `ZYME` | 55 | $29.33 | $29.32 | -0.55 | — | +0.00 | -0.55 | -0.55 | — |
| 2026-09-03 | `MMED` | 423 | — | $22.78 | +0.00 | $23.76 | +414.54 | +414.54 | +0.00 | +414.54 |
| 2026-09-04 | `MMED` | 423 | $23.76 | $23.88 | +50.76 | $23.84 | -16.92 | +33.84 | +465.30 | +448.38 |
| 2026-09-04 | `BAK` | 2 | — | $1.95 | +0.00 | $1.94 | -0.02 | -0.02 | +0.00 | -0.02 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +68.63 | ANGX, ARX, SNDK, MH, HLIT | — | $359.91 | $10,053.48 | ANGX×464, ARX×102, SNDK×1, MH×147, HLIT×151 |
| 2026-08-17 | +2.25 | $359.91 | ANGX×464, ARX×102, SNDK×1, MH×147, HLIT×151 | $10,215.56 | +162.08 | +14.85 | — | — | $359.91 | $10,230.40 | ANGX×464, ARX×102, SNDK×1, MH×147, HLIT×151 |
| 2026-08-18 | -6.20 | $359.91 | ANGX×464, ARX×102, SNDK×1, MH×147, HLIT×151 | $10,119.58 | -110.82 | -37.50 | — | — | $359.91 | $10,082.08 | ANGX×464, ARX×102, SNDK×1, MH×147, HLIT×151 |
| 2026-08-19 | -7.20 | $359.91 | ANGX×464, ARX×102, SNDK×1, MH×147, HLIT×151 | $10,122.41 | +40.33 | -88.16 | — | ARX, SNDK, MH, HLIT | $7,890.55 | $10,024.95 | ANGX×464 |
| 2026-08-20 | +1.12 | $7,890.55 | ANGX×464 | $10,011.03 | -13.92 | -201.13 | BHP, MRNA, HUMA, BTGO, ZLAB, CRSP, APA, AUTL | ANGX | $144.73 | $9,766.64 | BHP×13, MRNA×8, HUMA×1768, BTGO×189, ZLAB×47, CRSP×21, APA×27, AUTL×506 |
| 2026-08-21 | +3.25 | $144.73 | BHP×13, MRNA×8, HUMA×1768, BTGO×189, ZLAB×47, CRSP×21, APA×27, AUTL×506 | $9,898.88 | +132.24 | -42.75 | MARA, BTDR, HIVE | — | $75.74 | $9,855.42 | BHP×13, MRNA×8, HUMA×1768, BTGO×189, ZLAB×47, CRSP×21, APA×27, AUTL×506, MARA×2, BTDR×2, HIVE×7 |
| 2026-08-24 | -5.17 | $75.74 | BHP×13, MRNA×8, HUMA×1768, BTGO×189, ZLAB×47, CRSP×21, APA×27, AUTL×506, MARA×2, BTDR×2, HIVE×7 | $9,840.22 | -15.20 | -90.73 | — | — | $75.74 | $9,749.49 | BHP×13, MRNA×8, HUMA×1768, BTGO×189, ZLAB×47, CRSP×21, APA×27, AUTL×506, MARA×2, BTDR×2, HIVE×7 |
| 2026-08-25 | +1.80 | $75.74 | BHP×13, MRNA×8, HUMA×1768, BTGO×189, ZLAB×47, CRSP×21, APA×27, AUTL×506, MARA×2, BTDR×2, HIVE×7 | $9,746.59 | -2.90 | +14.78 | RUM, EZPW, REAX, BKKT, FCX, NVAX, AU | BHP, MRNA, HUMA, BTGO, ZLAB, CRSP, APA, AUTL | $151.26 | $9,708.60 | MARA×2, BTDR×2, HIVE×7, RUM×147, EZPW×39, REAX×57, BKKT×166, FCX×17, NVAX×155, AU×11 |
| 2026-08-26 | +2.02 | $151.26 | MARA×2, BTDR×2, HIVE×7, RUM×147, EZPW×39, REAX×57, BKKT×166, FCX×17, NVAX×155, AU×11 | $9,708.60 | -0.00 | +0.00 | — | — | $151.26 | $9,708.60 | MARA×2, BTDR×2, HIVE×7, RUM×147, EZPW×39, REAX×57, BKKT×166, FCX×17, NVAX×155, AU×11 |
| 2026-08-27 | — | $151.26 | MARA×2, BTDR×2, HIVE×7, RUM×147, EZPW×39, REAX×57, BKKT×166, FCX×17, NVAX×155, AU×11 | $10,110.30 | +401.70 | -240.64 | — | MARA, BTDR, HIVE | $216.38 | $9,868.91 | RUM×147, EZPW×39, REAX×57, BKKT×166, FCX×17, NVAX×155, AU×11 |
| 2026-08-28 | +0.75 | $216.38 | RUM×147, EZPW×39, REAX×57, BKKT×166, FCX×17, NVAX×155, AU×11 | $9,853.94 | -14.97 | +67.95 | CAPR, SEDG, SMTC, ERAS, BBWI, ZYME | RUM, EZPW, REAX, BKKT, FCX, NVAX, AU | $213.94 | $9,892.66 | CAPR×178, SEDG×48, SMTC×10, ERAS×84, BBWI×87, ZYME×55 |
| 2026-08-31 | -5.85 | $213.94 | CAPR×178, SEDG×48, SMTC×10, ERAS×84, BBWI×87, ZYME×55 | $9,474.21 | -418.45 | -37.24 | — | — | $213.94 | $9,436.97 | CAPR×178, SEDG×48, SMTC×10, ERAS×84, BBWI×87, ZYME×55 |
| 2026-09-01 | -6.30 | $213.94 | CAPR×178, SEDG×48, SMTC×10, ERAS×84, BBWI×87, ZYME×55 | $9,719.84 | +282.87 | -109.03 | — | — | $213.94 | $9,610.81 | CAPR×178, SEDG×48, SMTC×10, ERAS×84, BBWI×87, ZYME×55 |
| 2026-09-02 | -3.83 | $213.94 | CAPR×178, SEDG×48, SMTC×10, ERAS×84, BBWI×87, ZYME×55 | $9,659.37 | +48.56 | +0.00 | — | CAPR, SEDG, SMTC, ERAS, BBWI, ZYME | $9,645.88 | $9,645.88 | — |
| 2026-09-03 | -0.90 | $9,645.88 | — | $9,645.88 | -0.00 | +414.54 | MMED | — | $4.48 | $10,054.96 | MMED×423 |
| 2026-09-04 | — | $4.48 | MMED×423 | $10,105.72 | +50.76 | -16.94 | BAK | — | $0.54 | $10,088.74 | MMED×423, BAK×2 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 464 | $4.31 | $5.99 | — | $7,994.17 | — | combo gate; gate news=good,vol=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 102 | $19.57 | $2.30 | — | $5,995.74 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SNDK` | 1 | $1646.93 | $1.99 | — | $4,346.82 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.4; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 147 | $13.55 | $2.43 | — | $2,352.53 | — | combo gate; gate news=good,vol=good; list yday_gainer; 🔵; ⚪; ret5=+17.5; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 151 | $13.18 | $2.44 | — | $359.91 | — | combo gate; gate news=good,vol=good; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $359.91 | ▲ close $10,053.48 vs 09:30 $10,000.00 (session +68.63) | 16:00 close · cash $359.91 · equity $10,053.48 vs 09:30 $10,000.00 (+53.48; session marks +68.63) · 5 name(s) marked open→close (per-name table). ANGX×464 09:30 $4.31 → close $4.37 +27.84; ARX×102 09:30 $19.57 → close $19.58 +1.02; SNDK×1 09:30 $1646.93 → close $1641.11 -5.82; MH×147 09:30 $13.55 → close $13.10 -66.15; HLIT×151 09:30 $13.18 → close $13.92 +111.74 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $359.91 | ▲ 09:30 equity $10,215.56 vs yday $10,053.48 (+162.08) | 09:30 open · cash $359.91 (unchanged overnight, no fees) · equity $10,215.56 vs prior close $10,053.48 (+162.08) · 5 name(s) re-marked at the open (per-name table). ANGX×464 yday $4.37 → 09:30 $4.60 +106.72; ARX×102 yday $19.58 → 09:30 $19.57 -1.02; SNDK×1 yday $1641.11 → 09:30 $1700.74 +59.63; MH×147 yday $13.10 → 09:30 $13.16 +8.82; HLIT×151 yday $13.92 → 09:30 $13.84 -12.08 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $359.91 | ▲ close $10,230.40 vs 09:30 $10,215.56 (session +14.85) | 16:00 close · cash $359.91 · equity $10,230.40 vs 09:30 $10,215.56 (+14.84; session marks +14.85) · 5 name(s) marked open→close (per-name table). ANGX×464 09:30 $4.60 → close $4.71 +51.04; ARX×102 09:30 $19.57 → close $19.54 -3.06; SNDK×1 09:30 $1700.74 → close $1786.85 +86.11; MH×147 09:30 $13.16 → close $12.77 -57.33; HLIT×151 09:30 $13.84 → close $13.43 -61.91 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $359.91 | ▼ 09:30 equity $10,119.58 vs yday $10,230.40 (-110.82) | 09:30 open · cash $359.91 (unchanged overnight, no fees) · equity $10,119.58 vs prior close $10,230.40 (-110.82) · 5 name(s) re-marked at the open (per-name table). ANGX×464 yday $4.71 → 09:30 $4.79 +37.12; ARX×102 yday $19.54 → 09:30 $19.57 +3.06; SNDK×1 yday $1786.85 → 09:30 $1677.54 -109.31; MH×147 yday $12.77 → 09:30 $13.00 +33.81; HLIT×151 yday $13.43 → 09:30 $12.93 -75.50 | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $359.91 | ▼ close $10,082.08 vs 09:30 $10,119.58 (session -37.50) | 16:00 close · cash $359.91 · equity $10,082.08 vs 09:30 $10,119.58 (-37.50; session marks -37.50) · 5 name(s) marked open→close (per-name table). ANGX×464 09:30 $4.79 → close $4.85 +27.84; ARX×102 09:30 $19.57 → close $19.56 -1.02; SNDK×1 09:30 $1677.54 → close $1625.78 -51.76; MH×147 09:30 $13.00 → close $13.12 +17.64; HLIT×151 09:30 $12.93 → close $12.73 -30.20 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $359.91 | ▲ 09:30 equity $10,122.41 vs yday $10,082.08 (+40.33) | 09:30 open · cash $359.91 (unchanged overnight, no fees) · equity $10,122.41 vs prior close $10,082.08 (+40.33) · 5 name(s) re-marked at the open (per-name table). ANGX×464 yday $4.85 → 09:30 $4.79 -27.84; ARX×102 yday $19.56 → 09:30 $19.58 +2.04; SNDK×1 yday $1625.78 → 09:30 $1682.40 +56.62; MH×147 yday $13.12 → 09:30 $13.01 -16.17; HLIT×151 yday $12.73 → 09:30 $12.90 +25.67 | — |
| 2026-08-19 09:30 ET | **SELL** | `ARX` | 102 | $19.58 | $2.33 | $-3.60 | $2,354.74 | ▼ -3.60 after sell → book $10,120.08; vs 09:30 mark -2.33 | dropped from list after 3 sess (min 3) | join🟡 sector🟡 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-19 09:30 ET | **SELL** | `SNDK` | 1 | $1682.40 | $2.02 | $+31.47 | $4,035.13 | ▲ +31.47 after sell → book $10,118.06; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `MH` | 147 | $13.01 | $2.47 | $-84.28 | $5,945.13 | ▼ -84.28 after sell → book $10,115.59; vs 09:30 mark -2.47 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `HLIT` | 151 | $12.90 | $2.48 | $-47.21 | $7,890.55 | ▼ -47.21 after sell → book $10,113.11; vs 09:30 mark -2.48 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,890.55 | ▼ close $10,024.95 vs 09:30 $10,122.41 (session -88.16) | 16:00 close · cash $7,890.55 · equity $10,024.95 vs 09:30 $10,122.41 (-97.46; session marks -88.16) · 1 name(s) marked open→close (per-name table). ANGX×464 09:30 $4.79 → close $4.60 -88.16 | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,890.55 | ▼ 09:30 equity $10,011.03 vs yday $10,024.95 (-13.92) | 09:30 open · cash $7,890.55 (unchanged overnight, no fees) · equity $10,011.03 vs prior close $10,024.95 (-13.92) · 1 name(s) re-marked at the open (per-name table). ANGX×464 yday $4.60 → 09:30 $4.57 -13.92 | — |
| 2026-08-20 09:30 ET | **SELL** | `ANGX` | 464 | $4.57 | $6.08 | $+108.57 | $10,004.95 | ▲ +108.57 after sell → book $10,004.95; vs 09:30 mark -6.08 | dropped from list after 4 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $8,819.79 | — | combo gate; gate news=good,vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1250.62 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 8 | $150.14 | $2.01 | — | $7,616.65 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1250.62 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HUMA` | 1768 | $0.71 | $17.80 | — | $6,348.87 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ret5=+10.5; leftover $1250.62 | join🟡 sector🟡 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BTGO` | 189 | $6.61 | $2.56 | — | $5,097.97 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ret5=+25.3; leftover $1250.62 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ZLAB` | 47 | $26.57 | $2.13 | — | $3,847.05 | — | combo gate; gate news=good,vol=good; list ohlc_hot; 🔵; ret5=+4.8; leftover $1250.62 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 21 | $58.73 | $2.05 | — | $2,611.67 | — | combo gate; gate news=good,vol=good; list ohlc_hot; 🔵; ⚪; ret5=+10.8; leftover $1250.62 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 27 | $44.76 | $2.07 | — | $1,401.08 | — | combo gate; gate news=good,vol=good; list mover_buy; 🔵; ret5=+8.7; leftover $1250.62 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 506 | $2.47 | $6.53 | — | $144.73 | — | combo gate; gate news=good,vol=good; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $1250.62 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $144.73 | ▼ close $9,766.64 vs 09:30 $10,011.03 (session -201.13) | 16:00 close · cash $144.73 · equity $9,766.64 vs 09:30 $10,011.03 (-244.39; session marks -201.13) · 8 name(s) marked open→close (per-name table). BHP×13 09:30 $91.01 → close $93.63 +34.06; MRNA×8 09:30 $150.14 → close $133.32 -134.56; HUMA×1768 09:30 $0.71 → close $0.68 -45.97; BTGO×189 09:30 $6.61 → close $6.60 -0.95; ZLAB×47 09:30 $26.57 → close $26.02 -25.85; CRSP×21 09:30 $58.73 → close $58.12 -12.81; APA×27 09:30 $44.76 → close $44.39 -9.99; AUTL×506 09:30 $2.47 → close $2.46 -5.06 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $144.73 | ▲ 09:30 equity $9,898.88 vs yday $9,766.64 (+132.24) | 09:30 open · cash $144.73 (unchanged overnight, no fees) · equity $9,898.88 vs prior close $9,766.64 (+132.24) · 8 name(s) re-marked at the open (per-name table). BHP×13 yday $93.63 → 09:30 $95.72 +27.17; MRNA×8 yday $133.32 → 09:30 $133.11 -1.68; HUMA×1768 yday $0.68 → 09:30 $0.67 -12.38; BTGO×189 yday $6.60 → 09:30 $6.95 +66.15; ZLAB×47 yday $26.02 → 09:30 $26.25 +10.81; CRSP×21 yday $58.12 → 09:30 $59.72 +33.60; APA×27 yday $44.39 → 09:30 $44.52 +3.51; AUTL×506 yday $2.46 → 09:30 $2.47 +5.06 | — |
| 2026-08-21 09:30 ET | **BUY** | `MARA` | 2 | $11.70 | $0.24 | — | $121.09 | — | combo gate; gate news=good,vol=good; list yday_gainer,yday_mover; 🔵; ret5=+21.1; leftover $24.12 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BTDR` | 2 | $11.10 | $0.23 | — | $98.67 | — | combo gate; gate news=good,vol=good; list yday_gainer; ret5=+19.1; leftover $24.12 | join🔴 sector🟢 gen🟢 news🟢 digest🟢 judge🔴 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `HIVE` | 7 | $3.24 | $0.25 | — | $75.74 | — | combo gate; gate news=good,vol=good; list yday_gainer; 🔵; ret5=+21.3; leftover $24.12 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $75.74 | ▼ close $9,855.42 vs 09:30 $9,898.88 (session -42.75) | 16:00 close · cash $75.74 · equity $9,855.42 vs 09:30 $9,898.88 (-43.46; session marks -42.75) · 11 name(s) marked open→close (per-name table). BHP×13 09:30 $95.72 → close $97.03 +17.03; MRNA×8 09:30 $133.11 → close $145.13 +96.16; HUMA×1768 09:30 $0.67 → close $0.64 -56.58; BTGO×189 09:30 $6.95 → close $6.84 -20.79; ZLAB×47 09:30 $26.25 → close $26.01 -11.28; CRSP×21 09:30 $59.72 → close $59.50 -4.62; APA×27 09:30 $44.52 → close $43.39 -30.51; AUTL×506 09:30 $2.47 → close $2.41 -30.36; MARA×2 09:30 $11.70 → close $11.26 -0.88; BTDR×2 09:30 $11.10 → close $11.37 +0.55; HIVE×7 09:30 $3.24 → close $3.03 -1.47 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $75.74 | ▼ 09:30 equity $9,840.22 vs yday $9,855.42 (-15.20) | 09:30 open · cash $75.74 (unchanged overnight, no fees) · equity $9,840.22 vs prior close $9,855.42 (-15.20) · 11 name(s) re-marked at the open (per-name table). BHP×13 yday $97.03 → 09:30 $97.34 +4.03; MRNA×8 yday $145.13 → 09:30 $142.70 -19.44; HUMA×1768 yday $0.64 → 09:30 $0.68 +67.18; BTGO×189 yday $6.84 → 09:30 $6.87 +5.67; ZLAB×47 yday $26.01 → 09:30 $25.59 -19.74; CRSP×21 yday $59.50 → 09:30 $58.79 -14.91; APA×27 yday $43.39 → 09:30 $42.93 -12.42; AUTL×506 yday $2.41 → 09:30 $2.36 -25.30; MARA×2 yday $11.26 → 09:30 $11.18 -0.16; BTDR×2 yday $11.37 → 09:30 $11.49 +0.24; HIVE×7 yday $3.03 → 09:30 $2.98 -0.35 | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $75.74 | ▼ close $9,749.49 vs 09:30 $9,840.22 (session -90.73) | 16:00 close · cash $75.74 · equity $9,749.49 vs 09:30 $9,840.22 (-90.73; session marks -90.73) · 11 name(s) marked open→close (per-name table). BHP×13 09:30 $97.34 → close $96.66 -8.84; MRNA×8 09:30 $142.70 → close $139.27 -27.44; HUMA×1768 09:30 $0.68 → close $0.67 -17.68; BTGO×189 09:30 $6.87 → close $6.97 +18.90; ZLAB×47 09:30 $25.59 → close $25.51 -3.76; CRSP×21 09:30 $58.79 → close $56.91 -39.48; APA×27 09:30 $42.93 → close $42.10 -22.41; AUTL×506 09:30 $2.36 → close $2.38 +10.12; MARA×2 09:30 $11.18 → close $11.44 +0.52; BTDR×2 09:30 $11.49 → close $11.30 -0.38; HIVE×7 09:30 $2.98 → close $2.94 -0.28 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $75.74 | ▼ 09:30 equity $9,746.59 vs yday $9,749.49 (-2.90) | 09:30 open · cash $75.74 (unchanged overnight, no fees) · equity $9,746.59 vs prior close $9,749.49 (-2.90) · 11 name(s) re-marked at the open (per-name table). BHP×13 yday $96.66 → 09:30 $95.95 -9.23; MRNA×8 yday $139.27 → 09:30 $141.19 +15.36; HUMA×1768 yday $0.67 → 09:30 $0.67 +0.00; BTGO×189 yday $6.97 → 09:30 $6.89 -15.12; ZLAB×47 yday $25.51 → 09:30 $25.93 +19.74; CRSP×21 yday $56.91 → 09:30 $57.00 +1.89; APA×27 yday $42.10 → 09:30 $42.70 +16.20; AUTL×506 yday $2.38 → 09:30 $2.32 -30.36; MARA×2 yday $11.44 → 09:30 $11.28 -0.32; BTDR×2 yday $11.30 → 09:30 $11.19 -0.22; HIVE×7 yday $2.94 → 09:30 $2.82 -0.84 | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 13 | $95.95 | $2.05 | $+60.14 | $1,321.05 | ▲ +60.14 after sell → book $9,744.55; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `MRNA` | 8 | $141.19 | $2.03 | $-75.65 | $2,448.53 | ▼ -75.65 after sell → book $9,742.51; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HUMA` | 1768 | $0.67 | $17.45 | $-100.67 | $3,615.64 | ▼ -100.67 after sell → book $9,725.06; vs 09:30 mark -17.45 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BTGO` | 189 | $6.89 | $2.60 | $+48.71 | $4,915.25 | ▲ +48.71 after sell → book $9,722.46; vs 09:30 mark -2.60 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ZLAB` | 47 | $25.93 | $2.15 | $-34.36 | $6,131.81 | ▼ -34.36 after sell → book $9,720.31; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 21 | $57.00 | $2.07 | $-40.46 | $7,326.74 | ▼ -40.46 after sell → book $9,718.24; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `APA` | 27 | $42.70 | $2.09 | $-59.78 | $8,477.54 | ▼ -59.78 after sell → book $9,716.14; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AUTL` | 506 | $2.32 | $6.62 | $-89.05 | $9,644.84 | ▼ -89.05 after sell → book $9,709.52; vs 09:30 mark -6.62 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 147 | $9.36 | $2.43 | — | $8,266.49 | — | combo gate; gate news=good,vol=good; list yday_gainer; 🔵; ret5=+21.3; leftover $1377.83 | join🔴 sector🟡 gen🟡 news🟢 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 39 | $34.48 | $2.11 | — | $6,919.66 | — | combo gate; gate news=good,vol=good; list yday_gainer; 🔵; ⚪; ret5=+9.3; leftover $1377.83 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 57 | $24.00 | $2.16 | — | $5,549.50 | — | combo gate; gate news=good,vol=good; list yday_mover; ret5=+10.0; leftover $1377.83 | join🔴 sector🟡 gen🟡 news🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BKKT` | 166 | $8.28 | $2.49 | — | $4,172.54 | — | combo gate; gate news=good,vol=good; list ohlc_hot; 🔵; ret5=+12.3; leftover $1377.83 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 17 | $77.90 | $2.04 | — | $2,846.19 | — | combo gate; gate news=good,vol=good; list ohlc_hot,mover_buy; ⚪; ret5=+15.3; leftover $1377.83 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NVAX` | 155 | $8.88 | $2.46 | — | $1,467.34 | — | combo gate; gate news=good,vol=good; list ohlc_hot; 🔵; ret5=+11.1; leftover $1377.83 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 11 | $119.46 | $2.02 | — | $151.26 | — | combo gate; gate news=good,vol=good; list mover_buy; 🔵; ⚪; ret5=+25.9; leftover $1377.83 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $151.26 | ▲ close $9,708.60 vs 09:30 $9,746.59 (session +14.78) | 16:00 close · cash $151.26 · equity $9,708.60 vs 09:30 $9,746.59 (-37.99; session marks +14.78) · 10 name(s) marked open→close (per-name table). MARA×2 09:30 $11.28 → close $11.29 +0.02; BTDR×2 09:30 $11.19 → close $11.28 +0.18; HIVE×7 09:30 $2.82 → close $2.89 +0.49; RUM×147 09:30 $9.36 → close $9.35 -1.47; EZPW×39 09:30 $34.48 → close $34.69 +8.19; REAX×57 09:30 $24.00 → close $24.00 +0.00; BKKT×166 09:30 $8.28 → close $8.38 +16.60; FCX×17 09:30 $77.90 → close $77.49 -6.97; NVAX×155 09:30 $8.88 → close $8.93 +7.75; AU×11 09:30 $119.46 → close $118.55 -10.01 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $151.26 | ▲ 09:30 equity $9,708.60 vs yday $9,708.60 (-0.00) | 09:30 open · cash $151.26 (unchanged overnight, no fees) · equity $9,708.60 vs prior close $9,708.60 (-0.00) · 10 name(s) re-marked at the open (per-name table). MARA×2 yday $11.29 → 09:30 $11.29 +0.00; BTDR×2 yday $11.28 → 09:30 $11.28 +0.00; HIVE×7 yday $2.89 → 09:30 $2.89 +0.00; RUM×147 yday $9.35 → 09:30 $9.35 +0.00; EZPW×39 yday $34.69 → 09:30 $34.69 +0.00; REAX×57 yday $24.00 → 09:30 $24.00 +0.00; BKKT×166 yday $8.38 → 09:30 $8.38 +0.00; FCX×17 yday $77.49 → 09:30 $77.49 +0.00; NVAX×155 yday $8.93 → 09:30 $8.93 +0.00; AU×11 yday $118.55 → 09:30 $118.55 +0.00 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $151.26 | ▲ close $9,708.60 vs 09:30 $9,708.60 (session +0.00) | 16:00 close · cash $151.26 · equity $9,708.60 vs 09:30 $9,708.60 (-0.00; session marks +0.00) · 10 name(s) marked open→close (per-name table). MARA×2 09:30 $11.29 → close $11.29 +0.00; BTDR×2 09:30 $11.28 → close $11.28 +0.00; HIVE×7 09:30 $2.89 → close $2.89 +0.00; RUM×147 09:30 $9.35 → close $9.35 +0.00; EZPW×39 09:30 $34.69 → close $34.69 +0.00; REAX×57 09:30 $24.00 → close $24.00 +0.00; BKKT×166 09:30 $8.38 → close $8.38 +0.00; FCX×17 09:30 $77.49 → close $77.49 +0.00; NVAX×155 09:30 $8.93 → close $8.93 +0.00; AU×11 09:30 $118.55 → close $118.55 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $151.26 | ▲ 09:30 equity $10,110.30 vs yday $9,708.60 (+401.70) | 09:30 open · cash $151.26 (unchanged overnight, no fees) · equity $10,110.30 vs prior close $9,708.60 (+401.70) · 10 name(s) re-marked at the open (per-name table). MARA×2 yday $11.29 → 09:30 $11.56 +0.54; BTDR×2 yday $11.28 → 09:30 $11.05 -0.46; HIVE×7 yday $2.89 → 09:30 $2.95 +0.42; RUM×147 yday $9.35 → 09:30 $10.07 +105.84; EZPW×39 yday $34.69 → 09:30 $35.70 +39.39; REAX×57 yday $24.00 → 09:30 $26.61 +148.77; BKKT×166 yday $8.38 → 09:30 $8.38 +0.00; FCX×17 yday $77.49 → 09:30 $79.34 +31.45; NVAX×155 yday $8.93 → 09:30 $9.33 +62.00; AU×11 yday $118.55 → 09:30 $119.80 +13.75 | — |
| 2026-08-27 09:30 ET | **SELL** | `MARA` | 2 | $11.56 | $0.26 | $-0.78 | $174.12 | ▼ -0.78 after sell → book $10,110.04; vs 09:30 mark -0.26 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `BTDR` | 2 | $11.05 | $0.25 | $-0.56 | $195.97 | ▼ -0.56 after sell → book $10,109.79; vs 09:30 mark -0.25 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `HIVE` | 7 | $2.95 | $0.25 | $-2.53 | $216.38 | ▼ -2.53 after sell → book $10,109.55; vs 09:30 mark -0.24 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $216.38 | ▼ close $9,868.91 vs 09:30 $10,110.30 (session -240.64) | 16:00 close · cash $216.38 · equity $9,868.91 vs 09:30 $10,110.30 (-241.39; session marks -240.64) · 7 name(s) marked open→close (per-name table). RUM×147 09:30 $10.07 → close $9.38 -101.43; EZPW×39 09:30 $35.70 → close $33.90 -70.20; REAX×57 09:30 $26.61 → close $26.59 -1.14; BKKT×166 09:30 $8.38 → close $8.23 -24.90; FCX×17 09:30 $79.34 → close $79.00 -5.78; NVAX×155 09:30 $9.33 → close $9.21 -18.60; AU×11 09:30 $119.80 → close $118.11 -18.59 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $216.38 | ▼ 09:30 equity $9,853.94 vs yday $9,868.91 (-14.97) | 09:30 open · cash $216.38 (unchanged overnight, no fees) · equity $9,853.94 vs prior close $9,868.91 (-14.97) · 7 name(s) re-marked at the open (per-name table). RUM×147 yday $9.38 → 09:30 $9.51 +19.11; EZPW×39 yday $33.90 → 09:30 $33.50 -15.60; REAX×57 yday $26.59 → 09:30 $25.91 -38.76; BKKT×166 yday $8.23 → 09:30 $8.50 +44.82; FCX×17 yday $79.00 → 09:30 $78.83 -2.89; NVAX×155 yday $9.21 → 09:30 $9.12 -13.95; AU×11 yday $118.11 → 09:30 $117.41 -7.70 | — |
| 2026-08-28 09:30 ET | **SELL** | `RUM` | 147 | $9.51 | $2.47 | $+17.15 | $1,611.88 | ▲ +17.15 after sell → book $9,851.47; vs 09:30 mark -2.47 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `EZPW` | 39 | $33.50 | $2.13 | $-42.45 | $2,916.25 | ▼ -42.45 after sell → book $9,849.34; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `REAX` | 57 | $25.91 | $2.18 | $+104.53 | $4,390.94 | ▲ +104.53 after sell → book $9,847.16; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BKKT` | 166 | $8.50 | $2.53 | $+31.51 | $5,799.41 | ▲ +31.51 after sell → book $9,844.63; vs 09:30 mark -2.53 | dropped from list after 3 sess (min 3) | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `FCX` | 17 | $78.83 | $2.06 | $+11.71 | $7,137.46 | ▲ +11.71 after sell → book $9,842.57; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 catal🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `NVAX` | 155 | $9.12 | $2.49 | $+32.25 | $8,548.57 | ▲ +32.25 after sell → book $9,840.08; vs 09:30 mark -2.49 | dropped from list after 3 sess (min 3) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `AU` | 11 | $117.41 | $2.04 | $-26.62 | $9,838.03 | ▼ -26.62 after sell → book $9,838.03; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 178 | $9.19 | $2.52 | — | $8,199.69 | — | combo gate; gate news=good,vol=good; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $1639.67 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 48 | $33.78 | $2.13 | — | $6,576.12 | — | combo gate; gate news=good,vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.9; leftover $1639.67 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 10 | $149.40 | $2.02 | — | $5,080.10 | — | combo gate; gate news=good,vol=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=-11.6; leftover $1639.67 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ERAS` | 84 | $19.30 | $2.24 | — | $3,456.65 | — | combo gate; gate news=good,vol=good; list yday_gainer; ret5=-4.1; leftover $1639.67 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BBWI` | 87 | $18.68 | $2.25 | — | $1,829.24 | — | combo gate; gate news=good,vol=good; list yday_gainer; ret5=+0.2; leftover $1639.67 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ZYME` | 55 | $29.33 | $2.15 | — | $213.94 | — | combo gate; gate news=good,vol=good; list yday_gainer,ohlc_hot; ret5=+14.1; leftover $1639.67 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $213.94 | ▲ close $9,892.66 vs 09:30 $9,853.94 (session +67.95) | 16:00 close · cash $213.94 · equity $9,892.66 vs 09:30 $9,853.94 (+38.72; session marks +67.95) · 6 name(s) marked open→close (per-name table). CAPR×178 09:30 $9.19 → close $10.06 +154.86; SEDG×48 09:30 $33.78 → close $33.51 -12.96; SMTC×10 09:30 $149.40 → close $142.43 -69.70; ERAS×84 09:30 $19.30 → close $19.49 +15.96; BBWI×87 09:30 $18.68 → close $18.65 -2.61; ZYME×55 09:30 $29.33 → close $29.01 -17.60 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $213.94 | ▼ 09:30 equity $9,474.21 vs yday $9,892.66 (-418.45) | 09:30 open · cash $213.94 (unchanged overnight, no fees) · equity $9,474.21 vs prior close $9,892.66 (-418.45) · 6 name(s) re-marked at the open (per-name table). CAPR×178 yday $10.06 → 09:30 $9.44 -110.36; SEDG×48 yday $33.51 → 09:30 $31.50 -96.48; SMTC×10 yday $142.43 → 09:30 $133.04 -93.90; ERAS×84 yday $19.49 → 09:30 $17.90 -133.56; BBWI×87 yday $18.65 → 09:30 $19.30 +56.55; ZYME×55 yday $29.01 → 09:30 $28.27 -40.70 | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $213.94 | ▼ close $9,436.97 vs 09:30 $9,474.21 (session -37.24) | 16:00 close · cash $213.94 · equity $9,436.97 vs 09:30 $9,474.21 (-37.24; session marks -37.24) · 6 name(s) marked open→close (per-name table). CAPR×178 09:30 $9.44 → close $9.36 -14.24; SEDG×48 09:30 $31.50 → close $31.27 -11.04; SMTC×10 09:30 $133.04 → close $132.54 -5.00; ERAS×84 09:30 $17.90 → close $17.90 +0.00; BBWI×87 09:30 $19.30 → close $19.22 -6.96; ZYME×55 09:30 $28.27 → close $28.27 +0.00 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $213.94 | ▲ 09:30 equity $9,719.84 vs yday $9,436.97 (+282.87) | 09:30 open · cash $213.94 (unchanged overnight, no fees) · equity $9,719.84 vs prior close $9,436.97 (+282.87) · 6 name(s) re-marked at the open (per-name table). CAPR×178 yday $9.36 → 09:30 $10.43 +190.46; SEDG×48 yday $31.27 → 09:30 $32.22 +45.60; SMTC×10 yday $132.54 → 09:30 $131.65 -8.90; ERAS×84 yday $17.90 → 09:30 $18.00 +8.40; BBWI×87 yday $19.22 → 09:30 $19.10 -10.44; ZYME×55 yday $28.27 → 09:30 $29.32 +57.75 | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $213.94 | ▼ close $9,610.81 vs 09:30 $9,719.84 (session -109.03) | 16:00 close · cash $213.94 · equity $9,610.81 vs 09:30 $9,719.84 (-109.03; session marks -109.03) · 6 name(s) marked open→close (per-name table). CAPR×178 09:30 $10.43 → close $10.19 -42.72; SEDG×48 09:30 $32.22 → close $31.80 -20.16; SMTC×10 09:30 $131.65 → close $129.50 -21.50; ERAS×84 09:30 $18.00 → close $17.70 -25.20; BBWI×87 09:30 $19.10 → close $19.10 +0.00; ZYME×55 09:30 $29.32 → close $29.33 +0.55 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $213.94 | ▲ 09:30 equity $9,659.37 vs yday $9,610.81 (+48.56) | 09:30 open · cash $213.94 (unchanged overnight, no fees) · equity $9,659.37 vs prior close $9,610.81 (+48.56) · 6 name(s) re-marked at the open (per-name table). CAPR×178 yday $10.19 → 09:30 $10.77 +103.24; SEDG×48 yday $31.80 → 09:30 $31.87 +3.36; SMTC×10 yday $129.50 → 09:30 $127.63 -18.70; ERAS×84 yday $17.70 → 09:30 $17.58 -10.08; BBWI×87 yday $19.10 → 09:30 $18.77 -28.71; ZYME×55 yday $29.33 → 09:30 $29.32 -0.55 | — |
| 2026-09-02 09:30 ET | **SELL** | `CAPR` | 178 | $10.77 | $2.57 | $+276.15 | $2,128.43 | ▲ +276.15 after sell → book $9,656.80; vs 09:30 mark -2.57 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SEDG` | 48 | $31.87 | $2.16 | $-95.97 | $3,656.03 | ▼ -95.97 after sell → book $9,654.64; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SMTC` | 10 | $127.63 | $2.04 | $-221.76 | $4,930.29 | ▼ -221.76 after sell → book $9,652.60; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ERAS` | 84 | $17.58 | $2.27 | $-148.99 | $6,404.75 | ▼ -148.99 after sell → book $9,650.34; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BBWI` | 87 | $18.77 | $2.28 | $+3.30 | $8,035.46 | ▲ +3.30 after sell → book $9,648.06; vs 09:30 mark -2.28 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ZYME` | 55 | $29.32 | $2.18 | $-4.88 | $9,645.88 | ▼ -4.88 after sell → book $9,645.88; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,645.88 | ▲ close $9,645.88 vs 09:30 $9,659.37 (session +0.00) | 16:00 close · cash $9,645.88 · no lots left · equity $9,645.88. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,645.88 | ▲ 09:30 equity $9,645.88 vs yday $9,645.88 (-0.00) | 09:30 open · cash $9,645.88 · no holdings · equity $9,645.88 vs prior close $9,645.88 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 423 | $22.78 | $5.46 | — | $4.48 | — | combo gate; gate news=good,vol=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $9645.88 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.48 | ▲ close $10,054.96 vs 09:30 $9,645.88 (session +414.54) | 16:00 close · cash $4.48 · equity $10,054.96 vs 09:30 $9,645.88 (+409.08; session marks +414.54) · 1 name(s) marked open→close (per-name table). MMED×423 09:30 $22.78 → close $23.76 +414.54 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.48 | ▲ 09:30 equity $10,105.72 vs yday $10,054.96 (+50.76) | 09:30 open · cash $4.48 (unchanged overnight, no fees) · equity $10,105.72 vs prior close $10,054.96 (+50.76) · 1 name(s) re-marked at the open (per-name table). MMED×423 yday $23.76 → 09:30 $23.88 +50.76 | — |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 2 | $1.95 | $0.04 | — | $0.54 | — | combo gate; gate news=good,vol=good; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $4.48 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.54 | ▼ close $10,088.74 vs 09:30 $10,105.72 (session -16.94) | 16:00 close · cash $0.54 · equity $10,088.74 vs 09:30 $10,105.72 (-16.98; session marks -16.94) · 2 name(s) marked open→close (per-name table). MMED×423 09:30 $23.88 → close $23.84 -16.92; BAK×2 09:30 $1.95 → close $1.94 -0.02 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ARX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `SNDK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `MH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `HLIT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `ARX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `SNDK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `MH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `HLIT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `HTHT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ZLAB` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AUTL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `REAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KLAR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `MRNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HUMA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BTGO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ZLAB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `APA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 24.12 < 1 share @ 119.43 |
| 2026-08-21 | `FUTU` | cash | leftover split 24.12 < 1 share @ 115.18 |
| 2026-08-21 | `DE` | cash | leftover split 24.12 < 1 share @ 623.26 |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `MRNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HUMA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BTGO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ZLAB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `APA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `MARA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `BTDR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `HIVE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `NVAX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ABUS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `MARA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `BTDR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `HIVE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `MARA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `BTDR` | no_price | no 09:30 open — carry |
| 2026-08-26 | `HIVE` | no_price | no 09:30 open — carry |
| 2026-08-26 | `RUM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `EZPW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `REAX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `BKKT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `FCX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `NVAX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `AU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `RUM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `EZPW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `REAX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BKKT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `FCX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `NVAX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `AU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `CAPR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SEDG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ERAS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BBWI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ZYME` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RBRK` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `CAPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SEDG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ERAS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BBWI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ZYME` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-04 | `MMED` | min_hold | dropped but min-hold 1/3 sess — no sell |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `MMED` | 423 | 2026-09-03 @ $22.78 | combo gate; gate news=good,vol=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $9645.88 |
| `BAK` | 2 | 2026-09-04 @ $1.95 | combo gate; gate news=good,vol=good; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $4.48 |
