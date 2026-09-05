# Factor mine action — `probable_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `probable` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · baseline list, no extra gate

Cash book **-3.17%** ($9,684) · signal-only (no cash/fees) was +1.13%. Starts YES **4/17**. Fills 116 · skips 62 · realized $-351.51.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `probable` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $229.61.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `ANGX` | 290 | — | $4.31 | +0.00 | $4.37 | +17.40 | +17.40 | +0.00 | +17.40 |
| 2026-08-14 | `WWW` | 60 | — | $20.60 | +0.00 | $21.03 | +25.80 | +25.80 | +0.00 | +25.80 |
| 2026-08-14 | `HYLN` | 299 | — | $4.18 | +0.00 | $4.06 | -35.88 | -35.88 | +0.00 | -35.88 |
| 2026-08-14 | `WDC` | 2 | — | $503.50 | +0.00 | $508.80 | +10.60 | +10.60 | +0.00 | +10.60 |
| 2026-08-14 | `FOSL` | 221 | — | $5.64 | +0.00 | $5.57 | -15.47 | -15.47 | +0.00 | -15.47 |
| 2026-08-14 | `ADUR` | 75 | — | $16.50 | +0.00 | $16.17 | -24.75 | -24.75 | +0.00 | -24.75 |
| 2026-08-14 | `AIRS` | 370 | — | $3.37 | +0.00 | $3.43 | +22.20 | +22.20 | +0.00 | +22.20 |
| 2026-08-14 | `ALGM` | 28 | — | $44.06 | +0.00 | $44.39 | +9.24 | +9.24 | +0.00 | +9.24 |
| 2026-08-17 | `ANGX` | 290 | $4.37 | $4.60 | +66.70 | — | +0.00 | +66.70 | +84.10 | — |
| 2026-08-17 | `WWW` | 60 | $21.03 | $20.98 | -3.00 | — | +0.00 | -3.00 | +22.80 | — |
| 2026-08-17 | `HYLN` | 299 | $4.06 | $4.10 | +11.96 | — | +0.00 | +11.96 | -23.92 | — |
| 2026-08-17 | `WDC` | 2 | $508.80 | $525.53 | +33.46 | — | +0.00 | +33.46 | +44.06 | — |
| 2026-08-17 | `FOSL` | 221 | $5.57 | $5.50 | -15.47 | — | +0.00 | -15.47 | -30.94 | — |
| 2026-08-17 | `ADUR` | 75 | $16.17 | $15.73 | -33.00 | — | +0.00 | -33.00 | -57.75 | — |
| 2026-08-17 | `AIRS` | 370 | $3.43 | $3.40 | -12.95 | — | +0.00 | -12.95 | +9.25 | — |
| 2026-08-17 | `ALGM` | 28 | $44.39 | $45.32 | +26.04 | — | +0.00 | +26.04 | +35.28 | — |
| 2026-08-17 | `CDNL` | 31 | — | $39.85 | +0.00 | $39.23 | -19.22 | -19.22 | +0.00 | -19.22 |
| 2026-08-17 | `ABX` | 137 | — | $9.12 | +0.00 | $9.12 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-17 | `FCEL` | 56 | — | $22.37 | +0.00 | $22.36 | -0.56 | -0.56 | +0.00 | -0.56 |
| 2026-08-17 | `VERA` | 40 | — | $31.30 | +0.00 | $31.63 | +13.20 | +13.20 | +0.00 | +13.20 |
| 2026-08-17 | `CELC` | 13 | — | $92.99 | +0.00 | $92.44 | -7.15 | -7.15 | +0.00 | -7.15 |
| 2026-08-17 | `BW` | 121 | — | $10.35 | +0.00 | $9.92 | -52.03 | -52.03 | +0.00 | -52.03 |
| 2026-08-17 | `OCC` | 68 | — | $18.24 | +0.00 | $17.12 | -76.16 | -76.16 | +0.00 | -76.16 |
| 2026-08-17 | `ALM` | 77 | — | $16.20 | +0.00 | $16.36 | +12.32 | +12.32 | +0.00 | +12.32 |
| 2026-08-18 | `CDNL` | 31 | $39.23 | $41.57 | +72.54 | — | +0.00 | +72.54 | +53.32 | — |
| 2026-08-18 | `ABX` | 137 | $9.12 | $9.03 | -12.33 | — | +0.00 | -12.33 | -12.33 | — |
| 2026-08-18 | `FCEL` | 56 | $22.36 | $21.18 | -66.08 | — | +0.00 | -66.08 | -66.64 | — |
| 2026-08-18 | `VERA` | 40 | $31.63 | $31.31 | -12.80 | — | +0.00 | -12.80 | +0.40 | — |
| 2026-08-18 | `CELC` | 13 | $92.44 | $92.38 | -0.78 | — | +0.00 | -0.78 | -7.93 | — |
| 2026-08-18 | `BW` | 121 | $9.92 | $9.60 | -38.72 | — | +0.00 | -38.72 | -90.75 | — |
| 2026-08-18 | `OCC` | 68 | $17.12 | $16.20 | -62.56 | — | +0.00 | -62.56 | -138.72 | — |
| 2026-08-18 | `ALM` | 77 | $16.36 | $15.78 | -44.66 | — | +0.00 | -44.66 | -32.34 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `MRVI` | 164 | — | $7.38 | +0.00 | $8.26 | +144.32 | +144.32 | +0.00 | +144.32 |
| 2026-08-20 | `DNA` | 162 | — | $7.45 | +0.00 | $6.96 | -79.38 | -79.38 | +0.00 | -79.38 |
| 2026-08-20 | `MSTR` | 10 | — | $113.23 | +0.00 | $112.39 | -8.40 | -8.40 | +0.00 | -8.40 |
| 2026-08-20 | `EXK` | 112 | — | $10.77 | +0.00 | $10.97 | +22.40 | +22.40 | +0.00 | +22.40 |
| 2026-08-20 | `SCZM` | 128 | — | $9.46 | +0.00 | $9.76 | +38.40 | +38.40 | +0.00 | +38.40 |
| 2026-08-20 | `NG` | 144 | — | $8.38 | +0.00 | $8.66 | +40.32 | +40.32 | +0.00 | +40.32 |
| 2026-08-20 | `BLSH` | 41 | — | $29.20 | +0.00 | $28.44 | -31.16 | -31.16 | +0.00 | -31.16 |
| 2026-08-20 | `HYMC` | 44 | — | $27.25 | +0.00 | $26.14 | -48.84 | -48.84 | +0.00 | -48.84 |
| 2026-08-21 | `MRVI` | 164 | $8.26 | $8.20 | -9.84 | $8.70 | +82.00 | +72.16 | +134.48 | +216.48 |
| 2026-08-21 | `DNA` | 162 | $6.96 | $7.09 | +21.06 | — | +0.00 | +21.06 | -58.32 | — |
| 2026-08-21 | `MSTR` | 10 | $112.39 | $119.69 | +73.00 | — | +0.00 | +73.00 | +64.60 | — |
| 2026-08-21 | `EXK` | 112 | $10.97 | $11.34 | +41.44 | — | +0.00 | +41.44 | +63.84 | — |
| 2026-08-21 | `SCZM` | 128 | $9.76 | $10.26 | +64.00 | — | +0.00 | +64.00 | +102.40 | — |
| 2026-08-21 | `NG` | 144 | $8.66 | $9.02 | +51.84 | — | +0.00 | +51.84 | +92.16 | — |
| 2026-08-21 | `BLSH` | 41 | $28.44 | $29.75 | +53.71 | — | +0.00 | +53.71 | +22.55 | — |
| 2026-08-21 | `HYMC` | 44 | $26.14 | $27.40 | +55.44 | — | +0.00 | +55.44 | +6.60 | — |
| 2026-08-21 | `BTBT` | 753 | — | $1.66 | +0.00 | $1.53 | -97.89 | -97.89 | +0.00 | -97.89 |
| 2026-08-21 | `ENHA` | 731 | — | $1.71 | +0.00 | $1.72 | +7.31 | +7.31 | +0.00 | +7.31 |
| 2026-08-21 | `DE` | 2 | — | $623.26 | +0.00 | $647.47 | +48.42 | +48.42 | +0.00 | +48.42 |
| 2026-08-21 | `QDEL` | 83 | — | $14.96 | +0.00 | $14.74 | -18.26 | -18.26 | +0.00 | -18.26 |
| 2026-08-21 | `ORBS` | 1447 | — | $0.86 | +0.00 | $0.88 | +23.15 | +23.15 | +0.00 | +23.15 |
| 2026-08-21 | `GORO` | 402 | — | $3.11 | +0.00 | $3.19 | +32.16 | +32.16 | +0.00 | +32.16 |
| 2026-08-21 | `QTRX` | 390 | — | $3.11 | +0.00 | $2.99 | -46.80 | -46.80 | +0.00 | -46.80 |
| 2026-08-24 | `MRVI` | 164 | $8.70 | $8.59 | -18.04 | — | +0.00 | -18.04 | +198.44 | — |
| 2026-08-24 | `BTBT` | 753 | $1.53 | $1.55 | +15.06 | — | +0.00 | +15.06 | -82.83 | — |
| 2026-08-24 | `ENHA` | 731 | $1.72 | $1.74 | +14.62 | — | +0.00 | +14.62 | +21.93 | — |
| 2026-08-24 | `DE` | 2 | $647.47 | $653.62 | +12.30 | — | +0.00 | +12.30 | +60.72 | — |
| 2026-08-24 | `QDEL` | 83 | $14.74 | $14.71 | -2.49 | — | +0.00 | -2.49 | -20.75 | — |
| 2026-08-24 | `ORBS` | 1447 | $0.88 | $0.89 | +14.47 | — | +0.00 | +14.47 | +37.62 | — |
| 2026-08-24 | `GORO` | 402 | $3.19 | $3.20 | +4.02 | — | +0.00 | +4.02 | +36.18 | — |
| 2026-08-24 | `QTRX` | 390 | $2.99 | $2.98 | -3.90 | — | +0.00 | -3.90 | -50.70 | — |
| 2026-08-25 | `BMEA` | 776 | — | $1.62 | +0.00 | $1.61 | -7.76 | -7.76 | +0.00 | -7.76 |
| 2026-08-25 | `NPWR` | 628 | — | $2.00 | +0.00 | $2.02 | +12.56 | +12.56 | +0.00 | +12.56 |
| 2026-08-25 | `PUSA` | 339 | — | $3.70 | +0.00 | $3.91 | +71.19 | +71.19 | +0.00 | +71.19 |
| 2026-08-25 | `ALVO` | 240 | — | $5.22 | +0.00 | $5.25 | +7.20 | +7.20 | +0.00 | +7.20 |
| 2026-08-25 | `CAPR` | 185 | — | $6.79 | +0.00 | $7.19 | +74.00 | +74.00 | +0.00 | +74.00 |
| 2026-08-25 | `ALIT` | 84 | — | $14.86 | +0.00 | $14.87 | +0.84 | +0.84 | +0.00 | +0.84 |
| 2026-08-25 | `ZURA` | 197 | — | $6.38 | +0.00 | $6.50 | +23.64 | +23.64 | +0.00 | +23.64 |
| 2026-08-25 | `SAFX` | 3306 | — | $0.37 | +0.00 | $0.37 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-26 | `BMEA` | 776 | $1.61 | $1.61 | +0.00 | $1.61 | +0.00 | +0.00 | -7.76 | -7.76 |
| 2026-08-26 | `NPWR` | 628 | $2.02 | $2.02 | +0.00 | $2.02 | +0.00 | +0.00 | +12.56 | +12.56 |
| 2026-08-26 | `PUSA` | 339 | $3.91 | $3.91 | +0.00 | $3.91 | +0.00 | +0.00 | +71.19 | +71.19 |
| 2026-08-26 | `ALVO` | 240 | $5.25 | $5.25 | +0.00 | $5.25 | +0.00 | +0.00 | +7.20 | +7.20 |
| 2026-08-26 | `CAPR` | 185 | $7.19 | $7.19 | +0.00 | $7.19 | +0.00 | +0.00 | +74.00 | +74.00 |
| 2026-08-26 | `ALIT` | 84 | $14.87 | $14.87 | +0.00 | $14.87 | +0.00 | +0.00 | +0.84 | +0.84 |
| 2026-08-26 | `ZURA` | 197 | $6.50 | $6.50 | +0.00 | $6.50 | +0.00 | +0.00 | +23.64 | +23.64 |
| 2026-08-26 | `SAFX` | 3306 | $0.37 | $0.37 | +0.00 | $0.37 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-27 | `BMEA` | 776 | $1.61 | $1.75 | +108.64 | — | +0.00 | +108.64 | +100.88 | — |
| 2026-08-27 | `NPWR` | 628 | $2.02 | $1.93 | -56.52 | — | +0.00 | -56.52 | -43.96 | — |
| 2026-08-27 | `PUSA` | 339 | $3.91 | $3.84 | -23.73 | — | +0.00 | -23.73 | +47.46 | — |
| 2026-08-27 | `ALVO` | 240 | $5.25 | $4.98 | -64.80 | — | +0.00 | -64.80 | -57.60 | — |
| 2026-08-27 | `CAPR` | 185 | $7.19 | $8.29 | +203.50 | — | +0.00 | +203.50 | +277.50 | — |
| 2026-08-27 | `ALIT` | 84 | $14.87 | $14.85 | -1.68 | — | +0.00 | -1.68 | -0.84 | — |
| 2026-08-27 | `ZURA` | 197 | $6.50 | $6.13 | -72.89 | — | +0.00 | -72.89 | -49.25 | — |
| 2026-08-27 | `SAFX` | 3306 | $0.37 | $0.35 | -66.12 | — | +0.00 | -66.12 | -66.12 | — |
| 2026-08-28 | `ANF` | 8 | — | $144.70 | +0.00 | $145.75 | +8.40 | +8.40 | +0.00 | +8.40 |
| 2026-08-28 | `BHVN` | 74 | — | $16.95 | +0.00 | $16.12 | -61.42 | -61.42 | +0.00 | -61.42 |
| 2026-08-28 | `BZ` | 68 | — | $18.50 | +0.00 | $18.00 | -34.00 | -34.00 | +0.00 | -34.00 |
| 2026-08-28 | `CAPR` | 138 | — | $9.19 | +0.00 | $10.06 | +120.06 | +120.06 | +0.00 | +120.06 |
| 2026-08-28 | `LVWR` | 920 | — | $1.38 | +0.00 | $1.36 | -18.40 | -18.40 | +0.00 | -18.40 |
| 2026-08-28 | `SEDG` | 37 | — | $33.78 | +0.00 | $33.51 | -9.99 | -9.99 | +0.00 | -9.99 |
| 2026-08-28 | `SMTC` | 8 | — | $149.40 | +0.00 | $142.43 | -55.76 | -55.76 | +0.00 | -55.76 |
| 2026-08-28 | `GRRR` | 79 | — | $15.94 | +0.00 | $15.66 | -22.12 | -22.12 | +0.00 | -22.12 |
| 2026-08-31 | `ANF` | 8 | $145.75 | $148.67 | +23.36 | — | +0.00 | +23.36 | +31.76 | — |
| 2026-08-31 | `BHVN` | 74 | $16.12 | $15.44 | -50.32 | — | +0.00 | -50.32 | -111.74 | — |
| 2026-08-31 | `BZ` | 68 | $18.00 | $17.89 | -7.48 | — | +0.00 | -7.48 | -41.48 | — |
| 2026-08-31 | `CAPR` | 138 | $10.06 | $9.44 | -85.56 | — | +0.00 | -85.56 | +34.50 | — |
| 2026-08-31 | `LVWR` | 920 | $1.36 | $1.37 | +9.20 | — | +0.00 | +9.20 | -9.20 | — |
| 2026-08-31 | `SEDG` | 37 | $33.51 | $31.50 | -74.37 | — | +0.00 | -74.37 | -84.36 | — |
| 2026-08-31 | `SMTC` | 8 | $142.43 | $133.04 | -75.12 | — | +0.00 | -75.12 | -130.88 | — |
| 2026-08-31 | `GRRR` | 79 | $15.66 | $14.32 | -105.86 | — | +0.00 | -105.86 | -127.98 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `GPRO` | 990 | — | $1.22 | +0.00 | $1.69 | +465.30 | +465.30 | +0.00 | +465.30 |
| 2026-09-03 | `FRVO` | 65 | — | $18.40 | +0.00 | $17.98 | -27.30 | -27.30 | +0.00 | -27.30 |
| 2026-09-03 | `CRK` | 76 | — | $15.70 | +0.00 | $15.54 | -12.16 | -12.16 | +0.00 | -12.16 |
| 2026-09-03 | `MMED` | 53 | — | $22.78 | +0.00 | $23.76 | +51.94 | +51.94 | +0.00 | +51.94 |
| 2026-09-03 | `CTMX` | 324 | — | $3.72 | +0.00 | $3.72 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-09-03 | `SLN` | 82 | — | $14.70 | +0.00 | $14.79 | +7.38 | +7.38 | +0.00 | +7.38 |
| 2026-09-03 | `EIX` | 21 | — | $56.78 | +0.00 | $55.19 | -33.39 | -33.39 | +0.00 | -33.39 |
| 2026-09-03 | `CRDL` | 559 | — | $2.16 | +0.00 | $2.17 | +5.59 | +5.59 | +0.00 | +5.59 |
| 2026-09-04 | `GPRO` | 990 | $1.69 | $1.78 | +89.10 | $1.39 | -386.10 | -297.00 | +554.40 | +168.30 |
| 2026-09-04 | `FRVO` | 65 | $17.98 | $18.27 | +18.85 | — | +0.00 | +18.85 | -8.45 | — |
| 2026-09-04 | `CRK` | 76 | $15.54 | $15.45 | -6.84 | — | +0.00 | -6.84 | -19.00 | — |
| 2026-09-04 | `MMED` | 53 | $23.76 | $23.88 | +6.36 | — | +0.00 | +6.36 | +58.30 | — |
| 2026-09-04 | `CTMX` | 324 | $3.72 | $3.73 | +3.24 | — | +0.00 | +3.24 | +3.24 | — |
| 2026-09-04 | `SLN` | 82 | $14.79 | $14.85 | +4.92 | — | +0.00 | +4.92 | +12.30 | — |
| 2026-09-04 | `EIX` | 21 | $55.19 | $55.42 | +4.83 | — | +0.00 | +4.83 | -28.56 | — |
| 2026-09-04 | `CRDL` | 559 | $2.17 | $2.18 | +5.59 | — | +0.00 | +5.59 | +11.18 | — |
| 2026-09-04 | `BAK` | 617 | — | $1.95 | +0.00 | $1.94 | -6.17 | -6.17 | +0.00 | -6.17 |
| 2026-09-04 | `EOSE` | 337 | — | $3.57 | +0.00 | $3.50 | -23.59 | -23.59 | +0.00 | -23.59 |
| 2026-09-04 | `SLBT` | 392 | — | $3.07 | +0.00 | $3.15 | +31.36 | +31.36 | +0.00 | +31.36 |
| 2026-09-04 | `DELL` | 2 | — | $486.31 | +0.00 | $516.39 | +60.16 | +60.16 | +0.00 | +60.16 |
| 2026-09-04 | `MLYS` | 41 | — | $29.15 | +0.00 | $28.27 | -36.08 | -36.08 | +0.00 | -36.08 |
| 2026-09-04 | `CCOI` | 117 | — | $10.22 | +0.00 | $9.98 | -28.08 | -28.08 | +0.00 | -28.08 |
| 2026-09-04 | `SION` | 164 | — | $7.31 | +0.00 | $6.75 | -91.84 | -91.84 | +0.00 | -91.84 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +9.14 | ANGX, WWW, HYLN, WDC, FOSL, ADUR, AIRS, ALGM | — | $269.08 | $9,985.46 | ANGX×290, WWW×60, HYLN×299, WDC×2, FOSL×221, ADUR×75, AIRS×370, ALGM×28 |
| 2026-08-17 | +2.25 | $269.08 | ANGX×290, WWW×60, HYLN×299, WDC×2, FOSL×221, ADUR×75, AIRS×370, ALGM×28 | $10,059.20 | +73.74 | -129.60 | CDNL, ABX, FCEL, VERA, CELC, BW, OCC, ALM | ANGX, WWW, HYLN, WDC, FOSL, ADUR, AIRS, ALGM | $79.21 | $9,888.06 | CDNL×31, ABX×137, FCEL×56, VERA×40, CELC×13, BW×121, OCC×68, ALM×77 |
| 2026-08-18 | -6.20 | $79.21 | CDNL×31, ABX×137, FCEL×56, VERA×40, CELC×13, BW×121, OCC×68, ALM×77 | $9,722.67 | -165.39 | +0.00 | — | CDNL, ABX, FCEL, VERA, CELC, BW, OCC, ALM | $9,704.93 | $9,704.93 | — |
| 2026-08-19 | -7.20 | $9,704.93 | — | $9,704.93 | +0.00 | +0.00 | — | — | $9,704.93 | $9,704.93 | — |
| 2026-08-20 | +1.12 | $9,704.93 | — | $9,704.93 | +0.00 | +77.66 | MRVI, DNA, MSTR, EXK, SCZM, NG, BLSH, HYMC | — | $117.04 | $9,764.26 | MRVI×164, DNA×162, MSTR×10, EXK×112, SCZM×128, NG×144, BLSH×41, HYMC×44 |
| 2026-08-21 | +3.25 | $117.04 | MRVI×164, DNA×162, MSTR×10, EXK×112, SCZM×128, NG×144, BLSH×41, HYMC×44 | $10,114.91 | +350.65 | +30.09 | BTBT, ENHA, DE, QDEL, ORBS, GORO, QTRX | DNA, MSTR, EXK, SCZM, NG, BLSH, HYMC | $2.11 | $10,078.52 | MRVI×164, BTBT×753, ENHA×731, DE×2, QDEL×83, ORBS×1447, GORO×402, QTRX×390 |
| 2026-08-24 | -5.17 | $2.11 | MRVI×164, BTBT×753, ENHA×731, DE×2, QDEL×83, ORBS×1447, GORO×402, QTRX×390 | $10,114.56 | +36.04 | +0.00 | — | MRVI, BTBT, ENHA, DE, QDEL, ORBS, GORO, QTRX | $10,060.51 | $10,060.51 | — |
| 2026-08-25 | +1.80 | $10,060.51 | — | $10,060.51 | -0.00 | +181.67 | BMEA, NPWR, PUSA, ALVO, CAPR, ALIT, ZURA, SAFX | — | $0.72 | $10,187.08 | BMEA×776, NPWR×628, PUSA×339, ALVO×240, CAPR×185, ALIT×84, ZURA×197, SAFX×3306 |
| 2026-08-26 | +2.02 | $0.72 | BMEA×776, NPWR×628, PUSA×339, ALVO×240, CAPR×185, ALIT×84, ZURA×197, SAFX×3306 | $10,187.08 | -0.00 | +0.00 | — | — | $0.72 | $10,187.08 | BMEA×776, NPWR×628, PUSA×339, ALVO×240, CAPR×185, ALIT×84, ZURA×197, SAFX×3306 |
| 2026-08-27 | — | $0.72 | BMEA×776, NPWR×628, PUSA×339, ALVO×240, CAPR×185, ALIT×84, ZURA×197, SAFX×3306 | $10,213.48 | +26.40 | +0.00 | — | BMEA, NPWR, PUSA, ALVO, CAPR, ALIT, ZURA, SAFX | $10,158.00 | $10,158.00 | — |
| 2026-08-28 | +0.75 | $10,158.00 | — | $10,158.00 | +0.00 | -73.23 | ANF, BHVN, BZ, CAPR, LVWR, SEDG, SMTC, GRRR | — | $218.93 | $10,057.74 | ANF×8, BHVN×74, BZ×68, CAPR×138, LVWR×920, SEDG×37, SMTC×8, GRRR×79 |
| 2026-08-31 | -5.85 | $218.93 | ANF×8, BHVN×74, BZ×68, CAPR×138, LVWR×920, SEDG×37, SMTC×8, GRRR×79 | $9,691.59 | -366.15 | +0.00 | — | ANF, BHVN, BZ, CAPR, LVWR, SEDG, SMTC, GRRR | $9,664.23 | $9,664.23 | — |
| 2026-09-01 | -6.30 | $9,664.23 | — | $9,664.23 | +0.00 | +0.00 | — | — | $9,664.23 | $9,664.23 | — |
| 2026-09-02 | -3.83 | $9,664.23 | — | $9,664.23 | +0.00 | +0.00 | — | — | $9,664.23 | $9,664.23 | — |
| 2026-09-03 | -0.90 | $9,664.23 | — | $9,664.23 | +0.00 | +457.36 | GPRO, FRVO, CRK, MMED, CTMX, SLN, EIX, CRDL | — | $14.39 | $10,086.59 | GPRO×990, FRVO×65, CRK×76, MMED×53, CTMX×324, SLN×82, EIX×21, CRDL×559 |
| 2026-09-04 | — | $14.39 | GPRO×990, FRVO×65, CRK×76, MMED×53, CTMX×324, SLN×82, EIX×21, CRDL×559 | $10,212.64 | +126.05 | -480.34 | BAK, EOSE, SLBT, DELL, MLYS, CCOI, SION | FRVO, CRK, MMED, CTMX, SLN, EIX, CRDL | $229.61 | $9,683.50 | GPRO×990, BAK×617, EOSE×337, SLBT×392, DELL×2, MLYS×41, CCOI×117, SION×164 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 290 | $4.31 | $3.74 | — | $8,746.36 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `WWW` | 60 | $20.60 | $2.17 | — | $7,508.19 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=+4.4; leftover $1250.00 | join🟢 sector🔴 gen🟢 news🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 299 | $4.18 | $3.86 | — | $6,254.51 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `WDC` | 2 | $503.50 | $2.00 | — | $5,245.52 | — | baseline list, no extra gate; list probable; 🔵; ⚪; ret5=+7.9; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `FOSL` | 221 | $5.64 | $2.85 | — | $3,996.23 | — | baseline list, no extra gate; list probable; 🔵; ret5=-4.1; leftover $1250.00 | join🟢 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 75 | $16.50 | $2.21 | — | $2,756.51 | — | baseline list, no extra gate; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRS` | 370 | $3.37 | $4.77 | — | $1,504.84 | — | baseline list, no extra gate; list probable; ret5=-29.1; leftover $1250.00 | join🟢 sector🔴 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ALGM` | 28 | $44.06 | $2.07 | — | $269.08 | — | baseline list, no extra gate; list probable; 🔵; ret5=+3.9; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $269.08 | ▲ close $9,985.46 vs 09:30 $10,000.00 (session +9.14) | 16:00 close · cash $269.08 · equity $9,985.46 vs 09:30 $10,000.00 (-14.54; session marks +9.14) · 8 name(s) marked open→close (per-name table). ANGX×290 09:30 $4.31 → close $4.37 +17.40; WWW×60 09:30 $20.60 → close $21.03 +25.80; HYLN×299 09:30 $4.18 → close $4.06 -35.88; WDC×2 09:30 $503.50 → close $508.80 +10.60; FOSL×221 09:30 $5.64 → close $5.57 -15.47; ADUR×75 09:30 $16.50 → close $16.17 -24.75; AIRS×370 09:30 $3.37 → close $3.43 +22.20; ALGM×28 09:30 $44.06 → close $44.39 +9.24 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $269.08 | ▲ 09:30 equity $10,059.20 vs yday $9,985.46 (+73.74) | 09:30 open · cash $269.08 (unchanged overnight, no fees) · equity $10,059.20 vs prior close $9,985.46 (+73.74) · 8 name(s) re-marked at the open (per-name table). ANGX×290 yday $4.37 → 09:30 $4.60 +66.70; WWW×60 yday $21.03 → 09:30 $20.98 -3.00; HYLN×299 yday $4.06 → 09:30 $4.10 +11.96; WDC×2 yday $508.80 → 09:30 $525.53 +33.46; FOSL×221 yday $5.57 → 09:30 $5.50 -15.47; ADUR×75 yday $16.17 → 09:30 $15.73 -33.00; AIRS×370 yday $3.43 → 09:30 $3.40 -12.95; ALGM×28 yday $44.39 → 09:30 $45.32 +26.04 | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 290 | $4.60 | $3.80 | $+76.56 | $1,599.28 | ▲ +76.56 after sell → book $10,055.40; vs 09:30 mark -3.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `WWW` | 60 | $20.98 | $2.19 | $+18.44 | $2,855.89 | ▲ +18.44 after sell → book $10,053.21; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HYLN` | 299 | $4.10 | $3.92 | $-31.69 | $4,077.88 | ▼ -31.69 after sell → book $10,049.30; vs 09:30 mark -3.91 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `WDC` | 2 | $525.53 | $2.02 | $+40.05 | $5,126.92 | ▲ +40.05 after sell → book $10,047.28; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `FOSL` | 221 | $5.50 | $2.90 | $-36.69 | $6,339.52 | ▼ -36.69 after sell → book $10,044.38; vs 09:30 mark -2.90 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ADUR` | 75 | $15.73 | $2.24 | $-62.20 | $7,517.04 | ▼ -62.20 after sell → book $10,042.15; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRS` | 370 | $3.40 | $4.84 | $-0.37 | $8,768.34 | ▼ -0.37 after sell → book $10,037.30; vs 09:30 mark -4.85 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ALGM` | 28 | $45.32 | $2.09 | $+31.11 | $10,035.21 | ▲ +31.11 after sell → book $10,035.21; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `CDNL` | 31 | $39.85 | $2.08 | — | $8,797.77 | — | baseline list, no extra gate; list probable,yday_gainer; ⚪; ret5=-38.4; leftover $1254.40 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 137 | $9.12 | $2.40 | — | $7,545.93 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $1254.40 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `FCEL` | 56 | $22.37 | $2.16 | — | $6,291.05 | — | baseline list, no extra gate; list probable,yday_gainer; ⚪; ret5=+9.5; leftover $1254.40 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `VERA` | 40 | $31.30 | $2.11 | — | $5,036.94 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-3.8; leftover $1254.40 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CELC` | 13 | $92.99 | $2.03 | — | $3,826.05 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-0.8; leftover $1254.40 | join🟡 sector🔴 gen🟢 news🟢 judge🟢 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `BW` | 121 | $10.35 | $2.35 | — | $2,571.34 | — | baseline list, no extra gate; list probable; ⚪; ret5=+9.8; leftover $1254.40 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `OCC` | 68 | $18.24 | $2.19 | — | $1,328.83 | — | baseline list, no extra gate; list probable,ohlc_hot; ⚪; ret5=+9.5; leftover $1254.40 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ALM` | 77 | $16.20 | $2.22 | — | $79.21 | — | baseline list, no extra gate; list probable,ohlc_hot; 🔵; ⚪; ret5=+6.4; leftover $1254.40 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $79.21 | ▼ close $9,888.06 vs 09:30 $10,059.20 (session -129.60) | 16:00 close · cash $79.21 · equity $9,888.06 vs 09:30 $10,059.20 (-171.14; session marks -129.60) · 8 name(s) marked open→close (per-name table). CDNL×31 09:30 $39.85 → close $39.23 -19.22; ABX×137 09:30 $9.12 → close $9.12 +0.00; FCEL×56 09:30 $22.37 → close $22.36 -0.56; VERA×40 09:30 $31.30 → close $31.63 +13.20; CELC×13 09:30 $92.99 → close $92.44 -7.15; BW×121 09:30 $10.35 → close $9.92 -52.03; OCC×68 09:30 $18.24 → close $17.12 -76.16; ALM×77 09:30 $16.20 → close $16.36 +12.32 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $79.21 | ▼ 09:30 equity $9,722.67 vs yday $9,888.06 (-165.39) | 09:30 open · cash $79.21 (unchanged overnight, no fees) · equity $9,722.67 vs prior close $9,888.06 (-165.39) · 8 name(s) re-marked at the open (per-name table). CDNL×31 yday $39.23 → 09:30 $41.57 +72.54; ABX×137 yday $9.12 → 09:30 $9.03 -12.33; FCEL×56 yday $22.36 → 09:30 $21.18 -66.08; VERA×40 yday $31.63 → 09:30 $31.31 -12.80; CELC×13 yday $92.44 → 09:30 $92.38 -0.78; BW×121 yday $9.92 → 09:30 $9.60 -38.72; OCC×68 yday $17.12 → 09:30 $16.20 -62.56; ALM×77 yday $16.36 → 09:30 $15.78 -44.66 | — |
| 2026-08-18 09:30 ET | **SELL** | `CDNL` | 31 | $41.57 | $2.10 | $+49.13 | $1,365.77 | ▲ +49.13 after sell → book $9,720.56; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 137 | $9.03 | $2.43 | $-17.16 | $2,600.45 | ▼ -17.16 after sell → book $9,718.13; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FCEL` | 56 | $21.18 | $2.18 | $-70.98 | $3,784.35 | ▼ -70.98 after sell → book $9,715.95; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `VERA` | 40 | $31.31 | $2.13 | $-3.84 | $5,034.62 | ▼ -3.84 after sell → book $9,713.82; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CELC` | 13 | $92.38 | $2.05 | $-12.01 | $6,233.51 | ▼ -12.01 after sell → book $9,711.77; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `BW` | 121 | $9.60 | $2.38 | $-95.49 | $7,392.73 | ▼ -95.49 after sell → book $9,709.39; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `OCC` | 68 | $16.20 | $2.22 | $-143.13 | $8,492.12 | ▼ -143.13 after sell → book $9,707.18; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALM` | 77 | $15.78 | $2.24 | $-36.80 | $9,704.93 | ▼ -36.80 after sell → book $9,704.93; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,704.93 | ▲ close $9,704.93 vs 09:30 $9,722.67 (session +0.00) | 16:00 close · cash $9,704.93 · no lots left · equity $9,704.93. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,704.93 | ▲ 09:30 equity $9,704.93 vs yday $9,704.93 (+0.00) | 09:30 open · cash $9,704.93 · no holdings · equity $9,704.93 vs prior close $9,704.93 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,704.93 | ▲ close $9,704.93 vs 09:30 $9,704.93 (session +0.00) | 16:00 close · cash $9,704.93 · no lots left · equity $9,704.93. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,704.93 | ▲ 09:30 equity $9,704.93 vs yday $9,704.93 (+0.00) | 09:30 open · cash $9,704.93 · no holdings · equity $9,704.93 vs prior close $9,704.93 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `MRVI` | 164 | $7.38 | $2.48 | — | $8,492.13 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-8.5; leftover $1213.12 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `DNA` | 162 | $7.45 | $2.48 | — | $7,282.75 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+6.9; leftover $1213.12 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MSTR` | 10 | $113.23 | $2.02 | — | $6,148.43 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1213.12 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `EXK` | 112 | $10.77 | $2.33 | — | $4,939.87 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+3.6; leftover $1213.12 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SCZM` | 128 | $9.46 | $2.37 | — | $3,726.61 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=+7.6; leftover $1213.12 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NG` | 144 | $8.38 | $2.42 | — | $2,517.47 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+7.5; leftover $1213.12 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BLSH` | 41 | $29.20 | $2.11 | — | $1,318.16 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+9.3; leftover $1213.12 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HYMC` | 44 | $27.25 | $2.12 | — | $117.04 | — | baseline list, no extra gate; list probable; 🔵; ret5=+1.6; leftover $1213.12 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $117.04 | ▲ close $9,764.26 vs 09:30 $9,704.93 (session +77.66) | 16:00 close · cash $117.04 · equity $9,764.26 vs 09:30 $9,704.93 (+59.33; session marks +77.66) · 8 name(s) marked open→close (per-name table). MRVI×164 09:30 $7.38 → close $8.26 +144.32; DNA×162 09:30 $7.45 → close $6.96 -79.38; MSTR×10 09:30 $113.23 → close $112.39 -8.40; EXK×112 09:30 $10.77 → close $10.97 +22.40; SCZM×128 09:30 $9.46 → close $9.76 +38.40; NG×144 09:30 $8.38 → close $8.66 +40.32; BLSH×41 09:30 $29.20 → close $28.44 -31.16; HYMC×44 09:30 $27.25 → close $26.14 -48.84 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $117.04 | ▲ 09:30 equity $10,114.91 vs yday $9,764.26 (+350.65) | 09:30 open · cash $117.04 (unchanged overnight, no fees) · equity $10,114.91 vs prior close $9,764.26 (+350.65) · 8 name(s) re-marked at the open (per-name table). MRVI×164 yday $8.26 → 09:30 $8.20 -9.84; DNA×162 yday $6.96 → 09:30 $7.09 +21.06; MSTR×10 yday $112.39 → 09:30 $119.69 +73.00; EXK×112 yday $10.97 → 09:30 $11.34 +41.44; SCZM×128 yday $9.76 → 09:30 $10.26 +64.00; NG×144 yday $8.66 → 09:30 $9.02 +51.84; BLSH×41 yday $28.44 → 09:30 $29.75 +53.71; HYMC×44 yday $26.14 → 09:30 $27.40 +55.44 | — |
| 2026-08-21 09:30 ET | **SELL** | `DNA` | 162 | $7.09 | $2.51 | $-63.31 | $1,263.10 | ▼ -63.31 after sell → book $10,112.39; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `MSTR` | 10 | $119.69 | $2.04 | $+60.54 | $2,457.96 | ▲ +60.54 after sell → book $10,110.35; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `EXK` | 112 | $11.34 | $2.35 | $+59.16 | $3,725.69 | ▲ +59.16 after sell → book $10,108.00; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `SCZM` | 128 | $10.26 | $2.41 | $+97.62 | $5,036.56 | ▲ +97.62 after sell → book $10,105.59; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NG` | 144 | $9.02 | $2.46 | $+87.28 | $6,332.99 | ▲ +87.28 after sell → book $10,103.14; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `BLSH` | 41 | $29.75 | $2.13 | $+18.30 | $7,550.60 | ▲ +18.30 after sell → book $10,101.00; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HYMC` | 44 | $27.40 | $2.14 | $+2.34 | $8,754.06 | ▲ +2.34 after sell → book $10,098.86; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 753 | $1.66 | $9.71 | — | $7,494.37 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $1250.58 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ENHA` | 731 | $1.71 | $9.43 | — | $6,234.93 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-32.0; leftover $1250.58 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 2 | $623.26 | $2.00 | — | $4,986.41 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+1.4; leftover $1250.58 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `QDEL` | 83 | $14.96 | $2.24 | — | $3,742.49 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-1.6; leftover $1250.58 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 1447 | $0.86 | $16.84 | — | $2,475.44 | — | baseline list, no extra gate; list probable,yday_gainer,ohlc_hot; ret5=+9.7; leftover $1250.58 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `GORO` | 402 | $3.11 | $5.19 | — | $1,220.04 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=+7.1; leftover $1250.58 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `QTRX` | 390 | $3.11 | $5.03 | — | $2.11 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+9.1; leftover $1250.58 | join🟡 sector🟢 gen🟢 news🔴 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.11 | ▲ close $10,078.52 vs 09:30 $10,114.91 (session +30.09) | 16:00 close · cash $2.11 · equity $10,078.52 vs 09:30 $10,114.91 (-36.39; session marks +30.09) · 8 name(s) marked open→close (per-name table). MRVI×164 09:30 $8.20 → close $8.70 +82.00; BTBT×753 09:30 $1.66 → close $1.53 -97.89; ENHA×731 09:30 $1.71 → close $1.72 +7.31; DE×2 09:30 $623.26 → close $647.47 +48.42; QDEL×83 09:30 $14.96 → close $14.74 -18.26; ORBS×1447 09:30 $0.86 → close $0.88 +23.15; GORO×402 09:30 $3.11 → close $3.19 +32.16; QTRX×390 09:30 $3.11 → close $2.99 -46.80 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.11 | ▲ 09:30 equity $10,114.56 vs yday $10,078.52 (+36.04) | 09:30 open · cash $2.11 (unchanged overnight, no fees) · equity $10,114.56 vs prior close $10,078.52 (+36.04) · 8 name(s) re-marked at the open (per-name table). MRVI×164 yday $8.70 → 09:30 $8.59 -18.04; BTBT×753 yday $1.53 → 09:30 $1.55 +15.06; ENHA×731 yday $1.72 → 09:30 $1.74 +14.62; DE×2 yday $647.47 → 09:30 $653.62 +12.30; QDEL×83 yday $14.74 → 09:30 $14.71 -2.49; ORBS×1447 yday $0.88 → 09:30 $0.89 +14.47; GORO×402 yday $3.19 → 09:30 $3.20 +4.02; QTRX×390 yday $2.99 → 09:30 $2.98 -3.90 | — |
| 2026-08-24 09:30 ET | **SELL** | `MRVI` | 164 | $8.59 | $2.52 | $+193.44 | $1,408.34 | ▲ +193.44 after sell → book $10,112.03; vs 09:30 mark -2.53 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BTBT` | 753 | $1.55 | $9.85 | $-102.39 | $2,565.65 | ▼ -102.39 after sell → book $10,102.19; vs 09:30 mark -9.84 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ENHA` | 731 | $1.74 | $9.56 | $+2.94 | $3,828.02 | ▲ +2.94 after sell → book $10,092.62; vs 09:30 mark -9.57 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `DE` | 2 | $653.62 | $2.02 | $+56.71 | $5,133.25 | ▲ +56.71 after sell → book $10,090.61; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `QDEL` | 83 | $14.71 | $2.26 | $-25.25 | $6,351.92 | ▼ -25.25 after sell → book $10,088.35; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ORBS` | 1447 | $0.89 | $17.47 | $+3.31 | $7,622.28 | ▲ +3.31 after sell → book $10,070.88; vs 09:30 mark -17.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `GORO` | 402 | $3.20 | $5.26 | $+25.73 | $8,903.41 | ▲ +25.73 after sell → book $10,065.61; vs 09:30 mark -5.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `QTRX` | 390 | $2.98 | $5.11 | $-60.84 | $10,060.51 | ▼ -60.84 after sell → book $10,060.51; vs 09:30 mark -5.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,060.51 | ▲ close $10,060.51 vs 09:30 $10,114.56 (session +0.00) | 16:00 close · cash $10,060.51 · no lots left · equity $10,060.51. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,060.51 | ▲ 09:30 equity $10,060.51 vs yday $10,060.51 (-0.00) | 09:30 open · cash $10,060.51 · no holdings · equity $10,060.51 vs prior close $10,060.51 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 776 | $1.62 | $10.01 | — | $8,793.38 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $1257.56 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 628 | $2.00 | $8.10 | — | $7,529.28 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.8; leftover $1257.56 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `PUSA` | 339 | $3.70 | $4.37 | — | $6,270.60 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.8; leftover $1257.56 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 240 | $5.22 | $3.10 | — | $5,014.71 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+9.2; leftover $1257.56 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 185 | $6.79 | $2.54 | — | $3,756.01 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.4; leftover $1257.56 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALIT` | 84 | $14.86 | $2.24 | — | $2,505.53 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+3.0; leftover $1257.56 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟡 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 197 | $6.38 | $2.58 | — | $1,246.09 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=+5.0; leftover $1257.56 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SAFX` | 3306 | $0.37 | $22.15 | — | $0.72 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-26.5; leftover $1257.56 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🔴 vol🔴 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.72 | ▲ close $10,187.08 vs 09:30 $10,060.51 (session +181.67) | 16:00 close · cash $0.72 · equity $10,187.08 vs 09:30 $10,060.51 (+126.57; session marks +181.67) · 8 name(s) marked open→close (per-name table). BMEA×776 09:30 $1.62 → close $1.61 -7.76; NPWR×628 09:30 $2.00 → close $2.02 +12.56; PUSA×339 09:30 $3.70 → close $3.91 +71.19; ALVO×240 09:30 $5.22 → close $5.25 +7.20; CAPR×185 09:30 $6.79 → close $7.19 +74.00; ALIT×84 09:30 $14.86 → close $14.87 +0.84; ZURA×197 09:30 $6.38 → close $6.50 +23.64; SAFX×3306 09:30 $0.37 → close $0.37 +0.00 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.72 | ▲ 09:30 equity $10,187.08 vs yday $10,187.08 (-0.00) | 09:30 open · cash $0.72 (unchanged overnight, no fees) · equity $10,187.08 vs prior close $10,187.08 (-0.00) · 8 name(s) re-marked at the open (per-name table). BMEA×776 yday $1.61 → 09:30 $1.61 +0.00; NPWR×628 yday $2.02 → 09:30 $2.02 +0.00; PUSA×339 yday $3.91 → 09:30 $3.91 +0.00; ALVO×240 yday $5.25 → 09:30 $5.25 +0.00; CAPR×185 yday $7.19 → 09:30 $7.19 +0.00; ALIT×84 yday $14.87 → 09:30 $14.87 +0.00; ZURA×197 yday $6.50 → 09:30 $6.50 +0.00; SAFX×3306 yday $0.37 → 09:30 $0.37 +0.00 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.72 | ▲ close $10,187.08 vs 09:30 $10,187.08 (session +0.00) | 16:00 close · cash $0.72 · equity $10,187.08 vs 09:30 $10,187.08 (-0.00; session marks +0.00) · 8 name(s) marked open→close (per-name table). BMEA×776 09:30 $1.61 → close $1.61 +0.00; NPWR×628 09:30 $2.02 → close $2.02 +0.00; PUSA×339 09:30 $3.91 → close $3.91 +0.00; ALVO×240 09:30 $5.25 → close $5.25 +0.00; CAPR×185 09:30 $7.19 → close $7.19 +0.00; ALIT×84 09:30 $14.87 → close $14.87 +0.00; ZURA×197 09:30 $6.50 → close $6.50 +0.00; SAFX×3306 09:30 $0.37 → close $0.37 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.72 | ▲ 09:30 equity $10,213.48 vs yday $10,187.08 (+26.40) | 09:30 open · cash $0.72 (unchanged overnight, no fees) · equity $10,213.48 vs prior close $10,187.08 (+26.40) · 8 name(s) re-marked at the open (per-name table). BMEA×776 yday $1.61 → 09:30 $1.75 +108.64; NPWR×628 yday $2.02 → 09:30 $1.93 -56.52; PUSA×339 yday $3.91 → 09:30 $3.84 -23.73; ALVO×240 yday $5.25 → 09:30 $4.98 -64.80; CAPR×185 yday $7.19 → 09:30 $8.29 +203.50; ALIT×84 yday $14.87 → 09:30 $14.85 -1.68; ZURA×197 yday $6.50 → 09:30 $6.13 -72.89; SAFX×3306 yday $0.37 → 09:30 $0.35 -66.12 | — |
| 2026-08-27 09:30 ET | **SELL** | `BMEA` | 776 | $1.75 | $10.15 | $+80.72 | $1,348.57 | ▲ +80.72 after sell → book $10,203.33; vs 09:30 mark -10.15 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `NPWR` | 628 | $1.93 | $8.22 | $-60.28 | $2,552.39 | ▼ -60.28 after sell → book $10,195.11; vs 09:30 mark -8.22 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `PUSA` | 339 | $3.84 | $4.44 | $+38.65 | $3,849.71 | ▲ +38.65 after sell → book $10,190.67; vs 09:30 mark -4.44 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ALVO` | 240 | $4.98 | $3.15 | $-63.84 | $5,041.77 | ▼ -63.84 after sell → book $10,187.53; vs 09:30 mark -3.14 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CAPR` | 185 | $8.29 | $2.59 | $+272.37 | $6,572.83 | ▲ +272.37 after sell → book $10,184.94; vs 09:30 mark -2.59 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ALIT` | 84 | $14.85 | $2.27 | $-5.35 | $7,817.96 | ▼ -5.35 after sell → book $10,182.67; vs 09:30 mark -2.27 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ZURA` | 197 | $6.13 | $2.62 | $-54.45 | $9,022.95 | ▼ -54.45 after sell → book $10,180.05; vs 09:30 mark -2.62 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SAFX` | 3306 | $0.35 | $22.05 | $-110.32 | $10,158.00 | ▼ -110.32 after sell → book $10,158.00; vs 09:30 mark -22.05 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,158.00 | ▲ close $10,158.00 vs 09:30 $10,213.48 (session +0.00) | 16:00 close · cash $10,158.00 · no lots left · equity $10,158.00. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,158.00 | ▲ 09:30 equity $10,158.00 vs yday $10,158.00 (+0.00) | 09:30 open · cash $10,158.00 · no holdings · equity $10,158.00 vs prior close $10,158.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 8 | $144.70 | $2.01 | — | $8,998.39 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1269.75 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BHVN` | 74 | $16.95 | $2.21 | — | $7,741.88 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $1269.75 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 68 | $18.50 | $2.19 | — | $6,481.68 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $1269.75 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 138 | $9.19 | $2.40 | — | $5,211.06 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $1269.75 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `LVWR` | 920 | $1.38 | $11.87 | — | $3,929.59 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+0.0; leftover $1269.75 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 37 | $33.78 | $2.10 | — | $2,677.63 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.9; leftover $1269.75 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 8 | $149.40 | $2.01 | — | $1,480.41 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=-11.6; leftover $1269.75 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 79 | $15.94 | $2.23 | — | $218.93 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-1.9; leftover $1269.75 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $218.93 | ▼ close $10,057.74 vs 09:30 $10,158.00 (session -73.23) | 16:00 close · cash $218.93 · equity $10,057.74 vs 09:30 $10,158.00 (-100.26; session marks -73.23) · 8 name(s) marked open→close (per-name table). ANF×8 09:30 $144.70 → close $145.75 +8.40; BHVN×74 09:30 $16.95 → close $16.12 -61.42; BZ×68 09:30 $18.50 → close $18.00 -34.00; CAPR×138 09:30 $9.19 → close $10.06 +120.06; LVWR×920 09:30 $1.38 → close $1.36 -18.40; SEDG×37 09:30 $33.78 → close $33.51 -9.99; SMTC×8 09:30 $149.40 → close $142.43 -55.76; GRRR×79 09:30 $15.94 → close $15.66 -22.12 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $218.93 | ▼ 09:30 equity $9,691.59 vs yday $10,057.74 (-366.15) | 09:30 open · cash $218.93 (unchanged overnight, no fees) · equity $9,691.59 vs prior close $10,057.74 (-366.15) · 8 name(s) re-marked at the open (per-name table). ANF×8 yday $145.75 → 09:30 $148.67 +23.36; BHVN×74 yday $16.12 → 09:30 $15.44 -50.32; BZ×68 yday $18.00 → 09:30 $17.89 -7.48; CAPR×138 yday $10.06 → 09:30 $9.44 -85.56; LVWR×920 yday $1.36 → 09:30 $1.37 +9.20; SEDG×37 yday $33.51 → 09:30 $31.50 -74.37; SMTC×8 yday $142.43 → 09:30 $133.04 -75.12; GRRR×79 yday $15.66 → 09:30 $14.32 -105.86 | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 8 | $148.67 | $2.03 | $+27.71 | $1,406.25 | ▲ +27.71 after sell → book $9,689.55; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BHVN` | 74 | $15.44 | $2.23 | $-116.19 | $2,546.58 | ▼ -116.19 after sell → book $9,687.32; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BZ` | 68 | $17.89 | $2.22 | $-45.89 | $3,760.88 | ▼ -45.89 after sell → book $9,685.10; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 138 | $9.44 | $2.44 | $+29.66 | $5,061.17 | ▲ +29.66 after sell → book $9,682.67; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `LVWR` | 920 | $1.37 | $12.03 | $-33.10 | $6,309.54 | ▼ -33.10 after sell → book $9,670.64; vs 09:30 mark -12.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 37 | $31.50 | $2.12 | $-88.58 | $7,472.91 | ▼ -88.58 after sell → book $9,668.51; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 8 | $133.04 | $2.03 | $-134.93 | $8,535.20 | ▼ -134.93 after sell → book $9,666.48; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GRRR` | 79 | $14.32 | $2.25 | $-132.46 | $9,664.23 | ▼ -132.46 after sell → book $9,664.23; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,664.23 | ▲ close $9,664.23 vs 09:30 $9,691.59 (session +0.00) | 16:00 close · cash $9,664.23 · no lots left · equity $9,664.23. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,664.23 | ▲ 09:30 equity $9,664.23 vs yday $9,664.23 (+0.00) | 09:30 open · cash $9,664.23 · no holdings · equity $9,664.23 vs prior close $9,664.23 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,664.23 | ▲ close $9,664.23 vs 09:30 $9,664.23 (session +0.00) | 16:00 close · cash $9,664.23 · no lots left · equity $9,664.23. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,664.23 | ▲ 09:30 equity $9,664.23 vs yday $9,664.23 (+0.00) | 09:30 open · cash $9,664.23 · no holdings · equity $9,664.23 vs prior close $9,664.23 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,664.23 | ▲ close $9,664.23 vs 09:30 $9,664.23 (session +0.00) | 16:00 close · cash $9,664.23 · no lots left · equity $9,664.23. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,664.23 | ▲ 09:30 equity $9,664.23 vs yday $9,664.23 (+0.00) | 09:30 open · cash $9,664.23 · no holdings · equity $9,664.23 vs prior close $9,664.23 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 990 | $1.22 | $12.77 | — | $8,443.66 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1208.03 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRVO` | 65 | $18.40 | $2.19 | — | $7,245.47 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $1208.03 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 76 | $15.70 | $2.22 | — | $6,050.06 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1208.03 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 53 | $22.78 | $2.15 | — | $4,840.57 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1208.03 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CTMX` | 324 | $3.72 | $4.18 | — | $3,631.11 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=-2.4; leftover $1208.03 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SLN` | 82 | $14.70 | $2.24 | — | $2,423.47 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=-3.3; leftover $1208.03 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `EIX` | 21 | $56.78 | $2.05 | — | $1,229.04 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=+0.3; leftover $1208.03 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 559 | $2.16 | $7.21 | — | $14.39 | — | baseline list, no extra gate; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.3; leftover $1208.03 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14.39 | ▲ close $10,086.59 vs 09:30 $9,664.23 (session +457.36) | 16:00 close · cash $14.39 · equity $10,086.59 vs 09:30 $9,664.23 (+422.36; session marks +457.36) · 8 name(s) marked open→close (per-name table). GPRO×990 09:30 $1.22 → close $1.69 +465.30; FRVO×65 09:30 $18.40 → close $17.98 -27.30; CRK×76 09:30 $15.70 → close $15.54 -12.16; MMED×53 09:30 $22.78 → close $23.76 +51.94; CTMX×324 09:30 $3.72 → close $3.72 +0.00; SLN×82 09:30 $14.70 → close $14.79 +7.38; EIX×21 09:30 $56.78 → close $55.19 -33.39; CRDL×559 09:30 $2.16 → close $2.17 +5.59 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14.39 | ▲ 09:30 equity $10,212.64 vs yday $10,086.59 (+126.05) | 09:30 open · cash $14.39 (unchanged overnight, no fees) · equity $10,212.64 vs prior close $10,086.59 (+126.05) · 8 name(s) re-marked at the open (per-name table). GPRO×990 yday $1.69 → 09:30 $1.78 +89.10; FRVO×65 yday $17.98 → 09:30 $18.27 +18.85; CRK×76 yday $15.54 → 09:30 $15.45 -6.84; MMED×53 yday $23.76 → 09:30 $23.88 +6.36; CTMX×324 yday $3.72 → 09:30 $3.73 +3.24; SLN×82 yday $14.79 → 09:30 $14.85 +4.92; EIX×21 yday $55.19 → 09:30 $55.42 +4.83; CRDL×559 yday $2.17 → 09:30 $2.18 +5.59 | — |
| 2026-09-04 09:30 ET | **SELL** | `FRVO` | 65 | $18.27 | $2.21 | $-12.84 | $1,199.73 | ▼ -12.84 after sell → book $10,210.43; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 76 | $15.45 | $2.24 | $-23.46 | $2,371.69 | ▼ -23.46 after sell → book $10,208.19; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 53 | $23.88 | $2.17 | $+53.98 | $3,635.16 | ▲ +53.98 after sell → book $10,206.02; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CTMX` | 324 | $3.73 | $4.24 | $-5.18 | $4,839.44 | ▼ -5.18 after sell → book $10,201.78; vs 09:30 mark -4.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `SLN` | 82 | $14.85 | $2.26 | $+7.80 | $6,054.88 | ▲ +7.80 after sell → book $10,199.52; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `EIX` | 21 | $55.42 | $2.07 | $-32.69 | $7,216.63 | ▼ -32.69 after sell → book $10,197.45; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRDL` | 559 | $2.18 | $7.31 | $-3.35 | $8,427.93 | ▼ -3.35 after sell → book $10,190.13; vs 09:30 mark -7.32 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 617 | $1.95 | $7.96 | — | $7,216.82 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $1203.99 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `EOSE` | 337 | $3.57 | $4.35 | — | $6,009.39 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.2; leftover $1203.99 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `SLBT` | 392 | $3.07 | $5.06 | — | $4,800.89 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-0.4; leftover $1203.99 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 2 | $486.31 | $2.00 | — | $3,826.27 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-9.9; leftover $1203.99 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MLYS` | 41 | $29.15 | $2.11 | — | $2,629.01 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.0; leftover $1203.99 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `CCOI` | 117 | $10.22 | $2.34 | — | $1,430.93 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.9; leftover $1203.99 | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `SION` | 164 | $7.31 | $2.48 | — | $229.61 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.1; leftover $1203.99 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $229.61 | ▼ close $9,683.50 vs 09:30 $10,212.64 (session -480.34) | 16:00 close · cash $229.61 · equity $9,683.50 vs 09:30 $10,212.64 (-529.14; session marks -480.34) · 8 name(s) marked open→close (per-name table). GPRO×990 09:30 $1.78 → close $1.39 -386.10; BAK×617 09:30 $1.95 → close $1.94 -6.17; EOSE×337 09:30 $3.57 → close $3.50 -23.59; SLBT×392 09:30 $3.07 → close $3.15 +31.36; DELL×2 09:30 $486.31 → close $516.39 +60.16; MLYS×41 09:30 $29.15 → close $28.27 -36.08; CCOI×117 09:30 $10.22 → close $9.98 -28.08; SION×164 09:30 $7.31 → close $6.75 -91.84 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `JLHL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CBRS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COHR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TDTH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `PGEN` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `INDP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `NXE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `PURR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `GUTS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `RANI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SECZ` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ULTA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNTN` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNDY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SAFX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `USAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ALOY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-26 | `BMEA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `NPWR` | no_price | no 09:30 open — carry |
| 2026-08-26 | `PUSA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ALVO` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ALIT` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ZURA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `SAFX` | no_price | no 09:30 open — carry |
| 2026-08-26 | `KURA` | no_price | no 09:30 open |
| 2026-08-26 | `AVBP` | no_price | no 09:30 open |
| 2026-08-26 | `FLNC` | no_price | no 09:30 open |
| 2026-08-26 | `ABX` | no_price | no 09:30 open |
| 2026-08-26 | `AVEX` | no_price | no 09:30 open |
| 2026-08-26 | `ITG` | no_price | no 09:30 open |
| 2026-08-26 | `BE` | no_price | no 09:30 open |
| 2026-08-31 | `OKTA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `VEEV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RPD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ACDC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SNPS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `BRUN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `TRGP` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NEOV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `AME` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NMRA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CTMX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SLDB` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NTNX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NAGE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRVO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FATE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TII` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `AVXL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `BMO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `KMX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ZNTL` | hard_red | hard-red S=-3.83 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `GPRO` | 990 | 2026-09-03 @ $1.22 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1208.03 |
| `BAK` | 617 | 2026-09-04 @ $1.95 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $1203.99 |
| `EOSE` | 337 | 2026-09-04 @ $3.57 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.2; leftover $1203.99 |
| `SLBT` | 392 | 2026-09-04 @ $3.07 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-0.4; leftover $1203.99 |
| `DELL` | 2 | 2026-09-04 @ $486.31 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-9.9; leftover $1203.99 |
| `MLYS` | 41 | 2026-09-04 @ $29.15 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.0; leftover $1203.99 |
| `CCOI` | 117 | 2026-09-04 @ $10.22 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.9; leftover $1203.99 |
| `SION` | 164 | 2026-09-04 @ $7.31 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.1; leftover $1203.99 |
