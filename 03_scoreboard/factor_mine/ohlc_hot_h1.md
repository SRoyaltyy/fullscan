# Factor mine action — `ohlc_hot_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `ohlc_hot` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · baseline list, no extra gate

Cash book **-5.07%** ($9,493) · signal-only (no cash/fees) was -3.30%. Starts YES **5/17**. Fills 84 · skips 28 · realized $-418.83.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `ohlc_hot` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $13.27.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `ADUR` | 75 | — | $16.50 | +0.00 | $16.17 | -24.75 | -24.75 | +0.00 | -24.75 |
| 2026-08-14 | `ANRO` | 39 | — | $31.77 | +0.00 | $32.14 | +14.43 | +14.43 | +0.00 | +14.43 |
| 2026-08-14 | `LIFE` | 35 | — | $35.04 | +0.00 | $34.02 | -35.70 | -35.70 | +0.00 | -35.70 |
| 2026-08-14 | `VOYG` | 28 | — | $44.49 | +0.00 | $42.98 | -42.28 | -42.28 | +0.00 | -42.28 |
| 2026-08-14 | `LUNR` | 65 | — | $19.17 | +0.00 | $19.01 | -10.40 | -10.40 | +0.00 | -10.40 |
| 2026-08-14 | `BETA` | 49 | — | $25.21 | +0.00 | $24.86 | -17.15 | -17.15 | +0.00 | -17.15 |
| 2026-08-14 | `FORM` | 9 | — | $129.48 | +0.00 | $131.60 | +19.08 | +19.08 | +0.00 | +19.08 |
| 2026-08-14 | `ENTG` | 7 | — | $162.45 | +0.00 | $161.76 | -4.83 | -4.83 | +0.00 | -4.83 |
| 2026-08-17 | `ADUR` | 75 | $16.17 | $15.73 | -33.00 | — | +0.00 | -33.00 | -57.75 | — |
| 2026-08-17 | `ANRO` | 39 | $32.14 | $32.15 | +0.39 | — | +0.00 | +0.39 | +14.82 | — |
| 2026-08-17 | `LIFE` | 35 | $34.02 | $34.03 | +0.35 | — | +0.00 | +0.35 | -35.35 | — |
| 2026-08-17 | `VOYG` | 28 | $42.98 | $42.12 | -24.08 | — | +0.00 | -24.08 | -66.36 | — |
| 2026-08-17 | `LUNR` | 65 | $19.01 | $20.25 | +80.60 | $20.38 | +8.45 | +89.05 | +70.20 | +78.65 |
| 2026-08-17 | `BETA` | 49 | $24.86 | $24.61 | -12.25 | — | +0.00 | -12.25 | -29.40 | — |
| 2026-08-17 | `FORM` | 9 | $131.60 | $134.05 | +22.05 | — | +0.00 | +22.05 | +41.13 | — |
| 2026-08-17 | `ENTG` | 7 | $161.76 | $162.04 | +1.96 | — | +0.00 | +1.96 | -2.87 | — |
| 2026-08-17 | `OCC` | 67 | — | $18.24 | +0.00 | $17.12 | -75.04 | -75.04 | +0.00 | -75.04 |
| 2026-08-17 | `ALM` | 75 | — | $16.20 | +0.00 | $16.36 | +12.00 | +12.00 | +0.00 | +12.00 |
| 2026-08-17 | `LPTH` | 82 | — | $14.94 | +0.00 | $14.80 | -11.48 | -11.48 | +0.00 | -11.48 |
| 2026-08-17 | `AAOI` | 8 | — | $152.64 | +0.00 | $154.89 | +18.00 | +18.00 | +0.00 | +18.00 |
| 2026-08-17 | `CLYM` | 75 | — | $16.25 | +0.00 | $17.44 | +89.25 | +89.25 | +0.00 | +89.25 |
| 2026-08-17 | `BORR` | 267 | — | $4.59 | +0.00 | $4.50 | -24.03 | -24.03 | +0.00 | -24.03 |
| 2026-08-17 | `IOVA` | 179 | — | $6.84 | +0.00 | $7.10 | +46.54 | +46.54 | +0.00 | +46.54 |
| 2026-08-18 | `LUNR` | 65 | $20.38 | $19.31 | -69.55 | — | +0.00 | -69.55 | +9.10 | — |
| 2026-08-18 | `OCC` | 67 | $17.12 | $16.20 | -61.64 | — | +0.00 | -61.64 | -136.68 | — |
| 2026-08-18 | `ALM` | 75 | $16.36 | $15.78 | -43.50 | — | +0.00 | -43.50 | -31.50 | — |
| 2026-08-18 | `LPTH` | 82 | $14.80 | $14.01 | -64.78 | — | +0.00 | -64.78 | -76.26 | — |
| 2026-08-18 | `AAOI` | 8 | $154.89 | $146.20 | -69.52 | $131.41 | -118.32 | -187.84 | -51.52 | -169.84 |
| 2026-08-18 | `CLYM` | 75 | $17.44 | $16.90 | -40.50 | — | +0.00 | -40.50 | +48.75 | — |
| 2026-08-18 | `BORR` | 267 | $4.50 | $4.56 | +16.02 | — | +0.00 | +16.02 | -8.01 | — |
| 2026-08-18 | `IOVA` | 179 | $7.10 | $7.00 | -17.90 | $7.03 | +5.37 | -12.53 | +28.64 | +34.01 |
| 2026-08-19 | `AAOI` | 8 | $131.41 | $135.85 | +35.52 | — | +0.00 | +35.52 | -134.32 | — |
| 2026-08-19 | `IOVA` | 179 | $7.03 | $7.20 | +30.43 | — | +0.00 | +30.43 | +64.44 | — |
| 2026-08-20 | `AEM` | 5 | — | $204.45 | +0.00 | $212.04 | +37.95 | +37.95 | +0.00 | +37.95 |
| 2026-08-20 | `TWST` | 8 | — | $136.84 | +0.00 | $136.33 | -4.08 | -4.08 | +0.00 | -4.08 |
| 2026-08-20 | `ABTC` | 140 | — | $8.46 | +0.00 | $8.47 | +1.40 | +1.40 | +0.00 | +1.40 |
| 2026-08-20 | `HL` | 58 | — | $20.25 | +0.00 | $20.82 | +33.06 | +33.06 | +0.00 | +33.06 |
| 2026-08-20 | `SBET` | 157 | — | $7.55 | +0.00 | $7.59 | +6.28 | +6.28 | +0.00 | +6.28 |
| 2026-08-20 | `PPC` | 38 | — | $30.65 | +0.00 | $31.24 | +22.42 | +22.42 | +0.00 | +22.42 |
| 2026-08-20 | `ABCL` | 100 | — | $11.81 | +0.00 | $11.57 | -24.50 | -24.50 | +0.00 | -24.50 |
| 2026-08-20 | `SENS` | 133 | — | $8.91 | +0.00 | $8.82 | -11.97 | -11.97 | +0.00 | -11.97 |
| 2026-08-21 | `AEM` | 5 | $212.04 | $216.30 | +21.30 | $216.06 | -1.20 | +20.10 | +59.25 | +58.05 |
| 2026-08-21 | `TWST` | 8 | $136.33 | $138.43 | +16.80 | — | +0.00 | +16.80 | +12.72 | — |
| 2026-08-21 | `ABTC` | 140 | $8.47 | $8.66 | +26.60 | $7.93 | -102.20 | -75.60 | +28.00 | -74.20 |
| 2026-08-21 | `HL` | 58 | $20.82 | $21.33 | +29.58 | — | +0.00 | +29.58 | +62.64 | — |
| 2026-08-21 | `SBET` | 157 | $7.59 | $7.87 | +43.96 | — | +0.00 | +43.96 | +50.24 | — |
| 2026-08-21 | `PPC` | 38 | $31.24 | $31.13 | -4.18 | — | +0.00 | -4.18 | +18.24 | — |
| 2026-08-21 | `ABCL` | 100 | $11.57 | $11.57 | +0.00 | — | +0.00 | +0.00 | -24.50 | — |
| 2026-08-21 | `SENS` | 133 | $8.82 | $9.24 | +55.86 | — | +0.00 | +55.86 | +43.89 | — |
| 2026-08-21 | `ORBS` | 1438 | — | $0.86 | +0.00 | $0.88 | +23.01 | +23.01 | +0.00 | +23.01 |
| 2026-08-21 | `GRAL` | 15 | — | $78.88 | +0.00 | $79.54 | +9.90 | +9.90 | +0.00 | +9.90 |
| 2026-08-21 | `MSTR` | 10 | — | $119.69 | +0.00 | $119.25 | -4.40 | -4.40 | +0.00 | -4.40 |
| 2026-08-21 | `TRON` | 640 | — | $1.94 | +0.00 | $2.01 | +44.80 | +44.80 | +0.00 | +44.80 |
| 2026-08-21 | `XHG` | 276 | — | $4.49 | +0.00 | $4.41 | -22.08 | -22.08 | +0.00 | -22.08 |
| 2026-08-21 | `AUGO` | 13 | — | $89.10 | +0.00 | $87.26 | -23.92 | -23.92 | +0.00 | -23.92 |
| 2026-08-24 | `AEM` | 5 | $216.06 | $217.03 | +4.85 | — | +0.00 | +4.85 | +62.90 | — |
| 2026-08-24 | `ABTC` | 140 | $7.93 | $8.06 | +18.20 | — | +0.00 | +18.20 | -56.00 | — |
| 2026-08-24 | `ORBS` | 1438 | $0.88 | $0.89 | +14.38 | — | +0.00 | +14.38 | +37.39 | — |
| 2026-08-24 | `GRAL` | 15 | $79.54 | $81.87 | +34.95 | — | +0.00 | +34.95 | +44.85 | — |
| 2026-08-24 | `MSTR` | 10 | $119.25 | $121.76 | +25.10 | — | +0.00 | +25.10 | +20.70 | — |
| 2026-08-24 | `TRON` | 640 | $2.01 | $2.02 | +6.40 | — | +0.00 | +6.40 | +51.20 | — |
| 2026-08-24 | `XHG` | 276 | $4.41 | $4.24 | -46.92 | $4.06 | -49.68 | -96.60 | -69.00 | -118.68 |
| 2026-08-24 | `AUGO` | 13 | $87.26 | $89.87 | +33.93 | — | +0.00 | +33.93 | +10.01 | — |
| 2026-08-25 | `XHG` | 276 | $4.06 | $4.02 | -11.04 | $4.05 | +8.28 | -2.76 | -129.72 | -121.44 |
| 2026-08-25 | `DEFT` | 1902 | — | $0.64 | +0.00 | $0.62 | -38.04 | -38.04 | +0.00 | -38.04 |
| 2026-08-25 | `AMTX` | 654 | — | $1.86 | +0.00 | $1.86 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-25 | `NIQ` | 62 | — | $19.56 | +0.00 | $19.46 | -6.20 | -6.20 | +0.00 | -6.20 |
| 2026-08-25 | `OMER` | 64 | — | $18.75 | +0.00 | $19.03 | +17.92 | +17.92 | +0.00 | +17.92 |
| 2026-08-25 | `ERO` | 32 | — | $38.00 | +0.00 | $38.55 | +17.60 | +17.60 | +0.00 | +17.60 |
| 2026-08-25 | `TRLV` | 110 | — | $11.02 | +0.00 | $11.02 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-25 | `FUTU` | 10 | — | $118.02 | +0.00 | $118.50 | +4.80 | +4.80 | +0.00 | +4.80 |
| 2026-08-26 | `XHG` | 276 | $4.05 | $4.05 | +0.00 | $4.05 | +0.00 | +0.00 | -121.44 | -121.44 |
| 2026-08-26 | `DEFT` | 1902 | $0.62 | $0.62 | +0.00 | $0.62 | +0.00 | +0.00 | -38.04 | -38.04 |
| 2026-08-26 | `AMTX` | 654 | $1.86 | $1.86 | +0.00 | $1.86 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-26 | `NIQ` | 62 | $19.46 | $19.46 | +0.00 | $19.46 | +0.00 | +0.00 | -6.20 | -6.20 |
| 2026-08-26 | `OMER` | 64 | $19.03 | $19.03 | +0.00 | $19.03 | +0.00 | +0.00 | +17.92 | +17.92 |
| 2026-08-26 | `ERO` | 32 | $38.55 | $38.55 | +0.00 | $38.55 | +0.00 | +0.00 | +17.60 | +17.60 |
| 2026-08-26 | `TRLV` | 110 | $11.02 | $11.02 | +0.00 | $11.02 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-26 | `FUTU` | 10 | $118.50 | $118.50 | +0.00 | $118.50 | +0.00 | +0.00 | +4.80 | +4.80 |
| 2026-08-27 | `XHG` | 276 | $4.05 | $3.81 | -66.24 | — | +0.00 | -66.24 | -187.68 | — |
| 2026-08-27 | `DEFT` | 1902 | $0.62 | $0.60 | -38.04 | — | +0.00 | -38.04 | -76.08 | — |
| 2026-08-27 | `AMTX` | 654 | $1.86 | $1.91 | +32.70 | — | +0.00 | +32.70 | +32.70 | — |
| 2026-08-27 | `NIQ` | 62 | $19.46 | $19.20 | -16.12 | — | +0.00 | -16.12 | -22.32 | — |
| 2026-08-27 | `OMER` | 64 | $19.03 | $18.96 | -4.48 | — | +0.00 | -4.48 | +13.44 | — |
| 2026-08-27 | `ERO` | 32 | $38.55 | $40.51 | +62.72 | — | +0.00 | +62.72 | +80.32 | — |
| 2026-08-27 | `TRLV` | 110 | $11.02 | $11.22 | +22.00 | — | +0.00 | +22.00 | +22.00 | — |
| 2026-08-27 | `FUTU` | 10 | $118.50 | $124.67 | +61.70 | — | +0.00 | +61.70 | +66.50 | — |
| 2026-08-28 | `ZYME` | 40 | — | $29.33 | +0.00 | $29.01 | -12.80 | -12.80 | +0.00 | -12.80 |
| 2026-08-28 | `XHG` | 296 | — | $4.06 | +0.00 | $3.80 | -76.96 | -76.96 | +0.00 | -76.96 |
| 2026-08-28 | `NIQ` | 63 | — | $18.79 | +0.00 | $19.07 | +17.64 | +17.64 | +0.00 | +17.64 |
| 2026-08-28 | `DEFT` | 2003 | — | $0.60 | +0.00 | $0.65 | +100.15 | +100.15 | +0.00 | +100.15 |
| 2026-08-28 | `OMER` | 65 | — | $18.24 | +0.00 | $19.25 | +65.65 | +65.65 | +0.00 | +65.65 |
| 2026-08-28 | `ERO` | 30 | — | $39.20 | +0.00 | $39.82 | +18.60 | +18.60 | +0.00 | +18.60 |
| 2026-08-28 | `TRLV` | 105 | — | $11.38 | +0.00 | $11.03 | -36.75 | -36.75 | +0.00 | -36.75 |
| 2026-08-28 | `FUTU` | 9 | — | $128.00 | +0.00 | $124.57 | -30.87 | -30.87 | +0.00 | -30.87 |
| 2026-08-31 | `ZYME` | 40 | $29.01 | $28.27 | -29.60 | — | +0.00 | -29.60 | -42.40 | — |
| 2026-08-31 | `XHG` | 296 | $3.80 | $3.44 | -106.56 | $3.44 | +0.00 | -106.56 | -183.52 | -183.52 |
| 2026-08-31 | `NIQ` | 63 | $19.07 | $19.20 | +8.19 | $19.20 | +0.00 | +8.19 | +25.83 | +25.83 |
| 2026-08-31 | `DEFT` | 2003 | $0.65 | $0.62 | -60.09 | $0.62 | +0.00 | -60.09 | +40.06 | +40.06 |
| 2026-08-31 | `OMER` | 65 | $19.25 | $18.61 | -41.60 | $18.50 | -7.15 | -48.75 | +24.05 | +16.90 |
| 2026-08-31 | `ERO` | 30 | $39.82 | $38.60 | -36.60 | $38.49 | -3.30 | -39.90 | -18.00 | -21.30 |
| 2026-08-31 | `TRLV` | 105 | $11.03 | $12.41 | +144.90 | $12.41 | +0.00 | +144.90 | +108.15 | +108.15 |
| 2026-08-31 | `FUTU` | 9 | $124.57 | $122.82 | -15.75 | $124.04 | +10.98 | -4.77 | -46.62 | -35.64 |
| 2026-09-01 | `XHG` | 296 | $3.44 | $3.52 | +23.68 | $3.43 | -26.64 | -2.96 | -159.84 | -186.48 |
| 2026-09-01 | `NIQ` | 63 | $19.20 | $19.06 | -8.82 | — | +0.00 | -8.82 | +17.01 | — |
| 2026-09-01 | `DEFT` | 2003 | $0.62 | $0.59 | -60.09 | $0.61 | +40.06 | -20.03 | -20.03 | +20.03 |
| 2026-09-01 | `OMER` | 65 | $18.50 | $18.79 | +18.85 | $18.79 | +0.00 | +18.85 | +35.75 | +35.75 |
| 2026-09-01 | `ERO` | 30 | $38.49 | $37.30 | -35.70 | $36.01 | -38.70 | -74.40 | -57.00 | -95.70 |
| 2026-09-01 | `TRLV` | 105 | $12.41 | $11.89 | -54.60 | $11.89 | +0.00 | -54.60 | +53.55 | +53.55 |
| 2026-09-01 | `FUTU` | 9 | $124.04 | $122.22 | -16.38 | $120.88 | -12.06 | -28.44 | -52.02 | -64.08 |
| 2026-09-02 | `XHG` | 296 | $3.43 | $3.48 | +14.80 | $3.51 | +8.88 | +23.68 | -171.68 | -162.80 |
| 2026-09-02 | `DEFT` | 2003 | $0.61 | $0.63 | +40.06 | $0.66 | +60.09 | +100.15 | +60.09 | +120.18 |
| 2026-09-02 | `OMER` | 65 | $18.79 | $18.66 | -8.45 | $18.75 | +5.85 | -2.60 | +27.30 | +33.15 |
| 2026-09-02 | `ERO` | 30 | $36.01 | $35.95 | -1.80 | $34.82 | -33.90 | -35.70 | -97.50 | -131.40 |
| 2026-09-02 | `TRLV` | 105 | $11.89 | $11.54 | -36.75 | $11.74 | +21.00 | -15.75 | +16.80 | +37.80 |
| 2026-09-02 | `FUTU` | 9 | $120.88 | $119.82 | -9.54 | $119.28 | -4.86 | -14.40 | -73.62 | -78.48 |
| 2026-09-03 | `XHG` | 296 | $3.51 | $3.57 | +17.76 | $3.32 | -74.00 | -56.24 | -145.04 | -219.04 |
| 2026-09-03 | `DEFT` | 2003 | $0.66 | $0.67 | +20.03 | $0.65 | -40.06 | -20.03 | +140.21 | +100.15 |
| 2026-09-03 | `OMER` | 65 | $18.75 | $18.97 | +14.30 | $18.86 | -7.15 | +7.15 | +47.45 | +40.30 |
| 2026-09-03 | `ERO` | 30 | $34.82 | $35.62 | +24.00 | $34.76 | -25.80 | -1.80 | -107.40 | -133.20 |
| 2026-09-03 | `TRLV` | 105 | $11.74 | $11.78 | +4.20 | $11.69 | -9.45 | -5.25 | +42.00 | +32.55 |
| 2026-09-03 | `FUTU` | 9 | $119.28 | $119.46 | +1.62 | $118.08 | -12.42 | -10.80 | -76.86 | -89.28 |
| 2026-09-03 | `NVAX` | 118 | — | $10.27 | +0.00 | $10.32 | +5.90 | +5.90 | +0.00 | +5.90 |
| 2026-09-03 | `NIQ` | 65 | — | $18.60 | +0.00 | $18.35 | -16.25 | -16.25 | +0.00 | -16.25 |
| 2026-09-04 | `XHG` | 296 | $3.32 | $3.38 | +17.76 | $3.43 | +14.80 | +32.56 | -201.28 | -186.48 |
| 2026-09-04 | `DEFT` | 2003 | $0.65 | $0.65 | +0.00 | $0.68 | +60.09 | +60.09 | +100.15 | +160.24 |
| 2026-09-04 | `OMER` | 65 | $18.86 | $18.99 | +8.45 | $19.11 | +7.80 | +16.25 | +48.75 | +56.55 |
| 2026-09-04 | `ERO` | 30 | $34.76 | $35.82 | +31.80 | $35.32 | -15.00 | +16.80 | -101.40 | -116.40 |
| 2026-09-04 | `TRLV` | 105 | $11.69 | $11.89 | +21.00 | $11.99 | +10.50 | +31.50 | +53.55 | +64.05 |
| 2026-09-04 | `FUTU` | 9 | $118.08 | $118.19 | +0.99 | $122.01 | +34.38 | +35.37 | -88.29 | -53.91 |
| 2026-09-04 | `NVAX` | 118 | $10.32 | $10.41 | +10.62 | $10.34 | -8.26 | +2.36 | +16.52 | +8.26 |
| 2026-09-04 | `NIQ` | 65 | $18.35 | $18.66 | +20.15 | $18.82 | +10.40 | +30.55 | +3.90 | +14.30 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | -101.60 | ADUR, ANRO, LIFE, VOYG, LUNR, BETA, FORM, ENTG | — | $250.70 | $9,881.56 | ADUR×75, ANRO×39, LIFE×35, VOYG×28, LUNR×65, BETA×49, FORM×9, ENTG×7 |
| 2026-08-17 | +2.25 | $250.70 | ADUR×75, ANRO×39, LIFE×35, VOYG×28, LUNR×65, BETA×49, FORM×9, ENTG×7 | $9,917.58 | +36.02 | +63.69 | OCC, ALM, LPTH, AAOI, CLYM, BORR, IOVA | ADUR, ANRO, LIFE, VOYG, BETA, FORM, ENTG | $17.77 | $9,949.63 | LUNR×65, OCC×67, ALM×75, LPTH×82, AAOI×8, CLYM×75, BORR×267, IOVA×179 |
| 2026-08-18 | -6.20 | $17.77 | LUNR×65, OCC×67, ALM×75, LPTH×82, AAOI×8, CLYM×75, BORR×267, IOVA×179 | $9,598.26 | -351.37 | -112.95 | — | LUNR, OCC, ALM, LPTH, CLYM, BORR | $7,161.01 | $9,470.66 | AAOI×8, IOVA×179 |
| 2026-08-19 | -7.20 | $7,161.01 | AAOI×8, IOVA×179 | $9,536.61 | +65.95 | +0.00 | — | AAOI, IOVA | $9,532.01 | $9,532.01 | — |
| 2026-08-20 | +1.12 | $9,532.01 | — | $9,532.01 | -0.00 | +60.56 | AEM, TWST, ABTC, HL, SBET, PPC, ABCL, SENS | — | $321.72 | $9,574.73 | AEM×5, TWST×8, ABTC×140, HL×58, SBET×157, PPC×38, ABCL×100, SENS×133 |
| 2026-08-21 | +3.25 | $321.72 | AEM×5, TWST×8, ABTC×140, HL×58, SBET×157, PPC×38, ABCL×100, SENS×133 | $9,764.65 | +189.92 | -76.09 | ORBS, GRAL, MSTR, TRON, XHG, AUGO | TWST, HL, SBET, PPC, ABCL, SENS | $160.86 | $9,640.34 | AEM×5, ABTC×140, ORBS×1438, GRAL×15, MSTR×10, TRON×640, XHG×276, AUGO×13 |
| 2026-08-24 | -5.17 | $160.86 | AEM×5, ABTC×140, ORBS×1438, GRAL×15, MSTR×10, TRON×640, XHG×276, AUGO×13 | $9,731.23 | +90.89 | -49.68 | — | AEM, ABTC, ORBS, GRAL, MSTR, TRON, AUGO | $8,524.65 | $9,645.21 | XHG×276 |
| 2026-08-25 | +1.80 | $8,524.65 | XHG×276 | $9,634.17 | -11.04 | +4.36 | DEFT, AMTX, NIQ, OMER, ERO, TRLV, FUTU | — | $32.71 | $9,601.43 | XHG×276, DEFT×1902, AMTX×654, NIQ×62, OMER×64, ERO×32, TRLV×110, FUTU×10 |
| 2026-08-26 | +2.02 | $32.71 | XHG×276, DEFT×1902, AMTX×654, NIQ×62, OMER×64, ERO×32, TRLV×110, FUTU×10 | $9,601.43 | -0.00 | +0.00 | — | — | $32.71 | $9,601.43 | XHG×276, DEFT×1902, AMTX×654, NIQ×62, OMER×64, ERO×32, TRLV×110, FUTU×10 |
| 2026-08-27 | — | $32.71 | XHG×276, DEFT×1902, AMTX×654, NIQ×62, OMER×64, ERO×32, TRLV×110, FUTU×10 | $9,655.67 | +54.24 | +0.00 | — | XHG, DEFT, AMTX, NIQ, OMER, ERO, TRLV, FUTU | $9,615.16 | $9,615.16 | — |
| 2026-08-28 | +0.75 | $9,615.16 | — | $9,615.16 | -0.00 | +44.66 | ZYME, XHG, NIQ, DEFT, OMER, ERO, TRLV, FUTU | — | $111.41 | $9,625.10 | ZYME×40, XHG×296, NIQ×63, DEFT×2003, OMER×65, ERO×30, TRLV×105, FUTU×9 |
| 2026-08-31 | -5.85 | $111.41 | ZYME×40, XHG×296, NIQ×63, DEFT×2003, OMER×65, ERO×30, TRLV×105, FUTU×9 | $9,487.99 | -137.11 | +0.53 | — | ZYME | $1,240.08 | $9,486.39 | XHG×296, NIQ×63, DEFT×2003, OMER×65, ERO×30, TRLV×105, FUTU×9 |
| 2026-09-01 | -6.30 | $1,240.08 | XHG×296, NIQ×63, DEFT×2003, OMER×65, ERO×30, TRLV×105, FUTU×9 | $9,353.33 | -133.06 | -37.34 | — | NIQ | $2,438.66 | $9,313.79 | XHG×296, DEFT×2003, OMER×65, ERO×30, TRLV×105, FUTU×9 |
| 2026-09-02 | -3.83 | $2,438.66 | XHG×296, DEFT×2003, OMER×65, ERO×30, TRLV×105, FUTU×9 | $9,312.11 | -1.68 | +57.06 | — | — | $2,438.66 | $9,369.17 | XHG×296, DEFT×2003, OMER×65, ERO×30, TRLV×105, FUTU×9 |
| 2026-09-03 | -0.90 | $2,438.66 | XHG×296, DEFT×2003, OMER×65, ERO×30, TRLV×105, FUTU×9 | $9,451.08 | +81.91 | -179.23 | NVAX, NIQ | — | $13.27 | $9,267.32 | XHG×296, DEFT×2003, OMER×65, ERO×30, TRLV×105, FUTU×9, NVAX×118, NIQ×65 |
| 2026-09-04 | — | $13.27 | XHG×296, DEFT×2003, OMER×65, ERO×30, TRLV×105, FUTU×9, NVAX×118, NIQ×65 | $9,378.09 | +110.77 | +114.71 | — | — | $13.27 | $9,492.80 | XHG×296, DEFT×2003, OMER×65, ERO×30, TRLV×105, FUTU×9, NVAX×118, NIQ×65 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 75 | $16.50 | $2.21 | — | $8,760.28 | — | baseline list, no extra gate; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANRO` | 39 | $31.77 | $2.11 | — | $7,519.15 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; ret5=+13.5; leftover $1250.00 | join🟢 sector🔴 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LIFE` | 35 | $35.04 | $2.10 | — | $6,290.65 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ⚪; ret5=+16.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `VOYG` | 28 | $44.49 | $2.07 | — | $5,042.86 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+15.6; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LUNR` | 65 | $19.17 | $2.19 | — | $3,794.62 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🔴 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BETA` | 49 | $25.21 | $2.14 | — | $2,557.20 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ⚪; ret5=+15.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `FORM` | 9 | $129.48 | $2.02 | — | $1,389.86 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+14.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ENTG` | 7 | $162.45 | $2.01 | — | $250.70 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+14.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $250.70 | ▼ close $9,881.56 vs 09:30 $10,000.00 (session -101.60) | 16:00 close · cash $250.70 · equity $9,881.56 vs 09:30 $10,000.00 (-118.44; session marks -101.60) · 8 name(s) marked open→close (per-name table). ADUR×75 09:30 $16.50 → close $16.17 -24.75; ANRO×39 09:30 $31.77 → close $32.14 +14.43; LIFE×35 09:30 $35.04 → close $34.02 -35.70; VOYG×28 09:30 $44.49 → close $42.98 -42.28; LUNR×65 09:30 $19.17 → close $19.01 -10.40; BETA×49 09:30 $25.21 → close $24.86 -17.15; FORM×9 09:30 $129.48 → close $131.60 +19.08; ENTG×7 09:30 $162.45 → close $161.76 -4.83 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $250.70 | ▲ 09:30 equity $9,917.58 vs yday $9,881.56 (+36.02) | 09:30 open · cash $250.70 (unchanged overnight, no fees) · equity $9,917.58 vs prior close $9,881.56 (+36.02) · 8 name(s) re-marked at the open (per-name table). ADUR×75 yday $16.17 → 09:30 $15.73 -33.00; ANRO×39 yday $32.14 → 09:30 $32.15 +0.39; LIFE×35 yday $34.02 → 09:30 $34.03 +0.35; VOYG×28 yday $42.98 → 09:30 $42.12 -24.08; LUNR×65 yday $19.01 → 09:30 $20.25 +80.60; BETA×49 yday $24.86 → 09:30 $24.61 -12.25; FORM×9 yday $131.60 → 09:30 $134.05 +22.05; ENTG×7 yday $161.76 → 09:30 $162.04 +1.96 | — |
| 2026-08-17 09:30 ET | **SELL** | `ADUR` | 75 | $15.73 | $2.24 | $-62.20 | $1,428.21 | ▼ -62.20 after sell → book $9,915.34; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANRO` | 39 | $32.15 | $2.13 | $+10.59 | $2,679.93 | ▲ +10.59 after sell → book $9,913.21; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LIFE` | 35 | $34.03 | $2.12 | $-39.56 | $3,868.87 | ▼ -39.56 after sell → book $9,911.10; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VOYG` | 28 | $42.12 | $2.09 | $-70.53 | $5,046.14 | ▼ -70.53 after sell → book $9,909.01; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `BETA` | 49 | $24.61 | $2.16 | $-33.69 | $6,249.87 | ▼ -33.69 after sell → book $9,906.85; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `FORM` | 9 | $134.05 | $2.04 | $+37.08 | $7,454.28 | ▲ +37.08 after sell → book $9,904.81; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ENTG` | 7 | $162.04 | $2.03 | $-6.91 | $8,586.53 | ▼ -6.91 after sell → book $9,902.78; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `OCC` | 67 | $18.24 | $2.19 | — | $7,362.26 | — | baseline list, no extra gate; list probable,ohlc_hot; ⚪; ret5=+9.5; leftover $1226.65 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ALM` | 75 | $16.20 | $2.21 | — | $6,145.04 | — | baseline list, no extra gate; list probable,ohlc_hot; 🔵; ⚪; ret5=+6.4; leftover $1226.65 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `LPTH` | 82 | $14.94 | $2.24 | — | $4,917.73 | — | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; ⚪; ret5=+16.2; leftover $1226.65 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `AAOI` | 8 | $152.64 | $2.01 | — | $3,694.59 | — | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; ⚪; ret5=+10.8; leftover $1226.65 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CLYM` | 75 | $16.25 | $2.21 | — | $2,473.63 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; ret5=+16.6; leftover $1226.65 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `BORR` | 267 | $4.59 | $3.44 | — | $1,244.66 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; ⚪; ret5=+14.8; leftover $1226.65 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `IOVA` | 179 | $6.84 | $2.53 | — | $17.77 | — | baseline list, no extra gate; list ohlc_hot; ret5=+10.1; leftover $1226.65 | join🟡 sector🔴 gen🟢 news🟢 judge🟢 vol🟡 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17.77 | ▲ close $9,949.63 vs 09:30 $9,917.58 (session +63.69) | 16:00 close · cash $17.77 · equity $9,949.63 vs 09:30 $9,917.58 (+32.05; session marks +63.69) · 8 name(s) marked open→close (per-name table). LUNR×65 09:30 $20.25 → close $20.38 +8.45; OCC×67 09:30 $18.24 → close $17.12 -75.04; ALM×75 09:30 $16.20 → close $16.36 +12.00; LPTH×82 09:30 $14.94 → close $14.80 -11.48; AAOI×8 09:30 $152.64 → close $154.89 +18.00; CLYM×75 09:30 $16.25 → close $17.44 +89.25; BORR×267 09:30 $4.59 → close $4.50 -24.03; IOVA×179 09:30 $6.84 → close $7.10 +46.54 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17.77 | ▼ 09:30 equity $9,598.26 vs yday $9,949.63 (-351.37) | 09:30 open · cash $17.77 (unchanged overnight, no fees) · equity $9,598.26 vs prior close $9,949.63 (-351.37) · 8 name(s) re-marked at the open (per-name table). LUNR×65 yday $20.38 → 09:30 $19.31 -69.55; OCC×67 yday $17.12 → 09:30 $16.20 -61.64; ALM×75 yday $16.36 → 09:30 $15.78 -43.50; LPTH×82 yday $14.80 → 09:30 $14.01 -64.78; AAOI×8 yday $154.89 → 09:30 $146.20 -69.52; CLYM×75 yday $17.44 → 09:30 $16.90 -40.50; BORR×267 yday $4.50 → 09:30 $4.56 +16.02; IOVA×179 yday $7.10 → 09:30 $7.00 -17.90 | — |
| 2026-08-18 09:30 ET | **SELL** | `LUNR` | 65 | $19.31 | $2.21 | $+4.71 | $1,270.71 | ▲ +4.71 after sell → book $9,596.05; vs 09:30 mark -2.21 | dropped from list after 2 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `OCC` | 67 | $16.20 | $2.21 | $-141.08 | $2,353.90 | ▼ -141.08 after sell → book $9,593.84; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALM` | 75 | $15.78 | $2.24 | $-35.95 | $3,535.16 | ▼ -35.95 after sell → book $9,591.60; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `LPTH` | 82 | $14.01 | $2.26 | $-80.76 | $4,681.72 | ▼ -80.76 after sell → book $9,589.34; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CLYM` | 75 | $16.90 | $2.24 | $+44.30 | $5,946.99 | ▲ +44.30 after sell → book $9,587.11; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `BORR` | 267 | $4.56 | $3.50 | $-14.95 | $7,161.01 | ▼ -14.95 after sell → book $9,583.61; vs 09:30 mark -3.50 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,161.01 | ▼ close $9,470.66 vs 09:30 $9,598.26 (session -112.95) | 16:00 close · cash $7,161.01 · equity $9,470.66 vs 09:30 $9,598.26 (-127.60; session marks -112.95) · 2 name(s) marked open→close (per-name table). AAOI×8 09:30 $146.20 → close $131.41 -118.32; IOVA×179 09:30 $7.00 → close $7.03 +5.37 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,161.01 | ▲ 09:30 equity $9,536.61 vs yday $9,470.66 (+65.95) | 09:30 open · cash $7,161.01 (unchanged overnight, no fees) · equity $9,536.61 vs prior close $9,470.66 (+65.95) · 2 name(s) re-marked at the open (per-name table). AAOI×8 yday $131.41 → 09:30 $135.85 +35.52; IOVA×179 yday $7.03 → 09:30 $7.20 +30.43 | — |
| 2026-08-19 09:30 ET | **SELL** | `AAOI` | 8 | $135.85 | $2.03 | $-138.37 | $8,245.77 | ▼ -138.37 after sell → book $9,534.57; vs 09:30 mark -2.04 | dropped from list after 2 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 peer🔴 vol🟢 buy🟡 |
| 2026-08-19 09:30 ET | **SELL** | `IOVA` | 179 | $7.20 | $2.57 | $+59.35 | $9,532.01 | ▲ +59.35 after sell → book $9,532.01; vs 09:30 mark -2.56 | dropped from list after 2 sess (min 1) | join🔴 sector🟡 gen🔴 news🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,532.01 | ▲ close $9,532.01 vs 09:30 $9,536.61 (session +0.00) | 16:00 close · cash $9,532.01 · no lots left · equity $9,532.01. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,532.01 | ▲ 09:30 equity $9,532.01 vs yday $9,532.01 (-0.00) | 09:30 open · cash $9,532.01 · no holdings · equity $9,532.01 vs prior close $9,532.01 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `AEM` | 5 | $204.45 | $2.00 | — | $8,507.75 | — | baseline list, no extra gate; list flatten,ohlc_hot,mover_buy; 🔵; ret5=+12.2; leftover $1191.50 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `TWST` | 8 | $136.84 | $2.01 | — | $7,411.02 | — | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; 🔵; ⚪; ret5=+13.7; leftover $1191.50 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABTC` | 140 | $8.46 | $2.41 | — | $6,224.21 | — | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+14.0; leftover $1191.50 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HL` | 58 | $20.25 | $2.16 | — | $5,047.54 | — | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; 🔵; ⚪; ret5=+13.5; leftover $1191.50 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SBET` | 157 | $7.55 | $2.46 | — | $3,859.73 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+14.6; leftover $1191.50 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `PPC` | 38 | $30.65 | $2.10 | — | $2,692.93 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+16.5; leftover $1191.50 | join🟡 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABCL` | 100 | $11.81 | $2.29 | — | $1,509.14 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1191.50 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SENS` | 133 | $8.91 | $2.39 | — | $321.72 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ⚪; ret5=+14.9; leftover $1191.50 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $321.72 | ▲ close $9,574.73 vs 09:30 $9,532.01 (session +60.56) | 16:00 close · cash $321.72 · equity $9,574.73 vs 09:30 $9,532.01 (+42.72; session marks +60.56) · 8 name(s) marked open→close (per-name table). AEM×5 09:30 $204.45 → close $212.04 +37.95; TWST×8 09:30 $136.84 → close $136.33 -4.08; ABTC×140 09:30 $8.46 → close $8.47 +1.40; HL×58 09:30 $20.25 → close $20.82 +33.06; SBET×157 09:30 $7.55 → close $7.59 +6.28; PPC×38 09:30 $30.65 → close $31.24 +22.42; ABCL×100 09:30 $11.81 → close $11.57 -24.50; SENS×133 09:30 $8.91 → close $8.82 -11.97 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $321.72 | ▲ 09:30 equity $9,764.65 vs yday $9,574.73 (+189.92) | 09:30 open · cash $321.72 (unchanged overnight, no fees) · equity $9,764.65 vs prior close $9,574.73 (+189.92) · 8 name(s) re-marked at the open (per-name table). AEM×5 yday $212.04 → 09:30 $216.30 +21.30; TWST×8 yday $136.33 → 09:30 $138.43 +16.80; ABTC×140 yday $8.47 → 09:30 $8.66 +26.60; HL×58 yday $20.82 → 09:30 $21.33 +29.58; SBET×157 yday $7.59 → 09:30 $7.87 +43.96; PPC×38 yday $31.24 → 09:30 $31.13 -4.18; ABCL×100 yday $11.57 → 09:30 $11.57 +0.00; SENS×133 yday $8.82 → 09:30 $9.24 +55.86 | — |
| 2026-08-21 09:30 ET | **SELL** | `TWST` | 8 | $138.43 | $2.03 | $+8.67 | $1,427.13 | ▲ +8.67 after sell → book $9,762.62; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HL` | 58 | $21.33 | $2.18 | $+58.29 | $2,662.08 | ▲ +58.29 after sell → book $9,760.43; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `SBET` | 157 | $7.87 | $2.50 | $+45.28 | $3,895.17 | ▲ +45.28 after sell → book $9,757.93; vs 09:30 mark -2.50 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `PPC` | 38 | $31.13 | $2.12 | $+14.01 | $5,075.99 | ▲ +14.01 after sell → book $9,755.81; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ABCL` | 100 | $11.57 | $2.32 | $-29.11 | $6,230.67 | ▼ -29.11 after sell → book $9,753.49; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `SENS` | 133 | $9.24 | $2.42 | $+39.08 | $7,457.17 | ▲ +39.08 after sell → book $9,751.07; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 1438 | $0.86 | $16.74 | — | $6,198.00 | — | baseline list, no extra gate; list probable,yday_gainer,ohlc_hot; ret5=+9.7; leftover $1242.86 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `GRAL` | 15 | $78.88 | $2.04 | — | $5,012.77 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+14.3; leftover $1242.86 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `MSTR` | 10 | $119.69 | $2.02 | — | $3,813.85 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; ret5=+15.7; leftover $1242.86 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `TRON` | 640 | $1.94 | $8.26 | — | $2,563.99 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; ret5=+15.4; leftover $1242.86 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `XHG` | 276 | $4.49 | $3.56 | — | $1,321.19 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+12.7; leftover $1242.86 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUGO` | 13 | $89.10 | $2.03 | — | $160.86 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+15.8; leftover $1242.86 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $160.86 | ▼ close $9,640.34 vs 09:30 $9,764.65 (session -76.09) | 16:00 close · cash $160.86 · equity $9,640.34 vs 09:30 $9,764.65 (-124.31; session marks -76.09) · 8 name(s) marked open→close (per-name table). AEM×5 09:30 $216.30 → close $216.06 -1.20; ABTC×140 09:30 $8.66 → close $7.93 -102.20; ORBS×1438 09:30 $0.86 → close $0.88 +23.01; GRAL×15 09:30 $78.88 → close $79.54 +9.90; MSTR×10 09:30 $119.69 → close $119.25 -4.40; TRON×640 09:30 $1.94 → close $2.01 +44.80; XHG×276 09:30 $4.49 → close $4.41 -22.08; AUGO×13 09:30 $89.10 → close $87.26 -23.92 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $160.86 | ▲ 09:30 equity $9,731.23 vs yday $9,640.34 (+90.89) | 09:30 open · cash $160.86 (unchanged overnight, no fees) · equity $9,731.23 vs prior close $9,640.34 (+90.89) · 8 name(s) re-marked at the open (per-name table). AEM×5 yday $216.06 → 09:30 $217.03 +4.85; ABTC×140 yday $7.93 → 09:30 $8.06 +18.20; ORBS×1438 yday $0.88 → 09:30 $0.89 +14.38; GRAL×15 yday $79.54 → 09:30 $81.87 +34.95; MSTR×10 yday $119.25 → 09:30 $121.76 +25.10; TRON×640 yday $2.01 → 09:30 $2.02 +6.40; XHG×276 yday $4.41 → 09:30 $4.24 -46.92; AUGO×13 yday $87.26 → 09:30 $89.87 +33.93 | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 5 | $217.03 | $2.02 | $+58.87 | $1,243.99 | ▲ +58.87 after sell → book $9,729.21; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ABTC` | 140 | $8.06 | $2.44 | $-60.85 | $2,369.94 | ▼ -60.85 after sell → book $9,726.76; vs 09:30 mark -2.45 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ORBS` | 1438 | $0.89 | $17.36 | $+3.29 | $3,632.40 | ▲ +3.29 after sell → book $9,709.40; vs 09:30 mark -17.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `GRAL` | 15 | $81.87 | $2.06 | $+40.76 | $4,858.40 | ▲ +40.76 after sell → book $9,707.35; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MSTR` | 10 | $121.76 | $2.04 | $+16.64 | $6,073.96 | ▲ +16.64 after sell → book $9,705.31; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `TRON` | 640 | $2.02 | $8.37 | $+34.57 | $7,358.38 | ▲ +34.57 after sell → book $9,696.93; vs 09:30 mark -8.38 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUGO` | 13 | $89.87 | $2.05 | $+5.93 | $8,524.65 | ▲ +5.93 after sell → book $9,694.89; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,524.65 | ▼ close $9,645.21 vs 09:30 $9,731.23 (session -49.68) | 16:00 close · cash $8,524.65 · equity $9,645.21 vs 09:30 $9,731.23 (-86.02; session marks -49.68) · 1 name(s) marked open→close (per-name table). XHG×276 09:30 $4.24 → close $4.06 -49.68 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,524.65 | ▼ 09:30 equity $9,634.17 vs yday $9,645.21 (-11.04) | 09:30 open · cash $8,524.65 (unchanged overnight, no fees) · equity $9,634.17 vs prior close $9,645.21 (-11.04) · 1 name(s) re-marked at the open (per-name table). XHG×276 yday $4.06 → 09:30 $4.02 -11.04 | — |
| 2026-08-25 09:30 ET | **BUY** | `DEFT` | 1902 | $0.64 | $17.88 | — | $7,289.49 | — | baseline list, no extra gate; list yday_gainer,yday_mover,ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1217.81 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AMTX` | 654 | $1.86 | $8.44 | — | $6,064.61 | — | baseline list, no extra gate; list yday_mover,ohlc_hot; ⚪; ret5=+16.9; leftover $1217.81 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟡 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NIQ` | 62 | $19.56 | $2.18 | — | $4,849.71 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+7.6; leftover $1217.81 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `OMER` | 64 | $18.75 | $2.18 | — | $3,647.53 | — | baseline list, no extra gate; list ohlc_hot; ret5=+12.1; leftover $1217.81 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ERO` | 32 | $38.00 | $2.09 | — | $2,429.45 | — | baseline list, no extra gate; list ohlc_hot,mover_buy; ⚪; ret5=+16.6; leftover $1217.81 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `TRLV` | 110 | $11.02 | $2.32 | — | $1,214.93 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+15.0; leftover $1217.81 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FUTU` | 10 | $118.02 | $2.02 | — | $32.71 | — | baseline list, no extra gate; list ohlc_hot; ⚪; ret5=+17.5; leftover $1217.81 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $32.71 | ▲ close $9,601.43 vs 09:30 $9,634.17 (session +4.36) | 16:00 close · cash $32.71 · equity $9,601.43 vs 09:30 $9,634.17 (-32.74; session marks +4.36) · 8 name(s) marked open→close (per-name table). XHG×276 09:30 $4.02 → close $4.05 +8.28; DEFT×1902 09:30 $0.64 → close $0.62 -38.04; AMTX×654 09:30 $1.86 → close $1.86 +0.00; NIQ×62 09:30 $19.56 → close $19.46 -6.20; OMER×64 09:30 $18.75 → close $19.03 +17.92; ERO×32 09:30 $38.00 → close $38.55 +17.60; TRLV×110 09:30 $11.02 → close $11.02 +0.00; FUTU×10 09:30 $118.02 → close $118.50 +4.80 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $32.71 | ▲ 09:30 equity $9,601.43 vs yday $9,601.43 (-0.00) | 09:30 open · cash $32.71 (unchanged overnight, no fees) · equity $9,601.43 vs prior close $9,601.43 (-0.00) · 8 name(s) re-marked at the open (per-name table). XHG×276 yday $4.05 → 09:30 $4.05 +0.00; DEFT×1902 yday $0.62 → 09:30 $0.62 +0.00; AMTX×654 yday $1.86 → 09:30 $1.86 +0.00; NIQ×62 yday $19.46 → 09:30 $19.46 +0.00; OMER×64 yday $19.03 → 09:30 $19.03 +0.00; ERO×32 yday $38.55 → 09:30 $38.55 +0.00; TRLV×110 yday $11.02 → 09:30 $11.02 +0.00; FUTU×10 yday $118.50 → 09:30 $118.50 +0.00 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $32.71 | ▲ close $9,601.43 vs 09:30 $9,601.43 (session +0.00) | 16:00 close · cash $32.71 · equity $9,601.43 vs 09:30 $9,601.43 (-0.00; session marks +0.00) · 8 name(s) marked open→close (per-name table). XHG×276 09:30 $4.05 → close $4.05 +0.00; DEFT×1902 09:30 $0.62 → close $0.62 +0.00; AMTX×654 09:30 $1.86 → close $1.86 +0.00; NIQ×62 09:30 $19.46 → close $19.46 +0.00; OMER×64 09:30 $19.03 → close $19.03 +0.00; ERO×32 09:30 $38.55 → close $38.55 +0.00; TRLV×110 09:30 $11.02 → close $11.02 +0.00; FUTU×10 09:30 $118.50 → close $118.50 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $32.71 | ▲ 09:30 equity $9,655.67 vs yday $9,601.43 (+54.24) | 09:30 open · cash $32.71 (unchanged overnight, no fees) · equity $9,655.67 vs prior close $9,601.43 (+54.24) · 8 name(s) re-marked at the open (per-name table). XHG×276 yday $4.05 → 09:30 $3.81 -66.24; DEFT×1902 yday $0.62 → 09:30 $0.60 -38.04; AMTX×654 yday $1.86 → 09:30 $1.91 +32.70; NIQ×62 yday $19.46 → 09:30 $19.20 -16.12; OMER×64 yday $19.03 → 09:30 $18.96 -4.48; ERO×32 yday $38.55 → 09:30 $40.51 +62.72; TRLV×110 yday $11.02 → 09:30 $11.22 +22.00; FUTU×10 yday $118.50 → 09:30 $124.67 +61.70 | — |
| 2026-08-27 09:30 ET | **SELL** | `XHG` | 276 | $3.81 | $3.62 | $-194.86 | $1,080.65 | ▼ -194.86 after sell → book $9,652.05; vs 09:30 mark -3.62 | dropped from list after 4 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `DEFT` | 1902 | $0.60 | $17.44 | $-111.40 | $2,204.41 | ▼ -111.40 after sell → book $9,634.61; vs 09:30 mark -17.44 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `AMTX` | 654 | $1.91 | $8.56 | $+15.71 | $3,444.99 | ▲ +15.71 after sell → book $9,626.05; vs 09:30 mark -8.56 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `NIQ` | 62 | $19.20 | $2.20 | $-26.69 | $4,633.19 | ▼ -26.69 after sell → book $9,623.85; vs 09:30 mark -2.20 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `OMER` | 64 | $18.96 | $2.20 | $+9.06 | $5,844.43 | ▲ +9.06 after sell → book $9,621.65; vs 09:30 mark -2.20 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ERO` | 32 | $40.51 | $2.11 | $+76.13 | $7,138.65 | ▲ +76.13 after sell → book $9,619.55; vs 09:30 mark -2.10 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `TRLV` | 110 | $11.22 | $2.35 | $+17.33 | $8,370.50 | ▲ +17.33 after sell → book $9,617.20; vs 09:30 mark -2.35 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FUTU` | 10 | $124.67 | $2.04 | $+62.44 | $9,615.16 | ▲ +62.44 after sell → book $9,615.16; vs 09:30 mark -2.04 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,615.16 | ▲ close $9,615.16 vs 09:30 $9,655.67 (session +0.00) | 16:00 close · cash $9,615.16 · no lots left · equity $9,615.16. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,615.16 | ▲ 09:30 equity $9,615.16 vs yday $9,615.16 (-0.00) | 09:30 open · cash $9,615.16 · no holdings · equity $9,615.16 vs prior close $9,615.16 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **BUY** | `ZYME` | 40 | $29.33 | $2.11 | — | $8,439.85 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot; ret5=+14.1; leftover $1201.89 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `XHG` | 296 | $4.06 | $3.82 | — | $7,234.27 | — | baseline list, no extra gate; list ohlc_hot; ret5=+16.1; leftover $1201.89 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `NIQ` | 63 | $18.79 | $2.18 | — | $6,048.32 | — | baseline list, no extra gate; list ohlc_hot; ret5=+7.6; leftover $1201.89 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `DEFT` | 2003 | $0.60 | $18.03 | — | $4,828.49 | — | baseline list, no extra gate; list ohlc_hot; ret5=+17.6; leftover $1201.89 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🟡 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `OMER` | 65 | $18.24 | $2.19 | — | $3,640.71 | — | baseline list, no extra gate; list ohlc_hot; ret5=+12.1; leftover $1201.89 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ERO` | 30 | $39.20 | $2.08 | — | $2,462.63 | — | baseline list, no extra gate; list ohlc_hot; ret5=+16.6; leftover $1201.89 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `TRLV` | 105 | $11.38 | $2.31 | — | $1,265.42 | — | baseline list, no extra gate; list ohlc_hot; ret5=+15.0; leftover $1201.89 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `FUTU` | 9 | $128.00 | $2.02 | — | $111.41 | — | baseline list, no extra gate; list ohlc_hot; ret5=+17.5; leftover $1201.89 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🔴 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $111.41 | ▲ close $9,625.10 vs 09:30 $9,615.16 (session +44.66) | 16:00 close · cash $111.41 · equity $9,625.10 vs 09:30 $9,615.16 (+9.94; session marks +44.66) · 8 name(s) marked open→close (per-name table). ZYME×40 09:30 $29.33 → close $29.01 -12.80; XHG×296 09:30 $4.06 → close $3.80 -76.96; NIQ×63 09:30 $18.79 → close $19.07 +17.64; DEFT×2003 09:30 $0.60 → close $0.65 +100.15; OMER×65 09:30 $18.24 → close $19.25 +65.65; ERO×30 09:30 $39.20 → close $39.82 +18.60; TRLV×105 09:30 $11.38 → close $11.03 -36.75; FUTU×9 09:30 $128.00 → close $124.57 -30.87 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $111.41 | ▼ 09:30 equity $9,487.99 vs yday $9,625.10 (-137.11) | 09:30 open · cash $111.41 (unchanged overnight, no fees) · equity $9,487.99 vs prior close $9,625.10 (-137.11) · 8 name(s) re-marked at the open (per-name table). ZYME×40 yday $29.01 → 09:30 $28.27 -29.60; XHG×296 yday $3.80 → 09:30 $3.44 -106.56; NIQ×63 yday $19.07 → 09:30 $19.20 +8.19; DEFT×2003 yday $0.65 → 09:30 $0.62 -60.09; OMER×65 yday $19.25 → 09:30 $18.61 -41.60; ERO×30 yday $39.82 → 09:30 $38.60 -36.60; TRLV×105 yday $11.03 → 09:30 $12.41 +144.90; FUTU×9 yday $124.57 → 09:30 $122.82 -15.75 | — |
| 2026-08-31 09:30 ET | **SELL** | `ZYME` | 40 | $28.27 | $2.13 | $-46.64 | $1,240.08 | ▼ -46.64 after sell → book $9,485.86; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,240.08 | ▲ close $9,486.39 vs 09:30 $9,487.99 (session +0.53) | 16:00 close · cash $1,240.08 · equity $9,486.39 vs 09:30 $9,487.99 (-1.60; session marks +0.53) · 7 name(s) marked open→close (per-name table). XHG×296 09:30 $3.44 → close $3.44 +0.00; NIQ×63 09:30 $19.20 → close $19.20 +0.00; DEFT×2003 09:30 $0.62 → close $0.62 +0.00; OMER×65 09:30 $18.61 → close $18.50 -7.15; ERO×30 09:30 $38.60 → close $38.49 -3.30; TRLV×105 09:30 $12.41 → close $12.41 +0.00; FUTU×9 09:30 $122.82 → close $124.04 +10.98 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,240.08 | ▼ 09:30 equity $9,353.33 vs yday $9,486.39 (-133.06) | 09:30 open · cash $1,240.08 (unchanged overnight, no fees) · equity $9,353.33 vs prior close $9,486.39 (-133.06) · 7 name(s) re-marked at the open (per-name table). XHG×296 yday $3.44 → 09:30 $3.52 +23.68; NIQ×63 yday $19.20 → 09:30 $19.06 -8.82; DEFT×2003 yday $0.62 → 09:30 $0.59 -60.09; OMER×65 yday $18.50 → 09:30 $18.79 +18.85; ERO×30 yday $38.49 → 09:30 $37.30 -35.70; TRLV×105 yday $12.41 → 09:30 $11.89 -54.60; FUTU×9 yday $124.04 → 09:30 $122.22 -16.38 | — |
| 2026-09-01 09:30 ET | **SELL** | `NIQ` | 63 | $19.06 | $2.20 | $+12.63 | $2,438.66 | ▲ +12.63 after sell → book $9,351.13; vs 09:30 mark -2.20 | dropped from list after 2 sess (min 1) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,438.66 | ▼ close $9,313.79 vs 09:30 $9,353.33 (session -37.34) | 16:00 close · cash $2,438.66 · equity $9,313.79 vs 09:30 $9,353.33 (-39.54; session marks -37.34) · 6 name(s) marked open→close (per-name table). XHG×296 09:30 $3.52 → close $3.43 -26.64; DEFT×2003 09:30 $0.59 → close $0.61 +40.06; OMER×65 09:30 $18.79 → close $18.79 +0.00; ERO×30 09:30 $37.30 → close $36.01 -38.70; TRLV×105 09:30 $11.89 → close $11.89 +0.00; FUTU×9 09:30 $122.22 → close $120.88 -12.06 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,438.66 | ▼ 09:30 equity $9,312.11 vs yday $9,313.79 (-1.68) | 09:30 open · cash $2,438.66 (unchanged overnight, no fees) · equity $9,312.11 vs prior close $9,313.79 (-1.68) · 6 name(s) re-marked at the open (per-name table). XHG×296 yday $3.43 → 09:30 $3.48 +14.80; DEFT×2003 yday $0.61 → 09:30 $0.63 +40.06; OMER×65 yday $18.79 → 09:30 $18.66 -8.45; ERO×30 yday $36.01 → 09:30 $35.95 -1.80; TRLV×105 yday $11.89 → 09:30 $11.54 -36.75; FUTU×9 yday $120.88 → 09:30 $119.82 -9.54 | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,438.66 | ▲ close $9,369.17 vs 09:30 $9,312.11 (session +57.06) | 16:00 close · cash $2,438.66 · equity $9,369.17 vs 09:30 $9,312.11 (+57.06; session marks +57.06) · 6 name(s) marked open→close (per-name table). XHG×296 09:30 $3.48 → close $3.51 +8.88; DEFT×2003 09:30 $0.63 → close $0.66 +60.09; OMER×65 09:30 $18.66 → close $18.75 +5.85; ERO×30 09:30 $35.95 → close $34.82 -33.90; TRLV×105 09:30 $11.54 → close $11.74 +21.00; FUTU×9 09:30 $119.82 → close $119.28 -4.86 | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,438.66 | ▲ 09:30 equity $9,451.08 vs yday $9,369.17 (+81.91) | 09:30 open · cash $2,438.66 (unchanged overnight, no fees) · equity $9,451.08 vs prior close $9,369.17 (+81.91) · 6 name(s) re-marked at the open (per-name table). XHG×296 yday $3.51 → 09:30 $3.57 +17.76; DEFT×2003 yday $0.66 → 09:30 $0.67 +20.03; OMER×65 yday $18.75 → 09:30 $18.97 +14.30; ERO×30 yday $34.82 → 09:30 $35.62 +24.00; TRLV×105 yday $11.74 → 09:30 $11.78 +4.20; FUTU×9 yday $119.28 → 09:30 $119.46 +1.62 | — |
| 2026-09-03 09:30 ET | **BUY** | `NVAX` | 118 | $10.27 | $2.34 | — | $1,224.45 | — | baseline list, no extra gate; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+11.1; leftover $1219.33 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `NIQ` | 65 | $18.60 | $2.19 | — | $13.27 | — | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+7.6; leftover $1219.33 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟡 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13.27 | ▼ close $9,267.32 vs 09:30 $9,451.08 (session -179.23) | 16:00 close · cash $13.27 · equity $9,267.32 vs 09:30 $9,451.08 (-183.76; session marks -179.23) · 8 name(s) marked open→close (per-name table). XHG×296 09:30 $3.57 → close $3.32 -74.00; DEFT×2003 09:30 $0.67 → close $0.65 -40.06; OMER×65 09:30 $18.97 → close $18.86 -7.15; ERO×30 09:30 $35.62 → close $34.76 -25.80; TRLV×105 09:30 $11.78 → close $11.69 -9.45; FUTU×9 09:30 $119.46 → close $118.08 -12.42; NVAX×118 09:30 $10.27 → close $10.32 +5.90; NIQ×65 09:30 $18.60 → close $18.35 -16.25 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13.27 | ▲ 09:30 equity $9,378.09 vs yday $9,267.32 (+110.77) | 09:30 open · cash $13.27 (unchanged overnight, no fees) · equity $9,378.09 vs prior close $9,267.32 (+110.77) · 8 name(s) re-marked at the open (per-name table). XHG×296 yday $3.32 → 09:30 $3.38 +17.76; DEFT×2003 yday $0.65 → 09:30 $0.65 +0.00; OMER×65 yday $18.86 → 09:30 $18.99 +8.45; ERO×30 yday $34.76 → 09:30 $35.82 +31.80; TRLV×105 yday $11.69 → 09:30 $11.89 +21.00; FUTU×9 yday $118.08 → 09:30 $118.19 +0.99; NVAX×118 yday $10.32 → 09:30 $10.41 +10.62; NIQ×65 yday $18.35 → 09:30 $18.66 +20.15 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13.27 | ▲ close $9,492.80 vs 09:30 $9,378.09 (session +114.71) | 16:00 close · cash $13.27 · equity $9,492.80 vs 09:30 $9,378.09 (+114.71; session marks +114.71) · 8 name(s) marked open→close (per-name table). XHG×296 09:30 $3.38 → close $3.43 +14.80; DEFT×2003 09:30 $0.65 → close $0.68 +60.09; OMER×65 09:30 $18.99 → close $19.11 +7.80; ERO×30 09:30 $35.82 → close $35.32 -15.00; TRLV×105 09:30 $11.89 → close $11.99 +10.50; FUTU×9 09:30 $118.19 → close $122.01 +34.38; NVAX×118 09:30 $10.41 → close $10.34 -8.26; NIQ×65 09:30 $18.66 → close $18.82 +10.40 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `ANGX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `SMTC` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MRVL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FN` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ELMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `STDN` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SENS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TRGP` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `REAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `OABI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ABCL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `XNCR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `PAYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `BKKT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `GUTS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `UEC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `NIQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OMER` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-26 | `AMTX` | no_price | no 09:30 open — carry |
| 2026-08-26 | `HOOD` | no_price | no 09:30 open |
| 2026-08-31 | `CVI` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `HOOD` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TXG` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `CVI` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HOOD` | hard_red | hard-red S=-3.83 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `XHG` | 296 | 2026-08-28 @ $4.06 | baseline list, no extra gate; list ohlc_hot; ret5=+16.1; leftover $1201.89 |
| `DEFT` | 2003 | 2026-08-28 @ $0.60 | baseline list, no extra gate; list ohlc_hot; ret5=+17.6; leftover $1201.89 |
| `OMER` | 65 | 2026-08-28 @ $18.24 | baseline list, no extra gate; list ohlc_hot; ret5=+12.1; leftover $1201.89 |
| `ERO` | 30 | 2026-08-28 @ $39.20 | baseline list, no extra gate; list ohlc_hot; ret5=+16.6; leftover $1201.89 |
| `TRLV` | 105 | 2026-08-28 @ $11.38 | baseline list, no extra gate; list ohlc_hot; ret5=+15.0; leftover $1201.89 |
| `FUTU` | 9 | 2026-08-28 @ $128.00 | baseline list, no extra gate; list ohlc_hot; ret5=+17.5; leftover $1201.89 |
| `NVAX` | 118 | 2026-09-03 @ $10.27 | baseline list, no extra gate; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+11.1; leftover $1219.33 |
| `NIQ` | 65 | 2026-09-03 @ $18.60 | baseline list, no extra gate; list ohlc_hot; 🔵; ret5=+7.6; leftover $1219.33 |
