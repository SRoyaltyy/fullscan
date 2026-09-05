# Factor mine action — `union_vol_g_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ vol_g, no 🚨

Cash book **+0.64%** ($10,064) · signal-only (no cash/fees) was +1.35%. Starts YES **7/17**. Fills 118 · skips 46 · realized $+164.92.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `vol=good` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $299.25.

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
| 2026-08-17 | `CDNL` | 30 | — | $39.85 | +0.00 | $39.23 | -18.60 | -18.60 | +0.00 | -18.60 |
| 2026-08-17 | `ABX` | 133 | — | $9.12 | +0.00 | $9.12 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-17 | `VERA` | 38 | — | $31.30 | +0.00 | $31.63 | +12.54 | +12.54 | +0.00 | +12.54 |
| 2026-08-17 | `CAPR` | 177 | — | $6.87 | +0.00 | $7.45 | +102.66 | +102.66 | +0.00 | +102.66 |
| 2026-08-17 | `HTFL` | 29 | — | $41.23 | +0.00 | $41.94 | +20.59 | +20.59 | +0.00 | +20.59 |
| 2026-08-17 | `UMAC` | 37 | — | $32.55 | +0.00 | $30.15 | -88.80 | -88.80 | +0.00 | -88.80 |
| 2026-08-17 | `NPWR` | 633 | — | $1.92 | +0.00 | $1.73 | -120.27 | -120.27 | +0.00 | -120.27 |
| 2026-08-18 | `TMC` | 300 | $3.77 | $3.72 | -15.00 | — | +0.00 | -15.00 | -99.00 | — |
| 2026-08-18 | `CDNL` | 30 | $39.23 | $41.57 | +70.20 | — | +0.00 | +70.20 | +51.60 | — |
| 2026-08-18 | `ABX` | 133 | $9.12 | $9.03 | -11.97 | — | +0.00 | -11.97 | -11.97 | — |
| 2026-08-18 | `VERA` | 38 | $31.63 | $31.31 | -12.16 | — | +0.00 | -12.16 | +0.38 | — |
| 2026-08-18 | `CAPR` | 177 | $7.45 | $7.50 | +8.85 | — | +0.00 | +8.85 | +111.51 | — |
| 2026-08-18 | `HTFL` | 29 | $41.94 | $41.50 | -12.76 | — | +0.00 | -12.76 | +7.83 | — |
| 2026-08-18 | `UMAC` | 37 | $30.15 | $28.59 | -57.72 | — | +0.00 | -57.72 | -146.52 | — |
| 2026-08-18 | `NPWR` | 633 | $1.73 | $1.70 | -18.99 | — | +0.00 | -18.99 | -139.26 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `AG` | 57 | — | $20.55 | +0.00 | $21.19 | +36.48 | +36.48 | +0.00 | +36.48 |
| 2026-08-20 | `BHP` | 12 | — | $91.01 | +0.00 | $93.63 | +31.44 | +31.44 | +0.00 | +31.44 |
| 2026-08-20 | `CDE` | 57 | — | $20.65 | +0.00 | $21.11 | +26.22 | +26.22 | +0.00 | +26.22 |
| 2026-08-20 | `HDSN` | 204 | — | $5.77 | +0.00 | $5.57 | -40.80 | -40.80 | +0.00 | -40.80 |
| 2026-08-20 | `IAG` | 60 | — | $19.63 | +0.00 | $20.50 | +52.20 | +52.20 | +0.00 | +52.20 |
| 2026-08-20 | `KGC` | 39 | — | $29.63 | +0.00 | $31.43 | +70.20 | +70.20 | +0.00 | +70.20 |
| 2026-08-20 | `NFGC` | 675 | — | $1.75 | +0.00 | $1.75 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `WPM` | 8 | — | $144.54 | +0.00 | $150.25 | +45.68 | +45.68 | +0.00 | +45.68 |
| 2026-08-21 | `AG` | 57 | $21.19 | $21.90 | +40.47 | — | +0.00 | +40.47 | +76.95 | — |
| 2026-08-21 | `BHP` | 12 | $93.63 | $95.72 | +25.08 | — | +0.00 | +25.08 | +56.52 | — |
| 2026-08-21 | `CDE` | 57 | $21.11 | $21.75 | +36.48 | — | +0.00 | +36.48 | +62.70 | — |
| 2026-08-21 | `HDSN` | 204 | $5.57 | $5.67 | +20.40 | — | +0.00 | +20.40 | -20.40 | — |
| 2026-08-21 | `IAG` | 60 | $20.50 | $21.17 | +40.20 | — | +0.00 | +40.20 | +92.40 | — |
| 2026-08-21 | `KGC` | 39 | $31.43 | $32.17 | +28.86 | — | +0.00 | +28.86 | +99.06 | — |
| 2026-08-21 | `NFGC` | 675 | $1.75 | $1.79 | +27.00 | — | +0.00 | +27.00 | +27.00 | — |
| 2026-08-21 | `WPM` | 8 | $150.25 | $154.70 | +35.60 | — | +0.00 | +35.60 | +81.28 | — |
| 2026-08-21 | `AU` | 10 | — | $119.43 | +0.00 | $121.22 | +17.90 | +17.90 | +0.00 | +17.90 |
| 2026-08-21 | `AUPH` | 71 | — | $17.20 | +0.00 | $16.65 | -39.05 | -39.05 | +0.00 | -39.05 |
| 2026-08-21 | `AEM` | 5 | — | $216.30 | +0.00 | $216.06 | -1.20 | -1.20 | +0.00 | -1.20 |
| 2026-08-21 | `ARCT` | 111 | — | $11.13 | +0.00 | $13.45 | +257.52 | +257.52 | +0.00 | +257.52 |
| 2026-08-21 | `AUTL` | 500 | — | $2.47 | +0.00 | $2.41 | -30.00 | -30.00 | +0.00 | -30.00 |
| 2026-08-21 | `CRDL` | 640 | — | $1.93 | +0.00 | $1.86 | -44.80 | -44.80 | +0.00 | -44.80 |
| 2026-08-21 | `CRSP` | 20 | — | $59.72 | +0.00 | $59.50 | -4.40 | -4.40 | +0.00 | -4.40 |
| 2026-08-21 | `CYPH` | 936 | — | $1.32 | +0.00 | $1.42 | +93.60 | +93.60 | +0.00 | +93.60 |
| 2026-08-24 | `AU` | 10 | $121.22 | $120.50 | -7.20 | — | +0.00 | -7.20 | +10.70 | — |
| 2026-08-24 | `AUPH` | 71 | $16.65 | $16.60 | -3.55 | — | +0.00 | -3.55 | -42.60 | — |
| 2026-08-24 | `AEM` | 5 | $216.06 | $217.03 | +4.85 | — | +0.00 | +4.85 | +3.65 | — |
| 2026-08-24 | `ARCT` | 111 | $13.45 | $13.26 | -21.09 | — | +0.00 | -21.09 | +236.43 | — |
| 2026-08-24 | `AUTL` | 500 | $2.41 | $2.36 | -25.00 | — | +0.00 | -25.00 | -55.00 | — |
| 2026-08-24 | `CRDL` | 640 | $1.86 | $1.87 | +6.40 | — | +0.00 | +6.40 | -38.40 | — |
| 2026-08-24 | `CRSP` | 20 | $59.50 | $58.79 | -14.20 | — | +0.00 | -14.20 | -18.60 | — |
| 2026-08-24 | `CYPH` | 936 | $1.42 | $1.83 | +383.76 | — | +0.00 | +383.76 | +477.36 | — |
| 2026-08-25 | `BMEA` | 801 | — | $1.62 | +0.00 | $1.61 | -8.01 | -8.01 | +0.00 | -8.01 |
| 2026-08-25 | `NPWR` | 648 | — | $2.00 | +0.00 | $2.02 | +12.96 | +12.96 | +0.00 | +12.96 |
| 2026-08-25 | `PUSA` | 350 | — | $3.70 | +0.00 | $3.91 | +73.50 | +73.50 | +0.00 | +73.50 |
| 2026-08-25 | `ALVO` | 248 | — | $5.22 | +0.00 | $5.25 | +7.44 | +7.44 | +0.00 | +7.44 |
| 2026-08-25 | `CAPR` | 191 | — | $6.79 | +0.00 | $7.19 | +76.40 | +76.40 | +0.00 | +76.40 |
| 2026-08-25 | `ZURA` | 203 | — | $6.38 | +0.00 | $6.50 | +24.36 | +24.36 | +0.00 | +24.36 |
| 2026-08-25 | `SUJA` | 147 | — | $8.79 | +0.00 | $8.54 | -36.75 | -36.75 | +0.00 | -36.75 |
| 2026-08-25 | `CYPH` | 748 | — | $1.70 | +0.00 | $1.64 | -44.88 | -44.88 | +0.00 | -44.88 |
| 2026-08-26 | `BMEA` | 801 | $1.61 | $1.61 | +0.00 | $1.61 | +0.00 | +0.00 | -8.01 | -8.01 |
| 2026-08-26 | `NPWR` | 648 | $2.02 | $2.02 | +0.00 | $2.02 | +0.00 | +0.00 | +12.96 | +12.96 |
| 2026-08-26 | `PUSA` | 350 | $3.91 | $3.91 | +0.00 | $3.91 | +0.00 | +0.00 | +73.50 | +73.50 |
| 2026-08-26 | `ALVO` | 248 | $5.25 | $5.25 | +0.00 | $5.25 | +0.00 | +0.00 | +7.44 | +7.44 |
| 2026-08-26 | `CAPR` | 191 | $7.19 | $7.19 | +0.00 | $7.19 | +0.00 | +0.00 | +76.40 | +76.40 |
| 2026-08-26 | `ZURA` | 203 | $6.50 | $6.50 | +0.00 | $6.50 | +0.00 | +0.00 | +24.36 | +24.36 |
| 2026-08-26 | `SUJA` | 147 | $8.54 | $8.54 | +0.00 | $8.54 | +0.00 | +0.00 | -36.75 | -36.75 |
| 2026-08-26 | `CYPH` | 748 | $1.64 | $1.64 | +0.00 | $1.64 | +0.00 | +0.00 | -44.88 | -44.88 |
| 2026-08-27 | `BMEA` | 801 | $1.61 | $1.75 | +112.14 | — | +0.00 | +112.14 | +104.13 | — |
| 2026-08-27 | `NPWR` | 648 | $2.02 | $1.93 | -58.32 | — | +0.00 | -58.32 | -45.36 | — |
| 2026-08-27 | `PUSA` | 350 | $3.91 | $3.84 | -24.50 | — | +0.00 | -24.50 | +49.00 | — |
| 2026-08-27 | `ALVO` | 248 | $5.25 | $4.98 | -66.96 | — | +0.00 | -66.96 | -59.52 | — |
| 2026-08-27 | `CAPR` | 191 | $7.19 | $8.29 | +210.10 | — | +0.00 | +210.10 | +286.50 | — |
| 2026-08-27 | `ZURA` | 203 | $6.50 | $6.13 | -75.11 | — | +0.00 | -75.11 | -50.75 | — |
| 2026-08-27 | `SUJA` | 147 | $8.54 | $9.39 | +124.95 | — | +0.00 | +124.95 | +88.20 | — |
| 2026-08-27 | `CYPH` | 748 | $1.64 | $1.60 | -29.92 | — | +0.00 | -29.92 | -74.80 | — |
| 2026-08-28 | `ANF` | 9 | — | $144.70 | +0.00 | $145.75 | +9.45 | +9.45 | +0.00 | +9.45 |
| 2026-08-28 | `BHVN` | 78 | — | $16.95 | +0.00 | $16.12 | -64.74 | -64.74 | +0.00 | -64.74 |
| 2026-08-28 | `BZ` | 71 | — | $18.50 | +0.00 | $18.00 | -35.50 | -35.50 | +0.00 | -35.50 |
| 2026-08-28 | `CAPR` | 144 | — | $9.19 | +0.00 | $10.06 | +125.28 | +125.28 | +0.00 | +125.28 |
| 2026-08-28 | `SEDG` | 39 | — | $33.78 | +0.00 | $33.51 | -10.53 | -10.53 | +0.00 | -10.53 |
| 2026-08-28 | `SMTC` | 8 | — | $149.40 | +0.00 | $142.43 | -55.76 | -55.76 | +0.00 | -55.76 |
| 2026-08-28 | `URBN` | 16 | — | $82.70 | +0.00 | $78.79 | -62.56 | -62.56 | +0.00 | -62.56 |
| 2026-08-28 | `ERAS` | 68 | — | $19.30 | +0.00 | $19.49 | +12.92 | +12.92 | +0.00 | +12.92 |
| 2026-08-31 | `ANF` | 9 | $145.75 | $148.67 | +26.28 | — | +0.00 | +26.28 | +35.73 | — |
| 2026-08-31 | `BHVN` | 78 | $16.12 | $15.44 | -53.04 | — | +0.00 | -53.04 | -117.78 | — |
| 2026-08-31 | `BZ` | 71 | $18.00 | $17.89 | -7.81 | — | +0.00 | -7.81 | -43.31 | — |
| 2026-08-31 | `CAPR` | 144 | $10.06 | $9.44 | -89.28 | — | +0.00 | -89.28 | +36.00 | — |
| 2026-08-31 | `SEDG` | 39 | $33.51 | $31.50 | -78.39 | — | +0.00 | -78.39 | -88.92 | — |
| 2026-08-31 | `SMTC` | 8 | $142.43 | $133.04 | -75.12 | — | +0.00 | -75.12 | -130.88 | — |
| 2026-08-31 | `URBN` | 16 | $78.79 | $81.09 | +36.80 | — | +0.00 | +36.80 | -25.76 | — |
| 2026-08-31 | `ERAS` | 68 | $19.49 | $17.90 | -108.12 | — | +0.00 | -108.12 | -95.20 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `RVTY` | 10 | — | $125.94 | +0.00 | $130.94 | +50.00 | +50.00 | +0.00 | +50.00 |
| 2026-09-03 | `GPRO` | 1037 | — | $1.22 | +0.00 | $1.69 | +487.39 | +487.39 | +0.00 | +487.39 |
| 2026-09-03 | `FRVO` | 68 | — | $18.40 | +0.00 | $17.98 | -28.56 | -28.56 | +0.00 | -28.56 |
| 2026-09-03 | `CRK` | 80 | — | $15.70 | +0.00 | $15.54 | -12.80 | -12.80 | +0.00 | -12.80 |
| 2026-09-03 | `MMED` | 55 | — | $22.78 | +0.00 | $23.76 | +53.90 | +53.90 | +0.00 | +53.90 |
| 2026-09-03 | `CTMX` | 340 | — | $3.72 | +0.00 | $3.72 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-09-03 | `EIX` | 22 | — | $56.78 | +0.00 | $55.19 | -34.98 | -34.98 | +0.00 | -34.98 |
| 2026-09-03 | `CRDL` | 586 | — | $2.16 | +0.00 | $2.17 | +5.86 | +5.86 | +0.00 | +5.86 |
| 2026-09-04 | `RVTY` | 10 | $130.94 | $132.45 | +15.10 | — | +0.00 | +15.10 | +65.10 | — |
| 2026-09-04 | `GPRO` | 1037 | $1.69 | $1.78 | +93.33 | $1.39 | -404.43 | -311.10 | +580.72 | +176.29 |
| 2026-09-04 | `FRVO` | 68 | $17.98 | $18.27 | +19.72 | — | +0.00 | +19.72 | -8.84 | — |
| 2026-09-04 | `CRK` | 80 | $15.54 | $15.45 | -7.20 | — | +0.00 | -7.20 | -20.00 | — |
| 2026-09-04 | `MMED` | 55 | $23.76 | $23.88 | +6.60 | — | +0.00 | +6.60 | +60.50 | — |
| 2026-09-04 | `CTMX` | 340 | $3.72 | $3.73 | +3.40 | — | +0.00 | +3.40 | +3.40 | — |
| 2026-09-04 | `EIX` | 22 | $55.19 | $55.42 | +5.06 | — | +0.00 | +5.06 | -29.92 | — |
| 2026-09-04 | `CRDL` | 586 | $2.17 | $2.18 | +5.86 | — | +0.00 | +5.86 | +11.72 | — |
| 2026-09-04 | `CABA` | 349 | — | $3.63 | +0.00 | $3.48 | -52.35 | -52.35 | +0.00 | -52.35 |
| 2026-09-04 | `BAK` | 651 | — | $1.95 | +0.00 | $1.94 | -6.51 | -6.51 | +0.00 | -6.51 |
| 2026-09-04 | `EOSE` | 355 | — | $3.57 | +0.00 | $3.50 | -24.85 | -24.85 | +0.00 | -24.85 |
| 2026-09-04 | `DELL` | 2 | — | $486.31 | +0.00 | $516.39 | +60.16 | +60.16 | +0.00 | +60.16 |
| 2026-09-04 | `MLYS` | 43 | — | $29.15 | +0.00 | $28.27 | -37.84 | -37.84 | +0.00 | -37.84 |
| 2026-09-04 | `CCOI` | 124 | — | $10.22 | +0.00 | $9.98 | -29.76 | -29.76 | +0.00 | -29.76 |
| 2026-09-04 | `SGLD` | 195 | — | $6.48 | +0.00 | $5.73 | -146.25 | -146.25 | +0.00 | -146.25 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | -168.89 | BTBT, BETR, ANGX, HYLN, ADUR, ARX, AIRO, NCMI | — | $10.28 | $9,797.82 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, ARX×63, AIRO×112, NCMI×464 |
| 2026-08-17 | +2.25 | $10.28 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, ARX×63, AIRO×112, NCMI×464 | $9,768.32 | -29.50 | -175.88 | TMC, CDNL, ABX, VERA, CAPR, HTFL, UMAC, NPWR | BTBT, BETR, ANGX, HYLN, ADUR, ARX, AIRO, NCMI | $65.04 | $9,533.39 | TMC×300, CDNL×30, ABX×133, VERA×38, CAPR×177, HTFL×29, UMAC×37, NPWR×633 |
| 2026-08-18 | -6.20 | $65.04 | TMC×300, CDNL×30, ABX×133, VERA×38, CAPR×177, HTFL×29, UMAC×37, NPWR×633 | $9,483.84 | -49.55 | +0.00 | — | TMC, CDNL, ABX, VERA, CAPR, HTFL, UMAC, NPWR | $9,458.21 | $9,458.21 | — |
| 2026-08-19 | -7.20 | $9,458.21 | — | $9,458.21 | -0.00 | +0.00 | — | — | $9,458.21 | $9,458.21 | — |
| 2026-08-20 | +1.12 | $9,458.21 | — | $9,458.21 | -0.00 | +221.42 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | — | $145.69 | $9,655.65 | AG×57, BHP×12, CDE×57, HDSN×204, IAG×60, KGC×39, NFGC×675, WPM×8 |
| 2026-08-21 | +3.25 | $145.69 | AG×57, BHP×12, CDE×57, HDSN×204, IAG×60, KGC×39, NFGC×675, WPM×8 | $9,909.74 | +254.09 | +249.57 | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $215.54 | $10,097.66 | AU×10, AUPH×71, AEM×5, ARCT×111, AUTL×500, CRDL×640, CRSP×20, CYPH×936 |
| 2026-08-24 | -5.17 | $215.54 | AU×10, AUPH×71, AEM×5, ARCT×111, AUTL×500, CRDL×640, CRSP×20, CYPH×936 | $10,421.63 | +323.97 | +0.00 | — | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | $10,383.76 | $10,383.76 | — |
| 2026-08-25 | +1.80 | $10,383.76 | — | $10,383.76 | -0.00 | +105.02 | BMEA, NPWR, PUSA, ALVO, CAPR, ZURA, SUJA, CYPH | — | $1.15 | $10,445.11 | BMEA×801, NPWR×648, PUSA×350, ALVO×248, CAPR×191, ZURA×203, SUJA×147, CYPH×748 |
| 2026-08-26 | +2.02 | $1.15 | BMEA×801, NPWR×648, PUSA×350, ALVO×248, CAPR×191, ZURA×203, SUJA×147, CYPH×748 | $10,445.11 | +0.00 | +0.00 | — | — | $1.15 | $10,445.11 | BMEA×801, NPWR×648, PUSA×350, ALVO×248, CAPR×191, ZURA×203, SUJA×147, CYPH×748 |
| 2026-08-27 | — | $1.15 | BMEA×801, NPWR×648, PUSA×350, ALVO×248, CAPR×191, ZURA×203, SUJA×147, CYPH×748 | $10,637.49 | +192.38 | +0.00 | — | BMEA, NPWR, PUSA, ALVO, CAPR, ZURA, SUJA, CYPH | $10,593.18 | $10,593.18 | — |
| 2026-08-28 | +0.75 | $10,593.18 | — | $10,593.18 | +0.00 | -81.44 | ANF, BHVN, BZ, CAPR, SEDG, SMTC, URBN, ERAS | — | $166.48 | $10,494.52 | ANF×9, BHVN×78, BZ×71, CAPR×144, SEDG×39, SMTC×8, URBN×16, ERAS×68 |
| 2026-08-31 | -5.85 | $166.48 | ANF×9, BHVN×78, BZ×71, CAPR×144, SEDG×39, SMTC×8, URBN×16, ERAS×68 | $10,145.84 | -348.68 | +0.00 | — | ANF, BHVN, BZ, CAPR, SEDG, SMTC, URBN, ERAS | $10,128.44 | $10,128.44 | — |
| 2026-09-01 | -6.30 | $10,128.44 | — | $10,128.44 | +0.00 | +0.00 | — | — | $10,128.44 | $10,128.44 | — |
| 2026-09-02 | -3.83 | $10,128.44 | — | $10,128.44 | +0.00 | +0.00 | — | — | $10,128.44 | $10,128.44 | — |
| 2026-09-03 | -0.90 | $10,128.44 | — | $10,128.44 | +0.00 | +520.81 | RVTY, GPRO, FRVO, CRK, MMED, CTMX, EIX, CRDL | — | $28.11 | $10,613.28 | RVTY×10, GPRO×1037, FRVO×68, CRK×80, MMED×55, CTMX×340, EIX×22, CRDL×586 |
| 2026-09-04 | — | $28.11 | RVTY×10, GPRO×1037, FRVO×68, CRK×80, MMED×55, CTMX×340, EIX×22, CRDL×586 | $10,755.15 | +141.87 | -641.83 | CABA, BAK, EOSE, DELL, MLYS, CCOI, SGLD | RVTY, FRVO, CRK, MMED, CTMX, EIX, CRDL | $299.25 | $10,063.90 | GPRO×1037, CABA×349, BAK×651, EOSE×355, DELL×2, MLYS×43, CCOI×124, SGLD×195 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 833 | $1.50 | $10.75 | — | $8,739.75 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BETR` | 84 | $14.80 | $2.24 | — | $7,494.31 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten; 🔵; ⚪; ret5=-9.9; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 290 | $4.31 | $3.74 | — | $6,240.67 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 299 | $4.18 | $3.86 | — | $4,986.99 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 75 | $16.50 | $2.21 | — | $3,747.28 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 63 | $19.57 | $2.18 | — | $2,512.19 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 112 | $11.12 | $2.33 | — | $1,264.42 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NCMI` | 464 | $2.69 | $5.99 | — | $10.28 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=-33.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
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
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 300 | $4.05 | $3.87 | — | $8,515.71 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten; 🔵; ⚪; ret5=-12.3; leftover $1216.82 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CDNL` | 30 | $39.85 | $2.08 | — | $7,318.13 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer; ⚪; ret5=-38.4; leftover $1216.82 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 133 | $9.12 | $2.39 | — | $6,102.78 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $1216.82 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `VERA` | 38 | $31.30 | $2.10 | — | $4,911.27 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer; ret5=-3.8; leftover $1216.82 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 177 | $6.87 | $2.52 | — | $3,692.76 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; ret5=+62.6; leftover $1216.82 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 29 | $41.23 | $2.08 | — | $2,495.02 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; ret5=+46.0; leftover $1216.82 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `UMAC` | 37 | $32.55 | $2.10 | — | $1,288.57 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; ⚪; ret5=+30.4; leftover $1216.82 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `NPWR` | 633 | $1.92 | $8.17 | — | $65.04 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $1216.82 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $65.04 | ▼ close $9,533.39 vs 09:30 $9,768.32 (session -175.88) | 16:00 close · cash $65.04 · equity $9,533.39 vs 09:30 $9,768.32 (-234.93; session marks -175.88) · 8 name(s) marked open→close (per-name table). TMC×300 09:30 $4.05 → close $3.77 -84.00; CDNL×30 09:30 $39.85 → close $39.23 -18.60; ABX×133 09:30 $9.12 → close $9.12 +0.00; VERA×38 09:30 $31.30 → close $31.63 +12.54; CAPR×177 09:30 $6.87 → close $7.45 +102.66; HTFL×29 09:30 $41.23 → close $41.94 +20.59; UMAC×37 09:30 $32.55 → close $30.15 -88.80; NPWR×633 09:30 $1.92 → close $1.73 -120.27 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $65.04 | ▼ 09:30 equity $9,483.84 vs yday $9,533.39 (-49.55) | 09:30 open · cash $65.04 (unchanged overnight, no fees) · equity $9,483.84 vs prior close $9,533.39 (-49.55) · 8 name(s) re-marked at the open (per-name table). TMC×300 yday $3.77 → 09:30 $3.72 -15.00; CDNL×30 yday $39.23 → 09:30 $41.57 +70.20; ABX×133 yday $9.12 → 09:30 $9.03 -11.97; VERA×38 yday $31.63 → 09:30 $31.31 -12.16; CAPR×177 yday $7.45 → 09:30 $7.50 +8.85; HTFL×29 yday $41.94 → 09:30 $41.50 -12.76; UMAC×37 yday $30.15 → 09:30 $28.59 -57.72; NPWR×633 yday $1.73 → 09:30 $1.70 -18.99 | — |
| 2026-08-18 09:30 ET | **SELL** | `TMC` | 300 | $3.72 | $3.93 | $-106.80 | $1,177.11 | ▼ -106.80 after sell → book $9,479.91; vs 09:30 mark -3.93 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CDNL` | 30 | $41.57 | $2.10 | $+47.42 | $2,422.11 | ▲ +47.42 after sell → book $9,477.81; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 133 | $9.03 | $2.42 | $-16.78 | $3,620.68 | ▼ -16.78 after sell → book $9,475.39; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `VERA` | 38 | $31.31 | $2.12 | $-3.85 | $4,808.34 | ▼ -3.85 after sell → book $9,473.27; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CAPR` | 177 | $7.50 | $2.56 | $+106.43 | $6,133.27 | ▲ +106.43 after sell → book $9,470.70; vs 09:30 mark -2.57 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 29 | $41.50 | $2.10 | $+3.66 | $7,334.68 | ▲ +3.66 after sell → book $9,468.61; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `UMAC` | 37 | $28.59 | $2.12 | $-150.74 | $8,390.39 | ▼ -150.74 after sell → book $9,466.49; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `NPWR` | 633 | $1.70 | $8.28 | $-155.71 | $9,458.21 | ▼ -155.71 after sell → book $9,458.21; vs 09:30 mark -8.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,458.21 | ▲ close $9,458.21 vs 09:30 $9,483.84 (session +0.00) | 16:00 close · cash $9,458.21 · no lots left · equity $9,458.21. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,458.21 | ▲ 09:30 equity $9,458.21 vs yday $9,458.21 (-0.00) | 09:30 open · cash $9,458.21 · no holdings · equity $9,458.21 vs prior close $9,458.21 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,458.21 | ▲ close $9,458.21 vs 09:30 $9,458.21 (session +0.00) | 16:00 close · cash $9,458.21 · no lots left · equity $9,458.21. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,458.21 | ▲ 09:30 equity $9,458.21 vs yday $9,458.21 (-0.00) | 09:30 open · cash $9,458.21 · no holdings · equity $9,458.21 vs prior close $9,458.21 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 57 | $20.55 | $2.16 | — | $8,284.69 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1182.28 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 12 | $91.01 | $2.03 | — | $7,190.55 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1182.28 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 57 | $20.65 | $2.16 | — | $6,011.34 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1182.28 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 204 | $5.77 | $2.63 | — | $4,831.63 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1182.28 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 60 | $19.63 | $2.17 | — | $3,651.66 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1182.28 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 39 | $29.63 | $2.11 | — | $2,493.98 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1182.28 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 675 | $1.75 | $8.71 | — | $1,304.02 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1182.28 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $145.69 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1182.28 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $145.69 | ▲ close $9,655.65 vs 09:30 $9,458.21 (session +221.42) | 16:00 close · cash $145.69 · equity $9,655.65 vs 09:30 $9,458.21 (+197.44; session marks +221.42) · 8 name(s) marked open→close (per-name table). AG×57 09:30 $20.55 → close $21.19 +36.48; BHP×12 09:30 $91.01 → close $93.63 +31.44; CDE×57 09:30 $20.65 → close $21.11 +26.22; HDSN×204 09:30 $5.77 → close $5.57 -40.80; IAG×60 09:30 $19.63 → close $20.50 +52.20; KGC×39 09:30 $29.63 → close $31.43 +70.20; NFGC×675 09:30 $1.75 → close $1.75 +0.00; WPM×8 09:30 $144.54 → close $150.25 +45.68 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $145.69 | ▲ 09:30 equity $9,909.74 vs yday $9,655.65 (+254.09) | 09:30 open · cash $145.69 (unchanged overnight, no fees) · equity $9,909.74 vs prior close $9,655.65 (+254.09) · 8 name(s) re-marked at the open (per-name table). AG×57 yday $21.19 → 09:30 $21.90 +40.47; BHP×12 yday $93.63 → 09:30 $95.72 +25.08; CDE×57 yday $21.11 → 09:30 $21.75 +36.48; HDSN×204 yday $5.57 → 09:30 $5.67 +20.40; IAG×60 yday $20.50 → 09:30 $21.17 +40.20; KGC×39 yday $31.43 → 09:30 $32.17 +28.86; NFGC×675 yday $1.75 → 09:30 $1.79 +27.00; WPM×8 yday $150.25 → 09:30 $154.70 +35.60 | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 57 | $21.90 | $2.18 | $+72.61 | $1,391.81 | ▲ +72.61 after sell → book $9,907.56; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 12 | $95.72 | $2.05 | $+52.45 | $2,538.40 | ▲ +52.45 after sell → book $9,905.51; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 57 | $21.75 | $2.18 | $+58.36 | $3,775.97 | ▲ +58.36 after sell → book $9,903.33; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 204 | $5.67 | $2.68 | $-25.71 | $4,929.97 | ▼ -25.71 after sell → book $9,900.65; vs 09:30 mark -2.68 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 60 | $21.17 | $2.19 | $+88.04 | $6,197.98 | ▲ +88.04 after sell → book $9,898.46; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 39 | $32.17 | $2.13 | $+94.83 | $7,450.49 | ▲ +94.83 after sell → book $9,896.34; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 675 | $1.79 | $8.83 | $+9.46 | $8,649.91 | ▲ +9.46 after sell → book $9,887.51; vs 09:30 mark -8.83 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 8 | $154.70 | $2.03 | $+77.23 | $9,885.47 | ▲ +77.23 after sell → book $9,885.47; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $8,689.15 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+20.4; leftover $1235.68 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 71 | $17.20 | $2.20 | — | $7,465.75 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $1235.68 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 5 | $216.30 | $2.00 | — | $6,382.24 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1235.68 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 111 | $11.13 | $2.32 | — | $5,144.49 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1235.68 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 500 | $2.47 | $6.45 | — | $3,903.04 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $1235.68 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 640 | $1.93 | $8.26 | — | $2,659.59 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $1235.68 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 20 | $59.72 | $2.05 | — | $1,463.14 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1235.68 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 936 | $1.32 | $12.07 | — | $215.54 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1235.68 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $215.54 | ▲ close $10,097.66 vs 09:30 $9,909.74 (session +249.57) | 16:00 close · cash $215.54 · equity $10,097.66 vs 09:30 $9,909.74 (+187.92; session marks +249.57) · 8 name(s) marked open→close (per-name table). AU×10 09:30 $119.43 → close $121.22 +17.90; AUPH×71 09:30 $17.20 → close $16.65 -39.05; AEM×5 09:30 $216.30 → close $216.06 -1.20; ARCT×111 09:30 $11.13 → close $13.45 +257.52; AUTL×500 09:30 $2.47 → close $2.41 -30.00; CRDL×640 09:30 $1.93 → close $1.86 -44.80; CRSP×20 09:30 $59.72 → close $59.50 -4.40; CYPH×936 09:30 $1.32 → close $1.42 +93.60 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $215.54 | ▲ 09:30 equity $10,421.63 vs yday $10,097.66 (+323.97) | 09:30 open · cash $215.54 (unchanged overnight, no fees) · equity $10,421.63 vs prior close $10,097.66 (+323.97) · 8 name(s) re-marked at the open (per-name table). AU×10 yday $121.22 → 09:30 $120.50 -7.20; AUPH×71 yday $16.65 → 09:30 $16.60 -3.55; AEM×5 yday $216.06 → 09:30 $217.03 +4.85; ARCT×111 yday $13.45 → 09:30 $13.26 -21.09; AUTL×500 yday $2.41 → 09:30 $2.36 -25.00; CRDL×640 yday $1.86 → 09:30 $1.87 +6.40; CRSP×20 yday $59.50 → 09:30 $58.79 -14.20; CYPH×936 yday $1.42 → 09:30 $1.83 +383.76 | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.50 | $2.04 | $+6.64 | $1,418.50 | ▲ +6.64 after sell → book $10,419.59; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 71 | $16.60 | $2.22 | $-47.03 | $2,594.88 | ▼ -47.03 after sell → book $10,417.37; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 5 | $217.03 | $2.02 | $-0.38 | $3,678.00 | ▼ -0.38 after sell → book $10,415.34; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 111 | $13.26 | $2.35 | $+231.75 | $5,147.51 | ▲ +231.75 after sell → book $10,412.99; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 500 | $2.36 | $6.54 | $-67.99 | $6,320.97 | ▼ -67.99 after sell → book $10,406.45; vs 09:30 mark -6.54 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRDL` | 640 | $1.87 | $8.37 | $-55.03 | $7,509.39 | ▼ -55.03 after sell → book $10,398.07; vs 09:30 mark -8.38 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 20 | $58.79 | $2.07 | $-22.72 | $8,683.12 | ▼ -22.72 after sell → book $10,396.00; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 936 | $1.83 | $12.24 | $+453.04 | $10,383.76 | ▲ +453.04 after sell → book $10,383.76; vs 09:30 mark -12.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,383.76 | ▲ close $10,383.76 vs 09:30 $10,421.63 (session +0.00) | 16:00 close · cash $10,383.76 · no lots left · equity $10,383.76. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,383.76 | ▲ 09:30 equity $10,383.76 vs yday $10,383.76 (-0.00) | 09:30 open · cash $10,383.76 · no holdings · equity $10,383.76 vs prior close $10,383.76 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 801 | $1.62 | $10.33 | — | $9,075.81 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $1297.97 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 648 | $2.00 | $8.36 | — | $7,771.45 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.8; leftover $1297.97 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `PUSA` | 350 | $3.70 | $4.51 | — | $6,471.93 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.8; leftover $1297.97 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 248 | $5.22 | $3.20 | — | $5,174.17 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+9.2; leftover $1297.97 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 191 | $6.79 | $2.56 | — | $3,874.72 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.4; leftover $1297.97 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 203 | $6.38 | $2.62 | — | $2,576.96 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer; 🔵; ⚪; ret5=+5.0; leftover $1297.97 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SUJA` | 147 | $8.79 | $2.43 | — | $1,282.40 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.5; leftover $1297.97 | join🟡 sector🟡 gen🟡 news🟡 digest🟡 ab🟡 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 748 | $1.70 | $9.65 | — | $1.15 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+120.5; leftover $1297.97 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.15 | ▲ close $10,445.11 vs 09:30 $10,383.76 (session +105.02) | 16:00 close · cash $1.15 · equity $10,445.11 vs 09:30 $10,383.76 (+61.35; session marks +105.02) · 8 name(s) marked open→close (per-name table). BMEA×801 09:30 $1.62 → close $1.61 -8.01; NPWR×648 09:30 $2.00 → close $2.02 +12.96; PUSA×350 09:30 $3.70 → close $3.91 +73.50; ALVO×248 09:30 $5.22 → close $5.25 +7.44; CAPR×191 09:30 $6.79 → close $7.19 +76.40; ZURA×203 09:30 $6.38 → close $6.50 +24.36; SUJA×147 09:30 $8.79 → close $8.54 -36.75; CYPH×748 09:30 $1.70 → close $1.64 -44.88 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.15 | ▲ 09:30 equity $10,445.11 vs yday $10,445.11 (+0.00) | 09:30 open · cash $1.15 (unchanged overnight, no fees) · equity $10,445.11 vs prior close $10,445.11 (+0.00) · 8 name(s) re-marked at the open (per-name table). BMEA×801 yday $1.61 → 09:30 $1.61 +0.00; NPWR×648 yday $2.02 → 09:30 $2.02 +0.00; PUSA×350 yday $3.91 → 09:30 $3.91 +0.00; ALVO×248 yday $5.25 → 09:30 $5.25 +0.00; CAPR×191 yday $7.19 → 09:30 $7.19 +0.00; ZURA×203 yday $6.50 → 09:30 $6.50 +0.00; SUJA×147 yday $8.54 → 09:30 $8.54 +0.00; CYPH×748 yday $1.64 → 09:30 $1.64 +0.00 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.15 | ▲ close $10,445.11 vs 09:30 $10,445.11 (session +0.00) | 16:00 close · cash $1.15 · equity $10,445.11 vs 09:30 $10,445.11 (+0.00; session marks +0.00) · 8 name(s) marked open→close (per-name table). BMEA×801 09:30 $1.61 → close $1.61 +0.00; NPWR×648 09:30 $2.02 → close $2.02 +0.00; PUSA×350 09:30 $3.91 → close $3.91 +0.00; ALVO×248 09:30 $5.25 → close $5.25 +0.00; CAPR×191 09:30 $7.19 → close $7.19 +0.00; ZURA×203 09:30 $6.50 → close $6.50 +0.00; SUJA×147 09:30 $8.54 → close $8.54 +0.00; CYPH×748 09:30 $1.64 → close $1.64 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.15 | ▲ 09:30 equity $10,637.49 vs yday $10,445.11 (+192.38) | 09:30 open · cash $1.15 (unchanged overnight, no fees) · equity $10,637.49 vs prior close $10,445.11 (+192.38) · 8 name(s) re-marked at the open (per-name table). BMEA×801 yday $1.61 → 09:30 $1.75 +112.14; NPWR×648 yday $2.02 → 09:30 $1.93 -58.32; PUSA×350 yday $3.91 → 09:30 $3.84 -24.50; ALVO×248 yday $5.25 → 09:30 $4.98 -66.96; CAPR×191 yday $7.19 → 09:30 $8.29 +210.10; ZURA×203 yday $6.50 → 09:30 $6.13 -75.11; SUJA×147 yday $8.54 → 09:30 $9.39 +124.95; CYPH×748 yday $1.64 → 09:30 $1.60 -29.92 | — |
| 2026-08-27 09:30 ET | **SELL** | `BMEA` | 801 | $1.75 | $10.48 | $+83.32 | $1,392.42 | ▲ +83.32 after sell → book $10,627.01; vs 09:30 mark -10.48 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `NPWR` | 648 | $1.93 | $8.48 | $-62.20 | $2,634.59 | ▼ -62.20 after sell → book $10,618.54; vs 09:30 mark -8.47 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `PUSA` | 350 | $3.84 | $4.58 | $+39.90 | $3,974.00 | ▲ +39.90 after sell → book $10,613.95; vs 09:30 mark -4.59 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ALVO` | 248 | $4.98 | $3.25 | $-65.97 | $5,205.79 | ▼ -65.97 after sell → book $10,610.70; vs 09:30 mark -3.25 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CAPR` | 191 | $8.29 | $2.61 | $+281.33 | $6,786.58 | ▲ +281.33 after sell → book $10,608.10; vs 09:30 mark -2.60 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ZURA` | 203 | $6.13 | $2.66 | $-56.03 | $8,028.30 | ▼ -56.03 after sell → book $10,605.43; vs 09:30 mark -2.67 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SUJA` | 147 | $9.39 | $2.47 | $+83.30 | $9,406.17 | ▲ +83.30 after sell → book $10,602.97; vs 09:30 mark -2.46 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 748 | $1.60 | $9.78 | $-94.23 | $10,593.18 | ▼ -94.23 after sell → book $10,593.18; vs 09:30 mark -9.79 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,593.18 | ▲ close $10,593.18 vs 09:30 $10,637.49 (session +0.00) | 16:00 close · cash $10,593.18 · no lots left · equity $10,593.18. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,593.18 | ▲ 09:30 equity $10,593.18 vs yday $10,593.18 (+0.00) | 09:30 open · cash $10,593.18 · no holdings · equity $10,593.18 vs prior close $10,593.18 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 9 | $144.70 | $2.02 | — | $9,288.87 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1324.15 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BHVN` | 78 | $16.95 | $2.22 | — | $7,964.54 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $1324.15 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 71 | $18.50 | $2.20 | — | $6,648.84 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $1324.15 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 144 | $9.19 | $2.42 | — | $5,323.06 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $1324.15 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 39 | $33.78 | $2.11 | — | $4,003.53 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.9; leftover $1324.15 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 8 | $149.40 | $2.01 | — | $2,806.32 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=-11.6; leftover $1324.15 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 16 | $82.70 | $2.04 | — | $1,481.08 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; 🔵; ret5=-4.6; leftover $1324.15 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ERAS` | 68 | $19.30 | $2.19 | — | $166.48 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer; ret5=-4.1; leftover $1324.15 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $166.48 | ▼ close $10,494.52 vs 09:30 $10,593.18 (session -81.44) | 16:00 close · cash $166.48 · equity $10,494.52 vs 09:30 $10,593.18 (-98.66; session marks -81.44) · 8 name(s) marked open→close (per-name table). ANF×9 09:30 $144.70 → close $145.75 +9.45; BHVN×78 09:30 $16.95 → close $16.12 -64.74; BZ×71 09:30 $18.50 → close $18.00 -35.50; CAPR×144 09:30 $9.19 → close $10.06 +125.28; SEDG×39 09:30 $33.78 → close $33.51 -10.53; SMTC×8 09:30 $149.40 → close $142.43 -55.76; URBN×16 09:30 $82.70 → close $78.79 -62.56; ERAS×68 09:30 $19.30 → close $19.49 +12.92 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $166.48 | ▼ 09:30 equity $10,145.84 vs yday $10,494.52 (-348.68) | 09:30 open · cash $166.48 (unchanged overnight, no fees) · equity $10,145.84 vs prior close $10,494.52 (-348.68) · 8 name(s) re-marked at the open (per-name table). ANF×9 yday $145.75 → 09:30 $148.67 +26.28; BHVN×78 yday $16.12 → 09:30 $15.44 -53.04; BZ×71 yday $18.00 → 09:30 $17.89 -7.81; CAPR×144 yday $10.06 → 09:30 $9.44 -89.28; SEDG×39 yday $33.51 → 09:30 $31.50 -78.39; SMTC×8 yday $142.43 → 09:30 $133.04 -75.12; URBN×16 yday $78.79 → 09:30 $81.09 +36.80; ERAS×68 yday $19.49 → 09:30 $17.90 -108.12 | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 9 | $148.67 | $2.04 | $+31.68 | $1,502.48 | ▲ +31.68 after sell → book $10,143.81; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BHVN` | 78 | $15.44 | $2.25 | $-122.25 | $2,704.55 | ▼ -122.25 after sell → book $10,141.56; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BZ` | 71 | $17.89 | $2.22 | $-47.74 | $3,972.52 | ▼ -47.74 after sell → book $10,139.34; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 144 | $9.44 | $2.46 | $+31.12 | $5,329.42 | ▲ +31.12 after sell → book $10,136.88; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 39 | $31.50 | $2.13 | $-93.15 | $6,555.79 | ▼ -93.15 after sell → book $10,134.75; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 8 | $133.04 | $2.03 | $-134.93 | $7,618.08 | ▼ -134.93 after sell → book $10,132.72; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `URBN` | 16 | $81.09 | $2.06 | $-29.86 | $8,913.46 | ▼ -29.86 after sell → book $10,130.66; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ERAS` | 68 | $17.90 | $2.22 | $-99.61 | $10,128.44 | ▼ -99.61 after sell → book $10,128.44; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,128.44 | ▲ close $10,128.44 vs 09:30 $10,145.84 (session +0.00) | 16:00 close · cash $10,128.44 · no lots left · equity $10,128.44. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,128.44 | ▲ 09:30 equity $10,128.44 vs yday $10,128.44 (+0.00) | 09:30 open · cash $10,128.44 · no holdings · equity $10,128.44 vs prior close $10,128.44 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,128.44 | ▲ close $10,128.44 vs 09:30 $10,128.44 (session +0.00) | 16:00 close · cash $10,128.44 · no lots left · equity $10,128.44. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,128.44 | ▲ 09:30 equity $10,128.44 vs yday $10,128.44 (+0.00) | 09:30 open · cash $10,128.44 · no holdings · equity $10,128.44 vs prior close $10,128.44 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,128.44 | ▲ close $10,128.44 vs 09:30 $10,128.44 (session +0.00) | 16:00 close · cash $10,128.44 · no lots left · equity $10,128.44. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,128.44 | ▲ 09:30 equity $10,128.44 vs yday $10,128.44 (+0.00) | 09:30 open · cash $10,128.44 · no holdings · equity $10,128.44 vs prior close $10,128.44 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $125.94 | $2.02 | — | $8,867.02 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1266.06 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 1037 | $1.22 | $13.38 | — | $7,588.51 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1266.06 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRVO` | 68 | $18.40 | $2.19 | — | $6,335.11 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $1266.06 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 80 | $15.70 | $2.23 | — | $5,076.88 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1266.06 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 55 | $22.78 | $2.15 | — | $3,821.83 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1266.06 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CTMX` | 340 | $3.72 | $4.39 | — | $2,552.64 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer; 🔵; ret5=-2.4; leftover $1266.06 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `EIX` | 22 | $56.78 | $2.06 | — | $1,301.43 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer; ret5=+0.3; leftover $1266.06 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 586 | $2.16 | $7.56 | — | $28.11 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.3; leftover $1266.06 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $28.11 | ▲ close $10,613.28 vs 09:30 $10,128.44 (session +520.81) | 16:00 close · cash $28.11 · equity $10,613.28 vs 09:30 $10,128.44 (+484.84; session marks +520.81) · 8 name(s) marked open→close (per-name table). RVTY×10 09:30 $125.94 → close $130.94 +50.00; GPRO×1037 09:30 $1.22 → close $1.69 +487.39; FRVO×68 09:30 $18.40 → close $17.98 -28.56; CRK×80 09:30 $15.70 → close $15.54 -12.80; MMED×55 09:30 $22.78 → close $23.76 +53.90; CTMX×340 09:30 $3.72 → close $3.72 +0.00; EIX×22 09:30 $56.78 → close $55.19 -34.98; CRDL×586 09:30 $2.16 → close $2.17 +5.86 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $28.11 | ▲ 09:30 equity $10,755.15 vs yday $10,613.28 (+141.87) | 09:30 open · cash $28.11 (unchanged overnight, no fees) · equity $10,755.15 vs prior close $10,613.28 (+141.87) · 8 name(s) re-marked at the open (per-name table). RVTY×10 yday $130.94 → 09:30 $132.45 +15.10; GPRO×1037 yday $1.69 → 09:30 $1.78 +93.33; FRVO×68 yday $17.98 → 09:30 $18.27 +19.72; CRK×80 yday $15.54 → 09:30 $15.45 -7.20; MMED×55 yday $23.76 → 09:30 $23.88 +6.60; CTMX×340 yday $3.72 → 09:30 $3.73 +3.40; EIX×22 yday $55.19 → 09:30 $55.42 +5.06; CRDL×586 yday $2.17 → 09:30 $2.18 +5.86 | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 10 | $132.45 | $2.04 | $+61.04 | $1,350.57 | ▲ +61.04 after sell → book $10,753.11; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `FRVO` | 68 | $18.27 | $2.22 | $-13.25 | $2,590.71 | ▼ -13.25 after sell → book $10,750.89; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 80 | $15.45 | $2.25 | $-24.48 | $3,824.46 | ▼ -24.48 after sell → book $10,748.64; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 55 | $23.88 | $2.18 | $+56.17 | $5,135.68 | ▲ +56.17 after sell → book $10,746.46; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CTMX` | 340 | $3.73 | $4.45 | $-5.44 | $6,399.43 | ▼ -5.44 after sell → book $10,742.01; vs 09:30 mark -4.45 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `EIX` | 22 | $55.42 | $2.08 | $-34.05 | $7,616.59 | ▼ -34.05 after sell → book $10,739.93; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRDL` | 586 | $2.18 | $7.67 | $-3.51 | $8,886.41 | ▼ -3.51 after sell → book $10,732.27; vs 09:30 mark -7.66 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CABA` | 349 | $3.63 | $4.50 | — | $7,615.03 | — | union ∩ vol_g, no 🚨; gate vol=good; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1269.49 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 651 | $1.95 | $8.40 | — | $6,337.19 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $1269.49 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `EOSE` | 355 | $3.57 | $4.58 | — | $5,065.26 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.2; leftover $1269.49 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 2 | $486.31 | $2.00 | — | $4,090.64 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-9.9; leftover $1269.49 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MLYS` | 43 | $29.15 | $2.12 | — | $2,835.07 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.0; leftover $1269.49 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `CCOI` | 124 | $10.22 | $2.36 | — | $1,565.43 | — | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.9; leftover $1269.49 | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `SGLD` | 195 | $6.48 | $2.58 | — | $299.25 | — | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; ret5=+0.0; leftover $1269.49 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $299.25 | ▼ close $10,063.90 vs 09:30 $10,755.15 (session -641.83) | 16:00 close · cash $299.25 · equity $10,063.90 vs 09:30 $10,755.15 (-691.25; session marks -641.83) · 8 name(s) marked open→close (per-name table). GPRO×1037 09:30 $1.78 → close $1.39 -404.43; CABA×349 09:30 $3.63 → close $3.48 -52.35; BAK×651 09:30 $1.95 → close $1.94 -6.51; EOSE×355 09:30 $3.57 → close $3.50 -24.85; DELL×2 09:30 $486.31 → close $516.39 +60.16; MLYS×43 09:30 $29.15 → close $28.27 -37.84; CCOI×124 09:30 $10.22 → close $9.98 -29.76; SGLD×195 09:30 $6.48 → close $5.73 -146.25 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `NXE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `WFF` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OABI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AXTI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HIVE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ANGX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FIGR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `RANI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SECZ` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WEAV` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `HAE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `USAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BKKT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TMC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `GUTS` | hard_red | hard-red S=-5.17 sit; no new buys |
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
| 2026-08-31 | `OKTA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `VEEV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SNPS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `DFDV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRVO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `YDDL` | hard_red | hard-red S=-3.83 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `GPRO` | 1037 | 2026-09-03 @ $1.22 | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1266.06 |
| `CABA` | 349 | 2026-09-04 @ $3.63 | union ∩ vol_g, no 🚨; gate vol=good; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1269.49 |
| `BAK` | 651 | 2026-09-04 @ $1.95 | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $1269.49 |
| `EOSE` | 355 | 2026-09-04 @ $3.57 | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.2; leftover $1269.49 |
| `DELL` | 2 | 2026-09-04 @ $486.31 | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-9.9; leftover $1269.49 |
| `MLYS` | 43 | 2026-09-04 @ $29.15 | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.0; leftover $1269.49 |
| `CCOI` | 124 | 2026-09-04 @ $10.22 | union ∩ vol_g, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.9; leftover $1269.49 |
| `SGLD` | 195 | 2026-09-04 @ $6.48 | union ∩ vol_g, no 🚨; gate vol=good; list yday_gainer,yday_mover; ret5=+0.0; leftover $1269.49 |
