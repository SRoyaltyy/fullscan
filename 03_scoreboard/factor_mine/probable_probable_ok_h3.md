# Factor mine action — `probable_probable_ok_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `probable` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **-11.54%** ($8,846) · signal-only (no cash/fees) was -8.12%. Starts YES **4/17**. Fills 63 · skips 95 · realized $-1507.28.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `probable` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `last_green=True,ret_5_max=10.0` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9.23.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `ANGX` | 464 | — | $4.31 | +0.00 | $4.37 | +27.84 | +27.84 | +0.00 | +27.84 |
| 2026-08-14 | `HYLN` | 478 | — | $4.18 | +0.00 | $4.06 | -57.36 | -57.36 | +0.00 | -57.36 |
| 2026-08-14 | `WDC` | 3 | — | $503.50 | +0.00 | $508.80 | +15.90 | +15.90 | +0.00 | +15.90 |
| 2026-08-14 | `ADUR` | 121 | — | $16.50 | +0.00 | $16.17 | -39.93 | -39.93 | +0.00 | -39.93 |
| 2026-08-14 | `ALGM` | 45 | — | $44.06 | +0.00 | $44.39 | +14.85 | +14.85 | +0.00 | +14.85 |
| 2026-08-17 | `ANGX` | 464 | $4.37 | $4.60 | +106.72 | $4.71 | +51.04 | +157.76 | +134.56 | +185.60 |
| 2026-08-17 | `HYLN` | 478 | $4.06 | $4.10 | +19.12 | $4.09 | -4.78 | +14.34 | -38.24 | -43.02 |
| 2026-08-17 | `WDC` | 3 | $508.80 | $525.53 | +50.19 | $536.01 | +31.44 | +81.63 | +66.09 | +97.53 |
| 2026-08-17 | `ADUR` | 121 | $16.17 | $15.73 | -53.24 | $15.85 | +14.52 | -38.72 | -93.17 | -78.65 |
| 2026-08-17 | `ALGM` | 45 | $44.39 | $45.32 | +41.85 | $44.25 | -48.15 | -6.30 | +56.70 | +8.55 |
| 2026-08-17 | `CDNL` | 2 | — | $39.85 | +0.00 | $39.23 | -1.24 | -1.24 | +0.00 | -1.24 |
| 2026-08-17 | `ABX` | 9 | — | $9.12 | +0.00 | $9.12 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-17 | `VERA` | 2 | — | $31.30 | +0.00 | $31.63 | +0.66 | +0.66 | +0.00 | +0.66 |
| 2026-08-17 | `OCC` | 4 | — | $18.24 | +0.00 | $17.12 | -4.48 | -4.48 | +0.00 | -4.48 |
| 2026-08-17 | `ALM` | 5 | — | $16.20 | +0.00 | $16.36 | +0.80 | +0.80 | +0.00 | +0.80 |
| 2026-08-18 | `ANGX` | 464 | $4.71 | $4.79 | +37.12 | $4.85 | +27.84 | +64.96 | +222.72 | +250.56 |
| 2026-08-18 | `HYLN` | 478 | $4.09 | $3.95 | -66.92 | $3.86 | -43.02 | -109.94 | -109.94 | -152.96 |
| 2026-08-18 | `WDC` | 3 | $536.01 | $496.07 | -119.82 | $496.16 | +0.27 | -119.55 | -22.29 | -22.02 |
| 2026-08-18 | `ADUR` | 121 | $15.85 | $15.41 | -53.24 | $15.63 | +26.62 | -26.62 | -131.89 | -105.27 |
| 2026-08-18 | `ALGM` | 45 | $44.25 | $42.54 | -76.95 | $39.39 | -141.75 | -218.70 | -68.40 | -210.15 |
| 2026-08-18 | `CDNL` | 2 | $39.23 | $41.57 | +4.68 | $45.14 | +7.14 | +11.82 | +3.44 | +10.58 |
| 2026-08-18 | `ABX` | 9 | $9.12 | $9.03 | -0.81 | $9.01 | -0.18 | -0.99 | -0.81 | -0.99 |
| 2026-08-18 | `VERA` | 2 | $31.63 | $31.31 | -0.64 | $32.28 | +1.94 | +1.30 | +0.02 | +1.96 |
| 2026-08-18 | `OCC` | 4 | $17.12 | $16.20 | -3.68 | $16.20 | +0.00 | -3.68 | -8.16 | -8.16 |
| 2026-08-18 | `ALM` | 5 | $16.36 | $15.78 | -2.90 | $15.60 | -0.90 | -3.80 | -2.10 | -3.00 |
| 2026-08-19 | `ANGX` | 464 | $4.85 | $4.79 | -27.84 | — | +0.00 | -27.84 | +222.72 | — |
| 2026-08-19 | `HYLN` | 478 | $3.86 | $3.87 | +4.78 | — | +0.00 | +4.78 | -148.18 | — |
| 2026-08-19 | `WDC` | 3 | $496.16 | $494.28 | -5.64 | — | +0.00 | -5.64 | -27.66 | — |
| 2026-08-19 | `ADUR` | 121 | $15.63 | $15.65 | +2.42 | — | +0.00 | +2.42 | -102.85 | — |
| 2026-08-19 | `ALGM` | 45 | $39.39 | $40.00 | +27.45 | — | +0.00 | +27.45 | -182.70 | — |
| 2026-08-19 | `CDNL` | 2 | $45.14 | $44.83 | -0.62 | $43.33 | -3.00 | -3.62 | +9.96 | +6.96 |
| 2026-08-19 | `ABX` | 9 | $9.01 | $9.08 | +0.63 | $9.15 | +0.63 | +1.26 | -0.36 | +0.27 |
| 2026-08-19 | `VERA` | 2 | $32.28 | $32.88 | +1.20 | $32.27 | -1.21 | -0.01 | +3.16 | +1.95 |
| 2026-08-19 | `OCC` | 4 | $16.20 | $16.21 | +0.04 | $14.36 | -7.40 | -7.36 | -8.12 | -15.52 |
| 2026-08-19 | `ALM` | 5 | $15.60 | $16.05 | +2.25 | $16.18 | +0.65 | +2.90 | -0.75 | -0.10 |
| 2026-08-20 | `CDNL` | 2 | $43.33 | $43.13 | -0.40 | — | +0.00 | -0.40 | +6.56 | — |
| 2026-08-20 | `ABX` | 9 | $9.15 | $9.13 | -0.18 | — | +0.00 | -0.18 | +0.09 | — |
| 2026-08-20 | `VERA` | 2 | $32.27 | $32.30 | +0.04 | — | +0.00 | +0.04 | +1.99 | — |
| 2026-08-20 | `OCC` | 4 | $14.36 | $14.10 | -1.04 | — | +0.00 | -1.04 | -16.56 | — |
| 2026-08-20 | `ALM` | 5 | $16.18 | $15.81 | -1.85 | — | +0.00 | -1.85 | -1.95 | — |
| 2026-08-20 | `DNA` | 186 | — | $7.45 | +0.00 | $6.96 | -91.14 | -91.14 | +0.00 | -91.14 |
| 2026-08-20 | `MSTR` | 12 | — | $113.23 | +0.00 | $112.39 | -10.08 | -10.08 | +0.00 | -10.08 |
| 2026-08-20 | `EXK` | 128 | — | $10.77 | +0.00 | $10.97 | +25.60 | +25.60 | +0.00 | +25.60 |
| 2026-08-20 | `SCZM` | 146 | — | $9.46 | +0.00 | $9.76 | +43.80 | +43.80 | +0.00 | +43.80 |
| 2026-08-20 | `NG` | 165 | — | $8.38 | +0.00 | $8.66 | +46.20 | +46.20 | +0.00 | +46.20 |
| 2026-08-20 | `BLSH` | 47 | — | $29.20 | +0.00 | $28.44 | -35.72 | -35.72 | +0.00 | -35.72 |
| 2026-08-20 | `HYMC` | 50 | — | $27.25 | +0.00 | $26.14 | -55.50 | -55.50 | +0.00 | -55.50 |
| 2026-08-21 | `DNA` | 186 | $6.96 | $7.09 | +24.18 | $7.40 | +57.66 | +81.84 | -66.96 | -9.30 |
| 2026-08-21 | `MSTR` | 12 | $112.39 | $119.69 | +87.60 | $119.25 | -5.28 | +82.32 | +77.52 | +72.24 |
| 2026-08-21 | `EXK` | 128 | $10.97 | $11.34 | +47.36 | $10.62 | -92.16 | -44.80 | +72.96 | -19.20 |
| 2026-08-21 | `SCZM` | 146 | $9.76 | $10.26 | +73.00 | $9.68 | -85.41 | -12.41 | +116.80 | +31.39 |
| 2026-08-21 | `NG` | 165 | $8.66 | $9.02 | +59.40 | $8.72 | -49.50 | +9.90 | +105.60 | +56.10 |
| 2026-08-21 | `BLSH` | 47 | $28.44 | $29.75 | +61.57 | $30.41 | +31.02 | +92.59 | +25.85 | +56.87 |
| 2026-08-21 | `HYMC` | 50 | $26.14 | $27.40 | +63.00 | $27.07 | -16.50 | +46.50 | +7.50 | -9.00 |
| 2026-08-21 | `BTBT` | 8 | — | $1.66 | +0.00 | $1.53 | -1.04 | -1.04 | +0.00 | -1.04 |
| 2026-08-21 | `ORBS` | 15 | — | $0.86 | +0.00 | $0.88 | +0.24 | +0.24 | +0.00 | +0.24 |
| 2026-08-21 | `GORO` | 4 | — | $3.11 | +0.00 | $3.19 | +0.32 | +0.32 | +0.00 | +0.32 |
| 2026-08-24 | `DNA` | 186 | $7.40 | $7.26 | -26.04 | $6.98 | -52.08 | -78.12 | -35.34 | -87.42 |
| 2026-08-24 | `MSTR` | 12 | $119.25 | $121.76 | +30.12 | $124.59 | +33.96 | +64.08 | +102.36 | +136.32 |
| 2026-08-24 | `EXK` | 128 | $10.62 | $11.01 | +49.92 | $10.74 | -34.56 | +15.36 | +30.72 | -3.84 |
| 2026-08-24 | `SCZM` | 146 | $9.68 | $9.82 | +21.17 | $9.53 | -42.34 | -21.17 | +52.56 | +10.22 |
| 2026-08-24 | `NG` | 165 | $8.72 | $8.89 | +28.05 | $9.24 | +57.75 | +85.80 | +84.15 | +141.90 |
| 2026-08-24 | `BLSH` | 47 | $30.41 | $30.18 | -10.81 | $30.88 | +32.90 | +22.09 | +46.06 | +78.96 |
| 2026-08-24 | `HYMC` | 50 | $27.07 | $27.24 | +8.50 | $25.84 | -70.00 | -61.50 | -0.50 | -70.50 |
| 2026-08-24 | `BTBT` | 8 | $1.53 | $1.55 | +0.16 | $1.56 | +0.08 | +0.24 | -0.88 | -0.80 |
| 2026-08-24 | `ORBS` | 15 | $0.88 | $0.89 | +0.15 | $0.85 | -0.60 | -0.45 | +0.39 | -0.21 |
| 2026-08-24 | `GORO` | 4 | $3.19 | $3.20 | +0.04 | $3.57 | +1.48 | +1.52 | +0.36 | +1.84 |
| 2026-08-25 | `DNA` | 186 | $6.98 | $6.82 | -29.76 | — | +0.00 | -29.76 | -117.18 | — |
| 2026-08-25 | `MSTR` | 12 | $124.59 | $125.56 | +11.64 | — | +0.00 | +11.64 | +147.96 | — |
| 2026-08-25 | `EXK` | 128 | $10.74 | $10.72 | -2.56 | — | +0.00 | -2.56 | -6.40 | — |
| 2026-08-25 | `SCZM` | 146 | $9.53 | $9.57 | +5.84 | — | +0.00 | +5.84 | +16.06 | — |
| 2026-08-25 | `NG` | 165 | $9.24 | $9.34 | +16.50 | — | +0.00 | +16.50 | +158.40 | — |
| 2026-08-25 | `BLSH` | 47 | $30.88 | $31.00 | +5.64 | — | +0.00 | +5.64 | +84.60 | — |
| 2026-08-25 | `HYMC` | 50 | $25.84 | $25.73 | -5.50 | — | +0.00 | -5.50 | -76.00 | — |
| 2026-08-25 | `BTBT` | 8 | $1.56 | $1.55 | -0.08 | $1.53 | -0.16 | -0.24 | -0.88 | -1.04 |
| 2026-08-25 | `ORBS` | 15 | $0.85 | $0.85 | +0.00 | $0.84 | -0.15 | -0.15 | -0.21 | -0.36 |
| 2026-08-25 | `GORO` | 4 | $3.57 | $3.53 | -0.16 | $3.56 | +0.12 | -0.04 | +1.68 | +1.80 |
| 2026-08-25 | `NPWR` | 1230 | — | $2.00 | +0.00 | $2.02 | +24.60 | +24.60 | +0.00 | +24.60 |
| 2026-08-25 | `ALVO` | 471 | — | $5.22 | +0.00 | $5.25 | +14.13 | +14.13 | +0.00 | +14.13 |
| 2026-08-25 | `ALIT` | 165 | — | $14.86 | +0.00 | $14.87 | +1.65 | +1.65 | +0.00 | +1.65 |
| 2026-08-25 | `ZURA` | 382 | — | $6.38 | +0.00 | $6.50 | +45.84 | +45.84 | +0.00 | +45.84 |
| 2026-08-26 | `BTBT` | 8 | $1.53 | $1.53 | +0.00 | $1.53 | +0.00 | +0.00 | -1.04 | -1.04 |
| 2026-08-26 | `ORBS` | 15 | $0.84 | $0.84 | +0.00 | $0.84 | +0.00 | +0.00 | -0.36 | -0.36 |
| 2026-08-26 | `GORO` | 4 | $3.56 | $3.56 | +0.00 | $3.56 | +0.00 | +0.00 | +1.80 | +1.80 |
| 2026-08-26 | `NPWR` | 1230 | $2.02 | $2.02 | +0.00 | $2.02 | +0.00 | +0.00 | +24.60 | +24.60 |
| 2026-08-26 | `ALVO` | 471 | $5.25 | $5.25 | +0.00 | $5.25 | +0.00 | +0.00 | +14.13 | +14.13 |
| 2026-08-26 | `ALIT` | 165 | $14.87 | $14.87 | +0.00 | $14.87 | +0.00 | +0.00 | +1.65 | +1.65 |
| 2026-08-26 | `ZURA` | 382 | $6.50 | $6.50 | +0.00 | $6.50 | +0.00 | +0.00 | +45.84 | +45.84 |
| 2026-08-27 | `BTBT` | 8 | $1.53 | $1.53 | +0.00 | — | +0.00 | +0.00 | -1.04 | — |
| 2026-08-27 | `ORBS` | 15 | $0.84 | $0.80 | -0.60 | — | +0.00 | -0.60 | -0.96 | — |
| 2026-08-27 | `GORO` | 4 | $3.56 | $3.77 | +0.84 | — | +0.00 | +0.84 | +2.64 | — |
| 2026-08-27 | `NPWR` | 1230 | $2.02 | $1.93 | -110.70 | $1.81 | -147.60 | -258.30 | -86.10 | -233.70 |
| 2026-08-27 | `ALVO` | 471 | $5.25 | $4.98 | -127.17 | $4.91 | -32.97 | -160.14 | -113.04 | -146.01 |
| 2026-08-27 | `ALIT` | 165 | $14.87 | $14.85 | -3.30 | $14.33 | -85.80 | -89.10 | -1.65 | -87.45 |
| 2026-08-27 | `ZURA` | 382 | $6.50 | $6.13 | -141.34 | $5.99 | -53.48 | -194.82 | -95.50 | -148.98 |
| 2026-08-28 | `NPWR` | 1230 | $1.81 | $1.83 | +24.60 | — | +0.00 | +24.60 | -209.10 | — |
| 2026-08-28 | `ALVO` | 471 | $4.91 | $4.88 | -14.13 | — | +0.00 | -14.13 | -160.14 | — |
| 2026-08-28 | `ALIT` | 165 | $14.33 | $14.54 | +34.65 | — | +0.00 | +34.65 | -52.80 | — |
| 2026-08-28 | `ZURA` | 382 | $5.99 | $6.02 | +11.46 | — | +0.00 | +11.46 | -137.52 | — |
| 2026-08-28 | `ANF` | 12 | — | $144.70 | +0.00 | $145.75 | +12.60 | +12.60 | +0.00 | +12.60 |
| 2026-08-28 | `BHVN` | 109 | — | $16.95 | +0.00 | $16.12 | -90.47 | -90.47 | +0.00 | -90.47 |
| 2026-08-28 | `BZ` | 100 | — | $18.50 | +0.00 | $18.00 | -50.00 | -50.00 | +0.00 | -50.00 |
| 2026-08-28 | `LVWR` | 1342 | — | $1.38 | +0.00 | $1.36 | -26.84 | -26.84 | +0.00 | -26.84 |
| 2026-08-28 | `GRRR` | 116 | — | $15.94 | +0.00 | $15.66 | -32.48 | -32.48 | +0.00 | -32.48 |
| 2026-08-31 | `ANF` | 12 | $145.75 | $148.67 | +35.04 | $149.28 | +7.32 | +42.36 | +47.64 | +54.96 |
| 2026-08-31 | `BHVN` | 109 | $16.12 | $15.44 | -74.12 | $15.40 | -4.36 | -78.48 | -164.59 | -168.95 |
| 2026-08-31 | `BZ` | 100 | $18.00 | $17.89 | -11.00 | $17.90 | +1.00 | -10.00 | -61.00 | -60.00 |
| 2026-08-31 | `LVWR` | 1342 | $1.36 | $1.37 | +13.42 | $1.34 | -40.26 | -26.84 | -13.42 | -53.68 |
| 2026-08-31 | `GRRR` | 116 | $15.66 | $14.32 | -155.44 | $14.20 | -13.92 | -169.36 | -187.92 | -201.84 |
| 2026-09-01 | `ANF` | 12 | $149.28 | $142.47 | -81.72 | $143.00 | +6.36 | -75.36 | -26.76 | -20.40 |
| 2026-09-01 | `BHVN` | 109 | $15.40 | $15.45 | +5.45 | $15.45 | +0.00 | +5.45 | -163.50 | -163.50 |
| 2026-09-01 | `BZ` | 100 | $17.90 | $17.37 | -53.00 | $17.17 | -20.00 | -73.00 | -113.00 | -133.00 |
| 2026-09-01 | `LVWR` | 1342 | $1.34 | $1.22 | -161.04 | $1.18 | -53.68 | -214.72 | -214.72 | -268.40 |
| 2026-09-01 | `GRRR` | 116 | $14.20 | $15.05 | +98.60 | $14.80 | -29.00 | +69.60 | -103.24 | -132.24 |
| 2026-09-02 | `ANF` | 12 | $143.00 | $142.00 | -12.00 | — | +0.00 | -12.00 | -32.40 | — |
| 2026-09-02 | `BHVN` | 109 | $15.45 | $15.39 | -6.54 | — | +0.00 | -6.54 | -170.04 | — |
| 2026-09-02 | `BZ` | 100 | $17.17 | $17.29 | +12.00 | — | +0.00 | +12.00 | -121.00 | — |
| 2026-09-02 | `LVWR` | 1342 | $1.18 | $1.19 | +13.42 | — | +0.00 | +13.42 | -254.98 | — |
| 2026-09-02 | `GRRR` | 116 | $14.80 | $14.75 | -5.80 | — | +0.00 | -5.80 | -138.04 | — |
| 2026-09-03 | `GPRO` | 2320 | — | $1.22 | +0.00 | $1.69 | +1090.40 | +1090.40 | +0.00 | +1090.40 |
| 2026-09-03 | `CRK` | 180 | — | $15.70 | +0.00 | $15.54 | -28.80 | -28.80 | +0.00 | -28.80 |
| 2026-09-03 | `MMED` | 122 | — | $22.78 | +0.00 | $23.76 | +119.56 | +119.56 | +0.00 | +119.56 |
| 2026-09-04 | `GPRO` | 2320 | $1.69 | $1.78 | +208.80 | $1.39 | -904.80 | -696.00 | +1299.20 | +394.40 |
| 2026-09-04 | `CRK` | 180 | $15.54 | $15.45 | -16.20 | $14.95 | -90.00 | -106.20 | -45.00 | -135.00 |
| 2026-09-04 | `MMED` | 122 | $23.76 | $23.88 | +14.64 | $23.84 | -4.88 | +9.76 | +134.20 | +129.32 |
| 2026-09-04 | `BAK` | 3 | — | $1.95 | +0.00 | $1.94 | -0.03 | -0.03 | +0.00 | -0.03 |
| 2026-09-04 | `EOSE` | 2 | — | $3.57 | +0.00 | $3.50 | -0.14 | -0.14 | +0.00 | -0.14 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | -38.70 | ANGX, HYLN, WDC, ADUR, ALGM | — | $493.79 | $9,942.67 | ANGX×464, HYLN×478, WDC×3, ADUR×121, ALGM×45 |
| 2026-08-17 | +2.25 | $493.79 | ANGX×464, HYLN×478, WDC×3, ADUR×121, ALGM×45 | $10,107.31 | +164.64 | +39.81 | CDNL, ABX, VERA, OCC, ALM | — | $111.60 | $10,143.27 | ANGX×464, HYLN×478, WDC×3, ADUR×121, ALGM×45, CDNL×2, ABX×9, VERA×2, OCC×4, ALM×5 |
| 2026-08-18 | -6.20 | $111.60 | ANGX×464, HYLN×478, WDC×3, ADUR×121, ALGM×45, CDNL×2, ABX×9, VERA×2, OCC×4, ALM×5 | $9,860.11 | -283.16 | -122.04 | — | — | $111.60 | $9,738.07 | ANGX×464, HYLN×478, WDC×3, ADUR×121, ALGM×45, CDNL×2, ABX×9, VERA×2, OCC×4, ALM×5 |
| 2026-08-19 | -7.20 | $111.60 | ANGX×464, HYLN×478, WDC×3, ADUR×121, ALGM×45, CDNL×2, ABX×9, VERA×2, OCC×4, ALM×5 | $9,742.74 | +4.67 | -10.33 | — | ANGX, HYLN, WDC, ADUR, ALGM | $9,341.61 | $9,713.51 | CDNL×2, ABX×9, VERA×2, OCC×4, ALM×5 |
| 2026-08-20 | +1.12 | $9,341.61 | CDNL×2, ABX×9, VERA×2, OCC×4, ALM×5 | $9,710.08 | -3.43 | -76.84 | DNA, MSTR, EXK, SCZM, NG, BLSH, HYMC | CDNL, ABX, VERA, OCC, ALM | $68.32 | $9,613.26 | DNA×186, MSTR×12, EXK×128, SCZM×146, NG×165, BLSH×47, HYMC×50 |
| 2026-08-21 | +3.25 | $68.32 | DNA×186, MSTR×12, EXK×128, SCZM×146, NG×165, BLSH×47, HYMC×50 | $10,029.37 | +416.11 | -160.65 | BTBT, ORBS, GORO | — | $29.17 | $9,868.25 | DNA×186, MSTR×12, EXK×128, SCZM×146, NG×165, BLSH×47, HYMC×50, BTBT×8, ORBS×15, GORO×4 |
| 2026-08-24 | -5.17 | $29.17 | DNA×186, MSTR×12, EXK×128, SCZM×146, NG×165, BLSH×47, HYMC×50, BTBT×8, ORBS×15, GORO×4 | $9,969.51 | +101.26 | -73.41 | — | — | $29.17 | $9,896.10 | DNA×186, MSTR×12, EXK×128, SCZM×146, NG×165, BLSH×47, HYMC×50, BTBT×8, ORBS×15, GORO×4 |
| 2026-08-25 | +1.80 | $29.17 | DNA×186, MSTR×12, EXK×128, SCZM×146, NG×165, BLSH×47, HYMC×50, BTBT×8, ORBS×15, GORO×4 | $9,897.66 | +1.56 | +86.03 | NPWR, ALVO, ALIT, ZURA | DNA, MSTR, EXK, SCZM, NG, BLSH, HYMC | $5.01 | $9,937.99 | BTBT×8, ORBS×15, GORO×4, NPWR×1230, ALVO×471, ALIT×165, ZURA×382 |
| 2026-08-26 | +2.02 | $5.01 | BTBT×8, ORBS×15, GORO×4, NPWR×1230, ALVO×471, ALIT×165, ZURA×382 | $9,937.99 | +0.00 | +0.00 | — | — | $5.01 | $9,937.99 | BTBT×8, ORBS×15, GORO×4, NPWR×1230, ALVO×471, ALIT×165, ZURA×382 |
| 2026-08-27 | — | $5.01 | BTBT×8, ORBS×15, GORO×4, NPWR×1230, ALVO×471, ALIT×165, ZURA×382 | $9,555.72 | -382.27 | -319.85 | — | BTBT, ORBS, GORO | $43.80 | $9,235.34 | NPWR×1230, ALVO×471, ALIT×165, ZURA×382 |
| 2026-08-28 | +0.75 | $43.80 | NPWR×1230, ALVO×471, ALIT×165, ZURA×382 | $9,291.92 | +56.58 | -187.19 | ANF, BHVN, BZ, LVWR, GRRR | NPWR, ALVO, ALIT, ZURA | $100.88 | $9,048.64 | ANF×12, BHVN×109, BZ×100, LVWR×1342, GRRR×116 |
| 2026-08-31 | -5.85 | $100.88 | ANF×12, BHVN×109, BZ×100, LVWR×1342, GRRR×116 | $8,856.54 | -192.10 | -50.22 | — | — | $100.88 | $8,806.32 | ANF×12, BHVN×109, BZ×100, LVWR×1342, GRRR×116 |
| 2026-09-01 | -6.30 | $100.88 | ANF×12, BHVN×109, BZ×100, LVWR×1342, GRRR×116 | $8,614.61 | -191.71 | -96.32 | — | — | $100.88 | $8,518.29 | ANF×12, BHVN×109, BZ×100, LVWR×1342, GRRR×116 |
| 2026-09-02 | -3.83 | $100.88 | ANF×12, BHVN×109, BZ×100, LVWR×1342, GRRR×116 | $8,519.37 | +1.08 | +0.00 | — | ANF, BHVN, BZ, LVWR, GRRR | $8,492.74 | $8,492.74 | — |
| 2026-09-03 | -0.90 | $8,492.74 | — | $8,492.74 | -0.00 | +1,181.16 | GPRO, CRK, MMED | — | $22.36 | $9,639.08 | GPRO×2320, CRK×180, MMED×122 |
| 2026-09-04 | — | $22.36 | GPRO×2320, CRK×180, MMED×122 | $9,846.32 | +207.24 | -999.85 | BAK, EOSE | — | $9.23 | $8,846.33 | GPRO×2320, CRK×180, MMED×122, BAK×3, EOSE×2 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 464 | $4.31 | $5.99 | — | $7,994.17 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 478 | $4.18 | $6.17 | — | $5,989.97 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `WDC` | 3 | $503.50 | $2.00 | — | $4,477.47 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable; 🔵; ⚪; ret5=+7.9; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 121 | $16.50 | $2.35 | — | $2,478.62 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ALGM` | 45 | $44.06 | $2.12 | — | $493.79 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable; 🔵; ret5=+3.9; leftover $2000.00 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $493.79 | ▼ close $9,942.67 vs 09:30 $10,000.00 (session -38.70) | 16:00 close · cash $493.79 · equity $9,942.67 vs 09:30 $10,000.00 (-57.33; session marks -38.70) · 5 name(s) marked open→close (per-name table). ANGX×464 09:30 $4.31 → close $4.37 +27.84; HYLN×478 09:30 $4.18 → close $4.06 -57.36; WDC×3 09:30 $503.50 → close $508.80 +15.90; ADUR×121 09:30 $16.50 → close $16.17 -39.93; ALGM×45 09:30 $44.06 → close $44.39 +14.85 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $493.79 | ▲ 09:30 equity $10,107.31 vs yday $9,942.67 (+164.64) | 09:30 open · cash $493.79 (unchanged overnight, no fees) · equity $10,107.31 vs prior close $9,942.67 (+164.64) · 5 name(s) re-marked at the open (per-name table). ANGX×464 yday $4.37 → 09:30 $4.60 +106.72; HYLN×478 yday $4.06 → 09:30 $4.10 +19.12; WDC×3 yday $508.80 → 09:30 $525.53 +50.19; ADUR×121 yday $16.17 → 09:30 $15.73 -53.24; ALGM×45 yday $44.39 → 09:30 $45.32 +41.85 | — |
| 2026-08-17 09:30 ET | **BUY** | `CDNL` | 2 | $39.85 | $0.80 | — | $413.29 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ⚪; ret5=-38.4; leftover $82.30 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 9 | $9.12 | $0.85 | — | $330.36 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $82.30 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `VERA` | 2 | $31.30 | $0.63 | — | $267.13 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=-3.8; leftover $82.30 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `OCC` | 4 | $18.24 | $0.74 | — | $193.43 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,ohlc_hot; ⚪; ret5=+9.5; leftover $82.30 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ALM` | 5 | $16.20 | $0.82 | — | $111.60 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,ohlc_hot; 🔵; ⚪; ret5=+6.4; leftover $82.30 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $111.60 | ▲ close $10,143.27 vs 09:30 $10,107.31 (session +39.81) | 16:00 close · cash $111.60 · equity $10,143.27 vs 09:30 $10,107.31 (+35.96; session marks +39.81) · 10 name(s) marked open→close (per-name table). ANGX×464 09:30 $4.60 → close $4.71 +51.04; HYLN×478 09:30 $4.10 → close $4.09 -4.78; WDC×3 09:30 $525.53 → close $536.01 +31.44; ADUR×121 09:30 $15.73 → close $15.85 +14.52; ALGM×45 09:30 $45.32 → close $44.25 -48.15; CDNL×2 09:30 $39.85 → close $39.23 -1.24; ABX×9 09:30 $9.12 → close $9.12 +0.00; VERA×2 09:30 $31.30 → close $31.63 +0.66; OCC×4 09:30 $18.24 → close $17.12 -4.48; ALM×5 09:30 $16.20 → close $16.36 +0.80 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $111.60 | ▼ 09:30 equity $9,860.11 vs yday $10,143.27 (-283.16) | 09:30 open · cash $111.60 (unchanged overnight, no fees) · equity $9,860.11 vs prior close $10,143.27 (-283.16) · 10 name(s) re-marked at the open (per-name table). ANGX×464 yday $4.71 → 09:30 $4.79 +37.12; HYLN×478 yday $4.09 → 09:30 $3.95 -66.92; WDC×3 yday $536.01 → 09:30 $496.07 -119.82; ADUR×121 yday $15.85 → 09:30 $15.41 -53.24; ALGM×45 yday $44.25 → 09:30 $42.54 -76.95; CDNL×2 yday $39.23 → 09:30 $41.57 +4.68; ABX×9 yday $9.12 → 09:30 $9.03 -0.81; VERA×2 yday $31.63 → 09:30 $31.31 -0.64; OCC×4 yday $17.12 → 09:30 $16.20 -3.68; ALM×5 yday $16.36 → 09:30 $15.78 -2.90 | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $111.60 | ▼ close $9,738.07 vs 09:30 $9,860.11 (session -122.04) | 16:00 close · cash $111.60 · equity $9,738.07 vs 09:30 $9,860.11 (-122.04; session marks -122.04) · 10 name(s) marked open→close (per-name table). ANGX×464 09:30 $4.79 → close $4.85 +27.84; HYLN×478 09:30 $3.95 → close $3.86 -43.02; WDC×3 09:30 $496.07 → close $496.16 +0.27; ADUR×121 09:30 $15.41 → close $15.63 +26.62; ALGM×45 09:30 $42.54 → close $39.39 -141.75; CDNL×2 09:30 $41.57 → close $45.14 +7.14; ABX×9 09:30 $9.03 → close $9.01 -0.18; VERA×2 09:30 $31.31 → close $32.28 +1.94; OCC×4 09:30 $16.20 → close $16.20 +0.00; ALM×5 09:30 $15.78 → close $15.60 -0.90 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $111.60 | ▲ 09:30 equity $9,742.74 vs yday $9,738.07 (+4.67) | 09:30 open · cash $111.60 (unchanged overnight, no fees) · equity $9,742.74 vs prior close $9,738.07 (+4.67) · 10 name(s) re-marked at the open (per-name table). ANGX×464 yday $4.85 → 09:30 $4.79 -27.84; HYLN×478 yday $3.86 → 09:30 $3.87 +4.78; WDC×3 yday $496.16 → 09:30 $494.28 -5.64; ADUR×121 yday $15.63 → 09:30 $15.65 +2.42; ALGM×45 yday $39.39 → 09:30 $40.00 +27.45; CDNL×2 yday $45.14 → 09:30 $44.83 -0.62; ABX×9 yday $9.01 → 09:30 $9.08 +0.63; VERA×2 yday $32.28 → 09:30 $32.88 +1.20; OCC×4 yday $16.20 → 09:30 $16.21 +0.04; ALM×5 yday $15.60 → 09:30 $16.05 +2.25 | — |
| 2026-08-19 09:30 ET | **SELL** | `ANGX` | 464 | $4.79 | $6.08 | $+210.65 | $2,328.08 | ▲ +210.65 after sell → book $9,736.66; vs 09:30 mark -6.08 | dropped from list after 3 sess (min 3) | join🔴 sector🟡 gen🔴 news🟢 vol🟢 buy🟡 |
| 2026-08-19 09:30 ET | **SELL** | `HYLN` | 478 | $3.87 | $6.26 | $-160.61 | $4,171.68 | ▼ -160.61 after sell → book $9,730.40; vs 09:30 mark -6.26 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `WDC` | 3 | $494.28 | $2.02 | $-31.68 | $5,652.50 | ▼ -31.68 after sell → book $9,728.38; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ADUR` | 121 | $15.65 | $2.39 | $-107.59 | $7,543.76 | ▼ -107.59 after sell → book $9,725.99; vs 09:30 mark -2.39 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ALGM` | 45 | $40.00 | $2.15 | $-186.97 | $9,341.61 | ▼ -186.97 after sell → book $9,723.84; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,341.61 | ▼ close $9,713.51 vs 09:30 $9,742.74 (session -10.33) | 16:00 close · cash $9,341.61 · equity $9,713.51 vs 09:30 $9,742.74 (-29.23; session marks -10.33) · 5 name(s) marked open→close (per-name table). CDNL×2 09:30 $44.83 → close $43.33 -3.00; ABX×9 09:30 $9.08 → close $9.15 +0.63; VERA×2 09:30 $32.88 → close $32.27 -1.21; OCC×4 09:30 $16.21 → close $14.36 -7.40; ALM×5 09:30 $16.05 → close $16.18 +0.65 | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,341.61 | ▼ 09:30 equity $9,710.08 vs yday $9,713.51 (-3.43) | 09:30 open · cash $9,341.61 (unchanged overnight, no fees) · equity $9,710.08 vs prior close $9,713.51 (-3.43) · 5 name(s) re-marked at the open (per-name table). CDNL×2 yday $43.33 → 09:30 $43.13 -0.40; ABX×9 yday $9.15 → 09:30 $9.13 -0.18; VERA×2 yday $32.27 → 09:30 $32.30 +0.04; OCC×4 yday $14.36 → 09:30 $14.10 -1.04; ALM×5 yday $16.18 → 09:30 $15.81 -1.85 | — |
| 2026-08-20 09:30 ET | **SELL** | `CDNL` | 2 | $43.13 | $0.89 | $+4.87 | $9,426.98 | ▲ +4.87 after sell → book $9,709.19; vs 09:30 mark -0.89 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `ABX` | 9 | $9.13 | $0.87 | $-1.63 | $9,508.29 | ▼ -1.63 after sell → book $9,708.33; vs 09:30 mark -0.86 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `VERA` | 2 | $32.30 | $0.67 | $+0.69 | $9,572.20 | ▲ +0.69 after sell → book $9,707.65; vs 09:30 mark -0.68 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `OCC` | 4 | $14.10 | $0.60 | $-17.90 | $9,628.01 | ▼ -17.90 after sell → book $9,707.06; vs 09:30 mark -0.59 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `ALM` | 5 | $15.81 | $0.83 | $-3.60 | $9,706.23 | ▼ -3.60 after sell → book $9,706.23; vs 09:30 mark -0.83 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `DNA` | 186 | $7.45 | $2.55 | — | $8,317.98 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+6.9; leftover $1386.60 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MSTR` | 12 | $113.23 | $2.03 | — | $6,957.20 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1386.60 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `EXK` | 128 | $10.77 | $2.37 | — | $5,576.26 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+3.6; leftover $1386.60 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SCZM` | 146 | $9.46 | $2.43 | — | $4,192.68 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ⚪; ret5=+7.6; leftover $1386.60 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NG` | 165 | $8.38 | $2.48 | — | $2,807.49 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+7.5; leftover $1386.60 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BLSH` | 47 | $29.20 | $2.13 | — | $1,432.96 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+9.3; leftover $1386.60 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HYMC` | 50 | $27.25 | $2.14 | — | $68.32 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable; 🔵; ret5=+1.6; leftover $1386.60 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $68.32 | ▼ close $9,613.26 vs 09:30 $9,710.08 (session -76.84) | 16:00 close · cash $68.32 · equity $9,613.26 vs 09:30 $9,710.08 (-96.82; session marks -76.84) · 7 name(s) marked open→close (per-name table). DNA×186 09:30 $7.45 → close $6.96 -91.14; MSTR×12 09:30 $113.23 → close $112.39 -10.08; EXK×128 09:30 $10.77 → close $10.97 +25.60; SCZM×146 09:30 $9.46 → close $9.76 +43.80; NG×165 09:30 $8.38 → close $8.66 +46.20; BLSH×47 09:30 $29.20 → close $28.44 -35.72; HYMC×50 09:30 $27.25 → close $26.14 -55.50 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $68.32 | ▲ 09:30 equity $10,029.37 vs yday $9,613.26 (+416.11) | 09:30 open · cash $68.32 (unchanged overnight, no fees) · equity $10,029.37 vs prior close $9,613.26 (+416.11) · 7 name(s) re-marked at the open (per-name table). DNA×186 yday $6.96 → 09:30 $7.09 +24.18; MSTR×12 yday $112.39 → 09:30 $119.69 +87.60; EXK×128 yday $10.97 → 09:30 $11.34 +47.36; SCZM×146 yday $9.76 → 09:30 $10.26 +73.00; NG×165 yday $8.66 → 09:30 $9.02 +59.40; BLSH×47 yday $28.44 → 09:30 $29.75 +61.57; HYMC×50 yday $26.14 → 09:30 $27.40 +63.00 | — |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 8 | $1.66 | $0.16 | — | $54.88 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $13.66 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 15 | $0.86 | $0.17 | — | $41.75 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,ohlc_hot; ret5=+9.7; leftover $13.66 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `GORO` | 4 | $3.11 | $0.14 | — | $29.17 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; ret5=+7.1; leftover $13.66 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $29.17 | ▼ close $9,868.25 vs 09:30 $10,029.37 (session -160.65) | 16:00 close · cash $29.17 · equity $9,868.25 vs 09:30 $10,029.37 (-161.12; session marks -160.65) · 10 name(s) marked open→close (per-name table). DNA×186 09:30 $7.09 → close $7.40 +57.66; MSTR×12 09:30 $119.69 → close $119.25 -5.28; EXK×128 09:30 $11.34 → close $10.62 -92.16; SCZM×146 09:30 $10.26 → close $9.68 -85.41; NG×165 09:30 $9.02 → close $8.72 -49.50; BLSH×47 09:30 $29.75 → close $30.41 +31.02; HYMC×50 09:30 $27.40 → close $27.07 -16.50; BTBT×8 09:30 $1.66 → close $1.53 -1.04; ORBS×15 09:30 $0.86 → close $0.88 +0.24; GORO×4 09:30 $3.11 → close $3.19 +0.32 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $29.17 | ▲ 09:30 equity $9,969.51 vs yday $9,868.25 (+101.26) | 09:30 open · cash $29.17 (unchanged overnight, no fees) · equity $9,969.51 vs prior close $9,868.25 (+101.26) · 10 name(s) re-marked at the open (per-name table). DNA×186 yday $7.40 → 09:30 $7.26 -26.04; MSTR×12 yday $119.25 → 09:30 $121.76 +30.12; EXK×128 yday $10.62 → 09:30 $11.01 +49.92; SCZM×146 yday $9.68 → 09:30 $9.82 +21.17; NG×165 yday $8.72 → 09:30 $8.89 +28.05; BLSH×47 yday $30.41 → 09:30 $30.18 -10.81; HYMC×50 yday $27.07 → 09:30 $27.24 +8.50; BTBT×8 yday $1.53 → 09:30 $1.55 +0.16; ORBS×15 yday $0.88 → 09:30 $0.89 +0.15; GORO×4 yday $3.19 → 09:30 $3.20 +0.04 | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $29.17 | ▼ close $9,896.10 vs 09:30 $9,969.51 (session -73.41) | 16:00 close · cash $29.17 · equity $9,896.10 vs 09:30 $9,969.51 (-73.41; session marks -73.41) · 10 name(s) marked open→close (per-name table). DNA×186 09:30 $7.26 → close $6.98 -52.08; MSTR×12 09:30 $121.76 → close $124.59 +33.96; EXK×128 09:30 $11.01 → close $10.74 -34.56; SCZM×146 09:30 $9.82 → close $9.53 -42.34; NG×165 09:30 $8.89 → close $9.24 +57.75; BLSH×47 09:30 $30.18 → close $30.88 +32.90; HYMC×50 09:30 $27.24 → close $25.84 -70.00; BTBT×8 09:30 $1.55 → close $1.56 +0.08; ORBS×15 09:30 $0.89 → close $0.85 -0.60; GORO×4 09:30 $3.20 → close $3.57 +1.48 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $29.17 | ▲ 09:30 equity $9,897.66 vs yday $9,896.10 (+1.56) | 09:30 open · cash $29.17 (unchanged overnight, no fees) · equity $9,897.66 vs prior close $9,896.10 (+1.56) · 10 name(s) re-marked at the open (per-name table). DNA×186 yday $6.98 → 09:30 $6.82 -29.76; MSTR×12 yday $124.59 → 09:30 $125.56 +11.64; EXK×128 yday $10.74 → 09:30 $10.72 -2.56; SCZM×146 yday $9.53 → 09:30 $9.57 +5.84; NG×165 yday $9.24 → 09:30 $9.34 +16.50; BLSH×47 yday $30.88 → 09:30 $31.00 +5.64; HYMC×50 yday $25.84 → 09:30 $25.73 -5.50; BTBT×8 yday $1.56 → 09:30 $1.55 -0.08; ORBS×15 yday $0.85 → 09:30 $0.85 +0.00; GORO×4 yday $3.57 → 09:30 $3.53 -0.16 | — |
| 2026-08-25 09:30 ET | **SELL** | `DNA` | 186 | $6.82 | $2.59 | $-122.32 | $1,295.10 | ▼ -122.32 after sell → book $9,895.07; vs 09:30 mark -2.59 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `MSTR` | 12 | $125.56 | $2.05 | $+143.89 | $2,799.78 | ▲ +143.89 after sell → book $9,893.02; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `EXK` | 128 | $10.72 | $2.41 | $-11.18 | $4,169.53 | ▼ -11.18 after sell → book $9,890.62; vs 09:30 mark -2.40 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `SCZM` | 146 | $9.57 | $2.46 | $+11.17 | $5,564.29 | ▲ +11.17 after sell → book $9,888.16; vs 09:30 mark -2.46 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NG` | 165 | $9.34 | $2.52 | $+153.39 | $7,102.86 | ▲ +153.39 after sell → book $9,885.63; vs 09:30 mark -2.53 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BLSH` | 47 | $31.00 | $2.15 | $+80.32 | $8,557.71 | ▲ +80.32 after sell → book $9,883.48; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HYMC` | 50 | $25.73 | $2.16 | $-80.30 | $9,842.05 | ▼ -80.30 after sell → book $9,881.32; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 1230 | $2.00 | $15.87 | — | $7,366.18 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.8; leftover $2460.51 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 471 | $5.22 | $6.08 | — | $4,901.48 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+9.2; leftover $2460.51 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALIT` | 165 | $14.86 | $2.48 | — | $2,447.10 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+3.0; leftover $2460.51 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟡 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 382 | $6.38 | $4.93 | — | $5.01 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ⚪; ret5=+5.0; leftover $2460.51 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5.01 | ▲ close $9,937.99 vs 09:30 $9,897.66 (session +86.03) | 16:00 close · cash $5.01 · equity $9,937.99 vs 09:30 $9,897.66 (+40.33; session marks +86.03) · 7 name(s) marked open→close (per-name table). BTBT×8 09:30 $1.55 → close $1.53 -0.16; ORBS×15 09:30 $0.85 → close $0.84 -0.15; GORO×4 09:30 $3.53 → close $3.56 +0.12; NPWR×1230 09:30 $2.00 → close $2.02 +24.60; ALVO×471 09:30 $5.22 → close $5.25 +14.13; ALIT×165 09:30 $14.86 → close $14.87 +1.65; ZURA×382 09:30 $6.38 → close $6.50 +45.84 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5.01 | ▲ 09:30 equity $9,937.99 vs yday $9,937.99 (+0.00) | 09:30 open · cash $5.01 (unchanged overnight, no fees) · equity $9,937.99 vs prior close $9,937.99 (+0.00) · 7 name(s) re-marked at the open (per-name table). BTBT×8 yday $1.53 → 09:30 $1.53 +0.00; ORBS×15 yday $0.84 → 09:30 $0.84 +0.00; GORO×4 yday $3.56 → 09:30 $3.56 +0.00; NPWR×1230 yday $2.02 → 09:30 $2.02 +0.00; ALVO×471 yday $5.25 → 09:30 $5.25 +0.00; ALIT×165 yday $14.87 → 09:30 $14.87 +0.00; ZURA×382 yday $6.50 → 09:30 $6.50 +0.00 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5.01 | ▲ close $9,937.99 vs 09:30 $9,937.99 (session +0.00) | 16:00 close · cash $5.01 · equity $9,937.99 vs 09:30 $9,937.99 (+0.00; session marks +0.00) · 7 name(s) marked open→close (per-name table). BTBT×8 09:30 $1.53 → close $1.53 +0.00; ORBS×15 09:30 $0.84 → close $0.84 +0.00; GORO×4 09:30 $3.56 → close $3.56 +0.00; NPWR×1230 09:30 $2.02 → close $2.02 +0.00; ALVO×471 09:30 $5.25 → close $5.25 +0.00; ALIT×165 09:30 $14.87 → close $14.87 +0.00; ZURA×382 09:30 $6.50 → close $6.50 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5.01 | ▼ 09:30 equity $9,555.72 vs yday $9,937.99 (-382.27) | 09:30 open · cash $5.01 (unchanged overnight, no fees) · equity $9,555.72 vs prior close $9,937.99 (-382.27) · 7 name(s) re-marked at the open (per-name table). BTBT×8 yday $1.53 → 09:30 $1.53 +0.00; ORBS×15 yday $0.84 → 09:30 $0.80 -0.60; GORO×4 yday $3.56 → 09:30 $3.77 +0.84; NPWR×1230 yday $2.02 → 09:30 $1.93 -110.70; ALVO×471 yday $5.25 → 09:30 $4.98 -127.17; ALIT×165 yday $14.87 → 09:30 $14.85 -3.30; ZURA×382 yday $6.50 → 09:30 $6.13 -141.34 | — |
| 2026-08-27 09:30 ET | **SELL** | `BTBT` | 8 | $1.53 | $0.17 | $-1.36 | $17.09 | ▼ -1.36 after sell → book $9,555.56; vs 09:30 mark -0.16 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `ORBS` | 15 | $0.80 | $0.18 | $-1.32 | $28.90 | ▼ -1.32 after sell → book $9,555.37; vs 09:30 mark -0.19 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `GORO` | 4 | $3.77 | $0.18 | $+2.32 | $43.80 | ▲ +2.32 after sell → book $9,555.19; vs 09:30 mark -0.18 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $43.80 | ▼ close $9,235.34 vs 09:30 $9,555.72 (session -319.85) | 16:00 close · cash $43.80 · equity $9,235.34 vs 09:30 $9,555.72 (-320.38; session marks -319.85) · 4 name(s) marked open→close (per-name table). NPWR×1230 09:30 $1.93 → close $1.81 -147.60; ALVO×471 09:30 $4.98 → close $4.91 -32.97; ALIT×165 09:30 $14.85 → close $14.33 -85.80; ZURA×382 09:30 $6.13 → close $5.99 -53.48 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $43.80 | ▲ 09:30 equity $9,291.92 vs yday $9,235.34 (+56.58) | 09:30 open · cash $43.80 (unchanged overnight, no fees) · equity $9,291.92 vs prior close $9,235.34 (+56.58) · 4 name(s) re-marked at the open (per-name table). NPWR×1230 yday $1.81 → 09:30 $1.83 +24.60; ALVO×471 yday $4.91 → 09:30 $4.88 -14.13; ALIT×165 yday $14.33 → 09:30 $14.54 +34.65; ZURA×382 yday $5.99 → 09:30 $6.02 +11.46 | — |
| 2026-08-28 09:30 ET | **SELL** | `NPWR` | 1230 | $1.83 | $16.09 | $-241.06 | $2,278.61 | ▼ -241.06 after sell → book $9,275.83; vs 09:30 mark -16.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ALVO` | 471 | $4.88 | $6.17 | $-172.39 | $4,570.92 | ▼ -172.39 after sell → book $9,269.66; vs 09:30 mark -6.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ALIT` | 165 | $14.54 | $2.53 | $-57.82 | $6,967.48 | ▼ -57.82 after sell → book $9,267.12; vs 09:30 mark -2.54 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ZURA` | 382 | $6.02 | $5.01 | $-147.46 | $9,262.11 | ▼ -147.46 after sell → book $9,262.11; vs 09:30 mark -5.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 12 | $144.70 | $2.03 | — | $7,523.69 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1852.42 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BHVN` | 109 | $16.95 | $2.32 | — | $5,673.82 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $1852.42 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 100 | $18.50 | $2.29 | — | $3,821.53 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $1852.42 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `LVWR` | 1342 | $1.38 | $17.31 | — | $1,952.26 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; ret5=+0.0; leftover $1852.42 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 116 | $15.94 | $2.34 | — | $100.88 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-1.9; leftover $1852.42 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $100.88 | ▼ close $9,048.64 vs 09:30 $9,291.92 (session -187.19) | 16:00 close · cash $100.88 · equity $9,048.64 vs 09:30 $9,291.92 (-243.28; session marks -187.19) · 5 name(s) marked open→close (per-name table). ANF×12 09:30 $144.70 → close $145.75 +12.60; BHVN×109 09:30 $16.95 → close $16.12 -90.47; BZ×100 09:30 $18.50 → close $18.00 -50.00; LVWR×1342 09:30 $1.38 → close $1.36 -26.84; GRRR×116 09:30 $15.94 → close $15.66 -32.48 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $100.88 | ▼ 09:30 equity $8,856.54 vs yday $9,048.64 (-192.10) | 09:30 open · cash $100.88 (unchanged overnight, no fees) · equity $8,856.54 vs prior close $9,048.64 (-192.10) · 5 name(s) re-marked at the open (per-name table). ANF×12 yday $145.75 → 09:30 $148.67 +35.04; BHVN×109 yday $16.12 → 09:30 $15.44 -74.12; BZ×100 yday $18.00 → 09:30 $17.89 -11.00; LVWR×1342 yday $1.36 → 09:30 $1.37 +13.42; GRRR×116 yday $15.66 → 09:30 $14.32 -155.44 | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $100.88 | ▼ close $8,806.32 vs 09:30 $8,856.54 (session -50.22) | 16:00 close · cash $100.88 · equity $8,806.32 vs 09:30 $8,856.54 (-50.22; session marks -50.22) · 5 name(s) marked open→close (per-name table). ANF×12 09:30 $148.67 → close $149.28 +7.32; BHVN×109 09:30 $15.44 → close $15.40 -4.36; BZ×100 09:30 $17.89 → close $17.90 +1.00; LVWR×1342 09:30 $1.37 → close $1.34 -40.26; GRRR×116 09:30 $14.32 → close $14.20 -13.92 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $100.88 | ▼ 09:30 equity $8,614.61 vs yday $8,806.32 (-191.71) | 09:30 open · cash $100.88 (unchanged overnight, no fees) · equity $8,614.61 vs prior close $8,806.32 (-191.71) · 5 name(s) re-marked at the open (per-name table). ANF×12 yday $149.28 → 09:30 $142.47 -81.72; BHVN×109 yday $15.40 → 09:30 $15.45 +5.45; BZ×100 yday $17.90 → 09:30 $17.37 -53.00; LVWR×1342 yday $1.34 → 09:30 $1.22 -161.04; GRRR×116 yday $14.20 → 09:30 $15.05 +98.60 | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $100.88 | ▼ close $8,518.29 vs 09:30 $8,614.61 (session -96.32) | 16:00 close · cash $100.88 · equity $8,518.29 vs 09:30 $8,614.61 (-96.32; session marks -96.32) · 5 name(s) marked open→close (per-name table). ANF×12 09:30 $142.47 → close $143.00 +6.36; BHVN×109 09:30 $15.45 → close $15.45 +0.00; BZ×100 09:30 $17.37 → close $17.17 -20.00; LVWR×1342 09:30 $1.22 → close $1.18 -53.68; GRRR×116 09:30 $15.05 → close $14.80 -29.00 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $100.88 | ▲ 09:30 equity $8,519.37 vs yday $8,518.29 (+1.08) | 09:30 open · cash $100.88 (unchanged overnight, no fees) · equity $8,519.37 vs prior close $8,518.29 (+1.08) · 5 name(s) re-marked at the open (per-name table). ANF×12 yday $143.00 → 09:30 $142.00 -12.00; BHVN×109 yday $15.45 → 09:30 $15.39 -6.54; BZ×100 yday $17.17 → 09:30 $17.29 +12.00; LVWR×1342 yday $1.18 → 09:30 $1.19 +13.42; GRRR×116 yday $14.80 → 09:30 $14.75 -5.80 | — |
| 2026-09-02 09:30 ET | **SELL** | `ANF` | 12 | $142.00 | $2.05 | $-36.48 | $1,802.83 | ▼ -36.48 after sell → book $8,517.32; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BHVN` | 109 | $15.39 | $2.35 | $-174.71 | $3,477.99 | ▼ -174.71 after sell → book $8,514.97; vs 09:30 mark -2.35 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BZ` | 100 | $17.29 | $2.32 | $-125.61 | $5,204.67 | ▼ -125.61 after sell → book $8,512.65; vs 09:30 mark -2.32 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `LVWR` | 1342 | $1.19 | $17.55 | $-289.84 | $6,784.11 | ▼ -289.84 after sell → book $8,495.11; vs 09:30 mark -17.54 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `GRRR` | 116 | $14.75 | $2.37 | $-142.75 | $8,492.74 | ▼ -142.75 after sell → book $8,492.74; vs 09:30 mark -2.37 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,492.74 | ▲ close $8,492.74 vs 09:30 $8,519.37 (session +0.00) | 16:00 close · cash $8,492.74 · no lots left · equity $8,492.74. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,492.74 | ▲ 09:30 equity $8,492.74 vs yday $8,492.74 (-0.00) | 09:30 open · cash $8,492.74 · no holdings · equity $8,492.74 vs prior close $8,492.74 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 2320 | $1.22 | $29.93 | — | $5,632.41 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $2830.91 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 180 | $15.70 | $2.53 | — | $2,803.88 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $2830.91 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 122 | $22.78 | $2.36 | — | $22.36 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $2830.91 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $22.36 | ▲ close $9,639.08 vs 09:30 $8,492.74 (session +1,181.16) | 16:00 close · cash $22.36 · equity $9,639.08 vs 09:30 $8,492.74 (+1146.34; session marks +1181.16) · 3 name(s) marked open→close (per-name table). GPRO×2320 09:30 $1.22 → close $1.69 +1090.40; CRK×180 09:30 $15.70 → close $15.54 -28.80; MMED×122 09:30 $22.78 → close $23.76 +119.56 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $22.36 | ▲ 09:30 equity $9,846.32 vs yday $9,639.08 (+207.24) | 09:30 open · cash $22.36 (unchanged overnight, no fees) · equity $9,846.32 vs prior close $9,639.08 (+207.24) · 3 name(s) re-marked at the open (per-name table). GPRO×2320 yday $1.69 → 09:30 $1.78 +208.80; CRK×180 yday $15.54 → 09:30 $15.45 -16.20; MMED×122 yday $23.76 → 09:30 $23.88 +14.64 | — |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 3 | $1.95 | $0.07 | — | $16.44 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $7.45 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `EOSE` | 2 | $3.57 | $0.08 | — | $9.23 | — | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.2; leftover $7.45 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9.23 | ▼ close $8,846.33 vs 09:30 $9,846.32 (session -999.85) | 16:00 close · cash $9.23 · equity $8,846.33 vs 09:30 $9,846.32 (-999.99; session marks -999.85) · 5 name(s) marked open→close (per-name table). GPRO×2320 09:30 $1.78 → close $1.39 -904.80; CRK×180 09:30 $15.45 → close $14.95 -90.00; MMED×122 09:30 $23.88 → close $23.84 -4.88; BAK×3 09:30 $1.95 → close $1.94 -0.03; EOSE×2 09:30 $3.57 → close $3.50 -0.14 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `HYLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `WDC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ADUR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ALGM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `CELC` | cash | leftover split 82.30 < 1 share @ 92.99 |
| 2026-08-18 | `ANGX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `HYLN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `WDC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ADUR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ALGM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `CDNL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `ABX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `VERA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `OCC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `ALM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `PGEN` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `NXE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `CDNL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `ABX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `VERA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `OCC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `ALM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `GUTS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `RANI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SECZ` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ULTA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNDY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `DNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `MSTR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `EXK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `SCZM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BLSH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HYMC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `DE` | cash | leftover split 13.66 < 1 share @ 623.26 |
| 2026-08-21 | `QDEL` | cash | leftover split 13.66 < 1 share @ 14.96 |
| 2026-08-24 | `DNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `MSTR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `EXK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `SCZM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BLSH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HYMC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ORBS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `GORO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `USAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ALOY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ORBS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `GORO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `BTBT` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ORBS` | no_price | no 09:30 open — carry |
| 2026-08-26 | `GORO` | no_price | no 09:30 open — carry |
| 2026-08-26 | `NPWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ALVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ALIT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ZURA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `KURA` | no_price | no 09:30 open |
| 2026-08-26 | `ABX` | no_price | no 09:30 open |
| 2026-08-26 | `ITG` | no_price | no 09:30 open |
| 2026-08-27 | `NPWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ALVO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ALIT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ZURA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `ANF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BHVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BZ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `LVWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `GRRR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RPD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ACDC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `ANF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BHVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `LVWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `GRRR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `NMRA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NTNX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NAGE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TII` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `AVXL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `KMX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MMED` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DELL` | cash | leftover split 7.45 < 1 share @ 486.31 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `GPRO` | 2320 | 2026-09-03 @ $1.22 | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $2830.91 |
| `CRK` | 180 | 2026-09-03 @ $15.70 | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $2830.91 |
| `MMED` | 122 | 2026-09-03 @ $22.78 | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $2830.91 |
| `BAK` | 3 | 2026-09-04 @ $1.95 | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $7.45 |
| `EOSE` | 2 | 2026-09-04 @ $3.57 | combo gate; gate last_green=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.2; leftover $7.45 |
