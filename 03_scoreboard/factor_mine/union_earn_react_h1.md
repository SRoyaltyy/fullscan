# Factor mine action — `union_earn_react_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ earn_react, no 🚨

Cash book **+1.42%** ($10,142) · signal-only (no cash/fees) was -10.92%. Starts YES **4/17**. Fills 104 · skips 40 · realized $+149.88.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `earn_react=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $223.46.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `INO` | 6172 | — | $0.81 | +0.00 | $0.90 | +555.48 | +555.48 | +0.00 | +555.48 |
| 2026-08-13 | `VOR` | 223 | — | $22.01 | +0.00 | $23.29 | +285.44 | +285.44 | +0.00 | +285.44 |
| 2026-08-14 | `INO` | 6172 | $0.90 | $0.93 | +185.16 | — | +0.00 | +185.16 | +740.64 | — |
| 2026-08-14 | `VOR` | 223 | $23.29 | $23.33 | +8.92 | — | +0.00 | +8.92 | +294.36 | — |
| 2026-08-14 | `NMAX` | 137 | — | $9.89 | +0.00 | $10.87 | +133.57 | +133.57 | +0.00 | +133.57 |
| 2026-08-14 | `AIRJ` | 246 | — | $5.51 | +0.00 | $6.04 | +130.38 | +130.38 | +0.00 | +130.38 |
| 2026-08-14 | `AMAT` | 2 | — | $499.40 | +0.00 | $507.18 | +15.56 | +15.56 | +0.00 | +15.56 |
| 2026-08-14 | `AMPG` | 311 | — | $4.37 | +0.00 | $4.00 | -116.00 | -116.00 | +0.00 | -116.00 |
| 2026-08-14 | `BRUN` | 51 | — | $26.25 | +0.00 | $22.93 | -169.07 | -169.07 | +0.00 | -169.07 |
| 2026-08-14 | `BZAI` | 1776 | — | $0.77 | +0.00 | $0.59 | -307.25 | -307.25 | +0.00 | -307.25 |
| 2026-08-14 | `DEFT` | 2894 | — | $0.47 | +0.00 | $0.49 | +54.99 | +54.99 | +0.00 | +54.99 |
| 2026-08-14 | `DGXX` | 347 | — | $3.92 | +0.00 | $3.97 | +17.35 | +17.35 | +0.00 | +17.35 |
| 2026-08-17 | `NMAX` | 137 | $10.87 | $10.97 | +13.70 | — | +0.00 | +13.70 | +147.28 | — |
| 2026-08-17 | `AIRJ` | 246 | $6.04 | $6.22 | +44.28 | — | +0.00 | +44.28 | +174.66 | — |
| 2026-08-17 | `AMAT` | 2 | $507.18 | $517.45 | +20.53 | — | +0.00 | +20.53 | +36.09 | — |
| 2026-08-17 | `AMPG` | 311 | $4.00 | $4.09 | +29.54 | — | +0.00 | +29.54 | -86.46 | — |
| 2026-08-17 | `BRUN` | 51 | $22.93 | $23.00 | +3.57 | — | +0.00 | +3.57 | -165.50 | — |
| 2026-08-17 | `BZAI` | 1776 | $0.59 | $0.55 | -72.82 | — | +0.00 | -72.82 | -380.06 | — |
| 2026-08-17 | `DEFT` | 2894 | $0.49 | $0.47 | -40.52 | — | +0.00 | -40.52 | +14.47 | — |
| 2026-08-17 | `DGXX` | 347 | $3.97 | $3.96 | -3.47 | — | +0.00 | -3.47 | +13.88 | — |
| 2026-08-18 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `AAP` | 28 | — | $46.85 | +0.00 | $42.39 | -124.88 | -124.88 | +0.00 | -124.88 |
| 2026-08-20 | `AEG` | 145 | — | $9.01 | +0.00 | $9.01 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-20 | `ALVO` | 338 | — | $3.89 | +0.00 | $4.27 | +128.44 | +128.44 | +0.00 | +128.44 |
| 2026-08-20 | `ATAT` | 38 | — | $34.05 | +0.00 | $34.25 | +7.60 | +7.60 | +0.00 | +7.60 |
| 2026-08-20 | `ATHM` | 58 | — | $22.44 | +0.00 | $22.12 | -18.56 | -18.56 | +0.00 | -18.56 |
| 2026-08-20 | `BABA` | 10 | — | $123.47 | +0.00 | $130.53 | +70.60 | +70.60 | +0.00 | +70.60 |
| 2026-08-20 | `BILL` | 26 | — | $49.00 | +0.00 | $47.40 | -41.60 | -41.60 | +0.00 | -41.60 |
| 2026-08-20 | `BULL` | 132 | — | $9.94 | +0.00 | $8.85 | -143.88 | -143.88 | +0.00 | -143.88 |
| 2026-08-21 | `AAP` | 28 | $42.39 | $42.41 | +0.56 | — | +0.00 | +0.56 | -124.32 | — |
| 2026-08-21 | `AEG` | 145 | $9.01 | $9.04 | +4.35 | — | +0.00 | +4.35 | +4.35 | — |
| 2026-08-21 | `ALVO` | 338 | $4.27 | $4.32 | +16.90 | — | +0.00 | +16.90 | +145.34 | — |
| 2026-08-21 | `ATAT` | 38 | $34.25 | $34.31 | +2.28 | — | +0.00 | +2.28 | +9.88 | — |
| 2026-08-21 | `ATHM` | 58 | $22.12 | $22.20 | +4.64 | — | +0.00 | +4.64 | -13.92 | — |
| 2026-08-21 | `BABA` | 10 | $130.53 | $125.35 | -51.80 | — | +0.00 | -51.80 | +18.80 | — |
| 2026-08-21 | `BILL` | 26 | $47.40 | $47.50 | +2.60 | — | +0.00 | +2.60 | -39.00 | — |
| 2026-08-21 | `BULL` | 132 | $8.85 | $8.99 | +18.48 | — | +0.00 | +18.48 | -125.40 | — |
| 2026-08-21 | `BEKE` | 115 | — | $17.93 | +0.00 | $17.75 | -21.27 | -21.27 | +0.00 | -21.27 |
| 2026-08-21 | `BJ` | 22 | — | $93.98 | +0.00 | $96.42 | +53.68 | +53.68 | +0.00 | +53.68 |
| 2026-08-21 | `BKE` | 48 | — | $43.08 | +0.00 | $43.81 | +35.04 | +35.04 | +0.00 | +35.04 |
| 2026-08-21 | `PSEC` | 900 | — | $2.30 | +0.00 | $2.33 | +27.00 | +27.00 | +0.00 | +27.00 |
| 2026-08-21 | `ROST` | 8 | — | $243.85 | +0.00 | $239.04 | -38.48 | -38.48 | +0.00 | -38.48 |
| 2026-08-24 | `BEKE` | 115 | $17.75 | $18.06 | +35.65 | — | +0.00 | +35.65 | +14.38 | — |
| 2026-08-24 | `BJ` | 22 | $96.42 | $97.02 | +13.20 | — | +0.00 | +13.20 | +66.88 | — |
| 2026-08-24 | `BKE` | 48 | $43.81 | $44.54 | +35.04 | — | +0.00 | +35.04 | +70.08 | — |
| 2026-08-24 | `PSEC` | 900 | $2.33 | $2.34 | +9.00 | — | +0.00 | +9.00 | +36.00 | — |
| 2026-08-24 | `ROST` | 8 | $239.04 | $238.16 | -7.04 | — | +0.00 | -7.04 | -45.52 | — |
| 2026-08-25 | `BMO` | 7 | — | $172.40 | +0.00 | $175.00 | +18.20 | +18.20 | +0.00 | +18.20 |
| 2026-08-25 | `BNS` | 15 | — | $86.86 | +0.00 | $90.08 | +48.30 | +48.30 | +0.00 | +48.30 |
| 2026-08-25 | `BZ` | 85 | — | $15.34 | +0.00 | $16.32 | +83.30 | +83.30 | +0.00 | +83.30 |
| 2026-08-25 | `DKS` | 7 | — | $179.33 | +0.00 | $156.70 | -158.41 | -158.41 | +0.00 | -158.41 |
| 2026-08-25 | `EH` | 234 | — | $5.57 | +0.00 | $5.28 | -67.86 | -67.86 | +0.00 | -67.86 |
| 2026-08-25 | `GFI` | 27 | — | $47.68 | +0.00 | $48.36 | +18.36 | +18.36 | +0.00 | +18.36 |
| 2026-08-25 | `GRRR` | 91 | — | $14.26 | +0.00 | $14.20 | -5.46 | -5.46 | +0.00 | -5.46 |
| 2026-08-25 | `SHMD` | 277 | — | $4.71 | +0.00 | $4.71 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-26 | `BMO` | 7 | $175.00 | $175.00 | +0.00 | $175.00 | +0.00 | +0.00 | +18.20 | +18.20 |
| 2026-08-26 | `BNS` | 15 | $90.08 | $90.08 | +0.00 | $90.08 | +0.00 | +0.00 | +48.30 | +48.30 |
| 2026-08-26 | `BZ` | 85 | $16.32 | $16.32 | +0.00 | $16.32 | +0.00 | +0.00 | +83.30 | +83.30 |
| 2026-08-26 | `DKS` | 7 | $156.70 | $156.70 | +0.00 | $156.70 | +0.00 | +0.00 | -158.41 | -158.41 |
| 2026-08-26 | `EH` | 234 | $5.28 | $5.28 | +0.00 | $5.28 | +0.00 | +0.00 | -67.86 | -67.86 |
| 2026-08-26 | `GFI` | 27 | $48.36 | $48.36 | +0.00 | $48.36 | +0.00 | +0.00 | +18.36 | +18.36 |
| 2026-08-26 | `GRRR` | 91 | $14.20 | $14.20 | +0.00 | $14.20 | +0.00 | +0.00 | -5.46 | -5.46 |
| 2026-08-26 | `SHMD` | 277 | $4.71 | $4.71 | +0.00 | $4.71 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-27 | `BMO` | 7 | $175.00 | $173.22 | -12.46 | — | +0.00 | -12.46 | +5.74 | — |
| 2026-08-27 | `BNS` | 15 | $90.08 | $92.64 | +38.40 | — | +0.00 | +38.40 | +86.70 | — |
| 2026-08-27 | `BZ` | 85 | $16.32 | $16.77 | +38.25 | — | +0.00 | +38.25 | +121.55 | — |
| 2026-08-27 | `DKS` | 7 | $156.70 | $121.87 | -243.81 | — | +0.00 | -243.81 | -402.22 | — |
| 2026-08-27 | `EH` | 234 | $5.28 | $4.77 | -119.34 | — | +0.00 | -119.34 | -187.20 | — |
| 2026-08-27 | `GFI` | 27 | $48.36 | $48.24 | -3.24 | — | +0.00 | -3.24 | +15.12 | — |
| 2026-08-27 | `GRRR` | 91 | $14.20 | $14.03 | -15.47 | — | +0.00 | -15.47 | -20.93 | — |
| 2026-08-27 | `SHMD` | 277 | $4.71 | $3.38 | -368.41 | — | +0.00 | -368.41 | -368.41 | — |
| 2026-08-27 | `NVDA` | 45 | — | $212.64 | +0.00 | $209.66 | -134.10 | -134.10 | +0.00 | -134.10 |
| 2026-08-28 | `NVDA` | 45 | $209.66 | $222.86 | +594.00 | — | +0.00 | +594.00 | +459.90 | — |
| 2026-08-28 | `ADSK` | 4 | — | $261.47 | +0.00 | $270.58 | +36.44 | +36.44 | +0.00 | +36.44 |
| 2026-08-28 | `BBAR` | 84 | — | $14.96 | +0.00 | $14.60 | -30.24 | -30.24 | +0.00 | -30.24 |
| 2026-08-28 | `ESTC` | 15 | — | $82.64 | +0.00 | $83.74 | +16.50 | +16.50 | +0.00 | +16.50 |
| 2026-08-28 | `FINV` | 297 | — | $4.26 | +0.00 | $4.02 | -71.28 | -71.28 | +0.00 | -71.28 |
| 2026-08-28 | `FRO` | 29 | — | $42.51 | +0.00 | $43.75 | +35.96 | +35.96 | +0.00 | +35.96 |
| 2026-08-28 | `GAP` | 61 | — | $20.75 | +0.00 | $20.79 | +2.44 | +2.44 | +0.00 | +2.44 |
| 2026-08-28 | `HAFN` | 160 | — | $7.91 | +0.00 | $8.29 | +60.80 | +60.80 | +0.00 | +60.80 |
| 2026-08-28 | `IREN` | 31 | — | $40.65 | +0.00 | $40.53 | -3.72 | -3.72 | +0.00 | -3.72 |
| 2026-08-31 | `ADSK` | 4 | $270.58 | $258.50 | -48.32 | — | +0.00 | -48.32 | -11.88 | — |
| 2026-08-31 | `BBAR` | 84 | $14.60 | $14.50 | -8.40 | — | +0.00 | -8.40 | -38.64 | — |
| 2026-08-31 | `ESTC` | 15 | $83.74 | $99.99 | +243.75 | — | +0.00 | +243.75 | +260.25 | — |
| 2026-08-31 | `FINV` | 297 | $4.02 | $3.46 | -166.32 | — | +0.00 | -166.32 | -237.60 | — |
| 2026-08-31 | `FRO` | 29 | $43.75 | $43.54 | -6.09 | — | +0.00 | -6.09 | +29.87 | — |
| 2026-08-31 | `GAP` | 61 | $20.79 | $22.89 | +128.10 | — | +0.00 | +128.10 | +130.54 | — |
| 2026-08-31 | `HAFN` | 160 | $8.29 | $8.43 | +22.40 | — | +0.00 | +22.40 | +83.20 | — |
| 2026-08-31 | `IREN` | 31 | $40.53 | $35.71 | -149.42 | — | +0.00 | -149.42 | -153.14 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `AI` | 123 | — | $10.30 | +0.00 | $10.52 | +27.06 | +27.06 | +0.00 | +27.06 |
| 2026-09-03 | `AVGO` | 3 | — | $369.68 | +0.00 | $367.24 | -7.32 | -7.32 | +0.00 | -7.32 |
| 2026-09-03 | `CHPT` | 239 | — | $5.30 | +0.00 | $5.19 | -26.29 | -26.29 | +0.00 | -26.29 |
| 2026-09-03 | `CIEN` | 3 | — | $357.25 | +0.00 | $354.16 | -9.27 | -9.27 | +0.00 | -9.27 |
| 2026-09-03 | `CPB` | 53 | — | $23.80 | +0.00 | $23.78 | -1.06 | -1.06 | +0.00 | -1.06 |
| 2026-09-03 | `FIVE` | 5 | — | $244.98 | +0.00 | $243.08 | -9.50 | -9.50 | +0.00 | -9.50 |
| 2026-09-03 | `HPE` | 24 | — | $51.99 | +0.00 | $51.83 | -3.84 | -3.84 | +0.00 | -3.84 |
| 2026-09-03 | `MEI` | 69 | — | $18.22 | +0.00 | $18.10 | -8.28 | -8.28 | +0.00 | -8.28 |
| 2026-09-04 | `AI` | 123 | $10.52 | $10.74 | +27.06 | — | +0.00 | +27.06 | +54.12 | — |
| 2026-09-04 | `AVGO` | 3 | $367.24 | $351.74 | -46.50 | — | +0.00 | -46.50 | -53.82 | — |
| 2026-09-04 | `CHPT` | 239 | $5.19 | $6.90 | +408.69 | — | +0.00 | +408.69 | +382.40 | — |
| 2026-09-04 | `CIEN` | 3 | $354.16 | $354.49 | +0.99 | — | +0.00 | +0.99 | -8.28 | — |
| 2026-09-04 | `CPB` | 53 | $23.78 | $22.32 | -77.38 | — | +0.00 | -77.38 | -78.44 | — |
| 2026-09-04 | `FIVE` | 5 | $243.08 | $256.99 | +69.55 | — | +0.00 | +69.55 | +60.05 | — |
| 2026-09-04 | `HPE` | 24 | $51.83 | $47.60 | -101.52 | — | +0.00 | -101.52 | -105.36 | — |
| 2026-09-04 | `MEI` | 69 | $18.10 | $15.09 | -207.69 | — | +0.00 | -207.69 | -215.97 | — |
| 2026-09-04 | `AMBA` | 19 | — | $66.61 | +0.00 | $63.38 | -61.37 | -61.37 | +0.00 | -61.37 |
| 2026-09-04 | `ASAN` | 124 | — | $10.16 | +0.00 | $10.09 | -8.68 | -8.68 | +0.00 | -8.68 |
| 2026-09-04 | `DOCU` | 18 | — | $67.06 | +0.00 | $65.97 | -19.62 | -19.62 | +0.00 | -19.62 |
| 2026-09-04 | `DOMO` | 335 | — | $3.78 | +0.00 | $3.79 | +3.35 | +3.35 | +0.00 | +3.35 |
| 2026-09-04 | `GWRE` | 6 | — | $198.00 | +0.00 | $202.86 | +29.16 | +29.16 | +0.00 | +29.16 |
| 2026-09-04 | `IOT` | 33 | — | $37.69 | +0.00 | $38.75 | +34.98 | +34.98 | +0.00 | +34.98 |
| 2026-09-04 | `LULU` | 10 | — | $121.15 | +0.00 | $121.77 | +6.20 | +6.20 | +0.00 | +6.20 |
| 2026-09-04 | `MAMA` | 81 | — | $15.62 | +0.00 | $15.96 | +27.54 | +27.54 | +0.00 | +27.54 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +840.92 | INO, VOR | — | $21.06 | $10,769.53 | INO×6172, VOR×223 |
| 2026-08-14 | +5.50 | $21.06 | INO×6172, VOR×223 | $10,963.61 | +194.08 | -240.47 | NMAX, AIRJ, AMAT, AMPG, BRUN, BZAI, DEFT, DGXX | INO, VOR | $336.60 | $10,583.79 | NMAX×137, AIRJ×246, AMAT×2, AMPG×311, BRUN×51, BZAI×1776, DEFT×2894, DGXX×347 |
| 2026-08-17 | +2.25 | $336.60 | NMAX×137, AIRJ×246, AMAT×2, AMPG×311, BRUN×51, BZAI×1776, DEFT×2894, DGXX×347 | $10,578.62 | -5.17 | +0.00 | — | NMAX, AIRJ, AMAT, AMPG, BRUN, BZAI, DEFT, DGXX | $10,521.80 | $10,521.80 | — |
| 2026-08-18 | -6.20 | $10,521.80 | — | $10,521.80 | +0.00 | +0.00 | — | — | $10,521.80 | $10,521.80 | — |
| 2026-08-19 | -7.20 | $10,521.80 | — | $10,521.80 | +0.00 | +0.00 | — | — | $10,521.80 | $10,521.80 | — |
| 2026-08-20 | +1.12 | $10,521.80 | — | $10,521.80 | +0.00 | -122.28 | AAP, AEG, ALVO, ATAT, ATHM, BABA, BILL, BULL | — | $152.93 | $10,379.92 | AAP×28, AEG×145, ALVO×338, ATAT×38, ATHM×58, BABA×10, BILL×26, BULL×132 |
| 2026-08-21 | +3.25 | $152.93 | AAP×28, AEG×145, ALVO×338, ATAT×38, ATHM×58, BABA×10, BILL×26, BULL×132 | $10,377.93 | -1.99 | +55.97 | BEKE, BJ, BKE, PSEC, ROST | AAP, AEG, ALVO, ATAT, ATHM, BABA, BILL, BULL | $119.22 | $10,393.91 | BEKE×115, BJ×22, BKE×48, PSEC×900, ROST×8 |
| 2026-08-24 | -5.17 | $119.22 | BEKE×115, BJ×22, BKE×48, PSEC×900, ROST×8 | $10,479.76 | +85.85 | +0.00 | — | BEKE, BJ, BKE, PSEC, ROST | $10,459.33 | $10,459.33 | — |
| 2026-08-25 | +1.80 | $10,459.33 | — | $10,459.33 | -0.00 | -63.57 | BMO, BNS, BZ, DKS, EH, GFI, GRRR, SHMD | — | $178.12 | $10,376.53 | BMO×7, BNS×15, BZ×85, DKS×7, EH×234, GFI×27, GRRR×91, SHMD×277 |
| 2026-08-26 | +2.02 | $178.12 | BMO×7, BNS×15, BZ×85, DKS×7, EH×234, GFI×27, GRRR×91, SHMD×277 | $10,376.53 | +0.00 | +0.00 | — | — | $178.12 | $10,376.53 | BMO×7, BNS×15, BZ×85, DKS×7, EH×234, GFI×27, GRRR×91, SHMD×277 |
| 2026-08-27 | — | $178.12 | BMO×7, BNS×15, BZ×85, DKS×7, EH×234, GFI×27, GRRR×91, SHMD×277 | $9,690.45 | -686.08 | -134.10 | NVDA | BMO, BNS, BZ, DKS, EH, GFI, GRRR, SHMD | $100.06 | $9,534.76 | NVDA×45 |
| 2026-08-28 | +0.75 | $100.06 | NVDA×45 | $10,128.76 | +594.00 | +46.90 | ADSK, BBAR, ESTC, FINV, FRO, GAP, HAFN, IREN | NVDA | $276.00 | $10,154.53 | ADSK×4, BBAR×84, ESTC×15, FINV×297, FRO×29, GAP×61, HAFN×160, IREN×31 |
| 2026-08-31 | -5.85 | $276.00 | ADSK×4, BBAR×84, ESTC×15, FINV×297, FRO×29, GAP×61, HAFN×160, IREN×31 | $10,170.23 | +15.70 | +0.00 | — | ADSK, BBAR, ESTC, FINV, FRO, GAP, HAFN, IREN | $10,151.10 | $10,151.10 | — |
| 2026-09-01 | -6.30 | $10,151.10 | — | $10,151.10 | -0.00 | +0.00 | — | — | $10,151.10 | $10,151.10 | — |
| 2026-09-02 | -3.83 | $10,151.10 | — | $10,151.10 | -0.00 | +0.00 | — | — | $10,151.10 | $10,151.10 | — |
| 2026-09-03 | -0.90 | $10,151.10 | — | $10,151.10 | -0.00 | -38.50 | AI, AVGO, CHPT, CIEN, CPB, FIVE, HPE, MEI | — | $427.61 | $10,094.74 | AI×123, AVGO×3, CHPT×239, CIEN×3, CPB×53, FIVE×5, HPE×24, MEI×69 |
| 2026-09-04 | — | $427.61 | AI×123, AVGO×3, CHPT×239, CIEN×3, CPB×53, FIVE×5, HPE×24, MEI×69 | $10,167.94 | +73.20 | +11.56 | AMBA, ASAN, DOCU, DOMO, GWRE, IOT, LULU, MAMA | AI, AVGO, CHPT, CIEN, CPB, FIVE, HPE, MEI | $223.46 | $10,142.32 | AMBA×19, ASAN×124, DOCU×18, DOMO×335, GWRE×6, IOT×33, LULU×10, MAMA×81 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 6172 | $0.81 | $68.51 | — | $4,932.17 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list flatten; ⚪; ret5=+13.2; leftover $5000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 223 | $22.01 | $2.88 | — | $21.06 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list flatten; ⚪; ret5=+0.3; leftover $5000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21.06 | ▲ close $10,769.53 vs 09:30 $10,000.00 (session +840.92) | 16:00 close · cash $21.06 · equity $10,769.53 vs 09:30 $10,000.00 (+769.53; session marks +840.92) · 2 name(s) marked open→close (per-name table). INO×6172 09:30 $0.81 → close $0.90 +555.48; VOR×223 09:30 $22.01 → close $23.29 +285.44 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21.06 | ▲ 09:30 equity $10,963.61 vs yday $10,769.53 (+194.08) | 09:30 open · cash $21.06 (unchanged overnight, no fees) · equity $10,963.61 vs prior close $10,769.53 (+194.08) · 2 name(s) re-marked at the open (per-name table). INO×6172 yday $0.90 → 09:30 $0.93 +185.16; VOR×223 yday $23.29 → 09:30 $23.33 +8.92 | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 6172 | $0.93 | $76.99 | $+595.14 | $5,684.04 | ▲ +595.14 after sell → book $10,886.63; vs 09:30 mark -76.98 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `VOR` | 223 | $23.33 | $2.96 | $+288.53 | $10,883.67 | ▲ +288.53 after sell → book $10,883.67; vs 09:30 mark -2.96 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `NMAX` | 137 | $9.89 | $2.40 | — | $9,525.66 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list ohlc_hot,earn_react; 🔵; ⚪; ret5=+10.9; leftover $1360.46 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRJ` | 246 | $5.51 | $3.17 | — | $8,167.02 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=+13.1; leftover $1360.46 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AMAT` | 2 | $499.40 | $2.00 | — | $7,166.23 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+1.3; leftover $1360.46 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AMPG` | 311 | $4.37 | $4.01 | — | $5,803.77 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+10.3; leftover $1360.46 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BRUN` | 51 | $26.25 | $2.14 | — | $4,463.13 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=+31.2; leftover $1360.46 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BZAI` | 1776 | $0.77 | $18.93 | — | $3,083.78 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=+20.4; leftover $1360.46 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `DEFT` | 2894 | $0.47 | $22.28 | — | $1,701.32 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=+11.1; leftover $1360.46 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `DGXX` | 347 | $3.92 | $4.48 | — | $336.60 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=+10.1; leftover $1360.46 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $336.60 | ▼ close $10,583.79 vs 09:30 $10,963.61 (session -240.47) | 16:00 close · cash $336.60 · equity $10,583.79 vs 09:30 $10,963.61 (-379.82; session marks -240.47) · 8 name(s) marked open→close (per-name table). NMAX×137 09:30 $9.89 → close $10.87 +133.57; AIRJ×246 09:30 $5.51 → close $6.04 +130.38; AMAT×2 09:30 $499.40 → close $507.18 +15.56; AMPG×311 09:30 $4.37 → close $4.00 -116.00; BRUN×51 09:30 $26.25 → close $22.93 -169.07; BZAI×1776 09:30 $0.77 → close $0.59 -307.25; DEFT×2894 09:30 $0.47 → close $0.49 +54.99; DGXX×347 09:30 $3.92 → close $3.97 +17.35 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $336.60 | ▼ 09:30 equity $10,578.62 vs yday $10,583.79 (-5.17) | 09:30 open · cash $336.60 (unchanged overnight, no fees) · equity $10,578.62 vs prior close $10,583.79 (-5.17) · 8 name(s) re-marked at the open (per-name table). NMAX×137 yday $10.87 → 09:30 $10.97 +13.70; AIRJ×246 yday $6.04 → 09:30 $6.22 +44.28; AMAT×2 yday $507.18 → 09:30 $517.45 +20.53; AMPG×311 yday $4.00 → 09:30 $4.09 +29.54; BRUN×51 yday $22.93 → 09:30 $23.00 +3.57; BZAI×1776 yday $0.59 → 09:30 $0.55 -72.82; DEFT×2894 yday $0.49 → 09:30 $0.47 -40.52; DGXX×347 yday $3.97 → 09:30 $3.96 -3.47 | — |
| 2026-08-17 09:30 ET | **SELL** | `NMAX` | 137 | $10.97 | $2.44 | $+142.44 | $1,837.06 | ▲ +142.44 after sell → book $10,576.18; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `AIRJ` | 246 | $6.22 | $3.23 | $+168.26 | $3,363.95 | ▲ +168.26 after sell → book $10,572.95; vs 09:30 mark -3.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AMAT` | 2 | $517.45 | $2.02 | $+32.08 | $4,396.83 | ▲ +32.08 after sell → book $10,570.94; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AMPG` | 311 | $4.09 | $4.07 | $-94.54 | $5,664.74 | ▼ -94.54 after sell → book $10,566.86; vs 09:30 mark -4.08 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `BRUN` | 51 | $23.00 | $2.16 | $-169.80 | $6,835.58 | ▼ -169.80 after sell → book $10,564.70; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BZAI` | 1776 | $0.55 | $15.44 | $-414.43 | $7,800.50 | ▼ -414.43 after sell → book $10,549.27; vs 09:30 mark -15.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `DEFT` | 2894 | $0.47 | $22.92 | $-30.73 | $9,152.23 | ▼ -30.73 after sell → book $10,526.35; vs 09:30 mark -22.92 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `DGXX` | 347 | $3.96 | $4.54 | $+4.86 | $10,521.80 | ▲ +4.86 after sell → book $10,521.80; vs 09:30 mark -4.55 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,521.80 | ▲ close $10,521.80 vs 09:30 $10,578.62 (session +0.00) | 16:00 close · cash $10,521.80 · no lots left · equity $10,521.80. | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,521.80 | ▲ 09:30 equity $10,521.80 vs yday $10,521.80 (+0.00) | 09:30 open · cash $10,521.80 · no holdings · equity $10,521.80 vs prior close $10,521.80 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,521.80 | ▲ close $10,521.80 vs 09:30 $10,521.80 (session +0.00) | 16:00 close · cash $10,521.80 · no lots left · equity $10,521.80. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,521.80 | ▲ 09:30 equity $10,521.80 vs yday $10,521.80 (+0.00) | 09:30 open · cash $10,521.80 · no holdings · equity $10,521.80 vs prior close $10,521.80 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,521.80 | ▲ close $10,521.80 vs 09:30 $10,521.80 (session +0.00) | 16:00 close · cash $10,521.80 · no lots left · equity $10,521.80. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,521.80 | ▲ 09:30 equity $10,521.80 vs yday $10,521.80 (+0.00) | 09:30 open · cash $10,521.80 · no holdings · equity $10,521.80 vs prior close $10,521.80 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `AAP` | 28 | $46.85 | $2.07 | — | $9,207.93 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+5.0; leftover $1315.23 | join🔴 sector🔴 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AEG` | 145 | $9.01 | $2.42 | — | $7,899.05 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=-1.3; leftover $1315.23 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ALVO` | 338 | $3.89 | $4.36 | — | $6,579.87 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-0.5; leftover $1315.23 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ATAT` | 38 | $34.05 | $2.10 | — | $5,283.87 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+9.3; leftover $1315.23 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ATHM` | 58 | $22.44 | $2.16 | — | $3,980.18 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-2.1; leftover $1315.23 | join🔴 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 peer🔴 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BABA` | 10 | $123.47 | $2.02 | — | $2,743.46 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+2.9; leftover $1315.23 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BILL` | 26 | $49.00 | $2.07 | — | $1,467.40 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-2.0; leftover $1315.23 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BULL` | 132 | $9.94 | $2.39 | — | $152.93 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+12.6; leftover $1315.23 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $152.93 | ▼ close $10,379.92 vs 09:30 $10,521.80 (session -122.28) | 16:00 close · cash $152.93 · equity $10,379.92 vs 09:30 $10,521.80 (-141.88; session marks -122.28) · 8 name(s) marked open→close (per-name table). AAP×28 09:30 $46.85 → close $42.39 -124.88; AEG×145 09:30 $9.01 → close $9.01 +0.00; ALVO×338 09:30 $3.89 → close $4.27 +128.44; ATAT×38 09:30 $34.05 → close $34.25 +7.60; ATHM×58 09:30 $22.44 → close $22.12 -18.56; BABA×10 09:30 $123.47 → close $130.53 +70.60; BILL×26 09:30 $49.00 → close $47.40 -41.60; BULL×132 09:30 $9.94 → close $8.85 -143.88 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $152.93 | ▼ 09:30 equity $10,377.93 vs yday $10,379.92 (-1.99) | 09:30 open · cash $152.93 (unchanged overnight, no fees) · equity $10,377.93 vs prior close $10,379.92 (-1.99) · 8 name(s) re-marked at the open (per-name table). AAP×28 yday $42.39 → 09:30 $42.41 +0.56; AEG×145 yday $9.01 → 09:30 $9.04 +4.35; ALVO×338 yday $4.27 → 09:30 $4.32 +16.90; ATAT×38 yday $34.25 → 09:30 $34.31 +2.28; ATHM×58 yday $22.12 → 09:30 $22.20 +4.64; BABA×10 yday $130.53 → 09:30 $125.35 -51.80; BILL×26 yday $47.40 → 09:30 $47.50 +2.60; BULL×132 yday $8.85 → 09:30 $8.99 +18.48 | — |
| 2026-08-21 09:30 ET | **SELL** | `AAP` | 28 | $42.41 | $2.09 | $-128.49 | $1,338.32 | ▼ -128.49 after sell → book $10,375.84; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `AEG` | 145 | $9.04 | $2.46 | $-0.53 | $2,646.66 | ▼ -0.53 after sell → book $10,373.38; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ALVO` | 338 | $4.32 | $4.43 | $+136.55 | $4,102.39 | ▲ +136.55 after sell → book $10,368.95; vs 09:30 mark -4.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ATAT` | 38 | $34.31 | $2.12 | $+5.65 | $5,404.04 | ▲ +5.65 after sell → book $10,366.82; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ATHM` | 58 | $22.20 | $2.18 | $-18.27 | $6,689.46 | ▼ -18.27 after sell → book $10,364.64; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BABA` | 10 | $125.35 | $2.04 | $+14.74 | $7,940.92 | ▲ +14.74 after sell → book $10,362.60; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BILL` | 26 | $47.50 | $2.09 | $-43.16 | $9,173.83 | ▼ -43.16 after sell → book $10,360.51; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BULL` | 132 | $8.99 | $2.42 | $-130.20 | $10,358.09 | ▼ -130.20 after sell → book $10,358.09; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `BEKE` | 115 | $17.93 | $2.33 | — | $8,293.23 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=+0.2; leftover $2071.62 | join🟢 sector🟢 gen🟢 news🟡 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BJ` | 22 | $93.98 | $2.06 | — | $6,223.62 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-2.4; leftover $2071.62 | join🟡 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BKE` | 48 | $43.08 | $2.13 | — | $4,153.64 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-4.9; leftover $2071.62 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `PSEC` | 900 | $2.30 | $11.61 | — | $2,072.03 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-3.0; leftover $2071.62 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ROST` | 8 | $243.85 | $2.01 | — | $119.22 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-6.5; leftover $2071.62 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $119.22 | ▲ close $10,393.91 vs 09:30 $10,377.93 (session +55.97) | 16:00 close · cash $119.22 · equity $10,393.91 vs 09:30 $10,377.93 (+15.98; session marks +55.97) · 5 name(s) marked open→close (per-name table). BEKE×115 09:30 $17.93 → close $17.75 -21.27; BJ×22 09:30 $93.98 → close $96.42 +53.68; BKE×48 09:30 $43.08 → close $43.81 +35.04; PSEC×900 09:30 $2.30 → close $2.33 +27.00; ROST×8 09:30 $243.85 → close $239.04 -38.48 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $119.22 | ▲ 09:30 equity $10,479.76 vs yday $10,393.91 (+85.85) | 09:30 open · cash $119.22 (unchanged overnight, no fees) · equity $10,479.76 vs prior close $10,393.91 (+85.85) · 5 name(s) re-marked at the open (per-name table). BEKE×115 yday $17.75 → 09:30 $18.06 +35.65; BJ×22 yday $96.42 → 09:30 $97.02 +13.20; BKE×48 yday $43.81 → 09:30 $44.54 +35.04; PSEC×900 yday $2.33 → 09:30 $2.34 +9.00; ROST×8 yday $239.04 → 09:30 $238.16 -7.04 | — |
| 2026-08-24 09:30 ET | **SELL** | `BEKE` | 115 | $18.06 | $2.37 | $+9.67 | $2,193.75 | ▲ +9.67 after sell → book $10,477.39; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BJ` | 22 | $97.02 | $2.08 | $+62.74 | $4,326.11 | ▲ +62.74 after sell → book $10,475.31; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `BKE` | 48 | $44.54 | $2.16 | $+65.78 | $6,461.86 | ▲ +65.78 after sell → book $10,473.14; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `PSEC` | 900 | $2.34 | $11.78 | $+12.61 | $8,556.09 | ▲ +12.61 after sell → book $10,461.37; vs 09:30 mark -11.77 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ROST` | 8 | $238.16 | $2.04 | $-49.57 | $10,459.33 | ▼ -49.57 after sell → book $10,459.33; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,459.33 | ▲ close $10,459.33 vs 09:30 $10,479.76 (session +0.00) | 16:00 close · cash $10,459.33 · no lots left · equity $10,459.33. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,459.33 | ▲ 09:30 equity $10,459.33 vs yday $10,459.33 (-0.00) | 09:30 open · cash $10,459.33 · no holdings · equity $10,459.33 vs prior close $10,459.33 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `BMO` | 7 | $172.40 | $2.01 | — | $9,250.52 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-6.1; leftover $1307.42 | join🟢 sector🟡 gen🟡 news🔴 digest🔴 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BNS` | 15 | $86.86 | $2.04 | — | $7,945.58 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-4.3; leftover $1307.42 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BZ` | 85 | $15.34 | $2.25 | — | $6,639.44 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ⚪; ret5=+2.8; leftover $1307.42 | join🟢 sector🟡 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `DKS` | 7 | $179.33 | $2.01 | — | $5,382.12 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-9.3; leftover $1307.42 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `EH` | 234 | $5.57 | $3.02 | — | $4,075.72 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-7.1; leftover $1307.42 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `GFI` | 27 | $47.68 | $2.07 | — | $2,786.29 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ⚪; ret5=+18.8; leftover $1307.42 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `GRRR` | 91 | $14.26 | $2.26 | — | $1,486.36 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-1.9; leftover $1307.42 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SHMD` | 277 | $4.71 | $3.57 | — | $178.12 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-9.9; leftover $1307.42 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $178.12 | ▼ close $10,376.53 vs 09:30 $10,459.33 (session -63.57) | 16:00 close · cash $178.12 · equity $10,376.53 vs 09:30 $10,459.33 (-82.80; session marks -63.57) · 8 name(s) marked open→close (per-name table). BMO×7 09:30 $172.40 → close $175.00 +18.20; BNS×15 09:30 $86.86 → close $90.08 +48.30; BZ×85 09:30 $15.34 → close $16.32 +83.30; DKS×7 09:30 $179.33 → close $156.70 -158.41; EH×234 09:30 $5.57 → close $5.28 -67.86; GFI×27 09:30 $47.68 → close $48.36 +18.36; GRRR×91 09:30 $14.26 → close $14.20 -5.46; SHMD×277 09:30 $4.71 → close $4.71 +0.00 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $178.12 | ▲ 09:30 equity $10,376.53 vs yday $10,376.53 (+0.00) | 09:30 open · cash $178.12 (unchanged overnight, no fees) · equity $10,376.53 vs prior close $10,376.53 (+0.00) · 8 name(s) re-marked at the open (per-name table). BMO×7 yday $175.00 → 09:30 $175.00 +0.00; BNS×15 yday $90.08 → 09:30 $90.08 +0.00; BZ×85 yday $16.32 → 09:30 $16.32 +0.00; DKS×7 yday $156.70 → 09:30 $156.70 +0.00; EH×234 yday $5.28 → 09:30 $5.28 +0.00; GFI×27 yday $48.36 → 09:30 $48.36 +0.00; GRRR×91 yday $14.20 → 09:30 $14.20 +0.00; SHMD×277 yday $4.71 → 09:30 $4.71 +0.00 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $178.12 | ▲ close $10,376.53 vs 09:30 $10,376.53 (session +0.00) | 16:00 close · cash $178.12 · equity $10,376.53 vs 09:30 $10,376.53 (+0.00; session marks +0.00) · 8 name(s) marked open→close (per-name table). BMO×7 09:30 $175.00 → close $175.00 +0.00; BNS×15 09:30 $90.08 → close $90.08 +0.00; BZ×85 09:30 $16.32 → close $16.32 +0.00; DKS×7 09:30 $156.70 → close $156.70 +0.00; EH×234 09:30 $5.28 → close $5.28 +0.00; GFI×27 09:30 $48.36 → close $48.36 +0.00; GRRR×91 09:30 $14.20 → close $14.20 +0.00; SHMD×277 09:30 $4.71 → close $4.71 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $178.12 | ▼ 09:30 equity $9,690.45 vs yday $10,376.53 (-686.08) | 09:30 open · cash $178.12 (unchanged overnight, no fees) · equity $9,690.45 vs prior close $10,376.53 (-686.08) · 8 name(s) re-marked at the open (per-name table). BMO×7 yday $175.00 → 09:30 $173.22 -12.46; BNS×15 yday $90.08 → 09:30 $92.64 +38.40; BZ×85 yday $16.32 → 09:30 $16.77 +38.25; DKS×7 yday $156.70 → 09:30 $121.87 -243.81; EH×234 yday $5.28 → 09:30 $4.77 -119.34; GFI×27 yday $48.36 → 09:30 $48.24 -3.24; GRRR×91 yday $14.20 → 09:30 $14.03 -15.47; SHMD×277 yday $4.71 → 09:30 $3.38 -368.41 | — |
| 2026-08-27 09:30 ET | **SELL** | `BMO` | 7 | $173.22 | $2.03 | $+1.70 | $1,388.63 | ▲ +1.70 after sell → book $9,688.42; vs 09:30 mark -2.03 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BNS` | 15 | $92.64 | $2.06 | $+82.61 | $2,776.17 | ▲ +82.61 after sell → book $9,686.36; vs 09:30 mark -2.06 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BZ` | 85 | $16.77 | $2.27 | $+117.03 | $4,199.35 | ▲ +117.03 after sell → book $9,684.09; vs 09:30 mark -2.27 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `DKS` | 7 | $121.87 | $2.03 | $-406.26 | $5,050.41 | ▼ -406.26 after sell → book $9,682.06; vs 09:30 mark -2.03 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `EH` | 234 | $4.77 | $3.07 | $-193.29 | $6,163.52 | ▼ -193.29 after sell → book $9,678.99; vs 09:30 mark -3.07 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `GFI` | 27 | $48.24 | $2.09 | $+10.96 | $7,463.91 | ▲ +10.96 after sell → book $9,676.90; vs 09:30 mark -2.09 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `GRRR` | 91 | $14.03 | $2.29 | $-25.48 | $8,738.36 | ▼ -25.48 after sell → book $9,674.62; vs 09:30 mark -2.28 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SHMD` | 277 | $3.38 | $3.63 | $-375.61 | $9,670.99 | ▼ -375.61 after sell → book $9,670.99; vs 09:30 mark -3.63 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `NVDA` | 45 | $212.64 | $2.12 | — | $100.06 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list mover_buy; 🔵; ret5=-4.6; leftover $9670.99 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $100.06 | ▼ close $9,534.76 vs 09:30 $9,690.45 (session -134.10) | 16:00 close · cash $100.06 · equity $9,534.76 vs 09:30 $9,690.45 (-155.69; session marks -134.10) · 1 name(s) marked open→close (per-name table). NVDA×45 09:30 $212.64 → close $209.66 -134.10 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $100.06 | ▲ 09:30 equity $10,128.76 vs yday $9,534.76 (+594.00) | 09:30 open · cash $100.06 (unchanged overnight, no fees) · equity $10,128.76 vs prior close $9,534.76 (+594.00) · 1 name(s) re-marked at the open (per-name table). NVDA×45 yday $209.66 → 09:30 $222.86 +594.00 | — |
| 2026-08-28 09:30 ET | **SELL** | `NVDA` | 45 | $222.86 | $2.22 | $+455.56 | $10,126.55 | ▲ +455.56 after sell → book $10,126.55; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 4 | $261.47 | $2.00 | — | $9,078.66 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+0.9; leftover $1265.82 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BBAR` | 84 | $14.96 | $2.24 | — | $7,819.78 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-8.4; leftover $1265.82 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 peer🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ESTC` | 15 | $82.64 | $2.04 | — | $6,578.15 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-0.9; leftover $1265.82 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `FINV` | 297 | $4.26 | $3.83 | — | $5,309.10 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-0.7; leftover $1265.82 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `FRO` | 29 | $42.51 | $2.08 | — | $4,074.23 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+6.0; leftover $1265.82 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GAP` | 61 | $20.75 | $2.17 | — | $2,806.31 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-3.9; leftover $1265.82 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `HAFN` | 160 | $7.91 | $2.47 | — | $1,538.24 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+5.4; leftover $1265.82 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `IREN` | 31 | $40.65 | $2.08 | — | $276.00 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-4.9; leftover $1265.82 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $276.00 | ▲ close $10,154.53 vs 09:30 $10,128.76 (session +46.90) | 16:00 close · cash $276.00 · equity $10,154.53 vs 09:30 $10,128.76 (+25.77; session marks +46.90) · 8 name(s) marked open→close (per-name table). ADSK×4 09:30 $261.47 → close $270.58 +36.44; BBAR×84 09:30 $14.96 → close $14.60 -30.24; ESTC×15 09:30 $82.64 → close $83.74 +16.50; FINV×297 09:30 $4.26 → close $4.02 -71.28; FRO×29 09:30 $42.51 → close $43.75 +35.96; GAP×61 09:30 $20.75 → close $20.79 +2.44; HAFN×160 09:30 $7.91 → close $8.29 +60.80; IREN×31 09:30 $40.65 → close $40.53 -3.72 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $276.00 | ▲ 09:30 equity $10,170.23 vs yday $10,154.53 (+15.70) | 09:30 open · cash $276.00 (unchanged overnight, no fees) · equity $10,170.23 vs prior close $10,154.53 (+15.70) · 8 name(s) re-marked at the open (per-name table). ADSK×4 yday $270.58 → 09:30 $258.50 -48.32; BBAR×84 yday $14.60 → 09:30 $14.50 -8.40; ESTC×15 yday $83.74 → 09:30 $99.99 +243.75; FINV×297 yday $4.02 → 09:30 $3.46 -166.32; FRO×29 yday $43.75 → 09:30 $43.54 -6.09; GAP×61 yday $20.79 → 09:30 $22.89 +128.10; HAFN×160 yday $8.29 → 09:30 $8.43 +22.40; IREN×31 yday $40.53 → 09:30 $35.71 -149.42 | — |
| 2026-08-31 09:30 ET | **SELL** | `ADSK` | 4 | $258.50 | $2.02 | $-15.90 | $1,307.98 | ▼ -15.90 after sell → book $10,168.21; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBAR` | 84 | $14.50 | $2.27 | $-43.15 | $2,523.71 | ▼ -43.15 after sell → book $10,165.94; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ESTC` | 15 | $99.99 | $2.06 | $+256.16 | $4,021.51 | ▲ +256.16 after sell → book $10,163.89; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `FINV` | 297 | $3.46 | $3.89 | $-245.32 | $5,045.24 | ▼ -245.32 after sell → book $10,160.00; vs 09:30 mark -3.89 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `FRO` | 29 | $43.54 | $2.10 | $+25.70 | $6,305.80 | ▲ +25.70 after sell → book $10,157.90; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GAP` | 61 | $22.89 | $2.19 | $+126.17 | $7,699.90 | ▲ +126.17 after sell → book $10,155.71; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `HAFN` | 160 | $8.43 | $2.51 | $+78.22 | $9,046.19 | ▲ +78.22 after sell → book $10,153.20; vs 09:30 mark -2.51 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `IREN` | 31 | $35.71 | $2.10 | $-157.33 | $10,151.10 | ▼ -157.33 after sell → book $10,151.10; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,151.10 | ▲ close $10,151.10 vs 09:30 $10,170.23 (session +0.00) | 16:00 close · cash $10,151.10 · no lots left · equity $10,151.10. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,151.10 | ▲ 09:30 equity $10,151.10 vs yday $10,151.10 (-0.00) | 09:30 open · cash $10,151.10 · no holdings · equity $10,151.10 vs prior close $10,151.10 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,151.10 | ▲ close $10,151.10 vs 09:30 $10,151.10 (session +0.00) | 16:00 close · cash $10,151.10 · no lots left · equity $10,151.10. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,151.10 | ▲ 09:30 equity $10,151.10 vs yday $10,151.10 (-0.00) | 09:30 open · cash $10,151.10 · no holdings · equity $10,151.10 vs prior close $10,151.10 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,151.10 | ▲ close $10,151.10 vs 09:30 $10,151.10 (session +0.00) | 16:00 close · cash $10,151.10 · no lots left · equity $10,151.10. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,151.10 | ▲ 09:30 equity $10,151.10 vs yday $10,151.10 (-0.00) | 09:30 open · cash $10,151.10 · no holdings · equity $10,151.10 vs prior close $10,151.10 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `AI` | 123 | $10.30 | $2.36 | — | $8,881.84 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+3.7; leftover $1268.89 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 3 | $369.68 | $2.00 | — | $7,770.80 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-6.2; leftover $1268.89 | join🔴 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CHPT` | 239 | $5.30 | $3.08 | — | $6,501.01 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+1.1; leftover $1268.89 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CIEN` | 3 | $357.25 | $2.00 | — | $5,427.26 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-7.7; leftover $1268.89 | join🟡 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🔴 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CPB` | 53 | $23.80 | $2.15 | — | $4,163.72 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+0.5; leftover $1268.89 | join🔴 sector🟢 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FIVE` | 5 | $244.98 | $2.00 | — | $2,936.81 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+2.3; leftover $1268.89 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 24 | $51.99 | $2.06 | — | $1,686.99 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-9.0; leftover $1268.89 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MEI` | 69 | $18.22 | $2.20 | — | $427.61 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-16.7; leftover $1268.89 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟡 vol🔴 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $427.61 | ▼ close $10,094.74 vs 09:30 $10,151.10 (session -38.50) | 16:00 close · cash $427.61 · equity $10,094.74 vs 09:30 $10,151.10 (-56.36; session marks -38.50) · 8 name(s) marked open→close (per-name table). AI×123 09:30 $10.30 → close $10.52 +27.06; AVGO×3 09:30 $369.68 → close $367.24 -7.32; CHPT×239 09:30 $5.30 → close $5.19 -26.29; CIEN×3 09:30 $357.25 → close $354.16 -9.27; CPB×53 09:30 $23.80 → close $23.78 -1.06; FIVE×5 09:30 $244.98 → close $243.08 -9.50; HPE×24 09:30 $51.99 → close $51.83 -3.84; MEI×69 09:30 $18.22 → close $18.10 -8.28 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $427.61 | ▲ 09:30 equity $10,167.94 vs yday $10,094.74 (+73.20) | 09:30 open · cash $427.61 (unchanged overnight, no fees) · equity $10,167.94 vs prior close $10,094.74 (+73.20) · 8 name(s) re-marked at the open (per-name table). AI×123 yday $10.52 → 09:30 $10.74 +27.06; AVGO×3 yday $367.24 → 09:30 $351.74 -46.50; CHPT×239 yday $5.19 → 09:30 $6.90 +408.69; CIEN×3 yday $354.16 → 09:30 $354.49 +0.99; CPB×53 yday $23.78 → 09:30 $22.32 -77.38; FIVE×5 yday $243.08 → 09:30 $256.99 +69.55; HPE×24 yday $51.83 → 09:30 $47.60 -101.52; MEI×69 yday $18.10 → 09:30 $15.09 -207.69 | — |
| 2026-09-04 09:30 ET | **SELL** | `AI` | 123 | $10.74 | $2.39 | $+49.37 | $1,746.24 | ▲ +49.37 after sell → book $10,165.55; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 3 | $351.74 | $2.02 | $-57.84 | $2,799.44 | ▼ -57.84 after sell → book $10,163.53; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CHPT` | 239 | $6.90 | $3.14 | $+376.18 | $4,445.41 | ▲ +376.18 after sell → book $10,160.40; vs 09:30 mark -3.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CIEN` | 3 | $354.49 | $2.02 | $-12.30 | $5,506.86 | ▼ -12.30 after sell → book $10,158.38; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CPB` | 53 | $22.32 | $2.17 | $-82.76 | $6,687.65 | ▼ -82.76 after sell → book $10,156.21; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `FIVE` | 5 | $256.99 | $2.03 | $+56.02 | $7,970.57 | ▲ +56.02 after sell → book $10,154.18; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `HPE` | 24 | $47.60 | $2.08 | $-109.50 | $9,110.89 | ▼ -109.50 after sell → book $10,152.10; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MEI` | 69 | $15.09 | $2.22 | $-220.39 | $10,149.88 | ▼ -220.39 after sell → book $10,149.88; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `AMBA` | 19 | $66.61 | $2.05 | — | $8,882.25 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-10.1; leftover $1268.74 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ASAN` | 124 | $10.16 | $2.36 | — | $7,620.04 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+4.8; leftover $1268.74 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DOCU` | 18 | $67.06 | $2.04 | — | $6,410.92 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-0.1; leftover $1268.74 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DOMO` | 335 | $3.78 | $4.32 | — | $5,140.30 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-2.8; leftover $1268.74 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `GWRE` | 6 | $198.00 | $2.01 | — | $3,950.29 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+7.7; leftover $1268.74 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟡 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `IOT` | 33 | $37.69 | $2.09 | — | $2,704.43 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+0.4; leftover $1268.74 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `LULU` | 10 | $121.15 | $2.02 | — | $1,490.91 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+1.3; leftover $1268.74 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MAMA` | 81 | $15.62 | $2.23 | — | $223.46 | — | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-4.7; leftover $1268.74 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $223.46 | ▲ close $10,142.32 vs 09:30 $10,167.94 (session +11.56) | 16:00 close · cash $223.46 · equity $10,142.32 vs 09:30 $10,167.94 (-25.62; session marks +11.56) · 8 name(s) marked open→close (per-name table). AMBA×19 09:30 $66.61 → close $63.38 -61.37; ASAN×124 09:30 $10.16 → close $10.09 -8.68; DOCU×18 09:30 $67.06 → close $65.97 -19.62; DOMO×335 09:30 $3.78 → close $3.79 +3.35; GWRE×6 09:30 $198.00 → close $202.86 +29.16; IOT×33 09:30 $37.69 → close $38.75 +34.98; LULU×10 09:30 $121.15 → close $121.77 +6.20; MAMA×81 09:30 $15.62 → close $15.96 +27.54 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `JLHL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FN` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `DUOT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `KLAR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `VNET` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `DVLT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ADI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `JKHY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KC` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KEYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `LOW` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `LZB` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MRCY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `PDD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-26 | `BMO` | no_price | no 09:30 open — carry |
| 2026-08-26 | `BNS` | no_price | no 09:30 open — carry |
| 2026-08-26 | `BZ` | no_price | no 09:30 open — carry |
| 2026-08-26 | `DKS` | no_price | no 09:30 open — carry |
| 2026-08-26 | `EH` | no_price | no 09:30 open — carry |
| 2026-08-26 | `GFI` | no_price | no 09:30 open — carry |
| 2026-08-26 | `GRRR` | no_price | no 09:30 open — carry |
| 2026-08-26 | `SHMD` | no_price | no 09:30 open — carry |
| 2026-08-26 | `TIGR` | no_price | no 09:30 open |
| 2026-08-26 | `ANF` | no_price | no 09:30 open |
| 2026-08-26 | `BBWI` | no_price | no 09:30 open |
| 2026-08-26 | `BOX` | no_price | no 09:30 open |
| 2026-08-26 | `DY` | no_price | no 09:30 open |
| 2026-08-26 | `FSCO` | no_price | no 09:30 open |
| 2026-08-26 | `HEI` | no_price | no 09:30 open |
| 2026-08-26 | `INTU` | no_price | no 09:30 open |
| 2026-08-31 | `LX` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `NIO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `BF-B` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FCEL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GTLB` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MDB` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `OLLI` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PANW` | hard_red | hard-red S=-3.83 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `AMBA` | 19 | 2026-09-04 @ $66.61 | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-10.1; leftover $1268.74 |
| `ASAN` | 124 | 2026-09-04 @ $10.16 | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=+4.8; leftover $1268.74 |
| `DOCU` | 18 | 2026-09-04 @ $67.06 | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-0.1; leftover $1268.74 |
| `DOMO` | 335 | 2026-09-04 @ $3.78 | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; 🔵; ret5=-2.8; leftover $1268.74 |
| `GWRE` | 6 | 2026-09-04 @ $198.00 | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+7.7; leftover $1268.74 |
| `IOT` | 33 | 2026-09-04 @ $37.69 | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+0.4; leftover $1268.74 |
| `LULU` | 10 | 2026-09-04 @ $121.15 | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=+1.3; leftover $1268.74 |
| `MAMA` | 81 | 2026-09-04 @ $15.62 | union ∩ earn_react, no 🚨; gate earn_react=True; list earn_react; ret5=-4.7; leftover $1268.74 |
