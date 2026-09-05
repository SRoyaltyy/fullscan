# Factor mine action — `short_alarm_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **short** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · alarm

Cash book **+2.77%** ($10,277) · signal-only (no cash/fees) was +11.88%. Starts YES **12/17**. Fills 58 · skips 110 · realized $+276.81.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `alarm=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,276.82.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `WWW` | 30 | — | $20.60 | +0.00 | $21.03 | -12.90 | -12.90 | -0.00 | -12.90 |
| 2026-08-14 | `FOSL` | 110 | — | $5.64 | +0.00 | $5.57 | +7.70 | +7.70 | -0.00 | +7.70 |
| 2026-08-14 | `AIRS` | 185 | — | $3.37 | +0.00 | $3.43 | -11.10 | -11.10 | -0.00 | -11.10 |
| 2026-08-14 | `OMER` | 36 | — | $17.35 | +0.00 | $17.19 | +5.76 | +5.76 | -0.00 | +5.76 |
| 2026-08-14 | `MXCT` | 449 | — | $1.39 | +0.00 | $1.32 | +31.43 | +31.43 | -0.00 | +31.43 |
| 2026-08-14 | `AVAH` | 52 | — | $11.91 | +0.00 | $12.32 | -21.32 | -21.32 | -0.00 | -21.32 |
| 2026-08-14 | `CRMD` | 77 | — | $8.05 | +0.00 | $7.54 | +39.27 | +39.27 | -0.00 | +39.27 |
| 2026-08-14 | `LVWR` | 500 | — | $1.25 | +0.00 | $1.20 | +25.00 | +25.00 | -0.00 | +25.00 |
| 2026-08-17 | `WWW` | 30 | $21.03 | $20.98 | +1.50 | $19.83 | +34.50 | +36.00 | -11.40 | +23.10 |
| 2026-08-17 | `FOSL` | 110 | $5.57 | $5.50 | +7.70 | $5.74 | -26.40 | -18.70 | +15.40 | -11.00 |
| 2026-08-17 | `AIRS` | 185 | $3.43 | $3.40 | +6.48 | $3.08 | +57.35 | +63.83 | -4.62 | +52.73 |
| 2026-08-17 | `OMER` | 36 | $17.19 | $17.17 | +0.72 | $17.36 | -6.84 | -6.12 | +6.48 | -0.36 |
| 2026-08-17 | `MXCT` | 449 | $1.32 | $1.32 | +0.00 | $1.32 | +0.00 | +0.00 | +31.43 | +31.43 |
| 2026-08-17 | `AVAH` | 52 | $12.32 | $12.21 | +5.72 | $12.69 | -24.96 | -19.24 | -15.60 | -40.56 |
| 2026-08-17 | `CRMD` | 77 | $7.54 | $7.55 | -0.77 | $7.67 | -9.24 | -10.01 | +38.50 | +29.26 |
| 2026-08-17 | `LVWR` | 500 | $1.20 | $1.18 | +10.00 | $1.15 | +15.00 | +25.00 | +35.00 | +50.00 |
| 2026-08-17 | `HNST` | 130 | — | $4.81 | +0.00 | $4.70 | +14.30 | +14.30 | -0.00 | +14.30 |
| 2026-08-17 | `FCEL` | 28 | — | $22.37 | +0.00 | $22.36 | +0.28 | +0.28 | -0.00 | +0.28 |
| 2026-08-17 | `BW` | 60 | — | $10.35 | +0.00 | $9.92 | +25.80 | +25.80 | -0.00 | +25.80 |
| 2026-08-17 | `INO` | 588 | — | $1.07 | +0.00 | $1.15 | -47.04 | -47.04 | -0.00 | -47.04 |
| 2026-08-17 | `BYND` | 49 | — | $12.83 | +0.00 | $11.63 | +58.80 | +58.80 | -0.00 | +58.80 |
| 2026-08-17 | `AEHR` | 4 | — | $132.79 | +0.00 | $145.61 | -51.28 | -51.28 | -0.00 | -51.28 |
| 2026-08-17 | `LUNR` | 31 | — | $20.25 | +0.00 | $20.38 | -4.03 | -4.03 | -0.00 | -4.03 |
| 2026-08-17 | `IOVA` | 92 | — | $6.84 | +0.00 | $7.10 | -23.92 | -23.92 | -0.00 | -23.92 |
| 2026-08-18 | `WWW` | 30 | $19.83 | $19.95 | -3.60 | $19.99 | -1.20 | -4.80 | +19.50 | +18.30 |
| 2026-08-18 | `FOSL` | 110 | $5.74 | $5.78 | -4.40 | $5.50 | +30.80 | +26.40 | -15.40 | +15.40 |
| 2026-08-18 | `AIRS` | 185 | $3.08 | $3.01 | +13.88 | $2.69 | +58.27 | +72.15 | +66.60 | +124.88 |
| 2026-08-18 | `OMER` | 36 | $17.36 | $17.03 | +11.88 | $17.19 | -5.76 | +6.12 | +11.52 | +5.76 |
| 2026-08-18 | `MXCT` | 449 | $1.32 | $1.30 | +8.98 | $1.27 | +13.47 | +22.45 | +40.41 | +53.88 |
| 2026-08-18 | `AVAH` | 52 | $12.69 | $12.68 | +0.52 | $12.67 | +0.52 | +1.04 | -40.04 | -39.52 |
| 2026-08-18 | `CRMD` | 77 | $7.67 | $7.71 | -3.08 | $8.17 | -35.42 | -38.50 | +26.18 | -9.24 |
| 2026-08-18 | `LVWR` | 500 | $1.15 | $1.10 | +25.00 | $1.24 | -70.00 | -45.00 | +75.00 | +5.00 |
| 2026-08-18 | `HNST` | 130 | $4.70 | $4.67 | +3.90 | $4.75 | -10.40 | -6.50 | +18.20 | +7.80 |
| 2026-08-18 | `FCEL` | 28 | $22.36 | $21.18 | +33.04 | $21.70 | -14.56 | +18.48 | +33.32 | +18.76 |
| 2026-08-18 | `BW` | 60 | $9.92 | $9.60 | +19.20 | $9.14 | +27.60 | +46.80 | +45.00 | +72.60 |
| 2026-08-18 | `INO` | 588 | $1.15 | $1.14 | +5.88 | $1.20 | -35.28 | -29.40 | -41.16 | -76.44 |
| 2026-08-18 | `BYND` | 49 | $11.63 | $11.12 | +24.99 | $12.74 | -79.38 | -54.39 | +83.79 | +4.41 |
| 2026-08-18 | `AEHR` | 4 | $145.61 | $135.58 | +40.12 | $123.25 | +49.32 | +89.44 | -11.16 | +38.16 |
| 2026-08-18 | `LUNR` | 31 | $20.38 | $19.31 | +33.17 | $19.31 | +0.00 | +33.17 | +29.14 | +29.14 |
| 2026-08-18 | `IOVA` | 92 | $7.10 | $7.00 | +9.20 | $7.03 | -2.76 | +6.44 | -14.72 | -17.48 |
| 2026-08-19 | `WWW` | 30 | $19.99 | $20.08 | -2.70 | — | +0.00 | -2.70 | +15.60 | — |
| 2026-08-19 | `FOSL` | 110 | $5.50 | $5.54 | -4.40 | — | +0.00 | -4.40 | +11.00 | — |
| 2026-08-19 | `AIRS` | 185 | $2.69 | $2.71 | -2.78 | — | +0.00 | -2.78 | +122.10 | — |
| 2026-08-19 | `OMER` | 36 | $17.19 | $17.13 | +2.16 | — | +0.00 | +2.16 | +7.92 | — |
| 2026-08-19 | `MXCT` | 449 | $1.27 | $1.29 | -8.98 | — | +0.00 | -8.98 | +44.90 | — |
| 2026-08-19 | `AVAH` | 52 | $12.67 | $12.92 | -13.00 | — | +0.00 | -13.00 | -52.52 | — |
| 2026-08-19 | `CRMD` | 77 | $8.17 | $8.30 | -10.01 | — | +0.00 | -10.01 | -19.25 | — |
| 2026-08-19 | `LVWR` | 500 | $1.24 | $1.17 | +35.00 | — | +0.00 | +35.00 | +40.00 | — |
| 2026-08-19 | `HNST` | 130 | $4.75 | $4.80 | -6.50 | $5.02 | -28.60 | -35.10 | +1.30 | -27.30 |
| 2026-08-19 | `FCEL` | 28 | $21.70 | $21.48 | +6.16 | $20.30 | +33.04 | +39.20 | +24.92 | +57.96 |
| 2026-08-19 | `BW` | 60 | $9.14 | $9.14 | +0.00 | $9.11 | +1.80 | +1.80 | +72.60 | +74.40 |
| 2026-08-19 | `INO` | 588 | $1.20 | $1.22 | -11.76 | $1.30 | -47.04 | -58.80 | -88.20 | -135.24 |
| 2026-08-19 | `BYND` | 49 | $12.74 | $12.63 | +5.39 | $14.08 | -71.05 | -65.66 | +9.80 | -61.25 |
| 2026-08-19 | `AEHR` | 4 | $123.25 | $123.64 | -1.56 | $107.96 | +62.72 | +61.16 | +36.60 | +99.32 |
| 2026-08-19 | `LUNR` | 31 | $19.31 | $18.98 | +10.23 | $18.52 | +14.26 | +24.49 | +39.37 | +53.63 |
| 2026-08-19 | `IOVA` | 92 | $7.03 | $7.20 | -15.64 | $7.99 | -72.68 | -88.32 | -33.12 | -105.80 |
| 2026-08-20 | `HNST` | 130 | $5.02 | $4.98 | +5.20 | — | +0.00 | +5.20 | -22.10 | — |
| 2026-08-20 | `FCEL` | 28 | $20.30 | $20.21 | +2.52 | — | +0.00 | +2.52 | +60.48 | — |
| 2026-08-20 | `BW` | 60 | $9.11 | $9.05 | +3.60 | — | +0.00 | +3.60 | +78.00 | — |
| 2026-08-20 | `INO` | 588 | $1.30 | $1.30 | +0.00 | — | +0.00 | +0.00 | -135.24 | — |
| 2026-08-20 | `BYND` | 49 | $14.08 | $13.60 | +23.52 | — | +0.00 | +23.52 | -37.73 | — |
| 2026-08-20 | `AEHR` | 4 | $107.96 | $106.01 | +7.80 | — | +0.00 | +7.80 | +107.12 | — |
| 2026-08-20 | `LUNR` | 31 | $18.52 | $18.13 | +12.09 | — | +0.00 | +12.09 | +65.72 | — |
| 2026-08-20 | `IOVA` | 92 | $7.99 | $8.07 | -7.36 | — | +0.00 | -7.36 | -113.16 | — |
| 2026-08-21 | `YSS` | 108 | — | $9.26 | +0.00 | $9.32 | -6.48 | -6.48 | -0.00 | -6.48 |
| 2026-08-21 | `SMJF` | 88 | — | $11.35 | +0.00 | $11.41 | -5.28 | -5.28 | -0.00 | -5.28 |
| 2026-08-21 | `NOG` | 37 | — | $27.00 | +0.00 | $27.34 | -12.58 | -12.58 | -0.00 | -12.58 |
| 2026-08-21 | `CPRT` | 29 | — | $34.48 | +0.00 | $33.80 | +19.72 | +19.72 | -0.00 | +19.72 |
| 2026-08-21 | `FLO` | 146 | — | $6.90 | +0.00 | $6.95 | -7.30 | -7.30 | -0.00 | -7.30 |
| 2026-08-24 | `YSS` | 108 | $9.32 | $9.14 | +19.44 | $9.47 | -35.64 | -16.20 | +12.96 | -22.68 |
| 2026-08-24 | `SMJF` | 88 | $11.41 | $11.18 | +20.24 | $11.19 | -0.88 | +19.36 | +14.96 | +14.08 |
| 2026-08-24 | `NOG` | 37 | $27.34 | $27.09 | +9.25 | $26.49 | +22.20 | +31.45 | -3.33 | +18.87 |
| 2026-08-24 | `CPRT` | 29 | $33.80 | $33.98 | -5.22 | $33.19 | +22.91 | +17.69 | +14.50 | +37.41 |
| 2026-08-24 | `FLO` | 146 | $6.95 | $6.95 | +0.00 | $7.18 | -33.58 | -33.58 | -7.30 | -40.88 |
| 2026-08-25 | `YSS` | 108 | $9.47 | $9.77 | -32.40 | $9.99 | -23.76 | -56.16 | -55.08 | -78.84 |
| 2026-08-25 | `SMJF` | 88 | $11.19 | $11.20 | -0.88 | $11.25 | -4.40 | -5.28 | +13.20 | +8.80 |
| 2026-08-25 | `NOG` | 37 | $26.49 | $26.10 | +14.43 | $26.50 | -14.80 | -0.37 | +33.30 | +18.50 |
| 2026-08-25 | `CPRT` | 29 | $33.19 | $33.25 | -1.74 | $33.28 | -0.87 | -2.61 | +35.67 | +34.80 |
| 2026-08-25 | `FLO` | 146 | $7.18 | $7.36 | -26.28 | $7.18 | +26.28 | +0.00 | -67.16 | -40.88 |
| 2026-08-26 | `YSS` | 108 | $9.99 | $9.99 | +0.00 | $9.99 | +0.00 | +0.00 | -78.84 | -78.84 |
| 2026-08-26 | `SMJF` | 88 | $11.25 | $11.25 | +0.00 | $11.25 | +0.00 | +0.00 | +8.80 | +8.80 |
| 2026-08-26 | `NOG` | 37 | $26.50 | $26.50 | +0.00 | $26.50 | +0.00 | +0.00 | +18.50 | +18.50 |
| 2026-08-26 | `CPRT` | 29 | $33.28 | $33.28 | +0.00 | $33.28 | +0.00 | +0.00 | +34.80 | +34.80 |
| 2026-08-26 | `FLO` | 146 | $7.18 | $7.18 | +0.00 | $7.18 | +0.00 | +0.00 | -40.88 | -40.88 |
| 2026-08-27 | `YSS` | 108 | $9.99 | $9.20 | +85.32 | — | +0.00 | +85.32 | +6.48 | — |
| 2026-08-27 | `SMJF` | 88 | $11.25 | $11.15 | +8.80 | — | +0.00 | +8.80 | +17.60 | — |
| 2026-08-27 | `NOG` | 37 | $26.50 | $26.00 | +18.50 | — | +0.00 | +18.50 | +37.00 | — |
| 2026-08-27 | `CPRT` | 29 | $33.28 | $33.00 | +8.12 | — | +0.00 | +8.12 | +42.92 | — |
| 2026-08-27 | `FLO` | 146 | $7.18 | $7.13 | +7.30 | — | +0.00 | +7.30 | -33.58 | — |
| 2026-08-28 | `PYXS` | 191 | — | $3.31 | +0.00 | $3.32 | -1.91 | -1.91 | -0.00 | -1.91 |
| 2026-08-28 | `SAFX` | 1622 | — | $0.39 | +0.00 | $0.37 | +32.44 | +32.44 | -0.00 | +32.44 |
| 2026-08-28 | `XPOF` | 113 | — | $5.59 | +0.00 | $5.39 | +22.60 | +22.60 | -0.00 | +22.60 |
| 2026-08-28 | `APMD` | 21 | — | $29.50 | +0.00 | $28.72 | +16.38 | +16.38 | -0.00 | +16.38 |
| 2026-08-28 | `OPTU` | 596 | — | $1.06 | +0.00 | $1.02 | +23.84 | +23.84 | -0.00 | +23.84 |
| 2026-08-28 | `ABTC` | 75 | — | $8.41 | +0.00 | $8.76 | -26.25 | -26.25 | -0.00 | -26.25 |
| 2026-08-28 | `XHG` | 155 | — | $4.06 | +0.00 | $3.80 | +40.30 | +40.30 | -0.00 | +40.30 |
| 2026-08-28 | `DEFT` | 1054 | — | $0.60 | +0.00 | $0.65 | -52.70 | -52.70 | -0.00 | -52.70 |
| 2026-08-31 | `PYXS` | 191 | $3.32 | $3.23 | +17.19 | $3.23 | +0.00 | +17.19 | +15.28 | +15.28 |
| 2026-08-31 | `SAFX` | 1622 | $0.37 | $0.38 | -16.22 | $0.37 | +16.22 | +0.00 | +16.22 | +32.44 |
| 2026-08-31 | `XPOF` | 113 | $5.39 | $5.43 | -4.52 | $5.43 | +0.00 | -4.52 | +18.08 | +18.08 |
| 2026-08-31 | `APMD` | 21 | $28.72 | $29.80 | -22.68 | $29.80 | +0.00 | -22.68 | -6.30 | -6.30 |
| 2026-08-31 | `OPTU` | 596 | $1.02 | $1.02 | +0.00 | $1.02 | +0.00 | +0.00 | +23.84 | +23.84 |
| 2026-08-31 | `ABTC` | 75 | $8.76 | $7.73 | +77.25 | $7.81 | -6.00 | +71.25 | +51.00 | +45.00 |
| 2026-08-31 | `XHG` | 155 | $3.80 | $3.44 | +55.80 | $3.44 | +0.00 | +55.80 | +96.10 | +96.10 |
| 2026-08-31 | `DEFT` | 1054 | $0.65 | $0.62 | +31.62 | $0.62 | +0.00 | +31.62 | -21.08 | -21.08 |
| 2026-09-01 | `PYXS` | 191 | $3.23 | $3.14 | +17.19 | $3.14 | +0.00 | +17.19 | +32.47 | +32.47 |
| 2026-09-01 | `SAFX` | 1622 | $0.37 | $0.37 | +0.00 | $0.37 | +0.00 | +0.00 | +32.44 | +32.44 |
| 2026-09-01 | `XPOF` | 113 | $5.43 | $5.44 | -1.13 | $5.44 | +0.00 | -1.13 | +16.95 | +16.95 |
| 2026-09-01 | `APMD` | 21 | $29.80 | $25.90 | +81.90 | $26.00 | -2.10 | +79.80 | +75.60 | +73.50 |
| 2026-09-01 | `OPTU` | 596 | $1.02 | $0.97 | +29.80 | $0.97 | +0.00 | +29.80 | +53.64 | +53.64 |
| 2026-09-01 | `ABTC` | 75 | $7.81 | $8.09 | -21.00 | $7.86 | +17.25 | -3.75 | +24.00 | +41.25 |
| 2026-09-01 | `XHG` | 155 | $3.44 | $3.52 | -12.40 | $3.43 | +13.95 | +1.55 | +83.70 | +97.65 |
| 2026-09-01 | `DEFT` | 1054 | $0.62 | $0.59 | +31.62 | $0.61 | -21.08 | +10.54 | +10.54 | -10.54 |
| 2026-09-02 | `PYXS` | 191 | $3.14 | $3.24 | -19.10 | — | +0.00 | -19.10 | +13.37 | — |
| 2026-09-02 | `SAFX` | 1622 | $0.37 | $0.37 | +0.00 | — | +0.00 | +0.00 | +32.44 | — |
| 2026-09-02 | `XPOF` | 113 | $5.44 | $5.39 | +5.65 | — | +0.00 | +5.65 | +22.60 | — |
| 2026-09-02 | `APMD` | 21 | $26.00 | $26.11 | -2.31 | — | +0.00 | -2.31 | +71.19 | — |
| 2026-09-02 | `OPTU` | 596 | $0.97 | $0.99 | -11.92 | — | +0.00 | -11.92 | +41.72 | — |
| 2026-09-02 | `ABTC` | 75 | $7.86 | $7.91 | -3.75 | — | +0.00 | -3.75 | +37.50 | — |
| 2026-09-02 | `XHG` | 155 | $3.43 | $3.48 | -7.75 | — | +0.00 | -7.75 | +89.90 | — |
| 2026-09-02 | `DEFT` | 1054 | $0.61 | $0.63 | -21.08 | $0.66 | -31.62 | -52.70 | -31.62 | -63.24 |
| 2026-09-03 | `DEFT` | 1054 | $0.66 | $0.67 | -10.54 | — | +0.00 | -10.54 | -73.78 | — |
| 2026-09-04 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +63.84 | WWW, FOSL, AIRS, OMER, MXCT, AVAH, CRMD, LVWR | — | $14,948.61 | $10,037.72 | WWW×30, FOSL×110, AIRS×185, OMER×36, MXCT×449, AVAH×52, CRMD×77, LVWR×500 |
| 2026-08-17 | +2.25 | $14,948.61 | WWW×30, FOSL×110, AIRS×185, OMER×36, MXCT×449, AVAH×52, CRMD×77, LVWR×500 | $10,069.07 | +31.35 | +12.32 | HNST, FCEL, BW, INO, BYND, AEHR, LUNR, IOVA | — | $19,844.20 | $10,058.29 | WWW×30, FOSL×110, AIRS×185, OMER×36, MXCT×449, AVAH×52, CRMD×77, LVWR×500, HNST×130, FCEL×28, BW×60, INO×588, BYND×49, AEHR×4, LUNR×31, IOVA×92 |
| 2026-08-18 | -6.20 | $19,844.20 | WWW×30, FOSL×110, AIRS×185, OMER×36, MXCT×449, AVAH×52, CRMD×77, LVWR×500, HNST×130, FCEL×28, BW×60, INO×588, BYND×49, AEHR×4, LUNR×31, IOVA×92 | $10,276.97 | +218.68 | -74.78 | — | — | $19,844.20 | $10,202.19 | WWW×30, FOSL×110, AIRS×185, OMER×36, MXCT×449, AVAH×52, CRMD×77, LVWR×500, HNST×130, FCEL×28, BW×60, INO×588, BYND×49, AEHR×4, LUNR×31, IOVA×92 |
| 2026-08-19 | -7.20 | $19,844.20 | WWW×30, FOSL×110, AIRS×185, OMER×36, MXCT×449, AVAH×52, CRMD×77, LVWR×500, HNST×130, FCEL×28, BW×60, INO×588, BYND×49, AEHR×4, LUNR×31, IOVA×92 | $10,183.81 | -18.38 | -107.55 | — | WWW, FOSL, AIRS, OMER, MXCT, AVAH, CRMD, LVWR | $15,013.56 | $10,050.60 | HNST×130, FCEL×28, BW×60, INO×588, BYND×49, AEHR×4, LUNR×31, IOVA×92 |
| 2026-08-20 | +1.12 | $15,013.56 | HNST×130, FCEL×28, BW×60, INO×588, BYND×49, AEHR×4, LUNR×31, IOVA×92 | $10,097.97 | +47.37 | +0.00 | — | HNST, FCEL, BW, INO, BYND, AEHR, LUNR, IOVA | $10,075.28 | $10,075.28 | — |
| 2026-08-21 | +3.25 | $10,075.28 | — | $10,075.28 | -0.00 | -11.92 | YSS, SMJF, NOG, CPRT, FLO | — | $15,069.04 | $10,051.92 | YSS×108, SMJF×88, NOG×37, CPRT×29, FLO×146 |
| 2026-08-24 | -5.17 | $15,069.04 | YSS×108, SMJF×88, NOG×37, CPRT×29, FLO×146 | $10,095.63 | +43.71 | -24.99 | — | — | $15,069.04 | $10,070.64 | YSS×108, SMJF×88, NOG×37, CPRT×29, FLO×146 |
| 2026-08-25 | +1.80 | $15,069.04 | YSS×108, SMJF×88, NOG×37, CPRT×29, FLO×146 | $10,023.77 | -46.87 | -17.55 | — | — | $15,069.04 | $10,006.22 | YSS×108, SMJF×88, NOG×37, CPRT×29, FLO×146 |
| 2026-08-26 | +2.02 | $15,069.04 | YSS×108, SMJF×88, NOG×37, CPRT×29, FLO×146 | $10,006.22 | -0.00 | +0.00 | — | — | $15,069.04 | $10,006.22 | YSS×108, SMJF×88, NOG×37, CPRT×29, FLO×146 |
| 2026-08-27 | — | $15,069.04 | YSS×108, SMJF×88, NOG×37, CPRT×29, FLO×146 | $10,134.26 | +128.04 | +0.00 | — | YSS, SMJF, NOG, CPRT, FLO | $10,123.08 | $10,123.08 | — |
| 2026-08-28 | +0.75 | $10,123.08 | — | $10,123.08 | +0.00 | +54.70 | PYXS, SAFX, XPOF, APMD, OPTU, ABTC, XHG, DEFT | — | $15,122.41 | $10,136.94 | PYXS×191, SAFX×1622, XPOF×113, APMD×21, OPTU×596, ABTC×75, XHG×155, DEFT×1054 |
| 2026-08-31 | -5.85 | $15,122.41 | PYXS×191, SAFX×1622, XPOF×113, APMD×21, OPTU×596, ABTC×75, XHG×155, DEFT×1054 | $10,275.38 | +138.44 | +10.22 | — | — | $15,122.41 | $10,285.60 | PYXS×191, SAFX×1622, XPOF×113, APMD×21, OPTU×596, ABTC×75, XHG×155, DEFT×1054 |
| 2026-09-01 | -6.30 | $15,122.41 | PYXS×191, SAFX×1622, XPOF×113, APMD×21, OPTU×596, ABTC×75, XHG×155, DEFT×1054 | $10,411.58 | +125.98 | +8.02 | — | — | $15,122.41 | $10,419.60 | PYXS×191, SAFX×1622, XPOF×113, APMD×21, OPTU×596, ABTC×75, XHG×155, DEFT×1054 |
| 2026-09-02 | -3.83 | $15,122.41 | PYXS×191, SAFX×1622, XPOF×113, APMD×21, OPTU×596, ABTC×75, XHG×155, DEFT×1054 | $10,359.34 | -60.26 | -31.62 | — | PYXS, SAFX, XPOF, APMD, OPTU, ABTC, XHG | $10,993.22 | $10,297.58 | DEFT×1054 |
| 2026-09-03 | -0.90 | $10,993.22 | DEFT×1054 | $10,287.04 | -10.54 | +0.00 | — | DEFT | $10,276.82 | $10,276.82 | — |
| 2026-09-04 | — | $10,276.82 | — | $10,276.82 | -0.00 | +0.00 | — | — | $10,276.82 | $10,276.82 | — |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **SHORT** | `WWW` | 30 | $20.60 | $2.12 | — | $10,615.88 | — | alarm; gate alarm=True; list probable,yday_gainer; ret5=+4.4; leftover $625.00 | join🟢 sector🔴 gen🟢 news🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `FOSL` | 110 | $5.64 | $2.37 | — | $11,233.92 | — | alarm; gate alarm=True; list probable; 🔵; ret5=-4.1; leftover $625.00 | join🟢 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `AIRS` | 185 | $3.37 | $2.60 | — | $11,854.76 | — | alarm; gate alarm=True; list probable; ret5=-29.1; leftover $625.00 | join🟢 sector🔴 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `OMER` | 36 | $17.35 | $2.14 | — | $12,477.23 | — | alarm; gate alarm=True; list yday_gainer,yday_mover; 🔵; ret5=+31.9; leftover $625.00 | join🟢 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `MXCT` | 449 | $1.39 | $5.89 | — | $13,095.45 | — | alarm; gate alarm=True; list yday_gainer,yday_mover; 🔵; ret5=+25.2; leftover $625.00 | join🟢 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `AVAH` | 52 | $11.91 | $2.18 | — | $13,712.58 | — | alarm; gate alarm=True; list yday_gainer,yday_mover; 🔵; ret5=+21.3; leftover $625.00 | join🟢 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `CRMD` | 77 | $8.05 | $2.26 | — | $14,330.17 | — | alarm; gate alarm=True; list yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $625.00 | join🟢 sector🔴 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `LVWR` | 500 | $1.25 | $6.56 | — | $14,948.61 | — | alarm; gate alarm=True; list yday_gainer,yday_mover; ret5=+12.6; leftover $625.00 | join🟢 sector🔴 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14,948.61 | ▲ close $10,037.72 vs 09:30 $10,000.00 (session +63.84) | 16:00 close · cash $14,948.61 · equity $10,037.72 vs 09:30 $10,000.00 (+37.72; session marks +63.84) · 8 name(s) marked open→close (per-name table). WWW×30 09:30 $20.60 → close $21.03 -12.90; FOSL×110 09:30 $5.64 → close $5.57 +7.70; AIRS×185 09:30 $3.37 → close $3.43 -11.10; OMER×36 09:30 $17.35 → close $17.19 +5.76; MXCT×449 09:30 $1.39 → close $1.32 +31.43; AVAH×52 09:30 $11.91 → close $12.32 -21.32; CRMD×77 09:30 $8.05 → close $7.54 +39.27; LVWR×500 09:30 $1.25 → close $1.20 +25.00 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,948.61 | ▲ 09:30 equity $10,069.07 vs yday $10,037.72 (+31.35) | 09:30 open · cash $14,948.61 (unchanged overnight, no fees) · equity $10,069.07 vs prior close $10,037.72 (+31.35) · 8 name(s) re-marked at the open (per-name table). WWW×30 yday $21.03 → 09:30 $20.98 +1.50; FOSL×110 yday $5.57 → 09:30 $5.50 +7.70; AIRS×185 yday $3.43 → 09:30 $3.40 +6.48; OMER×36 yday $17.19 → 09:30 $17.17 +0.72; MXCT×449 yday $1.32 → 09:30 $1.32 -0.00; AVAH×52 yday $12.32 → 09:30 $12.21 +5.72; CRMD×77 yday $7.54 → 09:30 $7.55 -0.77; LVWR×500 yday $1.20 → 09:30 $1.18 +10.00 | — |
| 2026-08-17 09:30 ET | **SHORT** | `HNST` | 130 | $4.81 | $2.43 | — | $15,571.48 | — | alarm; gate alarm=True; list flatten; ⚪; ret5=-11.4; leftover $629.32 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `FCEL` | 28 | $22.37 | $2.11 | — | $16,195.73 | — | alarm; gate alarm=True; list probable,yday_gainer; ⚪; ret5=+9.5; leftover $629.32 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `BW` | 60 | $10.35 | $2.21 | — | $16,814.53 | — | alarm; gate alarm=True; list probable; ⚪; ret5=+9.8; leftover $629.32 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `INO` | 588 | $1.07 | $7.71 | — | $17,435.98 | — | alarm; gate alarm=True; list yday_gainer,yday_mover; ret5=+62.7; leftover $629.32 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `BYND` | 49 | $12.83 | $2.17 | — | $18,062.47 | — | alarm; gate alarm=True; list yday_gainer,yday_mover; ⚪; ret5=-34.1; leftover $629.32 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `AEHR` | 4 | $132.79 | $2.04 | — | $18,591.59 | — | alarm; gate alarm=True; list yday_gainer; ⚪; ret5=+30.1; leftover $629.32 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `LUNR` | 31 | $20.25 | $2.12 | — | $19,217.22 | — | alarm; gate alarm=True; list yday_gainer,ohlc_hot; ⚪; ret5=+15.9; leftover $629.32 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `IOVA` | 92 | $6.84 | $2.31 | — | $19,844.20 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+10.1; leftover $629.32 | join🟡 sector🔴 gen🟢 news🟢 judge🟢 vol🟡 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,844.20 | ▲ close $10,058.29 vs 09:30 $10,069.07 (session +12.32) | 16:00 close · cash $19,844.20 · equity $10,058.29 vs 09:30 $10,069.07 (-10.78; session marks +12.32) · 16 name(s) marked open→close (per-name table). WWW×30 09:30 $20.98 → close $19.83 +34.50; FOSL×110 09:30 $5.50 → close $5.74 -26.40; AIRS×185 09:30 $3.40 → close $3.08 +57.35; OMER×36 09:30 $17.17 → close $17.36 -6.84; MXCT×449 09:30 $1.32 → close $1.32 -0.00; AVAH×52 09:30 $12.21 → close $12.69 -24.96; CRMD×77 09:30 $7.55 → close $7.67 -9.24; LVWR×500 09:30 $1.18 → close $1.15 +15.00; HNST×130 09:30 $4.81 → close $4.70 +14.30; FCEL×28 09:30 $22.37 → close $22.36 +0.28; BW×60 09:30 $10.35 → close $9.92 +25.80; INO×588 09:30 $1.07 → close $1.15 -47.04; BYND×49 09:30 $12.83 → close $11.63 +58.80; AEHR×4 09:30 $132.79 → close $145.61 -51.28; LUNR×31 09:30 $20.25 → close $20.38 -4.03; IOVA×92 09:30 $6.84 → close $7.10 -23.92 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,844.20 | ▲ 09:30 equity $10,276.97 vs yday $10,058.29 (+218.68) | 09:30 open · cash $19,844.20 (unchanged overnight, no fees) · equity $10,276.97 vs prior close $10,058.29 (+218.68) · 16 name(s) re-marked at the open (per-name table). WWW×30 yday $19.83 → 09:30 $19.95 -3.60; FOSL×110 yday $5.74 → 09:30 $5.78 -4.40; AIRS×185 yday $3.08 → 09:30 $3.01 +13.88; OMER×36 yday $17.36 → 09:30 $17.03 +11.88; MXCT×449 yday $1.32 → 09:30 $1.30 +8.98; AVAH×52 yday $12.69 → 09:30 $12.68 +0.52; CRMD×77 yday $7.67 → 09:30 $7.71 -3.08; LVWR×500 yday $1.15 → 09:30 $1.10 +25.00; HNST×130 yday $4.70 → 09:30 $4.67 +3.90; FCEL×28 yday $22.36 → 09:30 $21.18 +33.04; BW×60 yday $9.92 → 09:30 $9.60 +19.20; INO×588 yday $1.15 → 09:30 $1.14 +5.88; BYND×49 yday $11.63 → 09:30 $11.12 +24.99; AEHR×4 yday $145.61 → 09:30 $135.58 +40.12; LUNR×31 yday $20.38 → 09:30 $19.31 +33.17; IOVA×92 yday $7.10 → 09:30 $7.00 +9.20 | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19,844.20 | ▼ close $10,202.19 vs 09:30 $10,276.97 (session -74.78) | 16:00 close · cash $19,844.20 · equity $10,202.19 vs 09:30 $10,276.97 (-74.78; session marks -74.78) · 16 name(s) marked open→close (per-name table). WWW×30 09:30 $19.95 → close $19.99 -1.20; FOSL×110 09:30 $5.78 → close $5.50 +30.80; AIRS×185 09:30 $3.01 → close $2.69 +58.27; OMER×36 09:30 $17.03 → close $17.19 -5.76; MXCT×449 09:30 $1.30 → close $1.27 +13.47; AVAH×52 09:30 $12.68 → close $12.67 +0.52; CRMD×77 09:30 $7.71 → close $8.17 -35.42; LVWR×500 09:30 $1.10 → close $1.24 -70.00; HNST×130 09:30 $4.67 → close $4.75 -10.40; FCEL×28 09:30 $21.18 → close $21.70 -14.56; BW×60 09:30 $9.60 → close $9.14 +27.60; INO×588 09:30 $1.14 → close $1.20 -35.28; BYND×49 09:30 $11.12 → close $12.74 -79.38; AEHR×4 09:30 $135.58 → close $123.25 +49.32; LUNR×31 09:30 $19.31 → close $19.31 -0.00; IOVA×92 09:30 $7.00 → close $7.03 -2.76 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,844.20 | ▼ 09:30 equity $10,183.81 vs yday $10,202.19 (-18.38) | 09:30 open · cash $19,844.20 (unchanged overnight, no fees) · equity $10,183.81 vs prior close $10,202.19 (-18.38) · 16 name(s) re-marked at the open (per-name table). WWW×30 yday $19.99 → 09:30 $20.08 -2.70; FOSL×110 yday $5.50 → 09:30 $5.54 -4.40; AIRS×185 yday $2.69 → 09:30 $2.71 -2.78; OMER×36 yday $17.19 → 09:30 $17.13 +2.16; MXCT×449 yday $1.27 → 09:30 $1.29 -8.98; AVAH×52 yday $12.67 → 09:30 $12.92 -13.00; CRMD×77 yday $8.17 → 09:30 $8.30 -10.01; LVWR×500 yday $1.24 → 09:30 $1.17 +35.00; HNST×130 yday $4.75 → 09:30 $4.80 -6.50; FCEL×28 yday $21.70 → 09:30 $21.48 +6.16; BW×60 yday $9.14 → 09:30 $9.14 -0.00; INO×588 yday $1.20 → 09:30 $1.22 -11.76; BYND×49 yday $12.74 → 09:30 $12.63 +5.39; AEHR×4 yday $123.25 → 09:30 $123.64 -1.56; LUNR×31 yday $19.31 → 09:30 $18.98 +10.23; IOVA×92 yday $7.03 → 09:30 $7.20 -15.64 | — |
| 2026-08-19 09:30 ET | **COVER** | `WWW` | 30 | $20.08 | $2.08 | $+11.40 | $19,239.72 | ▲ +11.40 after sell → book $10,181.73; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `FOSL` | 110 | $5.54 | $2.32 | $+6.31 | $18,628.00 | ▲ +6.31 after sell → book $10,179.41; vs 09:30 mark -2.32 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `AIRS` | 185 | $2.71 | $2.54 | $+116.95 | $18,124.10 | ▲ +116.95 after sell → book $10,176.86; vs 09:30 mark -2.55 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `OMER` | 36 | $17.13 | $2.10 | $+3.69 | $17,505.32 | ▲ +3.69 after sell → book $10,174.76; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `MXCT` | 449 | $1.29 | $5.79 | $+33.21 | $16,920.32 | ▲ +33.21 after sell → book $10,168.97; vs 09:30 mark -5.79 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `AVAH` | 52 | $12.92 | $2.15 | $-56.85 | $16,246.33 | ▼ -56.85 after sell → book $10,166.82; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `CRMD` | 77 | $8.30 | $2.22 | $-23.73 | $15,605.01 | ▼ -23.73 after sell → book $10,164.60; vs 09:30 mark -2.22 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `LVWR` | 500 | $1.17 | $6.45 | $+26.99 | $15,013.56 | ▲ +26.99 after sell → book $10,158.15; vs 09:30 mark -6.45 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,013.56 | ▼ close $10,050.60 vs 09:30 $10,183.81 (session -107.55) | 16:00 close · cash $15,013.56 · equity $10,050.60 vs 09:30 $10,183.81 (-133.21; session marks -107.55) · 8 name(s) marked open→close (per-name table). HNST×130 09:30 $4.80 → close $5.02 -28.60; FCEL×28 09:30 $21.48 → close $20.30 +33.04; BW×60 09:30 $9.14 → close $9.11 +1.80; INO×588 09:30 $1.22 → close $1.30 -47.04; BYND×49 09:30 $12.63 → close $14.08 -71.05; AEHR×4 09:30 $123.64 → close $107.96 +62.72; LUNR×31 09:30 $18.98 → close $18.52 +14.26; IOVA×92 09:30 $7.20 → close $7.99 -72.68 | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,013.56 | ▲ 09:30 equity $10,097.97 vs yday $10,050.60 (+47.37) | 09:30 open · cash $15,013.56 (unchanged overnight, no fees) · equity $10,097.97 vs prior close $10,050.60 (+47.37) · 8 name(s) re-marked at the open (per-name table). HNST×130 yday $5.02 → 09:30 $4.98 +5.20; FCEL×28 yday $20.30 → 09:30 $20.21 +2.52; BW×60 yday $9.11 → 09:30 $9.05 +3.60; INO×588 yday $1.30 → 09:30 $1.30 -0.00; BYND×49 yday $14.08 → 09:30 $13.60 +23.52; AEHR×4 yday $107.96 → 09:30 $106.01 +7.80; LUNR×31 yday $18.52 → 09:30 $18.13 +12.09; IOVA×92 yday $7.99 → 09:30 $8.07 -7.36 | — |
| 2026-08-20 09:30 ET | **COVER** | `HNST` | 130 | $4.98 | $2.38 | $-26.91 | $14,363.78 | ▼ -26.91 after sell → book $10,095.59; vs 09:30 mark -2.38 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `FCEL` | 28 | $20.21 | $2.07 | $+56.29 | $13,795.83 | ▲ +56.29 after sell → book $10,093.52; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `BW` | 60 | $9.05 | $2.17 | $+73.62 | $13,250.66 | ▲ +73.62 after sell → book $10,091.35; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `INO` | 588 | $1.30 | $7.59 | $-150.54 | $12,478.67 | ▼ -150.54 after sell → book $10,083.76; vs 09:30 mark -7.59 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `BYND` | 49 | $13.60 | $2.14 | $-42.04 | $11,810.14 | ▼ -42.04 after sell → book $10,081.63; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `AEHR` | 4 | $106.01 | $2.00 | $+103.08 | $11,384.10 | ▲ +103.08 after sell → book $10,079.63; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `LUNR` | 31 | $18.13 | $2.08 | $+61.52 | $10,819.98 | ▲ +61.52 after sell → book $10,077.54; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `IOVA` | 92 | $8.07 | $2.27 | $-117.73 | $10,075.28 | ▼ -117.73 after sell → book $10,075.28; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,075.28 | ▲ close $10,075.28 vs 09:30 $10,097.97 (session +0.00) | 16:00 close · cash $10,075.28 · no lots left · equity $10,075.28. | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,075.28 | ▲ 09:30 equity $10,075.28 vs yday $10,075.28 (-0.00) | 09:30 open · cash $10,075.28 · no holdings · equity $10,075.28 vs prior close $10,075.28 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-21 09:30 ET | **SHORT** | `YSS` | 108 | $9.26 | $2.37 | — | $11,072.99 | — | alarm; gate alarm=True; list yday_mover; ret5=-20.1; leftover $1007.53 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `SMJF` | 88 | $11.35 | $2.31 | — | $12,069.48 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+13.4; leftover $1007.53 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `NOG` | 37 | $27.00 | $2.15 | — | $13,066.33 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+10.1; leftover $1007.53 | join🟢 sector🔴 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟡 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `CPRT` | 29 | $34.48 | $2.12 | — | $14,064.13 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+16.8; leftover $1007.53 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `FLO` | 146 | $6.90 | $2.49 | — | $15,069.04 | — | alarm; gate alarm=True; list earn_react; ret5=-5.7; leftover $1007.53 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 vol🔴 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,069.04 | ▼ close $10,051.92 vs 09:30 $10,075.28 (session -11.92) | 16:00 close · cash $15,069.04 · equity $10,051.92 vs 09:30 $10,075.28 (-23.36; session marks -11.92) · 5 name(s) marked open→close (per-name table). YSS×108 09:30 $9.26 → close $9.32 -6.48; SMJF×88 09:30 $11.35 → close $11.41 -5.28; NOG×37 09:30 $27.00 → close $27.34 -12.58; CPRT×29 09:30 $34.48 → close $33.80 +19.72; FLO×146 09:30 $6.90 → close $6.95 -7.30 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,069.04 | ▲ 09:30 equity $10,095.63 vs yday $10,051.92 (+43.71) | 09:30 open · cash $15,069.04 (unchanged overnight, no fees) · equity $10,095.63 vs prior close $10,051.92 (+43.71) · 5 name(s) re-marked at the open (per-name table). YSS×108 yday $9.32 → 09:30 $9.14 +19.44; SMJF×88 yday $11.41 → 09:30 $11.18 +20.24; NOG×37 yday $27.34 → 09:30 $27.09 +9.25; CPRT×29 yday $33.80 → 09:30 $33.98 -5.22; FLO×146 yday $6.95 → 09:30 $6.95 -0.00 | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,069.04 | ▼ close $10,070.64 vs 09:30 $10,095.63 (session -24.99) | 16:00 close · cash $15,069.04 · equity $10,070.64 vs 09:30 $10,095.63 (-24.99; session marks -24.99) · 5 name(s) marked open→close (per-name table). YSS×108 09:30 $9.14 → close $9.47 -35.64; SMJF×88 09:30 $11.18 → close $11.19 -0.88; NOG×37 09:30 $27.09 → close $26.49 +22.20; CPRT×29 09:30 $33.98 → close $33.19 +22.91; FLO×146 09:30 $6.95 → close $7.18 -33.58 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,069.04 | ▼ 09:30 equity $10,023.77 vs yday $10,070.64 (-46.87) | 09:30 open · cash $15,069.04 (unchanged overnight, no fees) · equity $10,023.77 vs prior close $10,070.64 (-46.87) · 5 name(s) re-marked at the open (per-name table). YSS×108 yday $9.47 → 09:30 $9.77 -32.40; SMJF×88 yday $11.19 → 09:30 $11.20 -0.88; NOG×37 yday $26.49 → 09:30 $26.10 +14.43; CPRT×29 yday $33.19 → 09:30 $33.25 -1.74; FLO×146 yday $7.18 → 09:30 $7.36 -26.28 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,069.04 | ▼ close $10,006.22 vs 09:30 $10,023.77 (session -17.55) | 16:00 close · cash $15,069.04 · equity $10,006.22 vs 09:30 $10,023.77 (-17.55; session marks -17.55) · 5 name(s) marked open→close (per-name table). YSS×108 09:30 $9.77 → close $9.99 -23.76; SMJF×88 09:30 $11.20 → close $11.25 -4.40; NOG×37 09:30 $26.10 → close $26.50 -14.80; CPRT×29 09:30 $33.25 → close $33.28 -0.87; FLO×146 09:30 $7.36 → close $7.18 +26.28 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,069.04 | ▲ 09:30 equity $10,006.22 vs yday $10,006.22 (-0.00) | 09:30 open · cash $15,069.04 (unchanged overnight, no fees) · equity $10,006.22 vs prior close $10,006.22 (-0.00) · 5 name(s) re-marked at the open (per-name table). YSS×108 yday $9.99 → 09:30 $9.99 -0.00; SMJF×88 yday $11.25 → 09:30 $11.25 -0.00; NOG×37 yday $26.50 → 09:30 $26.50 -0.00; CPRT×29 yday $33.28 → 09:30 $33.28 -0.00; FLO×146 yday $7.18 → 09:30 $7.18 -0.00 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,069.04 | ▲ close $10,006.22 vs 09:30 $10,006.22 (session +0.00) | 16:00 close · cash $15,069.04 · equity $10,006.22 vs 09:30 $10,006.22 (-0.00; session marks +0.00) · 5 name(s) marked open→close (per-name table). YSS×108 09:30 $9.99 → close $9.99 -0.00; SMJF×88 09:30 $11.25 → close $11.25 -0.00; NOG×37 09:30 $26.50 → close $26.50 -0.00; CPRT×29 09:30 $33.28 → close $33.28 -0.00; FLO×146 09:30 $7.18 → close $7.18 -0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,069.04 | ▲ 09:30 equity $10,134.26 vs yday $10,006.22 (+128.04) | 09:30 open · cash $15,069.04 (unchanged overnight, no fees) · equity $10,134.26 vs prior close $10,006.22 (+128.04) · 5 name(s) re-marked at the open (per-name table). YSS×108 yday $9.99 → 09:30 $9.20 +85.32; SMJF×88 yday $11.25 → 09:30 $11.15 +8.80; NOG×37 yday $26.50 → 09:30 $26.00 +18.50; CPRT×29 yday $33.28 → 09:30 $33.00 +8.12; FLO×146 yday $7.18 → 09:30 $7.13 +7.30 | — |
| 2026-08-27 09:30 ET | **COVER** | `YSS` | 108 | $9.20 | $2.31 | $+1.80 | $14,073.12 | ▲ +1.80 after sell → book $10,131.94; vs 09:30 mark -2.32 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **COVER** | `SMJF` | 88 | $11.15 | $2.25 | $+13.04 | $13,089.67 | ▲ +13.04 after sell → book $10,129.69; vs 09:30 mark -2.25 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **COVER** | `NOG` | 37 | $26.00 | $2.10 | $+32.75 | $12,125.57 | ▲ +32.75 after sell → book $10,127.59; vs 09:30 mark -2.10 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **COVER** | `CPRT` | 29 | $33.00 | $2.08 | $+38.72 | $11,166.49 | ▲ +38.72 after sell → book $10,125.51; vs 09:30 mark -2.08 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **COVER** | `FLO` | 146 | $7.13 | $2.43 | $-38.50 | $10,123.08 | ▼ -38.50 after sell → book $10,123.08; vs 09:30 mark -2.43 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,123.08 | ▲ close $10,123.08 vs 09:30 $10,134.26 (session +0.00) | 16:00 close · cash $10,123.08 · no lots left · equity $10,123.08. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,123.08 | ▲ 09:30 equity $10,123.08 vs yday $10,123.08 (+0.00) | 09:30 open · cash $10,123.08 · no holdings · equity $10,123.08 vs prior close $10,123.08 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **SHORT** | `PYXS` | 191 | $3.31 | $2.62 | — | $10,752.67 | — | alarm; gate alarm=True; list yday_gainer; ret5=+2.3; leftover $632.69 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟡 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `SAFX` | 1622 | $0.39 | $11.49 | — | $11,373.76 | — | alarm; gate alarm=True; list yday_gainer; ret5=-26.5; leftover $632.69 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 heat🟡 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `XPOF` | 113 | $5.59 | $2.38 | — | $12,003.06 | — | alarm; gate alarm=True; list yday_gainer; ret5=+6.6; leftover $632.69 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `APMD` | 21 | $29.50 | $2.09 | — | $12,620.47 | — | alarm; gate alarm=True; list yday_gainer; ret5=-11.7; leftover $632.69 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `OPTU` | 596 | $1.06 | $7.81 | — | $13,244.41 | — | alarm; gate alarm=True; list yday_gainer; ret5=-7.8; leftover $632.69 | join🔴 sector🔴 gen🟡 news🟡 digest🟡 ab🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `ABTC` | 75 | $8.41 | $2.25 | — | $13,872.91 | — | alarm; gate alarm=True; list yday_mover; ret5=+9.2; leftover $632.69 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `XHG` | 155 | $4.06 | $2.51 | — | $14,499.70 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+16.1; leftover $632.69 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `DEFT` | 1054 | $0.60 | $9.69 | — | $15,122.41 | — | alarm; gate alarm=True; list ohlc_hot; ret5=+17.6; leftover $632.69 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🟡 vol🔴 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,122.41 | ▲ close $10,136.94 vs 09:30 $10,123.08 (session +54.70) | 16:00 close · cash $15,122.41 · equity $10,136.94 vs 09:30 $10,123.08 (+13.86; session marks +54.70) · 8 name(s) marked open→close (per-name table). PYXS×191 09:30 $3.31 → close $3.32 -1.91; SAFX×1622 09:30 $0.39 → close $0.37 +32.44; XPOF×113 09:30 $5.59 → close $5.39 +22.60; APMD×21 09:30 $29.50 → close $28.72 +16.38; OPTU×596 09:30 $1.06 → close $1.02 +23.84; ABTC×75 09:30 $8.41 → close $8.76 -26.25; XHG×155 09:30 $4.06 → close $3.80 +40.30; DEFT×1054 09:30 $0.60 → close $0.65 -52.70 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,122.41 | ▲ 09:30 equity $10,275.38 vs yday $10,136.94 (+138.44) | 09:30 open · cash $15,122.41 (unchanged overnight, no fees) · equity $10,275.38 vs prior close $10,136.94 (+138.44) · 8 name(s) re-marked at the open (per-name table). PYXS×191 yday $3.32 → 09:30 $3.23 +17.19; SAFX×1622 yday $0.37 → 09:30 $0.38 -16.22; XPOF×113 yday $5.39 → 09:30 $5.43 -4.52; APMD×21 yday $28.72 → 09:30 $29.80 -22.68; OPTU×596 yday $1.02 → 09:30 $1.02 -0.00; ABTC×75 yday $8.76 → 09:30 $7.73 +77.25; XHG×155 yday $3.80 → 09:30 $3.44 +55.80; DEFT×1054 yday $0.65 → 09:30 $0.62 +31.62 | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,122.41 | ▲ close $10,285.60 vs 09:30 $10,275.38 (session +10.22) | 16:00 close · cash $15,122.41 · equity $10,285.60 vs 09:30 $10,275.38 (+10.22; session marks +10.22) · 8 name(s) marked open→close (per-name table). PYXS×191 09:30 $3.23 → close $3.23 -0.00; SAFX×1622 09:30 $0.38 → close $0.37 +16.22; XPOF×113 09:30 $5.43 → close $5.43 -0.00; APMD×21 09:30 $29.80 → close $29.80 -0.00; OPTU×596 09:30 $1.02 → close $1.02 -0.00; ABTC×75 09:30 $7.73 → close $7.81 -6.00; XHG×155 09:30 $3.44 → close $3.44 -0.00; DEFT×1054 09:30 $0.62 → close $0.62 -0.00 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,122.41 | ▲ 09:30 equity $10,411.58 vs yday $10,285.60 (+125.98) | 09:30 open · cash $15,122.41 (unchanged overnight, no fees) · equity $10,411.58 vs prior close $10,285.60 (+125.98) · 8 name(s) re-marked at the open (per-name table). PYXS×191 yday $3.23 → 09:30 $3.14 +17.19; SAFX×1622 yday $0.37 → 09:30 $0.37 -0.00; XPOF×113 yday $5.43 → 09:30 $5.44 -1.13; APMD×21 yday $29.80 → 09:30 $25.90 +81.90; OPTU×596 yday $1.02 → 09:30 $0.97 +29.80; ABTC×75 yday $7.81 → 09:30 $8.09 -21.00; XHG×155 yday $3.44 → 09:30 $3.52 -12.40; DEFT×1054 yday $0.62 → 09:30 $0.59 +31.62 | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15,122.41 | ▲ close $10,419.60 vs 09:30 $10,411.58 (session +8.02) | 16:00 close · cash $15,122.41 · equity $10,419.60 vs 09:30 $10,411.58 (+8.02; session marks +8.02) · 8 name(s) marked open→close (per-name table). PYXS×191 09:30 $3.14 → close $3.14 -0.00; SAFX×1622 09:30 $0.37 → close $0.37 -0.00; XPOF×113 09:30 $5.44 → close $5.44 -0.00; APMD×21 09:30 $25.90 → close $26.00 -2.10; OPTU×596 09:30 $0.97 → close $0.97 -0.00; ABTC×75 09:30 $8.09 → close $7.86 +17.25; XHG×155 09:30 $3.52 → close $3.43 +13.95; DEFT×1054 09:30 $0.59 → close $0.61 -21.08 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,122.41 | ▼ 09:30 equity $10,359.34 vs yday $10,419.60 (-60.26) | 09:30 open · cash $15,122.41 (unchanged overnight, no fees) · equity $10,359.34 vs prior close $10,419.60 (-60.26) · 8 name(s) re-marked at the open (per-name table). PYXS×191 yday $3.14 → 09:30 $3.24 -19.10; SAFX×1622 yday $0.37 → 09:30 $0.37 -0.00; XPOF×113 yday $5.44 → 09:30 $5.39 +5.65; APMD×21 yday $26.00 → 09:30 $26.11 -2.31; OPTU×596 yday $0.97 → 09:30 $0.99 -11.92; ABTC×75 yday $7.86 → 09:30 $7.91 -3.75; XHG×155 yday $3.43 → 09:30 $3.48 -7.75; DEFT×1054 yday $0.61 → 09:30 $0.63 -21.08 | — |
| 2026-09-02 09:30 ET | **COVER** | `PYXS` | 191 | $3.24 | $2.56 | $+8.18 | $14,501.01 | ▲ +8.18 after sell → book $10,356.78; vs 09:30 mark -2.56 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `SAFX` | 1622 | $0.37 | $10.87 | $+10.08 | $13,890.00 | ▲ +10.08 after sell → book $10,345.91; vs 09:30 mark -10.87 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `XPOF` | 113 | $5.39 | $2.33 | $+17.90 | $13,278.60 | ▲ +17.90 after sell → book $10,343.58; vs 09:30 mark -2.33 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `APMD` | 21 | $26.11 | $2.05 | $+67.05 | $12,728.24 | ▲ +67.05 after sell → book $10,341.53; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `OPTU` | 596 | $0.99 | $7.66 | $+26.25 | $12,130.54 | ▲ +26.25 after sell → book $10,333.87; vs 09:30 mark -7.66 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `ABTC` | 75 | $7.91 | $2.21 | $+33.03 | $11,535.08 | ▲ +33.03 after sell → book $10,331.66; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `XHG` | 155 | $3.48 | $2.46 | $+84.94 | $10,993.22 | ▲ +84.94 after sell → book $10,329.20; vs 09:30 mark -2.46 | dropped from list after 3 sess (min 3) | join🟡 sector🔴 gen🔴 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,993.22 | ▼ close $10,297.58 vs 09:30 $10,359.34 (session -31.62) | 16:00 close · cash $10,993.22 · equity $10,297.58 vs 09:30 $10,359.34 (-61.76; session marks -31.62) · 1 name(s) marked open→close (per-name table). DEFT×1054 09:30 $0.63 → close $0.66 -31.62 | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,993.22 | ▼ 09:30 equity $10,287.04 vs yday $10,297.58 (-10.54) | 09:30 open · cash $10,993.22 (unchanged overnight, no fees) · equity $10,287.04 vs prior close $10,297.58 (-10.54) · 1 name(s) re-marked at the open (per-name table). DEFT×1054 yday $0.66 → 09:30 $0.67 -10.54 | — |
| 2026-09-03 09:30 ET | **COVER** | `DEFT` | 1054 | $0.67 | $10.22 | $-93.69 | $10,276.82 | ▼ -93.69 after sell → book $10,276.82; vs 09:30 mark -10.22 | dropped from list after 4 sess (min 3) | join🟡 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,276.82 | ▲ close $10,276.82 vs 09:30 $10,287.04 (session +0.00) | 16:00 close · cash $10,276.82 · no lots left · equity $10,276.82. | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,276.82 | ▲ 09:30 equity $10,276.82 vs yday $10,276.82 (-0.00) | 09:30 open · cash $10,276.82 · no holdings · equity $10,276.82 vs prior close $10,276.82 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,276.82 | ▲ close $10,276.82 vs 09:30 $10,276.82 (session +0.00) | 16:00 close · cash $10,276.82 · no lots left · equity $10,276.82. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `WWW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `FOSL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `AIRS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `OMER` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `MXCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `AVAH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `CRMD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `LVWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `WWW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `FOSL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `AIRS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `OMER` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `MXCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `AVAH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `CRMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `LVWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `HNST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `FCEL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `BW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `INO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `BYND` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `AEHR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `LUNR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `IOVA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `CBRS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COHR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TDTH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `SNDK` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `LITE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `WDC` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ENHA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `INV` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `HNST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `FCEL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `BW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `INO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `BYND` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `AEHR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `LUNR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `IOVA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `MUR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TRMD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TBPH` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `INMD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WFF` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `EYPT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `OABI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ABCL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `YSS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `SMJF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `NOG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CPRT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `FLO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `OCUL` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRMD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SAFX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `USDE` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CAN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ARCT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ASST` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SLS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `YSS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `SMJF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `NOG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CPRT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `FLO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `YSS` | no_price | no 09:30 open — carry |
| 2026-08-26 | `SMJF` | no_price | no 09:30 open — carry |
| 2026-08-26 | `NOG` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CPRT` | no_price | no 09:30 open — carry |
| 2026-08-26 | `FLO` | no_price | no 09:30 open — carry |
| 2026-08-31 | `PYXS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SAFX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `XPOF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `APMD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `OPTU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ABTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `XHG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `DEFT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ERO` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TRLV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `GUTS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WPM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `EGO` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FCX` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `AEM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `QMCO` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `PYXS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SAFX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `XPOF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `APMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `OPTU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ABTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `XHG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `DEFT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `AREC` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SNAP` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `STT` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `PURR` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `PTRN` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `PCG` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MNSO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `ED` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `PBR-A` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `BMO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `DUOL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ERO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FUTU` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `CVI` | hard_red | hard-red S=-3.83 sit; no new buys |
