# Factor mine action — `union_e_green_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **+47.92%** ($14,792) · signal-only (no cash/fees) was +23.38%. Starts YES **15/17**. Fills 64 · skips 96 · realized $+3500.43.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `earn_react=True,last_green=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $213.89.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `INO` | 12176 | — | $0.81 | +0.00 | $0.90 | +1095.84 | +1095.84 | +0.00 | +1095.84 |
| 2026-08-14 | `INO` | 12176 | $0.90 | $0.93 | +365.28 | $1.09 | +1948.16 | +2313.44 | +1461.12 | +3409.28 |
| 2026-08-17 | `INO` | 12176 | $1.09 | $1.07 | -243.52 | $1.15 | +974.08 | +730.56 | +3165.76 | +4139.84 |
| 2026-08-18 | `INO` | 12176 | $1.15 | $1.14 | -121.76 | — | +0.00 | -121.76 | +4018.08 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `ATAT` | 50 | — | $34.05 | +0.00 | $34.25 | +10.00 | +10.00 | +0.00 | +10.00 |
| 2026-08-20 | `ATHM` | 76 | — | $22.44 | +0.00 | $22.12 | -24.32 | -24.32 | +0.00 | -24.32 |
| 2026-08-20 | `BABA` | 13 | — | $123.47 | +0.00 | $130.53 | +91.78 | +91.78 | +0.00 | +91.78 |
| 2026-08-20 | `BULL` | 172 | — | $9.94 | +0.00 | $8.85 | -187.48 | -187.48 | +0.00 | -187.48 |
| 2026-08-20 | `COTY` | 672 | — | $2.55 | +0.00 | $2.75 | +134.40 | +134.40 | +0.00 | +134.40 |
| 2026-08-20 | `DQ` | 118 | — | $14.44 | +0.00 | $14.98 | +63.72 | +63.72 | +0.00 | +63.72 |
| 2026-08-20 | `FUTU` | 14 | — | $117.65 | +0.00 | $112.73 | -68.88 | -68.88 | +0.00 | -68.88 |
| 2026-08-20 | `IOND` | 26 | — | $65.60 | +0.00 | $68.77 | +82.42 | +82.42 | +0.00 | +82.42 |
| 2026-08-21 | `ATAT` | 50 | $34.25 | $34.31 | +3.00 | $34.75 | +22.00 | +25.00 | +13.00 | +35.00 |
| 2026-08-21 | `ATHM` | 76 | $22.12 | $22.20 | +6.08 | $22.22 | +1.52 | +7.60 | -18.24 | -16.72 |
| 2026-08-21 | `BABA` | 13 | $130.53 | $125.35 | -67.34 | $119.34 | -78.13 | -145.47 | +24.44 | -53.69 |
| 2026-08-21 | `BULL` | 172 | $8.85 | $8.99 | +24.08 | $8.78 | -36.12 | -12.04 | -163.40 | -199.52 |
| 2026-08-21 | `COTY` | 672 | $2.75 | $2.71 | -26.88 | $2.74 | +20.16 | -6.72 | +107.52 | +127.68 |
| 2026-08-21 | `DQ` | 118 | $14.98 | $15.00 | +2.36 | $13.58 | -167.56 | -165.20 | +66.08 | -101.48 |
| 2026-08-21 | `FUTU` | 14 | $112.73 | $115.18 | +34.30 | $123.64 | +118.44 | +152.74 | -34.58 | +83.86 |
| 2026-08-21 | `IOND` | 26 | $68.77 | $68.41 | -9.36 | $68.73 | +8.32 | -1.04 | +73.06 | +81.38 |
| 2026-08-21 | `BKE` | 1 | — | $43.08 | +0.00 | $43.81 | +0.73 | +0.73 | +0.00 | +0.73 |
| 2026-08-21 | `PSEC` | 29 | — | $2.30 | +0.00 | $2.33 | +0.87 | +0.87 | +0.00 | +0.87 |
| 2026-08-24 | `ATAT` | 50 | $34.75 | $34.70 | -2.50 | $34.83 | +6.50 | +4.00 | +32.50 | +39.00 |
| 2026-08-24 | `ATHM` | 76 | $22.22 | $21.78 | -33.44 | $21.85 | +5.32 | -28.12 | -50.16 | -44.84 |
| 2026-08-24 | `BABA` | 13 | $119.34 | $116.80 | -33.02 | $119.46 | +34.58 | +1.56 | -86.71 | -52.13 |
| 2026-08-24 | `BULL` | 172 | $8.78 | $8.54 | -41.28 | $8.73 | +32.68 | -8.60 | -240.80 | -208.12 |
| 2026-08-24 | `COTY` | 672 | $2.74 | $2.72 | -13.44 | $2.78 | +40.32 | +26.88 | +114.24 | +154.56 |
| 2026-08-24 | `DQ` | 118 | $13.58 | $13.55 | -3.54 | $14.15 | +70.80 | +67.26 | -105.02 | -34.22 |
| 2026-08-24 | `FUTU` | 14 | $123.64 | $120.87 | -38.78 | $116.49 | -61.32 | -100.10 | +45.08 | -16.24 |
| 2026-08-24 | `IOND` | 26 | $68.73 | $68.72 | -0.26 | $70.11 | +36.14 | +35.88 | +81.12 | +117.26 |
| 2026-08-24 | `BKE` | 1 | $43.81 | $44.54 | +0.73 | $44.46 | -0.08 | +0.65 | +1.46 | +1.38 |
| 2026-08-24 | `PSEC` | 29 | $2.33 | $2.34 | +0.29 | $2.31 | -0.87 | -0.58 | +1.16 | +0.29 |
| 2026-08-25 | `ATAT` | 50 | $34.83 | $34.75 | -4.00 | — | +0.00 | -4.00 | +35.00 | — |
| 2026-08-25 | `ATHM` | 76 | $21.85 | $21.85 | +0.00 | — | +0.00 | +0.00 | -44.84 | — |
| 2026-08-25 | `BABA` | 13 | $119.46 | $116.36 | -40.30 | — | +0.00 | -40.30 | -92.43 | — |
| 2026-08-25 | `BULL` | 172 | $8.73 | $8.54 | -32.68 | — | +0.00 | -32.68 | -240.80 | — |
| 2026-08-25 | `COTY` | 672 | $2.78 | $2.80 | +13.44 | — | +0.00 | +13.44 | +168.00 | — |
| 2026-08-25 | `DQ` | 118 | $14.15 | $14.04 | -12.98 | — | +0.00 | -12.98 | -47.20 | — |
| 2026-08-25 | `FUTU` | 14 | $116.49 | $118.02 | +21.42 | — | +0.00 | +21.42 | +5.18 | — |
| 2026-08-25 | `IOND` | 26 | $70.11 | $68.27 | -47.84 | — | +0.00 | -47.84 | +69.42 | — |
| 2026-08-25 | `BKE` | 1 | $44.46 | $44.41 | -0.05 | $44.41 | +0.00 | -0.05 | +1.33 | +1.33 |
| 2026-08-25 | `PSEC` | 29 | $2.31 | $2.32 | +0.29 | $2.33 | +0.29 | +0.58 | +0.58 | +0.87 |
| 2026-08-25 | `BNS` | 22 | — | $86.86 | +0.00 | $90.08 | +70.84 | +70.84 | +0.00 | +70.84 |
| 2026-08-25 | `BZ` | 124 | — | $15.34 | +0.00 | $16.32 | +121.52 | +121.52 | +0.00 | +121.52 |
| 2026-08-25 | `DKS` | 10 | — | $179.33 | +0.00 | $156.70 | -226.30 | -226.30 | +0.00 | -226.30 |
| 2026-08-25 | `GRRR` | 134 | — | $14.26 | +0.00 | $14.20 | -8.04 | -8.04 | +0.00 | -8.04 |
| 2026-08-25 | `SHMD` | 406 | — | $4.71 | +0.00 | $4.71 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-25 | `TUYA` | 1082 | — | $1.77 | +0.00 | $1.82 | +54.10 | +54.10 | +0.00 | +54.10 |
| 2026-08-25 | `VIPS` | 137 | — | $13.91 | +0.00 | $13.83 | -10.96 | -10.96 | +0.00 | -10.96 |
| 2026-08-26 | `BKE` | 1 | $44.41 | $44.41 | +0.00 | $44.41 | +0.00 | +0.00 | +1.33 | +1.33 |
| 2026-08-26 | `PSEC` | 29 | $2.33 | $2.33 | +0.00 | $2.33 | +0.00 | +0.00 | +0.87 | +0.87 |
| 2026-08-26 | `BNS` | 22 | $90.08 | $90.08 | +0.00 | $90.08 | +0.00 | +0.00 | +70.84 | +70.84 |
| 2026-08-26 | `BZ` | 124 | $16.32 | $16.32 | +0.00 | $16.32 | +0.00 | +0.00 | +121.52 | +121.52 |
| 2026-08-26 | `DKS` | 10 | $156.70 | $156.70 | +0.00 | $156.70 | +0.00 | +0.00 | -226.30 | -226.30 |
| 2026-08-26 | `GRRR` | 134 | $14.20 | $14.20 | +0.00 | $14.20 | +0.00 | +0.00 | -8.04 | -8.04 |
| 2026-08-26 | `SHMD` | 406 | $4.71 | $4.71 | +0.00 | $4.71 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-26 | `TUYA` | 1082 | $1.82 | $1.82 | +0.00 | $1.82 | +0.00 | +0.00 | +54.10 | +54.10 |
| 2026-08-26 | `VIPS` | 137 | $13.83 | $13.83 | +0.00 | $13.83 | +0.00 | +0.00 | -10.96 | -10.96 |
| 2026-08-27 | `BKE` | 1 | $44.41 | $44.39 | -0.02 | — | +0.00 | -0.02 | +1.31 | — |
| 2026-08-27 | `PSEC` | 29 | $2.33 | $2.35 | +0.58 | — | +0.00 | +0.58 | +1.45 | — |
| 2026-08-27 | `BNS` | 22 | $90.08 | $92.64 | +56.32 | $93.59 | +20.90 | +77.22 | +127.16 | +148.06 |
| 2026-08-27 | `BZ` | 124 | $16.32 | $16.77 | +55.80 | $18.84 | +256.68 | +312.48 | +177.32 | +434.00 |
| 2026-08-27 | `DKS` | 10 | $156.70 | $121.87 | -348.30 | $129.66 | +77.90 | -270.40 | -574.60 | -496.70 |
| 2026-08-27 | `GRRR` | 134 | $14.20 | $14.03 | -22.78 | $15.45 | +190.28 | +167.50 | -30.82 | +159.46 |
| 2026-08-27 | `SHMD` | 406 | $4.71 | $3.38 | -539.98 | $3.17 | -85.26 | -625.24 | -539.98 | -625.24 |
| 2026-08-27 | `TUYA` | 1082 | $1.82 | $1.78 | -43.28 | $1.83 | +54.10 | +10.82 | +10.82 | +64.92 |
| 2026-08-27 | `VIPS` | 137 | $13.83 | $14.00 | +23.29 | $14.08 | +10.96 | +34.25 | +12.33 | +23.29 |
| 2026-08-28 | `BNS` | 22 | $93.59 | $93.52 | -1.54 | — | +0.00 | -1.54 | +146.52 | — |
| 2026-08-28 | `BZ` | 124 | $18.84 | $18.50 | -42.16 | — | +0.00 | -42.16 | +391.84 | — |
| 2026-08-28 | `DKS` | 10 | $129.66 | $128.73 | -9.30 | — | +0.00 | -9.30 | -506.00 | — |
| 2026-08-28 | `GRRR` | 134 | $15.45 | $15.94 | +65.66 | — | +0.00 | +65.66 | +225.12 | — |
| 2026-08-28 | `SHMD` | 406 | $3.17 | $3.16 | -4.06 | — | +0.00 | -4.06 | -629.30 | — |
| 2026-08-28 | `TUYA` | 1082 | $1.83 | $1.85 | +21.64 | — | +0.00 | +21.64 | +86.56 | — |
| 2026-08-28 | `VIPS` | 137 | $14.08 | $14.00 | -10.96 | — | +0.00 | -10.96 | +12.33 | — |
| 2026-08-28 | `ADSK` | 6 | — | $261.47 | +0.00 | $270.58 | +54.66 | +54.66 | +0.00 | +54.66 |
| 2026-08-28 | `ESTC` | 19 | — | $82.64 | +0.00 | $83.74 | +20.90 | +20.90 | +0.00 | +20.90 |
| 2026-08-28 | `HAFN` | 208 | — | $7.91 | +0.00 | $8.29 | +79.04 | +79.04 | +0.00 | +79.04 |
| 2026-08-28 | `PD` | 132 | — | $12.45 | +0.00 | $12.63 | +23.76 | +23.76 | +0.00 | +23.76 |
| 2026-08-28 | `RBRK` | 16 | — | $101.99 | +0.00 | $107.02 | +80.48 | +80.48 | +0.00 | +80.48 |
| 2026-08-28 | `S` | 75 | — | $21.80 | +0.00 | $22.71 | +68.25 | +68.25 | +0.00 | +68.25 |
| 2026-08-28 | `ULTA` | 3 | — | $536.07 | +0.00 | $540.10 | +12.09 | +12.09 | +0.00 | +12.09 |
| 2026-08-28 | `WDAY` | 8 | — | $195.40 | +0.00 | $193.57 | -14.64 | -14.64 | +0.00 | -14.64 |
| 2026-08-31 | `ADSK` | 6 | $270.58 | $258.50 | -72.48 | $259.14 | +3.84 | -68.64 | -17.82 | -13.98 |
| 2026-08-31 | `ESTC` | 19 | $83.74 | $99.99 | +308.75 | $99.00 | -18.81 | +289.94 | +329.65 | +310.84 |
| 2026-08-31 | `HAFN` | 208 | $8.29 | $8.43 | +29.12 | $8.45 | +4.16 | +33.28 | +108.16 | +112.32 |
| 2026-08-31 | `PD` | 132 | $12.63 | $13.92 | +170.28 | $13.70 | -29.04 | +141.24 | +194.04 | +165.00 |
| 2026-08-31 | `RBRK` | 16 | $107.02 | $92.46 | -232.96 | $92.46 | +0.00 | -232.96 | -152.48 | -152.48 |
| 2026-08-31 | `S` | 75 | $22.71 | $21.48 | -92.25 | $21.50 | +1.50 | -90.75 | -24.00 | -22.50 |
| 2026-08-31 | `ULTA` | 3 | $540.10 | $517.50 | -67.80 | $517.50 | +0.00 | -67.80 | -55.71 | -55.71 |
| 2026-08-31 | `WDAY` | 8 | $193.57 | $202.96 | +75.12 | $203.45 | +3.92 | +79.04 | +60.48 | +64.40 |
| 2026-09-01 | `ADSK` | 6 | $259.14 | $258.17 | -5.82 | $259.89 | +10.32 | +4.50 | -19.80 | -9.48 |
| 2026-09-01 | `ESTC` | 19 | $99.00 | $96.54 | -46.74 | $96.07 | -8.93 | -55.67 | +264.10 | +255.17 |
| 2026-09-01 | `HAFN` | 208 | $8.45 | $8.43 | -4.16 | $8.41 | -4.16 | -8.32 | +108.16 | +104.00 |
| 2026-09-01 | `PD` | 132 | $13.70 | $13.89 | +25.08 | $13.89 | +0.00 | +25.08 | +190.08 | +190.08 |
| 2026-09-01 | `RBRK` | 16 | $92.46 | $90.89 | -25.12 | $91.50 | +9.76 | -15.36 | -177.60 | -167.84 |
| 2026-09-01 | `S` | 75 | $21.50 | $22.11 | +45.75 | $21.84 | -20.25 | +25.50 | +23.25 | +3.00 |
| 2026-09-01 | `ULTA` | 3 | $517.50 | $538.75 | +63.75 | $537.60 | -3.45 | +60.30 | +8.04 | +4.59 |
| 2026-09-01 | `WDAY` | 8 | $203.45 | $198.51 | -39.52 | $195.18 | -26.64 | -66.16 | +24.88 | -1.76 |
| 2026-09-02 | `ADSK` | 6 | $259.89 | $253.48 | -38.46 | — | +0.00 | -38.46 | -47.94 | — |
| 2026-09-02 | `ESTC` | 19 | $96.07 | $95.76 | -5.89 | — | +0.00 | -5.89 | +249.28 | — |
| 2026-09-02 | `HAFN` | 208 | $8.41 | $8.56 | +31.20 | — | +0.00 | +31.20 | +135.20 | — |
| 2026-09-02 | `PD` | 132 | $13.89 | $13.91 | +2.64 | — | +0.00 | +2.64 | +192.72 | — |
| 2026-09-02 | `RBRK` | 16 | $91.50 | $91.70 | +3.20 | — | +0.00 | +3.20 | -164.64 | — |
| 2026-09-02 | `S` | 75 | $21.84 | $21.72 | -9.00 | — | +0.00 | -9.00 | -6.00 | — |
| 2026-09-02 | `ULTA` | 3 | $537.60 | $527.84 | -29.28 | — | +0.00 | -29.28 | -24.69 | — |
| 2026-09-02 | `WDAY` | 8 | $195.18 | $196.36 | +9.44 | — | +0.00 | +9.44 | +7.68 | — |
| 2026-09-03 | `CHPT` | 318 | — | $5.30 | +0.00 | $5.19 | -34.98 | -34.98 | +0.00 | -34.98 |
| 2026-09-03 | `FIVE` | 6 | — | $244.98 | +0.00 | $243.08 | -11.40 | -11.40 | +0.00 | -11.40 |
| 2026-09-03 | `HPE` | 32 | — | $51.99 | +0.00 | $51.83 | -5.12 | -5.12 | +0.00 | -5.12 |
| 2026-09-03 | `MOMO` | 310 | — | $5.43 | +0.00 | $5.49 | +18.60 | +18.60 | +0.00 | +18.60 |
| 2026-09-03 | `NTSK` | 121 | — | $13.94 | +0.00 | $13.75 | -22.99 | -22.99 | +0.00 | -22.99 |
| 2026-09-03 | `PHR` | 143 | — | $11.79 | +0.00 | $11.85 | +8.58 | +8.58 | +0.00 | +8.58 |
| 2026-09-03 | `PVH` | 23 | — | $73.10 | +0.00 | $72.29 | -18.63 | -18.63 | +0.00 | -18.63 |
| 2026-09-03 | `SNOW` | 5 | — | $310.54 | +0.00 | $305.84 | -23.50 | -23.50 | +0.00 | -23.50 |
| 2026-09-04 | `CHPT` | 318 | $5.19 | $6.90 | +543.78 | $9.08 | +693.24 | +1237.02 | +508.80 | +1202.04 |
| 2026-09-04 | `FIVE` | 6 | $243.08 | $256.99 | +83.46 | $239.96 | -102.18 | -18.72 | +72.06 | -30.12 |
| 2026-09-04 | `HPE` | 32 | $51.83 | $47.60 | -135.36 | $54.44 | +218.88 | +83.52 | -140.48 | +78.40 |
| 2026-09-04 | `MOMO` | 310 | $5.49 | $5.50 | +3.10 | $5.10 | -124.00 | -120.90 | +21.70 | -102.30 |
| 2026-09-04 | `NTSK` | 121 | $13.75 | $15.51 | +212.96 | $14.34 | -141.57 | +71.39 | +189.97 | +48.40 |
| 2026-09-04 | `PHR` | 143 | $11.85 | $11.02 | -118.69 | $11.10 | +11.44 | -107.25 | -110.11 | -98.67 |
| 2026-09-04 | `PVH` | 23 | $72.29 | $74.96 | +61.41 | $72.46 | -57.50 | +3.91 | +42.78 | -14.72 |
| 2026-09-04 | `SNOW` | 5 | $305.84 | $377.24 | +357.00 | $356.47 | -103.85 | +253.15 | +333.50 | +229.65 |
| 2026-09-04 | `ASAN` | 4 | — | $10.16 | +0.00 | $10.09 | -0.28 | -0.28 | +0.00 | -0.28 |
| 2026-09-04 | `DOMO` | 12 | — | $3.78 | +0.00 | $3.79 | +0.12 | +0.12 | +0.00 | +0.12 |
| 2026-09-04 | `IOT` | 1 | — | $37.69 | +0.00 | $38.75 | +1.06 | +1.06 | +0.00 | +1.06 |
| 2026-09-04 | `MAMA` | 2 | — | $15.62 | +0.00 | $15.96 | +0.68 | +0.68 | +0.00 | +0.68 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +1,095.84 | INO | — | $2.29 | $10,960.69 | INO×12176 |
| 2026-08-14 | +5.50 | $2.29 | INO×12176 | $11,325.97 | +365.28 | +1,948.16 | — | — | $2.29 | $13,274.13 | INO×12176 |
| 2026-08-17 | +2.25 | $2.29 | INO×12176 | $13,030.61 | -243.52 | +974.08 | — | — | $2.29 | $14,004.69 | INO×12176 |
| 2026-08-18 | -6.20 | $2.29 | INO×12176 | $13,882.93 | -121.76 | +0.00 | — | INO | $13,723.72 | $13,723.72 | — |
| 2026-08-19 | -7.20 | $13,723.72 | — | $13,723.72 | +0.00 | +0.00 | — | — | $13,723.72 | $13,723.72 | — |
| 2026-08-20 | +1.12 | $13,723.72 | — | $13,723.72 | +0.00 | +101.64 | ATAT, ATHM, BABA, BULL, COTY, DQ, FUTU, IOND | — | $206.77 | $13,801.36 | ATAT×50, ATHM×76, BABA×13, BULL×172, COTY×672, DQ×118, FUTU×14, IOND×26 |
| 2026-08-21 | +3.25 | $206.77 | ATAT×50, ATHM×76, BABA×13, BULL×172, COTY×672, DQ×118, FUTU×14, IOND×26 | $13,767.60 | -33.76 | -109.77 | BKE, PSEC | — | $95.80 | $13,656.64 | ATAT×50, ATHM×76, BABA×13, BULL×172, COTY×672, DQ×118, FUTU×14, IOND×26, BKE×1, PSEC×29 |
| 2026-08-24 | -5.17 | $95.80 | ATAT×50, ATHM×76, BABA×13, BULL×172, COTY×672, DQ×118, FUTU×14, IOND×26, BKE×1, PSEC×29 | $13,491.40 | -165.24 | +164.07 | — | — | $95.80 | $13,655.47 | ATAT×50, ATHM×76, BABA×13, BULL×172, COTY×672, DQ×118, FUTU×14, IOND×26, BKE×1, PSEC×29 |
| 2026-08-25 | +1.80 | $95.80 | ATAT×50, ATHM×76, BABA×13, BULL×172, COTY×672, DQ×118, FUTU×14, IOND×26, BKE×1, PSEC×29 | $13,552.77 | -102.70 | +1.45 | BNS, BZ, DKS, GRRR, SHMD, TUYA, VIPS | ATAT, ATHM, BABA, BULL, COTY, DQ, FUTU, IOND | $136.04 | $13,499.47 | BKE×1, PSEC×29, BNS×22, BZ×124, DKS×10, GRRR×134, SHMD×406, TUYA×1082, VIPS×137 |
| 2026-08-26 | +2.02 | $136.04 | BKE×1, PSEC×29, BNS×22, BZ×124, DKS×10, GRRR×134, SHMD×406, TUYA×1082, VIPS×137 | $13,499.47 | -0.00 | +0.00 | — | — | $136.04 | $13,499.47 | BKE×1, PSEC×29, BNS×22, BZ×124, DKS×10, GRRR×134, SHMD×406, TUYA×1082, VIPS×137 |
| 2026-08-27 | — | $136.04 | BKE×1, PSEC×29, BNS×22, BZ×124, DKS×10, GRRR×134, SHMD×406, TUYA×1082, VIPS×137 | $12,681.10 | -818.37 | +525.56 | — | BKE, PSEC | $247.32 | $13,205.40 | BNS×22, BZ×124, DKS×10, GRRR×134, SHMD×406, TUYA×1082, VIPS×137 |
| 2026-08-28 | +0.75 | $247.32 | BNS×22, BZ×124, DKS×10, GRRR×134, SHMD×406, TUYA×1082, VIPS×137 | $13,224.68 | +19.28 | +324.54 | ADSK, ESTC, HAFN, PD, RBRK, S, ULTA, WDAY | BNS, BZ, DKS, GRRR, SHMD, TUYA, VIPS | $310.52 | $13,500.97 | ADSK×6, ESTC×19, HAFN×208, PD×132, RBRK×16, S×75, ULTA×3, WDAY×8 |
| 2026-08-31 | -5.85 | $310.52 | ADSK×6, ESTC×19, HAFN×208, PD×132, RBRK×16, S×75, ULTA×3, WDAY×8 | $13,618.75 | +117.78 | -34.43 | — | — | $310.52 | $13,584.32 | ADSK×6, ESTC×19, HAFN×208, PD×132, RBRK×16, S×75, ULTA×3, WDAY×8 |
| 2026-09-01 | -6.30 | $310.52 | ADSK×6, ESTC×19, HAFN×208, PD×132, RBRK×16, S×75, ULTA×3, WDAY×8 | $13,597.54 | +13.22 | -43.35 | — | — | $310.52 | $13,554.19 | ADSK×6, ESTC×19, HAFN×208, PD×132, RBRK×16, S×75, ULTA×3, WDAY×8 |
| 2026-09-02 | -3.83 | $310.52 | ADSK×6, ESTC×19, HAFN×208, PD×132, RBRK×16, S×75, ULTA×3, WDAY×8 | $13,518.04 | -36.15 | +0.00 | — | ADSK, ESTC, HAFN, PD, RBRK, S, ULTA, WDAY | $13,500.43 | $13,500.43 | — |
| 2026-09-03 | -0.90 | $13,500.43 | — | $13,500.43 | -0.00 | -89.44 | CHPT, FIVE, HPE, MOMO, NTSK, PHR, PVH, SNOW | — | $370.42 | $13,389.95 | CHPT×318, FIVE×6, HPE×32, MOMO×310, NTSK×121, PHR×143, PVH×23, SNOW×5 |
| 2026-09-04 | — | $370.42 | CHPT×318, FIVE×6, HPE×32, MOMO×310, NTSK×121, PHR×143, PVH×23, SNOW×5 | $14,397.61 | +1,007.66 | +396.04 | ASAN, DOMO, IOT, MAMA | — | $213.89 | $14,792.05 | CHPT×318, FIVE×6, HPE×32, MOMO×310, NTSK×121, PHR×143, PVH×23, SNOW×5, ASAN×4, DOMO×12, IOT×1, MAMA×2 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 12176 | $0.81 | $135.15 | — | $2.29 | — | combo gate; gate earn_react=True,last_green=True; list flatten; ⚪; ret5=+13.2; leftover $10000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.29 | ▲ close $10,960.69 vs 09:30 $10,000.00 (session +1,095.84) | 16:00 close · cash $2.29 · equity $10,960.69 vs 09:30 $10,000.00 (+960.69; session marks +1095.84) · 1 name(s) marked open→close (per-name table). INO×12176 09:30 $0.81 → close $0.90 +1095.84 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.29 | ▲ 09:30 equity $11,325.97 vs yday $10,960.69 (+365.28) | 09:30 open · cash $2.29 (unchanged overnight, no fees) · equity $11,325.97 vs prior close $10,960.69 (+365.28) · 1 name(s) re-marked at the open (per-name table). INO×12176 yday $0.90 → 09:30 $0.93 +365.28 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.29 | ▲ close $13,274.13 vs 09:30 $11,325.97 (session +1,948.16) | 16:00 close · cash $2.29 · equity $13,274.13 vs 09:30 $11,325.97 (+1948.16; session marks +1948.16) · 1 name(s) marked open→close (per-name table). INO×12176 09:30 $0.93 → close $1.09 +1948.16 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.29 | ▼ 09:30 equity $13,030.61 vs yday $13,274.13 (-243.52) | 09:30 open · cash $2.29 (unchanged overnight, no fees) · equity $13,030.61 vs prior close $13,274.13 (-243.52) · 1 name(s) re-marked at the open (per-name table). INO×12176 yday $1.09 → 09:30 $1.07 -243.52 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.29 | ▲ close $14,004.69 vs 09:30 $13,030.61 (session +974.08) | 16:00 close · cash $2.29 · equity $14,004.69 vs 09:30 $13,030.61 (+974.08; session marks +974.08) · 1 name(s) marked open→close (per-name table). INO×12176 09:30 $1.07 → close $1.15 +974.08 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.29 | ▼ 09:30 equity $13,882.93 vs yday $14,004.69 (-121.76) | 09:30 open · cash $2.29 (unchanged overnight, no fees) · equity $13,882.93 vs prior close $14,004.69 (-121.76) · 1 name(s) re-marked at the open (per-name table). INO×12176 yday $1.15 → 09:30 $1.14 -121.76 | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 12176 | $1.14 | $159.20 | $+3723.72 | $13,723.72 | ▲ +3,723.72 after sell → book $13,723.72; vs 09:30 mark -159.21 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,723.72 | ▲ close $13,723.72 vs 09:30 $13,882.93 (session +0.00) | 16:00 close · cash $13,723.72 · no lots left · equity $13,723.72. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,723.72 | ▲ 09:30 equity $13,723.72 vs yday $13,723.72 (+0.00) | 09:30 open · cash $13,723.72 · no holdings · equity $13,723.72 vs prior close $13,723.72 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,723.72 | ▲ close $13,723.72 vs 09:30 $13,723.72 (session +0.00) | 16:00 close · cash $13,723.72 · no lots left · equity $13,723.72. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,723.72 | ▲ 09:30 equity $13,723.72 vs yday $13,723.72 (+0.00) | 09:30 open · cash $13,723.72 · no holdings · equity $13,723.72 vs prior close $13,723.72 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `ATAT` | 50 | $34.05 | $2.14 | — | $12,019.08 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+9.3; leftover $1715.47 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ATHM` | 76 | $22.44 | $2.22 | — | $10,311.43 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=-2.1; leftover $1715.47 | join🔴 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 peer🔴 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BABA` | 13 | $123.47 | $2.03 | — | $8,704.29 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+2.9; leftover $1715.47 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BULL` | 172 | $9.94 | $2.51 | — | $6,992.10 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+12.6; leftover $1715.47 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `COTY` | 672 | $2.55 | $8.67 | — | $5,269.83 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ⚪; ret5=+9.8; leftover $1715.47 | join🟡 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `DQ` | 118 | $14.44 | $2.34 | — | $3,563.57 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-3.8; leftover $1715.47 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `FUTU` | 14 | $117.65 | $2.03 | — | $1,914.44 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+4.1; leftover $1715.47 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IOND` | 26 | $65.60 | $2.07 | — | $206.77 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ⚪; ret5=+3.7; leftover $1715.47 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $206.77 | ▲ close $13,801.36 vs 09:30 $13,723.72 (session +101.64) | 16:00 close · cash $206.77 · equity $13,801.36 vs 09:30 $13,723.72 (+77.64; session marks +101.64) · 8 name(s) marked open→close (per-name table). ATAT×50 09:30 $34.05 → close $34.25 +10.00; ATHM×76 09:30 $22.44 → close $22.12 -24.32; BABA×13 09:30 $123.47 → close $130.53 +91.78; BULL×172 09:30 $9.94 → close $8.85 -187.48; COTY×672 09:30 $2.55 → close $2.75 +134.40; DQ×118 09:30 $14.44 → close $14.98 +63.72; FUTU×14 09:30 $117.65 → close $112.73 -68.88; IOND×26 09:30 $65.60 → close $68.77 +82.42 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $206.77 | ▼ 09:30 equity $13,767.60 vs yday $13,801.36 (-33.76) | 09:30 open · cash $206.77 (unchanged overnight, no fees) · equity $13,767.60 vs prior close $13,801.36 (-33.76) · 8 name(s) re-marked at the open (per-name table). ATAT×50 yday $34.25 → 09:30 $34.31 +3.00; ATHM×76 yday $22.12 → 09:30 $22.20 +6.08; BABA×13 yday $130.53 → 09:30 $125.35 -67.34; BULL×172 yday $8.85 → 09:30 $8.99 +24.08; COTY×672 yday $2.75 → 09:30 $2.71 -26.88; DQ×118 yday $14.98 → 09:30 $15.00 +2.36; FUTU×14 yday $112.73 → 09:30 $115.18 +34.30; IOND×26 yday $68.77 → 09:30 $68.41 -9.36 | — |
| 2026-08-21 09:30 ET | **BUY** | `BKE` | 1 | $43.08 | $0.43 | — | $163.25 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=-4.9; leftover $68.92 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `PSEC` | 29 | $2.30 | $0.75 | — | $95.80 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=-3.0; leftover $68.92 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $95.80 | ▼ close $13,656.64 vs 09:30 $13,767.60 (session -109.77) | 16:00 close · cash $95.80 · equity $13,656.64 vs 09:30 $13,767.60 (-110.96; session marks -109.77) · 10 name(s) marked open→close (per-name table). ATAT×50 09:30 $34.31 → close $34.75 +22.00; ATHM×76 09:30 $22.20 → close $22.22 +1.52; BABA×13 09:30 $125.35 → close $119.34 -78.13; BULL×172 09:30 $8.99 → close $8.78 -36.12; COTY×672 09:30 $2.71 → close $2.74 +20.16; DQ×118 09:30 $15.00 → close $13.58 -167.56; FUTU×14 09:30 $115.18 → close $123.64 +118.44; IOND×26 09:30 $68.41 → close $68.73 +8.32; BKE×1 09:30 $43.08 → close $43.81 +0.73; PSEC×29 09:30 $2.30 → close $2.33 +0.87 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $95.80 | ▼ 09:30 equity $13,491.40 vs yday $13,656.64 (-165.24) | 09:30 open · cash $95.80 (unchanged overnight, no fees) · equity $13,491.40 vs prior close $13,656.64 (-165.24) · 10 name(s) re-marked at the open (per-name table). ATAT×50 yday $34.75 → 09:30 $34.70 -2.50; ATHM×76 yday $22.22 → 09:30 $21.78 -33.44; BABA×13 yday $119.34 → 09:30 $116.80 -33.02; BULL×172 yday $8.78 → 09:30 $8.54 -41.28; COTY×672 yday $2.74 → 09:30 $2.72 -13.44; DQ×118 yday $13.58 → 09:30 $13.55 -3.54; FUTU×14 yday $123.64 → 09:30 $120.87 -38.78; IOND×26 yday $68.73 → 09:30 $68.72 -0.26; BKE×1 yday $43.81 → 09:30 $44.54 +0.73; PSEC×29 yday $2.33 → 09:30 $2.34 +0.29 | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $95.80 | ▲ close $13,655.47 vs 09:30 $13,491.40 (session +164.07) | 16:00 close · cash $95.80 · equity $13,655.47 vs 09:30 $13,491.40 (+164.07; session marks +164.07) · 10 name(s) marked open→close (per-name table). ATAT×50 09:30 $34.70 → close $34.83 +6.50; ATHM×76 09:30 $21.78 → close $21.85 +5.32; BABA×13 09:30 $116.80 → close $119.46 +34.58; BULL×172 09:30 $8.54 → close $8.73 +32.68; COTY×672 09:30 $2.72 → close $2.78 +40.32; DQ×118 09:30 $13.55 → close $14.15 +70.80; FUTU×14 09:30 $120.87 → close $116.49 -61.32; IOND×26 09:30 $68.72 → close $70.11 +36.14; BKE×1 09:30 $44.54 → close $44.46 -0.08; PSEC×29 09:30 $2.34 → close $2.31 -0.87 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $95.80 | ▼ 09:30 equity $13,552.77 vs yday $13,655.47 (-102.70) | 09:30 open · cash $95.80 (unchanged overnight, no fees) · equity $13,552.77 vs prior close $13,655.47 (-102.70) · 10 name(s) re-marked at the open (per-name table). ATAT×50 yday $34.83 → 09:30 $34.75 -4.00; ATHM×76 yday $21.85 → 09:30 $21.85 +0.00; BABA×13 yday $119.46 → 09:30 $116.36 -40.30; BULL×172 yday $8.73 → 09:30 $8.54 -32.68; COTY×672 yday $2.78 → 09:30 $2.80 +13.44; DQ×118 yday $14.15 → 09:30 $14.04 -12.98; FUTU×14 yday $116.49 → 09:30 $118.02 +21.42; IOND×26 yday $70.11 → 09:30 $68.27 -47.84; BKE×1 yday $44.46 → 09:30 $44.41 -0.05; PSEC×29 yday $2.31 → 09:30 $2.32 +0.29 | — |
| 2026-08-25 09:30 ET | **SELL** | `ATAT` | 50 | $34.75 | $2.16 | $+30.70 | $1,831.14 | ▲ +30.70 after sell → book $13,550.61; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ATHM` | 76 | $21.85 | $2.24 | $-49.30 | $3,489.49 | ▼ -49.30 after sell → book $13,548.36; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BABA` | 13 | $116.36 | $2.05 | $-96.51 | $5,000.12 | ▼ -96.51 after sell → book $13,546.31; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BULL` | 172 | $8.54 | $2.55 | $-245.85 | $6,466.45 | ▼ -245.85 after sell → book $13,543.76; vs 09:30 mark -2.55 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `COTY` | 672 | $2.80 | $8.80 | $+150.54 | $8,339.26 | ▲ +150.54 after sell → book $13,534.97; vs 09:30 mark -8.79 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `DQ` | 118 | $14.04 | $2.38 | $-51.92 | $9,993.60 | ▼ -51.92 after sell → book $13,532.59; vs 09:30 mark -2.38 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `FUTU` | 14 | $118.02 | $2.06 | $+1.09 | $11,643.83 | ▲ +1.09 after sell → book $13,530.54; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **SELL** | `IOND` | 26 | $68.27 | $2.09 | $+65.26 | $13,416.76 | ▲ +65.26 after sell → book $13,528.45; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `BNS` | 22 | $86.86 | $2.06 | — | $11,503.78 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-4.3; leftover $1916.68 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BZ` | 124 | $15.34 | $2.36 | — | $9,599.26 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ⚪; ret5=+2.8; leftover $1916.68 | join🟢 sector🟡 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `DKS` | 10 | $179.33 | $2.02 | — | $7,803.94 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-9.3; leftover $1916.68 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `GRRR` | 134 | $14.26 | $2.39 | — | $5,890.71 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-1.9; leftover $1916.68 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SHMD` | 406 | $4.71 | $5.24 | — | $3,973.21 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-9.9; leftover $1916.68 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `TUYA` | 1082 | $1.77 | $13.96 | — | $2,044.11 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-1.1; leftover $1916.68 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `VIPS` | 137 | $13.91 | $2.40 | — | $136.04 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ⚪; ret5=+2.5; leftover $1916.68 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $136.04 | ▲ close $13,499.47 vs 09:30 $13,552.77 (session +1.45) | 16:00 close · cash $136.04 · equity $13,499.47 vs 09:30 $13,552.77 (-53.30; session marks +1.45) · 9 name(s) marked open→close (per-name table). BKE×1 09:30 $44.41 → close $44.41 +0.00; PSEC×29 09:30 $2.32 → close $2.33 +0.29; BNS×22 09:30 $86.86 → close $90.08 +70.84; BZ×124 09:30 $15.34 → close $16.32 +121.52; DKS×10 09:30 $179.33 → close $156.70 -226.30; GRRR×134 09:30 $14.26 → close $14.20 -8.04; SHMD×406 09:30 $4.71 → close $4.71 +0.00; TUYA×1082 09:30 $1.77 → close $1.82 +54.10; VIPS×137 09:30 $13.91 → close $13.83 -10.96 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $136.04 | ▲ 09:30 equity $13,499.47 vs yday $13,499.47 (-0.00) | 09:30 open · cash $136.04 (unchanged overnight, no fees) · equity $13,499.47 vs prior close $13,499.47 (-0.00) · 9 name(s) re-marked at the open (per-name table). BKE×1 yday $44.41 → 09:30 $44.41 +0.00; PSEC×29 yday $2.33 → 09:30 $2.33 +0.00; BNS×22 yday $90.08 → 09:30 $90.08 +0.00; BZ×124 yday $16.32 → 09:30 $16.32 +0.00; DKS×10 yday $156.70 → 09:30 $156.70 +0.00; GRRR×134 yday $14.20 → 09:30 $14.20 +0.00; SHMD×406 yday $4.71 → 09:30 $4.71 +0.00; TUYA×1082 yday $1.82 → 09:30 $1.82 +0.00; VIPS×137 yday $13.83 → 09:30 $13.83 +0.00 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $136.04 | ▲ close $13,499.47 vs 09:30 $13,499.47 (session +0.00) | 16:00 close · cash $136.04 · equity $13,499.47 vs 09:30 $13,499.47 (-0.00; session marks +0.00) · 9 name(s) marked open→close (per-name table). BKE×1 09:30 $44.41 → close $44.41 +0.00; PSEC×29 09:30 $2.33 → close $2.33 +0.00; BNS×22 09:30 $90.08 → close $90.08 +0.00; BZ×124 09:30 $16.32 → close $16.32 +0.00; DKS×10 09:30 $156.70 → close $156.70 +0.00; GRRR×134 09:30 $14.20 → close $14.20 +0.00; SHMD×406 09:30 $4.71 → close $4.71 +0.00; TUYA×1082 09:30 $1.82 → close $1.82 +0.00; VIPS×137 09:30 $13.83 → close $13.83 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $136.04 | ▼ 09:30 equity $12,681.10 vs yday $13,499.47 (-818.37) | 09:30 open · cash $136.04 (unchanged overnight, no fees) · equity $12,681.10 vs prior close $13,499.47 (-818.37) · 9 name(s) re-marked at the open (per-name table). BKE×1 yday $44.41 → 09:30 $44.39 -0.02; PSEC×29 yday $2.33 → 09:30 $2.35 +0.58; BNS×22 yday $90.08 → 09:30 $92.64 +56.32; BZ×124 yday $16.32 → 09:30 $16.77 +55.80; DKS×10 yday $156.70 → 09:30 $121.87 -348.30; GRRR×134 yday $14.20 → 09:30 $14.03 -22.78; SHMD×406 yday $4.71 → 09:30 $3.38 -539.98; TUYA×1082 yday $1.82 → 09:30 $1.78 -43.28; VIPS×137 yday $13.83 → 09:30 $14.00 +23.29 | — |
| 2026-08-27 09:30 ET | **SELL** | `BKE` | 1 | $44.39 | $0.47 | $+0.41 | $179.96 | ▲ +0.41 after sell → book $12,680.63; vs 09:30 mark -0.47 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `PSEC` | 29 | $2.35 | $0.79 | $-0.09 | $247.32 | ▼ -0.09 after sell → book $12,679.84; vs 09:30 mark -0.79 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $247.32 | ▲ close $13,205.40 vs 09:30 $12,681.10 (session +525.56) | 16:00 close · cash $247.32 · equity $13,205.40 vs 09:30 $12,681.10 (+524.30; session marks +525.56) · 7 name(s) marked open→close (per-name table). BNS×22 09:30 $92.64 → close $93.59 +20.90; BZ×124 09:30 $16.77 → close $18.84 +256.68; DKS×10 09:30 $121.87 → close $129.66 +77.90; GRRR×134 09:30 $14.03 → close $15.45 +190.28; SHMD×406 09:30 $3.38 → close $3.17 -85.26; TUYA×1082 09:30 $1.78 → close $1.83 +54.10; VIPS×137 09:30 $14.00 → close $14.08 +10.96 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $247.32 | ▲ 09:30 equity $13,224.68 vs yday $13,205.40 (+19.28) | 09:30 open · cash $247.32 (unchanged overnight, no fees) · equity $13,224.68 vs prior close $13,205.40 (+19.28) · 7 name(s) re-marked at the open (per-name table). BNS×22 yday $93.59 → 09:30 $93.52 -1.54; BZ×124 yday $18.84 → 09:30 $18.50 -42.16; DKS×10 yday $129.66 → 09:30 $128.73 -9.30; GRRR×134 yday $15.45 → 09:30 $15.94 +65.66; SHMD×406 yday $3.17 → 09:30 $3.16 -4.06; TUYA×1082 yday $1.83 → 09:30 $1.85 +21.64; VIPS×137 yday $14.08 → 09:30 $14.00 -10.96 | — |
| 2026-08-28 09:30 ET | **SELL** | `BNS` | 22 | $93.52 | $2.08 | $+142.38 | $2,302.68 | ▲ +142.38 after sell → book $13,222.60; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BZ` | 124 | $18.50 | $2.40 | $+387.08 | $4,594.28 | ▲ +387.08 after sell → book $13,220.20; vs 09:30 mark -2.40 | dropped from list after 3 sess (min 3) | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `DKS` | 10 | $128.73 | $2.04 | $-510.06 | $5,879.54 | ▼ -510.06 after sell → book $13,218.16; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `GRRR` | 134 | $15.94 | $2.43 | $+220.30 | $8,013.07 | ▲ +220.30 after sell → book $13,215.73; vs 09:30 mark -2.43 | dropped from list after 3 sess (min 3) | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `SHMD` | 406 | $3.16 | $5.32 | $-639.85 | $9,290.71 | ▼ -639.85 after sell → book $13,210.41; vs 09:30 mark -5.32 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `TUYA` | 1082 | $1.85 | $14.15 | $+58.45 | $11,278.26 | ▲ +58.45 after sell → book $13,196.26; vs 09:30 mark -14.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `VIPS` | 137 | $14.00 | $2.44 | $+7.49 | $13,193.82 | ▲ +7.49 after sell → book $13,193.82; vs 09:30 mark -2.44 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 6 | $261.47 | $2.01 | — | $11,622.99 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+0.9; leftover $1649.23 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ESTC` | 19 | $82.64 | $2.05 | — | $10,050.79 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=-0.9; leftover $1649.23 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `HAFN` | 208 | $7.91 | $2.68 | — | $8,402.82 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+5.4; leftover $1649.23 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `PD` | 132 | $12.45 | $2.39 | — | $6,757.04 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+3.5; leftover $1649.23 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `RBRK` | 16 | $101.99 | $2.04 | — | $5,123.16 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=-2.1; leftover $1649.23 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `S` | 75 | $21.80 | $2.21 | — | $3,485.94 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=-8.3; leftover $1649.23 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ULTA` | 3 | $536.07 | $2.00 | — | $1,875.73 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+2.1; leftover $1649.23 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `WDAY` | 8 | $195.40 | $2.01 | — | $310.52 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+0.7; leftover $1649.23 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $310.52 | ▲ close $13,500.97 vs 09:30 $13,224.68 (session +324.54) | 16:00 close · cash $310.52 · equity $13,500.97 vs 09:30 $13,224.68 (+276.29; session marks +324.54) · 8 name(s) marked open→close (per-name table). ADSK×6 09:30 $261.47 → close $270.58 +54.66; ESTC×19 09:30 $82.64 → close $83.74 +20.90; HAFN×208 09:30 $7.91 → close $8.29 +79.04; PD×132 09:30 $12.45 → close $12.63 +23.76; RBRK×16 09:30 $101.99 → close $107.02 +80.48; S×75 09:30 $21.80 → close $22.71 +68.25; ULTA×3 09:30 $536.07 → close $540.10 +12.09; WDAY×8 09:30 $195.40 → close $193.57 -14.64 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $310.52 | ▲ 09:30 equity $13,618.75 vs yday $13,500.97 (+117.78) | 09:30 open · cash $310.52 (unchanged overnight, no fees) · equity $13,618.75 vs prior close $13,500.97 (+117.78) · 8 name(s) re-marked at the open (per-name table). ADSK×6 yday $270.58 → 09:30 $258.50 -72.48; ESTC×19 yday $83.74 → 09:30 $99.99 +308.75; HAFN×208 yday $8.29 → 09:30 $8.43 +29.12; PD×132 yday $12.63 → 09:30 $13.92 +170.28; RBRK×16 yday $107.02 → 09:30 $92.46 -232.96; S×75 yday $22.71 → 09:30 $21.48 -92.25; ULTA×3 yday $540.10 → 09:30 $517.50 -67.80; WDAY×8 yday $193.57 → 09:30 $202.96 +75.12 | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $310.52 | ▼ close $13,584.32 vs 09:30 $13,618.75 (session -34.43) | 16:00 close · cash $310.52 · equity $13,584.32 vs 09:30 $13,618.75 (-34.43; session marks -34.43) · 8 name(s) marked open→close (per-name table). ADSK×6 09:30 $258.50 → close $259.14 +3.84; ESTC×19 09:30 $99.99 → close $99.00 -18.81; HAFN×208 09:30 $8.43 → close $8.45 +4.16; PD×132 09:30 $13.92 → close $13.70 -29.04; RBRK×16 09:30 $92.46 → close $92.46 +0.00; S×75 09:30 $21.48 → close $21.50 +1.50; ULTA×3 09:30 $517.50 → close $517.50 +0.00; WDAY×8 09:30 $202.96 → close $203.45 +3.92 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $310.52 | ▲ 09:30 equity $13,597.54 vs yday $13,584.32 (+13.22) | 09:30 open · cash $310.52 (unchanged overnight, no fees) · equity $13,597.54 vs prior close $13,584.32 (+13.22) · 8 name(s) re-marked at the open (per-name table). ADSK×6 yday $259.14 → 09:30 $258.17 -5.82; ESTC×19 yday $99.00 → 09:30 $96.54 -46.74; HAFN×208 yday $8.45 → 09:30 $8.43 -4.16; PD×132 yday $13.70 → 09:30 $13.89 +25.08; RBRK×16 yday $92.46 → 09:30 $90.89 -25.12; S×75 yday $21.50 → 09:30 $22.11 +45.75; ULTA×3 yday $517.50 → 09:30 $538.75 +63.75; WDAY×8 yday $203.45 → 09:30 $198.51 -39.52 | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $310.52 | ▼ close $13,554.19 vs 09:30 $13,597.54 (session -43.35) | 16:00 close · cash $310.52 · equity $13,554.19 vs 09:30 $13,597.54 (-43.35; session marks -43.35) · 8 name(s) marked open→close (per-name table). ADSK×6 09:30 $258.17 → close $259.89 +10.32; ESTC×19 09:30 $96.54 → close $96.07 -8.93; HAFN×208 09:30 $8.43 → close $8.41 -4.16; PD×132 09:30 $13.89 → close $13.89 +0.00; RBRK×16 09:30 $90.89 → close $91.50 +9.76; S×75 09:30 $22.11 → close $21.84 -20.25; ULTA×3 09:30 $538.75 → close $537.60 -3.45; WDAY×8 09:30 $198.51 → close $195.18 -26.64 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $310.52 | ▼ 09:30 equity $13,518.04 vs yday $13,554.19 (-36.15) | 09:30 open · cash $310.52 (unchanged overnight, no fees) · equity $13,518.04 vs prior close $13,554.19 (-36.15) · 8 name(s) re-marked at the open (per-name table). ADSK×6 yday $259.89 → 09:30 $253.48 -38.46; ESTC×19 yday $96.07 → 09:30 $95.76 -5.89; HAFN×208 yday $8.41 → 09:30 $8.56 +31.20; PD×132 yday $13.89 → 09:30 $13.91 +2.64; RBRK×16 yday $91.50 → 09:30 $91.70 +3.20; S×75 yday $21.84 → 09:30 $21.72 -9.00; ULTA×3 yday $537.60 → 09:30 $527.84 -29.28; WDAY×8 yday $195.18 → 09:30 $196.36 +9.44 | — |
| 2026-09-02 09:30 ET | **SELL** | `ADSK` | 6 | $253.48 | $2.03 | $-51.98 | $1,829.37 | ▼ -51.98 after sell → book $13,516.01; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ESTC` | 19 | $95.76 | $2.07 | $+245.16 | $3,646.74 | ▲ +245.16 after sell → book $13,513.94; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `HAFN` | 208 | $8.56 | $2.73 | $+129.78 | $5,424.49 | ▲ +129.78 after sell → book $13,511.21; vs 09:30 mark -2.73 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `PD` | 132 | $13.91 | $2.42 | $+187.91 | $7,258.18 | ▲ +187.91 after sell → book $13,508.78; vs 09:30 mark -2.43 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `RBRK` | 16 | $91.70 | $2.06 | $-168.74 | $8,723.32 | ▼ -168.74 after sell → book $13,506.72; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `S` | 75 | $21.72 | $2.24 | $-10.46 | $10,350.08 | ▼ -10.46 after sell → book $13,504.48; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ULTA` | 3 | $527.84 | $2.02 | $-28.71 | $11,931.58 | ▼ -28.71 after sell → book $13,502.46; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `WDAY` | 8 | $196.36 | $2.04 | $+3.63 | $13,500.43 | ▲ +3.63 after sell → book $13,500.43; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13,500.43 | ▲ close $13,500.43 vs 09:30 $13,518.04 (session +0.00) | 16:00 close · cash $13,500.43 · no lots left · equity $13,500.43. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,500.43 | ▲ 09:30 equity $13,500.43 vs yday $13,500.43 (-0.00) | 09:30 open · cash $13,500.43 · no holdings · equity $13,500.43 vs prior close $13,500.43 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `CHPT` | 318 | $5.30 | $4.10 | — | $11,810.92 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+1.1; leftover $1687.55 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FIVE` | 6 | $244.98 | $2.01 | — | $10,339.04 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+2.3; leftover $1687.55 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 32 | $51.99 | $2.09 | — | $8,673.27 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-9.0; leftover $1687.55 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MOMO` | 310 | $5.43 | $4.00 | — | $6,985.97 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+0.0; leftover $1687.55 | join🟢 sector🟡 gen🟡 news🟡 digest🟡 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `NTSK` | 121 | $13.94 | $2.35 | — | $5,296.88 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-8.2; leftover $1687.55 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `PHR` | 143 | $11.79 | $2.42 | — | $3,608.49 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-1.3; leftover $1687.55 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `PVH` | 23 | $73.10 | $2.06 | — | $1,925.13 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-4.8; leftover $1687.55 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SNOW` | 5 | $310.54 | $2.00 | — | $370.42 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+1.2; leftover $1687.55 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $370.42 | ▼ close $13,389.95 vs 09:30 $13,500.43 (session -89.44) | 16:00 close · cash $370.42 · equity $13,389.95 vs 09:30 $13,500.43 (-110.48; session marks -89.44) · 8 name(s) marked open→close (per-name table). CHPT×318 09:30 $5.30 → close $5.19 -34.98; FIVE×6 09:30 $244.98 → close $243.08 -11.40; HPE×32 09:30 $51.99 → close $51.83 -5.12; MOMO×310 09:30 $5.43 → close $5.49 +18.60; NTSK×121 09:30 $13.94 → close $13.75 -22.99; PHR×143 09:30 $11.79 → close $11.85 +8.58; PVH×23 09:30 $73.10 → close $72.29 -18.63; SNOW×5 09:30 $310.54 → close $305.84 -23.50 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $370.42 | ▲ 09:30 equity $14,397.61 vs yday $13,389.95 (+1,007.66) | 09:30 open · cash $370.42 (unchanged overnight, no fees) · equity $14,397.61 vs prior close $13,389.95 (+1007.66) · 8 name(s) re-marked at the open (per-name table). CHPT×318 yday $5.19 → 09:30 $6.90 +543.78; FIVE×6 yday $243.08 → 09:30 $256.99 +83.46; HPE×32 yday $51.83 → 09:30 $47.60 -135.36; MOMO×310 yday $5.49 → 09:30 $5.50 +3.10; NTSK×121 yday $13.75 → 09:30 $15.51 +212.96; PHR×143 yday $11.85 → 09:30 $11.02 -118.69; PVH×23 yday $72.29 → 09:30 $74.96 +61.41; SNOW×5 yday $305.84 → 09:30 $377.24 +357.00 | — |
| 2026-09-04 09:30 ET | **BUY** | `ASAN` | 4 | $10.16 | $0.42 | — | $329.37 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+4.8; leftover $46.30 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DOMO` | 12 | $3.78 | $0.49 | — | $283.52 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-2.8; leftover $46.30 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `IOT` | 1 | $37.69 | $0.38 | — | $245.45 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+0.4; leftover $46.30 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MAMA` | 2 | $15.62 | $0.32 | — | $213.89 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=-4.7; leftover $46.30 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $213.89 | ▲ close $14,792.05 vs 09:30 $14,397.61 (session +396.04) | 16:00 close · cash $213.89 · equity $14,792.05 vs 09:30 $14,397.61 (+394.44; session marks +396.04) · 12 name(s) marked open→close (per-name table). CHPT×318 09:30 $6.90 → close $9.08 +693.24; FIVE×6 09:30 $256.99 → close $239.96 -102.18; HPE×32 09:30 $47.60 → close $54.44 +218.88; MOMO×310 09:30 $5.50 → close $5.10 -124.00; NTSK×121 09:30 $15.51 → close $14.34 -141.57; PHR×143 09:30 $11.02 → close $11.10 +11.44; PVH×23 09:30 $74.96 → close $72.46 -57.50; SNOW×5 09:30 $377.24 → close $356.47 -103.85; ASAN×4 09:30 $10.16 → close $10.09 -0.28; DOMO×12 09:30 $3.78 → close $3.79 +0.12; IOT×1 09:30 $37.69 → close $38.75 +1.06; MAMA×2 09:30 $15.62 → close $15.96 +0.68 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `INO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `NMAX` | cash | leftover split 0.29 < 1 share @ 9.89 |
| 2026-08-14 | `AIRJ` | cash | leftover split 0.29 < 1 share @ 5.51 |
| 2026-08-14 | `BRUN` | cash | leftover split 0.29 < 1 share @ 26.25 |
| 2026-08-14 | `BZAI` | cash | leftover split 0.29 < 1 share @ 0.77 |
| 2026-08-14 | `DLO` | cash | leftover split 0.29 < 1 share @ 15.28 |
| 2026-08-14 | `ENHA` | cash | leftover split 0.29 < 1 share @ 2.31 |
| 2026-08-14 | `FIRY` | cash | leftover split 0.29 < 1 share @ 9.74 |
| 2026-08-14 | `GEMI` | cash | leftover split 0.29 < 1 share @ 3.90 |
| 2026-08-17 | `INO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `FN` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `VNET` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `DVLT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `JKHY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SQM` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `YMM` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `ATAT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ATHM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BABA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BULL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `COTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `DQ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `FUTU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IOND` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BJ` | cash | leftover split 68.92 < 1 share @ 93.98 |
| 2026-08-24 | `ATAT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ATHM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BABA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BULL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `COTY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `DQ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `FUTU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IOND` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BKE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `PSEC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-25 | `BKE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `PSEC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `BKE` | no_price | no 09:30 open — carry |
| 2026-08-26 | `PSEC` | no_price | no 09:30 open — carry |
| 2026-08-26 | `BNS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `BZ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `DKS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `GRRR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `SHMD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `TUYA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `VIPS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `TIGR` | no_price | no 09:30 open |
| 2026-08-26 | `ANF` | no_price | no 09:30 open |
| 2026-08-26 | `BOX` | no_price | no 09:30 open |
| 2026-08-26 | `HEI` | no_price | no 09:30 open |
| 2026-08-26 | `INTU` | no_price | no 09:30 open |
| 2026-08-26 | `KSS` | no_price | no 09:30 open |
| 2026-08-26 | `NCNO` | no_price | no 09:30 open |
| 2026-08-26 | `QMLS` | no_price | no 09:30 open |
| 2026-08-27 | `BNS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `DKS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `GRRR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `SHMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `TUYA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `VIPS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `ADSK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ESTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `HAFN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `PD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `RBRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `S` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ULTA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `WDAY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-01 | `ADSK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ESTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `HAFN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `PD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `RBRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `S` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ULTA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `WDAY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `NIO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `BF-B` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FCEL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MDB` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `OLLI` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PANW` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `CHPT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `FIVE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `HPE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MOMO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `NTSK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `PHR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `PVH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `SNOW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `AMBA` | cash | leftover split 46.30 < 1 share @ 66.61 |
| 2026-09-04 | `DOCU` | cash | leftover split 46.30 < 1 share @ 67.06 |
| 2026-09-04 | `GWRE` | cash | leftover split 46.30 < 1 share @ 198.00 |
| 2026-09-04 | `LULU` | cash | leftover split 46.30 < 1 share @ 121.15 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `CHPT` | 318 | 2026-09-03 @ $5.30 | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+1.1; leftover $1687.55 |
| `FIVE` | 6 | 2026-09-03 @ $244.98 | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+2.3; leftover $1687.55 |
| `HPE` | 32 | 2026-09-03 @ $51.99 | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-9.0; leftover $1687.55 |
| `MOMO` | 310 | 2026-09-03 @ $5.43 | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+0.0; leftover $1687.55 |
| `NTSK` | 121 | 2026-09-03 @ $13.94 | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-8.2; leftover $1687.55 |
| `PHR` | 143 | 2026-09-03 @ $11.79 | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-1.3; leftover $1687.55 |
| `PVH` | 23 | 2026-09-03 @ $73.10 | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-4.8; leftover $1687.55 |
| `SNOW` | 5 | 2026-09-03 @ $310.54 | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+1.2; leftover $1687.55 |
| `ASAN` | 4 | 2026-09-04 @ $10.16 | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+4.8; leftover $46.30 |
| `DOMO` | 12 | 2026-09-04 @ $3.78 | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-2.8; leftover $46.30 |
| `IOT` | 1 | 2026-09-04 @ $37.69 | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+0.4; leftover $46.30 |
| `MAMA` | 2 | 2026-09-04 @ $15.62 | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=-4.7; leftover $46.30 |
