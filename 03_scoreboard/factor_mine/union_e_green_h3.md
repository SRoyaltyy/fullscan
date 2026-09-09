# Factor mine action — `union_e_green_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **-9.69%** ($9,031) · signal-only (no cash/fees) was -31.83%. Starts YES **0/18**. Fills 57 · skips 90 · realized $-429.39.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: the name is in an earnings-reaction window (just reported, we are trading the reaction — not today's print).
- Must-have: the last finished bar was green (closed up).
- Must-not: the 🚨 alarm is on (cameras got worse overnight).
- Must-not: the news camera (does the morning packet like the headline?) is red.

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is allowed only when every must-have is true.
- A name is thrown out if any must-not is true.
- Keep the first 8 names in list order.
- Split leftover cash equally across *new* names (not ones we already hold).
- Skip a name if the slice cannot buy 1 share after fees.
- This is a LONG sleeve: it buys shares and wants the price to go up.

### When it sells

- Sell first, then buy. Never sell a ticker we do not hold.
- Minimum hold is 3 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 3 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `earn_react=True,last_green=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $52.69.

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
| 2026-08-24 | `ATAT` | 50 | $34.75 | $34.70 | -2.50 | $34.75 | +2.50 | +0.00 | +32.50 | +35.00 |
| 2026-08-24 | `ATHM` | 76 | $22.22 | $22.00 | -16.72 | $21.85 | -11.40 | -28.12 | -33.44 | -44.84 |
| 2026-08-24 | `BABA` | 13 | $119.34 | $116.90 | -31.72 | $118.47 | +20.41 | -11.31 | -85.41 | -65.00 |
| 2026-08-24 | `BULL` | 172 | $8.78 | $8.58 | -34.40 | $8.53 | -8.60 | -43.00 | -233.92 | -242.52 |
| 2026-08-24 | `COTY` | 672 | $2.74 | $2.69 | -33.60 | $2.79 | +67.20 | +33.60 | +94.08 | +161.28 |
| 2026-08-24 | `DQ` | 118 | $13.58 | $13.55 | -3.54 | $14.43 | +103.84 | +100.30 | -105.02 | -1.18 |
| 2026-08-24 | `FUTU` | 14 | $123.64 | $121.00 | -36.96 | $115.81 | -72.66 | -109.62 | +46.90 | -25.76 |
| 2026-08-24 | `IOND` | 26 | $68.73 | $68.73 | +0.00 | $69.55 | +21.32 | +21.32 | +81.38 | +102.70 |
| 2026-08-24 | `BKE` | 1 | $43.81 | $44.22 | +0.41 | $44.41 | +0.19 | +0.60 | +1.14 | +1.33 |
| 2026-08-24 | `PSEC` | 29 | $2.33 | $2.34 | +0.29 | $2.32 | -0.58 | -0.29 | +1.16 | +0.58 |
| 2026-08-25 | `ATAT` | 50 | $34.75 | $34.72 | -1.50 | — | +0.00 | -1.50 | +33.50 | — |
| 2026-08-25 | `ATHM` | 76 | $21.85 | $21.85 | +0.00 | — | +0.00 | +0.00 | -44.84 | — |
| 2026-08-25 | `BABA` | 13 | $118.47 | $117.94 | -6.89 | — | +0.00 | -6.89 | -71.89 | — |
| 2026-08-25 | `BULL` | 172 | $8.53 | $8.46 | -12.04 | — | +0.00 | -12.04 | -254.56 | — |
| 2026-08-25 | `COTY` | 672 | $2.79 | $2.75 | -26.88 | — | +0.00 | -26.88 | +134.40 | — |
| 2026-08-25 | `DQ` | 118 | $14.43 | $13.77 | -77.88 | — | +0.00 | -77.88 | -79.06 | — |
| 2026-08-25 | `FUTU` | 14 | $115.81 | $118.00 | +30.66 | — | +0.00 | +30.66 | +4.90 | — |
| 2026-08-25 | `IOND` | 26 | $69.55 | $69.00 | -14.30 | — | +0.00 | -14.30 | +88.40 | — |
| 2026-08-25 | `BKE` | 1 | $44.41 | $44.50 | +0.09 | $43.73 | -0.77 | -0.68 | +1.42 | +0.65 |
| 2026-08-25 | `PSEC` | 29 | $2.32 | $2.32 | +0.00 | $2.35 | +0.87 | +0.87 | +0.58 | +1.45 |
| 2026-08-25 | `SHMD` | 2934 | — | $4.54 | +0.00 | $3.42 | -3300.75 | -3300.75 | +0.00 | -3300.75 |
| 2026-08-26 | `BKE` | 1 | $43.73 | $44.39 | +0.66 | — | +0.00 | +0.66 | +1.31 | — |
| 2026-08-26 | `PSEC` | 29 | $2.35 | $2.35 | +0.00 | — | +0.00 | +0.00 | +1.45 | — |
| 2026-08-26 | `SHMD` | 2934 | $3.42 | $3.38 | -117.36 | $3.17 | -616.14 | -733.50 | -3418.11 | -4034.25 |
| 2026-08-26 | `TIGR` | 2 | — | $5.21 | +0.00 | $5.46 | +0.50 | +0.50 | +0.00 | +0.50 |
| 2026-08-26 | `LI` | 1 | — | $12.14 | +0.00 | $12.14 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-26 | `QFIN` | 1 | — | $9.76 | +0.00 | $9.35 | -0.41 | -0.41 | +0.00 | -0.41 |
| 2026-08-26 | `QMLS` | 2 | — | $6.47 | +0.00 | $6.10 | -0.74 | -0.74 | +0.00 | -0.74 |
| 2026-08-26 | `SFL` | 1 | — | $12.35 | +0.00 | $12.03 | -0.32 | -0.32 | +0.00 | -0.32 |
| 2026-08-27 | `SHMD` | 2934 | $3.17 | $3.16 | -29.34 | $3.40 | +704.16 | +674.82 | -4063.59 | -3359.43 |
| 2026-08-27 | `TIGR` | 2 | $5.46 | $5.49 | +0.06 | $5.06 | -0.86 | -0.80 | +0.56 | -0.30 |
| 2026-08-27 | `LI` | 1 | $12.14 | $12.35 | +0.21 | $12.18 | -0.17 | +0.04 | +0.21 | +0.04 |
| 2026-08-27 | `QFIN` | 1 | $9.35 | $9.42 | +0.07 | $9.17 | -0.25 | -0.18 | -0.34 | -0.59 |
| 2026-08-27 | `QMLS` | 2 | $6.10 | $6.33 | +0.46 | $6.47 | +0.28 | +0.74 | -0.28 | +0.00 |
| 2026-08-27 | `SFL` | 1 | $12.03 | $12.03 | +0.00 | $12.35 | +0.32 | +0.32 | -0.32 | +0.00 |
| 2026-08-28 | `SHMD` | 2934 | $3.40 | $3.38 | -58.68 | — | +0.00 | -58.68 | -3418.11 | — |
| 2026-08-28 | `TIGR` | 2 | $5.06 | $5.05 | -0.02 | $5.04 | -0.01 | -0.03 | -0.32 | -0.33 |
| 2026-08-28 | `LI` | 1 | $12.18 | $12.32 | +0.14 | $12.23 | -0.09 | +0.05 | +0.18 | +0.09 |
| 2026-08-28 | `QFIN` | 1 | $9.17 | $9.15 | -0.02 | $8.80 | -0.35 | -0.37 | -0.61 | -0.96 |
| 2026-08-28 | `QMLS` | 2 | $6.47 | $6.27 | -0.40 | $6.11 | -0.32 | -0.72 | -0.40 | -0.72 |
| 2026-08-28 | `SFL` | 1 | $12.35 | $12.37 | +0.02 | $12.37 | +0.00 | +0.02 | +0.02 | +0.02 |
| 2026-08-28 | `ADSK` | 4 | — | $261.16 | +0.00 | $260.66 | -2.00 | -2.00 | +0.00 | -2.00 |
| 2026-08-28 | `ESTC` | 11 | — | $103.89 | +0.00 | $99.91 | -43.78 | -43.78 | +0.00 | -43.78 |
| 2026-08-28 | `FRO` | 27 | — | $44.40 | +0.00 | $44.19 | -5.67 | -5.67 | +0.00 | -5.67 |
| 2026-08-28 | `GAP` | 50 | — | $24.69 | +0.00 | $23.48 | -60.50 | -60.50 | +0.00 | -60.50 |
| 2026-08-28 | `HAFN` | 148 | — | $8.35 | +0.00 | $8.47 | +17.76 | +17.76 | +0.00 | +17.76 |
| 2026-08-28 | `PD` | 94 | — | $13.09 | +0.00 | $13.83 | +69.56 | +69.56 | +0.00 | +69.56 |
| 2026-08-28 | `RBRK` | 12 | — | $98.95 | +0.00 | $93.05 | -70.80 | -70.80 | +0.00 | -70.80 |
| 2026-08-28 | `S` | 57 | — | $21.49 | +0.00 | $21.54 | +2.85 | +2.85 | +0.00 | +2.85 |
| 2026-08-31 | `TIGR` | 2 | $5.04 | $5.00 | -0.09 | — | +0.00 | -0.09 | -0.42 | — |
| 2026-08-31 | `LI` | 1 | $12.23 | $12.28 | +0.05 | — | +0.00 | +0.05 | +0.14 | — |
| 2026-08-31 | `QFIN` | 1 | $8.80 | $8.70 | -0.10 | — | +0.00 | -0.10 | -1.06 | — |
| 2026-08-31 | `QMLS` | 2 | $6.11 | $5.95 | -0.32 | — | +0.00 | -0.32 | -1.04 | — |
| 2026-08-31 | `SFL` | 1 | $12.37 | $12.51 | +0.14 | — | +0.00 | +0.14 | +0.16 | — |
| 2026-08-31 | `ADSK` | 4 | $260.66 | $257.71 | -11.80 | $258.53 | +3.28 | -8.52 | -13.80 | -10.52 |
| 2026-08-31 | `ESTC` | 11 | $99.91 | $98.00 | -21.01 | $97.55 | -4.95 | -25.96 | -64.79 | -69.74 |
| 2026-08-31 | `FRO` | 27 | $44.19 | $44.85 | +17.82 | $43.78 | -28.89 | -11.07 | +12.15 | -16.74 |
| 2026-08-31 | `GAP` | 50 | $23.48 | $22.98 | -25.00 | $22.31 | -33.50 | -58.50 | -85.50 | -119.00 |
| 2026-08-31 | `HAFN` | 148 | $8.47 | $8.53 | +8.88 | $8.44 | -13.32 | -4.44 | +26.64 | +13.32 |
| 2026-08-31 | `PD` | 94 | $13.83 | $13.58 | -23.50 | $14.14 | +52.64 | +29.14 | +46.06 | +98.70 |
| 2026-08-31 | `RBRK` | 12 | $93.05 | $92.83 | -2.64 | $93.04 | +2.52 | -0.12 | -73.44 | -70.92 |
| 2026-08-31 | `S` | 57 | $21.54 | $21.45 | -5.13 | $22.10 | +37.05 | +31.92 | -2.28 | +34.77 |
| 2026-09-01 | `ADSK` | 4 | $258.53 | $253.48 | -20.20 | $247.69 | -23.16 | -43.36 | -30.72 | -53.88 |
| 2026-09-01 | `ESTC` | 11 | $97.55 | $95.76 | -19.69 | $92.39 | -37.07 | -56.76 | -89.43 | -126.50 |
| 2026-09-01 | `FRO` | 27 | $43.78 | $44.39 | +16.47 | $44.32 | -1.89 | +14.58 | -0.27 | -2.16 |
| 2026-09-01 | `GAP` | 50 | $22.31 | $22.05 | -13.00 | $22.00 | -2.50 | -15.50 | -132.00 | -134.50 |
| 2026-09-01 | `HAFN` | 148 | $8.44 | $8.56 | +17.76 | $8.59 | +4.44 | +22.20 | +31.08 | +35.52 |
| 2026-09-01 | `PD` | 94 | $14.14 | $13.91 | -21.62 | $14.04 | +12.22 | -9.40 | +77.08 | +89.30 |
| 2026-09-01 | `RBRK` | 12 | $93.04 | $91.70 | -16.08 | $88.40 | -39.60 | -55.68 | -87.00 | -126.60 |
| 2026-09-01 | `S` | 57 | $22.10 | $21.72 | -21.66 | $20.63 | -62.13 | -83.79 | +13.11 | -49.02 |
| 2026-09-02 | `ADSK` | 4 | $247.69 | $246.70 | -3.96 | — | +0.00 | -3.96 | -57.84 | — |
| 2026-09-02 | `ESTC` | 11 | $92.39 | $92.00 | -4.29 | — | +0.00 | -4.29 | -130.79 | — |
| 2026-09-02 | `FRO` | 27 | $44.32 | $44.17 | -4.05 | — | +0.00 | -4.05 | -6.21 | — |
| 2026-09-02 | `GAP` | 50 | $22.00 | $21.97 | -1.50 | — | +0.00 | -1.50 | -136.00 | — |
| 2026-09-02 | `HAFN` | 148 | $8.59 | $8.58 | -1.48 | — | +0.00 | -1.48 | +34.04 | — |
| 2026-09-02 | `PD` | 94 | $14.04 | $14.00 | -3.76 | — | +0.00 | -3.76 | +85.54 | — |
| 2026-09-02 | `RBRK` | 12 | $88.40 | $89.00 | +7.20 | — | +0.00 | +7.20 | -119.40 | — |
| 2026-09-02 | `S` | 57 | $20.63 | $20.56 | -3.99 | — | +0.00 | -3.99 | -53.01 | — |
| 2026-09-03 | `AI` | 148 | — | $10.74 | +0.00 | $10.90 | +22.94 | +22.94 | +0.00 | +22.94 |
| 2026-09-03 | `MOMO` | 290 | — | $5.50 | +0.00 | $5.10 | -116.00 | -116.00 | +0.00 | -116.00 |
| 2026-09-03 | `PHR` | 144 | — | $11.02 | +0.00 | $11.10 | +11.52 | +11.52 | +0.00 | +11.52 |
| 2026-09-03 | `TTC` | 16 | — | $99.00 | +0.00 | $92.37 | -106.08 | -106.08 | +0.00 | -106.08 |
| 2026-09-03 | `VSXY` | 20 | — | $76.86 | +0.00 | $73.64 | -64.40 | -64.40 | +0.00 | -64.40 |
| 2026-09-03 | `WOOF` | 511 | — | $3.12 | +0.00 | $2.52 | -306.60 | -306.60 | +0.00 | -306.60 |
| 2026-09-04 | `AI` | 148 | $10.90 | $10.91 | +1.48 | $10.46 | -66.60 | -65.12 | +24.42 | -42.18 |
| 2026-09-04 | `MOMO` | 290 | $5.10 | $5.13 | +8.70 | $5.37 | +69.60 | +78.30 | -107.30 | -37.70 |
| 2026-09-04 | `PHR` | 144 | $11.10 | $11.08 | -2.88 | $10.94 | -20.16 | -23.04 | +8.64 | -11.52 |
| 2026-09-04 | `TTC` | 16 | $92.37 | $92.14 | -3.68 | $93.33 | +19.04 | +15.36 | -109.76 | -90.72 |
| 2026-09-04 | `VSXY` | 20 | $73.64 | $73.63 | -0.20 | $75.56 | +38.60 | +38.40 | -64.60 | -26.00 |
| 2026-09-04 | `WOOF` | 511 | $2.52 | $2.51 | -5.11 | $2.70 | +97.09 | +91.98 | -311.71 | -214.62 |
| 2026-09-04 | `DOMO` | 3 | — | $3.62 | +0.00 | $3.88 | +0.79 | +0.79 | +0.00 | +0.79 |
| 2026-09-08 | `AI` | 148 | $10.46 | $10.20 | -38.48 | $10.51 | +45.88 | +7.40 | -80.66 | -34.78 |
| 2026-09-08 | `MOMO` | 290 | $5.37 | $5.28 | -26.10 | $5.26 | -5.80 | -31.90 | -63.80 | -69.60 |
| 2026-09-08 | `PHR` | 144 | $10.94 | $10.49 | -64.80 | $10.22 | -38.88 | -103.68 | -76.32 | -115.20 |
| 2026-09-08 | `TTC` | 16 | $93.33 | $93.35 | +0.32 | $94.75 | +22.40 | +22.72 | -90.40 | -68.00 |
| 2026-09-08 | `VSXY` | 20 | $75.56 | $73.51 | -41.00 | $78.47 | +99.20 | +58.20 | -67.00 | +32.20 |
| 2026-09-08 | `WOOF` | 511 | $2.70 | $2.67 | -15.33 | $2.60 | -35.77 | -51.10 | -229.95 | -265.72 |
| 2026-09-08 | `DOMO` | 3 | $3.88 | $3.84 | -0.12 | $3.83 | -0.03 | -0.15 | +0.67 | +0.64 |

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
| 2026-08-24 | -5.17 | $95.80 | ATAT×50, ATHM×76, BABA×13, BULL×172, COTY×672, DQ×118, FUTU×14, IOND×26, BKE×1, PSEC×29 | $13,497.90 | -158.74 | +122.22 | — | — | $95.80 | $13,620.12 | ATAT×50, ATHM×76, BABA×13, BULL×172, COTY×672, DQ×118, FUTU×14, IOND×26, BKE×1, PSEC×29 |
| 2026-08-25 | +1.80 | $95.80 | ATAT×50, ATHM×76, BABA×13, BULL×172, COTY×672, DQ×118, FUTU×14, IOND×26, BKE×1, PSEC×29 | $13,511.38 | -108.74 | -3,300.65 | SHMD | ATAT, ATHM, BABA, BULL, COTY, DQ, FUTU, IOND | $2.40 | $10,148.56 | BKE×1, PSEC×29, SHMD×2934 |
| 2026-08-26 | +2.02 | $2.40 | BKE×1, PSEC×29, SHMD×2934 | $10,031.86 | -116.70 | -617.11 | TIGR, LI, QFIN, QMLS, SFL | BKE, PSEC | $55.47 | $9,412.89 | SHMD×2934, TIGR×2, LI×1, QFIN×1, QMLS×2, SFL×1 |
| 2026-08-27 | — | $55.47 | SHMD×2934, TIGR×2, LI×1, QFIN×1, QMLS×2, SFL×1 | $9,384.35 | -28.54 | +703.48 | — | — | $55.47 | $10,087.83 | SHMD×2934, TIGR×2, LI×1, QFIN×1, QMLS×2, SFL×1 |
| 2026-08-28 | +0.75 | $55.47 | SHMD×2934, TIGR×2, LI×1, QFIN×1, QMLS×2, SFL×1 | $10,028.87 | -58.96 | -93.35 | ADSK, ESTC, FRO, GAP, HAFN, PD, RBRK, S | SHMD | $417.53 | $9,879.98 | TIGR×2, LI×1, QFIN×1, QMLS×2, SFL×1, ADSK×4, ESTC×11, FRO×27, GAP×50, HAFN×148, PD×94, RBRK×12, S×57 |
| 2026-08-31 | -5.85 | $417.53 | TIGR×2, LI×1, QFIN×1, QMLS×2, SFL×1, ADSK×4, ESTC×11, FRO×27, GAP×50, HAFN×148, PD×94, RBRK×12, S×57 | $9,817.28 | -62.70 | +14.83 | — | TIGR, LI, QFIN, QMLS, SFL | $472.25 | $9,831.44 | ADSK×4, ESTC×11, FRO×27, GAP×50, HAFN×148, PD×94, RBRK×12, S×57 |
| 2026-09-01 | -6.30 | $472.25 | ADSK×4, ESTC×11, FRO×27, GAP×50, HAFN×148, PD×94, RBRK×12, S×57 | $9,753.42 | -78.02 | -149.69 | — | — | $472.25 | $9,603.73 | ADSK×4, ESTC×11, FRO×27, GAP×50, HAFN×148, PD×94, RBRK×12, S×57 |
| 2026-09-02 | -3.83 | $472.25 | ADSK×4, ESTC×11, FRO×27, GAP×50, HAFN×148, PD×94, RBRK×12, S×57 | $9,587.90 | -15.83 | +0.00 | — | ADSK, ESTC, FRO, GAP, HAFN, PD, RBRK, S | $9,570.59 | $9,570.59 | — |
| 2026-09-03 | -0.90 | $9,570.59 | — | $9,570.59 | -0.00 | -558.62 | AI, MOMO, PHR, TTC, VSXY, WOOF | — | $63.65 | $8,992.69 | AI×148, MOMO×290, PHR×144, TTC×16, VSXY×20, WOOF×511 |
| 2026-09-04 | +2.25 | $63.65 | AI×148, MOMO×290, PHR×144, TTC×16, VSXY×20, WOOF×511 | $8,991.00 | -1.69 | +138.36 | DOMO | — | $52.69 | $9,129.25 | AI×148, MOMO×290, PHR×144, TTC×16, VSXY×20, WOOF×511, DOMO×3 |
| 2026-09-08 | -11.47 | $52.69 | AI×148, MOMO×290, PHR×144, TTC×16, VSXY×20, WOOF×511, DOMO×3 | $8,943.74 | -185.51 | +87.00 | — | — | $52.69 | $9,030.74 | AI×148, MOMO×290, PHR×144, TTC×16, VSXY×20, WOOF×511, DOMO×3 |

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
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $95.80 | ▼ 09:30 equity $13,497.90 vs yday $13,656.64 (-158.74) | 09:30 open · cash $95.80 (unchanged overnight, no fees) · equity $13,497.90 vs prior close $13,656.64 (-158.74) · 10 name(s) re-marked at the open (per-name table). ATAT×50 yday $34.75 → 09:30 $34.70 -2.50; ATHM×76 yday $22.22 → 09:30 $22.00 -16.72; BABA×13 yday $119.34 → 09:30 $116.90 -31.72; BULL×172 yday $8.78 → 09:30 $8.58 -34.40; COTY×672 yday $2.74 → 09:30 $2.69 -33.60; DQ×118 yday $13.58 → 09:30 $13.55 -3.54; FUTU×14 yday $123.64 → 09:30 $121.00 -36.96; IOND×26 yday $68.73 → 09:30 $68.73 +0.00; BKE×1 yday $43.81 → 09:30 $44.22 +0.41; PSEC×29 yday $2.33 → 09:30 $2.34 +0.29 | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $95.80 | ▲ close $13,620.12 vs 09:30 $13,497.90 (session +122.22) | 16:00 close · cash $95.80 · equity $13,620.12 vs 09:30 $13,497.90 (+122.22; session marks +122.22) · 10 name(s) marked open→close (per-name table). ATAT×50 09:30 $34.70 → close $34.75 +2.50; ATHM×76 09:30 $22.00 → close $21.85 -11.40; BABA×13 09:30 $116.90 → close $118.47 +20.41; BULL×172 09:30 $8.58 → close $8.53 -8.60; COTY×672 09:30 $2.69 → close $2.79 +67.20; DQ×118 09:30 $13.55 → close $14.43 +103.84; FUTU×14 09:30 $121.00 → close $115.81 -72.66; IOND×26 09:30 $68.73 → close $69.55 +21.32; BKE×1 09:30 $44.22 → close $44.41 +0.19; PSEC×29 09:30 $2.34 → close $2.32 -0.58 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $95.80 | ▼ 09:30 equity $13,511.38 vs yday $13,620.12 (-108.74) | 09:30 open · cash $95.80 (unchanged overnight, no fees) · equity $13,511.38 vs prior close $13,620.12 (-108.74) · 10 name(s) re-marked at the open (per-name table). ATAT×50 yday $34.75 → 09:30 $34.72 -1.50; ATHM×76 yday $21.85 → 09:30 $21.85 +0.00; BABA×13 yday $118.47 → 09:30 $117.94 -6.89; BULL×172 yday $8.53 → 09:30 $8.46 -12.04; COTY×672 yday $2.79 → 09:30 $2.75 -26.88; DQ×118 yday $14.43 → 09:30 $13.77 -77.88; FUTU×14 yday $115.81 → 09:30 $118.00 +30.66; IOND×26 yday $69.55 → 09:30 $69.00 -14.30; BKE×1 yday $44.41 → 09:30 $44.50 +0.09; PSEC×29 yday $2.32 → 09:30 $2.32 +0.00 | — |
| 2026-08-25 09:30 ET | **SELL** | `ATAT` | 50 | $34.72 | $2.16 | $+29.20 | $1,829.64 | ▲ +29.20 after sell → book $13,509.22; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ATHM` | 76 | $21.85 | $2.24 | $-49.30 | $3,487.99 | ▼ -49.30 after sell → book $13,506.97; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BABA` | 13 | $117.94 | $2.05 | $-75.97 | $5,019.16 | ▼ -75.97 after sell → book $13,504.92; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BULL` | 172 | $8.46 | $2.55 | $-259.61 | $6,471.73 | ▼ -259.61 after sell → book $13,502.37; vs 09:30 mark -2.55 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `COTY` | 672 | $2.75 | $8.80 | $+116.94 | $8,310.94 | ▲ +116.94 after sell → book $13,493.58; vs 09:30 mark -8.79 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `DQ` | 118 | $13.77 | $2.38 | $-83.78 | $9,933.42 | ▼ -83.78 after sell → book $13,491.20; vs 09:30 mark -2.38 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `FUTU` | 14 | $118.00 | $2.06 | $+0.81 | $11,583.37 | ▲ +0.81 after sell → book $13,489.15; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IOND` | 26 | $69.00 | $2.09 | $+84.24 | $13,375.28 | ▲ +84.24 after sell → book $13,487.06; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `SHMD` | 2934 | $4.54 | $37.85 | — | $2.40 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-14.6; leftover $13375.28 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.40 | ▼ close $10,148.56 vs 09:30 $13,511.38 (session -3,300.65) | 16:00 close · cash $2.40 · equity $10,148.56 vs 09:30 $13,511.38 (-3362.82; session marks -3300.65) · 3 name(s) marked open→close (per-name table). BKE×1 09:30 $44.50 → close $43.73 -0.77; PSEC×29 09:30 $2.32 → close $2.35 +0.87; SHMD×2934 09:30 $4.54 → close $3.42 -3300.75 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.40 | ▼ 09:30 equity $10,031.86 vs yday $10,148.56 (-116.70) | 09:30 open · cash $2.40 (unchanged overnight, no fees) · equity $10,031.86 vs prior close $10,148.56 (-116.70) · 3 name(s) re-marked at the open (per-name table). BKE×1 yday $43.73 → 09:30 $44.39 +0.66; PSEC×29 yday $2.35 → 09:30 $2.35 +0.00; SHMD×2934 yday $3.42 → 09:30 $3.38 -117.36 | — |
| 2026-08-26 09:30 ET | **SELL** | `BKE` | 1 | $44.39 | $0.47 | $+0.41 | $46.32 | ▲ +0.41 after sell → book $10,031.39; vs 09:30 mark -0.47 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `PSEC` | 29 | $2.35 | $0.79 | $-0.09 | $113.68 | ▼ -0.09 after sell → book $10,030.60; vs 09:30 mark -0.79 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **BUY** | `TIGR` | 2 | $5.21 | $0.11 | — | $103.15 | — | combo gate; gate earn_react=True,last_green=True; list ohlc_hot,earn_react; 🔵; ret5=+14.3; leftover $14.21 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `LI` | 1 | $12.14 | $0.12 | — | $90.89 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+1.2; leftover $14.21 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `QFIN` | 1 | $9.76 | $0.10 | — | $81.03 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-4.4; leftover $14.21 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `QMLS` | 2 | $6.47 | $0.14 | — | $67.95 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-7.0; leftover $14.21 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `SFL` | 1 | $12.35 | $0.13 | — | $55.47 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=-1.7; leftover $14.21 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $55.47 | ▼ close $9,412.89 vs 09:30 $10,031.86 (session -617.11) | 16:00 close · cash $55.47 · equity $9,412.89 vs 09:30 $10,031.86 (-618.97; session marks -617.11) · 6 name(s) marked open→close (per-name table). SHMD×2934 09:30 $3.38 → close $3.17 -616.14; TIGR×2 09:30 $5.21 → close $5.46 +0.50; LI×1 09:30 $12.14 → close $12.14 +0.00; QFIN×1 09:30 $9.76 → close $9.35 -0.41; QMLS×2 09:30 $6.47 → close $6.10 -0.74; SFL×1 09:30 $12.35 → close $12.03 -0.32 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $55.47 | ▼ 09:30 equity $9,384.35 vs yday $9,412.89 (-28.54) | 09:30 open · cash $55.47 (unchanged overnight, no fees) · equity $9,384.35 vs prior close $9,412.89 (-28.54) · 6 name(s) re-marked at the open (per-name table). SHMD×2934 yday $3.17 → 09:30 $3.16 -29.34; TIGR×2 yday $5.46 → 09:30 $5.49 +0.06; LI×1 yday $12.14 → 09:30 $12.35 +0.21; QFIN×1 yday $9.35 → 09:30 $9.42 +0.07; QMLS×2 yday $6.10 → 09:30 $6.33 +0.46; SFL×1 yday $12.03 → 09:30 $12.03 +0.00 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $55.47 | ▲ close $10,087.83 vs 09:30 $9,384.35 (session +703.48) | 16:00 close · cash $55.47 · equity $10,087.83 vs 09:30 $9,384.35 (+703.48; session marks +703.48) · 6 name(s) marked open→close (per-name table). SHMD×2934 09:30 $3.16 → close $3.40 +704.16; TIGR×2 09:30 $5.49 → close $5.06 -0.86; LI×1 09:30 $12.35 → close $12.18 -0.17; QFIN×1 09:30 $9.42 → close $9.17 -0.25; QMLS×2 09:30 $6.33 → close $6.47 +0.28; SFL×1 09:30 $12.03 → close $12.35 +0.32 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $55.47 | ▼ 09:30 equity $10,028.87 vs yday $10,087.83 (-58.96) | 09:30 open · cash $55.47 (unchanged overnight, no fees) · equity $10,028.87 vs prior close $10,087.83 (-58.96) · 6 name(s) re-marked at the open (per-name table). SHMD×2934 yday $3.40 → 09:30 $3.38 -58.68; TIGR×2 yday $5.06 → 09:30 $5.05 -0.02; LI×1 yday $12.18 → 09:30 $12.32 +0.14; QFIN×1 yday $9.17 → 09:30 $9.15 -0.02; QMLS×2 yday $6.47 → 09:30 $6.27 -0.40; SFL×1 yday $12.35 → 09:30 $12.37 +0.02 | — |
| 2026-08-28 09:30 ET | **SELL** | `SHMD` | 2934 | $3.38 | $38.41 | $-3494.37 | $9,933.98 | ▼ -3,494.37 after sell → book $9,990.46; vs 09:30 mark -38.41 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 4 | $261.16 | $2.00 | — | $8,887.34 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+7.8; leftover $1241.75 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ESTC` | 11 | $103.89 | $2.02 | — | $7,742.52 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=-2.5; leftover $1241.75 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `FRO` | 27 | $44.40 | $2.07 | — | $6,541.65 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+0.4; leftover $1241.75 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GAP` | 50 | $24.69 | $2.14 | — | $5,305.01 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+5.8; leftover $1241.75 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `HAFN` | 148 | $8.35 | $2.43 | — | $4,066.78 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+5.1; leftover $1241.75 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `PD` | 94 | $13.09 | $2.27 | — | $2,834.05 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+4.2; leftover $1241.75 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `RBRK` | 12 | $98.95 | $2.03 | — | $1,644.62 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+9.7; leftover $1241.75 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `S` | 57 | $21.49 | $2.16 | — | $417.53 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+8.5; leftover $1241.75 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $417.53 | ▼ close $9,879.98 vs 09:30 $10,028.87 (session -93.35) | 16:00 close · cash $417.53 · equity $9,879.98 vs 09:30 $10,028.87 (-148.89; session marks -93.35) · 13 name(s) marked open→close (per-name table). TIGR×2 09:30 $5.05 → close $5.04 -0.01; LI×1 09:30 $12.32 → close $12.23 -0.09; QFIN×1 09:30 $9.15 → close $8.80 -0.35; QMLS×2 09:30 $6.27 → close $6.11 -0.32; SFL×1 09:30 $12.37 → close $12.37 +0.00; ADSK×4 09:30 $261.16 → close $260.66 -2.00; ESTC×11 09:30 $103.89 → close $99.91 -43.78; FRO×27 09:30 $44.40 → close $44.19 -5.67; GAP×50 09:30 $24.69 → close $23.48 -60.50; HAFN×148 09:30 $8.35 → close $8.47 +17.76; PD×94 09:30 $13.09 → close $13.83 +69.56; RBRK×12 09:30 $98.95 → close $93.05 -70.80; S×57 09:30 $21.49 → close $21.54 +2.85 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $417.53 | ▼ 09:30 equity $9,817.28 vs yday $9,879.98 (-62.70) | 09:30 open · cash $417.53 (unchanged overnight, no fees) · equity $9,817.28 vs prior close $9,879.98 (-62.70) · 13 name(s) re-marked at the open (per-name table). TIGR×2 yday $5.04 → 09:30 $5.00 -0.09; LI×1 yday $12.23 → 09:30 $12.28 +0.05; QFIN×1 yday $8.80 → 09:30 $8.70 -0.10; QMLS×2 yday $6.11 → 09:30 $5.95 -0.32; SFL×1 yday $12.37 → 09:30 $12.51 +0.14; ADSK×4 yday $260.66 → 09:30 $257.71 -11.80; ESTC×11 yday $99.91 → 09:30 $98.00 -21.01; FRO×27 yday $44.19 → 09:30 $44.85 +17.82; GAP×50 yday $23.48 → 09:30 $22.98 -25.00; HAFN×148 yday $8.47 → 09:30 $8.53 +8.88; PD×94 yday $13.83 → 09:30 $13.58 -23.50; RBRK×12 yday $93.05 → 09:30 $92.83 -2.64; S×57 yday $21.54 → 09:30 $21.45 -5.13 | — |
| 2026-08-31 09:30 ET | **SELL** | `TIGR` | 2 | $5.00 | $0.13 | $-0.66 | $427.40 | ▼ -0.66 after sell → book $9,817.15; vs 09:30 mark -0.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `LI` | 1 | $12.28 | $0.15 | $-0.13 | $439.54 | ▼ -0.13 after sell → book $9,817.01; vs 09:30 mark -0.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `QFIN` | 1 | $8.70 | $0.11 | $-1.27 | $448.13 | ▼ -1.27 after sell → book $9,816.90; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `QMLS` | 2 | $5.95 | $0.14 | $-1.32 | $459.88 | ▼ -1.32 after sell → book $9,816.75; vs 09:30 mark -0.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `SFL` | 1 | $12.51 | $0.15 | $-0.11 | $472.25 | ▼ -0.11 after sell → book $9,816.61; vs 09:30 mark -0.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $472.25 | ▲ close $9,831.44 vs 09:30 $9,817.28 (session +14.83) | 16:00 close · cash $472.25 · equity $9,831.44 vs 09:30 $9,817.28 (+14.16; session marks +14.83) · 8 name(s) marked open→close (per-name table). ADSK×4 09:30 $257.71 → close $258.53 +3.28; ESTC×11 09:30 $98.00 → close $97.55 -4.95; FRO×27 09:30 $44.85 → close $43.78 -28.89; GAP×50 09:30 $22.98 → close $22.31 -33.50; HAFN×148 09:30 $8.53 → close $8.44 -13.32; PD×94 09:30 $13.58 → close $14.14 +52.64; RBRK×12 09:30 $92.83 → close $93.04 +2.52; S×57 09:30 $21.45 → close $22.10 +37.05 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $472.25 | ▼ 09:30 equity $9,753.42 vs yday $9,831.44 (-78.02) | 09:30 open · cash $472.25 (unchanged overnight, no fees) · equity $9,753.42 vs prior close $9,831.44 (-78.02) · 8 name(s) re-marked at the open (per-name table). ADSK×4 yday $258.53 → 09:30 $253.48 -20.20; ESTC×11 yday $97.55 → 09:30 $95.76 -19.69; FRO×27 yday $43.78 → 09:30 $44.39 +16.47; GAP×50 yday $22.31 → 09:30 $22.05 -13.00; HAFN×148 yday $8.44 → 09:30 $8.56 +17.76; PD×94 yday $14.14 → 09:30 $13.91 -21.62; RBRK×12 yday $93.04 → 09:30 $91.70 -16.08; S×57 yday $22.10 → 09:30 $21.72 -21.66 | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $472.25 | ▼ close $9,603.73 vs 09:30 $9,753.42 (session -149.69) | 16:00 close · cash $472.25 · equity $9,603.73 vs 09:30 $9,753.42 (-149.69; session marks -149.69) · 8 name(s) marked open→close (per-name table). ADSK×4 09:30 $253.48 → close $247.69 -23.16; ESTC×11 09:30 $95.76 → close $92.39 -37.07; FRO×27 09:30 $44.39 → close $44.32 -1.89; GAP×50 09:30 $22.05 → close $22.00 -2.50; HAFN×148 09:30 $8.56 → close $8.59 +4.44; PD×94 09:30 $13.91 → close $14.04 +12.22; RBRK×12 09:30 $91.70 → close $88.40 -39.60; S×57 09:30 $21.72 → close $20.63 -62.13 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $472.25 | ▼ 09:30 equity $9,587.90 vs yday $9,603.73 (-15.83) | 09:30 open · cash $472.25 (unchanged overnight, no fees) · equity $9,587.90 vs prior close $9,603.73 (-15.83) · 8 name(s) re-marked at the open (per-name table). ADSK×4 yday $247.69 → 09:30 $246.70 -3.96; ESTC×11 yday $92.39 → 09:30 $92.00 -4.29; FRO×27 yday $44.32 → 09:30 $44.17 -4.05; GAP×50 yday $22.00 → 09:30 $21.97 -1.50; HAFN×148 yday $8.59 → 09:30 $8.58 -1.48; PD×94 yday $14.04 → 09:30 $14.00 -3.76; RBRK×12 yday $88.40 → 09:30 $89.00 +7.20; S×57 yday $20.63 → 09:30 $20.56 -3.99 | — |
| 2026-09-02 09:30 ET | **SELL** | `ADSK` | 4 | $246.70 | $2.02 | $-61.86 | $1,457.02 | ▼ -61.86 after sell → book $9,585.87; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ESTC` | 11 | $92.00 | $2.04 | $-134.86 | $2,466.98 | ▼ -134.86 after sell → book $9,583.83; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `FRO` | 27 | $44.17 | $2.09 | $-10.37 | $3,657.48 | ▼ -10.37 after sell → book $9,581.74; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `GAP` | 50 | $21.97 | $2.16 | $-140.30 | $4,753.82 | ▼ -140.30 after sell → book $9,579.58; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `HAFN` | 148 | $8.58 | $2.47 | $+29.14 | $6,021.19 | ▲ +29.14 after sell → book $9,577.11; vs 09:30 mark -2.47 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `PD` | 94 | $14.00 | $2.30 | $+80.97 | $7,334.89 | ▲ +80.97 after sell → book $9,574.81; vs 09:30 mark -2.30 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `RBRK` | 12 | $89.00 | $2.05 | $-123.47 | $8,400.85 | ▼ -123.47 after sell → book $9,572.77; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `S` | 57 | $20.56 | $2.18 | $-57.35 | $9,570.59 | ▼ -57.35 after sell → book $9,570.59; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,570.59 | ▲ close $9,570.59 vs 09:30 $9,587.90 (session +0.00) | 16:00 close · cash $9,570.59 · no lots left · equity $9,570.59. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,570.59 | ▲ 09:30 equity $9,570.59 vs yday $9,570.59 (-0.00) | 09:30 open · cash $9,570.59 · no holdings · equity $9,570.59 vs prior close $9,570.59 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `AI` | 148 | $10.74 | $2.43 | — | $7,977.89 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+8.5; leftover $1595.10 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MOMO` | 290 | $5.50 | $3.74 | — | $6,379.15 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-4.8; leftover $1595.10 | join🟢 sector🟡 gen🟡 news🟡 digest🟡 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `PHR` | 144 | $11.02 | $2.42 | — | $4,789.85 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+2.2; leftover $1595.10 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `TTC` | 16 | $99.00 | $2.04 | — | $3,203.81 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ⚪; ret5=-1.2; leftover $1595.10 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSXY` | 20 | $76.86 | $2.05 | — | $1,664.56 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-6.6; leftover $1595.10 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `WOOF` | 511 | $3.12 | $6.59 | — | $63.65 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=-5.1; leftover $1595.10 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $63.65 | ▼ close $8,992.69 vs 09:30 $9,570.59 (session -558.62) | 16:00 close · cash $63.65 · equity $8,992.69 vs 09:30 $9,570.59 (-577.90; session marks -558.62) · 6 name(s) marked open→close (per-name table). AI×148 09:30 $10.74 → close $10.90 +22.94; MOMO×290 09:30 $5.50 → close $5.10 -116.00; PHR×144 09:30 $11.02 → close $11.10 +11.52; TTC×16 09:30 $99.00 → close $92.37 -106.08; VSXY×20 09:30 $76.86 → close $73.64 -64.40; WOOF×511 09:30 $3.12 → close $2.52 -306.60 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $63.65 | ▼ 09:30 equity $8,991.00 vs yday $8,992.69 (-1.69) | 09:30 open · cash $63.65 (unchanged overnight, no fees) · equity $8,991.00 vs prior close $8,992.69 (-1.69) · 6 name(s) re-marked at the open (per-name table). AI×148 yday $10.90 → 09:30 $10.91 +1.48; MOMO×290 yday $5.10 → 09:30 $5.13 +8.70; PHR×144 yday $11.10 → 09:30 $11.08 -2.88; TTC×16 yday $92.37 → 09:30 $92.14 -3.68; VSXY×20 yday $73.64 → 09:30 $73.63 -0.20; WOOF×511 yday $2.52 → 09:30 $2.51 -5.11 | — |
| 2026-09-04 09:30 ET | **BUY** | `DOMO` | 3 | $3.62 | $0.12 | — | $52.69 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-3.1; leftover $12.73 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $52.69 | ▲ close $9,129.25 vs 09:30 $8,991.00 (session +138.36) | 16:00 close · cash $52.69 · equity $9,129.25 vs 09:30 $8,991.00 (+138.25; session marks +138.36) · 7 name(s) marked open→close (per-name table). AI×148 09:30 $10.91 → close $10.46 -66.60; MOMO×290 09:30 $5.13 → close $5.37 +69.60; PHR×144 09:30 $11.08 → close $10.94 -20.16; TTC×16 09:30 $92.14 → close $93.33 +19.04; VSXY×20 09:30 $73.63 → close $75.56 +38.60; WOOF×511 09:30 $2.51 → close $2.70 +97.09; DOMO×3 09:30 $3.62 → close $3.88 +0.79 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $52.69 | ▼ 09:30 equity $8,943.74 vs yday $9,129.25 (-185.51) | 09:30 open · cash $52.69 (unchanged overnight, no fees) · equity $8,943.74 vs prior close $9,129.25 (-185.51) · 7 name(s) re-marked at the open (per-name table). AI×148 yday $10.46 → 09:30 $10.20 -38.48; MOMO×290 yday $5.37 → 09:30 $5.28 -26.10; PHR×144 yday $10.94 → 09:30 $10.49 -64.80; TTC×16 yday $93.33 → 09:30 $93.35 +0.32; VSXY×20 yday $75.56 → 09:30 $73.51 -41.00; WOOF×511 yday $2.70 → 09:30 $2.67 -15.33; DOMO×3 yday $3.88 → 09:30 $3.84 -0.12 | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $52.69 | ▲ close $9,030.74 vs 09:30 $8,943.74 (session +87.00) | 16:00 close · cash $52.69 · equity $9,030.74 vs 09:30 $8,943.74 (+87.00; session marks +87.00) · 7 name(s) marked open→close (per-name table). AI×148 09:30 $10.20 → close $10.51 +45.88; MOMO×290 09:30 $5.28 → close $5.26 -5.80; PHR×144 09:30 $10.49 → close $10.22 -38.88; TTC×16 09:30 $93.35 → close $94.75 +22.40; VSXY×20 09:30 $73.51 → close $78.47 +99.20; WOOF×511 09:30 $2.67 → close $2.60 -35.77; DOMO×3 09:30 $3.84 → close $3.83 -0.03 | — |

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
| 2026-08-26 | `SHMD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `NCNO` | cash | leftover split 14.21 < 1 share @ 19.33 |
| 2026-08-26 | `SJM` | cash | leftover split 14.21 < 1 share @ 134.80 |
| 2026-08-26 | `SMTC` | cash | leftover split 14.21 < 1 share @ 130.90 |
| 2026-08-27 | `SHMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `TIGR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `LI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `QFIN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `QMLS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `SFL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `TIGR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `LI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `QFIN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `QMLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `SFL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `ADSK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ESTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `FRO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `GAP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `HAFN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `PD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `RBRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `S` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `LX` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `ADSK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ESTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `FRO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `GAP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `HAFN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `PD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `RBRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `S` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-02 | `FCEL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `AI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MOMO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `PHR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `TTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `VSXY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `WOOF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `GWRE` | cash | leftover split 12.73 < 1 share @ 167.55 |
| 2026-09-04 | `IOT` | cash | leftover split 12.73 < 1 share @ 44.90 |
| 2026-09-04 | `LULU` | cash | leftover split 12.73 < 1 share @ 98.15 |
| 2026-09-04 | `MAMA` | cash | leftover split 12.73 < 1 share @ 15.70 |
| 2026-09-08 | `AI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `MOMO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `PHR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `TTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `VSXY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `WOOF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `DOMO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `ABM` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `UNFI` | hard_red | hard-red S=-11.47 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `AI` | 148 | 2026-09-03 @ $10.74 | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+8.5; leftover $1595.10 |
| `MOMO` | 290 | 2026-09-03 @ $5.50 | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-4.8; leftover $1595.10 |
| `PHR` | 144 | 2026-09-03 @ $11.02 | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+2.2; leftover $1595.10 |
| `TTC` | 16 | 2026-09-03 @ $99.00 | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ⚪; ret5=-1.2; leftover $1595.10 |
| `VSXY` | 20 | 2026-09-03 @ $76.86 | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-6.6; leftover $1595.10 |
| `WOOF` | 511 | 2026-09-03 @ $3.12 | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=-5.1; leftover $1595.10 |
| `DOMO` | 3 | 2026-09-04 @ $3.62 | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-3.1; leftover $12.73 |
