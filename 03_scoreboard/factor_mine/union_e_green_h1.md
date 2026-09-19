# Factor mine action — `union_e_green_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **-22.25%** ($7,775) · signal-only (no cash/fees) was -18.85%. Starts YES **5/26**. Fills 106 · skips 15 · realized $-2225.39.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- Minimum hold is 1 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 1 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `earn_react=True,last_green=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $7,774.63.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | `INO` | 12176 | — | $0.81 | +0.00 | $0.90 | +1095.84 | +1095.84 | +0.00 | +1095.84 |
| 2026-08-14 | `INO` | 12176 | $0.90 | $0.93 | +365.28 | — | +0.00 | +365.28 | +1461.12 | — |
| 2026-08-14 | `NMAX` | 141 | — | $9.89 | +0.00 | $10.87 | +137.47 | +137.47 | +0.00 | +137.47 |
| 2026-08-14 | `AIRJ` | 253 | — | $5.51 | +0.00 | $6.04 | +134.09 | +134.09 | +0.00 | +134.09 |
| 2026-08-14 | `BRUN` | 53 | — | $26.25 | +0.00 | $22.93 | -175.70 | -175.70 | +0.00 | -175.70 |
| 2026-08-14 | `BZAI` | 1823 | — | $0.77 | +0.00 | $0.59 | -315.38 | -315.38 | +0.00 | -315.38 |
| 2026-08-14 | `DLO` | 91 | — | $15.28 | +0.00 | $14.17 | -101.01 | -101.01 | +0.00 | -101.01 |
| 2026-08-14 | `ENHA` | 604 | — | $2.31 | +0.00 | $1.96 | -211.40 | -211.40 | +0.00 | -211.40 |
| 2026-08-14 | `FIRY` | 143 | — | $9.74 | +0.00 | $9.50 | -34.32 | -34.32 | +0.00 | -34.32 |
| 2026-08-14 | `GEMI` | 352 | — | $3.90 | +0.00 | $3.92 | +7.04 | +7.04 | +0.00 | +7.04 |
| 2026-08-17 | `NMAX` | 141 | $10.87 | $10.97 | +14.10 | — | +0.00 | +14.10 | +151.58 | — |
| 2026-08-17 | `AIRJ` | 253 | $6.04 | $6.22 | +45.54 | — | +0.00 | +45.54 | +179.63 | — |
| 2026-08-17 | `BRUN` | 53 | $22.93 | $23.00 | +3.71 | — | +0.00 | +3.71 | -171.99 | — |
| 2026-08-17 | `BZAI` | 1823 | $0.59 | $0.55 | -74.74 | — | +0.00 | -74.74 | -390.12 | — |
| 2026-08-17 | `DLO` | 91 | $14.17 | $14.23 | +5.46 | — | +0.00 | +5.46 | -95.55 | — |
| 2026-08-17 | `ENHA` | 604 | $1.96 | $2.01 | +30.20 | — | +0.00 | +30.20 | -181.20 | — |
| 2026-08-17 | `FIRY` | 143 | $9.50 | $9.82 | +45.76 | — | +0.00 | +45.76 | +11.44 | — |
| 2026-08-17 | `GEMI` | 352 | $3.92 | $3.89 | -10.56 | — | +0.00 | -10.56 | -3.52 | — |
| 2026-08-18 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `ATAT` | 38 | — | $34.05 | +0.00 | $34.25 | +7.60 | +7.60 | +0.00 | +7.60 |
| 2026-08-20 | `ATHM` | 58 | — | $22.44 | +0.00 | $22.12 | -18.56 | -18.56 | +0.00 | -18.56 |
| 2026-08-20 | `BABA` | 10 | — | $123.47 | +0.00 | $130.53 | +70.60 | +70.60 | +0.00 | +70.60 |
| 2026-08-20 | `BULL` | 133 | — | $9.94 | +0.00 | $8.85 | -144.97 | -144.97 | +0.00 | -144.97 |
| 2026-08-20 | `COTY` | 519 | — | $2.55 | +0.00 | $2.75 | +103.80 | +103.80 | +0.00 | +103.80 |
| 2026-08-20 | `DQ` | 91 | — | $14.44 | +0.00 | $14.98 | +49.14 | +49.14 | +0.00 | +49.14 |
| 2026-08-20 | `FUTU` | 11 | — | $117.65 | +0.00 | $112.73 | -54.12 | -54.12 | +0.00 | -54.12 |
| 2026-08-20 | `IOND` | 20 | — | $65.60 | +0.00 | $68.77 | +63.40 | +63.40 | +0.00 | +63.40 |
| 2026-08-21 | `ATAT` | 38 | $34.25 | $34.31 | +2.28 | — | +0.00 | +2.28 | +9.88 | — |
| 2026-08-21 | `ATHM` | 58 | $22.12 | $22.20 | +4.64 | — | +0.00 | +4.64 | -13.92 | — |
| 2026-08-21 | `BABA` | 10 | $130.53 | $125.35 | -51.80 | — | +0.00 | -51.80 | +18.80 | — |
| 2026-08-21 | `BULL` | 133 | $8.85 | $8.99 | +18.62 | — | +0.00 | +18.62 | -126.35 | — |
| 2026-08-21 | `COTY` | 519 | $2.75 | $2.71 | -20.76 | — | +0.00 | -20.76 | +83.04 | — |
| 2026-08-21 | `DQ` | 91 | $14.98 | $15.00 | +1.82 | — | +0.00 | +1.82 | +50.96 | — |
| 2026-08-21 | `FUTU` | 11 | $112.73 | $115.18 | +26.95 | — | +0.00 | +26.95 | -27.17 | — |
| 2026-08-21 | `IOND` | 20 | $68.77 | $68.41 | -7.20 | — | +0.00 | -7.20 | +56.20 | — |
| 2026-08-21 | `BJ` | 37 | — | $93.98 | +0.00 | $96.42 | +90.28 | +90.28 | +0.00 | +90.28 |
| 2026-08-21 | `BKE` | 81 | — | $43.08 | +0.00 | $43.81 | +59.13 | +59.13 | +0.00 | +59.13 |
| 2026-08-21 | `PSEC` | 1535 | — | $2.30 | +0.00 | $2.33 | +46.05 | +46.05 | +0.00 | +46.05 |
| 2026-08-24 | `BJ` | 37 | $96.42 | $97.02 | +22.20 | — | +0.00 | +22.20 | +112.48 | — |
| 2026-08-24 | `BKE` | 81 | $43.81 | $44.22 | +33.21 | — | +0.00 | +33.21 | +92.34 | — |
| 2026-08-24 | `PSEC` | 1535 | $2.33 | $2.34 | +15.35 | — | +0.00 | +15.35 | +61.40 | — |
| 2026-08-25 | `SHMD` | 2372 | — | $4.54 | +0.00 | $3.42 | -2668.50 | -2668.50 | +0.00 | -2668.50 |
| 2026-08-26 | `SHMD` | 2372 | $3.42 | $3.38 | -94.88 | — | +0.00 | -94.88 | -2763.38 | — |
| 2026-08-26 | `TIGR` | 191 | — | $5.21 | +0.00 | $5.46 | +47.75 | +47.75 | +0.00 | +47.75 |
| 2026-08-26 | `LI` | 82 | — | $12.14 | +0.00 | $12.14 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-08-26 | `NCNO` | 51 | — | $19.33 | +0.00 | $21.51 | +111.18 | +111.18 | +0.00 | +111.18 |
| 2026-08-26 | `QFIN` | 102 | — | $9.76 | +0.00 | $9.35 | -41.82 | -41.82 | +0.00 | -41.82 |
| 2026-08-26 | `QMLS` | 154 | — | $6.47 | +0.00 | $6.10 | -56.98 | -56.98 | +0.00 | -56.98 |
| 2026-08-26 | `SFL` | 80 | — | $12.35 | +0.00 | $12.03 | -25.60 | -25.60 | +0.00 | -25.60 |
| 2026-08-26 | `SJM` | 7 | — | $134.80 | +0.00 | $130.90 | -27.30 | -27.30 | +0.00 | -27.30 |
| 2026-08-26 | `SMTC` | 7 | — | $130.90 | +0.00 | $140.80 | +69.30 | +69.30 | +0.00 | +69.30 |
| 2026-08-27 | `TIGR` | 191 | $5.46 | $5.49 | +5.73 | — | +0.00 | +5.73 | +53.48 | — |
| 2026-08-27 | `LI` | 82 | $12.14 | $12.35 | +17.22 | — | +0.00 | +17.22 | +17.22 | — |
| 2026-08-27 | `NCNO` | 51 | $21.51 | $22.03 | +26.52 | — | +0.00 | +26.52 | +137.70 | — |
| 2026-08-27 | `QFIN` | 102 | $9.35 | $9.42 | +7.14 | — | +0.00 | +7.14 | -34.68 | — |
| 2026-08-27 | `QMLS` | 154 | $6.10 | $6.33 | +35.42 | — | +0.00 | +35.42 | -21.56 | — |
| 2026-08-27 | `SFL` | 80 | $12.03 | $12.03 | +0.00 | — | +0.00 | +0.00 | -25.60 | — |
| 2026-08-27 | `SJM` | 7 | $130.90 | $130.29 | -4.27 | — | +0.00 | -4.27 | -31.57 | — |
| 2026-08-27 | `SMTC` | 7 | $140.80 | $149.40 | +60.20 | — | +0.00 | +60.20 | +129.50 | — |
| 2026-08-27 | `BBY` | 25 | — | $80.60 | +0.00 | $83.56 | +74.00 | +74.00 | +0.00 | +74.00 |
| 2026-08-27 | `HQY` | 21 | — | $97.16 | +0.00 | $93.39 | -79.17 | -79.17 | +0.00 | -79.17 |
| 2026-08-27 | `RY` | 9 | — | $206.82 | +0.00 | $204.54 | -20.52 | -20.52 | +0.00 | -20.52 |
| 2026-08-27 | `TD` | 17 | — | $120.17 | +0.00 | $121.09 | +15.64 | +15.64 | +0.00 | +15.64 |
| 2026-08-28 | `BBY` | 25 | $83.56 | $83.85 | +7.25 | — | +0.00 | +7.25 | +81.25 | — |
| 2026-08-28 | `HQY` | 21 | $93.39 | $93.62 | +4.83 | — | +0.00 | +4.83 | -74.34 | — |
| 2026-08-28 | `RY` | 9 | $204.54 | $205.50 | +8.64 | — | +0.00 | +8.64 | -11.88 | — |
| 2026-08-28 | `TD` | 17 | $121.09 | $122.07 | +16.66 | — | +0.00 | +16.66 | +32.30 | — |
| 2026-08-28 | `ADSK` | 3 | — | $261.16 | +0.00 | $260.66 | -1.50 | -1.50 | +0.00 | -1.50 |
| 2026-08-28 | `ESTC` | 9 | — | $103.89 | +0.00 | $99.91 | -35.82 | -35.82 | +0.00 | -35.82 |
| 2026-08-28 | `FRO` | 23 | — | $44.40 | +0.00 | $44.19 | -4.83 | -4.83 | +0.00 | -4.83 |
| 2026-08-28 | `GAP` | 41 | — | $24.69 | +0.00 | $23.48 | -49.61 | -49.61 | +0.00 | -49.61 |
| 2026-08-28 | `HAFN` | 122 | — | $8.35 | +0.00 | $8.47 | +14.64 | +14.64 | +0.00 | +14.64 |
| 2026-08-28 | `PD` | 78 | — | $13.09 | +0.00 | $13.83 | +57.72 | +57.72 | +0.00 | +57.72 |
| 2026-08-28 | `RBRK` | 10 | — | $98.95 | +0.00 | $93.05 | -59.00 | -59.00 | +0.00 | -59.00 |
| 2026-08-28 | `S` | 47 | — | $21.49 | +0.00 | $21.54 | +2.35 | +2.35 | +0.00 | +2.35 |
| 2026-08-31 | `ADSK` | 3 | $260.66 | $257.71 | -8.85 | — | +0.00 | -8.85 | -10.35 | — |
| 2026-08-31 | `ESTC` | 9 | $99.91 | $98.00 | -17.19 | — | +0.00 | -17.19 | -53.01 | — |
| 2026-08-31 | `FRO` | 23 | $44.19 | $44.85 | +15.18 | — | +0.00 | +15.18 | +10.35 | — |
| 2026-08-31 | `GAP` | 41 | $23.48 | $22.98 | -20.50 | — | +0.00 | -20.50 | -70.11 | — |
| 2026-08-31 | `HAFN` | 122 | $8.47 | $8.53 | +7.32 | — | +0.00 | +7.32 | +21.96 | — |
| 2026-08-31 | `PD` | 78 | $13.83 | $13.58 | -19.50 | — | +0.00 | -19.50 | +38.22 | — |
| 2026-08-31 | `RBRK` | 10 | $93.05 | $92.83 | -2.20 | — | +0.00 | -2.20 | -61.20 | — |
| 2026-08-31 | `S` | 47 | $21.54 | $21.45 | -4.23 | — | +0.00 | -4.23 | -1.88 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `AI` | 124 | — | $10.74 | +0.00 | $10.90 | +19.22 | +19.22 | +0.00 | +19.22 |
| 2026-09-03 | `MOMO` | 243 | — | $5.50 | +0.00 | $5.10 | -97.20 | -97.20 | +0.00 | -97.20 |
| 2026-09-03 | `PHR` | 121 | — | $11.02 | +0.00 | $11.10 | +9.68 | +9.68 | +0.00 | +9.68 |
| 2026-09-03 | `TTC` | 13 | — | $99.00 | +0.00 | $92.37 | -86.19 | -86.19 | +0.00 | -86.19 |
| 2026-09-03 | `VSXY` | 17 | — | $76.86 | +0.00 | $73.64 | -54.74 | -54.74 | +0.00 | -54.74 |
| 2026-09-03 | `WOOF` | 428 | — | $3.12 | +0.00 | $2.52 | -256.80 | -256.80 | +0.00 | -256.80 |
| 2026-09-04 | `AI` | 124 | $10.90 | $10.91 | +1.24 | — | +0.00 | +1.24 | +20.46 | — |
| 2026-09-04 | `MOMO` | 243 | $5.10 | $5.13 | +7.29 | — | +0.00 | +7.29 | -89.91 | — |
| 2026-09-04 | `PHR` | 121 | $11.10 | $11.08 | -2.42 | — | +0.00 | -2.42 | +7.26 | — |
| 2026-09-04 | `TTC` | 13 | $92.37 | $92.14 | -2.99 | — | +0.00 | -2.99 | -89.18 | — |
| 2026-09-04 | `VSXY` | 17 | $73.64 | $73.63 | -0.17 | — | +0.00 | -0.17 | -54.91 | — |
| 2026-09-04 | `WOOF` | 428 | $2.52 | $2.51 | -4.28 | — | +0.00 | -4.28 | -261.08 | — |
| 2026-09-04 | `DOMO` | 416 | — | $3.62 | +0.00 | $3.88 | +110.24 | +110.24 | +0.00 | +110.24 |
| 2026-09-04 | `GWRE` | 8 | — | $167.55 | +0.00 | $162.42 | -41.04 | -41.04 | +0.00 | -41.04 |
| 2026-09-04 | `IOT` | 33 | — | $44.90 | +0.00 | $40.20 | -155.10 | -155.10 | +0.00 | -155.10 |
| 2026-09-04 | `LULU` | 15 | — | $98.15 | +0.00 | $100.61 | +36.90 | +36.90 | +0.00 | +36.90 |
| 2026-09-04 | `MAMA` | 95 | — | $15.70 | +0.00 | $15.16 | -51.30 | -51.30 | +0.00 | -51.30 |
| 2026-09-08 | `DOMO` | 416 | $3.88 | $3.84 | -16.64 | — | +0.00 | -16.64 | +93.60 | — |
| 2026-09-08 | `GWRE` | 8 | $162.42 | $160.52 | -15.20 | — | +0.00 | -15.20 | -56.24 | — |
| 2026-09-08 | `IOT` | 33 | $40.20 | $39.56 | -21.12 | — | +0.00 | -21.12 | -176.22 | — |
| 2026-09-08 | `LULU` | 15 | $100.61 | $100.58 | -0.45 | — | +0.00 | -0.45 | +36.45 | — |
| 2026-09-08 | `MAMA` | 95 | $15.16 | $15.20 | +3.80 | — | +0.00 | +3.80 | -47.50 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `KR` | 131 | — | $56.02 | +0.00 | $58.49 | +323.57 | +323.57 | +0.00 | +323.57 |
| 2026-09-14 | `KR` | 131 | $58.49 | $59.31 | +107.42 | — | +0.00 | +107.42 | +430.99 | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-16 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-17 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-18 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +1,095.84 | INO | — | $2.29 | $10,960.69 | INO×12176 |
| 2026-08-14 | +5.50 | $2.29 | INO×12176 | $11,325.97 | +365.28 | -559.21 | NMAX, AIRJ, BRUN, BZAI, DLO, ENHA, FIRY, GEMI | INO | $1.85 | $10,570.62 | NMAX×141, AIRJ×253, BRUN×53, BZAI×1823, DLO×91, ENHA×604, FIRY×143, GEMI×352 |
| 2026-08-17 | +2.25 | $1.85 | NMAX×141, AIRJ×253, BRUN×53, BZAI×1823, DLO×91, ENHA×604, FIRY×143, GEMI×352 | $10,630.08 | +59.46 | +0.00 | — | NMAX, AIRJ, BRUN, BZAI, DLO, ENHA, FIRY, GEMI | $10,589.05 | $10,589.05 | — |
| 2026-08-18 | -6.20 | $10,589.05 | — | $10,589.05 | -0.00 | +0.00 | — | — | $10,589.05 | $10,589.05 | — |
| 2026-08-19 | -7.20 | $10,589.05 | — | $10,589.05 | -0.00 | +0.00 | — | — | $10,589.05 | $10,589.05 | — |
| 2026-08-20 | +1.12 | $10,589.05 | — | $10,589.05 | -0.00 | +76.89 | ATAT, ATHM, BABA, BULL, COTY, DQ, FUTU, IOND | — | $171.56 | $10,644.23 | ATAT×38, ATHM×58, BABA×10, BULL×133, COTY×519, DQ×91, FUTU×11, IOND×20 |
| 2026-08-21 | +3.25 | $171.56 | ATAT×38, ATHM×58, BABA×10, BULL×133, COTY×519, DQ×91, FUTU×11, IOND×20 | $10,618.78 | -25.45 | +195.46 | BJ, BKE, PSEC | ATAT, ATHM, BABA, BULL, COTY, DQ, FUTU, IOND | $75.44 | $10,768.14 | BJ×37, BKE×81, PSEC×1535 |
| 2026-08-24 | -5.17 | $75.44 | BJ×37, BKE×81, PSEC×1535 | $10,838.90 | +70.76 | +0.00 | — | BJ, BKE, PSEC | $10,814.40 | $10,814.40 | — |
| 2026-08-25 | +1.80 | $10,814.40 | — | $10,814.40 | +0.00 | -2,668.50 | SHMD | — | $3.06 | $8,115.30 | SHMD×2372 |
| 2026-08-26 | +2.02 | $3.06 | SHMD×2372 | $8,020.42 | -94.88 | +76.53 | TIGR, LI, NCNO, QFIN, QMLS, SFL, SJM, SMTC | SHMD | $155.20 | $8,047.95 | TIGR×191, LI×82, NCNO×51, QFIN×102, QMLS×154, SFL×80, SJM×7, SMTC×7 |
| 2026-08-27 | — | $155.20 | TIGR×191, LI×82, NCNO×51, QFIN×102, QMLS×154, SFL×80, SJM×7, SMTC×7 | $8,195.91 | +147.96 | -10.05 | BBY, HQY, RY, TD | TIGR, LI, NCNO, QFIN, QMLS, SFL, SJM, SMTC | $209.95 | $8,159.53 | BBY×25, HQY×21, RY×9, TD×17 |
| 2026-08-28 | +0.75 | $209.95 | BBY×25, HQY×21, RY×9, TD×17 | $8,196.91 | +37.38 | -76.05 | ADSK, ESTC, FRO, GAP, HAFN, PD, RBRK, S | BBY, HQY, RY, TD | $380.48 | $8,095.66 | ADSK×3, ESTC×9, FRO×23, GAP×41, HAFN×122, PD×78, RBRK×10, S×47 |
| 2026-08-31 | -5.85 | $380.48 | ADSK×3, ESTC×9, FRO×23, GAP×41, HAFN×122, PD×78, RBRK×10, S×47 | $8,045.69 | -49.97 | +0.00 | — | ADSK, ESTC, FRO, GAP, HAFN, PD, RBRK, S | $8,028.60 | $8,028.60 | — |
| 2026-09-01 | -6.30 | $8,028.60 | — | $8,028.60 | +0.00 | +0.00 | — | — | $8,028.60 | $8,028.60 | — |
| 2026-09-02 | -3.83 | $8,028.60 | — | $8,028.60 | +0.00 | +0.00 | — | — | $8,028.60 | $8,028.60 | — |
| 2026-09-03 | -0.90 | $8,028.60 | — | $8,028.60 | +0.00 | -466.03 | AI, MOMO, PHR, TTC, VSXY, WOOF | — | $79.88 | $7,545.13 | AI×124, MOMO×243, PHR×121, TTC×13, VSXY×17, WOOF×428 |
| 2026-09-04 | +2.25 | $79.88 | AI×124, MOMO×243, PHR×121, TTC×13, VSXY×17, WOOF×428 | $7,543.80 | -1.33 | -100.30 | DOMO, GWRE, IOT, LULU, MAMA | AI, MOMO, PHR, TTC, VSXY, WOOF | $222.66 | $7,412.05 | DOMO×416, GWRE×8, IOT×33, LULU×15, MAMA×95 |
| 2026-09-08 | -11.47 | $222.66 | DOMO×416, GWRE×8, IOT×33, LULU×15, MAMA×95 | $7,362.44 | -49.61 | +0.00 | — | DOMO, GWRE, IOT, LULU, MAMA | $7,348.49 | $7,348.49 | — |
| 2026-09-09 | -13.95 | $7,348.49 | — | $7,348.49 | -0.00 | +0.00 | — | — | $7,348.49 | $7,348.49 | — |
| 2026-09-10 | -13.28 | $7,348.49 | — | $7,348.49 | -0.00 | +0.00 | — | — | $7,348.49 | $7,348.49 | — |
| 2026-09-11 | +0.50 | $7,348.49 | — | $7,348.49 | -0.00 | +323.57 | KR | — | $7.48 | $7,669.67 | KR×131 |
| 2026-09-14 | -11.00 | $7.48 | KR×131 | $7,777.09 | +107.42 | +0.00 | — | KR | $7,774.63 | $7,774.63 | — |
| 2026-09-15 | -3.84 | $7,774.63 | — | $7,774.63 | -0.00 | +0.00 | — | — | $7,774.63 | $7,774.63 | — |
| 2026-09-16 | +5.30 | $7,774.63 | — | $7,774.63 | -0.00 | +0.00 | — | — | $7,774.63 | $7,774.63 | — |
| 2026-09-17 | +7.38 | $7,774.63 | — | $7,774.63 | -0.00 | +0.00 | — | — | $7,774.63 | $7,774.63 | — |
| 2026-09-18 | +4.86 | $7,774.63 | — | $7,774.63 | -0.00 | +0.00 | — | — | $7,774.63 | $7,774.63 | — |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 12176 | $0.81 | $135.15 | — | $2.29 | — | combo gate; gate earn_react=True,last_green=True; list flatten; ⚪; ret5=+13.2; leftover $10000.00 | join🟢 sector🟢 gen🟢 |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.29 | ▲ close $10,960.69 vs 09:30 $10,000.00 (session +1,095.84) | 16:00 close · cash $2.29 · equity $10,960.69 vs 09:30 $10,000.00 (+960.69; session marks +1095.84) · 1 name(s) marked open→close (per-name table). INO×12176 09:30 $0.81 → close $0.90 +1095.84 | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.29 | ▲ 09:30 equity $11,325.97 vs yday $10,960.69 (+365.28) | 09:30 open · cash $2.29 (unchanged overnight, no fees) · equity $11,325.97 vs prior close $10,960.69 (+365.28) · 1 name(s) re-marked at the open (per-name table). INO×12176 yday $0.90 → 09:30 $0.93 +365.28 | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 12176 | $0.93 | $151.88 | $+1174.09 | $11,174.09 | ▲ +1,174.09 after sell → book $11,174.09; vs 09:30 mark -151.88 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `NMAX` | 141 | $9.89 | $2.41 | — | $9,776.48 | — | combo gate; gate earn_react=True,last_green=True; list ohlc_hot,earn_react; 🔵; ⚪; ret5=+10.9; leftover $1396.76 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRJ` | 253 | $5.51 | $3.26 | — | $8,379.19 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ⚪; ret5=+13.1; leftover $1396.76 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BRUN` | 53 | $26.25 | $2.15 | — | $6,986.05 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ⚪; ret5=+31.2; leftover $1396.76 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BZAI` | 1823 | $0.77 | $19.43 | — | $5,570.20 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ⚪; ret5=+20.4; leftover $1396.76 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `DLO` | 91 | $15.28 | $2.26 | — | $4,177.46 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ⚪; ret5=-0.1; leftover $1396.76 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ENHA` | 604 | $2.31 | $7.79 | — | $2,774.43 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ⚪; ret5=-5.3; leftover $1396.76 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `FIRY` | 143 | $9.74 | $2.42 | — | $1,379.19 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+1.2; leftover $1396.76 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `GEMI` | 352 | $3.90 | $4.54 | — | $1.85 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ⚪; ret5=+8.0; leftover $1396.76 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.85 | ▼ close $10,570.62 vs 09:30 $11,325.97 (session -559.21) | 16:00 close · cash $1.85 · equity $10,570.62 vs 09:30 $11,325.97 (-755.35; session marks -559.21) · 8 name(s) marked open→close (per-name table). NMAX×141 09:30 $9.89 → close $10.87 +137.47; AIRJ×253 09:30 $5.51 → close $6.04 +134.09; BRUN×53 09:30 $26.25 → close $22.93 -175.70; BZAI×1823 09:30 $0.77 → close $0.59 -315.38; DLO×91 09:30 $15.28 → close $14.17 -101.01; ENHA×604 09:30 $2.31 → close $1.96 -211.40; FIRY×143 09:30 $9.74 → close $9.50 -34.32; GEMI×352 09:30 $3.90 → close $3.92 +7.04 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.85 | ▲ 09:30 equity $10,630.08 vs yday $10,570.62 (+59.46) | 09:30 open · cash $1.85 (unchanged overnight, no fees) · equity $10,630.08 vs prior close $10,570.62 (+59.46) · 8 name(s) re-marked at the open (per-name table). NMAX×141 yday $10.87 → 09:30 $10.97 +14.10; AIRJ×253 yday $6.04 → 09:30 $6.22 +45.54; BRUN×53 yday $22.93 → 09:30 $23.00 +3.71; BZAI×1823 yday $0.59 → 09:30 $0.55 -74.74; DLO×91 yday $14.17 → 09:30 $14.23 +5.46; ENHA×604 yday $1.96 → 09:30 $2.01 +30.20; FIRY×143 yday $9.50 → 09:30 $9.82 +45.76; GEMI×352 yday $3.92 → 09:30 $3.89 -10.56 | — |
| 2026-08-17 09:30 ET | **SELL** | `NMAX` | 141 | $10.97 | $2.45 | $+146.71 | $1,546.17 | ▲ +146.71 after sell → book $10,627.64; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `AIRJ` | 253 | $6.22 | $3.32 | $+173.05 | $3,116.51 | ▲ +173.05 after sell → book $10,624.32; vs 09:30 mark -3.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BRUN` | 53 | $23.00 | $2.17 | $-176.30 | $4,333.34 | ▼ -176.30 after sell → book $10,622.15; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `BZAI` | 1823 | $0.55 | $15.84 | $-425.40 | $5,323.79 | ▼ -425.40 after sell → book $10,606.30; vs 09:30 mark -15.85 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `DLO` | 91 | $14.23 | $2.29 | $-100.10 | $6,616.44 | ▼ -100.10 after sell → book $10,604.02; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ENHA` | 604 | $2.01 | $7.90 | $-196.89 | $7,822.57 | ▼ -196.89 after sell → book $10,596.11; vs 09:30 mark -7.91 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `FIRY` | 143 | $9.82 | $2.45 | $+6.57 | $9,224.38 | ▲ +6.57 after sell → book $10,593.66; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `GEMI` | 352 | $3.89 | $4.61 | $-12.67 | $10,589.05 | ▼ -12.67 after sell → book $10,589.05; vs 09:30 mark -4.61 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,589.05 | ▲ close $10,589.05 vs 09:30 $10,630.08 (session +0.00) | 16:00 close · cash $10,589.05 · no lots left · equity $10,589.05. | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,589.05 | ▲ 09:30 equity $10,589.05 vs yday $10,589.05 (-0.00) | 09:30 open · cash $10,589.05 · no holdings · equity $10,589.05 vs prior close $10,589.05 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,589.05 | ▲ close $10,589.05 vs 09:30 $10,589.05 (session +0.00) | 16:00 close · cash $10,589.05 · no lots left · equity $10,589.05. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,589.05 | ▲ 09:30 equity $10,589.05 vs yday $10,589.05 (-0.00) | 09:30 open · cash $10,589.05 · no holdings · equity $10,589.05 vs prior close $10,589.05 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,589.05 | ▲ close $10,589.05 vs 09:30 $10,589.05 (session +0.00) | 16:00 close · cash $10,589.05 · no lots left · equity $10,589.05. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,589.05 | ▲ 09:30 equity $10,589.05 vs yday $10,589.05 (-0.00) | 09:30 open · cash $10,589.05 · no holdings · equity $10,589.05 vs prior close $10,589.05 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `ATAT` | 38 | $34.05 | $2.10 | — | $9,293.05 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+9.3; leftover $1323.63 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ATHM` | 58 | $22.44 | $2.16 | — | $7,989.36 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=-2.1; leftover $1323.63 | join🔴 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 peer🔴 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BABA` | 10 | $123.47 | $2.02 | — | $6,752.64 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+2.9; leftover $1323.63 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BULL` | 133 | $9.94 | $2.39 | — | $5,428.23 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+12.6; leftover $1323.63 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `COTY` | 519 | $2.55 | $6.70 | — | $4,098.09 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ⚪; ret5=+9.8; leftover $1323.63 | join🟡 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `DQ` | 91 | $14.44 | $2.26 | — | $2,781.78 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-3.8; leftover $1323.63 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `FUTU` | 11 | $117.65 | $2.02 | — | $1,485.61 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+4.1; leftover $1323.63 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IOND` | 20 | $65.60 | $2.05 | — | $171.56 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ⚪; ret5=+3.7; leftover $1323.63 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $171.56 | ▲ close $10,644.23 vs 09:30 $10,589.05 (session +76.89) | 16:00 close · cash $171.56 · equity $10,644.23 vs 09:30 $10,589.05 (+55.18; session marks +76.89) · 8 name(s) marked open→close (per-name table). ATAT×38 09:30 $34.05 → close $34.25 +7.60; ATHM×58 09:30 $22.44 → close $22.12 -18.56; BABA×10 09:30 $123.47 → close $130.53 +70.60; BULL×133 09:30 $9.94 → close $8.85 -144.97; COTY×519 09:30 $2.55 → close $2.75 +103.80; DQ×91 09:30 $14.44 → close $14.98 +49.14; FUTU×11 09:30 $117.65 → close $112.73 -54.12; IOND×20 09:30 $65.60 → close $68.77 +63.40 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $171.56 | ▼ 09:30 equity $10,618.78 vs yday $10,644.23 (-25.45) | 09:30 open · cash $171.56 (unchanged overnight, no fees) · equity $10,618.78 vs prior close $10,644.23 (-25.45) · 8 name(s) re-marked at the open (per-name table). ATAT×38 yday $34.25 → 09:30 $34.31 +2.28; ATHM×58 yday $22.12 → 09:30 $22.20 +4.64; BABA×10 yday $130.53 → 09:30 $125.35 -51.80; BULL×133 yday $8.85 → 09:30 $8.99 +18.62; COTY×519 yday $2.75 → 09:30 $2.71 -20.76; DQ×91 yday $14.98 → 09:30 $15.00 +1.82; FUTU×11 yday $112.73 → 09:30 $115.18 +26.95; IOND×20 yday $68.77 → 09:30 $68.41 -7.20 | — |
| 2026-08-21 09:30 ET | **SELL** | `ATAT` | 38 | $34.31 | $2.12 | $+5.65 | $1,473.22 | ▲ +5.65 after sell → book $10,616.66; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ATHM` | 58 | $22.20 | $2.18 | $-18.27 | $2,758.63 | ▼ -18.27 after sell → book $10,614.47; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BABA` | 10 | $125.35 | $2.04 | $+14.74 | $4,010.09 | ▲ +14.74 after sell → book $10,612.43; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BULL` | 133 | $8.99 | $2.42 | $-131.16 | $5,203.34 | ▼ -131.16 after sell → book $10,610.01; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `COTY` | 519 | $2.71 | $6.79 | $+69.55 | $6,603.04 | ▲ +69.55 after sell → book $10,603.22; vs 09:30 mark -6.79 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `DQ` | 91 | $15.00 | $2.29 | $+46.41 | $7,965.75 | ▲ +46.41 after sell → book $10,600.93; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `FUTU` | 11 | $115.18 | $2.04 | $-31.24 | $9,230.69 | ▼ -31.24 after sell → book $10,598.89; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `IOND` | 20 | $68.41 | $2.07 | $+52.08 | $10,596.82 | ▲ +52.08 after sell → book $10,596.82; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `BJ` | 37 | $93.98 | $2.10 | — | $7,117.45 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=-2.4; leftover $3532.27 | join🟡 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BKE` | 81 | $43.08 | $2.23 | — | $3,625.74 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=-4.9; leftover $3532.27 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `PSEC` | 1535 | $2.30 | $19.80 | — | $75.44 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=-3.0; leftover $3532.27 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $75.44 | ▲ close $10,768.14 vs 09:30 $10,618.78 (session +195.46) | 16:00 close · cash $75.44 · equity $10,768.14 vs 09:30 $10,618.78 (+149.36; session marks +195.46) · 3 name(s) marked open→close (per-name table). BJ×37 09:30 $93.98 → close $96.42 +90.28; BKE×81 09:30 $43.08 → close $43.81 +59.13; PSEC×1535 09:30 $2.30 → close $2.33 +46.05 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $75.44 | ▲ 09:30 equity $10,838.90 vs yday $10,768.14 (+70.76) | 09:30 open · cash $75.44 (unchanged overnight, no fees) · equity $10,838.90 vs prior close $10,768.14 (+70.76) · 3 name(s) re-marked at the open (per-name table). BJ×37 yday $96.42 → 09:30 $97.02 +22.20; BKE×81 yday $43.81 → 09:30 $44.22 +33.21; PSEC×1535 yday $2.33 → 09:30 $2.34 +15.35 | — |
| 2026-08-24 09:30 ET | **SELL** | `BJ` | 37 | $97.02 | $2.14 | $+108.24 | $3,663.04 | ▲ +108.24 after sell → book $10,836.76; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `BKE` | 81 | $44.22 | $2.28 | $+87.83 | $7,242.59 | ▲ +87.83 after sell → book $10,834.49; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `PSEC` | 1535 | $2.34 | $20.09 | $+21.51 | $10,814.40 | ▲ +21.51 after sell → book $10,814.40; vs 09:30 mark -20.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,814.40 | ▲ close $10,814.40 vs 09:30 $10,838.90 (session +0.00) | 16:00 close · cash $10,814.40 · no lots left · equity $10,814.40. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,814.40 | ▲ 09:30 equity $10,814.40 vs yday $10,814.40 (+0.00) | 09:30 open · cash $10,814.40 · no holdings · equity $10,814.40 vs prior close $10,814.40 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `SHMD` | 2372 | $4.54 | $30.60 | — | $3.06 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-14.6; leftover $10814.40 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟡 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.06 | ▼ close $8,115.30 vs 09:30 $10,814.40 (session -2,668.50) | 16:00 close · cash $3.06 · equity $8,115.30 vs 09:30 $10,814.40 (-2699.10; session marks -2668.50) · 1 name(s) marked open→close (per-name table). SHMD×2372 09:30 $4.54 → close $3.42 -2668.50 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.06 | ▼ 09:30 equity $8,020.42 vs yday $8,115.30 (-94.88) | 09:30 open · cash $3.06 (unchanged overnight, no fees) · equity $8,020.42 vs prior close $8,115.30 (-94.88) · 1 name(s) re-marked at the open (per-name table). SHMD×2372 yday $3.42 → 09:30 $3.38 -94.88 | — |
| 2026-08-26 09:30 ET | **SELL** | `SHMD` | 2372 | $3.38 | $31.06 | $-2825.04 | $7,989.36 | ▼ -2,825.04 after sell → book $7,989.36; vs 09:30 mark -31.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `TIGR` | 191 | $5.21 | $2.56 | — | $6,991.69 | — | combo gate; gate earn_react=True,last_green=True; list ohlc_hot,earn_react; 🔵; ret5=+14.3; leftover $998.67 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `LI` | 82 | $12.14 | $2.24 | — | $5,993.98 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+1.2; leftover $998.67 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `NCNO` | 51 | $19.33 | $2.14 | — | $5,006.00 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+3.0; leftover $998.67 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `QFIN` | 102 | $9.76 | $2.30 | — | $4,008.19 | — | combo gate; gate earn_react=True,last_green=True; list earn_react,oppset; 🔵; ret5=-4.4; leftover $998.67 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `QMLS` | 154 | $6.47 | $2.45 | — | $3,009.36 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-7.0; leftover $998.67 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `SFL` | 80 | $12.35 | $2.23 | — | $2,019.12 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=-1.7; leftover $998.67 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `SJM` | 7 | $134.80 | $2.01 | — | $1,073.51 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+5.9; leftover $998.67 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `SMTC` | 7 | $130.90 | $2.01 | — | $155.20 | — | combo gate; gate earn_react=True,last_green=True; list earn_react,oppset; 🔵; ret5=-5.7; leftover $998.67 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $155.20 | ▲ close $8,047.95 vs 09:30 $8,020.42 (session +76.53) | 16:00 close · cash $155.20 · equity $8,047.95 vs 09:30 $8,020.42 (+27.53; session marks +76.53) · 8 name(s) marked open→close (per-name table). TIGR×191 09:30 $5.21 → close $5.46 +47.75; LI×82 09:30 $12.14 → close $12.14 +0.00; NCNO×51 09:30 $19.33 → close $21.51 +111.18; QFIN×102 09:30 $9.76 → close $9.35 -41.82; QMLS×154 09:30 $6.47 → close $6.10 -56.98; SFL×80 09:30 $12.35 → close $12.03 -25.60; SJM×7 09:30 $134.80 → close $130.90 -27.30; SMTC×7 09:30 $130.90 → close $140.80 +69.30 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $155.20 | ▲ 09:30 equity $8,195.91 vs yday $8,047.95 (+147.96) | 09:30 open · cash $155.20 (unchanged overnight, no fees) · equity $8,195.91 vs prior close $8,047.95 (+147.96) · 8 name(s) re-marked at the open (per-name table). TIGR×191 yday $5.46 → 09:30 $5.49 +5.73; LI×82 yday $12.14 → 09:30 $12.35 +17.22; NCNO×51 yday $21.51 → 09:30 $22.03 +26.52; QFIN×102 yday $9.35 → 09:30 $9.42 +7.14; QMLS×154 yday $6.10 → 09:30 $6.33 +35.42; SFL×80 yday $12.03 → 09:30 $12.03 +0.00; SJM×7 yday $130.90 → 09:30 $130.29 -4.27; SMTC×7 yday $140.80 → 09:30 $149.40 +60.20 | — |
| 2026-08-27 09:30 ET | **SELL** | `TIGR` | 191 | $5.49 | $2.60 | $+48.31 | $1,201.19 | ▲ +48.31 after sell → book $8,193.31; vs 09:30 mark -2.60 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `LI` | 82 | $12.35 | $2.26 | $+12.72 | $2,211.63 | ▲ +12.72 after sell → book $8,191.05; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `NCNO` | 51 | $22.03 | $2.16 | $+133.39 | $3,333.00 | ▲ +133.39 after sell → book $8,188.89; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `QFIN` | 102 | $9.42 | $2.32 | $-39.30 | $4,291.51 | ▼ -39.30 after sell → book $8,186.56; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `QMLS` | 154 | $6.33 | $2.49 | $-26.50 | $5,263.85 | ▼ -26.50 after sell → book $8,184.08; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🟡 ab🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `SFL` | 80 | $12.03 | $2.25 | $-30.08 | $6,223.99 | ▼ -30.08 after sell → book $8,181.82; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SJM` | 7 | $130.29 | $2.03 | $-35.61 | $7,133.99 | ▼ -35.61 after sell → book $8,179.79; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `SMTC` | 7 | $149.40 | $2.03 | $+125.46 | $8,177.76 | ▲ +125.46 after sell → book $8,177.76; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟡 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `BBY` | 25 | $80.60 | $2.06 | — | $6,160.69 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=-2.0; leftover $2044.44 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `HQY` | 21 | $97.16 | $2.05 | — | $4,118.28 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+0.5; leftover $2044.44 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `RY` | 9 | $206.82 | $2.02 | — | $2,254.88 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=-0.2; leftover $2044.44 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟡 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `TD` | 17 | $120.17 | $2.04 | — | $209.95 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+0.9; leftover $2044.44 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $209.95 | ▼ close $8,159.53 vs 09:30 $8,195.91 (session -10.05) | 16:00 close · cash $209.95 · equity $8,159.53 vs 09:30 $8,195.91 (-36.38; session marks -10.05) · 4 name(s) marked open→close (per-name table). BBY×25 09:30 $80.60 → close $83.56 +74.00; HQY×21 09:30 $97.16 → close $93.39 -79.17; RY×9 09:30 $206.82 → close $204.54 -20.52; TD×17 09:30 $120.17 → close $121.09 +15.64 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $209.95 | ▲ 09:30 equity $8,196.91 vs yday $8,159.53 (+37.38) | 09:30 open · cash $209.95 (unchanged overnight, no fees) · equity $8,196.91 vs prior close $8,159.53 (+37.38) · 4 name(s) re-marked at the open (per-name table). BBY×25 yday $83.56 → 09:30 $83.85 +7.25; HQY×21 yday $93.39 → 09:30 $93.62 +4.83; RY×9 yday $204.54 → 09:30 $205.50 +8.64; TD×17 yday $121.09 → 09:30 $122.07 +16.66 | — |
| 2026-08-28 09:30 ET | **SELL** | `BBY` | 25 | $83.85 | $2.09 | $+77.09 | $2,304.11 | ▲ +77.09 after sell → book $8,194.82; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `HQY` | 21 | $93.62 | $2.08 | $-78.47 | $4,268.05 | ▼ -78.47 after sell → book $8,192.74; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `RY` | 9 | $205.50 | $2.04 | $-15.94 | $6,115.51 | ▼ -15.94 after sell → book $8,190.70; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `TD` | 17 | $122.07 | $2.07 | $+28.19 | $8,188.63 | ▲ +28.19 after sell → book $8,188.63; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 3 | $261.16 | $2.00 | — | $7,403.15 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+7.8; leftover $1023.58 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ESTC` | 9 | $103.89 | $2.02 | — | $6,466.13 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=-2.5; leftover $1023.58 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `FRO` | 23 | $44.40 | $2.06 | — | $5,442.87 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+0.4; leftover $1023.58 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GAP` | 41 | $24.69 | $2.11 | — | $4,428.47 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+5.8; leftover $1023.58 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `HAFN` | 122 | $8.35 | $2.36 | — | $3,407.41 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+5.1; leftover $1023.58 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `PD` | 78 | $13.09 | $2.22 | — | $2,384.17 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+4.2; leftover $1023.58 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `RBRK` | 10 | $98.95 | $2.02 | — | $1,392.65 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+9.7; leftover $1023.58 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `S` | 47 | $21.49 | $2.13 | — | $380.48 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+8.5; leftover $1023.58 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $380.48 | ▼ close $8,095.66 vs 09:30 $8,196.91 (session -76.05) | 16:00 close · cash $380.48 · equity $8,095.66 vs 09:30 $8,196.91 (-101.24; session marks -76.05) · 8 name(s) marked open→close (per-name table). ADSK×3 09:30 $261.16 → close $260.66 -1.50; ESTC×9 09:30 $103.89 → close $99.91 -35.82; FRO×23 09:30 $44.40 → close $44.19 -4.83; GAP×41 09:30 $24.69 → close $23.48 -49.61; HAFN×122 09:30 $8.35 → close $8.47 +14.64; PD×78 09:30 $13.09 → close $13.83 +57.72; RBRK×10 09:30 $98.95 → close $93.05 -59.00; S×47 09:30 $21.49 → close $21.54 +2.35 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $380.48 | ▼ 09:30 equity $8,045.69 vs yday $8,095.66 (-49.97) | 09:30 open · cash $380.48 (unchanged overnight, no fees) · equity $8,045.69 vs prior close $8,095.66 (-49.97) · 8 name(s) re-marked at the open (per-name table). ADSK×3 yday $260.66 → 09:30 $257.71 -8.85; ESTC×9 yday $99.91 → 09:30 $98.00 -17.19; FRO×23 yday $44.19 → 09:30 $44.85 +15.18; GAP×41 yday $23.48 → 09:30 $22.98 -20.50; HAFN×122 yday $8.47 → 09:30 $8.53 +7.32; PD×78 yday $13.83 → 09:30 $13.58 -19.50; RBRK×10 yday $93.05 → 09:30 $92.83 -2.20; S×47 yday $21.54 → 09:30 $21.45 -4.23 | — |
| 2026-08-31 09:30 ET | **SELL** | `ADSK` | 3 | $257.71 | $2.02 | $-14.37 | $1,151.60 | ▼ -14.37 after sell → book $8,043.68; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ESTC` | 9 | $98.00 | $2.04 | $-57.06 | $2,031.56 | ▼ -57.06 after sell → book $8,041.64; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `FRO` | 23 | $44.85 | $2.08 | $+6.21 | $3,061.03 | ▲ +6.21 after sell → book $8,039.56; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GAP` | 41 | $22.98 | $2.13 | $-74.36 | $4,001.08 | ▼ -74.36 after sell → book $8,037.43; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟢 digest🟢 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `HAFN` | 122 | $8.53 | $2.39 | $+17.22 | $5,039.35 | ▲ +17.22 after sell → book $8,035.04; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `PD` | 78 | $13.58 | $2.25 | $+33.75 | $6,096.34 | ▲ +33.75 after sell → book $8,032.79; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `RBRK` | 10 | $92.83 | $2.04 | $-65.26 | $7,022.60 | ▼ -65.26 after sell → book $8,030.75; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟢 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `S` | 47 | $21.45 | $2.15 | $-6.16 | $8,028.60 | ▼ -6.16 after sell → book $8,028.60; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,028.60 | ▲ close $8,028.60 vs 09:30 $8,045.69 (session +0.00) | 16:00 close · cash $8,028.60 · no lots left · equity $8,028.60. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,028.60 | ▲ 09:30 equity $8,028.60 vs yday $8,028.60 (+0.00) | 09:30 open · cash $8,028.60 · no holdings · equity $8,028.60 vs prior close $8,028.60 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,028.60 | ▲ close $8,028.60 vs 09:30 $8,028.60 (session +0.00) | 16:00 close · cash $8,028.60 · no lots left · equity $8,028.60. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,028.60 | ▲ 09:30 equity $8,028.60 vs yday $8,028.60 (+0.00) | 09:30 open · cash $8,028.60 · no holdings · equity $8,028.60 vs prior close $8,028.60 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,028.60 | ▲ close $8,028.60 vs 09:30 $8,028.60 (session +0.00) | 16:00 close · cash $8,028.60 · no lots left · equity $8,028.60. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,028.60 | ▲ 09:30 equity $8,028.60 vs yday $8,028.60 (+0.00) | 09:30 open · cash $8,028.60 · no holdings · equity $8,028.60 vs prior close $8,028.60 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `AI` | 124 | $10.74 | $2.36 | — | $6,693.86 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+8.5; leftover $1338.10 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MOMO` | 243 | $5.50 | $3.13 | — | $5,354.23 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-4.8; leftover $1338.10 | join🟢 sector🟡 gen🟡 news🟡 digest🟡 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `PHR` | 121 | $11.02 | $2.35 | — | $4,018.45 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+2.2; leftover $1338.10 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `TTC` | 13 | $99.00 | $2.03 | — | $2,729.42 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ⚪; ret5=-1.2; leftover $1338.10 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSXY` | 17 | $76.86 | $2.04 | — | $1,420.76 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-6.6; leftover $1338.10 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `WOOF` | 428 | $3.12 | $5.52 | — | $79.88 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=-5.1; leftover $1338.10 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $79.88 | ▼ close $7,545.13 vs 09:30 $8,028.60 (session -466.03) | 16:00 close · cash $79.88 · equity $7,545.13 vs 09:30 $8,028.60 (-483.47; session marks -466.03) · 6 name(s) marked open→close (per-name table). AI×124 09:30 $10.74 → close $10.90 +19.22; MOMO×243 09:30 $5.50 → close $5.10 -97.20; PHR×121 09:30 $11.02 → close $11.10 +9.68; TTC×13 09:30 $99.00 → close $92.37 -86.19; VSXY×17 09:30 $76.86 → close $73.64 -54.74; WOOF×428 09:30 $3.12 → close $2.52 -256.80 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $79.88 | ▼ 09:30 equity $7,543.80 vs yday $7,545.13 (-1.33) | 09:30 open · cash $79.88 (unchanged overnight, no fees) · equity $7,543.80 vs prior close $7,545.13 (-1.33) · 6 name(s) re-marked at the open (per-name table). AI×124 yday $10.90 → 09:30 $10.91 +1.24; MOMO×243 yday $5.10 → 09:30 $5.13 +7.29; PHR×121 yday $11.10 → 09:30 $11.08 -2.42; TTC×13 yday $92.37 → 09:30 $92.14 -2.99; VSXY×17 yday $73.64 → 09:30 $73.63 -0.17; WOOF×428 yday $2.52 → 09:30 $2.51 -4.28 | — |
| 2026-09-04 09:30 ET | **SELL** | `AI` | 124 | $10.91 | $2.39 | $+15.70 | $1,430.33 | ▲ +15.70 after sell → book $7,541.41; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MOMO` | 243 | $5.13 | $3.19 | $-96.23 | $2,673.73 | ▼ -96.23 after sell → book $7,538.22; vs 09:30 mark -3.19 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `PHR` | 121 | $11.08 | $2.38 | $+2.52 | $4,012.03 | ▲ +2.52 after sell → book $7,535.84; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `TTC` | 13 | $92.14 | $2.05 | $-93.26 | $5,207.80 | ▼ -93.26 after sell → book $7,533.79; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | join🟡 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `VSXY` | 17 | $73.63 | $2.06 | $-59.01 | $6,457.45 | ▼ -59.01 after sell → book $7,531.73; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `WOOF` | 428 | $2.51 | $5.60 | $-272.20 | $7,526.13 | ▼ -272.20 after sell → book $7,526.13; vs 09:30 mark -5.60 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `DOMO` | 416 | $3.62 | $5.37 | — | $6,016.92 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-3.1; leftover $1505.23 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `GWRE` | 8 | $167.55 | $2.01 | — | $4,674.51 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+0.9; leftover $1505.23 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟡 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `IOT` | 33 | $44.90 | $2.09 | — | $3,190.72 | — | combo gate; gate earn_react=True,last_green=True; list earn_react,oppset; ret5=-7.5; leftover $1505.23 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `LULU` | 15 | $98.15 | $2.04 | — | $1,716.43 | — | combo gate; gate earn_react=True,last_green=True; list earn_react,oppset; ret5=+5.9; leftover $1505.23 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MAMA` | 95 | $15.70 | $2.27 | — | $222.66 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=-0.4; leftover $1505.23 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $222.66 | ▼ close $7,412.05 vs 09:30 $7,543.80 (session -100.30) | 16:00 close · cash $222.66 · equity $7,412.05 vs 09:30 $7,543.80 (-131.75; session marks -100.30) · 5 name(s) marked open→close (per-name table). DOMO×416 09:30 $3.62 → close $3.88 +110.24; GWRE×8 09:30 $167.55 → close $162.42 -41.04; IOT×33 09:30 $44.90 → close $40.20 -155.10; LULU×15 09:30 $98.15 → close $100.61 +36.90; MAMA×95 09:30 $15.70 → close $15.16 -51.30 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $222.66 | ▼ 09:30 equity $7,362.44 vs yday $7,412.05 (-49.61) | 09:30 open · cash $222.66 (unchanged overnight, no fees) · equity $7,362.44 vs prior close $7,412.05 (-49.61) · 5 name(s) re-marked at the open (per-name table). DOMO×416 yday $3.88 → 09:30 $3.84 -16.64; GWRE×8 yday $162.42 → 09:30 $160.52 -15.20; IOT×33 yday $40.20 → 09:30 $39.56 -21.12; LULU×15 yday $100.61 → 09:30 $100.58 -0.45; MAMA×95 yday $15.16 → 09:30 $15.20 +3.80 | — |
| 2026-09-08 09:30 ET | **SELL** | `DOMO` | 416 | $3.84 | $5.45 | $+82.79 | $1,814.65 | ▲ +82.79 after sell → book $7,356.99; vs 09:30 mark -5.45 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `GWRE` | 8 | $160.52 | $2.03 | $-60.29 | $3,096.78 | ▼ -60.29 after sell → book $7,354.96; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `IOT` | 33 | $39.56 | $2.11 | $-180.42 | $4,400.15 | ▼ -180.42 after sell → book $7,352.85; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `LULU` | 15 | $100.58 | $2.06 | $+32.36 | $5,906.79 | ▲ +32.36 after sell → book $7,350.79; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-08 09:30 ET | **SELL** | `MAMA` | 95 | $15.20 | $2.30 | $-52.08 | $7,348.49 | ▼ -52.08 after sell → book $7,348.49; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,348.49 | ▲ close $7,348.49 vs 09:30 $7,362.44 (session +0.00) | 16:00 close · cash $7,348.49 · no lots left · equity $7,348.49. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,348.49 | ▲ 09:30 equity $7,348.49 vs yday $7,348.49 (-0.00) | 09:30 open · cash $7,348.49 · no holdings · equity $7,348.49 vs prior close $7,348.49 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,348.49 | ▲ close $7,348.49 vs 09:30 $7,348.49 (session +0.00) | 16:00 close · cash $7,348.49 · no lots left · equity $7,348.49. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,348.49 | ▲ 09:30 equity $7,348.49 vs yday $7,348.49 (-0.00) | 09:30 open · cash $7,348.49 · no holdings · equity $7,348.49 vs prior close $7,348.49 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,348.49 | ▲ close $7,348.49 vs 09:30 $7,348.49 (session +0.00) | 16:00 close · cash $7,348.49 · no lots left · equity $7,348.49. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,348.49 | ▲ 09:30 equity $7,348.49 vs yday $7,348.49 (-0.00) | 09:30 open · cash $7,348.49 · no holdings · equity $7,348.49 vs prior close $7,348.49 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `KR` | 131 | $56.02 | $2.38 | — | $7.48 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=-2.2; leftover $7348.49 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7.48 | ▲ close $7,669.67 vs 09:30 $7,348.49 (session +323.57) | 16:00 close · cash $7.48 · equity $7,669.67 vs 09:30 $7,348.49 (+321.18; session marks +323.57) · 1 name(s) marked open→close (per-name table). KR×131 09:30 $56.02 → close $58.49 +323.57 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7.48 | ▲ 09:30 equity $7,777.09 vs yday $7,669.67 (+107.42) | 09:30 open · cash $7.48 (unchanged overnight, no fees) · equity $7,777.09 vs prior close $7,669.67 (+107.42) · 1 name(s) re-marked at the open (per-name table). KR×131 yday $58.49 → 09:30 $59.31 +107.42 | — |
| 2026-09-14 09:30 ET | **SELL** | `KR` | 131 | $59.31 | $2.47 | $+426.14 | $7,774.63 | ▲ +426.14 after sell → book $7,774.63; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,774.63 | ▲ close $7,774.63 vs 09:30 $7,777.09 (session +0.00) | 16:00 close · cash $7,774.63 · no lots left · equity $7,774.63. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,774.63 | ▲ 09:30 equity $7,774.63 vs yday $7,774.63 (-0.00) | 09:30 open · cash $7,774.63 · no holdings · equity $7,774.63 vs prior close $7,774.63 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,774.63 | ▲ close $7,774.63 vs 09:30 $7,774.63 (session +0.00) | 16:00 close · cash $7,774.63 · no lots left · equity $7,774.63. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,774.63 | ▲ 09:30 equity $7,774.63 vs yday $7,774.63 (-0.00) | 09:30 open · cash $7,774.63 · no holdings · equity $7,774.63 vs prior close $7,774.63 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,774.63 | ▲ close $7,774.63 vs 09:30 $7,774.63 (session +0.00) | 16:00 close · cash $7,774.63 · no lots left · equity $7,774.63. | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,774.63 | ▲ 09:30 equity $7,774.63 vs yday $7,774.63 (-0.00) | 09:30 open · cash $7,774.63 · no holdings · equity $7,774.63 vs prior close $7,774.63 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,774.63 | ▲ close $7,774.63 vs 09:30 $7,774.63 (session +0.00) | 16:00 close · cash $7,774.63 · no lots left · equity $7,774.63. | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,774.63 | ▲ 09:30 equity $7,774.63 vs yday $7,774.63 (-0.00) | 09:30 open · cash $7,774.63 · no holdings · equity $7,774.63 vs prior close $7,774.63 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,774.63 | ▲ close $7,774.63 vs 09:30 $7,774.63 (session +0.00) | 16:00 close · cash $7,774.63 · no lots left · equity $7,774.63. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `FN` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `VNET` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `DVLT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `JKHY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SQM` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `YMM` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-31 | `LX` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-02 | `FCEL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `ABM` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `UNFI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `AVO` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `DBI` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-15 | `HITI` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `PLAY` | hard_red | hard-red S=-3.84 sit; no new buys |
