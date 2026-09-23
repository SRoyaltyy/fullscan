# Factor mine action — `union_rsi_os_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ rsi_os, no 🚨

Cash book **-33.85%** ($6,615) · signal-only (no cash/fees) was -4.54%. Starts YES **0/29**. Fills 88 · skips 14 · realized $-2833.43.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: prior RSI is oversold (≤30) — Finviz prior export, else computed on prior bars.
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

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
- **Gate** `rsi_os=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $11.33.

Per-name 09:30 / close marks **PASS** — overnight $ sums to 09:30 equity vs prior close, and on no-fill days intraday $ sums to close equity vs 09:30. No session is skipped.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-08-13 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-14 | `NCMI` | 929 | — | $2.69 | +0.00 | $2.86 | +157.93 | +157.93 | +0.00 | +157.93 |
| 2026-08-14 | `QMLS` | 342 | — | $7.29 | +0.00 | $7.32 | +10.26 | +10.26 | +0.00 | +10.26 |
| 2026-08-14 | `CLBT` | 230 | — | $10.83 | +0.00 | $11.14 | +71.30 | +71.30 | +0.00 | +71.30 |
| 2026-08-14 | `YSS` | 247 | — | $10.06 | +0.00 | $10.93 | +214.89 | +214.89 | +0.00 | +214.89 |
| 2026-08-17 | `NCMI` | 929 | $2.86 | $2.80 | -55.74 | — | +0.00 | -55.74 | +102.19 | — |
| 2026-08-17 | `QMLS` | 342 | $7.32 | $7.24 | -27.36 | — | +0.00 | -27.36 | -17.10 | — |
| 2026-08-17 | `CLBT` | 230 | $11.14 | $11.19 | +11.50 | — | +0.00 | +11.50 | +82.80 | — |
| 2026-08-17 | `YSS` | 247 | $10.93 | $10.36 | -140.79 | — | +0.00 | -140.79 | +74.10 | — |
| 2026-08-17 | `CDNL` | 63 | — | $39.85 | +0.00 | $39.23 | -39.06 | -39.06 | +0.00 | -39.06 |
| 2026-08-17 | `INV` | 1573 | — | $1.62 | +0.00 | $1.39 | -369.66 | -369.66 | +0.00 | -369.66 |
| 2026-08-17 | `KLC` | 972 | — | $2.62 | +0.00 | $2.56 | -58.32 | -58.32 | +0.00 | -58.32 |
| 2026-08-17 | `CSAN` | 1017 | — | $2.50 | +0.00 | $2.52 | +20.34 | +20.34 | +0.00 | +20.34 |
| 2026-08-18 | `CDNL` | 63 | $39.23 | $41.57 | +147.42 | — | +0.00 | +147.42 | +108.36 | — |
| 2026-08-18 | `INV` | 1573 | $1.39 | $1.32 | -94.38 | — | +0.00 | -94.38 | -464.04 | — |
| 2026-08-18 | `KLC` | 972 | $2.56 | $2.52 | -38.88 | — | +0.00 | -38.88 | -97.20 | — |
| 2026-08-18 | `CSAN` | 1017 | $2.52 | $2.51 | -10.17 | — | +0.00 | -10.17 | +10.17 | — |
| 2026-08-19 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-20 | `LZB` | 287 | — | $33.61 | +0.00 | $33.65 | +11.48 | +11.48 | +0.00 | +11.48 |
| 2026-08-21 | `LZB` | 287 | $33.65 | $33.63 | -5.74 | — | +0.00 | -5.74 | +5.74 | — |
| 2026-08-21 | `AAP` | 113 | — | $42.41 | +0.00 | $42.58 | +19.21 | +19.21 | +0.00 | +19.21 |
| 2026-08-21 | `WMT` | 46 | — | $103.69 | +0.00 | $103.70 | +0.46 | +0.46 | +0.00 | +0.46 |
| 2026-08-24 | `AAP` | 113 | $42.58 | $43.05 | +53.11 | — | +0.00 | +53.11 | +72.32 | — |
| 2026-08-24 | `WMT` | 46 | $103.70 | $104.14 | +20.24 | — | +0.00 | +20.24 | +20.70 | — |
| 2026-08-25 | `QFIN` | 439 | — | $11.09 | +0.00 | $11.53 | +193.16 | +193.16 | +0.00 | +193.16 |
| 2026-08-25 | `QMLS` | 818 | — | $5.93 | +0.00 | $6.27 | +278.12 | +278.12 | +0.00 | +278.12 |
| 2026-08-26 | `QFIN` | 439 | $11.53 | $9.76 | -777.03 | $9.35 | -179.99 | -957.02 | -583.87 | -763.86 |
| 2026-08-26 | `QMLS` | 818 | $6.27 | $6.47 | +163.60 | $6.10 | -302.66 | -139.06 | +441.72 | +139.06 |
| 2026-08-27 | `QFIN` | 439 | $9.35 | $9.42 | +30.73 | $9.17 | -109.75 | -79.02 | -733.13 | -842.88 |
| 2026-08-27 | `QMLS` | 818 | $6.10 | $6.33 | +188.14 | $6.47 | +114.52 | +302.66 | +327.20 | +441.72 |
| 2026-08-28 | `QFIN` | 439 | $9.17 | $9.15 | -8.78 | $8.80 | -153.65 | -162.43 | -851.66 | -1005.31 |
| 2026-08-28 | `QMLS` | 818 | $6.47 | $6.27 | -163.60 | — | +0.00 | -163.60 | +278.12 | — |
| 2026-08-28 | `DY` | 8 | — | $306.34 | +0.00 | $294.34 | -96.00 | -96.00 | +0.00 | -96.00 |
| 2026-08-28 | `LX` | 2207 | — | $1.16 | +0.00 | $1.18 | +44.14 | +44.14 | +0.00 | +44.14 |
| 2026-08-31 | `QFIN` | 439 | $8.80 | $8.70 | -43.90 | — | +0.00 | -43.90 | -1049.21 | — |
| 2026-08-31 | `DY` | 8 | $294.34 | $298.01 | +29.36 | — | +0.00 | +29.36 | -66.64 | — |
| 2026-08-31 | `LX` | 2207 | $1.18 | $1.01 | -375.19 | $1.07 | +132.42 | -242.77 | -331.05 | -198.63 |
| 2026-09-01 | `LX` | 2207 | $1.07 | $1.01 | -132.42 | — | +0.00 | -132.42 | -331.05 | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `SION` | 144 | — | $7.31 | +0.00 | $6.75 | -80.64 | -80.64 | +0.00 | -80.64 |
| 2026-09-03 | `ALMS` | 102 | — | $10.38 | +0.00 | $11.36 | +100.47 | +100.47 | +0.00 | +100.47 |
| 2026-09-03 | `LX` | 1281 | — | $0.83 | +0.00 | $0.85 | +32.03 | +32.03 | +0.00 | +32.03 |
| 2026-09-03 | `EVTL` | 1655 | — | $0.64 | +0.00 | $0.60 | -66.20 | -66.20 | +0.00 | -66.20 |
| 2026-09-03 | `FJET` | 373 | — | $2.84 | +0.00 | $2.78 | -22.38 | -22.38 | +0.00 | -22.38 |
| 2026-09-03 | `OSW` | 48 | — | $22.00 | +0.00 | $22.27 | +12.96 | +12.96 | +0.00 | +12.96 |
| 2026-09-03 | `PL` | 53 | — | $19.86 | +0.00 | $18.35 | -80.03 | -80.03 | +0.00 | -80.03 |
| 2026-09-03 | `SWBI` | 80 | — | $12.78 | +0.00 | $12.27 | -40.80 | -40.80 | +0.00 | -40.80 |
| 2026-09-04 | `SION` | 144 | $6.75 | $6.68 | -10.08 | $7.18 | +72.00 | +61.92 | -90.72 | -18.72 |
| 2026-09-04 | `ALMS` | 102 | $11.36 | $11.23 | -13.26 | — | +0.00 | -13.26 | +87.21 | — |
| 2026-09-04 | `LX` | 1281 | $0.85 | $0.86 | +7.69 | — | +0.00 | +7.69 | +39.71 | — |
| 2026-09-04 | `EVTL` | 1655 | $0.60 | $0.60 | +0.00 | — | +0.00 | +0.00 | -66.20 | — |
| 2026-09-04 | `FJET` | 373 | $2.78 | $2.80 | +7.46 | — | +0.00 | +7.46 | -14.92 | — |
| 2026-09-04 | `OSW` | 48 | $22.27 | $22.27 | +0.00 | — | +0.00 | +0.00 | +12.96 | — |
| 2026-09-04 | `PL` | 53 | $18.35 | $19.64 | +68.37 | — | +0.00 | +68.37 | -11.66 | — |
| 2026-09-04 | `SWBI` | 80 | $12.27 | $14.12 | +148.00 | $12.89 | -98.40 | +49.60 | +107.20 | +8.80 |
| 2026-09-04 | `AIIO` | 3607 | — | $1.75 | +0.00 | $1.84 | +324.63 | +324.63 | +0.00 | +324.63 |
| 2026-09-08 | `SION` | 144 | $7.18 | $7.13 | -7.20 | — | +0.00 | -7.20 | -25.92 | — |
| 2026-09-08 | `SWBI` | 80 | $12.89 | $12.51 | -30.40 | — | +0.00 | -30.40 | -21.60 | — |
| 2026-09-08 | `AIIO` | 3607 | $1.84 | $1.81 | -108.21 | — | +0.00 | -108.21 | +216.42 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `NAVN` | 137 | — | $20.61 | +0.00 | $21.02 | +56.17 | +56.17 | +0.00 | +56.17 |
| 2026-09-11 | `RWT` | 805 | — | $3.52 | +0.00 | $3.55 | +24.15 | +24.15 | +0.00 | +24.15 |
| 2026-09-11 | `COO` | 51 | — | $54.66 | +0.00 | $53.91 | -38.25 | -38.25 | +0.00 | -38.25 |
| 2026-09-14 | `NAVN` | 137 | $21.02 | $21.10 | +10.96 | — | +0.00 | +10.96 | +67.13 | — |
| 2026-09-14 | `RWT` | 805 | $3.55 | $3.53 | -16.10 | — | +0.00 | -16.10 | +8.05 | — |
| 2026-09-14 | `COO` | 51 | $53.91 | $54.78 | +44.37 | — | +0.00 | +44.37 | +6.12 | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-16 | `ALHC` | 103 | — | $10.30 | +0.00 | $8.71 | -163.77 | -163.77 | +0.00 | -163.77 |
| 2026-09-16 | `PLAY` | 155 | — | $6.86 | +0.00 | $6.86 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-09-16 | `DVLT` | 6685 | — | $0.16 | +0.00 | $0.18 | +133.70 | +133.70 | +0.00 | +133.70 |
| 2026-09-16 | `NMRA` | 1267 | — | $0.84 | +0.00 | $0.77 | -89.96 | -89.96 | +0.00 | -89.96 |
| 2026-09-16 | `ZSQR` | 457 | — | $2.34 | +0.00 | $2.29 | -22.85 | -22.85 | +0.00 | -22.85 |
| 2026-09-16 | `CTMX` | 393 | — | $2.72 | +0.00 | $2.77 | +17.68 | +17.68 | +0.00 | +17.68 |
| 2026-09-16 | `CRBP` | 155 | — | $6.86 | +0.00 | $7.26 | +62.00 | +62.00 | +0.00 | +62.00 |
| 2026-09-16 | `EYPT` | 280 | — | $3.66 | +0.00 | $3.45 | -58.80 | -58.80 | +0.00 | -58.80 |
| 2026-09-17 | `ALHC` | 103 | $8.71 | $8.58 | -13.39 | $8.70 | +12.36 | -1.03 | -177.16 | -164.80 |
| 2026-09-17 | `PLAY` | 155 | $6.86 | $6.96 | +15.50 | — | +0.00 | +15.50 | +15.50 | — |
| 2026-09-17 | `DVLT` | 6685 | $0.18 | $0.17 | -66.85 | — | +0.00 | -66.85 | +66.85 | — |
| 2026-09-17 | `NMRA` | 1267 | $0.77 | $0.78 | +8.87 | — | +0.00 | +8.87 | -81.09 | — |
| 2026-09-17 | `ZSQR` | 457 | $2.29 | $2.35 | +27.42 | — | +0.00 | +27.42 | +4.57 | — |
| 2026-09-17 | `CTMX` | 393 | $2.77 | $2.83 | +25.54 | — | +0.00 | +25.54 | +43.23 | — |
| 2026-09-17 | `CRBP` | 155 | $7.26 | $7.26 | +0.00 | — | +0.00 | +0.00 | +62.00 | — |
| 2026-09-17 | `EYPT` | 280 | $3.45 | $3.57 | +33.60 | — | +0.00 | +33.60 | -25.20 | — |
| 2026-09-17 | `MRLN` | 3262 | — | $2.27 | +0.00 | $2.06 | -685.02 | -685.02 | +0.00 | -685.02 |
| 2026-09-18 | `ALHC` | 103 | $8.70 | $8.68 | -2.06 | — | +0.00 | -2.06 | -166.86 | — |
| 2026-09-18 | `MRLN` | 3262 | $2.06 | $2.07 | +32.62 | — | +0.00 | +32.62 | -652.40 | — |
| 2026-09-18 | `RARE` | 257 | — | $14.79 | +0.00 | $14.51 | -71.96 | -71.96 | +0.00 | -71.96 |
| 2026-09-18 | `FLNC` | 503 | — | $7.54 | +0.00 | $7.32 | -108.14 | -108.14 | +0.00 | -108.14 |
| 2026-09-21 | `RARE` | 257 | $14.51 | $14.58 | +17.99 | — | +0.00 | +17.99 | -53.97 | — |
| 2026-09-21 | `FLNC` | 503 | $7.32 | $7.36 | +20.12 | — | +0.00 | +20.12 | -88.02 | — |
| 2026-09-21 | `XENE` | 62 | — | $40.00 | +0.00 | $38.99 | -62.62 | -62.62 | +0.00 | -62.62 |
| 2026-09-21 | `SION` | 413 | — | $6.00 | +0.00 | $6.00 | +0.00 | +0.00 | +0.00 | +0.00 |
| 2026-09-21 | `THO` | 36 | — | $68.39 | +0.00 | $69.94 | +55.80 | +55.80 | +0.00 | +55.80 |
| 2026-09-22 | `XENE` | 62 | $38.99 | $39.10 | +6.82 | — | +0.00 | +6.82 | -55.80 | — |
| 2026-09-22 | `SION` | 413 | $6.00 | $5.99 | -4.13 | — | +0.00 | -4.13 | -4.13 | — |
| 2026-09-22 | `THO` | 36 | $69.94 | $70.64 | +25.20 | — | +0.00 | +25.20 | +81.00 | — |
| 2026-09-22 | `FJET` | 3643 | — | $2.03 | +0.00 | $2.02 | -36.43 | -36.43 | +0.00 | -36.43 |
| 2026-09-23 | `FJET` | 3643 | $2.02 | $1.98 | -145.72 | — | +0.00 | -145.72 | -182.15 | — |
| 2026-09-23 | `NMRA` | 2357 | — | $0.76 | +0.00 | $0.73 | -67.65 | -67.65 | +0.00 | -67.65 |
| 2026-09-23 | `CMPX` | 1468 | — | $1.22 | +0.00 | $1.17 | -73.40 | -73.40 | +0.00 | -73.40 |
| 2026-09-23 | `XNDU` | 299 | — | $5.99 | +0.00 | $5.33 | -197.34 | -197.34 | +0.00 | -197.34 |
| 2026-09-23 | `EVER` | 89 | — | $19.46 | +0.00 | $17.63 | -162.87 | -162.87 | +0.00 | -162.87 |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +454.38 | NCMI, QMLS, CLBT, YSS | — | $9.54 | $10,431.83 | NCMI×929, QMLS×342, CLBT×230, YSS×247 |
| 2026-08-17 | +2.25 | $9.54 | NCMI×929, QMLS×342, CLBT×230, YSS×247 | $10,219.44 | -212.39 | -446.70 | CDNL, INV, KLC, CSAN | NCMI, QMLS, CLBT, YSS | $0.44 | $9,701.70 | CDNL×63, INV×1573, KLC×972, CSAN×1017 |
| 2026-08-18 | -6.20 | $0.44 | CDNL×63, INV×1573, KLC×972, CSAN×1017 | $9,705.69 | +3.99 | +0.00 | — | CDNL, INV, KLC, CSAN | $9,656.88 | $9,656.88 | — |
| 2026-08-19 | -7.20 | $9,656.88 | — | $9,656.88 | -0.00 | +0.00 | — | — | $9,656.88 | $9,656.88 | — |
| 2026-08-20 | +1.12 | $9,656.88 | — | $9,656.88 | -0.00 | +11.48 | LZB | — | $7.11 | $9,664.66 | LZB×287 |
| 2026-08-21 | +3.25 | $7.11 | LZB×287 | $9,658.92 | -5.74 | +19.67 | AAP, WMT | LZB | $88.56 | $9,670.30 | AAP×113, WMT×46 |
| 2026-08-24 | -5.17 | $88.56 | AAP×113, WMT×46 | $9,743.65 | +73.35 | +0.00 | — | AAP, WMT | $9,739.09 | $9,739.09 | — |
| 2026-08-25 | +1.80 | $9,739.09 | — | $9,739.09 | -0.00 | +471.28 | QFIN, QMLS | — | $3.62 | $10,194.15 | QFIN×439, QMLS×818 |
| 2026-08-26 | +2.02 | $3.62 | QFIN×439, QMLS×818 | $9,580.72 | -613.43 | -482.65 | — | — | $3.62 | $9,098.07 | QFIN×439, QMLS×818 |
| 2026-08-27 | — | $3.62 | QFIN×439, QMLS×818 | $9,316.94 | +218.87 | +4.77 | — | — | $3.62 | $9,321.71 | QFIN×439, QMLS×818 |
| 2026-08-28 | +0.75 | $3.62 | QFIN×439, QMLS×818 | $9,149.33 | -172.38 | -205.51 | DY, LX | QMLS | $80.43 | $8,902.61 | QFIN×439, DY×8, LX×2207 |
| 2026-08-31 | -5.85 | $80.43 | QFIN×439, DY×8, LX×2207 | $8,512.88 | -389.73 | +132.42 | — | QFIN, DY | $6,276.00 | $8,637.49 | LX×2207 |
| 2026-09-01 | -6.30 | $6,276.00 | LX×2207 | $8,505.07 | -132.42 | +0.00 | — | LX | $8,476.22 | $8,476.22 | — |
| 2026-09-02 | -3.83 | $8,476.22 | — | $8,476.22 | -0.00 | +0.00 | — | — | $8,476.22 | $8,476.22 | — |
| 2026-09-03 | -0.90 | $8,476.22 | — | $8,476.22 | -0.00 | -144.59 | SION, ALMS, LX, EVTL, FJET, OSW, PL, SWBI | — | $10.40 | $8,285.58 | SION×144, ALMS×102, LX×1281, EVTL×1655, FJET×373, OSW×48, PL×53, SWBI×80 |
| 2026-09-04 | +2.25 | $10.40 | SION×144, ALMS×102, LX×1281, EVTL×1655, FJET×373, OSW×48, PL×53, SWBI×80 | $8,493.76 | +208.18 | +298.23 | AIIO | ALMS, LX, EVTL, FJET, OSW, PL | $1.69 | $8,703.69 | SION×144, SWBI×80, AIIO×3607 |
| 2026-09-08 | -11.47 | $1.69 | SION×144, SWBI×80, AIIO×3607 | $8,557.88 | -145.81 | +0.00 | — | SION, SWBI, AIIO | $8,505.99 | $8,505.99 | — |
| 2026-09-09 | -13.95 | $8,505.99 | — | $8,505.99 | +0.00 | +0.00 | — | — | $8,505.99 | $8,505.99 | — |
| 2026-09-10 | -13.28 | $8,505.99 | — | $8,505.99 | +0.00 | +0.00 | — | — | $8,505.99 | $8,505.99 | — |
| 2026-09-11 | +0.50 | $8,505.99 | — | $8,505.99 | +0.00 | +42.07 | NAVN, RWT, COO | — | $46.23 | $8,533.13 | NAVN×137, RWT×805, COO×51 |
| 2026-09-14 | -11.00 | $46.23 | NAVN×137, RWT×805, COO×51 | $8,572.36 | +39.23 | +0.00 | — | NAVN, RWT, COO | $8,557.20 | $8,557.20 | — |
| 2026-09-15 | -3.84 | $8,557.20 | — | $8,557.20 | +0.00 | +0.00 | — | — | $8,557.20 | $8,557.20 | — |
| 2026-09-16 | +5.30 | $8,557.20 | — | $8,557.20 | +0.00 | -122.00 | ALHC, PLAY, DVLT, NMRA, ZSQR, CTMX, CRBP, EYPT | — | $0.58 | $8,368.18 | ALHC×103, PLAY×155, DVLT×6685, NMRA×1267, ZSQR×457, CTMX×393, CRBP×155, EYPT×280 |
| 2026-09-17 | +7.38 | $0.58 | ALHC×103, PLAY×155, DVLT×6685, NMRA×1267, ZSQR×457, CTMX×393, CRBP×155, EYPT×280 | $8,398.87 | +30.69 | -672.66 | MRLN | PLAY, DVLT, NMRA, ZSQR, CTMX, CRBP, EYPT | $2.09 | $7,617.91 | ALHC×103, MRLN×3262 |
| 2026-09-18 | +4.86 | $2.09 | ALHC×103, MRLN×3262 | $7,648.47 | +30.56 | -180.10 | RARE, FLNC | ALHC, MRLN | $2.53 | $7,413.56 | RARE×257, FLNC×503 |
| 2026-09-21 | +12.87 | $2.53 | RARE×257, FLNC×503 | $7,451.67 | +38.11 | -6.82 | XENE, SION, THO | RARE, FLNC | $12.04 | $7,425.26 | XENE×62, SION×413, THO×36 |
| 2026-09-22 | -0.50 | $12.04 | XENE×62, SION×413, THO×36 | $7,453.15 | +27.89 | -36.43 | FJET | XENE, SION, THO | $1.12 | $7,359.98 | FJET×3643 |
| 2026-09-23 | +2.29 | $1.12 | FJET×3643 | $7,214.26 | -145.72 | -501.26 | NMRA, CMPX, XNDU, EVER | FJET | $11.33 | $6,615.31 | NMRA×2357, CMPX×1468, XNDU×299, EVER×89 |

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | 16:00 close · cash $10,000.00 · no lots left · equity $10,000.00. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `NCMI` | 929 | $2.69 | $11.98 | — | $7,489.01 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=-33.5; leftover $2500.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `QMLS` | 342 | $7.29 | $4.41 | — | $4,991.41 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.1; leftover $2500.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `CLBT` | 230 | $10.83 | $2.97 | — | $2,497.55 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ⚪; ret5=-30.1; leftover $2500.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `YSS` | 247 | $10.06 | $3.19 | — | $9.54 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list earn_react; 🔵; ⚪; ret5=+5.7; leftover $2500.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9.54 | ▲ close $10,431.83 vs 09:30 $10,000.00 (session +454.38) | 16:00 close · cash $9.54 · equity $10,431.83 vs 09:30 $10,000.00 (+431.83; session marks +454.38) · 4 name(s) marked open→close (per-name table). NCMI×929 09:30 $2.69 → close $2.86 +157.93; QMLS×342 09:30 $7.29 → close $7.32 +10.26; CLBT×230 09:30 $10.83 → close $11.14 +71.30; YSS×247 09:30 $10.06 → close $10.93 +214.89 | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9.54 | ▼ 09:30 equity $10,219.44 vs yday $10,431.83 (-212.39) | 09:30 open · cash $9.54 (unchanged overnight, no fees) · equity $10,219.44 vs prior close $10,431.83 (-212.39) · 4 name(s) re-marked at the open (per-name table). NCMI×929 yday $2.86 → 09:30 $2.80 -55.74; QMLS×342 yday $7.32 → 09:30 $7.24 -27.36; CLBT×230 yday $11.14 → 09:30 $11.19 +11.50; YSS×247 yday $10.93 → 09:30 $10.36 -140.79 | — |
| 2026-08-17 09:30 ET | **SELL** | `NCMI` | 929 | $2.80 | $12.16 | $+78.05 | $2,598.58 | ▲ +78.05 after sell → book $10,207.28; vs 09:30 mark -12.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `QMLS` | 342 | $7.24 | $4.49 | $-26.00 | $5,070.17 | ▼ -26.00 after sell → book $10,202.79; vs 09:30 mark -4.49 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `CLBT` | 230 | $11.19 | $3.03 | $+76.81 | $7,640.85 | ▲ +76.81 after sell → book $10,199.77; vs 09:30 mark -3.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `YSS` | 247 | $10.36 | $3.25 | $+67.67 | $10,196.52 | ▲ +67.67 after sell → book $10,196.52; vs 09:30 mark -3.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `CDNL` | 63 | $39.85 | $2.18 | — | $7,683.79 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list probable,yday_gainer; ⚪; ret5=-38.4; leftover $2549.13 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `INV` | 1573 | $1.62 | $20.29 | — | $5,115.24 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ⚪; ret5=-53.0; leftover $2549.13 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `KLC` | 972 | $2.62 | $12.54 | — | $2,556.06 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ⚪; ret5=-49.7; leftover $2549.13 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CSAN` | 1017 | $2.50 | $13.12 | — | $0.44 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list earn_react; 🔵; ⚪; ret5=-12.5; leftover $2549.13 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.44 | ▼ close $9,701.70 vs 09:30 $10,219.44 (session -446.70) | 16:00 close · cash $0.44 · equity $9,701.70 vs 09:30 $10,219.44 (-517.74; session marks -446.70) · 4 name(s) marked open→close (per-name table). CDNL×63 09:30 $39.85 → close $39.23 -39.06; INV×1573 09:30 $1.62 → close $1.39 -369.66; KLC×972 09:30 $2.62 → close $2.56 -58.32; CSAN×1017 09:30 $2.50 → close $2.52 +20.34 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.44 | ▲ 09:30 equity $9,705.69 vs yday $9,701.70 (+3.99) | 09:30 open · cash $0.44 (unchanged overnight, no fees) · equity $9,705.69 vs prior close $9,701.70 (+3.99) · 4 name(s) re-marked at the open (per-name table). CDNL×63 yday $39.23 → 09:30 $41.57 +147.42; INV×1573 yday $1.39 → 09:30 $1.32 -94.38; KLC×972 yday $2.56 → 09:30 $2.52 -38.88; CSAN×1017 yday $2.52 → 09:30 $2.51 -10.17 | — |
| 2026-08-18 09:30 ET | **SELL** | `CDNL` | 63 | $41.57 | $2.21 | $+103.97 | $2,617.14 | ▲ +103.97 after sell → book $9,703.48; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `INV` | 1573 | $1.32 | $20.57 | $-504.90 | $4,680.80 | ▼ -504.90 after sell → book $9,682.91; vs 09:30 mark -20.57 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `KLC` | 972 | $2.52 | $12.72 | $-122.46 | $7,117.52 | ▼ -122.46 after sell → book $9,670.19; vs 09:30 mark -12.72 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CSAN` | 1017 | $2.51 | $13.31 | $-16.26 | $9,656.88 | ▼ -16.26 after sell → book $9,656.88; vs 09:30 mark -13.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,656.88 | ▲ close $9,656.88 vs 09:30 $9,705.69 (session +0.00) | 16:00 close · cash $9,656.88 · no lots left · equity $9,656.88. | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,656.88 | ▲ 09:30 equity $9,656.88 vs yday $9,656.88 (-0.00) | 09:30 open · cash $9,656.88 · no holdings · equity $9,656.88 vs prior close $9,656.88 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,656.88 | ▲ close $9,656.88 vs 09:30 $9,656.88 (session +0.00) | 16:00 close · cash $9,656.88 · no lots left · equity $9,656.88. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,656.88 | ▲ 09:30 equity $9,656.88 vs yday $9,656.88 (-0.00) | 09:30 open · cash $9,656.88 · no holdings · equity $9,656.88 vs prior close $9,656.88 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `LZB` | 287 | $33.61 | $3.70 | — | $7.11 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-17.4; leftover $9656.88 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7.11 | ▲ close $9,664.66 vs 09:30 $9,656.88 (session +11.48) | 16:00 close · cash $7.11 · equity $9,664.66 vs 09:30 $9,656.88 (+7.78; session marks +11.48) · 1 name(s) marked open→close (per-name table). LZB×287 09:30 $33.61 → close $33.65 +11.48 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7.11 | ▼ 09:30 equity $9,658.92 vs yday $9,664.66 (-5.74) | 09:30 open · cash $7.11 (unchanged overnight, no fees) · equity $9,658.92 vs prior close $9,664.66 (-5.74) · 1 name(s) re-marked at the open (per-name table). LZB×287 yday $33.65 → 09:30 $33.63 -5.74 | — |
| 2026-08-21 09:30 ET | **SELL** | `LZB` | 287 | $33.63 | $3.83 | $-1.79 | $9,655.09 | ▼ -1.79 after sell → book $9,655.09; vs 09:30 mark -3.83 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AAP` | 113 | $42.41 | $2.33 | — | $4,860.43 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-26.1; leftover $4827.54 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `WMT` | 46 | $103.69 | $2.13 | — | $88.56 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-10.3; leftover $4827.54 | join🟢 sector🔴 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $88.56 | ▲ close $9,670.30 vs 09:30 $9,658.92 (session +19.67) | 16:00 close · cash $88.56 · equity $9,670.30 vs 09:30 $9,658.92 (+11.38; session marks +19.67) · 2 name(s) marked open→close (per-name table). AAP×113 09:30 $42.41 → close $42.58 +19.21; WMT×46 09:30 $103.69 → close $103.70 +0.46 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $88.56 | ▲ 09:30 equity $9,743.65 vs yday $9,670.30 (+73.35) | 09:30 open · cash $88.56 (unchanged overnight, no fees) · equity $9,743.65 vs prior close $9,670.30 (+73.35) · 2 name(s) re-marked at the open (per-name table). AAP×113 yday $42.58 → 09:30 $43.05 +53.11; WMT×46 yday $103.70 → 09:30 $104.14 +20.24 | — |
| 2026-08-24 09:30 ET | **SELL** | `AAP` | 113 | $43.05 | $2.39 | $+67.60 | $4,950.82 | ▲ +67.60 after sell → book $9,741.26; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `WMT` | 46 | $104.14 | $2.18 | $+16.40 | $9,739.09 | ▲ +16.40 after sell → book $9,739.09; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,739.09 | ▲ close $9,739.09 vs 09:30 $9,743.65 (session +0.00) | 16:00 close · cash $9,739.09 · no lots left · equity $9,739.09. | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,739.09 | ▲ 09:30 equity $9,739.09 vs yday $9,739.09 (-0.00) | 09:30 open · cash $9,739.09 · no holdings · equity $9,739.09 vs prior close $9,739.09 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `QFIN` | 439 | $11.09 | $5.66 | — | $4,864.92 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list overnight; 🔵; ret5=-8.0; leftover $4869.54 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `QMLS` | 818 | $5.93 | $10.55 | — | $3.62 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list overnight; 🔵; ret5=-17.5; leftover $4869.54 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 vol🔴 buy🟡 |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.62 | ▲ close $10,194.15 vs 09:30 $9,739.09 (session +471.28) | 16:00 close · cash $3.62 · equity $10,194.15 vs 09:30 $9,739.09 (+455.06; session marks +471.28) · 2 name(s) marked open→close (per-name table). QFIN×439 09:30 $11.09 → close $11.53 +193.16; QMLS×818 09:30 $5.93 → close $6.27 +278.12 | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.62 | ▼ 09:30 equity $9,580.72 vs yday $10,194.15 (-613.43) | 09:30 open · cash $3.62 (unchanged overnight, no fees) · equity $9,580.72 vs prior close $10,194.15 (-613.43) · 2 name(s) re-marked at the open (per-name table). QFIN×439 yday $11.53 → 09:30 $9.76 -777.03; QMLS×818 yday $6.27 → 09:30 $6.47 +163.60 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.62 | ▼ close $9,098.07 vs 09:30 $9,580.72 (session -482.65) | 16:00 close · cash $3.62 · equity $9,098.07 vs 09:30 $9,580.72 (-482.65; session marks -482.65) · 2 name(s) marked open→close (per-name table). QFIN×439 09:30 $9.76 → close $9.35 -179.99; QMLS×818 09:30 $6.47 → close $6.10 -302.66 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.62 | ▲ 09:30 equity $9,316.94 vs yday $9,098.07 (+218.87) | 09:30 open · cash $3.62 (unchanged overnight, no fees) · equity $9,316.94 vs prior close $9,098.07 (+218.87) · 2 name(s) re-marked at the open (per-name table). QFIN×439 yday $9.35 → 09:30 $9.42 +30.73; QMLS×818 yday $6.10 → 09:30 $6.33 +188.14 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.62 | ▲ close $9,321.71 vs 09:30 $9,316.94 (session +4.77) | 16:00 close · cash $3.62 · equity $9,321.71 vs 09:30 $9,316.94 (+4.77; session marks +4.77) · 2 name(s) marked open→close (per-name table). QFIN×439 09:30 $9.42 → close $9.17 -109.75; QMLS×818 09:30 $6.33 → close $6.47 +114.52 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.62 | ▼ 09:30 equity $9,149.33 vs yday $9,321.71 (-172.38) | 09:30 open · cash $3.62 (unchanged overnight, no fees) · equity $9,149.33 vs prior close $9,321.71 (-172.38) · 2 name(s) re-marked at the open (per-name table). QFIN×439 yday $9.17 → 09:30 $9.15 -8.78; QMLS×818 yday $6.47 → 09:30 $6.27 -163.60 | — |
| 2026-08-28 09:30 ET | **SELL** | `QMLS` | 818 | $6.27 | $10.73 | $+256.84 | $5,121.75 | ▲ +256.84 after sell → book $9,138.60; vs 09:30 mark -10.73 | dropped from list after 3 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `DY` | 8 | $306.34 | $2.01 | — | $2,669.02 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-23.0; leftover $2560.88 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `LX` | 2207 | $1.16 | $28.47 | — | $80.43 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list overnight; ret5=-13.8; leftover $2560.88 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $80.43 | ▼ close $8,902.61 vs 09:30 $9,149.33 (session -205.51) | 16:00 close · cash $80.43 · equity $8,902.61 vs 09:30 $9,149.33 (-246.72; session marks -205.51) · 3 name(s) marked open→close (per-name table). QFIN×439 09:30 $9.15 → close $8.80 -153.65; DY×8 09:30 $306.34 → close $294.34 -96.00; LX×2207 09:30 $1.16 → close $1.18 +44.14 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $80.43 | ▼ 09:30 equity $8,512.88 vs yday $8,902.61 (-389.73) | 09:30 open · cash $80.43 (unchanged overnight, no fees) · equity $8,512.88 vs prior close $8,902.61 (-389.73) · 3 name(s) re-marked at the open (per-name table). QFIN×439 yday $8.80 → 09:30 $8.70 -43.90; DY×8 yday $294.34 → 09:30 $298.01 +29.36; LX×2207 yday $1.18 → 09:30 $1.01 -375.19 | — |
| 2026-08-31 09:30 ET | **SELL** | `QFIN` | 439 | $8.70 | $5.77 | $-1060.64 | $3,893.96 | ▼ -1,060.64 after sell → book $8,507.11; vs 09:30 mark -5.77 | dropped from list after 4 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `DY` | 8 | $298.01 | $2.04 | $-70.70 | $6,276.00 | ▼ -70.70 after sell → book $8,505.07; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,276.00 | ▲ close $8,637.49 vs 09:30 $8,512.88 (session +132.42) | 16:00 close · cash $6,276.00 · equity $8,637.49 vs 09:30 $8,512.88 (+124.61; session marks +132.42) · 1 name(s) marked open→close (per-name table). LX×2207 09:30 $1.01 → close $1.07 +132.42 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,276.00 | ▼ 09:30 equity $8,505.07 vs yday $8,637.49 (-132.42) | 09:30 open · cash $6,276.00 (unchanged overnight, no fees) · equity $8,505.07 vs prior close $8,637.49 (-132.42) · 1 name(s) re-marked at the open (per-name table). LX×2207 yday $1.07 → 09:30 $1.01 -132.42 | — |
| 2026-09-01 09:30 ET | **SELL** | `LX` | 2207 | $1.01 | $28.85 | $-388.37 | $8,476.22 | ▼ -388.37 after sell → book $8,476.22; vs 09:30 mark -28.85 | dropped from list after 2 sess (min 1) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,476.22 | ▲ close $8,476.22 vs 09:30 $8,505.07 (session +0.00) | 16:00 close · cash $8,476.22 · no lots left · equity $8,476.22. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,476.22 | ▲ 09:30 equity $8,476.22 vs yday $8,476.22 (-0.00) | 09:30 open · cash $8,476.22 · no holdings · equity $8,476.22 vs prior close $8,476.22 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,476.22 | ▲ close $8,476.22 vs 09:30 $8,476.22 (session +0.00) | 16:00 close · cash $8,476.22 · no lots left · equity $8,476.22. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,476.22 | ▲ 09:30 equity $8,476.22 vs yday $8,476.22 (-0.00) | 09:30 open · cash $8,476.22 · no holdings · equity $8,476.22 vs prior close $8,476.22 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `SION` | 144 | $7.31 | $2.42 | — | $7,421.15 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_gainer; 🔵; ret5=+18.5; leftover $1059.53 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ALMS` | 102 | $10.38 | $2.30 | — | $6,360.61 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-56.2; leftover $1059.53 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `LX` | 1281 | $0.83 | $14.44 | — | $5,286.78 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-30.4; leftover $1059.53 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `EVTL` | 1655 | $0.64 | $15.56 | — | $4,212.03 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-22.0; leftover $1059.53 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FJET` | 373 | $2.84 | $4.81 | — | $3,147.90 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-26.9; leftover $1059.53 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `OSW` | 48 | $22.00 | $2.13 | — | $2,089.76 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-17.3; leftover $1059.53 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `PL` | 53 | $19.86 | $2.15 | — | $1,035.03 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list overnight; ret5=-5.5; leftover $1059.53 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟡 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SWBI` | 80 | $12.78 | $2.23 | — | $10.40 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list overnight; 🔵; ret5=-4.4; leftover $1059.53 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10.40 | ▼ close $8,285.58 vs 09:30 $8,476.22 (session -144.59) | 16:00 close · cash $10.40 · equity $8,285.58 vs 09:30 $8,476.22 (-190.64; session marks -144.59) · 8 name(s) marked open→close (per-name table). SION×144 09:30 $7.31 → close $6.75 -80.64; ALMS×102 09:30 $10.38 → close $11.36 +100.47; LX×1281 09:30 $0.83 → close $0.85 +32.03; EVTL×1655 09:30 $0.64 → close $0.60 -66.20; FJET×373 09:30 $2.84 → close $2.78 -22.38; OSW×48 09:30 $22.00 → close $22.27 +12.96; PL×53 09:30 $19.86 → close $18.35 -80.03; SWBI×80 09:30 $12.78 → close $12.27 -40.80 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10.40 | ▲ 09:30 equity $8,493.76 vs yday $8,285.58 (+208.18) | 09:30 open · cash $10.40 (unchanged overnight, no fees) · equity $8,493.76 vs prior close $8,285.58 (+208.18) · 8 name(s) re-marked at the open (per-name table). SION×144 yday $6.75 → 09:30 $6.68 -10.08; ALMS×102 yday $11.36 → 09:30 $11.23 -13.26; LX×1281 yday $0.85 → 09:30 $0.86 +7.69; EVTL×1655 yday $0.60 → 09:30 $0.60 +0.00; FJET×373 yday $2.78 → 09:30 $2.80 +7.46; OSW×48 yday $22.27 → 09:30 $22.27 +0.00; PL×53 yday $18.35 → 09:30 $19.64 +68.37; SWBI×80 yday $12.27 → 09:30 $14.12 +148.00 | — |
| 2026-09-04 09:30 ET | **SELL** | `ALMS` | 102 | $11.23 | $2.32 | $+82.59 | $1,153.54 | ▲ +82.59 after sell → book $8,491.44; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `LX` | 1281 | $0.86 | $15.06 | $+10.22 | $2,237.58 | ▲ +10.22 after sell → book $8,476.38; vs 09:30 mark -15.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `EVTL` | 1655 | $0.60 | $15.18 | $-96.94 | $3,215.40 | ▼ -96.94 after sell → book $8,461.20; vs 09:30 mark -15.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `FJET` | 373 | $2.80 | $4.88 | $-24.62 | $4,254.92 | ▼ -24.62 after sell → book $8,456.32; vs 09:30 mark -4.88 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `OSW` | 48 | $22.27 | $2.15 | $+8.67 | $5,321.72 | ▲ +8.67 after sell → book $8,454.16; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `PL` | 53 | $19.64 | $2.17 | $-15.98 | $6,360.47 | ▼ -15.98 after sell → book $8,451.99; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `AIIO` | 3607 | $1.75 | $46.53 | — | $1.69 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-24.6; leftover $6360.47 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.69 | ▲ close $8,703.69 vs 09:30 $8,493.76 (session +298.23) | 16:00 close · cash $1.69 · equity $8,703.69 vs 09:30 $8,493.76 (+209.93; session marks +298.23) · 3 name(s) marked open→close (per-name table). SION×144 09:30 $6.68 → close $7.18 +72.00; SWBI×80 09:30 $14.12 → close $12.89 -98.40; AIIO×3607 09:30 $1.75 → close $1.84 +324.63 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.69 | ▼ 09:30 equity $8,557.88 vs yday $8,703.69 (-145.81) | 09:30 open · cash $1.69 (unchanged overnight, no fees) · equity $8,557.88 vs prior close $8,703.69 (-145.81) · 3 name(s) re-marked at the open (per-name table). SION×144 yday $7.18 → 09:30 $7.13 -7.20; SWBI×80 yday $12.89 → 09:30 $12.51 -30.40; AIIO×3607 yday $1.84 → 09:30 $1.81 -108.21 | — |
| 2026-09-08 09:30 ET | **SELL** | `SION` | 144 | $7.13 | $2.46 | $-30.80 | $1,025.96 | ▼ -30.80 after sell → book $8,555.43; vs 09:30 mark -2.45 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `SWBI` | 80 | $12.51 | $2.25 | $-26.08 | $2,024.50 | ▼ -26.08 after sell → book $8,553.17; vs 09:30 mark -2.26 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `AIIO` | 3607 | $1.81 | $47.18 | $+122.71 | $8,505.99 | ▲ +122.71 after sell → book $8,505.99; vs 09:30 mark -47.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,505.99 | ▲ close $8,505.99 vs 09:30 $8,557.88 (session +0.00) | 16:00 close · cash $8,505.99 · no lots left · equity $8,505.99. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,505.99 | ▲ 09:30 equity $8,505.99 vs yday $8,505.99 (+0.00) | 09:30 open · cash $8,505.99 · no holdings · equity $8,505.99 vs prior close $8,505.99 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,505.99 | ▲ close $8,505.99 vs 09:30 $8,505.99 (session +0.00) | 16:00 close · cash $8,505.99 · no lots left · equity $8,505.99. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,505.99 | ▲ 09:30 equity $8,505.99 vs yday $8,505.99 (+0.00) | 09:30 open · cash $8,505.99 · no holdings · equity $8,505.99 vs prior close $8,505.99 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,505.99 | ▲ close $8,505.99 vs 09:30 $8,505.99 (session +0.00) | 16:00 close · cash $8,505.99 · no lots left · equity $8,505.99. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,505.99 | ▲ 09:30 equity $8,505.99 vs yday $8,505.99 (+0.00) | 09:30 open · cash $8,505.99 · no holdings · equity $8,505.99 vs prior close $8,505.99 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `NAVN` | 137 | $20.61 | $2.40 | — | $5,680.02 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-24.7; leftover $2835.33 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `RWT` | 805 | $3.52 | $10.38 | — | $2,836.04 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-19.2; leftover $2835.33 | join🔴 sector🔴 gen🟡 news🔴 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `COO` | 51 | $54.66 | $2.14 | — | $46.23 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-22.3; leftover $2835.33 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $46.23 | ▲ close $8,533.13 vs 09:30 $8,505.99 (session +42.07) | 16:00 close · cash $46.23 · equity $8,533.13 vs 09:30 $8,505.99 (+27.14; session marks +42.07) · 3 name(s) marked open→close (per-name table). NAVN×137 09:30 $20.61 → close $21.02 +56.17; RWT×805 09:30 $3.52 → close $3.55 +24.15; COO×51 09:30 $54.66 → close $53.91 -38.25 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $46.23 | ▲ 09:30 equity $8,572.36 vs yday $8,533.13 (+39.23) | 09:30 open · cash $46.23 (unchanged overnight, no fees) · equity $8,572.36 vs prior close $8,533.13 (+39.23) · 3 name(s) re-marked at the open (per-name table). NAVN×137 yday $21.02 → 09:30 $21.10 +10.96; RWT×805 yday $3.55 → 09:30 $3.53 -16.10; COO×51 yday $53.91 → 09:30 $54.78 +44.37 | — |
| 2026-09-14 09:30 ET | **SELL** | `NAVN` | 137 | $21.10 | $2.45 | $+62.28 | $2,934.49 | ▲ +62.28 after sell → book $8,569.92; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `RWT` | 805 | $3.53 | $10.54 | $-12.88 | $5,765.60 | ▼ -12.88 after sell → book $8,559.38; vs 09:30 mark -10.54 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `COO` | 51 | $54.78 | $2.18 | $+1.80 | $8,557.20 | ▲ +1.80 after sell → book $8,557.20; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,557.20 | ▲ close $8,557.20 vs 09:30 $8,572.36 (session +0.00) | 16:00 close · cash $8,557.20 · no lots left · equity $8,557.20. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,557.20 | ▲ 09:30 equity $8,557.20 vs yday $8,557.20 (+0.00) | 09:30 open · cash $8,557.20 · no holdings · equity $8,557.20 vs prior close $8,557.20 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,557.20 | ▲ close $8,557.20 vs 09:30 $8,557.20 (session +0.00) | 16:00 close · cash $8,557.20 · no lots left · equity $8,557.20. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,557.20 | ▲ 09:30 equity $8,557.20 vs yday $8,557.20 (+0.00) | 09:30 open · cash $8,557.20 · no holdings · equity $8,557.20 vs prior close $8,557.20 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 09:30 ET | **BUY** | `ALHC` | 103 | $10.30 | $2.30 | — | $7,494.00 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-23.0; leftover $1069.65 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `PLAY` | 155 | $6.86 | $2.46 | — | $6,428.25 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-22.4; leftover $1069.65 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `DVLT` | 6685 | $0.16 | $30.75 | — | $5,327.90 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-23.8; leftover $1069.65 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `NMRA` | 1267 | $0.84 | $14.49 | — | $4,244.05 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-33.5; leftover $1069.65 | join🔴 sector🟢 gen🟢 news🟡 digest🔴 ab🔴 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `ZSQR` | 457 | $2.34 | $5.90 | — | $3,168.78 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-25.2; leftover $1069.65 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `CTMX` | 393 | $2.72 | $5.07 | — | $2,094.75 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-26.3; leftover $1069.65 | join🔴 sector🟢 gen🟢 news🟡 digest🔴 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `CRBP` | 155 | $6.86 | $2.46 | — | $1,028.99 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-34.7; leftover $1069.65 | join🔴 sector🟢 gen🟢 news🟡 digest🔴 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-16 09:30 ET | **BUY** | `EYPT` | 280 | $3.66 | $3.61 | — | $0.58 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-19.7; leftover $1069.65 | join🟢 sector🟢 gen🟢 news🟡 digest🔴 ab🔴 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.58 | ▼ close $8,368.18 vs 09:30 $8,557.20 (session -122.00) | 16:00 close · cash $0.58 · equity $8,368.18 vs 09:30 $8,557.20 (-189.02; session marks -122.00) · 8 name(s) marked open→close (per-name table). ALHC×103 09:30 $10.30 → close $8.71 -163.77; PLAY×155 09:30 $6.86 → close $6.86 +0.00; DVLT×6685 09:30 $0.16 → close $0.18 +133.70; NMRA×1267 09:30 $0.84 → close $0.77 -89.96; ZSQR×457 09:30 $2.34 → close $2.29 -22.85; CTMX×393 09:30 $2.72 → close $2.77 +17.68; CRBP×155 09:30 $6.86 → close $7.26 +62.00; EYPT×280 09:30 $3.66 → close $3.45 -58.80 | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.58 | ▲ 09:30 equity $8,398.87 vs yday $8,368.18 (+30.69) | 09:30 open · cash $0.58 (unchanged overnight, no fees) · equity $8,398.87 vs prior close $8,368.18 (+30.69) · 8 name(s) re-marked at the open (per-name table). ALHC×103 yday $8.71 → 09:30 $8.58 -13.39; PLAY×155 yday $6.86 → 09:30 $6.96 +15.50; DVLT×6685 yday $0.18 → 09:30 $0.17 -66.85; NMRA×1267 yday $0.77 → 09:30 $0.78 +8.87; ZSQR×457 yday $2.29 → 09:30 $2.35 +27.42; CTMX×393 yday $2.77 → 09:30 $2.83 +25.54; CRBP×155 yday $7.26 → 09:30 $7.26 +0.00; EYPT×280 yday $3.45 → 09:30 $3.57 +33.60 | — |
| 2026-09-17 09:30 ET | **SELL** | `PLAY` | 155 | $6.96 | $2.49 | $+10.55 | $1,076.89 | ▲ +10.55 after sell → book $8,396.38; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `DVLT` | 6685 | $0.17 | $32.54 | $+3.56 | $2,180.80 | ▲ +3.56 after sell → book $8,363.84; vs 09:30 mark -32.54 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-17 09:30 ET | **SELL** | `NMRA` | 1267 | $0.78 | $13.90 | $-109.49 | $3,155.16 | ▼ -109.49 after sell → book $8,349.94; vs 09:30 mark -13.90 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `ZSQR` | 457 | $2.35 | $5.98 | $-7.31 | $4,223.13 | ▼ -7.31 after sell → book $8,343.96; vs 09:30 mark -5.98 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `CTMX` | 393 | $2.83 | $5.14 | $+33.02 | $5,330.17 | ▲ +33.02 after sell → book $8,338.81; vs 09:30 mark -5.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `CRBP` | 155 | $7.26 | $2.49 | $+57.05 | $6,452.98 | ▲ +57.05 after sell → book $8,336.32; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `EYPT` | 280 | $3.57 | $3.67 | $-32.48 | $7,448.91 | ▼ -32.48 after sell → book $8,332.65; vs 09:30 mark -3.67 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `MRLN` | 3262 | $2.27 | $42.08 | — | $2.09 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-29.6; leftover $7448.91 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.09 | ▼ close $7,617.91 vs 09:30 $8,398.87 (session -672.66) | 16:00 close · cash $2.09 · equity $7,617.91 vs 09:30 $8,398.87 (-780.96; session marks -672.66) · 2 name(s) marked open→close (per-name table). ALHC×103 09:30 $8.58 → close $8.70 +12.36; MRLN×3262 09:30 $2.27 → close $2.06 -685.02 | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.09 | ▲ 09:30 equity $7,648.47 vs yday $7,617.91 (+30.56) | 09:30 open · cash $2.09 (unchanged overnight, no fees) · equity $7,648.47 vs prior close $7,617.91 (+30.56) · 2 name(s) re-marked at the open (per-name table). ALHC×103 yday $8.70 → 09:30 $8.68 -2.06; MRLN×3262 yday $2.06 → 09:30 $2.07 +32.62 | — |
| 2026-09-18 09:30 ET | **SELL** | `ALHC` | 103 | $8.68 | $2.33 | $-171.49 | $893.81 | ▼ -171.49 after sell → book $7,646.15; vs 09:30 mark -2.32 | dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `MRLN` | 3262 | $2.07 | $42.68 | $-737.16 | $7,603.47 | ▼ -737.16 after sell → book $7,603.47; vs 09:30 mark -42.68 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 257 | $14.79 | $3.32 | — | $3,799.13 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $3801.74 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-18 09:30 ET | **BUY** | `FLNC` | 503 | $7.54 | $6.49 | — | $2.53 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-20.9; leftover $3801.74 | join🟡 sector🔴 gen🟢 news🟢 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.53 | ▼ close $7,413.56 vs 09:30 $7,648.47 (session -180.10) | 16:00 close · cash $2.53 · equity $7,413.56 vs 09:30 $7,648.47 (-234.91; session marks -180.10) · 2 name(s) marked open→close (per-name table). RARE×257 09:30 $14.79 → close $14.51 -71.96; FLNC×503 09:30 $7.54 → close $7.32 -108.14 | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.53 | ▲ 09:30 equity $7,451.67 vs yday $7,413.56 (+38.11) | 09:30 open · cash $2.53 (unchanged overnight, no fees) · equity $7,451.67 vs prior close $7,413.56 (+38.11) · 2 name(s) re-marked at the open (per-name table). RARE×257 yday $14.51 → 09:30 $14.58 +17.99; FLNC×503 yday $7.32 → 09:30 $7.36 +20.12 | — |
| 2026-09-21 09:30 ET | **SELL** | `RARE` | 257 | $14.58 | $3.39 | $-60.67 | $3,746.20 | ▼ -60.67 after sell → book $7,448.28; vs 09:30 mark -3.39 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `FLNC` | 503 | $7.36 | $6.60 | $-101.12 | $7,441.68 | ▼ -101.12 after sell → book $7,441.68; vs 09:30 mark -6.60 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `XENE` | 62 | $40.00 | $2.18 | — | $4,959.51 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-32.2; leftover $2480.56 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **BUY** | `SION` | 413 | $6.00 | $5.33 | — | $2,476.18 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-24.1; leftover $2480.56 | join🟢 sector🟢 gen🟢 news🟢 digest🟡 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-21 09:30 ET | **BUY** | `THO` | 36 | $68.39 | $2.10 | — | $12.04 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list overnight; ret5=-7.0; leftover $2480.56 | join🟢 sector🔴 gen🟢 news🟡 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12.04 | ▼ close $7,425.26 vs 09:30 $7,451.67 (session -6.82) | 16:00 close · cash $12.04 · equity $7,425.26 vs 09:30 $7,451.67 (-26.41; session marks -6.82) · 3 name(s) marked open→close (per-name table). XENE×62 09:30 $40.00 → close $38.99 -62.62; SION×413 09:30 $6.00 → close $6.00 +0.00; THO×36 09:30 $68.39 → close $69.94 +55.80 | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12.04 | ▲ 09:30 equity $7,453.15 vs yday $7,425.26 (+27.89) | 09:30 open · cash $12.04 (unchanged overnight, no fees) · equity $7,453.15 vs prior close $7,425.26 (+27.89) · 3 name(s) re-marked at the open (per-name table). XENE×62 yday $38.99 → 09:30 $39.10 +6.82; SION×413 yday $6.00 → 09:30 $5.99 -4.13; THO×36 yday $69.94 → 09:30 $70.64 +25.20 | — |
| 2026-09-22 09:30 ET | **SELL** | `XENE` | 62 | $39.10 | $2.21 | $-60.18 | $2,434.04 | ▼ -60.18 after sell → book $7,450.95; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `SION` | 413 | $5.99 | $5.42 | $-14.87 | $4,902.49 | ▼ -14.87 after sell → book $7,445.53; vs 09:30 mark -5.42 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `THO` | 36 | $70.64 | $2.13 | $+76.77 | $7,443.40 | ▲ +76.77 after sell → book $7,443.40; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 ab🟢 peer🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-22 09:30 ET | **BUY** | `FJET` | 3643 | $2.03 | $46.99 | — | $1.12 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list probable,yday_gainer,yday_mover; ret5=+0.5; leftover $7443.40 | join🔴 sector🔴 gen🔴 news🟡 digest🟢 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.12 | ▼ close $7,359.98 vs 09:30 $7,453.15 (session -36.43) | 16:00 close · cash $1.12 · equity $7,359.98 vs 09:30 $7,453.15 (-93.17; session marks -36.43) · 1 name(s) marked open→close (per-name table). FJET×3643 09:30 $2.03 → close $2.02 -36.43 | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.12 | ▼ 09:30 equity $7,214.26 vs yday $7,359.98 (-145.72) | 09:30 open · cash $1.12 (unchanged overnight, no fees) · equity $7,214.26 vs prior close $7,359.98 (-145.72) · 1 name(s) re-marked at the open (per-name table). FJET×3643 yday $2.02 → 09:30 $1.98 -145.72 | — |
| 2026-09-23 09:30 ET | **SELL** | `FJET` | 3643 | $1.98 | $47.66 | $-276.80 | $7,166.60 | ▼ -276.80 after sell → book $7,166.60; vs 09:30 mark -47.66 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `NMRA` | 2357 | $0.76 | $24.98 | — | $5,350.30 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_gainer,yday_mover; 🔵; ret5=-6.3; leftover $1791.65 | join🟢 sector🟢 gen🟢 news🟡 digest🟡 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-23 09:30 ET | **BUY** | `CMPX` | 1468 | $1.22 | $18.94 | — | $3,540.40 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-15.6; leftover $1791.65 | join🔴 sector🟢 gen🟢 news🟢 digest🟡 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-23 09:30 ET | **BUY** | `XNDU` | 299 | $5.99 | $3.86 | — | $1,745.53 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-10.9; leftover $1791.65 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 judge🟡 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-23 09:30 ET | **BUY** | `EVER` | 89 | $19.46 | $2.26 | — | $11.33 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-2.0; leftover $1791.65 | join🔴 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11.33 | ▼ close $6,615.31 vs 09:30 $7,214.26 (session -501.26) | 16:00 close · cash $11.33 · equity $6,615.31 vs 09:30 $7,214.26 (-598.95; session marks -501.26) · 4 name(s) marked open→close (per-name table). NMRA×2357 09:30 $0.76 → close $0.73 -67.65; CMPX×1468 09:30 $1.22 → close $1.17 -73.40; XNDU×299 09:30 $5.99 → close $5.33 -197.34; EVER×89 09:30 $19.46 → close $17.63 -162.87 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `EYPT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CIG` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `BIDU` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-26 | `DKS` | cash | leftover split 3.62 < 1 share @ 121.87 |
| 2026-08-27 | `DKS` | cash | leftover split 3.62 < 1 share @ 128.73 |
| 2026-09-08 | `SUNB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `DYN` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `NAMS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `TTAN` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `FJET` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `OCGN` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `XPOF` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `COO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-15 | `TCOM` | hard_red | hard-red S=-3.84 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `NMRA` | 2357 | 2026-09-23 @ $0.76 | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_gainer,yday_mover; 🔵; ret5=-6.3; leftover $1791.65 |
| `CMPX` | 1468 | 2026-09-23 @ $1.22 | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-15.6; leftover $1791.65 |
| `XNDU` | 299 | 2026-09-23 @ $5.99 | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-10.9; leftover $1791.65 |
| `EVER` | 89 | 2026-09-23 @ $19.46 | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-2.0; leftover $1791.65 |
