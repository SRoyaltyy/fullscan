# Factor mine action — `union_rsi_os_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ rsi_os, no 🚨

Cash book **-6.53%** ($9,347) · signal-only (no cash/fees) was -19.79%. Starts YES **13/24**. Fills 38 · skips 54 · realized $-652.93.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- Minimum hold is 3 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 3 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `rsi_os=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,347.07.

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
| 2026-08-17 | `NCMI` | 929 | $2.86 | $2.80 | -55.74 | $2.73 | -65.03 | -120.77 | +102.19 | +37.16 |
| 2026-08-17 | `QMLS` | 342 | $7.32 | $7.24 | -27.36 | $7.14 | -34.20 | -61.56 | -17.10 | -51.30 |
| 2026-08-17 | `CLBT` | 230 | $11.14 | $11.19 | +11.50 | $10.44 | -172.50 | -161.00 | +82.80 | -89.70 |
| 2026-08-17 | `YSS` | 247 | $10.93 | $10.36 | -140.79 | $10.66 | +74.10 | -66.69 | +74.10 | +148.20 |
| 2026-08-17 | `INV` | 1 | — | $1.62 | +0.00 | $1.39 | -0.24 | -0.24 | +0.00 | -0.24 |
| 2026-08-18 | `NCMI` | 929 | $2.73 | $2.71 | -18.58 | $2.52 | -176.51 | -195.09 | +18.58 | -157.93 |
| 2026-08-18 | `QMLS` | 342 | $7.14 | $6.85 | -99.18 | $6.74 | -37.62 | -136.80 | -150.48 | -188.10 |
| 2026-08-18 | `CLBT` | 230 | $10.44 | $10.44 | +0.00 | $11.00 | +128.80 | +128.80 | -89.70 | +39.10 |
| 2026-08-18 | `YSS` | 247 | $10.66 | $10.24 | -103.74 | $10.42 | +44.46 | -59.28 | +44.46 | +88.92 |
| 2026-08-18 | `INV` | 1 | $1.39 | $1.32 | -0.06 | $1.32 | +0.00 | -0.06 | -0.30 | -0.30 |
| 2026-08-19 | `NCMI` | 929 | $2.52 | $2.56 | +37.16 | — | +0.00 | +37.16 | -120.77 | — |
| 2026-08-19 | `QMLS` | 342 | $6.74 | $6.74 | +0.00 | — | +0.00 | +0.00 | -188.10 | — |
| 2026-08-19 | `CLBT` | 230 | $11.00 | $10.85 | -34.50 | — | +0.00 | -34.50 | +4.60 | — |
| 2026-08-19 | `YSS` | 247 | $10.42 | $10.32 | -24.70 | — | +0.00 | -24.70 | +64.22 | — |
| 2026-08-19 | `INV` | 1 | $1.32 | $1.39 | +0.06 | $1.54 | +0.15 | +0.21 | -0.23 | -0.08 |
| 2026-08-20 | `INV` | 1 | $1.54 | $1.55 | +0.01 | — | +0.00 | +0.01 | -0.07 | — |
| 2026-08-20 | `LZB` | 288 | — | $33.61 | +0.00 | $33.65 | +11.52 | +11.52 | +0.00 | +11.52 |
| 2026-08-21 | `LZB` | 288 | $33.65 | $33.63 | -5.76 | $33.71 | +23.04 | +17.28 | +5.76 | +28.80 |
| 2026-08-24 | `LZB` | 288 | $33.71 | $33.55 | -46.08 | $32.12 | -411.84 | -457.92 | -17.28 | -429.12 |
| 2026-08-25 | `LZB` | 288 | $32.12 | $32.33 | +60.48 | — | +0.00 | +60.48 | -368.64 | — |
| 2026-08-26 | `DKS` | 25 | — | $121.87 | +0.00 | $129.66 | +194.75 | +194.75 | +0.00 | +194.75 |
| 2026-08-26 | `QFIN` | 318 | — | $9.76 | +0.00 | $9.35 | -130.38 | -130.38 | +0.00 | -130.38 |
| 2026-08-26 | `QMLS` | 481 | — | $6.47 | +0.00 | $6.10 | -177.97 | -177.97 | +0.00 | -177.97 |
| 2026-08-27 | `DKS` | 25 | $129.66 | $128.73 | -23.25 | $131.77 | +76.00 | +52.75 | +171.50 | +247.50 |
| 2026-08-27 | `QFIN` | 318 | $9.35 | $9.42 | +22.26 | $9.17 | -79.50 | -57.24 | -108.12 | -187.62 |
| 2026-08-27 | `QMLS` | 481 | $6.10 | $6.33 | +110.63 | $6.47 | +67.34 | +177.97 | -67.34 | +0.00 |
| 2026-08-28 | `DKS` | 25 | $131.77 | $132.80 | +25.75 | $135.09 | +57.25 | +83.00 | +273.25 | +330.50 |
| 2026-08-28 | `QFIN` | 318 | $9.17 | $9.15 | -6.36 | $8.80 | -111.30 | -117.66 | -193.98 | -305.28 |
| 2026-08-28 | `QMLS` | 481 | $6.47 | $6.27 | -96.20 | $6.11 | -76.96 | -173.16 | -96.20 | -173.16 |
| 2026-08-31 | `DKS` | 25 | $135.09 | $136.75 | +41.50 | — | +0.00 | +41.50 | +372.00 | — |
| 2026-08-31 | `QFIN` | 318 | $8.80 | $8.70 | -31.80 | — | +0.00 | -31.80 | -337.08 | — |
| 2026-08-31 | `QMLS` | 481 | $6.11 | $5.95 | -76.96 | — | +0.00 | -76.96 | -250.12 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `SION` | 207 | — | $7.31 | +0.00 | $6.75 | -115.92 | -115.92 | +0.00 | -115.92 |
| 2026-09-03 | `ALMS` | 146 | — | $10.38 | +0.00 | $11.36 | +143.81 | +143.81 | +0.00 | +143.81 |
| 2026-09-03 | `LX` | 1833 | — | $0.83 | +0.00 | $0.85 | +45.83 | +45.83 | +0.00 | +45.83 |
| 2026-09-03 | `EVTL` | 2369 | — | $0.64 | +0.00 | $0.60 | -94.76 | -94.76 | +0.00 | -94.76 |
| 2026-09-03 | `FJET` | 533 | — | $2.84 | +0.00 | $2.78 | -31.98 | -31.98 | +0.00 | -31.98 |
| 2026-09-03 | `OSW` | 66 | — | $22.00 | +0.00 | $22.27 | +17.82 | +17.82 | +0.00 | +17.82 |
| 2026-09-04 | `SION` | 207 | $6.75 | $6.68 | -14.49 | $7.18 | +103.50 | +89.01 | -130.41 | -26.91 |
| 2026-09-04 | `ALMS` | 146 | $11.36 | $11.23 | -18.98 | $11.10 | -18.98 | -37.96 | +124.83 | +105.85 |
| 2026-09-04 | `LX` | 1833 | $0.85 | $0.86 | +11.00 | $0.88 | +43.99 | +54.99 | +56.82 | +100.82 |
| 2026-09-04 | `EVTL` | 2369 | $0.60 | $0.60 | +0.00 | $0.60 | -4.74 | -4.74 | -94.76 | -99.50 |
| 2026-09-04 | `FJET` | 533 | $2.78 | $2.80 | +10.66 | $2.80 | +0.00 | +10.66 | -21.32 | -21.32 |
| 2026-09-04 | `OSW` | 66 | $22.27 | $22.27 | +0.00 | $22.40 | +8.58 | +8.58 | +17.82 | +26.40 |
| 2026-09-04 | `AIIO` | 4 | — | $1.75 | +0.00 | $1.84 | +0.36 | +0.36 | +0.00 | +0.36 |
| 2026-09-08 | `SION` | 207 | $7.18 | $7.13 | -10.35 | $7.30 | +35.19 | +24.84 | -37.26 | -2.07 |
| 2026-09-08 | `ALMS` | 146 | $11.10 | $11.05 | -7.30 | $10.58 | -68.62 | -75.92 | +98.55 | +29.93 |
| 2026-09-08 | `LX` | 1833 | $0.88 | $0.90 | +32.99 | $0.85 | -86.15 | -53.16 | +133.81 | +47.66 |
| 2026-09-08 | `EVTL` | 2369 | $0.60 | $0.60 | +0.00 | $0.59 | -18.95 | -18.95 | -99.50 | -118.45 |
| 2026-09-08 | `FJET` | 533 | $2.80 | $2.80 | +0.00 | $2.63 | -90.61 | -90.61 | -21.32 | -111.93 |
| 2026-09-08 | `OSW` | 66 | $22.40 | $22.29 | -7.26 | $22.12 | -11.22 | -18.48 | +19.14 | +7.92 |
| 2026-09-08 | `AIIO` | 4 | $1.84 | $1.81 | -0.12 | $1.95 | +0.56 | +0.44 | +0.24 | +0.80 |
| 2026-09-09 | `SION` | 207 | $7.30 | $7.27 | -6.21 | — | +0.00 | -6.21 | -8.28 | — |
| 2026-09-09 | `ALMS` | 146 | $10.58 | $10.49 | -13.14 | — | +0.00 | -13.14 | +16.79 | — |
| 2026-09-09 | `LX` | 1833 | $0.85 | $0.83 | -32.99 | — | +0.00 | -32.99 | +14.66 | — |
| 2026-09-09 | `EVTL` | 2369 | $0.59 | $0.59 | +9.48 | — | +0.00 | +9.48 | -108.97 | — |
| 2026-09-09 | `FJET` | 533 | $2.63 | $2.63 | +0.00 | — | +0.00 | +0.00 | -111.93 | — |
| 2026-09-09 | `OSW` | 66 | $22.12 | $21.86 | -17.16 | — | +0.00 | -17.16 | -9.24 | — |
| 2026-09-09 | `AIIO` | 4 | $1.95 | $1.93 | -0.08 | $1.86 | -0.28 | -0.36 | +0.72 | +0.44 |
| 2026-09-10 | `AIIO` | 4 | $1.86 | $1.81 | -0.20 | — | +0.00 | -0.20 | +0.24 | — |
| 2026-09-11 | `NAVN` | 141 | — | $20.61 | +0.00 | $21.02 | +57.81 | +57.81 | +0.00 | +57.81 |
| 2026-09-11 | `RWT` | 831 | — | $3.52 | +0.00 | $3.55 | +24.93 | +24.93 | +0.00 | +24.93 |
| 2026-09-11 | `COO` | 53 | — | $54.66 | +0.00 | $53.91 | -39.75 | -39.75 | +0.00 | -39.75 |
| 2026-09-14 | `NAVN` | 141 | $21.02 | $21.10 | +11.28 | $21.37 | +38.07 | +49.35 | +69.09 | +107.16 |
| 2026-09-14 | `RWT` | 831 | $3.55 | $3.53 | -16.62 | $3.83 | +249.30 | +232.68 | +8.31 | +257.61 |
| 2026-09-14 | `COO` | 53 | $53.91 | $54.78 | +46.11 | $54.22 | -29.68 | +16.43 | +6.36 | -23.32 |
| 2026-09-15 | `NAVN` | 141 | $21.37 | $21.32 | -7.05 | $22.42 | +155.10 | +148.05 | +100.11 | +255.21 |
| 2026-09-15 | `RWT` | 831 | $3.83 | $3.80 | -24.93 | $3.91 | +91.41 | +66.48 | +232.68 | +324.09 |
| 2026-09-15 | `COO` | 53 | $54.22 | $54.40 | +9.54 | $53.27 | -59.89 | -50.35 | -13.78 | -73.67 |
| 2026-09-16 | `NAVN` | 141 | $22.42 | $22.27 | -21.15 | — | +0.00 | -21.15 | +234.06 | — |
| 2026-09-16 | `RWT` | 831 | $3.91 | $3.98 | +58.17 | — | +0.00 | +58.17 | +382.26 | — |
| 2026-09-16 | `COO` | 53 | $53.27 | $54.37 | +58.30 | — | +0.00 | +58.30 | -15.37 | — |

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | +454.38 | NCMI, QMLS, CLBT, YSS | — | $9.54 | $10,431.83 | NCMI×929, QMLS×342, CLBT×230, YSS×247 |
| 2026-08-17 | +2.25 | $9.54 | NCMI×929, QMLS×342, CLBT×230, YSS×247 | $10,219.44 | -212.39 | -197.87 | INV | — | $7.90 | $10,021.56 | NCMI×929, QMLS×342, CLBT×230, YSS×247, INV×1 |
| 2026-08-18 | -6.20 | $7.90 | NCMI×929, QMLS×342, CLBT×230, YSS×247, INV×1 | $9,800.00 | -221.56 | -40.87 | — | — | $7.90 | $9,759.13 | NCMI×929, QMLS×342, CLBT×230, YSS×247, INV×1 |
| 2026-08-19 | -7.20 | $7.90 | NCMI×929, QMLS×342, CLBT×230, YSS×247, INV×1 | $9,737.15 | -21.98 | +0.15 | — | NCMI, QMLS, CLBT, YSS | $9,712.84 | $9,714.38 | INV×1 |
| 2026-08-20 | +1.12 | $9,712.84 | INV×1 | $9,714.39 | +0.01 | +11.52 | LZB | INV | $30.96 | $9,722.16 | LZB×288 |
| 2026-08-21 | +3.25 | $30.96 | LZB×288 | $9,716.40 | -5.76 | +23.04 | — | — | $30.96 | $9,739.44 | LZB×288 |
| 2026-08-24 | -5.17 | $30.96 | LZB×288 | $9,693.36 | -46.08 | -411.84 | — | — | $30.96 | $9,281.52 | LZB×288 |
| 2026-08-25 | +1.80 | $30.96 | LZB×288 | $9,342.00 | +60.48 | +0.00 | — | LZB | $9,338.16 | $9,338.16 | — |
| 2026-08-26 | +2.02 | $9,338.16 | — | $9,338.16 | +0.00 | -113.60 | DKS, QFIN, QMLS | — | $63.29 | $9,212.19 | DKS×25, QFIN×318, QMLS×481 |
| 2026-08-27 | — | $63.29 | DKS×25, QFIN×318, QMLS×481 | $9,321.83 | +109.64 | +63.84 | — | — | $63.29 | $9,385.67 | DKS×25, QFIN×318, QMLS×481 |
| 2026-08-28 | +0.75 | $63.29 | DKS×25, QFIN×318, QMLS×481 | $9,308.86 | -76.81 | -131.01 | — | — | $63.29 | $9,177.85 | DKS×25, QFIN×318, QMLS×481 |
| 2026-08-31 | -5.85 | $63.29 | DKS×25, QFIN×318, QMLS×481 | $9,110.59 | -67.26 | +0.00 | — | DKS, QFIN, QMLS | $9,098.00 | $9,098.00 | — |
| 2026-09-01 | -6.30 | $9,098.00 | — | $9,098.00 | +0.00 | +0.00 | — | — | $9,098.00 | $9,098.00 | — |
| 2026-09-02 | -3.83 | $9,098.00 | — | $9,098.00 | +0.00 | +0.00 | — | — | $9,098.00 | $9,098.00 | — |
| 2026-09-03 | -0.90 | $9,098.00 | — | $9,098.00 | +0.00 | -35.20 | SION, ALMS, LX, EVTL, FJET, OSW | — | $15.22 | $9,005.71 | SION×207, ALMS×146, LX×1833, EVTL×2369, FJET×533, OSW×66 |
| 2026-09-04 | +2.25 | $15.22 | SION×207, ALMS×146, LX×1833, EVTL×2369, FJET×533, OSW×66 | $8,993.90 | -11.81 | +132.71 | AIIO | — | $8.14 | $9,126.53 | SION×207, ALMS×146, LX×1833, EVTL×2369, FJET×533, OSW×66, AIIO×4 |
| 2026-09-08 | -11.47 | $8.14 | SION×207, ALMS×146, LX×1833, EVTL×2369, FJET×533, OSW×66, AIIO×4 | $9,134.49 | +7.96 | -239.80 | — | — | $8.14 | $8,894.69 | SION×207, ALMS×146, LX×1833, EVTL×2369, FJET×533, OSW×66, AIIO×4 |
| 2026-09-09 | -13.95 | $8.14 | SION×207, ALMS×146, LX×1833, EVTL×2369, FJET×533, OSW×66, AIIO×4 | $8,834.58 | -60.11 | -0.28 | — | SION, ALMS, LX, EVTL, FJET, OSW | $8,769.79 | $8,777.23 | AIIO×4 |
| 2026-09-10 | -13.28 | $8,769.79 | AIIO×4 | $8,777.03 | -0.20 | +0.00 | — | AIIO | $8,776.93 | $8,776.93 | — |
| 2026-09-11 | +0.50 | $8,776.93 | — | $8,776.93 | -0.00 | +42.99 | NAVN, RWT, COO | — | $33.54 | $8,804.64 | NAVN×141, RWT×831, COO×53 |
| 2026-09-14 | -11.00 | $33.54 | NAVN×141, RWT×831, COO×53 | $8,845.41 | +40.77 | +257.69 | — | — | $33.54 | $9,103.10 | NAVN×141, RWT×831, COO×53 |
| 2026-09-15 | -3.84 | $33.54 | NAVN×141, RWT×831, COO×53 | $9,080.66 | -22.44 | +186.62 | — | — | $33.54 | $9,267.28 | NAVN×141, RWT×831, COO×53 |
| 2026-09-16 | +5.30 | $33.54 | NAVN×141, RWT×831, COO×53 | $9,362.60 | +95.32 | +0.00 | — | NAVN, RWT, COO | $9,347.07 | $9,347.07 | — |

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
| 2026-08-17 09:30 ET | **BUY** | `INV` | 1 | $1.62 | $0.02 | — | $7.90 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-53.0; leftover $2.39 | join🟢 sector🟢 gen🟢 news🟡 judge🔴 vol🟢 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7.90 | ▼ close $10,021.56 vs 09:30 $10,219.44 (session -197.87) | 16:00 close · cash $7.90 · equity $10,021.56 vs 09:30 $10,219.44 (-197.88; session marks -197.87) · 5 name(s) marked open→close (per-name table). NCMI×929 09:30 $2.80 → close $2.73 -65.03; QMLS×342 09:30 $7.24 → close $7.14 -34.20; CLBT×230 09:30 $11.19 → close $10.44 -172.50; YSS×247 09:30 $10.36 → close $10.66 +74.10; INV×1 09:30 $1.62 → close $1.39 -0.24 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7.90 | ▼ 09:30 equity $9,800.00 vs yday $10,021.56 (-221.56) | 09:30 open · cash $7.90 (unchanged overnight, no fees) · equity $9,800.00 vs prior close $10,021.56 (-221.56) · 5 name(s) re-marked at the open (per-name table). NCMI×929 yday $2.73 → 09:30 $2.71 -18.58; QMLS×342 yday $7.14 → 09:30 $6.85 -99.18; CLBT×230 yday $10.44 → 09:30 $10.44 +0.00; YSS×247 yday $10.66 → 09:30 $10.24 -103.74; INV×1 yday $1.39 → 09:30 $1.32 -0.06 | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7.90 | ▼ close $9,759.13 vs 09:30 $9,800.00 (session -40.87) | 16:00 close · cash $7.90 · equity $9,759.13 vs 09:30 $9,800.00 (-40.87; session marks -40.87) · 5 name(s) marked open→close (per-name table). NCMI×929 09:30 $2.71 → close $2.52 -176.51; QMLS×342 09:30 $6.85 → close $6.74 -37.62; CLBT×230 09:30 $10.44 → close $11.00 +128.80; YSS×247 09:30 $10.24 → close $10.42 +44.46; INV×1 09:30 $1.32 → close $1.32 +0.00 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7.90 | ▼ 09:30 equity $9,737.15 vs yday $9,759.13 (-21.98) | 09:30 open · cash $7.90 (unchanged overnight, no fees) · equity $9,737.15 vs prior close $9,759.13 (-21.98) · 5 name(s) re-marked at the open (per-name table). NCMI×929 yday $2.52 → 09:30 $2.56 +37.16; QMLS×342 yday $6.74 → 09:30 $6.74 +0.00; CLBT×230 yday $11.00 → 09:30 $10.85 -34.50; YSS×247 yday $10.42 → 09:30 $10.32 -24.70; INV×1 yday $1.32 → 09:30 $1.39 +0.06 | — |
| 2026-08-19 09:30 ET | **SELL** | `NCMI` | 929 | $2.56 | $12.16 | $-144.91 | $2,373.98 | ▼ -144.91 after sell → book $9,724.99; vs 09:30 mark -12.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `QMLS` | 342 | $6.74 | $4.49 | $-197.00 | $4,674.58 | ▼ -197.00 after sell → book $9,720.51; vs 09:30 mark -4.48 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `CLBT` | 230 | $10.85 | $3.03 | $-1.39 | $7,167.05 | ▼ -1.39 after sell → book $9,717.48; vs 09:30 mark -3.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `YSS` | 247 | $10.32 | $3.25 | $+57.79 | $9,712.84 | ▲ +57.79 after sell → book $9,714.23; vs 09:30 mark -3.25 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,712.84 | ▲ close $9,714.38 vs 09:30 $9,737.15 (session +0.15) | 16:00 close · cash $9,712.84 · equity $9,714.38 vs 09:30 $9,737.15 (-22.77; session marks +0.15) · 1 name(s) marked open→close (per-name table). INV×1 09:30 $1.39 → close $1.54 +0.15 | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,712.84 | ▲ 09:30 equity $9,714.39 vs yday $9,714.38 (+0.01) | 09:30 open · cash $9,712.84 (unchanged overnight, no fees) · equity $9,714.39 vs prior close $9,714.38 (+0.01) · 1 name(s) re-marked at the open (per-name table). INV×1 yday $1.54 → 09:30 $1.55 +0.01 | — |
| 2026-08-20 09:30 ET | **SELL** | `INV` | 1 | $1.55 | $0.04 | $-0.13 | $9,714.36 | ▼ -0.13 after sell → book $9,714.36; vs 09:30 mark -0.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `LZB` | 288 | $33.61 | $3.72 | — | $30.96 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-17.4; leftover $9714.36 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $30.96 | ▲ close $9,722.16 vs 09:30 $9,714.39 (session +11.52) | 16:00 close · cash $30.96 · equity $9,722.16 vs 09:30 $9,714.39 (+7.77; session marks +11.52) · 1 name(s) marked open→close (per-name table). LZB×288 09:30 $33.61 → close $33.65 +11.52 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $30.96 | ▼ 09:30 equity $9,716.40 vs yday $9,722.16 (-5.76) | 09:30 open · cash $30.96 (unchanged overnight, no fees) · equity $9,716.40 vs prior close $9,722.16 (-5.76) · 1 name(s) re-marked at the open (per-name table). LZB×288 yday $33.65 → 09:30 $33.63 -5.76 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $30.96 | ▲ close $9,739.44 vs 09:30 $9,716.40 (session +23.04) | 16:00 close · cash $30.96 · equity $9,739.44 vs 09:30 $9,716.40 (+23.04; session marks +23.04) · 1 name(s) marked open→close (per-name table). LZB×288 09:30 $33.63 → close $33.71 +23.04 | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $30.96 | ▼ 09:30 equity $9,693.36 vs yday $9,739.44 (-46.08) | 09:30 open · cash $30.96 (unchanged overnight, no fees) · equity $9,693.36 vs prior close $9,739.44 (-46.08) · 1 name(s) re-marked at the open (per-name table). LZB×288 yday $33.71 → 09:30 $33.55 -46.08 | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $30.96 | ▼ close $9,281.52 vs 09:30 $9,693.36 (session -411.84) | 16:00 close · cash $30.96 · equity $9,281.52 vs 09:30 $9,693.36 (-411.84; session marks -411.84) · 1 name(s) marked open→close (per-name table). LZB×288 09:30 $33.55 → close $32.12 -411.84 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $30.96 | ▲ 09:30 equity $9,342.00 vs yday $9,281.52 (+60.48) | 09:30 open · cash $30.96 (unchanged overnight, no fees) · equity $9,342.00 vs prior close $9,281.52 (+60.48) · 1 name(s) re-marked at the open (per-name table). LZB×288 yday $32.12 → 09:30 $32.33 +60.48 | — |
| 2026-08-25 09:30 ET | **SELL** | `LZB` | 288 | $32.33 | $3.84 | $-376.19 | $9,338.16 | ▼ -376.19 after sell → book $9,338.16; vs 09:30 mark -3.84 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,338.16 | ▲ close $9,338.16 vs 09:30 $9,342.00 (session +0.00) | 16:00 close · cash $9,338.16 · no lots left · equity $9,338.16. | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,338.16 | ▲ 09:30 equity $9,338.16 vs yday $9,338.16 (+0.00) | 09:30 open · cash $9,338.16 · no holdings · equity $9,338.16 vs prior close $9,338.16 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-26 09:30 ET | **BUY** | `DKS` | 25 | $121.87 | $2.06 | — | $6,289.35 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-35.1; leftover $3112.72 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `QFIN` | 318 | $9.76 | $4.10 | — | $3,181.57 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list earn_react; 🔵; ret5=-4.4; leftover $3112.72 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `QMLS` | 481 | $6.47 | $6.20 | — | $63.29 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list earn_react; 🔵; ret5=-7.0; leftover $3112.72 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $63.29 | ▼ close $9,212.19 vs 09:30 $9,338.16 (session -113.60) | 16:00 close · cash $63.29 · equity $9,212.19 vs 09:30 $9,338.16 (-125.97; session marks -113.60) · 3 name(s) marked open→close (per-name table). DKS×25 09:30 $121.87 → close $129.66 +194.75; QFIN×318 09:30 $9.76 → close $9.35 -130.38; QMLS×481 09:30 $6.47 → close $6.10 -177.97 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $63.29 | ▲ 09:30 equity $9,321.83 vs yday $9,212.19 (+109.64) | 09:30 open · cash $63.29 (unchanged overnight, no fees) · equity $9,321.83 vs prior close $9,212.19 (+109.64) · 3 name(s) re-marked at the open (per-name table). DKS×25 yday $129.66 → 09:30 $128.73 -23.25; QFIN×318 yday $9.35 → 09:30 $9.42 +22.26; QMLS×481 yday $6.10 → 09:30 $6.33 +110.63 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $63.29 | ▲ close $9,385.67 vs 09:30 $9,321.83 (session +63.84) | 16:00 close · cash $63.29 · equity $9,385.67 vs 09:30 $9,321.83 (+63.84; session marks +63.84) · 3 name(s) marked open→close (per-name table). DKS×25 09:30 $128.73 → close $131.77 +76.00; QFIN×318 09:30 $9.42 → close $9.17 -79.50; QMLS×481 09:30 $6.33 → close $6.47 +67.34 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $63.29 | ▼ 09:30 equity $9,308.86 vs yday $9,385.67 (-76.81) | 09:30 open · cash $63.29 (unchanged overnight, no fees) · equity $9,308.86 vs prior close $9,385.67 (-76.81) · 3 name(s) re-marked at the open (per-name table). DKS×25 yday $131.77 → 09:30 $132.80 +25.75; QFIN×318 yday $9.17 → 09:30 $9.15 -6.36; QMLS×481 yday $6.47 → 09:30 $6.27 -96.20 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $63.29 | ▼ close $9,177.85 vs 09:30 $9,308.86 (session -131.01) | 16:00 close · cash $63.29 · equity $9,177.85 vs 09:30 $9,308.86 (-131.01; session marks -131.01) · 3 name(s) marked open→close (per-name table). DKS×25 09:30 $132.80 → close $135.09 +57.25; QFIN×318 09:30 $9.15 → close $8.80 -111.30; QMLS×481 09:30 $6.27 → close $6.11 -76.96 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $63.29 | ▼ 09:30 equity $9,110.59 vs yday $9,177.85 (-67.26) | 09:30 open · cash $63.29 (unchanged overnight, no fees) · equity $9,110.59 vs prior close $9,177.85 (-67.26) · 3 name(s) re-marked at the open (per-name table). DKS×25 yday $135.09 → 09:30 $136.75 +41.50; QFIN×318 yday $8.80 → 09:30 $8.70 -31.80; QMLS×481 yday $6.11 → 09:30 $5.95 -76.96 | — |
| 2026-08-31 09:30 ET | **SELL** | `DKS` | 25 | $136.75 | $2.10 | $+367.83 | $3,479.94 | ▲ +367.83 after sell → book $9,108.49; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `QFIN` | 318 | $8.70 | $4.18 | $-345.36 | $6,242.36 | ▼ -345.36 after sell → book $9,104.31; vs 09:30 mark -4.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `QMLS` | 481 | $5.95 | $6.31 | $-262.63 | $9,098.00 | ▼ -262.63 after sell → book $9,098.00; vs 09:30 mark -6.31 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,098.00 | ▲ close $9,098.00 vs 09:30 $9,110.59 (session +0.00) | 16:00 close · cash $9,098.00 · no lots left · equity $9,098.00. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,098.00 | ▲ 09:30 equity $9,098.00 vs yday $9,098.00 (+0.00) | 09:30 open · cash $9,098.00 · no holdings · equity $9,098.00 vs prior close $9,098.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,098.00 | ▲ close $9,098.00 vs 09:30 $9,098.00 (session +0.00) | 16:00 close · cash $9,098.00 · no lots left · equity $9,098.00. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,098.00 | ▲ 09:30 equity $9,098.00 vs yday $9,098.00 (+0.00) | 09:30 open · cash $9,098.00 · no holdings · equity $9,098.00 vs prior close $9,098.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,098.00 | ▲ close $9,098.00 vs 09:30 $9,098.00 (session +0.00) | 16:00 close · cash $9,098.00 · no lots left · equity $9,098.00. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,098.00 | ▲ 09:30 equity $9,098.00 vs yday $9,098.00 (+0.00) | 09:30 open · cash $9,098.00 · no holdings · equity $9,098.00 vs prior close $9,098.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `SION` | 207 | $7.31 | $2.67 | — | $7,582.16 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_gainer; 🔵; ret5=+18.5; leftover $1516.33 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ALMS` | 146 | $10.38 | $2.43 | — | $6,064.99 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-56.2; leftover $1516.33 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `LX` | 1833 | $0.83 | $20.66 | — | $4,528.44 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-30.4; leftover $1516.33 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `EVTL` | 2369 | $0.64 | $22.27 | — | $2,990.01 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-22.0; leftover $1516.33 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FJET` | 533 | $2.84 | $6.88 | — | $1,469.41 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-26.9; leftover $1516.33 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `OSW` | 66 | $22.00 | $2.19 | — | $15.22 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-17.3; leftover $1516.33 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15.22 | ▼ close $9,005.71 vs 09:30 $9,098.00 (session -35.20) | 16:00 close · cash $15.22 · equity $9,005.71 vs 09:30 $9,098.00 (-92.29; session marks -35.20) · 6 name(s) marked open→close (per-name table). SION×207 09:30 $7.31 → close $6.75 -115.92; ALMS×146 09:30 $10.38 → close $11.36 +143.81; LX×1833 09:30 $0.83 → close $0.85 +45.83; EVTL×2369 09:30 $0.64 → close $0.60 -94.76; FJET×533 09:30 $2.84 → close $2.78 -31.98; OSW×66 09:30 $22.00 → close $22.27 +17.82 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15.22 | ▼ 09:30 equity $8,993.90 vs yday $9,005.71 (-11.81) | 09:30 open · cash $15.22 (unchanged overnight, no fees) · equity $8,993.90 vs prior close $9,005.71 (-11.81) · 6 name(s) re-marked at the open (per-name table). SION×207 yday $6.75 → 09:30 $6.68 -14.49; ALMS×146 yday $11.36 → 09:30 $11.23 -18.98; LX×1833 yday $0.85 → 09:30 $0.86 +11.00; EVTL×2369 yday $0.60 → 09:30 $0.60 +0.00; FJET×533 yday $2.78 → 09:30 $2.80 +10.66; OSW×66 yday $22.27 → 09:30 $22.27 +0.00 | — |
| 2026-09-04 09:30 ET | **BUY** | `AIIO` | 4 | $1.75 | $0.08 | — | $8.14 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-24.6; leftover $7.61 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8.14 | ▲ close $9,126.53 vs 09:30 $8,993.90 (session +132.71) | 16:00 close · cash $8.14 · equity $9,126.53 vs 09:30 $8,993.90 (+132.63; session marks +132.71) · 7 name(s) marked open→close (per-name table). SION×207 09:30 $6.68 → close $7.18 +103.50; ALMS×146 09:30 $11.23 → close $11.10 -18.98; LX×1833 09:30 $0.86 → close $0.88 +43.99; EVTL×2369 09:30 $0.60 → close $0.60 -4.74; FJET×533 09:30 $2.80 → close $2.80 +0.00; OSW×66 09:30 $22.27 → close $22.40 +8.58; AIIO×4 09:30 $1.75 → close $1.84 +0.36 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8.14 | ▲ 09:30 equity $9,134.49 vs yday $9,126.53 (+7.96) | 09:30 open · cash $8.14 (unchanged overnight, no fees) · equity $9,134.49 vs prior close $9,126.53 (+7.96) · 7 name(s) re-marked at the open (per-name table). SION×207 yday $7.18 → 09:30 $7.13 -10.35; ALMS×146 yday $11.10 → 09:30 $11.05 -7.30; LX×1833 yday $0.88 → 09:30 $0.90 +32.99; EVTL×2369 yday $0.60 → 09:30 $0.60 +0.00; FJET×533 yday $2.80 → 09:30 $2.80 +0.00; OSW×66 yday $22.40 → 09:30 $22.29 -7.26; AIIO×4 yday $1.84 → 09:30 $1.81 -0.12 | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8.14 | ▼ close $8,894.69 vs 09:30 $9,134.49 (session -239.80) | 16:00 close · cash $8.14 · equity $8,894.69 vs 09:30 $9,134.49 (-239.80; session marks -239.80) · 7 name(s) marked open→close (per-name table). SION×207 09:30 $7.13 → close $7.30 +35.19; ALMS×146 09:30 $11.05 → close $10.58 -68.62; LX×1833 09:30 $0.90 → close $0.85 -86.15; EVTL×2369 09:30 $0.60 → close $0.59 -18.95; FJET×533 09:30 $2.80 → close $2.63 -90.61; OSW×66 09:30 $22.29 → close $22.12 -11.22; AIIO×4 09:30 $1.81 → close $1.95 +0.56 | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8.14 | ▼ 09:30 equity $8,834.58 vs yday $8,894.69 (-60.11) | 09:30 open · cash $8.14 (unchanged overnight, no fees) · equity $8,834.58 vs prior close $8,894.69 (-60.11) · 7 name(s) re-marked at the open (per-name table). SION×207 yday $7.30 → 09:30 $7.27 -6.21; ALMS×146 yday $10.58 → 09:30 $10.49 -13.14; LX×1833 yday $0.85 → 09:30 $0.83 -32.99; EVTL×2369 yday $0.59 → 09:30 $0.59 +9.48; FJET×533 yday $2.63 → 09:30 $2.63 +0.00; OSW×66 yday $22.12 → 09:30 $21.86 -17.16; AIIO×4 yday $1.95 → 09:30 $1.93 -0.08 | — |
| 2026-09-09 09:30 ET | **SELL** | `SION` | 207 | $7.27 | $2.72 | $-13.67 | $1,510.32 | ▼ -13.67 after sell → book $8,831.87; vs 09:30 mark -2.71 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `ALMS` | 146 | $10.49 | $2.46 | $+11.90 | $3,039.39 | ▲ +11.90 after sell → book $8,829.40; vs 09:30 mark -2.47 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `LX` | 1833 | $0.83 | $21.12 | $-27.12 | $4,548.83 | ▼ -27.12 after sell → book $8,808.28; vs 09:30 mark -21.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `EVTL` | 2369 | $0.59 | $21.58 | $-152.83 | $5,934.43 | ▼ -152.83 after sell → book $8,786.70; vs 09:30 mark -21.58 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `FJET` | 533 | $2.63 | $6.98 | $-125.78 | $7,329.24 | ▼ -125.78 after sell → book $8,779.72; vs 09:30 mark -6.98 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `OSW` | 66 | $21.86 | $2.21 | $-13.64 | $8,769.79 | ▼ -13.64 after sell → book $8,777.51; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,769.79 | ▼ close $8,777.23 vs 09:30 $8,834.58 (session -0.28) | 16:00 close · cash $8,769.79 · equity $8,777.23 vs 09:30 $8,834.58 (-57.35; session marks -0.28) · 1 name(s) marked open→close (per-name table). AIIO×4 09:30 $1.93 → close $1.86 -0.28 | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,769.79 | ▼ 09:30 equity $8,777.03 vs yday $8,777.23 (-0.20) | 09:30 open · cash $8,769.79 (unchanged overnight, no fees) · equity $8,777.03 vs prior close $8,777.23 (-0.20) · 1 name(s) re-marked at the open (per-name table). AIIO×4 yday $1.86 → 09:30 $1.81 -0.20 | — |
| 2026-09-10 09:30 ET | **SELL** | `AIIO` | 4 | $1.81 | $0.10 | $+0.05 | $8,776.93 | ▲ +0.05 after sell → book $8,776.93; vs 09:30 mark -0.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,776.93 | ▲ close $8,776.93 vs 09:30 $8,777.03 (session +0.00) | 16:00 close · cash $8,776.93 · no lots left · equity $8,776.93. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,776.93 | ▲ 09:30 equity $8,776.93 vs yday $8,776.93 (-0.00) | 09:30 open · cash $8,776.93 · no holdings · equity $8,776.93 vs prior close $8,776.93 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `NAVN` | 141 | $20.61 | $2.41 | — | $5,868.50 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-24.7; leftover $2925.64 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `RWT` | 831 | $3.52 | $10.72 | — | $2,932.66 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-19.2; leftover $2925.64 | join🔴 sector🔴 gen🟡 news🔴 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `COO` | 53 | $54.66 | $2.15 | — | $33.54 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-22.3; leftover $2925.64 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $33.54 | ▲ close $8,804.64 vs 09:30 $8,776.93 (session +42.99) | 16:00 close · cash $33.54 · equity $8,804.64 vs 09:30 $8,776.93 (+27.71; session marks +42.99) · 3 name(s) marked open→close (per-name table). NAVN×141 09:30 $20.61 → close $21.02 +57.81; RWT×831 09:30 $3.52 → close $3.55 +24.93; COO×53 09:30 $54.66 → close $53.91 -39.75 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $33.54 | ▲ 09:30 equity $8,845.41 vs yday $8,804.64 (+40.77) | 09:30 open · cash $33.54 (unchanged overnight, no fees) · equity $8,845.41 vs prior close $8,804.64 (+40.77) · 3 name(s) re-marked at the open (per-name table). NAVN×141 yday $21.02 → 09:30 $21.10 +11.28; RWT×831 yday $3.55 → 09:30 $3.53 -16.62; COO×53 yday $53.91 → 09:30 $54.78 +46.11 | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $33.54 | ▲ close $9,103.10 vs 09:30 $8,845.41 (session +257.69) | 16:00 close · cash $33.54 · equity $9,103.10 vs 09:30 $8,845.41 (+257.69; session marks +257.69) · 3 name(s) marked open→close (per-name table). NAVN×141 09:30 $21.10 → close $21.37 +38.07; RWT×831 09:30 $3.53 → close $3.83 +249.30; COO×53 09:30 $54.78 → close $54.22 -29.68 | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $33.54 | ▼ 09:30 equity $9,080.66 vs yday $9,103.10 (-22.44) | 09:30 open · cash $33.54 (unchanged overnight, no fees) · equity $9,080.66 vs prior close $9,103.10 (-22.44) · 3 name(s) re-marked at the open (per-name table). NAVN×141 yday $21.37 → 09:30 $21.32 -7.05; RWT×831 yday $3.83 → 09:30 $3.80 -24.93; COO×53 yday $54.22 → 09:30 $54.40 +9.54 | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $33.54 | ▲ close $9,267.28 vs 09:30 $9,080.66 (session +186.62) | 16:00 close · cash $33.54 · equity $9,267.28 vs 09:30 $9,080.66 (+186.62; session marks +186.62) · 3 name(s) marked open→close (per-name table). NAVN×141 09:30 $21.32 → close $22.42 +155.10; RWT×831 09:30 $3.80 → close $3.91 +91.41; COO×53 09:30 $54.40 → close $53.27 -59.89 | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $33.54 | ▲ 09:30 equity $9,362.60 vs yday $9,267.28 (+95.32) | 09:30 open · cash $33.54 (unchanged overnight, no fees) · equity $9,362.60 vs prior close $9,267.28 (+95.32) · 3 name(s) re-marked at the open (per-name table). NAVN×141 yday $22.42 → 09:30 $22.27 -21.15; RWT×831 yday $3.91 → 09:30 $3.98 +58.17; COO×53 yday $53.27 → 09:30 $54.37 +58.30 | — |
| 2026-09-16 09:30 ET | **SELL** | `NAVN` | 141 | $22.27 | $2.46 | $+229.19 | $3,171.14 | ▲ +229.19 after sell → book $9,360.13; vs 09:30 mark -2.47 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `RWT` | 831 | $3.98 | $10.88 | $+360.66 | $6,467.64 | ▲ +360.66 after sell → book $9,349.25; vs 09:30 mark -10.88 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `COO` | 53 | $54.37 | $2.18 | $-19.70 | $9,347.07 | ▼ -19.70 after sell → book $9,347.07; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,347.07 | ▲ close $9,347.07 vs 09:30 $9,362.60 (session +0.00) | 16:00 close · cash $9,347.07 · no lots left · equity $9,347.07. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `NCMI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `QMLS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `CLBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `YSS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `CDNL` | cash | leftover split 2.39 < 1 share @ 39.85 |
| 2026-08-17 | `KLC` | cash | leftover split 2.39 < 1 share @ 2.62 |
| 2026-08-17 | `CSAN` | cash | leftover split 2.39 < 1 share @ 2.50 |
| 2026-08-18 | `NCMI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `QMLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `CLBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `YSS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `INV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `EYPT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CIG` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `INV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `BIDU` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `LZB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AAP` | cash | leftover split 15.48 < 1 share @ 42.41 |
| 2026-08-21 | `WMT` | cash | leftover split 15.48 < 1 share @ 103.69 |
| 2026-08-24 | `LZB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `DKS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `QFIN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `QMLS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `DKS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `QMLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `DY` | cash | leftover split 63.29 < 1 share @ 306.34 |
| 2026-08-31 | `LX` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-04 | `ALMS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `LX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `EVTL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `FJET` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `OSW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `SWBI` | cash | leftover split 7.61 < 1 share @ 14.12 |
| 2026-09-08 | `SION` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ALMS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `LX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `EVTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `FJET` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `OSW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `AIIO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-09 | `AIIO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `DYN` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `NAMS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `TTAN` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `FJET` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `OCGN` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `XPOF` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `COO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `NAVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `RWT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `COO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-15 | `NAVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `RWT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `COO` | min_hold | dropped but min-hold 2/3 sess — no sell |
