# Factor mine action — `union_rsi_os_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ rsi_os, no 🚨

Cash book **-10.59%** ($8,941) · signal-only (no cash/fees) was +4.98%. Starts YES **4/26**. Fills 54 · skips 11 · realized $-1059.21.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $8,940.80.

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
| 2026-08-25 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-08-26 | `DKS` | 26 | — | $121.87 | +0.00 | $129.66 | +202.54 | +202.54 | +0.00 | +202.54 |
| 2026-08-26 | `QFIN` | 332 | — | $9.76 | +0.00 | $9.35 | -136.12 | -136.12 | +0.00 | -136.12 |
| 2026-08-26 | `QMLS` | 501 | — | $6.47 | +0.00 | $6.10 | -185.37 | -185.37 | +0.00 | -185.37 |
| 2026-08-27 | `DKS` | 26 | $129.66 | $128.73 | -24.18 | — | +0.00 | -24.18 | +178.36 | — |
| 2026-08-27 | `QFIN` | 332 | $9.35 | $9.42 | +23.24 | — | +0.00 | +23.24 | -112.88 | — |
| 2026-08-27 | `QMLS` | 501 | $6.10 | $6.33 | +115.23 | — | +0.00 | +115.23 | -70.14 | — |
| 2026-08-28 | `QFIN` | 530 | — | $9.15 | +0.00 | $8.80 | -185.50 | -185.50 | +0.00 | -185.50 |
| 2026-08-28 | `DY` | 15 | — | $306.34 | +0.00 | $294.34 | -180.00 | -180.00 | +0.00 | -180.00 |
| 2026-08-31 | `QFIN` | 530 | $8.80 | $8.70 | -53.00 | — | +0.00 | -53.00 | -238.50 | — |
| 2026-08-31 | `DY` | 15 | $294.34 | $298.01 | +55.05 | — | +0.00 | +55.05 | -124.95 | — |
| 2026-09-01 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-02 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-03 | `SION` | 212 | — | $7.31 | +0.00 | $6.75 | -118.72 | -118.72 | +0.00 | -118.72 |
| 2026-09-03 | `ALMS` | 149 | — | $10.38 | +0.00 | $11.36 | +146.76 | +146.76 | +0.00 | +146.76 |
| 2026-09-03 | `LX` | 1879 | — | $0.83 | +0.00 | $0.85 | +46.98 | +46.98 | +0.00 | +46.98 |
| 2026-09-03 | `EVTL` | 2428 | — | $0.64 | +0.00 | $0.60 | -97.12 | -97.12 | +0.00 | -97.12 |
| 2026-09-03 | `FJET` | 547 | — | $2.84 | +0.00 | $2.78 | -32.82 | -32.82 | +0.00 | -32.82 |
| 2026-09-03 | `OSW` | 68 | — | $22.00 | +0.00 | $22.27 | +18.36 | +18.36 | +0.00 | +18.36 |
| 2026-09-04 | `SION` | 212 | $6.75 | $6.68 | -14.84 | $7.18 | +106.00 | +91.16 | -133.56 | -27.56 |
| 2026-09-04 | `ALMS` | 149 | $11.36 | $11.23 | -19.37 | — | +0.00 | -19.37 | +127.40 | — |
| 2026-09-04 | `LX` | 1879 | $0.85 | $0.86 | +11.27 | — | +0.00 | +11.27 | +58.25 | — |
| 2026-09-04 | `EVTL` | 2428 | $0.60 | $0.60 | +0.00 | — | +0.00 | +0.00 | -97.12 | — |
| 2026-09-04 | `FJET` | 547 | $2.78 | $2.80 | +10.94 | — | +0.00 | +10.94 | -21.88 | — |
| 2026-09-04 | `OSW` | 68 | $22.27 | $22.27 | +0.00 | — | +0.00 | +0.00 | +18.36 | — |
| 2026-09-04 | `AIIO` | 2213 | — | $1.75 | +0.00 | $1.84 | +199.17 | +199.17 | +0.00 | +199.17 |
| 2026-09-04 | `SWBI` | 272 | — | $14.12 | +0.00 | $12.89 | -334.56 | -334.56 | +0.00 | -334.56 |
| 2026-09-08 | `SION` | 212 | $7.18 | $7.13 | -10.60 | — | +0.00 | -10.60 | -38.16 | — |
| 2026-09-08 | `AIIO` | 2213 | $1.84 | $1.81 | -66.39 | — | +0.00 | -66.39 | +132.78 | — |
| 2026-09-08 | `SWBI` | 272 | $12.89 | $12.51 | -103.36 | — | +0.00 | -103.36 | -437.92 | — |
| 2026-09-09 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-10 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-11 | `NAVN` | 143 | — | $20.61 | +0.00 | $21.02 | +58.63 | +58.63 | +0.00 | +58.63 |
| 2026-09-11 | `RWT` | 841 | — | $3.52 | +0.00 | $3.55 | +25.23 | +25.23 | +0.00 | +25.23 |
| 2026-09-11 | `COO` | 54 | — | $54.66 | +0.00 | $53.91 | -40.50 | -40.50 | +0.00 | -40.50 |
| 2026-09-14 | `NAVN` | 143 | $21.02 | $21.10 | +11.44 | — | +0.00 | +11.44 | +70.07 | — |
| 2026-09-14 | `RWT` | 841 | $3.55 | $3.53 | -16.82 | — | +0.00 | -16.82 | +8.41 | — |
| 2026-09-14 | `COO` | 54 | $53.91 | $54.78 | +46.98 | — | +0.00 | +46.98 | +6.48 | — |
| 2026-09-15 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-16 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-17 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |
| 2026-09-18 | — | — | — | — | +0.00 | — | +0.00 | +0.00 | — | — |

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
| 2026-08-25 | +1.80 | $9,739.09 | — | $9,739.09 | -0.00 | +0.00 | — | — | $9,739.09 | $9,739.09 | — |
| 2026-08-26 | +2.02 | $9,739.09 | — | $9,739.09 | -0.00 | -118.95 | DKS, QFIN, QMLS | — | $75.86 | $9,607.32 | DKS×26, QFIN×332, QMLS×501 |
| 2026-08-27 | — | $75.86 | DKS×26, QFIN×332, QMLS×501 | $9,721.61 | +114.29 | +0.00 | — | DKS, QFIN, QMLS | $9,708.58 | $9,708.58 | — |
| 2026-08-28 | +0.75 | $9,708.58 | — | $9,708.58 | -0.00 | -365.50 | QFIN, DY | — | $255.10 | $9,334.20 | QFIN×530, DY×15 |
| 2026-08-31 | -5.85 | $255.10 | QFIN×530, DY×15 | $9,336.25 | +2.05 | +0.00 | — | QFIN, DY | $9,327.21 | $9,327.21 | — |
| 2026-09-01 | -6.30 | $9,327.21 | — | $9,327.21 | +0.00 | +0.00 | — | — | $9,327.21 | $9,327.21 | — |
| 2026-09-02 | -3.83 | $9,327.21 | — | $9,327.21 | +0.00 | +0.00 | — | — | $9,327.21 | $9,327.21 | — |
| 2026-09-03 | -0.90 | $9,327.21 | — | $9,327.21 | +0.00 | -36.56 | SION, ALMS, LX, EVTL, FJET, OSW | — | $15.86 | $9,232.23 | SION×212, ALMS×149, LX×1879, EVTL×2428, FJET×547, OSW×68 |
| 2026-09-04 | +2.25 | $15.86 | SION×212, ALMS×149, LX×1879, EVTL×2428, FJET×547, OSW×68 | $9,220.23 | -12.00 | -29.39 | AIIO, SWBI | ALMS, LX, EVTL, FJET, OSW | $2.42 | $9,102.58 | SION×212, AIIO×2213, SWBI×272 |
| 2026-09-08 | -11.47 | $2.42 | SION×212, AIIO×2213, SWBI×272 | $8,922.23 | -180.35 | +0.00 | — | SION, AIIO, SWBI | $8,886.92 | $8,886.92 | — |
| 2026-09-09 | -13.95 | $8,886.92 | — | $8,886.92 | +0.00 | +0.00 | — | — | $8,886.92 | $8,886.92 | — |
| 2026-09-10 | -13.28 | $8,886.92 | — | $8,886.92 | +0.00 | +0.00 | — | — | $8,886.92 | $8,886.92 | — |
| 2026-09-11 | +0.50 | $8,886.92 | — | $8,886.92 | +0.00 | +43.36 | NAVN, RWT, COO | — | $12.31 | $8,914.86 | NAVN×143, RWT×841, COO×54 |
| 2026-09-14 | -11.00 | $12.31 | NAVN×143, RWT×841, COO×54 | $8,956.46 | +41.60 | +0.00 | — | NAVN, RWT, COO | $8,940.80 | $8,940.80 | — |
| 2026-09-15 | -3.84 | $8,940.80 | — | $8,940.80 | -0.00 | +0.00 | — | — | $8,940.80 | $8,940.80 | — |
| 2026-09-16 | +5.30 | $8,940.80 | — | $8,940.80 | -0.00 | +0.00 | — | — | $8,940.80 | $8,940.80 | — |
| 2026-09-17 | +7.38 | $8,940.80 | — | $8,940.80 | -0.00 | +0.00 | — | — | $8,940.80 | $8,940.80 | — |
| 2026-09-18 | +4.86 | $8,940.80 | — | $8,940.80 | -0.00 | +0.00 | — | — | $8,940.80 | $8,940.80 | — |

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
| 2026-08-17 09:30 ET | **BUY** | `INV` | 1573 | $1.62 | $20.29 | — | $5,115.24 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-53.0; leftover $2549.13 | join🟢 sector🟢 gen🟢 news🟡 judge🔴 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `KLC` | 972 | $2.62 | $12.54 | — | $2,556.06 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ⚪; ret5=-49.7; leftover $2549.13 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CSAN` | 1017 | $2.50 | $13.12 | — | $0.44 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list earn_react; 🔵; ⚪; ret5=-12.5; leftover $2549.13 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.44 | ▼ close $9,701.70 vs 09:30 $10,219.44 (session -446.70) | 16:00 close · cash $0.44 · equity $9,701.70 vs 09:30 $10,219.44 (-517.74; session marks -446.70) · 4 name(s) marked open→close (per-name table). CDNL×63 09:30 $39.85 → close $39.23 -39.06; INV×1573 09:30 $1.62 → close $1.39 -369.66; KLC×972 09:30 $2.62 → close $2.56 -58.32; CSAN×1017 09:30 $2.50 → close $2.52 +20.34 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.44 | ▲ 09:30 equity $9,705.69 vs yday $9,701.70 (+3.99) | 09:30 open · cash $0.44 (unchanged overnight, no fees) · equity $9,705.69 vs prior close $9,701.70 (+3.99) · 4 name(s) re-marked at the open (per-name table). CDNL×63 yday $39.23 → 09:30 $41.57 +147.42; INV×1573 yday $1.39 → 09:30 $1.32 -94.38; KLC×972 yday $2.56 → 09:30 $2.52 -38.88; CSAN×1017 yday $2.52 → 09:30 $2.51 -10.17 | — |
| 2026-08-18 09:30 ET | **SELL** | `CDNL` | 63 | $41.57 | $2.21 | $+103.97 | $2,617.14 | ▲ +103.97 after sell → book $9,703.48; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `INV` | 1573 | $1.32 | $20.57 | $-504.90 | $4,680.80 | ▼ -504.90 after sell → book $9,682.91; vs 09:30 mark -20.57 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 judge🔴 vol🟢 buy🟡 |
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
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,739.09 | ▲ close $9,739.09 vs 09:30 $9,739.09 (session +0.00) | 16:00 close · cash $9,739.09 · no lots left · equity $9,739.09. | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,739.09 | ▲ 09:30 equity $9,739.09 vs yday $9,739.09 (-0.00) | 09:30 open · cash $9,739.09 · no holdings · equity $9,739.09 vs prior close $9,739.09 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-26 09:30 ET | **BUY** | `DKS` | 26 | $121.87 | $2.07 | — | $6,568.40 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-35.1; leftover $3246.36 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `QFIN` | 332 | $9.76 | $4.28 | — | $3,323.80 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list earn_react; 🔵; ret5=-4.4; leftover $3246.36 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **BUY** | `QMLS` | 501 | $6.47 | $6.46 | — | $75.86 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list earn_react; 🔵; ret5=-7.0; leftover $3246.36 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🔴 heat🟢 vol🔴 buy🟡 |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $75.86 | ▼ close $9,607.32 vs 09:30 $9,739.09 (session -118.95) | 16:00 close · cash $75.86 · equity $9,607.32 vs 09:30 $9,739.09 (-131.77; session marks -118.95) · 3 name(s) marked open→close (per-name table). DKS×26 09:30 $121.87 → close $129.66 +202.54; QFIN×332 09:30 $9.76 → close $9.35 -136.12; QMLS×501 09:30 $6.47 → close $6.10 -185.37 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $75.86 | ▲ 09:30 equity $9,721.61 vs yday $9,607.32 (+114.29) | 09:30 open · cash $75.86 (unchanged overnight, no fees) · equity $9,721.61 vs prior close $9,607.32 (+114.29) · 3 name(s) re-marked at the open (per-name table). DKS×26 yday $129.66 → 09:30 $128.73 -24.18; QFIN×332 yday $9.35 → 09:30 $9.42 +23.24; QMLS×501 yday $6.10 → 09:30 $6.33 +115.23 | — |
| 2026-08-27 09:30 ET | **SELL** | `DKS` | 26 | $128.73 | $2.10 | $+174.19 | $3,420.74 | ▲ +174.19 after sell → book $9,719.51; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `QFIN` | 332 | $9.42 | $4.36 | $-121.53 | $6,543.82 | ▼ -121.53 after sell → book $9,715.15; vs 09:30 mark -4.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `QMLS` | 501 | $6.33 | $6.57 | $-83.17 | $9,708.58 | ▼ -83.17 after sell → book $9,708.58; vs 09:30 mark -6.57 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,708.58 | ▲ close $9,708.58 vs 09:30 $9,721.61 (session +0.00) | 16:00 close · cash $9,708.58 · no lots left · equity $9,708.58. | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,708.58 | ▲ 09:30 equity $9,708.58 vs yday $9,708.58 (-0.00) | 09:30 open · cash $9,708.58 · no holdings · equity $9,708.58 vs prior close $9,708.58 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **BUY** | `QFIN` | 530 | $9.15 | $6.84 | — | $4,852.24 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-19.9; leftover $4854.29 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `DY` | 15 | $306.34 | $2.04 | — | $255.10 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-23.0; leftover $4854.29 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $255.10 | ▼ close $9,334.20 vs 09:30 $9,708.58 (session -365.50) | 16:00 close · cash $255.10 · equity $9,334.20 vs 09:30 $9,708.58 (-374.38; session marks -365.50) · 2 name(s) marked open→close (per-name table). QFIN×530 09:30 $9.15 → close $8.80 -185.50; DY×15 09:30 $306.34 → close $294.34 -180.00 | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $255.10 | ▲ 09:30 equity $9,336.25 vs yday $9,334.20 (+2.05) | 09:30 open · cash $255.10 (unchanged overnight, no fees) · equity $9,336.25 vs prior close $9,334.20 (+2.05) · 2 name(s) re-marked at the open (per-name table). QFIN×530 yday $8.80 → 09:30 $8.70 -53.00; DY×15 yday $294.34 → 09:30 $298.01 +55.05 | — |
| 2026-08-31 09:30 ET | **SELL** | `QFIN` | 530 | $8.70 | $6.96 | $-252.30 | $4,859.14 | ▼ -252.30 after sell → book $9,329.29; vs 09:30 mark -6.96 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `DY` | 15 | $298.01 | $2.08 | $-129.07 | $9,327.21 | ▼ -129.07 after sell → book $9,327.21; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,327.21 | ▲ close $9,327.21 vs 09:30 $9,336.25 (session +0.00) | 16:00 close · cash $9,327.21 · no lots left · equity $9,327.21. | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,327.21 | ▲ 09:30 equity $9,327.21 vs yday $9,327.21 (+0.00) | 09:30 open · cash $9,327.21 · no holdings · equity $9,327.21 vs prior close $9,327.21 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,327.21 | ▲ close $9,327.21 vs 09:30 $9,327.21 (session +0.00) | 16:00 close · cash $9,327.21 · no lots left · equity $9,327.21. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,327.21 | ▲ 09:30 equity $9,327.21 vs yday $9,327.21 (+0.00) | 09:30 open · cash $9,327.21 · no holdings · equity $9,327.21 vs prior close $9,327.21 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,327.21 | ▲ close $9,327.21 vs 09:30 $9,327.21 (session +0.00) | 16:00 close · cash $9,327.21 · no lots left · equity $9,327.21. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,327.21 | ▲ 09:30 equity $9,327.21 vs yday $9,327.21 (+0.00) | 09:30 open · cash $9,327.21 · no holdings · equity $9,327.21 vs prior close $9,327.21 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `SION` | 212 | $7.31 | $2.73 | — | $7,774.76 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_gainer; 🔵; ret5=+18.5; leftover $1554.54 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ALMS` | 149 | $10.38 | $2.44 | — | $6,226.44 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-56.2; leftover $1554.54 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `LX` | 1879 | $0.83 | $21.18 | — | $4,651.33 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-30.4; leftover $1554.54 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `EVTL` | 2428 | $0.64 | $22.82 | — | $3,074.59 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-22.0; leftover $1554.54 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FJET` | 547 | $2.84 | $7.06 | — | $1,514.06 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-26.9; leftover $1554.54 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `OSW` | 68 | $22.00 | $2.19 | — | $15.86 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-17.3; leftover $1554.54 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15.86 | ▼ close $9,232.23 vs 09:30 $9,327.21 (session -36.56) | 16:00 close · cash $15.86 · equity $9,232.23 vs 09:30 $9,327.21 (-94.98; session marks -36.56) · 6 name(s) marked open→close (per-name table). SION×212 09:30 $7.31 → close $6.75 -118.72; ALMS×149 09:30 $10.38 → close $11.36 +146.76; LX×1879 09:30 $0.83 → close $0.85 +46.98; EVTL×2428 09:30 $0.64 → close $0.60 -97.12; FJET×547 09:30 $2.84 → close $2.78 -32.82; OSW×68 09:30 $22.00 → close $22.27 +18.36 | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15.86 | ▼ 09:30 equity $9,220.23 vs yday $9,232.23 (-12.00) | 09:30 open · cash $15.86 (unchanged overnight, no fees) · equity $9,220.23 vs prior close $9,232.23 (-12.00) · 6 name(s) re-marked at the open (per-name table). SION×212 yday $6.75 → 09:30 $6.68 -14.84; ALMS×149 yday $11.36 → 09:30 $11.23 -19.37; LX×1879 yday $0.85 → 09:30 $0.86 +11.27; EVTL×2428 yday $0.60 → 09:30 $0.60 +0.00; FJET×547 yday $2.78 → 09:30 $2.80 +10.94; OSW×68 yday $22.27 → 09:30 $22.27 +0.00 | — |
| 2026-09-04 09:30 ET | **SELL** | `ALMS` | 149 | $11.23 | $2.48 | $+122.48 | $1,686.66 | ▲ +122.48 after sell → book $9,217.76; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `LX` | 1879 | $0.86 | $22.08 | $+14.99 | $3,276.75 | ▲ +14.99 after sell → book $9,195.67; vs 09:30 mark -22.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `EVTL` | 2428 | $0.60 | $22.27 | $-142.21 | $4,711.29 | ▼ -142.21 after sell → book $9,173.41; vs 09:30 mark -22.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `FJET` | 547 | $2.80 | $7.16 | $-36.10 | $6,235.73 | ▼ -36.10 after sell → book $9,166.25; vs 09:30 mark -7.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `OSW` | 68 | $22.27 | $2.22 | $+13.95 | $7,747.87 | ▲ +13.95 after sell → book $9,164.03; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `AIIO` | 2213 | $1.75 | $28.55 | — | $3,846.57 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-24.6; leftover $3873.94 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `SWBI` | 272 | $14.12 | $3.51 | — | $2.42 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list earn_react; ret5=-6.1; leftover $3873.94 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.42 | ▼ close $9,102.58 vs 09:30 $9,220.23 (session -29.39) | 16:00 close · cash $2.42 · equity $9,102.58 vs 09:30 $9,220.23 (-117.65; session marks -29.39) · 3 name(s) marked open→close (per-name table). SION×212 09:30 $6.68 → close $7.18 +106.00; AIIO×2213 09:30 $1.75 → close $1.84 +199.17; SWBI×272 09:30 $14.12 → close $12.89 -334.56 | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.42 | ▼ 09:30 equity $8,922.23 vs yday $9,102.58 (-180.35) | 09:30 open · cash $2.42 (unchanged overnight, no fees) · equity $8,922.23 vs prior close $9,102.58 (-180.35) · 3 name(s) re-marked at the open (per-name table). SION×212 yday $7.18 → 09:30 $7.13 -10.60; AIIO×2213 yday $1.84 → 09:30 $1.81 -66.39; SWBI×272 yday $12.89 → 09:30 $12.51 -103.36 | — |
| 2026-09-08 09:30 ET | **SELL** | `SION` | 212 | $7.13 | $2.78 | $-43.68 | $1,511.20 | ▼ -43.68 after sell → book $8,919.45; vs 09:30 mark -2.78 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `AIIO` | 2213 | $1.81 | $28.95 | $+75.29 | $5,487.79 | ▲ +75.29 after sell → book $8,890.51; vs 09:30 mark -28.94 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `SWBI` | 272 | $12.51 | $3.58 | $-445.01 | $8,886.92 | ▼ -445.01 after sell → book $8,886.92; vs 09:30 mark -3.59 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,886.92 | ▲ close $8,886.92 vs 09:30 $8,922.23 (session +0.00) | 16:00 close · cash $8,886.92 · no lots left · equity $8,886.92. | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,886.92 | ▲ 09:30 equity $8,886.92 vs yday $8,886.92 (+0.00) | 09:30 open · cash $8,886.92 · no holdings · equity $8,886.92 vs prior close $8,886.92 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,886.92 | ▲ close $8,886.92 vs 09:30 $8,886.92 (session +0.00) | 16:00 close · cash $8,886.92 · no lots left · equity $8,886.92. | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,886.92 | ▲ 09:30 equity $8,886.92 vs yday $8,886.92 (+0.00) | 09:30 open · cash $8,886.92 · no holdings · equity $8,886.92 vs prior close $8,886.92 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,886.92 | ▲ close $8,886.92 vs 09:30 $8,886.92 (session +0.00) | 16:00 close · cash $8,886.92 · no lots left · equity $8,886.92. | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,886.92 | ▲ 09:30 equity $8,886.92 vs yday $8,886.92 (+0.00) | 09:30 open · cash $8,886.92 · no holdings · equity $8,886.92 vs prior close $8,886.92 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-11 09:30 ET | **BUY** | `NAVN` | 143 | $20.61 | $2.42 | — | $5,937.28 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-24.7; leftover $2962.31 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `RWT` | 841 | $3.52 | $10.85 | — | $2,966.11 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; 🔵; ret5=-19.2; leftover $2962.31 | join🔴 sector🔴 gen🟡 news🔴 digest🟡 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 09:30 ET | **BUY** | `COO` | 54 | $54.66 | $2.15 | — | $12.31 | — | union ∩ rsi_os, no 🚨; gate rsi_os=True; list yday_mover; ret5=-22.3; leftover $2962.31 | join🟢 sector🟡 gen🟡 news🟡 digest🔴 judge🟢 ab🔴 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12.31 | ▲ close $8,914.86 vs 09:30 $8,886.92 (session +43.36) | 16:00 close · cash $12.31 · equity $8,914.86 vs 09:30 $8,886.92 (+27.94; session marks +43.36) · 3 name(s) marked open→close (per-name table). NAVN×143 09:30 $20.61 → close $21.02 +58.63; RWT×841 09:30 $3.52 → close $3.55 +25.23; COO×54 09:30 $54.66 → close $53.91 -40.50 | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12.31 | ▲ 09:30 equity $8,956.46 vs yday $8,914.86 (+41.60) | 09:30 open · cash $12.31 (unchanged overnight, no fees) · equity $8,956.46 vs prior close $8,914.86 (+41.60) · 3 name(s) re-marked at the open (per-name table). NAVN×143 yday $21.02 → 09:30 $21.10 +11.44; RWT×841 yday $3.55 → 09:30 $3.53 -16.82; COO×54 yday $53.91 → 09:30 $54.78 +46.98 | — |
| 2026-09-14 09:30 ET | **SELL** | `NAVN` | 143 | $21.10 | $2.47 | $+65.18 | $3,027.15 | ▲ +65.18 after sell → book $8,954.00; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `RWT` | 841 | $3.53 | $11.01 | $-13.45 | $5,984.87 | ▼ -13.45 after sell → book $8,942.99; vs 09:30 mark -11.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `COO` | 54 | $54.78 | $2.19 | $+2.14 | $8,940.80 | ▲ +2.14 after sell → book $8,940.80; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,940.80 | ▲ close $8,940.80 vs 09:30 $8,956.46 (session +0.00) | 16:00 close · cash $8,940.80 · no lots left · equity $8,940.80. | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,940.80 | ▲ 09:30 equity $8,940.80 vs yday $8,940.80 (-0.00) | 09:30 open · cash $8,940.80 · no holdings · equity $8,940.80 vs prior close $8,940.80 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,940.80 | ▲ close $8,940.80 vs 09:30 $8,940.80 (session +0.00) | 16:00 close · cash $8,940.80 · no lots left · equity $8,940.80. | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,940.80 | ▲ 09:30 equity $8,940.80 vs yday $8,940.80 (-0.00) | 09:30 open · cash $8,940.80 · no holdings · equity $8,940.80 vs prior close $8,940.80 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,940.80 | ▲ close $8,940.80 vs 09:30 $8,940.80 (session +0.00) | 16:00 close · cash $8,940.80 · no lots left · equity $8,940.80. | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,940.80 | ▲ 09:30 equity $8,940.80 vs yday $8,940.80 (-0.00) | 09:30 open · cash $8,940.80 · no holdings · equity $8,940.80 vs prior close $8,940.80 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,940.80 | ▲ close $8,940.80 vs 09:30 $8,940.80 (session +0.00) | 16:00 close · cash $8,940.80 · no lots left · equity $8,940.80. | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,940.80 | ▲ 09:30 equity $8,940.80 vs yday $8,940.80 (-0.00) | 09:30 open · cash $8,940.80 · no holdings · equity $8,940.80 vs prior close $8,940.80 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,940.80 | ▲ close $8,940.80 vs 09:30 $8,940.80 (session +0.00) | 16:00 close · cash $8,940.80 · no lots left · equity $8,940.80. | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `EYPT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CIG` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `BIDU` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-31 | `LX` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-09 | `DYN` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `NAMS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `TTAN` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `FJET` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `OCGN` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `XPOF` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `COO` | hard_red | hard-red S=-13.28 sit; no new buys |
