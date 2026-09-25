# Factor mine action — `union_e_green_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **-4.57%** ($9,543) · signal-only (no cash/fees) was -17.92%. Starts YES **5/30**. Fills 108 · skips 17 · realized $-2405.95.

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

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $7,594.06.

## Every lot, every session (09:30 mark and same-day change)

Cash does not change overnight and no fees print until a fill. While a lot stays on the book, the 09:30 open vs the prior close is an unrealized overnight move; the close vs that 09:30 open is the same-day unrealized move. Sum of overnight $ = 09:30 equity − prior close equity. On a no-fill day, sum of intraday $ = close equity − 09:30 equity. Bought-today names have overnight $ = 0 (they were not held at the prior close). Sold-at-open names have intraday $ = 0.

| Date | Ticker | Shares | Prior close | 09:30 open | Overnight $ | Close | Intraday $ | Day $ | vs entry @ open | vs entry @ close |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | Overnight $ | Intraday $ | Bought | Sold | Close cash | Close equity | Close held |
|---|---:|---:|---|---:|---:|---:|---|---|---:|---:|---|

## Fills (09:30 open snapshot, then buys / sells, then 16:00 close)

| Date | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | — | — |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 12176 | $0.81 | $135.15 | — | $2.29 | — | combo gate; gate earn_react=True,last_green=True; list flatten; ⚪; ret5=+13.2; leftover $10000.00 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.29 | ▲ close $10,960.69 vs 09:30 $10,000.00 (session +1,095.84) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.29 | ▲ 09:30 equity $11,325.97 vs yday $10,960.69 (+365.28) | — | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 12176 | $0.93 | $151.88 | $+1174.09 | $11,174.09 | ▲ +1,174.09 after sell → book $11,174.09; vs 09:30 mark -151.88 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `NMAX` | 141 | $9.89 | $2.41 | — | $9,776.48 | — | combo gate; gate earn_react=True,last_green=True; list ohlc_hot,earn_react; 🔵; ⚪; ret5=+10.9; leftover $1396.76 | — |
| 2026-08-14 09:30 ET | **BUY** | `AIRJ` | 253 | $5.51 | $3.26 | — | $8,379.19 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ⚪; ret5=+13.1; leftover $1396.76 | — |
| 2026-08-14 09:30 ET | **BUY** | `BRUN` | 53 | $26.25 | $2.15 | — | $6,986.05 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ⚪; ret5=+31.2; leftover $1396.76 | — |
| 2026-08-14 09:30 ET | **BUY** | `BZAI` | 1823 | $0.77 | $19.43 | — | $5,570.20 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ⚪; ret5=+20.4; leftover $1396.76 | — |
| 2026-08-14 09:30 ET | **BUY** | `DLO` | 91 | $15.28 | $2.26 | — | $4,177.46 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ⚪; ret5=-0.1; leftover $1396.76 | — |
| 2026-08-14 09:30 ET | **BUY** | `ENHA` | 604 | $2.31 | $7.79 | — | $2,774.43 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ⚪; ret5=-5.3; leftover $1396.76 | — |
| 2026-08-14 09:30 ET | **BUY** | `FIRY` | 143 | $9.74 | $2.42 | — | $1,379.19 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+1.2; leftover $1396.76 | — |
| 2026-08-14 09:30 ET | **BUY** | `GEMI` | 352 | $3.90 | $4.54 | — | $1.85 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ⚪; ret5=+8.0; leftover $1396.76 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.85 | ▼ close $10,570.62 vs 09:30 $11,325.97 (session -559.21) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.85 | ▲ 09:30 equity $10,630.08 vs yday $10,570.62 (+59.46) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `NMAX` | 141 | $10.97 | $2.45 | $+146.71 | $1,546.17 | ▲ +146.71 after sell → book $10,627.64; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRJ` | 253 | $6.22 | $3.32 | $+173.05 | $3,116.51 | ▲ +173.05 after sell → book $10,624.32; vs 09:30 mark -3.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BRUN` | 53 | $23.00 | $2.17 | $-176.30 | $4,333.34 | ▼ -176.30 after sell → book $10,622.15; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BZAI` | 1823 | $0.55 | $15.84 | $-425.40 | $5,323.79 | ▼ -425.40 after sell → book $10,606.30; vs 09:30 mark -15.85 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `DLO` | 91 | $14.23 | $2.29 | $-100.10 | $6,616.44 | ▼ -100.10 after sell → book $10,604.02; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ENHA` | 604 | $2.01 | $7.90 | $-196.89 | $7,822.57 | ▼ -196.89 after sell → book $10,596.11; vs 09:30 mark -7.91 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `FIRY` | 143 | $9.82 | $2.45 | $+6.57 | $9,224.38 | ▲ +6.57 after sell → book $10,593.66; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `GEMI` | 352 | $3.89 | $4.61 | $-12.67 | $10,589.05 | ▼ -12.67 after sell → book $10,589.05; vs 09:30 mark -4.61 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,589.05 | ▲ close $10,589.05 vs 09:30 $10,630.08 (session +0.00) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,589.05 | ▲ 09:30 equity $10,589.05 vs yday $10,589.05 (-0.00) | — | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,589.05 | ▲ close $10,589.05 vs 09:30 $10,589.05 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,589.05 | ▲ 09:30 equity $10,589.05 vs yday $10,589.05 (-0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,589.05 | ▲ close $10,589.05 vs 09:30 $10,589.05 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,589.05 | ▲ 09:30 equity $10,589.05 vs yday $10,589.05 (-0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `ATAT` | 38 | $34.05 | $2.10 | — | $9,293.05 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+9.3; leftover $1323.63 | — |
| 2026-08-20 09:30 ET | **BUY** | `ATHM` | 58 | $22.44 | $2.16 | — | $7,989.36 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=-2.1; leftover $1323.63 | — |
| 2026-08-20 09:30 ET | **BUY** | `BABA` | 10 | $123.47 | $2.02 | — | $6,752.64 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+2.9; leftover $1323.63 | — |
| 2026-08-20 09:30 ET | **BUY** | `BULL` | 133 | $9.94 | $2.39 | — | $5,428.23 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+12.6; leftover $1323.63 | — |
| 2026-08-20 09:30 ET | **BUY** | `COTY` | 519 | $2.55 | $6.70 | — | $4,098.09 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ⚪; ret5=+9.8; leftover $1323.63 | — |
| 2026-08-20 09:30 ET | **BUY** | `DQ` | 91 | $14.44 | $2.26 | — | $2,781.78 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-3.8; leftover $1323.63 | — |
| 2026-08-20 09:30 ET | **BUY** | `FUTU` | 11 | $117.65 | $2.02 | — | $1,485.61 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+4.1; leftover $1323.63 | — |
| 2026-08-20 09:30 ET | **BUY** | `IOND` | 20 | $65.60 | $2.05 | — | $171.56 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ⚪; ret5=+3.7; leftover $1323.63 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $171.56 | ▲ close $10,644.23 vs 09:30 $10,589.05 (session +76.89) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $171.56 | ▼ 09:30 equity $10,618.78 vs yday $10,644.23 (-25.45) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `ATAT` | 38 | $34.31 | $2.12 | $+5.65 | $1,473.22 | ▲ +5.65 after sell → book $10,616.66; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ATHM` | 58 | $22.20 | $2.18 | $-18.27 | $2,758.63 | ▼ -18.27 after sell → book $10,614.47; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BABA` | 10 | $125.35 | $2.04 | $+14.74 | $4,010.09 | ▲ +14.74 after sell → book $10,612.43; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BULL` | 133 | $8.99 | $2.42 | $-131.16 | $5,203.34 | ▼ -131.16 after sell → book $10,610.01; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `COTY` | 519 | $2.71 | $6.79 | $+69.55 | $6,603.04 | ▲ +69.55 after sell → book $10,603.22; vs 09:30 mark -6.79 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `DQ` | 91 | $15.00 | $2.29 | $+46.41 | $7,965.75 | ▲ +46.41 after sell → book $10,600.93; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `FUTU` | 11 | $115.18 | $2.04 | $-31.24 | $9,230.69 | ▼ -31.24 after sell → book $10,598.89; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IOND` | 20 | $68.41 | $2.07 | $+52.08 | $10,596.82 | ▲ +52.08 after sell → book $10,596.82; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `BJ` | 37 | $93.98 | $2.10 | — | $7,117.45 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=-2.4; leftover $3532.27 | — |
| 2026-08-21 09:30 ET | **BUY** | `BKE` | 81 | $43.08 | $2.23 | — | $3,625.74 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=-4.9; leftover $3532.27 | — |
| 2026-08-21 09:30 ET | **BUY** | `PSEC` | 1535 | $2.30 | $19.80 | — | $75.44 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=-3.0; leftover $3532.27 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $75.44 | ▲ close $10,768.14 vs 09:30 $10,618.78 (session +195.46) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $75.44 | ▲ 09:30 equity $10,838.90 vs yday $10,768.14 (+70.76) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `BJ` | 37 | $97.02 | $2.14 | $+108.24 | $3,663.04 | ▲ +108.24 after sell → book $10,836.76; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BKE` | 81 | $44.22 | $2.28 | $+87.83 | $7,242.59 | ▲ +87.83 after sell → book $10,834.49; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `PSEC` | 1535 | $2.34 | $20.09 | $+21.51 | $10,814.40 | ▲ +21.51 after sell → book $10,814.40; vs 09:30 mark -20.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,814.40 | ▲ close $10,814.40 vs 09:30 $10,838.90 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,814.40 | ▲ 09:30 equity $10,814.40 vs yday $10,814.40 (+0.00) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `SHMD` | 2372 | $4.54 | $30.60 | — | $3.06 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-14.6; leftover $10814.40 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.06 | ▼ close $8,115.30 vs 09:30 $10,814.40 (session -2,668.50) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.06 | ▼ 09:30 equity $8,020.42 vs yday $8,115.30 (-94.88) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `SHMD` | 2372 | $3.38 | $31.06 | $-2825.04 | $7,989.36 | ▼ -2,825.04 after sell → book $7,989.36; vs 09:30 mark -31.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `TIGR` | 191 | $5.21 | $2.56 | — | $6,991.69 | — | combo gate; gate earn_react=True,last_green=True; list ohlc_hot,earn_react; 🔵; ret5=+14.3; leftover $998.67 | — |
| 2026-08-26 09:30 ET | **BUY** | `LI` | 82 | $12.14 | $2.24 | — | $5,993.98 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+1.2; leftover $998.67 | — |
| 2026-08-26 09:30 ET | **BUY** | `NCNO` | 51 | $19.33 | $2.14 | — | $5,006.00 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+3.0; leftover $998.67 | — |
| 2026-08-26 09:30 ET | **BUY** | `QFIN` | 102 | $9.76 | $2.30 | — | $4,008.19 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-4.4; leftover $998.67 | — |
| 2026-08-26 09:30 ET | **BUY** | `QMLS` | 154 | $6.47 | $2.45 | — | $3,009.36 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-7.0; leftover $998.67 | — |
| 2026-08-26 09:30 ET | **BUY** | `SFL` | 80 | $12.35 | $2.23 | — | $2,019.12 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=-1.7; leftover $998.67 | — |
| 2026-08-26 09:30 ET | **BUY** | `SJM` | 7 | $134.80 | $2.01 | — | $1,073.51 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+5.9; leftover $998.67 | — |
| 2026-08-26 09:30 ET | **BUY** | `SMTC` | 7 | $130.90 | $2.01 | — | $155.20 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-5.7; leftover $998.67 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $155.20 | ▲ close $8,047.95 vs 09:30 $8,020.42 (session +76.53) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $155.20 | ▲ 09:30 equity $8,195.91 vs yday $8,047.95 (+147.96) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `TIGR` | 191 | $5.49 | $2.60 | $+48.31 | $1,201.19 | ▲ +48.31 after sell → book $8,193.31; vs 09:30 mark -2.60 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `LI` | 82 | $12.35 | $2.26 | $+12.72 | $2,211.63 | ▲ +12.72 after sell → book $8,191.05; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `NCNO` | 51 | $22.03 | $2.16 | $+133.39 | $3,333.00 | ▲ +133.39 after sell → book $8,188.89; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `QFIN` | 102 | $9.42 | $2.32 | $-39.30 | $4,291.51 | ▼ -39.30 after sell → book $8,186.56; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `QMLS` | 154 | $6.33 | $2.49 | $-26.50 | $5,263.85 | ▼ -26.50 after sell → book $8,184.08; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SFL` | 80 | $12.03 | $2.25 | $-30.08 | $6,223.99 | ▼ -30.08 after sell → book $8,181.82; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SJM` | 7 | $130.29 | $2.03 | $-35.61 | $7,133.99 | ▼ -35.61 after sell → book $8,179.79; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SMTC` | 7 | $149.40 | $2.03 | $+125.46 | $8,177.76 | ▲ +125.46 after sell → book $8,177.76; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `BBY` | 25 | $80.60 | $2.06 | — | $6,160.69 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=-2.0; leftover $2044.44 | — |
| 2026-08-27 09:30 ET | **BUY** | `HQY` | 21 | $97.16 | $2.05 | — | $4,118.28 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+0.5; leftover $2044.44 | — |
| 2026-08-27 09:30 ET | **BUY** | `RY` | 9 | $206.82 | $2.02 | — | $2,254.88 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=-0.2; leftover $2044.44 | — |
| 2026-08-27 09:30 ET | **BUY** | `TD` | 17 | $120.17 | $2.04 | — | $209.95 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+0.9; leftover $2044.44 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $209.95 | ▼ close $8,159.53 vs 09:30 $8,195.91 (session -10.05) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $209.95 | ▲ 09:30 equity $8,196.91 vs yday $8,159.53 (+37.38) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `BBY` | 25 | $83.85 | $2.09 | $+77.09 | $2,304.11 | ▲ +77.09 after sell → book $8,194.82; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `HQY` | 21 | $93.62 | $2.08 | $-78.47 | $4,268.05 | ▼ -78.47 after sell → book $8,192.74; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `RY` | 9 | $205.50 | $2.04 | $-15.94 | $6,115.51 | ▼ -15.94 after sell → book $8,190.70; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `TD` | 17 | $122.07 | $2.07 | $+28.19 | $8,188.63 | ▲ +28.19 after sell → book $8,188.63; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 3 | $261.16 | $2.00 | — | $7,403.15 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+7.8; leftover $1023.58 | — |
| 2026-08-28 09:30 ET | **BUY** | `ESTC` | 9 | $103.89 | $2.02 | — | $6,466.13 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=-2.5; leftover $1023.58 | — |
| 2026-08-28 09:30 ET | **BUY** | `FRO` | 23 | $44.40 | $2.06 | — | $5,442.87 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+0.4; leftover $1023.58 | — |
| 2026-08-28 09:30 ET | **BUY** | `GAP` | 41 | $24.69 | $2.11 | — | $4,428.47 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+5.8; leftover $1023.58 | — |
| 2026-08-28 09:30 ET | **BUY** | `HAFN` | 122 | $8.35 | $2.36 | — | $3,407.41 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+5.1; leftover $1023.58 | — |
| 2026-08-28 09:30 ET | **BUY** | `PD` | 78 | $13.09 | $2.22 | — | $2,384.17 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+4.2; leftover $1023.58 | — |
| 2026-08-28 09:30 ET | **BUY** | `RBRK` | 10 | $98.95 | $2.02 | — | $1,392.65 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+9.7; leftover $1023.58 | — |
| 2026-08-28 09:30 ET | **BUY** | `S` | 47 | $21.49 | $2.13 | — | $380.48 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+8.5; leftover $1023.58 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $380.48 | ▼ close $8,095.66 vs 09:30 $8,196.91 (session -76.05) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $380.48 | ▼ 09:30 equity $8,045.69 vs yday $8,095.66 (-49.97) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `ADSK` | 3 | $257.71 | $2.02 | $-14.37 | $1,151.60 | ▼ -14.37 after sell → book $8,043.68; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ESTC` | 9 | $98.00 | $2.04 | $-57.06 | $2,031.56 | ▼ -57.06 after sell → book $8,041.64; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `FRO` | 23 | $44.85 | $2.08 | $+6.21 | $3,061.03 | ▲ +6.21 after sell → book $8,039.56; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GAP` | 41 | $22.98 | $2.13 | $-74.36 | $4,001.08 | ▼ -74.36 after sell → book $8,037.43; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `HAFN` | 122 | $8.53 | $2.39 | $+17.22 | $5,039.35 | ▲ +17.22 after sell → book $8,035.04; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `PD` | 78 | $13.58 | $2.25 | $+33.75 | $6,096.34 | ▲ +33.75 after sell → book $8,032.79; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `RBRK` | 10 | $92.83 | $2.04 | $-65.26 | $7,022.60 | ▼ -65.26 after sell → book $8,030.75; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `S` | 47 | $21.45 | $2.15 | $-6.16 | $8,028.60 | ▼ -6.16 after sell → book $8,028.60; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,028.60 | ▲ close $8,028.60 vs 09:30 $8,045.69 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,028.60 | ▲ 09:30 equity $8,028.60 vs yday $8,028.60 (+0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,028.60 | ▲ close $8,028.60 vs 09:30 $8,028.60 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,028.60 | ▲ 09:30 equity $8,028.60 vs yday $8,028.60 (+0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,028.60 | ▲ close $8,028.60 vs 09:30 $8,028.60 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,028.60 | ▲ 09:30 equity $8,028.60 vs yday $8,028.60 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `AI` | 124 | $10.74 | $2.36 | — | $6,693.86 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+8.5; leftover $1338.10 | — |
| 2026-09-03 09:30 ET | **BUY** | `MOMO` | 243 | $5.50 | $3.13 | — | $5,354.23 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-4.8; leftover $1338.10 | — |
| 2026-09-03 09:30 ET | **BUY** | `PHR` | 121 | $11.02 | $2.35 | — | $4,018.45 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+2.2; leftover $1338.10 | — |
| 2026-09-03 09:30 ET | **BUY** | `TTC` | 13 | $99.00 | $2.03 | — | $2,729.42 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ⚪; ret5=-1.2; leftover $1338.10 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSXY` | 17 | $76.86 | $2.04 | — | $1,420.76 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-6.6; leftover $1338.10 | — |
| 2026-09-03 09:30 ET | **BUY** | `WOOF` | 428 | $3.12 | $5.52 | — | $79.88 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=-5.1; leftover $1338.10 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $79.88 | ▼ close $7,545.13 vs 09:30 $8,028.60 (session -466.03) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $79.88 | ▼ 09:30 equity $7,543.80 vs yday $7,545.13 (-1.33) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `AI` | 124 | $10.91 | $2.39 | $+15.70 | $1,430.33 | ▲ +15.70 after sell → book $7,541.41; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MOMO` | 243 | $5.13 | $3.19 | $-96.23 | $2,673.73 | ▼ -96.23 after sell → book $7,538.22; vs 09:30 mark -3.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `PHR` | 121 | $11.08 | $2.38 | $+2.52 | $4,012.03 | ▲ +2.52 after sell → book $7,535.84; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `TTC` | 13 | $92.14 | $2.05 | $-93.26 | $5,207.80 | ▼ -93.26 after sell → book $7,533.79; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `VSXY` | 17 | $73.63 | $2.06 | $-59.01 | $6,457.45 | ▼ -59.01 after sell → book $7,531.73; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `WOOF` | 428 | $2.51 | $5.60 | $-272.20 | $7,526.13 | ▼ -272.20 after sell → book $7,526.13; vs 09:30 mark -5.60 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `DOMO` | 416 | $3.62 | $5.37 | — | $6,016.92 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-3.1; leftover $1505.23 | — |
| 2026-09-04 09:30 ET | **BUY** | `GWRE` | 8 | $167.55 | $2.01 | — | $4,674.51 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+0.9; leftover $1505.23 | — |
| 2026-09-04 09:30 ET | **BUY** | `IOT` | 33 | $44.90 | $2.09 | — | $3,190.72 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=-7.5; leftover $1505.23 | — |
| 2026-09-04 09:30 ET | **BUY** | `LULU` | 15 | $98.15 | $2.04 | — | $1,716.43 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+5.9; leftover $1505.23 | — |
| 2026-09-04 09:30 ET | **BUY** | `MAMA` | 95 | $15.70 | $2.27 | — | $222.66 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=-0.4; leftover $1505.23 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $222.66 | ▼ close $7,412.05 vs 09:30 $7,543.80 (session -100.30) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $222.66 | ▼ 09:30 equity $7,362.44 vs yday $7,412.05 (-49.61) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `DOMO` | 416 | $3.84 | $5.45 | $+82.79 | $1,814.65 | ▲ +82.79 after sell → book $7,356.99; vs 09:30 mark -5.45 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `GWRE` | 8 | $160.52 | $2.03 | $-60.29 | $3,096.78 | ▼ -60.29 after sell → book $7,354.96; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `IOT` | 33 | $39.56 | $2.11 | $-180.42 | $4,400.15 | ▼ -180.42 after sell → book $7,352.85; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `LULU` | 15 | $100.58 | $2.06 | $+32.36 | $5,906.79 | ▲ +32.36 after sell → book $7,350.79; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MAMA` | 95 | $15.20 | $2.30 | $-52.08 | $7,348.49 | ▼ -52.08 after sell → book $7,348.49; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,348.49 | ▲ close $7,348.49 vs 09:30 $7,362.44 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,348.49 | ▲ 09:30 equity $7,348.49 vs yday $7,348.49 (-0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,348.49 | ▲ close $7,348.49 vs 09:30 $7,348.49 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,348.49 | ▲ 09:30 equity $7,348.49 vs yday $7,348.49 (-0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,348.49 | ▲ close $7,348.49 vs 09:30 $7,348.49 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,348.49 | ▲ 09:30 equity $7,348.49 vs yday $7,348.49 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `KR` | 131 | $56.02 | $2.38 | — | $7.48 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=-2.2; leftover $7348.49 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7.48 | ▲ close $7,669.67 vs 09:30 $7,348.49 (session +323.57) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7.48 | ▲ 09:30 equity $7,777.09 vs yday $7,669.67 (+107.42) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `KR` | 131 | $59.31 | $2.47 | $+426.14 | $7,774.63 | ▲ +426.14 after sell → book $7,774.63; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,774.63 | ▲ close $7,774.63 vs 09:30 $7,777.09 (session +0.00) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,774.63 | ▲ 09:30 equity $7,774.63 vs yday $7,774.63 (-0.00) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,774.63 | ▲ close $7,774.63 vs 09:30 $7,774.63 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,774.63 | ▲ 09:30 equity $7,774.63 vs yday $7,774.63 (-0.00) | — | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,774.63 | ▲ close $7,774.63 vs 09:30 $7,774.63 (session +0.00) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,774.63 | ▲ 09:30 equity $7,774.63 vs yday $7,774.63 (-0.00) | — | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,774.63 | ▲ close $7,774.63 vs 09:30 $7,774.63 (session +0.00) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,774.63 | ▲ 09:30 equity $7,774.63 vs yday $7,774.63 (-0.00) | — | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,774.63 | ▲ close $7,774.63 vs 09:30 $7,774.63 (session +0.00) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,774.63 | ▲ 09:30 equity $7,774.63 vs yday $7,774.63 (-0.00) | — | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,774.63 | ▲ close $7,774.63 vs 09:30 $7,774.63 (session +0.00) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,774.63 | ▲ 09:30 equity $7,774.63 vs yday $7,774.63 (-0.00) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,774.63 | ▲ close $7,774.63 vs 09:30 $7,774.63 (session +0.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,774.63 | ▲ 09:30 equity $7,774.63 vs yday $7,774.63 (-0.00) | — | — |
| 2026-09-23 09:30 ET | **BUY** | `CTAS` | 39 | $196.78 | $2.11 | — | $98.10 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ⚪; ret5=-2.0; leftover $7774.63 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $98.10 | ▼ close $7,584.93 vs 09:30 $7,774.63 (session -187.59) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $98.10 | ▲ 09:30 equity $7,596.24 vs yday $7,584.93 (+11.31) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `CTAS` | 39 | $192.26 | $2.18 | $-180.56 | $7,594.06 | ▼ -180.56 after sell → book $7,594.06; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,594.06 | ▲ close $7,594.06 vs 09:30 $7,596.24 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,543.06 | ▲ 09:30 equity $9,543.06 vs yday $9,543.06 (+0.00) | 09:30 open · cash $9,543.06 · no holdings · equity $9,543.06 vs prior close $9,543.06 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,543.06 | ▲ close $9,543.06 vs 09:30 $9,543.06 (session +0.00) | 16:00 close · cash $9,543.06 · no lots left · equity $9,543.06. | — |

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
| 2026-09-22 | `THO` | no_price | no 09:30 open |
| 2026-09-24 | `BB` | hard_red | hard-red S=-7.66 sit; no new buys |
