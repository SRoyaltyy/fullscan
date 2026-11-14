# Factor mine action — `union_e_green_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **-15.23%** ($8,477) · signal-only (no cash/fees) was -17.92%. Starts YES **5/30**. Fills 221 · skips 17 · realized $-2405.95.

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
| 2026-09-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,543.06 | ▲ 09:30 equity $9,543.06 vs yday $9,543.06 (+0.00) | 09:30 open · cash $9,543.06 · no holdings · equity $9,543.06 vs prior close $9,543.06 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,543.06 | ▲ close $9,543.06 vs 09:30 $9,543.06 (session +0.00) | 16:00 close · cash $9,543.06 · no lots left · equity $9,543.06. | — |
| 2026-09-29 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,543.06 | ▲ 09:30 equity $9,543.06 vs yday $9,543.06 (+0.00) | 09:30 open · cash $9,543.06 · no holdings · equity $9,543.06 vs prior close $9,543.06 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-29 09:30 ET | **BUY** | `CCL` | 195 | $24.39 | $2.58 | — | $4,784.43 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-0.8; leftover $4771.53 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `MTN` | 34 | $138.42 | $2.09 | — | $76.06 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-1.7; leftover $4771.53 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟡 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-29 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $76.06 | ▲ close $9,776.37 vs 09:30 $9,543.06 (session +237.98) | 16:00 close · cash $76.06 · equity $9,776.37 vs 09:30 $9,543.06 (+233.31; session marks +237.98) · 2 name(s) marked open→close (per-name table). CCL×195 09:30 $24.39 → close $25.11 +140.40; MTN×34 09:30 $138.42 → close $141.29 +97.58 | — |
| 2026-09-30 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $76.06 | ▲ 09:30 equity $9,776.37 vs yday $9,776.37 (-0.00) | 09:30 open · cash $76.06 (unchanged overnight, no fees) · equity $9,776.37 vs prior close $9,776.37 (-0.00) · 2 name(s) re-marked at the open (per-name table). CCL×195 yday $25.11 → 09:30 $25.11 +0.00; MTN×34 yday $141.29 → 09:30 $141.29 +0.00 | — |
| 2026-09-30 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $76.06 | ▲ close $9,776.37 vs 09:30 $9,776.37 (session +0.00) | 16:00 close · cash $76.06 · equity $9,776.37 vs 09:30 $9,776.37 (-0.00; session marks +0.00) · 2 name(s) marked open→close (per-name table). CCL×195 09:30 $25.11 → close $25.11 +0.00; MTN×34 09:30 $141.29 → close $141.29 +0.00 | — |
| 2026-10-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $76.06 | ▼ 09:30 equity $9,594.10 vs yday $9,776.37 (-182.27) | 09:30 open · cash $76.06 (unchanged overnight, no fees) · equity $9,594.10 vs prior close $9,776.37 (-182.27) · 2 name(s) re-marked at the open (per-name table). CCL×195 yday $25.11 → 09:30 $24.66 -87.75; MTN×34 yday $141.29 → 09:30 $138.51 -94.52 | — |
| 2026-10-01 09:30 ET | **SELL** | `CCL` | 195 | $24.66 | $2.65 | $+47.43 | $4,882.11 | ▲ +47.43 after sell → book $9,591.45; vs 09:30 mark -2.65 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `MTN` | 34 | $138.51 | $2.14 | $-1.17 | $9,589.31 | ▼ -1.17 after sell → book $9,589.31; vs 09:30 mark -2.14 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **BUY** | `ACN` | 22 | $215.98 | $2.06 | — | $4,835.70 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-0.1; leftover $4794.66 | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `PRGS` | 118 | $40.52 | $2.34 | — | $51.99 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-2.6; leftover $4794.66 | join🟢 sector🟢 gen🔴 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $51.99 | ▼ close $9,039.03 vs 09:30 $9,594.10 (session -545.88) | 16:00 close · cash $51.99 · equity $9,039.03 vs 09:30 $9,594.10 (-555.07; session marks -545.88) · 2 name(s) marked open→close (per-name table). ACN×22 09:30 $215.98 → close $212.30 -80.96; PRGS×118 09:30 $40.52 → close $36.58 -464.92 | — |
| 2026-10-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $51.99 | ▲ 09:30 equity $9,046.27 vs yday $9,039.03 (+7.24) | 09:30 open · cash $51.99 (unchanged overnight, no fees) · equity $9,046.27 vs prior close $9,039.03 (+7.24) · 2 name(s) re-marked at the open (per-name table). ACN×22 yday $212.30 → 09:30 $211.02 -28.16; PRGS×118 yday $36.58 → 09:30 $36.88 +35.40 | — |
| 2026-10-02 09:30 ET | **SELL** | `ACN` | 22 | $211.02 | $2.10 | $-113.28 | $4,692.33 | ▼ -113.28 after sell → book $9,044.17; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-02 09:30 ET | **SELL** | `PRGS` | 118 | $36.88 | $2.40 | $-434.26 | $9,041.77 | ▼ -434.26 after sell → book $9,041.77; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | — |
| 2026-10-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,041.77 | ▲ close $9,041.77 vs 09:30 $9,046.27 (session +0.00) | 16:00 close · cash $9,041.77 · no lots left · equity $9,041.77. | — |
| 2026-10-05 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,041.77 | ▲ 09:30 equity $9,041.77 vs yday $9,041.77 (+0.00) | 09:30 open · cash $9,041.77 · no holdings · equity $9,041.77 vs prior close $9,041.77 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-10-05 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,041.77 | ▲ close $9,041.77 vs 09:30 $9,041.77 (session +0.00) | 16:00 close · cash $9,041.77 · no lots left · equity $9,041.77. | — |
| 2026-10-06 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,041.77 | ▲ 09:30 equity $9,041.77 vs yday $9,041.77 (+0.00) | 09:30 open · cash $9,041.77 · no holdings · equity $9,041.77 vs prior close $9,041.77 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-10-06 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,041.77 | ▲ close $9,041.77 vs 09:30 $9,041.77 (session +0.00) | 16:00 close · cash $9,041.77 · no lots left · equity $9,041.77. | — |
| 2026-10-07 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,041.77 | ▲ 09:30 equity $9,041.77 vs yday $9,041.77 (+0.00) | 09:30 open · cash $9,041.77 · no holdings · equity $9,041.77 vs prior close $9,041.77 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-10-07 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,041.77 | ▲ close $9,041.77 vs 09:30 $9,041.77 (session +0.00) | 16:00 close · cash $9,041.77 · no lots left · equity $9,041.77. | — |
| 2026-10-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,041.77 | ▲ 09:30 equity $9,041.77 vs yday $9,041.77 (+0.00) | 09:30 open · cash $9,041.77 · no holdings · equity $9,041.77 vs prior close $9,041.77 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-10-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,041.77 | ▲ close $9,041.77 vs 09:30 $9,041.77 (session +0.00) | 16:00 close · cash $9,041.77 · no lots left · equity $9,041.77. | — |
| 2026-10-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,041.77 | ▲ 09:30 equity $9,041.77 vs yday $9,041.77 (+0.00) | 09:30 open · cash $9,041.77 · no holdings · equity $9,041.77 vs prior close $9,041.77 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-10-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,041.77 | ▲ close $9,041.77 vs 09:30 $9,041.77 (session +0.00) | 16:00 close · cash $9,041.77 · no lots left · equity $9,041.77. | — |
| 2026-10-12 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,041.77 | ▲ 09:30 equity $9,041.77 vs yday $9,041.77 (+0.00) | 09:30 open · cash $9,041.77 · no holdings · equity $9,041.77 vs prior close $9,041.77 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-10-12 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,041.77 | ▲ close $9,041.77 vs 09:30 $9,041.77 (session +0.00) | 16:00 close · cash $9,041.77 · no lots left · equity $9,041.77. | — |
| 2026-10-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,041.77 | ▲ 09:30 equity $9,041.77 vs yday $9,041.77 (+0.00) | 09:30 open · cash $9,041.77 · no holdings · equity $9,041.77 vs prior close $9,041.77 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-10-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,041.77 | ▲ close $9,041.77 vs 09:30 $9,041.77 (session +0.00) | 16:00 close · cash $9,041.77 · no lots left · equity $9,041.77. | — |
| 2026-10-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,041.77 | ▲ 09:30 equity $9,041.77 vs yday $9,041.77 (+0.00) | 09:30 open · cash $9,041.77 · no holdings · equity $9,041.77 vs prior close $9,041.77 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-10-14 09:30 ET | **BUY** | `ASML` | 2 | $1676.80 | $2.00 | — | $5,686.17 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=-6.3; leftover $4520.89 | join🟢 sector🟡 gen🔴 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-10-14 09:30 ET | **BUY** | `PGR` | 21 | $206.97 | $2.05 | — | $1,337.75 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+5.1; leftover $4520.89 | join🟢 sector🟡 gen🔴 news🟡 digest🟡 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-10-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,337.75 | ▲ close $9,120.41 vs 09:30 $9,041.77 (session +82.69) | 16:00 close · cash $1,337.75 · equity $9,120.41 vs 09:30 $9,041.77 (+78.64; session marks +82.69) · 2 name(s) marked open→close (per-name table). ASML×2 09:30 $1676.80 → close $1650.00 -53.60; PGR×21 09:30 $206.97 → close $213.46 +136.29 | — |
| 2026-10-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,337.75 | ▲ 09:30 equity $9,141.40 vs yday $9,120.41 (+20.99) | 09:30 open · cash $1,337.75 (unchanged overnight, no fees) · equity $9,141.40 vs prior close $9,120.41 (+20.99) · 2 name(s) re-marked at the open (per-name table). ASML×2 yday $1650.00 → 09:30 $1615.77 -68.47; PGR×21 yday $213.46 → 09:30 $217.72 +89.46 | — |
| 2026-10-15 09:30 ET | **SELL** | `ASML` | 2 | $1615.77 | $2.03 | $-126.10 | $4,567.25 | ▼ -126.10 after sell → book $9,139.37; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-10-15 09:30 ET | **SELL** | `PGR` | 21 | $217.72 | $2.10 | $+221.60 | $9,137.27 | ▲ +221.60 after sell → book $9,137.27; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-10-15 09:30 ET | **BUY** | `CMC` | 32 | $69.38 | $2.09 | — | $6,915.02 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+3.2; leftover $2284.32 | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-10-15 09:30 ET | **BUY** | `FHN` | 102 | $22.34 | $2.30 | — | $4,634.05 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=-1.4; leftover $2284.32 | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-10-15 09:30 ET | **BUY** | `PNC` | 10 | $208.49 | $2.02 | — | $2,547.13 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+0.0; leftover $2284.32 | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-10-15 09:30 ET | **BUY** | `SCHW` | 24 | $93.39 | $2.06 | — | $303.58 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+1.3; leftover $2284.32 | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-10-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $303.58 | ▲ close $9,136.38 vs 09:30 $9,141.40 (session +7.58) | 16:00 close · cash $303.58 · equity $9,136.38 vs 09:30 $9,141.40 (-5.02; session marks +7.58) · 4 name(s) marked open→close (per-name table). CMC×32 09:30 $69.38 → close $65.37 -128.32; FHN×102 09:30 $22.34 → close $22.86 +53.04; PNC×10 09:30 $208.49 → close $214.94 +64.50; SCHW×24 09:30 $93.39 → close $94.16 +18.36 | — |
| 2026-10-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $303.58 | ▲ 09:30 equity $9,180.46 vs yday $9,136.38 (+44.08) | 09:30 open · cash $303.58 (unchanged overnight, no fees) · equity $9,180.46 vs prior close $9,136.38 (+44.08) · 4 name(s) re-marked at the open (per-name table). CMC×32 yday $65.37 → 09:30 $66.19 +26.24; FHN×102 yday $22.86 → 09:30 $23.02 +16.32; PNC×10 yday $214.94 → 09:30 $216.58 +16.40; SCHW×24 yday $94.16 → 09:30 $93.54 -14.88 | — |
| 2026-10-16 09:30 ET | **SELL** | `CMC` | 32 | $66.19 | $2.11 | $-106.28 | $2,419.55 | ▼ -106.28 after sell → book $9,178.35; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-10-16 09:30 ET | **SELL** | `FHN` | 102 | $23.02 | $2.33 | $+64.73 | $4,765.26 | ▲ +64.73 after sell → book $9,176.02; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-10-16 09:30 ET | **SELL** | `PNC` | 10 | $216.58 | $2.05 | $+76.83 | $6,929.01 | ▲ +76.83 after sell → book $9,173.97; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-10-16 09:30 ET | **SELL** | `SCHW` | 24 | $93.54 | $2.09 | $-0.67 | $9,171.88 | ▼ -0.67 after sell → book $9,171.88; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-10-16 09:30 ET | **BUY** | `CFG` | 35 | $64.50 | $2.10 | — | $6,912.28 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-1.0; leftover $2292.97 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-10-16 09:30 ET | **BUY** | `IBKR` | 26 | $86.27 | $2.07 | — | $4,667.20 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-0.5; leftover $2292.97 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-10-16 09:30 ET | **BUY** | `JBHT` | 10 | $228.00 | $2.02 | — | $2,385.18 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+3.0; leftover $2292.97 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 judge🔴 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-10-16 09:30 ET | **BUY** | `MTB` | 10 | $221.49 | $2.02 | — | $168.26 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+0.5; leftover $2292.97 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-10-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $168.26 | ▼ close $8,976.40 vs 09:30 $9,180.46 (session -187.28) | 16:00 close · cash $168.26 · equity $8,976.40 vs 09:30 $9,180.46 (-204.06; session marks -187.28) · 4 name(s) marked open→close (per-name table). CFG×35 09:30 $64.50 → close $63.36 -39.90; IBKR×26 09:30 $86.27 → close $82.69 -93.08; JBHT×10 09:30 $228.00 → close $228.78 +7.80; MTB×10 09:30 $221.49 → close $215.28 -62.10 | — |
| 2026-10-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $168.26 | ▼ 09:30 equity $8,941.34 vs yday $8,976.40 (-35.06) | 09:30 open · cash $168.26 (unchanged overnight, no fees) · equity $8,941.34 vs prior close $8,976.40 (-35.06) · 4 name(s) re-marked at the open (per-name table). CFG×35 yday $63.36 → 09:30 $62.96 -14.00; IBKR×26 yday $82.69 → 09:30 $82.83 +3.64; JBHT×10 yday $228.78 → 09:30 $227.30 -14.80; MTB×10 yday $215.28 → 09:30 $214.29 -9.90 | — |
| 2026-10-19 09:30 ET | **SELL** | `CFG` | 35 | $62.96 | $2.12 | $-58.12 | $2,369.74 | ▼ -58.12 after sell → book $8,939.22; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-10-19 09:30 ET | **SELL** | `IBKR` | 26 | $82.83 | $2.10 | $-93.60 | $4,521.22 | ▼ -93.60 after sell → book $8,937.12; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-10-19 09:30 ET | **SELL** | `JBHT` | 10 | $227.30 | $2.05 | $-11.07 | $6,792.17 | ▼ -11.07 after sell → book $8,935.07; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-10-19 09:30 ET | **SELL** | `MTB` | 10 | $214.29 | $2.05 | $-76.07 | $8,933.03 | ▼ -76.07 after sell → book $8,933.03; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-10-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,933.03 | ▲ close $8,933.03 vs 09:30 $8,941.34 (session +0.00) | 16:00 close · cash $8,933.03 · no lots left · equity $8,933.03. | — |
| 2026-10-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,933.03 | ▲ 09:30 equity $8,933.03 vs yday $8,933.03 (+0.00) | 09:30 open · cash $8,933.03 · no holdings · equity $8,933.03 vs prior close $8,933.03 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-10-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,933.03 | ▲ close $8,933.03 vs 09:30 $8,933.03 (session +0.00) | 16:00 close · cash $8,933.03 · no lots left · equity $8,933.03. | — |
| 2026-10-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,933.03 | ▲ 09:30 equity $8,933.03 vs yday $8,933.03 (+0.00) | 09:30 open · cash $8,933.03 · no holdings · equity $8,933.03 vs prior close $8,933.03 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-10-21 09:30 ET | **BUY** | `BKU` | 52 | $42.79 | $2.15 | — | $6,705.80 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+1.5; leftover $2233.26 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-10-21 09:30 ET | **BUY** | `COF` | 10 | $206.37 | $2.02 | — | $4,640.08 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+1.8; leftover $2233.26 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-10-21 09:30 ET | **BUY** | `ISRG` | 5 | $402.50 | $2.00 | — | $2,625.60 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+3.8; leftover $2233.26 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-10-21 09:30 ET | **BUY** | `T` | 102 | $21.89 | $2.30 | — | $390.83 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+2.5; leftover $2233.26 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-10-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $390.83 | ▲ close $9,021.01 vs 09:30 $8,933.03 (session +96.45) | 16:00 close · cash $390.83 · equity $9,021.01 vs 09:30 $8,933.03 (+87.98; session marks +96.45) · 4 name(s) marked open→close (per-name table). BKU×52 09:30 $42.79 → close $42.08 -36.92; COF×10 09:30 $206.37 → close $206.66 +2.90; ISRG×5 09:30 $402.50 → close $428.12 +128.12; T×102 09:30 $21.89 → close $21.91 +2.35 | — |
| 2026-10-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $390.83 | ▲ 09:30 equity $9,025.75 vs yday $9,021.01 (+4.74) | 09:30 open · cash $390.83 (unchanged overnight, no fees) · equity $9,025.75 vs prior close $9,021.01 (+4.74) · 4 name(s) re-marked at the open (per-name table). BKU×52 yday $42.08 → 09:30 $41.80 -14.56; COF×10 yday $206.66 → 09:30 $205.94 -7.20; ISRG×5 yday $428.12 → 09:30 $430.67 +12.73; T×102 yday $21.91 → 09:30 $22.05 +13.77 | — |
| 2026-10-22 09:30 ET | **SELL** | `BKU` | 52 | $41.80 | $2.17 | $-55.80 | $2,562.26 | ▼ -55.80 after sell → book $9,023.57; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-10-22 09:30 ET | **SELL** | `COF` | 10 | $205.94 | $2.05 | $-8.37 | $4,619.61 | ▼ -8.37 after sell → book $9,021.53; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-10-22 09:30 ET | **SELL** | `ISRG` | 5 | $430.67 | $2.03 | $+136.81 | $6,770.90 | ▲ +136.81 after sell → book $9,019.49; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-10-22 09:30 ET | **SELL** | `T` | 102 | $22.05 | $2.33 | $+11.49 | $9,017.16 | ▲ +11.49 after sell → book $9,017.16; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-10-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,017.16 | ▲ close $9,017.16 vs 09:30 $9,025.75 (session +0.00) | 16:00 close · cash $9,017.16 · no lots left · equity $9,017.16. | — |
| 2026-10-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,017.16 | ▲ 09:30 equity $9,017.16 vs yday $9,017.16 (+0.00) | 09:30 open · cash $9,017.16 · no holdings · equity $9,017.16 vs prior close $9,017.16 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-10-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,017.16 | ▲ close $9,017.16 vs 09:30 $9,017.16 (session +0.00) | 16:00 close · cash $9,017.16 · no lots left · equity $9,017.16. | — |
| 2026-10-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,017.16 | ▲ 09:30 equity $9,017.16 vs yday $9,017.16 (+0.00) | 09:30 open · cash $9,017.16 · no holdings · equity $9,017.16 vs prior close $9,017.16 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-10-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,017.16 | ▲ close $9,017.16 vs 09:30 $9,017.16 (session +0.00) | 16:00 close · cash $9,017.16 · no lots left · equity $9,017.16. | — |
| 2026-10-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,017.16 | ▲ 09:30 equity $9,017.16 vs yday $9,017.16 (+0.00) | 09:30 open · cash $9,017.16 · no holdings · equity $9,017.16 vs prior close $9,017.16 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-10-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,017.16 | ▲ close $9,017.16 vs 09:30 $9,017.16 (session +0.00) | 16:00 close · cash $9,017.16 · no lots left · equity $9,017.16. | — |
| 2026-10-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,017.16 | ▲ 09:30 equity $9,017.16 vs yday $9,017.16 (+0.00) | 09:30 open · cash $9,017.16 · no holdings · equity $9,017.16 vs prior close $9,017.16 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-10-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,017.16 | ▲ close $9,017.16 vs 09:30 $9,017.16 (session +0.00) | 16:00 close · cash $9,017.16 · no lots left · equity $9,017.16. | — |
| 2026-10-29 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,017.16 | ▲ 09:30 equity $9,017.16 vs yday $9,017.16 (+0.00) | 09:30 open · cash $9,017.16 · no holdings · equity $9,017.16 vs prior close $9,017.16 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-10-29 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,017.16 | ▲ close $9,017.16 vs 09:30 $9,017.16 (session +0.00) | 16:00 close · cash $9,017.16 · no lots left · equity $9,017.16. | — |
| 2026-10-30 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,017.16 | ▲ 09:30 equity $9,017.16 vs yday $9,017.16 (+0.00) | 09:30 open · cash $9,017.16 · no holdings · equity $9,017.16 vs prior close $9,017.16 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-10-30 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,017.16 | ▲ close $9,017.16 vs 09:30 $9,017.16 (session +0.00) | 16:00 close · cash $9,017.16 · no lots left · equity $9,017.16. | — |
| 2026-11-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,017.16 | ▲ 09:30 equity $9,017.16 vs yday $9,017.16 (+0.00) | 09:30 open · cash $9,017.16 · no holdings · equity $9,017.16 vs prior close $9,017.16 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-11-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,017.16 | ▲ close $9,017.16 vs 09:30 $9,017.16 (session +0.00) | 16:00 close · cash $9,017.16 · no lots left · equity $9,017.16. | — |
| 2026-11-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,017.16 | ▲ 09:30 equity $9,017.16 vs yday $9,017.16 (+0.00) | 09:30 open · cash $9,017.16 · no holdings · equity $9,017.16 vs prior close $9,017.16 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-11-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,017.16 | ▲ close $9,017.16 vs 09:30 $9,017.16 (session +0.00) | 16:00 close · cash $9,017.16 · no lots left · equity $9,017.16. | — |
| 2026-11-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,017.16 | ▲ 09:30 equity $9,017.16 vs yday $9,017.16 (+0.00) | 09:30 open · cash $9,017.16 · no holdings · equity $9,017.16 vs prior close $9,017.16 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-11-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,017.16 | ▲ close $9,017.16 vs 09:30 $9,017.16 (session +0.00) | 16:00 close · cash $9,017.16 · no lots left · equity $9,017.16. | — |
| 2026-11-05 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,017.16 | ▲ 09:30 equity $9,017.16 vs yday $9,017.16 (+0.00) | 09:30 open · cash $9,017.16 · no holdings · equity $9,017.16 vs prior close $9,017.16 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-11-05 09:30 ET | **BUY** | `AMC` | 621 | $3.62 | $8.01 | — | $6,758.02 | — | combo gate; gate earn_react=True,last_green=True; list ohlc_hot; ret5=+17.7; leftover $2254.29 | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-11-05 09:30 ET | **BUY** | `DNUT` | 630 | $3.58 | $8.13 | — | $4,497.65 | — | combo gate; gate earn_react=True,last_green=True; list ohlc_hot; ret5=+15.4; leftover $2254.29 | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-11-05 09:30 ET | **BUY** | `EXPE` | 7 | $287.31 | $2.01 | — | $2,484.43 | — | combo gate; gate earn_react=True,last_green=True; list ohlc_hot; ret5=+14.6; leftover $2254.29 | join🟢 sector🔴 gen🔴 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-11-05 09:30 ET | **BUY** | `ARHS` | 176 | $12.79 | $2.52 | — | $230.87 | — | combo gate; gate earn_react=True,last_green=True; list ohlc_hot; ret5=+14.1; leftover $2254.29 | join🟢 sector🔴 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-11-05 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $230.87 | ▲ close $9,226.35 vs 09:30 $9,017.16 (session +229.87) | 16:00 close · cash $230.87 · equity $9,226.35 vs 09:30 $9,017.16 (+209.19; session marks +229.87) · 4 name(s) marked open→close (per-name table). AMC×621 09:30 $3.62 → close $3.08 -338.44; DNUT×630 09:30 $3.58 → close $4.60 +645.75; EXPE×7 09:30 $287.31 → close $272.48 -103.84; ARHS×176 09:30 $12.79 → close $12.94 +26.40 | — |
| 2026-11-06 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $230.87 | ▼ 09:30 equity $9,165.13 vs yday $9,226.35 (-61.22) | 09:30 open · cash $230.87 (unchanged overnight, no fees) · equity $9,165.13 vs prior close $9,226.35 (-61.22) · 4 name(s) re-marked at the open (per-name table). AMC×621 yday $3.08 → 09:30 $3.13 +31.05; ARHS×176 yday $12.94 → 09:30 $13.00 +10.56; DNUT×630 yday $4.60 → 09:30 $4.44 -100.80; EXPE×7 yday $272.48 → 09:30 $272.19 -2.03 | — |
| 2026-11-06 09:30 ET | **SELL** | `AMC` | 621 | $3.13 | $8.13 | $-323.54 | $2,166.47 | ▼ -323.54 after sell → book $9,157.00; vs 09:30 mark -8.13 | dropped from list after 1 sess (min 1) | — |
| 2026-11-06 09:30 ET | **SELL** | `ARHS` | 176 | $13.00 | $2.57 | $+31.88 | $4,451.90 | ▲ +31.88 after sell → book $9,154.44; vs 09:30 mark -2.56 | dropped from list after 1 sess (min 1) | — |
| 2026-11-06 09:30 ET | **SELL** | `DNUT` | 630 | $4.44 | $8.25 | $+528.57 | $7,240.85 | ▲ +528.57 after sell → book $9,146.18; vs 09:30 mark -8.26 | dropped from list after 1 sess (min 1) | — |
| 2026-11-06 09:30 ET | **SELL** | `EXPE` | 7 | $272.19 | $2.04 | $-109.92 | $9,144.14 | ▼ -109.92 after sell → book $9,144.14; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-11-06 09:30 ET | **BUY** | `SSP` | 381 | $3.00 | $4.91 | — | $7,996.23 | — | combo gate; gate earn_react=True,last_green=True; list ohlc_hot,earn_react; 🔵; ret5=+17.1; leftover $1143.02 | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-11-06 09:30 ET | **BUY** | `CHTR` | 9 | $115.74 | $2.02 | — | $6,952.55 | — | combo gate; gate earn_react=True,last_green=True; list ohlc_hot,earn_react; 🔵; ret5=+15.5; leftover $1143.02 | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-11-06 09:30 ET | **BUY** | `DKNG` | 65 | $17.34 | $2.19 | — | $5,823.27 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+13.5; leftover $1143.02 | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-11-06 09:30 ET | **BUY** | `EMA` | 22 | $49.98 | $2.06 | — | $4,721.65 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+4.8; leftover $1143.02 | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-11-06 09:30 ET | **BUY** | `EVC` | 164 | $6.94 | $2.48 | — | $3,581.01 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+19.6; leftover $1143.02 | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-11-06 09:30 ET | **BUY** | `FIP` | 357 | $3.20 | $4.61 | — | $2,434.00 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+2.4; leftover $1143.02 | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-11-06 09:30 ET | **BUY** | `PAR` | 57 | $19.72 | $2.16 | — | $1,307.80 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+7.8; leftover $1143.02 | join🔴 sector🟡 gen🔴 news🟢 digest🔴 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-11-06 09:30 ET | **BUY** | `SG` | 101 | $11.25 | $2.29 | — | $169.26 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+1.2; leftover $1143.02 | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-11-06 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $169.26 | ▲ close $9,809.78 vs 09:30 $9,165.13 (session +688.35) | 16:00 close · cash $169.26 · equity $9,809.78 vs 09:30 $9,165.13 (+644.65; session marks +688.35) · 8 name(s) marked open→close (per-name table). SSP×381 09:30 $3.00 → close $3.05 +19.05; CHTR×9 09:30 $115.74 → close $118.07 +20.97; DKNG×65 09:30 $17.34 → close $18.51 +76.05; EMA×22 09:30 $49.98 → close $50.69 +15.62; EVC×164 09:30 $6.94 → close $8.99 +336.20; FIP×357 09:30 $3.20 → close $3.47 +96.39; PAR×57 09:30 $19.72 → close $20.55 +47.31; SG×101 09:30 $11.25 → close $12.01 +76.76 | — |
| 2026-11-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $169.26 | ▲ 09:30 equity $9,842.57 vs yday $9,809.78 (+32.79) | 09:30 open · cash $169.26 (unchanged overnight, no fees) · equity $9,842.57 vs prior close $9,809.78 (+32.79) · 8 name(s) re-marked at the open (per-name table). CHTR×9 yday $118.07 → 09:30 $119.82 +15.75; DKNG×65 yday $18.51 → 09:30 $17.79 -47.13; EMA×22 yday $50.69 → 09:30 $51.04 +7.70; EVC×164 yday $8.99 → 09:30 $8.95 -6.56; FIP×357 yday $3.47 → 09:30 $3.50 +10.71; PAR×57 yday $20.55 → 09:30 $21.00 +25.65; SG×101 yday $12.01 → 09:30 $12.01 +0.00; SSP×381 yday $3.05 → 09:30 $3.12 +26.67 | — |
| 2026-11-09 09:30 ET | **SELL** | `CHTR` | 9 | $119.82 | $2.04 | $+32.67 | $1,245.60 | ▲ +32.67 after sell → book $9,840.54; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-11-09 09:30 ET | **SELL** | `DKNG` | 65 | $17.79 | $2.21 | $+24.53 | $2,399.42 | ▲ +24.53 after sell → book $9,838.33; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-11-09 09:30 ET | **SELL** | `EMA` | 22 | $51.04 | $2.08 | $+19.19 | $3,520.23 | ▲ +19.19 after sell → book $9,836.26; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-11-09 09:30 ET | **SELL** | `EVC` | 164 | $8.95 | $2.52 | $+324.64 | $4,985.51 | ▲ +324.64 after sell → book $9,833.74; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | — |
| 2026-11-09 09:30 ET | **SELL** | `FIP` | 357 | $3.50 | $4.67 | $+97.82 | $6,230.33 | ▲ +97.82 after sell → book $9,829.06; vs 09:30 mark -4.68 | dropped from list after 1 sess (min 1) | — |
| 2026-11-09 09:30 ET | **SELL** | `PAR` | 57 | $21.00 | $2.18 | $+68.62 | $7,425.15 | ▲ +68.62 after sell → book $9,826.88; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-11-09 09:30 ET | **SELL** | `SG` | 101 | $12.01 | $2.32 | $+72.15 | $8,635.84 | ▲ +72.15 after sell → book $9,824.56; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-11-09 09:30 ET | **SELL** | `SSP` | 381 | $3.12 | $4.99 | $+35.82 | $9,819.57 | ▲ +35.82 after sell → book $9,819.57; vs 09:30 mark -4.99 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-11-09 09:30 ET | **BUY** | `AIOT` | 377 | $3.25 | $4.86 | — | $8,589.46 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=-6.9; leftover $1227.45 | join🔴 sector🟡 gen🟢 news🟡 digest🔴 judge🔴 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-11-09 09:30 ET | **BUY** | `B` | 29 | $42.30 | $2.08 | — | $7,360.68 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+9.6; leftover $1227.45 | join🟡 sector🟡 gen🟢 news🟡 digest🔴 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-11-09 09:30 ET | **BUY** | `BCAX` | 137 | $8.90 | $2.40 | — | $6,138.98 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-5.3; leftover $1227.45 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-11-09 09:30 ET | **BUY** | `CECO` | 20 | $61.01 | $2.05 | — | $4,916.73 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=-13.4; leftover $1227.45 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🟡 vol🟢 buy🟡 |
| 2026-11-09 09:30 ET | **BUY** | `CEG` | 4 | $260.40 | $2.00 | — | $3,873.13 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-4.7; leftover $1227.45 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-11-09 09:30 ET | **BUY** | `CEVA` | 54 | $22.73 | $2.15 | — | $2,643.56 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-8.2; leftover $1227.45 | join🔴 sector🟡 gen🟢 news🟡 digest🔴 judge🔴 ab🔴 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-11-09 09:30 ET | **BUY** | `CNH` | 100 | $12.16 | $2.29 | — | $1,425.27 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=-1.7; leftover $1227.45 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-11-09 09:30 ET | **BUY** | `DOLE` | 90 | $13.50 | $2.26 | — | $208.01 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+6.2; leftover $1227.45 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟡 heat🔴 vol🟡 buy🟡 |
| 2026-11-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $208.01 | ▲ close $9,878.85 vs 09:30 $9,842.57 (session +79.37) | 16:00 close · cash $208.01 · equity $9,878.85 vs 09:30 $9,842.57 (+36.28; session marks +79.37) · 8 name(s) marked open→close (per-name table). AIOT×377 09:30 $3.25 → close $2.79 -173.42; B×29 09:30 $42.30 → close $42.27 -0.87; BCAX×137 09:30 $8.90 → close $9.98 +147.96; CECO×20 09:30 $61.01 → close $52.00 -180.20; CEG×4 09:30 $260.40 → close $276.09 +62.76; CEVA×54 09:30 $22.73 → close $24.84 +113.94; CNH×100 09:30 $12.16 → close $12.82 +66.00; DOLE×90 09:30 $13.50 → close $13.98 +43.20 | — |
| 2026-11-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $208.01 | ▼ 09:30 equity $9,732.57 vs yday $9,878.85 (-146.28) | 09:30 open · cash $208.01 (unchanged overnight, no fees) · equity $9,732.57 vs prior close $9,878.85 (-146.28) · 8 name(s) re-marked at the open (per-name table). AIOT×377 yday $2.79 → 09:30 $2.75 -15.08; B×29 yday $42.27 → 09:30 $40.71 -45.24; BCAX×137 yday $9.98 → 09:30 $9.76 -30.14; CECO×20 yday $52.00 → 09:30 $52.33 +6.60; CEG×4 yday $276.09 → 09:30 $270.25 -23.36; CEVA×54 yday $24.84 → 09:30 $24.35 -26.46; CNH×100 yday $12.82 → 09:30 $12.64 -18.00; DOLE×90 yday $13.98 → 09:30 $14.04 +5.40 | — |
| 2026-11-10 09:30 ET | **SELL** | `AIOT` | 377 | $2.75 | $4.94 | $-198.30 | $1,239.82 | ▼ -198.30 after sell → book $9,727.63; vs 09:30 mark -4.94 | dropped from list after 1 sess (min 1) | — |
| 2026-11-10 09:30 ET | **SELL** | `B` | 29 | $40.71 | $2.10 | $-50.28 | $2,418.32 | ▼ -50.28 after sell → book $9,725.54; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-11-10 09:30 ET | **SELL** | `BCAX` | 137 | $9.76 | $2.43 | $+112.98 | $3,753.00 | ▲ +112.98 after sell → book $9,723.10; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-11-10 09:30 ET | **SELL** | `CECO` | 20 | $52.33 | $2.07 | $-177.72 | $4,797.53 | ▼ -177.72 after sell → book $9,721.03; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-11-10 09:30 ET | **SELL** | `CEG` | 4 | $270.25 | $2.02 | $+35.38 | $5,876.51 | ▲ +35.38 after sell → book $9,719.01; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-11-10 09:30 ET | **SELL** | `CEVA` | 54 | $24.35 | $2.17 | $+83.16 | $7,189.24 | ▲ +83.16 after sell → book $9,716.84; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-11-10 09:30 ET | **SELL** | `CNH` | 100 | $12.64 | $2.32 | $+43.39 | $8,450.92 | ▲ +43.39 after sell → book $9,714.52; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-11-10 09:30 ET | **SELL** | `DOLE` | 90 | $14.04 | $2.29 | $+44.05 | $9,712.24 | ▲ +44.05 after sell → book $9,712.24; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-11-10 09:30 ET | **BUY** | `MNTK` | 559 | $2.17 | $7.21 | — | $8,492.00 | — | combo gate; gate earn_react=True,last_green=True; list ohlc_hot,earn_react; ret5=+15.4; leftover $1214.03 | join🔴 sector🟢 gen🟢 news🟡 digest🔴 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-11-10 09:30 ET | **BUY** | `ACHR` | 261 | $4.65 | $3.37 | — | $7,274.98 | — | combo gate; gate earn_react=True,last_green=True; list ohlc_hot,earn_react; 🔵; ret5=+15.3; leftover $1214.03 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-11-10 09:30 ET | **BUY** | `SENS` | 97 | $12.45 | $2.28 | — | $6,065.53 | — | combo gate; gate earn_react=True,last_green=True; list ohlc_hot,earn_react; ret5=+9.1; leftover $1214.03 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-11-10 09:30 ET | **BUY** | `AAON` | 16 | $71.64 | $2.04 | — | $4,917.25 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=-7.5; leftover $1214.03 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-11-10 09:30 ET | **BUY** | `ABCL` | 98 | $12.27 | $2.28 | — | $3,713.00 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+12.4; leftover $1214.03 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-11-10 09:30 ET | **BUY** | `ALH` | 48 | $24.93 | $2.13 | — | $2,514.23 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ⚪; ret5=+6.8; leftover $1214.03 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 heat🟢 vol🟡 buy🟡 |
| 2026-11-10 09:30 ET | **BUY** | `AMCX` | 153 | $7.92 | $2.45 | — | $1,300.02 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+10.8; leftover $1214.03 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-11-10 09:30 ET | **BUY** | `APPS` | 148 | $8.18 | $2.43 | — | $87.54 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+33.6; leftover $1214.03 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-11-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $87.54 | ▼ close $8,969.90 vs 09:30 $9,732.57 (session -718.15) | 16:00 close · cash $87.54 · equity $8,969.90 vs 09:30 $9,732.57 (-762.67; session marks -718.15) · 8 name(s) marked open→close (per-name table). MNTK×559 09:30 $2.17 → close $1.19 -547.82; ACHR×261 09:30 $4.65 → close $4.71 +15.66; SENS×97 09:30 $12.45 → close $10.03 -234.26; AAON×16 09:30 $71.64 → close $75.01 +53.92; ABCL×98 09:30 $12.27 → close $12.46 +19.11; ALH×48 09:30 $24.93 → close $25.07 +6.72; AMCX×153 09:30 $7.92 → close $8.61 +105.57; APPS×148 09:30 $8.18 → close $7.25 -137.05 | — |
| 2026-11-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $87.54 | ▲ 09:30 equity $9,132.78 vs yday $8,969.90 (+162.88) | 09:30 open · cash $87.54 (unchanged overnight, no fees) · equity $9,132.78 vs prior close $8,969.90 (+162.88) · 8 name(s) re-marked at the open (per-name table). AAON×16 yday $75.01 → 09:30 $76.32 +20.96; ABCL×98 yday $12.46 → 09:30 $12.78 +31.36; ACHR×261 yday $4.71 → 09:30 $4.85 +36.54; ALH×48 yday $25.07 → 09:30 $24.92 -7.20; AMCX×153 yday $8.61 → 09:30 $8.70 +13.77; APPS×148 yday $7.25 → 09:30 $7.38 +18.50; MNTK×559 yday $1.19 → 09:30 $1.25 +36.33; SENS×97 yday $10.03 → 09:30 $10.16 +12.61 | — |
| 2026-11-11 09:30 ET | **SELL** | `AAON` | 16 | $76.32 | $2.06 | $+70.78 | $1,306.60 | ▲ +70.78 after sell → book $9,130.72; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-11-11 09:30 ET | **SELL** | `ABCL` | 98 | $12.78 | $2.31 | $+45.88 | $2,556.73 | ▲ +45.88 after sell → book $9,128.41; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-11-11 09:30 ET | **SELL** | `ACHR` | 261 | $4.85 | $3.42 | $+45.41 | $3,819.16 | ▲ +45.41 after sell → book $9,124.99; vs 09:30 mark -3.42 | dropped from list after 1 sess (min 1) | — |
| 2026-11-11 09:30 ET | **SELL** | `ALH` | 48 | $24.92 | $2.15 | $-4.77 | $5,013.17 | ▼ -4.77 after sell → book $9,122.83; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 heat🟢 vol🟡 buy🟡 |
| 2026-11-11 09:30 ET | **SELL** | `AMCX` | 153 | $8.70 | $2.48 | $+114.41 | $6,341.78 | ▲ +114.41 after sell → book $9,120.35; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-11-11 09:30 ET | **SELL** | `APPS` | 148 | $7.38 | $2.47 | $-123.45 | $7,430.81 | ▼ -123.45 after sell → book $9,117.88; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-11-11 09:30 ET | **SELL** | `MNTK` | 559 | $1.25 | $7.31 | $-526.01 | $8,125.04 | ▼ -526.01 after sell → book $9,110.56; vs 09:30 mark -7.32 | dropped from list after 1 sess (min 1) | — |
| 2026-11-11 09:30 ET | **SELL** | `SENS` | 97 | $10.16 | $2.31 | $-226.23 | $9,108.26 | ▼ -226.23 after sell → book $9,108.26; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-11-11 09:30 ET | **BUY** | `ANGX` | 287 | $3.96 | $3.70 | — | $7,968.04 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+7.8; leftover $1138.53 | join🔴 sector🟡 gen🟢 news🟡 digest🔴 ab🟢 heat🟢 vol🟡 buy🟡 |
| 2026-11-11 09:30 ET | **BUY** | `ATRO` | 20 | $56.86 | $2.05 | — | $6,828.79 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+1.8; leftover $1138.53 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-11-11 09:30 ET | **BUY** | `BGS` | 373 | $3.05 | $4.81 | — | $5,686.32 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ⚪; ret5=+11.3; leftover $1138.53 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟡 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-11-11 09:30 ET | **BUY** | `CRWV` | 20 | $55.97 | $2.05 | — | $4,564.87 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ⚪; ret5=-9.3; leftover $1138.53 | join🟡 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 heat🟢 vol🟡 buy🟡 |
| 2026-11-11 09:30 ET | **BUY** | `FLY` | 47 | $23.96 | $2.13 | — | $3,436.43 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+39.3; leftover $1138.53 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-11-11 09:30 ET | **BUY** | `GO` | 90 | $12.52 | $2.26 | — | $2,307.37 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ⚪; ret5=+9.7; leftover $1138.53 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-11-11 09:30 ET | **BUY** | `ICL` | 245 | $4.63 | $3.16 | — | $1,169.86 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+7.1; leftover $1138.53 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-11-11 09:30 ET | **BUY** | `KOPN` | 315 | $3.61 | $4.06 | — | $28.65 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=-7.8; leftover $1138.53 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-11-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $28.65 | ▲ close $9,776.58 vs 09:30 $9,132.78 (session +692.55) | 16:00 close · cash $28.65 · equity $9,776.58 vs 09:30 $9,132.78 (+643.80; session marks +692.55) · 8 name(s) marked open→close (per-name table). ANGX×287 09:30 $3.96 → close $4.30 +97.58; ATRO×20 09:30 $56.86 → close $60.33 +69.40; BGS×373 09:30 $3.05 → close $3.67 +231.26; CRWV×20 09:30 $55.97 → close $60.77 +96.00; FLY×47 09:30 $23.96 → close $26.61 +124.36; GO×90 09:30 $12.52 → close $13.12 +54.00; ICL×245 09:30 $4.63 → close $4.57 -14.70; KOPN×315 09:30 $3.61 → close $3.72 +34.65 | — |
| 2026-11-12 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $28.65 | ▼ 09:30 equity $9,473.71 vs yday $9,776.58 (-302.87) | 09:30 open · cash $28.65 (unchanged overnight, no fees) · equity $9,473.71 vs prior close $9,776.58 (-302.87) · 8 name(s) re-marked at the open (per-name table). ANGX×287 yday $4.30 → 09:30 $4.22 -22.96; ATRO×20 yday $60.33 → 09:30 $58.05 -45.60; BGS×373 yday $3.67 → 09:30 $3.67 +0.00; CRWV×20 yday $60.77 → 09:30 $50.89 -197.55; FLY×47 yday $26.61 → 09:30 $26.18 -20.21; GO×90 yday $13.12 → 09:30 $13.08 -3.60; ICL×245 yday $4.57 → 09:30 $4.62 +12.25; KOPN×315 yday $3.72 → 09:30 $3.64 -25.20 | — |
| 2026-11-12 09:30 ET | **SELL** | `ANGX` | 287 | $4.22 | $3.76 | $+67.16 | $1,236.03 | ▲ +67.16 after sell → book $9,469.95; vs 09:30 mark -3.76 | dropped from list after 1 sess (min 1) | — |
| 2026-11-12 09:30 ET | **SELL** | `ATRO` | 20 | $58.05 | $2.07 | $+19.68 | $2,394.96 | ▲ +19.68 after sell → book $9,467.88; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-11-12 09:30 ET | **SELL** | `BGS` | 373 | $3.67 | $4.88 | $+221.56 | $3,758.98 | ▲ +221.56 after sell → book $9,462.99; vs 09:30 mark -4.89 | dropped from list after 1 sess (min 1) | — |
| 2026-11-12 09:30 ET | **SELL** | `CRWV` | 20 | $50.89 | $2.07 | $-105.67 | $4,774.76 | ▼ -105.67 after sell → book $9,460.92; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-11-12 09:30 ET | **SELL** | `FLY` | 47 | $26.18 | $2.15 | $+99.87 | $6,003.07 | ▲ +99.87 after sell → book $9,458.77; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-11-12 09:30 ET | **SELL** | `GO` | 90 | $13.08 | $2.28 | $+45.86 | $7,177.99 | ▲ +45.86 after sell → book $9,456.49; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-11-12 09:30 ET | **SELL** | `ICL` | 245 | $4.62 | $3.21 | $-8.82 | $8,306.67 | ▼ -8.82 after sell → book $9,453.27; vs 09:30 mark -3.22 | dropped from list after 1 sess (min 1) | — |
| 2026-11-12 09:30 ET | **SELL** | `KOPN` | 315 | $3.64 | $4.13 | $+1.26 | $9,449.15 | ▲ +1.26 after sell → book $9,449.15; vs 09:30 mark -4.12 | dropped from list after 1 sess (min 1) | — |
| 2026-11-12 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,449.15 | ▲ close $9,449.15 vs 09:30 $9,473.71 (session +0.00) | 16:00 close · cash $9,449.15 · no lots left · equity $9,449.15. | — |
| 2026-11-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,449.15 | ▲ 09:30 equity $9,449.15 vs yday $9,449.15 (+0.00) | 09:30 open · cash $9,449.15 · no holdings · equity $9,449.15 vs prior close $9,449.15 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-11-13 09:30 ET | **BUY** | `HTFL` | 32 | $57.94 | $2.09 | — | $7,592.98 | — | combo gate; gate earn_react=True,last_green=True; list ohlc_hot,earn_react; 🔵; ret5=+15.3; leftover $1889.83 | join🟡 sector🟡 gen🔴 news🟡 digest🔴 ab🟢 heat🟢 vol🟡 buy🟡 |
| 2026-11-13 09:30 ET | **BUY** | `ALMU` | 261 | $7.22 | $3.37 | — | $5,704.91 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+26.3; leftover $1889.83 | join🔴 sector🔴 gen🔴 news🟡 digest🟢 ab🔴 heat🔴 vol🟡 buy🟡 |
| 2026-11-13 09:30 ET | **BUY** | `CBRS` | 17 | $108.40 | $2.04 | — | $3,860.07 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+19.6; leftover $1889.83 | join🔴 sector🔴 gen🔴 news🟢 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-11-13 09:30 ET | **BUY** | `DGXX` | 733 | $2.58 | $9.46 | — | $1,963.14 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; ret5=+16.3; leftover $1889.83 | join🔴 sector🟡 gen🔴 news🟡 digest🟡 ab🔴 heat🔴 vol🟡 buy🟡 |
| 2026-11-13 09:30 ET | **BUY** | `NEOV` | 574 | $3.29 | $7.40 | — | $67.27 | — | combo gate; gate earn_react=True,last_green=True; list earn_react; 🔵; ret5=+35.1; leftover $1889.83 | join🔴 sector🟢 gen🔴 news🟡 digest🟡 judge🔴 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-11-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $67.27 | ▼ close $8,477.18 vs 09:30 $9,449.15 (session -947.62) | 16:00 close · cash $67.27 · equity $8,477.18 vs 09:30 $9,449.15 (-971.97; session marks -947.62) · 5 name(s) marked open→close (per-name table). HTFL×32 09:30 $57.94 → close $48.46 -303.36; ALMU×261 09:30 $7.22 → close $6.55 -175.16; CBRS×17 09:30 $108.40 → close $108.72 +5.44; DGXX×733 09:30 $2.58 → close $2.28 -216.24; NEOV×574 09:30 $3.29 → close $2.84 -258.30 | — |

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
