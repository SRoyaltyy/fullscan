# Factor mine action — `union_cond_n4_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 4 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · top 4 by cond

Cash book **-13.33%** ($8,667) · signal-only (no cash/fees) was -7.67%. Starts YES **1/30**. Fills 125 · skips 159 · realized $-515.67.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass the list as written and skip anything on the must-not list. They take up to 4 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Sort: how many morning cameras are green vs red.
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is thrown out if any must-not is true.
- Sort the keepers by how many morning cameras are green vs red and keep the top 4.
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
- **Gate** `none (list as ranked)` · **rank** `cond` · **top_n** 4.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $6,651.70.

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
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 41 | $59.80 | $2.11 | — | $7,546.09 | — | top 4 by cond; rank cond; list flatten; ⚪; ret5=-5.3; leftover $2500.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 84 | $29.74 | $2.24 | — | $5,045.69 | — | top 4 by cond; rank cond; list flatten; ⚪; ret5=-5.3; leftover $2500.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 3086 | $0.81 | $34.25 | — | $2,511.77 | — | top 4 by cond; rank cond; list flatten; ⚪; ret5=+13.2; leftover $2500.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 54 | $45.98 | $2.15 | — | $26.70 | — | top 4 by cond; rank cond; list flatten; ⚪; ret5=+12.3; leftover $2500.00 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $26.70 | ▲ close $10,107.25 vs 09:30 $10,000.00 (session +148.01) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $26.70 | ▲ 09:30 equity $10,171.79 vs yday $10,107.25 (+64.54) | — | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $26.70 | ▲ close $10,664.39 vs 09:30 $10,171.79 (session +492.60) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $26.70 | ▼ 09:30 equity $10,664.19 vs yday $10,664.39 (-0.20) | — | — |
| 2026-08-17 09:30 ET | **BUY** | `VERI` | 5 | $1.15 | $0.07 | — | $20.88 | — | top 4 by cond; rank cond; list yday_mover; ⚪; ret5=-12.2; leftover $6.67 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20.88 | ▲ close $10,878.62 vs 09:30 $10,664.19 (session +214.51) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20.88 | ▼ 09:30 equity $10,695.81 vs yday $10,878.62 (-182.81) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `BTSG` | 41 | $60.00 | $2.14 | $+3.94 | $2,478.73 | ▲ +3.94 after sell → book $10,693.66; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `HIMS` | 84 | $27.85 | $2.27 | $-163.28 | $4,815.86 | ▼ -163.28 after sell → book $10,691.39; vs 09:30 mark -2.27 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 3086 | $1.14 | $40.35 | $+943.78 | $8,293.55 | ▲ +943.78 after sell → book $10,651.04; vs 09:30 mark -40.35 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `IREN` | 54 | $43.56 | $2.18 | $-135.01 | $10,643.61 | ▼ -135.01 after sell → book $10,648.86; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,643.61 | ▼ close $10,648.58 vs 09:30 $10,695.81 (session -0.28) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,643.61 | ▲ 09:30 equity $10,648.61 vs yday $10,648.58 (+0.03) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,643.61 | ▼ close $10,648.44 vs 09:30 $10,648.61 (session -0.17) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,643.61 | ▼ 09:30 equity $10,648.42 vs yday $10,648.44 (-0.02) | — | — |
| 2026-08-20 09:30 ET | **SELL** | `VERI` | 5 | $0.96 | $0.08 | $-1.09 | $10,648.34 | ▼ -1.09 after sell → book $10,648.34; vs 09:30 mark -0.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 129 | $20.55 | $2.38 | — | $7,995.01 | — | top 4 by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $2662.08 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 29 | $91.01 | $2.08 | — | $5,353.65 | — | top 4 by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $2662.08 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 128 | $20.65 | $2.37 | — | $2,708.07 | — | top 4 by cond; rank cond; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $2662.08 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 461 | $5.77 | $5.95 | — | $42.15 | — | top 4 by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $2662.08 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $42.15 | ▲ close $10,760.78 vs 09:30 $10,648.42 (session +125.22) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $42.15 | ▲ 09:30 equity $11,041.00 vs yday $10,760.78 (+280.22) | — | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $42.15 | ▼ close $10,856.22 vs 09:30 $11,041.00 (session -184.78) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $42.15 | ▲ 09:30 equity $10,956.21 vs yday $10,856.22 (+99.99) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $42.15 | ▼ close $10,763.35 vs 09:30 $10,956.21 (session -192.86) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $42.15 | ▼ 09:30 equity $10,612.86 vs yday $10,763.35 (-150.49) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 129 | $20.32 | $2.42 | $-34.47 | $2,661.02 | ▼ -34.47 after sell → book $10,610.45; vs 09:30 mark -2.41 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 29 | $95.86 | $2.11 | $+136.46 | $5,438.85 | ▲ +136.46 after sell → book $10,608.34; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CDE` | 128 | $20.47 | $2.42 | $-27.83 | $8,056.59 | ▼ -27.83 after sell → book $10,605.92; vs 09:30 mark -2.42 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 461 | $5.53 | $6.04 | $-122.63 | $10,599.88 | ▼ -122.63 after sell → book $10,599.88; vs 09:30 mark -6.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 22 | $118.52 | $2.06 | — | $7,990.38 | — | top 4 by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $2649.97 | — |
| 2026-08-25 09:30 ET | **BUY** | `ERO` | 69 | $38.01 | $2.20 | — | $5,365.49 | — | top 4 by cond; rank cond; list mover_buy; ⚪; ret5=+10.4; leftover $2649.97 | — |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 34 | $77.13 | $2.09 | — | $2,740.98 | — | top 4 by cond; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $2649.97 | — |
| 2026-08-25 09:30 ET | **BUY** | `CNH` | 222 | $11.90 | $2.86 | — | $96.32 | — | top 4 by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+14.3; leftover $2649.97 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $96.32 | ▲ close $10,881.76 vs 09:30 $10,612.86 (session +291.09) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $96.32 | ▼ 09:30 equity $10,786.55 vs yday $10,881.76 (-95.21) | — | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $96.32 | ▼ close $10,667.94 vs 09:30 $10,786.55 (session -118.61) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $96.32 | ▼ 09:30 equity $10,644.00 vs yday $10,667.94 (-23.94) | — | — |
| 2026-08-27 09:30 ET | **BUY** | `GGB` | 5 | $4.57 | $0.24 | — | $73.22 | — | top 4 by cond; rank cond; list mover_buy; 🔵; ret5=+1.1; leftover $24.08 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $73.22 | ▲ close $10,652.84 vs 09:30 $10,644.00 (session +9.09) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $73.22 | ▲ 09:30 equity $10,722.51 vs yday $10,652.84 (+69.67) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `AU` | 22 | $119.19 | $2.09 | $+10.60 | $2,693.32 | ▲ +10.60 after sell → book $10,720.43; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ERO` | 69 | $40.12 | $2.23 | $+141.16 | $5,459.37 | ▲ +141.16 after sell → book $10,718.20; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `FCX` | 34 | $78.57 | $2.12 | $+44.74 | $8,128.62 | ▲ +44.74 after sell → book $10,716.07; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CNH` | 222 | $11.55 | $2.92 | $-83.49 | $10,689.80 | ▼ -83.49 after sell → book $10,713.15; vs 09:30 mark -2.92 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 8 | $324.41 | $2.01 | — | $8,092.51 | — | top 4 by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $2672.45 | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 18 | $141.76 | $2.04 | — | $5,538.78 | — | top 4 by cond; rank cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $2672.45 | — |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 6 | $400.42 | $2.01 | — | $3,134.26 | — | top 4 by cond; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $2672.45 | — |
| 2026-08-28 09:30 ET | **BUY** | `MPWR` | 2 | $1306.03 | $2.00 | — | $520.20 | — | top 4 by cond; rank cond; list mover_buy; 🔵; ret5=+0.0; leftover $2672.45 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $520.20 | ▼ close $10,247.13 vs 09:30 $10,722.51 (session -457.96) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $520.20 | ▲ 09:30 equity $10,299.31 vs yday $10,247.13 (+52.18) | — | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $520.20 | ▲ close $10,350.47 vs 09:30 $10,299.31 (session +51.16) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $520.20 | ▼ 09:30 equity $10,163.71 vs yday $10,350.47 (-186.76) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `GGB` | 5 | $4.57 | $0.26 | $-0.51 | $542.79 | ▼ -0.51 after sell → book $10,163.45; vs 09:30 mark -0.26 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $542.79 | ▼ close $10,091.71 vs 09:30 $10,163.71 (session -71.74) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $542.79 | ▼ 09:30 equity $10,074.45 vs yday $10,091.71 (-17.26) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `KEYS` | 8 | $318.04 | $2.04 | $-55.02 | $3,085.06 | ▼ -55.02 after sell → book $10,072.40; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SMTC` | 18 | $133.00 | $2.07 | $-161.80 | $5,476.99 | ▼ -161.80 after sell → book $10,070.33; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CIEN` | 6 | $357.25 | $2.04 | $-263.06 | $7,618.45 | ▼ -263.06 after sell → book $10,068.29; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `MPWR` | 2 | $1224.92 | $2.03 | $-166.24 | $10,066.27 | ▼ -166.24 after sell → book $10,066.27; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,066.27 | ▲ close $10,066.27 vs 09:30 $10,074.45 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,066.27 | ▲ 09:30 equity $10,066.27 vs yday $10,066.27 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 150 | $16.77 | $2.44 | — | $7,548.33 | — | top 4 by cond; rank cond; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $2516.57 | — |
| 2026-09-03 09:30 ET | **BUY** | `BMEA` | 1303 | $1.93 | $16.81 | — | $5,016.73 | — | top 4 by cond; rank cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $2516.57 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 1154 | $2.18 | $14.89 | — | $2,486.12 | — | top 4 by cond; rank cond; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $2516.57 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 57 | $42.93 | $2.16 | — | $36.95 | — | top 4 by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $2516.57 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $36.95 | ▼ close $9,738.34 vs 09:30 $10,066.27 (session -291.63) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $36.95 | ▼ 09:30 equity $9,712.29 vs yday $9,738.34 (-26.05) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `CABA` | 2 | $3.46 | $0.08 | — | $29.96 | — | top 4 by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $9.24 | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 3 | $2.52 | $0.08 | — | $22.31 | — | top 4 by cond; rank cond; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $9.24 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 1 | $6.71 | $0.07 | — | $15.53 | — | top 4 by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $9.24 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15.53 | ▲ close $10,001.55 vs 09:30 $9,712.29 (session +289.49) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15.53 | ▼ 09:30 equity $9,906.80 vs yday $10,001.55 (-94.75) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15.53 | ▼ close $9,855.07 vs 09:30 $9,906.80 (session -51.73) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15.53 | ▼ 09:30 equity $9,839.15 vs yday $9,855.07 (-15.92) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `ARCT` | 150 | $15.46 | $2.48 | $-201.42 | $2,332.05 | ▼ -201.42 after sell → book $9,836.67; vs 09:30 mark -2.48 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `BMEA` | 1303 | $1.94 | $17.05 | $-20.82 | $4,842.82 | ▼ -20.82 after sell → book $9,819.62; vs 09:30 mark -17.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CRDL` | 1154 | $2.22 | $15.10 | $+16.17 | $7,389.60 | ▲ +16.17 after sell → book $9,804.52; vs 09:30 mark -15.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `HRMY` | 57 | $42.01 | $2.19 | $-56.79 | $9,781.98 | ▼ -56.79 after sell → book $9,802.33; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,781.98 | ▼ close $9,800.77 vs 09:30 $9,839.15 (session -1.56) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,781.98 | ▼ 09:30 equity $9,800.45 vs yday $9,800.77 (-0.32) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `CABA` | 2 | $2.85 | $0.08 | $-1.38 | $9,787.60 | ▼ -1.38 after sell → book $9,800.37; vs 09:30 mark -0.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `ALEC` | 3 | $2.22 | $0.10 | $-1.08 | $9,794.17 | ▼ -1.08 after sell → book $9,800.28; vs 09:30 mark -0.09 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BHC` | 1 | $6.11 | $0.08 | $-0.75 | $9,800.19 | ▼ -0.75 after sell → book $9,800.19; vs 09:30 mark -0.09 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,800.19 | ▲ close $9,800.19 vs 09:30 $9,800.45 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,800.19 | ▲ 09:30 equity $9,800.19 vs yday $9,800.19 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `BAND` | 46 | $52.55 | $2.13 | — | $7,380.76 | — | top 4 by cond; rank cond; list probable,yday_gainer; 🔵; ⚪; ret5=+8.6; leftover $2450.05 | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 14 | $164.43 | $2.03 | — | $5,076.71 | — | top 4 by cond; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $2450.05 | — |
| 2026-09-11 09:30 ET | **BUY** | `PAGS` | 242 | $10.11 | $3.12 | — | $2,626.97 | — | top 4 by cond; rank cond; list yday_gainer; 🔵; ⚪; ret5=+2.9; leftover $2450.05 | — |
| 2026-09-11 09:30 ET | **BUY** | `SWKS` | 29 | $84.27 | $2.08 | — | $181.06 | — | top 4 by cond; rank cond; list yday_gainer,ohlc_hot; ret5=+17.2; leftover $2450.05 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $181.06 | ▲ close $9,912.19 vs 09:30 $9,800.19 (session +121.36) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $181.06 | ▼ 09:30 equity $9,694.08 vs yday $9,912.19 (-218.11) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $181.06 | ▼ close $9,162.34 vs 09:30 $9,694.08 (session -531.74) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $181.06 | ▲ 09:30 equity $9,208.10 vs yday $9,162.34 (+45.76) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $181.06 | ▲ close $9,387.68 vs 09:30 $9,208.10 (session +179.58) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $181.06 | ▼ 09:30 equity $9,241.48 vs yday $9,387.68 (-146.20) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `BAND` | 46 | $48.60 | $2.16 | $-185.98 | $2,414.51 | ▼ -185.98 after sell → book $9,239.33; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `ORCL` | 14 | $140.03 | $2.06 | $-345.69 | $4,372.87 | ▼ -345.69 after sell → book $9,237.27; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `PAGS` | 242 | $9.39 | $3.18 | $-180.54 | $6,642.07 | ▼ -180.54 after sell → book $9,234.09; vs 09:30 mark -3.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `QRVO` | 18 | $118.18 | $2.04 | — | $4,512.78 | — | top 4 by cond; rank cond; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+13.4; leftover $2214.02 | — |
| 2026-09-16 09:30 ET | **BUY** | `ATRC` | 39 | $55.66 | $2.11 | — | $2,339.94 | — | top 4 by cond; rank cond; list ohlc_hot; 🔵; ret5=+4.6; leftover $2214.02 | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 84 | $26.27 | $2.24 | — | $131.02 | — | top 4 by cond; rank cond; list yday_gainer; 🔵; ret5=+10.0; leftover $2214.02 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $131.02 | ▼ close $9,126.61 vs 09:30 $9,241.48 (session -101.09) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $131.02 | ▲ 09:30 equity $9,202.54 vs yday $9,126.61 (+75.93) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `SWKS` | 29 | $86.76 | $2.11 | $+68.03 | $2,644.95 | ▲ +68.03 after sell → book $9,200.43; vs 09:30 mark -2.11 | dropped from list after 4 sess (min 3) | — |
| 2026-09-17 09:30 ET | **BUY** | `BULL` | 83 | $7.95 | $2.24 | — | $1,982.86 | — | top 4 by cond; rank cond; list yday_mover; 🔵; ⚪; ret5=-18.4; leftover $661.24 | — |
| 2026-09-17 09:30 ET | **BUY** | `CIFR` | 36 | $18.04 | $2.10 | — | $1,331.50 | — | top 4 by cond; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=-1.1; leftover $661.24 | — |
| 2026-09-17 09:30 ET | **BUY** | `SABR` | 275 | $2.40 | $3.55 | — | $667.95 | — | top 4 by cond; rank cond; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.4; leftover $661.24 | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 3 | $170.85 | $2.00 | — | $153.41 | — | top 4 by cond; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $661.24 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $153.41 | ▲ close $9,259.45 vs 09:30 $9,202.54 (session +68.90) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $153.41 | ▲ 09:30 equity $9,341.87 vs yday $9,259.45 (+82.42) | — | — |
| 2026-09-18 09:30 ET | **BUY** | `FIVN` | 1 | $34.44 | $0.35 | — | $118.62 | — | top 4 by cond; rank cond; list flatten; 🔵; ⚪; ret5=+14.0; leftover $51.14 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $118.62 | ▼ close $9,203.16 vs 09:30 $9,341.87 (session -138.36) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $118.62 | ▲ 09:30 equity $9,309.12 vs yday $9,203.16 (+105.96) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `QRVO` | 18 | $118.44 | $2.07 | $+0.56 | $2,248.47 | ▲ +0.56 after sell → book $9,307.05; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `ATRC` | 39 | $58.23 | $2.14 | $+95.99 | $4,517.30 | ▲ +95.99 after sell → book $9,304.91; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `WAY` | 84 | $25.94 | $2.27 | $-32.24 | $6,693.99 | ▼ -32.24 after sell → book $9,302.64; vs 09:30 mark -2.27 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **BUY** | `VICR` | 9 | $230.25 | $2.02 | — | $4,619.72 | — | top 4 by cond; rank cond; list ohlc_hot; ret5=+12.5; leftover $2231.33 | — |
| 2026-09-21 09:30 ET | **BUY** | `COHR` | 6 | $326.48 | $2.01 | — | $2,658.83 | — | top 4 by cond; rank cond; list ohlc_hot; ret5=+3.9; leftover $2231.33 | — |
| 2026-09-21 09:30 ET | **BUY** | `FORM` | 18 | $123.00 | $2.04 | — | $442.79 | — | top 4 by cond; rank cond; list ohlc_hot; ret5=+3.0; leftover $2231.33 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $442.79 | ▼ close $9,109.61 vs 09:30 $9,309.12 (session -186.96) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $442.79 | ▼ 09:30 equity $9,030.26 vs yday $9,109.61 (-79.35) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `BULL` | 83 | $8.28 | $2.26 | $+22.47 | $1,127.35 | ▲ +22.47 after sell → book $9,028.00; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **SELL** | `CIFR` | 36 | $18.51 | $2.12 | $+12.88 | $1,791.59 | ▲ +12.88 after sell → book $9,025.88; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **BUY** | `ARM` | 1 | $319.41 | $1.99 | — | $1,470.19 | — | top 4 by cond; rank cond; list yday_gainer,yday_mover; ret5=+35.1; leftover $447.90 | — |
| 2026-09-22 09:30 ET | **BUY** | `DEFT` | 772 | $0.58 | $6.79 | — | $1,015.64 | — | top 4 by cond; rank cond; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $447.90 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,015.64 | ▼ close $8,992.89 vs 09:30 $9,030.26 (session -24.21) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,015.64 | ▲ 09:30 equity $9,497.96 vs yday $8,992.89 (+505.07) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `SABR` | 275 | $2.24 | $3.60 | $-51.15 | $1,628.03 | ▼ -51.15 after sell → book $9,494.36; vs 09:30 mark -3.60 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `SMTC` | 3 | $174.50 | $2.02 | $+6.93 | $2,149.51 | ▲ +6.93 after sell → book $9,492.34; vs 09:30 mark -2.02 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `FIVN` | 1 | $38.91 | $0.41 | $+3.71 | $2,188.01 | ▲ +3.71 after sell → book $9,491.93; vs 09:30 mark -0.41 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **BUY** | `A` | 3 | $166.54 | $2.00 | — | $1,686.39 | — | top 4 by cond; rank cond; list flatten; 🔵; ⚪; ret5=+10.3; leftover $547.00 | — |
| 2026-09-23 09:30 ET | **BUY** | `BFLY` | 55 | $9.90 | $2.15 | — | $1,139.73 | — | top 4 by cond; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+27.3; leftover $547.00 | — |
| 2026-09-23 09:30 ET | **BUY** | `DXCM` | 6 | $89.50 | $2.01 | — | $600.73 | — | top 4 by cond; rank cond; list flatten; 🔵; ⚪; ret5=+5.3; leftover $547.00 | — |
| 2026-09-23 09:30 ET | **BUY** | `HALO` | 4 | $116.85 | $2.00 | — | $131.32 | — | top 4 by cond; rank cond; list flatten; 🔵; ⚪; ret5=+3.3; leftover $547.00 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $131.32 | ▲ close $9,568.95 vs 09:30 $9,497.96 (session +85.19) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $131.32 | ▼ 09:30 equity $9,347.21 vs yday $9,568.95 (-221.74) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `VICR` | 9 | $274.61 | $2.05 | $+395.18 | $2,600.77 | ▲ +395.18 after sell → book $9,345.17; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `COHR` | 6 | $294.90 | $2.03 | $-193.52 | $4,368.13 | ▼ -193.52 after sell → book $9,343.13; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `FORM` | 18 | $126.98 | $2.07 | $+67.52 | $6,651.70 | ▲ +67.52 after sell → book $9,341.06; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,651.70 | ▲ close $9,390.52 vs 09:30 $9,347.21 (session +49.46) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,083.09 | ▲ 09:30 equity $8,351.52 vs yday $8,351.52 (-0.00) | 09:30 open · cash $8,083.09 (unchanged overnight, no fees) · equity $8,351.52 vs prior close $8,351.52 (-0.00) · 3 name(s) re-marked at the open (per-name table). BFLY×5 yday $9.41 → 09:30 $9.41 +0.00; FSLY×4 yday $26.68 → 09:30 $26.68 +0.00; UPXI×98 yday $1.17 → 09:30 $1.17 +0.00 | — |
| 2026-09-25 09:30 ET | **BUY** | `RSKD` | 257 | $7.85 | $3.32 | — | $6,062.32 | — | top 4 by cond; rank cond; list yday_gainer; 🔵; ⚪; ret5=+25.4; leftover $2020.77 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `CDNS` | 6 | $324.97 | $2.01 | — | $4,110.50 | — | top 4 by cond; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+14.7; leftover $2020.77 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `CYPH` | 504 | $4.00 | $6.50 | — | $2,085.48 | — | top 4 by cond; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+32.9; leftover $2020.77 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `DUOT` | 210 | $9.59 | $2.71 | — | $68.87 | — | top 4 by cond; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+12.4; leftover $2020.77 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $68.87 | ▼ close $8,289.42 vs 09:30 $8,351.52 (session -47.57) | 16:00 close · cash $68.87 · equity $8,289.42 vs 09:30 $8,351.52 (-62.10; session marks -47.57) · 7 name(s) marked open→close (per-name table). BFLY×5 09:30 $9.41 → close $9.41 -0.00; FSLY×4 09:30 $26.68 → close $26.68 +0.00; UPXI×98 09:30 $1.17 → close $1.17 -0.00; RSKD×257 09:30 $7.85 → close $7.78 -17.99; CDNS×6 09:30 $324.97 → close $326.13 +6.96; CYPH×504 09:30 $4.00 → close $4.12 +57.96; DUOT×210 09:30 $9.59 → close $9.14 -94.50 | — |
| 2026-09-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $68.87 | ▼ 09:30 equity $8,146.97 vs yday $8,289.42 (-142.45) | 09:30 open · cash $68.87 (unchanged overnight, no fees) · equity $8,146.97 vs prior close $8,289.42 (-142.45) · 7 name(s) re-marked at the open (per-name table). BFLY×5 yday $9.41 → 09:30 $9.28 -0.65; CDNS×6 yday $326.13 → 09:30 $318.99 -42.84; CYPH×504 yday $4.12 → 09:30 $4.03 -44.10; DUOT×210 yday $9.14 → 09:30 $8.99 -31.50; FSLY×4 yday $26.68 → 09:30 $24.45 -8.92; RSKD×257 yday $7.78 → 09:30 $7.72 -15.42; UPXI×98 yday $1.17 → 09:30 $1.18 +0.98 | — |
| 2026-09-28 09:30 ET | **SELL** | `BFLY` | 5 | $9.28 | $0.50 | $-4.11 | $114.77 | ▼ -4.11 after sell → book $8,146.47; vs 09:30 mark -0.50 | exit unpriced hold on first bar after 3 sess | — |
| 2026-09-28 09:30 ET | **SELL** | `FSLY` | 4 | $24.45 | $1.01 | $-16.42 | $211.56 | ▼ -16.42 after sell → book $8,145.46; vs 09:30 mark -1.01 | exit unpriced hold on first bar after 4 sess | — |
| 2026-09-28 09:30 ET | **SELL** | `UPXI` | 98 | $1.18 | $1.48 | $-7.88 | $325.72 | ▼ -7.88 after sell → book $8,143.98; vs 09:30 mark -1.48 | exit unpriced hold on first bar after 4 sess | — |
| 2026-09-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $325.72 | ▼ close $7,820.96 vs 09:30 $8,146.97 (session -323.02) | 16:00 close · cash $325.72 · equity $7,820.96 vs 09:30 $8,146.97 (-326.01; session marks -323.02) · 4 name(s) marked open→close (per-name table). CDNS×6 09:30 $318.99 → close $326.70 +46.26; CYPH×504 09:30 $4.03 → close $3.31 -364.14; DUOT×210 09:30 $8.99 → close $8.99 +0.00; RSKD×257 09:30 $7.72 → close $7.70 -5.14 | — |
| 2026-09-29 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $325.72 | ▼ 09:30 equity $7,807.75 vs yday $7,820.96 (-13.21) | 09:30 open · cash $325.72 (unchanged overnight, no fees) · equity $7,807.75 vs prior close $7,820.96 (-13.21) · 4 name(s) re-marked at the open (per-name table). CDNS×6 yday $326.70 → 09:30 $321.20 -33.00; CYPH×504 yday $3.31 → 09:30 $3.34 +15.12; DUOT×210 yday $8.99 → 09:30 $9.00 +2.10; RSKD×257 yday $7.70 → 09:30 $7.71 +2.57 | — |
| 2026-09-29 09:30 ET | **BUY** | `SONO` | 4 | $17.76 | $0.72 | — | $253.96 | — | top 4 by cond; rank cond; list flatten; ⚪; ret5=+9.8; leftover $81.43 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-29 09:30 ET | **BUY** | `AMC` | 24 | $3.32 | $0.87 | — | $173.41 | — | top 4 by cond; rank cond; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+13.8; leftover $81.43 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `ADMA` | 8 | $10.04 | $0.83 | — | $92.26 | — | top 4 by cond; rank cond; list probable,yday_gainer,ohlc_hot; ⚪; ret5=+6.5; leftover $81.43 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-29 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $92.26 | ▼ close $7,755.37 vs 09:30 $7,807.75 (session -49.96) | 16:00 close · cash $92.26 · equity $7,755.37 vs 09:30 $7,807.75 (-52.38; session marks -49.96) · 7 name(s) marked open→close (per-name table). CDNS×6 09:30 $321.20 → close $324.17 +17.82; CYPH×504 09:30 $3.34 → close $3.23 -55.44; DUOT×210 09:30 $9.00 → close $8.97 -6.30; RSKD×257 09:30 $7.71 → close $7.71 +0.00; SONO×4 09:30 $17.76 → close $17.85 +0.36; AMC×24 09:30 $3.32 → close $3.08 -5.76; ADMA×8 09:30 $10.04 → close $9.96 -0.64 | — |
| 2026-09-30 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $92.26 | ▲ 09:30 equity $7,755.37 vs yday $7,755.37 (+0.00) | 09:30 open · cash $92.26 (unchanged overnight, no fees) · equity $7,755.37 vs prior close $7,755.37 (+0.00) · 7 name(s) re-marked at the open (per-name table). ADMA×8 yday $9.96 → 09:30 $9.96 +0.00; AMC×24 yday $3.08 → 09:30 $3.08 +0.00; CDNS×6 yday $324.17 → 09:30 $324.17 +0.00; CYPH×504 yday $3.23 → 09:30 $3.23 +0.00; DUOT×210 yday $8.97 → 09:30 $8.97 +0.00; RSKD×257 yday $7.71 → 09:30 $7.71 +0.00; SONO×4 yday $17.85 → 09:30 $17.85 +0.00 | — |
| 2026-09-30 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $92.26 | ▲ close $7,755.37 vs 09:30 $7,755.37 (session +0.00) | 16:00 close · cash $92.26 · equity $7,755.37 vs 09:30 $7,755.37 (+0.00; session marks +0.00) · 7 name(s) marked open→close (per-name table). ADMA×8 09:30 $9.96 → close $9.96 +0.00; AMC×24 09:30 $3.08 → close $3.08 +0.00; CDNS×6 09:30 $324.17 → close $324.17 +0.00; CYPH×504 09:30 $3.23 → close $3.23 +0.00; DUOT×210 09:30 $8.97 → close $8.97 +0.00; RSKD×257 09:30 $7.71 → close $7.71 +0.00; SONO×4 09:30 $17.85 → close $17.85 +0.00 | — |
| 2026-10-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $92.26 | ▲ 09:30 equity $7,843.22 vs yday $7,755.37 (+87.85) | 09:30 open · cash $92.26 (unchanged overnight, no fees) · equity $7,843.22 vs prior close $7,755.37 (+87.85) · 7 name(s) re-marked at the open (per-name table). ADMA×8 yday $9.96 → 09:30 $10.07 +0.88; AMC×24 yday $3.08 → 09:30 $2.98 -2.40; CDNS×6 yday $324.17 → 09:30 $339.36 +91.14; CYPH×504 yday $3.23 → 09:30 $3.13 -50.40; DUOT×210 yday $8.97 → 09:30 $8.94 -6.30; RSKD×257 yday $7.71 → 09:30 $7.92 +53.97; SONO×4 yday $17.85 → 09:30 $18.09 +0.96 | — |
| 2026-10-01 09:30 ET | **SELL** | `ADMA` | 8 | $10.07 | $0.85 | $-1.44 | $171.97 | ▼ -1.44 after sell → book $7,842.37; vs 09:30 mark -0.85 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `AMC` | 24 | $2.98 | $0.81 | $-9.84 | $242.68 | ▼ -9.84 after sell → book $7,841.56; vs 09:30 mark -0.81 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `CDNS` | 6 | $339.36 | $2.03 | $+82.30 | $2,276.81 | ▲ +82.30 after sell → book $7,839.53; vs 09:30 mark -2.03 | exit unpriced hold on first bar after 4 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `CYPH` | 504 | $3.13 | $6.60 | $-454.10 | $3,847.73 | ▼ -454.10 after sell → book $7,832.93; vs 09:30 mark -6.60 | exit unpriced hold on first bar after 4 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `DUOT` | 210 | $8.94 | $2.76 | $-141.97 | $5,722.37 | ▼ -141.97 after sell → book $7,830.17; vs 09:30 mark -2.76 | exit unpriced hold on first bar after 4 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `RSKD` | 257 | $7.92 | $3.37 | $+11.30 | $7,754.44 | ▲ +11.30 after sell → book $7,826.80; vs 09:30 mark -3.37 | exit unpriced hold on first bar after 4 sess | join🟢 sector🟢 gen🔴 news🔴 digest🟢 judge🟡 ab🟡 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **SELL** | `SONO` | 4 | $18.09 | $0.76 | $-0.16 | $7,826.04 | ▼ -0.16 after sell → book $7,826.04; vs 09:30 mark -0.76 | exit unpriced hold on first bar after 2 sess | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `AVPT` | 137 | $14.27 | $2.40 | — | $5,868.65 | — | top 4 by cond; rank cond; list flatten,ohlc_hot; 🔵; ret5=+7.3; leftover $1956.51 | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-10-01 09:30 ET | **BUY** | `PAYS` | 144 | $13.50 | $2.42 | — | $3,922.23 | — | top 4 by cond; rank cond; list ohlc_hot; 🔵; ret5=+6.4; leftover $1956.51 | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-10-01 09:30 ET | **BUY** | `ETON` | 38 | $50.18 | $2.10 | — | $2,013.29 | — | top 4 by cond; rank cond; list yday_mover; ret5=-7.4; leftover $1956.51 | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-10-01 09:30 ET | **BUY** | `PDFS` | 38 | $51.41 | $2.10 | — | $57.60 | — | top 4 by cond; rank cond; list ohlc_hot; 🔵; ret5=+5.4; leftover $1956.51 | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $57.60 | ▲ close $8,058.46 vs 09:30 $7,843.22 (session +241.45) | 16:00 close · cash $57.60 · equity $8,058.46 vs 09:30 $7,843.22 (+215.24; session marks +241.45) · 4 name(s) marked open→close (per-name table). AVPT×137 09:30 $14.27 → close $14.08 -26.03; PAYS×144 09:30 $13.50 → close $14.36 +123.84; ETON×38 09:30 $50.18 → close $51.98 +68.40; PDFS×38 09:30 $51.41 → close $53.39 +75.24 | — |
| 2026-10-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $57.60 | ▲ 09:30 equity $8,140.18 vs yday $8,058.46 (+81.72) | 09:30 open · cash $57.60 (unchanged overnight, no fees) · equity $8,140.18 vs prior close $8,058.46 (+81.72) · 4 name(s) re-marked at the open (per-name table). AVPT×137 yday $14.08 → 09:30 $14.22 +19.18; ETON×38 yday $51.98 → 09:30 $52.42 +16.72; PAYS×144 yday $14.36 → 09:30 $14.19 -24.48; PDFS×38 yday $53.39 → 09:30 $55.24 +70.30 | — |
| 2026-10-02 09:30 ET | **BUY** | `ACRS` | 4 | $4.50 | $0.19 | — | $39.41 | — | top 4 by cond; rank cond; list yday_mover; 🔵; ⚪; ret5=-18.0; leftover $19.20 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `NAUT` | 11 | $1.67 | $0.22 | — | $20.88 | — | top 4 by cond; rank cond; list yday_gainer; 🔵; ⚪; ret5=+58.4; leftover $19.20 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20.88 | ▲ close $8,249.39 vs 09:30 $8,140.18 (session +109.61) | 16:00 close · cash $20.88 · equity $8,249.39 vs 09:30 $8,140.18 (+109.21; session marks +109.61) · 6 name(s) marked open→close (per-name table). AVPT×137 09:30 $14.22 → close $14.07 -20.55; ETON×38 09:30 $52.42 → close $55.36 +111.72; PAYS×144 09:30 $14.19 → close $14.23 +5.76; PDFS×38 09:30 $55.24 → close $55.50 +9.88; ACRS×4 09:30 $4.50 → close $4.39 -0.44; NAUT×11 09:30 $1.67 → close $1.96 +3.24 | — |
| 2026-10-05 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20.88 | ▲ 09:30 equity $8,269.25 vs yday $8,249.39 (+19.86) | 09:30 open · cash $20.88 (unchanged overnight, no fees) · equity $8,269.25 vs prior close $8,249.39 (+19.86) · 6 name(s) re-marked at the open (per-name table). ACRS×4 yday $4.39 → 09:30 $4.34 -0.20; AVPT×137 yday $14.07 → 09:30 $13.91 -21.92; ETON×38 yday $55.36 → 09:30 $55.85 +18.62; NAUT×11 yday $1.96 → 09:30 $2.18 +2.42; PAYS×144 yday $14.23 → 09:30 $14.32 +12.96; PDFS×38 yday $55.50 → 09:30 $55.71 +7.98 | — |
| 2026-10-05 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20.88 | ▲ close $8,444.19 vs 09:30 $8,269.25 (session +174.94) | 16:00 close · cash $20.88 · equity $8,444.19 vs 09:30 $8,269.25 (+174.94; session marks +174.94) · 6 name(s) marked open→close (per-name table). ACRS×4 09:30 $4.34 → close $4.27 -0.28; AVPT×137 09:30 $13.91 → close $14.66 +102.75; ETON×38 09:30 $55.85 → close $55.62 -8.74; NAUT×11 09:30 $2.18 → close $1.87 -3.41; PAYS×144 09:30 $14.32 → close $14.81 +70.56; PDFS×38 09:30 $55.71 → close $56.08 +14.06 | — |
| 2026-10-06 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20.88 | ▲ 09:30 equity $8,517.18 vs yday $8,444.19 (+72.99) | 09:30 open · cash $20.88 (unchanged overnight, no fees) · equity $8,517.18 vs prior close $8,444.19 (+72.99) · 6 name(s) re-marked at the open (per-name table). ACRS×4 yday $4.27 → 09:30 $4.22 -0.20; AVPT×137 yday $14.66 → 09:30 $14.77 +15.07; ETON×38 yday $55.62 → 09:30 $56.15 +20.14; NAUT×11 yday $1.87 → 09:30 $1.87 +0.00; PAYS×144 yday $14.81 → 09:30 $14.86 +7.20; PDFS×38 yday $56.08 → 09:30 $56.89 +30.78 | — |
| 2026-10-06 09:30 ET | **SELL** | `ETON` | 38 | $56.15 | $2.13 | $+222.62 | $2,152.45 | ▲ +222.62 after sell → book $8,515.05; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-10-06 09:30 ET | **SELL** | `PAYS` | 144 | $14.86 | $2.46 | $+190.95 | $4,289.83 | ▲ +190.95 after sell → book $8,512.59; vs 09:30 mark -2.46 | dropped from list after 3 sess (min 3) | — |
| 2026-10-06 09:30 ET | **SELL** | `PDFS` | 38 | $56.89 | $2.13 | $+204.00 | $6,449.51 | ▲ +204.00 after sell → book $8,510.45; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `GWRE` | 13 | $165.00 | $2.03 | — | $4,302.49 | — | top 4 by cond; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+12.4; leftover $2149.84 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `PTC` | 11 | $193.00 | $2.02 | — | $2,177.46 | — | top 4 by cond; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+37.6; leftover $2149.84 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `IOT` | 50 | $42.70 | $2.14 | — | $40.32 | — | top 4 by cond; rank cond; list ohlc_hot; ⚪; ret5=+11.8; leftover $2149.84 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-06 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $40.32 | ▼ close $8,422.18 vs 09:30 $8,517.18 (session -82.08) | 16:00 close · cash $40.32 · equity $8,422.18 vs 09:30 $8,517.18 (-95.00; session marks -82.08) · 6 name(s) marked open→close (per-name table). ACRS×4 09:30 $4.22 → close $3.70 -2.08; AVPT×137 09:30 $14.77 → close $14.59 -24.66; NAUT×11 09:30 $1.87 → close $1.77 -1.10; GWRE×13 09:30 $165.00 → close $164.52 -6.24; PTC×11 09:30 $193.00 → close $193.00 +0.00; IOT×50 09:30 $42.70 → close $41.74 -48.00 | — |
| 2026-10-07 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $40.32 | ▼ 09:30 equity $8,372.21 vs yday $8,422.18 (-49.97) | 09:30 open · cash $40.32 (unchanged overnight, no fees) · equity $8,372.21 vs prior close $8,422.18 (-49.97) · 6 name(s) re-marked at the open (per-name table). ACRS×4 yday $3.70 → 09:30 $3.58 -0.48; AVPT×137 yday $14.59 → 09:30 $14.51 -10.96; GWRE×13 yday $164.52 → 09:30 $162.01 -32.63; IOT×50 yday $41.74 → 09:30 $41.60 -7.00; NAUT×11 yday $1.77 → 09:30 $1.67 -1.10; PTC×11 yday $193.00 → 09:30 $193.20 +2.20 | — |
| 2026-10-07 09:30 ET | **SELL** | `ACRS` | 4 | $3.58 | $0.18 | $-4.05 | $54.46 | ▼ -4.05 after sell → book $8,372.03; vs 09:30 mark -0.18 | dropped from list after 3 sess (min 3) | — |
| 2026-10-07 09:30 ET | **SELL** | `AVPT` | 137 | $14.51 | $2.44 | $+28.04 | $2,039.90 | ▲ +28.04 after sell → book $8,369.60; vs 09:30 mark -2.43 | dropped from list after 4 sess (min 3) | — |
| 2026-10-07 09:30 ET | **SELL** | `NAUT` | 11 | $1.67 | $0.24 | $-0.40 | $2,058.03 | ▼ -0.40 after sell → book $8,369.36; vs 09:30 mark -0.24 | dropped from list after 3 sess (min 3) | — |
| 2026-10-07 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,058.03 | ▼ close $8,344.87 vs 09:30 $8,372.21 (session -24.49) | 16:00 close · cash $2,058.03 · equity $8,344.87 vs 09:30 $8,372.21 (-27.34; session marks -24.49) · 3 name(s) marked open→close (per-name table). GWRE×13 09:30 $162.01 → close $163.98 +25.61; IOT×50 09:30 $41.60 → close $40.51 -54.50; PTC×11 09:30 $193.20 → close $193.60 +4.40 | — |
| 2026-10-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,058.03 | ▲ 09:30 equity $8,344.87 vs yday $8,344.87 (+0.00) | 09:30 open · cash $2,058.03 (unchanged overnight, no fees) · equity $8,344.87 vs prior close $8,344.87 (+0.00) · 3 name(s) re-marked at the open (per-name table). GWRE×13 yday $163.98 → 09:30 $163.98 +0.00; IOT×50 yday $40.51 → 09:30 $40.51 +0.00; PTC×11 yday $193.60 → 09:30 $193.60 +0.00 | — |
| 2026-10-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,058.03 | ▲ close $8,344.87 vs 09:30 $8,344.87 (session +0.00) | 16:00 close · cash $2,058.03 · equity $8,344.87 vs 09:30 $8,344.87 (+0.00; session marks +0.00) · 3 name(s) marked open→close (per-name table). GWRE×13 09:30 $163.98 → close $163.98 +0.00; IOT×50 09:30 $40.51 → close $40.51 +0.00; PTC×11 09:30 $193.60 → close $193.60 +0.00 | — |
| 2026-10-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,058.03 | ▲ 09:30 equity $8,432.80 vs yday $8,344.87 (+87.93) | 09:30 open · cash $2,058.03 (unchanged overnight, no fees) · equity $8,432.80 vs prior close $8,344.87 (+87.93) · 3 name(s) re-marked at the open (per-name table). GWRE×13 yday $163.98 → 09:30 $165.90 +24.96; IOT×50 yday $40.51 → 09:30 $41.71 +60.00; PTC×11 yday $193.60 → 09:30 $193.87 +2.97 | — |
| 2026-10-09 09:30 ET | **SELL** | `GWRE` | 13 | $165.90 | $2.06 | $+7.61 | $4,212.67 | ▲ +7.61 after sell → book $8,430.74; vs 09:30 mark -2.06 | exit unpriced hold on first bar after 3 sess | — |
| 2026-10-09 09:30 ET | **SELL** | `IOT` | 50 | $41.71 | $2.17 | $-53.81 | $6,296.01 | ▼ -53.81 after sell → book $8,428.58; vs 09:30 mark -2.16 | exit unpriced hold on first bar after 3 sess | — |
| 2026-10-09 09:30 ET | **SELL** | `PTC` | 11 | $193.87 | $2.05 | $+5.50 | $8,426.53 | ▲ +5.50 after sell → book $8,426.53; vs 09:30 mark -2.05 | exit unpriced hold on first bar after 3 sess | — |
| 2026-10-09 09:30 ET | **BUY** | `PACS` | 48 | $43.75 | $2.13 | — | $6,324.39 | — | top 4 by cond; rank cond; list flatten; 🔵; ret5=+6.6; leftover $2106.63 | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-10-09 09:30 ET | **BUY** | `PCRX` | 57 | $36.33 | $2.16 | — | $4,251.42 | — | top 4 by cond; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+45.5; leftover $2106.63 | join🟢 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-09 09:30 ET | **BUY** | `ADCT` | 1333 | $1.58 | $17.20 | — | $2,128.09 | — | top 4 by cond; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+47.2; leftover $2106.63 | join🟡 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-09 09:30 ET | **BUY** | `ANGO` | 190 | $11.06 | $2.56 | — | $24.13 | — | top 4 by cond; rank cond; list yday_mover; 🔵; ret5=-25.7; leftover $2106.63 | join🟢 sector🟡 gen🔴 news🟡 digest🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $24.13 | ▲ close $8,667.09 vs 09:30 $8,432.80 (session +264.61) | 16:00 close · cash $24.13 · equity $8,667.09 vs 09:30 $8,432.80 (+234.29; session marks +264.61) · 4 name(s) marked open→close (per-name table). PACS×48 09:30 $43.75 → close $43.30 -21.60; PCRX×57 09:30 $36.33 → close $36.31 -1.14; ADCT×1333 09:30 $1.58 → close $1.73 +199.95; ANGO×190 09:30 $11.06 → close $11.52 +87.40 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `BTSG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `HIMS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `INO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `IREN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `BRUN` | cash | leftover split 6.67 < 1 share @ 26.25 |
| 2026-08-14 | `CLBT` | cash | leftover split 6.67 < 1 share @ 10.83 |
| 2026-08-14 | `HLIT` | cash | leftover split 6.67 < 1 share @ 13.18 |
| 2026-08-14 | `MNTN` | cash | leftover split 6.67 < 1 share @ 12.50 |
| 2026-08-17 | `BTSG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `HIMS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `INO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `IREN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `LPTH` | cash | leftover split 6.67 < 1 share @ 14.94 |
| 2026-08-17 | `DVN` | cash | leftover split 6.67 < 1 share @ 46.18 |
| 2026-08-17 | `EOG` | cash | leftover split 6.67 < 1 share @ 142.77 |
| 2026-08-18 | `VERI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `RLX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `VERI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ADI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BABA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BHP` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 10.54 < 1 share @ 119.43 |
| 2026-08-21 | `AUPH` | cash | leftover split 10.54 < 1 share @ 17.20 |
| 2026-08-21 | `AEM` | cash | leftover split 10.54 < 1 share @ 216.30 |
| 2026-08-21 | `ARCT` | cash | leftover split 10.54 < 1 share @ 11.13 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ERO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ALM` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-26 | `AU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ERO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `FCX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `CNH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `FNV` | cash | leftover split 24.08 < 1 share @ 267.02 |
| 2026-08-26 | `MOS` | cash | leftover split 24.08 < 1 share @ 24.84 |
| 2026-08-26 | `CM` | cash | leftover split 24.08 < 1 share @ 118.50 |
| 2026-08-26 | `FIGR` | cash | leftover split 24.08 < 1 share @ 40.50 |
| 2026-08-27 | `AU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ERO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `FCX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CNH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ACMR` | cash | leftover split 24.08 < 1 share @ 81.65 |
| 2026-08-27 | `MT` | cash | leftover split 24.08 < 1 share @ 74.54 |
| 2026-08-27 | `MU` | cash | leftover split 24.08 < 1 share @ 967.01 |
| 2026-08-28 | `GGB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `GGB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `KEYS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CIEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `MPWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TYL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ACIW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `KEYS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CIEN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `MPWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `NOV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NVDA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `RES` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `ALAB` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ACB` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ADM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ASND` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `HRMY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ATRC` | cash | leftover split 9.24 < 1 share @ 52.03 |
| 2026-09-08 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `HRMY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CABA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `ALEC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `BHC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `LOGI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `VNT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMCI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `CABA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `ALEC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `BHC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `GLW` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SWKS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `LITE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `OCC` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `BKV` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CVI` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `UGP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `BAND` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `ORCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `PAGS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `SWKS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `AVT` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `BE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DK` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `BAND` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `ORCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `PAGS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `SWKS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `CRWD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `PANW` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `S` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `QRVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `ATRC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `WAY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `QRVO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `WAY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `BULL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `CIFR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `SABR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `CRWD` | cash | leftover split 51.14 < 1 share @ 246.98 |
| 2026-09-18 | `ECO` | cash | leftover split 51.14 < 1 share @ 85.00 |
| 2026-09-21 | `BULL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `CIFR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `SABR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `FIVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `SABR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `SMTC` | no_price | no 09:30 open — carry |
| 2026-09-22 | `FIVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `VICR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `COHR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `FORM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `META` | cash | leftover split 447.90 < 1 share @ 731.40 |
| 2026-09-22 | `ARHS` | no_price | no 09:30 open |
| 2026-09-23 | `VICR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `COHR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `FORM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `ARM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `DEFT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `ARM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `DEFT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `A` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `BFLY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `DXCM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `HALO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `EGHT` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `FSLY` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `KVYO` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `OKTA` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ARM` | 1 | 2026-09-22 @ $319.41 | top 4 by cond; rank cond; list yday_gainer,yday_mover; ret5=+35.1; leftover $447.90 |
| `DEFT` | 772 | 2026-09-22 @ $0.58 | top 4 by cond; rank cond; list probable,yday_gainer,yday_mover; ret5=-1.7; leftover $447.90 |
| `A` | 3 | 2026-09-23 @ $166.54 | top 4 by cond; rank cond; list flatten; 🔵; ⚪; ret5=+10.3; leftover $547.00 |
| `BFLY` | 55 | 2026-09-23 @ $9.90 | top 4 by cond; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+27.3; leftover $547.00 |
| `DXCM` | 6 | 2026-09-23 @ $89.50 | top 4 by cond; rank cond; list flatten; 🔵; ⚪; ret5=+5.3; leftover $547.00 |
| `HALO` | 4 | 2026-09-23 @ $116.85 | top 4 by cond; rank cond; list flatten; 🔵; ⚪; ret5=+3.3; leftover $547.00 |
