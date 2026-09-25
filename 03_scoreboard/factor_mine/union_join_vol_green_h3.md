# Factor mine action — `union_join_vol_green_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **+3.35%** ($10,335) · signal-only (no cash/fees) was +58.93%. Starts YES **21/30**. Fills 167 · skips 230 · realized $-9.83.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: the join camera (do several factors agree?) is green.
- Must-have: the volume camera (is this name unusually active?) is green.
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
- **Gate** `join=good,vol=good,last_green=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $8,762.76.

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
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | — | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 833 | $1.50 | $10.75 | — | $8,739.75 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `BETR` | 84 | $14.80 | $2.24 | — | $7,494.31 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten; 🔵; ⚪; ret5=-9.9; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 290 | $4.31 | $3.74 | — | $6,240.67 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 299 | $4.18 | $3.86 | — | $4,986.99 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 75 | $16.50 | $2.21 | — | $3,747.28 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 112 | $11.12 | $2.33 | — | $2,499.51 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `NCMI` | 464 | $2.69 | $5.99 | — | $1,245.37 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=-33.5; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `QMLS` | 170 | $7.29 | $2.50 | — | $3.57 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.1; leftover $1250.00 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.57 | ▼ close $9,801.97 vs 09:30 $10,000.00 (session -164.42) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.57 | ▼ 09:30 equity $9,759.50 vs yday $9,801.97 (-42.47) | — | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.57 | ▲ close $9,785.73 vs 09:30 $9,759.50 (session +26.23) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.57 | ▼ 09:30 equity $9,552.99 vs yday $9,785.73 (-232.74) | — | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.57 | ▼ close $9,361.35 vs 09:30 $9,552.99 (session -191.64) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.57 | ▼ 09:30 equity $9,353.77 vs yday $9,361.35 (-7.58) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 833 | $1.42 | $10.89 | $-88.28 | $1,175.53 | ▼ -88.28 after sell → book $9,342.87; vs 09:30 mark -10.90 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BETR` | 84 | $13.03 | $2.27 | $-153.19 | $2,267.79 | ▼ -153.19 after sell → book $9,340.61; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ANGX` | 290 | $4.79 | $3.80 | $+131.66 | $3,653.09 | ▲ +131.66 after sell → book $9,336.81; vs 09:30 mark -3.80 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `HYLN` | 299 | $3.87 | $3.92 | $-100.46 | $4,806.30 | ▼ -100.46 after sell → book $9,332.89; vs 09:30 mark -3.92 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ADUR` | 75 | $15.65 | $2.24 | $-68.20 | $5,977.81 | ▼ -68.20 after sell → book $9,330.65; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `AIRO` | 112 | $9.10 | $2.35 | $-230.92 | $6,994.66 | ▼ -230.92 after sell → book $9,328.30; vs 09:30 mark -2.35 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `NCMI` | 464 | $2.56 | $6.07 | $-72.38 | $8,176.43 | ▼ -72.38 after sell → book $9,322.23; vs 09:30 mark -6.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `QMLS` | 170 | $6.74 | $2.54 | $-98.54 | $9,319.69 | ▼ -98.54 after sell → book $9,319.69; vs 09:30 mark -2.54 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,319.69 | ▲ close $9,319.69 vs 09:30 $9,353.77 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,319.69 | ▲ 09:30 equity $9,319.69 vs yday $9,319.69 (-0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 56 | $20.55 | $2.16 | — | $8,166.73 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1164.96 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 56 | $20.65 | $2.16 | — | $7,008.17 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1164.96 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 201 | $5.77 | $2.60 | — | $5,845.80 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1164.96 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 59 | $19.63 | $2.17 | — | $4,685.47 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1164.96 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 39 | $29.63 | $2.11 | — | $3,527.79 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1164.96 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 665 | $1.75 | $8.58 | — | $2,355.46 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1164.96 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $1,197.13 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1164.96 | — |
| 2026-08-20 09:30 ET | **BUY** | `ABUS` | 236 | $4.92 | $3.04 | — | $32.96 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1164.96 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $32.96 | ▲ close $9,448.07 vs 09:30 $9,319.69 (session +153.21) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $32.96 | ▲ 09:30 equity $9,775.84 vs yday $9,448.07 (+327.77) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 3 | $1.32 | $0.05 | — | $28.95 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $4.12 | — |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 2 | $1.66 | $0.04 | — | $25.60 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $4.12 | — |
| 2026-08-21 09:30 ET | **BUY** | `INDP` | 2 | $1.39 | $0.03 | — | $22.78 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+30.2; leftover $4.12 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $22.78 | ▼ close $9,759.97 vs 09:30 $9,775.84 (session -15.75) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $22.78 | ▲ 09:30 equity $9,845.87 vs yday $9,759.97 (+85.90) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $22.78 | ▼ close $9,823.08 vs 09:30 $9,845.87 (session -22.79) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $22.78 | ▼ 09:30 equity $9,695.00 vs yday $9,823.08 (-128.08) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 56 | $20.32 | $2.18 | $-17.22 | $1,158.52 | ▼ -17.22 after sell → book $9,692.82; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CDE` | 56 | $20.47 | $2.18 | $-14.42 | $2,302.67 | ▼ -14.42 after sell → book $9,690.65; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 201 | $5.53 | $2.64 | $-53.48 | $3,411.55 | ▼ -53.48 after sell → book $9,688.00; vs 09:30 mark -2.65 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 59 | $21.21 | $2.19 | $+88.87 | $4,660.76 | ▲ +88.87 after sell → book $9,685.82; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 39 | $32.32 | $2.13 | $+100.68 | $5,919.11 | ▲ +100.68 after sell → book $9,683.69; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 665 | $1.90 | $8.70 | $+82.47 | $7,173.91 | ▲ +82.47 after sell → book $9,674.99; vs 09:30 mark -8.70 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ABUS` | 236 | $5.25 | $3.09 | $+71.74 | $8,409.82 | ▲ +71.74 after sell → book $9,671.90; vs 09:30 mark -3.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 737 | $1.63 | $9.51 | — | $7,199.00 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; leftover $1201.40 | — |
| 2026-08-25 09:30 ET | **BUY** | `GORO` | 338 | $3.55 | $4.36 | — | $5,994.74 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+27.9; leftover $1201.40 | — |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 188 | $6.37 | $2.55 | — | $4,794.63 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ⚪; ret5=+10.9; leftover $1201.40 | — |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 34 | $35.05 | $2.09 | — | $3,600.83 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ⚪; ret5=+19.7; leftover $1201.40 | — |
| 2026-08-25 09:30 ET | **BUY** | `ETON` | 18 | $64.55 | $2.04 | — | $2,436.89 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+4.4; leftover $1201.40 | — |
| 2026-08-25 09:30 ET | **BUY** | `SUZ` | 133 | $8.98 | $2.39 | — | $1,240.16 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot,mover_buy; ⚪; ret5=+15.4; leftover $1201.40 | — |
| 2026-08-25 09:30 ET | **BUY** | `IAUX` | 632 | $1.90 | $8.15 | — | $31.21 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+16.4; leftover $1201.40 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $31.21 | ▲ close $9,869.62 vs 09:30 $9,695.00 (session +228.82) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $31.21 | ▼ 09:30 equity $9,790.25 vs yday $9,869.62 (-79.37) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `WPM` | 8 | $160.93 | $2.03 | $+127.07 | $1,316.61 | ▲ +127.07 after sell → book $9,788.22; vs 09:30 mark -2.03 | dropped from list after 4 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 3 | $1.60 | $0.08 | $+0.71 | $1,321.34 | ▲ +0.71 after sell → book $9,788.14; vs 09:30 mark -0.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `BTBT` | 2 | $1.53 | $0.06 | $-0.36 | $1,324.34 | ▼ -0.36 after sell → book $9,788.09; vs 09:30 mark -0.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `INDP` | 2 | $1.09 | $0.05 | $-0.68 | $1,326.47 | ▼ -0.68 after sell → book $9,788.04; vs 09:30 mark -0.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 227 | $5.81 | $2.93 | — | $4.67 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; 🔵; ⚪; ret5=+117.2; leftover $1326.47 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.67 | ▼ close $9,608.74 vs 09:30 $9,790.25 (session -176.36) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.67 | ▲ 09:30 equity $9,734.61 vs yday $9,608.74 (+125.87) | — | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.67 | ▲ close $10,158.66 vs 09:30 $9,734.61 (session +424.05) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.67 | ▼ 09:30 equity $9,995.57 vs yday $10,158.66 (-163.09) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `BMEA` | 737 | $1.69 | $9.64 | $+25.07 | $1,240.56 | ▲ +25.07 after sell → book $9,985.93; vs 09:30 mark -9.64 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `GORO` | 338 | $3.80 | $4.43 | $+75.71 | $2,520.54 | ▲ +75.71 after sell → book $9,981.51; vs 09:30 mark -4.42 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ZURA` | 188 | $5.88 | $2.60 | $-97.27 | $3,623.38 | ▼ -97.27 after sell → book $9,978.91; vs 09:30 mark -2.60 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `EZPW` | 34 | $34.50 | $2.11 | $-22.90 | $4,794.27 | ▼ -22.90 after sell → book $9,976.80; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ETON` | 18 | $61.98 | $2.06 | $-50.37 | $5,907.85 | ▼ -50.37 after sell → book $9,974.74; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `SUZ` | 133 | $9.05 | $2.42 | $+4.50 | $7,109.08 | ▲ +4.50 after sell → book $9,972.32; vs 09:30 mark -2.42 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `IAUX` | 632 | $1.93 | $8.27 | $+2.54 | $8,320.57 | ▲ +2.54 after sell → book $9,964.05; vs 09:30 mark -8.27 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 14 | $146.07 | $2.03 | — | $6,273.56 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+38.8; leftover $2080.14 | — |
| 2026-08-28 09:30 ET | **BUY** | `NCNO` | 89 | $23.30 | $2.26 | — | $4,197.60 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+14.5; leftover $2080.14 | — |
| 2026-08-28 09:30 ET | **BUY** | `TH` | 109 | $19.00 | $2.32 | — | $2,124.28 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; ret5=+7.5; leftover $2080.14 | — |
| 2026-08-28 09:30 ET | **BUY** | `GAP` | 84 | $24.69 | $2.24 | — | $48.08 | — | combo gate; gate join=good,vol=good,last_green=True; list earn_react; ret5=+5.8; leftover $2080.14 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $48.08 | ▼ close $9,700.86 vs 09:30 $9,995.57 (session -254.34) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $48.08 | ▼ 09:30 equity $9,577.70 vs yday $9,700.86 (-123.16) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `USDE` | 227 | $6.76 | $2.98 | $+209.74 | $1,579.62 | ▲ +209.74 after sell → book $9,574.73; vs 09:30 mark -2.97 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,579.62 | ▼ close $9,486.86 vs 09:30 $9,577.70 (session -87.87) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,579.62 | ▼ 09:30 equity $9,402.22 vs yday $9,486.86 (-84.64) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,579.62 | ▼ close $9,351.47 vs 09:30 $9,402.22 (session -50.75) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,579.62 | ▼ 09:30 equity $9,315.82 vs yday $9,351.47 (-35.65) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `ANF` | 14 | $139.65 | $2.06 | $-93.97 | $3,532.66 | ▼ -93.97 after sell → book $9,313.76; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `NCNO` | 89 | $22.20 | $2.29 | $-102.44 | $5,506.18 | ▼ -102.44 after sell → book $9,311.48; vs 09:30 mark -2.28 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `TH` | 109 | $17.98 | $2.35 | $-115.85 | $7,463.65 | ▼ -115.85 after sell → book $9,309.13; vs 09:30 mark -2.35 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `GAP` | 84 | $21.97 | $2.27 | $-232.99 | $9,306.85 | ▼ -232.99 after sell → book $9,306.85; vs 09:30 mark -2.28 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,306.85 | ▲ close $9,306.85 vs 09:30 $9,315.82 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,306.85 | ▲ 09:30 equity $9,306.85 vs yday $9,306.85 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 8 | $132.45 | $2.01 | — | $8,245.24 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1163.36 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 69 | $16.77 | $2.20 | — | $7,085.91 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1163.36 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 533 | $2.18 | $6.88 | — | $5,917.10 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $1163.36 | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 48 | $23.88 | $2.13 | — | $4,768.72 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1163.36 | — |
| 2026-09-03 09:30 ET | **BUY** | `NVAX` | 111 | $10.42 | $2.32 | — | $3,609.78 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+12.1; leftover $1163.36 | — |
| 2026-09-03 09:30 ET | **BUY** | `BMEA` | 602 | $1.93 | $7.77 | — | $2,440.16 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1163.36 | — |
| 2026-09-03 09:30 ET | **BUY** | `DUOL` | 7 | $161.54 | $2.01 | — | $1,307.36 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ret5=+12.0; leftover $1163.36 | — |
| 2026-09-03 09:30 ET | **BUY** | `ALMS` | 112 | $10.38 | $2.33 | — | $143.04 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; 🔵; ret5=-56.2; leftover $1163.36 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $143.04 | ▼ close $9,238.94 vs 09:30 $9,306.85 (session -40.27) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $143.04 | ▼ 09:30 equity $9,225.25 vs yday $9,238.94 (-13.69) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `BRR` | 7 | $2.51 | $0.20 | — | $125.27 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ⚪; ret5=+21.8; leftover $17.88 | — |
| 2026-09-04 09:30 ET | **BUY** | `DFDV` | 3 | $5.79 | $0.18 | — | $107.72 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ⚪; ret5=+15.2; leftover $17.88 | — |
| 2026-09-04 09:30 ET | **BUY** | `AHCO` | 2 | $6.32 | $0.13 | — | $94.95 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+10.7; leftover $17.88 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $94.95 | ▲ close $9,248.92 vs 09:30 $9,225.25 (session +24.18) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $94.95 | ▼ 09:30 equity $9,154.08 vs yday $9,248.92 (-94.84) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $94.95 | ▼ close $9,044.78 vs 09:30 $9,154.08 (session -109.30) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $94.95 | ▼ 09:30 equity $8,988.86 vs yday $9,044.78 (-55.92) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `RVTY` | 8 | $125.77 | $2.03 | $-57.49 | $1,099.07 | ▼ -57.49 after sell → book $8,986.82; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `ARCT` | 69 | $15.46 | $2.22 | $-94.81 | $2,163.59 | ▼ -94.81 after sell → book $8,984.60; vs 09:30 mark -2.22 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CRDL` | 533 | $2.22 | $6.97 | $+7.47 | $3,339.88 | ▲ +7.47 after sell → book $8,977.63; vs 09:30 mark -6.97 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MMED` | 48 | $23.22 | $2.15 | $-35.97 | $4,452.29 | ▼ -35.97 after sell → book $8,975.48; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `NVAX` | 111 | $10.02 | $2.35 | $-49.07 | $5,562.15 | ▼ -49.07 after sell → book $8,973.12; vs 09:30 mark -2.36 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `BMEA` | 602 | $1.94 | $7.88 | $-9.62 | $6,722.16 | ▼ -9.62 after sell → book $8,965.25; vs 09:30 mark -7.87 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `DUOL` | 7 | $145.58 | $2.03 | $-115.76 | $7,739.19 | ▼ -115.76 after sell → book $8,963.22; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `ALMS` | 112 | $10.49 | $2.35 | $+8.20 | $8,911.71 | ▲ +8.20 after sell → book $8,960.86; vs 09:30 mark -2.36 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,911.71 | ▼ close $8,959.45 vs 09:30 $8,988.86 (session -1.41) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,911.71 | ▼ 09:30 equity $8,958.96 vs yday $8,959.45 (-0.49) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `BRR` | 7 | $2.87 | $0.24 | $+2.08 | $8,931.56 | ▲ +2.08 after sell → book $8,958.72; vs 09:30 mark -0.24 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `DFDV` | 3 | $5.22 | $0.19 | $-2.08 | $8,947.04 | ▼ -2.08 after sell → book $8,958.54; vs 09:30 mark -0.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `AHCO` | 2 | $5.75 | $0.14 | $-1.41 | $8,958.39 | ▼ -1.41 after sell → book $8,958.39; vs 09:30 mark -0.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,958.39 | ▲ close $8,958.39 vs 09:30 $8,958.96 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,958.39 | ▲ 09:30 equity $8,958.39 vs yday $8,958.39 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `TYRA` | 47 | $23.63 | $2.13 | — | $7,845.65 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; ret5=-6.3; leftover $1119.80 | — |
| 2026-09-11 09:30 ET | **BUY** | `INDP` | 414 | $2.70 | $5.34 | — | $6,722.51 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+118.8; leftover $1119.80 | — |
| 2026-09-11 09:30 ET | **BUY** | `WLTH` | 102 | $10.95 | $2.30 | — | $5,603.32 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+20.8; leftover $1119.80 | — |
| 2026-09-11 09:30 ET | **BUY** | `BNC` | 228 | $4.91 | $2.94 | — | $4,480.90 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+76.3; leftover $1119.80 | — |
| 2026-09-11 09:30 ET | **BUY** | `SWKS` | 13 | $84.27 | $2.03 | — | $3,383.36 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot; ret5=+17.2; leftover $1119.80 | — |
| 2026-09-11 09:30 ET | **BUY** | `ASO` | 20 | $54.91 | $2.05 | — | $2,283.11 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ret5=+24.3; leftover $1119.80 | — |
| 2026-09-11 09:30 ET | **BUY** | `IRD` | 181 | $6.16 | $2.53 | — | $1,165.61 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ret5=+36.4; leftover $1119.80 | — |
| 2026-09-11 09:30 ET | **BUY** | `COO` | 20 | $54.66 | $2.05 | — | $70.36 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; ret5=-22.3; leftover $1119.80 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $70.36 | ▼ close $8,832.90 vs 09:30 $8,958.39 (session -104.12) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $70.36 | ▲ 09:30 equity $8,915.38 vs yday $8,832.90 (+82.48) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $70.36 | ▲ close $9,116.82 vs 09:30 $8,915.38 (session +201.44) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $70.36 | ▲ 09:30 equity $9,162.67 vs yday $9,116.82 (+45.85) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $70.36 | ▲ close $9,298.37 vs 09:30 $9,162.67 (session +135.70) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $70.36 | ▼ 09:30 equity $9,292.00 vs yday $9,298.37 (-6.37) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `TYRA` | 47 | $25.58 | $2.15 | $+87.37 | $1,270.47 | ▲ +87.37 after sell → book $9,289.85; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `INDP` | 414 | $3.66 | $5.42 | $+386.68 | $2,780.29 | ▲ +386.68 after sell → book $9,284.43; vs 09:30 mark -5.42 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `WLTH` | 102 | $10.82 | $2.32 | $-17.88 | $3,881.61 | ▼ -17.88 after sell → book $9,282.11; vs 09:30 mark -2.32 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `BNC` | 228 | $4.77 | $2.99 | $-37.85 | $4,966.18 | ▼ -37.85 after sell → book $9,279.12; vs 09:30 mark -2.99 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `ASO` | 20 | $50.69 | $2.07 | $-88.52 | $5,977.91 | ▼ -88.52 after sell → book $9,277.05; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `IRD` | 181 | $5.80 | $2.57 | $-70.27 | $7,025.14 | ▼ -70.27 after sell → book $9,274.48; vs 09:30 mark -2.57 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `COO` | 20 | $54.37 | $2.07 | $-9.92 | $8,110.47 | ▼ -9.92 after sell → book $9,272.41; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 15 | $77.12 | $2.04 | — | $6,951.63 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,ohlc_hot; ret5=+7.2; leftover $1158.64 | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 197 | $5.87 | $2.58 | — | $5,792.66 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $1158.64 | — |
| 2026-09-16 09:30 ET | **BUY** | `VAL` | 13 | $87.40 | $2.03 | — | $4,654.43 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ret5=+3.2; leftover $1158.64 | — |
| 2026-09-16 09:30 ET | **BUY** | `ADPT` | 42 | $27.09 | $2.12 | — | $3,514.54 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.6; leftover $1158.64 | — |
| 2026-09-16 09:30 ET | **BUY** | `SDGR` | 49 | $23.29 | $2.14 | — | $2,371.19 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ret5=+16.1; leftover $1158.64 | — |
| 2026-09-16 09:30 ET | **BUY** | `FPS` | 34 | $33.14 | $2.09 | — | $1,242.34 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ret5=-2.9; leftover $1158.64 | — |
| 2026-09-16 09:30 ET | **BUY** | `CAI` | 41 | $28.16 | $2.11 | — | $85.66 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot; ret5=+14.8; leftover $1158.64 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $85.66 | ▼ close $9,175.05 vs 09:30 $9,292.00 (session -82.25) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $85.66 | ▲ 09:30 equity $9,329.31 vs yday $9,175.05 (+154.26) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `SWKS` | 13 | $86.76 | $2.05 | $+28.29 | $1,211.49 | ▲ +28.29 after sell → book $9,327.26; vs 09:30 mark -2.05 | dropped from list after 4 sess (min 3) | — |
| 2026-09-17 09:30 ET | **BUY** | `RVTY` | 1 | $147.61 | $1.48 | — | $1,062.41 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,ohlc_hot; ret5=+17.7; leftover $173.07 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 22 | $7.59 | $1.74 | — | $893.69 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $173.07 | — |
| 2026-09-17 09:30 ET | **BUY** | `ARQT` | 6 | $25.95 | $1.57 | — | $736.41 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $173.07 | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 1 | $170.85 | $1.71 | — | $563.85 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $173.07 | — |
| 2026-09-17 09:30 ET | **BUY** | `CIFR` | 9 | $18.04 | $1.65 | — | $399.89 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-1.1; leftover $173.07 | — |
| 2026-09-17 09:30 ET | **BUY** | `BRKR` | 2 | $61.90 | $1.24 | — | $274.84 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot; 🔵; ret5=+11.5; leftover $173.07 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $274.84 | ▲ close $9,827.21 vs 09:30 $9,329.31 (session +509.36) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $274.84 | ▼ 09:30 equity $9,821.81 vs yday $9,827.21 (-5.40) | — | — |
| 2026-09-18 09:30 ET | **BUY** | `EYPT` | 9 | $3.95 | $0.38 | — | $238.91 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.1; leftover $39.26 | — |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 2 | $14.07 | $0.29 | — | $210.48 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $39.26 | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 2 | $14.79 | $0.30 | — | $180.60 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $39.26 | — |
| 2026-09-18 09:30 ET | **BUY** | `CYPH` | 12 | $3.04 | $0.40 | — | $143.78 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+39.5; leftover $39.26 | — |
| 2026-09-18 09:30 ET | **BUY** | `VITL` | 3 | $11.38 | $0.35 | — | $109.29 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+19.5; leftover $39.26 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $109.29 | ▼ close $9,752.80 vs 09:30 $9,821.81 (session -67.29) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $109.29 | ▲ 09:30 equity $9,821.72 vs yday $9,752.80 (+68.92) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `RDNT` | 15 | $76.27 | $2.06 | $-16.84 | $1,251.29 | ▼ -16.84 after sell → book $9,819.66; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `RIG` | 197 | $5.62 | $2.62 | $-54.45 | $2,355.80 | ▼ -54.45 after sell → book $9,817.04; vs 09:30 mark -2.62 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `VAL` | 13 | $83.46 | $2.05 | $-55.30 | $3,438.73 | ▼ -55.30 after sell → book $9,814.99; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `ADPT` | 42 | $28.69 | $2.14 | $+62.95 | $4,641.58 | ▲ +62.95 after sell → book $9,812.85; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `SDGR` | 49 | $29.43 | $2.16 | $+296.56 | $6,081.49 | ▲ +296.56 after sell → book $9,810.69; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `FPS` | 34 | $40.03 | $2.11 | $+230.06 | $7,440.40 | ▲ +230.06 after sell → book $9,808.58; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `CAI` | 41 | $30.23 | $2.13 | $+80.62 | $8,677.69 | ▲ +80.62 after sell → book $9,806.45; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 6 | $157.87 | $2.01 | — | $7,728.47 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten; ret5=+6.5; leftover $1084.71 | — |
| 2026-09-21 09:30 ET | **BUY** | `DXCM` | 12 | $88.83 | $2.03 | — | $6,660.48 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten; ret5=+7.6; leftover $1084.71 | — |
| 2026-09-21 09:30 ET | **BUY** | `BKKT` | 116 | $9.31 | $2.34 | — | $5,578.18 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; ret5=+5.3; leftover $1084.71 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTDR` | 80 | $13.47 | $2.23 | — | $4,497.95 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+8.4; leftover $1084.71 | — |
| 2026-09-21 09:30 ET | **BUY** | `SBET` | 108 | $9.99 | $2.31 | — | $3,416.72 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ret5=+5.3; leftover $1084.71 | — |
| 2026-09-21 09:30 ET | **BUY** | `TJGC` | 64 | $16.91 | $2.18 | — | $2,332.30 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+50.5; leftover $1084.71 | — |
| 2026-09-21 09:30 ET | **BUY** | `USDE` | 83 | $13.05 | $2.24 | — | $1,246.91 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+40.4; leftover $1084.71 | — |
| 2026-09-21 09:30 ET | **BUY** | `GEMI` | 188 | $5.75 | $2.55 | — | $162.41 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+30.3; leftover $1084.71 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $162.41 | ▲ close $9,801.20 vs 09:30 $9,821.72 (session +12.64) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $162.41 | ▲ 09:30 equity $9,836.30 vs yday $9,801.20 (+35.10) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `CIFR` | 9 | $18.51 | $1.71 | $+0.91 | $327.29 | ▲ +0.91 after sell → book $9,834.59; vs 09:30 mark -1.71 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **BUY** | `FSLY` | 1 | $28.02 | $0.28 | — | $298.99 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover,ohlc_hot; ret5=+10.3; leftover $54.55 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $298.99 | ▲ close $9,897.15 vs 09:30 $9,836.30 (session +62.84) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $298.99 | ▼ 09:30 equity $9,870.34 vs yday $9,897.15 (-26.81) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `RVTY` | 1 | $142.40 | $1.45 | $-8.14 | $439.94 | ▼ -8.14 after sell → book $9,868.89; vs 09:30 mark -1.45 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `PGEN` | 22 | $7.95 | $1.83 | $+4.35 | $613.00 | ▲ +4.35 after sell → book $9,867.05; vs 09:30 mark -1.84 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `ARQT` | 6 | $27.79 | $1.71 | $+7.76 | $778.04 | ▲ +7.76 after sell → book $9,865.35; vs 09:30 mark -1.70 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `SMTC` | 1 | $174.50 | $1.77 | $+0.17 | $950.77 | ▲ +0.17 after sell → book $9,863.58; vs 09:30 mark -1.77 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `BRKR` | 2 | $60.70 | $1.24 | $-4.88 | $1,070.93 | ▼ -4.88 after sell → book $9,862.34; vs 09:30 mark -1.24 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `EYPT` | 9 | $4.10 | $0.42 | $+0.55 | $1,107.42 | ▲ +0.55 after sell → book $9,861.93; vs 09:30 mark -0.41 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `BHVN` | 2 | $14.84 | $0.32 | $+0.93 | $1,136.77 | ▲ +0.93 after sell → book $9,861.60; vs 09:30 mark -0.33 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `RARE` | 2 | $15.40 | $0.33 | $+0.58 | $1,167.24 | ▲ +0.58 after sell → book $9,861.27; vs 09:30 mark -0.33 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `CYPH` | 12 | $3.82 | $0.51 | $+8.51 | $1,212.56 | ▲ +8.51 after sell → book $9,860.75; vs 09:30 mark -0.52 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `VITL` | 3 | $11.17 | $0.36 | $-1.34 | $1,245.71 | ▼ -1.34 after sell → book $9,860.39; vs 09:30 mark -0.36 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 10 | $20.65 | $2.02 | — | $1,037.19 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $207.62 | — |
| 2026-09-23 09:30 ET | **BUY** | `SGRY` | 13 | $15.72 | $2.03 | — | $830.80 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $207.62 | — |
| 2026-09-23 09:30 ET | **BUY** | `TNGX` | 8 | $25.40 | $2.01 | — | $625.59 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.0; leftover $207.62 | — |
| 2026-09-23 09:30 ET | **BUY** | `NMRA` | 270 | $0.77 | $2.88 | — | $415.34 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.3; leftover $207.62 | — |
| 2026-09-23 09:30 ET | **BUY** | `VKTX` | 4 | $41.76 | $1.68 | — | $246.62 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+36.4; leftover $207.62 | — |
| 2026-09-23 09:30 ET | **BUY** | `BFLY` | 20 | $9.90 | $2.04 | — | $46.58 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+27.3; leftover $207.62 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $46.58 | ▲ close $9,943.97 vs 09:30 $9,870.34 (session +96.25) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $46.58 | ▼ 09:30 equity $9,920.64 vs yday $9,943.97 (-23.33) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `A` | 6 | $163.95 | $2.03 | $+32.44 | $1,028.25 | ▲ +32.44 after sell → book $9,918.61; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `DXCM` | 12 | $87.67 | $2.05 | $-17.93 | $2,078.31 | ▼ -17.93 after sell → book $9,916.57; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `BKKT` | 116 | $8.67 | $2.37 | $-78.95 | $3,081.66 | ▼ -78.95 after sell → book $9,914.20; vs 09:30 mark -2.37 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `BTDR` | 80 | $12.26 | $2.25 | $-101.68 | $4,060.21 | ▼ -101.68 after sell → book $9,911.95; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `SBET` | 108 | $9.80 | $2.34 | $-25.18 | $5,116.26 | ▼ -25.18 after sell → book $9,909.60; vs 09:30 mark -2.35 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `TJGC` | 64 | $24.03 | $2.20 | $+451.29 | $6,651.98 | ▲ +451.29 after sell → book $9,907.40; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `USDE` | 83 | $12.76 | $2.26 | $-28.57 | $7,708.80 | ▼ -28.57 after sell → book $9,905.14; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `GEMI` | 188 | $5.62 | $2.60 | $-30.53 | $8,762.76 | ▼ -30.53 after sell → book $9,902.54; vs 09:30 mark -2.60 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,762.76 | ▼ close $9,897.42 vs 09:30 $9,920.64 (session -5.12) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,051.15 | ▲ 09:30 equity $10,211.32 vs yday $10,208.92 (+2.40) | 09:30 open · cash $9,051.15 (unchanged overnight, no fees) · equity $10,211.32 vs prior close $10,208.92 (+2.40) · 10 name(s) re-marked at the open (per-name table). FSLY×6 yday $26.68 → 09:30 $26.68 +0.00; GRPN×8 yday $20.89 → 09:30 $20.89 +0.00; MAZE×3 yday $26.21 → 09:30 $26.21 +0.00; NMRA×140 yday $0.70 → 09:30 $0.70 +0.00; OMER×5 yday $20.13 → 09:30 $20.61 +2.40; THO×2 yday $70.93 → 09:30 $70.93 +0.00; TNGX×4 yday $24.63 → 09:30 $24.63 +0.00; TTAN×1 yday $59.98 → 09:30 $59.98 +0.00; VKTX×2 yday $36.75 → 09:30 $36.75 +0.00; XXI×27 yday $6.63 → 09:30 $6.63 +0.00 | — |
| 2026-09-25 09:30 ET | **BUY** | `WRBY` | 43 | $26.27 | $2.12 | — | $7,919.42 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $1131.39 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ZSQR` | 293 | $3.86 | $3.78 | — | $6,784.66 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; leftover $1131.39 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TWST` | 6 | $184.00 | $2.01 | — | $5,678.65 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.3; leftover $1131.39 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 69 | $16.21 | $2.20 | — | $4,557.97 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; leftover $1131.39 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `GRAL` | 9 | $123.50 | $2.02 | — | $3,444.45 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+56.6; leftover $1131.39 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `QMCO` | 37 | $29.80 | $2.10 | — | $2,339.75 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+18.2; leftover $1131.39 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `CYPH` | 282 | $4.00 | $3.64 | — | $1,206.70 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+32.9; leftover $1131.39 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `CDNA` | 18 | $61.33 | $2.04 | — | $100.72 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+13.1; leftover $1131.39 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $100.72 | ▲ close $10,334.78 vs 09:30 $10,211.32 (session +143.36) | 16:00 close · cash $100.72 · equity $10,334.78 vs 09:30 $10,211.32 (+123.46; session marks +143.36) · 18 name(s) marked open→close (per-name table). FSLY×6 09:30 $26.68 → close $26.68 +0.00; GRPN×8 09:30 $20.89 → close $20.89 -0.00; MAZE×3 09:30 $26.21 → close $26.21 -0.00; NMRA×140 09:30 $0.70 → close $0.70 +0.00; OMER×5 09:30 $20.61 → close $20.08 -2.65; THO×2 09:30 $70.93 → close $70.93 +0.00; TNGX×4 09:30 $24.63 → close $24.63 -0.00; TTAN×1 09:30 $59.98 → close $59.98 -0.00; VKTX×2 09:30 $36.75 → close $36.75 +0.00; XXI×27 09:30 $6.63 → close $6.63 +0.00; WRBY×43 09:30 $26.27 → close $26.71 +18.92; ZSQR×293 09:30 $3.86 → close $3.78 -23.44; TWST×6 09:30 $184.00 → close $182.83 -7.02; SECZ×69 09:30 $16.21 → close $15.96 -17.25; GRAL×9 09:30 $123.50 → close $126.89 +30.51; QMCO×37 09:30 $29.80 → close $31.68 +69.56; CYPH×282 09:30 $4.00 → close $4.12 +32.43; CDNA×18 09:30 $61.33 → close $63.68 +42.30 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `BETR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `HYLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ADUR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `AIRO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `NCMI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `QMLS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ABX` | cash | leftover split 0.71 < 1 share @ 9.12 |
| 2026-08-17 | `ALOY` | cash | leftover split 0.71 < 1 share @ 14.66 |
| 2026-08-17 | `BORR` | cash | leftover split 0.71 < 1 share @ 4.59 |
| 2026-08-17 | `XHG` | cash | leftover split 0.71 < 1 share @ 4.19 |
| 2026-08-17 | `MP` | cash | leftover split 0.71 < 1 share @ 58.01 |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BETR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ANGX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `HYLN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ADUR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `AIRO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `NCMI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `QMLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ABUS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 4.12 < 1 share @ 119.43 |
| 2026-08-21 | `AUPH` | cash | leftover split 4.12 < 1 share @ 17.20 |
| 2026-08-21 | `AEM` | cash | leftover split 4.12 < 1 share @ 216.30 |
| 2026-08-21 | `ARCT` | cash | leftover split 4.12 < 1 share @ 11.13 |
| 2026-08-21 | `MRVI` | cash | leftover split 4.12 < 1 share @ 8.28 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ABUS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `INDP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TMC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ELMT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ERO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SCCO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `INDP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `GORO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ZURA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `EZPW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ETON` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `SUZ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `IAUX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `GORO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ZURA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `EZPW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ETON` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `SUZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `IAUX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `USDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `DKS` | cash | leftover split 4.67 < 1 share @ 128.73 |
| 2026-08-28 | `USDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `ANF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `NCNO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `TH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `GAP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `MRNA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TYL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `ANF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `NCNO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `TH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `GAP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MMED` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `NVAX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DUOL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ALMS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DELL` | cash | leftover split 17.88 < 1 share @ 513.78 |
| 2026-09-04 | `TARS` | cash | leftover split 17.88 < 1 share @ 82.70 |
| 2026-09-04 | `MDB` | cash | leftover split 17.88 < 1 share @ 378.34 |
| 2026-09-04 | `ASST` | cash | leftover split 17.88 < 1 share @ 25.18 |
| 2026-09-04 | `TDS` | cash | leftover split 17.88 < 1 share @ 37.44 |
| 2026-09-08 | `RVTY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `MMED` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `NVAX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `DUOL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ALMS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `BRR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `DFDV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `AHCO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `HOOD` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CRCL` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MRX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TRMD` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `BRR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `DFDV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `AHCO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-10 | `IRD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `NAUT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `INDP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `AMBA` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CMPS` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ARBE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `TYRA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `INDP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `WLTH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `BNC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `SWKS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `ASO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `IRD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `COO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `DELL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HPE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HPQ` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `INSP` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `GME` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DHT` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `AVT` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `TYRA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `INDP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `WLTH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `BNC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `SWKS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `ASO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `IRD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `COO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `RBRK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `NTSK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `S` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CRWD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `IOT` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SAFX` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ECO` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `RDNT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `RIG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `VAL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `ADPT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `SDGR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `CAI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `ILMN` | cash | leftover split 173.07 < 1 share @ 233.85 |
| 2026-09-18 | `RDNT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `RIG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `VAL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `ADPT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `FPS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `CAI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `PGEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `ARQT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `CIFR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `BRKR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `VICR` | cash | leftover split 39.26 < 1 share @ 219.62 |
| 2026-09-18 | `ECO` | cash | leftover split 39.26 < 1 share @ 85.00 |
| 2026-09-21 | `RVTY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `PGEN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `ARQT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `CIFR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `BRKR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `EYPT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `BHVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `RARE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `VITL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `RVTY` | no_price | no 09:30 open — carry |
| 2026-09-22 | `PGEN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `ARQT` | no_price | no 09:30 open — carry |
| 2026-09-22 | `SMTC` | no_price | no 09:30 open — carry |
| 2026-09-22 | `BRKR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `EYPT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `BHVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `RARE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `VITL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `A` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `DXCM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `BKKT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `BTDR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `SBET` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `TJGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `USDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `GEMI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `ARM` | cash | leftover split 54.55 < 1 share @ 319.41 |
| 2026-09-22 | `GRPN` | no_price | no 09:30 open |
| 2026-09-22 | `XXI` | no_price | no 09:30 open |
| 2026-09-22 | `META` | cash | leftover split 54.55 < 1 share @ 731.40 |
| 2026-09-22 | `THO` | no_price | no 09:30 open |
| 2026-09-23 | `BKKT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `BTDR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `SBET` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `TJGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `USDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `GEMI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `FSLY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `OMER` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `SGRY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `TNGX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `NMRA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `VKTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `BFLY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `CVE` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `QNC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `TLSA` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `GLND` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ACMR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `KVYO` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `VICR` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `FSLY` | 1 | 2026-09-22 @ $28.02 | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover,ohlc_hot; ret5=+10.3; leftover $54.55 |
| `OMER` | 10 | 2026-09-23 @ $20.65 | combo gate; gate join=good,vol=good,last_green=True; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $207.62 |
| `SGRY` | 13 | 2026-09-23 @ $15.72 | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $207.62 |
| `TNGX` | 8 | 2026-09-23 @ $25.40 | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.0; leftover $207.62 |
| `NMRA` | 270 | 2026-09-23 @ $0.77 | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.3; leftover $207.62 |
| `VKTX` | 4 | 2026-09-23 @ $41.76 | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+36.4; leftover $207.62 |
| `BFLY` | 20 | 2026-09-23 @ $9.90 | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+27.3; leftover $207.62 |
