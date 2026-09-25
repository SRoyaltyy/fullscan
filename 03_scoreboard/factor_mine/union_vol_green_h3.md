# Factor mine action — `union_vol_green_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **-7.06%** ($9,294) · signal-only (no cash/fees) was -2.67%. Starts YES **12/30**. Fills 185 · skips 272 · realized $-923.90.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
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
- **Gate** `vol=good,last_green=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $6,778.85.

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
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 833 | $1.50 | $10.75 | — | $8,739.75 | — | combo gate; gate vol=good,last_green=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `BETR` | 84 | $14.80 | $2.24 | — | $7,494.31 | — | combo gate; gate vol=good,last_green=True; list flatten; 🔵; ⚪; ret5=-9.9; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 290 | $4.31 | $3.74 | — | $6,240.67 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 299 | $4.18 | $3.86 | — | $4,986.99 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 75 | $16.50 | $2.21 | — | $3,747.28 | — | combo gate; gate vol=good,last_green=True; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 112 | $11.12 | $2.33 | — | $2,499.51 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `NCMI` | 464 | $2.69 | $5.99 | — | $1,245.37 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=-33.5; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `QMLS` | 170 | $7.29 | $2.50 | — | $3.57 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.1; leftover $1250.00 | — |
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
| 2026-08-20 09:30 ET | **BUY** | `AG` | 56 | $20.55 | $2.16 | — | $8,166.73 | — | combo gate; gate vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1164.96 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 56 | $20.65 | $2.16 | — | $7,008.17 | — | combo gate; gate vol=good,last_green=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1164.96 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 201 | $5.77 | $2.60 | — | $5,845.80 | — | combo gate; gate vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1164.96 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 59 | $19.63 | $2.17 | — | $4,685.47 | — | combo gate; gate vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1164.96 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 39 | $29.63 | $2.11 | — | $3,527.79 | — | combo gate; gate vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1164.96 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 665 | $1.75 | $8.58 | — | $2,355.46 | — | combo gate; gate vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1164.96 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $1,197.13 | — | combo gate; gate vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1164.96 | — |
| 2026-08-20 09:30 ET | **BUY** | `ABUS` | 236 | $4.92 | $3.04 | — | $32.96 | — | combo gate; gate vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1164.96 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $32.96 | ▲ close $9,448.07 vs 09:30 $9,319.69 (session +153.21) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $32.96 | ▲ 09:30 equity $9,775.84 vs yday $9,448.07 (+327.77) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 3 | $1.32 | $0.05 | — | $28.95 | — | combo gate; gate vol=good,last_green=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $4.12 | — |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 2 | $1.66 | $0.04 | — | $25.60 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $4.12 | — |
| 2026-08-21 09:30 ET | **BUY** | `GORO` | 1 | $3.11 | $0.03 | — | $22.45 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer; ret5=+7.1; leftover $4.12 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $22.45 | ▼ close $9,760.25 vs 09:30 $9,775.84 (session -15.47) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $22.45 | ▲ 09:30 equity $9,846.26 vs yday $9,760.25 (+86.01) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $22.45 | ▼ close $9,823.96 vs 09:30 $9,846.26 (session -22.30) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $22.45 | ▼ 09:30 equity $9,695.92 vs yday $9,823.96 (-128.04) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 56 | $20.32 | $2.18 | $-17.22 | $1,158.19 | ▼ -17.22 after sell → book $9,693.74; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CDE` | 56 | $20.47 | $2.18 | $-14.42 | $2,302.34 | ▼ -14.42 after sell → book $9,691.57; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 201 | $5.53 | $2.64 | $-53.48 | $3,411.22 | ▼ -53.48 after sell → book $9,688.92; vs 09:30 mark -2.65 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 59 | $21.21 | $2.19 | $+88.87 | $4,660.43 | ▲ +88.87 after sell → book $9,686.74; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 39 | $32.32 | $2.13 | $+100.68 | $5,918.78 | ▲ +100.68 after sell → book $9,684.61; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 665 | $1.90 | $8.70 | $+82.47 | $7,173.58 | ▲ +82.47 after sell → book $9,675.91; vs 09:30 mark -8.70 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `WPM` | 8 | $156.51 | $2.03 | $+91.71 | $8,423.63 | ▲ +91.71 after sell → book $9,673.88; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ABUS` | 236 | $5.25 | $3.09 | $+71.74 | $9,659.53 | ▲ +71.74 after sell → book $9,670.78; vs 09:30 mark -3.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `KURA` | 88 | $13.59 | $2.25 | — | $8,461.36 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1207.44 | — |
| 2026-08-25 09:30 ET | **BUY** | `CCOI` | 127 | $9.49 | $2.37 | — | $7,253.76 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer; 🔵; ret5=-11.5; leftover $1207.44 | — |
| 2026-08-25 09:30 ET | **BUY** | `LIFE` | 32 | $36.96 | $2.09 | — | $6,068.95 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer; 🔵; ret5=+4.4; leftover $1207.44 | — |
| 2026-08-25 09:30 ET | **BUY** | `ZIP` | 265 | $4.55 | $3.42 | — | $4,859.78 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer; 🔵; ret5=-6.6; leftover $1207.44 | — |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 740 | $1.63 | $9.55 | — | $3,644.04 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; leftover $1207.44 | — |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 603 | $2.00 | $7.78 | — | $2,430.26 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+15.0; leftover $1207.44 | — |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 230 | $5.24 | $2.97 | — | $1,222.09 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+35.3; leftover $1207.44 | — |
| 2026-08-25 09:30 ET | **BUY** | `SUJA` | 137 | $8.79 | $2.40 | — | $15.46 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.1; leftover $1207.44 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15.46 | ▲ close $9,760.52 vs 09:30 $9,695.92 (session +122.56) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15.46 | ▼ 09:30 equity $9,742.71 vs yday $9,760.52 (-17.81) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 3 | $1.60 | $0.08 | $+0.71 | $20.18 | ▲ +0.71 after sell → book $9,742.63; vs 09:30 mark -0.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `BTBT` | 2 | $1.53 | $0.06 | $-0.36 | $23.19 | ▼ -0.36 after sell → book $9,742.58; vs 09:30 mark -0.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `GORO` | 1 | $3.77 | $0.06 | $+0.57 | $26.90 | ▲ +0.57 after sell → book $9,742.52; vs 09:30 mark -0.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 4 | $5.81 | $0.24 | — | $3.41 | — | combo gate; gate vol=good,last_green=True; list yday_mover; 🔵; ⚪; ret5=+117.2; leftover $26.90 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.41 | ▼ close $9,626.04 vs 09:30 $9,742.71 (session -116.23) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.41 | ▼ 09:30 equity $9,623.02 vs yday $9,626.04 (-3.02) | — | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.41 | ▼ close $9,560.82 vs 09:30 $9,623.02 (session -62.20) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.41 | ▼ 09:30 equity $9,542.95 vs yday $9,560.82 (-17.87) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `KURA` | 88 | $13.05 | $2.28 | $-52.05 | $1,149.53 | ▼ -52.05 after sell → book $9,540.67; vs 09:30 mark -2.28 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CCOI` | 127 | $9.70 | $2.40 | $+21.90 | $2,379.03 | ▲ +21.90 after sell → book $9,538.27; vs 09:30 mark -2.40 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `LIFE` | 32 | $39.60 | $2.11 | $+80.29 | $3,644.13 | ▲ +80.29 after sell → book $9,536.17; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ZIP` | 265 | $4.21 | $3.47 | $-96.99 | $4,756.30 | ▼ -96.99 after sell → book $9,532.69; vs 09:30 mark -3.48 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BMEA` | 740 | $1.69 | $9.68 | $+25.18 | $5,997.22 | ▲ +25.18 after sell → book $9,523.01; vs 09:30 mark -9.68 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `NPWR` | 603 | $1.89 | $7.89 | $-82.00 | $7,129.01 | ▼ -82.00 after sell → book $9,515.13; vs 09:30 mark -7.88 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ALVO` | 230 | $4.84 | $3.02 | $-97.98 | $8,239.19 | ▼ -97.98 after sell → book $9,512.11; vs 09:30 mark -3.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `SUJA` | 137 | $9.08 | $2.43 | $+34.90 | $9,480.72 | ▲ +34.90 after sell → book $9,509.68; vs 09:30 mark -2.43 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 9 | $146.07 | $2.02 | — | $8,164.07 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+38.8; leftover $1354.39 | — |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 139 | $9.73 | $2.41 | — | $6,809.19 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+47.1; leftover $1354.39 | — |
| 2026-08-28 09:30 ET | **BUY** | `ERAS` | 70 | $19.25 | $2.20 | — | $5,459.49 | — | combo gate; gate vol=good,last_green=True; list yday_gainer; ret5=+14.1; leftover $1354.39 | — |
| 2026-08-28 09:30 ET | **BUY** | `SYRE` | 14 | $91.75 | $2.03 | — | $4,172.96 | — | combo gate; gate vol=good,last_green=True; list yday_mover; ret5=-13.2; leftover $1354.39 | — |
| 2026-08-28 09:30 ET | **BUY** | `NCNO` | 58 | $23.30 | $2.16 | — | $2,819.40 | — | combo gate; gate vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+14.5; leftover $1354.39 | — |
| 2026-08-28 09:30 ET | **BUY** | `TH` | 71 | $19.00 | $2.20 | — | $1,468.19 | — | combo gate; gate vol=good,last_green=True; list ohlc_hot; ret5=+7.5; leftover $1354.39 | — |
| 2026-08-28 09:30 ET | **BUY** | `GAP` | 54 | $24.69 | $2.15 | — | $132.78 | — | combo gate; gate vol=good,last_green=True; list earn_react; ret5=+5.8; leftover $1354.39 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $132.78 | ▼ close $9,274.14 vs 09:30 $9,542.95 (session -220.36) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $132.78 | ▼ 09:30 equity $9,153.67 vs yday $9,274.14 (-120.47) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `USDE` | 4 | $6.76 | $0.30 | $+3.25 | $159.52 | ▲ +3.25 after sell → book $9,153.36; vs 09:30 mark -0.31 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $159.52 | ▲ close $9,157.41 vs 09:30 $9,153.67 (session +4.04) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $159.52 | ▲ 09:30 equity $9,169.34 vs yday $9,157.41 (+11.93) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $159.52 | ▼ close $8,993.60 vs 09:30 $9,169.34 (session -175.74) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $159.52 | ▼ 09:30 equity $8,987.26 vs yday $8,993.60 (-6.34) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `ANF` | 9 | $139.65 | $2.04 | $-61.83 | $1,414.33 | ▼ -61.83 after sell → book $8,985.22; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CAPR` | 139 | $10.07 | $2.44 | $+42.41 | $2,811.62 | ▲ +42.41 after sell → book $8,982.78; vs 09:30 mark -2.44 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ERAS` | 70 | $16.97 | $2.22 | $-164.02 | $3,997.30 | ▼ -164.02 after sell → book $8,980.56; vs 09:30 mark -2.22 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SYRE` | 14 | $88.05 | $2.05 | $-55.88 | $5,227.95 | ▼ -55.88 after sell → book $8,978.51; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `NCNO` | 58 | $22.20 | $2.18 | $-68.15 | $6,513.36 | ▼ -68.15 after sell → book $8,976.32; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `TH` | 71 | $17.98 | $2.23 | $-76.85 | $7,787.72 | ▼ -76.85 after sell → book $8,974.10; vs 09:30 mark -2.22 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `GAP` | 54 | $21.97 | $2.17 | $-151.20 | $8,971.93 | ▼ -151.20 after sell → book $8,971.93; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,971.93 | ▲ close $8,971.93 vs 09:30 $8,987.26 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,971.93 | ▲ 09:30 equity $8,971.93 vs yday $8,971.93 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 8 | $132.45 | $2.01 | — | $7,910.31 | — | combo gate; gate vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1121.49 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 66 | $16.77 | $2.19 | — | $6,801.30 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1121.49 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 514 | $2.18 | $6.63 | — | $5,674.15 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $1121.49 | — |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 630 | $1.78 | $8.13 | — | $4,544.63 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+183.1; leftover $1121.49 | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 46 | $23.88 | $2.13 | — | $3,444.02 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1121.49 | — |
| 2026-09-03 09:30 ET | **BUY** | `NVAX` | 107 | $10.42 | $2.31 | — | $2,326.77 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+12.1; leftover $1121.49 | — |
| 2026-09-03 09:30 ET | **BUY** | `BMEA` | 581 | $1.93 | $7.49 | — | $1,197.94 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1121.49 | — |
| 2026-09-03 09:30 ET | **BUY** | `DUOL` | 6 | $161.54 | $2.01 | — | $226.69 | — | combo gate; gate vol=good,last_green=True; list yday_gainer; 🔵; ret5=+12.0; leftover $1121.49 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $226.69 | ▼ close $8,550.28 vs 09:30 $8,971.93 (session -388.74) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $226.69 | ▲ 09:30 equity $8,608.63 vs yday $8,550.28 (+58.35) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `BRR` | 11 | $2.51 | $0.31 | — | $198.77 | — | combo gate; gate vol=good,last_green=True; list yday_gainer; 🔵; ⚪; ret5=+21.8; leftover $28.34 | — |
| 2026-09-04 09:30 ET | **BUY** | `ASST` | 1 | $25.18 | $0.25 | — | $173.34 | — | combo gate; gate vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+16.0; leftover $28.34 | — |
| 2026-09-04 09:30 ET | **BUY** | `DFDV` | 4 | $5.79 | $0.24 | — | $149.94 | — | combo gate; gate vol=good,last_green=True; list ohlc_hot; 🔵; ⚪; ret5=+15.2; leftover $28.34 | — |
| 2026-09-04 09:30 ET | **BUY** | `RSKD` | 4 | $6.84 | $0.29 | — | $122.29 | — | combo gate; gate vol=good,last_green=True; list ohlc_hot; ret5=+13.2; leftover $28.34 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $122.29 | ▲ close $8,786.96 vs 09:30 $8,608.63 (session +179.42) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $122.29 | ▼ 09:30 equity $8,616.08 vs yday $8,786.96 (-170.88) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $122.29 | ▼ close $8,496.75 vs 09:30 $8,616.08 (session -119.33) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $122.29 | ▼ 09:30 equity $8,451.12 vs yday $8,496.75 (-45.63) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `RVTY` | 8 | $125.77 | $2.03 | $-57.49 | $1,126.42 | ▼ -57.49 after sell → book $8,449.09; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `ARCT` | 66 | $15.46 | $2.21 | $-90.86 | $2,144.57 | ▼ -90.86 after sell → book $8,446.88; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CRDL` | 514 | $2.22 | $6.73 | $+7.20 | $3,278.92 | ▲ +7.20 after sell → book $8,440.15; vs 09:30 mark -6.73 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `GPRO` | 630 | $1.45 | $8.24 | $-224.27 | $4,184.18 | ▼ -224.27 after sell → book $8,431.91; vs 09:30 mark -8.24 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MMED` | 46 | $23.22 | $2.15 | $-34.64 | $5,250.15 | ▼ -34.64 after sell → book $8,429.76; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `NVAX` | 107 | $10.02 | $2.34 | $-47.45 | $6,319.95 | ▼ -47.45 after sell → book $8,427.42; vs 09:30 mark -2.34 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `BMEA` | 581 | $1.94 | $7.60 | $-9.29 | $7,439.49 | ▼ -9.29 after sell → book $8,419.82; vs 09:30 mark -7.60 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `DUOL` | 6 | $145.58 | $2.03 | $-99.80 | $8,310.94 | ▼ -99.80 after sell → book $8,417.79; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,310.94 | ▼ close $8,414.53 vs 09:30 $8,451.12 (session -3.26) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,310.94 | ▼ 09:30 equity $8,413.13 vs yday $8,414.53 (-1.40) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `BRR` | 11 | $2.87 | $0.37 | $+3.28 | $8,342.15 | ▲ +3.28 after sell → book $8,412.77; vs 09:30 mark -0.36 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `ASST` | 1 | $26.06 | $0.28 | $+0.34 | $8,367.92 | ▲ +0.34 after sell → book $8,412.48; vs 09:30 mark -0.29 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `DFDV` | 4 | $5.22 | $0.24 | $-2.76 | $8,388.56 | ▼ -2.76 after sell → book $8,412.24; vs 09:30 mark -0.24 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `RSKD` | 4 | $5.92 | $0.27 | $-4.23 | $8,411.97 | ▼ -4.23 after sell → book $8,411.97; vs 09:30 mark -0.27 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,411.97 | ▲ close $8,411.97 vs 09:30 $8,413.13 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,411.97 | ▲ 09:30 equity $8,411.97 vs yday $8,411.97 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `TYRA` | 44 | $23.63 | $2.12 | — | $7,370.13 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer; ret5=-6.3; leftover $1051.50 | — |
| 2026-09-11 09:30 ET | **BUY** | `VIST` | 13 | $77.33 | $2.03 | — | $6,362.81 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer; ret5=+2.5; leftover $1051.50 | — |
| 2026-09-11 09:30 ET | **BUY** | `INDP` | 389 | $2.70 | $5.02 | — | $5,307.49 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+118.8; leftover $1051.50 | — |
| 2026-09-11 09:30 ET | **BUY** | `CMRC` | 335 | $3.13 | $4.32 | — | $4,254.62 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+24.2; leftover $1051.50 | — |
| 2026-09-11 09:30 ET | **BUY** | `WLTH` | 96 | $10.95 | $2.28 | — | $3,201.14 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+20.8; leftover $1051.50 | — |
| 2026-09-11 09:30 ET | **BUY** | `DBI` | 177 | $5.91 | $2.52 | — | $2,152.55 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover,ohlc_hot; ret5=+14.1; leftover $1051.50 | — |
| 2026-09-11 09:30 ET | **BUY** | `BNC` | 214 | $4.91 | $2.76 | — | $1,099.05 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+76.3; leftover $1051.50 | — |
| 2026-09-11 09:30 ET | **BUY** | `SWKS` | 12 | $84.27 | $2.03 | — | $85.79 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,ohlc_hot; ret5=+17.2; leftover $1051.50 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $85.79 | ▲ close $8,422.96 vs 09:30 $8,411.97 (session +34.06) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $85.79 | ▲ 09:30 equity $8,508.14 vs yday $8,422.96 (+85.18) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $85.79 | ▲ close $8,880.17 vs 09:30 $8,508.14 (session +372.03) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $85.79 | ▲ 09:30 equity $8,935.97 vs yday $8,880.17 (+55.80) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $85.79 | ▲ close $9,119.16 vs 09:30 $8,935.97 (session +183.19) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $85.79 | ▼ 09:30 equity $9,036.91 vs yday $9,119.16 (-82.25) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `TYRA` | 44 | $25.58 | $2.14 | $+81.54 | $1,209.16 | ▲ +81.54 after sell → book $9,034.76; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `VIST` | 13 | $76.75 | $2.05 | $-11.62 | $2,204.87 | ▼ -11.62 after sell → book $9,032.72; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `CMRC` | 335 | $3.48 | $4.39 | $+108.54 | $3,366.28 | ▲ +108.54 after sell → book $9,028.33; vs 09:30 mark -4.39 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `WLTH` | 96 | $10.82 | $2.30 | $-17.06 | $4,402.69 | ▼ -17.06 after sell → book $9,026.02; vs 09:30 mark -2.31 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `DBI` | 177 | $6.25 | $2.56 | $+55.10 | $5,506.38 | ▲ +55.10 after sell → book $9,023.46; vs 09:30 mark -2.56 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `BNC` | 214 | $4.77 | $2.81 | $-35.53 | $6,524.36 | ▼ -35.53 after sell → book $9,020.66; vs 09:30 mark -2.80 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 14 | $77.12 | $2.03 | — | $5,442.65 | — | combo gate; gate vol=good,last_green=True; list flatten,ohlc_hot; ret5=+7.2; leftover $1087.39 | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 185 | $5.87 | $2.54 | — | $4,354.15 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $1087.39 | — |
| 2026-09-16 09:30 ET | **BUY** | `QTRX` | 399 | $2.72 | $5.15 | — | $3,263.72 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer; ret5=-0.4; leftover $1087.39 | — |
| 2026-09-16 09:30 ET | **BUY** | `VAL` | 12 | $87.40 | $2.03 | — | $2,212.90 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer; 🔵; ret5=+3.2; leftover $1087.39 | — |
| 2026-09-16 09:30 ET | **BUY** | `ADPT` | 40 | $27.09 | $2.11 | — | $1,127.19 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.6; leftover $1087.39 | — |
| 2026-09-16 09:30 ET | **BUY** | `HQ` | 84 | $12.89 | $2.24 | — | $42.19 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; ret5=-18.2; leftover $1087.39 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $42.19 | ▼ close $8,800.95 vs 09:30 $9,036.91 (session -203.61) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $42.19 | ▲ 09:30 equity $8,909.17 vs yday $8,800.95 (+108.22) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `INDP` | 389 | $3.30 | $5.09 | $+223.29 | $1,320.79 | ▲ +223.29 after sell → book $8,904.07; vs 09:30 mark -5.10 | dropped from list after 4 sess (min 3) | — |
| 2026-09-17 09:30 ET | **SELL** | `SWKS` | 12 | $86.76 | $2.05 | $+25.81 | $2,359.87 | ▲ +25.81 after sell → book $8,902.03; vs 09:30 mark -2.04 | dropped from list after 4 sess (min 3) | — |
| 2026-09-17 09:30 ET | **BUY** | `ILMN` | 1 | $233.85 | $1.99 | — | $2,124.02 | — | combo gate; gate vol=good,last_green=True; list flatten,ohlc_hot; ret5=+11.7; leftover $294.98 | — |
| 2026-09-17 09:30 ET | **BUY** | `RVTY` | 1 | $147.61 | $1.48 | — | $1,974.93 | — | combo gate; gate vol=good,last_green=True; list flatten,ohlc_hot; ret5=+17.7; leftover $294.98 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 38 | $7.59 | $2.10 | — | $1,684.41 | — | combo gate; gate vol=good,last_green=True; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $294.98 | — |
| 2026-09-17 09:30 ET | **BUY** | `DVLT` | 1735 | $0.17 | $8.15 | — | $1,381.31 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; ret5=-14.3; leftover $294.98 | — |
| 2026-09-17 09:30 ET | **BUY** | `BRUN` | 18 | $15.87 | $2.04 | — | $1,093.60 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; ret5=-12.7; leftover $294.98 | — |
| 2026-09-17 09:30 ET | **BUY** | `ARQT` | 11 | $25.95 | $2.02 | — | $806.13 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $294.98 | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 1 | $170.85 | $1.71 | — | $633.57 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $294.98 | — |
| 2026-09-17 09:30 ET | **BUY** | `SABR` | 122 | $2.40 | $2.36 | — | $338.41 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.4; leftover $294.98 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $338.41 | ▲ close $9,025.85 vs 09:30 $8,909.17 (session +145.69) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $338.41 | ▲ 09:30 equity $9,065.35 vs yday $9,025.85 (+39.50) | — | — |
| 2026-09-18 09:30 ET | **BUY** | `TLSA` | 43 | $0.97 | $0.55 | — | $296.16 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+1.0; leftover $42.30 | — |
| 2026-09-18 09:30 ET | **BUY** | `EYPT` | 10 | $3.95 | $0.42 | — | $256.23 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.1; leftover $42.30 | — |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 3 | $14.07 | $0.43 | — | $213.59 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $42.30 | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 2 | $14.79 | $0.30 | — | $183.71 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $42.30 | — |
| 2026-09-18 09:30 ET | **BUY** | `DCX` | 119 | $0.35 | $0.78 | — | $140.80 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=-19.7; leftover $42.30 | — |
| 2026-09-18 09:30 ET | **BUY** | `SDGR` | 1 | $29.32 | $0.30 | — | $111.19 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+60.9; leftover $42.30 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $111.19 | ▼ close $8,912.47 vs 09:30 $9,065.35 (session -150.10) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $111.19 | ▲ 09:30 equity $8,964.94 vs yday $8,912.47 (+52.47) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `RDNT` | 14 | $76.27 | $2.05 | $-15.98 | $1,176.92 | ▼ -15.98 after sell → book $8,962.88; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `RIG` | 185 | $5.62 | $2.59 | $-51.38 | $2,214.03 | ▼ -51.38 after sell → book $8,960.30; vs 09:30 mark -2.58 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `QTRX` | 399 | $3.13 | $5.22 | $+153.22 | $3,457.68 | ▲ +153.22 after sell → book $8,955.08; vs 09:30 mark -5.22 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `VAL` | 12 | $83.46 | $2.05 | $-51.35 | $4,457.15 | ▼ -51.35 after sell → book $8,953.03; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `ADPT` | 40 | $28.69 | $2.13 | $+59.76 | $5,602.62 | ▲ +59.76 after sell → book $8,950.90; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `HQ` | 84 | $13.41 | $2.27 | $+39.17 | $6,726.79 | ▲ +39.17 after sell → book $8,948.63; vs 09:30 mark -2.27 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 5 | $157.87 | $2.00 | — | $5,935.44 | — | combo gate; gate vol=good,last_green=True; list flatten; ret5=+6.5; leftover $840.85 | — |
| 2026-09-21 09:30 ET | **BUY** | `DXCM` | 9 | $88.83 | $2.02 | — | $5,133.95 | — | combo gate; gate vol=good,last_green=True; list flatten; ret5=+7.6; leftover $840.85 | — |
| 2026-09-21 09:30 ET | **BUY** | `BKKT` | 90 | $9.31 | $2.26 | — | $4,293.79 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; ret5=+5.3; leftover $840.85 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTDR` | 62 | $13.47 | $2.18 | — | $3,456.17 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+8.4; leftover $840.85 | — |
| 2026-09-21 09:30 ET | **BUY** | `SBET` | 84 | $9.99 | $2.24 | — | $2,614.76 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer; 🔵; ret5=+5.3; leftover $840.85 | — |
| 2026-09-21 09:30 ET | **BUY** | `SGML` | 82 | $10.13 | $2.24 | — | $1,781.46 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer; ret5=+4.9; leftover $840.85 | — |
| 2026-09-21 09:30 ET | **BUY** | `TJGC` | 49 | $16.91 | $2.14 | — | $950.73 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+50.5; leftover $840.85 | — |
| 2026-09-21 09:30 ET | **BUY** | `USDE` | 64 | $13.05 | $2.18 | — | $113.35 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+40.4; leftover $840.85 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $113.35 | ▼ close $8,915.50 vs 09:30 $8,964.94 (session -15.88) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $113.35 | ▲ 09:30 equity $8,933.53 vs yday $8,915.50 (+18.03) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `DVLT` | 1735 | $0.16 | $8.28 | $-33.78 | $382.67 | ▼ -33.78 after sell → book $8,925.25; vs 09:30 mark -8.28 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 09:30 ET | **BUY** | `CRML` | 5 | $9.11 | $0.47 | — | $336.65 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+44.4; leftover $47.83 | — |
| 2026-09-22 09:30 ET | **BUY** | `IVVD` | 47 | $1.01 | $0.62 | — | $288.56 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+14.3; leftover $47.83 | — |
| 2026-09-22 09:30 ET | **BUY** | `NUAI` | 6 | $7.23 | $0.45 | — | $244.73 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+36.6; leftover $47.83 | — |
| 2026-09-22 09:30 ET | **BUY** | `SECZ` | 3 | $12.96 | $0.40 | — | $205.45 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+64.4; leftover $47.83 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $205.45 | ▲ close $8,979.93 vs 09:30 $8,933.53 (session +56.62) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $205.45 | ▼ 09:30 equity $8,978.75 vs yday $8,979.93 (-1.18) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `ILMN` | 1 | $248.79 | $2.01 | $+10.93 | $452.23 | ▲ +10.93 after sell → book $8,976.73; vs 09:30 mark -2.02 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `RVTY` | 1 | $142.40 | $1.45 | $-8.14 | $593.18 | ▼ -8.14 after sell → book $8,975.29; vs 09:30 mark -1.44 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `PGEN` | 38 | $7.95 | $2.12 | $+9.45 | $893.16 | ▲ +9.45 after sell → book $8,973.16; vs 09:30 mark -2.13 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `BRUN` | 18 | $17.10 | $2.06 | $+18.03 | $1,198.90 | ▲ +18.03 after sell → book $8,971.10; vs 09:30 mark -2.06 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `ARQT` | 11 | $27.79 | $2.04 | $+16.17 | $1,502.54 | ▲ +16.17 after sell → book $8,969.05; vs 09:30 mark -2.05 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `SMTC` | 1 | $174.50 | $1.77 | $+0.17 | $1,675.28 | ▲ +0.17 after sell → book $8,967.29; vs 09:30 mark -1.76 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `SABR` | 122 | $2.24 | $2.39 | $-24.26 | $1,946.17 | ▼ -24.26 after sell → book $8,964.90; vs 09:30 mark -2.39 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `TLSA` | 43 | $0.89 | $0.53 | $-4.52 | $1,983.91 | ▼ -4.52 after sell → book $8,964.37; vs 09:30 mark -0.53 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `EYPT` | 10 | $4.10 | $0.46 | $+0.62 | $2,024.45 | ▲ +0.62 after sell → book $8,963.91; vs 09:30 mark -0.46 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `BHVN` | 3 | $14.84 | $0.47 | $+1.40 | $2,068.49 | ▲ +1.40 after sell → book $8,963.43; vs 09:30 mark -0.48 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `RARE` | 2 | $15.40 | $0.33 | $+0.58 | $2,098.96 | ▲ +0.58 after sell → book $8,963.10; vs 09:30 mark -0.33 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `DCX` | 119 | $0.09 | $0.49 | $-32.81 | $2,109.06 | ▼ -32.81 after sell → book $8,962.61; vs 09:30 mark -0.49 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `SDGR` | 1 | $30.05 | $0.32 | $+0.11 | $2,138.78 | ▲ +0.11 after sell → book $8,962.28; vs 09:30 mark -0.33 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 17 | $20.65 | $2.04 | — | $1,785.69 | — | combo gate; gate vol=good,last_green=True; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $356.46 | — |
| 2026-09-23 09:30 ET | **BUY** | `INDP` | 90 | $3.93 | $2.26 | — | $1,429.73 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $356.46 | — |
| 2026-09-23 09:30 ET | **BUY** | `SGRY` | 22 | $15.72 | $2.06 | — | $1,081.84 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $356.46 | — |
| 2026-09-23 09:30 ET | **BUY** | `TNGX` | 14 | $25.40 | $2.03 | — | $724.21 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.0; leftover $356.46 | — |
| 2026-09-23 09:30 ET | **BUY** | `CLPT` | 22 | $15.55 | $2.06 | — | $380.05 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+3.8; leftover $356.46 | — |
| 2026-09-23 09:30 ET | **BUY** | `NMRA` | 464 | $0.77 | $4.96 | — | $18.74 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.3; leftover $356.46 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $18.74 | ▲ close $9,000.98 vs 09:30 $8,978.75 (session +54.09) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $18.74 | ▼ 09:30 equity $8,976.08 vs yday $9,000.98 (-24.90) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `A` | 5 | $163.95 | $2.02 | $+26.37 | $836.47 | ▲ +26.37 after sell → book $8,974.06; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `DXCM` | 9 | $87.67 | $2.04 | $-14.45 | $1,623.50 | ▼ -14.45 after sell → book $8,972.02; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `BKKT` | 90 | $8.67 | $2.28 | $-62.14 | $2,401.52 | ▼ -62.14 after sell → book $8,969.73; vs 09:30 mark -2.29 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `BTDR` | 62 | $12.26 | $2.20 | $-79.70 | $3,159.44 | ▼ -79.70 after sell → book $8,967.54; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `SBET` | 84 | $9.80 | $2.27 | $-20.47 | $3,980.38 | ▼ -20.47 after sell → book $8,965.27; vs 09:30 mark -2.27 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `SGML` | 82 | $9.89 | $2.26 | $-24.59 | $4,789.10 | ▼ -24.59 after sell → book $8,963.01; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `TJGC` | 49 | $24.03 | $2.16 | $+344.59 | $5,964.41 | ▲ +344.59 after sell → book $8,960.85; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `USDE` | 64 | $12.76 | $2.20 | $-22.94 | $6,778.85 | ▼ -22.94 after sell → book $8,958.65; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,778.85 | ▲ close $8,960.94 vs 09:30 $8,976.08 (session +2.29) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,877.03 | ▼ 09:30 equity $9,384.12 vs yday $9,389.25 (-5.13) | 09:30 open · cash $6,877.03 (unchanged overnight, no fees) · equity $9,384.12 vs prior close $9,389.25 (-5.13) · 10 name(s) re-marked at the open (per-name table). AIBZ×63 yday $4.31 → 09:30 $4.31 +0.00; ARM×1 yday $306.34 → 09:30 $306.34 +0.00; EU×302 yday $1.22 → 09:30 $1.22 +0.00; GRAL×3 yday $125.21 → 09:30 $123.50 -5.13; GRPN×14 yday $20.89 → 09:30 $20.89 +0.00; HELP×22 yday $12.59 → 09:30 $12.59 +0.00; INDP×3 yday $4.00 → 09:30 $4.00 +0.00; IVVD×317 yday $0.91 → 09:30 $0.91 +0.00; NMRA×20 yday $0.70 → 09:30 $0.70 +0.00; NUAI×44 yday $6.94 → 09:30 $6.94 +0.00 | — |
| 2026-09-25 09:30 ET | **SELL** | `GRAL` | 3 | $123.50 | $2.02 | $+46.23 | $7,245.51 | ▲ +46.23 after sell → book $9,382.10; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `WRBY` | 34 | $26.27 | $2.09 | — | $6,350.24 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $905.69 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `PL` | 50 | $17.91 | $2.14 | — | $5,452.60 | — | combo gate; gate vol=good,last_green=True; list probable; 🔵; ret5=+3.7; leftover $905.69 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TEM` | 10 | $83.69 | $2.02 | — | $4,613.63 | — | combo gate; gate vol=good,last_green=True; list probable,ohlc_hot; ⚪; ret5=+2.3; leftover $905.69 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `GLND` | 149 | $6.06 | $2.44 | — | $3,708.25 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+342.1; leftover $905.69 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ZSQR` | 234 | $3.86 | $3.02 | — | $2,801.99 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; leftover $905.69 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `DNA` | 88 | $10.20 | $2.25 | — | $1,902.14 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+30.7; leftover $905.69 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TWST` | 4 | $184.00 | $2.00 | — | $1,164.14 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.3; leftover $905.69 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 55 | $16.21 | $2.15 | — | $270.43 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; leftover $905.69 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $270.43 | ▼ close $9,293.94 vs 09:30 $9,384.12 (session -70.04) | 16:00 close · cash $270.43 · equity $9,293.94 vs 09:30 $9,384.12 (-90.18; session marks -70.04) · 17 name(s) marked open→close (per-name table). AIBZ×63 09:30 $4.31 → close $4.31 -0.00; ARM×1 09:30 $306.34 → close $306.34 -0.00; EU×302 09:30 $1.22 → close $1.22 +0.00; GRPN×14 09:30 $20.89 → close $20.89 -0.00; HELP×22 09:30 $12.59 → close $12.59 +0.00; INDP×3 09:30 $4.00 → close $4.00 +0.00; IVVD×317 09:30 $0.91 → close $0.91 -0.00; NMRA×20 09:30 $0.70 → close $0.70 +0.00; NUAI×44 09:30 $6.94 → close $6.94 +0.00; WRBY×34 09:30 $26.27 → close $26.71 +14.96; PL×50 09:30 $17.91 → close $17.43 -24.00; TEM×10 09:30 $83.69 → close $85.01 +13.15; GLND×149 09:30 $6.06 → close $5.54 -77.48; ZSQR×234 09:30 $3.86 → close $3.78 -18.72; DNA×88 09:30 $10.20 → close $10.66 +40.48; TWST×4 09:30 $184.00 → close $182.83 -4.68; SECZ×55 09:30 $16.21 → close $15.96 -13.75 | — |

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
| 2026-08-17 | `CDNL` | cash | leftover split 0.45 < 1 share @ 39.85 |
| 2026-08-17 | `ABX` | cash | leftover split 0.45 < 1 share @ 9.12 |
| 2026-08-17 | `VERA` | cash | leftover split 0.45 < 1 share @ 31.30 |
| 2026-08-17 | `HTFL` | cash | leftover split 0.45 < 1 share @ 41.23 |
| 2026-08-17 | `UMAC` | cash | leftover split 0.45 < 1 share @ 32.55 |
| 2026-08-17 | `NPWR` | cash | leftover split 0.45 < 1 share @ 1.92 |
| 2026-08-17 | `LPTH` | cash | leftover split 0.45 < 1 share @ 14.94 |
| 2026-08-17 | `NMAX` | cash | leftover split 0.45 < 1 share @ 10.97 |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BETR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `HYLN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ADUR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `AIRO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `NCMI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `QMLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `NXE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `WFF` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AXTI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HIVE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FIGR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HTHT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `RANI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SECZ` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `HAE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `IVVD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `CHRS` | hard_red | hard-red S=-7.20 sit; no new buys |
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
| 2026-08-21 | `DE` | cash | leftover split 4.12 < 1 share @ 623.26 |
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
| 2026-08-24 | `GORO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `USAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BKKT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TMC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `GUTS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `GORO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `KURA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `CCOI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `LIFE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ZIP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `NPWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ALVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `SUJA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `KURA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CCOI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `LIFE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ZIP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `NPWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ALVO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `SUJA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `USDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `DKS` | cash | leftover split 3.41 < 1 share @ 128.73 |
| 2026-08-28 | `USDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `ANF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CAPR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ERAS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SYRE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `NCNO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `TH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `GAP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TENB` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MRNA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CMRC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TYL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `ANF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CAPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ERAS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SYRE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `NCNO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `TH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `GAP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-02 | `FRVO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `GPRO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MMED` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `NVAX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DUOL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DELL` | cash | leftover split 28.34 < 1 share @ 513.78 |
| 2026-09-04 | `TARS` | cash | leftover split 28.34 < 1 share @ 82.70 |
| 2026-09-04 | `MDB` | cash | leftover split 28.34 < 1 share @ 378.34 |
| 2026-09-04 | `TDS` | cash | leftover split 28.34 < 1 share @ 37.44 |
| 2026-09-08 | `RVTY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `GPRO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `MMED` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `NVAX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `DUOL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `BRR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `ASST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `DFDV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `RSKD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `EVGO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MSTR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `BTBT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `HOOD` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CRCL` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `BRR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `ASST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `DFDV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `RSKD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `BIDU` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `SUNB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `IRD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `NAUT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ASO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `INDP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SSL` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `AMBA` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `TYRA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `VIST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `WLTH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `DBI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `BNC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `SWKS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `DK` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DELL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `VERI` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `ODD` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HPE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HPQ` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `TYRA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `VIST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `INDP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `CMRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `WLTH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `DBI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `BNC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `SWKS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `TENB` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `RBRK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CYPH` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `RPD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SES` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `NTSK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `S` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CRWD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `RDNT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `RIG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `QTRX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `VAL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `ADPT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `HQ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `RDNT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `RIG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `QTRX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `VAL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `ADPT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `HQ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `ILMN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `PGEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `DVLT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `BRUN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `ARQT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `SABR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `VICR` | cash | leftover split 42.30 < 1 share @ 219.62 |
| 2026-09-18 | `ECO` | cash | leftover split 42.30 < 1 share @ 85.00 |
| 2026-09-21 | `ILMN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `RVTY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `PGEN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `DVLT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `BRUN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `ARQT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `SABR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `TLSA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `EYPT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `BHVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `RARE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `DCX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `SDGR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `ILMN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `RVTY` | no_price | no 09:30 open — carry |
| 2026-09-22 | `PGEN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `BRUN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `ARQT` | no_price | no 09:30 open — carry |
| 2026-09-22 | `SMTC` | no_price | no 09:30 open — carry |
| 2026-09-22 | `SABR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `TLSA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `EYPT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `BHVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `RARE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `DCX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `SDGR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `A` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `DXCM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `BKKT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `BTDR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `SBET` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `SGML` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `TJGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `USDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `FJET` | no_price | no 09:30 open |
| 2026-09-22 | `GRAL` | no_price | no 09:30 open |
| 2026-09-22 | `EU` | no_price | no 09:30 open |
| 2026-09-22 | `ARM` | cash | leftover split 47.83 < 1 share @ 319.41 |
| 2026-09-23 | `BKKT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `BTDR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `SBET` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `SGML` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `TJGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `USDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `CRML` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `IVVD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `NUAI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `SECZ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `CRML` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `IVVD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `NUAI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `OMER` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `INDP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `SGRY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `TNGX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `CLPT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `NMRA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `CVE` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `INVZ` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `QNC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `NNBR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `TLSA` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `GLND` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `FSLY` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `CRML` | 5 | 2026-09-22 @ $9.11 | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+44.4; leftover $47.83 |
| `IVVD` | 47 | 2026-09-22 @ $1.01 | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+14.3; leftover $47.83 |
| `NUAI` | 6 | 2026-09-22 @ $7.23 | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+36.6; leftover $47.83 |
| `SECZ` | 3 | 2026-09-22 @ $12.96 | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+64.4; leftover $47.83 |
| `OMER` | 17 | 2026-09-23 @ $20.65 | combo gate; gate vol=good,last_green=True; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $356.46 |
| `INDP` | 90 | 2026-09-23 @ $3.93 | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $356.46 |
| `SGRY` | 22 | 2026-09-23 @ $15.72 | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $356.46 |
| `TNGX` | 14 | 2026-09-23 @ $25.40 | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.0; leftover $356.46 |
| `CLPT` | 22 | 2026-09-23 @ $15.55 | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+3.8; leftover $356.46 |
| `NMRA` | 464 | 2026-09-23 @ $0.77 | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.3; leftover $356.46 |
