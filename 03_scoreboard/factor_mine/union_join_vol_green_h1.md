# Factor mine action — `union_join_vol_green_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **-2.67%** ($9,733) · signal-only (no cash/fees) was +14.99%. Starts YES **24/30**. Fills 222 · skips 55 · realized $+1581.44.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- Minimum hold is 1 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 1 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `join=good,vol=good,last_green=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $11,581.46.

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
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 833 | $1.52 | $10.89 | $-4.98 | $1,258.83 | ▼ -4.98 after sell → book $9,748.60; vs 09:30 mark -10.90 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BETR` | 84 | $13.67 | $2.27 | $-99.43 | $2,404.85 | ▼ -99.43 after sell → book $9,746.34; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 290 | $4.60 | $3.80 | $+76.56 | $3,735.05 | ▲ +76.56 after sell → book $9,742.54; vs 09:30 mark -3.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HYLN` | 299 | $4.10 | $3.92 | $-31.69 | $4,957.03 | ▼ -31.69 after sell → book $9,738.62; vs 09:30 mark -3.92 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ADUR` | 75 | $15.73 | $2.24 | $-62.20 | $6,134.54 | ▼ -62.20 after sell → book $9,736.38; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 112 | $9.57 | $2.35 | $-178.28 | $7,204.03 | ▼ -178.28 after sell → book $9,734.03; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NCMI` | 464 | $2.80 | $6.07 | $+38.98 | $8,497.16 | ▲ +38.98 after sell → book $9,727.96; vs 09:30 mark -6.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `QMLS` | 170 | $7.24 | $2.54 | $-13.54 | $9,725.42 | ▼ -13.54 after sell → book $9,725.42; vs 09:30 mark -2.54 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 213 | $9.12 | $2.75 | — | $7,780.11 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $1945.08 | — |
| 2026-08-17 09:30 ET | **BUY** | `ALOY` | 132 | $14.66 | $2.39 | — | $5,842.60 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.0; leftover $1945.08 | — |
| 2026-08-17 09:30 ET | **BUY** | `BORR` | 423 | $4.59 | $5.46 | — | $3,895.58 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot; ⚪; ret5=+14.8; leftover $1945.08 | — |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 464 | $4.19 | $5.99 | — | $1,945.43 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; ⚪; ret5=+291.8; leftover $1945.08 | — |
| 2026-08-17 09:30 ET | **BUY** | `MP` | 33 | $58.01 | $2.09 | — | $29.01 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ⚪; ret5=+14.9; leftover $1945.08 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $29.01 | ▼ close $9,449.00 vs 09:30 $9,759.50 (session -257.75) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $29.01 | ▼ 09:30 equity $9,310.07 vs yday $9,449.00 (-138.93) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 213 | $9.03 | $2.80 | $-24.72 | $1,949.60 | ▼ -24.72 after sell → book $9,307.27; vs 09:30 mark -2.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALOY` | 132 | $13.19 | $2.42 | $-198.85 | $3,688.26 | ▼ -198.85 after sell → book $9,304.85; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `BORR` | 423 | $4.56 | $5.54 | $-23.69 | $5,611.60 | ▼ -23.69 after sell → book $9,299.31; vs 09:30 mark -5.54 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `XHG` | 464 | $3.94 | $6.08 | $-128.06 | $7,433.68 | ▼ -128.06 after sell → book $9,293.23; vs 09:30 mark -6.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `MP` | 33 | $56.35 | $2.11 | $-58.98 | $9,291.12 | ▼ -58.98 after sell → book $9,291.12; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,291.12 | ▲ close $9,291.12 vs 09:30 $9,310.07 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,291.12 | ▲ 09:30 equity $9,291.12 vs yday $9,291.12 (-0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,291.12 | ▲ close $9,291.12 vs 09:30 $9,291.12 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,291.12 | ▲ 09:30 equity $9,291.12 vs yday $9,291.12 (-0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 56 | $20.55 | $2.16 | — | $8,138.16 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1161.39 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 56 | $20.65 | $2.16 | — | $6,979.60 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1161.39 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 201 | $5.77 | $2.60 | — | $5,817.24 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1161.39 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 59 | $19.63 | $2.17 | — | $4,656.90 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1161.39 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 39 | $29.63 | $2.11 | — | $3,499.22 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1161.39 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 663 | $1.75 | $8.55 | — | $2,330.42 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1161.39 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $1,172.08 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1161.39 | — |
| 2026-08-20 09:30 ET | **BUY** | `ABUS` | 236 | $4.92 | $3.04 | — | $7.92 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1161.39 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7.92 | ▲ close $9,419.53 vs 09:30 $9,291.12 (session +153.21) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7.92 | ▲ 09:30 equity $9,747.22 vs yday $9,419.53 (+327.69) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 56 | $21.90 | $2.18 | $+71.26 | $1,232.14 | ▲ +71.26 after sell → book $9,745.04; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 56 | $21.75 | $2.18 | $+57.26 | $2,447.96 | ▲ +57.26 after sell → book $9,742.86; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 201 | $5.67 | $2.64 | $-25.34 | $3,584.99 | ▼ -25.34 after sell → book $9,740.22; vs 09:30 mark -2.64 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 59 | $21.17 | $2.19 | $+86.51 | $4,831.84 | ▲ +86.51 after sell → book $9,738.04; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 39 | $32.17 | $2.13 | $+94.83 | $6,084.34 | ▲ +94.83 after sell → book $9,735.91; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 663 | $1.79 | $8.67 | $+9.29 | $7,262.44 | ▲ +9.29 after sell → book $9,727.24; vs 09:30 mark -8.67 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 8 | $154.70 | $2.03 | $+77.23 | $8,498.00 | ▲ +77.23 after sell → book $9,725.20; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ABUS` | 236 | $5.20 | $3.09 | $+59.94 | $9,722.11 | ▲ +59.94 after sell → book $9,722.11; vs 09:30 mark -3.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $8,525.79 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1215.26 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 70 | $17.20 | $2.20 | — | $7,319.59 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $1215.26 | — |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 5 | $216.30 | $2.00 | — | $6,236.08 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1215.26 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 109 | $11.13 | $2.32 | — | $5,020.60 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1215.26 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 920 | $1.32 | $11.87 | — | $3,794.33 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1215.26 | — |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 732 | $1.66 | $9.44 | — | $2,569.77 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $1215.26 | — |
| 2026-08-21 09:30 ET | **BUY** | `INDP` | 874 | $1.39 | $11.27 | — | $1,343.63 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+30.2; leftover $1215.26 | — |
| 2026-08-21 09:30 ET | **BUY** | `MRVI` | 146 | $8.28 | $2.43 | — | $132.32 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.6; leftover $1215.26 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $132.32 | ▲ close $9,871.63 vs 09:30 $9,747.22 (session +193.08) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $132.32 | ▲ 09:30 equity $10,191.54 vs yday $9,871.63 (+319.91) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.51 | $2.04 | $+6.74 | $1,335.38 | ▲ +6.74 after sell → book $10,189.50; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 70 | $16.57 | $2.22 | $-48.52 | $2,493.06 | ▼ -48.52 after sell → book $10,187.28; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 5 | $217.03 | $2.02 | $-0.38 | $3,576.19 | ▼ -0.38 after sell → book $10,185.26; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 109 | $13.33 | $2.35 | $+235.14 | $5,026.81 | ▲ +235.14 after sell → book $10,182.91; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 920 | $1.83 | $12.03 | $+445.30 | $6,698.38 | ▲ +445.30 after sell → book $10,170.88; vs 09:30 mark -12.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BTBT` | 732 | $1.55 | $9.57 | $-99.54 | $7,823.40 | ▼ -99.54 after sell → book $10,161.30; vs 09:30 mark -9.58 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `INDP` | 874 | $1.24 | $11.43 | $-153.80 | $8,895.73 | ▼ -153.80 after sell → book $10,149.87; vs 09:30 mark -11.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MRVI` | 146 | $8.59 | $2.46 | $+40.37 | $10,147.41 | ▲ +40.37 after sell → book $10,147.41; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,147.41 | ▲ close $10,147.41 vs 09:30 $10,191.54 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,147.41 | ▲ 09:30 equity $10,147.41 vs yday $10,147.41 (-0.00) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 778 | $1.63 | $10.04 | — | $8,869.23 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; leftover $1268.43 | — |
| 2026-08-25 09:30 ET | **BUY** | `GORO` | 357 | $3.55 | $4.61 | — | $7,597.28 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+27.9; leftover $1268.43 | — |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 199 | $6.37 | $2.59 | — | $6,327.06 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ⚪; ret5=+10.9; leftover $1268.43 | — |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 36 | $35.05 | $2.10 | — | $5,063.16 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ⚪; ret5=+19.7; leftover $1268.43 | — |
| 2026-08-25 09:30 ET | **BUY** | `ETON` | 19 | $64.55 | $2.05 | — | $3,834.67 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+4.4; leftover $1268.43 | — |
| 2026-08-25 09:30 ET | **BUY** | `WPM` | 8 | $156.51 | $2.01 | — | $2,580.57 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot,mover_buy; ⚪; ret5=+17.4; leftover $1268.43 | — |
| 2026-08-25 09:30 ET | **BUY** | `SUZ` | 141 | $8.98 | $2.41 | — | $1,311.98 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot,mover_buy; ⚪; ret5=+15.4; leftover $1268.43 | — |
| 2026-08-25 09:30 ET | **BUY** | `IAUX` | 667 | $1.90 | $8.60 | — | $36.07 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+16.4; leftover $1268.43 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $36.07 | ▲ close $10,351.14 vs 09:30 $10,147.41 (session +238.14) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $36.07 | ▼ 09:30 equity $10,268.78 vs yday $10,351.14 (-82.36) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `BMEA` | 778 | $1.75 | $10.18 | $+77.04 | $1,391.29 | ▲ +77.04 after sell → book $10,258.61; vs 09:30 mark -10.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `GORO` | 357 | $3.77 | $4.68 | $+69.26 | $2,732.50 | ▲ +69.26 after sell → book $10,253.93; vs 09:30 mark -4.68 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZURA` | 199 | $6.13 | $2.63 | $-52.98 | $3,949.74 | ▼ -52.98 after sell → book $10,251.30; vs 09:30 mark -2.63 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EZPW` | 36 | $35.70 | $2.12 | $+19.18 | $5,232.82 | ▲ +19.18 after sell → book $10,249.18; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ETON` | 19 | $63.60 | $2.07 | $-22.16 | $6,439.16 | ▼ -22.16 after sell → book $10,247.12; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `WPM` | 8 | $160.93 | $2.03 | $+31.31 | $7,724.56 | ▲ +31.31 after sell → book $10,245.08; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `SUZ` | 141 | $9.03 | $2.45 | $+2.19 | $8,995.35 | ▲ +2.19 after sell → book $10,242.64; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `IAUX` | 667 | $1.87 | $8.72 | $-37.34 | $10,233.91 | ▼ -37.34 after sell → book $10,233.91; vs 09:30 mark -8.73 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 1757 | $5.81 | $22.67 | — | $3.08 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; 🔵; ⚪; ret5=+117.2; leftover $10233.91 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.08 | ▲ close $10,509.94 vs 09:30 $10,268.78 (session +298.69) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.08 | ▲ 09:30 equity $11,423.58 vs yday $10,509.94 (+913.64) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `USDE` | 1757 | $6.50 | $23.05 | $+1166.62 | $11,400.53 | ▲ +1,166.62 after sell → book $11,400.53; vs 09:30 mark -23.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `DKS` | 88 | $128.73 | $2.25 | — | $70.03 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; ret5=-32.2; leftover $11400.53 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $70.03 | ▲ close $11,665.79 vs 09:30 $11,423.58 (session +267.52) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $70.03 | ▲ 09:30 equity $11,756.43 vs yday $11,665.79 (+90.64) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `DKS` | 88 | $132.80 | $2.36 | $+353.54 | $11,754.07 | ▲ +353.54 after sell → book $11,754.07; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 20 | $146.07 | $2.05 | — | $8,830.62 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+38.8; leftover $2938.52 | — |
| 2026-08-28 09:30 ET | **BUY** | `NCNO` | 126 | $23.30 | $2.37 | — | $5,892.45 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+14.5; leftover $2938.52 | — |
| 2026-08-28 09:30 ET | **BUY** | `TH` | 154 | $19.00 | $2.45 | — | $2,964.00 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; ret5=+7.5; leftover $2938.52 | — |
| 2026-08-28 09:30 ET | **BUY** | `GAP` | 119 | $24.69 | $2.35 | — | $23.54 | — | combo gate; gate join=good,vol=good,last_green=True; list earn_react; ret5=+5.8; leftover $2938.52 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $23.54 | ▼ close $11,539.50 vs 09:30 $11,756.43 (session -205.35) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $23.54 | ▼ 09:30 equity $11,365.17 vs yday $11,539.50 (-174.33) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 20 | $148.03 | $2.08 | $+35.07 | $2,982.06 | ▲ +35.07 after sell → book $11,363.09; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `NCNO` | 126 | $22.66 | $2.41 | $-85.42 | $5,834.81 | ▼ -85.42 after sell → book $11,360.68; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TH` | 154 | $18.12 | $2.50 | $-139.70 | $8,623.56 | ▼ -139.70 after sell → book $11,358.18; vs 09:30 mark -2.50 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GAP` | 119 | $22.98 | $2.39 | $-208.23 | $11,355.79 | ▼ -208.23 after sell → book $11,355.79; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,355.79 | ▲ close $11,355.79 vs 09:30 $11,365.17 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,355.79 | ▲ 09:30 equity $11,355.79 vs yday $11,355.79 (+0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,355.79 | ▲ close $11,355.79 vs 09:30 $11,355.79 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,355.79 | ▲ 09:30 equity $11,355.79 vs yday $11,355.79 (+0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,355.79 | ▲ close $11,355.79 vs 09:30 $11,355.79 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,355.79 | ▲ 09:30 equity $11,355.79 vs yday $11,355.79 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $132.45 | $2.02 | — | $10,029.27 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1419.47 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 84 | $16.77 | $2.24 | — | $8,618.35 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1419.47 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 651 | $2.18 | $8.40 | — | $7,190.77 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $1419.47 | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 59 | $23.88 | $2.17 | — | $5,779.68 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1419.47 | — |
| 2026-09-03 09:30 ET | **BUY** | `NVAX` | 136 | $10.42 | $2.40 | — | $4,360.17 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+12.1; leftover $1419.47 | — |
| 2026-09-03 09:30 ET | **BUY** | `BMEA` | 735 | $1.93 | $9.48 | — | $2,932.13 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1419.47 | — |
| 2026-09-03 09:30 ET | **BUY** | `DUOL` | 8 | $161.54 | $2.01 | — | $1,637.80 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ret5=+12.0; leftover $1419.47 | — |
| 2026-09-03 09:30 ET | **BUY** | `ALMS` | 136 | $10.38 | $2.40 | — | $224.40 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; 🔵; ret5=-56.2; leftover $1419.47 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $224.40 | ▼ close $11,276.07 vs 09:30 $11,355.79 (session -48.60) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $224.40 | ▼ 09:30 equity $11,260.12 vs yday $11,276.07 (-15.95) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 10 | $130.03 | $2.04 | $-28.26 | $1,522.66 | ▼ -28.26 after sell → book $11,258.08; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 84 | $15.61 | $2.27 | $-101.95 | $2,831.64 | ▼ -101.95 after sell → book $11,255.82; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRDL` | 651 | $2.16 | $8.52 | $-29.94 | $4,229.28 | ▼ -29.94 after sell → book $11,247.30; vs 09:30 mark -8.52 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 59 | $23.84 | $2.19 | $-6.72 | $5,633.65 | ▼ -6.72 after sell → book $11,245.11; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `NVAX` | 136 | $10.50 | $2.43 | $+6.05 | $7,059.22 | ▲ +6.05 after sell → book $11,242.68; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `BMEA` | 735 | $1.90 | $9.61 | $-41.15 | $8,446.10 | ▼ -41.15 after sell → book $11,233.06; vs 09:30 mark -9.62 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DUOL` | 8 | $157.46 | $2.03 | $-36.69 | $9,703.75 | ▼ -36.69 after sell → book $11,231.03; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ALMS` | 136 | $11.23 | $2.43 | $+111.45 | $11,228.60 | ▲ +111.45 after sell → book $11,228.60; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 2 | $513.78 | $2.00 | — | $10,199.04 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+9.3; leftover $1403.57 | — |
| 2026-09-04 09:30 ET | **BUY** | `TARS` | 16 | $82.70 | $2.04 | — | $8,873.80 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+15.7; leftover $1403.57 | — |
| 2026-09-04 09:30 ET | **BUY** | `BRR` | 559 | $2.51 | $7.21 | — | $7,463.50 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ⚪; ret5=+21.8; leftover $1403.57 | — |
| 2026-09-04 09:30 ET | **BUY** | `MDB` | 3 | $378.34 | $2.00 | — | $6,326.48 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; ret5=-12.7; leftover $1403.57 | — |
| 2026-09-04 09:30 ET | **BUY** | `ASST` | 55 | $25.18 | $2.15 | — | $4,939.43 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+16.0; leftover $1403.57 | — |
| 2026-09-04 09:30 ET | **BUY** | `DFDV` | 242 | $5.79 | $3.12 | — | $3,535.13 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ⚪; ret5=+15.2; leftover $1403.57 | — |
| 2026-09-04 09:30 ET | **BUY** | `TDS` | 37 | $37.44 | $2.10 | — | $2,147.74 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ⚪; ret5=+14.1; leftover $1403.57 | — |
| 2026-09-04 09:30 ET | **BUY** | `AHCO` | 222 | $6.32 | $2.86 | — | $741.84 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; 🔵; ret5=+10.7; leftover $1403.57 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $741.84 | ▲ close $11,589.49 vs 09:30 $11,260.12 (session +384.38) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $741.84 | ▼ 09:30 equity $11,483.58 vs yday $11,589.49 (-105.91) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `DELL` | 2 | $521.15 | $2.02 | $+10.73 | $1,782.12 | ▲ +10.73 after sell → book $11,481.56; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `TARS` | 16 | $89.67 | $2.06 | $+107.42 | $3,214.79 | ▲ +107.42 after sell → book $11,479.51; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BRR` | 559 | $2.66 | $7.32 | $+69.32 | $4,694.41 | ▲ +69.32 after sell → book $11,472.19; vs 09:30 mark -7.32 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MDB` | 3 | $360.75 | $2.02 | $-56.79 | $5,774.64 | ▼ -56.79 after sell → book $11,470.17; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ASST` | 55 | $26.44 | $2.18 | $+64.97 | $7,226.66 | ▲ +64.97 after sell → book $11,467.99; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DFDV` | 242 | $5.81 | $3.17 | $-1.45 | $8,629.51 | ▼ -1.45 after sell → book $11,464.82; vs 09:30 mark -3.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `TDS` | 37 | $37.75 | $2.12 | $+7.25 | $10,024.14 | ▲ +7.25 after sell → book $11,462.70; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `AHCO` | 222 | $6.48 | $2.91 | $+29.74 | $11,459.79 | ▲ +29.74 after sell → book $11,459.79; vs 09:30 mark -2.91 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,459.79 | ▲ close $11,459.79 vs 09:30 $11,483.58 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,459.79 | ▲ 09:30 equity $11,459.79 vs yday $11,459.79 (-0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,459.79 | ▲ close $11,459.79 vs 09:30 $11,459.79 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,459.79 | ▲ 09:30 equity $11,459.79 vs yday $11,459.79 (-0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,459.79 | ▲ close $11,459.79 vs 09:30 $11,459.79 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,459.79 | ▲ 09:30 equity $11,459.79 vs yday $11,459.79 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `TYRA` | 60 | $23.63 | $2.17 | — | $10,039.82 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; ret5=-6.3; leftover $1432.47 | — |
| 2026-09-11 09:30 ET | **BUY** | `INDP` | 530 | $2.70 | $6.84 | — | $8,601.98 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+118.8; leftover $1432.47 | — |
| 2026-09-11 09:30 ET | **BUY** | `WLTH` | 130 | $10.95 | $2.38 | — | $7,176.10 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+20.8; leftover $1432.47 | — |
| 2026-09-11 09:30 ET | **BUY** | `BNC` | 291 | $4.91 | $3.75 | — | $5,743.54 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+76.3; leftover $1432.47 | — |
| 2026-09-11 09:30 ET | **BUY** | `SWKS` | 16 | $84.27 | $2.04 | — | $4,393.18 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot; ret5=+17.2; leftover $1432.47 | — |
| 2026-09-11 09:30 ET | **BUY** | `ASO` | 26 | $54.91 | $2.07 | — | $2,963.45 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ret5=+24.3; leftover $1432.47 | — |
| 2026-09-11 09:30 ET | **BUY** | `IRD` | 232 | $6.16 | $2.99 | — | $1,531.34 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ret5=+36.4; leftover $1432.47 | — |
| 2026-09-11 09:30 ET | **BUY** | `COO` | 26 | $54.66 | $2.07 | — | $108.11 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_mover; ret5=-22.3; leftover $1432.47 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $108.11 | ▼ close $11,300.11 vs 09:30 $11,459.79 (session -135.37) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $108.11 | ▲ 09:30 equity $11,406.92 vs yday $11,300.11 (+106.81) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `TYRA` | 60 | $23.20 | $2.19 | $-30.16 | $1,497.92 | ▼ -30.16 after sell → book $11,404.73; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `INDP` | 530 | $2.80 | $6.94 | $+39.23 | $2,974.98 | ▲ +39.23 after sell → book $11,397.79; vs 09:30 mark -6.94 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `WLTH` | 130 | $10.29 | $2.41 | $-90.59 | $4,310.27 | ▼ -90.59 after sell → book $11,395.38; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BNC` | 291 | $5.03 | $3.81 | $+27.35 | $5,770.18 | ▲ +27.35 after sell → book $11,391.56; vs 09:30 mark -3.82 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `SWKS` | 16 | $86.06 | $2.06 | $+24.54 | $7,145.09 | ▲ +24.54 after sell → book $11,389.51; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ASO` | 26 | $54.75 | $2.09 | $-8.32 | $8,566.50 | ▼ -8.32 after sell → book $11,387.42; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `IRD` | 232 | $6.02 | $3.04 | $-38.52 | $9,960.09 | ▼ -38.52 after sell → book $11,384.37; vs 09:30 mark -3.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `COO` | 26 | $54.78 | $2.09 | $-1.04 | $11,382.28 | ▼ -1.04 after sell → book $11,382.28; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,382.28 | ▲ close $11,382.28 vs 09:30 $11,406.92 (session +0.00) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,382.28 | ▲ 09:30 equity $11,382.28 vs yday $11,382.28 (+0.00) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,382.28 | ▲ close $11,382.28 vs 09:30 $11,382.28 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,382.28 | ▲ 09:30 equity $11,382.28 vs yday $11,382.28 (+0.00) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `RDNT` | 18 | $77.12 | $2.04 | — | $9,992.08 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,ohlc_hot; ret5=+7.2; leftover $1422.79 | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 242 | $5.87 | $3.12 | — | $8,568.42 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $1422.79 | — |
| 2026-09-16 09:30 ET | **BUY** | `VAL` | 16 | $87.40 | $2.04 | — | $7,167.98 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ret5=+3.2; leftover $1422.79 | — |
| 2026-09-16 09:30 ET | **BUY** | `ADPT` | 52 | $27.09 | $2.15 | — | $5,757.15 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.6; leftover $1422.79 | — |
| 2026-09-16 09:30 ET | **BUY** | `SWKS` | 15 | $89.38 | $2.04 | — | $4,414.42 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.4; leftover $1422.79 | — |
| 2026-09-16 09:30 ET | **BUY** | `SDGR` | 61 | $23.29 | $2.17 | — | $2,991.56 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ret5=+16.1; leftover $1422.79 | — |
| 2026-09-16 09:30 ET | **BUY** | `FPS` | 42 | $33.14 | $2.12 | — | $1,597.56 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer; 🔵; ret5=-2.9; leftover $1422.79 | — |
| 2026-09-16 09:30 ET | **BUY** | `CAI` | 50 | $28.16 | $2.14 | — | $187.42 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot; ret5=+14.8; leftover $1422.79 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $187.42 | ▼ close $11,268.66 vs 09:30 $11,382.28 (session -95.81) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $187.42 | ▲ 09:30 equity $11,457.42 vs yday $11,268.66 (+188.76) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `RDNT` | 18 | $76.44 | $2.06 | $-16.35 | $1,561.28 | ▼ -16.35 after sell → book $11,455.36; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `RIG` | 242 | $5.58 | $3.17 | $-76.47 | $2,908.46 | ▼ -76.47 after sell → book $11,452.18; vs 09:30 mark -3.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `VAL` | 16 | $83.20 | $2.06 | $-71.30 | $4,237.60 | ▼ -71.30 after sell → book $11,450.12; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `ADPT` | 52 | $28.23 | $2.17 | $+54.97 | $5,703.40 | ▲ +54.97 after sell → book $11,447.96; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SWKS` | 15 | $86.76 | $2.06 | $-43.39 | $7,002.74 | ▼ -43.39 after sell → book $11,445.90; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SDGR` | 61 | $24.09 | $2.19 | $+44.43 | $8,470.04 | ▲ +44.43 after sell → book $11,443.71; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `CAI` | 50 | $28.59 | $2.16 | $+17.45 | $9,897.62 | ▲ +17.45 after sell → book $11,441.54; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `ILMN` | 6 | $233.85 | $2.01 | — | $8,492.52 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,ohlc_hot; ret5=+11.7; leftover $1413.95 | — |
| 2026-09-17 09:30 ET | **BUY** | `RVTY` | 9 | $147.61 | $2.02 | — | $7,162.01 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,ohlc_hot; ret5=+17.7; leftover $1413.95 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 186 | $7.59 | $2.55 | — | $5,747.72 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $1413.95 | — |
| 2026-09-17 09:30 ET | **BUY** | `ARQT` | 54 | $25.95 | $2.15 | — | $4,344.27 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $1413.95 | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 8 | $170.85 | $2.01 | — | $2,975.46 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $1413.95 | — |
| 2026-09-17 09:30 ET | **BUY** | `CIFR` | 78 | $18.04 | $2.22 | — | $1,566.50 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-1.1; leftover $1413.95 | — |
| 2026-09-17 09:30 ET | **BUY** | `BRKR` | 22 | $61.90 | $2.06 | — | $202.65 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,ohlc_hot; 🔵; ret5=+11.5; leftover $1413.95 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $202.65 | ▲ close $11,619.42 vs 09:30 $11,457.42 (session +192.89) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $202.65 | ▲ 09:30 equity $11,811.95 vs yday $11,619.42 (+192.53) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `FPS` | 42 | $39.50 | $2.14 | $+262.86 | $1,859.51 | ▲ +262.86 after sell → book $11,809.81; vs 09:30 mark -2.14 | dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `ILMN` | 6 | $249.13 | $2.03 | $+87.64 | $3,352.26 | ▲ +87.64 after sell → book $11,807.78; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `RVTY` | 9 | $146.50 | $2.04 | $-14.04 | $4,668.72 | ▼ -14.04 after sell → book $11,805.74; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `PGEN` | 186 | $7.98 | $2.59 | $+67.40 | $6,150.41 | ▲ +67.40 after sell → book $11,803.15; vs 09:30 mark -2.59 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `ARQT` | 54 | $26.14 | $2.17 | $+5.93 | $7,559.79 | ▲ +5.93 after sell → book $11,800.97; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 8 | $182.33 | $2.04 | $+87.79 | $9,016.40 | ▲ +87.79 after sell → book $11,798.94; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `CIFR` | 78 | $17.80 | $2.25 | $-22.80 | $10,402.55 | ▼ -22.80 after sell → book $11,796.69; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BRKR` | 22 | $63.37 | $2.08 | $+28.21 | $11,794.61 | ▲ +28.21 after sell → book $11,794.61; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `VICR` | 6 | $219.62 | $2.01 | — | $10,474.89 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,yday_gainer,yday_mover; 🔵; ret5=+21.5; leftover $1474.33 | — |
| 2026-09-18 09:30 ET | **BUY** | `ECO` | 17 | $85.00 | $2.04 | — | $9,027.84 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten; 🔵; ⚪; ret5=+18.3; leftover $1474.33 | — |
| 2026-09-18 09:30 ET | **BUY** | `EYPT` | 373 | $3.95 | $4.81 | — | $7,549.68 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.1; leftover $1474.33 | — |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 104 | $14.07 | $2.30 | — | $6,084.10 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $1474.33 | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 99 | $14.79 | $2.29 | — | $4,617.60 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $1474.33 | — |
| 2026-09-18 09:30 ET | **BUY** | `SDGR` | 50 | $29.32 | $2.14 | — | $3,149.46 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+60.9; leftover $1474.33 | — |
| 2026-09-18 09:30 ET | **BUY** | `CYPH` | 485 | $3.04 | $6.26 | — | $1,671.23 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+39.5; leftover $1474.33 | — |
| 2026-09-18 09:30 ET | **BUY** | `VITL` | 129 | $11.38 | $2.38 | — | $200.84 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+19.5; leftover $1474.33 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $200.84 | ▲ close $12,014.04 vs 09:30 $11,811.95 (session +243.64) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $200.84 | ▲ 09:30 equity $12,288.93 vs yday $12,014.04 (+274.89) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `VICR` | 6 | $230.25 | $2.03 | $+59.74 | $1,580.31 | ▲ +59.74 after sell → book $12,286.90; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `ECO` | 17 | $82.83 | $2.06 | $-40.99 | $2,986.35 | ▼ -40.99 after sell → book $12,284.83; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `EYPT` | 373 | $3.87 | $4.89 | $-39.54 | $4,424.98 | ▼ -39.54 after sell → book $12,279.95; vs 09:30 mark -4.88 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `BHVN` | 104 | $13.90 | $2.33 | $-22.31 | $5,868.25 | ▼ -22.31 after sell → book $12,277.62; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RARE` | 99 | $14.58 | $2.31 | $-25.39 | $7,309.35 | ▼ -25.39 after sell → book $12,275.30; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `SDGR` | 50 | $29.43 | $2.16 | $+1.20 | $8,778.69 | ▲ +1.20 after sell → book $12,273.14; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `CYPH` | 485 | $4.00 | $6.35 | $+455.42 | $10,712.34 | ▲ +455.42 after sell → book $12,266.79; vs 09:30 mark -6.35 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `VITL` | 129 | $12.05 | $2.41 | $+81.64 | $12,264.38 | ▲ +81.64 after sell → book $12,264.38; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 9 | $157.87 | $2.02 | — | $10,841.53 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten; ret5=+6.5; leftover $1533.05 | — |
| 2026-09-21 09:30 ET | **BUY** | `DXCM` | 17 | $88.83 | $2.04 | — | $9,329.38 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten; ret5=+7.6; leftover $1533.05 | — |
| 2026-09-21 09:30 ET | **BUY** | `BKKT` | 164 | $9.31 | $2.48 | — | $7,800.06 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; ret5=+5.3; leftover $1533.05 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTDR` | 113 | $13.47 | $2.33 | — | $6,275.05 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+8.4; leftover $1533.05 | — |
| 2026-09-21 09:30 ET | **BUY** | `SBET` | 153 | $9.99 | $2.45 | — | $4,744.14 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer; 🔵; ret5=+5.3; leftover $1533.05 | — |
| 2026-09-21 09:30 ET | **BUY** | `TJGC` | 90 | $16.91 | $2.26 | — | $3,219.98 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+50.5; leftover $1533.05 | — |
| 2026-09-21 09:30 ET | **BUY** | `USDE` | 117 | $13.05 | $2.34 | — | $1,690.78 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+40.4; leftover $1533.05 | — |
| 2026-09-21 09:30 ET | **BUY** | `GEMI` | 266 | $5.75 | $3.43 | — | $156.52 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+30.3; leftover $1533.05 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $156.52 | ▲ close $12,290.33 vs 09:30 $12,288.93 (session +45.31) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $156.52 | ▲ 09:30 equity $12,342.54 vs yday $12,290.33 (+52.21) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `SBET` | 153 | $9.91 | $2.49 | $-17.18 | $1,670.27 | ▼ -17.18 after sell → book $12,340.06; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `USDE` | 117 | $12.99 | $2.37 | $-11.73 | $3,187.72 | ▼ -11.73 after sell → book $12,337.68; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `GEMI` | 266 | $6.05 | $3.49 | $+72.88 | $4,794.87 | ▲ +72.88 after sell → book $12,334.20; vs 09:30 mark -3.48 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **BUY** | `ARM` | 2 | $319.41 | $2.00 | — | $4,154.05 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+35.1; leftover $799.14 | — |
| 2026-09-22 09:30 ET | **BUY** | `FSLY` | 28 | $28.02 | $2.07 | — | $3,367.42 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover,ohlc_hot; ret5=+10.3; leftover $799.14 | — |
| 2026-09-22 09:30 ET | **BUY** | `META` | 1 | $731.40 | $1.99 | — | $2,634.02 | — | combo gate; gate join=good,vol=good,last_green=True; list ohlc_hot; ret5=+11.4; leftover $799.14 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,634.02 | ▼ close $12,306.59 vs 09:30 $12,342.54 (session -21.54) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,634.02 | ▲ 09:30 equity $12,322.46 vs yday $12,306.59 (+15.87) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `BKKT` | 164 | $9.50 | $2.52 | $+26.16 | $4,189.50 | ▲ +26.16 after sell → book $12,319.94; vs 09:30 mark -2.52 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `BTDR` | 113 | $12.84 | $2.36 | $-76.44 | $5,638.06 | ▼ -76.44 after sell → book $12,317.58; vs 09:30 mark -2.36 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `TJGC` | 90 | $16.92 | $2.29 | $-3.65 | $7,158.57 | ▼ -3.65 after sell → book $12,315.29; vs 09:30 mark -2.29 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `ARM` | 2 | $331.78 | $2.02 | $+20.73 | $7,820.12 | ▲ +20.73 after sell → book $12,313.28; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `FSLY` | 28 | $25.90 | $2.09 | $-63.53 | $8,543.22 | ▼ -63.53 after sell → book $12,311.18; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `META` | 1 | $747.60 | $2.01 | $+12.19 | $9,288.81 | ▲ +12.19 after sell → book $12,309.17; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 74 | $20.65 | $2.21 | — | $7,758.50 | — | combo gate; gate join=good,vol=good,last_green=True; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $1548.14 | — |
| 2026-09-23 09:30 ET | **BUY** | `SGRY` | 98 | $15.72 | $2.28 | — | $6,215.65 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $1548.14 | — |
| 2026-09-23 09:30 ET | **BUY** | `TNGX` | 60 | $25.40 | $2.17 | — | $4,689.48 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-9.0; leftover $1548.14 | — |
| 2026-09-23 09:30 ET | **BUY** | `NMRA` | 2015 | $0.77 | $21.52 | — | $3,120.44 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.3; leftover $1548.14 | — |
| 2026-09-23 09:30 ET | **BUY** | `VKTX` | 37 | $41.76 | $2.10 | — | $1,573.22 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+36.4; leftover $1548.14 | — |
| 2026-09-23 09:30 ET | **BUY** | `BFLY` | 156 | $9.90 | $2.46 | — | $26.37 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+27.3; leftover $1548.14 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $26.37 | ▼ close $11,883.51 vs 09:30 $12,322.46 (session -392.92) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $26.37 | ▼ 09:30 equity $11,618.35 vs yday $11,883.51 (-265.16) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `A` | 9 | $163.95 | $2.04 | $+50.66 | $1,499.88 | ▲ +50.66 after sell → book $11,616.31; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `DXCM` | 17 | $87.67 | $2.06 | $-23.74 | $2,988.29 | ▼ -23.74 after sell → book $11,614.24; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `OMER` | 74 | $20.52 | $2.24 | $-14.07 | $4,504.53 | ▼ -14.07 after sell → book $11,612.01; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SGRY` | 98 | $14.38 | $2.31 | $-135.92 | $5,911.46 | ▼ -135.92 after sell → book $11,609.70; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `TNGX` | 60 | $23.99 | $2.19 | $-88.96 | $7,348.67 | ▼ -88.96 after sell → book $11,607.50; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `NMRA` | 2015 | $0.75 | $21.42 | $-87.27 | $8,830.44 | ▼ -87.27 after sell → book $11,586.08; vs 09:30 mark -21.42 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `VKTX` | 37 | $36.02 | $2.12 | $-216.42 | $10,161.24 | ▼ -216.42 after sell → book $11,583.96; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `BFLY` | 156 | $9.12 | $2.50 | $-126.63 | $11,581.46 | ▼ -126.63 after sell → book $11,581.46; vs 09:30 mark -2.50 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,581.46 | ▲ close $11,581.46 vs 09:30 $11,618.35 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,599.48 | ▲ 09:30 equity $9,599.48 vs yday $9,599.48 (+0.00) | 09:30 open · cash $9,599.48 · no holdings · equity $9,599.48 vs prior close $9,599.48 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 09:30 ET | **BUY** | `WRBY` | 45 | $26.27 | $2.12 | — | $8,415.20 | — | combo gate; gate join=good,vol=good,last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $1199.93 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ZSQR` | 310 | $3.86 | $4.00 | — | $7,214.61 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; leftover $1199.93 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TWST` | 6 | $184.00 | $2.01 | — | $6,108.60 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.3; leftover $1199.93 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 74 | $16.21 | $2.21 | — | $4,906.85 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; leftover $1199.93 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `GRAL` | 9 | $123.50 | $2.02 | — | $3,793.33 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+56.6; leftover $1199.93 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `QMCO` | 40 | $29.80 | $2.11 | — | $2,599.22 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+18.2; leftover $1199.93 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `CYPH` | 299 | $4.00 | $3.86 | — | $1,397.87 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+32.9; leftover $1199.93 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `CDNA` | 19 | $61.33 | $2.05 | — | $230.55 | — | combo gate; gate join=good,vol=good,last_green=True; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+13.1; leftover $1199.93 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $230.55 | ▲ close $9,733.33 vs 09:30 $9,599.48 (session +154.23) | 16:00 close · cash $230.55 · equity $9,733.33 vs 09:30 $9,599.48 (+133.85; session marks +154.23) · 8 name(s) marked open→close (per-name table). WRBY×45 09:30 $26.27 → close $26.71 +19.80; ZSQR×310 09:30 $3.86 → close $3.78 -24.80; TWST×6 09:30 $184.00 → close $182.83 -7.02; SECZ×74 09:30 $16.21 → close $15.96 -18.50; GRAL×9 09:30 $123.50 → close $126.89 +30.51; QMCO×40 09:30 $29.80 → close $31.68 +75.20; CYPH×299 09:30 $4.00 → close $4.12 +34.39; CDNA×19 09:30 $61.33 → close $63.68 +44.65 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TMC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ELMT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ERO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SCCO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `MRNA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TYL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `HOOD` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CRCL` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MRX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TRMD` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-10 | `IRD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `NAUT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `INDP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `AMBA` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CMPS` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ARBE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `DELL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HPE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HPQ` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `INSP` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `GME` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DHT` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `AVT` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `RBRK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `NTSK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `S` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CRWD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `IOT` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SAFX` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ECO` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-22 | `A` | no_price | no 09:30 open — carry |
| 2026-09-22 | `DXCM` | no_price | no 09:30 open — carry |
| 2026-09-22 | `BKKT` | no_price | no 09:30 open — carry |
| 2026-09-22 | `BTDR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `TJGC` | no_price | no 09:30 open — carry |
| 2026-09-22 | `GRPN` | no_price | no 09:30 open |
| 2026-09-22 | `XXI` | no_price | no 09:30 open |
| 2026-09-22 | `THO` | no_price | no 09:30 open |
| 2026-09-24 | `CVE` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `QNC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `TLSA` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `GLND` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `FSLY` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ACMR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `KVYO` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `VICR` | hard_red | hard-red S=-7.66 sit; no new buys |
