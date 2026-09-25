# Factor mine action — `union_hot_n4_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 4 · rank `hot_score` · size `leftover` · sell `list` · S-boost `none` · top 4 by hot

Cash book **+26.36%** ($12,636) · signal-only (no cash/fees) was +89.81%. Starts YES **29/30**. Fills 114 · skips 47 · realized $+2978.17.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass the list as written and skip anything on the must-not list. They take up to 4 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Sort: how hot the prior tape looked.
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is thrown out if any must-not is true.
- Sort the keepers by how hot the prior tape looked and keep the top 4.
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
- **Gate** `none (list as ranked)` · **rank** `hot_score` · **top_n** 4.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,581.64.

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
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 54 | $45.98 | $2.15 | — | $7,514.93 | — | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+12.3; leftover $2500.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 107 | $23.33 | $2.31 | — | $5,016.31 | — | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+19.7; leftover $2500.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 49 | $50.62 | $2.14 | — | $2,533.63 | — | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+6.2; leftover $2500.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 3085 | $0.81 | $34.24 | — | $0.54 | — | top 4 by hot; rank hot_score; list flatten; ⚪; ret5=+13.2; leftover $2500.00 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.54 | ▲ close $10,345.37 vs 09:30 $10,000.00 (session +386.21) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.54 | ▲ 09:30 equity $10,412.10 vs yday $10,345.37 (+66.73) | — | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 54 | $44.09 | $2.18 | $-106.39 | $2,379.22 | ▼ -106.39 after sell → book $10,409.92; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 107 | $22.92 | $2.35 | $-48.53 | $4,829.31 | ▼ -48.53 after sell → book $10,407.57; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 49 | $55.29 | $2.17 | $+224.37 | $7,536.35 | ▲ +224.37 after sell → book $10,405.40; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 3085 | $0.93 | $38.48 | $+297.48 | $10,366.92 | ▲ +297.48 after sell → book $10,366.92; vs 09:30 mark -38.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `QMCO` | 105 | $24.68 | $2.31 | — | $7,773.22 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+111.3; leftover $2591.73 | — |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 132 | $19.57 | $2.39 | — | $5,187.59 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $2591.73 | — |
| 2026-08-14 09:30 ET | **BUY** | `ZENA` | 1178 | $2.20 | $15.20 | — | $2,580.79 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+54.3; leftover $2591.73 | — |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 231 | $11.12 | $2.98 | — | $9.09 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $2591.73 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9.09 | ▼ close $10,066.79 vs 09:30 $10,412.10 (session -277.26) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9.09 | ▼ 09:30 equity $9,866.28 vs yday $10,066.79 (-200.51) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `QMCO` | 105 | $24.83 | $2.34 | $+11.10 | $2,613.90 | ▲ +11.10 after sell → book $9,863.94; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 132 | $19.57 | $2.43 | $-4.81 | $5,194.71 | ▼ -4.81 after sell → book $9,861.51; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ZENA` | 1178 | $2.08 | $15.41 | $-166.08 | $7,635.43 | ▼ -166.08 after sell → book $9,846.10; vs 09:30 mark -15.41 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 231 | $9.57 | $3.04 | $-364.07 | $9,843.06 | ▼ -364.07 after sell → book $9,843.06; vs 09:30 mark -3.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 587 | $4.19 | $7.57 | — | $7,375.96 | — | top 4 by hot; rank hot_score; list yday_mover; ⚪; ret5=+291.8; leftover $2460.77 | — |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 358 | $6.87 | $4.62 | — | $4,911.88 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+62.6; leftover $2460.77 | — |
| 2026-08-17 09:30 ET | **BUY** | `STDN` | 180 | $13.64 | $2.53 | — | $2,454.15 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+57.7; leftover $2460.77 | — |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 59 | $41.23 | $2.17 | — | $19.42 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+46.0; leftover $2460.77 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19.42 | ▲ close $9,851.95 vs 09:30 $9,866.28 (session +25.77) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19.42 | ▲ 09:30 equity $9,861.50 vs yday $9,851.95 (+9.55) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `XHG` | 587 | $3.94 | $7.69 | $-162.01 | $2,324.51 | ▼ -162.01 after sell → book $9,853.81; vs 09:30 mark -7.69 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `STDN` | 180 | $13.31 | $2.58 | $-64.51 | $4,717.73 | ▼ -64.51 after sell → book $9,851.23; vs 09:30 mark -2.58 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 59 | $41.50 | $2.20 | $+11.57 | $7,164.03 | ▲ +11.57 after sell → book $9,849.03; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,164.03 | ▼ close $9,698.67 vs 09:30 $9,861.50 (session -150.36) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,164.03 | ▲ 09:30 equity $9,738.05 vs yday $9,698.67 (+39.38) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `CAPR` | 358 | $7.19 | $4.70 | $+105.24 | $9,733.36 | ▲ +105.24 after sell → book $9,733.36; vs 09:30 mark -4.69 | dropped from list after 2 sess (min 1) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,733.36 | ▲ close $9,733.36 vs 09:30 $9,738.05 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,733.36 | ▲ 09:30 equity $9,733.36 vs yday $9,733.36 (-0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 16 | $150.14 | $2.04 | — | $7,329.08 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $2433.34 | — |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 2115 | $1.15 | $27.28 | — | $4,869.54 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; leftover $2433.34 | — |
| 2026-08-20 09:30 ET | **BUY** | `ABCL` | 205 | $11.81 | $2.64 | — | $2,444.82 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $2433.34 | — |
| 2026-08-20 09:30 ET | **BUY** | `AZI` | 1767 | $1.37 | $22.79 | — | $1.24 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.7; leftover $2433.34 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.24 | ▼ close $9,567.54 vs 09:30 $9,733.36 (session -111.05) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.24 | ▲ 09:30 equity $9,874.47 vs yday $9,567.54 (+306.93) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `ABCL` | 205 | $11.57 | $2.70 | $-55.57 | $2,370.39 | ▼ -55.57 after sell → book $9,871.77; vs 09:30 mark -2.70 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AZI` | 1767 | $1.46 | $23.11 | $+113.13 | $4,927.10 | ▲ +113.13 after sell → book $9,848.66; vs 09:30 mark -23.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `XHG` | 548 | $4.49 | $7.07 | — | $2,459.51 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+12.7; leftover $2463.55 | — |
| 2026-08-21 09:30 ET | **BUY** | `CAPR` | 360 | $6.81 | $4.64 | — | $3.27 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+62.5; leftover $2463.55 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.27 | ▲ close $10,009.73 vs 09:30 $9,874.47 (session +172.78) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.27 | ▲ 09:30 equity $11,415.08 vs yday $10,009.73 (+1,405.35) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `MRNA` | 16 | $142.70 | $2.07 | $-123.14 | $2,284.40 | ▼ -123.14 after sell → book $11,413.01; vs 09:30 mark -2.07 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 2115 | $1.83 | $27.67 | $+1383.25 | $6,127.19 | ▲ +1,383.25 after sell → book $11,385.35; vs 09:30 mark -27.66 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `XHG` | 548 | $4.32 | $7.18 | $-107.41 | $8,487.37 | ▼ -107.41 after sell → book $11,378.17; vs 09:30 mark -7.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CAPR` | 360 | $8.03 | $4.73 | $+429.83 | $11,373.44 | ▲ +429.83 after sell → book $11,373.44; vs 09:30 mark -4.73 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,373.44 | ▲ close $11,373.44 vs 09:30 $11,415.08 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,373.44 | ▲ 09:30 equity $11,373.44 vs yday $11,373.44 (+0.00) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 117 | $24.11 | $2.34 | — | $8,550.23 | — | top 4 by hot; rank hot_score; list yday_mover; ret5=+891.7; leftover $2843.36 | — |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 1822 | $1.56 | $23.50 | — | $5,684.41 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; leftover $2843.36 | — |
| 2026-08-25 09:30 ET | **BUY** | `XHG` | 698 | $4.07 | $9.00 | — | $2,834.54 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+4.9; leftover $2843.36 | — |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 148 | $19.04 | $2.43 | — | $14.19 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+49.5; leftover $2843.36 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14.19 | ▲ close $12,300.26 vs 09:30 $11,373.44 (session +964.10) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14.19 | ▼ 09:30 equity $11,768.70 vs yday $12,300.26 (-531.56) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `REAX` | 117 | $26.61 | $2.39 | $+287.77 | $3,125.17 | ▲ +287.77 after sell → book $11,766.31; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 1822 | $1.60 | $23.83 | $+25.55 | $6,016.55 | ▲ +25.55 after sell → book $11,742.49; vs 09:30 mark -23.82 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ASST` | 148 | $20.72 | $2.48 | $+243.72 | $9,080.62 | ▲ +243.72 after sell → book $11,740.00; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `BYND` | 214 | $14.11 | $2.76 | — | $6,058.32 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=+11.4; leftover $3026.87 | — |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 520 | $5.81 | $6.71 | — | $3,030.41 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ⚪; ret5=+117.2; leftover $3026.87 | — |
| 2026-08-26 09:30 ET | **BUY** | `PURR` | 261 | $11.59 | $3.37 | — | $3.36 | — | top 4 by hot; rank hot_score; list overnight; 🔵; ret5=+64.9; leftover $3026.87 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.36 | ▲ close $12,013.50 vs 09:30 $11,768.70 (session +286.33) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.36 | ▲ 09:30 equity $12,435.02 vs yday $12,013.50 (+421.52) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `BYND` | 214 | $14.20 | $2.82 | $+13.68 | $3,039.34 | ▲ +13.68 after sell → book $12,432.20; vs 09:30 mark -2.82 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `USDE` | 520 | $6.50 | $6.82 | $+345.27 | $6,412.52 | ▲ +345.27 after sell → book $12,425.38; vs 09:30 mark -6.82 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `PURR` | 261 | $12.18 | $3.44 | $+148.49 | $9,588.06 | ▲ +148.49 after sell → book $12,421.94; vs 09:30 mark -3.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `CAPR` | 347 | $9.19 | $4.48 | — | $6,394.66 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover,ohlc_hot; ret5=+17.3; leftover $3196.02 | — |
| 2026-08-27 09:30 ET | **BUY** | `MRNA` | 22 | $144.18 | $2.06 | — | $3,220.64 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=-14.2; leftover $3196.02 | — |
| 2026-08-27 09:30 ET | **BUY** | `BZ` | 172 | $18.50 | $2.51 | — | $36.14 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+17.2; leftover $3196.02 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $36.14 | ▲ close $12,416.30 vs 09:30 $12,435.02 (session +3.39) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $36.14 | ▼ 09:30 equity $12,128.05 vs yday $12,416.30 (-288.25) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `XHG` | 698 | $3.69 | $9.14 | $-283.38 | $2,602.61 | ▼ -283.38 after sell → book $12,118.90; vs 09:30 mark -9.15 | dropped from list after 3 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `BZ` | 172 | $18.15 | $2.56 | $-65.27 | $5,721.86 | ▼ -65.27 after sell → book $12,116.35; vs 09:30 mark -2.55 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `BYND` | 204 | $14.00 | $2.63 | — | $2,863.22 | — | top 4 by hot; rank hot_score; list ohlc_hot; ret5=-3.3; leftover $2860.93 | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 19 | $146.07 | $2.05 | — | $85.85 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+38.8; leftover $2860.93 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $85.85 | ▼ close $12,096.78 vs 09:30 $12,128.05 (session -14.89) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $85.85 | ▼ 09:30 equity $11,962.36 vs yday $12,096.78 (-134.42) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 347 | $9.50 | $4.56 | $+98.53 | $3,377.79 | ▲ +98.53 after sell → book $11,957.80; vs 09:30 mark -4.56 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MRNA` | 22 | $134.10 | $2.09 | $-225.91 | $6,325.90 | ▼ -225.91 after sell → book $11,955.71; vs 09:30 mark -2.09 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 19 | $148.03 | $2.08 | $+33.11 | $9,136.39 | ▲ +33.11 after sell → book $11,953.63; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,136.39 | ▼ close $11,849.59 vs 09:30 $11,962.36 (session -104.04) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,136.39 | ▼ 09:30 equity $11,796.55 vs yday $11,849.59 (-53.04) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `BYND` | 204 | $13.04 | $2.69 | $-201.16 | $11,793.86 | ▼ -201.16 after sell → book $11,793.86; vs 09:30 mark -2.69 | dropped from list after 2 sess (min 1) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,793.86 | ▲ close $11,793.86 vs 09:30 $11,796.55 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,793.86 | ▲ 09:30 equity $11,793.86 vs yday $11,793.86 (+0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,793.86 | ▲ close $11,793.86 vs 09:30 $11,793.86 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,793.86 | ▲ 09:30 equity $11,793.86 vs yday $11,793.86 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 1656 | $1.78 | $21.36 | — | $8,824.82 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+183.1; leftover $2948.47 | — |
| 2026-09-03 09:30 ET | **BUY** | `REAX` | 160 | $18.40 | $2.47 | — | $5,878.35 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=-32.2; leftover $2948.47 | — |
| 2026-09-03 09:30 ET | **BUY** | `CNH` | 215 | $13.71 | $2.77 | — | $2,927.92 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+17.5; leftover $2948.47 | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 122 | $23.88 | $2.36 | — | $12.21 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $2948.47 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12.21 | ▼ close $11,142.13 vs 09:30 $11,793.86 (session -622.77) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12.21 | ▲ 09:30 equity $11,261.92 vs yday $11,142.13 (+119.79) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `REAX` | 160 | $18.15 | $2.52 | $-44.99 | $2,913.69 | ▼ -44.99 after sell → book $11,259.40; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNH` | 215 | $13.89 | $2.83 | $+33.09 | $5,897.21 | ▲ +33.09 after sell → book $11,256.57; vs 09:30 mark -2.83 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 122 | $23.84 | $2.40 | $-9.64 | $8,803.29 | ▼ -9.64 after sell → book $11,254.17; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ASST` | 116 | $25.18 | $2.34 | — | $5,880.07 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+16.0; leftover $2934.43 | — |
| 2026-09-04 09:30 ET | **BUY** | `USDE` | 372 | $7.87 | $4.80 | — | $2,947.63 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+8.7; leftover $2934.43 | — |
| 2026-09-04 09:30 ET | **BUY** | `DFDV` | 506 | $5.79 | $6.53 | — | $11.36 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+15.2; leftover $2934.43 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11.36 | ▲ close $11,894.98 vs 09:30 $11,261.92 (session +654.48) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11.36 | ▼ 09:30 equity $11,496.62 vs yday $11,894.98 (-398.36) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `GPRO` | 1656 | $1.56 | $21.66 | $-399.06 | $2,581.34 | ▼ -399.06 after sell → book $11,474.96; vs 09:30 mark -21.66 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ASST` | 116 | $26.44 | $2.38 | $+141.44 | $5,646.00 | ▲ +141.44 after sell → book $11,472.58; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `USDE` | 372 | $7.76 | $4.88 | $-50.60 | $8,527.84 | ▼ -50.60 after sell → book $11,467.70; vs 09:30 mark -4.88 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DFDV` | 506 | $5.81 | $6.63 | $-3.04 | $11,461.06 | ▼ -3.04 after sell → book $11,461.06; vs 09:30 mark -6.64 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,461.06 | ▲ close $11,461.06 vs 09:30 $11,496.62 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,461.06 | ▲ 09:30 equity $11,461.06 vs yday $11,461.06 (+0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,461.06 | ▲ close $11,461.06 vs 09:30 $11,461.06 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,461.06 | ▲ 09:30 equity $11,461.06 vs yday $11,461.06 (+0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,461.06 | ▲ close $11,461.06 vs 09:30 $11,461.06 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,461.06 | ▲ 09:30 equity $11,461.06 vs yday $11,461.06 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `INDP` | 1061 | $2.70 | $13.69 | — | $8,582.68 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+118.8; leftover $2865.27 | — |
| 2026-09-11 09:30 ET | **BUY** | `BNC` | 583 | $4.91 | $7.52 | — | $5,712.63 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+76.3; leftover $2865.27 | — |
| 2026-09-11 09:30 ET | **BUY** | `IRD` | 465 | $6.16 | $6.00 | — | $2,842.23 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+36.4; leftover $2865.27 | — |
| 2026-09-11 09:30 ET | **BUY** | `CMRC` | 904 | $3.13 | $11.66 | — | $1.05 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+24.2; leftover $2865.27 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.05 | ▲ close $11,715.54 vs 09:30 $11,461.06 (session +293.34) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.05 | ▲ 09:30 equity $11,876.68 vs yday $11,715.54 (+161.14) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `IRD` | 465 | $6.02 | $6.10 | $-77.20 | $2,794.25 | ▼ -77.20 after sell → book $11,870.58; vs 09:30 mark -6.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,794.25 | ▲ close $12,488.76 vs 09:30 $11,876.68 (session +618.18) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,794.25 | ▲ 09:30 equity $12,671.34 vs yday $12,488.76 (+182.58) | — | — |
| 2026-09-15 09:30 ET | **SELL** | `BNC` | 583 | $5.11 | $7.64 | $+101.44 | $5,765.74 | ▲ +101.44 after sell → book $12,663.70; vs 09:30 mark -7.64 | dropped from list after 2 sess (min 1) | — |
| 2026-09-15 09:30 ET | **SELL** | `CMRC` | 904 | $3.64 | $11.84 | $+437.54 | $9,044.46 | ▲ +437.54 after sell → book $12,651.86; vs 09:30 mark -11.84 | dropped from list after 2 sess (min 1) | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,044.46 | ▲ close $12,906.50 vs 09:30 $12,671.34 (session +254.64) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,044.46 | ▲ 09:30 equity $12,927.72 vs yday $12,906.50 (+21.22) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `HLP` | 1674 | $1.80 | $21.59 | — | $6,009.66 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.5; leftover $3014.82 | — |
| 2026-09-16 09:30 ET | **BUY** | `SDGR` | 129 | $23.29 | $2.38 | — | $3,002.88 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+16.1; leftover $3014.82 | — |
| 2026-09-16 09:30 ET | **BUY** | `SSL` | 205 | $14.62 | $2.64 | — | $3.13 | — | top 4 by hot; rank hot_score; list ohlc_hot; 🔵; ret5=+13.6; leftover $3014.82 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.13 | ▼ close $12,890.54 vs 09:30 $12,927.72 (session -10.56) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.13 | ▲ 09:30 equity $12,950.29 vs yday $12,890.54 (+59.75) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `SDGR` | 129 | $24.09 | $2.42 | $+98.40 | $3,108.32 | ▲ +98.40 after sell → book $12,947.87; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SSL` | 205 | $13.77 | $2.70 | $-179.60 | $5,928.47 | ▼ -179.60 after sell → book $12,945.17; vs 09:30 mark -2.70 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `BBNX` | 131 | $22.46 | $2.38 | — | $2,983.83 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+27.3; leftover $2964.23 | — |
| 2026-09-17 09:30 ET | **BUY** | `FPS` | 80 | $36.76 | $2.23 | — | $40.80 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover,ohlc_hot; ret5=+12.4; leftover $2964.23 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $40.80 | ▲ close $13,444.14 vs 09:30 $12,950.29 (session +503.58) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $40.80 | ▼ 09:30 equity $13,356.99 vs yday $13,444.14 (-87.15) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `HLP` | 1674 | $1.96 | $21.90 | $+224.35 | $3,299.94 | ▲ +224.35 after sell → book $13,335.09; vs 09:30 mark -21.90 | dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BBNX` | 131 | $21.30 | $2.43 | $-156.77 | $6,087.81 | ▼ -156.77 after sell → book $13,332.66; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `FPS` | 80 | $39.50 | $2.27 | $+214.70 | $9,245.54 | ▲ +214.70 after sell → book $13,330.39; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `SDGR` | 105 | $29.32 | $2.31 | — | $6,164.64 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+60.9; leftover $3081.85 | — |
| 2026-09-18 09:30 ET | **BUY** | `CYPH` | 1015 | $3.04 | $13.09 | — | $3,071.02 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+39.5; leftover $3081.85 | — |
| 2026-09-18 09:30 ET | **BUY** | `TEM` | 37 | $81.40 | $2.10 | — | $57.12 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.8; leftover $3081.85 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $57.12 | ▲ close $13,404.85 vs 09:30 $13,356.99 (session +91.95) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $57.12 | ▲ 09:30 equity $13,899.78 vs yday $13,404.85 (+494.93) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `INDP` | 1061 | $3.55 | $13.89 | $+874.27 | $3,809.77 | ▲ +874.27 after sell → book $13,885.88; vs 09:30 mark -13.90 | dropped from list after 6 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `SDGR` | 105 | $29.43 | $2.35 | $+6.90 | $6,897.58 | ▲ +6.90 after sell → book $13,883.54; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `CYPH` | 1015 | $4.00 | $13.29 | $+953.09 | $10,944.28 | ▲ +953.09 after sell → book $13,870.24; vs 09:30 mark -13.30 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TEM` | 37 | $79.08 | $2.13 | $-90.08 | $13,868.11 | ▼ -90.08 after sell → book $13,868.11; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `FEAM` | 1403 | $2.47 | $18.10 | — | $10,384.60 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+73.6; leftover $3467.03 | — |
| 2026-09-21 09:30 ET | **BUY** | `TJGC` | 205 | $16.91 | $2.64 | — | $6,915.40 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+50.5; leftover $3467.03 | — |
| 2026-09-21 09:30 ET | **BUY** | `LVWR` | 2101 | $1.65 | $27.10 | — | $3,421.65 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+43.0; leftover $3467.03 | — |
| 2026-09-21 09:30 ET | **BUY** | `SECZ` | 292 | $11.67 | $3.77 | — | $10.24 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+31.3; leftover $3467.03 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10.24 | ▲ close $14,250.11 vs 09:30 $13,899.78 (session +433.62) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10.24 | ▼ 09:30 equity $14,092.43 vs yday $14,250.11 (-157.68) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10.24 | ▲ close $14,104.11 vs 09:30 $14,092.43 (session +11.68) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10.24 | ▲ 09:30 equity $14,275.61 vs yday $14,104.11 (+171.50) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `TJGC` | 205 | $16.92 | $2.71 | $-3.30 | $3,476.14 | ▼ -3.30 after sell → book $14,272.91; vs 09:30 mark -2.70 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `LVWR` | 2101 | $1.41 | $27.48 | $-558.82 | $6,411.07 | ▼ -558.82 after sell → book $14,245.43; vs 09:30 mark -27.48 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `SECZ` | 292 | $12.80 | $3.85 | $+322.35 | $10,144.83 | ▲ +322.35 after sell → book $14,241.59; vs 09:30 mark -3.84 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `GLND` | 1252 | $2.70 | $16.15 | — | $6,748.28 | — | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+109.2; leftover $3381.61 | — |
| 2026-09-23 09:30 ET | **BUY** | `VKTX` | 80 | $41.76 | $2.23 | — | $3,405.25 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+36.4; leftover $3381.61 | — |
| 2026-09-23 09:30 ET | **BUY** | `SVIA` | 753 | $4.49 | $9.71 | — | $14.56 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; ret5=+26.4; leftover $3381.61 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $14.56 | ▼ close $13,728.39 vs 09:30 $14,275.61 (session -485.10) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14.56 | ▼ 09:30 equity $13,641.06 vs yday $13,728.39 (-87.33) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `FEAM` | 1403 | $2.68 | $18.36 | $+258.17 | $3,756.24 | ▲ +258.17 after sell → book $13,622.70; vs 09:30 mark -18.36 | dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `VKTX` | 80 | $36.02 | $2.27 | $-463.30 | $6,635.97 | ▼ -463.30 after sell → book $13,620.44; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SVIA` | 753 | $3.92 | $9.86 | $-445.02 | $9,581.64 | ▼ -445.02 after sell → book $13,610.57; vs 09:30 mark -9.87 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,581.64 | ▲ close $16,279.84 vs 09:30 $13,641.06 (session +2,669.26) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,809.00 | ▲ 09:30 equity $13,189.92 vs yday $12,499.09 (+690.83) | 09:30 open · cash $4,809.00 (unchanged overnight, no fees) · equity $13,189.92 vs prior close $12,499.09 (+690.83) · 2 name(s) re-marked at the open (per-name table). GLND×973 yday $5.35 → 09:30 $6.06 +690.83; VICR×9 yday $276.06 → 09:30 $276.06 +0.00 | — |
| 2026-09-25 09:30 ET | **BUY** | `TJGC` | 53 | $29.76 | $2.15 | — | $3,229.57 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+156.1; leftover $1603.00 | join🔴 sector🟡 gen🟢 news🟡 digest🔴 ab🟡 heat🟢 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 98 | $16.21 | $2.28 | — | $1,638.71 | — | top 4 by hot; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; leftover $1603.00 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `USDE` | 102 | $15.58 | $2.30 | — | $47.14 | — | top 4 by hot; rank hot_score; list yday_gainer; 🔵; ret5=+84.4; leftover $1603.00 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $47.14 | ▼ close $12,636.40 vs 09:30 $13,189.92 (session -546.79) | 16:00 close · cash $47.14 · equity $12,636.40 vs 09:30 $13,189.92 (-553.52; session marks -546.79) · 5 name(s) marked open→close (per-name table). GLND×973 09:30 $6.06 → close $5.54 -505.96; VICR×9 09:30 $276.06 → close $276.06 -0.00; TJGC×53 09:30 $29.76 → close $26.24 -186.56; SECZ×98 09:30 $16.21 → close $15.96 -24.50; USDE×102 09:30 $15.58 → close $17.25 +170.23 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `OABI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AVAH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ARX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `IVVD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `ALM` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OMER` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HMY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `REAX` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NEOV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DPRO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MRNA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `REAX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `USDE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MRNA` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CNH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `INDP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CRWV` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `INTC` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `UROY` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `INDP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `IRD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `NAUT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `GPRO` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `GPRO` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SES` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `NTSK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-22 | `FEAM` | no_price | no 09:30 open — carry |
| 2026-09-22 | `TJGC` | no_price | no 09:30 open — carry |
| 2026-09-22 | `LVWR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `GRAL` | no_price | no 09:30 open |
| 2026-09-22 | `CRML` | cash | leftover split 3.41 < 1 share @ 9.11 |
| 2026-09-22 | `NUAI` | cash | leftover split 3.41 < 1 share @ 7.23 |
| 2026-09-24 | `TJGC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `SECZ` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `VICR` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `GLND` | 1252 | 2026-09-23 @ $2.70 | top 4 by hot; rank hot_score; list yday_mover; 🔵; ret5=+109.2; leftover $3381.61 |
