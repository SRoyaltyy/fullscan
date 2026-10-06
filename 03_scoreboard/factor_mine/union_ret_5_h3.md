# Factor mine action — `union_ret_5_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `ret_5` · size `leftover` · sell `list` · S-boost `none` · rank by ret_5

Cash book **+3.93%** ($10,393) · signal-only (no cash/fees) was +97.62%. Starts YES **29/30**. Fills 244 · skips 293 · realized $+1779.39.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass the list as written and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Sort: the prior 5-session return (bigger first).
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is thrown out if any must-not is true.
- Sort the keepers by the prior 5-session return (bigger first) and keep the top 8.
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
- **Gate** `none (list as ranked)` · **rank** `ret_5` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $8,265.78.

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
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $8,761.36 | — | rank by ret_5; rank ret_5; list flatten; ⚪; ret5=+19.7; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $7,494.40 | — | rank by ret_5; rank ret_5; list flatten; ⚪; ret5=+13.2; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $6,250.87 | — | rank by ret_5; rank ret_5; list flatten; ⚪; ret5=+12.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $5,033.85 | — | rank by ret_5; rank ret_5; list flatten; ⚪; ret5=+6.2; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 56 | $22.01 | $2.16 | — | $3,799.14 | — | rank by ret_5; rank ret_5; list flatten; ⚪; ret5=+0.3; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $2,556.63 | — | rank by ret_5; rank ret_5; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 25 | $49.70 | $2.06 | — | $1,312.06 | — | rank by ret_5; rank ret_5; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $114.01 | — | rank by ret_5; rank ret_5; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $114.01 | ▲ close $10,265.50 vs 09:30 $10,000.00 (session +297.49) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $114.01 | ▲ 09:30 equity $10,276.78 vs yday $10,265.50 (+11.28) | — | — |
| 2026-08-14 09:30 ET | **BUY** | `ZENA` | 6 | $2.20 | $0.15 | — | $100.66 | — | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ⚪; ret5=+54.3; leftover $14.25 | — |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 1 | $11.12 | $0.11 | — | $89.43 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $14.25 | — |
| 2026-08-14 09:30 ET | **BUY** | `BCAR` | 2 | $6.09 | $0.13 | — | $77.12 | — | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ⚪; ret5=+27.6; leftover $14.25 | — |
| 2026-08-14 09:30 ET | **BUY** | `SIDU` | 5 | $2.55 | $0.14 | — | $64.23 | — | rank by ret_5; rank ret_5; list overnight; 🔵; ret5=+21.5; leftover $14.25 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $64.23 | ▲ close $10,556.73 vs 09:30 $10,276.78 (session +280.48) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $64.23 | ▼ 09:30 equity $10,529.03 vs yday $10,556.73 (-27.70) | — | — |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 1 | $4.19 | $0.04 | — | $59.99 | — | rank by ret_5; rank ret_5; list yday_mover; ⚪; ret5=+291.8; leftover $8.03 | — |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 1 | $6.87 | $0.07 | — | $53.05 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; ret5=+62.6; leftover $8.03 | — |
| 2026-08-17 09:30 ET | **BUY** | `KOPN` | 1 | $5.43 | $0.06 | — | $47.56 | — | rank by ret_5; rank ret_5; list yday_gainer; ⚪; ret5=+28.8; leftover $8.03 | — |
| 2026-08-17 09:30 ET | **BUY** | `NPWR` | 4 | $1.92 | $0.09 | — | $39.80 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $8.03 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $39.80 | ▲ close $10,624.33 vs 09:30 $10,529.03 (session +95.56) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $39.80 | ▼ 09:30 equity $10,510.92 vs yday $10,624.33 (-113.41) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `TNDM` | 53 | $22.16 | $2.17 | $-66.33 | $1,212.11 | ▼ -66.33 after sell → book $10,508.75; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 1543 | $1.14 | $20.17 | $+471.89 | $2,950.95 | ▲ +471.89 after sell → book $10,488.57; vs 09:30 mark -20.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `IREN` | 27 | $43.56 | $2.09 | $-69.50 | $4,124.98 | ▼ -69.50 after sell → book $10,486.48; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 24 | $51.77 | $2.08 | $+23.38 | $5,365.38 | ▲ +23.38 after sell → book $10,484.40; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `VOR` | 56 | $22.82 | $2.18 | $+41.02 | $6,641.12 | ▲ +41.02 after sell → book $10,482.22; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `SLS` | 106 | $12.66 | $2.34 | $+97.12 | $7,980.74 | ▲ +97.12 after sell → book $10,479.88; vs 09:30 mark -2.34 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGTX` | 25 | $49.28 | $2.08 | $-14.65 | $9,210.66 | ▼ -14.65 after sell → book $10,477.80; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `BTSG` | 20 | $60.00 | $2.07 | $-0.12 | $10,408.59 | ▼ -0.12 after sell → book $10,475.73; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,408.59 | ▼ close $10,475.67 vs 09:30 $10,510.92 (session -0.06) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,408.59 | ▲ 09:30 equity $10,476.09 vs yday $10,475.67 (+0.42) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `ZENA` | 6 | $2.01 | $0.16 | $-1.45 | $10,420.49 | ▼ -1.45 after sell → book $10,475.93; vs 09:30 mark -0.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `AIRO` | 1 | $9.10 | $0.11 | $-2.25 | $10,429.48 | ▼ -2.25 after sell → book $10,475.82; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BCAR` | 2 | $5.32 | $0.13 | $-1.80 | $10,439.98 | ▼ -1.80 after sell → book $10,475.68; vs 09:30 mark -0.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `SIDU` | 5 | $2.45 | $0.16 | $-0.80 | $10,452.08 | ▼ -0.80 after sell → book $10,475.53; vs 09:30 mark -0.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,452.08 | ▲ close $10,476.00 vs 09:30 $10,476.09 (session +0.47) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,452.08 | ▼ 09:30 equity $10,475.27 vs yday $10,476.00 (-0.73) | — | — |
| 2026-08-20 09:30 ET | **SELL** | `XHG` | 1 | $4.10 | $0.06 | $-0.20 | $10,456.11 | ▼ -0.20 after sell → book $10,475.20; vs 09:30 mark -0.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `CAPR` | 1 | $7.66 | $0.10 | $+0.62 | $10,463.67 | ▲ +0.62 after sell → book $10,475.10; vs 09:30 mark -0.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `KOPN` | 1 | $4.87 | $0.07 | $-0.69 | $10,468.47 | ▼ -0.69 after sell → book $10,475.03; vs 09:30 mark -0.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `NPWR` | 4 | $1.64 | $0.10 | $-1.31 | $10,474.93 | ▼ -1.31 after sell → book $10,474.93; vs 09:30 mark -0.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 8 | $150.14 | $2.01 | — | $9,271.80 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1309.37 | — |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 1138 | $1.15 | $14.68 | — | $7,948.42 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; leftover $1309.37 | — |
| 2026-08-20 09:30 ET | **BUY** | `AZI` | 955 | $1.37 | $12.32 | — | $6,627.75 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+30.7; leftover $1309.37 | — |
| 2026-08-20 09:30 ET | **BUY** | `BTGO` | 198 | $6.61 | $2.58 | — | $5,317.38 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+25.3; leftover $1309.37 | — |
| 2026-08-20 09:30 ET | **BUY** | `BNTX` | 12 | $109.06 | $2.03 | — | $4,006.63 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+22.0; leftover $1309.37 | — |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 530 | $2.47 | $6.84 | — | $2,690.69 | — | rank by ret_5; rank ret_5; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $1309.37 | — |
| 2026-08-20 09:30 ET | **BUY** | `ASST` | 81 | $16.00 | $2.23 | — | $1,392.46 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+18.9; leftover $1309.37 | — |
| 2026-08-20 09:30 ET | **BUY** | `BRR` | 629 | $2.08 | $8.11 | — | $76.03 | — | rank by ret_5; rank ret_5; list ohlc_hot; 🔵; ret5=+18.0; leftover $1309.37 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $76.03 | ▲ close $10,528.78 vs 09:30 $10,475.27 (session +104.65) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $76.03 | ▲ 09:30 equity $10,899.32 vs yday $10,528.78 (+370.54) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `CAPR` | 1 | $6.81 | $0.07 | — | $69.15 | — | rank by ret_5; rank ret_5; list yday_mover; 🔵; ret5=+62.5; leftover $12.67 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 1 | $11.13 | $0.11 | — | $57.90 | — | rank by ret_5; rank ret_5; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $12.67 | — |
| 2026-08-21 09:30 ET | **BUY** | `IOVA` | 1 | $9.08 | $0.09 | — | $48.73 | — | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ⚪; ret5=+37.9; leftover $12.67 | — |
| 2026-08-21 09:30 ET | **BUY** | `MRVI` | 1 | $8.28 | $0.09 | — | $40.36 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.6; leftover $12.67 | — |
| 2026-08-21 09:30 ET | **BUY** | `INO` | 10 | $1.23 | $0.15 | — | $27.91 | — | rank by ret_5; rank ret_5; list mover_buy; 🔵; ⚪; ret5=+34.4; leftover $12.67 | — |
| 2026-08-21 09:30 ET | **BUY** | `CAN` | 43 | $0.29 | $0.26 | — | $15.01 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+30.4; leftover $12.67 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15.01 | ▲ close $11,099.13 vs 09:30 $10,899.32 (session +200.58) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15.01 | ▲ 09:30 equity $11,568.42 vs yday $11,099.13 (+469.29) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $15.01 | ▼ close $11,255.10 vs 09:30 $11,568.42 (session -313.32) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15.01 | ▼ 09:30 equity $11,111.29 vs yday $11,255.10 (-143.81) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `MRNA` | 8 | $143.50 | $2.03 | $-57.17 | $1,160.98 | ▼ -57.17 after sell → book $11,109.25; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AZI` | 955 | $1.31 | $12.49 | $-82.11 | $2,399.54 | ▼ -82.11 after sell → book $11,096.76; vs 09:30 mark -12.49 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BTGO` | 198 | $6.75 | $2.63 | $+23.50 | $3,733.41 | ▲ +23.50 after sell → book $11,094.14; vs 09:30 mark -2.62 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BNTX` | 12 | $113.88 | $2.05 | $+53.77 | $5,097.92 | ▲ +53.77 after sell → book $11,092.09; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AUTL` | 530 | $2.38 | $6.94 | $-61.47 | $6,352.39 | ▼ -61.47 after sell → book $11,085.15; vs 09:30 mark -6.94 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BRR` | 629 | $2.15 | $8.23 | $+24.54 | $7,693.36 | ▲ +24.54 after sell → book $11,076.92; vs 09:30 mark -8.23 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 53 | $24.11 | $2.15 | — | $6,413.39 | — | rank by ret_5; rank ret_5; list yday_mover; ret5=+891.7; leftover $1282.23 | — |
| 2026-08-25 09:30 ET | **BUY** | `SUJA` | 145 | $8.79 | $2.42 | — | $5,136.41 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.1; leftover $1282.23 | — |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 244 | $5.24 | $3.15 | — | $3,854.70 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+35.3; leftover $1282.23 | — |
| 2026-08-25 09:30 ET | **BUY** | `FWDI` | 224 | $5.71 | $2.89 | — | $2,572.77 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+33.9; leftover $1282.23 | — |
| 2026-08-25 09:30 ET | **BUY** | `DEFT` | 2068 | $0.62 | $19.03 | — | $1,271.59 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+33.3; leftover $1282.23 | — |
| 2026-08-25 09:30 ET | **BUY** | `DFDV` | 312 | $4.06 | $4.02 | — | $0.84 | — | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ret5=+29.4; leftover $1282.23 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.84 | ▲ close $11,796.83 vs 09:30 $11,111.29 (session +753.56) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.84 | ▼ 09:30 equity $11,488.81 vs yday $11,796.83 (-308.02) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 1138 | $1.60 | $14.88 | $+482.54 | $1,806.76 | ▲ +482.54 after sell → book $11,473.92; vs 09:30 mark -14.89 | dropped from list after 4 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `ASST` | 81 | $20.72 | $2.26 | $+377.83 | $3,482.82 | ▲ +377.83 after sell → book $11,471.66; vs 09:30 mark -2.26 | dropped from list after 4 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `CAPR` | 1 | $8.29 | $0.11 | $+1.30 | $3,491.00 | ▲ +1.30 after sell → book $11,471.56; vs 09:30 mark -0.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `ARCT` | 1 | $15.35 | $0.18 | $+3.93 | $3,506.18 | ▲ +3.93 after sell → book $11,471.38; vs 09:30 mark -0.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `IOVA` | 1 | $8.34 | $0.11 | $-0.94 | $3,514.41 | ▼ -0.94 after sell → book $11,471.28; vs 09:30 mark -0.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `MRVI` | 1 | $8.85 | $0.11 | $+0.37 | $3,523.15 | ▲ +0.37 after sell → book $11,471.16; vs 09:30 mark -0.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `INO` | 10 | $1.28 | $0.18 | $+0.17 | $3,535.77 | ▲ +0.17 after sell → book $11,470.99; vs 09:30 mark -0.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `CAN` | 43 | $0.40 | $0.32 | $+3.85 | $3,552.52 | ▲ +3.85 after sell → book $11,470.67; vs 09:30 mark -0.32 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 87 | $5.81 | $2.25 | — | $3,044.80 | — | rank by ret_5; rank ret_5; list yday_mover; 🔵; ⚪; ret5=+117.2; leftover $507.50 | — |
| 2026-08-26 09:30 ET | **BUY** | `PURR` | 43 | $11.59 | $2.12 | — | $2,544.53 | — | rank by ret_5; rank ret_5; list overnight; 🔵; ret5=+64.9; leftover $507.50 | — |
| 2026-08-26 09:30 ET | **BUY** | `BTG` | 88 | $5.75 | $2.25 | — | $2,036.27 | — | rank by ret_5; rank ret_5; list ohlc_hot; 🔵; ret5=+17.9; leftover $507.50 | — |
| 2026-08-26 09:30 ET | **BUY** | `MNRO` | 36 | $14.00 | $2.10 | — | $1,530.18 | — | rank by ret_5; rank ret_5; list ohlc_hot; ret5=+17.8; leftover $507.50 | — |
| 2026-08-26 09:30 ET | **BUY** | `BRR` | 230 | $2.20 | $2.97 | — | $1,021.21 | — | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ret5=+17.8; leftover $507.50 | — |
| 2026-08-26 09:30 ET | **BUY** | `PEPG` | 148 | $3.41 | $2.43 | — | $514.09 | — | rank by ret_5; rank ret_5; list ohlc_hot; 🔵; ret5=+17.7; leftover $507.50 | — |
| 2026-08-26 09:30 ET | **BUY** | `AQST` | 99 | $5.08 | $2.29 | — | $8.89 | — | rank by ret_5; rank ret_5; list ohlc_hot; 🔵; ret5=+17.6; leftover $507.50 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8.89 | ▼ close $11,421.06 vs 09:30 $11,488.81 (session -33.20) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8.89 | ▲ 09:30 equity $11,661.41 vs yday $11,421.06 (+240.35) | — | — |
| 2026-08-27 09:30 ET | **BUY** | `INDP` | 1 | $1.13 | $0.01 | — | $7.74 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; ret5=+21.3; leftover $1.27 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7.74 | ▲ close $11,912.30 vs 09:30 $11,661.41 (session +250.91) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7.74 | ▼ 09:30 equity $11,798.84 vs yday $11,912.30 (-113.46) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `REAX` | 53 | $23.40 | $2.17 | $-41.95 | $1,245.77 | ▼ -41.95 after sell → book $11,796.67; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `SUJA` | 145 | $9.08 | $2.46 | $+37.17 | $2,559.91 | ▲ +37.17 after sell → book $11,794.21; vs 09:30 mark -2.46 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ALVO` | 244 | $4.84 | $3.20 | $-103.95 | $3,737.68 | ▼ -103.95 after sell → book $11,791.01; vs 09:30 mark -3.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `FWDI` | 224 | $6.73 | $2.94 | $+222.65 | $5,242.26 | ▲ +222.65 after sell → book $11,788.07; vs 09:30 mark -2.94 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `DEFT` | 2068 | $0.64 | $19.69 | $-7.70 | $6,535.75 | ▼ -7.70 after sell → book $11,768.38; vs 09:30 mark -19.69 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `DFDV` | 312 | $5.26 | $4.09 | $+366.29 | $8,172.78 | ▲ +366.29 after sell → book $11,764.29; vs 09:30 mark -4.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 104 | $9.73 | $2.30 | — | $7,158.56 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; ret5=+47.1; leftover $1021.60 | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 6 | $146.07 | $2.01 | — | $6,280.13 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+38.8; leftover $1021.60 | — |
| 2026-08-28 09:30 ET | **BUY** | `MEI` | 57 | $17.78 | $2.16 | — | $5,264.51 | — | rank by ret_5; rank ret_5; list mover_buy; 🔵; ⚪; ret5=+22.9; leftover $1021.60 | — |
| 2026-08-28 09:30 ET | **BUY** | `LVWR` | 734 | $1.39 | $9.47 | — | $4,234.78 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; ret5=+20.4; leftover $1021.60 | — |
| 2026-08-28 09:30 ET | **BUY** | `VYX` | 111 | $9.13 | $2.32 | — | $3,219.03 | — | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ret5=+20.0; leftover $1021.60 | — |
| 2026-08-28 09:30 ET | **BUY** | `BHVN` | 64 | $15.88 | $2.18 | — | $2,200.52 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; ret5=+19.4; leftover $1021.60 | — |
| 2026-08-28 09:30 ET | **BUY** | `SNPS` | 2 | $461.85 | $2.00 | — | $1,274.83 | — | rank by ret_5; rank ret_5; list ohlc_hot; ret5=+16.8; leftover $1021.60 | — |
| 2026-08-28 09:30 ET | **BUY** | `ADCT` | 920 | $1.11 | $11.87 | — | $241.76 | — | rank by ret_5; rank ret_5; list ohlc_hot; ret5=+16.7; leftover $1021.60 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $241.76 | ▼ close $11,569.90 vs 09:30 $11,798.84 (session -160.09) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $241.76 | ▼ 09:30 equity $11,496.61 vs yday $11,569.90 (-73.29) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `USDE` | 87 | $6.76 | $2.28 | $+78.12 | $827.60 | ▲ +78.12 after sell → book $11,494.33; vs 09:30 mark -2.28 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `PURR` | 43 | $12.19 | $2.14 | $+21.54 | $1,349.42 | ▲ +21.54 after sell → book $11,492.20; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `BTG` | 88 | $5.63 | $2.28 | $-15.09 | $1,842.58 | ▼ -15.09 after sell → book $11,489.92; vs 09:30 mark -2.28 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `MNRO` | 36 | $12.77 | $2.12 | $-48.50 | $2,300.18 | ▼ -48.50 after sell → book $11,487.80; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `BRR` | 230 | $2.23 | $3.02 | $+0.92 | $2,810.07 | ▲ +0.92 after sell → book $11,484.78; vs 09:30 mark -3.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `PEPG` | 148 | $3.04 | $2.47 | $-59.66 | $3,257.52 | ▼ -59.66 after sell → book $11,482.32; vs 09:30 mark -2.46 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `AQST` | 99 | $4.97 | $2.31 | $-15.00 | $3,747.73 | ▼ -15.00 after sell → book $11,480.00; vs 09:30 mark -2.32 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,747.73 | ▼ close $11,435.63 vs 09:30 $11,496.61 (session -44.37) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,747.73 | ▼ 09:30 equity $11,431.36 vs yday $11,435.63 (-4.27) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `INDP` | 1 | $1.10 | $0.03 | $-0.08 | $3,748.80 | ▼ -0.08 after sell → book $11,431.33; vs 09:30 mark -0.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,748.80 | ▲ close $11,462.86 vs 09:30 $11,431.36 (session +31.53) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,748.80 | ▲ 09:30 equity $11,481.57 vs yday $11,462.86 (+18.71) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `CAPR` | 104 | $10.07 | $2.33 | $+30.73 | $4,793.75 | ▲ +30.73 after sell → book $11,479.24; vs 09:30 mark -2.33 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ANF` | 6 | $139.65 | $2.03 | $-42.56 | $5,629.62 | ▼ -42.56 after sell → book $11,477.21; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `MEI` | 57 | $18.22 | $2.18 | $+20.74 | $6,665.98 | ▲ +20.74 after sell → book $11,475.03; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `LVWR` | 734 | $1.17 | $9.60 | $-180.55 | $7,515.16 | ▼ -180.55 after sell → book $11,465.43; vs 09:30 mark -9.60 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `VYX` | 111 | $8.73 | $2.35 | $-49.07 | $8,481.84 | ▼ -49.07 after sell → book $11,463.08; vs 09:30 mark -2.35 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BHVN` | 64 | $15.97 | $2.20 | $+1.38 | $9,501.72 | ▲ +1.38 after sell → book $11,460.88; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SNPS` | 2 | $413.78 | $2.02 | $-100.15 | $10,327.26 | ▼ -100.15 after sell → book $11,458.86; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ADCT` | 920 | $1.23 | $12.03 | $+86.50 | $11,446.83 | ▲ +86.50 after sell → book $11,446.83; vs 09:30 mark -12.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,446.83 | ▲ close $11,446.83 vs 09:30 $11,481.57 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,446.83 | ▲ 09:30 equity $11,446.83 vs yday $11,446.83 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 803 | $1.78 | $10.36 | — | $10,007.13 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; ret5=+183.1; leftover $1430.85 | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 59 | $23.88 | $2.17 | — | $8,596.04 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1430.85 | — |
| 2026-09-03 09:30 ET | **BUY** | `SION` | 195 | $7.31 | $2.58 | — | $7,168.02 | — | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ret5=+18.5; leftover $1430.85 | — |
| 2026-09-03 09:30 ET | **BUY** | `CNH` | 104 | $13.71 | $2.30 | — | $5,739.88 | — | rank by ret_5; rank ret_5; list ohlc_hot; 🔵; ret5=+17.5; leftover $1430.85 | — |
| 2026-09-03 09:30 ET | **BUY** | `PBR` | 67 | $21.18 | $2.19 | — | $4,318.62 | — | rank by ret_5; rank ret_5; list ohlc_hot; ret5=+17.5; leftover $1430.85 | — |
| 2026-09-03 09:30 ET | **BUY** | `PBR-A` | 74 | $19.16 | $2.21 | — | $2,898.57 | — | rank by ret_5; rank ret_5; list ohlc_hot; ret5=+17.4; leftover $1430.85 | — |
| 2026-09-03 09:30 ET | **BUY** | `TARS` | 17 | $82.76 | $2.04 | — | $1,489.61 | — | rank by ret_5; rank ret_5; list ohlc_hot; ret5=+17.1; leftover $1430.85 | — |
| 2026-09-03 09:30 ET | **BUY** | `FRVO` | 78 | $18.28 | $2.22 | — | $61.55 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; ret5=+16.5; leftover $1430.85 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $61.55 | ▼ close $10,841.94 vs 09:30 $11,446.83 (session -578.82) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $61.55 | ▲ 09:30 equity $10,872.06 vs yday $10,841.94 (+30.12) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `BRR` | 3 | $2.51 | $0.08 | — | $53.93 | — | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ⚪; ret5=+21.8; leftover $8.79 | — |
| 2026-09-04 09:30 ET | **BUY** | `LENZ` | 1 | $5.75 | $0.06 | — | $48.12 | — | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ⚪; ret5=+21.2; leftover $8.79 | — |
| 2026-09-04 09:30 ET | **BUY** | `IRD` | 1 | $4.53 | $0.05 | — | $43.54 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.5; leftover $8.79 | — |
| 2026-09-04 09:30 ET | **BUY** | `SLBT` | 2 | $3.15 | $0.07 | — | $37.18 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+19.3; leftover $8.79 | — |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 4 | $1.94 | $0.09 | — | $29.33 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; ret5=+18.3; leftover $8.79 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $29.33 | ▲ close $11,356.45 vs 09:30 $10,872.06 (session +484.74) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $29.33 | ▼ 09:30 equity $11,313.72 vs yday $11,356.45 (-42.73) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $29.33 | ▼ close $11,190.44 vs 09:30 $11,313.72 (session -123.28) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $29.33 | ▼ 09:30 equity $11,177.88 vs yday $11,190.44 (-12.56) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `GPRO` | 803 | $1.45 | $10.50 | $-285.85 | $1,183.17 | ▼ -285.85 after sell → book $11,167.37; vs 09:30 mark -10.51 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MMED` | 59 | $23.22 | $2.19 | $-43.29 | $2,550.97 | ▼ -43.29 after sell → book $11,165.19; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `SION` | 195 | $7.27 | $2.62 | $-12.99 | $3,966.00 | ▼ -12.99 after sell → book $11,162.57; vs 09:30 mark -2.62 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CNH` | 104 | $13.64 | $2.33 | $-11.91 | $5,382.23 | ▼ -11.91 after sell → book $11,160.24; vs 09:30 mark -2.33 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `PBR` | 67 | $21.11 | $2.21 | $-9.09 | $6,794.38 | ▼ -9.09 after sell → book $11,158.02; vs 09:30 mark -2.22 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `PBR-A` | 74 | $19.10 | $2.24 | $-8.89 | $8,205.55 | ▼ -8.89 after sell → book $11,155.79; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `TARS` | 17 | $86.31 | $2.06 | $+56.25 | $9,670.75 | ▲ +56.25 after sell → book $11,153.72; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `FRVO` | 78 | $18.60 | $2.25 | $+20.49 | $11,119.31 | ▲ +20.49 after sell → book $11,151.48; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,119.31 | ▲ close $11,151.63 vs 09:30 $11,177.88 (session +0.15) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,119.31 | ▲ 09:30 equity $11,151.68 vs yday $11,151.63 (+0.05) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `BRR` | 3 | $2.87 | $0.12 | $+0.88 | $11,127.80 | ▲ +0.88 after sell → book $11,151.56; vs 09:30 mark -0.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `LENZ` | 1 | $4.85 | $0.07 | $-1.03 | $11,132.58 | ▼ -1.03 after sell → book $11,151.49; vs 09:30 mark -0.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `SLBT` | 2 | $2.58 | $0.08 | $-1.29 | $11,137.66 | ▼ -1.29 after sell → book $11,151.41; vs 09:30 mark -0.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BAK` | 4 | $1.97 | $0.11 | $-0.08 | $11,145.43 | ▼ -0.08 after sell → book $11,151.30; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,145.43 | ▲ close $11,151.50 vs 09:30 $11,151.68 (session +0.20) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,145.43 | ▲ 09:30 equity $11,151.59 vs yday $11,151.50 (+0.09) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `INDP` | 589 | $2.70 | $7.60 | — | $9,547.53 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+118.8; leftover $1592.20 | — |
| 2026-09-11 09:30 ET | **BUY** | `BNC` | 324 | $4.91 | $4.18 | — | $7,952.51 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+76.3; leftover $1592.20 | — |
| 2026-09-11 09:30 ET | **BUY** | `CYPH` | 666 | $2.39 | $8.59 | — | $6,352.18 | — | rank by ret_5; rank ret_5; list yday_mover; 🔵; ret5=+31.0; leftover $1592.20 | — |
| 2026-09-11 09:30 ET | **BUY** | `ASO` | 28 | $54.91 | $2.07 | — | $4,812.63 | — | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ret5=+24.3; leftover $1592.20 | — |
| 2026-09-11 09:30 ET | **BUY** | `CMRC` | 508 | $3.13 | $6.55 | — | $3,216.04 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; ret5=+24.2; leftover $1592.20 | — |
| 2026-09-11 09:30 ET | **BUY** | `WLTH` | 145 | $10.95 | $2.42 | — | $1,625.86 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+20.8; leftover $1592.20 | — |
| 2026-09-11 09:30 ET | **BUY** | `APPS` | 134 | $11.88 | $2.39 | — | $31.55 | — | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ret5=+20.7; leftover $1592.20 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $31.55 | ▲ close $11,151.07 vs 09:30 $11,151.59 (session +33.29) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $31.55 | ▲ 09:30 equity $11,204.28 vs yday $11,151.07 (+53.21) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `IRD` | 1 | $6.02 | $0.08 | $+1.36 | $37.48 | ▲ +1.36 after sell → book $11,204.19; vs 09:30 mark -0.09 | dropped from list after 5 sess (min 3) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $37.48 | ▲ close $11,796.89 vs 09:30 $11,204.28 (session +592.70) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $37.48 | ▼ 09:30 equity $11,722.79 vs yday $11,796.89 (-74.10) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $37.48 | ▼ close $11,582.83 vs 09:30 $11,722.79 (session -139.96) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $37.48 | ▲ 09:30 equity $11,667.30 vs yday $11,582.83 (+84.47) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `BNC` | 324 | $4.77 | $4.25 | $-53.79 | $1,578.72 | ▼ -53.79 after sell → book $11,663.06; vs 09:30 mark -4.24 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `CYPH` | 666 | $2.49 | $8.72 | $+49.29 | $3,228.34 | ▲ +49.29 after sell → book $11,654.34; vs 09:30 mark -8.72 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `ASO` | 28 | $50.69 | $2.10 | $-122.33 | $4,645.57 | ▼ -122.33 after sell → book $11,652.25; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `CMRC` | 508 | $3.48 | $6.65 | $+164.60 | $6,406.76 | ▲ +164.60 after sell → book $11,645.60; vs 09:30 mark -6.65 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `WLTH` | 145 | $10.82 | $2.46 | $-23.74 | $7,973.20 | ▼ -23.74 after sell → book $11,643.14; vs 09:30 mark -2.46 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `APPS` | 134 | $11.30 | $2.43 | $-82.54 | $9,484.97 | ▼ -82.54 after sell → book $11,640.71; vs 09:30 mark -2.43 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `HLP` | 752 | $1.80 | $9.70 | — | $8,121.67 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+30.5; leftover $1355.00 | — |
| 2026-09-16 09:30 ET | **BUY** | `SWKS` | 15 | $89.38 | $2.04 | — | $6,778.93 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.4; leftover $1355.00 | — |
| 2026-09-16 09:30 ET | **BUY** | `REF` | 86 | $15.75 | $2.25 | — | $5,422.19 | — | rank by ret_5; rank ret_5; list yday_gainer,ohlc_hot; ret5=+17.3; leftover $1355.00 | — |
| 2026-09-16 09:30 ET | **BUY** | `SDGR` | 58 | $23.29 | $2.16 | — | $4,069.20 | — | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ret5=+16.1; leftover $1355.00 | — |
| 2026-09-16 09:30 ET | **BUY** | `FTRE` | 68 | $19.75 | $2.19 | — | $2,724.01 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+15.7; leftover $1355.00 | — |
| 2026-09-16 09:30 ET | **BUY** | `CRWD` | 5 | $236.92 | $2.00 | — | $1,537.40 | — | rank by ret_5; rank ret_5; list ohlc_hot; ret5=+15.5; leftover $1355.00 | — |
| 2026-09-16 09:30 ET | **BUY** | `CAI` | 48 | $28.16 | $2.13 | — | $183.59 | — | rank by ret_5; rank ret_5; list yday_gainer,ohlc_hot; ret5=+14.8; leftover $1355.00 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $183.59 | ▼ close $11,574.33 vs 09:30 $11,667.30 (session -43.90) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $183.59 | ▲ 09:30 equity $11,702.05 vs yday $11,574.33 (+127.72) | — | — |
| 2026-09-17 09:30 ET | **BUY** | `BBNX` | 1 | $22.46 | $0.23 | — | $160.90 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+27.3; leftover $30.60 | — |
| 2026-09-17 09:30 ET | **BUY** | `EMAT` | 7 | $3.86 | $0.29 | — | $133.59 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+18.7; leftover $30.60 | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 2 | $10.25 | $0.21 | — | $112.88 | — | rank by ret_5; rank ret_5; list flatten,ohlc_hot; 🔵; ret5=+17.1; leftover $30.60 | — |
| 2026-09-17 09:30 ET | **BUY** | `IQ` | 28 | $1.07 | $0.38 | — | $82.53 | — | rank by ret_5; rank ret_5; list yday_gainer,ohlc_hot; 🔵; ret5=+15.8; leftover $30.60 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $82.53 | ▲ close $12,548.83 vs 09:30 $11,702.05 (session +847.90) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $82.53 | ▼ 09:30 equity $12,369.78 vs yday $12,548.83 (-179.05) | — | — |
| 2026-09-18 09:30 ET | **BUY** | `CYPH` | 4 | $3.04 | $0.13 | — | $70.26 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+39.5; leftover $13.76 | — |
| 2026-09-18 09:30 ET | **BUY** | `LVWR` | 9 | $1.49 | $0.16 | — | $56.69 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+25.7; leftover $13.76 | — |
| 2026-09-18 09:30 ET | **BUY** | `RXT` | 3 | $3.94 | $0.13 | — | $44.74 | — | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ⚪; ret5=+25.2; leftover $13.76 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $44.74 | ▼ close $12,049.28 vs 09:30 $12,369.78 (session -320.08) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $44.74 | ▲ 09:30 equity $12,155.06 vs yday $12,049.28 (+105.78) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `INDP` | 589 | $3.55 | $7.71 | $+485.34 | $2,127.98 | ▲ +485.34 after sell → book $12,147.35; vs 09:30 mark -7.71 | dropped from list after 6 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `HLP` | 752 | $2.08 | $9.84 | $+191.02 | $3,682.30 | ▲ +191.02 after sell → book $12,137.51; vs 09:30 mark -9.84 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `SWKS` | 15 | $89.66 | $2.06 | $+0.11 | $5,025.15 | ▲ +0.11 after sell → book $12,135.46; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `REF` | 86 | $14.79 | $2.27 | $-87.08 | $6,294.81 | ▼ -87.08 after sell → book $12,133.18; vs 09:30 mark -2.28 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `SDGR` | 58 | $29.43 | $2.19 | $+351.77 | $7,999.57 | ▲ +351.77 after sell → book $12,131.00; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `FTRE` | 68 | $20.29 | $2.22 | $+32.31 | $9,377.07 | ▲ +32.31 after sell → book $12,128.78; vs 09:30 mark -2.22 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `CRWD` | 5 | $231.62 | $2.02 | $-30.53 | $10,533.14 | ▼ -30.53 after sell → book $12,126.75; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `CAI` | 48 | $30.23 | $2.16 | $+95.07 | $11,982.03 | ▲ +95.07 after sell → book $12,124.60; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **BUY** | `FEAM` | 693 | $2.47 | $8.94 | — | $10,261.38 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; ret5=+73.6; leftover $1711.72 | — |
| 2026-09-21 09:30 ET | **BUY** | `TJGC` | 101 | $16.91 | $2.29 | — | $8,551.18 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; ret5=+50.5; leftover $1711.72 | — |
| 2026-09-21 09:30 ET | **BUY** | `USDE` | 131 | $13.05 | $2.38 | — | $6,839.24 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+40.4; leftover $1711.72 | — |
| 2026-09-21 09:30 ET | **BUY** | `SECZ` | 146 | $11.67 | $2.43 | — | $5,133.00 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; ret5=+31.3; leftover $1711.72 | — |
| 2026-09-21 09:30 ET | **BUY** | `GEMI` | 297 | $5.75 | $3.83 | — | $3,419.93 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+30.3; leftover $1711.72 | — |
| 2026-09-21 09:30 ET | **BUY** | `ABTC` | 159 | $10.71 | $2.47 | — | $1,714.57 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+21.4; leftover $1711.72 | — |
| 2026-09-21 09:30 ET | **BUY** | `DFDV` | 262 | $6.51 | $3.38 | — | $5.57 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+19.3; leftover $1711.72 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5.57 | ▲ close $12,377.94 vs 09:30 $12,155.06 (session +279.07) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5.57 | ▼ 09:30 equity $12,369.92 vs yday $12,377.94 (-8.02) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `IOVA` | 2 | $10.18 | $0.23 | $-0.58 | $25.70 | ▼ -0.58 after sell → book $12,369.69; vs 09:30 mark -0.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $25.70 | ▲ close $12,417.27 vs 09:30 $12,369.92 (session +47.59) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $25.70 | ▲ 09:30 equity $12,434.71 vs yday $12,417.27 (+17.44) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `BBNX` | 1 | $23.00 | $0.25 | $+0.06 | $48.45 | ▲ +0.06 after sell → book $12,434.46; vs 09:30 mark -0.25 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `EMAT` | 7 | $3.65 | $0.30 | $-2.06 | $73.70 | ▼ -2.06 after sell → book $12,434.16; vs 09:30 mark -0.30 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `IQ` | 28 | $1.03 | $0.39 | $-1.90 | $102.15 | ▼ -1.90 after sell → book $12,433.77; vs 09:30 mark -0.39 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `CYPH` | 4 | $3.82 | $0.18 | $+2.82 | $117.25 | ▲ +2.82 after sell → book $12,433.59; vs 09:30 mark -0.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `LVWR` | 9 | $1.41 | $0.17 | $-1.05 | $129.76 | ▼ -1.05 after sell → book $12,433.41; vs 09:30 mark -0.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `RXT` | 3 | $4.07 | $0.15 | $+0.11 | $141.82 | ▲ +0.11 after sell → book $12,433.26; vs 09:30 mark -0.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **BUY** | `GLND` | 7 | $2.70 | $0.21 | — | $122.71 | — | rank by ret_5; rank ret_5; list yday_mover; 🔵; ret5=+109.2; leftover $20.26 | — |
| 2026-09-23 09:30 ET | **BUY** | `ORBS` | 17 | $1.15 | $0.25 | — | $102.91 | — | rank by ret_5; rank ret_5; list yday_gainer; ret5=+36.2; leftover $20.26 | — |
| 2026-09-23 09:30 ET | **BUY** | `EVTL` | 27 | $0.73 | $0.28 | — | $82.82 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+27.5; leftover $20.26 | — |
| 2026-09-23 09:30 ET | **BUY** | `BFLY` | 2 | $9.90 | $0.20 | — | $62.81 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+27.3; leftover $20.26 | — |
| 2026-09-23 09:30 ET | **BUY** | `SVIA` | 4 | $4.49 | $0.19 | — | $44.66 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; ret5=+26.4; leftover $20.26 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $44.66 | ▲ close $12,858.88 vs 09:30 $12,434.71 (session +426.75) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $44.66 | ▲ 09:30 equity $12,948.10 vs yday $12,858.88 (+89.22) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `FEAM` | 693 | $2.68 | $9.07 | $+127.52 | $1,892.83 | ▲ +127.52 after sell → book $12,939.03; vs 09:30 mark -9.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `USDE` | 131 | $12.76 | $2.42 | $-42.79 | $3,561.97 | ▼ -42.79 after sell → book $12,936.61; vs 09:30 mark -2.42 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `GEMI` | 297 | $5.62 | $3.89 | $-47.82 | $5,227.22 | ▼ -47.82 after sell → book $12,932.72; vs 09:30 mark -3.89 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `ABTC` | 159 | $9.64 | $2.51 | $-175.10 | $6,757.47 | ▼ -175.10 after sell → book $12,930.21; vs 09:30 mark -2.51 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `DFDV` | 262 | $5.77 | $3.44 | $-200.70 | $8,265.78 | ▼ -200.70 after sell → book $12,926.78; vs 09:30 mark -3.43 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,265.78 | ▲ close $13,635.26 vs 09:30 $12,948.10 (session +708.48) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,137.35 | ▲ 09:30 equity $10,949.66 vs yday $10,799.70 (+149.96) | 09:30 open · cash $7,137.35 (unchanged overnight, no fees) · equity $10,949.66 vs prior close $10,799.70 (+149.96) · 13 name(s) re-marked at the open (per-name table). AEHL×7 yday $8.96 → 09:30 $9.05 +0.63; AIBZ×30 yday $4.31 → 09:30 $4.31 +0.00; ARQQ×6 yday $23.12 → 09:30 $23.12 +0.00; ASX×3 yday $43.50 → 09:30 $43.50 +0.00; DNA×6 yday $10.25 → 09:30 $10.20 -0.30; EU×50 yday $1.22 → 09:30 $1.22 +0.00; FOSL×25 yday $5.85 → 09:30 $5.85 +0.00; GLND×22 yday $5.35 → 09:30 $6.06 +15.62; GRAL×1 yday $125.21 → 09:30 $123.50 -1.71; NUAI×21 yday $6.94 → 09:30 $6.94 +0.00; ORBS×52 yday $1.03 → 09:30 $1.03 +0.00; TJGC×87 yday $28.20 → 09:30 $29.76 +135.72; VKTX×1 yday $36.75 → 09:30 $36.75 +0.00 | — |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 110 | $16.21 | $2.32 | — | $5,351.93 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; leftover $1784.34 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `USDE` | 114 | $15.58 | $2.33 | — | $3,573.35 | — | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ret5=+84.4; leftover $1784.34 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ZSQR` | 462 | $3.86 | $5.96 | — | $1,784.07 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; leftover $1784.34 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `CYPH` | 444 | $4.00 | $5.73 | — | $0.13 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+32.9; leftover $1784.34 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.13 | ▼ close $10,800.82 vs 09:30 $10,949.66 (session -132.51) | 16:00 close · cash $0.13 · equity $10,800.82 vs 09:30 $10,949.66 (-148.84; session marks -132.51) · 17 name(s) marked open→close (per-name table). AEHL×7 09:30 $9.05 → close $9.36 +2.17; AIBZ×30 09:30 $4.31 → close $4.31 -0.00; ARQQ×6 09:30 $23.12 → close $23.12 +0.00; ASX×3 09:30 $43.50 → close $43.50 +0.00; DNA×6 09:30 $10.20 → close $10.66 +2.76; EU×50 09:30 $1.22 → close $1.22 +0.00; FOSL×25 09:30 $5.85 → close $5.85 -0.00; GLND×22 09:30 $6.06 → close $5.54 -11.44; GRAL×1 09:30 $123.50 → close $126.89 +3.39; NUAI×21 09:30 $6.94 → close $6.94 +0.00; ORBS×52 09:30 $1.03 → close $1.03 -0.00; TJGC×87 09:30 $29.76 → close $26.24 -306.24; VKTX×1 09:30 $36.75 → close $36.75 +0.00; SECZ×110 09:30 $16.21 → close $15.96 -27.50; USDE×114 09:30 $15.58 → close $17.25 +190.25; ZSQR×462 09:30 $3.86 → close $3.78 -36.96; CYPH×444 09:30 $4.00 → close $4.12 +51.06 | — |
| 2026-09-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.13 | ▼ 09:30 equity $10,710.16 vs yday $10,800.82 (-90.66) | 09:30 open · cash $0.13 (unchanged overnight, no fees) · equity $10,710.16 vs prior close $10,800.82 (-90.66) · 17 name(s) re-marked at the open (per-name table). AEHL×7 yday $9.36 → 09:30 $9.01 -2.45; AIBZ×30 yday $4.31 → 09:30 $4.02 -8.70; ARQQ×6 yday $23.12 → 09:30 $24.30 +7.08; ASX×3 yday $43.50 → 09:30 $44.13 +1.89; CYPH×444 yday $4.12 → 09:30 $4.03 -38.85; DNA×6 yday $10.66 → 09:30 $10.38 -1.71; EU×50 yday $1.22 → 09:30 $1.21 -0.50; FOSL×25 yday $5.85 → 09:30 $6.05 +5.00; GLND×22 yday $5.54 → 09:30 $5.14 -8.84; GRAL×1 yday $126.89 → 09:30 $128.90 +2.01; NUAI×21 yday $6.94 → 09:30 $6.98 +0.84; ORBS×52 yday $1.03 → 09:30 $1.15 +6.24; SECZ×110 yday $15.96 → 09:30 $16.00 +4.40; TJGC×87 yday $26.24 → 09:30 $26.50 +22.62; USDE×114 yday $17.25 → 09:30 $16.85 -45.60; VKTX×1 yday $36.75 → 09:30 $35.00 -1.75; ZSQR×462 yday $3.78 → 09:30 $3.71 -32.34 | — |
| 2026-09-28 09:30 ET | **SELL** | `AEHL` | 7 | $9.01 | $0.67 | $+8.50 | $62.53 | ▲ +8.50 after sell → book $10,709.48; vs 09:30 mark -0.68 | dropped from list after 3 sess (min 3) | — |
| 2026-09-28 09:30 ET | **SELL** | `AIBZ` | 30 | $4.02 | $1.32 | $-34.73 | $181.81 | ▼ -34.73 after sell → book $10,708.17; vs 09:30 mark -1.31 | exit unpriced hold on first bar after 4 sess | — |
| 2026-09-28 09:30 ET | **SELL** | `ARQQ` | 6 | $24.30 | $1.50 | $+1.82 | $326.12 | ▲ +1.82 after sell → book $10,706.67; vs 09:30 mark -1.50 | exit unpriced hold on first bar after 4 sess | — |
| 2026-09-28 09:30 ET | **SELL** | `ASX` | 3 | $44.13 | $1.35 | $+0.16 | $457.15 | ▲ +0.16 after sell → book $10,705.32; vs 09:30 mark -1.35 | exit unpriced hold on first bar after 4 sess | — |
| 2026-09-28 09:30 ET | **SELL** | `DNA` | 6 | $10.38 | $0.66 | $+6.24 | $518.74 | ▲ +6.24 after sell → book $10,704.66; vs 09:30 mark -0.66 | dropped from list after 3 sess (min 3) | — |
| 2026-09-28 09:30 ET | **SELL** | `EU` | 50 | $1.21 | $0.78 | $-1.03 | $578.47 | ▼ -1.03 after sell → book $10,703.88; vs 09:30 mark -0.78 | exit unpriced hold on first bar after 3 sess | — |
| 2026-09-28 09:30 ET | **SELL** | `FOSL` | 25 | $6.05 | $1.61 | $+0.09 | $728.11 | ▲ +0.09 after sell → book $10,702.28; vs 09:30 mark -1.60 | exit unpriced hold on first bar after 4 sess | — |
| 2026-09-28 09:30 ET | **SELL** | `GLND` | 22 | $5.14 | $1.22 | $+51.76 | $839.93 | ▲ +51.76 after sell → book $10,701.06; vs 09:30 mark -1.22 | dropped from list after 3 sess (min 3) | — |
| 2026-09-28 09:30 ET | **SELL** | `GRAL` | 1 | $128.90 | $1.31 | $+19.77 | $967.52 | ▲ +19.77 after sell → book $10,699.75; vs 09:30 mark -1.31 | dropped from list after 4 sess (min 3) | — |
| 2026-09-28 09:30 ET | **SELL** | `NUAI` | 21 | $6.98 | $1.55 | $-8.38 | $1,112.55 | ▼ -8.38 after sell → book $10,698.20; vs 09:30 mark -1.55 | exit unpriced hold on first bar after 4 sess | — |
| 2026-09-28 09:30 ET | **SELL** | `ORBS` | 52 | $1.15 | $0.77 | $-1.53 | $1,171.58 | ▼ -1.53 after sell → book $10,697.43; vs 09:30 mark -0.77 | exit unpriced hold on first bar after 3 sess | join🔴 sector🔴 gen🔴 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-28 09:30 ET | **SELL** | `TJGC` | 87 | $26.50 | $2.28 | $+829.80 | $3,474.79 | ▲ +829.80 after sell → book $10,695.14; vs 09:30 mark -2.29 | dropped from list after 5 sess (min 3) | — |
| 2026-09-28 09:30 ET | **SELL** | `VKTX` | 1 | $35.00 | $0.37 | $-7.55 | $3,509.42 | ▼ -7.55 after sell → book $10,694.77; vs 09:30 mark -0.37 | exit unpriced hold on first bar after 3 sess | — |
| 2026-09-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,509.42 | ▼ close $10,679.68 vs 09:30 $10,710.16 (session -15.09) | 16:00 close · cash $3,509.42 · equity $10,679.68 vs 09:30 $10,710.16 (-30.48; session marks -15.09) · 4 name(s) marked open→close (per-name table). CYPH×444 09:30 $4.03 → close $3.31 -320.79; SECZ×110 09:30 $16.00 → close $16.63 +69.30; USDE×114 09:30 $16.85 → close $15.56 -147.06; ZSQR×462 09:30 $3.71 → close $4.54 +383.46 | — |
| 2026-09-29 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,509.42 | ▲ 09:30 equity $10,723.52 vs yday $10,679.68 (+43.84) | 09:30 open · cash $3,509.42 (unchanged overnight, no fees) · equity $10,723.52 vs prior close $10,679.68 (+43.84) · 4 name(s) re-marked at the open (per-name table). CYPH×444 yday $3.31 → 09:30 $3.34 +13.32; SECZ×110 yday $16.63 → 09:30 $16.59 -4.40; USDE×114 yday $15.56 → 09:30 $16.15 +67.26; ZSQR×462 yday $4.54 → 09:30 $4.47 -32.34 | — |
| 2026-09-29 09:30 ET | **BUY** | `KOD` | 5 | $87.50 | $2.00 | — | $3,069.91 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+177.4; leftover $501.35 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `SRFM` | 421 | $1.19 | $5.43 | — | $2,563.49 | — | rank by ret_5; rank ret_5; list yday_gainer; ret5=+77.6; leftover $501.35 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `TJGC` | 18 | $27.68 | $2.04 | — | $2,063.21 | — | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ret5=+62.3; leftover $501.35 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 heat🟢 vol🔴 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `GLND` | 125 | $3.98 | $2.37 | — | $1,563.35 | — | rank by ret_5; rank ret_5; list yday_mover; ret5=+55.9; leftover $501.35 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `GRML` | 47 | $10.50 | $2.13 | — | $1,067.71 | — | rank by ret_5; rank ret_5; list yday_mover; ret5=+25.8; leftover $501.35 | join🔴 sector🟡 gen🟢 news🟡 digest🔴 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `QNC` | 243 | $2.06 | $3.13 | — | $564.00 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; ret5=+24.3; leftover $501.35 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `AEHL` | 60 | $8.26 | $2.17 | — | $66.53 | — | rank by ret_5; rank ret_5; list yday_mover; 🔵; ret5=+18.9; leftover $501.35 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-29 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $66.53 | ▼ close $10,515.96 vs 09:30 $10,723.52 (session -188.28) | 16:00 close · cash $66.53 · equity $10,515.96 vs 09:30 $10,723.52 (-207.56; session marks -188.28) · 11 name(s) marked open→close (per-name table). CYPH×444 09:30 $3.34 → close $3.23 -48.84; SECZ×110 09:30 $16.59 → close $15.84 -82.50; USDE×114 09:30 $16.15 → close $15.32 -94.62; ZSQR×462 09:30 $4.47 → close $4.15 -147.84; KOD×5 09:30 $87.50 → close $91.12 +18.10; SRFM×421 09:30 $1.19 → close $1.20 +4.21; TJGC×18 09:30 $27.68 → close $34.80 +128.07; GLND×125 09:30 $3.98 → close $4.75 +96.25; GRML×47 09:30 $10.50 → close $10.38 -5.88; QNC×243 09:30 $2.06 → close $1.75 -75.33; AEHL×60 09:30 $8.26 → close $8.59 +20.10 | — |
| 2026-09-30 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $66.53 | ▲ 09:30 equity $10,515.97 vs yday $10,515.96 (+0.01) | 09:30 open · cash $66.53 (unchanged overnight, no fees) · equity $10,515.97 vs prior close $10,515.96 (+0.01) · 11 name(s) re-marked at the open (per-name table). AEHL×60 yday $8.59 → 09:30 $8.59 +0.00; CYPH×444 yday $3.23 → 09:30 $3.23 +0.00; GLND×125 yday $4.75 → 09:30 $4.75 +0.00; GRML×47 yday $10.38 → 09:30 $10.38 +0.00; KOD×5 yday $91.12 → 09:30 $91.12 +0.00; QNC×243 yday $1.75 → 09:30 $1.75 +0.00; SECZ×110 yday $15.84 → 09:30 $15.84 +0.00; SRFM×421 yday $1.20 → 09:30 $1.20 +0.00; TJGC×18 yday $34.80 → 09:30 $34.80 +0.00; USDE×114 yday $15.32 → 09:30 $15.32 +0.00; ZSQR×462 yday $4.15 → 09:30 $4.15 +0.00 | — |
| 2026-09-30 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $66.53 | ▲ close $10,515.97 vs 09:30 $10,515.97 (session +0.00) | 16:00 close · cash $66.53 · equity $10,515.97 vs 09:30 $10,515.97 (-0.00; session marks +0.00) · 11 name(s) marked open→close (per-name table). AEHL×60 09:30 $8.59 → close $8.59 +0.00; CYPH×444 09:30 $3.23 → close $3.23 +0.00; GLND×125 09:30 $4.75 → close $4.75 +0.00; GRML×47 09:30 $10.38 → close $10.38 +0.00; KOD×5 09:30 $91.12 → close $91.12 +0.00; QNC×243 09:30 $1.75 → close $1.75 +0.00; SECZ×110 09:30 $15.84 → close $15.84 +0.00; SRFM×421 09:30 $1.20 → close $1.20 +0.00; TJGC×18 09:30 $34.80 → close $34.80 +0.00; USDE×114 09:30 $15.32 → close $15.32 +0.00; ZSQR×462 09:30 $4.15 → close $4.15 +0.00 | — |
| 2026-10-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $66.53 | ▼ 09:30 equity $10,340.02 vs yday $10,515.97 (-175.95) | 09:30 open · cash $66.53 (unchanged overnight, no fees) · equity $10,340.02 vs prior close $10,515.97 (-175.95) · 11 name(s) re-marked at the open (per-name table). AEHL×60 yday $8.59 → 09:30 $9.74 +69.00; CYPH×444 yday $3.23 → 09:30 $3.13 -44.40; GLND×125 yday $4.75 → 09:30 $5.06 +38.75; GRML×47 yday $10.38 → 09:30 $10.04 -15.75; KOD×5 yday $91.12 → 09:30 $95.41 +21.45; QNC×243 yday $1.75 → 09:30 $1.57 -43.74; SECZ×110 yday $15.84 → 09:30 $14.55 -141.90; SRFM×421 yday $1.20 → 09:30 $1.15 -21.05; TJGC×18 yday $34.80 → 09:30 $26.31 -152.73; USDE×114 yday $15.32 → 09:30 $16.04 +82.08; ZSQR×462 yday $4.15 → 09:30 $4.22 +32.34 | — |
| 2026-10-01 09:30 ET | **SELL** | `AEHL` | 60 | $9.74 | $2.19 | $+84.74 | $648.74 | ▲ +84.74 after sell → book $10,337.83; vs 09:30 mark -2.19 | exit unpriced hold on first bar after 2 sess | join🔴 sector🟢 gen🔴 news🔴 digest🟢 judge🔴 ab🔴 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-10-01 09:30 ET | **SELL** | `CYPH` | 444 | $3.13 | $5.81 | $-400.04 | $2,032.65 | ▼ -400.04 after sell → book $10,332.02; vs 09:30 mark -5.81 | exit unpriced hold on first bar after 4 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `GLND` | 125 | $5.06 | $2.40 | $+130.24 | $2,662.75 | ▲ +130.24 after sell → book $10,329.62; vs 09:30 mark -2.40 | exit unpriced hold on first bar after 2 sess | join🔴 sector🔴 gen🔴 news🟢 digest🟢 judge🟡 ab🟡 heat🔴 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **SELL** | `GRML` | 47 | $10.04 | $2.15 | $-25.90 | $3,132.48 | ▼ -25.90 after sell → book $10,327.47; vs 09:30 mark -2.15 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `KOD` | 5 | $95.41 | $2.02 | $+35.52 | $3,607.51 | ▲ +35.52 after sell → book $10,325.45; vs 09:30 mark -2.02 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `QNC` | 243 | $1.57 | $3.19 | $-125.39 | $3,985.83 | ▼ -125.39 after sell → book $10,322.26; vs 09:30 mark -3.19 | exit unpriced hold on first bar after 2 sess | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **SELL** | `SECZ` | 110 | $14.55 | $2.35 | $-187.27 | $5,583.98 | ▼ -187.27 after sell → book $10,319.91; vs 09:30 mark -2.35 | exit unpriced hold on first bar after 4 sess | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-10-01 09:30 ET | **SELL** | `SRFM` | 421 | $1.15 | $5.51 | $-27.78 | $6,062.62 | ▼ -27.78 after sell → book $10,314.40; vs 09:30 mark -5.51 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `TJGC` | 18 | $26.31 | $2.06 | $-28.77 | $6,534.13 | ▼ -28.77 after sell → book $10,312.33; vs 09:30 mark -2.07 | exit unpriced hold on first bar after 2 sess | join🟢 sector🟢 gen🔴 news🟡 digest🟢 ab🔴 heat🟢 vol🔴 buy🟡 |
| 2026-10-01 09:30 ET | **SELL** | `USDE` | 114 | $16.04 | $2.37 | $+47.62 | $8,360.33 | ▲ +47.62 after sell → book $10,309.97; vs 09:30 mark -2.36 | exit unpriced hold on first bar after 4 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `ZSQR` | 462 | $4.22 | $6.05 | $+154.31 | $10,303.92 | ▲ +154.31 after sell → book $10,303.92; vs 09:30 mark -6.05 | exit unpriced hold on first bar after 4 sess | — |
| 2026-10-01 09:30 ET | **BUY** | `PACB` | 733 | $2.34 | $9.46 | — | $8,579.24 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; ret5=+68.1; leftover $1717.32 | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟡 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `QSI` | 1431 | $1.20 | $18.46 | — | $6,843.58 | — | rank by ret_5; rank ret_5; list yday_gainer; ret5=+41.0; leftover $1717.32 | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `PMVP` | 1022 | $1.68 | $13.18 | — | $5,113.44 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; ret5=+21.9; leftover $1717.32 | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `QTRX` | 461 | $3.72 | $5.95 | — | $3,392.57 | — | rank by ret_5; rank ret_5; list ohlc_hot; ret5=+17.4; leftover $1717.32 | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `MX` | 454 | $3.78 | $5.86 | — | $1,670.59 | — | rank by ret_5; rank ret_5; list ohlc_hot; 🔵; ret5=+16.8; leftover $1717.32 | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **BUY** | `MNKD` | 433 | $3.84 | $5.59 | — | $2.29 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+16.8; leftover $1717.32 | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.29 | ▲ close $10,611.87 vs 09:30 $10,340.02 (session +366.44) | 16:00 close · cash $2.29 · equity $10,611.87 vs 09:30 $10,340.02 (+271.85; session marks +366.44) · 6 name(s) marked open→close (per-name table). PACB×733 09:30 $2.34 → close $2.51 +124.61; QSI×1431 09:30 $1.20 → close $1.43 +329.13; PMVP×1022 09:30 $1.68 → close $1.75 +71.54; QTRX×461 09:30 $3.72 → close $3.41 -142.91; MX×454 09:30 $3.78 → close $3.64 -63.56; MNKD×433 09:30 $3.84 → close $3.95 +47.63 | — |
| 2026-10-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.29 | ▲ 09:30 equity $10,618.34 vs yday $10,611.87 (+6.47) | 09:30 open · cash $2.29 (unchanged overnight, no fees) · equity $10,618.34 vs prior close $10,611.87 (+6.47) · 6 name(s) re-marked at the open (per-name table). MNKD×433 yday $3.95 → 09:30 $4.01 +25.98; MX×454 yday $3.64 → 09:30 $3.72 +36.32; PACB×733 yday $2.51 → 09:30 $2.50 -7.33; PMVP×1022 yday $1.75 → 09:30 $1.75 +0.00; QSI×1431 yday $1.43 → 09:30 $1.38 -71.55; QTRX×461 yday $3.41 → 09:30 $3.46 +23.05 | — |
| 2026-10-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.29 | ▲ close $10,905.70 vs 09:30 $10,618.34 (session +287.36) | 16:00 close · cash $2.29 · equity $10,905.70 vs 09:30 $10,618.34 (+287.36; session marks +287.36) · 6 name(s) marked open→close (per-name table). MNKD×433 09:30 $4.01 → close $3.80 -90.93; MX×454 09:30 $3.72 → close $3.68 -18.16; PACB×733 09:30 $2.50 → close $2.54 +29.32; PMVP×1022 09:30 $1.75 → close $1.79 +40.88; QSI×1431 09:30 $1.38 → close $1.55 +243.27; QTRX×461 09:30 $3.46 → close $3.64 +82.98 | — |
| 2026-10-05 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.29 | ▲ 09:30 equity $10,911.72 vs yday $10,905.70 (+6.01) | 09:30 open · cash $2.29 (unchanged overnight, no fees) · equity $10,911.72 vs prior close $10,905.70 (+6.01) · 6 name(s) re-marked at the open (per-name table). MNKD×433 yday $3.80 → 09:30 $3.82 +8.66; MX×454 yday $3.68 → 09:30 $3.70 +9.08; PACB×733 yday $2.54 → 09:30 $2.54 +0.00; PMVP×1022 yday $1.79 → 09:30 $1.80 +10.22; QSI×1431 yday $1.55 → 09:30 $1.52 -35.78; QTRX×461 yday $3.64 → 09:30 $3.67 +13.83 | — |
| 2026-10-05 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.29 | ▼ close $10,774.47 vs 09:30 $10,911.72 (session -137.24) | 16:00 close · cash $2.29 · equity $10,774.47 vs 09:30 $10,911.72 (-137.25; session marks -137.24) · 6 name(s) marked open→close (per-name table). MNKD×433 09:30 $3.82 → close $3.82 +0.00; MX×454 09:30 $3.70 → close $3.54 -72.64; PACB×733 09:30 $2.54 → close $2.87 +241.89; PMVP×1022 09:30 $1.80 → close $1.78 -20.44; QSI×1431 09:30 $1.52 → close $1.28 -350.59; QTRX×461 09:30 $3.67 → close $3.81 +64.54 | — |
| 2026-10-06 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.29 | ▲ 09:30 equity $10,884.84 vs yday $10,774.47 (+110.37) | 09:30 open · cash $2.29 (unchanged overnight, no fees) · equity $10,884.84 vs prior close $10,774.47 (+110.37) · 6 name(s) re-marked at the open (per-name table). MNKD×433 yday $3.82 → 09:30 $3.86 +17.32; MX×454 yday $3.54 → 09:30 $3.54 +0.00; PACB×733 yday $2.87 → 09:30 $2.99 +87.96; PMVP×1022 yday $1.78 → 09:30 $1.78 +0.00; QSI×1431 yday $1.28 → 09:30 $1.29 +14.31; QTRX×461 yday $3.81 → 09:30 $3.79 -9.22 | — |
| 2026-10-06 09:30 ET | **SELL** | `MNKD` | 433 | $3.86 | $5.67 | $-2.60 | $1,668.00 | ▼ -2.60 after sell → book $10,879.17; vs 09:30 mark -5.67 | dropped from list after 3 sess (min 3) | — |
| 2026-10-06 09:30 ET | **SELL** | `MX` | 454 | $3.54 | $5.94 | $-120.76 | $3,269.21 | ▼ -120.76 after sell → book $10,873.22; vs 09:30 mark -5.95 | dropped from list after 3 sess (min 3) | — |
| 2026-10-06 09:30 ET | **SELL** | `PACB` | 733 | $2.99 | $9.59 | $+457.40 | $5,451.29 | ▲ +457.40 after sell → book $10,863.63; vs 09:30 mark -9.59 | dropped from list after 3 sess (min 3) | join🔴 sector🟡 gen🟡 news🟡 digest🔴 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-06 09:30 ET | **SELL** | `PMVP` | 1022 | $1.78 | $13.37 | $+75.65 | $7,257.08 | ▲ +75.65 after sell → book $10,850.26; vs 09:30 mark -13.37 | dropped from list after 3 sess (min 3) | — |
| 2026-10-06 09:30 ET | **SELL** | `QSI` | 1431 | $1.29 | $18.71 | $+91.62 | $9,084.36 | ▲ +91.62 after sell → book $10,831.55; vs 09:30 mark -18.71 | dropped from list after 3 sess (min 3) | join🔴 sector🟡 gen🟡 news🟡 digest🔴 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-06 09:30 ET | **SELL** | `QTRX` | 461 | $3.79 | $6.04 | $+20.29 | $10,825.51 | ▲ +20.29 after sell → book $10,825.51; vs 09:30 mark -6.04 | dropped from list after 3 sess (min 3) | join🔴 sector🟡 gen🟡 news🟡 digest🔴 ab🔴 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `SDEV` | 388 | $3.48 | $5.01 | — | $9,470.27 | — | rank by ret_5; rank ret_5; list yday_mover; 🔵; ret5=+151.0; leftover $1353.19 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 judge🟡 ab🟡 heat🔴 vol🟢 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `QTEX` | 793 | $1.71 | $10.23 | — | $8,107.97 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+123.8; leftover $1353.19 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `DNA` | 89 | $15.08 | $2.26 | — | $6,763.59 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; ret5=+43.2; leftover $1353.19 | join🔴 sector🟡 gen🟡 news🟡 digest🔴 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `XP` | 46 | $29.20 | $2.13 | — | $5,418.27 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; ret5=+39.1; leftover $1353.19 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `PTC` | 7 | $193.00 | $2.01 | — | $4,065.26 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+37.6; leftover $1353.19 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `INTR` | 198 | $6.81 | $2.58 | — | $2,714.29 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+30.2; leftover $1353.19 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `BBD` | 299 | $4.52 | $3.86 | — | $1,358.95 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+29.4; leftover $1353.19 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-10-06 09:30 ET | **BUY** | `RXRX` | 281 | $4.80 | $3.62 | — | $6.53 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; ret5=+29.2; leftover $1353.19 | join🔴 sector🟡 gen🟡 news🟡 digest🔴 ab🔴 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-10-06 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6.53 | ▼ close $10,392.93 vs 09:30 $10,884.84 (session -400.88) | 16:00 close · cash $6.53 · equity $10,392.93 vs 09:30 $10,884.84 (-491.91; session marks -400.88) · 8 name(s) marked open→close (per-name table). SDEV×388 09:30 $3.48 → close $3.25 -89.24; QTEX×793 09:30 $1.71 → close $1.59 -91.19; DNA×89 09:30 $15.08 → close $12.41 -237.63; XP×46 09:30 $29.20 → close $29.73 +24.38; PTC×7 09:30 $193.00 → close $193.00 +0.00; INTR×198 09:30 $6.81 → close $7.03 +43.56; BBD×299 09:30 $4.52 → close $4.51 -2.99; RXRX×281 09:30 $4.80 → close $4.63 -47.77 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `TNDM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `INO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `IREN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `VOR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `SLS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TGTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `BTSG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `QMCO` | cash | leftover split 14.25 < 1 share @ 24.68 |
| 2026-08-14 | `ARX` | cash | leftover split 14.25 < 1 share @ 19.57 |
| 2026-08-14 | `BRUN` | cash | leftover split 14.25 < 1 share @ 26.25 |
| 2026-08-14 | `SNDK` | cash | leftover split 14.25 < 1 share @ 1646.93 |
| 2026-08-17 | `TNDM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `INO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `IREN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `VOR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `SLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TGTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `BTSG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `ZENA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `AIRO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `BCAR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `SIDU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `STDN` | cash | leftover split 8.03 < 1 share @ 13.64 |
| 2026-08-17 | `HTFL` | cash | leftover split 8.03 < 1 share @ 41.23 |
| 2026-08-17 | `UMAC` | cash | leftover split 8.03 < 1 share @ 32.55 |
| 2026-08-17 | `SMJF` | cash | leftover split 8.03 < 1 share @ 10.10 |
| 2026-08-18 | `ZENA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `AIRO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BCAR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `SIDU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `XHG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `KOPN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `NPWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `AVAH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AXTI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FIGR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CRDL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `QTRX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ALM` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `XHG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `CAPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `KOPN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `NPWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `ARX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WEAV` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `NMAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ANGX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `IVVD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DVLT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `AZI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BTGO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BNTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AUTL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ASST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BRR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `MRNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AZI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BTGO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BNTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ASST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BRR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CAPR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `IOVA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `MRVI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `INO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CAN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ALM` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HMY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `KGC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TMC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `GFI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SCCO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SBSW` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `CAPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `IOVA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `MRVI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `INO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CAN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `REAX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ALVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `FWDI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `DEFT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `DFDV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `REAX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `SUJA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ALVO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `FWDI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `DEFT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `DFDV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `USDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `PURR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `BTG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `MNRO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `BRR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `PEPG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `CAPR` | cash | leftover split 1.27 < 1 share @ 9.19 |
| 2026-08-27 | `BZ` | cash | leftover split 1.27 < 1 share @ 18.50 |
| 2026-08-27 | `VYX` | cash | leftover split 1.27 < 1 share @ 8.95 |
| 2026-08-27 | `CNDT` | cash | leftover split 1.27 < 1 share @ 1.67 |
| 2026-08-27 | `OABI` | cash | leftover split 1.27 < 1 share @ 4.81 |
| 2026-08-27 | `VERA` | cash | leftover split 1.27 < 1 share @ 36.70 |
| 2026-08-28 | `USDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `PURR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `BTG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `MNRO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `BRR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `PEPG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `AQST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `INDP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `INDP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `CAPR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ANF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `MEI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `LVWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `VYX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BHVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SNPS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ADCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `DFDV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `OKTA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RZLV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SPT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FWDI` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `CAPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ANF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `MEI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `LVWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `VYX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BHVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SNPS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ADCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `NEOV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DPRO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SKYX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CNXC` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NOW` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `METC` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `LENZ` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `XRX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `USDE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRVO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ZETA` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `SY` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `DFDV` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ASST` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `SUJA` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `MMED` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `SION` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CNH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `PBR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `PBR-A` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `TARS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `FRVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `FMC` | cash | leftover split 8.79 < 1 share @ 12.95 |
| 2026-09-04 | `FRNM` | cash | leftover split 8.79 < 1 share @ 16.40 |
| 2026-09-08 | `GPRO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `MMED` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `SION` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `PBR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `PBR-A` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `TARS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `FRVO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `BRR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `LENZ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `IRD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `SLBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `BAK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TWI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SECZ` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `AGCO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `USDE` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `BRR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `LENZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `IRD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `SLBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `BAK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `INDP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CRWV` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `RIOT` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `BTDR` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `INTC` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `AUR` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VSAT` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `ANGX` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `INDP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `COHU` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `MXL` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SKHY` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ARBE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `NAUT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `ASO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `WLTH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `APPS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `ODD` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HUT` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `VERI` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `SMR` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `INSP` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `BNC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `ASO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `CMRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `WLTH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `APPS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `SES` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `NTSK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `INSP` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `RPD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SION` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `S` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ZS` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `SWKS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `REF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `SDGR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `FTRE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `CRWD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `CAI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `RVTY` | cash | leftover split 30.60 < 1 share @ 147.61 |
| 2026-09-17 | `TEM` | cash | leftover split 30.60 < 1 share @ 72.70 |
| 2026-09-18 | `HLP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `SWKS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `REF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `FTRE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `CRWD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `CAI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `BBNX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `EMAT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `IOVA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `IQ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `TEM` | cash | leftover split 13.76 < 1 share @ 81.40 |
| 2026-09-18 | `VICR` | cash | leftover split 13.76 < 1 share @ 219.62 |
| 2026-09-18 | `RBRK` | cash | leftover split 13.76 < 1 share @ 108.55 |
| 2026-09-21 | `BBNX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `EMAT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `IOVA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `IQ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `RXT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `BBNX` | no_price | no 09:30 open — carry |
| 2026-09-22 | `EMAT` | no_price | no 09:30 open — carry |
| 2026-09-22 | `IQ` | no_price | no 09:30 open — carry |
| 2026-09-22 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `LVWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `RXT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `FEAM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `TJGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `USDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `GEMI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `ABTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `DFDV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `ARQQ` | no_price | no 09:30 open |
| 2026-09-22 | `CRML` | cash | leftover split 3.67 < 1 share @ 9.11 |
| 2026-09-22 | `GRAL` | no_price | no 09:30 open |
| 2026-09-22 | `NUAI` | cash | leftover split 3.67 < 1 share @ 7.23 |
| 2026-09-22 | `ARM` | cash | leftover split 3.67 < 1 share @ 319.41 |
| 2026-09-22 | `YSS` | no_price | no 09:30 open |
| 2026-09-22 | `AIBZ` | no_price | no 09:30 open |
| 2026-09-23 | `TJGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `USDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `SECZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `GEMI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `ABTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `DFDV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `VKTX` | cash | leftover split 20.26 < 1 share @ 41.76 |
| 2026-09-23 | `INOD` | cash | leftover split 20.26 < 1 share @ 70.84 |
| 2026-09-24 | `ORBS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `EVTL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `BFLY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `SVIA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `VICR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ZSQR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ACMR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `FSLY` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `SWRD` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `TJGC` | 101 | 2026-09-21 @ $16.91 | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; ret5=+50.5; leftover $1711.72 |
| `SECZ` | 146 | 2026-09-21 @ $11.67 | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; ret5=+31.3; leftover $1711.72 |
| `GLND` | 7 | 2026-09-23 @ $2.70 | rank by ret_5; rank ret_5; list yday_mover; 🔵; ret5=+109.2; leftover $20.26 |
| `ORBS` | 17 | 2026-09-23 @ $1.15 | rank by ret_5; rank ret_5; list yday_gainer; ret5=+36.2; leftover $20.26 |
| `EVTL` | 27 | 2026-09-23 @ $0.73 | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+27.5; leftover $20.26 |
| `BFLY` | 2 | 2026-09-23 @ $9.90 | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+27.3; leftover $20.26 |
| `SVIA` | 4 | 2026-09-23 @ $4.49 | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; ret5=+26.4; leftover $20.26 |
