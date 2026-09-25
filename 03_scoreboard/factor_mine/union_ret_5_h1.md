# Factor mine action — `union_ret_5_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `ret_5` · size `leftover` · sell `list` · S-boost `none` · rank by ret_5

Cash book **+11.44%** ($11,143) · signal-only (no cash/fees) was +36.33%. Starts YES **29/30**. Fills 254 · skips 100 · realized $+370.60.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass the list as written and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- Minimum hold is 1 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 1 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `ret_5` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $8,984.29.

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
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 53 | $22.92 | $2.17 | $-26.05 | $1,326.60 | ▼ -26.05 after sell → book $10,274.61; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 1543 | $0.93 | $19.25 | $+148.79 | $2,742.35 | ▲ +148.79 after sell → book $10,255.37; vs 09:30 mark -19.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 27 | $44.09 | $2.09 | $-55.19 | $3,930.69 | ▼ -55.19 after sell → book $10,253.28; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 24 | $55.29 | $2.08 | $+107.86 | $5,255.56 | ▲ +107.86 after sell → book $10,251.19; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `VOR` | 56 | $23.33 | $2.18 | $+69.58 | $6,559.87 | ▲ +69.58 after sell → book $10,249.02; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `SLS` | 106 | $12.40 | $2.34 | $+69.56 | $7,871.93 | ▲ +69.56 after sell → book $10,246.68; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TGTX` | 25 | $47.27 | $2.08 | $-64.90 | $9,051.59 | ▼ -64.90 after sell → book $10,244.59; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 20 | $59.65 | $2.07 | $-7.12 | $10,242.52 | ▼ -7.12 after sell → book $10,242.52; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `QMCO` | 51 | $24.68 | $2.14 | — | $8,981.70 | — | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ⚪; ret5=+111.3; leftover $1280.32 | — |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 65 | $19.57 | $2.19 | — | $7,707.47 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1280.32 | — |
| 2026-08-14 09:30 ET | **BUY** | `ZENA` | 581 | $2.20 | $7.49 | — | $6,421.77 | — | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ⚪; ret5=+54.3; leftover $1280.32 | — |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 115 | $11.12 | $2.33 | — | $5,140.64 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $1280.32 | — |
| 2026-08-14 09:30 ET | **BUY** | `BRUN` | 48 | $26.25 | $2.13 | — | $3,878.74 | — | rank by ret_5; rank ret_5; list earn_react; 🔵; ⚪; ret5=+31.2; leftover $1280.32 | — |
| 2026-08-14 09:30 ET | **BUY** | `BCAR` | 210 | $6.09 | $2.71 | — | $2,597.13 | — | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ⚪; ret5=+27.6; leftover $1280.32 | — |
| 2026-08-14 09:30 ET | **BUY** | `SIDU` | 502 | $2.55 | $6.48 | — | $1,310.56 | — | rank by ret_5; rank ret_5; list overnight; 🔵; ret5=+21.5; leftover $1280.32 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,310.56 | ▼ close $9,888.90 vs 09:30 $10,276.78 (session -328.15) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,310.56 | ▼ 09:30 equity $9,727.57 vs yday $9,888.90 (-161.33) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `QMCO` | 51 | $24.83 | $2.16 | $+3.34 | $2,574.72 | ▲ +3.34 after sell → book $9,725.41; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 65 | $19.57 | $2.21 | $-4.39 | $3,844.57 | ▼ -4.39 after sell → book $9,723.20; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ZENA` | 581 | $2.08 | $7.60 | $-81.91 | $5,048.35 | ▼ -81.91 after sell → book $9,715.60; vs 09:30 mark -7.60 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 115 | $9.57 | $2.36 | $-182.95 | $6,146.54 | ▼ -182.95 after sell → book $9,713.24; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BRUN` | 48 | $23.00 | $2.15 | $-160.05 | $7,248.38 | ▼ -160.05 after sell → book $9,711.08; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BCAR` | 210 | $5.99 | $2.75 | $-26.46 | $8,503.53 | ▼ -26.46 after sell → book $9,708.33; vs 09:30 mark -2.75 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SIDU` | 502 | $2.40 | $6.57 | $-88.34 | $9,701.76 | ▼ -88.34 after sell → book $9,701.76; vs 09:30 mark -6.57 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 289 | $4.19 | $3.73 | — | $8,487.12 | — | rank by ret_5; rank ret_5; list yday_mover; ⚪; ret5=+291.8; leftover $1212.72 | — |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 176 | $6.87 | $2.52 | — | $7,275.48 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; ret5=+62.6; leftover $1212.72 | — |
| 2026-08-17 09:30 ET | **BUY** | `STDN` | 88 | $13.64 | $2.25 | — | $6,072.91 | — | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ⚪; ret5=+57.7; leftover $1212.72 | — |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 29 | $41.23 | $2.08 | — | $4,875.16 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; ret5=+46.0; leftover $1212.72 | — |
| 2026-08-17 09:30 ET | **BUY** | `UMAC` | 37 | $32.55 | $2.10 | — | $3,668.71 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; ⚪; ret5=+30.4; leftover $1212.72 | — |
| 2026-08-17 09:30 ET | **BUY** | `KOPN` | 223 | $5.43 | $2.88 | — | $2,454.95 | — | rank by ret_5; rank ret_5; list yday_gainer; ⚪; ret5=+28.8; leftover $1212.72 | — |
| 2026-08-17 09:30 ET | **BUY** | `NPWR` | 631 | $1.92 | $8.14 | — | $1,235.29 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $1212.72 | — |
| 2026-08-17 09:30 ET | **BUY** | `SMJF` | 120 | $10.10 | $2.35 | — | $20.94 | — | rank by ret_5; rank ret_5; list mover_buy; ret5=+22.8; leftover $1212.72 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20.94 | ▼ close $9,497.21 vs 09:30 $9,727.57 (session -178.51) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20.94 | ▼ 09:30 equity $9,360.60 vs yday $9,497.21 (-136.61) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `XHG` | 289 | $3.94 | $3.79 | $-79.76 | $1,155.81 | ▼ -79.76 after sell → book $9,356.81; vs 09:30 mark -3.79 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `STDN` | 88 | $13.31 | $2.28 | $-33.57 | $2,324.81 | ▼ -33.57 after sell → book $9,354.53; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 29 | $41.50 | $2.10 | $+3.66 | $3,526.21 | ▲ +3.66 after sell → book $9,352.43; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `UMAC` | 37 | $28.59 | $2.12 | $-150.74 | $4,581.92 | ▼ -150.74 after sell → book $9,350.31; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `KOPN` | 223 | $5.03 | $2.92 | $-95.00 | $5,700.69 | ▼ -95.00 after sell → book $9,347.39; vs 09:30 mark -2.92 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `NPWR` | 631 | $1.70 | $8.25 | $-155.21 | $6,765.14 | ▼ -155.21 after sell → book $9,339.14; vs 09:30 mark -8.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `SMJF` | 120 | $10.45 | $2.38 | $+37.27 | $8,016.76 | ▲ +37.27 after sell → book $9,336.76; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,016.76 | ▼ close $9,262.84 vs 09:30 $9,360.60 (session -73.92) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,016.76 | ▲ 09:30 equity $9,282.20 vs yday $9,262.84 (+19.36) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `CAPR` | 176 | $7.19 | $2.56 | $+51.24 | $9,279.64 | ▲ +51.24 after sell → book $9,279.64; vs 09:30 mark -2.56 | dropped from list after 2 sess (min 1) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,279.64 | ▲ close $9,279.64 vs 09:30 $9,282.20 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,279.64 | ▲ 09:30 equity $9,279.64 vs yday $9,279.64 (-0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 7 | $150.14 | $2.01 | — | $8,226.65 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1159.95 | — |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 1008 | $1.15 | $13.00 | — | $7,054.44 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; leftover $1159.95 | — |
| 2026-08-20 09:30 ET | **BUY** | `AZI` | 846 | $1.37 | $10.91 | — | $5,884.51 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+30.7; leftover $1159.95 | — |
| 2026-08-20 09:30 ET | **BUY** | `BTGO` | 175 | $6.61 | $2.52 | — | $4,726.12 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+25.3; leftover $1159.95 | — |
| 2026-08-20 09:30 ET | **BUY** | `BNTX` | 10 | $109.06 | $2.02 | — | $3,633.50 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+22.0; leftover $1159.95 | — |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 469 | $2.47 | $6.05 | — | $2,469.02 | — | rank by ret_5; rank ret_5; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $1159.95 | — |
| 2026-08-20 09:30 ET | **BUY** | `ASST` | 72 | $16.00 | $2.21 | — | $1,314.81 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+18.9; leftover $1159.95 | — |
| 2026-08-20 09:30 ET | **BUY** | `BRR` | 557 | $2.08 | $7.19 | — | $149.07 | — | rank by ret_5; rank ret_5; list ohlc_hot; 🔵; ret5=+18.0; leftover $1159.95 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $149.07 | ▲ close $9,326.75 vs 09:30 $9,279.64 (session +93.01) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $149.07 | ▲ 09:30 equity $9,655.21 vs yday $9,326.75 (+328.46) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `AZI` | 846 | $1.46 | $11.06 | $+54.16 | $1,373.17 | ▲ +54.16 after sell → book $9,644.15; vs 09:30 mark -11.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTGO` | 175 | $6.95 | $2.55 | $+55.31 | $2,586.86 | ▲ +55.31 after sell → book $9,641.59; vs 09:30 mark -2.56 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BNTX` | 10 | $110.92 | $2.04 | $+14.54 | $3,694.02 | ▲ +14.54 after sell → book $9,639.55; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AUTL` | 469 | $2.47 | $6.14 | $-12.19 | $4,846.31 | ▼ -12.19 after sell → book $9,633.41; vs 09:30 mark -6.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ASST` | 72 | $17.66 | $2.23 | $+115.09 | $6,115.61 | ▲ +115.09 after sell → book $9,631.19; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BRR` | 557 | $2.25 | $7.29 | $+80.22 | $7,361.57 | ▲ +80.22 after sell → book $9,623.90; vs 09:30 mark -7.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `CAPR` | 180 | $6.81 | $2.53 | — | $6,133.24 | — | rank by ret_5; rank ret_5; list yday_mover; 🔵; ret5=+62.5; leftover $1226.93 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 110 | $11.13 | $2.32 | — | $4,906.62 | — | rank by ret_5; rank ret_5; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1226.93 | — |
| 2026-08-21 09:30 ET | **BUY** | `IOVA` | 135 | $9.08 | $2.40 | — | $3,678.42 | — | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ⚪; ret5=+37.9; leftover $1226.93 | — |
| 2026-08-21 09:30 ET | **BUY** | `MRVI` | 148 | $8.28 | $2.43 | — | $2,450.55 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.6; leftover $1226.93 | — |
| 2026-08-21 09:30 ET | **BUY** | `INO` | 997 | $1.23 | $12.86 | — | $1,211.38 | — | rank by ret_5; rank ret_5; list mover_buy; 🔵; ⚪; ret5=+34.4; leftover $1226.93 | — |
| 2026-08-21 09:30 ET | **BUY** | `CAN` | 4036 | $0.29 | $23.97 | — | $0.82 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+30.4; leftover $1226.93 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.82 | ▲ close $10,066.90 vs 09:30 $9,655.21 (session +489.52) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.82 | ▲ 09:30 equity $10,850.40 vs yday $10,066.90 (+783.50) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `MRNA` | 7 | $142.70 | $2.03 | $-56.12 | $997.69 | ▼ -56.12 after sell → book $10,848.37; vs 09:30 mark -2.03 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 1008 | $1.83 | $13.19 | $+659.25 | $2,829.14 | ▲ +659.25 after sell → book $10,835.18; vs 09:30 mark -13.19 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CAPR` | 180 | $8.03 | $2.57 | $+214.50 | $4,271.97 | ▲ +214.50 after sell → book $10,832.61; vs 09:30 mark -2.57 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 110 | $13.33 | $2.35 | $+237.33 | $5,735.92 | ▲ +237.33 after sell → book $10,830.26; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `IOVA` | 135 | $8.08 | $2.43 | $-139.82 | $6,824.29 | ▼ -139.82 after sell → book $10,827.83; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MRVI` | 148 | $8.59 | $2.47 | $+40.98 | $8,093.15 | ▲ +40.98 after sell → book $10,825.36; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `INO` | 997 | $1.19 | $13.04 | $-65.78 | $9,266.54 | ▼ -65.78 after sell → book $10,812.33; vs 09:30 mark -13.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CAN` | 4036 | $0.38 | $28.25 | $+306.98 | $10,784.08 | ▲ +306.98 after sell → book $10,784.08; vs 09:30 mark -28.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,784.08 | ▲ close $10,784.08 vs 09:30 $10,850.40 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,784.08 | ▲ 09:30 equity $10,784.08 vs yday $10,784.08 (-0.00) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 55 | $24.11 | $2.15 | — | $9,455.87 | — | rank by ret_5; rank ret_5; list yday_mover; ret5=+891.7; leftover $1348.01 | — |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 864 | $1.56 | $11.15 | — | $8,096.89 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; leftover $1348.01 | — |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 70 | $19.04 | $2.20 | — | $6,761.89 | — | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ret5=+49.5; leftover $1348.01 | — |
| 2026-08-25 09:30 ET | **BUY** | `SUJA` | 153 | $8.79 | $2.45 | — | $5,414.57 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.1; leftover $1348.01 | — |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 257 | $5.24 | $3.32 | — | $4,064.57 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+35.3; leftover $1348.01 | — |
| 2026-08-25 09:30 ET | **BUY** | `FWDI` | 236 | $5.71 | $3.04 | — | $2,713.97 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+33.9; leftover $1348.01 | — |
| 2026-08-25 09:30 ET | **BUY** | `DEFT` | 2174 | $0.62 | $20.00 | — | $1,346.09 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+33.3; leftover $1348.01 | — |
| 2026-08-25 09:30 ET | **BUY** | `DFDV` | 330 | $4.06 | $4.26 | — | $2.03 | — | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ret5=+29.4; leftover $1348.01 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.03 | ▲ close $11,457.58 vs 09:30 $10,784.08 (session +722.07) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.03 | ▼ 09:30 equity $11,159.38 vs yday $11,457.58 (-298.20) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `REAX` | 55 | $26.61 | $2.18 | $+133.17 | $1,463.40 | ▲ +133.17 after sell → book $11,157.21; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 864 | $1.60 | $11.30 | $+12.11 | $2,834.50 | ▲ +12.11 after sell → book $11,145.91; vs 09:30 mark -11.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ASST` | 70 | $20.72 | $2.22 | $+113.18 | $4,282.68 | ▲ +113.18 after sell → book $11,143.68; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ALVO` | 257 | $4.98 | $3.37 | $-73.50 | $5,559.17 | ▼ -73.50 after sell → book $11,140.32; vs 09:30 mark -3.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FWDI` | 236 | $5.97 | $3.09 | $+55.22 | $6,965.00 | ▲ +55.22 after sell → book $11,137.22; vs 09:30 mark -3.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `DEFT` | 2174 | $0.60 | $19.89 | $-87.72 | $8,245.16 | ▼ -87.72 after sell → book $11,117.33; vs 09:30 mark -19.89 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `DFDV` | 330 | $4.35 | $4.32 | $+87.12 | $9,676.33 | ▲ +87.12 after sell → book $11,113.00; vs 09:30 mark -4.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `USDE` | 237 | $5.81 | $3.06 | — | $8,296.31 | — | rank by ret_5; rank ret_5; list yday_mover; 🔵; ⚪; ret5=+117.2; leftover $1382.33 | — |
| 2026-08-26 09:30 ET | **BUY** | `PURR` | 119 | $11.59 | $2.35 | — | $6,915.34 | — | rank by ret_5; rank ret_5; list overnight; 🔵; ret5=+64.9; leftover $1382.33 | — |
| 2026-08-26 09:30 ET | **BUY** | `BTG` | 240 | $5.75 | $3.10 | — | $5,532.25 | — | rank by ret_5; rank ret_5; list ohlc_hot; 🔵; ret5=+17.9; leftover $1382.33 | — |
| 2026-08-26 09:30 ET | **BUY** | `MNRO` | 98 | $14.00 | $2.28 | — | $4,157.96 | — | rank by ret_5; rank ret_5; list ohlc_hot; ret5=+17.8; leftover $1382.33 | — |
| 2026-08-26 09:30 ET | **BUY** | `BRR` | 628 | $2.20 | $8.10 | — | $2,768.26 | — | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ret5=+17.8; leftover $1382.33 | — |
| 2026-08-26 09:30 ET | **BUY** | `PEPG` | 405 | $3.41 | $5.22 | — | $1,381.99 | — | rank by ret_5; rank ret_5; list ohlc_hot; 🔵; ret5=+17.7; leftover $1382.33 | — |
| 2026-08-26 09:30 ET | **BUY** | `AQST` | 271 | $5.08 | $3.50 | — | $1.81 | — | rank by ret_5; rank ret_5; list ohlc_hot; 🔵; ret5=+17.6; leftover $1382.33 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.81 | ▼ close $10,979.96 vs 09:30 $11,159.38 (session -105.44) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.81 | ▲ 09:30 equity $11,177.65 vs yday $10,979.96 (+197.69) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `SUJA` | 153 | $9.41 | $2.49 | $+89.93 | $1,439.06 | ▲ +89.93 after sell → book $11,175.17; vs 09:30 mark -2.48 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `USDE` | 237 | $6.50 | $3.11 | $+157.36 | $2,976.45 | ▲ +157.36 after sell → book $11,172.06; vs 09:30 mark -3.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `PURR` | 119 | $12.18 | $2.38 | $+66.08 | $4,423.49 | ▲ +66.08 after sell → book $11,169.68; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BTG` | 240 | $5.73 | $3.15 | $-11.04 | $5,795.54 | ▼ -11.04 after sell → book $11,166.53; vs 09:30 mark -3.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `MNRO` | 98 | $12.56 | $2.31 | $-145.71 | $7,024.11 | ▼ -145.71 after sell → book $11,164.22; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BRR` | 628 | $2.19 | $8.22 | $-22.60 | $8,391.22 | ▼ -22.60 after sell → book $11,156.01; vs 09:30 mark -8.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `PEPG` | 405 | $3.22 | $5.30 | $-87.48 | $9,690.01 | ▼ -87.48 after sell → book $11,150.70; vs 09:30 mark -5.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `INDP` | 1225 | $1.13 | $15.80 | — | $8,289.96 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; ret5=+21.3; leftover $1384.29 | — |
| 2026-08-27 09:30 ET | **BUY** | `CAPR` | 150 | $9.19 | $2.44 | — | $6,909.02 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover,ohlc_hot; ret5=+17.3; leftover $1384.29 | — |
| 2026-08-27 09:30 ET | **BUY** | `BZ` | 74 | $18.50 | $2.21 | — | $5,537.81 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; ret5=+17.2; leftover $1384.29 | — |
| 2026-08-27 09:30 ET | **BUY** | `VYX` | 154 | $8.95 | $2.45 | — | $4,157.06 | — | rank by ret_5; rank ret_5; list ohlc_hot; 🔵; ret5=+16.2; leftover $1384.29 | — |
| 2026-08-27 09:30 ET | **BUY** | `CNDT` | 828 | $1.67 | $10.68 | — | $2,763.62 | — | rank by ret_5; rank ret_5; list ohlc_hot; 🔵; ret5=+15.1; leftover $1384.29 | — |
| 2026-08-27 09:30 ET | **BUY** | `OABI` | 287 | $4.81 | $3.70 | — | $1,379.44 | — | rank by ret_5; rank ret_5; list ohlc_hot; ret5=+14.8; leftover $1384.29 | — |
| 2026-08-27 09:30 ET | **BUY** | `VERA` | 37 | $36.70 | $2.10 | — | $19.44 | — | rank by ret_5; rank ret_5; list ohlc_hot; ret5=+14.1; leftover $1384.29 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19.44 | ▲ close $11,137.51 vs 09:30 $11,177.65 (session +26.20) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19.44 | ▼ 09:30 equity $11,058.65 vs yday $11,137.51 (-78.86) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `AQST` | 271 | $5.11 | $3.55 | $+1.08 | $1,400.70 | ▲ +1.08 after sell → book $11,055.10; vs 09:30 mark -3.55 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `INDP` | 1225 | $1.16 | $16.02 | $+4.93 | $2,805.68 | ▲ +4.93 after sell → book $11,039.08; vs 09:30 mark -16.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `BZ` | 74 | $18.15 | $2.23 | $-30.35 | $4,146.55 | ▼ -30.35 after sell → book $11,036.85; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `CNDT` | 828 | $1.75 | $10.83 | $+44.73 | $5,584.72 | ▲ +44.73 after sell → book $11,026.02; vs 09:30 mark -10.83 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `OABI` | 287 | $4.54 | $3.76 | $-84.95 | $6,883.94 | ▼ -84.95 after sell → book $11,022.26; vs 09:30 mark -3.76 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `VERA` | 37 | $34.40 | $2.12 | $-89.32 | $8,154.62 | ▼ -89.32 after sell → book $11,020.14; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 9 | $146.07 | $2.02 | — | $6,837.97 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+38.8; leftover $1359.10 | — |
| 2026-08-28 09:30 ET | **BUY** | `MEI` | 76 | $17.78 | $2.22 | — | $5,484.47 | — | rank by ret_5; rank ret_5; list mover_buy; 🔵; ⚪; ret5=+22.9; leftover $1359.10 | — |
| 2026-08-28 09:30 ET | **BUY** | `LVWR` | 977 | $1.39 | $12.60 | — | $4,113.84 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; ret5=+20.4; leftover $1359.10 | — |
| 2026-08-28 09:30 ET | **BUY** | `BHVN` | 85 | $15.88 | $2.25 | — | $2,761.79 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; ret5=+19.4; leftover $1359.10 | — |
| 2026-08-28 09:30 ET | **BUY** | `SNPS` | 2 | $461.85 | $2.00 | — | $1,836.10 | — | rank by ret_5; rank ret_5; list ohlc_hot; ret5=+16.8; leftover $1359.10 | — |
| 2026-08-28 09:30 ET | **BUY** | `ADCT` | 1224 | $1.11 | $15.79 | — | $461.67 | — | rank by ret_5; rank ret_5; list ohlc_hot; ret5=+16.7; leftover $1359.10 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $461.67 | ▼ close $10,881.41 vs 09:30 $11,058.65 (session -101.86) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $461.67 | ▼ 09:30 equity $10,775.20 vs yday $10,881.41 (-106.21) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 150 | $9.50 | $2.48 | $+41.58 | $1,884.19 | ▲ +41.58 after sell → book $10,772.72; vs 09:30 mark -2.48 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `VYX` | 154 | $8.66 | $2.49 | $-49.60 | $3,215.34 | ▼ -49.60 after sell → book $10,770.23; vs 09:30 mark -2.49 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 9 | $148.03 | $2.04 | $+13.59 | $4,545.58 | ▲ +13.59 after sell → book $10,768.20; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MEI` | 76 | $18.15 | $2.24 | $+23.66 | $5,922.73 | ▲ +23.66 after sell → book $10,765.95; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `LVWR` | 977 | $1.30 | $12.78 | $-113.31 | $7,180.06 | ▼ -113.31 after sell → book $10,753.18; vs 09:30 mark -12.77 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BHVN` | 85 | $15.46 | $2.27 | $-40.21 | $8,491.89 | ▼ -40.21 after sell → book $10,750.91; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SNPS` | 2 | $437.95 | $2.02 | $-51.81 | $9,365.77 | ▼ -51.81 after sell → book $10,748.89; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ADCT` | 1224 | $1.13 | $16.00 | $-7.31 | $10,732.89 | ▼ -7.31 after sell → book $10,732.89; vs 09:30 mark -16.00 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,732.89 | ▲ close $10,732.89 vs 09:30 $10,775.20 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,732.89 | ▲ 09:30 equity $10,732.89 vs yday $10,732.89 (-0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,732.89 | ▲ close $10,732.89 vs 09:30 $10,732.89 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,732.89 | ▲ 09:30 equity $10,732.89 vs yday $10,732.89 (-0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,732.89 | ▲ close $10,732.89 vs 09:30 $10,732.89 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,732.89 | ▲ 09:30 equity $10,732.89 vs yday $10,732.89 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 753 | $1.78 | $9.71 | — | $9,382.83 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; ret5=+183.1; leftover $1341.61 | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 56 | $23.88 | $2.16 | — | $8,043.40 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1341.61 | — |
| 2026-09-03 09:30 ET | **BUY** | `SION` | 183 | $7.31 | $2.54 | — | $6,703.13 | — | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ret5=+18.5; leftover $1341.61 | — |
| 2026-09-03 09:30 ET | **BUY** | `CNH` | 97 | $13.71 | $2.28 | — | $5,370.98 | — | rank by ret_5; rank ret_5; list ohlc_hot; 🔵; ret5=+17.5; leftover $1341.61 | — |
| 2026-09-03 09:30 ET | **BUY** | `PBR` | 63 | $21.18 | $2.18 | — | $4,034.46 | — | rank by ret_5; rank ret_5; list ohlc_hot; ret5=+17.5; leftover $1341.61 | — |
| 2026-09-03 09:30 ET | **BUY** | `PBR-A` | 70 | $19.16 | $2.20 | — | $2,691.06 | — | rank by ret_5; rank ret_5; list ohlc_hot; ret5=+17.4; leftover $1341.61 | — |
| 2026-09-03 09:30 ET | **BUY** | `TARS` | 16 | $82.76 | $2.04 | — | $1,364.86 | — | rank by ret_5; rank ret_5; list ohlc_hot; ret5=+17.1; leftover $1341.61 | — |
| 2026-09-03 09:30 ET | **BUY** | `FRVO` | 73 | $18.28 | $2.21 | — | $28.21 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; ret5=+16.5; leftover $1341.61 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $28.21 | ▼ close $10,164.34 vs 09:30 $10,732.89 (session -543.23) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $28.21 | ▲ 09:30 equity $10,192.32 vs yday $10,164.34 (+27.98) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 56 | $23.84 | $2.18 | $-6.58 | $1,361.07 | ▼ -6.58 after sell → book $10,190.14; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `SION` | 183 | $6.68 | $2.58 | $-120.41 | $2,580.93 | ▼ -120.41 after sell → book $10,187.56; vs 09:30 mark -2.58 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNH` | 97 | $13.89 | $2.31 | $+12.87 | $3,925.95 | ▲ +12.87 after sell → book $10,185.25; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `PBR` | 63 | $20.25 | $2.20 | $-62.97 | $5,199.51 | ▼ -62.97 after sell → book $10,183.06; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `PBR-A` | 70 | $18.36 | $2.22 | $-60.42 | $6,482.48 | ▼ -60.42 after sell → book $10,180.83; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `TARS` | 16 | $82.70 | $2.06 | $-5.06 | $7,803.62 | ▼ -5.06 after sell → book $10,178.77; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `FRVO` | 73 | $17.27 | $2.23 | $-78.17 | $9,062.10 | ▼ -78.17 after sell → book $10,176.54; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `FMC` | 99 | $12.95 | $2.29 | — | $7,777.77 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; ret5=+21.8; leftover $1294.59 | — |
| 2026-09-04 09:30 ET | **BUY** | `BRR` | 515 | $2.51 | $6.64 | — | $6,478.47 | — | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ⚪; ret5=+21.8; leftover $1294.59 | — |
| 2026-09-04 09:30 ET | **BUY** | `FRNM` | 78 | $16.40 | $2.22 | — | $5,197.05 | — | rank by ret_5; rank ret_5; list mover_buy; 🔵; ⚪; ret5=+21.2; leftover $1294.59 | — |
| 2026-09-04 09:30 ET | **BUY** | `LENZ` | 225 | $5.75 | $2.90 | — | $3,900.40 | — | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ⚪; ret5=+21.2; leftover $1294.59 | — |
| 2026-09-04 09:30 ET | **BUY** | `IRD` | 285 | $4.53 | $3.68 | — | $2,605.67 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.5; leftover $1294.59 | — |
| 2026-09-04 09:30 ET | **BUY** | `SLBT` | 410 | $3.15 | $5.29 | — | $1,308.88 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+19.3; leftover $1294.59 | — |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 667 | $1.94 | $8.60 | — | $6.30 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; ret5=+18.3; leftover $1294.59 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6.30 | ▲ close $10,325.89 vs 09:30 $10,192.32 (session +180.97) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6.30 | ▼ 09:30 equity $10,262.83 vs yday $10,325.89 (-63.06) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `GPRO` | 753 | $1.56 | $9.85 | $-181.46 | $1,174.89 | ▼ -181.46 after sell → book $10,252.98; vs 09:30 mark -9.85 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `FMC` | 99 | $13.11 | $2.31 | $+11.24 | $2,470.47 | ▲ +11.24 after sell → book $10,250.67; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BRR` | 515 | $2.66 | $6.74 | $+63.87 | $3,833.63 | ▲ +63.87 after sell → book $10,243.93; vs 09:30 mark -6.74 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 78 | $16.74 | $2.25 | $+22.05 | $5,137.10 | ▲ +22.05 after sell → book $10,241.68; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `LENZ` | 225 | $5.95 | $2.95 | $+39.15 | $6,472.90 | ▲ +39.15 after sell → book $10,238.73; vs 09:30 mark -2.95 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `IRD` | 285 | $4.53 | $3.73 | $-7.41 | $7,760.22 | ▼ -7.41 after sell → book $10,235.00; vs 09:30 mark -3.73 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `SLBT` | 410 | $2.88 | $5.37 | $-121.36 | $8,935.65 | ▼ -121.36 after sell → book $10,229.63; vs 09:30 mark -5.37 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BAK` | 667 | $1.94 | $8.73 | $-17.33 | $10,220.90 | ▼ -17.33 after sell → book $10,220.90; vs 09:30 mark -8.73 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,220.90 | ▲ close $10,220.90 vs 09:30 $10,262.83 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,220.90 | ▲ 09:30 equity $10,220.90 vs yday $10,220.90 (+0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,220.90 | ▲ close $10,220.90 vs 09:30 $10,220.90 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,220.90 | ▲ 09:30 equity $10,220.90 vs yday $10,220.90 (+0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,220.90 | ▲ close $10,220.90 vs 09:30 $10,220.90 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,220.90 | ▲ 09:30 equity $10,220.90 vs yday $10,220.90 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `INDP` | 473 | $2.70 | $6.10 | — | $8,937.70 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+118.8; leftover $1277.61 | — |
| 2026-09-11 09:30 ET | **BUY** | `BNC` | 260 | $4.91 | $3.35 | — | $7,657.75 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+76.3; leftover $1277.61 | — |
| 2026-09-11 09:30 ET | **BUY** | `IRD` | 207 | $6.16 | $2.67 | — | $6,379.96 | — | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ret5=+36.4; leftover $1277.61 | — |
| 2026-09-11 09:30 ET | **BUY** | `CYPH` | 534 | $2.39 | $6.89 | — | $5,096.81 | — | rank by ret_5; rank ret_5; list yday_mover; 🔵; ret5=+31.0; leftover $1277.61 | — |
| 2026-09-11 09:30 ET | **BUY** | `ASO` | 23 | $54.91 | $2.06 | — | $3,831.82 | — | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ret5=+24.3; leftover $1277.61 | — |
| 2026-09-11 09:30 ET | **BUY** | `CMRC` | 408 | $3.13 | $5.26 | — | $2,549.52 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; ret5=+24.2; leftover $1277.61 | — |
| 2026-09-11 09:30 ET | **BUY** | `WLTH` | 116 | $10.95 | $2.34 | — | $1,276.98 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+20.8; leftover $1277.61 | — |
| 2026-09-11 09:30 ET | **BUY** | `APPS` | 107 | $11.88 | $2.31 | — | $3.51 | — | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ret5=+20.7; leftover $1277.61 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3.51 | ▲ close $10,192.58 vs 09:30 $10,220.90 (session +2.66) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.51 | ▲ 09:30 equity $10,230.91 vs yday $10,192.58 (+38.33) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `IRD` | 207 | $6.02 | $2.71 | $-34.37 | $1,246.93 | ▼ -34.37 after sell → book $10,228.19; vs 09:30 mark -2.72 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `CYPH` | 534 | $2.26 | $6.99 | $-83.30 | $2,446.79 | ▼ -83.30 after sell → book $10,221.21; vs 09:30 mark -6.98 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ASO` | 23 | $54.75 | $2.08 | $-7.82 | $3,703.96 | ▼ -7.82 after sell → book $10,219.13; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `WLTH` | 116 | $10.29 | $2.37 | $-81.27 | $4,895.23 | ▼ -81.27 after sell → book $10,216.76; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `APPS` | 107 | $11.75 | $2.34 | $-18.56 | $6,150.14 | ▼ -18.56 after sell → book $10,214.42; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,150.14 | ▲ close $10,490.68 vs 09:30 $10,230.91 (session +276.26) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,150.14 | ▲ 09:30 equity $10,572.06 vs yday $10,490.68 (+81.38) | — | — |
| 2026-09-15 09:30 ET | **SELL** | `BNC` | 260 | $5.11 | $3.41 | $+45.24 | $7,475.33 | ▲ +45.24 after sell → book $10,568.65; vs 09:30 mark -3.41 | dropped from list after 2 sess (min 1) | — |
| 2026-09-15 09:30 ET | **SELL** | `CMRC` | 408 | $3.64 | $5.34 | $+197.47 | $8,955.11 | ▲ +197.47 after sell → book $10,563.31; vs 09:30 mark -5.34 | dropped from list after 2 sess (min 1) | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,955.11 | ▲ close $10,676.83 vs 09:30 $10,572.06 (session +113.52) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,955.11 | ▲ 09:30 equity $10,686.29 vs yday $10,676.83 (+9.46) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `HLP` | 710 | $1.80 | $9.16 | — | $7,667.95 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+30.5; leftover $1279.30 | — |
| 2026-09-16 09:30 ET | **BUY** | `SWKS` | 14 | $89.38 | $2.03 | — | $6,414.60 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.4; leftover $1279.30 | — |
| 2026-09-16 09:30 ET | **BUY** | `REF` | 81 | $15.75 | $2.23 | — | $5,136.62 | — | rank by ret_5; rank ret_5; list yday_gainer,ohlc_hot; ret5=+17.3; leftover $1279.30 | — |
| 2026-09-16 09:30 ET | **BUY** | `SDGR` | 54 | $23.29 | $2.15 | — | $3,876.81 | — | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ret5=+16.1; leftover $1279.30 | — |
| 2026-09-16 09:30 ET | **BUY** | `FTRE` | 64 | $19.75 | $2.18 | — | $2,610.62 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+15.7; leftover $1279.30 | — |
| 2026-09-16 09:30 ET | **BUY** | `CRWD` | 5 | $236.92 | $2.00 | — | $1,424.02 | — | rank by ret_5; rank ret_5; list ohlc_hot; ret5=+15.5; leftover $1279.30 | — |
| 2026-09-16 09:30 ET | **BUY** | `CAI` | 45 | $28.16 | $2.12 | — | $154.69 | — | rank by ret_5; rank ret_5; list yday_gainer,ohlc_hot; ret5=+14.8; leftover $1279.30 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $154.69 | ▼ close $10,661.66 vs 09:30 $10,686.29 (session -2.74) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $154.69 | ▲ 09:30 equity $10,772.76 vs yday $10,661.66 (+111.10) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `SWKS` | 14 | $86.76 | $2.05 | $-40.76 | $1,367.28 | ▼ -40.76 after sell → book $10,770.71; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `REF` | 81 | $15.85 | $2.26 | $+3.61 | $2,648.87 | ▲ +3.61 after sell → book $10,768.45; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SDGR` | 54 | $24.09 | $2.17 | $+38.88 | $3,947.56 | ▲ +38.88 after sell → book $10,766.28; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `FTRE` | 64 | $20.31 | $2.20 | $+31.45 | $5,245.20 | ▲ +31.45 after sell → book $10,764.07; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `CRWD` | 5 | $236.04 | $2.02 | $-8.43 | $6,423.37 | ▼ -8.43 after sell → book $10,762.05; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `CAI` | 45 | $28.59 | $2.15 | $+15.30 | $7,708.00 | ▲ +15.30 after sell → book $10,759.90; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `BBNX` | 57 | $22.46 | $2.16 | — | $6,425.62 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+27.3; leftover $1284.67 | — |
| 2026-09-17 09:30 ET | **BUY** | `EMAT` | 332 | $3.86 | $4.28 | — | $5,139.82 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+18.7; leftover $1284.67 | — |
| 2026-09-17 09:30 ET | **BUY** | `RVTY` | 8 | $147.61 | $2.01 | — | $3,956.93 | — | rank by ret_5; rank ret_5; list flatten,ohlc_hot; ret5=+17.7; leftover $1284.67 | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 125 | $10.25 | $2.37 | — | $2,673.31 | — | rank by ret_5; rank ret_5; list flatten,ohlc_hot; 🔵; ret5=+17.1; leftover $1284.67 | — |
| 2026-09-17 09:30 ET | **BUY** | `IQ` | 1200 | $1.07 | $15.48 | — | $1,373.83 | — | rank by ret_5; rank ret_5; list yday_gainer,ohlc_hot; 🔵; ret5=+15.8; leftover $1284.67 | — |
| 2026-09-17 09:30 ET | **BUY** | `TEM` | 17 | $72.70 | $2.04 | — | $135.89 | — | rank by ret_5; rank ret_5; list ohlc_hot; ret5=+14.2; leftover $1284.67 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $135.89 | ▲ close $11,097.59 vs 09:30 $10,772.76 (session +366.03) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $135.89 | ▼ 09:30 equity $11,045.48 vs yday $11,097.59 (-52.11) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `HLP` | 710 | $1.96 | $9.29 | $+95.15 | $1,518.20 | ▲ +95.15 after sell → book $11,036.19; vs 09:30 mark -9.29 | dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BBNX` | 57 | $21.30 | $2.18 | $-70.46 | $2,730.12 | ▼ -70.46 after sell → book $11,034.01; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `EMAT` | 332 | $3.97 | $4.35 | $+27.89 | $4,043.81 | ▲ +27.89 after sell → book $11,029.66; vs 09:30 mark -4.35 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `RVTY` | 8 | $146.50 | $2.03 | $-12.93 | $5,213.78 | ▼ -12.93 after sell → book $11,027.63; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `IOVA` | 125 | $10.12 | $2.40 | $-21.01 | $6,476.38 | ▼ -21.01 after sell → book $11,025.23; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `IQ` | 1200 | $1.12 | $15.69 | $+28.83 | $7,804.69 | ▲ +28.83 after sell → book $11,009.54; vs 09:30 mark -15.69 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `SDGR` | 44 | $29.32 | $2.12 | — | $6,512.49 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+60.9; leftover $1300.78 | — |
| 2026-09-18 09:30 ET | **BUY** | `CYPH` | 428 | $3.04 | $5.52 | — | $5,207.99 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+39.5; leftover $1300.78 | — |
| 2026-09-18 09:30 ET | **BUY** | `LVWR` | 873 | $1.49 | $11.26 | — | $3,895.96 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+25.7; leftover $1300.78 | — |
| 2026-09-18 09:30 ET | **BUY** | `RXT` | 330 | $3.94 | $4.26 | — | $2,591.50 | — | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ⚪; ret5=+25.2; leftover $1300.78 | — |
| 2026-09-18 09:30 ET | **BUY** | `VICR` | 5 | $219.62 | $2.00 | — | $1,491.40 | — | rank by ret_5; rank ret_5; list flatten,yday_gainer,yday_mover; 🔵; ret5=+21.5; leftover $1300.78 | — |
| 2026-09-18 09:30 ET | **BUY** | `RBRK` | 11 | $108.55 | $2.02 | — | $295.32 | — | rank by ret_5; rank ret_5; list flatten; ⚪; ret5=+21.3; leftover $1300.78 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $295.32 | ▲ close $11,079.83 vs 09:30 $11,045.48 (session +97.48) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $295.32 | ▲ 09:30 equity $11,387.72 vs yday $11,079.83 (+307.89) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `INDP` | 473 | $3.55 | $6.19 | $+389.75 | $1,968.28 | ▲ +389.75 after sell → book $11,381.53; vs 09:30 mark -6.19 | dropped from list after 6 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TEM` | 17 | $79.08 | $2.06 | $+104.36 | $3,310.58 | ▲ +104.36 after sell → book $11,379.47; vs 09:30 mark -2.06 | dropped from list after 2 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `SDGR` | 44 | $29.43 | $2.14 | $+0.58 | $4,603.35 | ▲ +0.58 after sell → book $11,377.32; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `CYPH` | 428 | $4.00 | $5.61 | $+401.89 | $6,309.75 | ▲ +401.89 after sell → book $11,371.72; vs 09:30 mark -5.60 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RXT` | 330 | $3.90 | $4.32 | $-21.78 | $7,592.43 | ▼ -21.78 after sell → book $11,367.40; vs 09:30 mark -4.32 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `VICR` | 5 | $230.25 | $2.02 | $+49.12 | $8,741.65 | ▲ +49.12 after sell → book $11,365.37; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RBRK` | 11 | $107.57 | $2.04 | $-14.85 | $9,922.88 | ▼ -14.85 after sell → book $11,363.33; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `FEAM` | 573 | $2.47 | $7.39 | — | $8,500.18 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; ret5=+73.6; leftover $1417.55 | — |
| 2026-09-21 09:30 ET | **BUY** | `TJGC` | 83 | $16.91 | $2.24 | — | $7,094.41 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; ret5=+50.5; leftover $1417.55 | — |
| 2026-09-21 09:30 ET | **BUY** | `USDE` | 108 | $13.05 | $2.31 | — | $5,682.69 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+40.4; leftover $1417.55 | — |
| 2026-09-21 09:30 ET | **BUY** | `SECZ` | 121 | $11.67 | $2.35 | — | $4,268.27 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; ret5=+31.3; leftover $1417.55 | — |
| 2026-09-21 09:30 ET | **BUY** | `GEMI` | 246 | $5.75 | $3.17 | — | $2,849.37 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+30.3; leftover $1417.55 | — |
| 2026-09-21 09:30 ET | **BUY** | `ABTC` | 132 | $10.71 | $2.39 | — | $1,433.26 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+21.4; leftover $1417.55 | — |
| 2026-09-21 09:30 ET | **BUY** | `DFDV` | 217 | $6.51 | $2.80 | — | $17.79 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+19.3; leftover $1417.55 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17.79 | ▲ close $11,473.77 vs 09:30 $11,387.72 (session +133.10) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17.79 | ▼ 09:30 equity $11,466.54 vs yday $11,473.77 (-7.23) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `USDE` | 108 | $12.99 | $2.34 | $-11.14 | $1,418.37 | ▼ -11.14 after sell → book $11,464.20; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `GEMI` | 246 | $6.05 | $3.23 | $+67.40 | $2,904.67 | ▲ +67.40 after sell → book $11,460.97; vs 09:30 mark -3.23 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **BUY** | `CRML` | 45 | $9.11 | $2.12 | — | $2,492.60 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; ret5=+44.4; leftover $414.95 | — |
| 2026-09-22 09:30 ET | **BUY** | `NUAI` | 57 | $7.23 | $2.16 | — | $2,078.33 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; ret5=+36.6; leftover $414.95 | — |
| 2026-09-22 09:30 ET | **BUY** | `ARM` | 1 | $319.41 | $1.99 | — | $1,756.92 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; ret5=+35.1; leftover $414.95 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,756.92 | ▼ close $11,433.14 vs 09:30 $11,466.54 (session -21.56) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,756.92 | ▼ 09:30 equity $11,368.86 vs yday $11,433.14 (-64.28) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `LVWR` | 873 | $1.41 | $11.42 | $-92.52 | $2,976.44 | ▼ -92.52 after sell → book $11,357.45; vs 09:30 mark -11.41 | dropped from list after 3 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `TJGC` | 83 | $16.92 | $2.26 | $-3.67 | $4,378.53 | ▼ -3.67 after sell → book $11,355.18; vs 09:30 mark -2.27 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `SECZ` | 121 | $12.80 | $2.39 | $+131.99 | $5,924.95 | ▲ +131.99 after sell → book $11,352.80; vs 09:30 mark -2.38 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `ABTC` | 132 | $10.11 | $2.42 | $-84.00 | $7,257.05 | ▼ -84.00 after sell → book $11,350.38; vs 09:30 mark -2.42 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `DFDV` | 217 | $6.09 | $2.85 | $-96.79 | $8,575.73 | ▼ -96.79 after sell → book $11,347.53; vs 09:30 mark -2.85 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `CRML` | 45 | $8.39 | $2.15 | $-36.67 | $8,951.14 | ▼ -36.67 after sell → book $11,345.39; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `NUAI` | 57 | $6.83 | $2.18 | $-27.14 | $9,338.27 | ▼ -27.14 after sell → book $11,343.21; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `ARM` | 1 | $331.78 | $2.01 | $+8.36 | $9,668.03 | ▲ +8.36 after sell → book $11,341.19; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `GLND` | 511 | $2.70 | $6.59 | — | $8,281.74 | — | rank by ret_5; rank ret_5; list yday_mover; 🔵; ret5=+109.2; leftover $1381.15 | — |
| 2026-09-23 09:30 ET | **BUY** | `VKTX` | 33 | $41.76 | $2.09 | — | $6,901.57 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+36.4; leftover $1381.15 | — |
| 2026-09-23 09:30 ET | **BUY** | `ORBS` | 1200 | $1.15 | $15.48 | — | $5,506.09 | — | rank by ret_5; rank ret_5; list yday_gainer; ret5=+36.2; leftover $1381.15 | — |
| 2026-09-23 09:30 ET | **BUY** | `INOD` | 19 | $70.84 | $2.05 | — | $4,158.09 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+28.7; leftover $1381.15 | — |
| 2026-09-23 09:30 ET | **BUY** | `EVTL` | 1881 | $0.73 | $19.45 | — | $2,757.98 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+27.5; leftover $1381.15 | — |
| 2026-09-23 09:30 ET | **BUY** | `BFLY` | 139 | $9.90 | $2.41 | — | $1,379.48 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+27.3; leftover $1381.15 | — |
| 2026-09-23 09:30 ET | **BUY** | `SVIA` | 306 | $4.49 | $3.95 | — | $1.59 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; ret5=+26.4; leftover $1381.15 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.59 | ▼ close $10,812.28 vs 09:30 $11,368.86 (session -476.90) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.59 | ▼ 09:30 equity $10,680.89 vs yday $10,812.28 (-131.39) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `FEAM` | 573 | $2.68 | $7.50 | $+105.44 | $1,529.73 | ▲ +105.44 after sell → book $10,673.39; vs 09:30 mark -7.50 | dropped from list after 3 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `VKTX` | 33 | $36.02 | $2.11 | $-193.45 | $2,716.45 | ▼ -193.45 after sell → book $10,671.29; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `ORBS` | 1200 | $1.05 | $15.69 | $-151.17 | $3,960.76 | ▼ -151.17 after sell → book $10,655.60; vs 09:30 mark -15.69 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `INOD` | 19 | $70.50 | $2.07 | $-10.57 | $5,298.19 | ▼ -10.57 after sell → book $10,653.53; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `EVTL` | 1881 | $0.66 | $18.39 | $-176.28 | $6,522.01 | ▼ -176.28 after sell → book $10,635.14; vs 09:30 mark -18.39 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `BFLY` | 139 | $9.12 | $2.44 | $-113.27 | $7,787.25 | ▼ -113.27 after sell → book $10,632.70; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SVIA` | 306 | $3.92 | $4.01 | $-180.85 | $8,984.29 | ▼ -180.85 after sell → book $10,628.69; vs 09:30 mark -4.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,984.29 | ▲ close $11,718.14 vs 09:30 $10,680.89 (session +1,089.45) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,381.22 | ▲ 09:30 equity $11,351.84 vs yday $11,016.01 (+335.83) | 09:30 open · cash $7,381.22 (unchanged overnight, no fees) · equity $11,351.84 vs prior close $11,016.01 (+335.83) · 2 name(s) re-marked at the open (per-name table). GLND×473 yday $5.35 → 09:30 $6.06 +335.83; VICR×4 yday $276.06 → 09:30 $276.06 +0.00 | — |
| 2026-09-25 09:30 ET | **BUY** | `TJGC` | 35 | $29.76 | $2.10 | — | $6,337.52 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+156.1; leftover $1054.46 | join🔴 sector🟡 gen🟢 news🟡 digest🔴 ab🟡 heat🟢 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 65 | $16.21 | $2.19 | — | $5,281.69 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; leftover $1054.46 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `USDE` | 67 | $15.58 | $2.19 | — | $4,235.57 | — | rank by ret_5; rank ret_5; list yday_gainer; 🔵; ret5=+84.4; leftover $1054.46 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `GRAL` | 8 | $123.50 | $2.01 | — | $3,245.55 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+56.6; leftover $1054.46 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ZSQR` | 273 | $3.86 | $3.52 | — | $2,188.25 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; leftover $1054.46 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `CYPH` | 263 | $4.00 | $3.39 | — | $1,131.54 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+32.9; leftover $1054.46 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `DNA` | 103 | $10.20 | $2.30 | — | $78.64 | — | rank by ret_5; rank ret_5; list yday_gainer,yday_mover; 🔵; ret5=+30.7; leftover $1054.46 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $78.64 | ▼ close $11,143.45 vs 09:30 $11,351.84 (session -190.68) | 16:00 close · cash $78.64 · equity $11,143.45 vs 09:30 $11,351.84 (-208.39; session marks -190.68) · 9 name(s) marked open→close (per-name table). GLND×473 09:30 $6.06 → close $5.54 -245.96; VICR×4 09:30 $276.06 → close $276.06 -0.00; TJGC×35 09:30 $29.76 → close $26.24 -123.20; SECZ×65 09:30 $16.21 → close $15.96 -16.25; USDE×67 09:30 $15.58 → close $17.25 +111.82; GRAL×8 09:30 $123.50 → close $126.89 +27.12; ZSQR×273 09:30 $3.86 → close $3.78 -21.84; CYPH×263 09:30 $4.00 → close $4.12 +30.25; DNA×103 09:30 $10.20 → close $10.66 +47.38 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `SNDK` | cash | leftover split 1280.32 < 1 share @ 1646.93 |
| 2026-08-18 | `AVAH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AXTI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FIGR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CRDL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `QTRX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ALM` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `ARX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WEAV` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `NMAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ANGX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `IVVD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DVLT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `ALM` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HMY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `KGC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TMC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `GFI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SCCO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SBSW` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `DFDV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `OKTA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RZLV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SPT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FWDI` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
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
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CNH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TWI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SECZ` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `AGCO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `USDE` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `INDP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CRWV` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `RIOT` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `BTDR` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `INTC` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `AUR` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VSAT` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `ANGX` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `INDP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `IRD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `COHU` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `MXL` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SKHY` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ARBE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `NAUT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `ODD` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HUT` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `VERI` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `SMR` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `INSP` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `SES` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `NTSK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `INSP` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `RPD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `SION` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `S` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ZS` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-22 | `LVWR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `FEAM` | no_price | no 09:30 open — carry |
| 2026-09-22 | `TJGC` | no_price | no 09:30 open — carry |
| 2026-09-22 | `ABTC` | no_price | no 09:30 open — carry |
| 2026-09-22 | `DFDV` | no_price | no 09:30 open — carry |
| 2026-09-22 | `ARQQ` | no_price | no 09:30 open |
| 2026-09-22 | `GRAL` | no_price | no 09:30 open |
| 2026-09-22 | `YSS` | no_price | no 09:30 open |
| 2026-09-22 | `AIBZ` | no_price | no 09:30 open |
| 2026-09-24 | `TJGC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `SECZ` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `VICR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ZSQR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ACMR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `FSLY` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `SWRD` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `GLND` | 511 | 2026-09-23 @ $2.70 | rank by ret_5; rank ret_5; list yday_mover; 🔵; ret5=+109.2; leftover $1381.15 |
