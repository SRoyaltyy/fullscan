# Factor mine action — `union_overnight_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ overnight, no 🚨

Cash book **-17.05%** ($8,295) · signal-only (no cash/fees) was -45.53%. Starts YES **0/30**. Fills 89 · skips 176 · realized $-1797.14.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: the prior Finviz calendar said this name reports AMC today or BMO next session (the print is still ahead; we buy today 09:30 to own the next open).
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
- Minimum hold is 3 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 3 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `overnight=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $8,150.16.

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
| 2026-08-14 09:30 ET | **BUY** | `DUOT` | 353 | $9.43 | $4.55 | — | $6,666.66 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ⚪; ret5=+7.7; leftover $3333.33 | — |
| 2026-08-14 09:30 ET | **BUY** | `NUAI` | 658 | $5.06 | $8.49 | — | $3,328.69 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ⚪; ret5=-3.2; leftover $3333.33 | — |
| 2026-08-14 09:30 ET | **BUY** | `SIDU` | 1298 | $2.55 | $16.74 | — | $2.04 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=+21.5; leftover $3333.33 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.04 | ▼ close $9,928.73 vs 09:30 $10,000.00 (session -41.48) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.04 | ▲ 09:30 equity $10,192.39 vs yday $9,928.73 (+263.66) | — | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.04 | ▲ close $10,513.99 vs 09:30 $10,192.39 (session +321.60) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.04 | ▲ 09:30 equity $10,665.85 vs yday $10,513.99 (+151.86) | — | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.04 | ▼ close $10,635.97 vs 09:30 $10,665.85 (session -29.88) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.04 | ▼ 09:30 equity $10,624.24 vs yday $10,635.97 (-11.73) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `DUOT` | 353 | $11.52 | $4.64 | $+728.57 | $4,063.96 | ▲ +728.57 after sell → book $10,619.60; vs 09:30 mark -4.64 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `NUAI` | 658 | $5.13 | $8.62 | $+28.95 | $7,430.87 | ▲ +28.95 after sell → book $10,610.97; vs 09:30 mark -8.63 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `SIDU` | 1298 | $2.45 | $16.99 | $-163.53 | $10,593.99 | ▼ -163.53 after sell → book $10,593.99; vs 09:30 mark -16.98 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,593.99 | ▲ close $10,593.99 vs 09:30 $10,624.24 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,593.99 | ▲ 09:30 equity $10,593.99 vs yday $10,593.99 (-0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `BEKE` | 124 | $17.04 | $2.36 | — | $8,478.67 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=-0.2; leftover $2118.80 | — |
| 2026-08-20 09:30 ET | **BUY** | `BJ` | 23 | $88.91 | $2.06 | — | $6,431.68 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=-1.0; leftover $2118.80 | — |
| 2026-08-20 09:30 ET | **BUY** | `BKE` | 49 | $42.60 | $2.14 | — | $4,342.14 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=-4.6; leftover $2118.80 | — |
| 2026-08-20 09:30 ET | **BUY** | `FLO` | 285 | $7.43 | $3.68 | — | $2,220.92 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=+4.0; leftover $2118.80 | — |
| 2026-08-20 09:30 ET | **BUY** | `ROST` | 9 | $229.55 | $2.02 | — | $152.95 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight,overnight_mega; 🔵; ret5=-5.5; leftover $2118.80 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $152.95 | ▼ close $10,533.38 vs 09:30 $10,593.99 (session -48.36) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $152.95 | ▲ 09:30 equity $10,810.50 vs yday $10,533.38 (+277.12) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `XPEV` | 6 | $12.29 | $0.76 | — | $78.45 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=+1.9; leftover $76.47 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $78.45 | ▲ close $10,849.05 vs 09:30 $10,810.50 (session +39.31) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $78.45 | ▲ 09:30 equity $10,912.81 vs yday $10,849.05 (+63.76) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $78.45 | ▲ close $11,031.23 vs 09:30 $10,912.81 (session +118.42) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $78.45 | ▼ 09:30 equity $10,997.42 vs yday $11,031.23 (-33.81) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `BEKE` | 124 | $17.63 | $2.40 | $+68.40 | $2,262.17 | ▲ +68.40 after sell → book $10,995.02; vs 09:30 mark -2.40 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BJ` | 23 | $97.63 | $2.09 | $+196.41 | $4,505.58 | ▲ +196.41 after sell → book $10,992.94; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BKE` | 49 | $44.50 | $2.16 | $+88.80 | $6,683.91 | ▲ +88.80 after sell → book $10,990.77; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `FLO` | 285 | $7.25 | $3.74 | $-58.72 | $8,746.42 | ▼ -58.72 after sell → book $10,987.03; vs 09:30 mark -3.74 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ROST` | 9 | $241.50 | $2.04 | $+103.49 | $10,917.88 | ▲ +103.49 after sell → book $10,984.99; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `ANF` | 12 | $112.17 | $2.03 | — | $9,569.81 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ⚪; ret5=+6.8; leftover $1364.73 | — |
| 2026-08-25 09:30 ET | **BUY** | `BBWI` | 71 | $19.16 | $2.20 | — | $8,207.25 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=+3.0; leftover $1364.73 | — |
| 2026-08-25 09:30 ET | **BUY** | `BOX` | 40 | $33.33 | $2.11 | — | $6,871.94 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=+3.7; leftover $1364.73 | — |
| 2026-08-25 09:30 ET | **BUY** | `DCI` | 14 | $93.64 | $2.03 | — | $5,558.95 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=-2.4; leftover $1364.73 | — |
| 2026-08-25 09:30 ET | **BUY** | `DY` | 3 | $390.22 | $2.00 | — | $4,386.29 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=-12.0; leftover $1364.73 | — |
| 2026-08-25 09:30 ET | **BUY** | `FSCO` | 267 | $5.10 | $3.44 | — | $3,021.14 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=+0.2; leftover $1364.73 | — |
| 2026-08-25 09:30 ET | **BUY** | `HEI` | 3 | $357.15 | $2.00 | — | $1,947.69 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ⚪; ret5=-5.0; leftover $1364.73 | — |
| 2026-08-25 09:30 ET | **BUY** | `INTU` | 3 | $364.35 | $2.00 | — | $852.64 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight,overnight_mega; 🔵; ret5=+10.2; leftover $1364.73 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $852.64 | ▼ close $10,637.48 vs 09:30 $10,997.42 (session -329.69) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $852.64 | ▲ 09:30 equity $10,918.26 vs yday $10,637.48 (+280.78) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `XPEV` | 6 | $11.90 | $0.75 | $-3.85 | $923.29 | ▼ -3.85 after sell → book $10,917.51; vs 09:30 mark -0.75 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **BUY** | `STDN` | 8 | $13.95 | $1.14 | — | $810.55 | — | union ∩ overnight, no 🚨; gate overnight=True; list ohlc_hot,overnight; 🔵; ret5=+14.3; leftover $115.41 | — |
| 2026-08-26 09:30 ET | **BUY** | `BBY` | 1 | $85.19 | $0.85 | — | $724.51 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=-2.3; leftover $115.41 | — |
| 2026-08-26 09:30 ET | **BUY** | `BILI` | 7 | $16.22 | $1.16 | — | $609.81 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=-2.5; leftover $115.41 | — |
| 2026-08-26 09:30 ET | **BUY** | `CMBT` | 6 | $17.91 | $1.09 | — | $501.26 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=+3.9; leftover $115.41 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $501.26 | ▲ close $11,076.97 vs 09:30 $10,918.26 (session +163.70) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $501.26 | ▼ 09:30 equity $11,042.38 vs yday $11,076.97 (-34.59) | — | — |
| 2026-08-27 09:30 ET | **BUY** | `GAP` | 3 | $20.75 | $0.63 | — | $438.38 | — | union ∩ overnight, no 🚨; gate overnight=True; list ohlc_hot,overnight; ret5=+5.2; leftover $62.66 | — |
| 2026-08-27 09:30 ET | **BUY** | `BBAR` | 4 | $14.96 | $0.61 | — | $377.93 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=+3.0; leftover $62.66 | — |
| 2026-08-27 09:30 ET | **BUY** | `CHA` | 5 | $10.54 | $0.54 | — | $324.68 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=+2.5; leftover $62.66 | — |
| 2026-08-27 09:30 ET | **BUY** | `HAFN` | 7 | $7.91 | $0.57 | — | $268.74 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=-1.8; leftover $62.66 | — |
| 2026-08-27 09:30 ET | **BUY** | `MNSO` | 5 | $10.89 | $0.56 | — | $213.73 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=+2.7; leftover $62.66 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $213.73 | ▼ close $11,026.00 vs 09:30 $11,042.38 (session -13.46) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $213.73 | ▲ 09:30 equity $11,054.81 vs yday $11,026.00 (+28.81) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `ANF` | 12 | $146.07 | $2.05 | $+402.72 | $1,964.52 | ▲ +402.72 after sell → book $11,052.76; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BBWI` | 71 | $18.75 | $2.23 | $-33.54 | $3,293.54 | ▼ -33.54 after sell → book $11,050.53; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BOX` | 40 | $34.75 | $2.13 | $+52.56 | $4,681.41 | ▲ +52.56 after sell → book $11,048.40; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `DCI` | 14 | $92.25 | $2.05 | $-23.54 | $5,970.86 | ▼ -23.54 after sell → book $11,046.35; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `DY` | 3 | $306.34 | $2.02 | $-255.66 | $6,887.86 | ▼ -255.66 after sell → book $11,044.33; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `FSCO` | 267 | $5.12 | $3.50 | $-1.60 | $8,251.40 | ▼ -1.60 after sell → book $11,040.83; vs 09:30 mark -3.50 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `HEI` | 3 | $339.95 | $2.02 | $-55.62 | $9,269.23 | ▼ -55.62 after sell → book $11,038.81; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `INTU` | 3 | $347.82 | $2.02 | $-53.61 | $10,310.67 | ▼ -53.61 after sell → book $11,036.79; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `LX` | 4444 | $1.16 | $57.33 | — | $5,098.31 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=-13.8; leftover $5155.34 | — |
| 2026-08-28 09:30 ET | **BUY** | `SAIC` | 39 | $129.46 | $2.11 | — | $47.26 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=+2.1; leftover $5155.34 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $47.26 | ▼ close $10,918.42 vs 09:30 $11,054.81 (session -58.94) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $47.26 | ▼ 09:30 equity $10,726.55 vs yday $10,918.42 (-191.87) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `STDN` | 8 | $14.35 | $1.19 | $+0.87 | $160.87 | ▲ +0.87 after sell → book $10,725.36; vs 09:30 mark -1.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBY` | 1 | $81.94 | $0.84 | $-4.95 | $241.97 | ▼ -4.95 after sell → book $10,724.52; vs 09:30 mark -0.84 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `BILI` | 7 | $16.53 | $1.20 | $-0.18 | $356.48 | ▼ -0.18 after sell → book $10,723.32; vs 09:30 mark -1.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `CMBT` | 6 | $18.82 | $1.17 | $+3.20 | $468.23 | ▲ +3.20 after sell → book $10,722.15; vs 09:30 mark -1.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $468.23 | ▼ close $10,515.11 vs 09:30 $10,726.55 (session -207.04) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $468.23 | ▼ 09:30 equity $10,200.51 vs yday $10,515.11 (-314.60) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `GAP` | 3 | $22.05 | $0.69 | $+2.58 | $533.69 | ▲ +2.58 after sell → book $10,199.82; vs 09:30 mark -0.69 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `BBAR` | 4 | $14.82 | $0.62 | $-1.80 | $592.35 | ▼ -1.80 after sell → book $10,199.20; vs 09:30 mark -0.62 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CHA` | 5 | $11.63 | $0.62 | $+4.29 | $649.88 | ▲ +4.29 after sell → book $10,198.58; vs 09:30 mark -0.62 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `HAFN` | 7 | $8.56 | $0.64 | $+3.34 | $709.16 | ▲ +3.34 after sell → book $10,197.94; vs 09:30 mark -0.64 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `MNSO` | 5 | $9.39 | $0.50 | $-8.56 | $755.60 | ▼ -8.56 after sell → book $10,197.43; vs 09:30 mark -0.51 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $755.60 | ▼ close $9,619.63 vs 09:30 $10,200.51 (session -577.80) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $755.60 | ▲ 09:30 equity $9,699.77 vs yday $9,619.63 (+80.14) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `LX` | 4444 | $0.91 | $54.36 | $-1240.47 | $4,727.50 | ▼ -1,240.47 after sell → book $9,645.40; vs 09:30 mark -54.37 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SAIC` | 39 | $126.10 | $2.16 | $-135.30 | $9,643.25 | ▼ -135.30 after sell → book $9,643.25; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,643.25 | ▲ close $9,643.25 vs 09:30 $9,699.77 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,643.25 | ▲ 09:30 equity $9,643.25 vs yday $9,643.25 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `AMBA` | 18 | $66.61 | $2.04 | — | $8,442.22 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=-3.6; leftover $1205.41 | — |
| 2026-09-03 09:30 ET | **BUY** | `ASAN` | 118 | $10.16 | $2.34 | — | $7,241.00 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=+3.8; leftover $1205.41 | — |
| 2026-09-03 09:30 ET | **BUY** | `DOCU` | 17 | $67.06 | $2.04 | — | $6,098.94 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=+10.2; leftover $1205.41 | — |
| 2026-09-03 09:30 ET | **BUY** | `DOMO` | 318 | $3.78 | $4.10 | — | $4,892.80 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=-2.4; leftover $1205.41 | — |
| 2026-09-03 09:30 ET | **BUY** | `GWRE` | 6 | $198.00 | $2.01 | — | $3,702.79 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=+0.9; leftover $1205.41 | — |
| 2026-09-03 09:30 ET | **BUY** | `IOT` | 31 | $37.69 | $2.08 | — | $2,532.31 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=-7.7; leftover $1205.41 | — |
| 2026-09-03 09:30 ET | **BUY** | `LULU` | 9 | $121.15 | $2.02 | — | $1,439.95 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=+3.2; leftover $1205.41 | — |
| 2026-09-03 09:30 ET | **BUY** | `MAMA` | 77 | $15.62 | $2.22 | — | $234.99 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=-6.7; leftover $1205.41 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $234.99 | ▲ close $9,636.42 vs 09:30 $9,643.25 (session +12.03) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $234.99 | ▼ 09:30 equity $9,207.41 vs yday $9,636.42 (-429.01) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `ABM` | 2 | $46.79 | $0.94 | — | $140.47 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=+0.2; leftover $117.49 | — |
| 2026-09-04 09:30 ET | **BUY** | `UNFI` | 2 | $43.80 | $0.88 | — | $51.98 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=-7.7; leftover $117.49 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $51.98 | ▼ close $9,095.88 vs 09:30 $9,207.41 (session -109.70) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $51.98 | ▼ 09:30 equity $9,039.17 vs yday $9,095.88 (-56.71) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $51.98 | ▼ close $9,012.32 vs 09:30 $9,039.17 (session -26.85) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $51.98 | ▼ 09:30 equity $8,935.99 vs yday $9,012.32 (-76.33) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `AMBA` | 18 | $63.07 | $2.06 | $-67.83 | $1,185.18 | ▼ -67.83 after sell → book $8,933.93; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `ASAN` | 118 | $8.64 | $2.37 | $-184.08 | $2,202.33 | ▼ -184.08 after sell → book $8,931.56; vs 09:30 mark -2.37 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `DOCU` | 17 | $64.64 | $2.06 | $-45.24 | $3,299.14 | ▼ -45.24 after sell → book $8,929.49; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `DOMO` | 318 | $3.86 | $4.17 | $+17.17 | $4,522.46 | ▲ +17.17 after sell → book $8,925.33; vs 09:30 mark -4.16 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `GWRE` | 6 | $147.85 | $2.03 | $-304.94 | $5,407.53 | ▼ -304.94 after sell → book $8,923.30; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `IOT` | 31 | $39.60 | $2.10 | $+55.02 | $6,633.03 | ▲ +55.02 after sell → book $8,921.20; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `LULU` | 9 | $101.90 | $2.04 | $-177.30 | $7,548.09 | ▼ -177.30 after sell → book $8,919.16; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MAMA` | 77 | $15.31 | $2.24 | $-28.33 | $8,724.72 | ▼ -28.33 after sell → book $8,916.92; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,724.72 | ▼ close $8,913.42 vs 09:30 $8,935.99 (session -3.50) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,724.72 | ▲ 09:30 equity $8,913.98 vs yday $8,913.42 (+0.56) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `ABM` | 2 | $49.74 | $1.02 | $+3.94 | $8,823.18 | ▲ +3.94 after sell → book $8,912.96; vs 09:30 mark -1.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `UNFI` | 2 | $44.89 | $0.92 | $+0.37 | $8,912.03 | ▲ +0.37 after sell → book $8,912.03; vs 09:30 mark -0.93 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,912.03 | ▲ close $8,912.03 vs 09:30 $8,913.98 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,912.03 | ▲ 09:30 equity $8,912.03 vs yday $8,912.03 (+0.00) | — | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,912.03 | ▲ close $8,912.03 vs 09:30 $8,912.03 (session +0.00) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,912.03 | ▲ 09:30 equity $8,912.03 vs yday $8,912.03 (+0.00) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,912.03 | ▲ close $8,912.03 vs 09:30 $8,912.03 (session +0.00) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,912.03 | ▲ 09:30 equity $8,912.03 vs yday $8,912.03 (+0.00) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,912.03 | ▲ close $8,912.03 vs 09:30 $8,912.03 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,912.03 | ▲ 09:30 equity $8,912.03 vs yday $8,912.03 (+0.00) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `ALMU` | 324 | $13.75 | $4.18 | — | $4,452.85 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=-1.4; leftover $4456.02 | — |
| 2026-09-16 09:30 ET | **BUY** | `LEN` | 55 | $80.63 | $2.15 | — | $16.05 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=-0.4; leftover $4456.02 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16.05 | ▼ close $8,677.17 vs 09:30 $8,912.03 (session -228.53) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16.05 | ▼ 09:30 equity $8,103.09 vs yday $8,677.17 (-574.08) | — | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16.05 | ▲ close $8,140.13 vs 09:30 $8,103.09 (session +37.04) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16.05 | ▼ 09:30 equity $8,091.16 vs yday $8,140.13 (-48.97) | — | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16.05 | ▲ close $8,342.60 vs 09:30 $8,091.16 (session +251.44) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16.05 | ▲ 09:30 equity $8,502.45 vs yday $8,342.60 (+159.85) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `ALMU` | 324 | $13.12 | $4.27 | $-210.95 | $4,264.28 | ▼ -210.95 after sell → book $8,498.18; vs 09:30 mark -4.27 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `LEN` | 55 | $76.98 | $2.20 | $-205.10 | $8,495.98 | ▼ -205.10 after sell → book $8,495.98; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **BUY** | `ABVX` | 26 | $105.72 | $2.07 | — | $5,745.19 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=-11.5; leftover $2831.99 | — |
| 2026-09-21 09:30 ET | **BUY** | `MLKN` | 135 | $20.85 | $2.40 | — | $2,928.05 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=-2.5; leftover $2831.99 | — |
| 2026-09-21 09:30 ET | **BUY** | `THO` | 41 | $68.39 | $2.11 | — | $121.95 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=-7.0; leftover $2831.99 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $121.95 | ▼ close $8,422.39 vs 09:30 $8,502.45 (session -67.02) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $121.95 | ▲ 09:30 equity $8,422.39 vs yday $8,422.39 (-0.00) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $121.95 | ▲ close $8,422.39 vs 09:30 $8,422.39 (session +0.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $121.95 | ▼ 09:30 equity $8,273.16 vs yday $8,422.39 (-149.23) | — | — |
| 2026-09-23 09:30 ET | **BUY** | `BB` | 2 | $8.60 | $0.18 | — | $104.57 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=-0.4; leftover $20.32 | — |
| 2026-09-23 09:30 ET | **BUY** | `NEOV` | 5 | $3.40 | $0.18 | — | $87.38 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=-4.8; leftover $20.32 | — |
| 2026-09-23 09:30 ET | **BUY** | `SFIX` | 6 | $2.99 | $0.20 | — | $69.25 | — | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=+1.7; leftover $20.32 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $69.25 | ▼ close $8,236.27 vs 09:30 $8,273.16 (session -36.33) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $69.25 | ▼ 09:30 equity $8,200.80 vs yday $8,236.27 (-35.47) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `ABVX` | 26 | $92.97 | $2.10 | $-335.67 | $2,484.37 | ▼ -335.67 after sell → book $8,198.70; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `MLKN` | 135 | $19.96 | $2.44 | $-124.98 | $5,176.53 | ▼ -124.98 after sell → book $8,196.26; vs 09:30 mark -2.44 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `THO` | 41 | $72.58 | $2.15 | $+167.53 | $8,150.16 | ▲ +167.53 after sell → book $8,194.11; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,150.16 | ▼ close $8,192.86 vs 09:30 $8,200.80 (session -1.25) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,266.49 | ▼ 09:30 equity $8,295.78 vs yday $8,295.85 (-0.07) | 09:30 open · cash $8,266.49 (unchanged overnight, no fees) · equity $8,295.78 vs prior close $8,295.85 (-0.07) · 3 name(s) re-marked at the open (per-name table). BB×1 yday $8.73 → 09:30 $8.73 +0.00; NEOV×4 yday $2.39 → 09:30 $2.39 -0.02; SFIX×5 yday $2.21 → 09:30 $2.20 -0.05 | — |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,266.49 | ▼ close $8,294.75 vs 09:30 $8,295.78 (session -1.03) | 16:00 close · cash $8,266.49 · equity $8,294.75 vs 09:30 $8,295.78 (-1.03; session marks -1.03) · 3 name(s) marked open→close (per-name table). BB×1 09:30 $8.73 → close $8.73 -0.00; NEOV×4 09:30 $2.39 → close $2.19 -0.80; SFIX×5 09:30 $2.20 → close $2.15 -0.23 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `DUOT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `NUAI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `SIDU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `HSAI` | cash | leftover split 0.34 < 1 share @ 18.32 |
| 2026-08-17 | `IQ` | cash | leftover split 0.34 < 1 share @ 1.35 |
| 2026-08-17 | `KLAR` | cash | leftover split 0.34 < 1 share @ 20.67 |
| 2026-08-17 | `PONY` | cash | leftover split 0.34 < 1 share @ 8.16 |
| 2026-08-17 | `VNET` | cash | leftover split 0.34 < 1 share @ 7.75 |
| 2026-08-17 | `XP` | cash | leftover split 0.34 < 1 share @ 15.93 |
| 2026-08-18 | `DUOT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `NUAI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `SIDU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `DVLT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `EL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `JKHY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `KC` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `KEYS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `LOW` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MRCY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TGT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `AAP` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AEG` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ALVO` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ATAT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ATHM` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BABA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BILL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BULL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `BEKE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BJ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BKE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `FLO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ROST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `PDD` | cash | leftover split 76.47 < 1 share @ 90.03 |
| 2026-08-24 | `BEKE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BJ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BKE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `FLO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ROST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `XPEV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `BNS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BZ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `EH` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `GFI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SHMD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SLQT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TUYA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `VIPS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `XPEV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `ANF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `BBWI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `BOX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `DCI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `DY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `FSCO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `HEI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `INTU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `A` | cash | leftover split 115.41 < 1 share @ 152.45 |
| 2026-08-26 | `CM` | cash | leftover split 115.41 < 1 share @ 118.50 |
| 2026-08-26 | `CRM` | cash | leftover split 115.41 < 1 share @ 199.94 |
| 2026-08-26 | `CRWD` | cash | leftover split 115.41 < 1 share @ 182.75 |
| 2026-08-27 | `ANF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BBWI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BOX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `DCI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `DY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `FSCO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `HEI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `INTU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `STDN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `BBY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `BILI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `CMBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `ADSK` | cash | leftover split 62.66 < 1 share @ 261.47 |
| 2026-08-27 | `AFRM` | cash | leftover split 62.66 < 1 share @ 76.90 |
| 2026-08-27 | `ESTC` | cash | leftover split 62.66 < 1 share @ 82.65 |
| 2026-08-28 | `STDN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `BBY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `BILI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `CMBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `GAP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `BBAR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `CHA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `HAFN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `MNSO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `GAP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `BBAR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `CHA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `HAFN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `MNSO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `LX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SAIC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `YEXT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MDT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MMED` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NIO` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RZLV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SSL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `LX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SAIC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `GTLB` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `BF-B` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CRDO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DELL` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `FCEL` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MDB` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `PANW` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `AI` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `CHPT` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `CIEN` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `CPB` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FIVE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MOMO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `NTSK` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PHR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `AMBA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ASAN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DOCU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DOMO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `GWRE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `IOT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `LULU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MAMA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `AMBA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ASAN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `DOCU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `DOMO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `GWRE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `IOT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `LULU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `MAMA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ABM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `UNFI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `AVO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `BRZE` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CAL` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CNM` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `KFY` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `ODD` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SUNB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `ABM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `UNFI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `AVAV` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `COO` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `M` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `NAVN` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `DSGX` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `KR` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `LPTH` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ORCL` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `REF` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `RH` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `FPS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `PLAY` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `TCOM` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `ALMU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `LEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `ALMU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `LEN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `ABVX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `MLKN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `THO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `CBRL` | no_price | no 09:30 open |
| 2026-09-22 | `GIS` | cash | leftover split 30.49 < 1 share @ 35.96 |
| 2026-09-22 | `KBH` | cash | leftover split 30.49 < 1 share @ 49.39 |
| 2026-09-22 | `PAYX` | no_price | no 09:30 open |
| 2026-09-23 | `ABVX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `MLKN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `THO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `DRI` | cash | leftover split 20.32 < 1 share @ 215.10 |
| 2026-09-23 | `FUL` | cash | leftover split 20.32 < 1 share @ 50.51 |
| 2026-09-23 | `SNX` | cash | leftover split 20.32 < 1 share @ 283.46 |
| 2026-09-24 | `BB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `NEOV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `SFIX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `COST` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `BB` | 2 | 2026-09-23 @ $8.60 | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=-0.4; leftover $20.32 |
| `NEOV` | 5 | 2026-09-23 @ $3.40 | union ∩ overnight, no 🚨; gate overnight=True; list overnight; 🔵; ret5=-4.8; leftover $20.32 |
| `SFIX` | 6 | 2026-09-23 @ $2.99 | union ∩ overnight, no 🚨; gate overnight=True; list overnight; ret5=+1.7; leftover $20.32 |
