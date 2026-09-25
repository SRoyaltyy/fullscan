# Factor mine action — `union_news_head_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · prior-export headline🟢 only

Cash book **-13.59%** ($8,641) · signal-only (no cash/fees) was +13.09%. Starts YES **0/30**. Fills 166 · skips 65 · realized $-814.48.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Sort: how many morning cameras are green vs red.
- Must-have: the prior-export headline is green.
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is allowed only when every must-have is true.
- A name is thrown out if any must-not is true.
- Sort the keepers by how many morning cameras are green vs red and keep the top 8.
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
- **Gate** `headline=good` · **rank** `cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,185.54.

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
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 94 | $13.18 | $2.27 | — | $8,758.81 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 290 | $4.31 | $3.74 | — | $7,505.17 | — | prior-export headline🟢 only; gate headline=good; rank cond; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 63 | $19.57 | $2.18 | — | $6,270.08 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 92 | $13.55 | $2.27 | — | $5,021.21 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer; 🔵; ⚪; ret5=+17.5; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `VELO` | 81 | $15.38 | $2.23 | — | $3,773.20 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+16.3; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `S` | 52 | $23.77 | $2.15 | — | $2,534.75 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ret5=+14.8; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `ZS` | 6 | $190.00 | $2.01 | — | $1,392.75 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ret5=+15.7; leftover $1250.00 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,392.75 | ▲ close $10,019.55 vs 09:30 $10,000.00 (session +36.39) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,392.75 | ▲ 09:30 equity $10,071.64 vs yday $10,019.55 (+52.09) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `HLIT` | 94 | $13.84 | $2.30 | $+57.47 | $2,691.41 | ▲ +57.47 after sell → book $10,069.34; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 290 | $4.60 | $3.80 | $+76.56 | $4,021.61 | ▲ +76.56 after sell → book $10,065.54; vs 09:30 mark -3.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 63 | $19.57 | $2.20 | $-4.38 | $5,252.32 | ▼ -4.38 after sell → book $10,063.34; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MH` | 92 | $13.16 | $2.29 | $-40.44 | $6,460.75 | ▼ -40.44 after sell → book $10,061.05; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VELO` | 81 | $16.05 | $2.26 | $+49.78 | $7,758.54 | ▲ +49.78 after sell → book $10,058.79; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `S` | 52 | $22.50 | $2.17 | $-70.61 | $8,926.37 | ▼ -70.61 after sell → book $10,056.62; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ZS` | 6 | $188.38 | $2.03 | $-13.79 | $10,054.60 | ▼ -13.79 after sell → book $10,054.60; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `OUST` | 102 | $49.00 | $2.30 | — | $5,054.30 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer; ⚪; ret5=+12.2; leftover $5027.30 | — |
| 2026-08-17 09:30 ET | **BUY** | `CELC` | 54 | $92.99 | $2.15 | — | $30.69 | — | prior-export headline🟢 only; gate headline=good; rank cond; list probable,yday_gainer; ret5=-0.8; leftover $5027.30 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $30.69 | ▼ close $9,931.71 vs 09:30 $10,071.64 (session -118.44) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $30.69 | ▼ 09:30 equity $9,618.39 vs yday $9,931.71 (-313.32) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `OUST` | 102 | $45.09 | $2.35 | $-403.47 | $4,627.52 | ▼ -403.47 after sell → book $9,616.04; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CELC` | 54 | $92.38 | $2.20 | $-37.29 | $9,613.84 | ▼ -37.29 after sell → book $9,613.84; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,613.84 | ▲ close $9,613.84 vs 09:30 $9,618.39 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,613.84 | ▲ 09:30 equity $9,613.84 vs yday $9,613.84 (-0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,613.84 | ▲ close $9,613.84 vs 09:30 $9,613.84 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,613.84 | ▲ 09:30 equity $9,613.84 vs yday $9,613.84 (-0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $8,428.68 | — | prior-export headline🟢 only; gate headline=good; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1201.73 | — |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 486 | $2.47 | $6.27 | — | $7,221.99 | — | prior-export headline🟢 only; gate headline=good; rank cond; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $1201.73 | — |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 20 | $58.73 | $2.05 | — | $6,045.34 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.8; leftover $1201.73 | — |
| 2026-08-20 09:30 ET | **BUY** | `ASST` | 75 | $16.00 | $2.21 | — | $4,843.12 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+18.9; leftover $1201.73 | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 8 | $150.14 | $2.01 | — | $3,639.99 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1201.73 | — |
| 2026-08-20 09:30 ET | **BUY** | `ZLAB` | 45 | $26.57 | $2.12 | — | $2,442.21 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ret5=+4.8; leftover $1201.73 | — |
| 2026-08-20 09:30 ET | **BUY** | `TEAM` | 6 | $173.90 | $2.01 | — | $1,396.81 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ret5=+12.2; leftover $1201.73 | — |
| 2026-08-20 09:30 ET | **BUY** | `HUMA` | 1699 | $0.71 | $17.11 | — | $178.50 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+10.5; leftover $1201.73 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $178.50 | ▼ close $9,407.34 vs 09:30 $9,613.84 (session -170.67) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $178.50 | ▲ 09:30 equity $9,578.76 vs yday $9,407.34 (+171.42) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $1,420.81 | ▲ +57.15 after sell → book $9,576.71; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ASST` | 75 | $17.66 | $2.24 | $+120.05 | $2,743.08 | ▲ +120.05 after sell → book $9,574.47; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `MRNA` | 8 | $133.11 | $2.03 | $-140.29 | $3,805.92 | ▼ -140.29 after sell → book $9,572.44; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ZLAB` | 45 | $26.25 | $2.15 | $-18.67 | $4,985.03 | ▼ -18.67 after sell → book $9,570.29; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `TEAM` | 6 | $174.22 | $2.03 | $-2.12 | $6,028.32 | ▼ -2.12 after sell → book $9,568.27; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HUMA` | 1699 | $0.67 | $16.84 | $-90.02 | $7,156.61 | ▼ -90.02 after sell → book $9,551.43; vs 09:30 mark -16.84 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 10 | $115.18 | $2.02 | — | $6,002.79 | — | prior-export headline🟢 only; gate headline=good; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $1192.77 | — |
| 2026-08-21 09:30 ET | **BUY** | `GRAL` | 15 | $78.88 | $2.04 | — | $4,817.55 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+14.3; leftover $1192.77 | — |
| 2026-08-21 09:30 ET | **BUY** | `ABTC` | 137 | $8.66 | $2.40 | — | $3,628.73 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+15.7; leftover $1192.77 | — |
| 2026-08-21 09:30 ET | **BUY** | `HIVE` | 368 | $3.24 | $4.75 | — | $2,431.66 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer; 🔵; ret5=+21.3; leftover $1192.77 | — |
| 2026-08-21 09:30 ET | **BUY** | `MARA` | 101 | $11.70 | $2.29 | — | $1,247.67 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+21.1; leftover $1192.77 | — |
| 2026-08-21 09:30 ET | **BUY** | `BTDR` | 107 | $11.10 | $2.31 | — | $58.19 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer; ret5=+19.1; leftover $1192.77 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $58.19 | ▼ close $9,404.25 vs 09:30 $9,578.76 (session -131.37) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $58.19 | ▼ 09:30 equity $9,390.49 vs yday $9,404.25 (-13.76) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 486 | $2.40 | $6.36 | $-46.65 | $1,218.23 | ▼ -46.65 after sell → book $9,384.13; vs 09:30 mark -6.36 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 10 | $121.00 | $2.04 | $+54.14 | $2,426.19 | ▲ +54.14 after sell → book $9,382.09; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `GRAL` | 15 | $81.87 | $2.06 | $+40.76 | $3,652.19 | ▲ +40.76 after sell → book $9,380.04; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ABTC` | 137 | $8.00 | $2.43 | $-95.25 | $4,745.75 | ▼ -95.25 after sell → book $9,377.60; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `HIVE` | 368 | $2.99 | $4.82 | $-101.57 | $5,841.26 | ▼ -101.57 after sell → book $9,372.79; vs 09:30 mark -4.81 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MARA` | 101 | $11.17 | $2.32 | $-58.14 | $6,967.11 | ▼ -58.14 after sell → book $9,370.47; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BTDR` | 107 | $11.48 | $2.34 | $+36.55 | $8,193.13 | ▲ +36.55 after sell → book $9,368.13; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,193.13 | ▼ close $9,334.63 vs 09:30 $9,390.49 (session -33.50) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,193.13 | ▲ 09:30 equity $9,351.73 vs yday $9,334.63 (+17.10) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 20 | $57.93 | $2.07 | $-20.12 | $9,349.66 | ▼ -20.12 after sell → book $9,349.66; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 53 | $35.05 | $2.15 | — | $7,489.86 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer; 🔵; ⚪; ret5=+19.7; leftover $1869.93 | — |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 198 | $9.42 | $2.58 | — | $5,622.11 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+13.6; leftover $1869.93 | — |
| 2026-08-25 09:30 ET | **BUY** | `ZYME` | 64 | $28.86 | $2.18 | — | $3,772.89 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ret5=+13.7; leftover $1869.93 | — |
| 2026-08-25 09:30 ET | **BUY** | `REAX` | 77 | $24.11 | $2.22 | — | $1,914.20 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_mover; ret5=+891.7; leftover $1869.93 | — |
| 2026-08-25 09:30 ET | **BUY** | `EOLS` | 214 | $8.72 | $2.76 | — | $45.36 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ret5=+13.0; leftover $1869.93 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $45.36 | ▲ close $9,805.93 vs 09:30 $9,351.73 (session +468.17) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $45.36 | ▼ 09:30 equity $9,640.17 vs yday $9,805.93 (-165.76) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `EZPW` | 53 | $35.70 | $2.17 | $+30.13 | $1,935.29 | ▲ +30.13 after sell → book $9,638.00; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `RUM` | 198 | $10.07 | $2.63 | $+123.48 | $3,926.51 | ▲ +123.48 after sell → book $9,635.36; vs 09:30 mark -2.64 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZYME` | 64 | $27.56 | $2.21 | $-87.59 | $5,688.15 | ▼ -87.59 after sell → book $9,633.16; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `REAX` | 77 | $26.61 | $2.25 | $+188.03 | $7,734.87 | ▲ +188.03 after sell → book $9,630.91; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EOLS` | 214 | $8.86 | $2.81 | $+24.39 | $9,628.10 | ▲ +24.39 after sell → book $9,628.10; vs 09:30 mark -2.81 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `FNV` | 7 | $267.02 | $2.01 | — | $7,756.94 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ret5=+16.7; leftover $1925.62 | — |
| 2026-08-26 09:30 ET | **BUY** | `TRLV` | 171 | $11.22 | $2.50 | — | $5,835.82 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ret5=+16.8; leftover $1925.62 | — |
| 2026-08-26 09:30 ET | **BUY** | `CAPR` | 232 | $8.29 | $2.99 | — | $3,909.55 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.1; leftover $1925.62 | — |
| 2026-08-26 09:30 ET | **BUY** | `FWRD` | 110 | $17.41 | $2.32 | — | $1,992.13 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_mover; ret5=-9.2; leftover $1925.62 | — |
| 2026-08-26 09:30 ET | **BUY** | `FLNC` | 173 | $11.12 | $2.51 | — | $65.86 | — | prior-export headline🟢 only; gate headline=good; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.3; leftover $1925.62 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $65.86 | ▲ close $9,919.64 vs 09:30 $9,640.17 (session +303.88) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $65.86 | ▲ 09:30 equity $9,943.49 vs yday $9,919.64 (+23.85) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `FNV` | 7 | $267.23 | $2.04 | $-2.58 | $1,934.43 | ▼ -2.58 after sell → book $9,941.45; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `GEN` | 21 | $29.83 | $2.05 | — | $1,305.95 | — | prior-export headline🟢 only; gate headline=good; rank cond; list mover_buy; 🔵; ret5=+7.6; leftover $644.81 | — |
| 2026-08-27 09:30 ET | **BUY** | `SRRK` | 10 | $60.00 | $2.02 | — | $703.93 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; ret5=+6.2; leftover $644.81 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $703.93 | ▲ close $10,081.47 vs 09:30 $9,943.49 (session +144.09) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $703.93 | ▼ 09:30 equity $9,967.00 vs yday $10,081.47 (-114.47) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `TRLV` | 171 | $11.00 | $2.55 | $-42.67 | $2,582.38 | ▼ -42.67 after sell → book $9,964.45; vs 09:30 mark -2.55 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `FWRD` | 110 | $17.70 | $2.35 | $+27.23 | $4,527.03 | ▲ +27.23 after sell → book $9,962.10; vs 09:30 mark -2.35 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `FLNC` | 173 | $11.27 | $2.55 | $+20.89 | $6,474.19 | ▲ +20.89 after sell → book $9,959.55; vs 09:30 mark -2.55 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GEN` | 21 | $30.50 | $2.07 | $+9.94 | $7,112.61 | ▲ +9.94 after sell → book $9,957.47; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `SRRK` | 10 | $58.75 | $2.04 | $-16.56 | $7,698.07 | ▼ -16.56 after sell → book $9,955.43; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 7 | $141.76 | $2.01 | — | $6,703.74 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1099.72 | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 33 | $32.90 | $2.09 | — | $5,615.95 | — | prior-export headline🟢 only; gate headline=good; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1099.72 | — |
| 2026-08-28 09:30 ET | **BUY** | `TLS` | 228 | $4.82 | $2.94 | — | $4,514.05 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ret5=+8.8; leftover $1099.72 | — |
| 2026-08-28 09:30 ET | **BUY** | `TH` | 57 | $19.00 | $2.16 | — | $3,428.89 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; ret5=+7.5; leftover $1099.72 | — |
| 2026-08-28 09:30 ET | **BUY** | `OPTX` | 127 | $8.61 | $2.37 | — | $2,333.05 | — | prior-export headline🟢 only; gate headline=good; rank cond; list probable,yday_gainer; ret5=-0.7; leftover $1099.72 | — |
| 2026-08-28 09:30 ET | **BUY** | `BBWI` | 58 | $18.75 | $2.16 | — | $1,243.39 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer; ret5=-5.0; leftover $1099.72 | — |
| 2026-08-28 09:30 ET | **BUY** | `ERAS` | 57 | $19.25 | $2.16 | — | $143.98 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer; ret5=+14.1; leftover $1099.72 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $143.98 | ▼ close $9,697.56 vs 09:30 $9,967.00 (session -241.98) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $143.98 | ▼ 09:30 equity $9,648.96 vs yday $9,697.56 (-48.60) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 232 | $9.50 | $3.05 | $+274.68 | $2,344.93 | ▲ +274.68 after sell → book $9,645.91; vs 09:30 mark -3.05 | dropped from list after 3 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 7 | $132.30 | $2.03 | $-70.26 | $3,269.00 | ▼ -70.26 after sell → book $9,643.88; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 33 | $31.15 | $2.11 | $-61.95 | $4,294.84 | ▼ -61.95 after sell → book $9,641.77; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TLS` | 228 | $4.81 | $2.99 | $-8.21 | $5,388.53 | ▼ -8.21 after sell → book $9,638.78; vs 09:30 mark -2.99 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `OPTX` | 127 | $8.52 | $2.40 | $-16.20 | $6,468.17 | ▼ -16.20 after sell → book $9,636.38; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BBWI` | 58 | $19.25 | $2.18 | $+24.65 | $7,582.48 | ▲ +24.65 after sell → book $9,634.20; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ERAS` | 57 | $17.87 | $2.18 | $-83.00 | $8,598.89 | ▼ -83.00 after sell → book $9,632.02; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,598.89 | ▲ close $9,654.53 vs 09:30 $9,648.96 (session +22.51) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,598.89 | ▼ 09:30 equity $9,650.54 vs yday $9,654.53 (-3.99) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `TH` | 57 | $18.45 | $2.18 | $-35.69 | $9,648.36 | ▼ -35.69 after sell → book $9,648.36; vs 09:30 mark -2.18 | dropped from list after 2 sess (min 1) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,648.36 | ▲ close $9,648.36 vs 09:30 $9,650.54 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,648.36 | ▲ 09:30 equity $9,648.36 vs yday $9,648.36 (+0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,648.36 | ▲ close $9,648.36 vs 09:30 $9,648.36 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,648.36 | ▲ 09:30 equity $9,648.36 vs yday $9,648.36 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `CXW` | 49 | $32.31 | $2.14 | — | $8,063.03 | — | prior-export headline🟢 only; gate headline=good; rank cond; list mover_buy; 🔵; ⚪; ret5=-4.1; leftover $1608.06 | — |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 101 | $15.87 | $2.29 | — | $6,457.87 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1608.06 | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 67 | $23.88 | $2.19 | — | $4,855.72 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1608.06 | — |
| 2026-09-03 09:30 ET | **BUY** | `DE` | 2 | $703.25 | $2.00 | — | $3,447.22 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.1; leftover $1608.06 | — |
| 2026-09-03 09:30 ET | **BUY** | `CNXC` | 48 | $32.88 | $2.13 | — | $1,866.85 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer; 🔵; ret5=+16.2; leftover $1608.06 | — |
| 2026-09-03 09:30 ET | **BUY** | `OPTX` | 211 | $7.59 | $2.72 | — | $262.64 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_mover; ret5=-11.5; leftover $1608.06 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $262.64 | ▲ close $9,819.14 vs 09:30 $9,648.36 (session +184.25) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $262.64 | ▼ 09:30 equity $9,742.65 vs yday $9,819.14 (-76.49) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `CXW` | 49 | $33.46 | $2.16 | $+52.05 | $1,900.02 | ▲ +52.05 after sell → book $9,740.49; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 67 | $23.84 | $2.21 | $-7.09 | $3,495.08 | ▼ -7.09 after sell → book $9,738.27; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DE` | 2 | $692.03 | $2.02 | $-26.45 | $4,877.13 | ▼ -26.45 after sell → book $9,736.26; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNXC` | 48 | $32.48 | $2.16 | $-23.49 | $6,434.01 | ▼ -23.49 after sell → book $9,734.10; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `OPTX` | 211 | $7.79 | $2.77 | $+36.71 | $8,074.93 | ▲ +36.71 after sell → book $9,731.33; vs 09:30 mark -2.77 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 53 | $75.65 | $2.15 | — | $4,063.33 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+6.0; leftover $4037.46 | — |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 2080 | $1.94 | $26.83 | — | $1.30 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,yday_mover; ret5=+18.3; leftover $4037.46 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.30 | ▲ close $9,728.12 vs 09:30 $9,742.65 (session +25.77) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.30 | ▲ 09:30 equity $9,905.76 vs yday $9,728.12 (+177.64) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 101 | $16.74 | $2.32 | $+83.25 | $1,689.71 | ▲ +83.25 after sell → book $9,903.43; vs 09:30 mark -2.33 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BAK` | 2080 | $1.94 | $27.21 | $-54.04 | $5,697.70 | ▼ -54.04 after sell → book $9,876.22; vs 09:30 mark -27.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,697.70 | ▼ close $9,763.33 vs 09:30 $9,905.76 (session -112.89) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,697.70 | ▼ 09:30 equity $9,757.50 vs yday $9,763.33 (-5.83) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `MRX` | 53 | $76.60 | $2.19 | $+46.01 | $9,755.31 | ▲ +46.01 after sell → book $9,755.31; vs 09:30 mark -2.19 | dropped from list after 2 sess (min 1) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,755.31 | ▲ close $9,755.31 vs 09:30 $9,757.50 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,755.31 | ▲ 09:30 equity $9,755.31 vs yday $9,755.31 (+0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,755.31 | ▲ close $9,755.31 vs 09:30 $9,755.31 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,755.31 | ▲ 09:30 equity $9,755.31 vs yday $9,755.31 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 9 | $164.43 | $2.02 | — | $8,273.43 | — | prior-export headline🟢 only; gate headline=good; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $1625.89 | — |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 6 | $242.17 | $2.01 | — | $6,818.40 | — | prior-export headline🟢 only; gate headline=good; rank cond; list earn_react; ret5=-11.1; leftover $1625.89 | — |
| 2026-09-11 09:30 ET | **BUY** | `AVTR` | 108 | $15.01 | $2.31 | — | $5,195.00 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ret5=+4.9; leftover $1625.89 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 766 | $2.12 | $9.88 | — | $3,561.20 | — | prior-export headline🟢 only; gate headline=good; rank cond; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $1625.89 | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 797 | $2.04 | $10.28 | — | $1,925.04 | — | prior-export headline🟢 only; gate headline=good; rank cond; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $1625.89 | — |
| 2026-09-11 09:30 ET | **BUY** | `RH` | 11 | $135.71 | $2.02 | — | $430.21 | — | prior-export headline🟢 only; gate headline=good; rank cond; list earn_react; ret5=-9.2; leftover $1625.89 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $430.21 | ▼ close $9,565.61 vs 09:30 $9,755.31 (session -161.18) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $430.21 | ▼ 09:30 equity $9,495.68 vs yday $9,565.61 (-69.93) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 9 | $141.42 | $2.04 | $-211.14 | $1,700.95 | ▼ -211.14 after sell → book $9,493.64; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ADBE` | 6 | $261.51 | $2.03 | $+112.00 | $3,267.98 | ▲ +112.00 after sell → book $9,491.61; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BAK` | 766 | $2.05 | $10.02 | $-73.52 | $4,828.26 | ▼ -73.52 after sell → book $9,481.59; vs 09:30 mark -10.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `AMTX` | 797 | $2.01 | $10.43 | $-44.62 | $6,419.80 | ▼ -44.62 after sell → book $9,471.16; vs 09:30 mark -10.43 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `RH` | 11 | $131.40 | $2.04 | $-51.48 | $7,863.16 | ▼ -51.48 after sell → book $9,469.12; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,863.16 | ▲ close $9,500.44 vs 09:30 $9,495.68 (session +31.32) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,863.16 | ▲ 09:30 equity $9,506.92 vs yday $9,500.44 (+6.48) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,863.16 | ▲ close $9,530.68 vs 09:30 $9,506.92 (session +23.76) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,863.16 | ▲ 09:30 equity $9,540.40 vs yday $9,530.68 (+9.72) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 149 | $26.27 | $2.44 | — | $3,946.49 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer; 🔵; ret5=+10.0; leftover $3931.58 | — |
| 2026-09-16 09:30 ET | **BUY** | `SION` | 565 | $6.95 | $7.29 | — | $12.45 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_mover; ret5=-5.8; leftover $3931.58 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $12.45 | ▲ close $9,637.84 vs 09:30 $9,540.40 (session +107.17) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12.45 | ▲ 09:30 equity $9,777.47 vs yday $9,637.84 (+139.63) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `WAY` | 149 | $26.51 | $2.49 | $+30.83 | $3,959.95 | ▲ +30.83 after sell → book $9,774.98; vs 09:30 mark -2.49 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SION` | 565 | $7.27 | $7.42 | $+166.10 | $8,060.08 | ▲ +166.10 after sell → book $9,767.56; vs 09:30 mark -7.42 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 9 | $170.85 | $2.02 | — | $6,520.42 | — | prior-export headline🟢 only; gate headline=good; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $1612.02 | — |
| 2026-09-17 09:30 ET | **BUY** | `GME` | 72 | $22.12 | $2.21 | — | $4,925.57 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ret5=+10.5; leftover $1612.02 | — |
| 2026-09-17 09:30 ET | **BUY** | `JBHT` | 6 | $238.60 | $2.01 | — | $3,491.96 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_mover; ret5=-11.6; leftover $1612.02 | — |
| 2026-09-17 09:30 ET | **BUY** | `TNDM` | 90 | $17.72 | $2.26 | — | $1,894.90 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer; 🔵; ret5=-8.3; leftover $1612.02 | — |
| 2026-09-17 09:30 ET | **BUY** | `BAK` | 910 | $1.77 | $11.74 | — | $272.46 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_mover; ret5=-10.2; leftover $1612.02 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $272.46 | ▲ close $9,828.89 vs 09:30 $9,777.47 (session +81.56) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $272.46 | ▲ 09:30 equity $9,849.39 vs yday $9,828.89 (+20.50) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `AVTR` | 108 | $15.87 | $2.35 | $+88.22 | $1,984.08 | ▲ +88.22 after sell → book $9,847.05; vs 09:30 mark -2.34 | dropped from list after 5 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 9 | $182.33 | $2.04 | $+99.26 | $3,623.01 | ▲ +99.26 after sell → book $9,845.01; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `JBHT` | 6 | $236.80 | $2.03 | $-14.84 | $5,041.78 | ▼ -14.84 after sell → book $9,842.98; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `TNDM` | 90 | $17.13 | $2.29 | $-57.65 | $6,581.19 | ▼ -57.65 after sell → book $9,840.69; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `BAK` | 910 | $1.77 | $11.90 | $-23.64 | $8,179.99 | ▼ -23.64 after sell → book $9,828.79; vs 09:30 mark -11.90 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 97 | $20.91 | $2.28 | — | $6,149.44 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $2045.00 | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 138 | $14.79 | $2.40 | — | $4,106.01 | — | prior-export headline🟢 only; gate headline=good; rank cond; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $2045.00 | — |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 145 | $14.07 | $2.42 | — | $2,063.44 | — | prior-export headline🟢 only; gate headline=good; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $2045.00 | — |
| 2026-09-18 09:30 ET | **BUY** | `FLNC` | 271 | $7.54 | $3.50 | — | $17.96 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_mover; 🔵; ret5=-20.9; leftover $2045.00 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17.96 | ▼ close $9,664.47 vs 09:30 $9,849.39 (session -153.71) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17.96 | ▲ 09:30 equity $9,780.27 vs yday $9,664.47 (+115.80) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `GME` | 72 | $22.78 | $2.23 | $+43.08 | $1,655.89 | ▲ +43.08 after sell → book $9,778.04; vs 09:30 mark -2.23 | dropped from list after 2 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TH` | 97 | $21.65 | $2.31 | $+67.19 | $3,753.62 | ▲ +67.19 after sell → book $9,775.72; vs 09:30 mark -2.32 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RARE` | 138 | $14.58 | $2.44 | $-33.83 | $5,763.22 | ▼ -33.83 after sell → book $9,773.28; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `BHVN` | 145 | $13.90 | $2.47 | $-29.54 | $7,776.26 | ▼ -29.54 after sell → book $9,770.82; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `FLNC` | 271 | $7.36 | $3.56 | $-54.48 | $9,767.26 | ▼ -54.48 after sell → book $9,767.26; vs 09:30 mark -3.56 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `VICR` | 7 | $230.25 | $2.01 | — | $8,153.50 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; ret5=+12.5; leftover $1627.88 | — |
| 2026-09-21 09:30 ET | **BUY** | `SMTC` | 8 | $190.30 | $2.01 | — | $6,629.08 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; ret5=+10.6; leftover $1627.88 | — |
| 2026-09-21 09:30 ET | **BUY** | `GLXY` | 62 | $25.95 | $2.18 | — | $5,018.01 | — | prior-export headline🟢 only; gate headline=good; rank cond; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $1627.88 | — |
| 2026-09-21 09:30 ET | **BUY** | `MARA` | 116 | $13.94 | $2.34 | — | $3,398.63 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+10.5; leftover $1627.88 | — |
| 2026-09-21 09:30 ET | **BUY** | `SION` | 271 | $6.00 | $3.50 | — | $1,769.13 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_mover; ret5=-24.1; leftover $1627.88 | — |
| 2026-09-21 09:30 ET | **BUY** | `AMTX` | 757 | $2.15 | $9.77 | — | $131.82 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+7.5; leftover $1627.88 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $131.82 | ▼ close $9,483.03 vs 09:30 $9,780.27 (session -262.43) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $131.82 | ▼ 09:30 equity $9,462.92 vs yday $9,483.03 (-20.11) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `MARA` | 116 | $13.13 | $2.37 | $-98.67 | $1,652.53 | ▼ -98.67 after sell → book $9,460.55; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `SION` | 271 | $5.99 | $3.55 | $-9.76 | $3,272.27 | ▼ -9.76 after sell → book $9,457.00; vs 09:30 mark -3.55 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **BUY** | `MRNA` | 3 | $168.50 | $2.00 | — | $2,764.77 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; ret5=+17.9; leftover $545.38 | — |
| 2026-09-22 09:30 ET | **BUY** | `IVVD` | 539 | $1.01 | $6.95 | — | $2,213.42 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,yday_mover; ret5=+14.3; leftover $545.38 | — |
| 2026-09-22 09:30 ET | **BUY** | `DGXX` | 126 | $4.30 | $2.37 | — | $1,669.25 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; ret5=+16.9; leftover $545.38 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,669.25 | ▲ close $9,452.98 vs 09:30 $9,462.92 (session +7.31) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,669.25 | ▲ 09:30 equity $9,764.91 vs yday $9,452.98 (+311.93) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `VICR` | 7 | $266.50 | $2.04 | $+249.70 | $3,532.72 | ▲ +249.70 after sell → book $9,762.87; vs 09:30 mark -2.04 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `SMTC` | 8 | $174.50 | $2.04 | $-130.45 | $4,926.68 | ▼ -130.45 after sell → book $9,760.84; vs 09:30 mark -2.03 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `GLXY` | 62 | $26.58 | $2.20 | $+34.68 | $6,572.44 | ▲ +34.68 after sell → book $9,758.64; vs 09:30 mark -2.20 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `AMTX` | 757 | $2.09 | $9.90 | $-65.09 | $8,144.67 | ▼ -65.09 after sell → book $9,748.74; vs 09:30 mark -9.90 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `MRNA` | 3 | $183.41 | $2.02 | $+40.70 | $8,692.87 | ▲ +40.70 after sell → book $9,746.72; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `IVVD` | 539 | $0.95 | $6.84 | $-46.13 | $9,198.08 | ▼ -46.13 after sell → book $9,739.88; vs 09:30 mark -6.84 | dropped from list after 1 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `PGEN` | 231 | $7.95 | $2.98 | — | $7,358.65 | — | prior-export headline🟢 only; gate headline=good; rank cond; list flatten; 🔵; ⚪; ret5=+12.4; leftover $1839.62 | — |
| 2026-09-23 09:30 ET | **BUY** | `SGRY` | 117 | $15.72 | $2.34 | — | $5,517.07 | — | prior-export headline🟢 only; gate headline=good; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $1839.62 | — |
| 2026-09-23 09:30 ET | **BUY** | `VERI` | 1415 | $1.30 | $18.25 | — | $3,659.32 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ret5=+15.3; leftover $1839.62 | — |
| 2026-09-23 09:30 ET | **BUY** | `CMPX` | 1507 | $1.22 | $19.44 | — | $1,801.34 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_mover; 🔵; ret5=-33.0; leftover $1839.62 | — |
| 2026-09-23 09:30 ET | **BUY** | `BLSH` | 44 | $40.00 | $2.12 | — | $39.21 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ret5=+6.7; leftover $1839.62 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $39.21 | ▼ close $9,346.47 vs 09:30 $9,764.91 (session -348.27) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $39.21 | ▼ 09:30 equity $9,233.69 vs yday $9,346.47 (-112.78) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `DGXX` | 126 | $4.12 | $2.40 | $-27.45 | $555.93 | ▼ -27.45 after sell → book $9,231.29; vs 09:30 mark -2.40 | dropped from list after 2 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 231 | $7.38 | $3.03 | $-137.68 | $2,257.68 | ▼ -137.68 after sell → book $9,228.26; vs 09:30 mark -3.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SGRY` | 117 | $14.38 | $2.37 | $-161.49 | $3,937.77 | ▼ -161.49 after sell → book $9,225.89; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `VERI` | 1415 | $1.27 | $18.50 | $-79.21 | $5,716.32 | ▼ -79.21 after sell → book $9,207.39; vs 09:30 mark -18.50 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `CMPX` | 1507 | $1.17 | $19.70 | $-114.49 | $7,459.80 | ▼ -114.49 after sell → book $9,187.68; vs 09:30 mark -19.71 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `BLSH` | 44 | $39.27 | $2.15 | $-36.39 | $9,185.54 | ▼ -36.39 after sell → book $9,185.54; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,185.54 | ▲ close $9,185.54 vs 09:30 $9,233.69 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,756.96 | ▲ 09:30 equity $8,756.96 vs yday $8,756.96 (+0.00) | 09:30 open · cash $8,756.96 · no holdings · equity $8,756.96 vs prior close $8,756.96 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 09:30 ET | **BUY** | `ZSQR` | 567 | $3.86 | $7.31 | — | $6,561.03 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; leftover $2189.24 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ILMN` | 8 | $272.16 | $2.01 | — | $4,381.73 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+11.7; leftover $2189.24 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 135 | $16.21 | $2.40 | — | $2,190.99 | — | prior-export headline🟢 only; gate headline=good; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; leftover $2189.24 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `RKLB` | 29 | $74.15 | $2.08 | — | $38.56 | — | prior-export headline🟢 only; gate headline=good; rank cond; list ohlc_hot; ret5=+8.5; leftover $2189.24 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $38.56 | ▼ close $8,640.97 vs 09:30 $8,756.96 (session -102.19) | 16:00 close · cash $38.56 · equity $8,640.97 vs 09:30 $8,756.96 (-115.99; session marks -102.19) · 4 name(s) marked open→close (per-name table). ZSQR×567 09:30 $3.86 → close $3.78 -45.36; ILMN×8 09:30 $272.16 → close $270.00 -17.28; SECZ×135 09:30 $16.21 → close $15.96 -33.75; RKLB×29 09:30 $74.15 → close $73.95 -5.80 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `SNDK` | cash | leftover split 1250.00 < 1 share @ 1646.93 |
| 2026-08-18 | `ZLAB` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AUTL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `GO` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HTHT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `IOVA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ANGX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `REAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ANGX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KLAR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `ABUS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `NVAX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TRLV` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-27 | `ASML` | cash | leftover split 644.81 < 1 share @ 1746.53 |
| 2026-08-31 | `HAL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RBRK` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MRNA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `DVN` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NTNX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `PANW` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SLBT` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CTMX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CNXC` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TRLV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `BCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `EOLS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `RLMD` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRNM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `AVXL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `DE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TEL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `POWW` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `JCI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `TH` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `DFDV` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CHYM` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `WLTH` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ASO` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `AVTR` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `GME` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `TXG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `SLS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TXG` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-22 | `VICR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `SMTC` | no_price | no 09:30 open — carry |
| 2026-09-22 | `GLXY` | no_price | no 09:30 open — carry |
| 2026-09-22 | `AMTX` | no_price | no 09:30 open — carry |
| 2026-09-22 | `WBD` | no_price | no 09:30 open |
| 2026-09-22 | `GRAL` | no_price | no 09:30 open |
| 2026-09-22 | `YSS` | no_price | no 09:30 open |
| 2026-09-24 | `OKTA` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ADCT` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ZSQR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `AMPL` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `NNBR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `NEWP` | hard_red | hard-red S=-7.66 sit; no new buys |
