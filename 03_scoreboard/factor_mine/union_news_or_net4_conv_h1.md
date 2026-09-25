# Factor mine action — `union_news_or_net4_conv_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 4 · rank `cond` · size `conviction` · sell `list` · S-boost `none` · OR news + net≥4; 70% leftover if #1 net ≥ 5

Cash book **-21.24%** ($7,876) · signal-only (no cash/fees) was -7.44%. Starts YES **12/30**. Fills 97 · skips 31 · realized $+852.81.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 4 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Sort: how many morning cameras are green vs red.
- Must-have: the morning news packet OR the prior-export headline is green.
- Must-have: camera net (+G −R) is at least 4.
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is allowed only when every must-have is true.
- A name is thrown out if any must-not is true.
- Sort the keepers by how many morning cameras are green vs red and keep the top 4.
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
- **Gate** `news_or_headline=True,cam_net_min=4` · **rank** `cond` · **top_n** 4.
- **Size** `conviction` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,852.83.

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
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 531 | $13.18 | $6.85 | — | $2,994.57 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $7000.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 232 | $4.31 | $2.99 | — | $1,991.66 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1000.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 51 | $19.57 | $2.14 | — | $991.44 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1000.00 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $991.44 | ▲ close $10,395.38 vs 09:30 $10,000.00 (session +407.37) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $991.44 | ▲ 09:30 equity $10,405.75 vs yday $10,395.38 (+10.37) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `HLIT` | 531 | $13.84 | $7.00 | $+336.61 | $8,333.49 | ▲ +336.61 after sell → book $10,398.76; vs 09:30 mark -6.99 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 232 | $4.60 | $3.04 | $+61.25 | $9,397.65 | ▲ +61.25 after sell → book $10,395.72; vs 09:30 mark -3.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 51 | $19.57 | $2.16 | $-4.31 | $10,393.55 | ▼ -4.31 after sell → book $10,393.55; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 112 | $46.18 | $2.33 | — | $5,219.07 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten; 🔵; ret5=+6.7; leftover $5196.78 | — |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 24 | $142.77 | $2.06 | — | $1,790.53 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten; 🔵; ret5=+5.8; leftover $3464.52 | — |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 8 | $202.70 | $2.01 | — | $166.91 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten; 🔵; ret5=+8.3; leftover $1732.26 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $166.91 | ▲ close $10,652.67 vs 09:30 $10,405.75 (session +265.52) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $166.91 | ▲ 09:30 equity $10,767.31 vs yday $10,652.67 (+114.64) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 112 | $48.00 | $2.39 | $+199.13 | $5,540.52 | ▲ +199.13 after sell → book $10,764.92; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 24 | $148.04 | $2.10 | $+122.32 | $9,091.38 | ▲ +122.32 after sell → book $10,762.82; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 8 | $208.93 | $2.04 | $+45.79 | $10,760.79 | ▲ +45.79 after sell → book $10,760.79; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,760.79 | ▲ close $10,760.79 vs 09:30 $10,767.31 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,760.79 | ▲ 09:30 equity $10,760.79 vs yday $10,760.79 (-0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,760.79 | ▲ close $10,760.79 vs 09:30 $10,760.79 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,760.79 | ▲ 09:30 equity $10,760.79 vs yday $10,760.79 (-0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 82 | $91.01 | $2.24 | — | $3,295.73 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $7532.55 | — |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 24 | $44.76 | $2.06 | — | $2,219.43 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+8.7; leftover $1076.08 | — |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 435 | $2.47 | $5.61 | — | $1,139.37 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $1076.08 | — |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 18 | $58.73 | $2.04 | — | $80.18 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.8; leftover $1076.08 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $80.18 | ▲ close $10,939.46 vs 09:30 $10,760.79 (session +190.63) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $80.18 | ▲ 09:30 equity $11,147.11 vs yday $10,939.46 (+207.65) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 82 | $95.72 | $2.31 | $+381.67 | $7,926.91 | ▲ +381.67 after sell → book $11,144.80; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `APA` | 24 | $44.52 | $2.08 | $-9.90 | $8,993.31 | ▼ -9.90 after sell → book $11,142.72; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 52 | $119.43 | $2.15 | — | $2,780.80 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $6295.32 | — |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 23 | $115.18 | $2.06 | — | $129.60 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $2697.99 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $129.60 | ▲ close $11,396.11 vs 09:30 $11,147.11 (session +257.60) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $129.60 | ▼ 09:30 equity $11,280.62 vs yday $11,396.11 (-115.49) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 435 | $2.40 | $5.69 | $-41.76 | $1,167.91 | ▼ -41.76 after sell → book $11,274.93; vs 09:30 mark -5.69 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 52 | $120.51 | $2.21 | $+51.81 | $7,432.22 | ▲ +51.81 after sell → book $11,272.72; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 23 | $121.00 | $2.09 | $+129.71 | $10,213.13 | ▲ +129.71 after sell → book $11,270.63; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,213.13 | ▼ close $11,240.48 vs 09:30 $11,280.62 (session -30.15) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,213.13 | ▲ 09:30 equity $11,255.87 vs yday $11,240.48 (+15.39) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 18 | $57.93 | $2.06 | $-18.51 | $11,253.81 | ▼ -18.51 after sell → book $11,253.81; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 66 | $118.52 | $2.19 | — | $3,429.30 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $7877.67 | — |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 21 | $77.13 | $2.05 | — | $1,807.52 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $1688.07 | — |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 48 | $35.05 | $2.13 | — | $122.98 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer; 🔵; ⚪; ret5=+19.7; leftover $1688.07 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $122.98 | ▲ close $11,635.87 vs 09:30 $11,255.87 (session +388.44) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $122.98 | ▼ 09:30 equity $11,409.52 vs yday $11,635.87 (-226.35) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 66 | $119.80 | $2.26 | $+80.03 | $8,027.52 | ▲ +80.03 after sell → book $11,407.26; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 21 | $79.34 | $2.08 | $+42.28 | $9,691.58 | ▲ +42.28 after sell → book $11,405.18; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EZPW` | 48 | $35.70 | $2.16 | $+26.91 | $11,403.03 | ▲ +26.91 after sell → book $11,403.03; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `FNV` | 29 | $267.02 | $2.08 | — | $3,657.37 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ret5=+16.7; leftover $7982.12 | — |
| 2026-08-26 09:30 ET | **BUY** | `CM` | 28 | $118.50 | $2.07 | — | $337.30 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list overnight,overnight_mega; 🔵; ret5=-2.7; leftover $3420.91 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $337.30 | ▲ close $11,400.63 vs 09:30 $11,409.52 (session +1.75) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $337.30 | ▲ 09:30 equity $11,412.53 vs yday $11,400.63 (+11.90) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `FNV` | 29 | $267.23 | $2.15 | $+1.86 | $8,084.82 | ▲ +1.86 after sell → book $11,410.38; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 69 | $81.65 | $2.20 | — | $2,448.77 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+2.0; leftover $5659.37 | — |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $967.01 | $1.99 | — | $1,479.77 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+0.1; leftover $1212.72 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,479.77 | ▼ close $11,184.49 vs 09:30 $11,412.53 (session -221.70) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,479.77 | ▼ 09:30 equity $11,107.17 vs yday $11,184.49 (-77.32) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `CM` | 28 | $115.66 | $2.11 | $-83.70 | $4,716.14 | ▼ -83.70 after sell → book $11,105.06; vs 09:30 mark -2.11 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 69 | $79.27 | $2.25 | $-168.67 | $10,183.51 | ▼ -168.67 after sell → book $11,102.80; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $919.29 | $2.01 | $-51.73 | $11,100.79 | ▼ -51.73 after sell → book $11,100.79; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 23 | $324.41 | $2.06 | — | $3,637.30 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $7770.55 | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 7 | $141.76 | $2.01 | — | $2,642.97 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1110.08 | — |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 2 | $400.42 | $2.00 | — | $1,840.14 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1110.08 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,840.14 | ▼ close $10,874.52 vs 09:30 $11,107.17 (session -220.21) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,840.14 | ▲ 09:30 equity $10,940.39 vs yday $10,874.52 (+65.87) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 23 | $322.49 | $2.13 | $-48.35 | $9,255.28 | ▼ -48.35 after sell → book $10,938.26; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 7 | $132.30 | $2.03 | $-70.26 | $10,179.35 | ▼ -70.26 after sell → book $10,936.23; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CIEN` | 2 | $378.44 | $2.02 | $-47.97 | $10,934.21 | ▼ -47.97 after sell → book $10,934.21; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,934.21 | ▲ close $10,934.21 vs 09:30 $10,940.39 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,934.21 | ▲ 09:30 equity $10,934.21 vs yday $10,934.21 (+0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,934.21 | ▲ close $10,934.21 vs 09:30 $10,934.21 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,934.21 | ▲ 09:30 equity $10,934.21 vs yday $10,934.21 (+0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,934.21 | ▲ close $10,934.21 vs 09:30 $10,934.21 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,934.21 | ▲ 09:30 equity $10,934.21 vs yday $10,934.21 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 21 | $351.74 | $2.05 | — | $3,545.62 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $7653.95 | — |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 2 | $486.31 | $2.00 | — | $2,571.00 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+6.1; leftover $1093.42 | — |
| 2026-09-03 09:30 ET | **BUY** | `CXW` | 33 | $32.31 | $2.09 | — | $1,502.68 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ⚪; ret5=-4.1; leftover $1093.42 | — |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 68 | $15.87 | $2.19 | — | $421.33 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1093.42 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $421.33 | ▲ close $11,214.45 vs 09:30 $10,934.21 (session +288.57) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $421.33 | ▲ 09:30 equity $11,221.97 vs yday $11,214.45 (+7.52) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 21 | $359.70 | $2.12 | $+162.98 | $7,972.90 | ▲ +162.98 after sell → book $11,219.84; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DELL` | 2 | $513.78 | $2.02 | $+50.93 | $8,998.45 | ▲ +50.93 after sell → book $11,217.83; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CXW` | 33 | $33.46 | $2.11 | $+33.75 | $10,100.52 | ▲ +33.75 after sell → book $11,215.72; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 26 | $263.36 | $2.07 | — | $3,251.09 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $7070.36 | — |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 20 | $75.65 | $2.05 | — | $1,736.04 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+6.0; leftover $1515.08 | — |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 6 | $236.82 | $2.01 | — | $313.11 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; ret5=+8.1; leftover $1515.08 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $313.11 | ▲ close $11,244.79 vs 09:30 $11,221.97 (session +35.20) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $313.11 | ▼ 09:30 equity $11,231.51 vs yday $11,244.79 (-13.28) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 68 | $16.74 | $2.22 | $+54.75 | $1,449.22 | ▲ +54.75 after sell → book $11,229.30; vs 09:30 mark -2.21 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 26 | $253.72 | $2.13 | $-254.84 | $8,043.81 | ▼ -254.84 after sell → book $11,227.17; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MRX` | 20 | $78.84 | $2.07 | $+59.68 | $9,618.54 | ▲ +59.68 after sell → book $11,225.10; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BE` | 6 | $267.76 | $2.03 | $+181.60 | $11,223.06 | ▲ +181.60 after sell → book $11,223.06; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,223.06 | ▲ close $11,223.06 vs 09:30 $11,231.51 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,223.06 | ▲ 09:30 equity $11,223.06 vs yday $11,223.06 (+0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,223.06 | ▲ close $11,223.06 vs 09:30 $11,223.06 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,223.06 | ▲ 09:30 equity $11,223.06 vs yday $11,223.06 (+0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,223.06 | ▲ close $11,223.06 vs 09:30 $11,223.06 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,223.06 | ▲ 09:30 equity $11,223.06 vs yday $11,223.06 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 68 | $164.43 | $2.19 | — | $39.63 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $11223.06 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $39.63 | ▼ close $10,258.67 vs 09:30 $11,223.06 (session -962.20) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $39.63 | ▼ 09:30 equity $9,656.19 vs yday $10,258.67 (-602.48) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 68 | $141.42 | $2.28 | $-1569.16 | $9,653.91 | ▼ -1,569.16 after sell → book $9,653.91; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,653.91 | ▲ close $9,653.91 vs 09:30 $9,656.19 (session +0.00) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,653.91 | ▲ 09:30 equity $9,653.91 vs yday $9,653.91 (-0.00) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,653.91 | ▲ close $9,653.91 vs 09:30 $9,653.91 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,653.91 | ▲ 09:30 equity $9,653.91 vs yday $9,653.91 (-0.00) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 257 | $26.27 | $3.32 | — | $2,899.20 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer; 🔵; ret5=+10.0; leftover $6757.74 | — |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 7 | $189.17 | $2.01 | — | $1,573.00 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ret5=+7.9; leftover $1448.09 | — |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 36 | $39.99 | $2.10 | — | $131.26 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ret5=+9.3; leftover $1448.09 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $131.26 | ▼ close $9,632.53 vs 09:30 $9,653.91 (session -13.95) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $131.26 | ▼ 09:30 equity $9,629.30 vs yday $9,632.53 (-3.23) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `WAY` | 257 | $26.51 | $3.41 | $+54.95 | $6,940.92 | ▲ +54.95 after sell → book $9,625.89; vs 09:30 mark -3.41 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `QCOM` | 7 | $190.35 | $2.03 | $+4.22 | $8,271.34 | ▲ +4.22 after sell → book $9,623.86; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SM` | 36 | $37.57 | $2.12 | $-91.34 | $9,621.74 | ▼ -91.34 after sell → book $9,621.74; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 39 | $170.85 | $2.11 | — | $2,956.48 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $6735.22 | — |
| 2026-09-17 09:30 ET | **BUY** | `AVTR` | 60 | $15.81 | $2.17 | — | $2,005.71 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ret5=+4.9; leftover $962.17 | — |
| 2026-09-17 09:30 ET | **BUY** | `GME` | 43 | $22.12 | $2.12 | — | $1,052.43 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ret5=+10.5; leftover $962.17 | — |
| 2026-09-17 09:30 ET | **BUY** | `JBHT` | 4 | $238.60 | $2.00 | — | $96.03 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_mover; ret5=-11.6; leftover $962.17 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $96.03 | ▲ close $9,923.35 vs 09:30 $9,629.30 (session +310.01) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $96.03 | ▲ 09:30 equity $10,091.00 vs yday $9,923.35 (+167.65) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 39 | $182.33 | $2.17 | $+443.44 | $7,204.73 | ▲ +443.44 after sell → book $10,088.83; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `AVTR` | 60 | $15.87 | $2.19 | $-0.76 | $8,154.74 | ▼ -0.76 after sell → book $10,086.64; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `JBHT` | 4 | $236.80 | $2.02 | $-11.22 | $9,099.92 | ▼ -11.22 after sell → book $10,084.62; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 304 | $20.91 | $3.92 | — | $2,739.36 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $6369.94 | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 92 | $14.79 | $2.27 | — | $1,376.41 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $1364.99 | — |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 97 | $14.07 | $2.28 | — | $9.34 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $1364.99 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9.34 | ▲ close $10,080.68 vs 09:30 $10,091.00 (session +4.53) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9.34 | ▲ 09:30 equity $10,260.14 vs yday $10,080.68 (+179.46) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `GME` | 43 | $22.78 | $2.14 | $+24.12 | $986.74 | ▲ +24.12 after sell → book $10,258.00; vs 09:30 mark -2.14 | dropped from list after 2 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TH` | 304 | $21.65 | $4.02 | $+217.01 | $7,564.31 | ▲ +217.01 after sell → book $10,253.97; vs 09:30 mark -4.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RARE` | 92 | $14.58 | $2.29 | $-23.88 | $8,903.38 | ▼ -23.88 after sell → book $10,251.68; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `BHVN` | 97 | $13.90 | $2.31 | $-21.08 | $10,249.37 | ▼ -21.08 after sell → book $10,249.37; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `VICR` | 31 | $230.25 | $2.08 | — | $3,109.54 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; ret5=+12.5; leftover $7174.56 | — |
| 2026-09-21 09:30 ET | **BUY** | `SMTC` | 8 | $190.30 | $2.01 | — | $1,585.13 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; ret5=+10.6; leftover $1537.41 | — |
| 2026-09-21 09:30 ET | **BUY** | `GLXY` | 59 | $25.95 | $2.17 | — | $51.91 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $1537.41 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $51.91 | ▼ close $9,949.90 vs 09:30 $10,260.14 (session -293.21) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $51.91 | ▲ 09:30 equity $9,949.90 vs yday $9,949.90 (+0.00) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $51.91 | ▲ close $9,949.90 vs 09:30 $9,949.90 (session +0.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $51.91 | ▲ 09:30 equity $11,277.63 vs yday $9,949.90 (+1,327.73) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `VICR` | 31 | $266.50 | $2.16 | $+1119.51 | $8,311.25 | ▲ +1,119.51 after sell → book $11,275.47; vs 09:30 mark -2.16 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `SMTC` | 8 | $174.50 | $2.04 | $-130.45 | $9,705.22 | ▼ -130.45 after sell → book $11,273.44; vs 09:30 mark -2.03 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `GLXY` | 59 | $26.58 | $2.19 | $+32.81 | $11,271.25 | ▲ +32.81 after sell → book $11,271.25; vs 09:30 mark -2.19 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `CTAS` | 40 | $196.78 | $2.11 | — | $3,397.94 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list earn_react; 🔵; ⚪; ret5=-2.0; leftover $7889.87 | — |
| 2026-09-23 09:30 ET | **BUY** | `PGEN` | 141 | $7.95 | $2.41 | — | $2,274.57 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten; 🔵; ⚪; ret5=+12.4; leftover $1127.12 | — |
| 2026-09-23 09:30 ET | **BUY** | `SGRY` | 71 | $15.72 | $2.20 | — | $1,156.25 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $1127.12 | — |
| 2026-09-23 09:30 ET | **BUY** | `VERI` | 867 | $1.30 | $11.18 | — | $17.97 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ret5=+15.3; leftover $1127.12 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $17.97 | ▼ close $10,898.00 vs 09:30 $11,277.63 (session -355.34) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $17.97 | ▼ 09:30 equity $10,871.02 vs yday $10,898.00 (-26.98) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `CTAS` | 40 | $192.26 | $2.18 | $-185.09 | $7,706.18 | ▼ -185.09 after sell → book $10,868.83; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 141 | $7.38 | $2.45 | $-85.23 | $8,744.32 | ▼ -85.23 after sell → book $10,866.39; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SGRY` | 71 | $14.38 | $2.22 | $-99.57 | $9,763.07 | ▼ -99.57 after sell → book $10,864.16; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `VERI` | 867 | $1.27 | $11.34 | $-48.53 | $10,852.83 | ▼ -48.53 after sell → book $10,852.83; vs 09:30 mark -11.33 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,852.83 | ▲ close $10,852.83 vs 09:30 $10,871.02 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,032.11 | ▲ 09:30 equity $8,032.11 vs yday $8,032.11 (+0.00) | 09:30 open · cash $8,032.11 · no holdings · equity $8,032.11 vs prior close $8,032.11 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 09:30 ET | **BUY** | `ZSQR` | 1456 | $3.86 | $18.78 | — | $2,393.17 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; leftover $5622.48 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ILMN` | 2 | $272.16 | $2.00 | — | $1,846.85 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+11.7; leftover $803.21 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 49 | $16.21 | $2.14 | — | $1,050.42 | — | OR news + net≥4; 70% leftover if #1 net ≥ 5; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; leftover $803.21 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,050.42 | ▼ close $7,876.14 vs 09:30 $8,032.11 (session -133.05) | 16:00 close · cash $1,050.42 · equity $7,876.14 vs 09:30 $8,032.11 (-155.97; session marks -133.05) · 3 name(s) marked open→close (per-name table). ZSQR×1456 09:30 $3.86 → close $3.78 -116.48; ILMN×2 09:30 $272.16 → close $270.00 -4.32; SECZ×49 09:30 $16.21 → close $15.96 -12.25 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `SNDK` | cash | leftover split 1000.00 < 1 share @ 1646.93 |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ABUS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-27 | `ASML` | cash | leftover split 1212.72 < 1 share @ 1746.53 |
| 2026-08-28 | `MPWR` | cash | leftover split 1110.08 < 1 share @ 1306.03 |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CDNS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `HAL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `NVDA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DVN` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `BCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `EOLS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `LITE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `HPE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `BE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-22 | `VICR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `SMTC` | no_price | no 09:30 open — carry |
| 2026-09-22 | `GLXY` | no_price | no 09:30 open — carry |
| 2026-09-24 | `OKTA` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ACMR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `EOG` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ADCT` | hard_red | hard-red S=-7.66 sit; no new buys |
