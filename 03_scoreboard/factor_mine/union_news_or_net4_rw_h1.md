# Factor mine action — `union_news_or_net4_rw_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 4 · rank `cond` · size `rank_w` · sell `list` · S-boost `none` · OR news + net≥4; leftover weighted by camera rank

Cash book **-22.01%** ($7,799) · signal-only (no cash/fees) was -7.44%. Starts YES **5/30**. Fills 99 · skips 30 · realized $-225.56.

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
- Split leftover cash by rank (first name gets the biggest slice).
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
- **Size** `rank_w` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,774.46.

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
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 303 | $13.18 | $3.91 | — | $6,002.55 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $4000.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `SNDK` | 1 | $1646.93 | $1.99 | — | $4,353.63 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.4; leftover $3000.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 464 | $4.31 | $5.99 | — | $2,347.80 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $2000.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 51 | $19.57 | $2.14 | — | $1,347.59 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1000.00 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,347.59 | ▲ close $10,232.72 vs 09:30 $10,000.00 (session +246.75) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,347.59 | ▲ 09:30 equity $10,374.32 vs yday $10,232.72 (+141.60) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `HLIT` | 303 | $13.84 | $3.99 | $+192.08 | $5,537.12 | ▲ +192.08 after sell → book $10,370.33; vs 09:30 mark -3.99 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SNDK` | 1 | $1700.74 | $2.02 | $+49.81 | $7,235.85 | ▲ +49.81 after sell → book $10,368.32; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 464 | $4.60 | $6.08 | $+122.49 | $9,364.17 | ▲ +122.49 after sell → book $10,362.24; vs 09:30 mark -6.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 51 | $19.57 | $2.16 | $-4.31 | $10,360.07 | ▼ -4.31 after sell → book $10,360.07; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 112 | $46.18 | $2.33 | — | $5,185.59 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten; 🔵; ret5=+6.7; leftover $5180.04 | — |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 24 | $142.77 | $2.06 | — | $1,757.04 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten; 🔵; ret5=+5.8; leftover $3453.36 | — |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 8 | $202.70 | $2.01 | — | $133.43 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten; 🔵; ret5=+8.3; leftover $1726.68 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $133.43 | ▲ close $10,619.19 vs 09:30 $10,374.32 (session +265.52) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $133.43 | ▲ 09:30 equity $10,733.83 vs yday $10,619.19 (+114.64) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 112 | $48.00 | $2.39 | $+199.13 | $5,507.04 | ▲ +199.13 after sell → book $10,731.44; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 24 | $148.04 | $2.10 | $+122.32 | $9,057.90 | ▲ +122.32 after sell → book $10,729.34; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 8 | $208.93 | $2.04 | $+45.79 | $10,727.31 | ▲ +45.79 after sell → book $10,727.31; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,727.31 | ▲ close $10,727.31 vs 09:30 $10,733.83 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,727.31 | ▲ 09:30 equity $10,727.31 vs yday $10,727.31 (-0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,727.31 | ▲ close $10,727.31 vs 09:30 $10,727.31 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,727.31 | ▲ 09:30 equity $10,727.31 vs yday $10,727.31 (-0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 47 | $91.01 | $2.13 | — | $6,447.70 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $4290.92 | — |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 71 | $44.76 | $2.20 | — | $3,267.54 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+8.7; leftover $3218.19 | — |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 868 | $2.47 | $11.20 | — | $1,112.38 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $2145.46 | — |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 18 | $58.73 | $2.04 | — | $53.20 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.8; leftover $1072.73 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $53.20 | ▲ close $10,786.94 vs 09:30 $10,727.31 (session +77.21) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $53.20 | ▲ 09:30 equity $10,931.88 vs yday $10,786.94 (+144.94) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 47 | $95.72 | $2.18 | $+217.06 | $4,549.86 | ▲ +217.06 after sell → book $10,929.70; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `APA` | 71 | $44.52 | $2.24 | $-21.48 | $7,708.54 | ▼ -21.48 after sell → book $10,927.46; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 43 | $119.43 | $2.12 | — | $2,570.93 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $5139.03 | — |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 22 | $115.18 | $2.06 | — | $34.92 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $2569.51 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $34.92 | ▲ close $11,130.34 vs 09:30 $10,931.88 (session +207.05) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $34.92 | ▼ 09:30 equity $11,019.55 vs yday $11,130.34 (-110.79) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 868 | $2.40 | $11.36 | $-83.32 | $2,106.76 | ▼ -83.32 after sell → book $11,008.19; vs 09:30 mark -11.36 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 43 | $120.51 | $2.17 | $+42.15 | $7,286.52 | ▲ +42.15 after sell → book $11,006.02; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 22 | $121.00 | $2.09 | $+123.90 | $9,946.43 | ▲ +123.90 after sell → book $11,003.93; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,946.43 | ▼ close $10,973.78 vs 09:30 $11,019.55 (session -30.15) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,946.43 | ▲ 09:30 equity $10,989.17 vs yday $10,973.78 (+15.39) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 18 | $57.93 | $2.06 | $-18.51 | $10,987.11 | ▼ -18.51 after sell → book $10,987.11; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 46 | $118.52 | $2.13 | — | $5,533.06 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $5493.55 | — |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 47 | $77.13 | $2.13 | — | $1,905.82 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $3662.37 | — |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 52 | $35.05 | $2.15 | — | $81.07 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer; 🔵; ⚪; ret5=+19.7; leftover $1831.18 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $81.07 | ▲ close $11,344.74 vs 09:30 $10,989.17 (session +364.04) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $81.07 | ▼ 09:30 equity $11,177.25 vs yday $11,344.74 (-167.49) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 46 | $119.80 | $2.18 | $+54.57 | $5,589.69 | ▲ +54.57 after sell → book $11,175.07; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 47 | $79.34 | $2.17 | $+99.57 | $9,316.50 | ▲ +99.57 after sell → book $11,172.90; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EZPW` | 52 | $35.70 | $2.17 | $+29.48 | $11,170.73 | ▲ +29.48 after sell → book $11,170.73; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `FNV` | 27 | $267.02 | $2.07 | — | $3,959.12 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ret5=+16.7; leftover $7447.15 | — |
| 2026-08-26 09:30 ET | **BUY** | `CM` | 31 | $118.50 | $2.08 | — | $283.54 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list overnight,overnight_mega; 🔵; ret5=-2.7; leftover $3723.58 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $283.54 | ▲ close $11,166.73 vs 09:30 $11,177.25 (session +0.15) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $283.54 | ▲ 09:30 equity $11,180.62 vs yday $11,166.73 (+13.89) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `FNV` | 27 | $267.23 | $2.14 | $+1.46 | $7,496.61 | ▲ +1.46 after sell → book $11,178.48; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 45 | $81.65 | $2.12 | — | $3,820.23 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+2.0; leftover $3748.30 | — |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 2 | $967.01 | $2.00 | — | $1,884.22 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+0.1; leftover $2498.87 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,884.22 | ▼ close $10,937.09 vs 09:30 $11,180.62 (session -237.27) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,884.22 | ▼ 09:30 equity $10,875.41 vs yday $10,937.09 (-61.68) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `CM` | 31 | $115.66 | $2.12 | $-92.24 | $5,467.55 | ▼ -92.24 after sell → book $10,873.28; vs 09:30 mark -2.13 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 45 | $79.27 | $2.16 | $-111.39 | $9,032.54 | ▼ -111.39 after sell → book $10,871.12; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 2 | $919.29 | $2.02 | $-99.46 | $10,869.10 | ▼ -99.46 after sell → book $10,869.10; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 13 | $324.41 | $2.03 | — | $6,649.74 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $4347.64 | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 23 | $141.76 | $2.06 | — | $3,387.20 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $3260.73 | — |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 5 | $400.42 | $2.00 | — | $1,383.10 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $2173.82 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,383.10 | ▼ close $10,451.82 vs 09:30 $10,875.41 (session -411.19) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,383.10 | ▲ 09:30 equity $10,510.57 vs yday $10,451.82 (+58.75) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 13 | $322.49 | $2.07 | $-29.06 | $5,573.39 | ▼ -29.06 after sell → book $10,508.49; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 23 | $132.30 | $2.09 | $-221.73 | $8,614.20 | ▼ -221.73 after sell → book $10,506.40; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CIEN` | 5 | $378.44 | $2.03 | $-113.94 | $10,504.37 | ▼ -113.94 after sell → book $10,504.37; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,504.37 | ▲ close $10,504.37 vs 09:30 $10,510.57 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,504.37 | ▲ 09:30 equity $10,504.37 vs yday $10,504.37 (+0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,504.37 | ▲ close $10,504.37 vs 09:30 $10,504.37 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,504.37 | ▲ 09:30 equity $10,504.37 vs yday $10,504.37 (+0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,504.37 | ▲ close $10,504.37 vs 09:30 $10,504.37 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,504.37 | ▲ 09:30 equity $10,504.37 vs yday $10,504.37 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 11 | $351.74 | $2.02 | — | $6,633.21 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $4201.75 | — |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 6 | $486.31 | $2.01 | — | $3,713.34 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ret5=+6.1; leftover $3151.31 | — |
| 2026-09-03 09:30 ET | **BUY** | `CXW` | 65 | $32.31 | $2.19 | — | $1,611.00 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list mover_buy; 🔵; ⚪; ret5=-4.1; leftover $2100.87 | — |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 66 | $15.87 | $2.19 | — | $561.40 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1050.44 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $561.40 | ▲ close $10,891.80 vs 09:30 $10,504.37 (session +395.83) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $561.40 | ▼ 09:30 equity $10,858.08 vs yday $10,891.80 (-33.72) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 11 | $359.70 | $2.06 | $+83.47 | $4,516.03 | ▲ +83.47 after sell → book $10,856.01; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DELL` | 6 | $513.78 | $2.04 | $+160.77 | $7,596.67 | ▲ +160.77 after sell → book $10,853.97; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CXW` | 65 | $33.46 | $2.21 | $+70.35 | $9,769.36 | ▲ +70.35 after sell → book $10,851.76; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 18 | $263.36 | $2.04 | — | $5,026.83 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $4884.68 | — |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 43 | $75.65 | $2.12 | — | $1,771.76 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+6.0; leftover $3256.45 | — |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 6 | $236.82 | $2.01 | — | $348.84 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; ret5=+8.1; leftover $1628.23 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $348.84 | ▲ close $10,974.27 vs 09:30 $10,858.08 (session +128.68) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $348.84 | ▲ 09:30 equity $11,017.32 vs yday $10,974.27 (+43.05) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 66 | $16.74 | $2.21 | $+53.02 | $1,451.47 | ▲ +53.02 after sell → book $11,015.11; vs 09:30 mark -2.21 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 18 | $253.72 | $2.09 | $-177.65 | $6,016.34 | ▼ -177.65 after sell → book $11,013.02; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MRX` | 43 | $78.84 | $2.16 | $+132.89 | $9,404.30 | ▲ +132.89 after sell → book $11,010.86; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BE` | 6 | $267.76 | $2.03 | $+181.60 | $11,008.83 | ▲ +181.60 after sell → book $11,008.83; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,008.83 | ▲ close $11,008.83 vs 09:30 $11,017.32 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,008.83 | ▲ 09:30 equity $11,008.83 vs yday $11,008.83 (-0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,008.83 | ▲ close $11,008.83 vs 09:30 $11,008.83 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,008.83 | ▲ 09:30 equity $11,008.83 vs yday $11,008.83 (-0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,008.83 | ▲ close $11,008.83 vs 09:30 $11,008.83 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,008.83 | ▲ 09:30 equity $11,008.83 vs yday $11,008.83 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 66 | $164.43 | $2.19 | — | $154.26 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $11008.83 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $154.26 | ▼ close $10,072.74 vs 09:30 $11,008.83 (session -933.90) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $154.26 | ▼ 09:30 equity $9,487.98 vs yday $10,072.74 (-584.76) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 66 | $141.42 | $2.27 | $-1523.12 | $9,485.71 | ▼ -1,523.12 after sell → book $9,485.71; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,485.71 | ▲ close $9,485.71 vs 09:30 $9,487.98 (session +0.00) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,485.71 | ▲ 09:30 equity $9,485.71 vs yday $9,485.71 (-0.00) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,485.71 | ▲ close $9,485.71 vs 09:30 $9,485.71 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,485.71 | ▲ 09:30 equity $9,485.71 vs yday $9,485.71 (-0.00) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 180 | $26.27 | $2.53 | — | $4,754.58 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer; 🔵; ret5=+10.0; leftover $4742.85 | — |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 16 | $189.17 | $2.04 | — | $1,725.82 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ret5=+7.9; leftover $3161.90 | — |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 39 | $39.99 | $2.11 | — | $164.10 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ret5=+9.3; leftover $1580.95 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $164.10 | ▼ close $9,395.98 vs 09:30 $9,485.71 (session -83.05) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $164.10 | ▲ 09:30 equity $9,446.73 vs yday $9,395.98 (+50.75) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `WAY` | 180 | $26.51 | $2.60 | $+38.07 | $4,933.30 | ▲ +38.07 after sell → book $9,444.13; vs 09:30 mark -2.60 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `QCOM` | 16 | $190.35 | $2.07 | $+14.77 | $7,976.83 | ▲ +14.77 after sell → book $9,442.06; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SM` | 39 | $37.57 | $2.13 | $-98.62 | $9,439.93 | ▼ -98.62 after sell → book $9,439.93; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 22 | $170.85 | $2.06 | — | $5,679.18 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $3775.97 | — |
| 2026-09-17 09:30 ET | **BUY** | `AVTR` | 179 | $15.81 | $2.53 | — | $2,846.66 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ret5=+4.9; leftover $2831.98 | — |
| 2026-09-17 09:30 ET | **BUY** | `GME` | 85 | $22.12 | $2.25 | — | $964.22 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ret5=+10.5; leftover $1887.99 | — |
| 2026-09-17 09:30 ET | **BUY** | `JBHT` | 3 | $238.60 | $2.00 | — | $246.42 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_mover; ret5=-11.6; leftover $943.99 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $246.42 | ▲ close $9,651.39 vs 09:30 $9,446.73 (session +220.28) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $246.42 | ▲ 09:30 equity $9,755.31 vs yday $9,651.39 (+103.92) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 22 | $182.33 | $2.10 | $+248.41 | $4,255.58 | ▲ +248.41 after sell → book $9,753.21; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `AVTR` | 179 | $15.87 | $2.58 | $+5.63 | $7,093.73 | ▲ +5.63 after sell → book $9,750.63; vs 09:30 mark -2.58 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `JBHT` | 3 | $236.80 | $2.02 | $-9.42 | $7,802.11 | ▼ -9.42 after sell → book $9,748.61; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 186 | $20.91 | $2.55 | — | $3,910.30 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $3901.05 | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 175 | $14.79 | $2.52 | — | $1,319.54 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $2600.70 | — |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 92 | $14.07 | $2.27 | — | $22.83 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $1300.35 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $22.83 | ▼ close $9,680.86 vs 09:30 $9,755.31 (session -60.42) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $22.83 | ▲ 09:30 equity $9,816.33 vs yday $9,680.86 (+135.47) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `GME` | 85 | $22.78 | $2.27 | $+51.58 | $1,956.86 | ▲ +51.58 after sell → book $9,814.06; vs 09:30 mark -2.27 | dropped from list after 2 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TH` | 186 | $21.65 | $2.61 | $+132.48 | $5,981.14 | ▲ +132.48 after sell → book $9,811.44; vs 09:30 mark -2.62 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RARE` | 175 | $14.58 | $2.56 | $-41.83 | $8,530.08 | ▼ -41.83 after sell → book $9,808.88; vs 09:30 mark -2.56 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `BHVN` | 92 | $13.90 | $2.29 | $-20.20 | $9,806.59 | ▼ -20.20 after sell → book $9,806.59; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `VICR` | 21 | $230.25 | $2.05 | — | $4,969.29 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; ret5=+12.5; leftover $4903.29 | — |
| 2026-09-21 09:30 ET | **BUY** | `SMTC` | 17 | $190.30 | $2.04 | — | $1,732.14 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; ret5=+10.6; leftover $3268.86 | — |
| 2026-09-21 09:30 ET | **BUY** | `GLXY` | 62 | $25.95 | $2.18 | — | $121.07 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $1634.43 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $121.07 | ▼ close $9,454.60 vs 09:30 $9,816.33 (session -345.72) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $121.07 | ▲ 09:30 equity $9,454.60 vs yday $9,454.60 (-0.00) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $121.07 | ▲ close $9,454.60 vs 09:30 $9,454.60 (session +0.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $121.07 | ▲ 09:30 equity $10,332.03 vs yday $9,454.60 (+877.43) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `VICR` | 21 | $266.50 | $2.11 | $+757.09 | $5,715.46 | ▲ +757.09 after sell → book $10,329.92; vs 09:30 mark -2.11 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `SMTC` | 17 | $174.50 | $2.07 | $-272.72 | $8,679.89 | ▼ -272.72 after sell → book $10,327.85; vs 09:30 mark -2.07 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `GLXY` | 62 | $26.58 | $2.20 | $+34.68 | $10,325.65 | ▲ +34.68 after sell → book $10,325.65; vs 09:30 mark -2.20 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `CTAS` | 20 | $196.78 | $2.05 | — | $6,388.00 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list earn_react; 🔵; ⚪; ret5=-2.0; leftover $4130.26 | — |
| 2026-09-23 09:30 ET | **BUY** | `PGEN` | 389 | $7.95 | $5.02 | — | $3,290.43 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list flatten; 🔵; ⚪; ret5=+12.4; leftover $3097.69 | — |
| 2026-09-23 09:30 ET | **BUY** | `SGRY` | 131 | $15.72 | $2.38 | — | $1,228.73 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $2065.13 | — |
| 2026-09-23 09:30 ET | **BUY** | `VERI` | 794 | $1.30 | $10.24 | — | $186.28 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ret5=+15.3; leftover $1032.56 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $186.28 | ▼ close $9,851.46 vs 09:30 $10,332.03 (session -454.49) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $186.28 | ▼ 09:30 equity $9,794.46 vs yday $9,851.46 (-57.00) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `CTAS` | 20 | $192.26 | $2.09 | $-94.54 | $4,029.39 | ▼ -94.54 after sell → book $9,792.37; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 389 | $7.38 | $5.11 | $-231.85 | $6,895.11 | ▼ -231.85 after sell → book $9,787.27; vs 09:30 mark -5.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SGRY` | 131 | $14.38 | $2.42 | $-180.34 | $8,776.47 | ▼ -180.34 after sell → book $9,784.85; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `VERI` | 794 | $1.27 | $10.38 | $-44.45 | $9,774.46 | ▼ -44.45 after sell → book $9,774.46; vs 09:30 mark -10.39 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,774.46 | ▲ close $9,774.46 vs 09:30 $9,794.46 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,921.21 | ▲ 09:30 equity $7,921.21 vs yday $7,921.21 (+0.00) | 09:30 open · cash $7,921.21 · no holdings · equity $7,921.21 vs prior close $7,921.21 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 09:30 ET | **BUY** | `ZSQR` | 820 | $3.86 | $10.58 | — | $4,745.43 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; leftover $3168.48 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ILMN` | 8 | $272.16 | $2.01 | — | $2,566.14 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+11.7; leftover $2376.36 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 97 | $16.21 | $2.28 | — | $991.49 | — | OR news + net≥4; leftover weighted by camera rank; gate news_or_headline=True,cam_net_min=4; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; leftover $1584.24 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $991.49 | ▼ close $7,799.21 vs 09:30 $7,921.21 (session -107.13) | 16:00 close · cash $991.49 · equity $7,799.21 vs 09:30 $7,921.21 (-122.00; session marks -107.13) · 3 name(s) marked open→close (per-name table). ZSQR×820 09:30 $3.86 → close $3.78 -65.60; ILMN×8 09:30 $272.16 → close $270.00 -17.28; SECZ×97 09:30 $16.21 → close $15.96 -24.25 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ABUS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-27 | `ASML` | cash | leftover split 1249.43 < 1 share @ 1746.53 |
| 2026-08-28 | `MPWR` | cash | leftover split 1086.91 < 1 share @ 1306.03 |
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
