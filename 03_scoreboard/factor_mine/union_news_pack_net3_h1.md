# Factor mine action — `union_news_pack_net3_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · packet🟢 and camera net ≥ 3

Cash book **-18.82%** ($8,118) · signal-only (no cash/fees) was +3.78%. Starts YES **14/30**. Fills 63 · skips 24 · realized $+978.05.

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
- Must-have: the morning news packet box is green.
- Must-have: camera net (+G −R) is at least 3.
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
- **Gate** `news_box=good,cam_net_min=3` · **rank** `cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10,978.05.

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
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 27 | $120.00 | $2.07 | — | $6,757.93 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list flatten; 🔵; ret5=+0.6; leftover $3333.33 | — |
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 9 | $359.83 | $2.02 | — | $3,517.44 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list flatten; 🔵; ret5=+5.9; leftover $3333.33 | — |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 22 | $146.90 | $2.06 | — | $283.59 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list flatten; 🔵; ret5=+3.6; leftover $3333.33 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $283.59 | ▲ close $10,215.59 vs 09:30 $10,000.00 (session +221.73) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $283.59 | ▲ 09:30 equity $10,320.45 vs yday $10,215.59 (+104.86) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `NRG` | 27 | $127.40 | $2.11 | $+195.62 | $3,721.28 | ▲ +195.62 after sell → book $10,318.34; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `TLN` | 9 | $367.88 | $2.05 | $+68.38 | $7,030.14 | ▲ +68.38 after sell → book $10,316.28; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VST` | 22 | $149.37 | $2.09 | $+50.19 | $10,314.19 | ▲ +50.19 after sell → book $10,314.19; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 74 | $46.18 | $2.21 | — | $6,894.66 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list flatten; 🔵; ret5=+6.7; leftover $3438.06 | — |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 24 | $142.77 | $2.06 | — | $3,466.12 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list flatten; 🔵; ret5=+5.8; leftover $3438.06 | — |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 16 | $202.70 | $2.04 | — | $220.88 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list flatten; 🔵; ret5=+8.3; leftover $3438.06 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $220.88 | ▲ close $10,549.30 vs 09:30 $10,320.45 (session +241.42) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $220.88 | ▲ 09:30 equity $10,668.72 vs yday $10,549.30 (+119.42) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 74 | $48.00 | $2.25 | $+130.22 | $3,770.63 | ▲ +130.22 after sell → book $10,666.47; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 24 | $148.04 | $2.10 | $+122.32 | $7,321.49 | ▲ +122.32 after sell → book $10,664.37; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 16 | $208.93 | $2.07 | $+95.57 | $10,662.29 | ▲ +95.57 after sell → book $10,662.29; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,662.29 | ▲ close $10,662.29 vs 09:30 $10,668.72 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,662.29 | ▲ 09:30 equity $10,662.29 vs yday $10,662.29 (+0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,662.29 | ▲ close $10,662.29 vs 09:30 $10,662.29 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,662.29 | ▲ 09:30 equity $10,662.29 vs yday $10,662.29 (+0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 238 | $44.76 | $3.07 | — | $6.34 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list mover_buy; 🔵; ret5=+8.7; leftover $10662.29 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6.34 | ▼ close $10,571.16 vs 09:30 $10,662.29 (session -88.06) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6.34 | ▲ 09:30 equity $10,602.10 vs yday $10,571.16 (+30.94) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `APA` | 238 | $44.52 | $3.19 | $-63.38 | $10,598.91 | ▼ -63.38 after sell → book $10,598.91; vs 09:30 mark -3.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 44 | $119.43 | $2.12 | — | $5,341.87 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $5299.45 | — |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 8 | $623.26 | $2.01 | — | $353.77 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list probable,yday_gainer; 🔵; ret5=+1.4; leftover $5299.45 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $353.77 | ▲ close $10,867.21 vs 09:30 $10,602.10 (session +272.44) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $353.77 | ▲ 09:30 equity $10,880.53 vs yday $10,867.21 (+13.32) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 44 | $120.51 | $2.17 | $+43.22 | $5,654.04 | ▲ +43.22 after sell → book $10,878.36; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `DE` | 8 | $653.04 | $2.07 | $+234.16 | $10,876.29 | ▲ +234.16 after sell → book $10,876.29; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,876.29 | ▲ close $10,876.29 vs 09:30 $10,880.53 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,876.29 | ▲ 09:30 equity $10,876.29 vs yday $10,876.29 (+0.00) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 45 | $118.52 | $2.12 | — | $5,540.77 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $5438.15 | — |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 70 | $77.13 | $2.20 | — | $139.47 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $5438.15 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $139.47 | ▲ close $11,285.72 vs 09:30 $10,876.29 (session +413.75) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $139.47 | ▼ 09:30 equity $11,084.27 vs yday $11,285.72 (-201.45) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 45 | $119.80 | $2.18 | $+53.30 | $5,528.29 | ▲ +53.30 after sell → book $11,082.09; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 70 | $79.34 | $2.26 | $+150.24 | $11,079.83 | ▲ +150.24 after sell → book $11,079.83; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `FNV` | 20 | $267.02 | $2.05 | — | $5,737.38 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list ohlc_hot; 🔵; ret5=+16.7; leftover $5539.92 | — |
| 2026-08-26 09:30 ET | **BUY** | `CM` | 46 | $118.50 | $2.13 | — | $284.25 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list overnight,overnight_mega; 🔵; ret5=-2.7; leftover $5539.92 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $284.25 | ▼ close $11,068.85 vs 09:30 $11,084.27 (session -6.80) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $284.25 | ▲ 09:30 equity $11,092.27 vs yday $11,068.85 (+23.42) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `FNV` | 20 | $267.23 | $2.10 | $+0.05 | $5,626.75 | ▲ +0.05 after sell → book $11,090.17; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 9 | $81.65 | $2.02 | — | $4,889.88 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list mover_buy; 🔵; ret5=+2.0; leftover $803.82 | — |
| 2026-08-27 09:30 ET | **BUY** | `LRCX` | 2 | $318.88 | $2.00 | — | $4,250.13 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $803.82 | — |
| 2026-08-27 09:30 ET | **BUY** | `NVDA` | 3 | $222.86 | $2.00 | — | $3,579.55 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list mover_buy; 🔵; ret5=-3.6; leftover $803.82 | — |
| 2026-08-27 09:30 ET | **BUY** | `ADSK` | 3 | $261.47 | $2.00 | — | $2,793.14 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list overnight,overnight_mega; 🔵; ret5=+1.4; leftover $803.82 | — |
| 2026-08-27 09:30 ET | **BUY** | `AXTI` | 11 | $70.30 | $2.02 | — | $2,017.82 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list yday_gainer,yday_mover; 🔵; ret5=-11.2; leftover $803.82 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,017.82 | ▼ close $10,893.83 vs 09:30 $11,092.27 (session -186.31) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,017.82 | ▼ 09:30 equity $10,871.42 vs yday $10,893.83 (-22.41) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `CM` | 46 | $115.66 | $2.18 | $-134.95 | $7,336.00 | ▼ -134.95 after sell → book $10,869.24; vs 09:30 mark -2.18 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 9 | $79.27 | $2.04 | $-25.47 | $8,047.39 | ▼ -25.47 after sell → book $10,867.20; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `LRCX` | 2 | $318.03 | $2.02 | $-5.71 | $8,681.43 | ▼ -5.71 after sell → book $10,865.18; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `NVDA` | 3 | $227.36 | $2.02 | $+9.48 | $9,361.49 | ▲ +9.48 after sell → book $10,863.16; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `AXTI` | 11 | $65.29 | $2.04 | $-59.18 | $10,077.64 | ▼ -59.18 after sell → book $10,861.12; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 7 | $324.41 | $2.01 | — | $7,804.76 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $2519.41 | — |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 6 | $400.42 | $2.01 | — | $5,400.23 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $2519.41 | — |
| 2026-08-28 09:30 ET | **BUY** | `MPWR` | 1 | $1306.03 | $1.99 | — | $4,092.21 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list mover_buy; 🔵; ret5=+0.0; leftover $2519.41 | — |
| 2026-08-28 09:30 ET | **BUY** | `DDOG` | 10 | $240.22 | $2.02 | — | $1,687.99 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list mover_buy; 🔵; ret5=+4.5; leftover $2519.41 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,687.99 | ▼ close $10,606.46 vs 09:30 $10,871.42 (session -246.63) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,687.99 | ▼ 09:30 equity $10,590.74 vs yday $10,606.46 (-15.72) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `ADSK` | 3 | $257.71 | $2.02 | $-15.30 | $2,459.10 | ▼ -15.30 after sell → book $10,588.72; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 7 | $322.49 | $2.04 | $-17.49 | $4,714.49 | ▼ -17.49 after sell → book $10,586.68; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CIEN` | 6 | $378.44 | $2.04 | $-135.92 | $6,983.10 | ▼ -135.92 after sell → book $10,584.65; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MPWR` | 1 | $1261.90 | $2.01 | $-48.14 | $8,242.98 | ▼ -48.14 after sell → book $10,582.63; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `DDOG` | 10 | $233.97 | $2.05 | $-66.62 | $10,580.58 | ▼ -66.62 after sell → book $10,580.58; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,580.58 | ▲ close $10,580.58 vs 09:30 $10,590.74 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,580.58 | ▲ 09:30 equity $10,580.58 vs yday $10,580.58 (+0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,580.58 | ▲ close $10,580.58 vs 09:30 $10,580.58 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,580.58 | ▲ 09:30 equity $10,580.58 vs yday $10,580.58 (+0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,580.58 | ▲ close $10,580.58 vs 09:30 $10,580.58 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,580.58 | ▲ 09:30 equity $10,580.58 vs yday $10,580.58 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 10 | $351.74 | $2.02 | — | $7,061.16 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $3526.86 | — |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 7 | $486.31 | $2.01 | — | $3,654.98 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list mover_buy; 🔵; ret5=+6.1; leftover $3526.86 | — |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 74 | $47.60 | $2.21 | — | $130.37 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list earn_react; 🔵; ret5=-6.2; leftover $3526.86 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $130.37 | ▲ close $11,345.26 vs 09:30 $10,580.58 (session +770.92) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $130.37 | ▼ 09:30 equity $11,308.73 vs yday $11,345.26 (-36.53) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 10 | $359.70 | $2.06 | $+75.52 | $3,725.31 | ▲ +75.52 after sell → book $11,306.67; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DELL` | 7 | $513.78 | $2.05 | $+188.23 | $7,319.72 | ▲ +188.23 after sell → book $11,304.62; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `HPE` | 74 | $53.85 | $2.26 | $+458.03 | $11,302.37 | ▲ +458.03 after sell → book $11,302.37; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 21 | $263.36 | $2.05 | — | $5,769.75 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $5651.18 | — |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 23 | $236.82 | $2.06 | — | $320.83 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list ohlc_hot; ret5=+8.1; leftover $5651.18 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $320.83 | ▲ close $11,580.67 vs 09:30 $11,308.73 (session +282.42) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $320.83 | ▲ 09:30 equity $11,807.43 vs yday $11,580.67 (+226.76) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 21 | $253.72 | $2.11 | $-206.60 | $5,646.85 | ▼ -206.60 after sell → book $11,805.33; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BE` | 23 | $267.76 | $2.12 | $+707.44 | $11,803.21 | ▲ +707.44 after sell → book $11,803.21; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,803.21 | ▲ close $11,803.21 vs 09:30 $11,807.43 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,803.21 | ▲ 09:30 equity $11,803.21 vs yday $11,803.21 (-0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,803.21 | ▲ close $11,803.21 vs 09:30 $11,803.21 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,803.21 | ▲ 09:30 equity $11,803.21 vs yday $11,803.21 (-0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,803.21 | ▲ close $11,803.21 vs 09:30 $11,803.21 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,803.21 | ▲ 09:30 equity $11,803.21 vs yday $11,803.21 (-0.00) | — | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,803.21 | ▲ close $11,803.21 vs 09:30 $11,803.21 (session +0.00) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,803.21 | ▲ 09:30 equity $11,803.21 vs yday $11,803.21 (-0.00) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,803.21 | ▲ close $11,803.21 vs 09:30 $11,803.21 (session +0.00) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,803.21 | ▲ 09:30 equity $11,803.21 vs yday $11,803.21 (-0.00) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,803.21 | ▲ close $11,803.21 vs 09:30 $11,803.21 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,803.21 | ▲ 09:30 equity $11,803.21 vs yday $11,803.21 (-0.00) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 31 | $189.17 | $2.08 | — | $5,936.86 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list ohlc_hot; 🔵; ret5=+7.9; leftover $5901.60 | — |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 147 | $39.99 | $2.43 | — | $55.90 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list ohlc_hot; 🔵; ret5=+9.3; leftover $5901.60 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $55.90 | ▼ close $11,395.46 vs 09:30 $11,803.21 (session -403.24) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $55.90 | ▲ 09:30 equity $11,479.54 vs yday $11,395.46 (+84.08) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `QCOM` | 31 | $190.35 | $2.14 | $+32.36 | $5,954.61 | ▲ +32.36 after sell → book $11,477.40; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SM` | 147 | $37.57 | $2.50 | $-360.67 | $11,474.90 | ▼ -360.67 after sell → book $11,474.90; vs 09:30 mark -2.50 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `LITE` | 12 | $934.88 | $2.03 | — | $254.31 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list yday_gainer; ret5=-7.0; leftover $11474.90 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $254.31 | ▼ close $10,977.63 vs 09:30 $11,479.54 (session -495.24) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $254.31 | ▲ 09:30 equity $11,242.23 vs yday $10,977.63 (+264.60) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `LITE` | 12 | $915.66 | $2.12 | $-234.79 | $11,240.11 | ▼ -234.79 after sell → book $11,240.11; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,240.11 | ▲ close $11,240.11 vs 09:30 $11,242.23 (session +0.00) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,240.11 | ▲ 09:30 equity $11,240.11 vs yday $11,240.11 (-0.00) | — | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,240.11 | ▲ close $11,240.11 vs 09:30 $11,240.11 (session +0.00) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,240.11 | ▲ 09:30 equity $11,240.11 vs yday $11,240.11 (-0.00) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,240.11 | ▲ close $11,240.11 vs 09:30 $11,240.11 (session +0.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,240.11 | ▲ 09:30 equity $11,240.11 vs yday $11,240.11 (-0.00) | — | — |
| 2026-09-23 09:30 ET | **BUY** | `CTAS` | 57 | $196.78 | $2.16 | — | $21.48 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list earn_react; 🔵; ⚪; ret5=-2.0; leftover $11240.11 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21.48 | ▼ close $10,963.77 vs 09:30 $11,240.11 (session -274.17) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21.48 | ▲ 09:30 equity $10,980.30 vs yday $10,963.77 (+16.53) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `CTAS` | 57 | $192.26 | $2.26 | $-262.06 | $10,978.05 | ▼ -262.06 after sell → book $10,978.05; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,978.05 | ▲ close $10,978.05 vs 09:30 $10,980.30 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,834.29 | ▲ 09:30 equity $7,834.29 vs yday $7,834.29 (+0.00) | 09:30 open · cash $7,834.29 · no holdings · equity $7,834.29 vs prior close $7,834.29 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 09:30 ET | **BUY** | `COST` | 8 | $887.00 | $2.01 | — | $736.28 | — | packet🟢 and camera net ≥ 3; gate news_box=good,cam_net_min=3; rank cond; list earn_react; 🔵; ret5=+0.3; leftover $7834.29 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $736.28 | ▲ close $8,118.40 vs 09:30 $7,834.29 (session +286.12) | 16:00 close · cash $736.28 · equity $8,118.40 vs 09:30 $7,834.29 (+284.11; session marks +286.12) · 1 name(s) marked open→close (per-name table). COST×8 09:30 $887.00 → close $922.76 +286.12 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-27 | `MU` | cash | leftover split 803.82 < 1 share @ 967.01 |
| 2026-08-27 | `ASML` | cash | leftover split 803.82 < 1 share @ 1746.53 |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CDNS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `NVDA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NOW` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `JCI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `LITE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `HPE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ORCL` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `BE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-24 | `ACMR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `EOG` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `RRC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CVE` | hard_red | hard-red S=-7.66 sit; no new buys |
