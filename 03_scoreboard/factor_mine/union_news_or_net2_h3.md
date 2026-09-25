# Factor mine action — `union_news_or_net2_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · packet🟢 OR headline🟢 and camera net ≥ 2

Cash book **-18.49%** ($8,151) · signal-only (no cash/fees) was -9.69%. Starts YES **1/30**. Fills 133 · skips 190 · realized $-1348.46.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Sort: how many morning cameras are green vs red.
- Must-have: the morning news packet OR the prior-export headline is green.
- Must-have: camera net (+G −R) is at least 2.
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
- Minimum hold is 3 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 3 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `news_or_headline=True,cam_net_min=2` · **rank** `cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $5,843.29.

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
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 94 | $13.18 | $2.27 | — | $8,758.81 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 290 | $4.31 | $3.74 | — | $7,505.17 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 63 | $19.57 | $2.18 | — | $6,270.08 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 92 | $13.55 | $2.27 | — | $5,021.21 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer; 🔵; ⚪; ret5=+17.5; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `VELO` | 81 | $15.38 | $2.23 | — | $3,773.20 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+16.3; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 10 | $120.00 | $2.02 | — | $2,571.18 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list flatten; 🔵; ret5=+0.6; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `S` | 52 | $23.77 | $2.15 | — | $1,332.73 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; 🔵; ret5=+14.8; leftover $1250.00 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,332.73 | ▲ close $10,120.33 vs 09:30 $10,000.00 (session +137.19) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,332.73 | ▲ 09:30 equity $10,155.37 vs yday $10,120.33 (+35.04) | — | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 7 | $46.18 | $2.01 | — | $1,007.46 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list flatten; 🔵; ret5=+6.7; leftover $333.18 | — |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 2 | $142.77 | $2.00 | — | $719.93 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list flatten; 🔵; ret5=+5.8; leftover $333.18 | — |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 1 | $202.70 | $1.99 | — | $515.23 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list flatten; 🔵; ret5=+8.3; leftover $333.18 | — |
| 2026-08-17 09:30 ET | **BUY** | `OUST` | 6 | $49.00 | $2.01 | — | $219.23 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer; ⚪; ret5=+12.2; leftover $333.18 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $219.23 | ▼ close $9,995.30 vs 09:30 $10,155.37 (session -152.07) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $219.23 | ▼ 09:30 equity $9,902.34 vs yday $9,995.30 (-92.96) | — | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $219.23 | ▼ close $9,871.95 vs 09:30 $9,902.34 (session -30.39) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $219.23 | ▼ 09:30 equity $9,858.04 vs yday $9,871.95 (-13.91) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `HLIT` | 94 | $12.90 | $2.30 | $-30.89 | $1,429.53 | ▼ -30.89 after sell → book $9,855.74; vs 09:30 mark -2.30 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ANGX` | 290 | $4.79 | $3.80 | $+131.66 | $2,814.83 | ▲ +131.66 after sell → book $9,851.94; vs 09:30 mark -3.80 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ARX` | 63 | $19.58 | $2.20 | $-3.75 | $4,046.17 | ▼ -3.75 after sell → book $9,849.74; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `MH` | 92 | $13.01 | $2.29 | $-54.24 | $5,240.80 | ▼ -54.24 after sell → book $9,847.45; vs 09:30 mark -2.29 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `VELO` | 81 | $14.51 | $2.26 | $-74.96 | $6,413.85 | ▼ -74.96 after sell → book $9,845.19; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `NRG` | 10 | $116.20 | $2.04 | $-42.06 | $7,573.81 | ▼ -42.06 after sell → book $9,843.15; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `S` | 52 | $22.37 | $2.17 | $-77.37 | $8,734.88 | ▼ -77.37 after sell → book $9,840.98; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,734.88 | ▼ close $9,820.08 vs 09:30 $9,858.04 (session -20.90) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,734.88 | ▲ 09:30 equity $9,838.21 vs yday $9,820.08 (+18.13) | — | — |
| 2026-08-20 09:30 ET | **SELL** | `DVN` | 7 | $49.02 | $2.03 | $+15.84 | $9,075.99 | ▲ +15.84 after sell → book $9,836.18; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `EOG` | 2 | $151.45 | $2.02 | $+13.35 | $9,376.88 | ▲ +13.35 after sell → book $9,834.17; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `FANG` | 1 | $213.51 | $2.01 | $+6.80 | $9,588.37 | ▲ +6.80 after sell → book $9,832.15; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `OUST` | 6 | $40.63 | $2.03 | $-54.26 | $9,830.13 | ▼ -54.26 after sell → book $9,830.13; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $8,644.97 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1228.77 | — |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 27 | $44.76 | $2.07 | — | $7,434.38 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list mover_buy; 🔵; ret5=+8.7; leftover $1228.77 | — |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 497 | $2.47 | $6.41 | — | $6,200.37 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $1228.77 | — |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 20 | $58.73 | $2.05 | — | $5,023.72 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.8; leftover $1228.77 | — |
| 2026-08-20 09:30 ET | **BUY** | `ASST` | 76 | $16.00 | $2.22 | — | $3,805.51 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+18.9; leftover $1228.77 | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 8 | $150.14 | $2.01 | — | $2,602.37 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1228.77 | — |
| 2026-08-20 09:30 ET | **BUY** | `ZLAB` | 46 | $26.57 | $2.13 | — | $1,378.02 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; 🔵; ret5=+4.8; leftover $1228.77 | — |
| 2026-08-20 09:30 ET | **BUY** | `TEAM` | 7 | $173.90 | $2.01 | — | $158.71 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; 🔵; ret5=+12.2; leftover $1228.77 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $158.71 | ▼ close $9,673.18 vs 09:30 $9,838.21 (session -136.01) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $158.71 | ▲ 09:30 equity $9,861.18 vs yday $9,673.18 (+188.00) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `ABTC` | 3 | $8.66 | $0.27 | — | $132.46 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+15.7; leftover $26.45 | — |
| 2026-08-21 09:30 ET | **BUY** | `HIVE` | 8 | $3.24 | $0.28 | — | $106.26 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer; 🔵; ret5=+21.3; leftover $26.45 | — |
| 2026-08-21 09:30 ET | **BUY** | `MARA` | 2 | $11.70 | $0.24 | — | $82.62 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+21.1; leftover $26.45 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $82.62 | ▲ close $9,918.75 vs 09:30 $9,861.18 (session +58.36) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $82.62 | ▼ 09:30 equity $9,867.06 vs yday $9,918.75 (-51.69) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $82.62 | ▲ close $9,870.22 vs 09:30 $9,867.06 (session +3.16) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $82.62 | ▼ 09:30 equity $9,845.84 vs yday $9,870.22 (-24.38) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 13 | $95.86 | $2.05 | $+58.97 | $1,326.75 | ▲ +58.97 after sell → book $9,843.79; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `APA` | 27 | $41.38 | $2.09 | $-95.42 | $2,441.92 | ▼ -95.42 after sell → book $9,841.70; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AUTL` | 497 | $2.38 | $6.50 | $-57.65 | $3,618.28 | ▼ -57.65 after sell → book $9,835.20; vs 09:30 mark -6.50 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 20 | $57.93 | $2.07 | $-20.12 | $4,774.81 | ▼ -20.12 after sell → book $9,833.13; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ASST` | 76 | $19.04 | $2.24 | $+226.58 | $6,219.61 | ▲ +226.58 after sell → book $9,830.89; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `MRNA` | 8 | $143.50 | $2.03 | $-57.17 | $7,365.57 | ▼ -57.17 after sell → book $9,828.85; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ZLAB` | 46 | $26.04 | $2.15 | $-28.66 | $8,561.26 | ▼ -28.66 after sell → book $9,826.70; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `TEAM` | 7 | $170.64 | $2.03 | $-26.86 | $9,753.71 | ▼ -26.86 after sell → book $9,824.67; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 16 | $118.52 | $2.04 | — | $7,855.35 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $1950.74 | — |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 25 | $77.13 | $2.06 | — | $5,925.04 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $1950.74 | — |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 55 | $35.05 | $2.15 | — | $3,995.13 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer; 🔵; ⚪; ret5=+19.7; leftover $1950.74 | — |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 207 | $9.42 | $2.67 | — | $2,042.52 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+13.6; leftover $1950.74 | — |
| 2026-08-25 09:30 ET | **BUY** | `ZYME` | 67 | $28.86 | $2.19 | — | $106.71 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; 🔵; ret5=+13.7; leftover $1950.74 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $106.71 | ▲ close $10,049.99 vs 09:30 $9,845.84 (session +236.44) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $106.71 | ▼ 09:30 equity $9,974.76 vs yday $10,049.99 (-75.23) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `ABTC` | 3 | $8.84 | $0.29 | $-0.02 | $132.94 | ▼ -0.02 after sell → book $9,974.47; vs 09:30 mark -0.29 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `HIVE` | 8 | $2.95 | $0.28 | $-2.88 | $156.26 | ▼ -2.88 after sell → book $9,974.19; vs 09:30 mark -0.28 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `MARA` | 2 | $11.56 | $0.26 | $-0.78 | $179.12 | ▼ -0.78 after sell → book $9,973.93; vs 09:30 mark -0.26 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $179.12 | ▼ close $9,812.44 vs 09:30 $9,974.76 (session -161.49) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $179.12 | ▼ 09:30 equity $9,804.61 vs yday $9,812.44 (-7.83) | — | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $179.12 | ▲ close $9,822.25 vs 09:30 $9,804.61 (session +17.64) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $179.12 | ▼ 09:30 equity $9,809.98 vs yday $9,822.25 (-12.27) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `AU` | 16 | $119.19 | $2.06 | $+6.62 | $2,084.10 | ▲ +6.62 after sell → book $9,807.92; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `FCX` | 25 | $78.57 | $2.09 | $+31.84 | $4,046.26 | ▲ +31.84 after sell → book $9,805.83; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `EZPW` | 55 | $34.50 | $2.18 | $-34.59 | $5,941.58 | ▼ -34.59 after sell → book $9,803.65; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `RUM` | 207 | $9.30 | $2.72 | $-30.23 | $7,863.96 | ▼ -30.23 after sell → book $9,800.93; vs 09:30 mark -2.72 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ZYME` | 67 | $28.91 | $2.22 | $-1.06 | $9,798.71 | ▼ -1.06 after sell → book $9,798.71; vs 09:30 mark -2.22 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 3 | $324.41 | $2.00 | — | $8,823.48 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1224.84 | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 8 | $141.76 | $2.01 | — | $7,687.39 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1224.84 | — |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 3 | $400.42 | $2.00 | — | $6,484.13 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1224.84 | — |
| 2026-08-28 09:30 ET | **BUY** | `DDOG` | 5 | $240.22 | $2.00 | — | $5,281.02 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list mover_buy; 🔵; ret5=+4.5; leftover $1224.84 | — |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 4 | $261.16 | $2.00 | — | $4,234.38 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list earn_react; ret5=+7.8; leftover $1224.84 | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 37 | $32.90 | $2.10 | — | $3,014.98 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1224.84 | — |
| 2026-08-28 09:30 ET | **BUY** | `TLS` | 254 | $4.82 | $3.28 | — | $1,787.42 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; 🔵; ret5=+8.8; leftover $1224.84 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,787.42 | ▼ close $9,538.38 vs 09:30 $9,809.98 (session -244.93) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,787.42 | ▼ 09:30 equity $9,523.57 vs yday $9,538.38 (-14.81) | — | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,787.42 | ▲ close $9,602.60 vs 09:30 $9,523.57 (session +79.03) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,787.42 | ▼ 09:30 equity $9,472.63 vs yday $9,602.60 (-129.97) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,787.42 | ▼ close $9,395.35 vs 09:30 $9,472.63 (session -77.28) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,787.42 | ▼ 09:30 equity $9,362.35 vs yday $9,395.35 (-33.00) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `KEYS` | 3 | $318.04 | $2.02 | $-23.13 | $2,739.52 | ▼ -23.13 after sell → book $9,360.33; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SMTC` | 8 | $133.00 | $2.03 | $-74.13 | $3,801.49 | ▼ -74.13 after sell → book $9,358.30; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CIEN` | 3 | $357.25 | $2.02 | $-133.53 | $4,871.22 | ▼ -133.53 after sell → book $9,356.28; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `DDOG` | 5 | $219.46 | $2.02 | $-107.83 | $5,966.50 | ▼ -107.83 after sell → book $9,354.26; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ADSK` | 4 | $246.70 | $2.02 | $-61.86 | $6,951.27 | ▼ -61.86 after sell → book $9,352.23; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SEDG` | 37 | $32.42 | $2.12 | $-21.98 | $8,148.69 | ▼ -21.98 after sell → book $9,350.11; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `TLS` | 254 | $4.73 | $3.33 | $-29.47 | $9,346.78 | ▼ -29.47 after sell → book $9,346.78; vs 09:30 mark -3.33 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,346.78 | ▲ close $9,346.78 vs 09:30 $9,362.35 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,346.78 | ▲ 09:30 equity $9,346.78 vs yday $9,346.78 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 3 | $351.74 | $2.00 | — | $8,289.57 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $1168.35 | — |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 2 | $486.31 | $2.00 | — | $7,314.95 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list mover_buy; 🔵; ret5=+6.1; leftover $1168.35 | — |
| 2026-09-03 09:30 ET | **BUY** | `CXW` | 36 | $32.31 | $2.10 | — | $6,149.69 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list mover_buy; 🔵; ⚪; ret5=-4.1; leftover $1168.35 | — |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 73 | $15.87 | $2.21 | — | $4,988.97 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1168.35 | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 48 | $23.88 | $2.13 | — | $3,840.60 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1168.35 | — |
| 2026-09-03 09:30 ET | **BUY** | `DE` | 1 | $703.25 | $1.99 | — | $3,135.36 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.1; leftover $1168.35 | — |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 24 | $47.60 | $2.06 | — | $1,990.89 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list earn_react; 🔵; ret5=-6.2; leftover $1168.35 | — |
| 2026-09-03 09:30 ET | **BUY** | `CNXC` | 35 | $32.88 | $2.10 | — | $838.00 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer; 🔵; ret5=+16.2; leftover $1168.35 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $838.00 | ▲ close $9,682.76 vs 09:30 $9,346.78 (session +352.56) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $838.00 | ▼ 09:30 equity $9,611.97 vs yday $9,682.76 (-70.79) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 2 | $75.65 | $1.52 | — | $685.18 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+6.0; leftover $209.50 | — |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 107 | $1.94 | $2.31 | — | $475.29 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer,yday_mover; ret5=+18.3; leftover $209.50 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $475.29 | ▼ close $9,581.28 vs 09:30 $9,611.97 (session -26.86) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $475.29 | ▲ 09:30 equity $9,603.35 vs yday $9,581.28 (+22.07) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $475.29 | ▼ close $9,570.92 vs 09:30 $9,603.35 (session -32.43) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $475.29 | ▲ 09:30 equity $9,595.03 vs yday $9,570.92 (+24.11) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `AVGO` | 3 | $366.23 | $2.02 | $+39.45 | $1,571.96 | ▲ +39.45 after sell → book $9,593.01; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `DELL` | 2 | $538.47 | $2.02 | $+100.31 | $2,646.88 | ▲ +100.31 after sell → book $9,591.00; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CXW` | 36 | $35.09 | $2.12 | $+95.86 | $3,908.01 | ▲ +95.86 after sell → book $9,588.88; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `FRNM` | 73 | $15.96 | $2.23 | $+2.13 | $5,070.85 | ▲ +2.13 after sell → book $9,586.65; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MMED` | 48 | $23.22 | $2.15 | $-35.97 | $6,183.26 | ▼ -35.97 after sell → book $9,584.50; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `DE` | 1 | $681.32 | $2.01 | $-25.94 | $6,862.57 | ▼ -25.94 after sell → book $9,582.48; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `HPE` | 24 | $56.94 | $2.08 | $+220.02 | $8,227.04 | ▲ +220.02 after sell → book $9,580.40; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CNXC` | 35 | $28.13 | $2.12 | $-170.46 | $9,209.48 | ▼ -170.46 after sell → book $9,578.28; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,209.48 | ▼ close $9,571.71 vs 09:30 $9,595.03 (session -6.58) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,209.48 | ▼ 09:30 equity $9,570.27 vs yday $9,571.71 (-1.44) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `MRX` | 2 | $75.00 | $1.53 | $-4.35 | $9,357.95 | ▼ -4.35 after sell → book $9,568.74; vs 09:30 mark -1.53 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BAK` | 107 | $1.97 | $2.34 | $-1.44 | $9,566.40 | ▼ -1.44 after sell → book $9,566.40; vs 09:30 mark -2.34 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,566.40 | ▲ close $9,566.40 vs 09:30 $9,570.27 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,566.40 | ▲ 09:30 equity $9,566.40 vs yday $9,566.40 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 14 | $164.43 | $2.03 | — | $7,262.35 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $2391.60 | — |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 9 | $242.17 | $2.02 | — | $5,080.81 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list earn_react; ret5=-11.1; leftover $2391.60 | — |
| 2026-09-11 09:30 ET | **BUY** | `AVTR` | 159 | $15.01 | $2.47 | — | $2,691.75 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; 🔵; ret5=+4.9; leftover $2391.60 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 1128 | $2.12 | $14.55 | — | $285.84 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $2391.60 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $285.84 | ▼ close $9,360.86 vs 09:30 $9,566.40 (session -184.48) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $285.84 | ▼ 09:30 equity $9,296.04 vs yday $9,360.86 (-64.82) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $285.84 | ▲ close $9,381.02 vs 09:30 $9,296.04 (session +84.98) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $285.84 | ▼ 09:30 equity $9,348.12 vs yday $9,381.02 (-32.90) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $285.84 | ▲ close $9,394.34 vs 09:30 $9,348.12 (session +46.22) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $285.84 | ▼ 09:30 equity $9,071.11 vs yday $9,394.34 (-323.23) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `ORCL` | 14 | $140.03 | $2.06 | $-345.69 | $2,244.20 | ▼ -345.69 after sell → book $9,069.05; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `ADBE` | 9 | $253.34 | $2.05 | $+96.47 | $4,522.21 | ▲ +96.47 after sell → book $9,067.00; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `BAK` | 1128 | $1.84 | $14.76 | $-345.15 | $6,582.98 | ▼ -345.15 after sell → book $9,052.25; vs 09:30 mark -14.75 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 83 | $26.27 | $2.24 | — | $4,400.33 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer; 🔵; ret5=+10.0; leftover $2194.33 | — |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 11 | $189.17 | $2.02 | — | $2,317.44 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; 🔵; ret5=+7.9; leftover $2194.33 | — |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 54 | $39.99 | $2.15 | — | $155.83 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; 🔵; ret5=+9.3; leftover $2194.33 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $155.83 | ▼ close $8,938.67 vs 09:30 $9,071.11 (session -107.17) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $155.83 | ▲ 09:30 equity $8,992.58 vs yday $8,938.67 (+53.91) | — | — |
| 2026-09-17 09:30 ET | **BUY** | `GME` | 1 | $22.12 | $0.22 | — | $133.48 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; 🔵; ret5=+10.5; leftover $31.17 | — |
| 2026-09-17 09:30 ET | **BUY** | `TNDM` | 1 | $17.72 | $0.18 | — | $115.58 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer; 🔵; ret5=-8.3; leftover $31.17 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $115.58 | ▼ close $8,949.84 vs 09:30 $8,992.58 (session -42.33) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $115.58 | ▲ 09:30 equity $9,011.51 vs yday $8,949.84 (+61.67) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `AVTR` | 159 | $15.87 | $2.51 | $+131.76 | $2,636.40 | ▲ +131.76 after sell → book $9,009.00; vs 09:30 mark -2.51 | dropped from list after 5 sess (min 3) | — |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 42 | $20.91 | $2.12 | — | $1,756.06 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $878.80 | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 59 | $14.79 | $2.17 | — | $881.28 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $878.80 | — |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 62 | $14.07 | $2.18 | — | $6.77 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $878.80 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6.77 | ▼ close $8,717.87 vs 09:30 $9,011.51 (session -284.67) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6.77 | ▲ 09:30 equity $8,756.75 vs yday $8,717.87 (+38.88) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `WAY` | 83 | $25.94 | $2.27 | $-31.90 | $2,157.52 | ▼ -31.90 after sell → book $8,754.48; vs 09:30 mark -2.27 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `QCOM` | 11 | $180.61 | $2.05 | $-98.23 | $4,142.18 | ▼ -98.23 after sell → book $8,752.43; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `SM` | 54 | $35.91 | $2.18 | $-224.65 | $6,079.14 | ▼ -224.65 after sell → book $8,750.25; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **BUY** | `VICR` | 5 | $230.25 | $2.00 | — | $4,925.89 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; ret5=+12.5; leftover $1215.83 | — |
| 2026-09-21 09:30 ET | **BUY** | `SMTC` | 6 | $190.30 | $2.01 | — | $3,782.08 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; ret5=+10.6; leftover $1215.83 | — |
| 2026-09-21 09:30 ET | **BUY** | `GLXY` | 46 | $25.95 | $2.13 | — | $2,586.25 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $1215.83 | — |
| 2026-09-21 09:30 ET | **BUY** | `MARA` | 87 | $13.94 | $2.25 | — | $1,371.22 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+10.5; leftover $1215.83 | — |
| 2026-09-21 09:30 ET | **BUY** | `SION` | 202 | $6.00 | $2.61 | — | $156.61 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_mover; ret5=-24.1; leftover $1215.83 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $156.61 | ▼ close $8,587.04 vs 09:30 $8,756.75 (session -152.21) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $156.61 | ▼ 09:30 equity $8,580.38 vs yday $8,587.04 (-6.66) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `GME` | 1 | $23.50 | $0.26 | $+0.90 | $179.86 | ▲ +0.90 after sell → book $8,580.13; vs 09:30 mark -0.25 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $179.86 | ▲ close $8,678.95 vs 09:30 $8,580.38 (session +98.83) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $179.86 | ▲ 09:30 equity $8,904.67 vs yday $8,678.95 (+225.72) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `TNDM` | 1 | $17.44 | $0.20 | $-0.66 | $197.10 | ▼ -0.66 after sell → book $8,904.47; vs 09:30 mark -0.20 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `TH` | 42 | $21.15 | $2.14 | $+5.83 | $1,083.26 | ▲ +5.83 after sell → book $8,902.33; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `RARE` | 59 | $15.40 | $2.19 | $+31.64 | $1,989.68 | ▲ +31.64 after sell → book $8,900.15; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `BHVN` | 62 | $14.84 | $2.20 | $+43.37 | $2,907.56 | ▲ +43.37 after sell → book $8,897.95; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **BUY** | `CTAS` | 2 | $196.78 | $2.00 | — | $2,512.00 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list earn_react; 🔵; ⚪; ret5=-2.0; leftover $484.59 | — |
| 2026-09-23 09:30 ET | **BUY** | `PGEN` | 60 | $7.95 | $2.17 | — | $2,032.83 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list flatten; 🔵; ⚪; ret5=+12.4; leftover $484.59 | — |
| 2026-09-23 09:30 ET | **BUY** | `SGRY` | 30 | $15.72 | $2.08 | — | $1,559.15 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $484.59 | — |
| 2026-09-23 09:30 ET | **BUY** | `VERI` | 372 | $1.30 | $4.80 | — | $1,070.75 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; 🔵; ret5=+15.3; leftover $484.59 | — |
| 2026-09-23 09:30 ET | **BUY** | `CMPX` | 397 | $1.22 | $5.12 | — | $581.29 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_mover; 🔵; ret5=-33.0; leftover $484.59 | — |
| 2026-09-23 09:30 ET | **BUY** | `BLSH` | 12 | $40.00 | $2.03 | — | $99.27 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; 🔵; ret5=+6.7; leftover $484.59 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $99.27 | ▼ close $8,684.69 vs 09:30 $8,904.67 (session -195.07) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $99.27 | ▼ 09:30 equity $8,521.31 vs yday $8,684.69 (-163.38) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `VICR` | 5 | $274.61 | $2.03 | $+217.77 | $1,470.29 | ▲ +217.77 after sell → book $8,519.28; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `SMTC` | 6 | $164.04 | $2.03 | $-161.60 | $2,452.50 | ▼ -161.60 after sell → book $8,517.25; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `GLXY` | 46 | $25.00 | $2.15 | $-48.21 | $3,600.13 | ▼ -48.21 after sell → book $8,515.11; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `MARA` | 87 | $13.07 | $2.28 | $-80.22 | $4,734.94 | ▼ -80.22 after sell → book $8,512.83; vs 09:30 mark -2.28 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `SION` | 202 | $5.50 | $2.65 | $-106.26 | $5,843.29 | ▼ -106.26 after sell → book $8,510.18; vs 09:30 mark -2.65 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $5,843.29 | ▲ close $8,548.29 vs 09:30 $8,521.31 (session +38.11) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,907.75 | ▲ 09:30 equity $8,200.78 vs yday $8,200.78 (-0.00) | 09:30 open · cash $7,907.75 (unchanged overnight, no fees) · equity $8,200.78 vs prior close $8,200.78 (-0.00) · 3 name(s) re-marked at the open (per-name table). PGEN×13 yday $7.70 → 09:30 $7.70 +0.00; SGRY×6 yday $14.20 → 09:30 $14.20 +0.00; VERI×81 yday $1.33 → 09:30 $1.33 +0.00 | — |
| 2026-09-25 09:30 ET | **BUY** | `ZSQR` | 409 | $3.86 | $5.28 | — | $6,323.73 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; leftover $1581.55 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ILMN` | 5 | $272.16 | $2.00 | — | $4,960.93 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+11.7; leftover $1581.55 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 97 | $16.21 | $2.28 | — | $3,386.28 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; leftover $1581.55 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `COST` | 1 | $887.00 | $1.99 | — | $2,497.28 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list earn_react; 🔵; ret5=+0.3; leftover $1581.55 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `RKLB` | 21 | $74.15 | $2.05 | — | $938.08 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; ret5=+8.5; leftover $1581.55 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $938.08 | ▼ close $8,150.97 vs 09:30 $8,200.78 (session -36.21) | 16:00 close · cash $938.08 · equity $8,150.97 vs 09:30 $8,200.78 (-49.81; session marks -36.21) · 8 name(s) marked open→close (per-name table). PGEN×13 09:30 $7.70 → close $7.70 -0.00; SGRY×6 09:30 $14.20 → close $14.20 -0.00; VERI×81 09:30 $1.33 → close $1.33 +0.00; ZSQR×409 09:30 $3.86 → close $3.78 -32.72; ILMN×5 09:30 $272.16 → close $270.00 -10.80; SECZ×97 09:30 $16.21 → close $15.96 -24.25; COST×1 09:30 $887.00 → close $922.76 +35.76; RKLB×21 09:30 $74.15 → close $73.95 -4.20 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `SNDK` | cash | leftover split 1250.00 < 1 share @ 1646.93 |
| 2026-08-17 | `HLIT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ARX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `MH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `VELO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `NRG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `S` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `HLIT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ANGX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ARX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `MH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `VELO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `NRG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `S` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `DVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `EOG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `FANG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `OUST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `DVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `EOG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `FANG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `OUST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `APA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ASST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `MRNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ZLAB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `TEAM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 26.45 < 1 share @ 119.43 |
| 2026-08-21 | `FUTU` | cash | leftover split 26.45 < 1 share @ 115.18 |
| 2026-08-21 | `GRAL` | cash | leftover split 26.45 < 1 share @ 78.88 |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `APA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ASST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `MRNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ZLAB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `TEAM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ABTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `HIVE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `MARA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ABUS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `NVAX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `ABTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `HIVE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `MARA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `AU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `FCX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `EZPW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `RUM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ZYME` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `FNV` | cash | leftover split 89.56 < 1 share @ 267.02 |
| 2026-08-26 | `CM` | cash | leftover split 89.56 < 1 share @ 118.50 |
| 2026-08-27 | `AU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `FCX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `EZPW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RUM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ZYME` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ACMR` | cash | leftover split 22.39 < 1 share @ 81.65 |
| 2026-08-27 | `MU` | cash | leftover split 22.39 < 1 share @ 967.01 |
| 2026-08-27 | `ASML` | cash | leftover split 22.39 < 1 share @ 1746.53 |
| 2026-08-27 | `CM` | cash | leftover split 22.39 < 1 share @ 118.77 |
| 2026-08-27 | `GEN` | cash | leftover split 22.39 < 1 share @ 29.83 |
| 2026-08-27 | `LRCX` | cash | leftover split 22.39 < 1 share @ 318.88 |
| 2026-08-27 | `NVDA` | cash | leftover split 22.39 < 1 share @ 222.86 |
| 2026-08-27 | `ADSK` | cash | leftover split 22.39 < 1 share @ 261.47 |
| 2026-08-28 | `MPWR` | cash | leftover split 1224.84 < 1 share @ 1306.03 |
| 2026-08-31 | `KEYS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CIEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `DDOG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ADSK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SEDG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `TLS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CDNS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `HAL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RBRK` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `KEYS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CIEN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `DDOG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ADSK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SEDG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `TLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `NVDA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DVN` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NOW` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NTNX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `BCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `EOLS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `RLMD` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `AVGO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DELL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CXW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MMED` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `HPE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CNXC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRM` | cash | leftover split 209.50 < 1 share @ 263.36 |
| 2026-09-04 | `BE` | cash | leftover split 209.50 < 1 share @ 236.82 |
| 2026-09-08 | `AVGO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `DELL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CXW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `FRNM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `MMED` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `DE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `HPE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CNXC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `BAK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `JCI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MSTR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `MRX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `BAK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `LITE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `HPE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ORCL` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `ORCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `ADBE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `AVTR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `BAK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `BE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `ORCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `ADBE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `BAK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TXG` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `WAY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `QCOM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `SM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `SMTC` | cash | leftover split 31.17 < 1 share @ 170.85 |
| 2026-09-17 | `JBHT` | cash | leftover split 31.17 < 1 share @ 238.60 |
| 2026-09-17 | `LITE` | cash | leftover split 31.17 < 1 share @ 934.88 |
| 2026-09-18 | `WAY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `QCOM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `SM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `TNDM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `GME` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `TNDM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `TH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `RARE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `BHVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `TNDM` | no_price | no 09:30 open — carry |
| 2026-09-22 | `TH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `RARE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `BHVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `VICR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `GLXY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `MARA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `SION` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `WBD` | no_price | no 09:30 open |
| 2026-09-23 | `VICR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `GLXY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `MARA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `SION` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `CTAS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `PGEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `SGRY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `VERI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `CMPX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `BLSH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `OKTA` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ACMR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `EOG` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ADCT` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `RRC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ZSQR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CVE` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `AMPL` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `CTAS` | 2 | 2026-09-23 @ $196.78 | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list earn_react; 🔵; ⚪; ret5=-2.0; leftover $484.59 |
| `PGEN` | 60 | 2026-09-23 @ $7.95 | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list flatten; 🔵; ⚪; ret5=+12.4; leftover $484.59 |
| `SGRY` | 30 | 2026-09-23 @ $15.72 | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $484.59 |
| `VERI` | 372 | 2026-09-23 @ $1.30 | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; 🔵; ret5=+15.3; leftover $484.59 |
| `CMPX` | 397 | 2026-09-23 @ $1.22 | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_mover; 🔵; ret5=-33.0; leftover $484.59 |
| `BLSH` | 12 | 2026-09-23 @ $40.00 | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; 🔵; ret5=+6.7; leftover $484.59 |
