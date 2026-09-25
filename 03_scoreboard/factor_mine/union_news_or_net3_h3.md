# Factor mine action — `union_news_or_net3_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · packet🟢 OR headline🟢 and camera net ≥ 3

Cash book **-23.73%** ($7,627) · signal-only (no cash/fees) was -21.14%. Starts YES **1/30**. Fills 120 · skips 162 · realized $-2181.08.

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
- Minimum hold is 3 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 3 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `news_or_headline=True,cam_net_min=3` · **rank** `cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $7,583.64.

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
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 94 | $13.18 | $2.27 | — | $8,758.81 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 290 | $4.31 | $3.74 | — | $7,505.17 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 63 | $19.57 | $2.18 | — | $6,270.08 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 92 | $13.55 | $2.27 | — | $5,021.21 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list yday_gainer; 🔵; ⚪; ret5=+17.5; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `VELO` | 81 | $15.38 | $2.23 | — | $3,773.20 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+16.3; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 10 | $120.00 | $2.02 | — | $2,571.18 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list flatten; 🔵; ret5=+0.6; leftover $1250.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `S` | 52 | $23.77 | $2.15 | — | $1,332.73 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot; 🔵; ret5=+14.8; leftover $1250.00 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,332.73 | ▲ close $10,120.33 vs 09:30 $10,000.00 (session +137.19) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,332.73 | ▲ 09:30 equity $10,155.37 vs yday $10,120.33 (+35.04) | — | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 7 | $46.18 | $2.01 | — | $1,007.46 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list flatten; 🔵; ret5=+6.7; leftover $333.18 | — |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 2 | $142.77 | $2.00 | — | $719.93 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list flatten; 🔵; ret5=+5.8; leftover $333.18 | — |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 1 | $202.70 | $1.99 | — | $515.23 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list flatten; 🔵; ret5=+8.3; leftover $333.18 | — |
| 2026-08-17 09:30 ET | **BUY** | `OUST` | 6 | $49.00 | $2.01 | — | $219.23 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list yday_gainer; ⚪; ret5=+12.2; leftover $333.18 | — |
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
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $8,644.97 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1228.77 | — |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 27 | $44.76 | $2.07 | — | $7,434.38 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list mover_buy; 🔵; ret5=+8.7; leftover $1228.77 | — |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 497 | $2.47 | $6.41 | — | $6,200.37 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $1228.77 | — |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 20 | $58.73 | $2.05 | — | $5,023.72 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.8; leftover $1228.77 | — |
| 2026-08-20 09:30 ET | **BUY** | `ASST` | 76 | $16.00 | $2.22 | — | $3,805.51 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+18.9; leftover $1228.77 | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 8 | $150.14 | $2.01 | — | $2,602.37 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1228.77 | — |
| 2026-08-20 09:30 ET | **BUY** | `ZLAB` | 46 | $26.57 | $2.13 | — | $1,378.02 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot; 🔵; ret5=+4.8; leftover $1228.77 | — |
| 2026-08-20 09:30 ET | **BUY** | `HUMA` | 1737 | $0.71 | $17.49 | — | $132.47 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+10.5; leftover $1228.77 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $132.47 | ▼ close $9,605.47 vs 09:30 $9,838.21 (session -188.24) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $132.47 | ▲ 09:30 equity $9,786.14 vs yday $9,605.47 (+180.67) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `ABTC` | 2 | $8.66 | $0.18 | — | $114.97 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+15.7; leftover $22.08 | — |
| 2026-08-21 09:30 ET | **BUY** | `HIVE` | 6 | $3.24 | $0.21 | — | $95.32 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list yday_gainer; 🔵; ret5=+21.3; leftover $22.08 | — |
| 2026-08-21 09:30 ET | **BUY** | `MARA` | 1 | $11.70 | $0.12 | — | $83.50 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+21.1; leftover $22.08 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $83.50 | ▲ close $9,806.87 vs 09:30 $9,786.14 (session +21.24) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $83.50 | ▲ 09:30 equity $9,830.17 vs yday $9,806.87 (+23.30) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $83.50 | ▼ close $9,796.15 vs 09:30 $9,830.17 (session -34.02) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $83.50 | ▼ 09:30 equity $9,778.44 vs yday $9,796.15 (-17.71) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 13 | $95.86 | $2.05 | $+58.97 | $1,327.63 | ▲ +58.97 after sell → book $9,776.39; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `APA` | 27 | $41.38 | $2.09 | $-95.42 | $2,442.80 | ▼ -95.42 after sell → book $9,774.30; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AUTL` | 497 | $2.38 | $6.50 | $-57.65 | $3,619.16 | ▼ -57.65 after sell → book $9,767.80; vs 09:30 mark -6.50 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 20 | $57.93 | $2.07 | $-20.12 | $4,775.69 | ▼ -20.12 after sell → book $9,765.73; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ASST` | 76 | $19.04 | $2.24 | $+226.58 | $6,220.49 | ▲ +226.58 after sell → book $9,763.49; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `MRNA` | 8 | $143.50 | $2.03 | $-57.17 | $7,366.45 | ▼ -57.17 after sell → book $9,761.45; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ZLAB` | 46 | $26.04 | $2.15 | $-28.66 | $8,562.14 | ▼ -28.66 after sell → book $9,759.31; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HUMA` | 1737 | $0.66 | $17.03 | $-110.95 | $9,696.75 | ▼ -110.95 after sell → book $9,742.28; vs 09:30 mark -17.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 27 | $118.52 | $2.07 | — | $6,494.64 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $3232.25 | — |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 41 | $77.13 | $2.11 | — | $3,330.20 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $3232.25 | — |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 92 | $35.05 | $2.27 | — | $103.33 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list yday_gainer; 🔵; ⚪; ret5=+19.7; leftover $3232.25 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $103.33 | ▲ close $10,000.76 vs 09:30 $9,778.44 (session +264.93) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $103.33 | ▼ 09:30 equity $9,922.21 vs yday $10,000.76 (-78.55) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `ABTC` | 2 | $8.84 | $0.20 | $-0.02 | $120.81 | ▼ -0.02 after sell → book $9,922.01; vs 09:30 mark -0.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `HIVE` | 6 | $2.95 | $0.21 | $-2.17 | $138.29 | ▼ -2.17 after sell → book $9,921.79; vs 09:30 mark -0.22 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `MARA` | 1 | $11.56 | $0.14 | $-0.40 | $149.71 | ▼ -0.40 after sell → book $9,921.65; vs 09:30 mark -0.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $149.71 | ▼ close $9,696.48 vs 09:30 $9,922.21 (session -225.17) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $149.71 | ▼ 09:30 equity $9,633.81 vs yday $9,696.48 (-62.67) | — | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $149.71 | ▲ close $9,727.45 vs 09:30 $9,633.81 (session +93.64) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $149.71 | ▲ 09:30 equity $9,763.21 vs yday $9,727.45 (+35.76) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `AU` | 27 | $119.19 | $2.11 | $+13.91 | $3,365.74 | ▲ +13.91 after sell → book $9,761.11; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `FCX` | 41 | $78.57 | $2.15 | $+54.78 | $6,584.96 | ▲ +54.78 after sell → book $9,758.96; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `EZPW` | 92 | $34.50 | $2.31 | $-55.17 | $9,756.65 | ▼ -55.17 after sell → book $9,756.65; vs 09:30 mark -2.31 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 3 | $324.41 | $2.00 | — | $8,781.42 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1219.58 | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 8 | $141.76 | $2.01 | — | $7,645.33 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1219.58 | — |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 3 | $400.42 | $2.00 | — | $6,442.07 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1219.58 | — |
| 2026-08-28 09:30 ET | **BUY** | `DDOG` | 5 | $240.22 | $2.00 | — | $5,238.96 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list mover_buy; 🔵; ret5=+4.5; leftover $1219.58 | — |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 4 | $261.16 | $2.00 | — | $4,192.32 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list earn_react; ret5=+7.8; leftover $1219.58 | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 37 | $32.90 | $2.10 | — | $2,972.92 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1219.58 | — |
| 2026-08-28 09:30 ET | **BUY** | `TLS` | 253 | $4.82 | $3.26 | — | $1,750.20 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot; 🔵; ret5=+8.8; leftover $1219.58 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,750.20 | ▼ close $9,496.37 vs 09:30 $9,763.21 (session -244.90) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,750.20 | ▼ 09:30 equity $9,481.53 vs yday $9,496.37 (-14.84) | — | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,750.20 | ▲ close $9,560.56 vs 09:30 $9,481.53 (session +79.02) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,750.20 | ▼ 09:30 equity $9,430.64 vs yday $9,560.56 (-129.92) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,750.20 | ▼ close $9,353.41 vs 09:30 $9,430.64 (session -77.23) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,750.20 | ▼ 09:30 equity $9,320.40 vs yday $9,353.41 (-33.01) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `KEYS` | 3 | $318.04 | $2.02 | $-23.13 | $2,702.30 | ▼ -23.13 after sell → book $9,318.38; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SMTC` | 8 | $133.00 | $2.03 | $-74.13 | $3,764.26 | ▼ -74.13 after sell → book $9,316.34; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CIEN` | 3 | $357.25 | $2.02 | $-133.53 | $4,834.00 | ▼ -133.53 after sell → book $9,314.33; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `DDOG` | 5 | $219.46 | $2.02 | $-107.83 | $5,929.27 | ▼ -107.83 after sell → book $9,312.30; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ADSK` | 4 | $246.70 | $2.02 | $-61.86 | $6,914.05 | ▼ -61.86 after sell → book $9,310.28; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SEDG` | 37 | $32.42 | $2.12 | $-21.98 | $8,111.47 | ▼ -21.98 after sell → book $9,308.16; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `TLS` | 253 | $4.73 | $3.32 | $-29.35 | $9,304.84 | ▼ -29.35 after sell → book $9,304.84; vs 09:30 mark -3.32 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,304.84 | ▲ close $9,304.84 vs 09:30 $9,320.40 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,304.84 | ▲ 09:30 equity $9,304.84 vs yday $9,304.84 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 3 | $351.74 | $2.00 | — | $8,247.62 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $1329.26 | — |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 2 | $486.31 | $2.00 | — | $7,273.01 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list mover_buy; 🔵; ret5=+6.1; leftover $1329.26 | — |
| 2026-09-03 09:30 ET | **BUY** | `CXW` | 41 | $32.31 | $2.11 | — | $5,946.18 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list mover_buy; 🔵; ⚪; ret5=-4.1; leftover $1329.26 | — |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 83 | $15.87 | $2.24 | — | $4,626.73 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1329.26 | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 55 | $23.88 | $2.15 | — | $3,311.18 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1329.26 | — |
| 2026-09-03 09:30 ET | **BUY** | `DE` | 1 | $703.25 | $1.99 | — | $2,605.94 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.1; leftover $1329.26 | — |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 27 | $47.60 | $2.07 | — | $1,318.67 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list earn_react; 🔵; ret5=-6.2; leftover $1329.26 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,318.67 | ▲ close $9,681.18 vs 09:30 $9,304.84 (session +390.90) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,318.67 | ▼ 09:30 equity $9,615.57 vs yday $9,681.18 (-65.61) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 1 | $263.36 | $1.99 | — | $1,053.31 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $329.67 | — |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 4 | $75.65 | $2.00 | — | $748.71 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+6.0; leftover $329.67 | — |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 1 | $236.82 | $1.99 | — | $509.90 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot; ret5=+8.1; leftover $329.67 | — |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 169 | $1.94 | $2.50 | — | $179.54 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list yday_gainer,yday_mover; ret5=+18.3; leftover $329.67 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $179.54 | ▼ close $9,601.43 vs 09:30 $9,615.57 (session -5.65) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $179.54 | ▲ 09:30 equity $9,653.93 vs yday $9,601.43 (+52.50) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $179.54 | ▲ close $9,751.00 vs 09:30 $9,653.93 (session +97.07) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $179.54 | ▲ 09:30 equity $9,784.05 vs yday $9,751.00 (+33.05) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `AVGO` | 3 | $366.23 | $2.02 | $+39.45 | $1,276.21 | ▲ +39.45 after sell → book $9,782.03; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `DELL` | 2 | $538.47 | $2.02 | $+100.31 | $2,351.14 | ▲ +100.31 after sell → book $9,780.01; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CXW` | 41 | $35.09 | $2.13 | $+109.73 | $3,787.69 | ▲ +109.73 after sell → book $9,777.88; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `FRNM` | 83 | $15.96 | $2.26 | $+2.97 | $5,110.11 | ▲ +2.97 after sell → book $9,775.61; vs 09:30 mark -2.27 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MMED` | 55 | $23.22 | $2.18 | $-40.63 | $6,385.03 | ▼ -40.63 after sell → book $9,773.44; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `DE` | 1 | $681.32 | $2.01 | $-25.94 | $7,064.34 | ▼ -25.94 after sell → book $9,771.42; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `HPE` | 27 | $56.94 | $2.09 | $+248.02 | $8,599.63 | ▲ +248.02 after sell → book $9,769.33; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,599.63 | ▼ close $9,748.88 vs 09:30 $9,784.05 (session -20.46) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,599.63 | ▼ 09:30 equity $9,738.61 vs yday $9,748.88 (-10.27) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `CRM` | 1 | $245.35 | $2.01 | $-22.02 | $8,842.96 | ▼ -22.02 after sell → book $9,736.60; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `MRX` | 4 | $75.00 | $2.02 | $-6.62 | $9,140.94 | ▼ -6.62 after sell → book $9,734.58; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BE` | 1 | $260.71 | $2.01 | $+19.88 | $9,399.64 | ▲ +19.88 after sell → book $9,732.57; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BAK` | 169 | $1.97 | $2.54 | $+0.04 | $9,730.03 | ▲ +0.04 after sell → book $9,730.03; vs 09:30 mark -2.54 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,730.03 | ▲ close $9,730.03 vs 09:30 $9,738.61 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,730.03 | ▲ 09:30 equity $9,730.03 vs yday $9,730.03 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 59 | $164.43 | $2.17 | — | $26.49 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $9730.03 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $26.49 | ▼ close $8,893.01 vs 09:30 $9,730.03 (session -834.85) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $26.49 | ▼ 09:30 equity $8,370.27 vs yday $8,893.01 (-522.74) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $26.49 | ▲ close $8,569.10 vs 09:30 $8,370.27 (session +198.83) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $26.49 | ▼ 09:30 equity $8,490.63 vs yday $8,569.10 (-78.47) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $26.49 | ▼ close $8,307.14 vs 09:30 $8,490.63 (session -183.49) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $26.49 | ▼ 09:30 equity $8,288.26 vs yday $8,307.14 (-18.88) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `ORCL` | 59 | $140.03 | $2.24 | $-1444.01 | $8,286.02 | ▼ -1,444.01 after sell → book $8,286.02; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 78 | $26.27 | $2.22 | — | $6,234.74 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list yday_gainer; 🔵; ret5=+10.0; leftover $2071.51 | — |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 10 | $189.17 | $2.02 | — | $4,341.02 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot; 🔵; ret5=+7.9; leftover $2071.51 | — |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 51 | $39.99 | $2.14 | — | $2,299.38 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot; 🔵; ret5=+9.3; leftover $2071.51 | — |
| 2026-09-16 09:30 ET | **BUY** | `AVTR` | 133 | $15.53 | $2.39 | — | $231.51 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot; ret5=+4.9; leftover $2071.51 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $231.51 | ▼ close $8,176.22 vs 09:30 $8,288.26 (session -101.03) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $231.51 | ▲ 09:30 equity $8,221.59 vs yday $8,176.22 (+45.37) | — | — |
| 2026-09-17 09:30 ET | **BUY** | `GME` | 2 | $22.12 | $0.45 | — | $186.82 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot; 🔵; ret5=+10.5; leftover $57.88 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $186.82 | ▼ close $8,182.09 vs 09:30 $8,221.59 (session -39.05) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $186.82 | ▲ 09:30 equity $8,239.20 vs yday $8,182.09 (+57.11) | — | — |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 2 | $20.91 | $0.42 | — | $144.57 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $62.27 | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 4 | $14.79 | $0.60 | — | $84.81 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $62.27 | — |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 4 | $14.07 | $0.57 | — | $27.95 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $62.27 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $27.95 | ▼ close $7,956.44 vs 09:30 $8,239.20 (session -281.15) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $27.95 | ▼ 09:30 equity $7,938.43 vs yday $7,956.44 (-18.01) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `WAY` | 78 | $25.94 | $2.25 | $-30.22 | $2,049.02 | ▼ -30.22 after sell → book $7,936.18; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `QCOM` | 10 | $180.61 | $2.04 | $-89.66 | $3,853.08 | ▼ -89.66 after sell → book $7,934.14; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `SM` | 51 | $35.91 | $2.17 | $-212.39 | $5,682.32 | ▼ -212.39 after sell → book $7,931.97; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `AVTR` | 133 | $15.39 | $2.43 | $-23.44 | $7,726.76 | ▼ -23.44 after sell → book $7,929.54; vs 09:30 mark -2.43 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **BUY** | `VICR` | 8 | $230.25 | $2.01 | — | $5,882.75 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot; ret5=+12.5; leftover $1931.69 | — |
| 2026-09-21 09:30 ET | **BUY** | `SMTC` | 10 | $190.30 | $2.02 | — | $3,977.73 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot; ret5=+10.6; leftover $1931.69 | — |
| 2026-09-21 09:30 ET | **BUY** | `GLXY` | 74 | $25.95 | $2.21 | — | $2,055.22 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $1931.69 | — |
| 2026-09-21 09:30 ET | **BUY** | `MARA` | 138 | $13.94 | $2.40 | — | $129.09 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+10.5; leftover $1931.69 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $129.09 | ▼ close $7,659.37 vs 09:30 $7,938.43 (session -261.52) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $129.09 | ▼ 09:30 equity $7,640.67 vs yday $7,659.37 (-18.70) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `GME` | 2 | $23.50 | $0.50 | $+1.82 | $175.60 | ▲ +1.82 after sell → book $7,640.18; vs 09:30 mark -0.49 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $175.60 | ▲ close $7,712.52 vs 09:30 $7,640.67 (session +72.34) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $175.60 | ▲ 09:30 equity $8,038.88 vs yday $7,712.52 (+326.36) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `TH` | 2 | $21.15 | $0.45 | $-0.39 | $217.45 | ▼ -0.39 after sell → book $8,038.43; vs 09:30 mark -0.45 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `RARE` | 4 | $15.40 | $0.65 | $+1.19 | $278.40 | ▲ +1.19 after sell → book $8,037.78; vs 09:30 mark -0.65 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `BHVN` | 4 | $14.84 | $0.63 | $+1.88 | $337.13 | ▲ +1.88 after sell → book $8,037.15; vs 09:30 mark -0.63 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **BUY** | `PGEN` | 8 | $7.95 | $0.66 | — | $272.87 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list flatten; 🔵; ⚪; ret5=+12.4; leftover $67.43 | — |
| 2026-09-23 09:30 ET | **BUY** | `SGRY` | 4 | $15.72 | $0.64 | — | $209.35 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $67.43 | — |
| 2026-09-23 09:30 ET | **BUY** | `VERI` | 51 | $1.30 | $0.82 | — | $142.24 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot; 🔵; ret5=+15.3; leftover $67.43 | — |
| 2026-09-23 09:30 ET | **BUY** | `BLSH` | 1 | $40.00 | $0.40 | — | $101.83 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot; 🔵; ret5=+6.7; leftover $67.43 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $101.83 | ▲ close $8,034.83 vs 09:30 $8,038.88 (session +0.20) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $101.83 | ▼ 09:30 equity $7,813.00 vs yday $8,034.83 (-221.83) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `VICR` | 8 | $274.61 | $2.04 | $+350.82 | $2,296.67 | ▲ +350.82 after sell → book $7,810.96; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `SMTC` | 10 | $164.04 | $2.04 | $-266.66 | $3,935.03 | ▼ -266.66 after sell → book $7,808.92; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `GLXY` | 74 | $25.00 | $2.24 | $-75.12 | $5,782.42 | ▼ -75.12 after sell → book $7,806.68; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `MARA` | 138 | $13.07 | $2.44 | $-124.91 | $7,583.64 | ▼ -124.91 after sell → book $7,804.24; vs 09:30 mark -2.44 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,583.64 | ▲ close $7,809.56 vs 09:30 $7,813.00 (session +5.32) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,167.56 | ▲ 09:30 equity $7,645.49 vs yday $7,645.49 (-0.00) | 09:30 open · cash $7,167.56 (unchanged overnight, no fees) · equity $7,645.49 vs prior close $7,645.49 (-0.00) · 3 name(s) re-marked at the open (per-name table). PGEN×21 yday $7.70 → 09:30 $7.70 +0.00; SGRY×10 yday $14.20 → 09:30 $14.20 +0.00; VERI×131 yday $1.33 → 09:30 $1.33 +0.00 | — |
| 2026-09-25 09:30 ET | **BUY** | `ZSQR` | 464 | $3.86 | $5.99 | — | $5,370.53 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; leftover $1791.89 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ILMN` | 6 | $272.16 | $2.01 | — | $3,735.57 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+11.7; leftover $1791.89 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 110 | $16.21 | $2.32 | — | $1,950.15 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; leftover $1791.89 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `COST` | 2 | $887.00 | $2.00 | — | $174.15 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list earn_react; 🔵; ret5=+0.3; leftover $1791.89 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $174.15 | ▼ close $7,627.13 vs 09:30 $7,645.49 (session -6.05) | 16:00 close · cash $174.15 · equity $7,627.13 vs 09:30 $7,645.49 (-18.36; session marks -6.05) · 7 name(s) marked open→close (per-name table). PGEN×21 09:30 $7.70 → close $7.70 -0.00; SGRY×10 09:30 $14.20 → close $14.20 -0.00; VERI×131 09:30 $1.33 → close $1.33 +0.00; ZSQR×464 09:30 $3.86 → close $3.78 -37.12; ILMN×6 09:30 $272.16 → close $270.00 -12.96; SECZ×110 09:30 $16.21 → close $15.96 -27.50; COST×2 09:30 $887.00 → close $922.76 +71.53 | — |

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
| 2026-08-19 | `DVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `EOG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `FANG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `OUST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `APA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ASST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `MRNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ZLAB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HUMA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 22.08 < 1 share @ 119.43 |
| 2026-08-21 | `FUTU` | cash | leftover split 22.08 < 1 share @ 115.18 |
| 2026-08-21 | `GRAL` | cash | leftover split 22.08 < 1 share @ 78.88 |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `APA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ASST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `MRNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ZLAB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HUMA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ABTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `HIVE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `MARA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ABUS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `ABTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `HIVE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `MARA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `AU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `FCX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `EZPW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `FNV` | cash | leftover split 74.86 < 1 share @ 267.02 |
| 2026-08-26 | `CM` | cash | leftover split 74.86 < 1 share @ 118.50 |
| 2026-08-27 | `AU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `FCX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `EZPW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ACMR` | cash | leftover split 18.71 < 1 share @ 81.65 |
| 2026-08-27 | `MU` | cash | leftover split 18.71 < 1 share @ 967.01 |
| 2026-08-27 | `ASML` | cash | leftover split 18.71 < 1 share @ 1746.53 |
| 2026-08-27 | `CM` | cash | leftover split 18.71 < 1 share @ 118.77 |
| 2026-08-27 | `GEN` | cash | leftover split 18.71 < 1 share @ 29.83 |
| 2026-08-27 | `LRCX` | cash | leftover split 18.71 < 1 share @ 318.88 |
| 2026-08-27 | `NVDA` | cash | leftover split 18.71 < 1 share @ 222.86 |
| 2026-08-27 | `ADSK` | cash | leftover split 18.71 < 1 share @ 261.47 |
| 2026-08-28 | `MPWR` | cash | leftover split 1219.58 < 1 share @ 1306.03 |
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
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `BCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `EOLS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `AVGO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DELL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CXW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MMED` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `HPE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `AVGO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `DELL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CXW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `FRNM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `MMED` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `DE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `HPE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CRM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `BE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `BAK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `JCI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `CRM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `MRX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `BE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `BAK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `LITE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `HPE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ORCL` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `ORCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `BE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `ORCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `WAY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `QCOM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `SM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `SMTC` | cash | leftover split 57.88 < 1 share @ 170.85 |
| 2026-09-17 | `JBHT` | cash | leftover split 57.88 < 1 share @ 238.60 |
| 2026-09-17 | `LITE` | cash | leftover split 57.88 < 1 share @ 934.88 |
| 2026-09-18 | `WAY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `QCOM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `SM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `AVTR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `GME` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `TH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `RARE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `BHVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `TH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `RARE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `BHVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `VICR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `GLXY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `MARA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `VICR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `GLXY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `MARA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `CTAS` | cash | leftover split 67.43 < 1 share @ 196.78 |
| 2026-09-24 | `PGEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `SGRY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `VERI` | min_hold | dropped but min-hold 1/3 sess — no sell |
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
| `PGEN` | 8 | 2026-09-23 @ $7.95 | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list flatten; 🔵; ⚪; ret5=+12.4; leftover $67.43 |
| `SGRY` | 4 | 2026-09-23 @ $15.72 | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $67.43 |
| `VERI` | 51 | 2026-09-23 @ $1.30 | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot; 🔵; ret5=+15.3; leftover $67.43 |
| `BLSH` | 1 | 2026-09-23 @ $40.00 | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot; 🔵; ret5=+6.7; leftover $67.43 |
