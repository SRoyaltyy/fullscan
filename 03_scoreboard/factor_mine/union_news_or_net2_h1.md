# Factor mine action — `union_news_or_net2_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · packet🟢 OR headline🟢 and camera net ≥ 2

Cash book **-14.09%** ($8,591) · signal-only (no cash/fees) was -3.45%. Starts YES **0/30**. Fills 171 · skips 55 · realized $-720.09.

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
- Minimum hold is 1 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 1 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `news_or_headline=True,cam_net_min=2` · **rank** `cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,279.92.

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
| 2026-08-17 09:30 ET | **SELL** | `HLIT` | 94 | $13.84 | $2.30 | $+57.47 | $2,631.40 | ▲ +57.47 after sell → book $10,153.08; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 290 | $4.60 | $3.80 | $+76.56 | $3,961.60 | ▲ +76.56 after sell → book $10,149.28; vs 09:30 mark -3.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 63 | $19.57 | $2.20 | $-4.38 | $5,192.31 | ▼ -4.38 after sell → book $10,147.08; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MH` | 92 | $13.16 | $2.29 | $-40.44 | $6,400.73 | ▼ -40.44 after sell → book $10,144.78; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VELO` | 81 | $16.05 | $2.26 | $+49.78 | $7,698.53 | ▲ +49.78 after sell → book $10,142.53; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NRG` | 10 | $127.40 | $2.04 | $+69.94 | $8,970.49 | ▲ +69.94 after sell → book $10,140.49; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `S` | 52 | $22.50 | $2.17 | $-70.61 | $10,138.32 | ▼ -70.61 after sell → book $10,138.32; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 54 | $46.18 | $2.15 | — | $7,642.45 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list flatten; 🔵; ret5=+6.7; leftover $2534.58 | — |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 17 | $142.77 | $2.04 | — | $5,213.32 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list flatten; 🔵; ret5=+5.8; leftover $2534.58 | — |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 12 | $202.70 | $2.03 | — | $2,778.89 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list flatten; 🔵; ret5=+8.3; leftover $2534.58 | — |
| 2026-08-17 09:30 ET | **BUY** | `OUST` | 51 | $49.00 | $2.14 | — | $277.75 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer; ⚪; ret5=+12.2; leftover $2534.58 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $277.75 | ▲ close $10,261.19 vs 09:30 $10,155.37 (session +131.23) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $277.75 | ▼ 09:30 equity $10,193.18 vs yday $10,261.19 (-68.01) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 54 | $48.00 | $2.18 | $+93.95 | $2,867.57 | ▲ +93.95 after sell → book $10,191.00; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 17 | $148.04 | $2.07 | $+85.48 | $5,382.18 | ▲ +85.48 after sell → book $10,188.93; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 12 | $208.93 | $2.06 | $+70.68 | $7,887.28 | ▲ +70.68 after sell → book $10,186.87; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `OUST` | 51 | $45.09 | $2.17 | $-203.72 | $10,184.70 | ▼ -203.72 after sell → book $10,184.70; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,184.70 | ▲ close $10,184.70 vs 09:30 $10,193.18 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,184.70 | ▲ 09:30 equity $10,184.70 vs yday $10,184.70 (-0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,184.70 | ▲ close $10,184.70 vs 09:30 $10,184.70 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,184.70 | ▲ 09:30 equity $10,184.70 vs yday $10,184.70 (-0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $8,999.54 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1273.09 | — |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 28 | $44.76 | $2.07 | — | $7,744.19 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list mover_buy; 🔵; ret5=+8.7; leftover $1273.09 | — |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 515 | $2.47 | $6.64 | — | $6,465.49 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $1273.09 | — |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 21 | $58.73 | $2.05 | — | $5,230.11 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.8; leftover $1273.09 | — |
| 2026-08-20 09:30 ET | **BUY** | `ASST` | 79 | $16.00 | $2.23 | — | $3,963.88 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+18.9; leftover $1273.09 | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 8 | $150.14 | $2.01 | — | $2,760.75 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1273.09 | — |
| 2026-08-20 09:30 ET | **BUY** | `ZLAB` | 47 | $26.57 | $2.13 | — | $1,509.83 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; 🔵; ret5=+4.8; leftover $1273.09 | — |
| 2026-08-20 09:30 ET | **BUY** | `TEAM` | 7 | $173.90 | $2.01 | — | $290.52 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; 🔵; ret5=+12.2; leftover $1273.09 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $290.52 | ▼ close $10,026.19 vs 09:30 $10,184.70 (session -137.33) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $290.52 | ▲ 09:30 equity $10,220.92 vs yday $10,026.19 (+194.73) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $1,532.83 | ▲ +57.15 after sell → book $10,218.87; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `APA` | 28 | $44.52 | $2.09 | $-10.89 | $2,777.29 | ▼ -10.89 after sell → book $10,216.77; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ASST` | 79 | $17.66 | $2.25 | $+126.66 | $4,170.18 | ▲ +126.66 after sell → book $10,214.52; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `MRNA` | 8 | $133.11 | $2.03 | $-140.29 | $5,233.03 | ▼ -140.29 after sell → book $10,212.49; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ZLAB` | 47 | $26.25 | $2.15 | $-19.32 | $6,464.63 | ▼ -19.32 after sell → book $10,210.34; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `TEAM` | 7 | $174.22 | $2.03 | $-1.80 | $7,682.14 | ▼ -1.80 after sell → book $10,208.31; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $6,485.82 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1280.36 | — |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 11 | $115.18 | $2.02 | — | $5,216.81 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $1280.36 | — |
| 2026-08-21 09:30 ET | **BUY** | `GRAL` | 16 | $78.88 | $2.04 | — | $3,952.69 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+14.3; leftover $1280.36 | — |
| 2026-08-21 09:30 ET | **BUY** | `ABTC` | 147 | $8.66 | $2.43 | — | $2,677.24 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+15.7; leftover $1280.36 | — |
| 2026-08-21 09:30 ET | **BUY** | `HIVE` | 395 | $3.24 | $5.10 | — | $1,392.35 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer; 🔵; ret5=+21.3; leftover $1280.36 | — |
| 2026-08-21 09:30 ET | **BUY** | `MARA` | 109 | $11.70 | $2.32 | — | $114.73 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+21.1; leftover $1280.36 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $114.73 | ▼ close $10,040.16 vs 09:30 $10,220.92 (session -152.22) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $114.73 | ▼ 09:30 equity $10,005.08 vs yday $10,040.16 (-35.08) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 515 | $2.40 | $6.74 | $-49.43 | $1,343.99 | ▼ -49.43 after sell → book $9,998.34; vs 09:30 mark -6.74 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.51 | $2.04 | $+6.74 | $2,547.05 | ▲ +6.74 after sell → book $9,996.30; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 11 | $121.00 | $2.04 | $+59.95 | $3,876.01 | ▲ +59.95 after sell → book $9,994.26; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `GRAL` | 16 | $81.87 | $2.06 | $+43.74 | $5,183.87 | ▲ +43.74 after sell → book $9,992.20; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ABTC` | 147 | $8.00 | $2.47 | $-101.92 | $6,357.40 | ▼ -101.92 after sell → book $9,989.73; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `HIVE` | 395 | $2.99 | $5.17 | $-109.02 | $7,533.28 | ▼ -109.02 after sell → book $9,984.56; vs 09:30 mark -5.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MARA` | 109 | $11.17 | $2.35 | $-62.43 | $8,748.47 | ▼ -62.43 after sell → book $9,982.22; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,748.47 | ▼ close $9,947.04 vs 09:30 $10,005.08 (session -35.17) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,748.47 | ▲ 09:30 equity $9,965.00 vs yday $9,947.04 (+17.96) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 21 | $57.93 | $2.07 | $-20.93 | $9,962.93 | ▼ -20.93 after sell → book $9,962.93; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 16 | $118.52 | $2.04 | — | $8,064.57 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $1992.59 | — |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 25 | $77.13 | $2.06 | — | $6,134.25 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $1992.59 | — |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 56 | $35.05 | $2.16 | — | $4,169.29 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer; 🔵; ⚪; ret5=+19.7; leftover $1992.59 | — |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 211 | $9.42 | $2.72 | — | $2,178.95 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+13.6; leftover $1992.59 | — |
| 2026-08-25 09:30 ET | **BUY** | `ZYME` | 69 | $28.86 | $2.20 | — | $185.42 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; 🔵; ret5=+13.7; leftover $1992.59 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $185.42 | ▲ close $10,184.25 vs 09:30 $9,965.00 (session +232.50) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $185.42 | ▼ 09:30 equity $10,111.33 vs yday $10,184.25 (-72.92) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 16 | $119.80 | $2.06 | $+16.38 | $2,100.15 | ▲ +16.38 after sell → book $10,109.26; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 25 | $79.34 | $2.09 | $+51.09 | $4,081.56 | ▲ +51.09 after sell → book $10,107.17; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EZPW` | 56 | $35.70 | $2.18 | $+32.06 | $6,078.58 | ▲ +32.06 after sell → book $10,104.99; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `RUM` | 211 | $10.07 | $2.77 | $+131.65 | $8,200.57 | ▲ +131.65 after sell → book $10,102.21; vs 09:30 mark -2.78 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZYME` | 69 | $27.56 | $2.22 | $-94.12 | $10,099.99 | ▼ -94.12 after sell → book $10,099.99; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `FNV` | 18 | $267.02 | $2.04 | — | $5,291.59 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; 🔵; ret5=+16.7; leftover $5049.99 | — |
| 2026-08-26 09:30 ET | **BUY** | `CM` | 42 | $118.50 | $2.12 | — | $312.47 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list overnight,overnight_mega; 🔵; ret5=-2.7; leftover $5049.99 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $312.47 | ▼ close $10,089.53 vs 09:30 $10,111.33 (session -6.30) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $312.47 | ▲ 09:30 equity $10,110.95 vs yday $10,089.53 (+21.42) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `FNV` | 18 | $267.23 | $2.09 | $-0.36 | $5,120.52 | ▼ -0.36 after sell → book $10,108.86; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 8 | $81.65 | $2.01 | — | $4,465.30 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list mover_buy; 🔵; ret5=+2.0; leftover $731.50 | — |
| 2026-08-27 09:30 ET | **BUY** | `GEN` | 24 | $29.83 | $2.06 | — | $3,747.32 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list mover_buy; 🔵; ret5=+7.6; leftover $731.50 | — |
| 2026-08-27 09:30 ET | **BUY** | `LRCX` | 2 | $318.88 | $2.00 | — | $3,107.56 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $731.50 | — |
| 2026-08-27 09:30 ET | **BUY** | `NVDA` | 3 | $222.86 | $2.00 | — | $2,436.99 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list mover_buy; 🔵; ret5=-3.6; leftover $731.50 | — |
| 2026-08-27 09:30 ET | **BUY** | `ADSK` | 2 | $261.47 | $2.00 | — | $1,912.05 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list overnight,overnight_mega; 🔵; ret5=+1.4; leftover $731.50 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,912.05 | ▼ close $9,973.51 vs 09:30 $10,110.95 (session -125.28) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,912.05 | ▲ 09:30 equity $9,976.39 vs yday $9,973.51 (+2.88) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `CM` | 42 | $115.66 | $2.16 | $-123.56 | $6,767.61 | ▼ -123.56 after sell → book $9,974.23; vs 09:30 mark -2.16 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 8 | $79.27 | $2.03 | $-23.09 | $7,399.73 | ▼ -23.09 after sell → book $9,972.19; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GEN` | 24 | $30.50 | $2.08 | $+11.94 | $8,129.65 | ▲ +11.94 after sell → book $9,970.11; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `LRCX` | 2 | $318.03 | $2.02 | $-5.71 | $8,763.69 | ▼ -5.71 after sell → book $9,968.09; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `NVDA` | 3 | $227.36 | $2.02 | $+9.48 | $9,443.75 | ▲ +9.48 after sell → book $9,966.07; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 4 | $324.41 | $2.00 | — | $8,144.11 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1349.11 | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 9 | $141.76 | $2.02 | — | $6,866.26 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1349.11 | — |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 3 | $400.42 | $2.00 | — | $5,663.00 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1349.11 | — |
| 2026-08-28 09:30 ET | **BUY** | `MPWR` | 1 | $1306.03 | $1.99 | — | $4,354.97 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list mover_buy; 🔵; ret5=+0.0; leftover $1349.11 | — |
| 2026-08-28 09:30 ET | **BUY** | `DDOG` | 5 | $240.22 | $2.00 | — | $3,151.87 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list mover_buy; 🔵; ret5=+4.5; leftover $1349.11 | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 41 | $32.90 | $2.11 | — | $1,800.86 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1349.11 | — |
| 2026-08-28 09:30 ET | **BUY** | `TLS` | 279 | $4.82 | $3.60 | — | $452.48 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; 🔵; ret5=+8.8; leftover $1349.11 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $452.48 | ▼ close $9,634.91 vs 09:30 $9,976.39 (session -315.44) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $452.48 | ▼ 09:30 equity $9,634.74 vs yday $9,634.91 (-0.17) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `ADSK` | 2 | $257.71 | $2.02 | $-11.53 | $965.88 | ▼ -11.53 after sell → book $9,632.72; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 4 | $322.49 | $2.02 | $-11.70 | $2,253.82 | ▼ -11.70 after sell → book $9,630.70; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 9 | $132.30 | $2.04 | $-89.19 | $3,442.48 | ▼ -89.19 after sell → book $9,628.67; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CIEN` | 3 | $378.44 | $2.02 | $-69.96 | $4,575.78 | ▼ -69.96 after sell → book $9,626.65; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MPWR` | 1 | $1261.90 | $2.01 | $-48.14 | $5,835.67 | ▼ -48.14 after sell → book $9,624.63; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `DDOG` | 5 | $233.97 | $2.02 | $-35.31 | $7,003.47 | ▼ -35.31 after sell → book $9,622.61; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 41 | $31.15 | $2.13 | $-76.00 | $8,278.49 | ▼ -76.00 after sell → book $9,620.48; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TLS` | 279 | $4.81 | $3.66 | $-10.05 | $9,616.82 | ▼ -10.05 after sell → book $9,616.82; vs 09:30 mark -3.66 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,616.82 | ▲ close $9,616.82 vs 09:30 $9,634.74 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,616.82 | ▲ 09:30 equity $9,616.82 vs yday $9,616.82 (-0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,616.82 | ▲ close $9,616.82 vs 09:30 $9,616.82 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,616.82 | ▲ 09:30 equity $9,616.82 vs yday $9,616.82 (-0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,616.82 | ▲ close $9,616.82 vs 09:30 $9,616.82 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,616.82 | ▲ 09:30 equity $9,616.82 vs yday $9,616.82 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 3 | $351.74 | $2.00 | — | $8,559.60 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $1202.10 | — |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 2 | $486.31 | $2.00 | — | $7,584.98 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list mover_buy; 🔵; ret5=+6.1; leftover $1202.10 | — |
| 2026-09-03 09:30 ET | **BUY** | `CXW` | 37 | $32.31 | $2.10 | — | $6,387.41 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list mover_buy; 🔵; ⚪; ret5=-4.1; leftover $1202.10 | — |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 75 | $15.87 | $2.21 | — | $5,194.95 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1202.10 | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 50 | $23.88 | $2.14 | — | $3,998.81 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1202.10 | — |
| 2026-09-03 09:30 ET | **BUY** | `DE` | 1 | $703.25 | $1.99 | — | $3,293.57 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.1; leftover $1202.10 | — |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 25 | $47.60 | $2.06 | — | $2,101.50 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list earn_react; 🔵; ret5=-6.2; leftover $1202.10 | — |
| 2026-09-03 09:30 ET | **BUY** | `CNXC` | 36 | $32.88 | $2.10 | — | $915.72 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer; 🔵; ret5=+16.2; leftover $1202.10 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $915.72 | ▲ close $9,962.91 vs 09:30 $9,616.82 (session +362.70) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $915.72 | ▼ 09:30 equity $9,889.96 vs yday $9,962.91 (-72.95) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 3 | $359.70 | $2.02 | $+19.86 | $1,992.80 | ▲ +19.86 after sell → book $9,887.94; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DELL` | 2 | $513.78 | $2.02 | $+50.93 | $3,018.35 | ▲ +50.93 after sell → book $9,885.93; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CXW` | 37 | $33.46 | $2.12 | $+38.33 | $4,254.25 | ▲ +38.33 after sell → book $9,883.81; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 50 | $23.84 | $2.16 | $-6.30 | $5,444.09 | ▼ -6.30 after sell → book $9,881.65; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DE` | 1 | $692.03 | $2.01 | $-15.23 | $6,134.10 | ▼ -15.23 after sell → book $9,879.63; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `HPE` | 25 | $53.85 | $2.09 | $+152.10 | $7,478.27 | ▲ +152.10 after sell → book $9,877.55; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CNXC` | 36 | $32.48 | $2.12 | $-18.62 | $8,645.43 | ▼ -18.62 after sell → book $9,875.43; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 8 | $263.36 | $2.01 | — | $6,536.54 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $2161.36 | — |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 28 | $75.65 | $2.07 | — | $4,416.26 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+6.0; leftover $2161.36 | — |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 9 | $236.82 | $2.02 | — | $2,282.86 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; ret5=+8.1; leftover $2161.36 | — |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 1114 | $1.94 | $14.37 | — | $107.33 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer,yday_mover; ret5=+18.3; leftover $2161.36 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $107.33 | ▲ close $9,977.27 vs 09:30 $9,889.96 (session +122.32) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $107.33 | ▲ 09:30 equity $10,171.11 vs yday $9,977.27 (+193.84) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 75 | $16.74 | $2.24 | $+60.80 | $1,360.60 | ▲ +60.80 after sell → book $10,168.88; vs 09:30 mark -2.23 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 8 | $253.72 | $2.04 | $-81.17 | $3,388.32 | ▼ -81.17 after sell → book $10,166.84; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BE` | 9 | $267.76 | $2.05 | $+274.40 | $5,796.11 | ▲ +274.40 after sell → book $10,164.79; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BAK` | 1114 | $1.94 | $14.57 | $-28.94 | $7,942.70 | ▼ -28.94 after sell → book $10,150.22; vs 09:30 mark -14.57 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,942.70 | ▼ close $10,090.58 vs 09:30 $10,171.11 (session -59.64) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,942.70 | ▼ 09:30 equity $10,087.50 vs yday $10,090.58 (-3.08) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `MRX` | 28 | $76.60 | $2.10 | $+22.42 | $10,085.40 | ▲ +22.42 after sell → book $10,085.40; vs 09:30 mark -2.10 | dropped from list after 2 sess (min 1) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,085.40 | ▲ close $10,085.40 vs 09:30 $10,087.50 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,085.40 | ▲ 09:30 equity $10,085.40 vs yday $10,085.40 (-0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,085.40 | ▲ close $10,085.40 vs 09:30 $10,085.40 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,085.40 | ▲ 09:30 equity $10,085.40 vs yday $10,085.40 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 15 | $164.43 | $2.04 | — | $7,616.91 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $2521.35 | — |
| 2026-09-11 09:30 ET | **BUY** | `ADBE` | 10 | $242.17 | $2.02 | — | $5,193.19 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list earn_react; ret5=-11.1; leftover $2521.35 | — |
| 2026-09-11 09:30 ET | **BUY** | `AVTR` | 167 | $15.01 | $2.49 | — | $2,684.03 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; 🔵; ret5=+4.9; leftover $2521.35 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 1189 | $2.12 | $15.34 | — | $148.01 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $2521.35 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $148.01 | ▼ close $9,870.90 vs 09:30 $10,085.40 (session -192.61) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $148.01 | ▼ 09:30 equity $9,805.15 vs yday $9,870.90 (-65.75) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 15 | $141.42 | $2.06 | $-349.25 | $2,267.25 | ▼ -349.25 after sell → book $9,803.09; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ADBE` | 10 | $261.51 | $2.05 | $+189.33 | $4,880.30 | ▲ +189.33 after sell → book $9,801.04; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `AVTR` | 167 | $14.87 | $2.54 | $-28.41 | $7,361.05 | ▼ -28.41 after sell → book $9,798.50; vs 09:30 mark -2.54 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BAK` | 1189 | $2.05 | $15.55 | $-114.12 | $9,782.95 | ▼ -114.12 after sell → book $9,782.95; vs 09:30 mark -15.55 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,782.95 | ▲ close $9,782.95 vs 09:30 $9,805.15 (session +0.00) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,782.95 | ▲ 09:30 equity $9,782.95 vs yday $9,782.95 (-0.00) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,782.95 | ▲ close $9,782.95 vs 09:30 $9,782.95 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,782.95 | ▲ 09:30 equity $9,782.95 vs yday $9,782.95 (-0.00) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 93 | $26.27 | $2.27 | — | $7,337.57 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer; 🔵; ret5=+10.0; leftover $2445.74 | — |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 12 | $189.17 | $2.03 | — | $5,065.50 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; 🔵; ret5=+7.9; leftover $2445.74 | — |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 61 | $39.99 | $2.17 | — | $2,623.94 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; 🔵; ret5=+9.3; leftover $2445.74 | — |
| 2026-09-16 09:30 ET | **BUY** | `AVTR` | 157 | $15.53 | $2.46 | — | $183.27 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; ret5=+4.9; leftover $2445.74 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $183.27 | ▼ close $9,652.75 vs 09:30 $9,782.95 (session -121.27) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $183.27 | ▲ 09:30 equity $9,706.84 vs yday $9,652.75 (+54.09) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `WAY` | 93 | $26.51 | $2.30 | $+17.75 | $2,646.39 | ▲ +17.75 after sell → book $9,704.53; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `QCOM` | 12 | $190.35 | $2.05 | $+10.08 | $4,928.54 | ▲ +10.08 after sell → book $9,702.48; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SM` | 61 | $37.57 | $2.20 | $-151.99 | $7,218.11 | ▼ -151.99 after sell → book $9,700.28; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 8 | $170.85 | $2.01 | — | $5,849.29 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $1443.62 | — |
| 2026-09-17 09:30 ET | **BUY** | `GME` | 65 | $22.12 | $2.19 | — | $4,409.31 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; 🔵; ret5=+10.5; leftover $1443.62 | — |
| 2026-09-17 09:30 ET | **BUY** | `JBHT` | 6 | $238.60 | $2.01 | — | $2,975.70 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_mover; ret5=-11.6; leftover $1443.62 | — |
| 2026-09-17 09:30 ET | **BUY** | `LITE` | 1 | $934.88 | $1.99 | — | $2,038.83 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer; ret5=-7.0; leftover $1443.62 | — |
| 2026-09-17 09:30 ET | **BUY** | `TNDM` | 81 | $17.72 | $2.23 | — | $601.27 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer; 🔵; ret5=-8.3; leftover $1443.62 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $601.27 | ▲ close $9,706.90 vs 09:30 $9,706.84 (session +17.06) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $601.27 | ▲ 09:30 equity $9,763.99 vs yday $9,706.90 (+57.09) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `AVTR` | 157 | $15.87 | $2.51 | $+48.41 | $3,090.36 | ▲ +48.41 after sell → book $9,761.49; vs 09:30 mark -2.50 | dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 8 | $182.33 | $2.04 | $+87.79 | $4,546.96 | ▲ +87.79 after sell → book $9,759.45; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `JBHT` | 6 | $236.80 | $2.03 | $-14.84 | $5,965.73 | ▼ -14.84 after sell → book $9,757.42; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `LITE` | 1 | $915.66 | $2.01 | $-23.23 | $6,879.38 | ▼ -23.23 after sell → book $9,755.41; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `TNDM` | 81 | $17.13 | $2.26 | $-52.28 | $8,264.65 | ▼ -52.28 after sell → book $9,753.15; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 131 | $20.91 | $2.38 | — | $5,523.06 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $2754.88 | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 186 | $14.79 | $2.55 | — | $2,769.57 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $2754.88 | — |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 195 | $14.07 | $2.58 | — | $23.34 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $2754.88 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $23.34 | ▼ close $9,625.59 vs 09:30 $9,763.99 (session -120.05) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $23.34 | ▲ 09:30 equity $9,762.57 vs yday $9,625.59 (+136.98) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `GME` | 65 | $22.78 | $2.21 | $+38.51 | $1,501.84 | ▲ +38.51 after sell → book $9,760.37; vs 09:30 mark -2.20 | dropped from list after 2 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TH` | 131 | $21.65 | $2.43 | $+92.13 | $4,335.56 | ▲ +92.13 after sell → book $9,757.94; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RARE` | 186 | $14.58 | $2.60 | $-44.21 | $7,044.84 | ▼ -44.21 after sell → book $9,755.34; vs 09:30 mark -2.60 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `BHVN` | 195 | $13.90 | $2.63 | $-38.35 | $9,752.71 | ▼ -38.35 after sell → book $9,752.71; vs 09:30 mark -2.63 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `VICR` | 8 | $230.25 | $2.01 | — | $7,908.70 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; ret5=+12.5; leftover $1950.54 | — |
| 2026-09-21 09:30 ET | **BUY** | `SMTC` | 10 | $190.30 | $2.02 | — | $6,003.68 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; ret5=+10.6; leftover $1950.54 | — |
| 2026-09-21 09:30 ET | **BUY** | `GLXY` | 75 | $25.95 | $2.21 | — | $4,055.21 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $1950.54 | — |
| 2026-09-21 09:30 ET | **BUY** | `MARA` | 139 | $13.94 | $2.41 | — | $2,115.14 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+10.5; leftover $1950.54 | — |
| 2026-09-21 09:30 ET | **BUY** | `SION` | 325 | $6.00 | $4.19 | — | $160.95 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_mover; ret5=-24.1; leftover $1950.54 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $160.95 | ▼ close $9,477.02 vs 09:30 $9,762.57 (session -262.84) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $160.95 | ▼ 09:30 equity $9,452.92 vs yday $9,477.02 (-24.10) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `MARA` | 139 | $13.13 | $2.44 | $-117.44 | $1,983.58 | ▼ -117.44 after sell → book $9,450.48; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 09:30 ET | **SELL** | `SION` | 325 | $5.99 | $4.26 | $-11.70 | $3,926.06 | ▼ -11.70 after sell → book $9,446.21; vs 09:30 mark -4.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,926.06 | ▲ close $9,446.21 vs 09:30 $9,452.92 (session +0.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,926.06 | ▲ 09:30 equity $9,796.56 vs yday $9,446.21 (+350.35) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `VICR` | 8 | $266.50 | $2.04 | $+285.94 | $6,056.02 | ▲ +285.94 after sell → book $9,794.52; vs 09:30 mark -2.04 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `SMTC` | 10 | $174.50 | $2.04 | $-162.06 | $7,798.98 | ▼ -162.06 after sell → book $9,792.48; vs 09:30 mark -2.04 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `GLXY` | 75 | $26.58 | $2.24 | $+42.79 | $9,790.24 | ▲ +42.79 after sell → book $9,790.24; vs 09:30 mark -2.24 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `CTAS` | 8 | $196.78 | $2.01 | — | $8,213.98 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list earn_react; 🔵; ⚪; ret5=-2.0; leftover $1631.71 | — |
| 2026-09-23 09:30 ET | **BUY** | `PGEN` | 205 | $7.95 | $2.64 | — | $6,581.59 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list flatten; 🔵; ⚪; ret5=+12.4; leftover $1631.71 | — |
| 2026-09-23 09:30 ET | **BUY** | `SGRY` | 103 | $15.72 | $2.30 | — | $4,960.13 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $1631.71 | — |
| 2026-09-23 09:30 ET | **BUY** | `VERI` | 1255 | $1.30 | $16.19 | — | $3,312.44 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; 🔵; ret5=+15.3; leftover $1631.71 | — |
| 2026-09-23 09:30 ET | **BUY** | `CMPX` | 1337 | $1.22 | $17.25 | — | $1,664.05 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_mover; 🔵; ret5=-33.0; leftover $1631.71 | — |
| 2026-09-23 09:30 ET | **BUY** | `BLSH` | 40 | $40.00 | $2.11 | — | $61.94 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; 🔵; ret5=+6.7; leftover $1631.71 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $61.94 | ▼ close $9,410.22 vs 09:30 $9,796.56 (session -337.51) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $61.94 | ▼ 09:30 equity $9,323.00 vs yday $9,410.22 (-87.22) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `CTAS` | 8 | $192.26 | $2.04 | $-40.21 | $1,597.99 | ▼ -40.21 after sell → book $9,320.97; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 205 | $7.38 | $2.69 | $-122.19 | $3,108.20 | ▼ -122.19 after sell → book $9,318.28; vs 09:30 mark -2.69 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SGRY` | 103 | $14.38 | $2.33 | $-142.65 | $4,587.01 | ▼ -142.65 after sell → book $9,315.95; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `VERI` | 1255 | $1.27 | $16.41 | $-70.25 | $6,164.45 | ▼ -70.25 after sell → book $9,299.54; vs 09:30 mark -16.41 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `CMPX` | 1337 | $1.17 | $17.48 | $-101.58 | $7,711.25 | ▼ -101.58 after sell → book $9,282.05; vs 09:30 mark -17.49 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `BLSH` | 40 | $39.27 | $2.13 | $-33.44 | $9,279.92 | ▼ -33.44 after sell → book $9,279.92; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,279.92 | ▲ close $9,279.92 vs 09:30 $9,323.00 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,648.84 | ▲ 09:30 equity $8,648.84 vs yday $8,648.84 (+0.00) | 09:30 open · cash $8,648.84 · no holdings · equity $8,648.84 vs prior close $8,648.84 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 09:30 ET | **BUY** | `ZSQR` | 448 | $3.86 | $5.78 | — | $6,913.78 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; leftover $1729.77 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ILMN` | 6 | $272.16 | $2.01 | — | $5,278.81 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+11.7; leftover $1729.77 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 106 | $16.21 | $2.31 | — | $3,558.24 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; leftover $1729.77 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `COST` | 1 | $887.00 | $1.99 | — | $2,669.25 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list earn_react; 🔵; ret5=+0.3; leftover $1729.77 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `RKLB` | 23 | $74.15 | $2.06 | — | $961.74 | — | packet🟢 OR headline🟢 and camera net ≥ 2; gate news_or_headline=True,cam_net_min=2; rank cond; list ohlc_hot; ret5=+8.5; leftover $1729.77 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $961.74 | ▼ close $8,590.56 vs 09:30 $8,648.84 (session -44.14) | 16:00 close · cash $961.74 · equity $8,590.56 vs 09:30 $8,648.84 (-58.28; session marks -44.14) · 5 name(s) marked open→close (per-name table). ZSQR×448 09:30 $3.86 → close $3.78 -35.84; ILMN×6 09:30 $272.16 → close $270.00 -12.96; SECZ×106 09:30 $16.21 → close $15.96 -26.50; COST×1 09:30 $887.00 → close $922.76 +35.76; RKLB×23 09:30 $74.15 → close $73.95 -4.60 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `SNDK` | cash | leftover split 1250.00 < 1 share @ 1646.93 |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ABUS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `NVAX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-27 | `MU` | cash | leftover split 731.50 < 1 share @ 967.01 |
| 2026-08-27 | `ASML` | cash | leftover split 731.50 < 1 share @ 1746.53 |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CDNS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `HAL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RBRK` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `NVDA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DVN` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NOW` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NTNX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `BCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `EOLS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `RLMD` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `JCI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MSTR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `LITE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `HPE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ORCL` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `BE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `AVTR` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TXG` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-22 | `VICR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `SMTC` | no_price | no 09:30 open — carry |
| 2026-09-22 | `GLXY` | no_price | no 09:30 open — carry |
| 2026-09-22 | `WBD` | no_price | no 09:30 open |
| 2026-09-24 | `OKTA` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ACMR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `EOG` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ADCT` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `RRC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ZSQR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CVE` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `AMPL` | hard_red | hard-red S=-7.66 sit; no new buys |
