# Factor mine action — `union_news_or_net3_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · packet🟢 OR headline🟢 and camera net ≥ 3

Cash book **-22.09%** ($7,790) · signal-only (no cash/fees) was -9.17%. Starts YES **0/30**. Fills 152 · skips 40 · realized $-1690.89.

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
- **Gate** `news_or_headline=True,cam_net_min=3` · **rank** `cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $8,309.12.

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
| 2026-08-17 09:30 ET | **SELL** | `HLIT` | 94 | $13.84 | $2.30 | $+57.47 | $2,631.40 | ▲ +57.47 after sell → book $10,153.08; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 290 | $4.60 | $3.80 | $+76.56 | $3,961.60 | ▲ +76.56 after sell → book $10,149.28; vs 09:30 mark -3.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 63 | $19.57 | $2.20 | $-4.38 | $5,192.31 | ▼ -4.38 after sell → book $10,147.08; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MH` | 92 | $13.16 | $2.29 | $-40.44 | $6,400.73 | ▼ -40.44 after sell → book $10,144.78; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VELO` | 81 | $16.05 | $2.26 | $+49.78 | $7,698.53 | ▲ +49.78 after sell → book $10,142.53; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NRG` | 10 | $127.40 | $2.04 | $+69.94 | $8,970.49 | ▲ +69.94 after sell → book $10,140.49; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `S` | 52 | $22.50 | $2.17 | $-70.61 | $10,138.32 | ▼ -70.61 after sell → book $10,138.32; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 54 | $46.18 | $2.15 | — | $7,642.45 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list flatten; 🔵; ret5=+6.7; leftover $2534.58 | — |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 17 | $142.77 | $2.04 | — | $5,213.32 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list flatten; 🔵; ret5=+5.8; leftover $2534.58 | — |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 12 | $202.70 | $2.03 | — | $2,778.89 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list flatten; 🔵; ret5=+8.3; leftover $2534.58 | — |
| 2026-08-17 09:30 ET | **BUY** | `OUST` | 51 | $49.00 | $2.14 | — | $277.75 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list yday_gainer; ⚪; ret5=+12.2; leftover $2534.58 | — |
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
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $8,999.54 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1273.09 | — |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 28 | $44.76 | $2.07 | — | $7,744.19 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list mover_buy; 🔵; ret5=+8.7; leftover $1273.09 | — |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 515 | $2.47 | $6.64 | — | $6,465.49 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $1273.09 | — |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 21 | $58.73 | $2.05 | — | $5,230.11 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.8; leftover $1273.09 | — |
| 2026-08-20 09:30 ET | **BUY** | `ASST` | 79 | $16.00 | $2.23 | — | $3,963.88 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+18.9; leftover $1273.09 | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 8 | $150.14 | $2.01 | — | $2,760.75 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1273.09 | — |
| 2026-08-20 09:30 ET | **BUY** | `ZLAB` | 47 | $26.57 | $2.13 | — | $1,509.83 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot; 🔵; ret5=+4.8; leftover $1273.09 | — |
| 2026-08-20 09:30 ET | **BUY** | `HUMA` | 1800 | $0.71 | $18.13 | — | $219.10 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+10.5; leftover $1273.09 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $219.10 | ▼ close $9,956.20 vs 09:30 $10,184.70 (session -191.20) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $219.10 | ▲ 09:30 equity $10,143.16 vs yday $9,956.20 (+186.96) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $1,461.41 | ▲ +57.15 after sell → book $10,141.11; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `APA` | 28 | $44.52 | $2.09 | $-10.89 | $2,705.88 | ▼ -10.89 after sell → book $10,139.02; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ASST` | 79 | $17.66 | $2.25 | $+126.66 | $4,098.77 | ▲ +126.66 after sell → book $10,136.77; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `MRNA` | 8 | $133.11 | $2.03 | $-140.29 | $5,161.61 | ▼ -140.29 after sell → book $10,134.73; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ZLAB` | 47 | $26.25 | $2.15 | $-19.32 | $6,393.21 | ▼ -19.32 after sell → book $10,132.58; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HUMA` | 1800 | $0.67 | $17.84 | $-95.37 | $7,588.57 | ▼ -95.37 after sell → book $10,114.74; vs 09:30 mark -17.84 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $6,392.25 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1264.76 | — |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 10 | $115.18 | $2.02 | — | $5,238.43 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $1264.76 | — |
| 2026-08-21 09:30 ET | **BUY** | `GRAL` | 16 | $78.88 | $2.04 | — | $3,974.31 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+14.3; leftover $1264.76 | — |
| 2026-08-21 09:30 ET | **BUY** | `ABTC` | 146 | $8.66 | $2.43 | — | $2,707.52 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+15.7; leftover $1264.76 | — |
| 2026-08-21 09:30 ET | **BUY** | `HIVE` | 390 | $3.24 | $5.03 | — | $1,438.89 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list yday_gainer; 🔵; ret5=+21.3; leftover $1264.76 | — |
| 2026-08-21 09:30 ET | **BUY** | `MARA` | 108 | $11.70 | $2.31 | — | $172.98 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+21.1; leftover $1264.76 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $172.98 | ▼ close $9,940.43 vs 09:30 $10,143.16 (session -158.46) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $172.98 | ▼ 09:30 equity $9,908.21 vs yday $9,940.43 (-32.22) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 515 | $2.40 | $6.74 | $-49.43 | $1,402.24 | ▼ -49.43 after sell → book $9,901.47; vs 09:30 mark -6.74 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.51 | $2.04 | $+6.74 | $2,605.30 | ▲ +6.74 after sell → book $9,899.43; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 10 | $121.00 | $2.04 | $+54.14 | $3,813.26 | ▲ +54.14 after sell → book $9,897.39; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `GRAL` | 16 | $81.87 | $2.06 | $+43.74 | $5,121.12 | ▲ +43.74 after sell → book $9,895.33; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ABTC` | 146 | $8.00 | $2.46 | $-101.25 | $6,286.66 | ▼ -101.25 after sell → book $9,892.87; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `HIVE` | 390 | $2.99 | $5.11 | $-107.64 | $7,447.65 | ▼ -107.64 after sell → book $9,887.76; vs 09:30 mark -5.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MARA` | 108 | $11.17 | $2.34 | $-61.90 | $8,651.67 | ▼ -61.90 after sell → book $9,885.42; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,651.67 | ▼ close $9,850.25 vs 09:30 $9,908.21 (session -35.17) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,651.67 | ▲ 09:30 equity $9,868.20 vs yday $9,850.25 (+17.95) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 21 | $57.93 | $2.07 | $-20.93 | $9,866.13 | ▼ -20.93 after sell → book $9,866.13; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 27 | $118.52 | $2.07 | — | $6,664.02 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $3288.71 | — |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 42 | $77.13 | $2.12 | — | $3,422.44 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $3288.71 | — |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 93 | $35.05 | $2.27 | — | $160.52 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list yday_gainer; 🔵; ⚪; ret5=+19.7; leftover $3288.71 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $160.52 | ▲ close $10,124.66 vs 09:30 $9,868.20 (session +264.99) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $160.52 | ▼ 09:30 equity $10,047.50 vs yday $10,124.66 (-77.16) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 27 | $119.80 | $2.11 | $+30.38 | $3,393.02 | ▲ +30.38 after sell → book $10,045.40; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 42 | $79.34 | $2.15 | $+88.55 | $6,723.14 | ▲ +88.55 after sell → book $10,043.24; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `EZPW` | 93 | $35.70 | $2.31 | $+55.87 | $10,040.93 | ▲ +55.87 after sell → book $10,040.93; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `FNV` | 18 | $267.02 | $2.04 | — | $5,232.53 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot; 🔵; ret5=+16.7; leftover $5020.47 | — |
| 2026-08-26 09:30 ET | **BUY** | `CM` | 42 | $118.50 | $2.12 | — | $253.41 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list overnight,overnight_mega; 🔵; ret5=-2.7; leftover $5020.47 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $253.41 | ▼ close $10,030.47 vs 09:30 $10,047.50 (session -6.30) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $253.41 | ▲ 09:30 equity $10,051.89 vs yday $10,030.47 (+21.42) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `FNV` | 18 | $267.23 | $2.09 | $-0.36 | $5,061.46 | ▼ -0.36 after sell → book $10,049.80; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 8 | $81.65 | $2.01 | — | $4,406.25 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list mover_buy; 🔵; ret5=+2.0; leftover $723.07 | — |
| 2026-08-27 09:30 ET | **BUY** | `GEN` | 24 | $29.83 | $2.06 | — | $3,688.26 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list mover_buy; 🔵; ret5=+7.6; leftover $723.07 | — |
| 2026-08-27 09:30 ET | **BUY** | `LRCX` | 2 | $318.88 | $2.00 | — | $3,048.51 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $723.07 | — |
| 2026-08-27 09:30 ET | **BUY** | `NVDA` | 3 | $222.86 | $2.00 | — | $2,377.93 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list mover_buy; 🔵; ret5=-3.6; leftover $723.07 | — |
| 2026-08-27 09:30 ET | **BUY** | `ADSK` | 2 | $261.47 | $2.00 | — | $1,852.99 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list overnight,overnight_mega; 🔵; ret5=+1.4; leftover $723.07 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,852.99 | ▼ close $9,914.45 vs 09:30 $10,051.89 (session -125.28) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,852.99 | ▲ 09:30 equity $9,917.33 vs yday $9,914.45 (+2.88) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `CM` | 42 | $115.66 | $2.16 | $-123.56 | $6,708.55 | ▼ -123.56 after sell → book $9,915.17; vs 09:30 mark -2.16 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 8 | $79.27 | $2.03 | $-23.09 | $7,340.67 | ▼ -23.09 after sell → book $9,913.13; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GEN` | 24 | $30.50 | $2.08 | $+11.94 | $8,070.59 | ▲ +11.94 after sell → book $9,911.05; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `LRCX` | 2 | $318.03 | $2.02 | $-5.71 | $8,704.64 | ▼ -5.71 after sell → book $9,909.04; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `NVDA` | 3 | $227.36 | $2.02 | $+9.48 | $9,384.70 | ▲ +9.48 after sell → book $9,907.02; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 4 | $324.41 | $2.00 | — | $8,085.06 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1340.67 | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 9 | $141.76 | $2.02 | — | $6,807.20 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1340.67 | — |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 3 | $400.42 | $2.00 | — | $5,603.94 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1340.67 | — |
| 2026-08-28 09:30 ET | **BUY** | `MPWR` | 1 | $1306.03 | $1.99 | — | $4,295.92 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list mover_buy; 🔵; ret5=+0.0; leftover $1340.67 | — |
| 2026-08-28 09:30 ET | **BUY** | `DDOG` | 5 | $240.22 | $2.00 | — | $3,092.81 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list mover_buy; 🔵; ret5=+4.5; leftover $1340.67 | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 40 | $32.90 | $2.11 | — | $1,774.70 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1340.67 | — |
| 2026-08-28 09:30 ET | **BUY** | `TLS` | 278 | $4.82 | $3.59 | — | $431.16 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot; 🔵; ret5=+8.8; leftover $1340.67 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $431.16 | ▼ close $9,577.39 vs 09:30 $9,917.33 (session -313.92) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $431.16 | ▲ 09:30 equity $9,577.46 vs yday $9,577.39 (+0.07) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `ADSK` | 2 | $257.71 | $2.02 | $-11.53 | $944.56 | ▼ -11.53 after sell → book $9,575.44; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 4 | $322.49 | $2.02 | $-11.70 | $2,232.50 | ▼ -11.70 after sell → book $9,573.42; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 9 | $132.30 | $2.04 | $-89.19 | $3,421.16 | ▼ -89.19 after sell → book $9,571.38; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CIEN` | 3 | $378.44 | $2.02 | $-69.96 | $4,554.46 | ▼ -69.96 after sell → book $9,569.37; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MPWR` | 1 | $1261.90 | $2.01 | $-48.14 | $5,814.35 | ▼ -48.14 after sell → book $9,567.35; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `DDOG` | 5 | $233.97 | $2.02 | $-35.31 | $6,982.15 | ▼ -35.31 after sell → book $9,565.33; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 40 | $31.15 | $2.13 | $-74.24 | $8,226.02 | ▼ -74.24 after sell → book $9,563.20; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TLS` | 278 | $4.81 | $3.64 | $-10.01 | $9,559.55 | ▼ -10.01 after sell → book $9,559.55; vs 09:30 mark -3.65 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,559.55 | ▲ close $9,559.55 vs 09:30 $9,577.46 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,559.55 | ▲ 09:30 equity $9,559.55 vs yday $9,559.55 (+0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,559.55 | ▲ close $9,559.55 vs 09:30 $9,559.55 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,559.55 | ▲ 09:30 equity $9,559.55 vs yday $9,559.55 (+0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,559.55 | ▲ close $9,559.55 vs 09:30 $9,559.55 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,559.55 | ▲ 09:30 equity $9,559.55 vs yday $9,559.55 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 3 | $351.74 | $2.00 | — | $8,502.34 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $1365.65 | — |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 2 | $486.31 | $2.00 | — | $7,527.72 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list mover_buy; 🔵; ret5=+6.1; leftover $1365.65 | — |
| 2026-09-03 09:30 ET | **BUY** | `CXW` | 42 | $32.31 | $2.12 | — | $6,168.58 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list mover_buy; 🔵; ⚪; ret5=-4.1; leftover $1365.65 | — |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 86 | $15.87 | $2.25 | — | $4,801.52 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1365.65 | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 57 | $23.88 | $2.16 | — | $3,438.19 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1365.65 | — |
| 2026-09-03 09:30 ET | **BUY** | `DE` | 1 | $703.25 | $1.99 | — | $2,732.95 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.1; leftover $1365.65 | — |
| 2026-09-03 09:30 ET | **BUY** | `HPE` | 28 | $47.60 | $2.07 | — | $1,398.08 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list earn_react; 🔵; ret5=-6.2; leftover $1365.65 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,398.08 | ▲ close $9,947.07 vs 09:30 $9,559.55 (session +402.10) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,398.08 | ▼ 09:30 equity $9,879.17 vs yday $9,947.07 (-67.90) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 3 | $359.70 | $2.02 | $+19.86 | $2,475.16 | ▲ +19.86 after sell → book $9,877.15; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DELL` | 2 | $513.78 | $2.02 | $+50.93 | $3,500.70 | ▲ +50.93 after sell → book $9,875.13; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CXW` | 42 | $33.46 | $2.14 | $+44.05 | $4,903.89 | ▲ +44.05 after sell → book $9,873.00; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 57 | $23.84 | $2.18 | $-6.62 | $6,260.58 | ▼ -6.62 after sell → book $9,870.81; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DE` | 1 | $692.03 | $2.01 | $-15.23 | $6,950.60 | ▼ -15.23 after sell → book $9,868.80; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `HPE` | 28 | $53.85 | $2.10 | $+170.83 | $8,456.30 | ▲ +170.83 after sell → book $9,866.70; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 8 | $263.36 | $2.01 | — | $6,347.41 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $2114.08 | — |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 27 | $75.65 | $2.07 | — | $4,302.79 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+6.0; leftover $2114.08 | — |
| 2026-09-04 09:30 ET | **BUY** | `BE` | 8 | $236.82 | $2.01 | — | $2,406.22 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot; ret5=+8.1; leftover $2114.08 | — |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 1089 | $1.94 | $14.05 | — | $279.51 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list yday_gainer,yday_mover; ret5=+18.3; leftover $2114.08 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $279.51 | ▲ close $9,950.47 vs 09:30 $9,879.17 (session +103.91) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $279.51 | ▲ 09:30 equity $10,132.33 vs yday $9,950.47 (+181.86) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 86 | $16.74 | $2.27 | $+70.30 | $1,716.87 | ▲ +70.30 after sell → book $10,130.05; vs 09:30 mark -2.28 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 8 | $253.72 | $2.04 | $-81.17 | $3,744.59 | ▼ -81.17 after sell → book $10,128.01; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BE` | 8 | $267.76 | $2.04 | $+243.46 | $5,884.63 | ▲ +243.46 after sell → book $10,125.97; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BAK` | 1089 | $1.94 | $14.25 | $-28.29 | $7,983.05 | ▼ -28.29 after sell → book $10,111.73; vs 09:30 mark -14.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,983.05 | ▼ close $10,054.22 vs 09:30 $10,132.33 (session -57.51) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,983.05 | ▼ 09:30 equity $10,051.25 vs yday $10,054.22 (-2.97) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `MRX` | 27 | $76.60 | $2.10 | $+21.48 | $10,049.15 | ▲ +21.48 after sell → book $10,049.15; vs 09:30 mark -2.10 | dropped from list after 2 sess (min 1) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,049.15 | ▲ close $10,049.15 vs 09:30 $10,051.25 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,049.15 | ▲ 09:30 equity $10,049.15 vs yday $10,049.15 (-0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,049.15 | ▲ close $10,049.15 vs 09:30 $10,049.15 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,049.15 | ▲ 09:30 equity $10,049.15 vs yday $10,049.15 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 61 | $164.43 | $2.17 | — | $16.75 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $10049.15 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16.75 | ▼ close $9,183.83 vs 09:30 $10,049.15 (session -863.15) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16.75 | ▼ 09:30 equity $8,643.37 vs yday $9,183.83 (-540.46) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 61 | $141.42 | $2.25 | $-1408.04 | $8,641.11 | ▼ -1,408.04 after sell → book $8,641.11; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,641.11 | ▲ close $8,641.11 vs 09:30 $8,643.37 (session +0.00) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,641.11 | ▲ 09:30 equity $8,641.11 vs yday $8,641.11 (+0.00) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,641.11 | ▲ close $8,641.11 vs 09:30 $8,641.11 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,641.11 | ▲ 09:30 equity $8,641.11 vs yday $8,641.11 (+0.00) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 82 | $26.27 | $2.24 | — | $6,484.74 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list yday_gainer; 🔵; ret5=+10.0; leftover $2160.28 | — |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 11 | $189.17 | $2.02 | — | $4,401.84 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot; 🔵; ret5=+7.9; leftover $2160.28 | — |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 54 | $39.99 | $2.15 | — | $2,240.23 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot; 🔵; ret5=+9.3; leftover $2160.28 | — |
| 2026-09-16 09:30 ET | **BUY** | `AVTR` | 139 | $15.53 | $2.41 | — | $79.16 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot; ret5=+4.9; leftover $2160.28 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $79.16 | ▼ close $8,523.21 vs 09:30 $8,641.11 (session -109.09) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $79.16 | ▲ 09:30 equity $8,573.20 vs yday $8,523.21 (+49.99) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `WAY` | 82 | $26.51 | $2.27 | $+15.18 | $2,250.71 | ▲ +15.18 after sell → book $8,570.93; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `QCOM` | 11 | $190.35 | $2.05 | $+8.91 | $4,342.51 | ▲ +8.91 after sell → book $8,568.88; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SM` | 54 | $37.57 | $2.18 | $-135.01 | $6,369.11 | ▼ -135.01 after sell → book $8,566.70; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 9 | $170.85 | $2.02 | — | $4,829.44 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $1592.28 | — |
| 2026-09-17 09:30 ET | **BUY** | `GME` | 71 | $22.12 | $2.20 | — | $3,256.72 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot; 🔵; ret5=+10.5; leftover $1592.28 | — |
| 2026-09-17 09:30 ET | **BUY** | `JBHT` | 6 | $238.60 | $2.01 | — | $1,823.11 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list yday_mover; ret5=-11.6; leftover $1592.28 | — |
| 2026-09-17 09:30 ET | **BUY** | `LITE` | 1 | $934.88 | $1.99 | — | $886.24 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list yday_gainer; ret5=-7.0; leftover $1592.28 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $886.24 | ▲ close $8,625.57 vs 09:30 $8,573.20 (session +67.09) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $886.24 | ▲ 09:30 equity $8,695.50 vs yday $8,625.57 (+69.93) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `AVTR` | 139 | $15.87 | $2.45 | $+42.41 | $3,089.72 | ▲ +42.41 after sell → book $8,693.05; vs 09:30 mark -2.45 | dropped from list after 2 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 9 | $182.33 | $2.04 | $+99.26 | $4,728.65 | ▲ +99.26 after sell → book $8,691.01; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `JBHT` | 6 | $236.80 | $2.03 | $-14.84 | $6,147.42 | ▼ -14.84 after sell → book $8,688.98; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `LITE` | 1 | $915.66 | $2.01 | $-23.23 | $7,061.07 | ▼ -23.23 after sell → book $8,686.97; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 112 | $20.91 | $2.33 | — | $4,716.82 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $2353.69 | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 159 | $14.79 | $2.47 | — | $2,362.75 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $2353.69 | — |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 167 | $14.07 | $2.49 | — | $10.57 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $2353.69 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10.57 | ▼ close $8,572.92 vs 09:30 $8,695.50 (session -106.77) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10.57 | ▲ 09:30 equity $8,692.27 vs yday $8,572.92 (+119.35) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `GME` | 71 | $22.78 | $2.23 | $+42.43 | $1,625.72 | ▲ +42.43 after sell → book $8,690.04; vs 09:30 mark -2.23 | dropped from list after 2 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TH` | 112 | $21.65 | $2.36 | $+78.19 | $4,048.15 | ▲ +78.19 after sell → book $8,687.67; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RARE` | 159 | $14.58 | $2.51 | $-38.37 | $6,363.86 | ▼ -38.37 after sell → book $8,685.16; vs 09:30 mark -2.51 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `BHVN` | 167 | $13.90 | $2.54 | $-33.42 | $8,682.62 | ▼ -33.42 after sell → book $8,682.62; vs 09:30 mark -2.54 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `VICR` | 9 | $230.25 | $2.02 | — | $6,608.36 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot; ret5=+12.5; leftover $2170.66 | — |
| 2026-09-21 09:30 ET | **BUY** | `SMTC` | 11 | $190.30 | $2.02 | — | $4,513.03 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot; ret5=+10.6; leftover $2170.66 | — |
| 2026-09-21 09:30 ET | **BUY** | `GLXY` | 83 | $25.95 | $2.24 | — | $2,356.95 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $2170.66 | — |
| 2026-09-21 09:30 ET | **BUY** | `MARA` | 155 | $13.94 | $2.46 | — | $193.79 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+10.5; leftover $2170.66 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $193.79 | ▼ close $8,382.17 vs 09:30 $8,692.27 (session -291.72) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $193.79 | ▼ 09:30 equity $8,358.92 vs yday $8,382.17 (-23.25) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `MARA` | 155 | $13.13 | $2.50 | $-130.50 | $2,226.44 | ▼ -130.50 after sell → book $8,356.42; vs 09:30 mark -2.50 | dropped from list after 1 sess (min 1) | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,226.44 | ▲ close $8,356.42 vs 09:30 $8,358.92 (session +0.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,226.44 | ▲ 09:30 equity $8,750.58 vs yday $8,356.42 (+394.16) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `VICR` | 9 | $266.50 | $2.05 | $+322.19 | $4,622.90 | ▲ +322.19 after sell → book $8,748.54; vs 09:30 mark -2.04 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `SMTC` | 11 | $174.50 | $2.05 | $-177.87 | $6,540.35 | ▼ -177.87 after sell → book $8,746.49; vs 09:30 mark -2.05 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `GLXY` | 83 | $26.58 | $2.27 | $+47.78 | $8,744.22 | ▲ +47.78 after sell → book $8,744.22; vs 09:30 mark -2.27 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `CTAS` | 8 | $196.78 | $2.01 | — | $7,167.96 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list earn_react; 🔵; ⚪; ret5=-2.0; leftover $1748.84 | — |
| 2026-09-23 09:30 ET | **BUY** | `PGEN` | 219 | $7.95 | $2.83 | — | $5,424.09 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list flatten; 🔵; ⚪; ret5=+12.4; leftover $1748.84 | — |
| 2026-09-23 09:30 ET | **BUY** | `SGRY` | 111 | $15.72 | $2.32 | — | $3,676.85 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $1748.84 | — |
| 2026-09-23 09:30 ET | **BUY** | `VERI` | 1345 | $1.30 | $17.35 | — | $1,911.00 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot; 🔵; ret5=+15.3; leftover $1748.84 | — |
| 2026-09-23 09:30 ET | **BUY** | `BLSH` | 43 | $40.00 | $2.12 | — | $188.88 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot; 🔵; ret5=+6.7; leftover $1748.84 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $188.88 | ▼ close $8,429.94 vs 09:30 $8,750.58 (session -287.65) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $188.88 | ▼ 09:30 equity $8,336.12 vs yday $8,429.94 (-93.82) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `CTAS` | 8 | $192.26 | $2.04 | $-40.21 | $1,724.92 | ▼ -40.21 after sell → book $8,334.08; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 219 | $7.38 | $2.87 | $-130.53 | $3,338.27 | ▼ -130.53 after sell → book $8,331.21; vs 09:30 mark -2.87 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SGRY` | 111 | $14.38 | $2.35 | $-153.42 | $4,932.09 | ▼ -153.42 after sell → book $8,328.85; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `VERI` | 1345 | $1.27 | $17.59 | $-75.29 | $6,622.65 | ▼ -75.29 after sell → book $8,311.26; vs 09:30 mark -17.59 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `BLSH` | 43 | $39.27 | $2.14 | $-35.65 | $8,309.12 | ▼ -35.65 after sell → book $8,309.12; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,309.12 | ▲ close $8,309.12 vs 09:30 $8,336.12 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,817.41 | ▲ 09:30 equity $7,817.41 vs yday $7,817.41 (+0.00) | 09:30 open · cash $7,817.41 · no holdings · equity $7,817.41 vs prior close $7,817.41 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 09:30 ET | **BUY** | `ZSQR` | 506 | $3.86 | $6.53 | — | $5,857.72 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; leftover $1954.35 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ILMN` | 7 | $272.16 | $2.01 | — | $3,950.59 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+11.7; leftover $1954.35 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 120 | $16.21 | $2.35 | — | $2,003.04 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; leftover $1954.35 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `COST` | 2 | $887.00 | $2.00 | — | $227.05 | — | packet🟢 OR headline🟢 and camera net ≥ 3; gate news_or_headline=True,cam_net_min=3; rank cond; list earn_react; 🔵; ret5=+0.3; leftover $1954.35 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $227.05 | ▼ close $7,790.46 vs 09:30 $7,817.41 (session -14.07) | 16:00 close · cash $227.05 · equity $7,790.46 vs 09:30 $7,817.41 (-26.95; session marks -14.07) · 4 name(s) marked open→close (per-name table). ZSQR×506 09:30 $3.86 → close $3.78 -40.48; ILMN×7 09:30 $272.16 → close $270.00 -15.12; SECZ×120 09:30 $16.21 → close $15.96 -30.00; COST×2 09:30 $887.00 → close $922.76 +71.53 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `SNDK` | cash | leftover split 1250.00 < 1 share @ 1646.93 |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ABUS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-27 | `MU` | cash | leftover split 723.07 < 1 share @ 967.01 |
| 2026-08-27 | `ASML` | cash | leftover split 723.07 < 1 share @ 1746.53 |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CDNS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `HAL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `NVDA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DVN` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NOW` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `BCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `EOLS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `JCI` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `LITE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `HPE` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `META` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `ORCL` | hard_red | hard-red S=-13.28 sit; no new buys |
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
| 2026-09-24 | `RRC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ZSQR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CVE` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `AMPL` | hard_red | hard-red S=-7.66 sit; no new buys |
