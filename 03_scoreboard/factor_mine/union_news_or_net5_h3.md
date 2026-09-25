# Factor mine action — `union_news_or_net5_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · packet🟢 OR headline🟢 and camera net ≥ 5

Cash book **-23.25%** ($7,675) · signal-only (no cash/fees) was -14.84%. Starts YES **1/30**. Fills 74 · skips 100 · realized $-2151.30.

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
- Must-have: camera net (+G −R) is at least 5.
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
- **Gate** `news_or_headline=True,cam_net_min=5` · **rank** `cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $7,625.06.

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
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 379 | $13.18 | $4.89 | — | $4,999.89 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $5000.00 | — |
| 2026-08-14 09:30 ET | **BUY** | `SNDK` | 3 | $1646.93 | $2.00 | — | $57.10 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.4; leftover $5000.00 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $57.10 | ▲ close $10,256.11 vs 09:30 $10,000.00 (session +263.00) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $57.10 | ▲ 09:30 equity $10,404.70 vs yday $10,256.11 (+148.59) | — | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $57.10 | ▲ close $10,507.62 vs 09:30 $10,404.70 (session +102.93) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $57.10 | ▼ 09:30 equity $9,990.19 vs yday $10,507.62 (-517.43) | — | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $57.10 | ▼ close $9,759.11 vs 09:30 $9,990.19 (session -231.08) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $57.10 | ▲ 09:30 equity $9,993.42 vs yday $9,759.11 (+234.31) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `HLIT` | 379 | $12.90 | $4.99 | $-116.00 | $4,941.21 | ▼ -116.00 after sell → book $9,988.43; vs 09:30 mark -4.99 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `SNDK` | 3 | $1682.40 | $2.05 | $+102.38 | $9,986.38 | ▲ +102.38 after sell → book $9,986.38; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,986.38 | ▲ close $9,986.38 vs 09:30 $9,993.42 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,986.38 | ▲ 09:30 equity $9,986.38 vs yday $9,986.38 (-0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 27 | $91.01 | $2.07 | — | $7,527.04 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $2496.59 | — |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 55 | $44.76 | $2.15 | — | $5,063.08 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list mover_buy; 🔵; ret5=+8.7; leftover $2496.59 | — |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 1010 | $2.47 | $13.03 | — | $2,555.35 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $2496.59 | — |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 42 | $58.73 | $2.12 | — | $86.58 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.8; leftover $2496.59 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $86.58 | ▲ close $9,981.68 vs 09:30 $9,986.38 (session +14.67) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $86.58 | ▲ 09:30 equity $10,122.56 vs yday $9,981.68 (+140.88) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `ABTC` | 1 | $8.66 | $0.09 | — | $77.83 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+15.7; leftover $14.43 | — |
| 2026-08-21 09:30 ET | **BUY** | `HIVE` | 4 | $3.24 | $0.14 | — | $64.72 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list yday_gainer; 🔵; ret5=+21.3; leftover $14.43 | — |
| 2026-08-21 09:30 ET | **BUY** | `MARA` | 1 | $11.70 | $0.12 | — | $52.90 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+21.1; leftover $14.43 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $52.90 | ▼ close $10,023.57 vs 09:30 $10,122.56 (session -98.63) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $52.90 | ▼ 09:30 equity $9,964.05 vs yday $10,023.57 (-59.52) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $52.90 | ▼ close $9,830.02 vs 09:30 $9,964.05 (session -134.03) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $52.90 | ▼ 09:30 equity $9,785.05 vs yday $9,830.02 (-44.97) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 27 | $95.86 | $2.10 | $+126.78 | $2,639.02 | ▲ +126.78 after sell → book $9,782.95; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `APA` | 55 | $41.38 | $2.18 | $-190.24 | $4,912.74 | ▼ -190.24 after sell → book $9,780.77; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AUTL` | 1010 | $2.38 | $13.22 | $-117.14 | $7,303.32 | ▼ -117.14 after sell → book $9,767.55; vs 09:30 mark -13.22 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 42 | $57.93 | $2.15 | $-37.86 | $9,734.24 | ▼ -37.86 after sell → book $9,765.41; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 27 | $118.52 | $2.07 | — | $6,532.13 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $3244.75 | — |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 42 | $77.13 | $2.12 | — | $3,290.55 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $3244.75 | — |
| 2026-08-25 09:30 ET | **BUY** | `EZPW` | 92 | $35.05 | $2.27 | — | $63.68 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list yday_gainer; 🔵; ⚪; ret5=+19.7; leftover $3244.75 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $63.68 | ▲ close $10,025.74 vs 09:30 $9,785.05 (session +266.79) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $63.68 | ▼ 09:30 equity $9,947.16 vs yday $10,025.74 (-78.58) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `ABTC` | 1 | $8.84 | $0.11 | $-0.02 | $72.41 | ▼ -0.02 after sell → book $9,947.05; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `HIVE` | 4 | $2.95 | $0.15 | $-1.45 | $84.06 | ▼ -1.45 after sell → book $9,946.90; vs 09:30 mark -0.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `MARA` | 1 | $11.56 | $0.14 | $-0.40 | $95.48 | ▼ -0.40 after sell → book $9,946.76; vs 09:30 mark -0.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $95.48 | ▼ close $9,721.25 vs 09:30 $9,947.16 (session -225.51) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $95.48 | ▼ 09:30 equity $9,658.41 vs yday $9,721.25 (-62.84) | — | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $95.48 | ▲ close $9,751.64 vs 09:30 $9,658.41 (session +93.23) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $95.48 | ▲ 09:30 equity $9,787.55 vs yday $9,751.64 (+35.91) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `AU` | 27 | $119.19 | $2.11 | $+13.91 | $3,311.51 | ▲ +13.91 after sell → book $9,785.45; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `FCX` | 42 | $78.57 | $2.15 | $+56.21 | $6,609.30 | ▲ +56.21 after sell → book $9,783.30; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `EZPW` | 92 | $34.50 | $2.31 | $-55.17 | $9,780.99 | ▼ -55.17 after sell → book $9,780.99; vs 09:30 mark -2.31 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 4 | $324.41 | $2.00 | — | $8,481.35 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1397.28 | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 9 | $141.76 | $2.02 | — | $7,203.49 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1397.28 | — |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 3 | $400.42 | $2.00 | — | $6,000.23 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1397.28 | — |
| 2026-08-28 09:30 ET | **BUY** | `MPWR` | 1 | $1306.03 | $1.99 | — | $4,692.21 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list mover_buy; 🔵; ret5=+0.0; leftover $1397.28 | — |
| 2026-08-28 09:30 ET | **BUY** | `DDOG` | 5 | $240.22 | $2.00 | — | $3,489.10 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list mover_buy; 🔵; ret5=+4.5; leftover $1397.28 | — |
| 2026-08-28 09:30 ET | **BUY** | `ADSK` | 5 | $261.16 | $2.00 | — | $2,181.30 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list earn_react; ret5=+7.8; leftover $1397.28 | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 42 | $32.90 | $2.12 | — | $797.38 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1397.28 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $797.38 | ▼ close $9,456.79 vs 09:30 $9,787.55 (session -310.06) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $797.38 | ▼ 09:30 equity $9,441.94 vs yday $9,456.79 (-14.85) | — | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $797.38 | ▲ close $9,531.24 vs 09:30 $9,441.94 (session +89.30) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $797.38 | ▼ 09:30 equity $9,378.05 vs yday $9,531.24 (-153.19) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $797.38 | ▼ close $9,294.07 vs 09:30 $9,378.05 (session -83.98) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $797.38 | ▼ 09:30 equity $9,255.65 vs yday $9,294.07 (-38.42) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `KEYS` | 4 | $318.04 | $2.02 | $-29.50 | $2,067.52 | ▼ -29.50 after sell → book $9,253.63; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SMTC` | 9 | $133.00 | $2.04 | $-82.89 | $3,262.48 | ▼ -82.89 after sell → book $9,251.59; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CIEN` | 3 | $357.25 | $2.02 | $-133.53 | $4,332.21 | ▼ -133.53 after sell → book $9,249.57; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `MPWR` | 1 | $1224.92 | $2.01 | $-85.12 | $5,555.12 | ▼ -85.12 after sell → book $9,247.56; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `DDOG` | 5 | $219.46 | $2.02 | $-107.83 | $6,650.40 | ▼ -107.83 after sell → book $9,245.54; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ADSK` | 5 | $246.70 | $2.02 | $-76.33 | $7,881.87 | ▼ -76.33 after sell → book $9,243.51; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SEDG` | 42 | $32.42 | $2.14 | $-24.41 | $9,241.37 | ▼ -24.41 after sell → book $9,241.37; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,241.37 | ▲ close $9,241.37 vs 09:30 $9,255.65 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,241.37 | ▲ 09:30 equity $9,241.37 vs yday $9,241.37 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 4 | $351.74 | $2.00 | — | $7,832.41 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $1540.23 | — |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 3 | $486.31 | $2.00 | — | $6,371.48 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list mover_buy; 🔵; ret5=+6.1; leftover $1540.23 | — |
| 2026-09-03 09:30 ET | **BUY** | `CXW` | 47 | $32.31 | $2.13 | — | $4,850.78 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list mover_buy; 🔵; ⚪; ret5=-4.1; leftover $1540.23 | — |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 97 | $15.87 | $2.28 | — | $3,309.11 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1540.23 | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 64 | $23.88 | $2.18 | — | $1,778.61 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1540.23 | — |
| 2026-09-03 09:30 ET | **BUY** | `DE` | 2 | $703.25 | $2.00 | — | $370.11 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.1; leftover $1540.23 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $370.11 | ▲ close $9,483.82 vs 09:30 $9,241.37 (session +255.04) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $370.11 | ▼ 09:30 equity $9,423.49 vs yday $9,483.82 (-60.33) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 1 | $75.65 | $0.76 | — | $293.70 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+6.0; leftover $123.37 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $293.70 | ▲ close $9,467.05 vs 09:30 $9,423.49 (session +44.32) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $293.70 | ▲ 09:30 equity $9,492.18 vs yday $9,467.05 (+25.13) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $293.70 | ▲ close $9,498.61 vs 09:30 $9,492.18 (session +6.43) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $293.70 | ▼ 09:30 equity $9,496.70 vs yday $9,498.61 (-1.91) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `AVGO` | 4 | $366.23 | $2.02 | $+53.93 | $1,756.60 | ▲ +53.93 after sell → book $9,494.68; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `DELL` | 3 | $538.47 | $2.02 | $+152.46 | $3,369.99 | ▲ +152.46 after sell → book $9,492.66; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CXW` | 47 | $35.09 | $2.15 | $+126.37 | $5,017.06 | ▲ +126.37 after sell → book $9,490.50; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `FRNM` | 97 | $15.96 | $2.31 | $+4.14 | $6,562.87 | ▲ +4.14 after sell → book $9,488.19; vs 09:30 mark -2.31 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MMED` | 64 | $23.22 | $2.20 | $-46.63 | $8,046.75 | ▼ -46.63 after sell → book $9,485.99; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `DE` | 2 | $681.32 | $2.02 | $-47.87 | $9,407.37 | ▼ -47.87 after sell → book $9,483.97; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,407.37 | ▼ close $9,483.09 vs 09:30 $9,496.70 (session -0.88) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,407.37 | ▼ 09:30 equity $9,482.37 vs yday $9,483.09 (-0.72) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `MRX` | 1 | $75.00 | $0.77 | $-2.18 | $9,481.60 | ▼ -2.18 after sell → book $9,481.60; vs 09:30 mark -0.77 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,481.60 | ▲ close $9,481.60 vs 09:30 $9,482.37 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,481.60 | ▲ 09:30 equity $9,481.60 vs yday $9,481.60 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 57 | $164.43 | $2.16 | — | $106.93 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $9481.60 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $106.93 | ▼ close $8,672.89 vs 09:30 $9,481.60 (session -806.55) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $106.93 | ▼ 09:30 equity $8,167.87 vs yday $8,672.89 (-505.02) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $106.93 | ▲ close $8,359.96 vs 09:30 $8,167.87 (session +192.09) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $106.93 | ▼ 09:30 equity $8,284.15 vs yday $8,359.96 (-75.81) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $106.93 | ▼ close $8,106.88 vs 09:30 $8,284.15 (session -177.27) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $106.93 | ▼ 09:30 equity $8,088.64 vs yday $8,106.88 (-18.24) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `ORCL` | 57 | $140.03 | $2.23 | $-1395.20 | $8,086.40 | ▼ -1,395.20 after sell → book $8,086.40; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 102 | $26.27 | $2.30 | — | $5,404.57 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list yday_gainer; 🔵; ret5=+10.0; leftover $2695.47 | — |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 14 | $189.17 | $2.03 | — | $2,754.16 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot; 🔵; ret5=+7.9; leftover $2695.47 | — |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 67 | $39.99 | $2.19 | — | $72.63 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot; 🔵; ret5=+9.3; leftover $2695.47 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $72.63 | ▼ close $7,929.29 vs 09:30 $8,088.64 (session -150.59) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $72.63 | ▲ 09:30 equity $7,958.74 vs yday $7,929.29 (+29.45) | — | — |
| 2026-09-17 09:30 ET | **BUY** | `AVTR` | 1 | $15.81 | $0.16 | — | $56.66 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot; 🔵; ret5=+4.9; leftover $24.21 | — |
| 2026-09-17 09:30 ET | **BUY** | `GME` | 1 | $22.12 | $0.22 | — | $34.32 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot; 🔵; ret5=+10.5; leftover $24.21 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $34.32 | ▼ close $7,895.90 vs 09:30 $7,958.74 (session -62.46) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $34.32 | ▲ 09:30 equity $7,971.04 vs yday $7,895.90 (+75.14) | — | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $34.32 | ▼ close $7,654.87 vs 09:30 $7,971.04 (session -316.17) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $34.32 | ▼ 09:30 equity $7,652.88 vs yday $7,654.87 (-1.99) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `WAY` | 102 | $25.94 | $2.33 | $-38.29 | $2,677.87 | ▼ -38.29 after sell → book $7,650.55; vs 09:30 mark -2.33 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `QCOM` | 14 | $180.61 | $2.06 | $-123.93 | $5,204.34 | ▼ -123.93 after sell → book $7,648.48; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `SM` | 67 | $35.91 | $2.22 | $-277.77 | $7,608.09 | ▼ -277.77 after sell → book $7,646.26; vs 09:30 mark -2.22 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **BUY** | `VICR` | 16 | $230.25 | $2.04 | — | $3,922.05 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot; ret5=+12.5; leftover $3804.05 | — |
| 2026-09-21 09:30 ET | **BUY** | `SMTC` | 19 | $190.30 | $2.05 | — | $304.31 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot; ret5=+10.6; leftover $3804.05 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $304.31 | ▼ close $7,294.60 vs 09:30 $7,652.88 (session -347.58) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $304.31 | ▲ 09:30 equity $7,295.34 vs yday $7,294.60 (+0.74) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `GME` | 1 | $23.50 | $0.26 | $+0.90 | $327.55 | ▲ +0.90 after sell → book $7,295.08; vs 09:30 mark -0.26 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $327.55 | ▲ close $7,295.08 vs 09:30 $7,295.34 (session +0.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $327.55 | ▲ 09:30 equity $7,922.00 vs yday $7,295.08 (+626.92) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `AVTR` | 1 | $14.95 | $0.17 | $-1.19 | $342.33 | ▼ -1.19 after sell → book $7,921.83; vs 09:30 mark -0.17 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **BUY** | `PGEN` | 14 | $7.95 | $1.16 | — | $229.87 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list flatten; 🔵; ⚪; ret5=+12.4; leftover $114.11 | — |
| 2026-09-23 09:30 ET | **BUY** | `SGRY` | 7 | $15.72 | $1.12 | — | $118.71 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $114.11 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $118.71 | ▲ close $8,072.62 vs 09:30 $7,922.00 (session +153.07) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $118.71 | ▼ 09:30 equity $7,833.21 vs yday $8,072.62 (-239.41) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `VICR` | 16 | $274.61 | $2.08 | $+705.64 | $4,510.39 | ▲ +705.64 after sell → book $7,831.13; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `SMTC` | 19 | $164.04 | $2.08 | $-503.07 | $7,625.06 | ▼ -503.07 after sell → book $7,829.04; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,625.06 | ▲ close $7,832.26 vs 09:30 $7,833.21 (session +3.22) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,263.78 | ▲ 09:30 equity $7,694.82 vs yday $7,694.82 (-0.00) | 09:30 open · cash $7,263.78 (unchanged overnight, no fees) · equity $7,694.82 vs prior close $7,694.82 (-0.00) · 3 name(s) re-marked at the open (per-name table). PGEN×19 yday $7.70 → 09:30 $7.70 +0.00; SGRY×9 yday $14.20 → 09:30 $14.20 +0.00; VERI×118 yday $1.33 → 09:30 $1.33 +0.00 | — |
| 2026-09-25 09:30 ET | **BUY** | `ZSQR` | 470 | $3.86 | $6.06 | — | $5,443.52 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; leftover $1815.94 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ILMN` | 6 | $272.16 | $2.01 | — | $3,808.55 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+11.7; leftover $1815.94 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 112 | $16.21 | $2.33 | — | $1,990.70 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; leftover $1815.94 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `COST` | 2 | $887.00 | $2.00 | — | $214.71 | — | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list earn_react; 🔵; ret5=+0.3; leftover $1815.94 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $214.71 | ▼ close $7,675.40 vs 09:30 $7,694.82 (session -7.03) | 16:00 close · cash $214.71 · equity $7,675.40 vs 09:30 $7,694.82 (-19.42; session marks -7.03) · 7 name(s) marked open→close (per-name table). PGEN×19 09:30 $7.70 → close $7.70 -0.00; SGRY×9 09:30 $14.20 → close $14.20 -0.00; VERI×118 09:30 $1.33 → close $1.33 +0.00; ZSQR×470 09:30 $3.86 → close $3.78 -37.60; ILMN×6 09:30 $272.16 → close $270.00 -12.96; SECZ×112 09:30 $16.21 → close $15.96 -28.00; COST×2 09:30 $887.00 → close $922.76 +71.53 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `HLIT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `SNDK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `HLIT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `SNDK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `APA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 14.43 < 1 share @ 119.43 |
| 2026-08-21 | `FUTU` | cash | leftover split 14.43 < 1 share @ 115.18 |
| 2026-08-21 | `GRAL` | cash | leftover split 14.43 < 1 share @ 78.88 |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `APA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ABTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `HIVE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `MARA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `ABTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `HIVE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `MARA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `AU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `FCX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `EZPW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `FNV` | cash | leftover split 47.74 < 1 share @ 267.02 |
| 2026-08-26 | `CM` | cash | leftover split 47.74 < 1 share @ 118.50 |
| 2026-08-27 | `AU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `FCX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `EZPW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ACMR` | cash | leftover split 11.94 < 1 share @ 81.65 |
| 2026-08-27 | `MU` | cash | leftover split 11.94 < 1 share @ 967.01 |
| 2026-08-27 | `ASML` | cash | leftover split 11.94 < 1 share @ 1746.53 |
| 2026-08-27 | `CM` | cash | leftover split 11.94 < 1 share @ 118.77 |
| 2026-08-27 | `GEN` | cash | leftover split 11.94 < 1 share @ 29.83 |
| 2026-08-27 | `LRCX` | cash | leftover split 11.94 < 1 share @ 318.88 |
| 2026-08-27 | `NVDA` | cash | leftover split 11.94 < 1 share @ 222.86 |
| 2026-08-27 | `ADSK` | cash | leftover split 11.94 < 1 share @ 261.47 |
| 2026-08-31 | `KEYS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CIEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `MPWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `DDOG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ADSK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SEDG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CDNS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `HAL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `KEYS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CIEN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `MPWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `DDOG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ADSK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SEDG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `NVDA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `AVGO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DELL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CXW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MMED` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRM` | cash | leftover split 123.37 < 1 share @ 263.36 |
| 2026-09-04 | `BE` | cash | leftover split 123.37 < 1 share @ 236.82 |
| 2026-09-08 | `AVGO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `DELL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CXW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `FRNM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `MMED` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `DE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `MRX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-09 | `MRX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `ORCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-15 | `ORCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `WAY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `QCOM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `SM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `SMTC` | cash | leftover split 24.21 < 1 share @ 170.85 |
| 2026-09-18 | `WAY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `QCOM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `SM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `AVTR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `TH` | cash | leftover split 11.44 < 1 share @ 20.91 |
| 2026-09-18 | `RARE` | cash | leftover split 11.44 < 1 share @ 14.79 |
| 2026-09-18 | `BHVN` | cash | leftover split 11.44 < 1 share @ 14.07 |
| 2026-09-21 | `AVTR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `GME` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `AVTR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `VICR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `VICR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `CTAS` | cash | leftover split 114.11 < 1 share @ 196.78 |
| 2026-09-24 | `PGEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `SGRY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `OKTA` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `ACMR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `EOG` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `PGEN` | 14 | 2026-09-23 @ $7.95 | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list flatten; 🔵; ⚪; ret5=+12.4; leftover $114.11 |
| `SGRY` | 7 | 2026-09-23 @ $15.72 | packet🟢 OR headline🟢 and camera net ≥ 5; gate news_or_headline=True,cam_net_min=5; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $114.11 |
