# Factor mine action — `union_news_g_cam61_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · merged news🟢 and cameras +6 −≤1

Cash book **-26.36%** ($7,364) · signal-only (no cash/fees) was -22.84%. Starts YES **1/30**. Fills 70 · skips 85 · realized $-2058.05.

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
- Must-have: the news camera (does the morning packet like the headline?) is green.
- Must-have: at least 6 green cameras (the +G half of +G −R).
- Must-have: at most 1 red cameras (the −R half of +G −R; 🚨 is not counted here).
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
- **Gate** `news=good,n_pos_min=6,cam_bad_max=1` · **rank** `cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $7,774.22.

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
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | — | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | — | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,000.00 | ▲ close $10,000.00 vs 09:30 $10,000.00 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 27 | $91.01 | $2.07 | — | $7,540.66 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $2500.00 | — |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 55 | $44.76 | $2.15 | — | $5,076.70 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+8.7; leftover $2500.00 | — |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 1012 | $2.47 | $13.05 | — | $2,564.01 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $2500.00 | — |
| 2026-08-20 09:30 ET | **BUY** | `CRSP` | 42 | $58.73 | $2.12 | — | $95.23 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+10.8; leftover $2500.00 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $95.23 | ▲ close $9,995.25 vs 09:30 $10,000.00 (session +14.65) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $95.23 | ▲ 09:30 equity $10,136.15 vs yday $9,995.25 (+140.90) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `ABTC` | 1 | $8.66 | $0.09 | — | $86.48 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+15.7; leftover $15.87 | — |
| 2026-08-21 09:30 ET | **BUY** | `HIVE` | 4 | $3.24 | $0.14 | — | $73.38 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list yday_gainer; 🔵; ret5=+21.3; leftover $15.87 | — |
| 2026-08-21 09:30 ET | **BUY** | `MARA` | 1 | $11.70 | $0.12 | — | $61.56 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+21.1; leftover $15.87 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $61.56 | ▼ close $10,037.05 vs 09:30 $10,136.15 (session -98.75) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $61.56 | ▼ 09:30 equity $9,977.51 vs yday $10,037.05 (-59.54) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $61.56 | ▼ close $9,843.36 vs 09:30 $9,977.51 (session -134.15) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $61.56 | ▼ 09:30 equity $9,798.47 vs yday $9,843.36 (-44.89) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 27 | $95.86 | $2.10 | $+126.78 | $2,647.68 | ▲ +126.78 after sell → book $9,796.37; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `APA` | 55 | $41.38 | $2.18 | $-190.24 | $4,921.40 | ▼ -190.24 after sell → book $9,794.19; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `AUTL` | 1012 | $2.38 | $13.24 | $-117.38 | $7,316.71 | ▼ -117.38 after sell → book $9,780.94; vs 09:30 mark -13.25 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 42 | $57.93 | $2.15 | $-37.86 | $9,747.63 | ▼ -37.86 after sell → book $9,778.80; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 41 | $118.52 | $2.11 | — | $4,886.20 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $4873.81 | — |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 63 | $77.13 | $2.18 | — | $24.83 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $4873.81 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $24.83 | ▲ close $10,151.30 vs 09:30 $9,798.47 (session +376.79) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $24.83 | ▼ 09:30 equity $9,967.25 vs yday $10,151.30 (-184.05) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `ABTC` | 1 | $8.84 | $0.11 | $-0.02 | $33.56 | ▼ -0.02 after sell → book $9,967.14; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `HIVE` | 4 | $2.95 | $0.15 | $-1.45 | $45.21 | ▼ -1.45 after sell → book $9,966.99; vs 09:30 mark -0.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `MARA` | 1 | $11.56 | $0.14 | $-0.40 | $56.63 | ▼ -0.40 after sell → book $9,966.85; vs 09:30 mark -0.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $56.63 | ▼ close $9,876.14 vs 09:30 $9,967.25 (session -90.71) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $56.63 | ▼ 09:30 equity $9,836.73 vs yday $9,876.14 (-39.41) | — | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $56.63 | ▲ close $9,851.49 vs 09:30 $9,836.73 (session +14.76) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $56.63 | ▲ 09:30 equity $9,893.33 vs yday $9,851.49 (+41.84) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `AU` | 41 | $119.19 | $2.16 | $+23.19 | $4,941.26 | ▲ +23.19 after sell → book $9,891.17; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `FCX` | 63 | $78.57 | $2.23 | $+86.31 | $9,888.94 | ▲ +86.31 after sell → book $9,888.94; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 5 | $324.41 | $2.00 | — | $8,264.88 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1648.16 | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 11 | $141.76 | $2.02 | — | $6,703.50 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1648.16 | — |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 4 | $400.42 | $2.00 | — | $5,099.82 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1648.16 | — |
| 2026-08-28 09:30 ET | **BUY** | `MPWR` | 1 | $1306.03 | $1.99 | — | $3,791.79 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+0.0; leftover $1648.16 | — |
| 2026-08-28 09:30 ET | **BUY** | `DDOG` | 6 | $240.22 | $2.01 | — | $2,348.47 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+4.5; leftover $1648.16 | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 50 | $32.90 | $2.14 | — | $701.33 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1648.16 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $701.33 | ▼ close $9,506.45 vs 09:30 $9,893.33 (session -370.32) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $701.33 | ▼ 09:30 equity $9,506.03 vs yday $9,506.45 (-0.42) | — | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $701.33 | ▲ close $9,608.60 vs 09:30 $9,506.03 (session +102.57) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $701.33 | ▼ 09:30 equity $9,456.06 vs yday $9,608.60 (-152.54) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $701.33 | ▼ close $9,387.47 vs 09:30 $9,456.06 (session -68.59) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $701.33 | ▼ 09:30 equity $9,346.21 vs yday $9,387.47 (-41.26) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `KEYS` | 5 | $318.04 | $2.03 | $-35.88 | $2,289.50 | ▼ -35.88 after sell → book $9,344.18; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SMTC` | 11 | $133.00 | $2.04 | $-100.43 | $3,750.45 | ▼ -100.43 after sell → book $9,342.13; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CIEN` | 4 | $357.25 | $2.02 | $-176.71 | $5,177.43 | ▼ -176.71 after sell → book $9,340.11; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `MPWR` | 1 | $1224.92 | $2.01 | $-85.12 | $6,400.34 | ▼ -85.12 after sell → book $9,338.10; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `DDOG` | 6 | $219.46 | $2.03 | $-128.60 | $7,715.07 | ▼ -128.60 after sell → book $9,336.07; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SEDG` | 50 | $32.42 | $2.16 | $-28.30 | $9,333.91 | ▼ -28.30 after sell → book $9,333.91; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,333.91 | ▲ close $9,333.91 vs 09:30 $9,346.21 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,333.91 | ▲ 09:30 equity $9,333.91 vs yday $9,333.91 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 5 | $351.74 | $2.00 | — | $7,573.20 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $1866.78 | — |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 3 | $486.31 | $2.00 | — | $6,112.27 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+6.1; leftover $1866.78 | — |
| 2026-09-03 09:30 ET | **BUY** | `CXW` | 57 | $32.31 | $2.16 | — | $4,268.44 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list mover_buy; 🔵; ⚪; ret5=-4.1; leftover $1866.78 | — |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 117 | $15.87 | $2.34 | — | $2,409.31 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1866.78 | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 78 | $23.88 | $2.22 | — | $544.45 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1866.78 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $544.45 | ▲ close $9,634.86 vs 09:30 $9,333.91 (session +311.68) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $544.45 | ▼ 09:30 equity $9,569.83 vs yday $9,634.86 (-65.03) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 1 | $263.36 | $1.99 | — | $279.09 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $272.22 | — |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 3 | $75.65 | $2.00 | — | $50.14 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+6.0; leftover $272.22 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $50.14 | ▲ close $9,609.46 vs 09:30 $9,569.83 (session +43.63) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $50.14 | ▲ 09:30 equity $9,653.22 vs yday $9,609.46 (+43.76) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $50.14 | ▲ close $9,661.47 vs 09:30 $9,653.22 (session +8.25) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $50.14 | ▼ 09:30 equity $9,654.89 vs yday $9,661.47 (-6.58) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `AVGO` | 5 | $366.23 | $2.03 | $+68.42 | $1,879.26 | ▲ +68.42 after sell → book $9,652.86; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `DELL` | 3 | $538.47 | $2.02 | $+152.46 | $3,492.65 | ▲ +152.46 after sell → book $9,650.84; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CXW` | 57 | $35.09 | $2.19 | $+154.11 | $5,490.59 | ▲ +154.11 after sell → book $9,648.65; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `FRNM` | 117 | $15.96 | $2.38 | $+5.81 | $7,355.54 | ▲ +5.81 after sell → book $9,646.28; vs 09:30 mark -2.37 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MMED` | 78 | $23.22 | $2.25 | $-55.96 | $9,164.45 | ▼ -55.96 after sell → book $9,644.03; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,164.45 | ▼ close $9,635.77 vs 09:30 $9,654.89 (session -8.26) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,164.45 | ▼ 09:30 equity $9,634.80 vs yday $9,635.77 (-0.97) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `CRM` | 1 | $245.35 | $2.01 | $-22.02 | $9,407.78 | ▼ -22.02 after sell → book $9,632.78; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `MRX` | 3 | $75.00 | $2.02 | $-5.97 | $9,630.77 | ▼ -5.97 after sell → book $9,630.77; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,630.77 | ▲ close $9,630.77 vs 09:30 $9,634.80 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,630.77 | ▲ 09:30 equity $9,630.77 vs yday $9,630.77 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 58 | $164.43 | $2.16 | — | $91.66 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $9630.77 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $91.66 | ▼ close $8,807.90 vs 09:30 $9,630.77 (session -820.70) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $91.66 | ▼ 09:30 equity $8,294.02 vs yday $8,807.90 (-513.88) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $91.66 | ▲ close $8,489.48 vs 09:30 $8,294.02 (session +195.46) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $91.66 | ▼ 09:30 equity $8,412.34 vs yday $8,489.48 (-77.14) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $91.66 | ▼ close $8,231.96 vs 09:30 $8,412.34 (session -180.38) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $91.66 | ▼ 09:30 equity $8,213.40 vs yday $8,231.96 (-18.56) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `ORCL` | 58 | $140.03 | $2.24 | $-1419.60 | $8,211.16 | ▼ -1,419.60 after sell → book $8,211.16; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 104 | $26.27 | $2.30 | — | $5,476.78 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list yday_gainer; 🔵; ret5=+10.0; leftover $2737.05 | — |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 14 | $189.17 | $2.03 | — | $2,826.37 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ret5=+7.9; leftover $2737.05 | — |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 68 | $39.99 | $2.19 | — | $104.85 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ret5=+9.3; leftover $2737.05 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $104.85 | ▼ close $8,052.85 vs 09:30 $8,213.40 (session -151.78) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $104.85 | ▲ 09:30 equity $8,081.55 vs yday $8,052.85 (+28.70) | — | — |
| 2026-09-17 09:30 ET | **BUY** | `AVTR` | 2 | $15.81 | $0.32 | — | $72.91 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ret5=+4.9; leftover $34.95 | — |
| 2026-09-17 09:30 ET | **BUY** | `GME` | 1 | $22.12 | $0.22 | — | $50.57 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ret5=+10.5; leftover $34.95 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $50.57 | ▼ close $8,018.00 vs 09:30 $8,081.55 (session -63.01) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $50.57 | ▲ 09:30 equity $8,093.93 vs yday $8,018.00 (+75.93) | — | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 1 | $14.79 | $0.15 | — | $35.63 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $16.86 | — |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 1 | $14.07 | $0.14 | — | $21.41 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $16.86 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $21.41 | ▼ close $7,773.90 vs 09:30 $8,093.93 (session -319.73) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21.41 | ▼ 09:30 equity $7,771.63 vs yday $7,773.90 (-2.27) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `WAY` | 104 | $25.94 | $2.34 | $-38.96 | $2,716.83 | ▼ -38.96 after sell → book $7,769.29; vs 09:30 mark -2.34 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `QCOM` | 14 | $180.61 | $2.06 | $-123.93 | $5,243.31 | ▼ -123.93 after sell → book $7,767.23; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `SM` | 68 | $35.91 | $2.22 | $-281.86 | $7,682.97 | ▼ -281.86 after sell → book $7,765.01; vs 09:30 mark -2.22 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **BUY** | `VICR` | 16 | $230.25 | $2.04 | — | $3,996.93 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list ohlc_hot; ret5=+12.5; leftover $3841.48 | — |
| 2026-09-21 09:30 ET | **BUY** | `SMTC` | 20 | $190.30 | $2.05 | — | $188.88 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list ohlc_hot; ret5=+10.6; leftover $3841.48 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $188.88 | ▼ close $7,400.51 vs 09:30 $7,771.63 (session -360.41) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $188.88 | ▲ 09:30 equity $7,401.38 vs yday $7,400.51 (+0.87) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `GME` | 1 | $23.50 | $0.26 | $+0.90 | $212.12 | ▲ +0.90 after sell → book $7,401.12; vs 09:30 mark -0.26 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $212.12 | ▲ close $7,401.95 vs 09:30 $7,401.38 (session +0.84) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $212.12 | ▲ 09:30 equity $8,026.26 vs yday $7,401.95 (+624.31) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `AVTR` | 2 | $14.95 | $0.33 | $-2.37 | $241.69 | ▼ -2.37 after sell → book $8,025.93; vs 09:30 mark -0.33 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `RARE` | 1 | $15.40 | $0.18 | $+0.28 | $256.92 | ▲ +0.28 after sell → book $8,025.76; vs 09:30 mark -0.17 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `BHVN` | 1 | $14.84 | $0.17 | $+0.45 | $271.59 | ▲ +0.45 after sell → book $8,025.59; vs 09:30 mark -0.17 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **BUY** | `PGEN` | 11 | $7.95 | $0.91 | — | $183.23 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list flatten; 🔵; ⚪; ret5=+12.4; leftover $90.53 | — |
| 2026-09-23 09:30 ET | **BUY** | `SGRY` | 5 | $15.72 | $0.80 | — | $103.83 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $90.53 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $103.83 | ▲ close $8,175.63 vs 09:30 $8,026.26 (session +151.75) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $103.83 | ▼ 09:30 equity $7,931.47 vs yday $8,175.63 (-244.16) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `VICR` | 16 | $274.61 | $2.08 | $+705.64 | $4,495.50 | ▲ +705.64 after sell → book $7,929.38; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `SMTC` | 20 | $164.04 | $2.09 | $-529.34 | $7,774.22 | ▼ -529.34 after sell → book $7,927.30; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,774.22 | ▲ close $7,929.92 vs 09:30 $7,931.47 (session +2.62) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,145.84 | ▲ 09:30 equity $7,382.64 vs yday $7,382.64 (-0.00) | 09:30 open · cash $7,145.84 (unchanged overnight, no fees) · equity $7,382.64 vs prior close $7,382.64 (-0.00) · 2 name(s) re-marked at the open (per-name table). PGEN×16 yday $7.70 → 09:30 $7.70 +0.00; SGRY×8 yday $14.20 → 09:30 $14.20 +0.00 | — |
| 2026-09-25 09:30 ET | **BUY** | `ZSQR` | 462 | $3.86 | $5.96 | — | $5,356.56 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; leftover $1786.46 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ILMN` | 6 | $272.16 | $2.01 | — | $3,721.59 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+11.7; leftover $1786.46 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 110 | $16.21 | $2.32 | — | $1,936.17 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; leftover $1786.46 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `COST` | 2 | $887.00 | $2.00 | — | $160.18 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list earn_react; 🔵; ret5=+0.3; leftover $1786.46 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $160.18 | ▼ close $7,364.47 vs 09:30 $7,382.64 (session -5.89) | 16:00 close · cash $160.18 · equity $7,364.47 vs 09:30 $7,382.64 (-18.17; session marks -5.89) · 6 name(s) marked open→close (per-name table). PGEN×16 09:30 $7.70 → close $7.70 -0.00; SGRY×8 09:30 $14.20 → close $14.20 -0.00; ZSQR×462 09:30 $3.86 → close $3.78 -36.96; ILMN×6 09:30 $272.16 → close $270.00 -12.96; SECZ×110 09:30 $16.21 → close $15.96 -27.50; COST×2 09:30 $887.00 → close $922.76 +71.53 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `APA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 15.87 < 1 share @ 119.43 |
| 2026-08-21 | `FUTU` | cash | leftover split 15.87 < 1 share @ 115.18 |
| 2026-08-21 | `GRAL` | cash | leftover split 15.87 < 1 share @ 78.88 |
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
| 2026-08-26 | `CM` | cash | leftover split 56.63 < 1 share @ 118.50 |
| 2026-08-27 | `AU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `FCX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ACMR` | cash | leftover split 8.09 < 1 share @ 81.65 |
| 2026-08-27 | `MU` | cash | leftover split 8.09 < 1 share @ 967.01 |
| 2026-08-27 | `ASML` | cash | leftover split 8.09 < 1 share @ 1746.53 |
| 2026-08-27 | `CM` | cash | leftover split 8.09 < 1 share @ 118.77 |
| 2026-08-27 | `GEN` | cash | leftover split 8.09 < 1 share @ 29.83 |
| 2026-08-27 | `LRCX` | cash | leftover split 8.09 < 1 share @ 318.88 |
| 2026-08-27 | `NVDA` | cash | leftover split 8.09 < 1 share @ 222.86 |
| 2026-08-31 | `KEYS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CIEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `MPWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `DDOG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SEDG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CDNS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `HAL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `KEYS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CIEN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `MPWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `DDOG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SEDG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-04 | `AVGO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DELL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CXW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MMED` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `AVGO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `DELL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CXW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `FRNM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `MMED` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CRM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `MRX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-09 | `CRM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `MRX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `ORCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-15 | `ORCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-17 | `WAY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `QCOM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `SM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `SMTC` | cash | leftover split 34.95 < 1 share @ 170.85 |
| 2026-09-18 | `WAY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `QCOM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `SM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `AVTR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `TH` | cash | leftover split 16.86 < 1 share @ 20.91 |
| 2026-09-21 | `AVTR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `GME` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `RARE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `BHVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `AVTR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `RARE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `BHVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `VICR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `VICR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `CTAS` | cash | leftover split 90.53 < 1 share @ 196.78 |
| 2026-09-24 | `PGEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `SGRY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `OKTA` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `EOG` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `PGEN` | 11 | 2026-09-23 @ $7.95 | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list flatten; 🔵; ⚪; ret5=+12.4; leftover $90.53 |
| `SGRY` | 5 | 2026-09-23 @ $15.72 | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $90.53 |
