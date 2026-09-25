# Factor mine action — `union_news_g_cam71_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · merged news🟢 and cameras +7 −≤1

Cash book **-26.40%** ($7,360) · signal-only (no cash/fees) was -9.70%. Starts YES **2/30**. Fills 48 · skips 63 · realized $-1714.70.

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
- Must-have: at least 7 green cameras (the +G half of +G −R).
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
- **Gate** `news=good,n_pos_min=7,cam_bad_max=1` · **rank** `cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $8,181.45.

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
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 54 | $91.01 | $2.15 | — | $5,083.31 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $5000.00 | — |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 111 | $44.76 | $2.32 | — | $112.62 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+8.7; leftover $5000.00 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $112.62 | ▲ close $10,095.93 vs 09:30 $10,000.00 (session +100.41) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $112.62 | ▲ 09:30 equity $10,223.23 vs yday $10,095.93 (+127.30) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 9 | $2.47 | $0.25 | — | $90.15 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $22.52 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $90.15 | ▼ close $10,167.75 vs 09:30 $10,223.23 (session -55.23) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $90.15 | ▼ 09:30 equity $10,131.72 vs yday $10,167.75 (-36.03) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $90.15 | ▼ close $10,124.79 vs 09:30 $10,131.72 (session -6.93) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $90.15 | ▼ 09:30 equity $9,881.19 vs yday $10,124.79 (-243.60) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 54 | $95.86 | $2.20 | $+257.54 | $5,264.38 | ▲ +257.54 after sell → book $9,878.98; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `APA` | 111 | $41.38 | $2.38 | $-379.88 | $9,855.18 | ▼ -379.88 after sell → book $9,876.60; vs 09:30 mark -2.38 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 41 | $118.52 | $2.11 | — | $4,993.75 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $4927.59 | — |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 63 | $77.13 | $2.18 | — | $132.38 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $4927.59 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $132.38 | ▲ close $10,247.66 vs 09:30 $9,881.19 (session +375.35) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $132.38 | ▼ 09:30 equity $10,064.29 vs yday $10,247.66 (-183.37) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `AUTL` | 9 | $2.41 | $0.26 | $-1.05 | $153.81 | ▼ -1.05 after sell → book $10,064.03; vs 09:30 mark -0.26 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **BUY** | `CM` | 1 | $118.50 | $1.19 | — | $34.12 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list overnight,overnight_mega; 🔵; ret5=-2.7; leftover $153.81 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $34.12 | ▼ close $9,971.83 vs 09:30 $10,064.29 (session -91.01) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $34.12 | ▼ 09:30 equity $9,932.99 vs yday $9,971.83 (-38.84) | — | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $34.12 | ▲ close $9,943.82 vs 09:30 $9,932.99 (session +10.83) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $34.12 | ▲ 09:30 equity $9,986.48 vs yday $9,943.82 (+42.66) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `AU` | 41 | $119.19 | $2.16 | $+23.19 | $4,918.75 | ▲ +23.19 after sell → book $9,984.32; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `FCX` | 63 | $78.57 | $2.23 | $+86.31 | $9,866.43 | ▲ +86.31 after sell → book $9,982.09; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 6 | $324.41 | $2.01 | — | $7,917.96 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1973.29 | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 13 | $141.76 | $2.03 | — | $6,073.05 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1973.29 | — |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 4 | $400.42 | $2.00 | — | $4,469.37 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1973.29 | — |
| 2026-08-28 09:30 ET | **BUY** | `MPWR` | 1 | $1306.03 | $1.99 | — | $3,161.35 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+0.0; leftover $1973.29 | — |
| 2026-08-28 09:30 ET | **BUY** | `DDOG` | 8 | $240.22 | $2.01 | — | $1,237.57 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+4.5; leftover $1973.29 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,237.57 | ▼ close $9,642.79 vs 09:30 $9,986.48 (session -329.25) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,237.57 | ▲ 09:30 equity $9,654.25 vs yday $9,642.79 (+11.46) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `CM` | 1 | $114.46 | $1.17 | $-6.40 | $1,350.87 | ▼ -6.40 after sell → book $9,653.09; vs 09:30 mark -1.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,350.87 | ▲ close $9,710.84 vs 09:30 $9,654.25 (session +57.75) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,350.87 | ▼ 09:30 equity $9,554.59 vs yday $9,710.84 (-156.25) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,350.87 | ▼ close $9,444.00 vs 09:30 $9,554.59 (session -110.59) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,350.87 | ▼ 09:30 equity $9,397.71 vs yday $9,444.00 (-46.29) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `KEYS` | 6 | $318.04 | $2.03 | $-42.26 | $3,257.07 | ▼ -42.26 after sell → book $9,395.67; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SMTC` | 13 | $133.00 | $2.05 | $-117.96 | $4,984.02 | ▼ -117.96 after sell → book $9,393.62; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CIEN` | 4 | $357.25 | $2.02 | $-176.71 | $6,411.00 | ▼ -176.71 after sell → book $9,391.60; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `MPWR` | 1 | $1224.92 | $2.01 | $-85.12 | $7,633.90 | ▼ -85.12 after sell → book $9,389.58; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `DDOG` | 8 | $219.46 | $2.04 | $-170.13 | $9,387.54 | ▼ -170.13 after sell → book $9,387.54; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,387.54 | ▲ close $9,387.54 vs 09:30 $9,397.71 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,387.54 | ▲ 09:30 equity $9,387.54 vs yday $9,387.54 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 13 | $351.74 | $2.03 | — | $4,812.90 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $4693.77 | — |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 9 | $486.31 | $2.02 | — | $434.09 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+6.1; leftover $4693.77 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $434.09 | ▲ close $9,724.68 vs 09:30 $9,387.54 (session +341.18) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $434.09 | ▲ 09:30 equity $9,734.21 vs yday $9,724.68 (+9.53) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `FRNM` | 8 | $16.40 | $1.34 | — | $301.55 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.2; leftover $144.70 | — |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 1 | $75.65 | $0.76 | — | $225.14 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+6.0; leftover $144.70 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $225.14 | ▲ close $9,803.85 vs 09:30 $9,734.21 (session +71.74) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $225.14 | ▲ 09:30 equity $9,856.09 vs yday $9,803.85 (+52.24) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $225.14 | ▲ close $10,025.97 vs 09:30 $9,856.09 (session +169.88) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $225.14 | ▲ 09:30 equity $10,036.64 vs yday $10,025.97 (+10.67) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `AVGO` | 13 | $366.23 | $2.08 | $+184.26 | $4,984.06 | ▲ +184.26 after sell → book $10,034.57; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `DELL` | 9 | $538.47 | $2.07 | $+465.36 | $9,828.22 | ▲ +465.36 after sell → book $10,032.50; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,828.22 | ▼ close $10,029.94 vs 09:30 $10,036.64 (session -2.56) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,828.22 | ▼ 09:30 equity $10,028.34 vs yday $10,029.94 (-1.60) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `FRNM` | 8 | $15.64 | $1.30 | $-8.71 | $9,952.05 | ▼ -8.71 after sell → book $10,027.05; vs 09:30 mark -1.29 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `MRX` | 1 | $75.00 | $0.77 | $-2.18 | $10,026.27 | ▼ -2.18 after sell → book $10,026.27; vs 09:30 mark -0.78 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,026.27 | ▲ close $10,026.27 vs 09:30 $10,028.34 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,026.27 | ▲ 09:30 equity $10,026.27 vs yday $10,026.27 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 60 | $164.43 | $2.17 | — | $158.30 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $10026.27 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $158.30 | ▼ close $9,175.10 vs 09:30 $10,026.27 (session -849.00) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $158.30 | ▼ 09:30 equity $8,643.50 vs yday $9,175.10 (-531.60) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $158.30 | ▲ close $8,845.70 vs 09:30 $8,643.50 (session +202.20) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $158.30 | ▼ 09:30 equity $8,765.90 vs yday $8,845.70 (-79.80) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $158.30 | ▼ close $8,579.30 vs 09:30 $8,765.90 (session -186.60) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $158.30 | ▼ 09:30 equity $8,560.10 vs yday $8,579.30 (-19.20) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `ORCL` | 60 | $140.03 | $2.25 | $-1468.42 | $8,557.86 | ▼ -1,468.42 after sell → book $8,557.86; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 108 | $26.27 | $2.31 | — | $5,718.38 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list yday_gainer; 🔵; ret5=+10.0; leftover $2852.62 | — |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 15 | $189.17 | $2.04 | — | $2,878.80 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ret5=+7.9; leftover $2852.62 | — |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 71 | $39.99 | $2.20 | — | $37.30 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ret5=+9.3; leftover $2852.62 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $37.30 | ▼ close $8,390.98 vs 09:30 $8,560.10 (session -160.32) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $37.30 | ▲ 09:30 equity $8,423.10 vs yday $8,390.98 (+32.12) | — | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $37.30 | ▼ close $8,355.90 vs 09:30 $8,423.10 (session -67.20) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $37.30 | ▲ 09:30 equity $8,435.77 vs yday $8,355.90 (+79.87) | — | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $37.30 | ▼ close $8,099.25 vs 09:30 $8,435.77 (session -336.52) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $37.30 | ▼ 09:30 equity $8,097.58 vs yday $8,099.25 (-1.67) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `WAY` | 108 | $25.94 | $2.35 | $-40.31 | $2,836.47 | ▼ -40.31 after sell → book $8,095.23; vs 09:30 mark -2.35 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `QCOM` | 15 | $180.61 | $2.07 | $-132.50 | $5,543.55 | ▼ -132.50 after sell → book $8,093.16; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `SM` | 71 | $35.91 | $2.24 | $-294.12 | $8,090.93 | ▼ -294.12 after sell → book $8,090.93; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **BUY** | `VICR` | 17 | $230.25 | $2.04 | — | $4,174.64 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list ohlc_hot; ret5=+12.5; leftover $4045.46 | — |
| 2026-09-21 09:30 ET | **BUY** | `SMTC` | 21 | $190.30 | $2.05 | — | $176.28 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list ohlc_hot; ret5=+10.6; leftover $4045.46 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $176.28 | ▼ close $7,707.35 vs 09:30 $8,097.58 (session -379.48) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $176.28 | ▲ 09:30 equity $7,707.35 vs yday $7,707.35 (+0.00) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $176.28 | ▲ close $7,707.35 vs 09:30 $7,707.35 (session +0.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $176.28 | ▲ 09:30 equity $8,371.28 vs yday $7,707.35 (+663.93) | — | — |
| 2026-09-23 09:30 ET | **BUY** | `PGEN` | 7 | $7.95 | $0.58 | — | $120.06 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list flatten; 🔵; ⚪; ret5=+12.4; leftover $58.76 | — |
| 2026-09-23 09:30 ET | **BUY** | `SGRY` | 3 | $15.72 | $0.48 | — | $72.41 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $58.76 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $72.41 | ▲ close $8,537.82 vs 09:30 $8,371.28 (session +167.60) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $72.41 | ▼ 09:30 equity $8,280.42 vs yday $8,537.82 (-257.40) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `VICR` | 17 | $274.61 | $2.09 | $+749.99 | $4,738.70 | ▲ +749.99 after sell → book $8,278.34; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `SMTC` | 21 | $164.04 | $2.09 | $-555.60 | $8,181.45 | ▼ -555.60 after sell → book $8,276.25; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,181.45 | ▲ close $8,277.95 vs 09:30 $8,280.42 (session +1.70) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,379.90 | ▲ 09:30 equity $7,379.90 vs yday $7,379.90 (+0.00) | 09:30 open · cash $7,379.90 · no holdings · equity $7,379.90 vs prior close $7,379.90 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 09:30 ET | **BUY** | `ZSQR` | 477 | $3.86 | $6.15 | — | $5,532.53 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; leftover $1844.97 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ILMN` | 6 | $272.16 | $2.01 | — | $3,897.56 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+11.7; leftover $1844.97 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 113 | $16.21 | $2.33 | — | $2,063.50 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; leftover $1844.97 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `COST` | 2 | $887.00 | $2.00 | — | $287.50 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list earn_react; 🔵; ret5=+0.3; leftover $1844.97 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $287.50 | ▼ close $7,359.57 vs 09:30 $7,379.90 (session -7.84) | 16:00 close · cash $287.50 · equity $7,359.57 vs 09:30 $7,379.90 (-20.33; session marks -7.84) · 4 name(s) marked open→close (per-name table). ZSQR×477 09:30 $3.86 → close $3.78 -38.16; ILMN×6 09:30 $272.16 → close $270.00 -12.96; SECZ×113 09:30 $16.21 → close $15.96 -28.25; COST×2 09:30 $887.00 → close $922.76 +71.53 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `APA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 22.52 < 1 share @ 119.43 |
| 2026-08-21 | `CRSP` | cash | leftover split 22.52 < 1 share @ 59.72 |
| 2026-08-21 | `FUTU` | cash | leftover split 22.52 < 1 share @ 115.18 |
| 2026-08-21 | `GRAL` | cash | leftover split 22.52 < 1 share @ 78.88 |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `APA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `AU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `FCX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `AU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `FCX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ACMR` | cash | leftover split 5.69 < 1 share @ 81.65 |
| 2026-08-27 | `MU` | cash | leftover split 5.69 < 1 share @ 967.01 |
| 2026-08-27 | `ASML` | cash | leftover split 5.69 < 1 share @ 1746.53 |
| 2026-08-27 | `GEN` | cash | leftover split 5.69 < 1 share @ 29.83 |
| 2026-08-27 | `LRCX` | cash | leftover split 5.69 < 1 share @ 318.88 |
| 2026-08-27 | `NVDA` | cash | leftover split 5.69 < 1 share @ 222.86 |
| 2026-08-28 | `CM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `KEYS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CIEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `MPWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `DDOG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `KEYS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CIEN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `MPWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `DDOG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-04 | `AVGO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DELL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRM` | cash | leftover split 144.70 < 1 share @ 263.36 |
| 2026-09-08 | `AVGO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `DELL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `FRNM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `MRX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-09 | `FRNM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `MRX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-14 | `ORCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-15 | `ORCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-17 | `WAY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `QCOM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `SM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `SMTC` | cash | leftover split 37.30 < 1 share @ 170.85 |
| 2026-09-18 | `WAY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `QCOM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `SM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `TH` | cash | leftover split 12.43 < 1 share @ 20.91 |
| 2026-09-18 | `GME` | cash | leftover split 12.43 < 1 share @ 22.90 |
| 2026-09-18 | `RARE` | cash | leftover split 12.43 < 1 share @ 14.79 |
| 2026-09-22 | `VICR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-23 | `VICR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `CTAS` | cash | leftover split 58.76 < 1 share @ 196.78 |
| 2026-09-24 | `PGEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `SGRY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `OKTA` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `EOG` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `PGEN` | 7 | 2026-09-23 @ $7.95 | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list flatten; 🔵; ⚪; ret5=+12.4; leftover $58.76 |
| `SGRY` | 3 | 2026-09-23 @ $15.72 | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $58.76 |
