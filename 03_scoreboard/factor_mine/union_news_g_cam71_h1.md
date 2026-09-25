# Factor mine action — `union_news_g_cam71_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · merged news🟢 and cameras +7 −≤1

Cash book **-12.98%** ($8,702) · signal-only (no cash/fees) was -11.63%. Starts YES **4/30**. Fills 70 · skips 12 · realized $-910.91.

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
- Minimum hold is 1 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 1 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `news=good,n_pos_min=7,cam_bad_max=1` · **rank** `cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,089.10.

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
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 54 | $95.72 | $2.20 | $+249.98 | $5,279.30 | ▲ +249.98 after sell → book $10,221.02; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `APA` | 111 | $44.52 | $2.38 | $-31.34 | $10,218.64 | ▼ -31.34 after sell → book $10,218.64; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 17 | $119.43 | $2.04 | — | $8,186.29 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $2043.73 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 827 | $2.47 | $10.67 | — | $6,132.93 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $2043.73 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 34 | $59.72 | $2.09 | — | $4,100.36 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $2043.73 | — |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 17 | $115.18 | $2.04 | — | $2,140.26 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $2043.73 | — |
| 2026-08-21 09:30 ET | **BUY** | `GRAL` | 25 | $78.88 | $2.06 | — | $166.19 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+14.3; leftover $2043.73 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $166.19 | ▲ close $10,333.38 vs 09:30 $10,223.23 (session +133.65) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $166.19 | ▼ 09:30 equity $10,300.91 vs yday $10,333.38 (-32.47) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 17 | $120.51 | $2.07 | $+14.25 | $2,212.80 | ▲ +14.25 after sell → book $10,298.85; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 827 | $2.40 | $10.82 | $-79.38 | $4,186.77 | ▼ -79.38 after sell → book $10,288.02; vs 09:30 mark -10.83 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 34 | $58.75 | $2.12 | $-37.19 | $6,182.16 | ▼ -37.19 after sell → book $10,285.91; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 17 | $121.00 | $2.07 | $+94.83 | $8,237.09 | ▲ +94.83 after sell → book $10,283.84; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `GRAL` | 25 | $81.87 | $2.09 | $+70.59 | $10,281.75 | ▲ +70.59 after sell → book $10,281.75; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,281.75 | ▲ close $10,281.75 vs 09:30 $10,300.91 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,281.75 | ▲ 09:30 equity $10,281.75 vs yday $10,281.75 (-0.00) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 43 | $118.52 | $2.12 | — | $5,183.27 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $5140.87 | — |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 66 | $77.13 | $2.19 | — | $90.50 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $5140.87 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $90.50 | ▲ close $10,670.33 vs 09:30 $10,281.75 (session +392.89) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $90.50 | ▼ 09:30 equity $10,478.34 vs yday $10,670.33 (-191.99) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 43 | $119.80 | $2.17 | $+50.75 | $5,239.73 | ▲ +50.75 after sell → book $10,476.17; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 66 | $79.34 | $2.24 | $+141.43 | $10,473.93 | ▲ +141.43 after sell → book $10,473.93; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `CM` | 88 | $118.50 | $2.25 | — | $43.68 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list overnight,overnight_mega; 🔵; ret5=-2.7; leftover $10473.93 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $43.68 | ▼ close $10,445.28 vs 09:30 $10,478.34 (session -26.40) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $43.68 | ▲ 09:30 equity $10,495.44 vs yday $10,445.28 (+50.16) | — | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $43.68 | ▼ close $10,149.60 vs 09:30 $10,495.44 (session -345.84) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $43.68 | ▲ 09:30 equity $10,221.76 vs yday $10,149.60 (+72.16) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `CM` | 88 | $115.66 | $2.35 | $-254.52 | $10,219.41 | ▼ -254.52 after sell → book $10,219.41; vs 09:30 mark -2.35 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 6 | $324.41 | $2.01 | — | $8,270.94 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $2043.88 | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 14 | $141.76 | $2.03 | — | $6,284.27 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $2043.88 | — |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 5 | $400.42 | $2.00 | — | $4,280.16 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $2043.88 | — |
| 2026-08-28 09:30 ET | **BUY** | `MPWR` | 1 | $1306.03 | $1.99 | — | $2,972.14 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+0.0; leftover $2043.88 | — |
| 2026-08-28 09:30 ET | **BUY** | `DDOG` | 8 | $240.22 | $2.01 | — | $1,048.36 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+4.5; leftover $2043.88 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,048.36 | ▼ close $9,848.86 vs 09:30 $10,221.76 (session -360.49) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,048.36 | ▲ 09:30 equity $9,861.32 vs yday $9,848.86 (+12.46) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 6 | $322.49 | $2.03 | $-15.56 | $2,981.27 | ▼ -15.56 after sell → book $9,859.29; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 14 | $132.30 | $2.06 | $-136.53 | $4,831.41 | ▼ -136.53 after sell → book $9,857.23; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CIEN` | 5 | $378.44 | $2.03 | $-113.94 | $6,721.58 | ▼ -113.94 after sell → book $9,855.20; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MPWR` | 1 | $1261.90 | $2.01 | $-48.14 | $7,981.47 | ▼ -48.14 after sell → book $9,853.19; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `DDOG` | 8 | $233.97 | $2.04 | $-54.09 | $9,851.15 | ▼ -54.09 after sell → book $9,851.15; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,851.15 | ▲ close $9,851.15 vs 09:30 $9,861.32 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,851.15 | ▲ 09:30 equity $9,851.15 vs yday $9,851.15 (+0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,851.15 | ▲ close $9,851.15 vs 09:30 $9,851.15 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,851.15 | ▲ 09:30 equity $9,851.15 vs yday $9,851.15 (+0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,851.15 | ▲ close $9,851.15 vs 09:30 $9,851.15 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,851.15 | ▲ 09:30 equity $9,851.15 vs yday $9,851.15 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 14 | $351.74 | $2.03 | — | $4,924.76 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $4925.58 | — |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 10 | $486.31 | $2.02 | — | $59.64 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+6.1; leftover $4925.58 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $59.64 | ▲ close $10,223.78 vs 09:30 $9,851.15 (session +376.68) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $59.64 | ▲ 09:30 equity $10,233.24 vs yday $10,223.78 (+9.46) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 14 | $359.70 | $2.08 | $+107.33 | $5,093.36 | ▲ +107.33 after sell → book $10,231.16; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DELL` | 10 | $513.78 | $2.07 | $+270.61 | $10,229.09 | ▲ +270.61 after sell → book $10,229.09; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 12 | $263.36 | $2.03 | — | $7,066.74 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $3409.70 | — |
| 2026-09-04 09:30 ET | **BUY** | `FRNM` | 207 | $16.40 | $2.67 | — | $3,669.27 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.2; leftover $3409.70 | — |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 45 | $75.65 | $2.12 | — | $262.89 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+6.0; leftover $3409.70 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $262.89 | ▲ close $10,271.97 vs 09:30 $10,233.24 (session +49.71) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $262.89 | ▲ 09:30 equity $10,320.51 vs yday $10,271.97 (+48.54) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 12 | $253.72 | $2.06 | $-119.77 | $3,305.47 | ▼ -119.77 after sell → book $10,318.45; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 207 | $16.74 | $2.73 | $+64.98 | $6,767.92 | ▲ +64.98 after sell → book $10,315.72; vs 09:30 mark -2.73 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MRX` | 45 | $78.84 | $2.16 | $+139.26 | $10,313.56 | ▲ +139.26 after sell → book $10,313.56; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,313.56 | ▲ close $10,313.56 vs 09:30 $10,320.51 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,313.56 | ▲ 09:30 equity $10,313.56 vs yday $10,313.56 (-0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,313.56 | ▲ close $10,313.56 vs 09:30 $10,313.56 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,313.56 | ▲ 09:30 equity $10,313.56 vs yday $10,313.56 (-0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,313.56 | ▲ close $10,313.56 vs 09:30 $10,313.56 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,313.56 | ▲ 09:30 equity $10,313.56 vs yday $10,313.56 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 62 | $164.43 | $2.18 | — | $116.72 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $10313.56 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $116.72 | ▼ close $9,434.08 vs 09:30 $10,313.56 (session -877.30) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $116.72 | ▼ 09:30 equity $8,884.76 vs yday $9,434.08 (-549.32) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 62 | $141.42 | $2.26 | $-1431.05 | $8,882.51 | ▼ -1,431.05 after sell → book $8,882.51; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,882.51 | ▲ close $8,882.51 vs 09:30 $8,884.76 (session +0.00) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,882.51 | ▲ 09:30 equity $8,882.51 vs yday $8,882.51 (-0.00) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,882.51 | ▲ close $8,882.51 vs 09:30 $8,882.51 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,882.51 | ▲ 09:30 equity $8,882.51 vs yday $8,882.51 (-0.00) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 112 | $26.27 | $2.33 | — | $5,937.94 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list yday_gainer; 🔵; ret5=+10.0; leftover $2960.84 | — |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 15 | $189.17 | $2.04 | — | $3,098.35 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ret5=+7.9; leftover $2960.84 | — |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 74 | $39.99 | $2.21 | — | $136.88 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ret5=+9.3; leftover $2960.84 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $136.88 | ▼ close $8,711.40 vs 09:30 $8,882.51 (session -164.53) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $136.88 | ▲ 09:30 equity $8,741.43 vs yday $8,711.40 (+30.03) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `WAY` | 112 | $26.51 | $2.37 | $+22.19 | $3,103.63 | ▲ +22.19 after sell → book $8,739.06; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `QCOM` | 15 | $190.35 | $2.07 | $+13.60 | $5,956.82 | ▲ +13.60 after sell → book $8,737.00; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SM` | 74 | $37.57 | $2.25 | $-183.54 | $8,734.75 | ▼ -183.54 after sell → book $8,734.75; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 51 | $170.85 | $2.14 | — | $19.26 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $8734.75 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19.26 | ▲ close $9,106.95 vs 09:30 $8,741.43 (session +374.34) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19.26 | ▲ 09:30 equity $9,318.09 vs yday $9,106.95 (+211.14) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 51 | $182.33 | $2.23 | $+581.11 | $9,315.86 | ▲ +581.11 after sell → book $9,315.86; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 148 | $20.91 | $2.43 | — | $6,218.75 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $3105.29 | — |
| 2026-09-18 09:30 ET | **BUY** | `GME` | 135 | $22.90 | $2.40 | — | $3,124.85 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+11.7; leftover $3105.29 | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 209 | $14.79 | $2.70 | — | $31.04 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $3105.29 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $31.04 | ▼ close $9,256.15 vs 09:30 $9,318.09 (session -52.18) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $31.04 | ▲ 09:30 equity $9,357.76 vs yday $9,256.15 (+101.61) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `TH` | 148 | $21.65 | $2.48 | $+104.60 | $3,232.76 | ▲ +104.60 after sell → book $9,355.28; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `GME` | 135 | $22.78 | $2.44 | $-21.04 | $6,305.62 | ▼ -21.04 after sell → book $9,352.84; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RARE` | 209 | $14.58 | $2.76 | $-49.34 | $9,350.08 | ▼ -49.34 after sell → book $9,350.08; vs 09:30 mark -2.76 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `VICR` | 20 | $230.25 | $2.05 | — | $4,743.03 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list ohlc_hot; ret5=+12.5; leftover $4675.04 | — |
| 2026-09-21 09:30 ET | **BUY** | `SMTC` | 24 | $190.30 | $2.06 | — | $173.77 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list ohlc_hot; ret5=+10.6; leftover $4675.04 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $173.77 | ▼ close $8,908.65 vs 09:30 $9,357.76 (session -437.32) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $173.77 | ▲ 09:30 equity $8,908.65 vs yday $8,908.65 (+0.00) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $173.77 | ▲ close $8,908.65 vs 09:30 $8,908.65 (session +0.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $173.77 | ▲ 09:30 equity $9,691.77 vs yday $8,908.65 (+783.12) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `VICR` | 20 | $266.50 | $2.10 | $+720.85 | $5,501.67 | ▲ +720.85 after sell → book $9,689.67; vs 09:30 mark -2.10 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `SMTC` | 24 | $174.50 | $2.11 | $-383.37 | $9,687.56 | ▼ -383.37 after sell → book $9,687.56; vs 09:30 mark -2.11 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `CTAS` | 16 | $196.78 | $2.04 | — | $6,537.05 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list earn_react; 🔵; ⚪; ret5=-2.0; leftover $3229.19 | — |
| 2026-09-23 09:30 ET | **BUY** | `PGEN` | 406 | $7.95 | $5.24 | — | $3,304.11 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list flatten; 🔵; ⚪; ret5=+12.4; leftover $3229.19 | — |
| 2026-09-23 09:30 ET | **BUY** | `SGRY` | 205 | $15.72 | $2.64 | — | $78.86 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $3229.19 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $78.86 | ▼ close $9,155.82 vs 09:30 $9,691.77 (session -521.82) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $78.86 | ▼ 09:30 equity $9,099.20 vs yday $9,155.82 (-56.62) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `CTAS` | 16 | $192.26 | $2.07 | $-76.43 | $3,152.95 | ▼ -76.43 after sell → book $9,097.13; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 406 | $7.38 | $5.33 | $-241.99 | $6,143.90 | ▼ -241.99 after sell → book $9,091.80; vs 09:30 mark -5.33 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SGRY` | 205 | $14.38 | $2.70 | $-280.05 | $9,089.10 | ▼ -280.05 after sell → book $9,089.10; vs 09:30 mark -2.70 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,089.10 | ▲ close $9,089.10 vs 09:30 $9,099.20 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,740.22 | ▲ 09:30 equity $8,740.22 vs yday $8,740.22 (+0.00) | 09:30 open · cash $8,740.22 · no holdings · equity $8,740.22 vs prior close $8,740.22 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 09:30 ET | **BUY** | `ZSQR` | 566 | $3.86 | $7.30 | — | $6,548.16 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; leftover $2185.05 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ILMN` | 8 | $272.16 | $2.01 | — | $4,368.86 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+11.7; leftover $2185.05 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 134 | $16.21 | $2.39 | — | $2,194.33 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; leftover $2185.05 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `COST` | 2 | $887.00 | $2.00 | — | $418.34 | — | merged news🟢 and cameras +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list earn_react; 🔵; ret5=+0.3; leftover $2185.05 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $418.34 | ▼ close $8,701.99 vs 09:30 $8,740.22 (session -24.53) | 16:00 close · cash $418.34 · equity $8,701.99 vs 09:30 $8,740.22 (-38.23; session marks -24.53) · 4 name(s) marked open→close (per-name table). ZSQR×566 09:30 $3.86 → close $3.78 -45.28; ILMN×8 09:30 $272.16 → close $270.00 -17.28; SECZ×134 09:30 $16.21 → close $15.96 -33.50; COST×2 09:30 $887.00 → close $922.76 +71.53 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-27 | `ACMR` | cash | leftover split 7.28 < 1 share @ 81.65 |
| 2026-08-27 | `MU` | cash | leftover split 7.28 < 1 share @ 967.01 |
| 2026-08-27 | `ASML` | cash | leftover split 7.28 < 1 share @ 1746.53 |
| 2026-08-27 | `GEN` | cash | leftover split 7.28 < 1 share @ 29.83 |
| 2026-08-27 | `LRCX` | cash | leftover split 7.28 < 1 share @ 318.88 |
| 2026-08-27 | `NVDA` | cash | leftover split 7.28 < 1 share @ 222.86 |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-22 | `VICR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `SMTC` | no_price | no 09:30 open — carry |
| 2026-09-24 | `OKTA` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `EOG` | hard_red | hard-red S=-7.66 sit; no new buys |
