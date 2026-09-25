# Factor mine action — `union_news_g_cam71_conv_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 4 · rank `cond` · size `conviction` · sell `list` · S-boost `none` · news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5

Cash book **-15.30%** ($8,470) · signal-only (no cash/fees) was -13.48%. Starts YES **6/30**. Fills 63 · skips 10 · realized $-54.32.

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
- Must-have: the news camera (does the morning packet like the headline?) is green.
- Must-have: at least 7 green cameras (the +G half of +G −R).
- Must-have: at most 1 red cameras (the −R half of +G −R; 🚨 is not counted here).
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is allowed only when every must-have is true.
- A name is thrown out if any must-not is true.
- Sort the keepers by how many morning cameras are green vs red and keep the top 4.
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
- **Gate** `news=good,n_pos_min=7,cam_bad_max=1` · **rank** `cond` · **top_n** 4.
- **Size** `conviction` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,945.69.

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
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 76 | $91.01 | $2.22 | — | $3,081.02 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $7000.00 | — |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 67 | $44.76 | $2.19 | — | $79.91 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+8.7; leftover $3000.00 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $79.91 | ▲ close $10,169.92 vs 09:30 $10,000.00 (session +174.33) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $79.91 | ▲ 09:30 equity $10,337.47 vs yday $10,169.92 (+167.55) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 76 | $95.72 | $2.29 | $+353.45 | $7,352.34 | ▲ +353.45 after sell → book $10,335.18; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `APA` | 67 | $44.52 | $2.23 | $-20.50 | $10,332.96 | ▼ -20.50 after sell → book $10,332.96; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 60 | $119.43 | $2.17 | — | $3,164.99 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $7233.07 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 418 | $2.47 | $5.39 | — | $2,127.13 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $1033.30 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 17 | $59.72 | $2.04 | — | $1,109.85 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1033.30 | — |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 8 | $115.18 | $2.01 | — | $186.40 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $1033.30 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $186.40 | ▲ close $10,467.60 vs 09:30 $10,337.47 (session +146.26) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $186.40 | ▼ 09:30 equity $10,386.95 vs yday $10,467.60 (-80.65) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 60 | $120.51 | $2.24 | $+60.39 | $7,414.76 | ▲ +60.39 after sell → book $10,384.71; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 418 | $2.40 | $5.47 | $-40.12 | $8,412.49 | ▼ -40.12 after sell → book $10,379.24; vs 09:30 mark -5.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 17 | $58.75 | $2.06 | $-20.59 | $9,409.18 | ▼ -20.59 after sell → book $10,377.18; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 8 | $121.00 | $2.03 | $+42.51 | $10,375.14 | ▲ +42.51 after sell → book $10,375.14; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,375.14 | ▲ close $10,375.14 vs 09:30 $10,386.95 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,375.14 | ▲ 09:30 equity $10,375.14 vs yday $10,375.14 (+0.00) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 61 | $118.52 | $2.17 | — | $3,143.25 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $7262.60 | — |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 40 | $77.13 | $2.11 | — | $55.94 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $3112.54 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $55.94 | ▲ close $10,779.13 vs 09:30 $10,375.14 (session +408.27) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $55.94 | ▼ 09:30 equity $10,537.34 vs yday $10,779.13 (-241.79) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 61 | $119.80 | $2.24 | $+73.67 | $7,361.50 | ▲ +73.67 after sell → book $10,535.10; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 40 | $79.34 | $2.15 | $+84.14 | $10,532.95 | ▲ +84.14 after sell → book $10,532.95; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `CM` | 88 | $118.50 | $2.25 | — | $102.70 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list overnight,overnight_mega; 🔵; ret5=-2.7; leftover $10532.95 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $102.70 | ▼ close $10,504.30 vs 09:30 $10,537.34 (session -26.40) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $102.70 | ▲ 09:30 equity $10,554.46 vs yday $10,504.30 (+50.16) | — | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $102.70 | ▼ close $10,208.62 vs 09:30 $10,554.46 (session -345.84) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $102.70 | ▲ 09:30 equity $10,280.78 vs yday $10,208.62 (+72.16) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `CM` | 88 | $115.66 | $2.35 | $-254.52 | $10,278.43 | ▼ -254.52 after sell → book $10,278.43; vs 09:30 mark -2.35 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 22 | $324.41 | $2.06 | — | $3,139.35 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $7194.90 | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 7 | $141.76 | $2.01 | — | $2,145.02 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1027.84 | — |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 2 | $400.42 | $2.00 | — | $1,342.19 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1027.84 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,342.19 | ▼ close $10,056.60 vs 09:30 $10,280.78 (session -215.77) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,342.19 | ▲ 09:30 equity $10,119.95 vs yday $10,056.60 (+63.35) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 22 | $322.49 | $2.12 | $-46.42 | $8,434.84 | ▼ -46.42 after sell → book $10,117.82; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 7 | $132.30 | $2.03 | $-70.26 | $9,358.91 | ▼ -70.26 after sell → book $10,115.79; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CIEN` | 2 | $378.44 | $2.02 | $-47.97 | $10,113.78 | ▼ -47.97 after sell → book $10,113.78; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,113.78 | ▲ close $10,113.78 vs 09:30 $10,119.95 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,113.78 | ▲ 09:30 equity $10,113.78 vs yday $10,113.78 (-0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,113.78 | ▲ close $10,113.78 vs 09:30 $10,113.78 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,113.78 | ▲ 09:30 equity $10,113.78 vs yday $10,113.78 (-0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,113.78 | ▲ close $10,113.78 vs 09:30 $10,113.78 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,113.78 | ▲ 09:30 equity $10,113.78 vs yday $10,113.78 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 20 | $351.74 | $2.05 | — | $3,076.93 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $7079.64 | — |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 6 | $486.31 | $2.01 | — | $157.06 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+6.1; leftover $3034.13 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $157.06 | ▲ close $10,398.60 vs 09:30 $10,113.78 (session +288.88) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $157.06 | ▲ 09:30 equity $10,433.74 vs yday $10,398.60 (+35.14) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 20 | $359.70 | $2.12 | $+155.03 | $7,348.94 | ▲ +155.03 after sell → book $10,431.62; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DELL` | 6 | $513.78 | $2.04 | $+160.77 | $10,429.58 | ▲ +160.77 after sell → book $10,429.58; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 27 | $263.36 | $2.07 | — | $3,316.79 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $7300.71 | — |
| 2026-09-04 09:30 ET | **BUY** | `FRNM` | 95 | $16.40 | $2.27 | — | $1,756.51 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.2; leftover $1564.44 | — |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 20 | $75.65 | $2.05 | — | $241.46 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+6.0; leftover $1564.44 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $241.46 | ▼ close $10,355.52 vs 09:30 $10,433.74 (session -67.66) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $241.46 | ▼ 09:30 equity $10,259.00 vs yday $10,355.52 (-96.52) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 27 | $253.72 | $2.14 | $-264.49 | $7,089.77 | ▼ -264.49 after sell → book $10,256.87; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 95 | $16.74 | $2.30 | $+27.72 | $8,677.76 | ▲ +27.72 after sell → book $10,254.56; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MRX` | 20 | $78.84 | $2.07 | $+59.68 | $10,252.49 | ▲ +59.68 after sell → book $10,252.49; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,252.49 | ▲ close $10,252.49 vs 09:30 $10,259.00 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,252.49 | ▲ 09:30 equity $10,252.49 vs yday $10,252.49 (+0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,252.49 | ▲ close $10,252.49 vs 09:30 $10,252.49 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,252.49 | ▲ 09:30 equity $10,252.49 vs yday $10,252.49 (+0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,252.49 | ▲ close $10,252.49 vs 09:30 $10,252.49 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,252.49 | ▲ 09:30 equity $10,252.49 vs yday $10,252.49 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 62 | $164.43 | $2.18 | — | $55.66 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $10252.49 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $55.66 | ▼ close $9,373.02 vs 09:30 $10,252.49 (session -877.30) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $55.66 | ▼ 09:30 equity $8,823.70 vs yday $9,373.02 (-549.32) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 62 | $141.42 | $2.26 | $-1431.05 | $8,821.44 | ▼ -1,431.05 after sell → book $8,821.44; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,821.44 | ▲ close $8,821.44 vs 09:30 $8,823.70 (session +0.00) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,821.44 | ▲ 09:30 equity $8,821.44 vs yday $8,821.44 (-0.00) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,821.44 | ▲ close $8,821.44 vs 09:30 $8,821.44 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,821.44 | ▲ 09:30 equity $8,821.44 vs yday $8,821.44 (-0.00) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 235 | $26.27 | $3.03 | — | $2,644.96 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list yday_gainer; 🔵; ret5=+10.0; leftover $6175.01 | — |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 6 | $189.17 | $2.01 | — | $1,507.93 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ret5=+7.9; leftover $1323.22 | — |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 33 | $39.99 | $2.09 | — | $186.17 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ret5=+9.3; leftover $1323.22 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $186.17 | ▼ close $8,803.14 vs 09:30 $8,821.44 (session -11.17) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $186.17 | ▼ 09:30 equity $8,797.93 vs yday $8,803.14 (-5.21) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `WAY` | 235 | $26.51 | $3.12 | $+50.25 | $6,412.90 | ▲ +50.25 after sell → book $8,794.81; vs 09:30 mark -3.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `QCOM` | 6 | $190.35 | $2.03 | $+3.04 | $7,552.97 | ▲ +3.04 after sell → book $8,792.78; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SM` | 33 | $37.57 | $2.11 | $-84.06 | $8,790.67 | ▼ -84.06 after sell → book $8,790.67; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 51 | $170.85 | $2.14 | — | $75.18 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $8790.67 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $75.18 | ▲ close $9,162.87 vs 09:30 $8,797.93 (session +374.34) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $75.18 | ▲ 09:30 equity $9,374.01 vs yday $9,162.87 (+211.14) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 51 | $182.33 | $2.23 | $+581.11 | $9,371.78 | ▲ +581.11 after sell → book $9,371.78; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 313 | $20.91 | $4.04 | — | $2,822.92 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $6560.25 | — |
| 2026-09-18 09:30 ET | **BUY** | `GME` | 61 | $22.90 | $2.17 | — | $1,423.84 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+11.7; leftover $1405.77 | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 95 | $14.79 | $2.27 | — | $16.52 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $1405.77 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $16.52 | ▲ close $9,408.48 vs 09:30 $9,374.01 (session +45.18) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $16.52 | ▲ 09:30 equity $9,567.65 vs yday $9,408.48 (+159.17) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `TH` | 313 | $21.65 | $4.14 | $+223.44 | $6,788.82 | ▲ +223.44 after sell → book $9,563.50; vs 09:30 mark -4.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `GME` | 61 | $22.78 | $2.19 | $-11.69 | $8,176.21 | ▼ -11.69 after sell → book $9,561.31; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RARE` | 95 | $14.58 | $2.30 | $-24.53 | $9,559.01 | ▼ -24.53 after sell → book $9,559.01; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `VICR` | 29 | $230.25 | $2.08 | — | $2,879.68 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list ohlc_hot; ret5=+12.5; leftover $6691.31 | — |
| 2026-09-21 09:30 ET | **BUY** | `SMTC` | 15 | $190.30 | $2.04 | — | $23.15 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list ohlc_hot; ret5=+10.6; leftover $2867.70 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $23.15 | ▼ close $9,176.80 vs 09:30 $9,567.65 (session -378.10) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $23.15 | ▲ 09:30 equity $9,176.80 vs yday $9,176.80 (-0.00) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $23.15 | ▲ close $9,176.80 vs 09:30 $9,176.80 (session +0.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $23.15 | ▲ 09:30 equity $10,369.15 vs yday $9,176.80 (+1,192.35) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `VICR` | 29 | $266.50 | $2.15 | $+1047.02 | $7,749.50 | ▲ +1,047.02 after sell → book $10,367.00; vs 09:30 mark -2.15 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `SMTC` | 15 | $174.50 | $2.07 | $-241.10 | $10,364.93 | ▼ -241.10 after sell → book $10,364.93; vs 09:30 mark -2.07 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `CTAS` | 36 | $196.78 | $2.10 | — | $3,278.75 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list earn_react; 🔵; ⚪; ret5=-2.0; leftover $7255.45 | — |
| 2026-09-23 09:30 ET | **BUY** | `PGEN` | 195 | $7.95 | $2.58 | — | $1,725.93 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list flatten; 🔵; ⚪; ret5=+12.4; leftover $1554.74 | — |
| 2026-09-23 09:30 ET | **BUY** | `SGRY` | 98 | $15.72 | $2.28 | — | $183.08 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $1554.74 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $183.08 | ▼ close $9,971.68 vs 09:30 $10,369.15 (session -386.29) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $183.08 | ▼ 09:30 equity $9,952.78 vs yday $9,971.68 (-18.90) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `CTAS` | 36 | $192.26 | $2.16 | $-166.98 | $7,102.28 | ▼ -166.98 after sell → book $9,950.62; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 195 | $7.38 | $2.62 | $-116.34 | $8,538.76 | ▼ -116.34 after sell → book $9,948.00; vs 09:30 mark -2.62 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SGRY` | 98 | $14.38 | $2.31 | $-135.92 | $9,945.69 | ▼ -135.92 after sell → book $9,945.69; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,945.69 | ▲ close $9,945.69 vs 09:30 $9,952.78 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,638.93 | ▲ 09:30 equity $8,638.93 vs yday $8,638.93 (+0.00) | 09:30 open · cash $8,638.93 · no holdings · equity $8,638.93 vs prior close $8,638.93 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 09:30 ET | **BUY** | `ZSQR` | 1566 | $3.86 | $20.20 | — | $2,573.97 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; leftover $6047.25 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ILMN` | 3 | $272.16 | $2.00 | — | $1,755.49 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+11.7; leftover $863.89 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 53 | $16.21 | $2.15 | — | $894.21 | — | news🟢 +7 −≤1; 70% leftover if #1 net ≥ 5; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; leftover $863.89 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $894.21 | ▼ close $8,469.57 vs 09:30 $8,638.93 (session -145.01) | 16:00 close · cash $894.21 · equity $8,469.57 vs 09:30 $8,638.93 (-169.36; session marks -145.01) · 3 name(s) marked open→close (per-name table). ZSQR×1566 09:30 $3.86 → close $3.78 -125.28; ILMN×3 09:30 $272.16 → close $270.00 -6.48; SECZ×53 09:30 $16.21 → close $15.96 -13.25 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-27 | `ACMR` | cash | leftover split 71.89 < 1 share @ 81.65 |
| 2026-08-27 | `MU` | cash | leftover split 15.41 < 1 share @ 967.01 |
| 2026-08-27 | `ASML` | cash | leftover split 15.41 < 1 share @ 1746.53 |
| 2026-08-28 | `MPWR` | cash | leftover split 1027.84 < 1 share @ 1306.03 |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-22 | `VICR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `SMTC` | no_price | no 09:30 open — carry |
| 2026-09-24 | `OKTA` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `EOG` | hard_red | hard-red S=-7.66 sit; no new buys |
