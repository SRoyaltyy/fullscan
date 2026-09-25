# Factor mine action — `union_news_g_cam61_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · merged news🟢 and cameras +6 −≤1

Cash book **-20.67%** ($7,933) · signal-only (no cash/fees) was -17.43%. Starts YES **1/30**. Fills 86 · skips 17 · realized $-1774.27.

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
- Minimum hold is 1 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 1 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `news=good,n_pos_min=6,cam_bad_max=1` · **rank** `cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $8,225.74.

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
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 27 | $95.72 | $2.10 | $+123.00 | $2,677.57 | ▲ +123.00 after sell → book $10,134.05; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `APA` | 55 | $44.52 | $2.18 | $-17.54 | $5,123.99 | ▼ -17.54 after sell → book $10,131.87; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 7 | $119.43 | $2.01 | — | $4,285.97 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $854.00 | — |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 7 | $115.18 | $2.01 | — | $3,477.69 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $854.00 | — |
| 2026-08-21 09:30 ET | **BUY** | `GRAL` | 10 | $78.88 | $2.02 | — | $2,686.87 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+14.3; leftover $854.00 | — |
| 2026-08-21 09:30 ET | **BUY** | `ABTC` | 98 | $8.66 | $2.28 | — | $1,835.91 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list yday_gainer,ohlc_hot; 🔵; ret5=+15.7; leftover $854.00 | — |
| 2026-08-21 09:30 ET | **BUY** | `HIVE` | 263 | $3.24 | $3.39 | — | $980.40 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list yday_gainer; 🔵; ret5=+21.3; leftover $854.00 | — |
| 2026-08-21 09:30 ET | **BUY** | `MARA` | 72 | $11.70 | $2.21 | — | $135.79 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list yday_gainer,yday_mover; 🔵; ret5=+21.1; leftover $854.00 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $135.79 | ▼ close $9,967.88 vs 09:30 $10,136.15 (session -150.06) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $135.79 | ▼ 09:30 equity $9,915.97 vs yday $9,967.88 (-51.91) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 1012 | $2.40 | $13.24 | $-97.14 | $2,551.35 | ▼ -97.14 after sell → book $9,902.73; vs 09:30 mark -13.24 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 7 | $120.51 | $2.03 | $+3.52 | $3,392.89 | ▲ +3.52 after sell → book $9,900.70; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 7 | $121.00 | $2.03 | $+36.70 | $4,237.86 | ▲ +36.70 after sell → book $9,898.67; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `GRAL` | 10 | $81.87 | $2.04 | $+25.84 | $5,054.52 | ▲ +25.84 after sell → book $9,896.63; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ABTC` | 98 | $8.00 | $2.31 | $-69.27 | $5,836.21 | ▼ -69.27 after sell → book $9,894.32; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `HIVE` | 263 | $2.99 | $3.45 | $-72.59 | $6,619.13 | ▼ -72.59 after sell → book $9,890.87; vs 09:30 mark -3.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `MARA` | 72 | $11.17 | $2.23 | $-42.59 | $7,421.14 | ▼ -42.59 after sell → book $9,888.64; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,421.14 | ▼ close $9,818.29 vs 09:30 $9,915.97 (session -70.35) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,421.14 | ▲ 09:30 equity $9,854.20 vs yday $9,818.29 (+35.91) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 42 | $57.93 | $2.15 | $-37.86 | $9,852.06 | ▼ -37.86 after sell → book $9,852.06; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 41 | $118.52 | $2.11 | — | $4,990.62 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $4926.03 | — |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 63 | $77.13 | $2.18 | — | $129.26 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $4926.03 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $129.26 | ▲ close $10,222.58 vs 09:30 $9,854.20 (session +374.81) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $129.26 | ▼ 09:30 equity $10,039.48 vs yday $10,222.58 (-183.10) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 41 | $119.80 | $2.16 | $+48.20 | $5,038.89 | ▲ +48.20 after sell → book $10,037.31; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 63 | $79.34 | $2.23 | $+134.82 | $10,035.08 | ▲ +134.82 after sell → book $10,035.08; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `CM` | 84 | $118.50 | $2.24 | — | $78.84 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list overnight,overnight_mega; 🔵; ret5=-2.7; leftover $10035.08 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $78.84 | ▼ close $10,007.64 vs 09:30 $10,039.48 (session -25.20) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $78.84 | ▲ 09:30 equity $10,055.52 vs yday $10,007.64 (+47.88) | — | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $78.84 | ▼ close $9,725.40 vs 09:30 $10,055.52 (session -330.12) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $78.84 | ▲ 09:30 equity $9,794.28 vs yday $9,725.40 (+68.88) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `CM` | 84 | $115.66 | $2.33 | $-243.14 | $9,791.95 | ▼ -243.14 after sell → book $9,791.95; vs 09:30 mark -2.33 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 5 | $324.41 | $2.00 | — | $8,167.89 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1631.99 | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 11 | $141.76 | $2.02 | — | $6,606.51 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1631.99 | — |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 4 | $400.42 | $2.00 | — | $5,002.83 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+1.9; leftover $1631.99 | — |
| 2026-08-28 09:30 ET | **BUY** | `MPWR` | 1 | $1306.03 | $1.99 | — | $3,694.81 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+0.0; leftover $1631.99 | — |
| 2026-08-28 09:30 ET | **BUY** | `DDOG` | 6 | $240.22 | $2.01 | — | $2,251.48 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+4.5; leftover $1631.99 | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 49 | $32.90 | $2.14 | — | $637.24 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1631.99 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $637.24 | ▼ close $9,410.95 vs 09:30 $9,794.28 (session -368.83) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $637.24 | ▼ 09:30 equity $9,410.79 vs yday $9,410.95 (-0.16) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 5 | $322.49 | $2.03 | $-13.63 | $2,247.66 | ▼ -13.63 after sell → book $9,408.76; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 11 | $132.30 | $2.04 | $-108.13 | $3,700.92 | ▼ -108.13 after sell → book $9,406.72; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CIEN` | 4 | $378.44 | $2.02 | $-91.95 | $5,212.65 | ▼ -91.95 after sell → book $9,404.69; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MPWR` | 1 | $1261.90 | $2.01 | $-48.14 | $6,472.54 | ▼ -48.14 after sell → book $9,402.68; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `DDOG` | 6 | $233.97 | $2.03 | $-41.57 | $7,874.30 | ▼ -41.57 after sell → book $9,400.65; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 49 | $31.15 | $2.16 | $-90.05 | $9,398.49 | ▼ -90.05 after sell → book $9,398.49; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,398.49 | ▲ close $9,398.49 vs 09:30 $9,410.79 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,398.49 | ▲ 09:30 equity $9,398.49 vs yday $9,398.49 (+0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,398.49 | ▲ close $9,398.49 vs 09:30 $9,398.49 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,398.49 | ▲ 09:30 equity $9,398.49 vs yday $9,398.49 (+0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,398.49 | ▲ close $9,398.49 vs 09:30 $9,398.49 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,398.49 | ▲ 09:30 equity $9,398.49 vs yday $9,398.49 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 5 | $351.74 | $2.00 | — | $7,637.79 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $1879.70 | — |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 3 | $486.31 | $2.00 | — | $6,176.86 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+6.1; leftover $1879.70 | — |
| 2026-09-03 09:30 ET | **BUY** | `CXW` | 58 | $32.31 | $2.16 | — | $4,300.71 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list mover_buy; 🔵; ⚪; ret5=-4.1; leftover $1879.70 | — |
| 2026-09-03 09:30 ET | **BUY** | `FRNM` | 118 | $15.87 | $2.34 | — | $2,425.71 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1879.70 | — |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 78 | $23.88 | $2.22 | — | $560.85 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.9; leftover $1879.70 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $560.85 | ▲ close $9,701.82 vs 09:30 $9,398.49 (session +314.06) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $560.85 | ▼ 09:30 equity $9,636.09 vs yday $9,701.82 (-65.73) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 5 | $359.70 | $2.03 | $+35.77 | $2,357.32 | ▲ +35.77 after sell → book $9,634.06; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DELL` | 3 | $513.78 | $2.02 | $+78.39 | $3,896.64 | ▲ +78.39 after sell → book $9,632.04; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CXW` | 58 | $33.46 | $2.19 | $+62.35 | $5,835.13 | ▲ +62.35 after sell → book $9,629.85; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 78 | $23.84 | $2.25 | $-7.60 | $7,692.39 | ▼ -7.60 after sell → book $9,627.59; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 14 | $263.36 | $2.03 | — | $4,003.32 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $3846.20 | — |
| 2026-09-04 09:30 ET | **BUY** | `MRX` | 50 | $75.65 | $2.14 | — | $218.68 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+6.0; leftover $3846.20 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $218.68 | ▲ close $9,685.98 vs 09:30 $9,636.09 (session +62.56) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $218.68 | ▲ 09:30 equity $9,688.08 vs yday $9,685.98 (+2.10) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 118 | $16.74 | $2.38 | $+97.94 | $2,191.62 | ▲ +97.94 after sell → book $9,685.70; vs 09:30 mark -2.38 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 14 | $253.72 | $2.07 | $-139.06 | $5,741.63 | ▼ -139.06 after sell → book $9,683.63; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `MRX` | 50 | $78.84 | $2.18 | $+155.18 | $9,681.45 | ▲ +155.18 after sell → book $9,681.45; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,681.45 | ▲ close $9,681.45 vs 09:30 $9,688.08 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,681.45 | ▲ 09:30 equity $9,681.45 vs yday $9,681.45 (+0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,681.45 | ▲ close $9,681.45 vs 09:30 $9,681.45 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,681.45 | ▲ 09:30 equity $9,681.45 vs yday $9,681.45 (+0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,681.45 | ▲ close $9,681.45 vs 09:30 $9,681.45 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,681.45 | ▲ 09:30 equity $9,681.45 vs yday $9,681.45 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 58 | $164.43 | $2.16 | — | $142.35 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $9681.45 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $142.35 | ▼ close $8,858.59 vs 09:30 $9,681.45 (session -820.70) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $142.35 | ▼ 09:30 equity $8,344.71 vs yday $8,858.59 (-513.88) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 58 | $141.42 | $2.24 | $-1338.98 | $8,342.47 | ▼ -1,338.98 after sell → book $8,342.47; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,342.47 | ▲ close $8,342.47 vs 09:30 $8,344.71 (session +0.00) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,342.47 | ▲ 09:30 equity $8,342.47 vs yday $8,342.47 (-0.00) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,342.47 | ▲ close $8,342.47 vs 09:30 $8,342.47 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,342.47 | ▲ 09:30 equity $8,342.47 vs yday $8,342.47 (-0.00) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 105 | $26.27 | $2.31 | — | $5,581.81 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list yday_gainer; 🔵; ret5=+10.0; leftover $2780.82 | — |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 14 | $189.17 | $2.03 | — | $2,931.40 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ret5=+7.9; leftover $2780.82 | — |
| 2026-09-16 09:30 ET | **BUY** | `SM` | 69 | $39.99 | $2.20 | — | $169.89 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ret5=+9.3; leftover $2780.82 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $169.89 | ▼ close $8,182.64 vs 09:30 $8,342.47 (session -153.29) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $169.89 | ▲ 09:30 equity $8,210.67 vs yday $8,182.64 (+28.03) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `WAY` | 105 | $26.51 | $2.34 | $+20.55 | $2,951.10 | ▲ +20.55 after sell → book $8,208.33; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `QCOM` | 14 | $190.35 | $2.06 | $+12.42 | $5,613.94 | ▲ +12.42 after sell → book $8,206.27; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SM` | 69 | $37.57 | $2.23 | $-171.41 | $8,204.04 | ▼ -171.41 after sell → book $8,204.04; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 16 | $170.85 | $2.04 | — | $5,468.40 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $2734.68 | — |
| 2026-09-17 09:30 ET | **BUY** | `AVTR` | 172 | $15.81 | $2.51 | — | $2,746.57 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ret5=+4.9; leftover $2734.68 | — |
| 2026-09-17 09:30 ET | **BUY** | `GME` | 123 | $22.12 | $2.36 | — | $23.45 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ret5=+10.5; leftover $2734.68 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $23.45 | ▲ close $8,403.12 vs 09:30 $8,210.67 (session +205.99) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $23.45 | ▲ 09:30 equity $8,487.07 vs yday $8,403.12 (+83.95) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 16 | $182.33 | $2.07 | $+179.57 | $2,938.66 | ▲ +179.57 after sell → book $8,485.00; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **SELL** | `AVTR` | 172 | $15.87 | $2.56 | $+5.26 | $5,665.75 | ▲ +5.26 after sell → book $8,482.45; vs 09:30 mark -2.55 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 90 | $20.91 | $2.26 | — | $3,781.59 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $1888.58 | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 127 | $14.79 | $2.37 | — | $1,900.88 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $1888.58 | — |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 134 | $14.07 | $2.39 | — | $13.11 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $1888.58 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13.11 | ▼ close $8,372.78 vs 09:30 $8,487.07 (session -102.64) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13.11 | ▲ 09:30 equity $8,477.81 vs yday $8,372.78 (+105.03) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `GME` | 123 | $22.78 | $2.40 | $+76.42 | $2,812.65 | ▲ +76.42 after sell → book $8,475.41; vs 09:30 mark -2.40 | dropped from list after 2 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TH` | 90 | $21.65 | $2.29 | $+62.05 | $4,758.86 | ▲ +62.05 after sell → book $8,473.12; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RARE` | 127 | $14.58 | $2.41 | $-31.45 | $6,608.11 | ▼ -31.45 after sell → book $8,470.71; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `BHVN` | 134 | $13.90 | $2.43 | $-27.60 | $8,468.28 | ▼ -27.60 after sell → book $8,468.28; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `VICR` | 18 | $230.25 | $2.04 | — | $4,321.74 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list ohlc_hot; ret5=+12.5; leftover $4234.14 | — |
| 2026-09-21 09:30 ET | **BUY** | `SMTC` | 22 | $190.30 | $2.06 | — | $133.08 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list ohlc_hot; ret5=+10.6; leftover $4234.14 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $133.08 | ▼ close $8,065.42 vs 09:30 $8,477.81 (session -398.76) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $133.08 | ▲ 09:30 equity $8,065.42 vs yday $8,065.42 (+0.00) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $133.08 | ▲ close $8,065.42 vs 09:30 $8,065.42 (session +0.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $133.08 | ▲ 09:30 equity $8,769.08 vs yday $8,065.42 (+703.66) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `VICR` | 18 | $266.50 | $2.09 | $+648.36 | $4,927.99 | ▲ +648.36 after sell → book $8,766.99; vs 09:30 mark -2.09 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `SMTC` | 22 | $174.50 | $2.10 | $-351.75 | $8,764.90 | ▼ -351.75 after sell → book $8,764.90; vs 09:30 mark -2.09 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `CTAS` | 14 | $196.78 | $2.03 | — | $6,007.94 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list earn_react; 🔵; ⚪; ret5=-2.0; leftover $2921.63 | — |
| 2026-09-23 09:30 ET | **BUY** | `PGEN` | 367 | $7.95 | $4.73 | — | $3,085.56 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list flatten; 🔵; ⚪; ret5=+12.4; leftover $2921.63 | — |
| 2026-09-23 09:30 ET | **BUY** | `SGRY` | 185 | $15.72 | $2.54 | — | $174.81 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $2921.63 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $174.81 | ▼ close $8,286.47 vs 09:30 $8,769.08 (session -469.11) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $174.81 | ▼ 09:30 equity $8,235.21 vs yday $8,286.47 (-51.26) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `CTAS` | 14 | $192.26 | $2.06 | $-67.38 | $2,864.39 | ▼ -67.38 after sell → book $8,233.15; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 367 | $7.38 | $4.82 | $-218.74 | $5,568.03 | ▼ -218.74 after sell → book $8,228.33; vs 09:30 mark -4.82 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `SGRY` | 185 | $14.38 | $2.60 | $-253.04 | $8,225.74 | ▼ -253.04 after sell → book $8,225.74; vs 09:30 mark -2.59 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,225.74 | ▲ close $8,225.74 vs 09:30 $8,235.21 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,961.57 | ▲ 09:30 equity $7,961.57 vs yday $7,961.57 (+0.00) | 09:30 open · cash $7,961.57 · no holdings · equity $7,961.57 vs prior close $7,961.57 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 09:30 ET | **BUY** | `ZSQR` | 515 | $3.86 | $6.64 | — | $5,967.03 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; leftover $1990.39 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ILMN` | 7 | $272.16 | $2.01 | — | $4,059.90 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+11.7; leftover $1990.39 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 122 | $16.21 | $2.36 | — | $2,079.92 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; leftover $1990.39 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `COST` | 2 | $887.00 | $2.00 | — | $303.92 | — | merged news🟢 and cameras +6 −≤1; gate news=good,n_pos_min=6,cam_bad_max=1; rank cond; list earn_react; 🔵; ret5=+0.3; leftover $1990.39 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $303.92 | ▼ close $7,933.27 vs 09:30 $7,961.57 (session -15.29) | 16:00 close · cash $303.92 · equity $7,933.27 vs 09:30 $7,961.57 (-28.30; session marks -15.29) · 4 name(s) marked open→close (per-name table). ZSQR×515 09:30 $3.86 → close $3.78 -41.20; ILMN×7 09:30 $272.16 → close $270.00 -15.12; SECZ×122 09:30 $16.21 → close $15.96 -30.50; COST×2 09:30 $887.00 → close $922.76 +71.53 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-27 | `ACMR` | cash | leftover split 13.14 < 1 share @ 81.65 |
| 2026-08-27 | `MU` | cash | leftover split 13.14 < 1 share @ 967.01 |
| 2026-08-27 | `ASML` | cash | leftover split 13.14 < 1 share @ 1746.53 |
| 2026-08-27 | `GEN` | cash | leftover split 13.14 < 1 share @ 29.83 |
| 2026-08-27 | `LRCX` | cash | leftover split 13.14 < 1 share @ 318.88 |
| 2026-08-27 | `NVDA` | cash | leftover split 13.14 < 1 share @ 222.86 |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CDNS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `HAL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-10 | `AMBQ` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-22 | `VICR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `SMTC` | no_price | no 09:30 open — carry |
| 2026-09-24 | `OKTA` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `EOG` | hard_red | hard-red S=-7.66 sit; no new buys |
