# Factor mine action — `union_news_g_cam71_n2_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 2 · rank `cond` · size `topheavy` · sell `list` · S-boost `none` · topheavy leftover on news🟢 +7 −≤1

Cash book **-13.63%** ($8,637) · signal-only (no cash/fees) was -13.89%. Starts YES **4/30**. Fills 52 · skips 6 · realized $-1349.66.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 2 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- Sort the keepers by how many morning cameras are green vs red and keep the top 2.
- Give about 40% of leftover cash to the first name; split the rest.
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
- **Gate** `news=good,n_pos_min=7,cam_bad_max=1` · **rank** `cond` · **top_n** 2.
- **Size** `topheavy` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $8,650.35.

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
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 43 | $91.01 | $2.12 | — | $6,084.45 | — | topheavy leftover on news🟢 +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $4000.00 | — |
| 2026-08-20 09:30 ET | **BUY** | `APA` | 134 | $44.76 | $2.39 | — | $84.22 | — | topheavy leftover on news🟢 +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+8.7; leftover $6000.00 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $84.22 | ▲ close $10,058.57 vs 09:30 $10,000.00 (session +63.08) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $84.22 | ▲ 09:30 equity $10,165.86 vs yday $10,058.57 (+107.29) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 43 | $95.72 | $2.16 | $+198.25 | $4,198.02 | ▲ +198.25 after sell → book $10,163.70; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `APA` | 134 | $44.52 | $2.46 | $-37.01 | $10,161.24 | ▼ -37.01 after sell → book $10,161.24; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 34 | $119.43 | $2.09 | — | $6,098.52 | — | topheavy leftover on news🟢 +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $4064.49 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 2456 | $2.47 | $31.68 | — | $0.52 | — | topheavy leftover on news🟢 +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $6096.74 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.52 | ▼ close $10,040.96 vs 09:30 $10,165.86 (session -86.50) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.52 | ▼ 09:30 equity $9,992.26 vs yday $10,040.96 (-48.70) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 34 | $120.51 | $2.13 | $+32.49 | $4,095.73 | ▲ +32.49 after sell → book $9,990.13; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 2456 | $2.40 | $32.14 | $-235.74 | $9,957.99 | ▼ -235.74 after sell → book $9,957.99; vs 09:30 mark -32.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,957.99 | ▲ close $9,957.99 vs 09:30 $9,992.26 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,957.99 | ▲ 09:30 equity $9,957.99 vs yday $9,957.99 (-0.00) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 33 | $118.52 | $2.09 | — | $6,044.74 | — | topheavy leftover on news🟢 +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.7; leftover $3983.20 | — |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 77 | $77.13 | $2.22 | — | $103.51 | — | topheavy leftover on news🟢 +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $5974.79 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $103.51 | ▲ close $10,328.45 vs 09:30 $9,957.99 (session +374.77) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $103.51 | ▼ 09:30 equity $10,166.09 vs yday $10,328.45 (-162.36) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `AU` | 33 | $119.80 | $2.13 | $+38.02 | $4,054.78 | ▲ +38.02 after sell → book $10,163.96; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 77 | $79.34 | $2.28 | $+165.67 | $10,161.68 | ▲ +165.67 after sell → book $10,161.68; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **BUY** | `CM` | 85 | $118.50 | $2.25 | — | $86.93 | — | topheavy leftover on news🟢 +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list overnight,overnight_mega; 🔵; ret5=-2.7; leftover $10161.68 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $86.93 | ▼ close $10,133.93 vs 09:30 $10,166.09 (session -25.50) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $86.93 | ▲ 09:30 equity $10,182.38 vs yday $10,133.93 (+48.45) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `CM` | 85 | $118.77 | $2.34 | $+18.37 | $10,180.04 | ▲ +18.37 after sell → book $10,180.04; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 49 | $81.65 | $2.14 | — | $6,177.05 | — | topheavy leftover on news🟢 +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+2.0; leftover $4072.02 | — |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 6 | $967.01 | $2.01 | — | $372.99 | — | topheavy leftover on news🟢 +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+0.1; leftover $6108.02 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $372.99 | ▼ close $9,929.34 vs 09:30 $10,182.38 (session -246.56) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $372.99 | ▼ 09:30 equity $9,772.96 vs yday $9,929.34 (-156.38) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 49 | $79.27 | $2.18 | $-120.94 | $4,255.04 | ▼ -120.94 after sell → book $9,770.78; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 6 | $919.29 | $2.06 | $-290.39 | $9,768.72 | ▼ -290.39 after sell → book $9,768.72; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 12 | $324.41 | $2.03 | — | $5,873.77 | — | topheavy leftover on news🟢 +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $3907.49 | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 41 | $141.76 | $2.11 | — | $59.50 | — | topheavy leftover on news🟢 +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $5861.23 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $59.50 | ▼ close $9,277.11 vs 09:30 $9,772.96 (session -487.47) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $59.50 | ▲ 09:30 equity $9,353.68 vs yday $9,277.11 (+76.57) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 12 | $322.49 | $2.07 | $-27.13 | $3,927.31 | ▼ -27.13 after sell → book $9,351.61; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 41 | $132.30 | $2.17 | $-392.14 | $9,349.44 | ▼ -392.14 after sell → book $9,349.44; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,349.44 | ▲ close $9,349.44 vs 09:30 $9,353.68 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,349.44 | ▲ 09:30 equity $9,349.44 vs yday $9,349.44 (+0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,349.44 | ▲ close $9,349.44 vs 09:30 $9,349.44 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,349.44 | ▲ 09:30 equity $9,349.44 vs yday $9,349.44 (+0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,349.44 | ▲ close $9,349.44 vs 09:30 $9,349.44 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,349.44 | ▲ 09:30 equity $9,349.44 vs yday $9,349.44 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `AVGO` | 10 | $351.74 | $2.02 | — | $5,830.02 | — | topheavy leftover on news🟢 +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list earn_react; 🔵; ret5=+3.3; leftover $3739.78 | — |
| 2026-09-03 09:30 ET | **BUY** | `DELL` | 11 | $486.31 | $2.02 | — | $478.59 | — | topheavy leftover on news🟢 +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ret5=+6.1; leftover $5609.67 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $478.59 | ▲ close $9,730.48 vs 09:30 $9,349.44 (session +385.08) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $478.59 | ▼ 09:30 equity $9,727.17 vs yday $9,730.48 (-3.31) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `AVGO` | 10 | $359.70 | $2.06 | $+75.52 | $4,073.53 | ▲ +75.52 after sell → book $9,725.11; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DELL` | 11 | $513.78 | $2.08 | $+298.07 | $9,723.03 | ▲ +298.07 after sell → book $9,723.03; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 14 | $263.36 | $2.03 | — | $6,033.96 | — | topheavy leftover on news🟢 +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $3889.21 | — |
| 2026-09-04 09:30 ET | **BUY** | `FRNM` | 355 | $16.40 | $4.58 | — | $207.38 | — | topheavy leftover on news🟢 +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.2; leftover $5833.82 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $207.38 | ▼ close $9,626.65 vs 09:30 $9,727.17 (session -89.77) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $207.38 | ▲ 09:30 equity $9,702.16 vs yday $9,626.65 (+75.51) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 14 | $253.72 | $2.07 | $-139.06 | $3,757.39 | ▼ -139.06 after sell → book $9,700.09; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 355 | $16.74 | $4.69 | $+111.43 | $9,695.40 | ▲ +111.43 after sell → book $9,695.40; vs 09:30 mark -4.69 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,695.40 | ▲ close $9,695.40 vs 09:30 $9,702.16 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,695.40 | ▲ 09:30 equity $9,695.40 vs yday $9,695.40 (+0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,695.40 | ▲ close $9,695.40 vs 09:30 $9,695.40 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,695.40 | ▲ 09:30 equity $9,695.40 vs yday $9,695.40 (+0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,695.40 | ▲ close $9,695.40 vs 09:30 $9,695.40 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,695.40 | ▲ 09:30 equity $9,695.40 vs yday $9,695.40 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 58 | $164.43 | $2.16 | — | $156.30 | — | topheavy leftover on news🟢 +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list flatten,earn_react; ⚪; ret5=+4.9; leftover $9695.40 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $156.30 | ▼ close $8,872.54 vs 09:30 $9,695.40 (session -820.70) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $156.30 | ▼ 09:30 equity $8,358.66 vs yday $8,872.54 (-513.88) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 58 | $141.42 | $2.24 | $-1338.98 | $8,356.42 | ▼ -1,338.98 after sell → book $8,356.42; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,356.42 | ▲ close $8,356.42 vs 09:30 $8,358.66 (session +0.00) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,356.42 | ▲ 09:30 equity $8,356.42 vs yday $8,356.42 (+0.00) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,356.42 | ▲ close $8,356.42 vs 09:30 $8,356.42 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,356.42 | ▲ 09:30 equity $8,356.42 vs yday $8,356.42 (+0.00) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `WAY` | 127 | $26.27 | $2.37 | — | $5,017.76 | — | topheavy leftover on news🟢 +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list yday_gainer; 🔵; ret5=+10.0; leftover $3342.57 | — |
| 2026-09-16 09:30 ET | **BUY** | `QCOM` | 26 | $189.17 | $2.07 | — | $97.27 | — | topheavy leftover on news🟢 +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ret5=+7.9; leftover $5013.85 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $97.27 | ▼ close $8,280.04 vs 09:30 $8,356.42 (session -71.94) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $97.27 | ▲ 09:30 equity $8,413.14 vs yday $8,280.04 (+133.10) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `WAY` | 127 | $26.51 | $2.42 | $+25.69 | $3,461.62 | ▲ +25.69 after sell → book $8,410.72; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `QCOM` | 26 | $190.35 | $2.12 | $+26.49 | $8,408.61 | ▲ +26.49 after sell → book $8,408.61; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `SMTC` | 49 | $170.85 | $2.14 | — | $34.82 | — | topheavy leftover on news🟢 +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.2; leftover $8408.61 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $34.82 | ▲ close $8,766.13 vs 09:30 $8,413.14 (session +359.66) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $34.82 | ▲ 09:30 equity $8,968.99 vs yday $8,766.13 (+202.86) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `SMTC` | 49 | $182.33 | $2.22 | $+558.16 | $8,966.77 | ▲ +558.16 after sell → book $8,966.77; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 171 | $20.91 | $2.50 | — | $5,388.66 | — | topheavy leftover on news🟢 +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $3586.71 | — |
| 2026-09-18 09:30 ET | **BUY** | `GME` | 234 | $22.90 | $3.02 | — | $27.04 | — | topheavy leftover on news🟢 +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+11.7; leftover $5380.06 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $27.04 | ▼ close $8,948.29 vs 09:30 $8,968.99 (session -12.96) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $27.04 | ▲ 09:30 equity $9,059.71 vs yday $8,948.29 (+111.42) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `TH` | 171 | $21.65 | $2.56 | $+121.48 | $3,726.63 | ▲ +121.48 after sell → book $9,057.15; vs 09:30 mark -2.56 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `GME` | 234 | $22.78 | $3.10 | $-34.20 | $9,054.05 | ▼ -34.20 after sell → book $9,054.05; vs 09:30 mark -3.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **BUY** | `VICR` | 15 | $230.25 | $2.04 | — | $5,598.26 | — | topheavy leftover on news🟢 +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list ohlc_hot; ret5=+12.5; leftover $3621.62 | — |
| 2026-09-21 09:30 ET | **BUY** | `SMTC` | 28 | $190.30 | $2.07 | — | $267.79 | — | topheavy leftover on news🟢 +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list ohlc_hot; ret5=+10.6; leftover $5432.43 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $267.79 | ▼ close $8,592.65 vs 09:30 $9,059.71 (session -457.29) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $267.79 | ▲ 09:30 equity $8,592.65 vs yday $8,592.65 (-0.00) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $267.79 | ▲ close $8,592.65 vs 09:30 $8,592.65 (session +0.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $267.79 | ▲ 09:30 equity $9,151.29 vs yday $8,592.65 (+558.64) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `VICR` | 15 | $266.50 | $2.08 | $+539.64 | $4,263.21 | ▲ +539.64 after sell → book $9,149.21; vs 09:30 mark -2.08 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **SELL** | `SMTC` | 28 | $174.50 | $2.12 | $-446.60 | $9,147.09 | ▼ -446.60 after sell → book $9,147.09; vs 09:30 mark -2.12 | dropped from list after 2 sess (min 1) | — |
| 2026-09-23 09:30 ET | **BUY** | `CTAS` | 18 | $196.78 | $2.04 | — | $5,603.00 | — | topheavy leftover on news🟢 +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list earn_react; 🔵; ⚪; ret5=-2.0; leftover $3658.84 | — |
| 2026-09-23 09:30 ET | **BUY** | `PGEN` | 690 | $7.95 | $8.90 | — | $108.60 | — | topheavy leftover on news🟢 +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list flatten; 🔵; ⚪; ret5=+12.4; leftover $5488.25 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $108.60 | ▼ close $8,697.66 vs 09:30 $9,151.29 (session -438.48) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $108.60 | ▼ 09:30 equity $8,661.48 vs yday $8,697.66 (-36.18) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `CTAS` | 18 | $192.26 | $2.08 | $-85.49 | $3,567.20 | ▼ -85.49 after sell → book $8,659.40; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 690 | $7.38 | $9.06 | $-411.26 | $8,650.35 | ▼ -411.26 after sell → book $8,650.35; vs 09:30 mark -9.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,650.35 | ▲ close $8,650.35 vs 09:30 $8,661.48 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,764.03 | ▲ 09:30 equity $8,764.03 vs yday $8,764.03 (+0.00) | 09:30 open · cash $8,764.03 · no holdings · equity $8,764.03 vs prior close $8,764.03 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 09:30 ET | **BUY** | `ZSQR` | 908 | $3.86 | $11.71 | — | $5,247.44 | — | topheavy leftover on news🟢 +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; leftover $3505.61 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ILMN` | 19 | $272.16 | $2.05 | — | $74.35 | — | topheavy leftover on news🟢 +7 −≤1; gate news=good,n_pos_min=7,cam_bad_max=1; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+11.7; leftover $5258.42 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $74.35 | ▼ close $8,636.59 vs 09:30 $8,764.03 (session -113.68) | 16:00 close · cash $74.35 · equity $8,636.59 vs 09:30 $8,764.03 (-127.44; session marks -113.68) · 2 name(s) marked open→close (per-name table). ZSQR×908 09:30 $3.86 → close $3.78 -72.64; ILMN×19 09:30 $272.16 → close $270.00 -41.04 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-22 | `VICR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `SMTC` | no_price | no 09:30 open — carry |
| 2026-09-24 | `OKTA` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `EOG` | hard_red | hard-red S=-7.66 sit; no new buys |
