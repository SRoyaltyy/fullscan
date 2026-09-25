# Factor mine action — `union_white_yday_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · union looker: 0 red cameras + yesterday up, rank +G−R

Cash book **-12.73%** ($8,727) · signal-only (no cash/fees) was +1.95%. Starts YES **3/30**. Fills 184 · skips 2 · realized $-327.39.

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
- Must-have: no morning camera is red (the 'white' / all-clear row).
- Must-have: yesterday's session was up (prior close-to-close Change% > 0, or last finished bar green if the % is missing).
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
- **Gate** `zero_red=True,yday_up=True` · **rank** `cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $9,672.56.

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
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 41 | $59.80 | $2.11 | — | $7,546.09 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten; ⚪; ret5=-5.3; leftover $2500.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 54 | $45.98 | $2.15 | — | $5,061.02 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten; ⚪; ret5=+12.3; leftover $2500.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 213 | $11.70 | $2.75 | — | $2,566.17 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten; ⚪; ret5=-0.8; leftover $2500.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 49 | $50.62 | $2.14 | — | $83.49 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten; ⚪; ret5=+6.2; leftover $2500.00 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $83.49 | ▲ close $10,279.02 vs 09:30 $10,000.00 (session +288.17) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $83.49 | ▼ 09:30 equity $10,260.41 vs yday $10,279.02 (-18.61) | — | — |
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 41 | $59.65 | $2.14 | $-10.41 | $2,527.00 | ▼ -10.41 after sell → book $10,258.27; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 54 | $44.09 | $2.18 | $-106.39 | $4,905.68 | ▼ -106.39 after sell → book $10,256.09; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `SLS` | 213 | $12.40 | $2.80 | $+143.55 | $7,544.08 | ▲ +143.55 after sell → book $10,253.29; vs 09:30 mark -2.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 49 | $55.29 | $2.17 | $+224.37 | $10,251.12 | ▲ +224.37 after sell → book $10,251.12; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `BRUN` | 48 | $26.25 | $2.13 | — | $8,989.22 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list earn_react; 🔵; ⚪; ret5=+31.2; leftover $1281.39 | — |
| 2026-08-14 09:30 ET | **BUY** | `HLIT` | 97 | $13.18 | $2.28 | — | $7,708.48 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer; 🔵; ⚪; ret5=+12.0; leftover $1281.39 | — |
| 2026-08-14 09:30 ET | **BUY** | `MNTN` | 102 | $12.50 | $2.30 | — | $6,431.19 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+11.7; leftover $1281.39 | — |
| 2026-08-14 09:30 ET | **BUY** | `QMCO` | 51 | $24.68 | $2.14 | — | $5,170.36 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer; 🔵; ⚪; ret5=+111.3; leftover $1281.39 | — |
| 2026-08-14 09:30 ET | **BUY** | `QMLS` | 175 | $7.29 | $2.52 | — | $3,892.10 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.1; leftover $1281.39 | — |
| 2026-08-14 09:30 ET | **BUY** | `ZENA` | 582 | $2.20 | $7.51 | — | $2,604.19 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer; 🔵; ⚪; ret5=+54.3; leftover $1281.39 | — |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 77 | $16.50 | $2.22 | — | $1,331.47 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1281.39 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,331.47 | ▼ close $10,162.57 vs 09:30 $10,260.41 (session -67.45) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,331.47 | ▼ 09:30 equity $10,000.76 vs yday $10,162.57 (-161.81) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `BRUN` | 48 | $23.00 | $2.15 | $-160.05 | $2,433.32 | ▼ -160.05 after sell → book $9,998.61; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HLIT` | 97 | $13.84 | $2.31 | $+59.43 | $3,773.49 | ▲ +59.43 after sell → book $9,996.30; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MNTN` | 102 | $12.40 | $2.32 | $-14.82 | $5,035.96 | ▼ -14.82 after sell → book $9,993.97; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `QMCO` | 51 | $24.83 | $2.16 | $+3.34 | $6,300.13 | ▲ +3.34 after sell → book $9,991.81; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `QMLS` | 175 | $7.24 | $2.55 | $-13.82 | $7,564.58 | ▼ -13.82 after sell → book $9,989.26; vs 09:30 mark -2.55 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ZENA` | 582 | $2.08 | $7.61 | $-82.05 | $8,770.43 | ▼ -82.05 after sell → book $9,981.64; vs 09:30 mark -7.62 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ADUR` | 77 | $15.73 | $2.24 | $-63.75 | $9,979.40 | ▼ -63.75 after sell → book $9,979.40; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `LPTH` | 83 | $14.94 | $2.24 | — | $8,737.14 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer,yday_mover,ohlc_hot; ⚪; ret5=+16.2; leftover $1247.42 | — |
| 2026-08-17 09:30 ET | **BUY** | `AAOI` | 8 | $152.64 | $2.01 | — | $7,514.01 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer,yday_mover,ohlc_hot; ⚪; ret5=+10.8; leftover $1247.42 | — |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 136 | $9.12 | $2.40 | — | $6,271.29 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $1247.42 | — |
| 2026-08-17 09:30 ET | **BUY** | `ALOY` | 85 | $14.66 | $2.25 | — | $5,022.94 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.0; leftover $1247.42 | — |
| 2026-08-17 09:30 ET | **BUY** | `BORR` | 271 | $4.59 | $3.50 | — | $3,775.56 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer,ohlc_hot; ⚪; ret5=+14.8; leftover $1247.42 | — |
| 2026-08-17 09:30 ET | **BUY** | `INDI` | 268 | $4.65 | $3.46 | — | $2,525.90 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list ohlc_hot; ⚪; ret5=+16.6; leftover $1247.42 | — |
| 2026-08-17 09:30 ET | **BUY** | `KOPN` | 229 | $5.43 | $2.95 | — | $1,279.48 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer; ⚪; ret5=+28.8; leftover $1247.42 | — |
| 2026-08-17 09:30 ET | **BUY** | `MP` | 21 | $58.01 | $2.05 | — | $59.21 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+14.9; leftover $1247.42 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $59.21 | ▼ close $9,873.50 vs 09:30 $10,000.76 (session -85.04) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $59.21 | ▼ 09:30 equity $9,512.49 vs yday $9,873.50 (-361.01) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `LPTH` | 83 | $14.01 | $2.26 | $-81.69 | $1,219.78 | ▼ -81.69 after sell → book $9,510.23; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `AAOI` | 8 | $146.20 | $2.03 | $-55.57 | $2,387.35 | ▼ -55.57 after sell → book $9,508.20; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 136 | $9.03 | $2.43 | $-17.07 | $3,613.00 | ▼ -17.07 after sell → book $9,505.77; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALOY` | 85 | $13.19 | $2.27 | $-129.46 | $4,731.88 | ▼ -129.46 after sell → book $9,503.50; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `BORR` | 271 | $4.56 | $3.55 | $-15.18 | $5,964.09 | ▼ -15.18 after sell → book $9,499.95; vs 09:30 mark -3.55 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `INDI` | 268 | $4.48 | $3.51 | $-52.53 | $7,161.21 | ▼ -52.53 after sell → book $9,496.43; vs 09:30 mark -3.52 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `KOPN` | 229 | $5.03 | $3.00 | $-97.56 | $8,310.08 | ▼ -97.56 after sell → book $9,493.43; vs 09:30 mark -3.00 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `MP` | 21 | $56.35 | $2.07 | $-38.99 | $9,491.36 | ▼ -38.99 after sell → book $9,491.36; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,491.36 | ▲ close $9,491.36 vs 09:30 $9,512.49 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,491.36 | ▲ 09:30 equity $9,491.36 vs yday $9,491.36 (-0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,491.36 | ▲ close $9,491.36 vs 09:30 $9,491.36 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,491.36 | ▲ 09:30 equity $9,491.36 vs yday $9,491.36 (-0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 57 | $20.55 | $2.16 | — | $8,317.85 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1186.42 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,132.69 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1186.42 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 57 | $20.65 | $2.16 | — | $5,953.48 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1186.42 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 205 | $5.77 | $2.64 | — | $4,767.98 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1186.42 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 60 | $19.63 | $2.17 | — | $3,588.01 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1186.42 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 40 | $29.63 | $2.11 | — | $2,400.70 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1186.42 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 677 | $1.75 | $8.73 | — | $1,207.22 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1186.42 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $48.89 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1186.42 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $48.89 | ▲ close $9,692.98 vs 09:30 $9,491.36 (session +225.64) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $48.89 | ▲ 09:30 equity $9,950.08 vs yday $9,692.98 (+257.10) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 57 | $21.90 | $2.18 | $+72.61 | $1,295.00 | ▲ +72.61 after sell → book $9,947.89; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $2,537.32 | ▲ +57.15 after sell → book $9,945.85; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 57 | $21.75 | $2.18 | $+58.36 | $3,774.88 | ▲ +58.36 after sell → book $9,943.66; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 205 | $5.67 | $2.69 | $-25.83 | $4,934.55 | ▼ -25.83 after sell → book $9,940.98; vs 09:30 mark -2.68 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 60 | $21.17 | $2.19 | $+88.04 | $6,202.56 | ▲ +88.04 after sell → book $9,938.79; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 40 | $32.17 | $2.13 | $+97.36 | $7,487.23 | ▲ +97.36 after sell → book $9,936.66; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 677 | $1.79 | $8.86 | $+9.49 | $8,690.20 | ▲ +9.49 after sell → book $9,927.80; vs 09:30 mark -8.86 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 8 | $154.70 | $2.03 | $+77.23 | $9,925.77 | ▲ +77.23 after sell → book $9,925.77; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $8,729.45 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1240.72 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 72 | $17.20 | $2.21 | — | $7,488.84 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $1240.72 | — |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 5 | $216.30 | $2.00 | — | $6,405.33 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1240.72 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 111 | $11.13 | $2.32 | — | $5,167.58 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1240.72 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 939 | $1.32 | $12.11 | — | $3,915.99 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1240.72 | — |
| 2026-08-21 09:30 ET | **BUY** | `FUTU` | 10 | $115.18 | $2.02 | — | $2,762.17 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.8; leftover $1240.72 | — |
| 2026-08-21 09:30 ET | **BUY** | `GRAL` | 15 | $78.88 | $2.04 | — | $1,576.93 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+14.3; leftover $1240.72 | — |
| 2026-08-21 09:30 ET | **BUY** | `ILMN` | 5 | $212.40 | $2.00 | — | $512.93 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+10.7; leftover $1240.72 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $512.93 | ▲ close $10,357.06 vs 09:30 $9,950.08 (session +458.02) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $512.93 | ▲ 09:30 equity $10,712.17 vs yday $10,357.06 (+355.11) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.51 | $2.04 | $+6.74 | $1,715.99 | ▲ +6.74 after sell → book $10,710.13; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 72 | $16.57 | $2.23 | $-49.79 | $2,906.80 | ▼ -49.79 after sell → book $10,707.90; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 5 | $217.03 | $2.02 | $-0.38 | $3,989.93 | ▼ -0.38 after sell → book $10,705.88; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 111 | $13.33 | $2.35 | $+239.52 | $5,467.20 | ▲ +239.52 after sell → book $10,703.52; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 939 | $1.83 | $12.28 | $+454.49 | $7,173.29 | ▲ +454.49 after sell → book $10,691.24; vs 09:30 mark -12.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `FUTU` | 10 | $121.00 | $2.04 | $+54.14 | $8,381.25 | ▲ +54.14 after sell → book $10,689.20; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `GRAL` | 15 | $81.87 | $2.06 | $+40.76 | $9,607.24 | ▲ +40.76 after sell → book $10,687.14; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ILMN` | 5 | $215.98 | $2.02 | $+13.87 | $10,685.12 | ▲ +13.87 after sell → book $10,685.12; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,685.12 | ▲ close $10,685.12 vs 09:30 $10,712.17 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,685.12 | ▲ 09:30 equity $10,685.12 vs yday $10,685.12 (-0.00) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 17 | $77.13 | $2.04 | — | $9,371.87 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $1335.64 | — |
| 2026-08-25 09:30 ET | **BUY** | `RHI` | 30 | $43.76 | $2.08 | — | $8,056.99 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+6.2; leftover $1335.64 | — |
| 2026-08-25 09:30 ET | **BUY** | `SUZ` | 148 | $8.98 | $2.43 | — | $6,725.51 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list ohlc_hot,mover_buy; ⚪; ret5=+15.4; leftover $1335.64 | — |
| 2026-08-25 09:30 ET | **BUY** | `VALE` | 88 | $15.01 | $2.25 | — | $5,402.38 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list mover_buy; ⚪; ret5=+9.4; leftover $1335.64 | — |
| 2026-08-25 09:30 ET | **BUY** | `WPM` | 8 | $156.51 | $2.01 | — | $4,148.29 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list ohlc_hot,mover_buy; ⚪; ret5=+17.4; leftover $1335.64 | — |
| 2026-08-25 09:30 ET | **BUY** | `AVAH` | 98 | $13.62 | $2.28 | — | $2,810.75 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.0; leftover $1335.64 | — |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 819 | $1.63 | $10.57 | — | $1,465.22 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; leftover $1335.64 | — |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 856 | $1.56 | $11.04 | — | $118.82 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; leftover $1335.64 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $118.82 | ▲ close $10,972.06 vs 09:30 $10,685.12 (session +321.65) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $118.82 | ▼ 09:30 equity $10,918.58 vs yday $10,972.06 (-53.48) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `FCX` | 17 | $79.34 | $2.06 | $+33.47 | $1,465.53 | ▲ +33.47 after sell → book $10,916.52; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `RHI` | 30 | $44.33 | $2.10 | $+12.92 | $2,793.33 | ▲ +12.92 after sell → book $10,914.42; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `SUZ` | 148 | $9.03 | $2.47 | $+2.50 | $4,127.30 | ▲ +2.50 after sell → book $10,911.95; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `VALE` | 88 | $15.37 | $2.28 | $+27.15 | $5,477.58 | ▲ +27.15 after sell → book $10,909.67; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `WPM` | 8 | $160.93 | $2.03 | $+31.31 | $6,762.99 | ▲ +31.31 after sell → book $10,907.63; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `AVAH` | 98 | $13.65 | $2.31 | $-2.14 | $8,098.38 | ▼ -2.14 after sell → book $10,905.32; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `BMEA` | 819 | $1.75 | $10.71 | $+81.10 | $9,525.01 | ▲ +81.10 after sell → book $10,894.61; vs 09:30 mark -10.71 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 856 | $1.60 | $11.20 | $+12.00 | $10,883.42 | ▲ +12.00 after sell → book $10,883.42; vs 09:30 mark -11.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,883.42 | ▲ close $10,883.42 vs 09:30 $10,918.58 (session +0.00) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,883.42 | ▲ 09:30 equity $10,883.42 vs yday $10,883.42 (-0.00) | — | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,883.42 | ▲ close $10,883.42 vs 09:30 $10,883.42 (session +0.00) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,883.42 | ▲ 09:30 equity $10,883.42 vs yday $10,883.42 (-0.00) | — | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 4 | $324.41 | $2.00 | — | $9,583.77 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1360.43 | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 9 | $141.76 | $2.02 | — | $8,305.92 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1360.43 | — |
| 2026-08-28 09:30 ET | **BUY** | `AVT` | 14 | $91.49 | $2.03 | — | $7,023.02 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+5.3; leftover $1360.43 | — |
| 2026-08-28 09:30 ET | **BUY** | `CGNX` | 21 | $62.82 | $2.05 | — | $5,701.75 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+5.4; leftover $1360.43 | — |
| 2026-08-28 09:30 ET | **BUY** | `COHR` | 4 | $289.44 | $2.00 | — | $4,541.99 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+1.8; leftover $1360.43 | — |
| 2026-08-28 09:30 ET | **BUY** | `LSCC` | 11 | $119.76 | $2.02 | — | $3,222.61 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.5; leftover $1360.43 | — |
| 2026-08-28 09:30 ET | **BUY** | `MEI` | 76 | $17.78 | $2.22 | — | $1,869.11 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+22.9; leftover $1360.43 | — |
| 2026-08-28 09:30 ET | **BUY** | `MTSI` | 4 | $275.20 | $2.00 | — | $766.31 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+4.1; leftover $1360.43 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $766.31 | ▼ close $10,557.44 vs 09:30 $10,883.42 (session -309.63) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $766.31 | ▲ 09:30 equity $10,607.49 vs yday $10,557.44 (+50.05) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 4 | $322.49 | $2.02 | $-11.70 | $2,054.24 | ▼ -11.70 after sell → book $10,605.46; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 9 | $132.30 | $2.04 | $-89.19 | $3,242.91 | ▼ -89.19 after sell → book $10,603.43; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `AVT` | 14 | $89.39 | $2.05 | $-33.48 | $4,492.32 | ▼ -33.48 after sell → book $10,601.38; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CGNX` | 21 | $60.46 | $2.07 | $-53.69 | $5,759.90 | ▼ -53.69 after sell → book $10,599.30; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `COHR` | 4 | $280.25 | $2.02 | $-40.78 | $6,878.88 | ▼ -40.78 after sell → book $10,597.28; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `LSCC` | 11 | $115.56 | $2.04 | $-50.27 | $8,148.00 | ▼ -50.27 after sell → book $10,595.24; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MEI` | 76 | $18.15 | $2.24 | $+23.66 | $9,525.16 | ▲ +23.66 after sell → book $10,593.00; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `MTSI` | 4 | $266.96 | $2.02 | $-36.98 | $10,590.97 | ▼ -36.98 after sell → book $10,590.97; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,590.97 | ▲ close $10,590.97 vs 09:30 $10,607.49 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,590.97 | ▲ 09:30 equity $10,590.97 vs yday $10,590.97 (+0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,590.97 | ▲ close $10,590.97 vs 09:30 $10,590.97 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,590.97 | ▲ 09:30 equity $10,590.97 vs yday $10,590.97 (+0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,590.97 | ▲ close $10,590.97 vs 09:30 $10,590.97 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,590.97 | ▲ 09:30 equity $10,590.97 vs yday $10,590.97 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 78 | $16.77 | $2.22 | — | $9,280.69 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1323.87 | — |
| 2026-09-03 09:30 ET | **BUY** | `BMEA` | 685 | $1.93 | $8.84 | — | $7,949.80 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1323.87 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 607 | $2.18 | $7.83 | — | $6,618.71 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $1323.87 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 30 | $42.93 | $2.08 | — | $5,328.73 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1323.87 | — |
| 2026-09-03 09:30 ET | **BUY** | `NVAX` | 127 | $10.42 | $2.37 | — | $4,003.02 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+12.1; leftover $1323.87 | — |
| 2026-09-03 09:30 ET | **BUY** | `PBH` | 24 | $53.45 | $2.06 | — | $2,718.16 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+1.3; leftover $1323.87 | — |
| 2026-09-03 09:30 ET | **BUY** | `PCRX` | 49 | $26.74 | $2.14 | — | $1,405.76 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.6; leftover $1323.87 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $132.45 | $2.02 | — | $211.70 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1323.87 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $211.70 | ▼ close $10,354.34 vs 09:30 $10,590.97 (session -207.08) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $211.70 | ▼ 09:30 equity $10,326.49 vs yday $10,354.34 (-27.85) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 78 | $15.61 | $2.25 | $-94.95 | $1,427.03 | ▼ -94.95 after sell → book $10,324.24; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `BMEA` | 685 | $1.90 | $8.96 | $-38.35 | $2,719.57 | ▼ -38.35 after sell → book $10,315.28; vs 09:30 mark -8.96 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRDL` | 607 | $2.16 | $7.94 | $-27.91 | $4,022.75 | ▼ -27.91 after sell → book $10,307.34; vs 09:30 mark -7.94 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `HRMY` | 30 | $41.50 | $2.10 | $-47.08 | $5,265.65 | ▼ -47.08 after sell → book $10,305.24; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `NVAX` | 127 | $10.50 | $2.40 | $+5.39 | $6,596.74 | ▲ +5.39 after sell → book $10,302.83; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `PBH` | 24 | $51.80 | $2.08 | $-43.74 | $7,837.86 | ▼ -43.74 after sell → book $10,300.75; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `PCRX` | 49 | $26.38 | $2.16 | $-21.93 | $9,128.32 | ▼ -21.93 after sell → book $10,298.59; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 9 | $130.03 | $2.04 | $-25.83 | $10,296.56 | ▼ -25.83 after sell → book $10,296.56; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 4 | $263.36 | $2.00 | — | $9,241.12 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $1287.07 | — |
| 2026-09-04 09:30 ET | **BUY** | `TARS` | 15 | $82.70 | $2.04 | — | $7,998.58 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+15.7; leftover $1287.07 | — |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 2 | $513.78 | $2.00 | — | $6,969.02 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+9.3; leftover $1287.07 | — |
| 2026-09-04 09:30 ET | **BUY** | `FRNM` | 78 | $16.40 | $2.22 | — | $5,687.60 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.2; leftover $1287.07 | — |
| 2026-09-04 09:30 ET | **BUY** | `IRD` | 284 | $4.53 | $3.66 | — | $4,397.42 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.5; leftover $1287.07 | — |
| 2026-09-04 09:30 ET | **BUY** | `LENZ` | 223 | $5.75 | $2.88 | — | $3,112.29 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer; 🔵; ⚪; ret5=+21.2; leftover $1287.07 | — |
| 2026-09-04 09:30 ET | **BUY** | `PIPR` | 16 | $76.55 | $2.04 | — | $1,885.45 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+4.2; leftover $1287.07 | — |
| 2026-09-04 09:30 ET | **BUY** | `TDS` | 34 | $37.44 | $2.09 | — | $610.40 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+14.1; leftover $1287.07 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $610.40 | ▲ close $10,503.70 vs 09:30 $10,326.49 (session +226.07) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $610.40 | ▼ 09:30 equity $10,441.46 vs yday $10,503.70 (-62.24) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `CRM` | 4 | $253.72 | $2.02 | $-42.58 | $1,623.26 | ▼ -42.58 after sell → book $10,439.44; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `TARS` | 15 | $89.67 | $2.06 | $+100.46 | $2,966.25 | ▲ +100.46 after sell → book $10,437.38; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `DELL` | 2 | $521.15 | $2.02 | $+10.73 | $4,006.54 | ▲ +10.73 after sell → book $10,435.37; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `FRNM` | 78 | $16.74 | $2.25 | $+22.05 | $5,310.01 | ▲ +22.05 after sell → book $10,433.12; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `IRD` | 284 | $4.53 | $3.72 | $-7.38 | $6,592.81 | ▼ -7.38 after sell → book $10,429.40; vs 09:30 mark -3.72 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `LENZ` | 223 | $5.95 | $2.92 | $+38.80 | $7,916.73 | ▲ +38.80 after sell → book $10,426.47; vs 09:30 mark -2.93 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `PIPR` | 16 | $76.64 | $2.06 | $-2.66 | $9,140.92 | ▼ -2.66 after sell → book $10,424.42; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `TDS` | 34 | $37.75 | $2.11 | $+6.34 | $10,422.30 | ▲ +6.34 after sell → book $10,422.30; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,422.30 | ▲ close $10,422.30 vs 09:30 $10,441.46 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,422.30 | ▲ 09:30 equity $10,422.30 vs yday $10,422.30 (+0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,422.30 | ▲ close $10,422.30 vs 09:30 $10,422.30 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,422.30 | ▲ 09:30 equity $10,422.30 vs yday $10,422.30 (+0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,422.30 | ▲ close $10,422.30 vs 09:30 $10,422.30 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,422.30 | ▲ 09:30 equity $10,422.30 vs yday $10,422.30 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `BAND` | 49 | $52.55 | $2.14 | — | $7,845.22 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list probable,yday_gainer; 🔵; ⚪; ret5=+8.6; leftover $2605.58 | — |
| 2026-09-11 09:30 ET | **BUY** | `PAGS` | 257 | $10.11 | $3.32 | — | $5,243.63 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer; 🔵; ⚪; ret5=+2.9; leftover $2605.58 | — |
| 2026-09-11 09:30 ET | **BUY** | `PAYP` | 142 | $18.30 | $2.42 | — | $2,642.62 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $2605.58 | — |
| 2026-09-11 09:30 ET | **BUY** | `ZSQR` | 801 | $3.25 | $10.33 | — | $29.03 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer; 🔵; ⚪; ret5=+3.6; leftover $2605.58 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $29.03 | ▲ close $10,495.47 vs 09:30 $10,422.30 (session +91.37) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $29.03 | ▼ 09:30 equity $10,433.95 vs yday $10,495.47 (-61.52) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `BAND` | 49 | $56.90 | $2.17 | $+208.84 | $2,814.96 | ▲ +208.84 after sell → book $10,431.78; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `PAGS` | 257 | $10.00 | $3.38 | $-34.96 | $5,381.58 | ▼ -34.96 after sell → book $10,428.40; vs 09:30 mark -3.38 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `PAYP` | 142 | $18.28 | $2.46 | $-7.72 | $7,974.88 | ▼ -7.72 after sell → book $10,425.94; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ZSQR` | 801 | $3.06 | $10.49 | $-173.01 | $10,415.46 | ▼ -173.01 after sell → book $10,415.46; vs 09:30 mark -10.48 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,415.46 | ▲ close $10,415.46 vs 09:30 $10,433.95 (session +0.00) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,415.46 | ▲ 09:30 equity $10,415.46 vs yday $10,415.46 (-0.00) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,415.46 | ▲ close $10,415.46 vs 09:30 $10,415.46 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,415.46 | ▲ 09:30 equity $10,415.46 vs yday $10,415.46 (-0.00) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `QRVO` | 44 | $118.18 | $2.12 | — | $5,213.42 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+13.4; leftover $5207.73 | — |
| 2026-09-16 09:30 ET | **BUY** | `SWKS` | 58 | $89.38 | $2.16 | — | $27.21 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.4; leftover $5207.73 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $27.21 | ▼ close $10,006.11 vs 09:30 $10,415.46 (session -405.06) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $27.21 | ▲ 09:30 equity $10,114.89 vs yday $10,006.11 (+108.78) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `QRVO` | 44 | $114.90 | $2.17 | $-148.61 | $5,080.64 | ▼ -148.61 after sell → book $10,112.72; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `SWKS` | 58 | $86.76 | $2.21 | $-156.34 | $10,110.51 | ▼ -156.34 after sell → book $10,110.51; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,110.51 | ▲ close $10,110.51 vs 09:30 $10,114.89 (session +0.00) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,110.51 | ▲ 09:30 equity $10,110.51 vs yday $10,110.51 (-0.00) | — | — |
| 2026-09-18 09:30 ET | **BUY** | `CRWD` | 5 | $246.98 | $2.00 | — | $8,873.60 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1263.81 | — |
| 2026-09-18 09:30 ET | **BUY** | `FIVN` | 36 | $34.44 | $2.10 | — | $7,631.66 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten; 🔵; ⚪; ret5=+14.0; leftover $1263.81 | — |
| 2026-09-18 09:30 ET | **BUY** | `ATRC` | 21 | $58.51 | $2.05 | — | $6,400.90 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+11.6; leftover $1263.81 | — |
| 2026-09-18 09:30 ET | **BUY** | `ECO` | 14 | $85.00 | $2.03 | — | $5,208.87 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten; 🔵; ⚪; ret5=+18.3; leftover $1263.81 | — |
| 2026-09-18 09:30 ET | **BUY** | `PGEN` | 158 | $7.98 | $2.46 | — | $3,945.56 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+17.8; leftover $1263.81 | — |
| 2026-09-18 09:30 ET | **BUY** | `RBRK` | 11 | $108.55 | $2.02 | — | $2,749.49 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten; ⚪; ret5=+21.3; leftover $1263.81 | — |
| 2026-09-18 09:30 ET | **BUY** | `RXT` | 320 | $3.94 | $4.13 | — | $1,484.56 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer; 🔵; ⚪; ret5=+25.2; leftover $1263.81 | — |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 60 | $20.91 | $2.17 | — | $227.79 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $1263.81 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $227.79 | ▼ close $9,884.81 vs 09:30 $10,110.51 (session -206.72) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $227.79 | ▲ 09:30 equity $9,925.33 vs yday $9,884.81 (+40.52) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `CRWD` | 5 | $231.62 | $2.02 | $-80.83 | $1,383.87 | ▼ -80.83 after sell → book $9,923.31; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `FIVN` | 36 | $33.00 | $2.12 | $-56.06 | $2,569.75 | ▼ -56.06 after sell → book $9,921.19; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `ATRC` | 21 | $58.23 | $2.07 | $-10.01 | $3,790.51 | ▼ -10.01 after sell → book $9,919.12; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `ECO` | 14 | $82.83 | $2.05 | $-34.46 | $4,948.07 | ▼ -34.46 after sell → book $9,917.06; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `PGEN` | 158 | $7.84 | $2.50 | $-27.08 | $6,184.29 | ▼ -27.08 after sell → book $9,914.56; vs 09:30 mark -2.50 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RBRK` | 11 | $107.57 | $2.04 | $-14.85 | $7,365.52 | ▼ -14.85 after sell → book $9,912.52; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RXT` | 320 | $3.90 | $4.19 | $-21.12 | $8,609.33 | ▼ -21.12 after sell → book $9,908.33; vs 09:30 mark -4.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TH` | 60 | $21.65 | $2.19 | $+40.04 | $9,906.14 | ▲ +40.04 after sell → book $9,906.14; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,906.14 | ▲ close $9,906.14 vs 09:30 $9,925.33 (session +0.00) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,906.14 | ▲ 09:30 equity $9,906.14 vs yday $9,906.14 (+0.00) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,906.14 | ▲ close $9,906.14 vs 09:30 $9,906.14 (session +0.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,906.14 | ▲ 09:30 equity $9,906.14 vs yday $9,906.14 (+0.00) | — | — |
| 2026-09-23 09:30 ET | **BUY** | `A` | 7 | $166.54 | $2.01 | — | $8,738.35 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten; 🔵; ⚪; ret5=+10.3; leftover $1238.27 | — |
| 2026-09-23 09:30 ET | **BUY** | `BFLY` | 125 | $9.90 | $2.37 | — | $7,498.48 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+27.3; leftover $1238.27 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 59 | $20.65 | $2.17 | — | $6,277.97 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $1238.27 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 126 | $9.81 | $2.37 | — | $5,039.54 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten; 🔵; ⚪; ret5=+4.0; leftover $1238.27 | — |
| 2026-09-23 09:30 ET | **BUY** | `AMRX` | 62 | $19.70 | $2.18 | — | $3,815.96 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+17.1; leftover $1238.27 | — |
| 2026-09-23 09:30 ET | **BUY** | `ARQT` | 44 | $27.79 | $2.12 | — | $2,591.08 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten; 🔵; ⚪; ret5=+7.0; leftover $1238.27 | — |
| 2026-09-23 09:30 ET | **BUY** | `VICR` | 4 | $266.50 | $2.00 | — | $1,523.08 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.2; leftover $1238.27 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,523.08 | ▼ close $9,768.00 vs 09:30 $9,906.14 (session -122.93) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,523.08 | ▼ 09:30 equity $9,687.93 vs yday $9,768.00 (-80.07) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `A` | 7 | $163.95 | $2.03 | $-22.17 | $2,668.70 | ▼ -22.17 after sell → book $9,685.90; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `BFLY` | 125 | $9.12 | $2.40 | $-102.26 | $3,806.30 | ▼ -102.26 after sell → book $9,683.50; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `OMER` | 59 | $20.52 | $2.19 | $-12.02 | $5,014.80 | ▼ -12.02 after sell → book $9,681.32; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `ADMA` | 126 | $9.67 | $2.40 | $-22.41 | $6,230.82 | ▼ -22.41 after sell → book $9,678.92; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `AMRX` | 62 | $19.29 | $2.20 | $-29.79 | $7,424.60 | ▼ -29.79 after sell → book $9,676.72; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `ARQT` | 44 | $26.22 | $2.14 | $-73.34 | $8,576.14 | ▼ -73.34 after sell → book $9,674.58; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `VICR` | 4 | $274.61 | $2.02 | $+28.42 | $9,672.56 | ▲ +28.42 after sell → book $9,672.56; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,672.56 | ▲ close $9,672.56 vs 09:30 $9,687.93 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,684.76 | ▲ 09:30 equity $8,684.76 vs yday $8,684.76 (+0.00) | 09:30 open · cash $8,684.76 · no holdings · equity $8,684.76 vs prior close $8,684.76 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 09:30 ET | **BUY** | `RSKD` | 138 | $7.85 | $2.40 | — | $7,599.06 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer; 🔵; ⚪; ret5=+25.4; leftover $1085.60 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `CDNS` | 3 | $324.97 | $2.00 | — | $6,622.15 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+14.7; leftover $1085.60 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `CYPH` | 271 | $4.00 | $3.50 | — | $5,533.30 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+32.9; leftover $1085.60 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `DUOT` | 113 | $9.59 | $2.33 | — | $4,447.30 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+12.4; leftover $1085.60 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `GRAL` | 8 | $123.50 | $2.01 | — | $3,457.28 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+56.6; leftover $1085.60 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `HALO` | 9 | $115.36 | $2.02 | — | $2,417.03 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list flatten; 🔵; ⚪; ret5=+5.1; leftover $1085.60 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `PACB` | 691 | $1.57 | $8.91 | — | $1,323.24 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list yday_gainer,yday_mover,ohlc_hot; 🔵; ⚪; ret5=+14.5; leftover $1085.60 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `PRGO` | 73 | $14.81 | $2.21 | — | $239.90 | — | union looker: 0 red cameras + yesterday up, rank +G−R; gate zero_red=True,yday_up=True; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.9; leftover $1085.60 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $239.90 | ▲ close $8,726.57 vs 09:30 $8,684.76 (session +67.20) | 16:00 close · cash $239.90 · equity $8,726.57 vs 09:30 $8,684.76 (+41.81; session marks +67.20) · 8 name(s) marked open→close (per-name table). RSKD×138 09:30 $7.85 → close $7.78 -9.66; CDNS×3 09:30 $324.97 → close $326.13 +3.48; CYPH×271 09:30 $4.00 → close $4.12 +31.17; DUOT×113 09:30 $9.59 → close $9.14 -50.85; GRAL×8 09:30 $123.50 → close $126.89 +27.12; HALO×9 09:30 $115.36 → close $113.90 -13.14; PACB×691 09:30 $1.57 → close $1.62 +34.55; PRGO×73 09:30 $14.81 → close $15.42 +44.53 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `SNDK` | cash | leftover split 1281.39 < 1 share @ 1646.93 |
| 2026-09-23 | `MPWR` | cash | leftover split 1238.27 < 1 share @ 1367.08 |
