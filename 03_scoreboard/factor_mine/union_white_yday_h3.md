# Factor mine action — `union_white_yday_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · union looker hold 3: 0 red + yesterday up

Cash book **-11.16%** ($8,884) · signal-only (no cash/fees) was -11.78%. Starts YES **2/30**. Fills 120 · skips 140 · realized $-1313.49.

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
- Minimum hold is 3 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 3 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `zero_red=True,yday_up=True` · **rank** `cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $1,206.93.

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
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 41 | $59.80 | $2.11 | — | $7,546.09 | — | union looker hold 3: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten; ⚪; ret5=-5.3; leftover $2500.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 54 | $45.98 | $2.15 | — | $5,061.02 | — | union looker hold 3: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten; ⚪; ret5=+12.3; leftover $2500.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 213 | $11.70 | $2.75 | — | $2,566.17 | — | union looker hold 3: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten; ⚪; ret5=-0.8; leftover $2500.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 49 | $50.62 | $2.14 | — | $83.49 | — | union looker hold 3: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten; ⚪; ret5=+6.2; leftover $2500.00 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $83.49 | ▲ close $10,279.02 vs 09:30 $10,000.00 (session +288.17) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $83.49 | ▼ 09:30 equity $10,260.41 vs yday $10,279.02 (-18.61) | — | — |
| 2026-08-14 09:30 ET | **BUY** | `QMLS` | 1 | $7.29 | $0.08 | — | $76.13 | — | union looker hold 3: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.1; leftover $10.44 | — |
| 2026-08-14 09:30 ET | **BUY** | `ZENA` | 4 | $2.20 | $0.10 | — | $67.23 | — | union looker hold 3: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list yday_gainer; 🔵; ⚪; ret5=+54.3; leftover $10.44 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $67.23 | ▲ close $10,313.07 vs 09:30 $10,260.41 (session +52.83) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $67.23 | ▲ 09:30 equity $10,357.49 vs yday $10,313.07 (+44.42) | — | — |
| 2026-08-17 09:30 ET | **BUY** | `BORR` | 1 | $4.59 | $0.05 | — | $62.59 | — | union looker hold 3: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list yday_gainer,ohlc_hot; ⚪; ret5=+14.8; leftover $8.40 | — |
| 2026-08-17 09:30 ET | **BUY** | `INDI` | 1 | $4.65 | $0.05 | — | $57.89 | — | union looker hold 3: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list ohlc_hot; ⚪; ret5=+16.6; leftover $8.40 | — |
| 2026-08-17 09:30 ET | **BUY** | `KOPN` | 1 | $5.43 | $0.06 | — | $52.40 | — | union looker hold 3: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list yday_gainer; ⚪; ret5=+28.8; leftover $8.40 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $52.40 | ▼ close $10,288.18 vs 09:30 $10,357.49 (session -69.15) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $52.40 | ▼ 09:30 equity $10,126.65 vs yday $10,288.18 (-161.53) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `BTSG` | 41 | $60.00 | $2.14 | $+3.94 | $2,510.26 | ▲ +3.94 after sell → book $10,124.51; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `IREN` | 54 | $43.56 | $2.18 | $-135.01 | $4,860.32 | ▼ -135.01 after sell → book $10,122.33; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `SLS` | 213 | $12.66 | $2.80 | $+198.93 | $7,554.09 | ▲ +198.93 after sell → book $10,119.52; vs 09:30 mark -2.81 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 49 | $51.77 | $2.17 | $+51.89 | $10,088.66 | ▲ +51.89 after sell → book $10,117.36; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,088.66 | ▲ close $10,117.37 vs 09:30 $10,126.65 (session +0.01) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,088.66 | ▲ 09:30 equity $10,117.46 vs yday $10,117.37 (+0.09) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `QMLS` | 1 | $6.74 | $0.09 | $-0.72 | $10,095.31 | ▼ -0.72 after sell → book $10,117.37; vs 09:30 mark -0.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ZENA` | 4 | $2.01 | $0.11 | $-0.97 | $10,103.23 | ▼ -0.97 after sell → book $10,117.25; vs 09:30 mark -0.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,103.23 | ▼ close $10,116.57 vs 09:30 $10,117.46 (session -0.69) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,103.23 | ▼ 09:30 equity $10,116.46 vs yday $10,116.57 (-0.11) | — | — |
| 2026-08-20 09:30 ET | **SELL** | `BORR` | 1 | $4.46 | $0.07 | $-0.25 | $10,107.63 | ▼ -0.25 after sell → book $10,116.40; vs 09:30 mark -0.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `INDI` | 1 | $3.90 | $0.06 | $-0.86 | $10,111.46 | ▼ -0.86 after sell → book $10,116.33; vs 09:30 mark -0.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `KOPN` | 1 | $4.87 | $0.07 | $-0.69 | $10,116.26 | ▼ -0.69 after sell → book $10,116.26; vs 09:30 mark -0.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 61 | $20.55 | $2.17 | — | $8,860.54 | — | union looker hold 3: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1264.53 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,675.38 | — | union looker hold 3: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1264.53 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 61 | $20.65 | $2.17 | — | $6,413.56 | — | union looker hold 3: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1264.53 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 219 | $5.77 | $2.83 | — | $5,147.10 | — | union looker hold 3: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1264.53 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 64 | $19.63 | $2.18 | — | $3,888.60 | — | union looker hold 3: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1264.53 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 42 | $29.63 | $2.12 | — | $2,642.02 | — | union looker hold 3: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1264.53 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 722 | $1.75 | $9.31 | — | $1,369.21 | — | union looker hold 3: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1264.53 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $210.88 | — | union looker hold 3: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1264.53 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $210.88 | ▲ close $10,325.76 vs 09:30 $10,116.46 (session +234.32) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $210.88 | ▲ 09:30 equity $10,595.62 vs yday $10,325.76 (+269.86) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 1 | $17.20 | $0.17 | — | $193.50 | — | union looker hold 3: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $26.36 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 2 | $11.13 | $0.23 | — | $171.01 | — | union looker hold 3: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $26.36 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 19 | $1.32 | $0.31 | — | $145.63 | — | union looker hold 3: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $26.36 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $145.63 | ▲ close $10,595.78 vs 09:30 $10,595.62 (session +0.87) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $145.63 | ▲ 09:30 equity $10,705.43 vs yday $10,595.78 (+109.65) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $145.63 | ▼ close $10,672.36 vs 09:30 $10,705.43 (session -33.07) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $145.63 | ▼ 09:30 equity $10,504.34 vs yday $10,672.36 (-168.02) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 61 | $20.32 | $2.19 | $-18.40 | $1,382.95 | ▼ -18.40 after sell → book $10,502.14; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 13 | $95.86 | $2.05 | $+58.97 | $2,627.08 | ▲ +58.97 after sell → book $10,500.09; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CDE` | 61 | $20.47 | $2.19 | $-15.35 | $3,873.56 | ▼ -15.35 after sell → book $10,497.90; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 219 | $5.53 | $2.87 | $-58.26 | $5,081.76 | ▼ -58.26 after sell → book $10,495.03; vs 09:30 mark -2.87 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 64 | $21.21 | $2.20 | $+96.73 | $6,436.99 | ▲ +96.73 after sell → book $10,492.82; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 42 | $32.32 | $2.14 | $+108.73 | $7,792.30 | ▲ +108.73 after sell → book $10,490.69; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 722 | $1.90 | $9.44 | $+89.54 | $9,154.65 | ▲ +89.54 after sell → book $10,481.24; vs 09:30 mark -9.45 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 19 | $77.13 | $2.05 | — | $7,687.14 | — | union looker hold 3: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list mover_buy; ⚪; ret5=+13.8; leftover $1525.78 | — |
| 2026-08-25 09:30 ET | **BUY** | `RHI` | 34 | $43.76 | $2.09 | — | $6,197.20 | — | union looker hold 3: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+6.2; leftover $1525.78 | — |
| 2026-08-25 09:30 ET | **BUY** | `SUZ` | 169 | $8.98 | $2.50 | — | $4,677.09 | — | union looker hold 3: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list ohlc_hot,mover_buy; ⚪; ret5=+15.4; leftover $1525.78 | — |
| 2026-08-25 09:30 ET | **BUY** | `VALE` | 101 | $15.01 | $2.29 | — | $3,158.78 | — | union looker hold 3: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list mover_buy; ⚪; ret5=+9.4; leftover $1525.78 | — |
| 2026-08-25 09:30 ET | **BUY** | `AVAH` | 111 | $13.62 | $2.32 | — | $1,644.09 | — | union looker hold 3: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.0; leftover $1525.78 | — |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 936 | $1.63 | $12.07 | — | $106.33 | — | union looker hold 3: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; leftover $1525.78 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $106.33 | ▲ close $10,741.94 vs 09:30 $10,504.34 (session +284.02) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $106.33 | ▼ 09:30 equity $10,722.42 vs yday $10,741.94 (-19.52) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `WPM` | 8 | $160.93 | $2.03 | $+127.07 | $1,391.74 | ▲ +127.07 after sell → book $10,720.39; vs 09:30 mark -2.03 | dropped from list after 4 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `AUPH` | 1 | $16.60 | $0.19 | $-0.96 | $1,408.15 | ▼ -0.96 after sell → book $10,720.20; vs 09:30 mark -0.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `ARCT` | 2 | $15.35 | $0.33 | $+7.88 | $1,438.52 | ▲ +7.88 after sell → book $10,719.87; vs 09:30 mark -0.33 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 19 | $1.60 | $0.38 | $+4.63 | $1,468.53 | ▲ +4.63 after sell → book $10,719.48; vs 09:30 mark -0.39 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,468.53 | ▼ close $10,638.29 vs 09:30 $10,722.42 (session -81.19) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,468.53 | ▲ 09:30 equity $10,649.59 vs yday $10,638.29 (+11.30) | — | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,468.53 | ▲ close $10,654.54 vs 09:30 $10,649.59 (session +4.95) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,468.53 | ▲ 09:30 equity $10,672.17 vs yday $10,654.54 (+17.63) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `FCX` | 19 | $78.57 | $2.07 | $+23.24 | $2,959.30 | ▲ +23.24 after sell → book $10,670.11; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `RHI` | 34 | $44.51 | $2.11 | $+21.29 | $4,470.52 | ▲ +21.29 after sell → book $10,667.99; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `SUZ` | 169 | $9.05 | $2.54 | $+6.80 | $5,997.43 | ▲ +6.80 after sell → book $10,665.45; vs 09:30 mark -2.54 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `VALE` | 101 | $15.28 | $2.32 | $+22.65 | $7,538.39 | ▲ +22.65 after sell → book $10,663.13; vs 09:30 mark -2.32 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `AVAH` | 111 | $13.90 | $2.35 | $+25.85 | $9,078.94 | ▲ +25.85 after sell → book $10,660.78; vs 09:30 mark -2.35 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BMEA` | 936 | $1.69 | $12.24 | $+31.84 | $10,648.54 | ▲ +31.84 after sell → book $10,648.54; vs 09:30 mark -12.24 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 4 | $324.41 | $2.00 | — | $9,348.89 | — | union looker hold 3: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1331.07 | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 9 | $141.76 | $2.02 | — | $8,071.04 | — | union looker hold 3: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1331.07 | — |
| 2026-08-28 09:30 ET | **BUY** | `AVT` | 14 | $91.49 | $2.03 | — | $6,788.15 | — | union looker hold 3: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+5.3; leftover $1331.07 | — |
| 2026-08-28 09:30 ET | **BUY** | `CGNX` | 21 | $62.82 | $2.05 | — | $5,466.87 | — | union looker hold 3: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+5.4; leftover $1331.07 | — |
| 2026-08-28 09:30 ET | **BUY** | `COHR` | 4 | $289.44 | $2.00 | — | $4,307.11 | — | union looker hold 3: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+1.8; leftover $1331.07 | — |
| 2026-08-28 09:30 ET | **BUY** | `LSCC` | 11 | $119.76 | $2.02 | — | $2,987.73 | — | union looker hold 3: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.5; leftover $1331.07 | — |
| 2026-08-28 09:30 ET | **BUY** | `MEI` | 74 | $17.78 | $2.21 | — | $1,669.80 | — | union looker hold 3: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+22.9; leftover $1331.07 | — |
| 2026-08-28 09:30 ET | **BUY** | `MTSI` | 4 | $275.20 | $2.00 | — | $566.99 | — | union looker hold 3: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+4.1; leftover $1331.07 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $566.99 | ▼ close $10,321.70 vs 09:30 $10,672.17 (session -310.49) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $566.99 | ▲ 09:30 equity $10,371.87 vs yday $10,321.70 (+50.17) | — | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $566.99 | ▲ close $10,372.83 vs 09:30 $10,371.87 (session +0.96) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $566.99 | ▼ 09:30 equity $10,182.60 vs yday $10,372.83 (-190.23) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $566.99 | ▲ close $10,267.96 vs 09:30 $10,182.60 (session +85.36) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $566.99 | ▼ 09:30 equity $10,250.41 vs yday $10,267.96 (-17.55) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `KEYS` | 4 | $318.04 | $2.02 | $-29.50 | $1,837.13 | ▼ -29.50 after sell → book $10,248.39; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SMTC` | 9 | $133.00 | $2.04 | $-82.89 | $3,032.09 | ▼ -82.89 after sell → book $10,246.35; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `AVT` | 14 | $89.39 | $2.05 | $-33.48 | $4,281.50 | ▼ -33.48 after sell → book $10,244.30; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CGNX` | 21 | $60.37 | $2.07 | $-55.58 | $5,547.20 | ▼ -55.58 after sell → book $10,242.23; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `COHR` | 4 | $268.12 | $2.02 | $-89.30 | $6,617.66 | ▼ -89.30 after sell → book $10,240.21; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `LSCC` | 11 | $112.09 | $2.04 | $-88.44 | $7,848.60 | ▼ -88.44 after sell → book $10,238.16; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `MEI` | 74 | $18.22 | $2.24 | $+28.11 | $9,194.65 | ▲ +28.11 after sell → book $10,235.93; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `MTSI` | 4 | $260.32 | $2.02 | $-63.54 | $10,233.91 | ▼ -63.54 after sell → book $10,233.91; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,233.91 | ▲ close $10,233.91 vs 09:30 $10,250.41 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,233.91 | ▲ 09:30 equity $10,233.91 vs yday $10,233.91 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 76 | $16.77 | $2.22 | — | $8,957.17 | — | union looker hold 3: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1279.24 | — |
| 2026-09-03 09:30 ET | **BUY** | `BMEA` | 662 | $1.93 | $8.54 | — | $7,670.97 | — | union looker hold 3: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+13.2; leftover $1279.24 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 586 | $2.18 | $7.56 | — | $6,385.93 | — | union looker hold 3: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $1279.24 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 29 | $42.93 | $2.08 | — | $5,138.88 | — | union looker hold 3: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1279.24 | — |
| 2026-09-03 09:30 ET | **BUY** | `NVAX` | 122 | $10.42 | $2.36 | — | $3,865.29 | — | union looker hold 3: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+12.1; leftover $1279.24 | — |
| 2026-09-03 09:30 ET | **BUY** | `PBH` | 23 | $53.45 | $2.06 | — | $2,633.88 | — | union looker hold 3: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+1.3; leftover $1279.24 | — |
| 2026-09-03 09:30 ET | **BUY** | `PCRX` | 47 | $26.74 | $2.13 | — | $1,374.97 | — | union looker hold 3: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+2.6; leftover $1279.24 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $132.45 | $2.02 | — | $180.90 | — | union looker hold 3: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1279.24 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $180.90 | ▼ close $10,003.81 vs 09:30 $10,233.91 (session -201.14) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $180.90 | ▼ 09:30 equity $9,976.85 vs yday $10,003.81 (-26.96) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `FRNM` | 1 | $16.40 | $0.17 | — | $164.33 | — | union looker hold 3: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list mover_buy; 🔵; ⚪; ret5=+21.2; leftover $22.61 | — |
| 2026-09-04 09:30 ET | **BUY** | `IRD` | 4 | $4.53 | $0.19 | — | $146.02 | — | union looker hold 3: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.5; leftover $22.61 | — |
| 2026-09-04 09:30 ET | **BUY** | `LENZ` | 3 | $5.75 | $0.18 | — | $128.59 | — | union looker hold 3: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list yday_gainer; 🔵; ⚪; ret5=+21.2; leftover $22.61 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $128.59 | ▲ close $10,101.34 vs 09:30 $9,976.85 (session +125.03) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $128.59 | ▼ 09:30 equity $9,992.19 vs yday $10,101.34 (-109.15) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $128.59 | ▼ close $9,912.38 vs 09:30 $9,992.19 (session -79.81) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $128.59 | ▼ 09:30 equity $9,857.87 vs yday $9,912.38 (-54.51) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `ARCT` | 76 | $15.46 | $2.24 | $-104.02 | $1,301.31 | ▼ -104.02 after sell → book $9,855.63; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `BMEA` | 662 | $1.94 | $8.66 | $-10.58 | $2,576.93 | ▼ -10.58 after sell → book $9,846.97; vs 09:30 mark -8.66 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CRDL` | 586 | $2.22 | $7.67 | $+8.21 | $3,870.18 | ▲ +8.21 after sell → book $9,839.30; vs 09:30 mark -7.67 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `HRMY` | 29 | $42.01 | $2.10 | $-30.85 | $5,086.37 | ▼ -30.85 after sell → book $9,837.20; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `NVAX` | 122 | $10.02 | $2.39 | $-53.54 | $6,306.43 | ▼ -53.54 after sell → book $9,834.82; vs 09:30 mark -2.38 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `PBH` | 23 | $49.53 | $2.08 | $-94.30 | $7,443.54 | ▼ -94.30 after sell → book $9,832.74; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `PCRX` | 47 | $25.62 | $2.15 | $-56.92 | $8,645.53 | ▼ -56.92 after sell → book $9,830.59; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `RVTY` | 9 | $125.77 | $2.04 | $-64.17 | $9,775.42 | ▼ -64.17 after sell → book $9,828.55; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,775.42 | ▲ close $9,828.82 vs 09:30 $9,857.87 (session +0.27) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,775.42 | ▲ 09:30 equity $9,829.09 vs yday $9,828.82 (+0.27) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `FRNM` | 1 | $15.64 | $0.18 | $-1.11 | $9,790.88 | ▼ -1.11 after sell → book $9,828.91; vs 09:30 mark -0.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `IRD` | 4 | $5.87 | $0.27 | $+4.90 | $9,814.09 | ▲ +4.90 after sell → book $9,828.64; vs 09:30 mark -0.27 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `LENZ` | 3 | $4.85 | $0.17 | $-3.06 | $9,828.47 | ▼ -3.06 after sell → book $9,828.47; vs 09:30 mark -0.17 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,828.47 | ▲ close $9,828.47 vs 09:30 $9,829.09 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,828.47 | ▲ 09:30 equity $9,828.47 vs yday $9,828.47 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `BAND` | 46 | $52.55 | $2.13 | — | $7,409.04 | — | union looker hold 3: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list probable,yday_gainer; 🔵; ⚪; ret5=+8.6; leftover $2457.12 | — |
| 2026-09-11 09:30 ET | **BUY** | `PAGS` | 243 | $10.11 | $3.13 | — | $4,949.18 | — | union looker hold 3: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list yday_gainer; 🔵; ⚪; ret5=+2.9; leftover $2457.12 | — |
| 2026-09-11 09:30 ET | **BUY** | `PAYP` | 134 | $18.30 | $2.39 | — | $2,494.58 | — | union looker hold 3: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $2457.12 | — |
| 2026-09-11 09:30 ET | **BUY** | `ZSQR` | 756 | $3.25 | $9.75 | — | $27.83 | — | union looker hold 3: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list yday_gainer; 🔵; ⚪; ret5=+3.6; leftover $2457.12 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $27.83 | ▲ close $9,896.23 vs 09:30 $9,828.47 (session +85.17) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $27.83 | ▼ 09:30 equity $9,838.11 vs yday $9,896.23 (-58.12) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $27.83 | ▼ close $9,279.34 vs 09:30 $9,838.11 (session -558.77) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $27.83 | ▼ 09:30 equity $9,259.47 vs yday $9,279.34 (-19.87) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $27.83 | ▼ close $8,803.41 vs 09:30 $9,259.47 (session -456.06) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $27.83 | ▼ 09:30 equity $8,690.06 vs yday $8,803.41 (-113.35) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `BAND` | 46 | $48.60 | $2.16 | $-185.98 | $2,261.28 | ▼ -185.98 after sell → book $8,687.91; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `PAGS` | 243 | $9.39 | $3.19 | $-181.29 | $4,539.85 | ▼ -181.29 after sell → book $8,684.71; vs 09:30 mark -3.20 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `PAYP` | 134 | $17.73 | $2.43 | $-81.21 | $6,913.24 | ▼ -81.21 after sell → book $8,682.28; vs 09:30 mark -2.43 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `ZSQR` | 756 | $2.34 | $9.89 | $-707.60 | $8,672.39 | ▼ -707.60 after sell → book $8,672.39; vs 09:30 mark -9.89 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `QRVO` | 36 | $118.18 | $2.10 | — | $4,415.81 | — | union looker hold 3: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+13.4; leftover $4336.19 | — |
| 2026-09-16 09:30 ET | **BUY** | `SWKS` | 48 | $89.38 | $2.13 | — | $123.44 | — | union looker hold 3: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.4; leftover $4336.19 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $123.44 | ▼ close $8,334.68 vs 09:30 $8,690.06 (session -333.48) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $123.44 | ▲ 09:30 equity $8,424.32 vs yday $8,334.68 (+89.64) | — | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $123.44 | ▲ close $8,809.16 vs 09:30 $8,424.32 (session +384.84) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $123.44 | ▲ 09:30 equity $8,889.20 vs yday $8,809.16 (+80.04) | — | — |
| 2026-09-18 09:30 ET | **BUY** | `PGEN` | 1 | $7.98 | $0.08 | — | $115.37 | — | union looker hold 3: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+17.8; leftover $15.43 | — |
| 2026-09-18 09:30 ET | **BUY** | `RXT` | 3 | $3.94 | $0.13 | — | $103.43 | — | union looker hold 3: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list yday_gainer; 🔵; ⚪; ret5=+25.2; leftover $15.43 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $103.43 | ▼ close $8,601.57 vs 09:30 $8,889.20 (session -287.42) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $103.43 | ▲ 09:30 equity $8,690.49 vs yday $8,601.57 (+88.92) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `QRVO` | 36 | $118.44 | $2.14 | $+5.12 | $4,365.12 | ▲ +5.12 after sell → book $8,688.34; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `SWKS` | 48 | $89.66 | $2.18 | $+9.13 | $8,666.63 | ▲ +9.13 after sell → book $8,686.17; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,666.63 | ▲ close $8,686.45 vs 09:30 $8,690.49 (session +0.28) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,666.63 | ▲ 09:30 equity $8,686.45 vs yday $8,686.45 (-0.00) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,666.63 | ▲ close $8,686.45 vs 09:30 $8,686.45 (session +0.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,666.63 | ▲ 09:30 equity $8,686.79 vs yday $8,686.45 (+0.34) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `PGEN` | 1 | $7.95 | $0.10 | $-0.22 | $8,674.47 | ▼ -0.22 after sell → book $8,686.68; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `RXT` | 3 | $4.07 | $0.15 | $+0.11 | $8,686.53 | ▲ +0.11 after sell → book $8,686.53; vs 09:30 mark -0.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **BUY** | `A` | 6 | $166.54 | $2.01 | — | $7,685.28 | — | union looker hold 3: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten; 🔵; ⚪; ret5=+10.3; leftover $1085.82 | — |
| 2026-09-23 09:30 ET | **BUY** | `BFLY` | 109 | $9.90 | $2.32 | — | $6,603.87 | — | union looker hold 3: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+27.3; leftover $1085.82 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 52 | $20.65 | $2.15 | — | $5,527.92 | — | union looker hold 3: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $1085.82 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 110 | $9.81 | $2.32 | — | $4,446.50 | — | union looker hold 3: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten; 🔵; ⚪; ret5=+4.0; leftover $1085.82 | — |
| 2026-09-23 09:30 ET | **BUY** | `AMRX` | 55 | $19.70 | $2.15 | — | $3,360.85 | — | union looker hold 3: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+17.1; leftover $1085.82 | — |
| 2026-09-23 09:30 ET | **BUY** | `ARQT` | 39 | $27.79 | $2.11 | — | $2,274.93 | — | union looker hold 3: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten; 🔵; ⚪; ret5=+7.0; leftover $1085.82 | — |
| 2026-09-23 09:30 ET | **BUY** | `VICR` | 4 | $266.50 | $2.00 | — | $1,206.93 | — | union looker hold 3: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.2; leftover $1085.82 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,206.93 | ▼ close $8,571.74 vs 09:30 $8,686.79 (session -99.74) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,206.93 | ▼ 09:30 equity $8,497.42 vs yday $8,571.74 (-74.32) | — | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,206.93 | ▲ close $8,581.66 vs 09:30 $8,497.42 (session +84.25) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,827.63 | ▲ 09:30 equity $8,829.12 vs yday $8,824.66 (+4.46) | 09:30 open · cash $7,827.63 (unchanged overnight, no fees) · equity $8,829.12 vs prior close $8,824.66 (+4.46) · 6 name(s) re-marked at the open (per-name table). ADMA×19 yday $9.52 → 09:30 $9.52 +0.00; BFLY×18 yday $9.41 → 09:30 $9.41 +0.00; HALO×1 yday $115.22 → 09:30 $115.36 +0.14; IBRX×20 yday $8.64 → 09:30 $8.64 +0.00; NEOG×13 yday $13.66 → 09:30 $13.66 +0.00; OMER×9 yday $20.13 → 09:30 $20.61 +4.32 | — |
| 2026-09-25 09:30 ET | **BUY** | `RSKD` | 142 | $7.85 | $2.42 | — | $6,710.51 | — | union looker hold 3: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list yday_gainer; 🔵; ⚪; ret5=+25.4; leftover $1118.23 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `CDNS` | 3 | $324.97 | $2.00 | — | $5,733.60 | — | union looker hold 3: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+14.7; leftover $1118.23 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `CYPH` | 279 | $4.00 | $3.60 | — | $4,612.61 | — | union looker hold 3: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+32.9; leftover $1118.23 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `DUOT` | 116 | $9.59 | $2.34 | — | $3,497.83 | — | union looker hold 3: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+12.4; leftover $1118.23 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `GRAL` | 9 | $123.50 | $2.02 | — | $2,384.32 | — | union looker hold 3: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+56.6; leftover $1118.23 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `PACB` | 712 | $1.57 | $9.18 | — | $1,257.29 | — | union looker hold 3: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list yday_gainer,yday_mover,ohlc_hot; 🔵; ⚪; ret5=+14.5; leftover $1118.23 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `PRGO` | 75 | $14.81 | $2.21 | — | $144.33 | — | union looker hold 3: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+8.9; leftover $1118.23 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $144.33 | ▲ close $8,884.41 vs 09:30 $8,829.12 (session +79.06) | 16:00 close · cash $144.33 · equity $8,884.41 vs 09:30 $8,829.12 (+55.29; session marks +79.06) · 13 name(s) marked open→close (per-name table). ADMA×19 09:30 $9.52 → close $9.52 +0.00; BFLY×18 09:30 $9.41 → close $9.41 -0.00; HALO×1 09:30 $115.36 → close $113.90 -1.46; IBRX×20 09:30 $8.64 → close $8.64 +0.00; NEOG×13 09:30 $13.66 → close $13.66 -0.00; OMER×9 09:30 $20.61 → close $20.08 -4.77; RSKD×142 09:30 $7.85 → close $7.78 -9.94; CDNS×3 09:30 $324.97 → close $326.13 +3.48; CYPH×279 09:30 $4.00 → close $4.12 +32.09; DUOT×116 09:30 $9.59 → close $9.14 -52.20; GRAL×9 09:30 $123.50 → close $126.89 +30.51; PACB×712 09:30 $1.57 → close $1.62 +35.60; PRGO×75 09:30 $14.81 → close $15.42 +45.75 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `BTSG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `IREN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `SLS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `BRUN` | cash | leftover split 10.44 < 1 share @ 26.25 |
| 2026-08-14 | `HLIT` | cash | leftover split 10.44 < 1 share @ 13.18 |
| 2026-08-14 | `MNTN` | cash | leftover split 10.44 < 1 share @ 12.50 |
| 2026-08-14 | `QMCO` | cash | leftover split 10.44 < 1 share @ 24.68 |
| 2026-08-14 | `SNDK` | cash | leftover split 10.44 < 1 share @ 1646.93 |
| 2026-08-14 | `ADUR` | cash | leftover split 10.44 < 1 share @ 16.50 |
| 2026-08-17 | `BTSG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `IREN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `SLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `QMLS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ZENA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `LPTH` | cash | leftover split 8.40 < 1 share @ 14.94 |
| 2026-08-17 | `AAOI` | cash | leftover split 8.40 < 1 share @ 152.64 |
| 2026-08-17 | `ABX` | cash | leftover split 8.40 < 1 share @ 9.12 |
| 2026-08-17 | `ALOY` | cash | leftover split 8.40 < 1 share @ 14.66 |
| 2026-08-17 | `MP` | cash | leftover split 8.40 < 1 share @ 58.01 |
| 2026-08-18 | `QMLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ZENA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BORR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `INDI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `KOPN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-19 | `BORR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `INDI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `KOPN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 26.36 < 1 share @ 119.43 |
| 2026-08-21 | `AEM` | cash | leftover split 26.36 < 1 share @ 216.30 |
| 2026-08-21 | `FUTU` | cash | leftover split 26.36 < 1 share @ 115.18 |
| 2026-08-21 | `GRAL` | cash | leftover split 26.36 < 1 share @ 78.88 |
| 2026-08-21 | `ILMN` | cash | leftover split 26.36 < 1 share @ 212.40 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-25 | `AUPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `FCX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `RHI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `SUZ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `VALE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `AVAH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `FCX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RHI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `SUZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `VALE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `AVAH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `KEYS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `AVT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CGNX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `COHR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `LSCC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `MEI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `MTSI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-01 | `KEYS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `AVT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CGNX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `COHR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `LSCC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `MEI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `MTSI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-04 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `HRMY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `NVAX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `PBH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `PCRX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRM` | cash | leftover split 22.61 < 1 share @ 263.36 |
| 2026-09-04 | `TARS` | cash | leftover split 22.61 < 1 share @ 82.70 |
| 2026-09-04 | `DELL` | cash | leftover split 22.61 < 1 share @ 513.78 |
| 2026-09-04 | `PIPR` | cash | leftover split 22.61 < 1 share @ 76.55 |
| 2026-09-04 | `TDS` | cash | leftover split 22.61 < 1 share @ 37.44 |
| 2026-09-08 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `HRMY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `NVAX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `PBH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `PCRX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `RVTY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `FRNM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `IRD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `LENZ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-09 | `FRNM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `IRD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `LENZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-14 | `BAND` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `PAGS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `PAYP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `ZSQR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-15 | `BAND` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `PAGS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `PAYP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `ZSQR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-17 | `QRVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `SWKS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `QRVO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `SWKS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `CRWD` | cash | leftover split 15.43 < 1 share @ 246.98 |
| 2026-09-18 | `FIVN` | cash | leftover split 15.43 < 1 share @ 34.44 |
| 2026-09-18 | `ATRC` | cash | leftover split 15.43 < 1 share @ 58.51 |
| 2026-09-18 | `ECO` | cash | leftover split 15.43 < 1 share @ 85.00 |
| 2026-09-18 | `RBRK` | cash | leftover split 15.43 < 1 share @ 108.55 |
| 2026-09-18 | `TH` | cash | leftover split 15.43 < 1 share @ 20.91 |
| 2026-09-21 | `PGEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `RXT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `PGEN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `RXT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `MPWR` | cash | leftover split 1085.82 < 1 share @ 1367.08 |
| 2026-09-24 | `A` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `BFLY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `OMER` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `ADMA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `AMRX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `ARQT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `VICR` | min_hold | dropped but min-hold 1/3 sess — no sell |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `A` | 6 | 2026-09-23 @ $166.54 | union looker hold 3: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten; 🔵; ⚪; ret5=+10.3; leftover $1085.82 |
| `BFLY` | 109 | 2026-09-23 @ $9.90 | union looker hold 3: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+27.3; leftover $1085.82 |
| `OMER` | 52 | 2026-09-23 @ $20.65 | union looker hold 3: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $1085.82 |
| `ADMA` | 110 | 2026-09-23 @ $9.81 | union looker hold 3: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten; 🔵; ⚪; ret5=+4.0; leftover $1085.82 |
| `AMRX` | 55 | 2026-09-23 @ $19.70 | union looker hold 3: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+17.1; leftover $1085.82 |
| `ARQT` | 39 | 2026-09-23 @ $27.79 | union looker hold 3: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list flatten; 🔵; ⚪; ret5=+7.0; leftover $1085.82 |
| `VICR` | 4 | 2026-09-23 @ $266.50 | union looker hold 3: 0 red + yesterday up; gate zero_red=True,yday_up=True; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.2; leftover $1085.82 |
