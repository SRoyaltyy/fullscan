# Factor mine action — `union_white_any_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · −0 red + (yday up or major catalyst), then Score

Cash book **-13.06%** ($8,694) · signal-only (no cash/fees) was -10.91%. Starts YES **0/30**. Fills 233 · skips 0 · realized $-1084.78.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 1 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Sort: the morning-board Score (100 minus list rank) — only after the pool is chosen.
- Must-have: at most 0 red cameras (the −R half of +G −R; 🚨 is not counted here).
- Must-have: yesterday's session was up, or a major good catalyst (EPS beat / catal green / earnings-react that is not a miss).
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is allowed only when every must-have is true.
- A name is thrown out if any must-not is true.
- Sort the keepers by the morning-board Score (100 minus list rank) — only after the pool is chosen and keep the top 8.
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
- **Gate** `cam_bad_max=0,yday_or_catalyst=True` · **rank** `list` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $8,915.20.

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
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 27 | $59.80 | $2.07 | — | $8,383.33 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; ⚪; ret5=-5.3; leftover $1666.67 | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 36 | $45.98 | $2.10 | — | $6,725.95 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; ⚪; ret5=+12.3; leftover $1666.67 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 32 | $50.62 | $2.09 | — | $5,103.92 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; ⚪; ret5=+6.2; leftover $1666.67 | — |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 142 | $11.70 | $2.42 | — | $3,440.11 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; ⚪; ret5=-0.8; leftover $1666.67 | — |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 2057 | $0.81 | $22.83 | — | $1,751.10 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; ⚪; ret5=+13.2; leftover $1666.67 | — |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 71 | $23.33 | $2.20 | — | $92.47 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; ⚪; ret5=+19.7; leftover $1666.67 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $92.47 | ▲ close $10,326.53 vs 09:30 $10,000.00 (session +360.24) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $92.47 | ▲ 09:30 equity $10,360.67 vs yday $10,326.53 (+34.14) | — | — |
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 27 | $59.65 | $2.09 | $-8.21 | $1,700.93 | ▼ -8.21 after sell → book $10,358.58; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 36 | $44.09 | $2.12 | $-72.26 | $3,286.05 | ▼ -72.26 after sell → book $10,356.46; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 32 | $55.29 | $2.11 | $+145.14 | $5,053.22 | ▲ +145.14 after sell → book $10,354.35; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `SLS` | 142 | $12.40 | $2.45 | $+94.53 | $6,811.56 | ▲ +94.53 after sell → book $10,351.89; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 2057 | $0.93 | $25.66 | $+198.35 | $8,698.91 | ▲ +198.35 after sell → book $10,326.23; vs 09:30 mark -25.66 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 71 | $22.92 | $2.23 | $-33.54 | $10,324.01 | ▼ -33.54 after sell → book $10,324.01; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `DAVE` | 3 | $330.91 | $2.00 | — | $9,329.28 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=-8.6; leftover $1290.50 | — |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 22 | $57.61 | $2.06 | — | $8,059.80 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+5.7; leftover $1290.50 | — |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 1377 | $0.94 | $17.03 | — | $6,752.52 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+0.5; leftover $1290.50 | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 860 | $1.50 | $11.09 | — | $5,451.43 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1290.50 | — |
| 2026-08-14 09:30 ET | **BUY** | `BETR` | 87 | $14.80 | $2.25 | — | $4,161.57 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=-9.9; leftover $1290.50 | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 299 | $4.31 | $3.86 | — | $2,869.03 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1290.50 | — |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 308 | $4.18 | $3.97 | — | $1,577.61 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $1290.50 | — |
| 2026-08-14 09:30 ET | **BUY** | `WDC` | 2 | $503.50 | $2.00 | — | $568.62 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list probable; 🔵; ⚪; ret5=+7.9; leftover $1290.50 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $568.62 | ▼ close $10,160.90 vs 09:30 $10,360.67 (session -118.85) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $568.62 | ▲ 09:30 equity $10,232.27 vs yday $10,160.90 (+71.37) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `DAVE` | 3 | $336.94 | $2.02 | $+14.07 | $1,577.42 | ▲ +14.07 after sell → book $10,230.25; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SLG` | 22 | $55.37 | $2.08 | $-53.41 | $2,793.48 | ▼ -53.41 after sell → book $10,228.17; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LDI` | 1377 | $0.91 | $16.86 | $-75.20 | $4,025.56 | ▼ -75.20 after sell → book $10,211.31; vs 09:30 mark -16.86 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 860 | $1.52 | $11.25 | $-5.14 | $5,321.52 | ▼ -5.14 after sell → book $10,200.07; vs 09:30 mark -11.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BETR` | 87 | $13.67 | $2.28 | $-102.84 | $6,508.53 | ▼ -102.84 after sell → book $10,197.79; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 299 | $4.60 | $3.92 | $+78.94 | $7,880.01 | ▲ +78.94 after sell → book $10,193.87; vs 09:30 mark -3.92 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HYLN` | 308 | $4.10 | $4.03 | $-32.65 | $9,138.78 | ▼ -32.65 after sell → book $10,189.84; vs 09:30 mark -4.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `WDC` | 2 | $525.53 | $2.02 | $+40.05 | $10,187.82 | ▲ +40.05 after sell → book $10,187.82; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 393 | $3.24 | $5.07 | — | $8,909.43 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; ⚪; ret5=+0.3; leftover $1273.48 | — |
| 2026-08-17 09:30 ET | **BUY** | `CDNL` | 31 | $39.85 | $2.08 | — | $7,672.00 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list probable,yday_gainer; ⚪; ret5=-38.4; leftover $1273.48 | — |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 139 | $9.12 | $2.41 | — | $6,401.91 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $1273.48 | — |
| 2026-08-17 09:30 ET | **BUY** | `OCC` | 69 | $18.24 | $2.20 | — | $5,141.16 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list probable,ohlc_hot; ⚪; ret5=+9.5; leftover $1273.48 | — |
| 2026-08-17 09:30 ET | **BUY** | `ALM` | 78 | $16.20 | $2.22 | — | $3,875.33 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list probable,ohlc_hot; 🔵; ⚪; ret5=+6.4; leftover $1273.48 | — |
| 2026-08-17 09:30 ET | **BUY** | `UMAC` | 39 | $32.55 | $2.11 | — | $2,603.77 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,yday_mover; ⚪; ret5=+30.4; leftover $1273.48 | — |
| 2026-08-17 09:30 ET | **BUY** | `NPWR` | 663 | $1.92 | $8.55 | — | $1,322.26 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $1273.48 | — |
| 2026-08-17 09:30 ET | **BUY** | `LPTH` | 85 | $14.94 | $2.25 | — | $50.12 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,yday_mover,ohlc_hot; ⚪; ret5=+16.2; leftover $1273.48 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $50.12 | ▼ close $9,825.80 vs 09:30 $10,232.27 (session -335.14) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $50.12 | ▼ 09:30 equity $9,597.79 vs yday $9,825.80 (-228.01) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `DNN` | 393 | $3.11 | $5.14 | $-61.30 | $1,267.20 | ▼ -61.30 after sell → book $9,592.64; vs 09:30 mark -5.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CDNL` | 31 | $41.57 | $2.10 | $+49.13 | $2,553.77 | ▲ +49.13 after sell → book $9,590.54; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 139 | $9.03 | $2.44 | $-17.36 | $3,806.50 | ▼ -17.36 after sell → book $9,588.10; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `OCC` | 69 | $16.20 | $2.22 | $-145.18 | $4,922.08 | ▼ -145.18 after sell → book $9,585.88; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALM` | 78 | $15.78 | $2.25 | $-37.23 | $6,150.67 | ▼ -37.23 after sell → book $9,583.63; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `UMAC` | 39 | $28.59 | $2.13 | $-158.67 | $7,263.56 | ▼ -158.67 after sell → book $9,581.51; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `NPWR` | 663 | $1.70 | $8.67 | $-163.09 | $8,381.98 | ▼ -163.09 after sell → book $9,572.83; vs 09:30 mark -8.68 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `LPTH` | 85 | $14.01 | $2.27 | $-83.56 | $9,570.56 | ▼ -83.56 after sell → book $9,570.56; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,570.56 | ▲ close $9,570.56 vs 09:30 $9,597.79 (session +0.00) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,570.56 | ▲ 09:30 equity $9,570.56 vs yday $9,570.56 (+0.00) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,570.56 | ▲ close $9,570.56 vs 09:30 $9,570.56 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,570.56 | ▲ 09:30 equity $9,570.56 vs yday $9,570.56 (+0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 58 | $20.55 | $2.16 | — | $8,376.50 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1196.32 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,191.34 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1196.32 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 57 | $20.65 | $2.16 | — | $6,012.13 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1196.32 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 207 | $5.77 | $2.67 | — | $4,815.07 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1196.32 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 60 | $19.63 | $2.17 | — | $3,635.10 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1196.32 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 40 | $29.63 | $2.11 | — | $2,447.79 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1196.32 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 683 | $1.75 | $8.81 | — | $1,243.73 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1196.32 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $85.40 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1196.32 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $85.40 | ▲ close $9,772.32 vs 09:30 $9,570.56 (session +225.88) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $85.40 | ▲ 09:30 equity $10,030.57 vs yday $9,772.32 (+258.25) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 58 | $21.90 | $2.18 | $+73.95 | $1,353.41 | ▲ +73.95 after sell → book $10,028.38; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $2,595.72 | ▲ +57.15 after sell → book $10,026.33; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 57 | $21.75 | $2.18 | $+58.36 | $3,833.29 | ▲ +58.36 after sell → book $10,024.15; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 207 | $5.67 | $2.71 | $-26.08 | $5,004.27 | ▼ -26.08 after sell → book $10,021.44; vs 09:30 mark -2.71 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 60 | $21.17 | $2.19 | $+88.04 | $6,272.28 | ▲ +88.04 after sell → book $10,019.25; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 40 | $32.17 | $2.13 | $+97.36 | $7,556.95 | ▲ +97.36 after sell → book $10,017.12; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 683 | $1.79 | $8.93 | $+9.58 | $8,770.58 | ▲ +9.58 after sell → book $10,008.18; vs 09:30 mark -8.94 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 8 | $154.70 | $2.03 | $+77.23 | $10,006.15 | ▲ +77.23 after sell → book $10,006.15; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $8,809.83 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+21.1; leftover $1250.77 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 72 | $17.20 | $2.21 | — | $7,569.22 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $1250.77 | — |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 5 | $216.30 | $2.00 | — | $6,485.72 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1250.77 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 112 | $11.13 | $2.33 | — | $5,236.83 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1250.77 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 506 | $2.47 | $6.53 | — | $3,980.48 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $1250.77 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 648 | $1.93 | $8.36 | — | $2,721.48 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $1250.77 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 20 | $59.72 | $2.05 | — | $1,525.03 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1250.77 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 947 | $1.32 | $12.22 | — | $262.78 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1250.77 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $262.78 | ▲ close $10,219.96 vs 09:30 $10,030.57 (session +251.52) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $262.78 | ▲ 09:30 equity $10,579.68 vs yday $10,219.96 (+359.72) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.51 | $2.04 | $+6.74 | $1,465.84 | ▲ +6.74 after sell → book $10,577.64; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 72 | $16.57 | $2.23 | $-49.79 | $2,656.65 | ▼ -49.79 after sell → book $10,575.41; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 5 | $217.03 | $2.02 | $-0.38 | $3,739.77 | ▼ -0.38 after sell → book $10,573.38; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 112 | $13.33 | $2.36 | $+241.72 | $5,230.38 | ▲ +241.72 after sell → book $10,571.03; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 506 | $2.40 | $6.62 | $-48.57 | $6,438.16 | ▼ -48.57 after sell → book $10,564.41; vs 09:30 mark -6.62 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRDL` | 648 | $1.88 | $8.48 | $-49.24 | $7,647.92 | ▼ -49.24 after sell → book $10,555.93; vs 09:30 mark -8.48 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 20 | $58.75 | $2.07 | $-23.52 | $8,820.85 | ▼ -23.52 after sell → book $10,553.86; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 947 | $1.83 | $12.39 | $+458.37 | $10,541.47 | ▲ +458.37 after sell → book $10,541.47; vs 09:30 mark -12.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,541.47 | ▲ close $10,541.47 vs 09:30 $10,579.68 (session +0.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,541.47 | ▲ 09:30 equity $10,541.47 vs yday $10,541.47 (+0.00) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 55 | $23.77 | $2.15 | — | $9,231.97 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; ⚪; ret5=+13.0; leftover $1317.68 | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 157 | $8.35 | $2.46 | — | $7,918.56 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+8.0; leftover $1317.68 | — |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 808 | $1.63 | $10.42 | — | $6,591.09 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; leftover $1317.68 | — |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 251 | $5.24 | $3.24 | — | $5,272.62 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+35.3; leftover $1317.68 | — |
| 2026-08-25 09:30 ET | **BUY** | `SUJA` | 149 | $8.79 | $2.44 | — | $3,960.47 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.1; leftover $1317.68 | — |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 844 | $1.56 | $10.89 | — | $2,632.94 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+138.3; leftover $1317.68 | — |
| 2026-08-25 09:30 ET | **BUY** | `DEFT` | 2125 | $0.62 | $19.55 | — | $1,295.89 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+33.3; leftover $1317.68 | — |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 203 | $6.37 | $2.62 | — | $0.16 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer; 🔵; ⚪; ret5=+10.9; leftover $1317.68 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.16 | ▲ close $10,685.11 vs 09:30 $10,541.47 (session +197.41) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.16 | ▼ 09:30 equity $10,649.23 vs yday $10,685.11 (-35.88) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `MOS` | 55 | $24.84 | $2.18 | $+54.52 | $1,364.19 | ▲ +54.52 after sell → book $10,647.06; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CRMD` | 157 | $8.60 | $2.50 | $+34.29 | $2,711.89 | ▲ +34.29 after sell → book $10,644.56; vs 09:30 mark -2.50 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `BMEA` | 808 | $1.75 | $10.57 | $+80.01 | $4,119.36 | ▲ +80.01 after sell → book $10,633.99; vs 09:30 mark -10.57 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ALVO` | 251 | $4.98 | $3.29 | $-71.79 | $5,366.05 | ▼ -71.79 after sell → book $10,630.70; vs 09:30 mark -3.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `SUJA` | 149 | $9.39 | $2.47 | $+84.49 | $6,762.69 | ▲ +84.49 after sell → book $10,628.23; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 844 | $1.60 | $11.04 | $+11.83 | $8,102.05 | ▲ +11.83 after sell → book $10,617.19; vs 09:30 mark -11.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `DEFT` | 2125 | $0.60 | $19.45 | $-85.75 | $9,353.35 | ▼ -85.75 after sell → book $10,597.74; vs 09:30 mark -19.45 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 09:30 ET | **SELL** | `ZURA` | 203 | $6.13 | $2.66 | $-54.00 | $10,595.08 | ▼ -54.00 after sell → book $10,595.08; vs 09:30 mark -2.66 | dropped from list after 1 sess (min 1) | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,595.08 | ▲ close $10,595.08 vs 09:30 $10,649.23 (session +0.00) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,595.08 | ▲ 09:30 equity $10,595.08 vs yday $10,595.08 (+0.00) | — | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,595.08 | ▲ close $10,595.08 vs 09:30 $10,595.08 (session +0.00) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,595.08 | ▲ 09:30 equity $10,595.08 vs yday $10,595.08 (+0.00) | — | — |
| 2026-08-28 09:30 ET | **BUY** | `SIMO` | 5 | $252.24 | $2.00 | — | $9,331.88 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list probable,yday_gainer; ⚪; ret5=+2.2; leftover $1324.39 | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 9 | $141.76 | $2.02 | — | $8,054.02 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1324.39 | — |
| 2026-08-28 09:30 ET | **BUY** | `TTMI` | 10 | $122.81 | $2.02 | — | $6,823.90 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+9.0; leftover $1324.39 | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 4 | $324.41 | $2.00 | — | $5,524.26 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1324.39 | — |
| 2026-08-28 09:30 ET | **BUY** | `AVT` | 14 | $91.49 | $2.03 | — | $4,241.37 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list mover_buy; 🔵; ⚪; ret5=+5.3; leftover $1324.39 | — |
| 2026-08-28 09:30 ET | **BUY** | `CGNX` | 21 | $62.82 | $2.05 | — | $2,920.09 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list mover_buy; 🔵; ⚪; ret5=+5.4; leftover $1324.39 | — |
| 2026-08-28 09:30 ET | **BUY** | `COHR` | 4 | $289.44 | $2.00 | — | $1,760.33 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list mover_buy; 🔵; ⚪; ret5=+1.8; leftover $1324.39 | — |
| 2026-08-28 09:30 ET | **BUY** | `LSCC` | 11 | $119.76 | $2.02 | — | $440.95 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list mover_buy; 🔵; ⚪; ret5=+2.5; leftover $1324.39 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $440.95 | ▼ close $10,202.59 vs 09:30 $10,595.08 (session -376.34) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $440.95 | ▲ 09:30 equity $10,258.44 vs yday $10,202.59 (+55.85) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `SIMO` | 5 | $247.05 | $2.02 | $-29.98 | $1,674.17 | ▼ -29.98 after sell → book $10,256.41; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 9 | $132.30 | $2.04 | $-89.19 | $2,862.84 | ▼ -89.19 after sell → book $10,254.38; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TTMI` | 10 | $118.83 | $2.04 | $-43.86 | $4,049.10 | ▼ -43.86 after sell → book $10,252.34; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 4 | $322.49 | $2.02 | $-11.70 | $5,337.03 | ▼ -11.70 after sell → book $10,250.31; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `AVT` | 14 | $89.39 | $2.05 | $-33.48 | $6,586.44 | ▼ -33.48 after sell → book $10,248.26; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CGNX` | 21 | $60.46 | $2.07 | $-53.69 | $7,854.03 | ▼ -53.69 after sell → book $10,246.19; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `COHR` | 4 | $280.25 | $2.02 | $-40.78 | $8,973.01 | ▼ -40.78 after sell → book $10,244.17; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `LSCC` | 11 | $115.56 | $2.04 | $-50.27 | $10,242.12 | ▼ -50.27 after sell → book $10,242.12; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,242.12 | ▲ close $10,242.12 vs 09:30 $10,258.44 (session +0.00) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,242.12 | ▲ 09:30 equity $10,242.12 vs yday $10,242.12 (+0.00) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,242.12 | ▲ close $10,242.12 vs 09:30 $10,242.12 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,242.12 | ▲ 09:30 equity $10,242.12 vs yday $10,242.12 (+0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,242.12 | ▲ close $10,242.12 vs 09:30 $10,242.12 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,242.12 | ▲ 09:30 equity $10,242.12 vs yday $10,242.12 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 24 | $52.88 | $2.06 | — | $8,970.94 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1280.27 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 29 | $42.93 | $2.08 | — | $7,723.89 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1280.27 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 352 | $3.63 | $4.54 | — | $6,441.59 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1280.27 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 159 | $8.03 | $2.47 | — | $5,162.36 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1280.27 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $132.45 | $2.02 | — | $3,968.29 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1280.27 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 76 | $16.77 | $2.22 | — | $2,691.55 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1280.27 | — |
| 2026-09-03 09:30 ET | **BUY** | `SLN` | 86 | $14.85 | $2.25 | — | $1,412.20 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list probable,yday_gainer; 🔵; ⚪; ret5=+9.3; leftover $1280.27 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 587 | $2.18 | $7.57 | — | $124.97 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $1280.27 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $124.97 | ▼ close $9,989.82 vs 09:30 $10,242.12 (session -227.10) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $124.97 | ▼ 09:30 equity $9,935.53 vs yday $9,989.82 (-54.29) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `HRMY` | 29 | $41.50 | $2.10 | $-45.64 | $1,326.37 | ▼ -45.64 after sell → book $9,933.43; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 159 | $7.91 | $2.50 | $-24.05 | $2,581.56 | ▼ -24.05 after sell → book $9,930.93; vs 09:30 mark -2.50 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 9 | $130.03 | $2.04 | $-25.83 | $3,749.79 | ▼ -25.83 after sell → book $9,928.89; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 76 | $15.61 | $2.24 | $-92.62 | $4,933.91 | ▼ -92.62 after sell → book $9,926.65; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `SLN` | 86 | $14.63 | $2.27 | $-23.44 | $6,189.82 | ▼ -23.44 after sell → book $9,924.38; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRDL` | 587 | $2.16 | $7.68 | $-26.99 | $7,450.06 | ▼ -26.99 after sell → book $9,916.70; vs 09:30 mark -7.68 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 492 | $2.52 | $6.35 | — | $6,203.87 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $1241.68 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 185 | $6.71 | $2.54 | — | $4,959.98 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1241.68 | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 653 | $1.90 | $8.42 | — | $3,710.85 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $1241.68 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 259 | $4.78 | $3.34 | — | $2,469.49 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $1241.68 | — |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 780 | $1.59 | $10.06 | — | $1,219.23 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1241.68 | — |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 107 | $11.31 | $2.31 | — | $6.75 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $1241.68 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6.75 | ▼ close $9,833.05 vs 09:30 $9,935.53 (session -50.63) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6.75 | ▼ 09:30 equity $9,795.60 vs yday $9,833.05 (-37.45) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `ATRC` | 24 | $54.31 | $2.08 | $+30.18 | $1,308.11 | ▲ +30.18 after sell → book $9,793.52; vs 09:30 mark -2.08 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `CABA` | 352 | $3.43 | $4.61 | $-79.55 | $2,510.86 | ▼ -79.55 after sell → book $9,788.91; vs 09:30 mark -4.61 | dropped from list after 2 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `ALEC` | 492 | $2.38 | $6.44 | $-81.67 | $3,675.38 | ▼ -81.67 after sell → book $9,782.47; vs 09:30 mark -6.44 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BHC` | 185 | $6.57 | $2.59 | $-31.03 | $4,888.24 | ▼ -31.03 after sell → book $9,779.88; vs 09:30 mark -2.59 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `BMEA` | 653 | $2.00 | $8.54 | $+48.33 | $6,185.70 | ▲ +48.33 after sell → book $9,771.34; vs 09:30 mark -8.54 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OABI` | 259 | $4.30 | $3.39 | $-131.06 | $7,296.01 | ▼ -131.06 after sell → book $9,767.95; vs 09:30 mark -3.39 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `OPK` | 780 | $1.63 | $10.20 | $+10.94 | $8,557.21 | ▲ +10.94 after sell → book $9,757.75; vs 09:30 mark -10.20 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 09:30 ET | **SELL** | `VIR` | 107 | $11.22 | $2.34 | $-14.28 | $9,755.41 | ▼ -14.28 after sell → book $9,755.41; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,755.41 | ▲ close $9,755.41 vs 09:30 $9,795.60 (session +0.00) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,755.41 | ▲ 09:30 equity $9,755.41 vs yday $9,755.41 (-0.00) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,755.41 | ▲ close $9,755.41 vs 09:30 $9,755.41 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,755.41 | ▲ 09:30 equity $9,755.41 vs yday $9,755.41 (-0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,755.41 | ▲ close $9,755.41 vs 09:30 $9,755.41 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,755.41 | ▲ 09:30 equity $9,755.41 vs yday $9,755.41 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 11 | $164.43 | $2.02 | — | $7,944.65 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,earn_react; ⚪; ret5=+4.9; leftover $1951.08 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAND` | 37 | $52.55 | $2.10 | — | $5,998.20 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list probable,yday_gainer; 🔵; ⚪; ret5=+8.6; leftover $1951.08 | — |
| 2026-09-11 09:30 ET | **BUY** | `PAGS` | 192 | $10.11 | $2.57 | — | $4,054.52 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer; 🔵; ⚪; ret5=+2.9; leftover $1951.08 | — |
| 2026-09-11 09:30 ET | **BUY** | `ZSQR` | 600 | $3.25 | $7.74 | — | $2,096.78 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer; 🔵; ⚪; ret5=+3.6; leftover $1951.08 | — |
| 2026-09-11 09:30 ET | **BUY** | `PAYP` | 106 | $18.30 | $2.31 | — | $154.67 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1951.08 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $154.67 | ▼ close $9,652.68 vs 09:30 $9,755.41 (session -85.99) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $154.67 | ▼ 09:30 equity $9,509.27 vs yday $9,652.68 (-143.41) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `ORCL` | 11 | $141.42 | $2.05 | $-257.18 | $1,708.24 | ▼ -257.18 after sell → book $9,507.22; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `BAND` | 37 | $56.90 | $2.13 | $+156.72 | $3,811.42 | ▲ +156.72 after sell → book $9,505.10; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `PAGS` | 192 | $10.00 | $2.61 | $-26.30 | $5,728.80 | ▼ -26.30 after sell → book $9,502.48; vs 09:30 mark -2.62 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `ZSQR` | 600 | $3.06 | $7.85 | $-129.59 | $7,556.95 | ▼ -129.59 after sell → book $9,494.63; vs 09:30 mark -7.85 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 09:30 ET | **SELL** | `PAYP` | 106 | $18.28 | $2.34 | $-6.77 | $9,492.29 | ▼ -6.77 after sell → book $9,492.29; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,492.29 | ▲ close $9,492.29 vs 09:30 $9,509.27 (session +0.00) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,492.29 | ▲ 09:30 equity $9,492.29 vs yday $9,492.29 (-0.00) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,492.29 | ▲ close $9,492.29 vs 09:30 $9,492.29 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,492.29 | ▲ 09:30 equity $9,492.29 vs yday $9,492.29 (-0.00) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `SWKS` | 53 | $89.38 | $2.15 | — | $4,753.00 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.4; leftover $4746.14 | — |
| 2026-09-16 09:30 ET | **BUY** | `QRVO` | 40 | $118.18 | $2.11 | — | $23.69 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+13.4; leftover $4746.14 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $23.69 | ▼ close $9,118.76 vs 09:30 $9,492.29 (session -369.27) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $23.69 | ▲ 09:30 equity $9,217.97 vs yday $9,118.76 (+99.21) | — | — |
| 2026-09-17 09:30 ET | **SELL** | `SWKS` | 53 | $86.76 | $2.20 | $-143.20 | $4,619.77 | ▼ -143.20 after sell → book $9,215.77; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **SELL** | `QRVO` | 40 | $114.90 | $2.16 | $-135.47 | $9,213.62 | ▼ -135.47 after sell → book $9,213.62; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-09-17 09:30 ET | **BUY** | `BULL` | 1157 | $7.95 | $14.93 | — | $0.54 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_mover; 🔵; ⚪; ret5=-18.4; leftover $9213.62 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.54 | ▼ close $8,921.01 vs 09:30 $9,217.97 (session -277.68) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.54 | ▲ 09:30 equity $9,082.99 vs yday $8,921.01 (+161.98) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `BULL` | 1157 | $7.85 | $15.19 | $-145.82 | $9,067.80 | ▼ -145.82 after sell → book $9,067.80; vs 09:30 mark -15.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-18 09:30 ET | **BUY** | `RBRK` | 10 | $108.55 | $2.02 | — | $7,980.28 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; ⚪; ret5=+21.3; leftover $1133.48 | — |
| 2026-09-18 09:30 ET | **BUY** | `ECO` | 13 | $85.00 | $2.03 | — | $6,873.25 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+18.3; leftover $1133.48 | — |
| 2026-09-18 09:30 ET | **BUY** | `FIVN` | 32 | $34.44 | $2.09 | — | $5,769.09 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+14.0; leftover $1133.48 | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 76 | $14.79 | $2.22 | — | $4,642.83 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $1133.48 | — |
| 2026-09-18 09:30 ET | **BUY** | `SDGR` | 38 | $29.32 | $2.10 | — | $3,526.56 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+60.9; leftover $1133.48 | — |
| 2026-09-18 09:30 ET | **BUY** | `CYPH` | 373 | $3.04 | $4.81 | — | $2,389.70 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+39.5; leftover $1133.48 | — |
| 2026-09-18 09:30 ET | **BUY** | `TEM` | 13 | $81.40 | $2.03 | — | $1,329.47 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.8; leftover $1133.48 | — |
| 2026-09-18 09:30 ET | **BUY** | `RXT` | 287 | $3.94 | $3.70 | — | $194.99 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer; 🔵; ⚪; ret5=+25.2; leftover $1133.48 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $194.99 | ▲ close $9,056.32 vs 09:30 $9,082.99 (session +9.51) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $194.99 | ▲ 09:30 equity $9,269.24 vs yday $9,056.32 (+212.92) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `RBRK` | 10 | $107.57 | $2.04 | $-13.86 | $1,268.65 | ▼ -13.86 after sell → book $9,267.20; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `ECO` | 13 | $82.83 | $2.05 | $-32.29 | $2,343.39 | ▼ -32.29 after sell → book $9,265.15; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `FIVN` | 32 | $33.00 | $2.11 | $-50.27 | $3,397.28 | ▼ -50.27 after sell → book $9,263.04; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RARE` | 76 | $14.58 | $2.24 | $-20.42 | $4,503.12 | ▼ -20.42 after sell → book $9,260.80; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `SDGR` | 38 | $29.43 | $2.12 | $-0.05 | $5,619.34 | ▼ -0.05 after sell → book $9,258.68; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `CYPH` | 373 | $4.00 | $4.89 | $+350.25 | $7,106.45 | ▲ +350.25 after sell → book $9,253.79; vs 09:30 mark -4.89 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `TEM` | 13 | $79.08 | $2.05 | $-34.24 | $8,132.44 | ▼ -34.24 after sell → book $9,251.74; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 09:30 ET | **SELL** | `RXT` | 287 | $3.90 | $3.76 | $-18.94 | $9,247.98 | ▼ -18.94 after sell → book $9,247.98; vs 09:30 mark -3.76 | dropped from list after 1 sess (min 1) | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,247.98 | ▲ close $9,247.98 vs 09:30 $9,269.24 (session +0.00) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,247.98 | ▲ 09:30 equity $9,247.98 vs yday $9,247.98 (+0.00) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,247.98 | ▲ close $9,247.98 vs 09:30 $9,247.98 (session +0.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,247.98 | ▲ 09:30 equity $9,247.98 vs yday $9,247.98 (+0.00) | — | — |
| 2026-09-23 09:30 ET | **BUY** | `DXCM` | 12 | $89.50 | $2.03 | — | $8,171.96 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+5.3; leftover $1156.00 | — |
| 2026-09-23 09:30 ET | **BUY** | `A` | 6 | $166.54 | $2.01 | — | $7,170.71 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+10.3; leftover $1156.00 | — |
| 2026-09-23 09:30 ET | **BUY** | `HALO` | 9 | $116.85 | $2.02 | — | $6,117.04 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+3.3; leftover $1156.00 | — |
| 2026-09-23 09:30 ET | **BUY** | `ARQT` | 41 | $27.79 | $2.11 | — | $4,975.54 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+7.0; leftover $1156.00 | — |
| 2026-09-23 09:30 ET | **BUY** | `PGEN` | 145 | $7.95 | $2.42 | — | $3,820.36 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+12.4; leftover $1156.00 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 117 | $9.81 | $2.34 | — | $2,670.25 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+4.0; leftover $1156.00 | — |
| 2026-09-23 09:30 ET | **BUY** | `FTRE` | 57 | $20.25 | $2.16 | — | $1,513.84 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+15.0; leftover $1156.00 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 55 | $20.65 | $2.15 | — | $375.94 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $1156.00 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $375.94 | ▼ close $8,981.20 vs 09:30 $9,247.98 (session -249.54) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $375.94 | ▼ 09:30 equity $8,932.63 vs yday $8,981.20 (-48.57) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `DXCM` | 12 | $87.67 | $2.05 | $-25.97 | $1,425.99 | ▼ -25.97 after sell → book $8,930.58; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `A` | 6 | $163.95 | $2.03 | $-19.58 | $2,407.66 | ▼ -19.58 after sell → book $8,928.55; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `HALO` | 9 | $112.22 | $2.04 | $-45.72 | $3,415.60 | ▼ -45.72 after sell → book $8,926.51; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `ARQT` | 41 | $26.22 | $2.13 | $-68.62 | $4,488.49 | ▼ -68.62 after sell → book $8,924.38; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 145 | $7.38 | $2.46 | $-87.53 | $5,556.13 | ▼ -87.53 after sell → book $8,921.92; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `ADMA` | 117 | $9.67 | $2.37 | $-21.09 | $6,685.15 | ▼ -21.09 after sell → book $8,919.55; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `FTRE` | 57 | $19.40 | $2.18 | $-52.79 | $7,788.77 | ▼ -52.79 after sell → book $8,917.37; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 09:30 ET | **SELL** | `OMER` | 55 | $20.52 | $2.17 | $-11.48 | $8,915.20 | ▼ -11.48 after sell → book $8,915.20; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,915.20 | ▲ close $8,915.20 vs 09:30 $8,932.63 (session +0.00) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,689.09 | ▲ 09:30 equity $8,689.09 vs yday $8,689.09 (+0.00) | 09:30 open · cash $8,689.09 · no holdings · equity $8,689.09 vs prior close $8,689.09 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-25 09:30 ET | **BUY** | `HALO` | 9 | $115.36 | $2.02 | — | $7,648.83 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+5.1; leftover $1086.14 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 141 | $7.65 | $2.41 | — | $6,567.77 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+5.2; leftover $1086.14 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TXG` | 12 | $83.76 | $2.03 | — | $5,560.62 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list probable,yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1086.14 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TEM` | 12 | $83.69 | $2.03 | — | $4,554.26 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list probable,ohlc_hot; ⚪; ret5=+2.3; leftover $1086.14 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ZSQR` | 281 | $3.86 | $3.62 | — | $3,465.97 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; leftover $1086.14 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TWST` | 5 | $184.00 | $2.00 | — | $2,543.97 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.3; leftover $1086.14 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 67 | $16.21 | $2.19 | — | $1,455.71 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; leftover $1086.14 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `GRAL` | 8 | $123.50 | $2.01 | — | $465.69 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+56.6; leftover $1086.14 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $465.69 | ▲ close $8,671.80 vs 09:30 $8,689.09 (session +1.03) | 16:00 close · cash $465.69 · equity $8,671.80 vs 09:30 $8,689.09 (-17.29; session marks +1.03) · 8 name(s) marked open→close (per-name table). HALO×9 09:30 $115.36 → close $113.90 -13.14; MRVI×141 09:30 $7.65 → close $7.60 -7.05; TXG×12 09:30 $83.76 → close $85.71 +23.40; TEM×12 09:30 $83.69 → close $85.01 +15.78; ZSQR×281 09:30 $3.86 → close $3.78 -22.48; TWST×5 09:30 $184.00 → close $182.83 -5.85; SECZ×67 09:30 $16.21 → close $15.96 -16.75; GRAL×8 09:30 $123.50 → close $126.89 +27.12 | — |
| 2026-09-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $465.69 | ▼ 09:30 equity $8,633.42 vs yday $8,671.80 (-38.38) | 09:30 open · cash $465.69 (unchanged overnight, no fees) · equity $8,633.42 vs prior close $8,671.80 (-38.38) · 8 name(s) re-marked at the open (per-name table). GRAL×8 yday $126.89 → 09:30 $128.90 +16.08; HALO×9 yday $113.90 → 09:30 $113.34 -5.04; MRVI×141 yday $7.60 → 09:30 $7.49 -15.51; SECZ×67 yday $15.96 → 09:30 $16.00 +2.68; TEM×12 yday $85.01 → 09:30 $83.57 -17.28; TWST×5 yday $182.83 → 09:30 $181.87 -4.80; TXG×12 yday $85.71 → 09:30 $86.14 +5.16; ZSQR×281 yday $3.78 → 09:30 $3.71 -19.67 | — |
| 2026-09-28 09:30 ET | **SELL** | `GRAL` | 8 | $128.90 | $2.03 | $+39.15 | $1,494.86 | ▲ +39.15 after sell → book $8,631.39; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-28 09:30 ET | **SELL** | `HALO` | 9 | $113.34 | $2.04 | $-22.23 | $2,512.88 | ▼ -22.23 after sell → book $8,629.35; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-28 09:30 ET | **SELL** | `MRVI` | 141 | $7.49 | $2.45 | $-27.42 | $3,566.52 | ▼ -27.42 after sell → book $8,626.90; vs 09:30 mark -2.45 | dropped from list after 1 sess (min 1) | — |
| 2026-09-28 09:30 ET | **SELL** | `SECZ` | 67 | $16.00 | $2.21 | $-18.47 | $4,636.31 | ▼ -18.47 after sell → book $8,624.69; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-09-28 09:30 ET | **SELL** | `TEM` | 12 | $83.57 | $2.05 | $-5.57 | $5,637.10 | ▼ -5.57 after sell → book $8,622.64; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-28 09:30 ET | **SELL** | `TWST` | 5 | $181.87 | $2.02 | $-14.68 | $6,544.43 | ▼ -14.68 after sell → book $8,620.62; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-09-28 09:30 ET | **SELL** | `TXG` | 12 | $86.14 | $2.05 | $+24.49 | $7,576.06 | ▲ +24.49 after sell → book $8,618.57; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-09-28 09:30 ET | **SELL** | `ZSQR` | 281 | $3.71 | $3.68 | $-49.46 | $8,614.89 | ▼ -49.46 after sell → book $8,614.89; vs 09:30 mark -3.68 | dropped from list after 1 sess (min 1) | — |
| 2026-09-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,614.89 | ▲ close $8,614.89 vs 09:30 $8,633.42 (session +0.00) | 16:00 close · cash $8,614.89 · no lots left · equity $8,614.89. | — |
| 2026-09-29 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,614.89 | ▲ 09:30 equity $8,614.89 vs yday $8,614.89 (+0.00) | 09:30 open · cash $8,614.89 · no holdings · equity $8,614.89 vs prior close $8,614.89 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-29 09:30 ET | **BUY** | `SN` | 5 | $184.05 | $2.00 | — | $7,692.63 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+8.7; leftover $1076.86 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `PDFS` | 21 | $50.25 | $2.05 | — | $6,635.33 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; ⚪; ret5=+6.5; leftover $1076.86 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `ADMA` | 107 | $10.04 | $2.31 | — | $5,558.74 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list probable,yday_gainer,ohlc_hot; ⚪; ret5=+6.5; leftover $1076.86 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `ZSQR` | 240 | $4.47 | $3.10 | — | $4,482.85 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,yday_mover; ⚪; ret5=+72.0; leftover $1076.86 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `BB` | 121 | $8.86 | $2.35 | — | $3,408.43 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer; ⚪; ret5=+3.2; leftover $1076.86 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `VFC` | 74 | $14.51 | $2.21 | — | $2,332.48 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list ohlc_hot; 🔵; ⚪; ret5=+10.5; leftover $1076.86 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `RGEN` | 5 | $196.00 | $2.00 | — | $1,350.48 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list ohlc_hot; 🔵; ⚪; ret5=+7.4; leftover $1076.86 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-29 09:30 ET | **BUY** | `KMX` | 17 | $60.41 | $2.04 | — | $321.55 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list earn_react; 🔵; ⚪; ret5=-2.3; leftover $1076.86 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-29 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $321.55 | ▼ close $8,448.52 vs 09:30 $8,614.89 (session -148.30) | 16:00 close · cash $321.55 · equity $8,448.52 vs 09:30 $8,614.89 (-166.37; session marks -148.30) · 8 name(s) marked open→close (per-name table). SN×5 09:30 $184.05 → close $182.44 -8.05; PDFS×21 09:30 $50.25 → close $49.77 -10.08; ADMA×107 09:30 $10.04 → close $9.96 -8.56; ZSQR×240 09:30 $4.47 → close $4.15 -76.80; BB×121 09:30 $8.86 → close $8.72 -16.94; VFC×74 09:30 $14.51 → close $14.50 -0.74; RGEN×5 09:30 $196.00 → close $194.57 -7.15; KMX×17 09:30 $60.41 → close $59.23 -19.98 | — |
| 2026-09-30 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $321.55 | ▲ 09:30 equity $8,448.52 vs yday $8,448.52 (+0.00) | 09:30 open · cash $321.55 (unchanged overnight, no fees) · equity $8,448.52 vs prior close $8,448.52 (+0.00) · 8 name(s) re-marked at the open (per-name table). ADMA×107 yday $9.96 → 09:30 $9.96 +0.00; BB×121 yday $8.72 → 09:30 $8.72 +0.00; KMX×17 yday $59.23 → 09:30 $59.23 +0.00; PDFS×21 yday $49.77 → 09:30 $49.77 +0.00; RGEN×5 yday $194.57 → 09:30 $194.57 +0.00; SN×5 yday $182.44 → 09:30 $182.44 +0.00; VFC×74 yday $14.50 → 09:30 $14.50 +0.00; ZSQR×240 yday $4.15 → 09:30 $4.15 +0.00 | — |
| 2026-09-30 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $321.55 | ▲ close $8,448.52 vs 09:30 $8,448.52 (session +0.00) | 16:00 close · cash $321.55 · equity $8,448.52 vs 09:30 $8,448.52 (+0.00; session marks +0.00) · 8 name(s) marked open→close (per-name table). ADMA×107 09:30 $9.96 → close $9.96 +0.00; BB×121 09:30 $8.72 → close $8.72 +0.00; KMX×17 09:30 $59.23 → close $59.23 +0.00; PDFS×21 09:30 $49.77 → close $49.77 +0.00; RGEN×5 09:30 $194.57 → close $194.57 +0.00; SN×5 09:30 $182.44 → close $182.44 +0.00; VFC×74 09:30 $14.50 → close $14.50 +0.00; ZSQR×240 09:30 $4.15 → close $4.15 +0.00 | — |
| 2026-10-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $321.55 | ▼ 09:30 equity $8,442.75 vs yday $8,448.52 (-5.77) | 09:30 open · cash $321.55 (unchanged overnight, no fees) · equity $8,442.75 vs prior close $8,448.52 (-5.77) · 8 name(s) re-marked at the open (per-name table). ADMA×107 yday $9.96 → 09:30 $10.07 +11.77; BB×121 yday $8.72 → 09:30 $8.97 +30.25; KMX×17 yday $59.23 → 09:30 $55.27 -67.32; PDFS×21 yday $49.77 → 09:30 $51.41 +34.44; RGEN×5 yday $194.57 → 09:30 $194.00 -2.85; SN×5 yday $182.44 → 09:30 $182.44 +0.00; VFC×74 yday $14.50 → 09:30 $14.11 -28.86; ZSQR×240 yday $4.15 → 09:30 $4.22 +16.80 | — |
| 2026-10-01 09:30 ET | **SELL** | `ADMA` | 107 | $10.07 | $2.34 | $-1.44 | $1,396.70 | ▼ -1.44 after sell → book $8,440.41; vs 09:30 mark -2.34 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `BB` | 121 | $8.97 | $2.38 | $+8.57 | $2,479.69 | ▲ +8.57 after sell → book $8,438.03; vs 09:30 mark -2.38 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `KMX` | 17 | $55.27 | $2.06 | $-91.40 | $3,417.22 | ▼ -91.40 after sell → book $8,435.97; vs 09:30 mark -2.06 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `PDFS` | 21 | $51.41 | $2.07 | $+20.23 | $4,494.75 | ▲ +20.23 after sell → book $8,433.89; vs 09:30 mark -2.08 | exit unpriced hold on first bar after 2 sess | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-01 09:30 ET | **SELL** | `RGEN` | 5 | $194.00 | $2.02 | $-14.03 | $5,462.73 | ▼ -14.03 after sell → book $8,431.87; vs 09:30 mark -2.02 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `VFC` | 74 | $14.11 | $2.23 | $-34.05 | $6,504.63 | ▼ -34.05 after sell → book $8,429.63; vs 09:30 mark -2.24 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 09:30 ET | **SELL** | `ZSQR` | 240 | $4.22 | $3.15 | $-66.24 | $7,514.29 | ▼ -66.24 after sell → book $8,426.49; vs 09:30 mark -3.14 | exit unpriced hold on first bar after 2 sess | — |
| 2026-10-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,514.29 | ▲ close $8,426.49 vs 09:30 $8,442.75 (session +0.00) | 16:00 close · cash $7,514.29 · equity $8,426.49 vs 09:30 $8,442.75 (-16.26; session marks +0.00) · 1 name(s) marked open→close (per-name table). SN×5 09:30 $182.44 → close $182.44 +0.00 | — |
| 2026-10-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,514.29 | ▲ 09:30 equity $8,432.14 vs yday $8,426.49 (+5.65) | 09:30 open · cash $7,514.29 (unchanged overnight, no fees) · equity $8,432.14 vs prior close $8,426.49 (+5.65) · 1 name(s) re-marked at the open (per-name table). SN×5 yday $182.44 → 09:30 $183.57 +5.65 | — |
| 2026-10-02 09:30 ET | **SELL** | `SN` | 5 | $183.57 | $2.02 | $-6.43 | $8,430.11 | ▼ -6.43 after sell → book $8,430.11; vs 09:30 mark -2.03 | exit unpriced hold on first bar after 3 sess | — |
| 2026-10-02 09:30 ET | **BUY** | `CDNA` | 15 | $66.33 | $2.04 | — | $7,433.13 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,ohlc_hot; 🔵; ⚪; ret5=+7.9; leftover $1053.76 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `ETON` | 20 | $52.42 | $2.05 | — | $6,382.68 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=-12.6; leftover $1053.76 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `INOD` | 14 | $73.05 | $2.03 | — | $5,357.95 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list probable,yday_gainer; 🔵; ⚪; ret5=-0.0; leftover $1053.76 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟡 heat🟢 vol🟢 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `SNPS` | 2 | $497.86 | $2.00 | — | $4,360.24 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+15.4; leftover $1053.76 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `NAUT` | 632 | $1.67 | $8.15 | — | $3,299.81 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer; 🔵; ⚪; ret5=+58.4; leftover $1053.76 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `FOSL` | 151 | $6.95 | $2.44 | — | $2,247.92 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list ohlc_hot; 🔵; ⚪; ret5=+16.2; leftover $1053.76 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `ARHS` | 104 | $10.12 | $2.30 | — | $1,193.13 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list ohlc_hot; 🔵; ⚪; ret5=+4.2; leftover $1053.76 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-02 09:30 ET | **BUY** | `PDFS` | 19 | $55.24 | $2.05 | — | $141.53 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list ohlc_hot; ⚪; ret5=+11.5; leftover $1053.76 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-10-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $141.53 | ▲ close $8,601.74 vs 09:30 $8,432.14 (session +194.68) | 16:00 close · cash $141.53 · equity $8,601.74 vs 09:30 $8,432.14 (+169.60; session marks +194.68) · 8 name(s) marked open→close (per-name table). CDNA×15 09:30 $66.33 → close $67.15 +12.30; ETON×20 09:30 $52.42 → close $55.36 +58.80; INOD×14 09:30 $73.05 → close $70.07 -41.72; SNPS×2 09:30 $497.86 → close $489.90 -15.91; NAUT×632 09:30 $1.67 → close $1.96 +186.44; FOSL×151 09:30 $6.95 → close $6.80 -22.65; ARHS×104 09:30 $10.12 → close $10.24 +12.48; PDFS×19 09:30 $55.24 → close $55.50 +4.94 | — |
| 2026-10-05 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $141.53 | ▲ 09:30 equity $8,784.28 vs yday $8,601.74 (+182.54) | 09:30 open · cash $141.53 (unchanged overnight, no fees) · equity $8,784.28 vs prior close $8,601.74 (+182.54) · 8 name(s) re-marked at the open (per-name table). ARHS×104 yday $10.24 → 09:30 $10.23 -1.04; CDNA×15 yday $67.15 → 09:30 $66.90 -3.75; ETON×20 yday $55.36 → 09:30 $55.85 +9.80; FOSL×151 yday $6.80 → 09:30 $6.86 +9.06; INOD×14 yday $70.07 → 09:30 $70.98 +12.74; NAUT×632 yday $1.96 → 09:30 $2.18 +139.04; PDFS×19 yday $55.50 → 09:30 $55.71 +3.99; SNPS×2 yday $489.90 → 09:30 $496.25 +12.70 | — |
| 2026-10-05 09:30 ET | **SELL** | `ARHS` | 104 | $10.23 | $2.33 | $+6.81 | $1,203.12 | ▲ +6.81 after sell → book $8,781.95; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-05 09:30 ET | **SELL** | `CDNA` | 15 | $66.90 | $2.06 | $+4.46 | $2,204.57 | ▲ +4.46 after sell → book $8,779.90; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-10-05 09:30 ET | **SELL** | `ETON` | 20 | $55.85 | $2.07 | $+64.48 | $3,319.50 | ▲ +64.48 after sell → book $8,777.83; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-10-05 09:30 ET | **SELL** | `FOSL` | 151 | $6.86 | $2.48 | $-18.51 | $4,352.88 | ▼ -18.51 after sell → book $8,775.35; vs 09:30 mark -2.48 | dropped from list after 1 sess (min 1) | — |
| 2026-10-05 09:30 ET | **SELL** | `INOD` | 14 | $70.98 | $2.05 | $-33.06 | $5,344.55 | ▼ -33.06 after sell → book $8,773.30; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-10-05 09:30 ET | **SELL** | `NAUT` | 632 | $2.18 | $8.27 | $+309.06 | $6,714.04 | ▲ +309.06 after sell → book $8,765.03; vs 09:30 mark -8.27 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟡 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-05 09:30 ET | **SELL** | `PDFS` | 19 | $55.71 | $2.07 | $+4.82 | $7,770.46 | ▲ +4.82 after sell → book $8,762.96; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-10-05 09:30 ET | **SELL** | `SNPS` | 2 | $496.25 | $2.02 | $-7.22 | $8,760.94 | ▼ -7.22 after sell → book $8,760.94; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-10-05 09:30 ET | **BUY** | `NTAP` | 38 | $225.47 | $2.10 | — | $190.98 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list ohlc_hot; ⚪; ret5=+12.5; leftover $8760.94 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-10-05 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $190.98 | ▼ close $8,694.24 vs 09:30 $8,784.28 (session -64.60) | 16:00 close · cash $190.98 · equity $8,694.24 vs 09:30 $8,784.28 (-90.04; session marks -64.60) · 1 name(s) marked open→close (per-name table). NTAP×38 09:30 $225.47 → close $223.77 -64.60 | — |
