# Factor mine action — `union_white_any_h2`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **2** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · −0 red + (yday up or major catalyst), then Score

Cash book **-9.68%** ($9,032) · signal-only (no cash/fees) was +6.04%. Starts YES **5/30**. Fills 168 · skips 94 · realized $-316.93.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 2 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- Minimum hold is 2 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 2 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `cam_bad_max=0,yday_or_catalyst=True` · **rank** `list` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **2**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $165.44.

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
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 12 | $0.94 | $0.15 | — | $81.08 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+0.5; leftover $11.56 | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 7 | $1.50 | $0.13 | — | $70.45 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+9.2; leftover $11.56 | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 2 | $4.31 | $0.09 | — | $61.74 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $11.56 | — |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 2 | $4.18 | $0.09 | — | $53.29 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $11.56 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $53.29 | ▲ close $10,711.20 vs 09:30 $10,360.67 (session +350.99) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $53.29 | ▼ 09:30 equity $10,684.81 vs yday $10,711.20 (-26.39) | — | — |
| 2026-08-17 09:30 ET | **SELL** | `BTSG` | 27 | $61.69 | $2.09 | $+46.86 | $1,716.83 | ▲ +46.86 after sell → book $10,682.72; vs 09:30 mark -2.09 | dropped from list after 2 sess (min 2) | — |
| 2026-08-17 09:30 ET | **SELL** | `IREN` | 36 | $45.23 | $2.12 | $-31.22 | $3,342.99 | ▼ -31.22 after sell → book $10,680.60; vs 09:30 mark -2.12 | dropped from list after 2 sess (min 2) | — |
| 2026-08-17 09:30 ET | **SELL** | `TPG` | 32 | $52.67 | $2.11 | $+61.30 | $5,026.32 | ▲ +61.30 after sell → book $10,678.49; vs 09:30 mark -2.11 | dropped from list after 2 sess (min 2) | — |
| 2026-08-17 09:30 ET | **SELL** | `SLS` | 142 | $12.78 | $2.45 | $+148.49 | $6,838.62 | ▲ +148.49 after sell → book $10,676.04; vs 09:30 mark -2.45 | dropped from list after 2 sess (min 2) | — |
| 2026-08-17 09:30 ET | **SELL** | `INO` | 2057 | $1.07 | $26.89 | $+485.09 | $9,012.72 | ▲ +485.09 after sell → book $10,649.14; vs 09:30 mark -26.90 | dropped from list after 2 sess (min 2) | — |
| 2026-08-17 09:30 ET | **SELL** | `TNDM` | 71 | $22.50 | $2.23 | $-63.36 | $10,607.99 | ▼ -63.36 after sell → book $10,646.91; vs 09:30 mark -2.23 | dropped from list after 2 sess (min 2) | — |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 409 | $3.24 | $5.28 | — | $9,277.55 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; ⚪; ret5=+0.3; leftover $1326.00 | — |
| 2026-08-17 09:30 ET | **BUY** | `CDNL` | 33 | $39.85 | $2.09 | — | $7,960.41 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list probable,yday_gainer; ⚪; ret5=-38.4; leftover $1326.00 | — |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 145 | $9.12 | $2.42 | — | $6,635.59 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $1326.00 | — |
| 2026-08-17 09:30 ET | **BUY** | `OCC` | 72 | $18.24 | $2.21 | — | $5,320.10 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list probable,ohlc_hot; ⚪; ret5=+9.5; leftover $1326.00 | — |
| 2026-08-17 09:30 ET | **BUY** | `ALM` | 81 | $16.20 | $2.23 | — | $4,005.67 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list probable,ohlc_hot; 🔵; ⚪; ret5=+6.4; leftover $1326.00 | — |
| 2026-08-17 09:30 ET | **BUY** | `UMAC` | 40 | $32.55 | $2.11 | — | $2,701.56 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,yday_mover; ⚪; ret5=+30.4; leftover $1326.00 | — |
| 2026-08-17 09:30 ET | **BUY** | `NPWR` | 690 | $1.92 | $8.90 | — | $1,367.86 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $1326.00 | — |
| 2026-08-17 09:30 ET | **BUY** | `LPTH` | 88 | $14.94 | $2.25 | — | $50.89 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,yday_mover,ohlc_hot; ⚪; ret5=+16.2; leftover $1326.00 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $50.89 | ▼ close $10,271.79 vs 09:30 $10,684.81 (session -347.63) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $50.89 | ▼ 09:30 equity $10,036.80 vs yday $10,271.79 (-234.99) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `LDI` | 12 | $0.87 | $0.16 | $-1.11 | $61.17 | ▼ -1.11 after sell → book $10,036.64; vs 09:30 mark -0.16 | dropped from list after 2 sess (min 2) | — |
| 2026-08-18 09:30 ET | **SELL** | `BTBT` | 7 | $1.54 | $0.15 | $+0.01 | $71.80 | ▲ +0.01 after sell → book $10,036.49; vs 09:30 mark -0.15 | dropped from list after 2 sess (min 2) | — |
| 2026-08-18 09:30 ET | **SELL** | `ANGX` | 2 | $4.79 | $0.12 | $+0.75 | $81.25 | ▲ +0.75 after sell → book $10,036.36; vs 09:30 mark -0.13 | dropped from list after 2 sess (min 2) | — |
| 2026-08-18 09:30 ET | **SELL** | `HYLN` | 2 | $3.95 | $0.10 | $-0.65 | $89.05 | ▼ -0.65 after sell → book $10,036.26; vs 09:30 mark -0.10 | dropped from list after 2 sess (min 2) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $89.05 | ▲ close $10,216.53 vs 09:30 $10,036.80 (session +180.27) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $89.05 | ▲ 09:30 equity $10,292.32 vs yday $10,216.53 (+75.79) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `DNN` | 409 | $3.19 | $5.35 | $-31.08 | $1,388.41 | ▼ -31.08 after sell → book $10,286.97; vs 09:30 mark -5.35 | dropped from list after 2 sess (min 2) | — |
| 2026-08-19 09:30 ET | **SELL** | `CDNL` | 33 | $44.83 | $2.11 | $+160.14 | $2,865.68 | ▲ +160.14 after sell → book $10,284.85; vs 09:30 mark -2.12 | dropped from list after 2 sess (min 2) | — |
| 2026-08-19 09:30 ET | **SELL** | `ABX` | 145 | $9.08 | $2.46 | $-10.68 | $4,179.82 | ▼ -10.68 after sell → book $10,282.39; vs 09:30 mark -2.46 | dropped from list after 2 sess (min 2) | — |
| 2026-08-19 09:30 ET | **SELL** | `OCC` | 72 | $16.21 | $2.23 | $-150.59 | $5,344.72 | ▼ -150.59 after sell → book $10,280.17; vs 09:30 mark -2.22 | dropped from list after 2 sess (min 2) | — |
| 2026-08-19 09:30 ET | **SELL** | `ALM` | 81 | $16.05 | $2.26 | $-16.64 | $6,642.51 | ▼ -16.64 after sell → book $10,277.91; vs 09:30 mark -2.26 | dropped from list after 2 sess (min 2) | — |
| 2026-08-19 09:30 ET | **SELL** | `UMAC` | 40 | $30.10 | $2.13 | $-102.24 | $7,844.38 | ▼ -102.24 after sell → book $10,275.78; vs 09:30 mark -2.13 | dropped from list after 2 sess (min 2) | — |
| 2026-08-19 09:30 ET | **SELL** | `NPWR` | 690 | $1.70 | $9.03 | $-169.73 | $9,008.35 | ▼ -169.73 after sell → book $10,266.75; vs 09:30 mark -9.03 | dropped from list after 2 sess (min 2) | — |
| 2026-08-19 09:30 ET | **SELL** | `LPTH` | 88 | $14.30 | $2.28 | $-60.85 | $10,264.48 | ▼ -60.85 after sell → book $10,264.48; vs 09:30 mark -2.27 | dropped from list after 2 sess (min 2) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,264.48 | ▲ close $10,264.48 vs 09:30 $10,292.32 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,264.48 | ▲ 09:30 equity $10,264.48 vs yday $10,264.48 (-0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 62 | $20.55 | $2.18 | — | $8,988.20 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1283.06 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 14 | $91.01 | $2.03 | — | $7,712.03 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1283.06 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 62 | $20.65 | $2.18 | — | $6,429.55 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1283.06 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 222 | $5.77 | $2.86 | — | $5,145.75 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1283.06 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 65 | $19.63 | $2.19 | — | $3,867.61 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1283.06 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 43 | $29.63 | $2.12 | — | $2,591.40 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1283.06 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 733 | $1.75 | $9.46 | — | $1,299.20 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1283.06 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $140.86 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1283.06 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $140.86 | ▲ close $10,479.56 vs 09:30 $10,264.48 (session +240.11) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $140.86 | ▲ 09:30 equity $10,755.01 vs yday $10,479.56 (+275.45) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 1 | $17.20 | $0.17 | — | $123.49 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $17.61 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 1 | $11.13 | $0.11 | — | $112.25 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $17.61 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 7 | $2.47 | $0.19 | — | $94.76 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $17.61 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 9 | $1.93 | $0.20 | — | $77.19 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $17.61 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 13 | $1.32 | $0.21 | — | $59.82 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $17.61 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $59.82 | ▼ close $10,751.73 vs 09:30 $10,755.01 (session -2.39) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $59.82 | ▲ 09:30 equity $10,860.84 vs yday $10,751.73 (+109.11) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `AG` | 62 | $21.30 | $2.20 | $+42.13 | $1,378.22 | ▲ +42.13 after sell → book $10,858.64; vs 09:30 mark -2.20 | dropped from list after 2 sess (min 2) | — |
| 2026-08-24 09:30 ET | **SELL** | `BHP` | 14 | $97.31 | $2.05 | $+84.12 | $2,738.51 | ▲ +84.12 after sell → book $10,856.59; vs 09:30 mark -2.05 | dropped from list after 2 sess (min 2) | — |
| 2026-08-24 09:30 ET | **SELL** | `CDE` | 62 | $21.26 | $2.20 | $+33.45 | $4,054.43 | ▲ +33.45 after sell → book $10,854.39; vs 09:30 mark -2.20 | dropped from list after 2 sess (min 2) | — |
| 2026-08-24 09:30 ET | **SELL** | `HDSN` | 222 | $5.69 | $2.91 | $-23.53 | $5,314.70 | ▼ -23.53 after sell → book $10,851.48; vs 09:30 mark -2.91 | dropped from list after 2 sess (min 2) | — |
| 2026-08-24 09:30 ET | **SELL** | `IAG` | 65 | $21.38 | $2.21 | $+109.36 | $6,702.20 | ▲ +109.36 after sell → book $10,849.28; vs 09:30 mark -2.20 | dropped from list after 2 sess (min 2) | — |
| 2026-08-24 09:30 ET | **SELL** | `KGC` | 43 | $33.03 | $2.14 | $+141.94 | $8,120.35 | ▲ +141.94 after sell → book $10,847.14; vs 09:30 mark -2.14 | dropped from list after 2 sess (min 2) | — |
| 2026-08-24 09:30 ET | **SELL** | `NFGC` | 733 | $1.86 | $9.59 | $+61.59 | $9,474.14 | ▲ +61.59 after sell → book $10,837.55; vs 09:30 mark -9.59 | dropped from list after 2 sess (min 2) | — |
| 2026-08-24 09:30 ET | **SELL** | `WPM` | 8 | $159.50 | $2.03 | $+115.63 | $10,748.10 | ▲ +115.63 after sell → book $10,835.51; vs 09:30 mark -2.04 | dropped from list after 2 sess (min 2) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,748.10 | ▼ close $10,833.97 vs 09:30 $10,860.84 (session -1.54) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,748.10 | ▼ 09:30 equity $10,832.80 vs yday $10,833.97 (-1.17) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `AUPH` | 1 | $16.63 | $0.19 | $-0.93 | $10,764.54 | ▼ -0.93 after sell → book $10,832.61; vs 09:30 mark -0.19 | dropped from list after 2 sess (min 2) | — |
| 2026-08-25 09:30 ET | **SELL** | `ARCT` | 1 | $14.12 | $0.16 | $+2.71 | $10,778.50 | ▲ +2.71 after sell → book $10,832.45; vs 09:30 mark -0.16 | dropped from list after 2 sess (min 2) | — |
| 2026-08-25 09:30 ET | **SELL** | `AUTL` | 7 | $2.38 | $0.21 | $-1.03 | $10,794.95 | ▼ -1.03 after sell → book $10,832.24; vs 09:30 mark -0.21 | dropped from list after 2 sess (min 2) | — |
| 2026-08-25 09:30 ET | **SELL** | `CRDL` | 9 | $1.89 | $0.22 | $-0.78 | $10,811.74 | ▼ -0.78 after sell → book $10,832.02; vs 09:30 mark -0.22 | dropped from list after 2 sess (min 2) | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 64 | $23.77 | $2.18 | — | $9,288.28 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; ⚪; ret5=+13.0; leftover $1544.53 | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 184 | $8.35 | $2.54 | — | $7,749.34 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+8.0; leftover $1544.53 | — |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 947 | $1.63 | $12.22 | — | $6,193.51 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; leftover $1544.53 | — |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 294 | $5.24 | $3.79 | — | $4,649.16 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+35.3; leftover $1544.53 | — |
| 2026-08-25 09:30 ET | **BUY** | `SUJA` | 175 | $8.79 | $2.52 | — | $3,108.40 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.1; leftover $1544.53 | — |
| 2026-08-25 09:30 ET | **BUY** | `DEFT` | 2491 | $0.62 | $22.92 | — | $1,541.06 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+33.3; leftover $1544.53 | — |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 241 | $6.37 | $3.11 | — | $2.78 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer; 🔵; ⚪; ret5=+10.9; leftover $1544.53 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2.78 | ▲ close $10,935.86 vs 09:30 $10,832.80 (session +153.11) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2.78 | ▼ 09:30 equity $10,932.04 vs yday $10,935.86 (-3.82) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 13 | $1.60 | $0.27 | $+3.16 | $23.31 | ▲ +3.16 after sell → book $10,931.78; vs 09:30 mark -0.26 | dropped from list after 3 sess (min 2) | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $23.31 | ▼ close $10,739.01 vs 09:30 $10,932.04 (session -192.76) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $23.31 | ▲ 09:30 equity $10,786.18 vs yday $10,739.01 (+47.17) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `MOS` | 64 | $24.00 | $2.20 | $+10.33 | $1,557.11 | ▲ +10.33 after sell → book $10,783.97; vs 09:30 mark -2.21 | dropped from list after 2 sess (min 2) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRMD` | 184 | $8.49 | $2.58 | $+20.63 | $3,116.68 | ▲ +20.63 after sell → book $10,781.39; vs 09:30 mark -2.58 | dropped from list after 2 sess (min 2) | — |
| 2026-08-27 09:30 ET | **SELL** | `BMEA` | 947 | $1.74 | $12.39 | $+79.57 | $4,752.08 | ▲ +79.57 after sell → book $10,769.00; vs 09:30 mark -12.39 | dropped from list after 2 sess (min 2) | — |
| 2026-08-27 09:30 ET | **SELL** | `ALVO` | 294 | $4.88 | $3.85 | $-113.49 | $6,182.94 | ▼ -113.49 after sell → book $10,765.15; vs 09:30 mark -3.85 | dropped from list after 2 sess (min 2) | — |
| 2026-08-27 09:30 ET | **SELL** | `SUJA` | 175 | $9.41 | $2.56 | $+103.43 | $7,827.14 | ▲ +103.43 after sell → book $10,762.59; vs 09:30 mark -2.56 | dropped from list after 2 sess (min 2) | — |
| 2026-08-27 09:30 ET | **SELL** | `DEFT` | 2491 | $0.60 | $22.74 | $-105.45 | $9,289.03 | ▼ -105.45 after sell → book $10,739.85; vs 09:30 mark -22.74 | dropped from list after 2 sess (min 2) | — |
| 2026-08-27 09:30 ET | **SELL** | `ZURA` | 241 | $6.02 | $3.16 | $-90.62 | $10,736.69 | ▼ -90.62 after sell → book $10,736.69; vs 09:30 mark -3.16 | dropped from list after 2 sess (min 2) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,736.69 | ▲ close $10,736.69 vs 09:30 $10,786.18 (session +0.00) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,736.69 | ▲ 09:30 equity $10,736.69 vs yday $10,736.69 (-0.00) | — | — |
| 2026-08-28 09:30 ET | **BUY** | `SIMO` | 5 | $252.24 | $2.00 | — | $9,473.48 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list probable,yday_gainer; ⚪; ret5=+2.2; leftover $1342.09 | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 9 | $141.76 | $2.02 | — | $8,195.63 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1342.09 | — |
| 2026-08-28 09:30 ET | **BUY** | `TTMI` | 10 | $122.81 | $2.02 | — | $6,965.51 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+9.0; leftover $1342.09 | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 4 | $324.41 | $2.00 | — | $5,665.86 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1342.09 | — |
| 2026-08-28 09:30 ET | **BUY** | `AVT` | 14 | $91.49 | $2.03 | — | $4,382.97 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list mover_buy; 🔵; ⚪; ret5=+5.3; leftover $1342.09 | — |
| 2026-08-28 09:30 ET | **BUY** | `CGNX` | 21 | $62.82 | $2.05 | — | $3,061.70 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list mover_buy; 🔵; ⚪; ret5=+5.4; leftover $1342.09 | — |
| 2026-08-28 09:30 ET | **BUY** | `COHR` | 4 | $289.44 | $2.00 | — | $1,901.94 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list mover_buy; 🔵; ⚪; ret5=+1.8; leftover $1342.09 | — |
| 2026-08-28 09:30 ET | **BUY** | `LSCC` | 11 | $119.76 | $2.02 | — | $582.55 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list mover_buy; 🔵; ⚪; ret5=+2.5; leftover $1342.09 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $582.55 | ▼ close $10,344.19 vs 09:30 $10,736.69 (session -376.34) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $582.55 | ▲ 09:30 equity $10,400.04 vs yday $10,344.19 (+55.85) | — | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $582.55 | ▲ close $10,407.45 vs 09:30 $10,400.04 (session +7.41) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $582.55 | ▼ 09:30 equity $10,210.19 vs yday $10,407.45 (-197.26) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `SIMO` | 5 | $240.09 | $2.02 | $-64.78 | $1,780.98 | ▼ -64.78 after sell → book $10,208.17; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 2) | — |
| 2026-09-01 09:30 ET | **SELL** | `SMTC` | 9 | $127.63 | $2.04 | $-131.22 | $2,927.61 | ▼ -131.22 after sell → book $10,206.13; vs 09:30 mark -2.04 | dropped from list after 2 sess (min 2) | — |
| 2026-09-01 09:30 ET | **SELL** | `TTMI` | 10 | $116.68 | $2.04 | $-65.36 | $4,092.37 | ▼ -65.36 after sell → book $10,204.09; vs 09:30 mark -2.04 | dropped from list after 2 sess (min 2) | — |
| 2026-09-01 09:30 ET | **SELL** | `KEYS` | 4 | $321.47 | $2.02 | $-15.78 | $5,376.23 | ▼ -15.78 after sell → book $10,202.07; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 2) | — |
| 2026-09-01 09:30 ET | **SELL** | `AVT` | 14 | $88.58 | $2.05 | $-44.82 | $6,614.30 | ▼ -44.82 after sell → book $10,200.02; vs 09:30 mark -2.05 | dropped from list after 2 sess (min 2) | — |
| 2026-09-01 09:30 ET | **SELL** | `CGNX` | 21 | $59.72 | $2.07 | $-69.23 | $7,866.34 | ▼ -69.23 after sell → book $10,197.94; vs 09:30 mark -2.08 | dropped from list after 2 sess (min 2) | — |
| 2026-09-01 09:30 ET | **SELL** | `COHR` | 4 | $270.50 | $2.02 | $-79.78 | $8,946.32 | ▼ -79.78 after sell → book $10,195.92; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 2) | — |
| 2026-09-01 09:30 ET | **SELL** | `LSCC` | 11 | $113.60 | $2.04 | $-71.83 | $10,193.88 | ▼ -71.83 after sell → book $10,193.88; vs 09:30 mark -2.04 | dropped from list after 2 sess (min 2) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,193.88 | ▲ close $10,193.88 vs 09:30 $10,210.19 (session +0.00) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,193.88 | ▲ 09:30 equity $10,193.88 vs yday $10,193.88 (-0.00) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,193.88 | ▲ close $10,193.88 vs 09:30 $10,193.88 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,193.88 | ▲ 09:30 equity $10,193.88 vs yday $10,193.88 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 24 | $52.88 | $2.06 | — | $8,922.70 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1274.23 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 29 | $42.93 | $2.08 | — | $7,675.65 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1274.23 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 351 | $3.63 | $4.53 | — | $6,396.99 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1274.23 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 158 | $8.03 | $2.46 | — | $5,125.79 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1274.23 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $132.45 | $2.02 | — | $3,931.72 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1274.23 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 75 | $16.77 | $2.21 | — | $2,671.76 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1274.23 | — |
| 2026-09-03 09:30 ET | **BUY** | `SLN` | 85 | $14.85 | $2.25 | — | $1,407.26 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list probable,yday_gainer; 🔵; ⚪; ret5=+9.3; leftover $1274.23 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 584 | $2.18 | $7.53 | — | $126.61 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $1274.23 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $126.61 | ▼ close $9,943.17 vs 09:30 $10,193.88 (session -225.57) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $126.61 | ▼ 09:30 equity $9,889.08 vs yday $9,943.17 (-54.09) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 8 | $2.52 | $0.23 | — | $106.22 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $21.10 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 3 | $6.71 | $0.21 | — | $85.88 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $21.10 | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 11 | $1.90 | $0.24 | — | $64.74 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $21.10 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 4 | $4.78 | $0.20 | — | $45.42 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $21.10 | — |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 13 | $1.59 | $0.25 | — | $24.50 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $21.10 | — |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 1 | $11.31 | $0.12 | — | $13.07 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $21.10 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $13.07 | ▲ close $9,983.52 vs 09:30 $9,889.08 (session +95.68) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13.07 | ▼ 09:30 equity $9,962.15 vs yday $9,983.52 (-21.37) | — | — |
| 2026-09-08 09:30 ET | **SELL** | `ATRC` | 24 | $54.31 | $2.08 | $+30.18 | $1,314.43 | ▲ +30.18 after sell → book $9,960.07; vs 09:30 mark -2.08 | dropped from list after 2 sess (min 2) | — |
| 2026-09-08 09:30 ET | **SELL** | `HRMY` | 29 | $42.20 | $2.10 | $-25.34 | $2,536.14 | ▼ -25.34 after sell → book $9,957.98; vs 09:30 mark -2.09 | dropped from list after 2 sess (min 2) | — |
| 2026-09-08 09:30 ET | **SELL** | `CABA` | 351 | $3.43 | $4.60 | $-79.32 | $3,735.47 | ▼ -79.32 after sell → book $9,953.38; vs 09:30 mark -4.60 | dropped from list after 2 sess (min 2) | — |
| 2026-09-08 09:30 ET | **SELL** | `VSTM` | 158 | $8.20 | $2.50 | $+21.90 | $5,028.57 | ▲ +21.90 after sell → book $9,950.88; vs 09:30 mark -2.50 | dropped from list after 2 sess (min 2) | — |
| 2026-09-08 09:30 ET | **SELL** | `RVTY` | 9 | $128.50 | $2.04 | $-39.60 | $6,183.03 | ▼ -39.60 after sell → book $9,948.84; vs 09:30 mark -2.04 | dropped from list after 2 sess (min 2) | — |
| 2026-09-08 09:30 ET | **SELL** | `ARCT` | 75 | $15.47 | $2.24 | $-101.95 | $7,341.04 | ▼ -101.95 after sell → book $9,946.60; vs 09:30 mark -2.24 | dropped from list after 2 sess (min 2) | — |
| 2026-09-08 09:30 ET | **SELL** | `SLN` | 85 | $14.24 | $2.27 | $-56.36 | $8,549.18 | ▼ -56.36 after sell → book $9,944.34; vs 09:30 mark -2.26 | dropped from list after 2 sess (min 2) | — |
| 2026-09-08 09:30 ET | **SELL** | `CRDL` | 584 | $2.20 | $7.64 | $-3.49 | $9,826.33 | ▼ -3.49 after sell → book $9,936.69; vs 09:30 mark -7.65 | dropped from list after 2 sess (min 2) | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,826.33 | ▼ close $9,935.42 vs 09:30 $9,962.15 (session -1.27) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,826.33 | ▼ 09:30 equity $9,934.99 vs yday $9,935.42 (-0.43) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `ALEC` | 8 | $2.47 | $0.24 | $-0.87 | $9,845.85 | ▼ -0.87 after sell → book $9,934.75; vs 09:30 mark -0.24 | dropped from list after 2 sess (min 2) | — |
| 2026-09-09 09:30 ET | **SELL** | `BHC` | 3 | $6.38 | $0.22 | $-1.42 | $9,864.77 | ▼ -1.42 after sell → book $9,934.53; vs 09:30 mark -0.22 | dropped from list after 2 sess (min 2) | — |
| 2026-09-09 09:30 ET | **SELL** | `BMEA` | 11 | $1.94 | $0.27 | $-0.07 | $9,885.85 | ▼ -0.07 after sell → book $9,934.27; vs 09:30 mark -0.26 | dropped from list after 2 sess (min 2) | — |
| 2026-09-09 09:30 ET | **SELL** | `OABI` | 4 | $4.21 | $0.20 | $-2.68 | $9,902.49 | ▼ -2.68 after sell → book $9,934.07; vs 09:30 mark -0.20 | dropped from list after 2 sess (min 2) | — |
| 2026-09-09 09:30 ET | **SELL** | `OPK` | 13 | $1.58 | $0.26 | $-0.64 | $9,922.76 | ▼ -0.64 after sell → book $9,933.80; vs 09:30 mark -0.27 | dropped from list after 2 sess (min 2) | — |
| 2026-09-09 09:30 ET | **SELL** | `VIR` | 1 | $11.04 | $0.13 | $-0.52 | $9,933.67 | ▼ -0.52 after sell → book $9,933.67; vs 09:30 mark -0.13 | dropped from list after 2 sess (min 2) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,933.67 | ▲ close $9,933.67 vs 09:30 $9,934.99 (session +0.00) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,933.67 | ▲ 09:30 equity $9,933.67 vs yday $9,933.67 (-0.00) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,933.67 | ▲ close $9,933.67 vs 09:30 $9,933.67 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,933.67 | ▲ 09:30 equity $9,933.67 vs yday $9,933.67 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 12 | $164.43 | $2.03 | — | $7,958.48 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,earn_react; ⚪; ret5=+4.9; leftover $1986.73 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAND` | 37 | $52.55 | $2.10 | — | $6,012.03 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list probable,yday_gainer; 🔵; ⚪; ret5=+8.6; leftover $1986.73 | — |
| 2026-09-11 09:30 ET | **BUY** | `PAGS` | 196 | $10.11 | $2.58 | — | $4,027.89 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer; 🔵; ⚪; ret5=+2.9; leftover $1986.73 | — |
| 2026-09-11 09:30 ET | **BUY** | `ZSQR` | 611 | $3.25 | $7.88 | — | $2,034.26 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer; 🔵; ⚪; ret5=+3.6; leftover $1986.73 | — |
| 2026-09-11 09:30 ET | **BUY** | `PAYP` | 108 | $18.30 | $2.31 | — | $55.55 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1986.73 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $55.55 | ▼ close $9,814.99 vs 09:30 $9,933.67 (session -101.78) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $55.55 | ▼ 09:30 equity $9,661.79 vs yday $9,814.99 (-153.20) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $55.55 | ▼ close $9,251.94 vs 09:30 $9,661.79 (session -409.85) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $55.55 | ▼ 09:30 equity $9,219.94 vs yday $9,251.94 (-32.00) | — | — |
| 2026-09-15 09:30 ET | **SELL** | `ORCL` | 12 | $143.46 | $2.05 | $-255.72 | $1,775.02 | ▼ -255.72 after sell → book $9,217.89; vs 09:30 mark -2.05 | dropped from list after 2 sess (min 2) | — |
| 2026-09-15 09:30 ET | **SELL** | `BAND` | 37 | $49.51 | $2.13 | $-116.71 | $3,604.76 | ▼ -116.71 after sell → book $9,215.76; vs 09:30 mark -2.13 | dropped from list after 2 sess (min 2) | — |
| 2026-09-15 09:30 ET | **SELL** | `PAGS` | 196 | $9.94 | $2.63 | $-38.52 | $5,550.38 | ▼ -38.52 after sell → book $9,213.14; vs 09:30 mark -2.62 | dropped from list after 2 sess (min 2) | — |
| 2026-09-15 09:30 ET | **SELL** | `ZSQR` | 611 | $2.76 | $8.00 | $-315.27 | $7,228.74 | ▼ -315.27 after sell → book $9,205.14; vs 09:30 mark -8.00 | dropped from list after 2 sess (min 2) | — |
| 2026-09-15 09:30 ET | **SELL** | `PAYP` | 108 | $18.30 | $2.35 | $-4.66 | $9,202.79 | ▼ -4.66 after sell → book $9,202.79; vs 09:30 mark -2.35 | dropped from list after 2 sess (min 2) | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,202.79 | ▲ close $9,202.79 vs 09:30 $9,219.94 (session +0.00) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,202.79 | ▲ 09:30 equity $9,202.79 vs yday $9,202.79 (+0.00) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `SWKS` | 51 | $89.38 | $2.14 | — | $4,642.27 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.4; leftover $4601.40 | — |
| 2026-09-16 09:30 ET | **BUY** | `QRVO` | 38 | $118.18 | $2.10 | — | $149.32 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+13.4; leftover $4601.40 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $149.32 | ▼ close $8,845.27 vs 09:30 $9,202.79 (session -353.27) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $149.32 | ▲ 09:30 equity $8,940.28 vs yday $8,845.27 (+95.01) | — | — |
| 2026-09-17 09:30 ET | **BUY** | `BULL` | 18 | $7.95 | $1.49 | — | $4.74 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_mover; 🔵; ⚪; ret5=-18.4; leftover $149.32 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.74 | ▲ close $9,342.22 vs 09:30 $8,940.28 (session +403.42) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.74 | ▲ 09:30 equity $9,429.47 vs yday $9,342.22 (+87.25) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `SWKS` | 51 | $92.05 | $2.19 | $+131.84 | $4,697.10 | ▲ +131.84 after sell → book $9,427.28; vs 09:30 mark -2.19 | dropped from list after 2 sess (min 2) | — |
| 2026-09-18 09:30 ET | **SELL** | `QRVO` | 38 | $120.76 | $2.15 | $+93.79 | $9,283.83 | ▲ +93.79 after sell → book $9,425.13; vs 09:30 mark -2.15 | dropped from list after 2 sess (min 2) | — |
| 2026-09-18 09:30 ET | **BUY** | `RBRK` | 10 | $108.55 | $2.02 | — | $8,196.31 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; ⚪; ret5=+21.3; leftover $1160.48 | — |
| 2026-09-18 09:30 ET | **BUY** | `ECO` | 13 | $85.00 | $2.03 | — | $7,089.28 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+18.3; leftover $1160.48 | — |
| 2026-09-18 09:30 ET | **BUY** | `FIVN` | 33 | $34.44 | $2.09 | — | $5,950.67 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+14.0; leftover $1160.48 | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 78 | $14.79 | $2.22 | — | $4,794.83 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $1160.48 | — |
| 2026-09-18 09:30 ET | **BUY** | `SDGR` | 39 | $29.32 | $2.11 | — | $3,649.24 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+60.9; leftover $1160.48 | — |
| 2026-09-18 09:30 ET | **BUY** | `CYPH` | 382 | $3.04 | $4.93 | — | $2,484.94 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+39.5; leftover $1160.48 | — |
| 2026-09-18 09:30 ET | **BUY** | `TEM` | 14 | $81.40 | $2.03 | — | $1,343.31 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.8; leftover $1160.48 | — |
| 2026-09-18 09:30 ET | **BUY** | `RXT` | 294 | $3.94 | $3.79 | — | $181.16 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer; 🔵; ⚪; ret5=+25.2; leftover $1160.48 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $181.16 | ▲ close $9,418.34 vs 09:30 $9,429.47 (session +14.43) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $181.16 | ▲ 09:30 equity $9,643.64 vs yday $9,418.34 (+225.30) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `BULL` | 18 | $8.57 | $1.62 | $+8.06 | $333.80 | ▲ +8.06 after sell → book $9,642.02; vs 09:30 mark -1.62 | dropped from list after 2 sess (min 2) | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $333.80 | ▲ close $9,659.96 vs 09:30 $9,643.64 (session +17.94) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $333.80 | ▲ 09:30 equity $9,711.56 vs yday $9,659.96 (+51.60) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `RARE` | 78 | $14.78 | $2.25 | $-5.25 | $1,484.39 | ▼ -5.25 after sell → book $9,709.31; vs 09:30 mark -2.25 | dropped from list after 2 sess (min 2) | — |
| 2026-09-22 09:30 ET | **SELL** | `CYPH` | 382 | $3.51 | $5.00 | $+171.52 | $2,820.21 | ▲ +171.52 after sell → book $9,704.31; vs 09:30 mark -5.00 | dropped from list after 2 sess (min 2) | — |
| 2026-09-22 09:30 ET | **SELL** | `TEM` | 14 | $77.99 | $2.05 | $-51.82 | $3,910.02 | ▼ -51.82 after sell → book $9,702.26; vs 09:30 mark -2.05 | dropped from list after 2 sess (min 2) | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $3,910.02 | ▲ close $9,702.26 vs 09:30 $9,711.56 (session +0.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,910.02 | ▼ 09:30 equity $9,695.16 vs yday $9,702.26 (-7.10) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `RBRK` | 10 | $112.46 | $2.04 | $+35.04 | $5,032.58 | ▲ +35.04 after sell → book $9,693.12; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 2) | — |
| 2026-09-23 09:30 ET | **SELL** | `ECO` | 13 | $77.55 | $2.05 | $-100.93 | $6,038.68 | ▼ -100.93 after sell → book $9,691.07; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 2) | — |
| 2026-09-23 09:30 ET | **SELL** | `FIVN` | 33 | $38.91 | $2.11 | $+143.15 | $7,320.44 | ▲ +143.15 after sell → book $9,688.97; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 2) | — |
| 2026-09-23 09:30 ET | **SELL** | `SDGR` | 39 | $30.05 | $2.13 | $+24.24 | $8,490.26 | ▲ +24.24 after sell → book $9,686.84; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 2) | — |
| 2026-09-23 09:30 ET | **SELL** | `RXT` | 294 | $4.07 | $3.85 | $+30.58 | $9,682.99 | ▲ +30.58 after sell → book $9,682.99; vs 09:30 mark -3.85 | dropped from list after 3 sess (min 2) | — |
| 2026-09-23 09:30 ET | **BUY** | `DXCM` | 13 | $89.50 | $2.03 | — | $8,517.46 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+5.3; leftover $1210.37 | — |
| 2026-09-23 09:30 ET | **BUY** | `A` | 7 | $166.54 | $2.01 | — | $7,349.67 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+10.3; leftover $1210.37 | — |
| 2026-09-23 09:30 ET | **BUY** | `HALO` | 10 | $116.85 | $2.02 | — | $6,179.15 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+3.3; leftover $1210.37 | — |
| 2026-09-23 09:30 ET | **BUY** | `ARQT` | 43 | $27.79 | $2.12 | — | $4,982.06 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+7.0; leftover $1210.37 | — |
| 2026-09-23 09:30 ET | **BUY** | `PGEN` | 152 | $7.95 | $2.45 | — | $3,771.21 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+12.4; leftover $1210.37 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 123 | $9.81 | $2.36 | — | $2,562.22 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+4.0; leftover $1210.37 | — |
| 2026-09-23 09:30 ET | **BUY** | `FTRE` | 59 | $20.25 | $2.17 | — | $1,365.31 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+15.0; leftover $1210.37 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 58 | $20.65 | $2.16 | — | $165.44 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $1210.37 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $165.44 | ▼ close $9,400.49 vs 09:30 $9,695.16 (session -265.18) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $165.44 | ▼ 09:30 equity $9,348.46 vs yday $9,400.49 (-52.03) | — | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $165.44 | ▲ close $9,484.32 vs 09:30 $9,348.46 (session +135.87) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,230.82 | ▲ 09:30 equity $9,064.27 vs yday $9,024.53 (+39.74) | 09:30 open · cash $1,230.82 (unchanged overnight, no fees) · equity $9,064.27 vs prior close $9,024.53 (+39.74) · 5 name(s) re-marked at the open (per-name table). ADMA×166 yday $9.52 → 09:30 $9.52 +0.00; ARQT×58 yday $26.27 → 09:30 $26.27 +0.00; FTRE×80 yday $20.02 → 09:30 $20.02 +0.00; HALO×13 yday $115.22 → 09:30 $115.36 +1.82; OMER×79 yday $20.13 → 09:30 $20.61 +37.92 | — |
| 2026-09-25 09:30 ET | **SELL** | `OMER` | 79 | $20.61 | $2.25 | $-7.64 | $2,856.76 | ▼ -7.64 after sell → book $9,062.02; vs 09:30 mark -2.25 | dropped from list after 2 sess (min 2) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 53 | $7.65 | $2.15 | — | $2,449.16 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+5.2; leftover $408.11 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TXG` | 4 | $83.76 | $2.00 | — | $2,112.12 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list probable,yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $408.11 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TEM` | 4 | $83.69 | $2.00 | — | $1,775.33 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list probable,ohlc_hot; ⚪; ret5=+2.3; leftover $408.11 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ZSQR` | 105 | $3.86 | $2.31 | — | $1,367.73 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; leftover $408.11 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TWST` | 2 | $184.00 | $2.00 | — | $997.73 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.3; leftover $408.11 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 25 | $16.21 | $2.06 | — | $590.42 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; leftover $408.11 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `GRAL` | 3 | $123.50 | $2.00 | — | $217.92 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+56.6; leftover $408.11 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $217.92 | ▼ close $9,032.11 vs 09:30 $9,064.27 (session -15.39) | 16:00 close · cash $217.92 · equity $9,032.11 vs 09:30 $9,064.27 (-32.16; session marks -15.39) · 11 name(s) marked open→close (per-name table). ADMA×166 09:30 $9.52 → close $9.52 +0.00; ARQT×58 09:30 $26.27 → close $26.27 +0.00; FTRE×80 09:30 $20.02 → close $20.02 +0.00; HALO×13 09:30 $115.36 → close $113.90 -18.98; MRVI×53 09:30 $7.65 → close $7.60 -2.65; TXG×4 09:30 $83.76 → close $85.71 +7.80; TEM×4 09:30 $83.69 → close $85.01 +5.26; ZSQR×105 09:30 $3.86 → close $3.78 -8.40; TWST×2 09:30 $184.00 → close $182.83 -2.34; SECZ×25 09:30 $16.21 → close $15.96 -6.25; GRAL×3 09:30 $123.50 → close $126.89 +10.17 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `BTSG` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-14 | `IREN` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-14 | `SLS` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-14 | `INO` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-14 | `TNDM` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-14 | `DAVE` | cash | leftover split 11.56 < 1 share @ 330.91 |
| 2026-08-14 | `SLG` | cash | leftover split 11.56 < 1 share @ 57.61 |
| 2026-08-14 | `BETR` | cash | leftover split 11.56 < 1 share @ 14.80 |
| 2026-08-14 | `WDC` | cash | leftover split 11.56 < 1 share @ 503.50 |
| 2026-08-17 | `LDI` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-17 | `HYLN` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-18 | `DNN` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-18 | `CDNL` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-18 | `ABX` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-18 | `OCC` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-18 | `ALM` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-18 | `UMAC` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-18 | `NPWR` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-18 | `LPTH` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 17.61 < 1 share @ 119.43 |
| 2026-08-21 | `AEM` | cash | leftover split 17.61 < 1 share @ 216.30 |
| 2026-08-21 | `CRSP` | cash | leftover split 17.61 < 1 share @ 59.72 |
| 2026-08-24 | `AUPH` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-24 | `ARCT` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-24 | `CRDL` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-26 | `MOS` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-26 | `CRMD` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-26 | `BMEA` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-26 | `ALVO` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-26 | `SUJA` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-26 | `DEFT` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-26 | `ZURA` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-31 | `SIMO` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-31 | `TTMI` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-31 | `KEYS` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-31 | `AVT` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-31 | `CGNX` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-31 | `COHR` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-08-31 | `LSCC` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-04 | `HRMY` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-04 | `VSTM` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-04 | `ARCT` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-04 | `SLN` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-04 | `CRDL` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-08 | `ALEC` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-08 | `BHC` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-08 | `BMEA` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-08 | `OABI` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-08 | `OPK` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-08 | `VIR` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-14 | `ORCL` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-14 | `BAND` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-14 | `PAGS` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-14 | `ZSQR` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-14 | `PAYP` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-17 | `SWKS` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-17 | `QRVO` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-18 | `BULL` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-21 | `RBRK` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-21 | `ECO` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-21 | `FIVN` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-21 | `RARE` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-21 | `SDGR` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-21 | `CYPH` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-21 | `TEM` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-21 | `RXT` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-22 | `RBRK` | no_price | no 09:30 open — carry |
| 2026-09-22 | `ECO` | no_price | no 09:30 open — carry |
| 2026-09-22 | `FIVN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `SDGR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `RXT` | no_price | no 09:30 open — carry |
| 2026-09-24 | `DXCM` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-24 | `A` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-24 | `HALO` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-24 | `ARQT` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-24 | `PGEN` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-24 | `ADMA` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-24 | `FTRE` | min_hold | dropped but min-hold 1/2 sess — no sell |
| 2026-09-24 | `OMER` | min_hold | dropped but min-hold 1/2 sess — no sell |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `DXCM` | 13 | 2026-09-23 @ $89.50 | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+5.3; leftover $1210.37 |
| `A` | 7 | 2026-09-23 @ $166.54 | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+10.3; leftover $1210.37 |
| `HALO` | 10 | 2026-09-23 @ $116.85 | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+3.3; leftover $1210.37 |
| `ARQT` | 43 | 2026-09-23 @ $27.79 | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+7.0; leftover $1210.37 |
| `PGEN` | 152 | 2026-09-23 @ $7.95 | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+12.4; leftover $1210.37 |
| `ADMA` | 123 | 2026-09-23 @ $9.81 | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+4.0; leftover $1210.37 |
| `FTRE` | 59 | 2026-09-23 @ $20.25 | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+15.0; leftover $1210.37 |
| `OMER` | 58 | 2026-09-23 @ $20.65 | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $1210.37 |
