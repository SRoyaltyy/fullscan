# Factor mine action — `union_white_any_h5`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **5** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · −0 red + (yday up or major catalyst), then Score

Cash book **-10.17%** ($8,983) · signal-only (no cash/fees) was -11.72%. Starts YES **1/30**. Fills 133 · skips 277 · realized $+163.47.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 5 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- Minimum hold is 5 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 5 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `cam_bad_max=0,yday_or_catalyst=True` · **rank** `list` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **5**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $353.01.

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
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 2 | $3.24 | $0.07 | — | $46.74 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; ⚪; ret5=+0.3; leftover $6.66 | — |
| 2026-08-17 09:30 ET | **BUY** | `NPWR` | 3 | $1.92 | $0.07 | — | $40.91 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $6.66 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $40.91 | ▲ close $10,786.74 vs 09:30 $10,684.81 (session +102.06) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $40.91 | ▼ 09:30 equity $10,651.79 vs yday $10,786.74 (-134.95) | — | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $40.91 | ▲ close $10,886.60 vs 09:30 $10,651.79 (session +234.81) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $40.91 | ▲ 09:30 equity $11,016.58 vs yday $10,886.60 (+129.98) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $40.91 | ▲ close $11,241.59 vs 09:30 $11,016.58 (session +225.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $40.91 | ▼ 09:30 equity $11,179.18 vs yday $11,241.59 (-62.41) | — | — |
| 2026-08-20 09:30 ET | **SELL** | `BTSG` | 27 | $58.64 | $2.09 | $-35.48 | $1,622.10 | ▼ -35.48 after sell → book $11,177.09; vs 09:30 mark -2.09 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `IREN` | 36 | $42.46 | $2.12 | $-130.94 | $3,148.54 | ▼ -130.94 after sell → book $11,174.97; vs 09:30 mark -2.12 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TPG` | 32 | $53.06 | $2.11 | $+73.78 | $4,844.35 | ▲ +73.78 after sell → book $11,172.86; vs 09:30 mark -2.11 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `SLS` | 142 | $13.84 | $2.46 | $+299.01 | $6,807.17 | ▲ +299.01 after sell → book $11,170.40; vs 09:30 mark -2.46 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `INO` | 2057 | $1.30 | $26.90 | $+958.20 | $9,454.38 | ▲ +958.20 after sell → book $11,143.50; vs 09:30 mark -26.90 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TNDM` | 71 | $23.11 | $2.23 | $-20.05 | $11,092.96 | ▼ -20.05 after sell → book $11,141.28; vs 09:30 mark -2.22 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 67 | $20.55 | $2.19 | — | $9,713.92 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1386.62 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 15 | $91.01 | $2.04 | — | $8,346.73 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1386.62 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 67 | $20.65 | $2.19 | — | $6,960.99 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1386.62 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 240 | $5.77 | $3.10 | — | $5,573.10 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1386.62 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 70 | $19.63 | $2.20 | — | $4,196.80 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1386.62 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 46 | $29.63 | $2.13 | — | $2,831.69 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1386.62 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 792 | $1.75 | $10.22 | — | $1,435.47 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1386.62 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 9 | $144.54 | $2.02 | — | $132.59 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1386.62 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $132.59 | ▲ close $11,375.32 vs 09:30 $11,179.18 (session +260.12) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $132.59 | ▲ 09:30 equity $11,674.67 vs yday $11,375.32 (+299.35) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `LDI` | 12 | $0.87 | $0.16 | $-1.15 | $142.84 | ▼ -1.15 after sell → book $11,674.51; vs 09:30 mark -0.16 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTBT` | 7 | $1.66 | $0.16 | $+0.84 | $154.30 | ▲ +0.84 after sell → book $11,674.35; vs 09:30 mark -0.16 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `ANGX` | 2 | $4.43 | $0.11 | $+0.03 | $163.05 | ▲ +0.03 after sell → book $11,674.24; vs 09:30 mark -0.11 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `HYLN` | 2 | $3.42 | $0.09 | $-1.70 | $169.79 | ▼ -1.70 after sell → book $11,674.14; vs 09:30 mark -0.10 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 1 | $17.20 | $0.17 | — | $152.42 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $21.22 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 1 | $11.13 | $0.11 | — | $141.17 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $21.22 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 8 | $2.47 | $0.22 | — | $121.19 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $21.22 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 10 | $1.93 | $0.22 | — | $101.67 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $21.22 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 16 | $1.32 | $0.26 | — | $80.29 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $21.22 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $80.29 | ▼ close $11,671.82 vs 09:30 $11,674.67 (session -1.33) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $80.29 | ▲ 09:30 equity $11,791.36 vs yday $11,671.82 (+119.54) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `DNN` | 2 | $3.50 | $0.10 | $+0.35 | $87.19 | ▲ +0.35 after sell → book $11,791.26; vs 09:30 mark -0.10 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `NPWR` | 3 | $1.84 | $0.08 | $-0.39 | $92.63 | ▼ -0.39 after sell → book $11,791.18; vs 09:30 mark -0.08 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $92.63 | ▼ close $11,753.65 vs 09:30 $11,791.36 (session -37.53) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $92.63 | ▼ 09:30 equity $11,569.12 vs yday $11,753.65 (-184.53) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 1 | $8.35 | $0.09 | — | $84.19 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+8.0; leftover $13.23 | — |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 8 | $1.63 | $0.15 | — | $71.00 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; leftover $13.23 | — |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 2 | $5.24 | $0.11 | — | $60.41 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+35.3; leftover $13.23 | — |
| 2026-08-25 09:30 ET | **BUY** | `SUJA` | 1 | $8.79 | $0.09 | — | $51.53 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.1; leftover $13.23 | — |
| 2026-08-25 09:30 ET | **BUY** | `DEFT` | 21 | $0.62 | $0.19 | — | $38.31 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+33.3; leftover $13.23 | — |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 2 | $6.37 | $0.13 | — | $25.44 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer; 🔵; ⚪; ret5=+10.9; leftover $13.23 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $25.44 | ▲ close $12,034.44 vs 09:30 $11,569.12 (session +466.09) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $25.44 | ▼ 09:30 equity $11,816.41 vs yday $12,034.44 (-218.03) | — | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $25.44 | ▼ close $11,650.47 vs 09:30 $11,816.41 (session -165.94) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $25.44 | ▲ 09:30 equity $11,677.07 vs yday $11,650.47 (+26.60) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `AG` | 67 | $20.93 | $2.21 | $+21.06 | $1,425.54 | ▲ +21.06 after sell → book $11,674.86; vs 09:30 mark -2.21 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `BHP` | 15 | $95.52 | $2.06 | $+63.56 | $2,856.28 | ▲ +63.56 after sell → book $11,672.81; vs 09:30 mark -2.05 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `CDE` | 67 | $21.31 | $2.21 | $+39.82 | $4,281.84 | ▲ +39.82 after sell → book $11,670.59; vs 09:30 mark -2.22 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `HDSN` | 240 | $5.49 | $3.15 | $-73.44 | $5,596.29 | ▼ -73.44 after sell → book $11,667.45; vs 09:30 mark -3.14 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `IAG` | 70 | $21.47 | $2.22 | $+124.38 | $7,096.97 | ▲ +124.38 after sell → book $11,665.22; vs 09:30 mark -2.23 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `KGC` | 46 | $32.32 | $2.15 | $+119.46 | $8,581.54 | ▲ +119.46 after sell → book $11,663.07; vs 09:30 mark -2.15 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `NFGC` | 792 | $1.91 | $10.36 | $+106.14 | $10,083.90 | ▲ +106.14 after sell → book $11,652.71; vs 09:30 mark -10.36 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `WPM` | 9 | $155.89 | $2.04 | $+98.09 | $11,484.87 | ▲ +98.09 after sell → book $11,650.67; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $11,484.87 | ▲ close $11,653.11 vs 09:30 $11,677.07 (session +2.45) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,484.87 | ▼ 09:30 equity $11,650.91 vs yday $11,653.11 (-2.20) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `AUPH` | 1 | $16.44 | $0.19 | $-1.12 | $11,501.12 | ▼ -1.12 after sell → book $11,650.72; vs 09:30 mark -0.19 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `ARCT` | 1 | $15.43 | $0.18 | $+4.01 | $11,516.37 | ▲ +4.01 after sell → book $11,650.55; vs 09:30 mark -0.17 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `AUTL` | 8 | $2.35 | $0.23 | $-1.41 | $11,534.94 | ▼ -1.41 after sell → book $11,650.32; vs 09:30 mark -0.23 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRDL` | 10 | $2.06 | $0.26 | $+0.82 | $11,555.28 | ▲ +0.82 after sell → book $11,650.06; vs 09:30 mark -0.26 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CYPH` | 16 | $1.82 | $0.36 | $+7.38 | $11,584.05 | ▲ +7.38 after sell → book $11,649.70; vs 09:30 mark -0.36 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **BUY** | `SIMO` | 5 | $252.24 | $2.00 | — | $10,320.84 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list probable,yday_gainer; ⚪; ret5=+2.2; leftover $1448.01 | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 10 | $141.76 | $2.02 | — | $8,901.22 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1448.01 | — |
| 2026-08-28 09:30 ET | **BUY** | `TTMI` | 11 | $122.81 | $2.02 | — | $7,548.29 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+9.0; leftover $1448.01 | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 4 | $324.41 | $2.00 | — | $6,248.65 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1448.01 | — |
| 2026-08-28 09:30 ET | **BUY** | `AVT` | 15 | $91.49 | $2.04 | — | $4,874.26 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list mover_buy; 🔵; ⚪; ret5=+5.3; leftover $1448.01 | — |
| 2026-08-28 09:30 ET | **BUY** | `CGNX` | 23 | $62.82 | $2.06 | — | $3,427.34 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list mover_buy; 🔵; ⚪; ret5=+5.4; leftover $1448.01 | — |
| 2026-08-28 09:30 ET | **BUY** | `COHR` | 5 | $289.44 | $2.00 | — | $1,978.14 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list mover_buy; 🔵; ⚪; ret5=+1.8; leftover $1448.01 | — |
| 2026-08-28 09:30 ET | **BUY** | `LSCC` | 12 | $119.76 | $2.03 | — | $538.99 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list mover_buy; 🔵; ⚪; ret5=+2.5; leftover $1448.01 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $538.99 | ▼ close $11,219.45 vs 09:30 $11,650.91 (session -414.08) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $538.99 | ▲ 09:30 equity $11,279.58 vs yday $11,219.45 (+60.13) | — | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $538.99 | ▲ close $11,286.31 vs 09:30 $11,279.58 (session +6.74) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $538.99 | ▼ 09:30 equity $11,069.16 vs yday $11,286.31 (-217.15) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `CRMD` | 1 | $8.25 | $0.11 | $-0.29 | $547.13 | ▼ -0.29 after sell → book $11,069.05; vs 09:30 mark -0.11 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `BMEA` | 8 | $1.68 | $0.18 | $+0.07 | $560.40 | ▲ +0.07 after sell → book $11,068.88; vs 09:30 mark -0.17 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `ALVO` | 2 | $5.08 | $0.13 | $-0.56 | $570.43 | ▼ -0.56 after sell → book $11,068.75; vs 09:30 mark -0.13 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `SUJA` | 1 | $9.98 | $0.12 | $+0.98 | $580.29 | ▲ +0.98 after sell → book $11,068.63; vs 09:30 mark -0.12 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `DEFT` | 21 | $0.63 | $0.22 | $-0.20 | $593.30 | ▼ -0.20 after sell → book $11,068.41; vs 09:30 mark -0.22 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `ZURA` | 2 | $5.52 | $0.14 | $-1.97 | $604.20 | ▼ -1.97 after sell → book $11,068.27; vs 09:30 mark -0.14 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $604.20 | ▲ close $11,082.36 vs 09:30 $11,069.16 (session +14.09) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $604.20 | ▼ 09:30 equity $11,056.37 vs yday $11,082.36 (-25.99) | — | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $604.20 | ▲ close $11,151.40 vs 09:30 $11,056.37 (session +95.03) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $604.20 | ▼ 09:30 equity $11,069.97 vs yday $11,151.40 (-81.43) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 1 | $52.88 | $0.53 | — | $550.79 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+9.2; leftover $75.53 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 1 | $42.93 | $0.43 | — | $507.43 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $75.53 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 20 | $3.63 | $0.79 | — | $434.04 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $75.53 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 9 | $8.03 | $0.75 | — | $361.02 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $75.53 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 4 | $16.77 | $0.68 | — | $293.26 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $75.53 | — |
| 2026-09-03 09:30 ET | **BUY** | `SLN` | 5 | $14.85 | $0.76 | — | $218.25 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list probable,yday_gainer; 🔵; ⚪; ret5=+9.3; leftover $75.53 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 34 | $2.18 | $0.84 | — | $143.29 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $75.53 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $143.29 | ▲ close $11,127.01 vs 09:30 $11,069.97 (session +61.82) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $143.29 | ▲ 09:30 equity $11,285.14 vs yday $11,127.01 (+158.13) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `SIMO` | 5 | $239.23 | $2.02 | $-69.08 | $1,337.42 | ▼ -69.08 after sell → book $11,283.12; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `SMTC` | 10 | $138.71 | $2.04 | $-34.56 | $2,722.47 | ▼ -34.56 after sell → book $11,281.07; vs 09:30 mark -2.05 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `TTMI` | 11 | $118.58 | $2.04 | $-50.60 | $4,024.81 | ▼ -50.60 after sell → book $11,279.03; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `KEYS` | 4 | $326.10 | $2.02 | $+2.74 | $5,327.19 | ▲ +2.74 after sell → book $11,277.01; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `AVT` | 15 | $91.02 | $2.06 | $-11.14 | $6,690.43 | ▼ -11.14 after sell → book $11,274.95; vs 09:30 mark -2.06 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `CGNX` | 23 | $60.96 | $2.08 | $-46.92 | $8,090.43 | ▼ -46.92 after sell → book $11,272.87; vs 09:30 mark -2.08 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `COHR` | 5 | $269.69 | $2.03 | $-102.78 | $9,436.86 | ▼ -102.78 after sell → book $11,270.85; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `LSCC` | 12 | $115.92 | $2.05 | $-50.15 | $10,825.85 | ▼ -50.15 after sell → book $11,268.80; vs 09:30 mark -2.05 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 715 | $2.52 | $9.22 | — | $9,014.83 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $1804.31 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 268 | $6.71 | $3.46 | — | $7,213.09 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1804.31 | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 949 | $1.90 | $12.24 | — | $5,397.75 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $1804.31 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 377 | $4.78 | $4.86 | — | $3,590.82 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $1804.31 | — |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 1134 | $1.59 | $14.63 | — | $1,773.14 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1804.31 | — |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 156 | $11.31 | $2.46 | — | $6.32 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $1804.31 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6.32 | ▼ close $11,166.00 vs 09:30 $11,285.14 (session -55.93) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6.32 | ▼ 09:30 equity $11,033.41 vs yday $11,166.00 (-132.59) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6.32 | ▼ close $10,913.17 vs 09:30 $11,033.41 (session -120.24) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6.32 | ▼ 09:30 equity $10,862.58 vs yday $10,913.17 (-50.59) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6.32 | ▼ close $10,406.24 vs 09:30 $10,862.58 (session -456.34) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6.32 | ▼ 09:30 equity $10,251.26 vs yday $10,406.24 (-154.98) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6.32 | ▼ close $10,013.00 vs 09:30 $10,251.26 (session -238.26) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6.32 | ▲ 09:30 equity $10,139.76 vs yday $10,013.00 (+126.76) | — | — |
| 2026-09-11 09:30 ET | **SELL** | `ATRC` | 1 | $53.53 | $0.56 | $-0.44 | $59.29 | ▼ -0.44 after sell → book $10,139.20; vs 09:30 mark -0.56 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `HRMY` | 1 | $41.30 | $0.44 | $-2.50 | $100.15 | ▼ -2.50 after sell → book $10,138.76; vs 09:30 mark -0.44 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `CABA` | 20 | $2.77 | $0.63 | $-18.62 | $154.92 | ▼ -18.62 after sell → book $10,138.13; vs 09:30 mark -0.63 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `VSTM` | 9 | $7.70 | $0.74 | $-4.46 | $223.48 | ▼ -4.46 after sell → book $10,137.39; vs 09:30 mark -0.74 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `ARCT` | 4 | $14.06 | $0.59 | $-12.12 | $279.12 | ▼ -12.12 after sell → book $10,136.79; vs 09:30 mark -0.60 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `SLN` | 5 | $13.32 | $0.70 | $-9.11 | $345.02 | ▼ -9.11 after sell → book $10,136.09; vs 09:30 mark -0.70 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `CRDL` | 34 | $2.03 | $0.81 | $-6.76 | $413.23 | ▼ -6.76 after sell → book $10,135.28; vs 09:30 mark -0.81 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **BUY** | `BAND` | 1 | $52.55 | $0.53 | — | $360.15 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list probable,yday_gainer; 🔵; ⚪; ret5=+8.6; leftover $82.65 | — |
| 2026-09-11 09:30 ET | **BUY** | `PAGS` | 8 | $10.11 | $0.83 | — | $278.44 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer; 🔵; ⚪; ret5=+2.9; leftover $82.65 | — |
| 2026-09-11 09:30 ET | **BUY** | `ZSQR` | 25 | $3.25 | $0.89 | — | $196.30 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer; 🔵; ⚪; ret5=+3.6; leftover $82.65 | — |
| 2026-09-11 09:30 ET | **BUY** | `PAYP` | 4 | $18.30 | $0.74 | — | $122.36 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $82.65 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $122.36 | ▲ close $10,134.07 vs 09:30 $10,139.76 (session +1.78) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $122.36 | ▲ 09:30 equity $10,196.85 vs yday $10,134.07 (+62.78) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `ALEC` | 715 | $2.15 | $9.35 | $-283.13 | $1,650.25 | ▼ -283.13 after sell → book $10,187.50; vs 09:30 mark -9.35 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `BHC` | 268 | $5.93 | $3.51 | $-216.01 | $3,235.98 | ▼ -216.01 after sell → book $10,183.98; vs 09:30 mark -3.52 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `BMEA` | 949 | $1.72 | $12.41 | $-200.22 | $4,851.10 | ▼ -200.22 after sell → book $10,171.57; vs 09:30 mark -12.41 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `OABI` | 377 | $4.13 | $4.94 | $-254.85 | $6,403.17 | ▼ -254.85 after sell → book $10,166.63; vs 09:30 mark -4.94 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `OPK` | 1134 | $1.59 | $14.83 | $-29.46 | $8,191.40 | ▼ -29.46 after sell → book $10,151.80; vs 09:30 mark -14.83 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `VIR` | 156 | $10.73 | $2.50 | $-95.44 | $9,862.79 | ▼ -95.44 after sell → book $10,149.31; vs 09:30 mark -2.49 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,862.79 | ▼ close $10,134.79 vs 09:30 $10,196.85 (session -14.52) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,862.79 | ▼ 09:30 equity $10,134.02 vs yday $10,134.79 (-0.77) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,862.79 | ▼ close $10,118.86 vs 09:30 $10,134.02 (session -15.16) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,862.79 | ▼ 09:30 equity $10,115.93 vs yday $10,118.86 (-2.93) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `SWKS` | 55 | $89.38 | $2.15 | — | $4,944.73 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.4; leftover $4931.39 | — |
| 2026-09-16 09:30 ET | **BUY** | `QRVO` | 41 | $118.18 | $2.11 | — | $97.24 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+13.4; leftover $4931.39 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $97.24 | ▼ close $9,730.00 vs 09:30 $10,115.93 (session -381.66) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $97.24 | ▲ 09:30 equity $9,836.18 vs yday $9,730.00 (+106.18) | — | — |
| 2026-09-17 09:30 ET | **BUY** | `BULL` | 12 | $7.95 | $0.99 | — | $0.85 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_mover; 🔵; ⚪; ret5=-18.4; leftover $97.24 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $0.85 | ▲ close $10,278.01 vs 09:30 $9,836.18 (session +442.82) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.85 | ▲ 09:30 equity $10,370.68 vs yday $10,278.01 (+92.67) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `BAND` | 1 | $51.19 | $0.53 | $-2.43 | $51.50 | ▼ -2.43 after sell → book $10,370.15; vs 09:30 mark -0.53 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `PAGS` | 8 | $9.55 | $0.81 | $-6.12 | $127.09 | ▼ -6.12 after sell → book $10,369.34; vs 09:30 mark -0.81 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `ZSQR` | 25 | $2.50 | $0.72 | $-20.36 | $188.87 | ▼ -20.36 after sell → book $10,368.62; vs 09:30 mark -0.72 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `PAYP` | 4 | $17.91 | $0.75 | $-3.05 | $259.76 | ▼ -3.05 after sell → book $10,367.87; vs 09:30 mark -0.75 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **BUY** | `RARE` | 2 | $14.79 | $0.30 | — | $229.88 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $32.47 | — |
| 2026-09-18 09:30 ET | **BUY** | `SDGR` | 1 | $29.32 | $0.30 | — | $200.26 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+60.9; leftover $32.47 | — |
| 2026-09-18 09:30 ET | **BUY** | `CYPH` | 10 | $3.04 | $0.33 | — | $169.58 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+39.5; leftover $32.47 | — |
| 2026-09-18 09:30 ET | **BUY** | `RXT` | 8 | $3.94 | $0.34 | — | $137.72 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer; 🔵; ⚪; ret5=+25.2; leftover $32.47 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $137.72 | ▼ close $10,047.34 vs 09:30 $10,370.68 (session -319.26) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $137.72 | ▲ 09:30 equity $10,157.69 vs yday $10,047.34 (+110.35) | — | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $137.72 | ▼ close $10,048.89 vs 09:30 $10,157.69 (session -108.80) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $137.72 | ▲ 09:30 equity $10,050.55 vs yday $10,048.89 (+1.66) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $137.72 | ▲ close $10,052.20 vs 09:30 $10,050.55 (session +1.65) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $137.72 | ▲ 09:30 equity $10,183.28 vs yday $10,052.20 (+131.08) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `SWKS` | 55 | $90.21 | $2.20 | $+41.29 | $5,097.07 | ▲ +41.29 after sell → book $10,181.08; vs 09:30 mark -2.20 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `QRVO` | 41 | $118.44 | $2.16 | $+6.39 | $9,950.94 | ▲ +6.39 after sell → book $10,178.91; vs 09:30 mark -2.17 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **BUY** | `DXCM` | 13 | $89.50 | $2.03 | — | $8,785.41 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+5.3; leftover $1243.87 | — |
| 2026-09-23 09:30 ET | **BUY** | `A` | 7 | $166.54 | $2.01 | — | $7,617.62 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+10.3; leftover $1243.87 | — |
| 2026-09-23 09:30 ET | **BUY** | `HALO` | 10 | $116.85 | $2.02 | — | $6,447.10 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+3.3; leftover $1243.87 | — |
| 2026-09-23 09:30 ET | **BUY** | `ARQT` | 44 | $27.79 | $2.12 | — | $5,222.22 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+7.0; leftover $1243.87 | — |
| 2026-09-23 09:30 ET | **BUY** | `PGEN` | 156 | $7.95 | $2.46 | — | $3,979.56 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+12.4; leftover $1243.87 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 126 | $9.81 | $2.37 | — | $2,741.14 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+4.0; leftover $1243.87 | — |
| 2026-09-23 09:30 ET | **BUY** | `FTRE` | 61 | $20.25 | $2.17 | — | $1,503.71 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+15.0; leftover $1243.87 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 60 | $20.65 | $2.17 | — | $262.54 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $1243.87 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $262.54 | ▼ close $9,882.64 vs 09:30 $10,183.28 (session -278.92) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $262.54 | ▼ 09:30 equity $9,825.95 vs yday $9,882.64 (-56.69) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `BULL` | 12 | $7.62 | $0.97 | $-5.92 | $353.01 | ▼ -5.92 after sell → book $9,824.98; vs 09:30 mark -0.97 | dropped from list after 5 sess (min 5) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $353.01 | ▲ close $9,971.21 vs 09:30 $9,825.95 (session +146.24) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $411.09 | ▲ 09:30 equity $9,024.52 vs yday $9,002.02 (+22.50) | 09:30 open · cash $411.09 (unchanged overnight, no fees) · equity $9,024.52 vs prior close $9,002.02 (+22.50) · 8 name(s) re-marked at the open (per-name table). A×6 yday $172.84 → 09:30 $171.98 -5.16; ADMA×115 yday $9.52 → 09:30 $9.52 +0.00; ARQT×40 yday $26.27 → 09:30 $26.27 +0.00; DXCM×12 yday $87.47 → 09:30 $87.47 +0.00; FTRE×56 yday $20.02 → 09:30 $20.02 +0.00; HALO×9 yday $115.22 → 09:30 $115.36 +1.26; OMER×55 yday $20.13 → 09:30 $20.61 +26.40; PGEN×142 yday $7.70 → 09:30 $7.70 +0.00 | — |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 7 | $7.65 | $0.56 | — | $356.98 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+5.2; leftover $58.73 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ZSQR` | 15 | $3.86 | $0.62 | — | $298.46 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; leftover $58.73 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 3 | $16.21 | $0.50 | — | $249.33 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; leftover $58.73 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $249.33 | ▼ close $8,983.11 vs 09:30 $9,024.52 (session -39.73) | 16:00 close · cash $249.33 · equity $8,983.11 vs 09:30 $9,024.52 (-41.41; session marks -39.73) · 11 name(s) marked open→close (per-name table). A×6 09:30 $171.98 → close $172.79 +4.86; ADMA×115 09:30 $9.52 → close $9.52 +0.00; ARQT×40 09:30 $26.27 → close $26.27 +0.00; DXCM×12 09:30 $87.47 → close $87.47 +0.00; FTRE×56 09:30 $20.02 → close $20.02 +0.00; HALO×9 09:30 $115.36 → close $113.90 -13.14; OMER×55 09:30 $20.61 → close $20.08 -29.15; PGEN×142 09:30 $7.70 → close $7.70 -0.00; MRVI×7 09:30 $7.65 → close $7.60 -0.35; ZSQR×15 09:30 $3.86 → close $3.78 -1.20; SECZ×3 09:30 $16.21 → close $15.96 -0.75 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `BTSG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `IREN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `SLS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `INO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `TNDM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `DAVE` | cash | leftover split 11.56 < 1 share @ 330.91 |
| 2026-08-14 | `SLG` | cash | leftover split 11.56 < 1 share @ 57.61 |
| 2026-08-14 | `BETR` | cash | leftover split 11.56 < 1 share @ 14.80 |
| 2026-08-14 | `WDC` | cash | leftover split 11.56 < 1 share @ 503.50 |
| 2026-08-17 | `BTSG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `IREN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `SLS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `INO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `TNDM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `LDI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `HYLN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `CDNL` | cash | leftover split 6.66 < 1 share @ 39.85 |
| 2026-08-17 | `ABX` | cash | leftover split 6.66 < 1 share @ 9.12 |
| 2026-08-17 | `OCC` | cash | leftover split 6.66 < 1 share @ 18.24 |
| 2026-08-17 | `ALM` | cash | leftover split 6.66 < 1 share @ 16.20 |
| 2026-08-17 | `UMAC` | cash | leftover split 6.66 < 1 share @ 32.55 |
| 2026-08-17 | `LPTH` | cash | leftover split 6.66 < 1 share @ 14.94 |
| 2026-08-18 | `BTSG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `IREN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `TPG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `SLS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `INO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `TNDM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `LDI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `ANGX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `HYLN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `DNN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `NPWR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-19 | `BTSG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `IREN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `TPG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `SLS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `INO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `TNDM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `LDI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `BTBT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `ANGX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `HYLN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `DNN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `NPWR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-20 | `LDI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `BTBT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `ANGX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `HYLN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `DNN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `NPWR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-21 | `DNN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `NPWR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 21.22 < 1 share @ 119.43 |
| 2026-08-21 | `AEM` | cash | leftover split 21.22 < 1 share @ 216.30 |
| 2026-08-21 | `CRSP` | cash | leftover split 21.22 < 1 share @ 59.72 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `AUPH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `ARCT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `CRDL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-25 | `AG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `BHP` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `CDE` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `HDSN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `IAG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `KGC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `NFGC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `WPM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `AUPH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `CRDL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `MOS` | cash | leftover split 13.23 < 1 share @ 23.77 |
| 2026-08-26 | `AG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `BHP` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `CDE` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `HDSN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `IAG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `KGC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `NFGC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `WPM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `AUPH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `ARCT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `AUTL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `CRDL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `CYPH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `CRMD` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `BMEA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `ALVO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `SUJA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `DEFT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `ZURA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-27 | `AUPH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `ARCT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `AUTL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `CRDL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `CYPH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `CRMD` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `BMEA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `ALVO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `SUJA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `DEFT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `ZURA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-28 | `CRMD` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `BMEA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `ALVO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `SUJA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `DEFT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `ZURA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-31 | `CRMD` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `BMEA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `ALVO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `SUJA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `DEFT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `ZURA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `SIMO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `TTMI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `KEYS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `AVT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `CGNX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `COHR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `LSCC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-01 | `SIMO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `SMTC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `TTMI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `KEYS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `AVT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `CGNX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `COHR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `LSCC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-02 | `SIMO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `SMTC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `TTMI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `KEYS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `AVT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `CGNX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `COHR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `LSCC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-03 | `SIMO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `SMTC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `TTMI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `KEYS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `AVT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `CGNX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `COHR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `LSCC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `RVTY` | cash | leftover split 75.53 < 1 share @ 132.45 |
| 2026-09-04 | `HRMY` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `VSTM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `ARCT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `SLN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `CRDL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `ATRC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `HRMY` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `CABA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `VSTM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `ARCT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `SLN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `CRDL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `ALEC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `BHC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `BMEA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `OABI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `OPK` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `VIR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-09 | `ATRC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `HRMY` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `CABA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `VSTM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `ARCT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `SLN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `CRDL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `ALEC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `BHC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `BMEA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `OABI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `OPK` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `VIR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-10 | `ATRC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `HRMY` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `CABA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `VSTM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `ARCT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `SLN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `CRDL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `ALEC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `BHC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `BMEA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `OABI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `OPK` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `VIR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-11 | `ALEC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `BHC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `BMEA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `OABI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `OPK` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `VIR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `ORCL` | cash | leftover split 82.65 < 1 share @ 164.43 |
| 2026-09-14 | `BAND` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `PAGS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `ZSQR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `PAYP` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-15 | `BAND` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `PAGS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `ZSQR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `PAYP` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-16 | `BAND` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `PAGS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `ZSQR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `PAYP` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-17 | `BAND` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `PAGS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `ZSQR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `PAYP` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `SWKS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `QRVO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-18 | `SWKS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `QRVO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `BULL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-18 | `RBRK` | cash | leftover split 32.47 < 1 share @ 108.55 |
| 2026-09-18 | `ECO` | cash | leftover split 32.47 < 1 share @ 85.00 |
| 2026-09-18 | `FIVN` | cash | leftover split 32.47 < 1 share @ 34.44 |
| 2026-09-18 | `TEM` | cash | leftover split 32.47 < 1 share @ 81.40 |
| 2026-09-21 | `SWKS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `QRVO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `BULL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-21 | `RARE` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `SDGR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `CYPH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `RXT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `SWKS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `QRVO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `BULL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-22 | `RARE` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `SDGR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `CYPH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `RXT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-23 | `BULL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-23 | `RARE` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `SDGR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `CYPH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `RXT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `RARE` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `SDGR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `CYPH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `RXT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `DXCM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `A` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `HALO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `ARQT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `PGEN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `ADMA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `FTRE` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `OMER` | min_hold | dropped but min-hold 1/5 sess — no sell |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `RARE` | 2 | 2026-09-18 @ $14.79 | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list probable,yday_gainer; 🔵; ⚪; ret5=+0.7; leftover $32.47 |
| `SDGR` | 1 | 2026-09-18 @ $29.32 | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+60.9; leftover $32.47 |
| `CYPH` | 10 | 2026-09-18 @ $3.04 | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+39.5; leftover $32.47 |
| `RXT` | 8 | 2026-09-18 @ $3.94 | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer; 🔵; ⚪; ret5=+25.2; leftover $32.47 |
| `DXCM` | 13 | 2026-09-23 @ $89.50 | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+5.3; leftover $1243.87 |
| `A` | 7 | 2026-09-23 @ $166.54 | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+10.3; leftover $1243.87 |
| `HALO` | 10 | 2026-09-23 @ $116.85 | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+3.3; leftover $1243.87 |
| `ARQT` | 44 | 2026-09-23 @ $27.79 | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+7.0; leftover $1243.87 |
| `PGEN` | 156 | 2026-09-23 @ $7.95 | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+12.4; leftover $1243.87 |
| `ADMA` | 126 | 2026-09-23 @ $9.81 | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+4.0; leftover $1243.87 |
| `FTRE` | 61 | 2026-09-23 @ $20.25 | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+15.0; leftover $1243.87 |
| `OMER` | 60 | 2026-09-23 @ $20.65 | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $1243.87 |
