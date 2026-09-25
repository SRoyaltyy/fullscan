# Factor mine action — `union_white_any_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · −0 red + (yday up or major catalyst), then Score

Cash book **-19.63%** ($8,037) · signal-only (no cash/fees) was -4.29%. Starts YES **2/30**. Fills 137 · skips 149 · realized $-1383.61.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- Minimum hold is 3 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 3 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `cam_bad_max=0,yday_or_catalyst=True` · **rank** `list` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $128.68.

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
| 2026-08-18 09:30 ET | **SELL** | `BTSG` | 27 | $60.00 | $2.09 | $+1.24 | $1,658.82 | ▲ +1.24 after sell → book $10,649.70; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `IREN` | 36 | $43.56 | $2.12 | $-91.34 | $3,224.86 | ▼ -91.34 after sell → book $10,647.58; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 32 | $51.77 | $2.11 | $+32.50 | $4,879.39 | ▲ +32.50 after sell → book $10,645.47; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `SLS` | 142 | $12.66 | $2.45 | $+131.45 | $6,674.66 | ▲ +131.45 after sell → book $10,643.02; vs 09:30 mark -2.45 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 2057 | $1.14 | $26.90 | $+629.08 | $8,992.74 | ▲ +629.08 after sell → book $10,616.12; vs 09:30 mark -26.90 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TNDM` | 71 | $22.16 | $2.23 | $-87.50 | $10,563.87 | ▼ -87.50 after sell → book $10,613.89; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,563.87 | ▼ close $10,612.99 vs 09:30 $10,651.79 (session -0.90) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,563.87 | ▲ 09:30 equity $10,613.17 vs yday $10,612.99 (+0.18) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `LDI` | 12 | $0.88 | $0.16 | $-0.99 | $10,574.27 | ▼ -0.99 after sell → book $10,613.01; vs 09:30 mark -0.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 7 | $1.42 | $0.14 | $-0.83 | $10,584.07 | ▼ -0.83 after sell → book $10,612.87; vs 09:30 mark -0.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ANGX` | 2 | $4.79 | $0.12 | $+0.75 | $10,593.53 | ▲ +0.75 after sell → book $10,612.75; vs 09:30 mark -0.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `HYLN` | 2 | $3.87 | $0.10 | $-0.81 | $10,601.17 | ▼ -0.81 after sell → book $10,612.65; vs 09:30 mark -0.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,601.17 | ▼ close $10,612.62 vs 09:30 $10,613.17 (session -0.03) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,601.17 | ▼ 09:30 equity $10,612.49 vs yday $10,612.62 (-0.13) | — | — |
| 2026-08-20 09:30 ET | **SELL** | `DNN` | 2 | $3.20 | $0.09 | $-0.24 | $10,607.48 | ▼ -0.24 after sell → book $10,612.40; vs 09:30 mark -0.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `NPWR` | 3 | $1.64 | $0.08 | $-0.98 | $10,612.32 | ▼ -0.98 after sell → book $10,612.32; vs 09:30 mark -0.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 64 | $20.55 | $2.18 | — | $9,294.94 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1326.54 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 14 | $91.01 | $2.03 | — | $8,018.76 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1326.54 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 64 | $20.65 | $2.18 | — | $6,694.98 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1326.54 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 229 | $5.77 | $2.95 | — | $5,370.70 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1326.54 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 67 | $19.63 | $2.19 | — | $4,053.30 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1326.54 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 44 | $29.63 | $2.12 | — | $2,747.45 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1326.54 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 758 | $1.75 | $9.78 | — | $1,411.18 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1326.54 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 9 | $144.54 | $2.02 | — | $108.30 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1326.54 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $108.30 | ▲ close $10,837.02 vs 09:30 $10,612.49 (session +250.16) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $108.30 | ▲ 09:30 equity $11,123.40 vs yday $10,837.02 (+286.38) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 1 | $11.13 | $0.11 | — | $97.05 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $13.54 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 5 | $2.47 | $0.14 | — | $84.57 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $13.54 | — |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 7 | $1.93 | $0.16 | — | $70.90 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $13.54 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 10 | $1.32 | $0.16 | — | $57.54 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $13.54 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $57.54 | ▼ close $11,122.35 vs 09:30 $11,123.40 (session -0.48) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $57.54 | ▲ 09:30 equity $11,234.68 vs yday $11,122.35 (+112.33) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $57.54 | ▼ close $11,200.37 vs 09:30 $11,234.68 (session -34.31) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $57.54 | ▼ 09:30 equity $11,023.30 vs yday $11,200.37 (-177.07) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 64 | $20.32 | $2.20 | $-19.11 | $1,355.82 | ▼ -19.11 after sell → book $11,021.10; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 14 | $95.86 | $2.05 | $+63.82 | $2,695.80 | ▲ +63.82 after sell → book $11,019.04; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CDE` | 64 | $20.47 | $2.20 | $-15.91 | $4,003.68 | ▼ -15.91 after sell → book $11,016.84; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 229 | $5.53 | $3.00 | $-60.92 | $5,267.05 | ▼ -60.92 after sell → book $11,013.84; vs 09:30 mark -3.00 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 67 | $21.21 | $2.21 | $+101.46 | $6,685.90 | ▲ +101.46 after sell → book $11,011.62; vs 09:30 mark -2.22 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 44 | $32.32 | $2.14 | $+114.09 | $8,105.84 | ▲ +114.09 after sell → book $11,009.48; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 758 | $1.90 | $9.92 | $+94.01 | $9,536.12 | ▲ +94.01 after sell → book $10,999.56; vs 09:30 mark -9.92 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `WPM` | 9 | $156.51 | $2.04 | $+103.67 | $10,942.68 | ▲ +103.67 after sell → book $10,997.53; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 65 | $23.77 | $2.19 | — | $9,395.44 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; ⚪; ret5=+13.0; leftover $1563.24 | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 187 | $8.35 | $2.55 | — | $7,831.44 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+8.0; leftover $1563.24 | — |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 959 | $1.63 | $12.37 | — | $6,255.90 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; leftover $1563.24 | — |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 298 | $5.24 | $3.84 | — | $4,690.53 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+35.3; leftover $1563.24 | — |
| 2026-08-25 09:30 ET | **BUY** | `SUJA` | 177 | $8.79 | $2.52 | — | $3,132.18 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.1; leftover $1563.24 | — |
| 2026-08-25 09:30 ET | **BUY** | `DEFT` | 2521 | $0.62 | $23.19 | — | $1,545.97 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+33.3; leftover $1563.24 | — |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 242 | $6.37 | $3.12 | — | $1.31 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer; 🔵; ⚪; ret5=+10.9; leftover $1563.24 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1.31 | ▲ close $11,105.12 vs 09:30 $11,023.30 (session +157.38) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.31 | ▼ 09:30 equity $11,101.85 vs yday $11,105.12 (-3.27) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `ARCT` | 1 | $15.35 | $0.18 | $+3.93 | $16.48 | ▲ +3.93 after sell → book $11,101.68; vs 09:30 mark -0.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `AUTL` | 5 | $2.41 | $0.16 | $-0.59 | $28.38 | ▼ -0.59 after sell → book $11,101.52; vs 09:30 mark -0.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `CRDL` | 7 | $2.03 | $0.18 | $+0.36 | $42.40 | ▲ +0.36 after sell → book $11,101.34; vs 09:30 mark -0.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `CYPH` | 10 | $1.60 | $0.21 | $+2.43 | $58.19 | ▲ +2.43 after sell → book $11,101.13; vs 09:30 mark -0.21 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $58.19 | ▼ close $10,905.92 vs 09:30 $11,101.85 (session -195.20) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $58.19 | ▲ 09:30 equity $10,953.65 vs yday $10,905.92 (+47.73) | — | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $58.19 | ▼ close $10,871.79 vs 09:30 $10,953.65 (session -81.85) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $58.19 | ▼ 09:30 equity $10,857.29 vs yday $10,871.79 (-14.50) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `MOS` | 65 | $23.95 | $2.21 | $+7.31 | $1,612.74 | ▲ +7.31 after sell → book $10,855.08; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRMD` | 187 | $8.28 | $2.59 | $-18.24 | $3,158.50 | ▼ -18.24 after sell → book $10,852.49; vs 09:30 mark -2.59 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BMEA` | 959 | $1.69 | $12.54 | $+32.63 | $4,766.67 | ▲ +32.63 after sell → book $10,839.94; vs 09:30 mark -12.55 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ALVO` | 298 | $4.84 | $3.91 | $-126.95 | $6,205.08 | ▼ -126.95 after sell → book $10,836.04; vs 09:30 mark -3.90 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `SUJA` | 177 | $9.08 | $2.56 | $+46.25 | $7,809.68 | ▲ +46.25 after sell → book $10,833.47; vs 09:30 mark -2.57 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `DEFT` | 2521 | $0.64 | $24.00 | $-9.38 | $9,386.51 | ▼ -9.38 after sell → book $10,809.47; vs 09:30 mark -24.00 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ZURA` | 242 | $5.88 | $3.17 | $-124.88 | $10,806.30 | ▼ -124.88 after sell → book $10,806.30; vs 09:30 mark -3.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `SIMO` | 5 | $252.24 | $2.00 | — | $9,543.09 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list probable,yday_gainer; ⚪; ret5=+2.2; leftover $1350.79 | — |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 9 | $141.76 | $2.02 | — | $8,265.24 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+14.1; leftover $1350.79 | — |
| 2026-08-28 09:30 ET | **BUY** | `TTMI` | 10 | $122.81 | $2.02 | — | $7,035.12 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+9.0; leftover $1350.79 | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 4 | $324.41 | $2.00 | — | $5,735.47 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list mover_buy; 🔵; ⚪; ret5=+2.9; leftover $1350.79 | — |
| 2026-08-28 09:30 ET | **BUY** | `AVT` | 14 | $91.49 | $2.03 | — | $4,452.58 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list mover_buy; 🔵; ⚪; ret5=+5.3; leftover $1350.79 | — |
| 2026-08-28 09:30 ET | **BUY** | `CGNX` | 21 | $62.82 | $2.05 | — | $3,131.31 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list mover_buy; 🔵; ⚪; ret5=+5.4; leftover $1350.79 | — |
| 2026-08-28 09:30 ET | **BUY** | `COHR` | 4 | $289.44 | $2.00 | — | $1,971.55 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list mover_buy; 🔵; ⚪; ret5=+1.8; leftover $1350.79 | — |
| 2026-08-28 09:30 ET | **BUY** | `LSCC` | 11 | $119.76 | $2.02 | — | $652.16 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list mover_buy; 🔵; ⚪; ret5=+2.5; leftover $1350.79 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $652.16 | ▼ close $10,413.80 vs 09:30 $10,857.29 (session -376.34) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $652.16 | ▲ 09:30 equity $10,469.65 vs yday $10,413.80 (+55.85) | — | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $652.16 | ▲ close $10,477.06 vs 09:30 $10,469.65 (session +7.41) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $652.16 | ▼ 09:30 equity $10,279.80 vs yday $10,477.06 (-197.26) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $652.16 | ▲ close $10,289.46 vs 09:30 $10,279.80 (session +9.66) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $652.16 | ▼ 09:30 equity $10,266.77 vs yday $10,289.46 (-22.69) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `SIMO` | 5 | $235.71 | $2.02 | $-86.68 | $1,828.69 | ▼ -86.68 after sell → book $10,264.75; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SMTC` | 9 | $133.00 | $2.04 | $-82.89 | $3,023.65 | ▼ -82.89 after sell → book $10,262.71; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `TTMI` | 10 | $114.22 | $2.04 | $-89.96 | $4,163.81 | ▼ -89.96 after sell → book $10,260.67; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `KEYS` | 4 | $318.04 | $2.02 | $-29.50 | $5,433.95 | ▼ -29.50 after sell → book $10,258.65; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `AVT` | 14 | $89.39 | $2.05 | $-33.48 | $6,683.36 | ▼ -33.48 after sell → book $10,256.60; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CGNX` | 21 | $60.37 | $2.07 | $-55.58 | $7,949.06 | ▼ -55.58 after sell → book $10,254.53; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `COHR` | 4 | $268.12 | $2.02 | $-89.30 | $9,019.51 | ▼ -89.30 after sell → book $10,252.50; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `LSCC` | 11 | $112.09 | $2.04 | $-88.44 | $10,250.46 | ▼ -88.44 after sell → book $10,250.46; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,250.46 | ▲ close $10,250.46 vs 09:30 $10,266.77 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,250.46 | ▲ 09:30 equity $10,250.46 vs yday $10,250.46 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 24 | $52.88 | $2.06 | — | $8,979.28 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1281.31 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 29 | $42.93 | $2.08 | — | $7,732.23 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1281.31 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 352 | $3.63 | $4.54 | — | $6,449.93 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1281.31 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 159 | $8.03 | $2.47 | — | $5,170.69 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1281.31 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $132.45 | $2.02 | — | $3,976.63 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1281.31 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 76 | $16.77 | $2.22 | — | $2,699.89 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1281.31 | — |
| 2026-09-03 09:30 ET | **BUY** | `SLN` | 86 | $14.85 | $2.25 | — | $1,420.54 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list probable,yday_gainer; 🔵; ⚪; ret5=+9.3; leftover $1281.31 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 587 | $2.18 | $7.57 | — | $133.31 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $1281.31 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $133.31 | ▼ close $9,998.16 vs 09:30 $10,250.46 (session -227.10) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $133.31 | ▼ 09:30 equity $9,943.87 vs yday $9,998.16 (-54.29) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 8 | $2.52 | $0.23 | — | $112.92 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $22.22 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 3 | $6.71 | $0.21 | — | $92.58 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $22.22 | — |
| 2026-09-04 09:30 ET | **BUY** | `BMEA` | 11 | $1.90 | $0.24 | — | $71.44 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+13.7; leftover $22.22 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 4 | $4.78 | $0.20 | — | $52.12 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $22.22 | — |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 13 | $1.59 | $0.25 | — | $31.20 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $22.22 | — |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 1 | $11.31 | $0.12 | — | $19.78 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $22.22 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19.78 | ▲ close $10,038.90 vs 09:30 $9,943.87 (session +96.27) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19.78 | ▼ 09:30 equity $10,016.80 vs yday $10,038.90 (-22.10) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19.78 | ▼ close $9,886.26 vs 09:30 $10,016.80 (session -130.54) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19.78 | ▼ 09:30 equity $9,830.35 vs yday $9,886.26 (-55.91) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `ATRC` | 24 | $53.16 | $2.08 | $+2.58 | $1,293.53 | ▲ +2.58 after sell → book $9,828.26; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `HRMY` | 29 | $42.01 | $2.10 | $-30.85 | $2,509.73 | ▼ -30.85 after sell → book $9,826.17; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CABA` | 352 | $3.28 | $4.61 | $-132.35 | $3,659.68 | ▼ -132.35 after sell → book $9,821.56; vs 09:30 mark -4.61 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `VSTM` | 159 | $8.01 | $2.50 | $-8.15 | $4,930.76 | ▼ -8.15 after sell → book $9,819.05; vs 09:30 mark -2.51 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `RVTY` | 9 | $125.77 | $2.04 | $-64.17 | $6,060.66 | ▼ -64.17 after sell → book $9,817.02; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `ARCT` | 76 | $15.46 | $2.24 | $-104.02 | $7,233.38 | ▼ -104.02 after sell → book $9,814.78; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `SLN` | 86 | $13.60 | $2.27 | $-112.02 | $8,400.70 | ▼ -112.02 after sell → book $9,812.50; vs 09:30 mark -2.28 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CRDL` | 587 | $2.22 | $7.68 | $+8.23 | $9,696.16 | ▲ +8.23 after sell → book $9,804.82; vs 09:30 mark -7.68 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,696.16 | ▼ close $9,799.99 vs 09:30 $9,830.35 (session -4.83) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,696.16 | ▼ 09:30 equity $9,798.53 vs yday $9,799.99 (-1.46) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `ALEC` | 8 | $2.22 | $0.22 | $-2.85 | $9,713.70 | ▼ -2.85 after sell → book $9,798.31; vs 09:30 mark -0.22 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BHC` | 3 | $6.11 | $0.21 | $-2.22 | $9,731.82 | ▼ -2.22 after sell → book $9,798.10; vs 09:30 mark -0.21 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BMEA` | 11 | $1.83 | $0.25 | $-1.27 | $9,751.69 | ▼ -1.27 after sell → book $9,797.84; vs 09:30 mark -0.26 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `OABI` | 4 | $3.92 | $0.19 | $-3.82 | $9,767.19 | ▼ -3.82 after sell → book $9,797.65; vs 09:30 mark -0.19 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `OPK` | 13 | $1.53 | $0.26 | $-1.28 | $9,786.83 | ▼ -1.28 after sell → book $9,797.40; vs 09:30 mark -0.25 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `VIR` | 1 | $10.57 | $0.13 | $-0.98 | $9,797.27 | ▼ -0.98 after sell → book $9,797.27; vs 09:30 mark -0.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,797.27 | ▲ close $9,797.27 vs 09:30 $9,798.53 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,797.27 | ▲ 09:30 equity $9,797.27 vs yday $9,797.27 (-0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `ORCL` | 11 | $164.43 | $2.02 | — | $7,986.51 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,earn_react; ⚪; ret5=+4.9; leftover $1959.45 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAND` | 37 | $52.55 | $2.10 | — | $6,040.06 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list probable,yday_gainer; 🔵; ⚪; ret5=+8.6; leftover $1959.45 | — |
| 2026-09-11 09:30 ET | **BUY** | `PAGS` | 193 | $10.11 | $2.57 | — | $4,086.26 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer; 🔵; ⚪; ret5=+2.9; leftover $1959.45 | — |
| 2026-09-11 09:30 ET | **BUY** | `ZSQR` | 602 | $3.25 | $7.77 | — | $2,122.00 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer; 🔵; ⚪; ret5=+3.6; leftover $1959.45 | — |
| 2026-09-11 09:30 ET | **BUY** | `PAYP` | 107 | $18.30 | $2.31 | — | $161.59 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1959.45 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $161.59 | ▼ close $9,694.31 vs 09:30 $9,797.27 (session -86.19) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $161.59 | ▼ 09:30 equity $9,550.59 vs yday $9,694.31 (-143.72) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $161.59 | ▼ close $9,139.93 vs 09:30 $9,550.59 (session -410.66) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $161.59 | ▼ 09:30 equity $9,109.56 vs yday $9,139.93 (-30.37) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $161.59 | ▼ close $8,712.42 vs 09:30 $9,109.56 (session -397.14) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $161.59 | ▼ 09:30 equity $8,618.18 vs yday $8,712.42 (-94.24) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `ORCL` | 11 | $140.03 | $2.05 | $-272.47 | $1,699.87 | ▼ -272.47 after sell → book $8,616.13; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `BAND` | 37 | $48.60 | $2.13 | $-150.38 | $3,495.95 | ▼ -150.38 after sell → book $8,614.01; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `PAGS` | 193 | $9.39 | $2.62 | $-144.14 | $5,305.60 | ▼ -144.14 after sell → book $8,611.39; vs 09:30 mark -2.62 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `ZSQR` | 602 | $2.34 | $7.88 | $-563.46 | $6,706.40 | ▼ -563.46 after sell → book $8,603.51; vs 09:30 mark -7.88 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `PAYP` | 107 | $17.73 | $2.34 | $-65.64 | $8,601.17 | ▼ -65.64 after sell → book $8,601.17; vs 09:30 mark -2.34 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `SWKS` | 48 | $89.38 | $2.13 | — | $4,308.80 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+19.4; leftover $4300.59 | — |
| 2026-09-16 09:30 ET | **BUY** | `QRVO` | 36 | $118.18 | $2.10 | — | $52.22 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+13.4; leftover $4300.59 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $52.22 | ▼ close $8,263.46 vs 09:30 $8,618.18 (session -333.48) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $52.22 | ▲ 09:30 equity $8,353.10 vs yday $8,263.46 (+89.64) | — | — |
| 2026-09-17 09:30 ET | **BUY** | `BULL` | 6 | $7.95 | $0.49 | — | $4.02 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_mover; 🔵; ⚪; ret5=-18.4; leftover $52.22 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.02 | ▲ close $8,736.00 vs 09:30 $8,353.10 (session +383.40) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.02 | ▲ 09:30 equity $8,816.88 vs yday $8,736.00 (+80.88) | — | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $4.02 | ▼ close $8,532.48 vs 09:30 $8,816.88 (session -284.40) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.02 | ▲ 09:30 equity $8,622.96 vs yday $8,532.48 (+90.48) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `SWKS` | 48 | $89.66 | $2.18 | $+9.13 | $4,305.53 | ▲ +9.13 after sell → book $8,620.79; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `QRVO` | 36 | $118.44 | $2.14 | $+5.12 | $8,567.22 | ▲ +5.12 after sell → book $8,618.64; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,567.22 | ▼ close $8,616.72 vs 09:30 $8,622.96 (session -1.92) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,567.22 | ▲ 09:30 equity $8,616.87 vs yday $8,616.72 (+0.15) | — | — |
| 2026-09-22 09:30 ET | **SELL** | `BULL` | 6 | $8.28 | $0.53 | $+0.92 | $8,616.34 | ▲ +0.92 after sell → book $8,616.34; vs 09:30 mark -0.53 | dropped from list after 3 sess (min 3) | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $8,616.34 | ▲ close $8,616.34 vs 09:30 $8,616.87 (session +0.00) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,616.34 | ▲ 09:30 equity $8,616.34 vs yday $8,616.34 (-0.00) | — | — |
| 2026-09-23 09:30 ET | **BUY** | `DXCM` | 12 | $89.50 | $2.03 | — | $7,540.31 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+5.3; leftover $1077.04 | — |
| 2026-09-23 09:30 ET | **BUY** | `A` | 6 | $166.54 | $2.01 | — | $6,539.06 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+10.3; leftover $1077.04 | — |
| 2026-09-23 09:30 ET | **BUY** | `HALO` | 9 | $116.85 | $2.02 | — | $5,485.40 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+3.3; leftover $1077.04 | — |
| 2026-09-23 09:30 ET | **BUY** | `ARQT` | 38 | $27.79 | $2.10 | — | $4,427.27 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+7.0; leftover $1077.04 | — |
| 2026-09-23 09:30 ET | **BUY** | `PGEN` | 135 | $7.95 | $2.40 | — | $3,351.63 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+12.4; leftover $1077.04 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 109 | $9.81 | $2.32 | — | $2,280.02 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+4.0; leftover $1077.04 | — |
| 2026-09-23 09:30 ET | **BUY** | `FTRE` | 53 | $20.25 | $2.15 | — | $1,204.62 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+15.0; leftover $1077.04 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 52 | $20.65 | $2.15 | — | $128.68 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $1077.04 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $128.68 | ▼ close $8,362.50 vs 09:30 $8,616.34 (session -236.68) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $128.68 | ▼ 09:30 equity $8,316.39 vs yday $8,362.50 (-46.11) | — | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $128.68 | ▲ close $8,435.60 vs 09:30 $8,316.39 (session +119.21) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,454.62 | ▲ 09:30 equity $8,042.60 vs yday $8,039.58 (+3.02) | 09:30 open · cash $7,454.62 (unchanged overnight, no fees) · equity $8,042.60 vs prior close $8,039.58 (+3.02) · 5 name(s) re-marked at the open (per-name table). ADMA×13 yday $9.52 → 09:30 $9.52 +0.00; ARQT×4 yday $26.27 → 09:30 $26.27 +0.00; FTRE×6 yday $20.02 → 09:30 $20.02 +0.00; HALO×1 yday $115.22 → 09:30 $115.36 +0.14; OMER×6 yday $20.13 → 09:30 $20.61 +2.88 | — |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 139 | $7.65 | $2.41 | — | $6,388.86 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+5.2; leftover $1064.95 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TXG` | 12 | $83.76 | $2.03 | — | $5,381.72 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list probable,yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1064.95 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TEM` | 12 | $83.69 | $2.03 | — | $4,375.35 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list probable,ohlc_hot; ⚪; ret5=+2.3; leftover $1064.95 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `ZSQR` | 275 | $3.86 | $3.55 | — | $3,310.30 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+46.1; leftover $1064.95 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TWST` | 5 | $184.00 | $2.00 | — | $2,388.30 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.3; leftover $1064.95 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SECZ` | 65 | $16.21 | $2.19 | — | $1,332.46 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+85.1; leftover $1064.95 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟡 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `GRAL` | 8 | $123.50 | $2.01 | — | $342.45 | — | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+56.6; leftover $1064.95 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $342.45 | ▲ close $8,037.00 vs 09:30 $8,042.60 (session +10.61) | 16:00 close · cash $342.45 · equity $8,037.00 vs 09:30 $8,042.60 (-5.60; session marks +10.61) · 12 name(s) marked open→close (per-name table). ADMA×13 09:30 $9.52 → close $9.52 +0.00; ARQT×4 09:30 $26.27 → close $26.27 +0.00; FTRE×6 09:30 $20.02 → close $20.02 +0.00; HALO×1 09:30 $115.36 → close $113.90 -1.46; OMER×6 09:30 $20.61 → close $20.08 -3.18; MRVI×139 09:30 $7.65 → close $7.60 -6.95; TXG×12 09:30 $83.76 → close $85.71 +23.40; TEM×12 09:30 $83.69 → close $85.01 +15.78; ZSQR×275 09:30 $3.86 → close $3.78 -22.00; TWST×5 09:30 $184.00 → close $182.83 -5.85; SECZ×65 09:30 $16.21 → close $15.96 -16.25; GRAL×8 09:30 $123.50 → close $126.89 +27.12 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `BTSG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `IREN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `SLS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `INO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TNDM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `DAVE` | cash | leftover split 11.56 < 1 share @ 330.91 |
| 2026-08-14 | `SLG` | cash | leftover split 11.56 < 1 share @ 57.61 |
| 2026-08-14 | `BETR` | cash | leftover split 11.56 < 1 share @ 14.80 |
| 2026-08-14 | `WDC` | cash | leftover split 11.56 < 1 share @ 503.50 |
| 2026-08-17 | `BTSG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `IREN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `SLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `INO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TNDM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `LDI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `HYLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `CDNL` | cash | leftover split 6.66 < 1 share @ 39.85 |
| 2026-08-17 | `ABX` | cash | leftover split 6.66 < 1 share @ 9.12 |
| 2026-08-17 | `OCC` | cash | leftover split 6.66 < 1 share @ 18.24 |
| 2026-08-17 | `ALM` | cash | leftover split 6.66 < 1 share @ 16.20 |
| 2026-08-17 | `UMAC` | cash | leftover split 6.66 < 1 share @ 32.55 |
| 2026-08-17 | `LPTH` | cash | leftover split 6.66 < 1 share @ 14.94 |
| 2026-08-18 | `LDI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ANGX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `HYLN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `DNN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `NPWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-19 | `DNN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `NPWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 13.54 < 1 share @ 119.43 |
| 2026-08-21 | `AUPH` | cash | leftover split 13.54 < 1 share @ 17.20 |
| 2026-08-21 | `AEM` | cash | leftover split 13.54 < 1 share @ 216.30 |
| 2026-08-21 | `CRSP` | cash | leftover split 13.54 < 1 share @ 59.72 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `MOS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `CRMD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ALVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `SUJA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `DEFT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ZURA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `MOS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CRMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ALVO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `SUJA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `DEFT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ZURA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SIMO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `TTMI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `KEYS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `AVT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CGNX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `COHR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `LSCC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-01 | `SIMO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `TTMI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `KEYS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `AVT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CGNX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `COHR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `LSCC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-04 | `HRMY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `VSTM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `SLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `ATRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `HRMY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CABA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `VSTM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `RVTY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `SLN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ALEC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `BHC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `OABI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `OPK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `VIR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-09 | `ALEC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `BHC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `OABI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `OPK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `VIR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-14 | `ORCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `BAND` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `PAGS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `ZSQR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `PAYP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-15 | `ORCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `BAND` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `PAGS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `ZSQR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `PAYP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-17 | `SWKS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `QRVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `SWKS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `QRVO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `BULL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `RBRK` | cash | leftover split 0.50 < 1 share @ 108.55 |
| 2026-09-18 | `ECO` | cash | leftover split 0.50 < 1 share @ 85.00 |
| 2026-09-18 | `FIVN` | cash | leftover split 0.50 < 1 share @ 34.44 |
| 2026-09-18 | `RARE` | cash | leftover split 0.50 < 1 share @ 14.79 |
| 2026-09-18 | `SDGR` | cash | leftover split 0.50 < 1 share @ 29.32 |
| 2026-09-18 | `CYPH` | cash | leftover split 0.50 < 1 share @ 3.04 |
| 2026-09-18 | `TEM` | cash | leftover split 0.50 < 1 share @ 81.40 |
| 2026-09-18 | `RXT` | cash | leftover split 0.50 < 1 share @ 3.94 |
| 2026-09-21 | `BULL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `DXCM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `A` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `HALO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `ARQT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `PGEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `ADMA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `FTRE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `OMER` | min_hold | dropped but min-hold 1/3 sess — no sell |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `DXCM` | 12 | 2026-09-23 @ $89.50 | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+5.3; leftover $1077.04 |
| `A` | 6 | 2026-09-23 @ $166.54 | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+10.3; leftover $1077.04 |
| `HALO` | 9 | 2026-09-23 @ $116.85 | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+3.3; leftover $1077.04 |
| `ARQT` | 38 | 2026-09-23 @ $27.79 | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+7.0; leftover $1077.04 |
| `PGEN` | 135 | 2026-09-23 @ $7.95 | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+12.4; leftover $1077.04 |
| `ADMA` | 109 | 2026-09-23 @ $9.81 | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+4.0; leftover $1077.04 |
| `FTRE` | 53 | 2026-09-23 @ $20.25 | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten; 🔵; ⚪; ret5=+15.0; leftover $1077.04 |
| `OMER` | 52 | 2026-09-23 @ $20.65 | −0 red + (yday up or major catalyst), then Score; gate cam_bad_max=0,yday_or_catalyst=True; rank list; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $1077.04 |
