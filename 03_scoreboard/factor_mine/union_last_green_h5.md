# Factor mine action — `union_last_green_h5`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **5** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ last_green hold 5, no 🚨

Cash book **-4.48%** ($9,552) · signal-only (no cash/fees) was +44.35%. Starts YES **8/30**. Fills 145 · skips 450 · realized $-144.10.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 5 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: the last finished bar was green (closed up).
- Must-not: the 🚨 alarm is on (cameras got worse overnight).

### When it buys

- At 09:30, take names on the mixed morning shopping list (every name that showed up on any 09:30 list that day) that pass the must-haves.
- If morning S ≤ −3, buy nobody new (hard-red sit).
- A name is allowed only when every must-have is true.
- A name is thrown out if any must-not is true.
- Keep the first 8 names in list order.
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
- **Gate** `last_green=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **5**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $129.01.

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
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 33 | $59.80 | $2.09 | — | $8,024.51 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten; ⚪; ret5=-5.3; leftover $2000.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 43 | $45.98 | $2.12 | — | $6,045.25 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten; ⚪; ret5=+12.3; leftover $2000.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 39 | $50.62 | $2.11 | — | $4,068.84 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten; ⚪; ret5=+6.2; leftover $2000.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 2469 | $0.81 | $27.41 | — | $2,041.54 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten; ⚪; ret5=+13.2; leftover $2000.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 85 | $23.33 | $2.25 | — | $56.25 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten; ⚪; ret5=+19.7; leftover $2000.00 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $56.25 | ▲ close $10,286.85 vs 09:30 $10,000.00 (session +322.82) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $56.25 | ▲ 09:30 equity $10,321.25 vs yday $10,286.85 (+34.40) | — | — |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 7 | $0.94 | $0.09 | — | $49.60 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten; 🔵; ⚪; ret5=+0.5; leftover $7.03 | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 4 | $1.50 | $0.07 | — | $43.53 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $7.03 | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 1 | $4.31 | $0.05 | — | $39.18 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $7.03 | — |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 1 | $4.18 | $0.04 | — | $34.95 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $7.03 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $34.95 | ▲ close $10,677.53 vs 09:30 $10,321.25 (session +356.53) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $34.95 | ▼ 09:30 equity $10,645.20 vs yday $10,677.53 (-32.33) | — | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $34.95 | ▲ close $10,729.57 vs 09:30 $10,645.20 (session +84.37) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $34.95 | ▼ 09:30 equity $10,626.31 vs yday $10,729.57 (-103.26) | — | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $34.95 | ▲ close $10,833.60 vs 09:30 $10,626.31 (session +207.29) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $34.95 | ▲ 09:30 equity $10,928.57 vs yday $10,833.60 (+94.97) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $34.95 | ▲ close $11,132.78 vs 09:30 $10,928.57 (session +204.22) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $34.95 | ▼ 09:30 equity $11,059.34 vs yday $11,132.78 (-73.44) | — | — |
| 2026-08-20 09:30 ET | **SELL** | `BTSG` | 33 | $58.64 | $2.11 | $-42.48 | $1,967.96 | ▼ -42.48 after sell → book $11,057.22; vs 09:30 mark -2.12 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `IREN` | 43 | $42.46 | $2.14 | $-155.62 | $3,791.59 | ▼ -155.62 after sell → book $11,055.08; vs 09:30 mark -2.14 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TPG` | 39 | $53.06 | $2.13 | $+90.79 | $5,858.80 | ▲ +90.79 after sell → book $11,052.95; vs 09:30 mark -2.13 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `INO` | 2469 | $1.30 | $32.29 | $+1150.12 | $9,036.21 | ▲ +1,150.12 after sell → book $11,020.66; vs 09:30 mark -32.29 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TNDM` | 85 | $23.11 | $2.27 | $-23.22 | $10,998.29 | ▼ -23.22 after sell → book $11,018.39; vs 09:30 mark -2.27 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 66 | $20.55 | $2.19 | — | $9,639.80 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1374.79 | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 66 | $20.65 | $2.19 | — | $8,274.71 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1374.79 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 238 | $5.77 | $3.07 | — | $6,898.38 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1374.79 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 70 | $19.63 | $2.20 | — | $5,522.08 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1374.79 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 46 | $29.63 | $2.13 | — | $4,156.97 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1374.79 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 785 | $1.75 | $10.13 | — | $2,773.10 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1374.79 | — |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 9 | $144.54 | $2.02 | — | $1,470.22 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1374.79 | — |
| 2026-08-20 09:30 ET | **BUY** | `ABUS` | 279 | $4.92 | $3.60 | — | $93.94 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1374.79 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $93.94 | ▲ close $11,169.22 vs 09:30 $11,059.34 (session +178.35) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $93.94 | ▲ 09:30 equity $11,554.83 vs yday $11,169.22 (+385.61) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `LDI` | 7 | $0.87 | $0.10 | $-0.68 | $99.91 | ▼ -0.68 after sell → book $11,554.73; vs 09:30 mark -0.10 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `ANGX` | 1 | $4.43 | $0.07 | $+0.01 | $104.27 | ▲ +0.01 after sell → book $11,554.66; vs 09:30 mark -0.07 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `HYLN` | 1 | $3.42 | $0.06 | $-0.86 | $107.63 | ▼ -0.86 after sell → book $11,554.60; vs 09:30 mark -0.06 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 1 | $11.13 | $0.11 | — | $96.39 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $15.38 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 11 | $1.32 | $0.18 | — | $81.69 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $15.38 | — |
| 2026-08-21 09:30 ET | **BUY** | `QDEL` | 1 | $14.96 | $0.15 | — | $66.58 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer; ret5=-1.6; leftover $15.38 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $66.58 | ▼ close $11,537.18 vs 09:30 $11,554.83 (session -16.98) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $66.58 | ▲ 09:30 equity $11,640.96 vs yday $11,537.18 (+103.78) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `BTBT` | 4 | $1.55 | $0.09 | $+0.03 | $72.69 | ▲ +0.03 after sell → book $11,640.87; vs 09:30 mark -0.09 | dropped from list after 6 sess (min 5) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $72.69 | ▼ close $11,613.71 vs 09:30 $11,640.96 (session -27.16) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $72.69 | ▼ 09:30 equity $11,463.00 vs yday $11,613.71 (-150.71) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `SAFX` | 25 | $0.36 | $0.16 | — | $63.57 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer; ret5=-15.6; leftover $9.09 | — |
| 2026-08-25 09:30 ET | **BUY** | `ZIP` | 1 | $4.55 | $0.05 | — | $58.97 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ret5=-6.6; leftover $9.09 | — |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 5 | $1.63 | $0.10 | — | $50.73 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+17.8; leftover $9.09 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $50.73 | ▲ close $11,867.41 vs 09:30 $11,463.00 (session +404.72) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $50.73 | ▼ 09:30 equity $11,674.22 vs yday $11,867.41 (-193.19) | — | — |
| 2026-08-26 09:30 ET | **BUY** | `RZLT` | 1 | $5.01 | $0.05 | — | $45.66 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten,yday_gainer; 🔵; ret5=+7.5; leftover $6.34 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $45.66 | ▼ close $11,519.49 vs 09:30 $11,674.22 (session -154.66) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $45.66 | ▲ 09:30 equity $11,549.52 vs yday $11,519.49 (+30.03) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `AG` | 66 | $20.93 | $2.21 | $+20.68 | $1,424.83 | ▲ +20.68 after sell → book $11,547.31; vs 09:30 mark -2.21 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `CDE` | 66 | $21.31 | $2.21 | $+39.16 | $2,829.08 | ▲ +39.16 after sell → book $11,545.10; vs 09:30 mark -2.21 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `HDSN` | 238 | $5.49 | $3.12 | $-72.83 | $4,132.58 | ▼ -72.83 after sell → book $11,541.98; vs 09:30 mark -3.12 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `IAG` | 70 | $21.47 | $2.22 | $+124.38 | $5,633.26 | ▲ +124.38 after sell → book $11,539.76; vs 09:30 mark -2.22 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `KGC` | 46 | $32.32 | $2.15 | $+119.46 | $7,117.83 | ▲ +119.46 after sell → book $11,537.61; vs 09:30 mark -2.15 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `NFGC` | 785 | $1.91 | $10.27 | $+105.20 | $8,606.91 | ▲ +105.20 after sell → book $11,527.34; vs 09:30 mark -10.27 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `WPM` | 9 | $155.89 | $2.04 | $+98.09 | $10,007.88 | ▲ +98.09 after sell → book $11,525.30; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `ABUS` | 279 | $5.16 | $3.66 | $+59.70 | $11,443.87 | ▲ +59.70 after sell → book $11,521.65; vs 09:30 mark -3.65 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 34 | $41.44 | $2.09 | — | $10,032.81 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten; ret5=+3.1; leftover $1430.48 | — |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 99 | $14.42 | $2.29 | — | $8,602.95 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten; ret5=+7.1; leftover $1430.48 | — |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 550 | $2.60 | $7.09 | — | $7,165.85 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten,ohlc_hot; ret5=+13.0; leftover $1430.48 | — |
| 2026-08-27 09:30 ET | **BUY** | `ITG` | 115 | $12.36 | $2.33 | — | $5,742.12 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; ret5=-3.0; leftover $1430.48 | — |
| 2026-08-27 09:30 ET | **BUY** | `BE` | 6 | $227.10 | $2.01 | — | $4,377.51 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+5.6; leftover $1430.48 | — |
| 2026-08-27 09:30 ET | **BUY** | `INDP` | 1265 | $1.13 | $16.32 | — | $2,931.74 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list yday_gainer,yday_mover; ret5=+21.3; leftover $1430.48 | — |
| 2026-08-27 09:30 ET | **BUY** | `CAPR` | 155 | $9.19 | $2.46 | — | $1,504.83 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list yday_gainer,yday_mover,ohlc_hot; ret5=+17.3; leftover $1430.48 | — |
| 2026-08-27 09:30 ET | **BUY** | `BZ` | 77 | $18.50 | $2.22 | — | $78.11 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list yday_gainer,yday_mover; ret5=+17.2; leftover $1430.48 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $78.11 | ▲ close $11,672.16 vs 09:30 $11,549.52 (session +187.32) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $78.11 | ▼ 09:30 equity $11,635.10 vs yday $11,672.16 (-37.06) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `ARCT` | 1 | $15.43 | $0.18 | $+4.01 | $93.37 | ▲ +4.01 after sell → book $11,634.92; vs 09:30 mark -0.18 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CYPH` | 11 | $1.82 | $0.25 | $+5.07 | $113.13 | ▲ +5.07 after sell → book $11,634.67; vs 09:30 mark -0.25 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `QDEL` | 1 | $15.09 | $0.17 | $-0.20 | $128.05 | ▼ -0.20 after sell → book $11,634.49; vs 09:30 mark -0.18 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **BUY** | `OPTX` | 3 | $8.61 | $0.27 | — | $101.95 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer; ret5=-0.7; leftover $32.01 | — |
| 2026-08-28 09:30 ET | **BUY** | `VYX` | 3 | $9.13 | $0.28 | — | $74.28 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list yday_gainer; 🔵; ret5=+20.0; leftover $32.01 | — |
| 2026-08-28 09:30 ET | **BUY** | `EQ` | 13 | $2.46 | $0.36 | — | $41.94 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list yday_gainer; ret5=+7.9; leftover $32.01 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $41.94 | ▼ close $11,356.70 vs 09:30 $11,635.10 (session -276.89) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $41.94 | ▲ 09:30 equity $11,382.98 vs yday $11,356.70 (+26.28) | — | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $41.94 | ▲ close $11,407.91 vs 09:30 $11,382.98 (session +24.93) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $41.94 | ▲ 09:30 equity $11,613.84 vs yday $11,407.91 (+205.93) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `SAFX` | 25 | $0.36 | $0.19 | $-0.18 | $50.88 | ▼ -0.18 after sell → book $11,613.65; vs 09:30 mark -0.19 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `ZIP` | 1 | $4.14 | $0.06 | $-0.52 | $54.95 | ▼ -0.52 after sell → book $11,613.58; vs 09:30 mark -0.07 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `BMEA` | 5 | $1.68 | $0.12 | $+0.03 | $63.24 | ▲ +0.03 after sell → book $11,613.47; vs 09:30 mark -0.11 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $63.24 | ▼ close $11,534.59 vs 09:30 $11,613.84 (session -78.88) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $63.24 | ▲ 09:30 equity $11,544.03 vs yday $11,534.59 (+9.44) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `RZLT` | 1 | $4.50 | $0.07 | $-0.63 | $67.67 | ▼ -0.63 after sell → book $11,543.96; vs 09:30 mark -0.07 | dropped from list after 5 sess (min 5) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $67.67 | ▲ close $11,584.71 vs 09:30 $11,544.03 (session +40.75) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $67.67 | ▲ 09:30 equity $11,617.26 vs yday $11,584.71 (+32.55) | — | — |
| 2026-09-03 09:30 ET | **SELL** | `RRC` | 34 | $42.43 | $2.11 | $+29.45 | $1,508.17 | ▲ +29.45 after sell → book $11,615.14; vs 09:30 mark -2.12 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `CRK` | 99 | $15.45 | $2.32 | $+97.37 | $3,035.41 | ▲ +97.37 after sell → book $11,612.83; vs 09:30 mark -2.31 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `SLI` | 550 | $2.49 | $7.20 | $-74.79 | $4,397.71 | ▼ -74.79 after sell → book $11,605.63; vs 09:30 mark -7.20 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `ITG` | 115 | $12.96 | $2.37 | $+64.30 | $5,885.75 | ▲ +64.30 after sell → book $11,603.27; vs 09:30 mark -2.36 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `BE` | 6 | $219.00 | $2.03 | $-52.64 | $7,197.72 | ▼ -52.64 after sell → book $11,601.24; vs 09:30 mark -2.03 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `INDP` | 1265 | $1.16 | $16.54 | $+5.09 | $8,648.58 | ▲ +5.09 after sell → book $11,584.70; vs 09:30 mark -16.54 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `CAPR` | 155 | $9.83 | $2.49 | $+94.25 | $10,169.73 | ▲ +94.25 after sell → book $11,582.20; vs 09:30 mark -2.50 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `BZ` | 77 | $17.31 | $2.24 | $-96.10 | $11,500.36 | ▼ -96.10 after sell → book $11,579.96; vs 09:30 mark -2.24 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 27 | $52.88 | $2.07 | — | $10,070.53 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1437.54 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 33 | $42.93 | $2.09 | — | $8,651.75 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1437.54 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 396 | $3.63 | $5.11 | — | $7,209.16 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1437.54 | — |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 179 | $8.03 | $2.53 | — | $5,769.26 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1437.54 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $132.45 | $2.02 | — | $4,442.74 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1437.54 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 85 | $16.77 | $2.25 | — | $3,015.05 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1437.54 | — |
| 2026-09-03 09:30 ET | **BUY** | `SLN` | 96 | $14.85 | $2.28 | — | $1,587.17 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=+9.3; leftover $1437.54 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 659 | $2.18 | $8.50 | — | $142.05 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $1437.54 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $142.05 | ▼ close $11,297.55 vs 09:30 $11,617.26 (session -255.57) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $142.05 | ▼ 09:30 equity $11,236.62 vs yday $11,297.55 (-60.93) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `OPTX` | 3 | $7.79 | $0.26 | $-2.99 | $165.16 | ▼ -2.99 after sell → book $11,236.36; vs 09:30 mark -0.26 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `VYX` | 3 | $8.97 | $0.30 | $-1.06 | $191.77 | ▼ -1.06 after sell → book $11,236.06; vs 09:30 mark -0.30 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `EQ` | 13 | $2.22 | $0.35 | $-3.83 | $220.28 | ▼ -3.83 after sell → book $11,235.71; vs 09:30 mark -0.35 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **BUY** | `SLBT` | 8 | $3.15 | $0.28 | — | $194.81 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+19.3; leftover $27.54 | — |
| 2026-09-04 09:30 ET | **BUY** | `BRR` | 10 | $2.51 | $0.28 | — | $169.42 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list yday_gainer; 🔵; ⚪; ret5=+21.8; leftover $27.54 | — |
| 2026-09-04 09:30 ET | **BUY** | `FCEL` | 1 | $14.52 | $0.15 | — | $154.76 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list yday_mover; ret5=-24.1; leftover $27.54 | — |
| 2026-09-04 09:30 ET | **BUY** | `ASST` | 1 | $25.18 | $0.25 | — | $129.32 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list ohlc_hot; 🔵; ret5=+16.0; leftover $27.54 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $129.32 | ▲ close $11,345.60 vs 09:30 $11,236.62 (session +110.85) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $129.32 | ▼ 09:30 equity $11,322.42 vs yday $11,345.60 (-23.18) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $129.32 | ▼ close $11,179.33 vs 09:30 $11,322.42 (session -143.09) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $129.32 | ▼ 09:30 equity $11,118.38 vs yday $11,179.33 (-60.95) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $129.32 | ▼ close $10,898.97 vs 09:30 $11,118.38 (session -219.42) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $129.32 | ▼ 09:30 equity $10,722.45 vs yday $10,898.97 (-176.52) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $129.32 | ▼ close $10,433.93 vs 09:30 $10,722.45 (session -288.52) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $129.32 | ▲ 09:30 equity $10,539.08 vs yday $10,433.93 (+105.15) | — | — |
| 2026-09-11 09:30 ET | **SELL** | `ATRC` | 27 | $53.53 | $2.09 | $+13.39 | $1,572.54 | ▲ +13.39 after sell → book $10,536.99; vs 09:30 mark -2.09 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `HRMY` | 33 | $41.30 | $2.11 | $-57.99 | $2,933.33 | ▼ -57.99 after sell → book $10,534.88; vs 09:30 mark -2.11 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `CABA` | 396 | $2.77 | $5.18 | $-350.85 | $4,025.06 | ▼ -350.85 after sell → book $10,529.69; vs 09:30 mark -5.19 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `VSTM` | 179 | $7.70 | $2.57 | $-64.16 | $5,400.80 | ▼ -64.16 after sell → book $10,527.13; vs 09:30 mark -2.56 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `RVTY` | 10 | $122.40 | $2.04 | $-104.56 | $6,622.76 | ▼ -104.56 after sell → book $10,525.09; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `ARCT` | 85 | $14.06 | $2.27 | $-234.86 | $7,815.59 | ▼ -234.86 after sell → book $10,522.82; vs 09:30 mark -2.27 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `SLN` | 96 | $13.32 | $2.30 | $-151.46 | $9,092.00 | ▼ -151.46 after sell → book $10,520.51; vs 09:30 mark -2.31 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `CRDL` | 659 | $2.03 | $8.62 | $-115.97 | $10,421.15 | ▼ -115.97 after sell → book $10,511.89; vs 09:30 mark -8.62 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **BUY** | `SANM` | 6 | $206.84 | $2.01 | — | $9,178.10 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten; ret5=+8.3; leftover $1302.64 | — |
| 2026-09-11 09:30 ET | **BUY** | `COHU` | 23 | $56.09 | $2.06 | — | $7,885.98 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten; 🔵; ret5=+19.6; leftover $1302.64 | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 638 | $2.04 | $8.23 | — | $6,576.23 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $1302.64 | — |
| 2026-09-11 09:30 ET | **BUY** | `CLOV` | 274 | $4.75 | $3.53 | — | $5,271.19 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.5; leftover $1302.64 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 614 | $2.12 | $7.92 | — | $3,961.59 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $1302.64 | — |
| 2026-09-11 09:30 ET | **BUY** | `TYRA` | 55 | $23.63 | $2.15 | — | $2,659.79 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer; ret5=-6.3; leftover $1302.64 | — |
| 2026-09-11 09:30 ET | **BUY** | `FUBO` | 112 | $11.55 | $2.33 | — | $1,363.86 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $1302.64 | — |
| 2026-09-11 09:30 ET | **BUY** | `RDDT` | 8 | $157.55 | $2.01 | — | $101.45 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ret5=-1.7; leftover $1302.64 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $101.45 | ▼ close $10,446.83 vs 09:30 $10,539.08 (session -34.82) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $101.45 | ▼ 09:30 equity $10,344.19 vs yday $10,446.83 (-102.64) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `SLBT` | 8 | $2.02 | $0.21 | $-9.52 | $117.40 | ▼ -9.52 after sell → book $10,343.98; vs 09:30 mark -0.21 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `FCEL` | 1 | $14.88 | $0.17 | $+0.04 | $132.11 | ▲ +0.04 after sell → book $10,343.81; vs 09:30 mark -0.17 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `ASST` | 1 | $27.73 | $0.30 | $+1.99 | $159.54 | ▲ +1.99 after sell → book $10,343.51; vs 09:30 mark -0.30 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $159.54 | ▲ close $10,399.26 vs 09:30 $10,344.19 (session +55.75) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $159.54 | ▼ 09:30 equity $10,345.98 vs yday $10,399.26 (-53.28) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $159.54 | ▼ close $10,172.14 vs 09:30 $10,345.98 (session -173.84) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $159.54 | ▼ 09:30 equity $10,066.91 vs yday $10,172.14 (-105.23) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `BBNX` | 1 | $18.61 | $0.19 | — | $140.74 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; ret5=-0.5; leftover $19.94 | — |
| 2026-09-16 09:30 ET | **BUY** | `ARQQ` | 1 | $18.21 | $0.19 | — | $122.34 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer; ret5=-19.1; leftover $19.94 | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 3 | $5.87 | $0.19 | — | $104.55 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $19.94 | — |
| 2026-09-16 09:30 ET | **BUY** | `QTRX` | 7 | $2.72 | $0.21 | — | $85.30 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer; ret5=-0.4; leftover $19.94 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $85.30 | ▼ close $9,879.23 vs 09:30 $10,066.91 (session -186.91) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $85.30 | ▲ 09:30 equity $10,004.78 vs yday $9,879.23 (+125.55) | — | — |
| 2026-09-17 09:30 ET | **BUY** | `IOVA` | 1 | $10.25 | $0.11 | — | $74.94 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten,ohlc_hot; 🔵; ret5=+17.1; leftover $10.66 | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 1 | $7.59 | $0.08 | — | $67.27 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $10.66 | — |
| 2026-09-17 09:30 ET | **BUY** | `DVLT` | 62 | $0.17 | $0.29 | — | $56.44 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; ret5=-14.3; leftover $10.66 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $56.44 | ▼ close $9,854.13 vs 09:30 $10,004.78 (session -150.17) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $56.44 | ▲ 09:30 equity $9,884.97 vs yday $9,854.13 (+30.84) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `BRR` | 10 | $3.57 | $0.41 | $+9.91 | $91.73 | ▲ +9.91 after sell → book $9,884.56; vs 09:30 mark -0.41 | dropped from list after 9 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `SANM` | 6 | $197.76 | $2.03 | $-58.52 | $1,276.27 | ▼ -58.52 after sell → book $9,882.54; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `COHU` | 23 | $55.80 | $2.08 | $-10.81 | $2,557.59 | ▼ -10.81 after sell → book $9,880.46; vs 09:30 mark -2.08 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `AMTX` | 638 | $1.90 | $8.35 | $-105.90 | $3,761.44 | ▼ -105.90 after sell → book $9,872.11; vs 09:30 mark -8.35 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `CLOV` | 274 | $4.50 | $3.59 | $-75.62 | $4,990.85 | ▼ -75.62 after sell → book $9,868.52; vs 09:30 mark -3.59 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `BAK` | 614 | $1.77 | $8.03 | $-230.85 | $6,069.60 | ▼ -230.85 after sell → book $9,860.49; vs 09:30 mark -8.03 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `TYRA` | 55 | $24.58 | $2.18 | $+47.92 | $7,419.32 | ▲ +47.92 after sell → book $9,858.31; vs 09:30 mark -2.18 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `FUBO` | 112 | $9.90 | $2.35 | $-189.48 | $8,525.77 | ▼ -189.48 after sell → book $9,855.96; vs 09:30 mark -2.35 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `RDDT` | 8 | $152.69 | $2.03 | $-42.93 | $9,745.25 | ▼ -42.93 after sell → book $9,853.92; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **BUY** | `RBRK` | 11 | $108.55 | $2.02 | — | $8,549.18 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten; ⚪; ret5=+21.3; leftover $1218.16 | — |
| 2026-09-18 09:30 ET | **BUY** | `VICR` | 5 | $219.62 | $2.00 | — | $7,449.08 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten,yday_gainer,yday_mover; 🔵; ret5=+21.5; leftover $1218.16 | — |
| 2026-09-18 09:30 ET | **BUY** | `ECO` | 14 | $85.00 | $2.03 | — | $6,257.04 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten; 🔵; ⚪; ret5=+18.3; leftover $1218.16 | — |
| 2026-09-18 09:30 ET | **BUY** | `TLSA` | 1255 | $0.97 | $15.94 | — | $5,023.75 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+1.0; leftover $1218.16 | — |
| 2026-09-18 09:30 ET | **BUY** | `EYPT` | 308 | $3.95 | $3.97 | — | $3,803.18 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.1; leftover $1218.16 | — |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 86 | $14.07 | $2.25 | — | $2,590.91 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $1218.16 | — |
| 2026-09-18 09:30 ET | **BUY** | `BNC` | 208 | $5.83 | $2.68 | — | $1,375.59 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $1218.16 | — |
| 2026-09-18 09:30 ET | **BUY** | `DDD` | 340 | $3.58 | $4.39 | — | $154.00 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ret5=+8.3; leftover $1218.16 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $154.00 | ▼ close $9,715.68 vs 09:30 $9,884.97 (session -102.95) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $154.00 | ▲ 09:30 equity $9,920.54 vs yday $9,715.68 (+204.86) | — | — |
| 2026-09-21 09:30 ET | **BUY** | `BKKT` | 2 | $9.31 | $0.19 | — | $135.19 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; ret5=+5.3; leftover $19.25 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTDR` | 1 | $13.47 | $0.14 | — | $121.58 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+8.4; leftover $19.25 | — |
| 2026-09-21 09:30 ET | **BUY** | `ORBS` | 17 | $1.11 | $0.24 | — | $102.47 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; ret5=+6.9; leftover $19.25 | — |
| 2026-09-21 09:30 ET | **BUY** | `SBET` | 1 | $9.99 | $0.10 | — | $92.38 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ret5=+5.3; leftover $19.25 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTBT` | 10 | $1.82 | $0.21 | — | $73.91 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ret5=+8.8; leftover $19.25 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $73.91 | ▲ close $9,921.59 vs 09:30 $9,920.54 (session +1.94) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $73.91 | ▼ 09:30 equity $9,864.83 vs yday $9,921.59 (-56.76) | — | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $73.91 | ▲ close $9,877.63 vs 09:30 $9,864.83 (session +12.80) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $73.91 | ▲ 09:30 equity $10,107.70 vs yday $9,877.63 (+230.07) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `BBNX` | 1 | $23.00 | $0.25 | $+3.95 | $96.66 | ▲ +3.95 after sell → book $10,107.45; vs 09:30 mark -0.25 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `ARQQ` | 1 | $23.30 | $0.26 | $+4.65 | $119.71 | ▲ +4.65 after sell → book $10,107.20; vs 09:30 mark -0.25 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `RIG` | 3 | $5.53 | $0.19 | $-1.40 | $136.10 | ▼ -1.40 after sell → book $10,107.00; vs 09:30 mark -0.20 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `QTRX` | 7 | $3.17 | $0.26 | $+2.68 | $158.03 | ▲ +2.68 after sell → book $10,106.74; vs 09:30 mark -0.26 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 2 | $9.81 | $0.20 | — | $138.21 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten; 🔵; ⚪; ret5=+4.0; leftover $19.75 | — |
| 2026-09-23 09:30 ET | **BUY** | `INDP` | 5 | $3.93 | $0.21 | — | $118.34 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $19.75 | — |
| 2026-09-23 09:30 ET | **BUY** | `SGRY` | 1 | $15.72 | $0.16 | — | $102.46 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $19.75 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $102.46 | ▲ close $10,140.56 vs 09:30 $10,107.70 (session +34.40) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $102.46 | ▼ 09:30 equity $10,059.52 vs yday $10,140.56 (-81.04) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `IOVA` | 1 | $10.39 | $0.13 | $-0.09 | $112.73 | ▼ -0.09 after sell → book $10,059.40; vs 09:30 mark -0.12 | dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 1 | $7.38 | $0.10 | $-0.39 | $120.01 | ▼ -0.39 after sell → book $10,059.30; vs 09:30 mark -0.10 | dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `DVLT` | 62 | $0.15 | $0.30 | $-1.83 | $129.01 | ▼ -1.83 after sell → book $10,059.00; vs 09:30 mark -0.30 | dropped from list after 5 sess (min 5) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $129.01 | ▼ close $10,017.12 vs 09:30 $10,059.52 (session -41.88) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $209.29 | ▲ 09:30 equity $9,598.88 vs yday $9,572.76 (+26.12) | 09:30 open · cash $209.29 (unchanged overnight, no fees) · equity $9,598.88 vs prior close $9,572.76 (+26.12) · 14 name(s) re-marked at the open (per-name table). A×8 yday $172.84 → 09:30 $171.98 -6.88; ADMA×139 yday $9.52 → 09:30 $9.52 +0.00; ARQT×49 yday $26.27 → 09:30 $26.27 +0.00; BHVN×1 yday $13.19 → 09:30 $13.19 +0.00; BNC×4 yday $6.26 → 09:30 $6.26 +0.00; CYPH×3 yday $4.08 → 09:30 $4.00 -0.22; DDD×6 yday $3.43 → 09:30 $3.43 +0.00; DXCM×15 yday $87.47 → 09:30 $87.47 +0.00; EYPT×5 yday $3.65 → 09:30 $3.65 +0.00; FTRE×67 yday $20.02 → 09:30 $20.02 +0.00; HALO×11 yday $115.22 → 09:30 $115.36 +1.54; OMER×66 yday $20.13 → 09:30 $20.61 +31.68; ORBS×11 yday $1.03 → 09:30 $1.03 +0.00; RANI×27 yday $0.75 → 09:30 $0.75 +0.00 | — |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 3 | $7.65 | $0.24 | — | $186.10 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten; 🔵; ⚪; ret5=+5.2; leftover $29.90 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `WRBY` | 1 | $26.27 | $0.27 | — | $159.57 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $29.90 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `AEHL` | 3 | $9.05 | $0.28 | — | $132.14 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer; ret5=-27.1; leftover $29.90 | join🔴 sector🟡 gen🟢 news🔴 digest🟢 ab🔴 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `BRVE` | 1 | $23.58 | $0.24 | — | $108.32 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ret5=-15.7; leftover $29.90 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `HLP` | 13 | $2.20 | $0.33 | — | $79.39 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,ohlc_hot; 🔵; ret5=+8.9; leftover $29.90 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $79.39 | ▼ close $9,551.70 vs 09:30 $9,598.88 (session -45.82) | 16:00 close · cash $79.39 · equity $9,551.70 vs 09:30 $9,598.88 (-47.18; session marks -45.82) · 19 name(s) marked open→close (per-name table). A×8 09:30 $171.98 → close $172.79 +6.48; ADMA×139 09:30 $9.52 → close $9.52 +0.00; ARQT×49 09:30 $26.27 → close $26.27 +0.00; BHVN×1 09:30 $13.19 → close $13.19 -0.00; BNC×4 09:30 $6.26 → close $6.26 +0.00; CYPH×3 09:30 $4.00 → close $4.12 +0.35; DDD×6 09:30 $3.43 → close $3.43 +0.00; DXCM×15 09:30 $87.47 → close $87.47 +0.00; EYPT×5 09:30 $3.65 → close $3.65 +0.00; FTRE×67 09:30 $20.02 → close $20.02 +0.00; HALO×11 09:30 $115.36 → close $113.90 -16.06; OMER×66 09:30 $20.61 → close $20.08 -34.98; ORBS×11 09:30 $1.03 → close $1.03 -0.00; RANI×27 09:30 $0.75 → close $0.75 +0.00; MRVI×3 09:30 $7.65 → close $7.60 -0.15; WRBY×1 09:30 $26.27 → close $26.71 +0.44; AEHL×3 09:30 $9.05 → close $9.36 +0.93; BRVE×1 09:30 $23.58 → close $20.62 -2.96; HLP×13 09:30 $2.20 → close $2.21 +0.13 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `BTSG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `IREN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `INO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `TNDM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `VST` | cash | leftover split 7.03 < 1 share @ 146.90 |
| 2026-08-14 | `DAVE` | cash | leftover split 7.03 < 1 share @ 330.91 |
| 2026-08-14 | `SLG` | cash | leftover split 7.03 < 1 share @ 57.61 |
| 2026-08-14 | `BETR` | cash | leftover split 7.03 < 1 share @ 14.80 |
| 2026-08-17 | `BTSG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `IREN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `INO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `TNDM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `LDI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `HYLN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `DVN` | cash | leftover split 4.37 < 1 share @ 46.18 |
| 2026-08-17 | `EOG` | cash | leftover split 4.37 < 1 share @ 142.77 |
| 2026-08-17 | `FANG` | cash | leftover split 4.37 < 1 share @ 202.70 |
| 2026-08-17 | `NB` | cash | leftover split 4.37 < 1 share @ 5.07 |
| 2026-08-17 | `CDNL` | cash | leftover split 4.37 < 1 share @ 39.85 |
| 2026-08-17 | `ABX` | cash | leftover split 4.37 < 1 share @ 9.12 |
| 2026-08-17 | `VERA` | cash | leftover split 4.37 < 1 share @ 31.30 |
| 2026-08-17 | `CELC` | cash | leftover split 4.37 < 1 share @ 92.99 |
| 2026-08-18 | `BTSG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `IREN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `TPG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `INO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `TNDM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `LDI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `ANGX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `HYLN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MUR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MLYS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OBE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CYPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `BTSG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `IREN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `TPG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `INO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `TNDM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `LDI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `BTBT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `ANGX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `HYLN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `MLYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `GUTS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `RANI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SECZ` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ULTA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNDY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | `LDI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `BTBT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `ANGX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `HYLN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `ABUS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 15.38 < 1 share @ 119.43 |
| 2026-08-21 | `AUPH` | cash | leftover split 15.38 < 1 share @ 17.20 |
| 2026-08-21 | `AEM` | cash | leftover split 15.38 < 1 share @ 216.30 |
| 2026-08-21 | `DE` | cash | leftover split 15.38 < 1 share @ 623.26 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `ABUS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `ARCT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `QDEL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `AG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `CDE` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `HDSN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `IAG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `KGC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `NFGC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `WPM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `ABUS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `CYPH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `QDEL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `VITL` | cash | leftover split 9.09 < 1 share @ 11.12 |
| 2026-08-25 | `KURA` | cash | leftover split 9.09 < 1 share @ 13.59 |
| 2026-08-25 | `CCOI` | cash | leftover split 9.09 < 1 share @ 9.49 |
| 2026-08-25 | `LIFE` | cash | leftover split 9.09 < 1 share @ 36.96 |
| 2026-08-25 | `ADIG` | cash | leftover split 9.09 < 1 share @ 21.79 |
| 2026-08-26 | `AG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `CDE` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `HDSN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `IAG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `KGC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `NFGC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `WPM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `ABUS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `ARCT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `CYPH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `QDEL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `SAFX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `ZIP` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `BMEA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `HCA` | cash | leftover split 6.34 < 1 share @ 427.50 |
| 2026-08-26 | `MOS` | cash | leftover split 6.34 < 1 share @ 24.84 |
| 2026-08-26 | `CRMD` | cash | leftover split 6.34 < 1 share @ 8.60 |
| 2026-08-26 | `AVBP` | cash | leftover split 6.34 < 1 share @ 31.21 |
| 2026-08-26 | `ABX` | cash | leftover split 6.34 < 1 share @ 9.83 |
| 2026-08-26 | `ITG` | cash | leftover split 6.34 < 1 share @ 12.04 |
| 2026-08-26 | `SENS` | cash | leftover split 6.34 < 1 share @ 9.48 |
| 2026-08-27 | `ARCT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `CYPH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `QDEL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `SAFX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `ZIP` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `BMEA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `RZLT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-28 | `SAFX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `ZIP` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `BMEA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `RZLT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-28 | `ITG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-28 | `BE` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-28 | `INDP` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-28 | `BZ` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-28 | `ANF` | cash | leftover split 32.01 < 1 share @ 146.07 |
| 2026-08-31 | `SAFX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `ZIP` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `BMEA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `RZLT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-31 | `RRC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `CRK` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `ITG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `BE` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `INDP` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `CAPR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `BZ` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `OPTX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `VYX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `EQ` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RPD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TENB` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WEN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MRNA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `RZLT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-01 | `RRC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `CRK` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `SLI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `ITG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `BE` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `INDP` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `CAPR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `BZ` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `OPTX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `VYX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `EQ` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MTDR` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `RES` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `FTI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KMI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TRGP` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CTMX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `RRC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `CRK` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `SLI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `ITG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `BE` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `INDP` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `CAPR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `BZ` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `OPTX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `VYX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `EQ` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `PCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HRMY` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VSTM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MGTX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FATE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `AVXL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ZNTL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | `OPTX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `VYX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `EQ` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-04 | `ATRC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `HRMY` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `CABA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `VSTM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `ARCT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `SLN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `CRDL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `CRM` | cash | leftover split 27.54 < 1 share @ 263.36 |
| 2026-09-04 | `DELL` | cash | leftover split 27.54 < 1 share @ 513.78 |
| 2026-09-04 | `TARS` | cash | leftover split 27.54 < 1 share @ 82.70 |
| 2026-09-04 | `MDB` | cash | leftover split 27.54 < 1 share @ 378.34 |
| 2026-09-08 | `ATRC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `HRMY` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `CABA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `VSTM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `RVTY` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `ARCT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `SLN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `CRDL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `SLBT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `BRR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `FCEL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `ASST` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `SUZ` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MRLN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `EVGO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHPT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CYPH` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MSTR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SMMT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `BTBT` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `ATRC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `HRMY` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `CABA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `VSTM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `RVTY` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `ARCT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `SLN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `CRDL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `SLBT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `BRR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `FCEL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `ASST` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `PCG` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `CIG` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `UPB` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `LFMD` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `SSL` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `INDP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `WDS` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `HELP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `ATRC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `HRMY` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `CABA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `VSTM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `RVTY` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `ARCT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `SLN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `CRDL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `SLBT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `BRR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `FCEL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `ASST` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `BAND` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `DDOG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `INSP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SUNB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `TNGX` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `IRD` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SIG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-11 | `SLBT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `BRR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `FCEL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `ASST` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-14 | `BRR` | no_price | no 09:30 open — carry |
| 2026-09-14 | `SANM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `COHU` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `AMTX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `CLOV` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `BAK` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `TYRA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `FUBO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `RDDT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DK` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `NVT` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DELL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HTFL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `CAN` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `USDE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `FPS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `BRR` | no_price | no 09:30 open — carry |
| 2026-09-15 | `SANM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `COHU` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `AMTX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `CLOV` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `BAK` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `FUBO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `RDDT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `SMMT` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `WAY` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ATRC` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `TENB` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `RBRK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `QLYS` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CYPH` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-16 | `BRR` | no_price | no 09:30 open — carry |
| 2026-09-16 | `SANM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `COHU` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `AMTX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `CLOV` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `BAK` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `TYRA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `FUBO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `RDDT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `IQV` | cash | leftover split 19.94 < 1 share @ 270.89 |
| 2026-09-16 | `RDNT` | cash | leftover split 19.94 < 1 share @ 77.12 |
| 2026-09-16 | `BLFS` | cash | leftover split 19.94 < 1 share @ 36.46 |
| 2026-09-16 | `TEM` | cash | leftover split 19.94 < 1 share @ 68.79 |
| 2026-09-17 | `BRR` | no_price | no 09:30 open — carry |
| 2026-09-17 | `SANM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `COHU` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `AMTX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `CLOV` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `BAK` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `TYRA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `FUBO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `RDDT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `BBNX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `ARQQ` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `RIG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `QTRX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `ILMN` | cash | leftover split 10.66 < 1 share @ 233.85 |
| 2026-09-17 | `TWST` | cash | leftover split 10.66 < 1 share @ 151.43 |
| 2026-09-17 | `RVTY` | cash | leftover split 10.66 < 1 share @ 147.61 |
| 2026-09-17 | `AMN` | cash | leftover split 10.66 < 1 share @ 34.93 |
| 2026-09-17 | `BRUN` | cash | leftover split 10.66 < 1 share @ 15.87 |
| 2026-09-18 | `BBNX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `ARQQ` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `RIG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `QTRX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `IOVA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-18 | `PGEN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-18 | `DVLT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `BBNX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `ARQQ` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `RIG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `QTRX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `IOVA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-21 | `PGEN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-21 | `DVLT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-21 | `RBRK` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `VICR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `ECO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `TLSA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `EYPT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `BHVN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `BNC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `DDD` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `A` | cash | leftover split 19.25 < 1 share @ 157.87 |
| 2026-09-21 | `HUM` | cash | leftover split 19.25 < 1 share @ 386.20 |
| 2026-09-21 | `DXCM` | cash | leftover split 19.25 < 1 share @ 88.83 |
| 2026-09-22 | `BBNX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `ARQQ` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `RIG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `QTRX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `IOVA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-22 | `PGEN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-22 | `DVLT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-22 | `RBRK` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `VICR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `ECO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `TLSA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `EYPT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `BHVN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `BNC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `DDD` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `BKKT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `BTDR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `ORBS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `SBET` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `BTBT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `MKC` | no_price | no 09:30 open |
| 2026-09-22 | `EL` | no_price | no 09:30 open |
| 2026-09-22 | `USFD` | cash | leftover split 9.24 < 1 share @ 93.97 |
| 2026-09-22 | `TDC` | no_price | no 09:30 open |
| 2026-09-22 | `FJET` | no_price | no 09:30 open |
| 2026-09-22 | `APPS` | no_price | no 09:30 open |
| 2026-09-22 | `ALOY` | cash | leftover split 9.24 < 1 share @ 9.40 |
| 2026-09-22 | `MRAM` | no_price | no 09:30 open |
| 2026-09-23 | `IOVA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-23 | `PGEN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-23 | `DVLT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-23 | `RBRK` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `VICR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `ECO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `TLSA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `EYPT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `BHVN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `BNC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `DDD` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `BKKT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-23 | `BTDR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-23 | `ORBS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-23 | `SBET` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-23 | `BTBT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-23 | `DXCM` | cash | leftover split 19.75 < 1 share @ 89.50 |
| 2026-09-23 | `A` | cash | leftover split 19.75 < 1 share @ 166.54 |
| 2026-09-23 | `ARQT` | cash | leftover split 19.75 < 1 share @ 27.79 |
| 2026-09-23 | `OMER` | cash | leftover split 19.75 < 1 share @ 20.65 |
| 2026-09-23 | `TNGX` | cash | leftover split 19.75 < 1 share @ 25.40 |
| 2026-09-24 | `RBRK` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `VICR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `ECO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `TLSA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `EYPT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `BHVN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `BNC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `DDD` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `BKKT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `BTDR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `ORBS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `SBET` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `BTBT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `ADMA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `INDP` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `SGRY` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `EOG` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CVE` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `RRC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CHKP` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `S` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `INVZ` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `QNC` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `NNBR` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `RBRK` | 11 | 2026-09-18 @ $108.55 | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten; ⚪; ret5=+21.3; leftover $1218.16 |
| `VICR` | 5 | 2026-09-18 @ $219.62 | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten,yday_gainer,yday_mover; 🔵; ret5=+21.5; leftover $1218.16 |
| `ECO` | 14 | 2026-09-18 @ $85.00 | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten; 🔵; ⚪; ret5=+18.3; leftover $1218.16 |
| `TLSA` | 1255 | 2026-09-18 @ $0.97 | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+1.0; leftover $1218.16 |
| `EYPT` | 308 | 2026-09-18 @ $3.95 | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-6.1; leftover $1218.16 |
| `BHVN` | 86 | 2026-09-18 @ $14.07 | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $1218.16 |
| `BNC` | 208 | 2026-09-18 @ $5.83 | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $1218.16 |
| `DDD` | 340 | 2026-09-18 @ $3.58 | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ret5=+8.3; leftover $1218.16 |
| `BKKT` | 2 | 2026-09-21 @ $9.31 | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; ret5=+5.3; leftover $19.25 |
| `BTDR` | 1 | 2026-09-21 @ $13.47 | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+8.4; leftover $19.25 |
| `ORBS` | 17 | 2026-09-21 @ $1.11 | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; ret5=+6.9; leftover $19.25 |
| `SBET` | 1 | 2026-09-21 @ $9.99 | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ret5=+5.3; leftover $19.25 |
| `BTBT` | 10 | 2026-09-21 @ $1.82 | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ret5=+8.8; leftover $19.25 |
| `ADMA` | 2 | 2026-09-23 @ $9.81 | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten; 🔵; ⚪; ret5=+4.0; leftover $19.75 |
| `INDP` | 5 | 2026-09-23 @ $3.93 | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $19.75 |
| `SGRY` | 1 | 2026-09-23 @ $15.72 | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-2.7; leftover $19.75 |
