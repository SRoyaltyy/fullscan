# Factor mine action — `coil_h3_exit_alarm`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · coil, exit on 🚨

Cash book **-20.76%** ($7,924) · signal-only (no cash/fees) was -18.91%. Starts YES **1/30**. Fills 176 · skips 290 · realized $-1290.13.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 3 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

### What it looks at (inputs)

- Shopping list: the mixed morning shopping list (every name that showed up on any 09:30 list that day).
- Clock: 09:30 ET only. The sleeve never peeks at today's Change%, Gap, RelVol, or the printed book to decide.
- News, if used, is the morning packet box or yesterday's headline — never a later scrape.
- Money: leftover cash from yesterday + the lots we already hold. It can only spend cash it has and only sell shares it holds.
- Fill price: the 09:30 open, whole shares, Futubull fees.
- Morning weather S: if S ≤ −3 the sleeve sits (no new buys).
- Must-have: prior 5-session return is at least 0%.
- Must-have: prior 5-session return is at most 10% (not already exploded).
- Must-have: prior relative volume is at most 2.2 (not a blow-off).
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
- Minimum hold is 3 session(s) — the buy morning counts as 1.
- Early exit: sell at the next 09:30 if 🚨 prints, even inside the minimum hold.
- List-drop: after 3 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $7,614.85.

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
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 98 | $50.62 | $2.28 | — | $5,036.64 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=+6.2; leftover $5000.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 227 | $22.01 | $2.93 | — | $37.44 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten; ⚪; ret5=+0.3; leftover $5000.00 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $37.44 | ▲ close $10,677.03 vs 09:30 $10,000.00 (session +682.25) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $37.44 | ▲ 09:30 equity $10,751.77 vs yday $10,677.03 (+74.74) | — | — |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 4 | $0.94 | $0.05 | — | $33.65 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+0.5; leftover $4.68 | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 3 | $1.50 | $0.05 | — | $29.09 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+9.2; leftover $4.68 | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 1 | $4.31 | $0.05 | — | $24.74 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $4.68 | — |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 1 | $4.18 | $0.04 | — | $20.51 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $4.68 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20.51 | ▼ close $10,461.99 vs 09:30 $10,751.77 (session -289.59) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20.51 | ▼ 09:30 equity $10,399.63 vs yday $10,461.99 (-62.36) | — | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20.51 | ▼ close $10,334.34 vs 09:30 $10,399.63 (session -65.29) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20.51 | ▼ 09:30 equity $10,290.95 vs yday $10,334.34 (-43.39) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 98 | $51.77 | $2.34 | $+107.76 | $5,091.63 | ▲ +107.76 after sell → book $10,288.61; vs 09:30 mark -2.34 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `VOR` | 227 | $22.82 | $3.01 | $+177.93 | $10,268.76 | ▲ +177.93 after sell → book $10,285.60; vs 09:30 mark -3.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,268.76 | ▼ close $10,285.26 vs 09:30 $10,290.95 (session -0.35) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,268.76 | ▼ 09:30 equity $10,285.20 vs yday $10,285.26 (-0.06) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `LDI` | 4 | $0.88 | $0.07 | $-0.34 | $10,272.22 | ▼ -0.34 after sell → book $10,285.14; vs 09:30 mark -0.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 3 | $1.42 | $0.07 | $-0.37 | $10,276.40 | ▼ -0.37 after sell → book $10,285.06; vs 09:30 mark -0.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ANGX` | 1 | $4.79 | $0.07 | $+0.36 | $10,281.12 | ▲ +0.36 after sell → book $10,284.99; vs 09:30 mark -0.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `HYLN` | 1 | $3.87 | $0.06 | $-0.42 | $10,284.93 | ▼ -0.42 after sell → book $10,284.93; vs 09:30 mark -0.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,284.93 | ▲ close $10,284.93 vs 09:30 $10,285.20 (session +0.00) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,284.93 | ▲ 09:30 equity $10,284.93 vs yday $10,284.93 (+0.00) | — | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 62 | $20.55 | $2.18 | — | $9,008.66 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1285.62 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 14 | $91.01 | $2.03 | — | $7,732.48 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1285.62 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 222 | $5.77 | $2.86 | — | $6,448.68 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1285.62 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 65 | $19.63 | $2.19 | — | $5,170.55 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1285.62 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 43 | $29.63 | $2.12 | — | $3,894.34 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1285.62 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 734 | $1.75 | $9.47 | — | $2,600.37 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1285.62 | — |
| 2026-08-20 09:30 ET | **BUY** | `DNA` | 172 | $7.45 | $2.51 | — | $1,316.46 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ret5=+6.9; leftover $1285.62 | — |
| 2026-08-20 09:30 ET | **BUY** | `EXK` | 119 | $10.77 | $2.35 | — | $32.48 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+3.6; leftover $1285.62 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $32.48 | ▲ close $10,364.66 vs 09:30 $10,284.93 (session +105.43) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $32.48 | ▲ 09:30 equity $10,631.26 vs yday $10,364.66 (+266.60) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 2 | $1.66 | $0.04 | — | $29.13 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $4.06 | — |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 4 | $0.86 | $0.05 | — | $25.62 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,ohlc_hot; ret5=+9.7; leftover $4.06 | — |
| 2026-08-21 09:30 ET | **BUY** | `QTRX` | 1 | $3.11 | $0.03 | — | $22.48 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+9.1; leftover $4.06 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $22.48 | ▼ close $10,617.83 vs 09:30 $10,631.26 (session -13.32) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $22.48 | ▲ 09:30 equity $10,705.91 vs yday $10,617.83 (+88.08) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $22.48 | ▼ close $10,584.72 vs 09:30 $10,705.91 (session -121.19) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $22.48 | ▼ 09:30 equity $10,460.21 vs yday $10,584.72 (-124.51) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 62 | $20.32 | $2.20 | $-18.63 | $1,280.12 | ▼ -18.63 after sell → book $10,458.01; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 14 | $95.86 | $2.05 | $+63.82 | $2,620.11 | ▲ +63.82 after sell → book $10,455.96; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 222 | $5.53 | $2.91 | $-59.05 | $3,844.86 | ▼ -59.05 after sell → book $10,453.05; vs 09:30 mark -2.91 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 65 | $21.21 | $2.21 | $+98.31 | $5,221.30 | ▲ +98.31 after sell → book $10,450.84; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 43 | $32.32 | $2.14 | $+111.41 | $6,608.92 | ▲ +111.41 after sell → book $10,448.70; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 734 | $1.90 | $9.60 | $+91.03 | $7,993.92 | ▲ +91.03 after sell → book $10,439.10; vs 09:30 mark -9.60 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `DNA` | 172 | $6.94 | $2.54 | $-92.77 | $9,185.06 | ▼ -92.77 after sell → book $10,436.56; vs 09:30 mark -2.54 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `EXK` | 119 | $10.44 | $2.38 | $-43.99 | $10,425.04 | ▼ -43.99 after sell → book $10,434.18; vs 09:30 mark -2.38 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 118 | $10.98 | $2.34 | — | $9,127.05 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ret5=+1.2; leftover $1303.13 | — |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 21 | $61.19 | $2.05 | — | $7,840.01 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ret5=+7.4; leftover $1303.13 | — |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 156 | $8.35 | $2.46 | — | $6,534.95 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+8.0; leftover $1303.13 | — |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 263 | $4.94 | $3.39 | — | $5,232.34 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten; ret5=+7.1; leftover $1303.13 | — |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 3 | $426.97 | $2.00 | — | $3,949.43 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten; ret5=+6.0; leftover $1303.13 | — |
| 2026-08-25 09:30 ET | **BUY** | `KURA` | 95 | $13.59 | $2.27 | — | $2,656.11 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1303.13 | — |
| 2026-08-25 09:30 ET | **BUY** | `LIFE` | 35 | $36.96 | $2.10 | — | $1,360.41 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+4.4; leftover $1303.13 | — |
| 2026-08-25 09:30 ET | **BUY** | `ADIG` | 59 | $21.79 | $2.17 | — | $72.64 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable; 🔵; ret5=+3.1; leftover $1303.13 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $72.64 | ▲ close $10,541.95 vs 09:30 $10,460.21 (session +126.55) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $72.64 | ▼ 09:30 equity $10,476.40 vs yday $10,541.95 (-65.55) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `BTBT` | 2 | $1.53 | $0.06 | $-0.36 | $75.64 | ▼ -0.36 after sell → book $10,476.34; vs 09:30 mark -0.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `ORBS` | 4 | $0.80 | $0.06 | $-0.38 | $78.76 | ▼ -0.38 after sell → book $10,476.28; vs 09:30 mark -0.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `QTRX` | 1 | $2.83 | $0.05 | $-0.37 | $81.54 | ▼ -0.37 after sell → book $10,476.23; vs 09:30 mark -0.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **BUY** | `ABX` | 2 | $9.83 | $0.20 | — | $61.67 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ret5=+9.7; leftover $20.38 | — |
| 2026-08-26 09:30 ET | **BUY** | `SENS` | 2 | $9.48 | $0.20 | — | $42.52 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover; ret5=+9.6; leftover $20.38 | — |
| 2026-08-26 09:30 ET | **BUY** | `ACRS` | 3 | $6.53 | $0.20 | — | $22.72 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list yday_gainer; 🔵; ret5=+3.6; leftover $20.38 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $22.72 | ▼ close $10,447.39 vs 09:30 $10,476.40 (session -28.23) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $22.72 | ▲ 09:30 equity $10,478.23 vs yday $10,447.39 (+30.84) | — | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $22.72 | ▼ close $10,421.18 vs 09:30 $10,478.23 (session -57.05) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $22.72 | ▲ 09:30 equity $10,439.40 vs yday $10,421.18 (+18.22) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `OCUL` | 118 | $10.97 | $2.37 | $-5.90 | $1,314.81 | ▼ -5.90 after sell → book $10,437.03; vs 09:30 mark -2.37 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `INSP` | 21 | $60.52 | $2.07 | $-18.20 | $2,583.66 | ▼ -18.20 after sell → book $10,434.96; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRMD` | 156 | $8.28 | $2.49 | $-15.87 | $3,872.84 | ▼ -15.87 after sell → book $10,432.46; vs 09:30 mark -2.50 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `RZLT` | 263 | $4.95 | $3.45 | $-4.21 | $5,171.25 | ▼ -4.21 after sell → book $10,429.02; vs 09:30 mark -3.44 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `HCA` | 3 | $423.76 | $2.02 | $-13.65 | $6,440.51 | ▼ -13.65 after sell → book $10,427.00; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `KURA` | 95 | $13.05 | $2.30 | $-55.88 | $7,677.96 | ▼ -55.88 after sell → book $10,424.70; vs 09:30 mark -2.30 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `LIFE` | 35 | $39.60 | $2.12 | $+88.19 | $9,061.84 | ▲ +88.19 after sell → book $10,422.58; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ADIG` | 59 | $22.10 | $2.19 | $+13.94 | $10,363.55 | ▲ +13.94 after sell → book $10,420.39; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `RRC` | 31 | $41.74 | $2.08 | — | $9,067.53 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten; ret5=+2.4; leftover $1295.44 | — |
| 2026-08-28 09:30 ET | **BUY** | `CRK` | 88 | $14.63 | $2.25 | — | $7,777.84 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten; ret5=+5.8; leftover $1295.44 | — |
| 2026-08-28 09:30 ET | **BUY** | `MOS` | 54 | $23.95 | $2.15 | — | $6,482.38 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten; ret5=+1.8; leftover $1295.44 | — |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 39 | $32.90 | $2.11 | — | $5,197.18 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.4; leftover $1295.44 | — |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 82 | $15.66 | $2.24 | — | $3,910.82 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.7; leftover $1295.44 | — |
| 2026-08-28 09:30 ET | **BUY** | `SIMO` | 5 | $252.24 | $2.00 | — | $2,647.62 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; ⚪; ret5=+2.2; leftover $1295.44 | — |
| 2026-08-28 09:30 ET | **BUY** | `TTMI` | 10 | $122.81 | $2.02 | — | $1,417.50 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+9.0; leftover $1295.44 | — |
| 2026-08-28 09:30 ET | **BUY** | `EQ` | 526 | $2.46 | $6.79 | — | $116.75 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list yday_gainer; ret5=+7.9; leftover $1295.44 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $116.75 | ▼ close $10,069.40 vs 09:30 $10,439.40 (session -329.35) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $116.75 | ▲ 09:30 equity $10,112.58 vs yday $10,069.40 (+43.18) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `ABX` | 2 | $9.74 | $0.22 | $-0.60 | $136.01 | ▼ -0.60 after sell → book $10,112.36; vs 09:30 mark -0.22 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `SENS` | 2 | $9.29 | $0.21 | $-0.79 | $154.38 | ▼ -0.79 after sell → book $10,112.15; vs 09:30 mark -0.21 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `ACRS` | 3 | $5.97 | $0.21 | $-2.09 | $172.08 | ▼ -2.09 after sell → book $10,111.94; vs 09:30 mark -0.21 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $172.08 | ▲ close $10,131.82 vs 09:30 $10,112.58 (session +19.88) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $172.08 | ▲ 09:30 equity $10,160.32 vs yday $10,131.82 (+28.50) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $172.08 | ▼ close $10,159.36 vs 09:30 $10,160.32 (session -0.96) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $172.08 | ▼ 09:30 equity $10,076.35 vs yday $10,159.36 (-83.01) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `RRC` | 31 | $42.10 | $2.10 | $+6.97 | $1,475.08 | ▲ +6.97 after sell → book $10,074.25; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CRK` | 88 | $15.70 | $2.28 | $+89.63 | $2,854.40 | ▲ +89.63 after sell → book $10,071.97; vs 09:30 mark -2.28 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `MOS` | 54 | $24.70 | $2.17 | $+36.18 | $4,186.02 | ▲ +36.18 after sell → book $10,069.79; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SEDG` | 39 | $32.42 | $2.13 | $-22.95 | $5,448.28 | ▼ -22.95 after sell → book $10,067.67; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `GRRR` | 82 | $13.92 | $2.26 | $-147.18 | $6,587.46 | ▼ -147.18 after sell → book $10,065.41; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SIMO` | 5 | $235.71 | $2.02 | $-86.68 | $7,763.98 | ▼ -86.68 after sell → book $10,063.38; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `TTMI` | 10 | $114.22 | $2.04 | $-89.96 | $8,904.14 | ▼ -89.96 after sell → book $10,061.34; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `EQ` | 526 | $2.20 | $6.88 | $-150.43 | $10,054.46 | ▼ -150.43 after sell → book $10,054.46; vs 09:30 mark -6.88 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,054.46 | ▲ close $10,054.46 vs 09:30 $10,076.35 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,054.46 | ▲ 09:30 equity $10,054.46 vs yday $10,054.46 (-0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 23 | $52.88 | $2.06 | — | $8,836.16 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1256.81 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 29 | $42.93 | $2.08 | — | $7,589.11 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1256.81 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 346 | $3.63 | $4.46 | — | $6,328.67 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1256.81 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $132.45 | $2.02 | — | $5,134.60 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1256.81 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 81 | $15.45 | $2.23 | — | $3,880.92 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $1256.81 | — |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 8 | $145.94 | $2.01 | — | $2,711.35 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+0.8; leftover $1256.81 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 74 | $16.77 | $2.21 | — | $1,468.15 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1256.81 | — |
| 2026-09-03 09:30 ET | **BUY** | `SLN` | 84 | $14.85 | $2.24 | — | $218.51 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; 🔵; ⚪; ret5=+9.3; leftover $1256.81 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $218.51 | ▼ close $9,814.49 vs 09:30 $10,054.46 (session -220.65) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $218.51 | ▼ 09:30 equity $9,814.15 vs yday $9,814.49 (-0.34) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 17 | $2.52 | $0.48 | — | $175.19 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $43.70 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 6 | $6.71 | $0.42 | — | $134.51 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $43.70 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 9 | $4.78 | $0.46 | — | $91.03 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $43.70 | — |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 3 | $11.31 | $0.35 | — | $56.76 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $43.70 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $56.76 | ▼ close $9,790.57 vs 09:30 $9,814.15 (session -21.88) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $56.76 | ▼ 09:30 equity $9,789.49 vs yday $9,790.57 (-1.08) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $56.76 | ▼ close $9,597.17 vs 09:30 $9,789.49 (session -192.32) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $56.76 | ▼ 09:30 equity $9,552.58 vs yday $9,597.17 (-44.59) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `ATRC` | 23 | $53.16 | $2.08 | $+2.30 | $1,277.36 | ▲ +2.30 after sell → book $9,550.50; vs 09:30 mark -2.08 | exit 🚨 after 3 sess | — |
| 2026-09-09 09:30 ET | **SELL** | `HRMY` | 29 | $42.01 | $2.10 | $-30.85 | $2,493.55 | ▼ -30.85 after sell → book $9,548.40; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CABA` | 346 | $3.28 | $4.53 | $-130.09 | $3,623.90 | ▼ -130.09 after sell → book $9,543.87; vs 09:30 mark -4.53 | exit 🚨 after 3 sess | — |
| 2026-09-09 09:30 ET | **SELL** | `RVTY` | 9 | $125.77 | $2.04 | $-64.17 | $4,753.79 | ▼ -64.17 after sell → book $9,541.83; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CRK` | 81 | $15.16 | $2.26 | $-27.98 | $5,979.50 | ▼ -27.98 after sell → book $9,539.58; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `MRNA` | 8 | $140.29 | $2.03 | $-49.25 | $7,099.82 | ▼ -49.25 after sell → book $9,537.54; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `ARCT` | 74 | $15.46 | $2.23 | $-101.39 | $8,241.63 | ▼ -101.39 after sell → book $9,535.31; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `SLN` | 84 | $13.60 | $2.27 | $-109.51 | $9,381.76 | ▼ -109.51 after sell → book $9,533.04; vs 09:30 mark -2.27 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,381.76 | ▼ close $9,525.88 vs 09:30 $9,552.58 (session -7.17) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,381.76 | ▼ 09:30 equity $9,523.17 vs yday $9,525.88 (-2.71) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `ALEC` | 17 | $2.22 | $0.45 | $-6.03 | $9,419.05 | ▼ -6.03 after sell → book $9,522.72; vs 09:30 mark -0.45 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BHC` | 6 | $6.11 | $0.40 | $-4.43 | $9,455.31 | ▼ -4.43 after sell → book $9,522.32; vs 09:30 mark -0.40 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `OABI` | 9 | $3.92 | $0.40 | $-8.58 | $9,490.21 | ▼ -8.58 after sell → book $9,521.92; vs 09:30 mark -0.40 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `VIR` | 3 | $10.57 | $0.35 | $-2.91 | $9,521.57 | ▼ -2.91 after sell → book $9,521.57; vs 09:30 mark -0.35 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,521.57 | ▲ close $9,521.57 vs 09:30 $9,523.17 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,521.57 | ▲ 09:30 equity $9,521.57 vs yday $9,521.57 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `SANM` | 5 | $206.84 | $2.00 | — | $8,485.37 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten; ret5=+8.3; leftover $1190.20 | — |
| 2026-09-11 09:30 ET | **BUY** | `NVT` | 7 | $157.78 | $2.01 | — | $7,378.90 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ret5=+4.7; leftover $1190.20 | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 583 | $2.04 | $7.52 | — | $6,182.05 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $1190.20 | — |
| 2026-09-11 09:30 ET | **BUY** | `CLOV` | 250 | $4.75 | $3.23 | — | $4,991.33 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.5; leftover $1190.20 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 561 | $2.12 | $7.24 | — | $3,794.77 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $1190.20 | — |
| 2026-09-11 09:30 ET | **BUY** | `FUBO` | 103 | $11.55 | $2.30 | — | $2,602.82 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $1190.20 | — |
| 2026-09-11 09:30 ET | **BUY** | `VIST` | 15 | $77.33 | $2.04 | — | $1,440.84 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; ret5=+2.5; leftover $1190.20 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAND` | 22 | $52.55 | $2.06 | — | $282.68 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; 🔵; ⚪; ret5=+8.6; leftover $1190.20 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $282.68 | ▲ close $9,625.83 vs 09:30 $9,521.57 (session +132.65) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $282.68 | ▼ 09:30 equity $9,491.04 vs yday $9,625.83 (-134.79) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `CLOV` | 250 | $4.82 | $3.28 | $+11.00 | $1,484.41 | ▲ +11.00 after sell → book $9,487.77; vs 09:30 mark -3.27 | exit 🚨 after 1 sess | — |
| 2026-09-14 09:30 ET | **SELL** | `BAND` | 22 | $56.90 | $2.08 | $+91.57 | $2,734.13 | ▲ +91.57 after sell → book $9,485.69; vs 09:30 mark -2.08 | exit 🚨 after 1 sess | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,734.13 | ▼ close $9,374.03 vs 09:30 $9,491.04 (session -111.66) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,734.13 | ▲ 09:30 equity $9,398.18 vs yday $9,374.03 (+24.15) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $2,734.13 | ▼ close $9,310.54 vs 09:30 $9,398.18 (session -87.64) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $2,734.13 | ▼ 09:30 equity $9,135.47 vs yday $9,310.54 (-175.07) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `SANM` | 5 | $194.84 | $2.02 | $-64.03 | $3,706.31 | ▼ -64.03 after sell → book $9,133.45; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `NVT` | 7 | $147.79 | $2.03 | $-73.97 | $4,738.80 | ▼ -73.97 after sell → book $9,131.41; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `AMTX` | 583 | $1.89 | $7.63 | $-102.60 | $5,833.05 | ▼ -102.60 after sell → book $9,123.79; vs 09:30 mark -7.62 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `BAK` | 561 | $1.84 | $7.34 | $-171.66 | $6,857.95 | ▼ -171.66 after sell → book $9,116.45; vs 09:30 mark -7.34 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `FUBO` | 103 | $10.75 | $2.33 | $-87.03 | $7,962.87 | ▼ -87.03 after sell → book $9,114.12; vs 09:30 mark -2.33 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `VIST` | 15 | $76.75 | $2.06 | $-12.79 | $9,112.07 | ▼ -12.79 after sell → book $9,112.07; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `IQV` | 4 | $270.89 | $2.00 | — | $8,026.50 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten; ret5=+4.0; leftover $1139.01 | — |
| 2026-09-16 09:30 ET | **BUY** | `AVAH` | 79 | $14.31 | $2.23 | — | $6,893.79 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten; ret5=+4.8; leftover $1139.01 | — |
| 2026-09-16 09:30 ET | **BUY** | `BLFS` | 31 | $36.46 | $2.08 | — | $5,761.44 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ret5=+2.9; leftover $1139.01 | — |
| 2026-09-16 09:30 ET | **BUY** | `TEM` | 16 | $68.79 | $2.04 | — | $4,658.77 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+7.1; leftover $1139.01 | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 194 | $5.87 | $2.57 | — | $3,517.41 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $1139.01 | — |
| 2026-09-16 09:30 ET | **BUY** | `VAL` | 13 | $87.40 | $2.03 | — | $2,379.18 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+3.2; leftover $1139.01 | — |
| 2026-09-16 09:30 ET | **BUY** | `MRCY` | 13 | $87.52 | $2.03 | — | $1,239.40 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list yday_gainer; 🔵; ret5=+4.3; leftover $1139.01 | — |
| 2026-09-16 09:30 ET | **BUY** | `QLYS` | 6 | $179.60 | $2.01 | — | $159.79 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list yday_gainer; ret5=+8.8; leftover $1139.01 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $159.79 | ▼ close $8,980.67 vs 09:30 $9,135.47 (session -114.41) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $159.79 | ▲ 09:30 equity $9,093.98 vs yday $8,980.67 (+113.31) | — | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 2 | $7.59 | $0.16 | — | $144.45 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $19.97 | — |
| 2026-09-17 09:30 ET | **BUY** | `SABR` | 8 | $2.40 | $0.22 | — | $125.03 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.4; leftover $19.97 | — |
| 2026-09-17 09:30 ET | **BUY** | `AIB` | 13 | $1.46 | $0.23 | — | $105.82 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list yday_gainer; 🔵; ret5=+4.4; leftover $19.97 | — |
| 2026-09-17 09:30 ET | **BUY** | `BYND` | 1 | $11.19 | $0.11 | — | $94.52 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list yday_gainer; 🔵; ret5=+0.5; leftover $19.97 | — |
| 2026-09-17 09:30 ET | **BUY** | `QTRX` | 6 | $2.94 | $0.19 | — | $76.69 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list yday_gainer,ohlc_hot; 🔵; ret5=+9.8; leftover $19.97 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $76.69 | ▲ close $9,148.62 vs 09:30 $9,093.98 (session +55.55) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $76.69 | ▲ 09:30 equity $9,174.13 vs yday $9,148.62 (+25.51) | — | — |
| 2026-09-18 09:30 ET | **BUY** | `BNC` | 1 | $5.83 | $0.06 | — | $70.79 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.6; leftover $9.59 | — |
| 2026-09-18 09:30 ET | **BUY** | `DDD` | 2 | $3.58 | $0.08 | — | $63.56 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+8.3; leftover $9.59 | — |
| 2026-09-18 09:30 ET | **BUY** | `RANI` | 11 | $0.85 | $0.13 | — | $54.08 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; ret5=+3.6; leftover $9.59 | — |
| 2026-09-18 09:30 ET | **BUY** | `SHLS` | 1 | $7.64 | $0.08 | — | $46.36 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list yday_gainer; 🔵; ret5=+7.6; leftover $9.59 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $46.36 | ▼ close $9,029.11 vs 09:30 $9,174.13 (session -144.68) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $46.36 | ▲ 09:30 equity $9,062.61 vs yday $9,029.11 (+33.50) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `IQV` | 4 | $266.76 | $2.02 | $-20.54 | $1,111.38 | ▼ -20.54 after sell → book $9,060.59; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `AVAH` | 79 | $13.65 | $2.25 | $-56.62 | $2,187.48 | ▼ -56.62 after sell → book $9,058.34; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `BLFS` | 31 | $36.70 | $2.10 | $+3.25 | $3,323.08 | ▲ +3.25 after sell → book $9,056.23; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `TEM` | 16 | $79.08 | $2.06 | $+160.54 | $4,586.30 | ▲ +160.54 after sell → book $9,054.18; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `RIG` | 194 | $5.62 | $2.61 | $-53.69 | $5,673.96 | ▼ -53.69 after sell → book $9,051.56; vs 09:30 mark -2.62 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `VAL` | 13 | $83.46 | $2.05 | $-55.30 | $6,756.89 | ▼ -55.30 after sell → book $9,049.51; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `MRCY` | 13 | $86.52 | $2.05 | $-17.08 | $7,879.61 | ▼ -17.08 after sell → book $9,047.46; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `QLYS` | 6 | $175.63 | $2.03 | $-27.86 | $8,931.36 | ▼ -27.86 after sell → book $9,045.44; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `PGEN` | 2 | $7.84 | $0.18 | $+0.16 | $8,946.85 | ▲ +0.16 after sell → book $9,045.25; vs 09:30 mark -0.19 | exit 🚨 after 2 sess | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 7 | $157.87 | $2.01 | — | $7,839.75 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten; ret5=+6.5; leftover $1118.36 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTDR` | 82 | $13.47 | $2.24 | — | $6,732.57 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+8.4; leftover $1118.36 | — |
| 2026-09-21 09:30 ET | **BUY** | `ORBS` | 1007 | $1.11 | $12.99 | — | $5,601.81 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover; ret5=+6.9; leftover $1118.36 | — |
| 2026-09-21 09:30 ET | **BUY** | `SBET` | 111 | $9.99 | $2.32 | — | $4,490.59 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+5.3; leftover $1118.36 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTBT` | 612 | $1.82 | $7.89 | — | $3,365.80 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+8.8; leftover $1118.36 | — |
| 2026-09-21 09:30 ET | **BUY** | `SGML` | 110 | $10.13 | $2.32 | — | $2,248.63 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; ret5=+4.9; leftover $1118.36 | — |
| 2026-09-21 09:30 ET | **BUY** | `GLXY` | 43 | $25.95 | $2.12 | — | $1,130.66 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $1118.36 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,130.66 | ▼ close $8,968.83 vs 09:30 $9,062.61 (session -44.52) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,130.66 | ▼ 09:30 equity $8,945.49 vs yday $8,968.83 (-23.34) | — | — |
| 2026-09-22 09:30 ET | **BUY** | `ALOY` | 15 | $9.40 | $1.46 | — | $988.21 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; ret5=+9.5; leftover $141.33 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $988.21 | ▲ close $9,134.67 vs 09:30 $8,945.49 (session +190.63) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $988.21 | ▼ 09:30 equity $9,120.93 vs yday $9,134.67 (-13.74) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `SABR` | 8 | $2.24 | $0.22 | $-1.72 | $1,005.90 | ▼ -1.72 after sell → book $9,120.70; vs 09:30 mark -0.23 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `AIB` | 13 | $1.37 | $0.24 | $-1.64 | $1,023.48 | ▼ -1.64 after sell → book $9,120.47; vs 09:30 mark -0.23 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `BYND` | 1 | $11.01 | $0.13 | $-0.43 | $1,034.35 | ▼ -0.43 after sell → book $9,120.33; vs 09:30 mark -0.14 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `QTRX` | 6 | $3.17 | $0.23 | $+0.96 | $1,053.14 | ▲ +0.96 after sell → book $9,120.10; vs 09:30 mark -0.23 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `BNC` | 1 | $6.29 | $0.09 | $+0.31 | $1,059.35 | ▲ +0.31 after sell → book $9,120.02; vs 09:30 mark -0.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `DDD` | 2 | $3.59 | $0.10 | $-0.16 | $1,066.43 | ▼ -0.16 after sell → book $9,119.92; vs 09:30 mark -0.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `RANI` | 11 | $0.81 | $0.14 | $-0.71 | $1,075.20 | ▼ -0.71 after sell → book $9,119.78; vs 09:30 mark -0.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `SHLS` | 1 | $8.15 | $0.10 | $+0.33 | $1,083.24 | ▲ +0.33 after sell → book $9,119.67; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **BUY** | `DXCM` | 1 | $89.50 | $0.90 | — | $992.85 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+5.3; leftover $135.41 | — |
| 2026-09-23 09:30 ET | **BUY** | `HALO` | 1 | $116.85 | $1.17 | — | $874.82 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+3.3; leftover $135.41 | — |
| 2026-09-23 09:30 ET | **BUY** | `ARQT` | 4 | $27.79 | $1.12 | — | $762.54 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+7.0; leftover $135.41 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 13 | $9.81 | $1.31 | — | $633.70 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+4.0; leftover $135.41 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 6 | $20.65 | $1.26 | — | $508.54 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $135.41 | — |
| 2026-09-23 09:30 ET | **BUY** | `INDP` | 34 | $3.93 | $1.44 | — | $373.48 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $135.41 | — |
| 2026-09-23 09:30 ET | **BUY** | `CLPT` | 8 | $15.55 | $1.27 | — | $247.81 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ret5=+3.8; leftover $135.41 | — |
| 2026-09-23 09:30 ET | **BUY** | `BLLN` | 1 | $116.00 | $1.16 | — | $130.65 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; 🔵; ⚪; ret5=+3.0; leftover $135.41 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $130.65 | ▼ close $8,809.89 vs 09:30 $9,120.93 (session -300.15) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $130.65 | ▼ 09:30 equity $8,685.53 vs yday $8,809.89 (-124.36) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `A` | 7 | $163.95 | $2.03 | $+38.52 | $1,276.27 | ▲ +38.52 after sell → book $8,683.50; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `BTDR` | 82 | $12.26 | $2.26 | $-104.13 | $2,279.33 | ▼ -104.13 after sell → book $8,681.24; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `ORBS` | 1007 | $1.05 | $13.17 | $-86.58 | $3,323.51 | ▼ -86.58 after sell → book $8,668.07; vs 09:30 mark -13.17 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `SBET` | 111 | $9.80 | $2.35 | $-25.76 | $4,408.96 | ▼ -25.76 after sell → book $8,665.72; vs 09:30 mark -2.35 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `BTBT` | 612 | $1.73 | $8.01 | $-77.10 | $5,456.65 | ▼ -77.10 after sell → book $8,657.71; vs 09:30 mark -8.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `SGML` | 110 | $9.89 | $2.35 | $-31.62 | $6,542.21 | ▼ -31.62 after sell → book $8,655.37; vs 09:30 mark -2.34 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `GLXY` | 43 | $25.00 | $2.14 | $-45.32 | $7,614.85 | ▼ -45.32 after sell → book $8,653.23; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $7,614.85 | ▲ close $8,672.74 vs 09:30 $8,685.53 (session +19.52) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5,689.45 | ▲ 09:30 equity $7,933.85 vs yday $7,933.85 (+0.00) | 09:30 open · cash $5,689.45 (unchanged overnight, no fees) · equity $7,933.85 vs prior close $7,933.85 (+0.00) · 12 name(s) re-marked at the open (per-name table). ANAB×6 yday $51.70 → 09:30 $51.70 +0.00; APPS×29 yday $10.88 → 09:30 $10.88 +0.00; ARHS×40 yday $9.47 → 09:30 $9.47 +0.00; BTQ×125 yday $2.79 → 09:30 $2.79 +0.00; CNXC×1 yday $29.39 → 09:30 $29.39 +0.00; FTRE×2 yday $20.02 → 09:30 $20.02 +0.00; GT×8 yday $5.07 → 09:30 $5.07 +0.00; HYMC×2 yday $20.89 → 09:30 $20.89 +0.00; INDP×11 yday $4.00 → 09:30 $4.00 +0.00; NN×22 yday $14.45 → 09:30 $14.45 +0.00; NTSK×19 yday $18.57 → 09:30 $18.57 +0.00; TNGX×1 yday $24.63 → 09:30 $24.63 +0.00 | — |
| 2026-09-25 09:30 ET | **BUY** | `HALO` | 6 | $115.36 | $2.01 | — | $4,995.28 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+5.1; leftover $711.18 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `OMER` | 34 | $20.61 | $2.09 | — | $4,292.45 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ret5=+9.1; leftover $711.18 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `BLFS` | 18 | $38.51 | $2.04 | — | $3,597.23 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten; ret5=+4.7; leftover $711.18 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 92 | $7.65 | $2.27 | — | $2,891.16 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+5.2; leftover $711.18 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TXG` | 8 | $83.76 | $2.01 | — | $2,219.07 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $711.18 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `HLP` | 323 | $2.20 | $4.17 | — | $1,504.30 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,ohlc_hot; 🔵; ret5=+8.9; leftover $711.18 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SATL` | 118 | $6.00 | $2.34 | — | $793.96 | — | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,ohlc_hot; 🔵; ret5=+7.8; leftover $711.18 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $793.96 | ▲ close $7,924.07 vs 09:30 $7,933.85 (session +7.15) | 16:00 close · cash $793.96 · equity $7,924.07 vs 09:30 $7,933.85 (-9.78; session marks +7.15) · 19 name(s) marked open→close (per-name table). ANAB×6 09:30 $51.70 → close $51.70 +0.00; APPS×29 09:30 $10.88 → close $10.88 +0.00; ARHS×40 09:30 $9.47 → close $9.47 +0.00; BTQ×125 09:30 $2.79 → close $2.79 -0.00; CNXC×1 09:30 $29.39 → close $29.39 -0.00; FTRE×2 09:30 $20.02 → close $20.02 +0.00; GT×8 09:30 $5.07 → close $5.07 +0.00; HYMC×2 09:30 $20.89 → close $20.89 -0.00; INDP×11 09:30 $4.00 → close $4.00 +0.00; NN×22 09:30 $14.45 → close $14.45 -0.00; NTSK×19 09:30 $18.57 → close $18.57 -0.00; TNGX×1 09:30 $24.63 → close $24.63 -0.00; HALO×6 09:30 $115.36 → close $113.90 -8.76; OMER×34 09:30 $20.61 → close $20.08 -18.02; BLFS×18 09:30 $38.51 → close $38.49 -0.36; MRVI×92 09:30 $7.65 → close $7.60 -4.60; TXG×8 09:30 $83.76 → close $85.71 +15.60; HLP×323 09:30 $2.20 → close $2.21 +3.23; SATL×118 09:30 $6.00 → close $6.17 +20.06 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `VOR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TLN` | cash | leftover split 4.68 < 1 share @ 359.83 |
| 2026-08-14 | `VST` | cash | leftover split 4.68 < 1 share @ 146.90 |
| 2026-08-14 | `NRG` | cash | leftover split 4.68 < 1 share @ 120.00 |
| 2026-08-14 | `SLG` | cash | leftover split 4.68 < 1 share @ 57.61 |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `VOR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `LDI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `HYLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `DVN` | cash | leftover split 2.56 < 1 share @ 46.18 |
| 2026-08-17 | `EOG` | cash | leftover split 2.56 < 1 share @ 142.77 |
| 2026-08-17 | `FANG` | cash | leftover split 2.56 < 1 share @ 202.70 |
| 2026-08-17 | `TGB` | cash | leftover split 2.56 < 1 share @ 8.46 |
| 2026-08-17 | `DNN` | cash | leftover split 2.56 < 1 share @ 3.24 |
| 2026-08-17 | `OCC` | cash | leftover split 2.56 < 1 share @ 18.24 |
| 2026-08-17 | `ALM` | cash | leftover split 2.56 < 1 share @ 16.20 |
| 2026-08-17 | `NEWP` | cash | leftover split 2.56 < 1 share @ 6.94 |
| 2026-08-18 | `LDI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ANGX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `HYLN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CYPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TBPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `PURR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `VNET` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `GUTS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNTN` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNDY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AEHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MXL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `DNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `EXK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CRSP` | cash | leftover split 4.06 < 1 share @ 59.72 |
| 2026-08-21 | `CF` | cash | leftover split 4.06 < 1 share @ 127.43 |
| 2026-08-21 | `EMBC` | cash | leftover split 4.06 < 1 share @ 5.43 |
| 2026-08-21 | `TXG` | cash | leftover split 4.06 < 1 share @ 64.39 |
| 2026-08-21 | `DXYZ` | cash | leftover split 4.06 < 1 share @ 34.89 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `DNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `EXK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ORBS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `QTRX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ABAT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `PDD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ORBS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `QTRX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `OCUL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `KURA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `LIFE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ADIG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `BE` | cash | leftover split 20.38 < 1 share @ 213.94 |
| 2026-08-27 | `OCUL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `INSP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CRMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RZLT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `HCA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `LIFE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ADIG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ACRS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `RRC` | cash | leftover split 4.54 < 1 share @ 41.44 |
| 2026-08-27 | `CRK` | cash | leftover split 4.54 < 1 share @ 14.42 |
| 2026-08-27 | `MOS` | cash | leftover split 4.54 < 1 share @ 24.00 |
| 2026-08-27 | `AVBP` | cash | leftover split 4.54 < 1 share @ 30.79 |
| 2026-08-27 | `BE` | cash | leftover split 4.54 < 1 share @ 227.10 |
| 2026-08-28 | `ABX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `SENS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `ACRS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `RRC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `MOS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SEDG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `GRRR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SIMO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `TTMI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `EQ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ACDC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FIG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PANW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SAIL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FROG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `RRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `MOS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SEDG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `GRRR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SIMO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `TTMI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `EQ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MTDR` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `RES` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KOS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OIS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `FTI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KMI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `PCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HRMY` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VSTM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TEL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `YDDL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `CYPH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ARCT` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MRNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `SLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRM` | cash | leftover split 43.70 < 1 share @ 263.36 |
| 2026-09-08 | `ATRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `HRMY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CABA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `RVTY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `MRNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `SLN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ALEC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `BHC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `OABI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `VIR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `GGB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SUZ` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHGG` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `MRLN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `EVGO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `BMNR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `ALEC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `OABI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `VIR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `AR` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VET` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `GRNT` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VENU` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `LFMD` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `KEP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `ZIM` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `UGP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `DDOG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CHYM` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SUNB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `TNGX` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `TJGC` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `SANM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `AMTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `BAK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `FUBO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `VIST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DK` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DELL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HTFL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `FPS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `NTAP` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `XRX` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `SANM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `NVT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `AMTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `BAK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `FUBO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `VIST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `SMMT` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `ATRC` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `RBRK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `QLYS` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CYPH` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `IVVD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `LFMD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `RBLX` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `IQV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `AVAH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `BLFS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `TEM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `RIG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `VAL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `MRCY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `QLYS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `AMN` | cash | leftover split 19.97 < 1 share @ 34.93 |
| 2026-09-17 | `SMTC` | cash | leftover split 19.97 < 1 share @ 170.85 |
| 2026-09-17 | `FTAI` | cash | leftover split 19.97 < 1 share @ 196.50 |
| 2026-09-18 | `IQV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `AVAH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `BLFS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `TEM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `RIG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `VAL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `MRCY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `QLYS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `PGEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `SABR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `AIB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `BYND` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `QTRX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `BHVN` | cash | leftover split 9.59 < 1 share @ 14.07 |
| 2026-09-18 | `XE` | cash | leftover split 9.59 < 1 share @ 16.28 |
| 2026-09-18 | `AMD` | cash | leftover split 9.59 < 1 share @ 547.37 |
| 2026-09-18 | `SYM` | cash | leftover split 9.59 < 1 share @ 44.70 |
| 2026-09-21 | `SABR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `AIB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `BYND` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `QTRX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `BNC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `DDD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `RANI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `SHLS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `SNDK` | cash | leftover split 1118.36 < 1 share @ 1826.00 |
| 2026-09-22 | `SABR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `AIB` | no_price | no 09:30 open — carry |
| 2026-09-22 | `BYND` | no_price | no 09:30 open — carry |
| 2026-09-22 | `QTRX` | no_price | no 09:30 open — carry |
| 2026-09-22 | `BNC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `DDD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `RANI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `SHLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `A` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `BTDR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `ORBS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `SBET` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `SGML` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `GLXY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `APPS` | no_price | no 09:30 open |
| 2026-09-22 | `MRAM` | no_price | no 09:30 open |
| 2026-09-22 | `ARHS` | no_price | no 09:30 open |
| 2026-09-22 | `RMBS` | no_price | no 09:30 open |
| 2026-09-22 | `EMAT` | no_price | no 09:30 open |
| 2026-09-22 | `NTSK` | no_price | no 09:30 open |
| 2026-09-22 | `ZS` | no_price | no 09:30 open |
| 2026-09-23 | `A` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `BTDR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `ORBS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `SBET` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `SGML` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `GLXY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-23 | `ALOY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `ALOY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-24 | `DXCM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `HALO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `ARQT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `ADMA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `OMER` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `INDP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `CLPT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `BLLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `CVE` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `S` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `INVZ` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `NNBR` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `DH` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CRWD` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `LXEO` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `PBLS` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ALOY` | 15 | 2026-09-22 @ $9.40 | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; ret5=+9.5; leftover $141.33 |
| `DXCM` | 1 | 2026-09-23 @ $89.50 | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+5.3; leftover $135.41 |
| `HALO` | 1 | 2026-09-23 @ $116.85 | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+3.3; leftover $135.41 |
| `ARQT` | 4 | 2026-09-23 @ $27.79 | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+7.0; leftover $135.41 |
| `ADMA` | 13 | 2026-09-23 @ $9.81 | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+4.0; leftover $135.41 |
| `OMER` | 6 | 2026-09-23 @ $20.65 | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $135.41 |
| `INDP` | 34 | 2026-09-23 @ $3.93 | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $135.41 |
| `CLPT` | 8 | 2026-09-23 @ $15.55 | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ret5=+3.8; leftover $135.41 |
| `BLLN` | 1 | 2026-09-23 @ $116.00 | coil, exit on 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_max=2.2; list probable,yday_gainer; 🔵; ⚪; ret5=+3.0; leftover $135.41 |
