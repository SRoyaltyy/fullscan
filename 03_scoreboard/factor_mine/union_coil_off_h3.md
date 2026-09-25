# Factor mine action — `union_coil_off_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ coil_off, no 🚨

Cash book **-23.12%** ($7,688) · signal-only (no cash/fees) was -24.98%. Starts YES **1/30**. Fills 180 · skips 306 · realized $-1766.84.

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
- Must-have: prior relative volume is at least 0.7.
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
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 3 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $6,868.84.

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
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 98 | $50.62 | $2.28 | — | $5,036.64 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ⚪; ret5=+6.2; leftover $5000.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 227 | $22.01 | $2.93 | — | $37.44 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ⚪; ret5=+0.3; leftover $5000.00 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $37.44 | ▲ close $10,677.03 vs 09:30 $10,000.00 (session +682.25) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $37.44 | ▲ 09:30 equity $10,751.77 vs yday $10,677.03 (+74.74) | — | — |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 4 | $0.94 | $0.05 | — | $33.65 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+0.5; leftover $4.68 | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 3 | $1.50 | $0.05 | — | $29.09 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+9.2; leftover $4.68 | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 1 | $4.31 | $0.05 | — | $24.74 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $4.68 | — |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 1 | $4.18 | $0.04 | — | $20.51 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $4.68 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20.51 | ▼ close $10,461.99 vs 09:30 $10,751.77 (session -289.59) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20.51 | ▼ 09:30 equity $10,399.63 vs yday $10,461.99 (-62.36) | — | — |
| 2026-08-17 09:30 ET | **BUY** | `IQ` | 1 | $1.35 | $0.02 | — | $19.15 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list overnight; ⚪; ret5=+1.5; leftover $2.56 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19.15 | ▼ close $10,334.31 vs 09:30 $10,399.63 (session -65.31) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19.15 | ▼ 09:30 equity $10,290.86 vs yday $10,334.31 (-43.45) | — | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 98 | $51.77 | $2.34 | $+107.76 | $5,090.26 | ▲ +107.76 after sell → book $10,288.51; vs 09:30 mark -2.35 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `VOR` | 227 | $22.82 | $3.01 | $+177.93 | $10,267.40 | ▲ +177.93 after sell → book $10,285.51; vs 09:30 mark -3.00 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,267.40 | ▼ close $10,285.12 vs 09:30 $10,290.86 (session -0.39) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,267.40 | ▼ 09:30 equity $10,285.03 vs yday $10,285.12 (-0.09) | — | — |
| 2026-08-19 09:30 ET | **SELL** | `LDI` | 4 | $0.88 | $0.07 | $-0.34 | $10,270.85 | ▼ -0.34 after sell → book $10,284.96; vs 09:30 mark -0.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 3 | $1.42 | $0.07 | $-0.37 | $10,275.04 | ▼ -0.37 after sell → book $10,284.89; vs 09:30 mark -0.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ANGX` | 1 | $4.79 | $0.07 | $+0.36 | $10,279.76 | ▲ +0.36 after sell → book $10,284.82; vs 09:30 mark -0.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `HYLN` | 1 | $3.87 | $0.06 | $-0.42 | $10,283.57 | ▼ -0.42 after sell → book $10,284.76; vs 09:30 mark -0.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $10,283.57 | ▼ close $10,284.70 vs 09:30 $10,285.03 (session -0.06) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,283.57 | ▼ 09:30 equity $10,284.69 vs yday $10,284.70 (-0.01) | — | — |
| 2026-08-20 09:30 ET | **SELL** | `IQ` | 1 | $1.12 | $0.03 | $-0.28 | $10,284.65 | ▼ -0.28 after sell → book $10,284.65; vs 09:30 mark -0.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 62 | $20.55 | $2.18 | — | $9,008.38 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1285.58 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 14 | $91.01 | $2.03 | — | $7,732.20 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1285.58 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 222 | $5.77 | $2.86 | — | $6,448.40 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1285.58 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 65 | $19.63 | $2.19 | — | $5,170.26 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1285.58 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 43 | $29.63 | $2.12 | — | $3,894.06 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1285.58 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 734 | $1.75 | $9.47 | — | $2,600.09 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1285.58 | — |
| 2026-08-20 09:30 ET | **BUY** | `DNA` | 172 | $7.45 | $2.51 | — | $1,316.18 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ret5=+6.9; leftover $1285.58 | — |
| 2026-08-20 09:30 ET | **BUY** | `EXK` | 119 | $10.77 | $2.35 | — | $32.20 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+3.6; leftover $1285.58 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $32.20 | ▲ close $10,364.38 vs 09:30 $10,284.69 (session +105.43) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $32.20 | ▲ 09:30 equity $10,630.98 vs yday $10,364.38 (+266.60) | — | — |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 2 | $1.66 | $0.04 | — | $28.84 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $4.03 | — |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 4 | $0.86 | $0.05 | — | $25.34 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,ohlc_hot; ret5=+9.7; leftover $4.03 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $25.34 | ▼ close $10,617.70 vs 09:30 $10,630.98 (session -13.20) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $25.34 | ▲ 09:30 equity $10,705.78 vs yday $10,617.70 (+88.08) | — | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $25.34 | ▼ close $10,584.78 vs 09:30 $10,705.78 (session -121.00) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $25.34 | ▼ 09:30 equity $10,460.27 vs yday $10,584.78 (-124.51) | — | — |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 62 | $20.32 | $2.20 | $-18.63 | $1,282.99 | ▼ -18.63 after sell → book $10,458.08; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 14 | $95.86 | $2.05 | $+63.82 | $2,622.97 | ▲ +63.82 after sell → book $10,456.02; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 222 | $5.53 | $2.91 | $-59.05 | $3,847.72 | ▼ -59.05 after sell → book $10,453.11; vs 09:30 mark -2.91 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 65 | $21.21 | $2.21 | $+98.31 | $5,224.17 | ▲ +98.31 after sell → book $10,450.91; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 43 | $32.32 | $2.14 | $+111.41 | $6,611.79 | ▲ +111.41 after sell → book $10,448.77; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 734 | $1.90 | $9.60 | $+91.03 | $7,996.78 | ▲ +91.03 after sell → book $10,439.16; vs 09:30 mark -9.61 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `DNA` | 172 | $6.94 | $2.54 | $-92.77 | $9,187.92 | ▼ -92.77 after sell → book $10,436.62; vs 09:30 mark -2.54 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `EXK` | 119 | $10.44 | $2.38 | $-43.99 | $10,427.90 | ▼ -43.99 after sell → book $10,434.24; vs 09:30 mark -2.38 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 118 | $10.98 | $2.34 | — | $9,129.92 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ret5=+1.2; leftover $1303.49 | — |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 263 | $4.94 | $3.39 | — | $7,827.31 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ret5=+7.1; leftover $1303.49 | — |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 3 | $426.97 | $2.00 | — | $6,544.40 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ret5=+6.0; leftover $1303.49 | — |
| 2026-08-25 09:30 ET | **BUY** | `KURA` | 95 | $13.59 | $2.27 | — | $5,251.07 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1303.49 | — |
| 2026-08-25 09:30 ET | **BUY** | `LIFE` | 35 | $36.96 | $2.10 | — | $3,955.38 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+4.4; leftover $1303.49 | — |
| 2026-08-25 09:30 ET | **BUY** | `AMTX` | 686 | $1.90 | $8.85 | — | $2,643.13 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_mover; ⚪; ret5=+5.0; leftover $1303.49 | — |
| 2026-08-25 09:30 ET | **BUY** | `AVAH` | 95 | $13.62 | $2.27 | — | $1,346.48 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot,mover_buy; 🔵; ⚪; ret5=+8.0; leftover $1303.49 | — |
| 2026-08-25 09:30 ET | **BUY** | `ETON` | 20 | $64.55 | $2.05 | — | $53.43 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+4.4; leftover $1303.49 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $53.43 | ▲ close $10,450.50 vs 09:30 $10,460.27 (session +41.53) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $53.43 | ▼ 09:30 equity $10,445.28 vs yday $10,450.50 (-5.22) | — | — |
| 2026-08-26 09:30 ET | **SELL** | `BTBT` | 2 | $1.53 | $0.06 | $-0.36 | $56.43 | ▼ -0.36 after sell → book $10,445.22; vs 09:30 mark -0.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **SELL** | `ORBS` | 4 | $0.80 | $0.06 | $-0.38 | $59.55 | ▼ -0.38 after sell → book $10,445.16; vs 09:30 mark -0.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-26 09:30 ET | **BUY** | `ACRS` | 1 | $6.53 | $0.07 | — | $52.95 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ret5=+3.6; leftover $8.51 | — |
| 2026-08-26 09:30 ET | **BUY** | `TMCI` | 1 | $4.78 | $0.05 | — | $48.12 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ret5=+8.1; leftover $8.51 | — |
| 2026-08-26 09:30 ET | **BUY** | `CRDL` | 4 | $2.03 | $0.09 | — | $39.91 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; ret5=+5.5; leftover $8.51 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $39.91 | ▼ close $10,382.77 vs 09:30 $10,445.28 (session -62.18) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $39.91 | ▼ 09:30 equity $10,359.54 vs yday $10,382.77 (-23.23) | — | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $39.91 | ▲ close $10,385.05 vs 09:30 $10,359.54 (session +25.51) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $39.91 | ▲ 09:30 equity $10,408.88 vs yday $10,385.05 (+23.83) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `OCUL` | 118 | $10.97 | $2.37 | $-5.90 | $1,331.99 | ▼ -5.90 after sell → book $10,406.50; vs 09:30 mark -2.38 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `RZLT` | 263 | $4.95 | $3.45 | $-4.21 | $2,630.40 | ▼ -4.21 after sell → book $10,403.06; vs 09:30 mark -3.44 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `HCA` | 3 | $423.76 | $2.02 | $-13.65 | $3,899.66 | ▼ -13.65 after sell → book $10,401.04; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `KURA` | 95 | $13.05 | $2.30 | $-55.88 | $5,137.11 | ▼ -55.88 after sell → book $10,398.74; vs 09:30 mark -2.30 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `LIFE` | 35 | $39.60 | $2.12 | $+88.19 | $6,520.99 | ▲ +88.19 after sell → book $10,396.62; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `AMTX` | 686 | $1.89 | $8.97 | $-24.68 | $7,808.56 | ▼ -24.68 after sell → book $10,387.65; vs 09:30 mark -8.97 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `AVAH` | 95 | $13.90 | $2.30 | $+21.55 | $9,126.76 | ▲ +21.55 after sell → book $10,385.35; vs 09:30 mark -2.30 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ETON` | 20 | $61.98 | $2.07 | $-55.52 | $10,364.29 | ▼ -55.52 after sell → book $10,383.28; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `CRK` | 101 | $14.63 | $2.29 | — | $8,884.36 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ret5=+5.8; leftover $1480.61 | — |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 94 | $15.66 | $2.27 | — | $7,410.05 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.7; leftover $1480.61 | — |
| 2026-08-28 09:30 ET | **BUY** | `TTMI` | 12 | $122.81 | $2.03 | — | $5,934.31 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+9.0; leftover $1480.61 | — |
| 2026-08-28 09:30 ET | **BUY** | `EQ` | 601 | $2.46 | $7.75 | — | $4,448.09 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; ret5=+7.9; leftover $1480.61 | — |
| 2026-08-28 09:30 ET | **BUY** | `BTSG` | 24 | $60.54 | $2.06 | — | $2,993.07 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; ret5=+2.3; leftover $1480.61 | — |
| 2026-08-28 09:30 ET | **BUY** | `ZYME` | 51 | $28.91 | $2.14 | — | $1,516.52 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; ret5=+9.2; leftover $1480.61 | — |
| 2026-08-28 09:30 ET | **BUY** | `ADBT` | 296 | $4.99 | $3.82 | — | $35.66 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_mover; ret5=+0.0; leftover $1480.61 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $35.66 | ▼ close $10,050.03 vs 09:30 $10,408.88 (session -310.88) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $35.66 | ▼ 09:30 equity $10,045.70 vs yday $10,050.03 (-4.33) | — | — |
| 2026-08-31 09:30 ET | **SELL** | `ACRS` | 1 | $5.97 | $0.08 | $-0.71 | $41.55 | ▼ -0.71 after sell → book $10,045.62; vs 09:30 mark -0.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `TMCI` | 1 | $4.60 | $0.07 | $-0.30 | $46.08 | ▼ -0.30 after sell → book $10,045.55; vs 09:30 mark -0.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 09:30 ET | **SELL** | `CRDL` | 4 | $1.92 | $0.11 | $-0.64 | $53.65 | ▼ -0.64 after sell → book $10,045.44; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $53.65 | ▼ close $9,961.50 vs 09:30 $10,045.70 (session -83.94) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $53.65 | ▲ 09:30 equity $10,008.10 vs yday $9,961.50 (+46.60) | — | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $53.65 | ▼ close $9,730.59 vs 09:30 $10,008.10 (session -277.51) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $53.65 | ▼ 09:30 equity $9,659.07 vs yday $9,730.59 (-71.52) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `CRK` | 101 | $15.70 | $2.32 | $+103.45 | $1,637.03 | ▲ +103.45 after sell → book $9,656.75; vs 09:30 mark -2.32 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `GRRR` | 94 | $13.92 | $2.30 | $-168.13 | $2,943.21 | ▼ -168.13 after sell → book $9,654.45; vs 09:30 mark -2.30 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `TTMI` | 12 | $114.22 | $2.05 | $-107.15 | $4,311.80 | ▼ -107.15 after sell → book $9,652.40; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `EQ` | 601 | $2.20 | $7.86 | $-171.88 | $5,626.14 | ▼ -171.88 after sell → book $9,644.54; vs 09:30 mark -7.86 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BTSG` | 24 | $59.16 | $2.08 | $-37.27 | $7,043.89 | ▼ -37.27 after sell → book $9,642.45; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ZYME` | 51 | $30.00 | $2.17 | $+51.28 | $8,571.73 | ▲ +51.28 after sell → book $9,640.29; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ADBT` | 296 | $3.61 | $3.88 | $-416.18 | $9,636.41 | ▼ -416.18 after sell → book $9,636.41; vs 09:30 mark -3.88 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,636.41 | ▲ close $9,636.41 vs 09:30 $9,659.07 (session +0.00) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,636.41 | ▲ 09:30 equity $9,636.41 vs yday $9,636.41 (+0.00) | — | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 22 | $52.88 | $2.06 | — | $8,471.00 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1204.55 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 28 | $42.93 | $2.07 | — | $7,266.88 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1204.55 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 331 | $3.63 | $4.27 | — | $6,061.08 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1204.55 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $132.45 | $2.02 | — | $4,867.02 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1204.55 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 77 | $15.45 | $2.22 | — | $3,675.14 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+7.2; leftover $1204.55 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 71 | $16.77 | $2.20 | — | $2,482.27 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1204.55 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 552 | $2.18 | $7.12 | — | $1,271.79 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $1204.55 | — |
| 2026-09-03 09:30 ET | **BUY** | `SDGR` | 57 | $21.03 | $2.16 | — | $70.92 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ⚪; ret5=+7.2; leftover $1204.55 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $70.92 | ▼ close $9,353.37 vs 09:30 $9,636.41 (session -258.92) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $70.92 | ▼ 09:30 equity $9,321.80 vs yday $9,353.37 (-31.57) | — | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 5 | $2.52 | $0.14 | — | $58.18 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $14.18 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 2 | $6.71 | $0.14 | — | $44.62 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $14.18 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 2 | $4.78 | $0.10 | — | $34.96 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $14.18 | — |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 1 | $11.31 | $0.12 | — | $23.53 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $14.18 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $23.53 | ▲ close $9,363.76 vs 09:30 $9,321.80 (session +42.45) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $23.53 | ▲ 09:30 equity $9,375.50 vs yday $9,363.76 (+11.74) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $23.53 | ▼ close $9,298.71 vs 09:30 $9,375.50 (session -76.79) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $23.53 | ▼ 09:30 equity $9,255.09 vs yday $9,298.71 (-43.62) | — | — |
| 2026-09-09 09:30 ET | **SELL** | `ATRC` | 22 | $53.16 | $2.08 | $+2.03 | $1,190.97 | ▲ +2.03 after sell → book $9,253.01; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `HRMY` | 28 | $42.01 | $2.09 | $-29.93 | $2,365.16 | ▼ -29.93 after sell → book $9,250.92; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CABA` | 331 | $3.28 | $4.33 | $-124.45 | $3,446.51 | ▼ -124.45 after sell → book $9,246.59; vs 09:30 mark -4.33 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `RVTY` | 9 | $125.77 | $2.04 | $-64.17 | $4,576.40 | ▼ -64.17 after sell → book $9,244.55; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CRK` | 77 | $15.16 | $2.24 | $-26.79 | $5,741.47 | ▼ -26.79 after sell → book $9,242.30; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `ARCT` | 71 | $15.46 | $2.22 | $-97.44 | $6,836.91 | ▼ -97.44 after sell → book $9,240.08; vs 09:30 mark -2.22 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `CRDL` | 552 | $2.22 | $7.22 | $+7.74 | $8,055.13 | ▲ +7.74 after sell → book $9,232.86; vs 09:30 mark -7.22 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 09:30 ET | **SELL** | `SDGR` | 57 | $19.88 | $2.18 | $-69.89 | $9,186.11 | ▼ -69.89 after sell → book $9,230.68; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,186.11 | ▼ close $9,228.62 vs 09:30 $9,255.09 (session -2.06) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,186.11 | ▼ 09:30 equity $9,227.84 vs yday $9,228.62 (-0.78) | — | — |
| 2026-09-10 09:30 ET | **SELL** | `ALEC` | 5 | $2.22 | $0.15 | $-1.79 | $9,197.06 | ▼ -1.79 after sell → book $9,227.69; vs 09:30 mark -0.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `BHC` | 2 | $6.11 | $0.15 | $-1.49 | $9,209.13 | ▼ -1.49 after sell → book $9,227.55; vs 09:30 mark -0.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `OABI` | 2 | $3.92 | $0.10 | $-1.92 | $9,216.87 | ▼ -1.92 after sell → book $9,227.44; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 09:30 ET | **SELL** | `VIR` | 1 | $10.57 | $0.13 | $-0.98 | $9,227.31 | ▼ -0.98 after sell → book $9,227.31; vs 09:30 mark -0.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $9,227.31 | ▲ close $9,227.31 vs 09:30 $9,227.84 (session +0.00) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,227.31 | ▲ 09:30 equity $9,227.31 vs yday $9,227.31 (+0.00) | — | — |
| 2026-09-11 09:30 ET | **BUY** | `NVT` | 7 | $157.78 | $2.01 | — | $8,120.84 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ret5=+4.7; leftover $1153.41 | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 565 | $2.04 | $7.29 | — | $6,960.95 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $1153.41 | — |
| 2026-09-11 09:30 ET | **BUY** | `CLOV` | 242 | $4.75 | $3.12 | — | $5,808.33 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.5; leftover $1153.41 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 544 | $2.12 | $7.02 | — | $4,648.03 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $1153.41 | — |
| 2026-09-11 09:30 ET | **BUY** | `FUBO` | 99 | $11.55 | $2.29 | — | $3,502.30 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $1153.41 | — |
| 2026-09-11 09:30 ET | **BUY** | `VIST` | 14 | $77.33 | $2.03 | — | $2,417.65 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; ret5=+2.5; leftover $1153.41 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAND` | 21 | $52.55 | $2.05 | — | $1,312.04 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ⚪; ret5=+8.6; leftover $1153.41 | — |
| 2026-09-11 09:30 ET | **BUY** | `PAGS` | 114 | $10.11 | $2.33 | — | $157.17 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ⚪; ret5=+2.9; leftover $1153.41 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $157.17 | ▲ close $9,284.64 vs 09:30 $9,227.31 (session +85.47) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $157.17 | ▼ 09:30 equity $9,183.20 vs yday $9,284.64 (-101.44) | — | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $157.17 | ▼ close $9,018.30 vs 09:30 $9,183.20 (session -164.90) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $157.17 | ▲ 09:30 equity $9,034.39 vs yday $9,018.30 (+16.09) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $157.17 | ▼ close $8,859.53 vs 09:30 $9,034.39 (session -174.86) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $157.17 | ▼ 09:30 equity $8,634.98 vs yday $8,859.53 (-224.55) | — | — |
| 2026-09-16 09:30 ET | **SELL** | `NVT` | 7 | $147.79 | $2.03 | $-73.97 | $1,189.67 | ▼ -73.97 after sell → book $8,632.95; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `AMTX` | 565 | $1.89 | $7.39 | $-99.43 | $2,250.13 | ▼ -99.43 after sell → book $8,625.56; vs 09:30 mark -7.39 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `CLOV` | 242 | $4.73 | $3.17 | $-11.13 | $3,391.62 | ▼ -11.13 after sell → book $8,622.39; vs 09:30 mark -3.17 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `BAK` | 544 | $1.84 | $7.12 | $-166.46 | $4,385.46 | ▼ -166.46 after sell → book $8,615.27; vs 09:30 mark -7.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `FUBO` | 99 | $10.75 | $2.31 | $-83.80 | $5,447.39 | ▼ -83.80 after sell → book $8,612.95; vs 09:30 mark -2.32 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `VIST` | 14 | $76.75 | $2.05 | $-12.20 | $6,519.84 | ▼ -12.20 after sell → book $8,610.90; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `BAND` | 21 | $48.60 | $2.07 | $-87.08 | $7,538.37 | ▼ -87.08 after sell → book $8,608.83; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **SELL** | `PAGS` | 114 | $9.39 | $2.36 | $-86.77 | $8,606.47 | ▼ -86.77 after sell → book $8,606.47; vs 09:30 mark -2.36 | dropped from list after 3 sess (min 3) | — |
| 2026-09-16 09:30 ET | **BUY** | `IQV` | 3 | $270.89 | $2.00 | — | $7,791.80 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ret5=+4.0; leftover $1075.81 | — |
| 2026-09-16 09:30 ET | **BUY** | `AVAH` | 75 | $14.31 | $2.21 | — | $6,716.33 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ret5=+4.8; leftover $1075.81 | — |
| 2026-09-16 09:30 ET | **BUY** | `BLFS` | 29 | $36.46 | $2.08 | — | $5,656.92 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ret5=+2.9; leftover $1075.81 | — |
| 2026-09-16 09:30 ET | **BUY** | `TEM` | 15 | $68.79 | $2.04 | — | $4,623.03 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+7.1; leftover $1075.81 | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 183 | $5.87 | $2.54 | — | $3,546.28 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $1075.81 | — |
| 2026-09-16 09:30 ET | **BUY** | `VAL` | 12 | $87.40 | $2.03 | — | $2,495.46 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+3.2; leftover $1075.81 | — |
| 2026-09-16 09:30 ET | **BUY** | `MRCY` | 12 | $87.52 | $2.03 | — | $1,443.19 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ret5=+4.3; leftover $1075.81 | — |
| 2026-09-16 09:30 ET | **BUY** | `QLYS` | 5 | $179.60 | $2.00 | — | $543.19 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; ret5=+8.8; leftover $1075.81 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $543.19 | ▼ close $8,482.25 vs 09:30 $8,634.98 (session -107.30) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $543.19 | ▲ 09:30 equity $8,586.20 vs yday $8,482.25 (+103.95) | — | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 8 | $7.59 | $0.63 | — | $481.83 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $67.90 | — |
| 2026-09-17 09:30 ET | **BUY** | `AMN` | 1 | $34.93 | $0.35 | — | $446.55 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ret5=+1.6; leftover $67.90 | — |
| 2026-09-17 09:30 ET | **BUY** | `SABR` | 28 | $2.40 | $0.76 | — | $378.60 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.4; leftover $67.90 | — |
| 2026-09-17 09:30 ET | **BUY** | `AIB` | 46 | $1.46 | $0.81 | — | $310.63 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ret5=+4.4; leftover $67.90 | — |
| 2026-09-17 09:30 ET | **BUY** | `BYND` | 6 | $11.19 | $0.69 | — | $242.80 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ret5=+0.5; leftover $67.90 | — |
| 2026-09-17 09:30 ET | **BUY** | `QTRX` | 23 | $2.94 | $0.75 | — | $174.43 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer,ohlc_hot; 🔵; ret5=+9.8; leftover $67.90 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $174.43 | ▲ close $8,642.32 vs 09:30 $8,586.20 (session +60.11) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $174.43 | ▲ 09:30 equity $8,666.31 vs yday $8,642.32 (+23.99) | — | — |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 1 | $14.07 | $0.14 | — | $160.22 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $21.80 | — |
| 2026-09-18 09:30 ET | **BUY** | `DDD` | 6 | $3.58 | $0.23 | — | $138.51 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+8.3; leftover $21.80 | — |
| 2026-09-18 09:30 ET | **BUY** | `RANI` | 25 | $0.85 | $0.29 | — | $116.97 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; ret5=+3.6; leftover $21.80 | — |
| 2026-09-18 09:30 ET | **BUY** | `SHLS` | 2 | $7.64 | $0.16 | — | $101.53 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ret5=+7.6; leftover $21.80 | — |
| 2026-09-18 09:30 ET | **BUY** | `XE` | 1 | $16.28 | $0.17 | — | $85.08 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ret5=+3.1; leftover $21.80 | — |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 1 | $20.91 | $0.21 | — | $63.96 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $21.80 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $63.96 | ▼ close $8,531.20 vs 09:30 $8,666.31 (session -133.91) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $63.96 | ▲ 09:30 equity $8,565.01 vs yday $8,531.20 (+33.81) | — | — |
| 2026-09-21 09:30 ET | **SELL** | `IQV` | 3 | $266.76 | $2.02 | $-16.41 | $862.22 | ▼ -16.41 after sell → book $8,562.99; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `AVAH` | 75 | $13.65 | $2.24 | $-53.95 | $1,883.74 | ▼ -53.95 after sell → book $8,560.76; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `BLFS` | 29 | $36.70 | $2.10 | $+2.79 | $2,945.94 | ▲ +2.79 after sell → book $8,558.66; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `TEM` | 15 | $79.08 | $2.06 | $+150.26 | $4,130.08 | ▲ +150.26 after sell → book $8,556.60; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `RIG` | 183 | $5.62 | $2.58 | $-50.87 | $5,155.96 | ▼ -50.87 after sell → book $8,554.02; vs 09:30 mark -2.58 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `VAL` | 12 | $83.46 | $2.05 | $-51.35 | $6,155.44 | ▼ -51.35 after sell → book $8,551.98; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `MRCY` | 12 | $86.52 | $2.05 | $-16.07 | $7,191.63 | ▼ -16.07 after sell → book $8,549.93; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **SELL** | `QLYS` | 5 | $175.63 | $2.02 | $-23.88 | $8,067.76 | ▼ -23.88 after sell → book $8,547.91; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-21 09:30 ET | **BUY** | `A` | 6 | $157.87 | $2.01 | — | $7,118.53 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ret5=+6.5; leftover $1008.47 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTDR` | 74 | $13.47 | $2.21 | — | $6,119.17 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+8.4; leftover $1008.47 | — |
| 2026-09-21 09:30 ET | **BUY** | `ORBS` | 908 | $1.11 | $11.71 | — | $5,099.57 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; ret5=+6.9; leftover $1008.47 | — |
| 2026-09-21 09:30 ET | **BUY** | `SBET` | 100 | $9.99 | $2.29 | — | $4,098.28 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+5.3; leftover $1008.47 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTBT` | 552 | $1.82 | $7.12 | — | $3,083.76 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+8.8; leftover $1008.47 | — |
| 2026-09-21 09:30 ET | **BUY** | `SGML` | 99 | $10.13 | $2.29 | — | $2,078.11 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; ret5=+4.9; leftover $1008.47 | — |
| 2026-09-21 09:30 ET | **BUY** | `GLXY` | 38 | $25.95 | $2.10 | — | $1,089.91 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $1008.47 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,089.91 | ▼ close $8,473.78 vs 09:30 $8,565.01 (session -44.39) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,089.91 | ▼ 09:30 equity $8,453.11 vs yday $8,473.78 (-20.67) | — | — |
| 2026-09-22 09:30 ET | **BUY** | `ALOY` | 16 | $9.40 | $1.55 | — | $937.95 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; ret5=+9.5; leftover $155.70 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $937.95 | ▲ close $8,622.34 vs 09:30 $8,453.11 (session +170.78) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $937.95 | ▼ 09:30 equity $8,605.51 vs yday $8,622.34 (-16.83) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `PGEN` | 8 | $7.95 | $0.68 | $+1.57 | $1,000.87 | ▲ +1.57 after sell → book $8,604.83; vs 09:30 mark -0.68 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `AMN` | 1 | $34.78 | $0.37 | $-0.87 | $1,035.28 | ▼ -0.87 after sell → book $8,604.46; vs 09:30 mark -0.37 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `SABR` | 28 | $2.24 | $0.73 | $-5.97 | $1,097.27 | ▼ -5.97 after sell → book $8,603.73; vs 09:30 mark -0.73 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `AIB` | 46 | $1.37 | $0.79 | $-5.74 | $1,159.50 | ▼ -5.74 after sell → book $8,602.94; vs 09:30 mark -0.79 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `BYND` | 6 | $11.01 | $0.70 | $-2.47 | $1,224.87 | ▼ -2.47 after sell → book $8,602.25; vs 09:30 mark -0.69 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `QTRX` | 23 | $3.17 | $0.82 | $+3.73 | $1,296.96 | ▲ +3.73 after sell → book $8,601.43; vs 09:30 mark -0.82 | dropped from list after 4 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `BHVN` | 1 | $14.84 | $0.17 | $+0.45 | $1,311.63 | ▲ +0.45 after sell → book $8,601.26; vs 09:30 mark -0.17 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `DDD` | 6 | $3.59 | $0.25 | $-0.43 | $1,332.91 | ▼ -0.43 after sell → book $8,601.00; vs 09:30 mark -0.26 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `RANI` | 25 | $0.81 | $0.30 | $-1.59 | $1,352.87 | ▼ -1.59 after sell → book $8,600.71; vs 09:30 mark -0.29 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `SHLS` | 2 | $8.15 | $0.19 | $+0.67 | $1,368.98 | ▲ +0.67 after sell → book $8,600.52; vs 09:30 mark -0.19 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `XE` | 1 | $16.17 | $0.18 | $-0.46 | $1,384.96 | ▼ -0.46 after sell → book $8,600.33; vs 09:30 mark -0.19 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **SELL** | `TH` | 1 | $21.15 | $0.23 | $-0.21 | $1,405.88 | ▼ -0.21 after sell → book $8,600.10; vs 09:30 mark -0.23 | dropped from list after 3 sess (min 3) | — |
| 2026-09-23 09:30 ET | **BUY** | `DXCM` | 1 | $89.50 | $0.90 | — | $1,315.48 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+5.3; leftover $175.73 | — |
| 2026-09-23 09:30 ET | **BUY** | `HALO` | 1 | $116.85 | $1.17 | — | $1,197.46 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+3.3; leftover $175.73 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 17 | $9.81 | $1.72 | — | $1,028.97 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+4.0; leftover $175.73 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 8 | $20.65 | $1.68 | — | $862.09 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $175.73 | — |
| 2026-09-23 09:30 ET | **BUY** | `INDP` | 44 | $3.93 | $1.86 | — | $687.31 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $175.73 | — |
| 2026-09-23 09:30 ET | **BUY** | `MNRO` | 12 | $14.14 | $1.73 | — | $515.90 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; ret5=+0.3; leftover $175.73 | — |
| 2026-09-23 09:30 ET | **BUY** | `NTSK` | 9 | $18.57 | $1.70 | — | $347.03 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+8.2; leftover $175.73 | — |
| 2026-09-23 09:30 ET | **BUY** | `HIMS` | 5 | $30.40 | $1.53 | — | $193.49 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+9.3; leftover $175.73 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $193.49 | ▼ close $8,313.66 vs 09:30 $8,605.51 (session -274.14) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $193.49 | ▼ 09:30 equity $8,202.34 vs yday $8,313.66 (-111.32) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `A` | 6 | $163.95 | $2.03 | $+32.44 | $1,175.16 | ▲ +32.44 after sell → book $8,200.31; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `BTDR` | 74 | $12.26 | $2.23 | $-94.36 | $2,080.17 | ▼ -94.36 after sell → book $8,198.07; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `ORBS` | 908 | $1.05 | $11.87 | $-78.07 | $3,021.69 | ▼ -78.07 after sell → book $8,186.20; vs 09:30 mark -11.87 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `SBET` | 100 | $9.80 | $2.32 | $-23.61 | $3,999.38 | ▼ -23.61 after sell → book $8,183.88; vs 09:30 mark -2.32 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `BTBT` | 552 | $1.73 | $7.22 | $-69.54 | $4,944.36 | ▼ -69.54 after sell → book $8,176.66; vs 09:30 mark -7.22 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `SGML` | 99 | $9.89 | $2.31 | $-28.86 | $5,921.15 | ▼ -28.86 after sell → book $8,174.35; vs 09:30 mark -2.31 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 09:30 ET | **SELL** | `GLXY` | 38 | $25.00 | $2.12 | $-40.52 | $6,868.84 | ▼ -40.52 after sell → book $8,172.22; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $6,868.84 | ▲ close $8,188.02 vs 09:30 $8,202.34 (session +15.80) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6,325.95 | ▼ 09:30 equity $7,700.20 vs yday $7,700.34 (-0.14) | 09:30 open · cash $6,325.95 (unchanged overnight, no fees) · equity $7,700.20 vs prior close $7,700.34 (-0.14) · 8 name(s) re-marked at the open (per-name table). ANAB×4 yday $51.70 → 09:30 $51.70 +0.00; APPS×18 yday $10.88 → 09:30 $10.88 +0.00; ARHS×25 yday $9.47 → 09:30 $9.47 +0.00; BTQ×78 yday $2.79 → 09:30 $2.79 +0.00; INDP×12 yday $4.00 → 09:30 $4.00 +0.00; NN×14 yday $14.45 → 09:30 $14.45 +0.00; NTSK×12 yday $18.57 → 09:30 $18.57 +0.00; SAIL×2 yday $22.12 → 09:30 $22.05 -0.14 | — |
| 2026-09-25 09:30 ET | **BUY** | `HALO` | 6 | $115.36 | $2.01 | — | $5,631.78 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+5.1; leftover $790.74 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `BLFS` | 20 | $38.51 | $2.05 | — | $4,859.53 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ret5=+4.7; leftover $790.74 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 103 | $7.65 | $2.30 | — | $4,069.28 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+5.2; leftover $790.74 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `TXG` | 9 | $83.76 | $2.02 | — | $3,313.43 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $790.74 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `HLP` | 359 | $2.20 | $4.63 | — | $2,518.99 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,ohlc_hot; 🔵; ret5=+8.9; leftover $790.74 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SATL` | 131 | $6.00 | $2.38 | — | $1,730.61 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,ohlc_hot; 🔵; ret5=+7.8; leftover $790.74 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `PL` | 44 | $17.91 | $2.12 | — | $940.45 | — | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable; 🔵; ret5=+3.7; leftover $790.74 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $940.45 | ▲ close $7,687.85 vs 09:30 $7,700.20 (session +5.16) | 16:00 close · cash $940.45 · equity $7,687.85 vs 09:30 $7,700.20 (-12.35; session marks +5.16) · 15 name(s) marked open→close (per-name table). ANAB×4 09:30 $51.70 → close $51.70 +0.00; APPS×18 09:30 $10.88 → close $10.88 +0.00; ARHS×25 09:30 $9.47 → close $9.47 +0.00; BTQ×78 09:30 $2.79 → close $2.79 -0.00; INDP×12 09:30 $4.00 → close $4.00 +0.00; NN×14 09:30 $14.45 → close $14.45 -0.00; NTSK×12 09:30 $18.57 → close $18.57 -0.00; SAIL×2 09:30 $22.05 → close $20.64 -2.82; HALO×6 09:30 $115.36 → close $113.90 -8.76; BLFS×20 09:30 $38.51 → close $38.49 -0.40; MRVI×103 09:30 $7.65 → close $7.60 -5.15; TXG×9 09:30 $83.76 → close $85.71 +17.55; HLP×359 09:30 $2.20 → close $2.21 +3.59; SATL×131 09:30 $6.00 → close $6.17 +22.27; PL×44 09:30 $17.91 → close $17.43 -21.12 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `VOR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TLN` | cash | leftover split 4.68 < 1 share @ 359.83 |
| 2026-08-14 | `SLG` | cash | leftover split 4.68 < 1 share @ 57.61 |
| 2026-08-14 | `WDC` | cash | leftover split 4.68 < 1 share @ 503.50 |
| 2026-08-14 | `ADUR` | cash | leftover split 4.68 < 1 share @ 16.50 |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `VOR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `LDI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `HYLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `DVN` | cash | leftover split 2.56 < 1 share @ 46.18 |
| 2026-08-17 | `DNN` | cash | leftover split 2.56 < 1 share @ 3.24 |
| 2026-08-17 | `OCC` | cash | leftover split 2.56 < 1 share @ 18.24 |
| 2026-08-17 | `ALM` | cash | leftover split 2.56 < 1 share @ 16.20 |
| 2026-08-17 | `NEWP` | cash | leftover split 2.56 < 1 share @ 6.94 |
| 2026-08-17 | `KLAR` | cash | leftover split 2.56 < 1 share @ 20.67 |
| 2026-08-17 | `VNET` | cash | leftover split 2.56 < 1 share @ 7.75 |
| 2026-08-18 | `LDI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ANGX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `HYLN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `IQ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CYPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `PURR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `VNET` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `DVLT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `IQ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `GUTS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNTN` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AEHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MXL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `PAYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `DNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `EXK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CRSP` | cash | leftover split 4.03 < 1 share @ 59.72 |
| 2026-08-21 | `CF` | cash | leftover split 4.03 < 1 share @ 127.43 |
| 2026-08-21 | `EMBC` | cash | leftover split 4.03 < 1 share @ 5.43 |
| 2026-08-21 | `TXG` | cash | leftover split 4.03 < 1 share @ 64.39 |
| 2026-08-21 | `DXYZ` | cash | leftover split 4.03 < 1 share @ 34.89 |
| 2026-08-21 | `BEKE` | cash | leftover split 4.03 < 1 share @ 17.93 |
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
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ABAT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `PDD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BZ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ORBS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `OCUL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `HCA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `KURA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `LIFE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `AMTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `AVAH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ETON` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `INSP` | cash | leftover split 8.51 < 1 share @ 60.07 |
| 2026-08-26 | `CRMD` | cash | leftover split 8.51 < 1 share @ 8.60 |
| 2026-08-26 | `SENS` | cash | leftover split 8.51 < 1 share @ 9.48 |
| 2026-08-26 | `BE` | cash | leftover split 8.51 < 1 share @ 213.94 |
| 2026-08-27 | `OCUL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `HCA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `LIFE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `AMTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `AVAH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ETON` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ACRS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `TMCI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-27 | `RRC` | cash | leftover split 6.65 < 1 share @ 41.44 |
| 2026-08-27 | `CRK` | cash | leftover split 6.65 < 1 share @ 14.42 |
| 2026-08-27 | `MOS` | cash | leftover split 6.65 < 1 share @ 24.00 |
| 2026-08-27 | `AVBP` | cash | leftover split 6.65 < 1 share @ 30.79 |
| 2026-08-27 | `ABX` | cash | leftover split 6.65 < 1 share @ 9.68 |
| 2026-08-27 | `BE` | cash | leftover split 6.65 < 1 share @ 227.10 |
| 2026-08-28 | `ACRS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `TMCI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `GRRR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `TTMI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `EQ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BTSG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ZYME` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ADBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ACDC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FIG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PANW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SAIL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FROG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `CRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `GRRR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `TTMI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `EQ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BTSG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ZYME` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ADBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `RES` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KOS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OIS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `FTI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KMI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OKE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `PCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HRMY` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VSTM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TEL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `CYPH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ARCT` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `NVAX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `SDGR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRM` | cash | leftover split 14.18 < 1 share @ 263.36 |
| 2026-09-08 | `ATRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `HRMY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CABA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `RVTY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `SDGR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-08 | `ALEC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `BHC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `OABI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `VIR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-08 | `GGB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHGG` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `EVGO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `BMNR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `PURR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `HAFN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `ALEC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `BHC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `OABI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `VIR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-09 | `AR` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VET` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `GRNT` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VENU` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `LFMD` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `KEP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `ZIM` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `OBE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `UGP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `DDOG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CHYM` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SUNB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `TNGX` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `TJGC` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-14 | `NVT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `AMTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `CLOV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `BAK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `FUBO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `VIST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `BAND` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `PAGS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DK` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DELL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HTFL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `FPS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `NTAP` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `XRX` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `IMSR` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `NVT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `AMTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `CLOV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `BAK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `FUBO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `VIST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `BAND` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `PAGS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-15 | `ATRC` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `RBRK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `QLYS` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CYPH` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `IVVD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `LFMD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `IOT` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-17 | `IQV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `AVAH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `BLFS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `TEM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `RIG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `VAL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `MRCY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `QLYS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-17 | `SMTC` | cash | leftover split 67.90 < 1 share @ 170.85 |
| 2026-09-17 | `FTAI` | cash | leftover split 67.90 < 1 share @ 196.50 |
| 2026-09-18 | `IQV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `AVAH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `BLFS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `TEM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `RIG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `VAL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `MRCY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `QLYS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-18 | `PGEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `AMN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `SABR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `AIB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `BYND` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `QTRX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-18 | `AMD` | cash | leftover split 21.80 < 1 share @ 547.37 |
| 2026-09-18 | `SYM` | cash | leftover split 21.80 < 1 share @ 44.70 |
| 2026-09-21 | `PGEN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `AMN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `SABR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `AIB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `BYND` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `QTRX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-21 | `BHVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `DDD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `RANI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `SHLS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `XE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `TH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-21 | `SNDK` | cash | leftover split 1008.47 < 1 share @ 1826.00 |
| 2026-09-22 | `PGEN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `AMN` | no_price | no 09:30 open — carry |
| 2026-09-22 | `SABR` | no_price | no 09:30 open — carry |
| 2026-09-22 | `AIB` | no_price | no 09:30 open — carry |
| 2026-09-22 | `BYND` | no_price | no 09:30 open — carry |
| 2026-09-22 | `QTRX` | no_price | no 09:30 open — carry |
| 2026-09-22 | `BHVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `DDD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `RANI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `SHLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `XE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `TH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-22 | `A` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `BTDR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `ORBS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `SBET` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `SGML` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `GLXY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-22 | `APPS` | no_price | no 09:30 open |
| 2026-09-22 | `ARHS` | no_price | no 09:30 open |
| 2026-09-22 | `RMBS` | no_price | no 09:30 open |
| 2026-09-22 | `EMAT` | no_price | no 09:30 open |
| 2026-09-22 | `NTSK` | no_price | no 09:30 open |
| 2026-09-22 | `ANAB` | no_price | no 09:30 open |
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
| 2026-09-24 | `ADMA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `OMER` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `INDP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `MNRO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `HIMS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-24 | `CVE` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `S` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `DH` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `CRWD` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `PBLS` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `OKTA` | hard_red | hard-red S=-7.66 sit; no new buys |
| 2026-09-24 | `RNG` | hard_red | hard-red S=-7.66 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ALOY` | 16 | 2026-09-22 @ $9.40 | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; ret5=+9.5; leftover $155.70 |
| `DXCM` | 1 | 2026-09-23 @ $89.50 | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+5.3; leftover $175.73 |
| `HALO` | 1 | 2026-09-23 @ $116.85 | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+3.3; leftover $175.73 |
| `ADMA` | 17 | 2026-09-23 @ $9.81 | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+4.0; leftover $175.73 |
| `OMER` | 8 | 2026-09-23 @ $20.65 | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $175.73 |
| `INDP` | 44 | 2026-09-23 @ $3.93 | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $175.73 |
| `MNRO` | 12 | 2026-09-23 @ $14.14 | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; ret5=+0.3; leftover $175.73 |
| `NTSK` | 9 | 2026-09-23 @ $18.57 | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+8.2; leftover $175.73 |
| `HIMS` | 5 | 2026-09-23 @ $30.40 | union ∩ coil_off, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+9.3; leftover $175.73 |
