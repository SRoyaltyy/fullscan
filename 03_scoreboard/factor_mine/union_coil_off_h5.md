# Factor mine action — `union_coil_off_h5`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **5** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ coil_off hold 5, no 🚨

Cash book **-13.65%** ($8,635) · signal-only (no cash/fees) was -38.99%. Starts YES **0/30**. Fills 154 · skips 444 · realized $-915.44.

## How this sleeve decides (like you are 10)

Imagine a kid with $10,000 at the 09:30 school bell. They look at the mixed morning shopping list (every name that showed up on any 09:30 list that day) and only buy names that pass every must-have on the checklist and skip anything on the must-not list. They take up to 8 names, spend leftover cash on whole shares, and hold at least 5 morning(s). They sell when the name falls off the list (after the timer). They never peek at today's report card (Change%) to pick. This sleeve bets the price will rise.

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
- Minimum hold is 5 session(s) — the buy morning counts as 1.
- No extra panic button — only the hold timer and the sell rule below.
- List-drop: after 5 session(s), sell at the 09:30 open if the name is no longer on today's list. If it fell off earlier, we still wait out the minimum hold.
- Fills are at the 09:30 open. Fees come out of cash. Overnight, cash does not change.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **5**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $353.48.

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
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 98 | $50.62 | $2.28 | — | $5,036.64 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ⚪; ret5=+6.2; leftover $5000.00 | — |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 227 | $22.01 | $2.93 | — | $37.44 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ⚪; ret5=+0.3; leftover $5000.00 | — |
| 2026-08-13 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $37.44 | ▲ close $10,677.03 vs 09:30 $10,000.00 (session +682.25) | — | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $37.44 | ▲ 09:30 equity $10,751.77 vs yday $10,677.03 (+74.74) | — | — |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 4 | $0.94 | $0.05 | — | $33.65 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+0.5; leftover $4.68 | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 3 | $1.50 | $0.05 | — | $29.09 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+9.2; leftover $4.68 | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 1 | $4.31 | $0.05 | — | $24.74 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $4.68 | — |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 1 | $4.18 | $0.04 | — | $20.51 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $4.68 | — |
| 2026-08-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $20.51 | ▼ close $10,461.99 vs 09:30 $10,751.77 (session -289.59) | — | — |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20.51 | ▼ 09:30 equity $10,399.63 vs yday $10,461.99 (-62.36) | — | — |
| 2026-08-17 09:30 ET | **BUY** | `IQ` | 1 | $1.35 | $0.02 | — | $19.15 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list overnight; ⚪; ret5=+1.5; leftover $2.56 | — |
| 2026-08-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19.15 | ▼ close $10,334.31 vs 09:30 $10,399.63 (session -65.31) | — | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19.15 | ▼ 09:30 equity $10,290.86 vs yday $10,334.31 (-43.45) | — | — |
| 2026-08-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19.15 | ▲ close $10,419.39 vs 09:30 $10,290.86 (session +128.53) | — | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19.15 | ▲ 09:30 equity $10,617.61 vs yday $10,419.39 (+198.22) | — | — |
| 2026-08-19 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $19.15 | ▼ close $10,600.55 vs 09:30 $10,617.61 (session -17.06) | — | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19.15 | ▼ 09:30 equity $10,468.52 vs yday $10,600.55 (-132.03) | — | — |
| 2026-08-20 09:30 ET | **SELL** | `TPG` | 98 | $53.06 | $2.34 | $+234.18 | $5,216.68 | ▲ +234.18 after sell → book $10,466.18; vs 09:30 mark -2.34 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `VOR` | 227 | $23.05 | $3.01 | $+230.14 | $10,446.03 | ▲ +230.14 after sell → book $10,463.17; vs 09:30 mark -3.01 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 63 | $20.55 | $2.18 | — | $9,149.20 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1305.75 | — |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 14 | $91.01 | $2.03 | — | $7,873.02 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1305.75 | — |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 226 | $5.77 | $2.92 | — | $6,566.09 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1305.75 | — |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 66 | $19.63 | $2.19 | — | $5,268.32 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1305.75 | — |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 44 | $29.63 | $2.12 | — | $3,962.48 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1305.75 | — |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 746 | $1.75 | $9.62 | — | $2,647.36 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1305.75 | — |
| 2026-08-20 09:30 ET | **BUY** | `DNA` | 175 | $7.45 | $2.52 | — | $1,341.09 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ret5=+6.9; leftover $1305.75 | — |
| 2026-08-20 09:30 ET | **BUY** | `EXK` | 121 | $10.77 | $2.35 | — | $35.57 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+3.6; leftover $1305.75 | — |
| 2026-08-20 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $35.57 | ▲ close $10,544.00 vs 09:30 $10,468.52 (session +106.76) | — | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $35.57 | ▲ 09:30 equity $10,815.05 vs yday $10,544.00 (+271.05) | — | — |
| 2026-08-21 09:30 ET | **SELL** | `LDI` | 4 | $0.87 | $0.07 | $-0.40 | $38.97 | ▼ -0.40 after sell → book $10,814.98; vs 09:30 mark -0.07 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `ANGX` | 1 | $4.43 | $0.07 | $+0.01 | $43.33 | ▲ +0.01 after sell → book $10,814.91; vs 09:30 mark -0.07 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `HYLN` | 1 | $3.42 | $0.06 | $-0.86 | $46.69 | ▼ -0.86 after sell → book $10,814.85; vs 09:30 mark -0.06 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 7 | $0.86 | $0.08 | — | $40.56 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,ohlc_hot; ret5=+9.7; leftover $6.67 | — |
| 2026-08-21 09:30 ET | **BUY** | `EMBC` | 1 | $5.43 | $0.06 | — | $35.08 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; 🔵; ⚪; ret5=+7.0; leftover $6.67 | — |
| 2026-08-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $35.08 | ▼ close $10,800.92 vs 09:30 $10,815.05 (session -13.80) | — | — |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $35.08 | ▲ 09:30 equity $10,890.46 vs yday $10,800.92 (+89.54) | — | — |
| 2026-08-24 09:30 ET | **SELL** | `BTBT` | 3 | $1.55 | $0.08 | $+0.02 | $39.65 | ▲ +0.02 after sell → book $10,890.39; vs 09:30 mark -0.07 | dropped from list after 6 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `IQ` | 1 | $1.05 | $0.03 | $-0.35 | $40.67 | ▼ -0.35 after sell → book $10,890.35; vs 09:30 mark -0.04 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $40.67 | ▼ close $10,766.95 vs 09:30 $10,890.46 (session -123.41) | — | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $40.67 | ▼ 09:30 equity $10,640.53 vs yday $10,766.95 (-126.42) | — | — |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 1 | $4.94 | $0.05 | — | $35.68 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ret5=+7.1; leftover $5.08 | — |
| 2026-08-25 09:30 ET | **BUY** | `AMTX` | 2 | $1.90 | $0.04 | — | $31.83 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_mover; ⚪; ret5=+5.0; leftover $5.08 | — |
| 2026-08-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $31.83 | ▲ close $11,126.86 vs 09:30 $10,640.53 (session +486.43) | — | — |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $31.83 | ▼ 09:30 equity $10,915.04 vs yday $11,126.86 (-211.82) | — | — |
| 2026-08-26 09:30 ET | **BUY** | `CRDL` | 2 | $2.03 | $0.05 | — | $27.73 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; ret5=+5.5; leftover $4.55 | — |
| 2026-08-26 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $27.73 | ▼ close $10,761.97 vs 09:30 $10,915.04 (session -153.03) | — | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $27.73 | ▲ 09:30 equity $10,803.25 vs yday $10,761.97 (+41.28) | — | — |
| 2026-08-27 09:30 ET | **SELL** | `AG` | 63 | $20.93 | $2.20 | $+19.56 | $1,344.12 | ▲ +19.56 after sell → book $10,801.05; vs 09:30 mark -2.20 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `BHP` | 14 | $95.52 | $2.05 | $+59.06 | $2,679.34 | ▲ +59.06 after sell → book $10,798.99; vs 09:30 mark -2.06 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `HDSN` | 226 | $5.49 | $2.96 | $-69.16 | $3,917.12 | ▼ -69.16 after sell → book $10,796.03; vs 09:30 mark -2.96 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `IAG` | 66 | $21.47 | $2.21 | $+117.04 | $5,331.93 | ▲ +117.04 after sell → book $10,793.82; vs 09:30 mark -2.21 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `KGC` | 44 | $32.32 | $2.14 | $+114.09 | $6,751.87 | ▲ +114.09 after sell → book $10,791.68; vs 09:30 mark -2.14 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `NFGC` | 746 | $1.91 | $9.76 | $+99.98 | $8,166.97 | ▲ +99.98 after sell → book $10,781.92; vs 09:30 mark -9.76 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `DNA` | 175 | $7.34 | $2.55 | $-24.32 | $9,448.91 | ▼ -24.32 after sell → book $10,779.36; vs 09:30 mark -2.56 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `EXK` | 121 | $10.80 | $2.38 | $-1.11 | $10,753.33 | ▼ -1.11 after sell → book $10,776.98; vs 09:30 mark -2.38 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 37 | $41.44 | $2.10 | — | $9,217.95 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ret5=+3.1; leftover $1536.19 | — |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 106 | $14.42 | $2.31 | — | $7,687.12 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ret5=+7.1; leftover $1536.19 | — |
| 2026-08-27 09:30 ET | **BUY** | `MOS` | 64 | $24.00 | $2.18 | — | $6,148.94 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ret5=+8.7; leftover $1536.19 | — |
| 2026-08-27 09:30 ET | **BUY** | `KURA` | 118 | $12.98 | $2.34 | — | $4,614.95 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; ret5=+4.2; leftover $1536.19 | — |
| 2026-08-27 09:30 ET | **BUY** | `AVBP` | 49 | $30.79 | $2.14 | — | $3,104.11 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; ret5=+3.7; leftover $1536.19 | — |
| 2026-08-27 09:30 ET | **BUY** | `ABX` | 158 | $9.68 | $2.46 | — | $1,572.20 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; ret5=+6.9; leftover $1536.19 | — |
| 2026-08-27 09:30 ET | **BUY** | `BE` | 6 | $227.10 | $2.01 | — | $207.60 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ret5=+5.6; leftover $1536.19 | — |
| 2026-08-27 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $207.60 | ▲ close $10,776.55 vs 09:30 $10,803.25 (session +15.12) | — | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $207.60 | ▼ 09:30 equity $10,750.41 vs yday $10,776.55 (-26.14) | — | — |
| 2026-08-28 09:30 ET | **SELL** | `ORBS` | 7 | $0.83 | $0.10 | $-0.39 | $213.33 | ▼ -0.39 after sell → book $10,750.31; vs 09:30 mark -0.10 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `EMBC` | 1 | $5.03 | $0.07 | $-0.53 | $218.29 | ▼ -0.53 after sell → book $10,750.24; vs 09:30 mark -0.07 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 2 | $15.66 | $0.32 | — | $186.65 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ret5=+2.7; leftover $36.38 | — |
| 2026-08-28 09:30 ET | **BUY** | `EQ` | 14 | $2.46 | $0.39 | — | $151.83 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; ret5=+7.9; leftover $36.38 | — |
| 2026-08-28 09:30 ET | **BUY** | `ZYME` | 1 | $28.91 | $0.29 | — | $122.62 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; ret5=+9.2; leftover $36.38 | — |
| 2026-08-28 09:30 ET | **BUY** | `ADBT` | 7 | $4.99 | $0.37 | — | $87.32 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_mover; ret5=+0.0; leftover $36.38 | — |
| 2026-08-28 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $87.32 | ▼ close $10,559.93 vs 09:30 $10,750.41 (session -188.94) | — | — |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $87.32 | ▲ 09:30 equity $10,594.35 vs yday $10,559.93 (+34.42) | — | — |
| 2026-08-31 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $87.32 | ▼ close $10,544.60 vs 09:30 $10,594.35 (session -49.75) | — | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $87.32 | ▲ 09:30 equity $10,647.49 vs yday $10,544.60 (+102.89) | — | — |
| 2026-09-01 09:30 ET | **SELL** | `RZLT` | 1 | $4.64 | $0.07 | $-0.42 | $91.89 | ▼ -0.42 after sell → book $10,647.42; vs 09:30 mark -0.07 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `AMTX` | 2 | $1.88 | $0.06 | $-0.15 | $95.59 | ▼ -0.15 after sell → book $10,647.36; vs 09:30 mark -0.06 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $95.59 | ▲ close $10,802.61 vs 09:30 $10,647.49 (session +155.25) | — | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $95.59 | ▼ 09:30 equity $10,757.85 vs yday $10,802.61 (-44.76) | — | — |
| 2026-09-02 09:30 ET | **SELL** | `CRDL` | 2 | $2.16 | $0.07 | $+0.14 | $99.84 | ▲ +0.14 after sell → book $10,757.78; vs 09:30 mark -0.07 | dropped from list after 5 sess (min 5) | — |
| 2026-09-02 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $99.84 | ▲ close $10,928.50 vs 09:30 $10,757.85 (session +170.72) | — | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $99.84 | ▲ 09:30 equity $10,985.67 vs yday $10,928.50 (+57.17) | — | — |
| 2026-09-03 09:30 ET | **SELL** | `RRC` | 37 | $42.43 | $2.12 | $+32.41 | $1,667.63 | ▲ +32.41 after sell → book $10,983.55; vs 09:30 mark -2.12 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `MOS` | 64 | $26.12 | $2.21 | $+131.29 | $3,337.10 | ▲ +131.29 after sell → book $10,981.34; vs 09:30 mark -2.21 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `KURA` | 118 | $13.25 | $2.38 | $+27.14 | $4,898.23 | ▲ +27.14 after sell → book $10,978.97; vs 09:30 mark -2.37 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `AVBP` | 49 | $30.58 | $2.16 | $-14.59 | $6,394.49 | ▼ -14.59 after sell → book $10,976.81; vs 09:30 mark -2.16 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `ABX` | 158 | $9.68 | $2.50 | $-4.97 | $7,921.42 | ▼ -4.97 after sell → book $10,974.30; vs 09:30 mark -2.51 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `BE` | 6 | $219.00 | $2.03 | $-52.64 | $9,233.40 | ▼ -52.64 after sell → book $10,972.28; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 24 | $52.88 | $2.06 | — | $7,962.21 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1319.06 | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 30 | $42.93 | $2.08 | — | $6,672.23 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+8.1; leftover $1319.06 | — |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 363 | $3.63 | $4.68 | — | $5,349.86 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,ohlc_hot; 🔵; ⚪; ret5=+9.8; leftover $1319.06 | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $132.45 | $2.02 | — | $4,155.79 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+3.6; leftover $1319.06 | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 78 | $16.77 | $2.22 | — | $2,845.51 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+5.7; leftover $1319.06 | — |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 605 | $2.18 | $7.80 | — | $1,518.81 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+1.4; leftover $1319.06 | — |
| 2026-09-03 09:30 ET | **BUY** | `SDGR` | 62 | $21.03 | $2.18 | — | $212.77 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ⚪; ret5=+7.2; leftover $1319.06 | — |
| 2026-09-03 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $212.77 | ▼ close $10,646.52 vs 09:30 $10,985.67 (session -302.71) | — | — |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $212.77 | ▼ 09:30 equity $10,613.26 vs yday $10,646.52 (-33.26) | — | — |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 106 | $15.00 | $2.34 | $+56.83 | $1,800.43 | ▲ +56.83 after sell → book $10,610.92; vs 09:30 mark -2.34 | dropped from list after 6 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `GRRR` | 2 | $13.56 | $0.30 | $-4.82 | $1,827.25 | ▼ -4.82 after sell → book $10,610.62; vs 09:30 mark -0.30 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `EQ` | 14 | $2.22 | $0.37 | $-4.12 | $1,857.96 | ▼ -4.12 after sell → book $10,610.25; vs 09:30 mark -0.37 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `ZYME` | 1 | $29.81 | $0.32 | $+0.29 | $1,887.45 | ▲ +0.29 after sell → book $10,609.93; vs 09:30 mark -0.32 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `ADBT` | 7 | $0.31 | $0.06 | $-33.19 | $1,889.56 | ▼ -33.19 after sell → book $10,609.87; vs 09:30 mark -0.06 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 149 | $2.52 | $2.44 | — | $1,511.64 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+5.0; leftover $377.91 | — |
| 2026-09-04 09:30 ET | **BUY** | `BHC` | 56 | $6.71 | $2.16 | — | $1,133.72 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+7.3; leftover $377.91 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 79 | $4.78 | $2.23 | — | $753.88 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.0; leftover $377.91 | — |
| 2026-09-04 09:30 ET | **BUY** | `VIR` | 33 | $11.31 | $2.09 | — | $378.56 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+1.2; leftover $377.91 | — |
| 2026-09-04 09:30 ET | **BUY** | `CRM` | 1 | $263.36 | $1.99 | — | $113.20 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,mover_buy; 🔵; ⚪; ret5=+4.9; leftover $377.91 | — |
| 2026-09-04 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $113.20 | ▼ close $10,570.22 vs 09:30 $10,613.26 (session -28.75) | — | — |
| 2026-09-08 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $113.20 | ▼ 09:30 equity $10,540.05 vs yday $10,570.22 (-30.17) | — | — |
| 2026-09-08 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $113.20 | ▼ close $10,480.78 vs 09:30 $10,540.05 (session -59.27) | — | — |
| 2026-09-09 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $113.20 | ▼ 09:30 equity $10,425.45 vs yday $10,480.78 (-55.33) | — | — |
| 2026-09-09 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $113.20 | ▼ close $10,129.31 vs 09:30 $10,425.45 (session -296.15) | — | — |
| 2026-09-10 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $113.20 | ▼ 09:30 equity $9,940.75 vs yday $10,129.31 (-188.56) | — | — |
| 2026-09-10 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $113.20 | ▼ close $9,717.23 vs 09:30 $9,940.75 (session -223.52) | — | — |
| 2026-09-11 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $113.20 | ▲ 09:30 equity $9,820.29 vs yday $9,717.23 (+103.06) | — | — |
| 2026-09-11 09:30 ET | **SELL** | `ATRC` | 24 | $53.53 | $2.08 | $+11.46 | $1,395.84 | ▲ +11.46 after sell → book $9,818.21; vs 09:30 mark -2.08 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `HRMY` | 30 | $41.30 | $2.10 | $-53.08 | $2,632.74 | ▼ -53.08 after sell → book $9,816.11; vs 09:30 mark -2.10 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `CABA` | 363 | $2.77 | $4.75 | $-321.62 | $3,633.50 | ▼ -321.62 after sell → book $9,811.36; vs 09:30 mark -4.75 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `RVTY` | 9 | $122.40 | $2.04 | $-94.50 | $4,733.06 | ▼ -94.50 after sell → book $9,809.32; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `ARCT` | 78 | $14.06 | $2.25 | $-215.85 | $5,827.49 | ▼ -215.85 after sell → book $9,807.07; vs 09:30 mark -2.25 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `CRDL` | 605 | $2.03 | $7.91 | $-106.47 | $7,047.73 | ▼ -106.47 after sell → book $9,799.16; vs 09:30 mark -7.91 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **SELL** | `SDGR` | 62 | $18.93 | $2.20 | $-134.57 | $8,219.19 | ▼ -134.57 after sell → book $9,796.96; vs 09:30 mark -2.20 | dropped from list after 5 sess (min 5) | — |
| 2026-09-11 09:30 ET | **BUY** | `NVT` | 6 | $157.78 | $2.01 | — | $7,270.50 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ret5=+4.7; leftover $1027.40 | — |
| 2026-09-11 09:30 ET | **BUY** | `AMTX` | 503 | $2.04 | $6.49 | — | $6,237.90 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+6.8; leftover $1027.40 | — |
| 2026-09-11 09:30 ET | **BUY** | `CLOV` | 216 | $4.75 | $2.79 | — | $5,209.11 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+9.5; leftover $1027.40 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAK` | 484 | $2.12 | $6.24 | — | $4,176.79 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,ohlc_hot; 🔵; ret5=+7.1; leftover $1027.40 | — |
| 2026-09-11 09:30 ET | **BUY** | `FUBO` | 88 | $11.55 | $2.25 | — | $3,158.13 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $1027.40 | — |
| 2026-09-11 09:30 ET | **BUY** | `VIST` | 13 | $77.33 | $2.03 | — | $2,150.81 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; ret5=+2.5; leftover $1027.40 | — |
| 2026-09-11 09:30 ET | **BUY** | `BAND` | 19 | $52.55 | $2.05 | — | $1,150.32 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ⚪; ret5=+8.6; leftover $1027.40 | — |
| 2026-09-11 09:30 ET | **BUY** | `PAGS` | 101 | $10.11 | $2.29 | — | $126.91 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ⚪; ret5=+2.9; leftover $1027.40 | — |
| 2026-09-11 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $126.91 | ▲ close $9,835.59 vs 09:30 $9,820.29 (session +64.78) | — | — |
| 2026-09-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $126.91 | ▼ 09:30 equity $9,770.48 vs yday $9,835.59 (-65.11) | — | — |
| 2026-09-14 09:30 ET | **SELL** | `ALEC` | 149 | $2.15 | $2.47 | $-60.04 | $444.79 | ▼ -60.04 after sell → book $9,768.01; vs 09:30 mark -2.47 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `BHC` | 56 | $5.93 | $2.18 | $-48.02 | $774.69 | ▼ -48.02 after sell → book $9,765.83; vs 09:30 mark -2.18 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `OABI` | 79 | $4.13 | $2.25 | $-55.83 | $1,098.71 | ▼ -55.83 after sell → book $9,763.58; vs 09:30 mark -2.25 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `VIR` | 33 | $10.73 | $2.11 | $-23.34 | $1,450.69 | ▼ -23.34 after sell → book $9,761.47; vs 09:30 mark -2.11 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 09:30 ET | **SELL** | `CRM` | 1 | $255.75 | $2.01 | $-11.62 | $1,704.43 | ▼ -11.62 after sell → book $9,759.46; vs 09:30 mark -2.01 | dropped from list after 5 sess (min 5) | — |
| 2026-09-14 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,704.43 | ▼ close $9,610.81 vs 09:30 $9,770.48 (session -148.65) | — | — |
| 2026-09-15 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,704.43 | ▲ 09:30 equity $9,624.43 vs yday $9,610.81 (+13.62) | — | — |
| 2026-09-15 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $1,704.43 | ▼ close $9,470.82 vs 09:30 $9,624.43 (session -153.61) | — | — |
| 2026-09-16 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,704.43 | ▼ 09:30 equity $9,269.62 vs yday $9,470.82 (-201.20) | — | — |
| 2026-09-16 09:30 ET | **BUY** | `AVAH` | 14 | $14.31 | $2.03 | — | $1,502.06 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ret5=+4.8; leftover $213.05 | — |
| 2026-09-16 09:30 ET | **BUY** | `BLFS` | 5 | $36.46 | $1.84 | — | $1,317.92 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ret5=+2.9; leftover $213.05 | — |
| 2026-09-16 09:30 ET | **BUY** | `TEM` | 3 | $68.79 | $2.00 | — | $1,109.55 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+7.1; leftover $213.05 | — |
| 2026-09-16 09:30 ET | **BUY** | `RIG` | 36 | $5.87 | $2.10 | — | $896.13 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+3.1; leftover $213.05 | — |
| 2026-09-16 09:30 ET | **BUY** | `VAL` | 2 | $87.40 | $1.75 | — | $719.58 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+3.2; leftover $213.05 | — |
| 2026-09-16 09:30 ET | **BUY** | `MRCY` | 2 | $87.52 | $1.76 | — | $542.78 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ret5=+4.3; leftover $213.05 | — |
| 2026-09-16 09:30 ET | **BUY** | `QLYS` | 1 | $179.60 | $1.80 | — | $361.38 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; ret5=+8.8; leftover $213.05 | — |
| 2026-09-16 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $361.38 | ▼ close $9,141.08 vs 09:30 $9,269.62 (session -115.26) | — | — |
| 2026-09-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $361.38 | ▲ 09:30 equity $9,224.20 vs yday $9,141.08 (+83.12) | — | — |
| 2026-09-17 09:30 ET | **BUY** | `PGEN` | 5 | $7.59 | $0.39 | — | $323.04 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,ohlc_hot; 🔵; ret5=+9.4; leftover $45.17 | — |
| 2026-09-17 09:30 ET | **BUY** | `AMN` | 1 | $34.93 | $0.35 | — | $287.76 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; ret5=+1.6; leftover $45.17 | — |
| 2026-09-17 09:30 ET | **BUY** | `SABR` | 18 | $2.40 | $0.49 | — | $244.07 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+8.4; leftover $45.17 | — |
| 2026-09-17 09:30 ET | **BUY** | `AIB` | 30 | $1.46 | $0.53 | — | $199.74 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ret5=+4.4; leftover $45.17 | — |
| 2026-09-17 09:30 ET | **BUY** | `BYND` | 4 | $11.19 | $0.46 | — | $154.52 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ret5=+0.5; leftover $45.17 | — |
| 2026-09-17 09:30 ET | **BUY** | `QTRX` | 15 | $2.94 | $0.49 | — | $109.94 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer,ohlc_hot; 🔵; ret5=+9.8; leftover $45.17 | — |
| 2026-09-17 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $109.94 | ▼ close $9,154.51 vs 09:30 $9,224.20 (session -66.99) | — | — |
| 2026-09-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $109.94 | ▲ 09:30 equity $9,167.12 vs yday $9,154.51 (+12.61) | — | — |
| 2026-09-18 09:30 ET | **SELL** | `NVT` | 6 | $152.71 | $2.03 | $-34.46 | $1,024.17 | ▼ -34.46 after sell → book $9,165.10; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `AMTX` | 503 | $1.90 | $6.58 | $-83.49 | $1,973.29 | ▼ -83.49 after sell → book $9,158.51; vs 09:30 mark -6.59 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `CLOV` | 216 | $4.50 | $2.83 | $-59.62 | $2,942.46 | ▼ -59.62 after sell → book $9,155.68; vs 09:30 mark -2.83 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `BAK` | 484 | $1.77 | $6.33 | $-181.98 | $3,792.80 | ▼ -181.98 after sell → book $9,149.35; vs 09:30 mark -6.33 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `FUBO` | 88 | $9.90 | $2.28 | $-149.73 | $4,661.72 | ▼ -149.73 after sell → book $9,147.07; vs 09:30 mark -2.28 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `VIST` | 13 | $72.99 | $2.05 | $-60.50 | $5,608.54 | ▼ -60.50 after sell → book $9,145.02; vs 09:30 mark -2.05 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `BAND` | 19 | $51.19 | $2.07 | $-30.05 | $6,578.99 | ▼ -30.05 after sell → book $9,142.95; vs 09:30 mark -2.07 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **SELL** | `PAGS` | 101 | $9.55 | $2.32 | $-61.17 | $7,541.22 | ▼ -61.17 after sell → book $9,140.63; vs 09:30 mark -2.32 | dropped from list after 5 sess (min 5) | — |
| 2026-09-18 09:30 ET | **BUY** | `BHVN` | 66 | $14.07 | $2.19 | — | $6,610.41 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $942.65 | — |
| 2026-09-18 09:30 ET | **BUY** | `DDD` | 263 | $3.58 | $3.39 | — | $5,665.48 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+8.3; leftover $942.65 | — |
| 2026-09-18 09:30 ET | **BUY** | `RANI` | 1109 | $0.85 | $12.75 | — | $4,710.08 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; ret5=+3.6; leftover $942.65 | — |
| 2026-09-18 09:30 ET | **BUY** | `SHLS` | 123 | $7.64 | $2.36 | — | $3,768.00 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ret5=+7.6; leftover $942.65 | — |
| 2026-09-18 09:30 ET | **BUY** | `XE` | 57 | $16.28 | $2.16 | — | $2,837.88 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ret5=+3.1; leftover $942.65 | — |
| 2026-09-18 09:30 ET | **BUY** | `AMD` | 1 | $547.37 | $1.99 | — | $2,288.52 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; ret5=+8.2; leftover $942.65 | — |
| 2026-09-18 09:30 ET | **BUY** | `SYM` | 21 | $44.70 | $2.05 | — | $1,347.76 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+8.5; leftover $942.65 | — |
| 2026-09-18 09:30 ET | **BUY** | `TH` | 45 | $20.91 | $2.12 | — | $404.69 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $942.65 | — |
| 2026-09-18 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $404.69 | ▼ close $9,015.17 vs 09:30 $9,167.12 (session -96.44) | — | — |
| 2026-09-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $404.69 | ▲ 09:30 equity $9,164.62 vs yday $9,015.17 (+149.45) | — | — |
| 2026-09-21 09:30 ET | **BUY** | `BTDR` | 3 | $13.47 | $0.41 | — | $363.85 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+8.4; leftover $50.59 | — |
| 2026-09-21 09:30 ET | **BUY** | `ORBS` | 45 | $1.11 | $0.63 | — | $313.26 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; ret5=+6.9; leftover $50.59 | — |
| 2026-09-21 09:30 ET | **BUY** | `SBET` | 5 | $9.99 | $0.51 | — | $262.80 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+5.3; leftover $50.59 | — |
| 2026-09-21 09:30 ET | **BUY** | `BTBT` | 27 | $1.82 | $0.57 | — | $212.95 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+8.8; leftover $50.59 | — |
| 2026-09-21 09:30 ET | **BUY** | `SGML` | 4 | $10.13 | $0.42 | — | $171.99 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; ret5=+4.9; leftover $50.59 | — |
| 2026-09-21 09:30 ET | **BUY** | `GLXY` | 1 | $25.95 | $0.26 | — | $145.78 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $50.59 | — |
| 2026-09-21 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $145.78 | ▼ close $9,091.60 vs 09:30 $9,164.62 (session -70.19) | — | — |
| 2026-09-22 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $145.78 | ▼ 09:30 equity $9,085.67 vs yday $9,091.60 (-5.93) | — | — |
| 2026-09-22 09:30 ET | **BUY** | `ALOY` | 2 | $9.40 | $0.19 | — | $126.79 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; ret5=+9.5; leftover $20.83 | — |
| 2026-09-22 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $126.79 | ▲ close $9,104.99 vs 09:30 $9,085.67 (session +19.52) | — | — |
| 2026-09-23 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $126.79 | ▲ 09:30 equity $9,235.27 vs yday $9,104.99 (+130.28) | — | — |
| 2026-09-23 09:30 ET | **SELL** | `AVAH` | 14 | $13.12 | $1.90 | $-20.59 | $308.57 | ▼ -20.59 after sell → book $9,233.37; vs 09:30 mark -1.90 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `BLFS` | 5 | $38.04 | $1.94 | $+4.12 | $496.83 | ▲ +4.12 after sell → book $9,231.44; vs 09:30 mark -1.93 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `TEM` | 3 | $76.47 | $2.02 | $+19.02 | $724.22 | ▲ +19.02 after sell → book $9,229.42; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `RIG` | 36 | $5.53 | $2.11 | $-16.45 | $921.19 | ▼ -16.45 after sell → book $9,227.30; vs 09:30 mark -2.12 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `VAL` | 2 | $83.39 | $1.69 | $-11.47 | $1,086.28 | ▼ -11.47 after sell → book $9,225.61; vs 09:30 mark -1.69 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `MRCY` | 2 | $83.03 | $1.69 | $-12.42 | $1,250.65 | ▼ -12.42 after sell → book $9,223.92; vs 09:30 mark -1.69 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **SELL** | `QLYS` | 1 | $184.19 | $1.86 | $+0.93 | $1,432.97 | ▲ +0.93 after sell → book $9,222.06; vs 09:30 mark -1.86 | dropped from list after 5 sess (min 5) | — |
| 2026-09-23 09:30 ET | **BUY** | `DXCM` | 2 | $89.50 | $1.80 | — | $1,252.18 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+5.3; leftover $179.12 | — |
| 2026-09-23 09:30 ET | **BUY** | `HALO` | 1 | $116.85 | $1.17 | — | $1,134.16 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+3.3; leftover $179.12 | — |
| 2026-09-23 09:30 ET | **BUY** | `ADMA` | 18 | $9.81 | $1.82 | — | $955.76 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+4.0; leftover $179.12 | — |
| 2026-09-23 09:30 ET | **BUY** | `OMER` | 8 | $20.65 | $1.68 | — | $788.88 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $179.12 | — |
| 2026-09-23 09:30 ET | **BUY** | `INDP` | 45 | $3.93 | $1.90 | — | $610.13 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $179.12 | — |
| 2026-09-23 09:30 ET | **BUY** | `MNRO` | 12 | $14.14 | $1.73 | — | $438.71 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; ret5=+0.3; leftover $179.12 | — |
| 2026-09-23 09:30 ET | **BUY** | `NTSK` | 9 | $18.57 | $1.70 | — | $269.84 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+8.2; leftover $179.12 | — |
| 2026-09-23 09:30 ET | **BUY** | `HIMS` | 5 | $30.40 | $1.53 | — | $116.31 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+9.3; leftover $179.12 | — |
| 2026-09-23 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $116.31 | ▼ close $8,942.75 vs 09:30 $9,235.27 (session -265.97) | — | — |
| 2026-09-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $116.31 | ▼ 09:30 equity $8,854.94 vs yday $8,942.75 (-87.81) | — | — |
| 2026-09-24 09:30 ET | **SELL** | `PGEN` | 5 | $7.38 | $0.40 | $-1.85 | $152.80 | ▼ -1.85 after sell → book $8,854.54; vs 09:30 mark -0.40 | dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `AMN` | 1 | $33.82 | $0.36 | $-1.82 | $186.26 | ▼ -1.82 after sell → book $8,854.18; vs 09:30 mark -0.36 | dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `SABR` | 18 | $2.17 | $0.46 | $-5.09 | $224.86 | ▼ -5.09 after sell → book $8,853.71; vs 09:30 mark -0.47 | dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `AIB` | 30 | $1.42 | $0.54 | $-2.26 | $266.92 | ▼ -2.26 after sell → book $8,853.18; vs 09:30 mark -0.53 | dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `BYND` | 4 | $10.07 | $0.43 | $-5.37 | $306.77 | ▼ -5.37 after sell → book $8,852.74; vs 09:30 mark -0.44 | dropped from list after 5 sess (min 5) | — |
| 2026-09-24 09:30 ET | **SELL** | `QTRX` | 15 | $3.15 | $0.54 | $+2.13 | $353.48 | ▲ +2.13 after sell → book $8,852.20; vs 09:30 mark -0.54 | dropped from list after 5 sess (min 5) | — |
| 2026-09-24 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $353.48 | ▼ close $8,835.37 vs 09:30 $8,854.94 (session -16.83) | — | — |
| 2026-09-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $108.69 | ▼ 09:30 equity $8,912.64 vs yday $8,926.43 (-13.79) | 09:30 open · cash $108.69 (unchanged overnight, no fees) · equity $8,912.64 vs prior close $8,926.43 (-13.79) · 12 name(s) re-marked at the open (per-name table). AIB×16 yday $1.42 → 09:30 $1.42 +0.00; BHVN×1 yday $13.19 → 09:30 $13.19 +0.00; BTBT×3 yday $1.79 → 09:30 $1.79 +0.00; BTQ×2 yday $2.79 → 09:30 $2.79 +0.00; DDD×5 yday $3.43 → 09:30 $3.43 +0.00; HELP×1 yday $12.59 → 09:30 $12.59 +0.00; INDP×1083 yday $4.00 → 09:30 $4.00 +0.00; ORBS×5 yday $1.03 → 09:30 $1.03 +0.00; RANI×21 yday $0.75 → 09:30 $0.75 +0.00; SAIL×197 yday $22.12 → 09:30 $22.05 -13.79; SHLS×2 yday $7.45 → 09:30 $7.45 +0.00; XE×1 yday $15.72 → 09:30 $15.72 +0.00 | — |
| 2026-09-25 09:30 ET | **BUY** | `MRVI` | 1 | $7.65 | $0.08 | — | $100.96 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+5.2; leftover $13.59 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `HLP` | 6 | $2.20 | $0.15 | — | $87.61 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,ohlc_hot; 🔵; ret5=+8.9; leftover $13.59 | join🟢 sector🟡 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-25 09:30 ET | **BUY** | `SATL` | 2 | $6.00 | $0.13 | — | $75.48 | — | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,ohlc_hot; 🔵; ret5=+7.8; leftover $13.59 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-25 16:00 ET | **CLOSE** | 16:00 close | — | — | — | — | $75.48 | ▼ close $8,634.86 vs 09:30 $8,912.64 (session -277.42) | 16:00 close · cash $75.48 · equity $8,634.86 vs 09:30 $8,912.64 (-277.78; session marks -277.42) · 15 name(s) marked open→close (per-name table). AIB×16 09:30 $1.42 → close $1.42 -0.00; BHVN×1 09:30 $13.19 → close $13.19 -0.00; BTBT×3 09:30 $1.79 → close $1.79 -0.00; BTQ×2 09:30 $2.79 → close $2.79 -0.00; DDD×5 09:30 $3.43 → close $3.43 +0.00; HELP×1 09:30 $12.59 → close $12.59 +0.00; INDP×1083 09:30 $4.00 → close $4.00 +0.00; ORBS×5 09:30 $1.03 → close $1.03 -0.00; RANI×21 09:30 $0.75 → close $0.75 +0.00; SAIL×197 09:30 $22.05 → close $20.64 -277.77; SHLS×2 09:30 $7.45 → close $7.45 -0.00; XE×1 09:30 $15.72 → close $15.72 +0.00; MRVI×1 09:30 $7.65 → close $7.60 -0.05; HLP×6 09:30 $2.20 → close $2.21 +0.06; SATL×2 09:30 $6.00 → close $6.17 +0.34 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `VOR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `TLN` | cash | leftover split 4.68 < 1 share @ 359.83 |
| 2026-08-14 | `SLG` | cash | leftover split 4.68 < 1 share @ 57.61 |
| 2026-08-14 | `WDC` | cash | leftover split 4.68 < 1 share @ 503.50 |
| 2026-08-14 | `ADUR` | cash | leftover split 4.68 < 1 share @ 16.50 |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `VOR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `LDI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `HYLN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `DVN` | cash | leftover split 2.56 < 1 share @ 46.18 |
| 2026-08-17 | `DNN` | cash | leftover split 2.56 < 1 share @ 3.24 |
| 2026-08-17 | `OCC` | cash | leftover split 2.56 < 1 share @ 18.24 |
| 2026-08-17 | `ALM` | cash | leftover split 2.56 < 1 share @ 16.20 |
| 2026-08-17 | `NEWP` | cash | leftover split 2.56 < 1 share @ 6.94 |
| 2026-08-17 | `KLAR` | cash | leftover split 2.56 < 1 share @ 20.67 |
| 2026-08-17 | `VNET` | cash | leftover split 2.56 < 1 share @ 7.75 |
| 2026-08-18 | `TPG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `VOR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `LDI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `ANGX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `HYLN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `IQ` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CYPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `PURR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `VNET` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `DVLT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `TPG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `VOR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `LDI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `BTBT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `ANGX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `HYLN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `IQ` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `GUTS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNTN` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AEHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MXL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `PAYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | `LDI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `BTBT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `ANGX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `HYLN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `IQ` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-21 | `IQ` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `DNA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `EXK` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `CRSP` | cash | leftover split 6.67 < 1 share @ 59.72 |
| 2026-08-21 | `CF` | cash | leftover split 6.67 < 1 share @ 127.43 |
| 2026-08-21 | `TXG` | cash | leftover split 6.67 < 1 share @ 64.39 |
| 2026-08-21 | `DXYZ` | cash | leftover split 6.67 < 1 share @ 34.89 |
| 2026-08-21 | `BEKE` | cash | leftover split 6.67 < 1 share @ 17.93 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `DNA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `EXK` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `ORBS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `EMBC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ABAT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `PDD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BZ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `AG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `BHP` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `HDSN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `IAG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `KGC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `NFGC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `DNA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `EXK` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `ORBS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `EMBC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `OCUL` | cash | leftover split 5.08 < 1 share @ 10.98 |
| 2026-08-25 | `HCA` | cash | leftover split 5.08 < 1 share @ 426.97 |
| 2026-08-25 | `KURA` | cash | leftover split 5.08 < 1 share @ 13.59 |
| 2026-08-25 | `LIFE` | cash | leftover split 5.08 < 1 share @ 36.96 |
| 2026-08-25 | `AVAH` | cash | leftover split 5.08 < 1 share @ 13.62 |
| 2026-08-25 | `ETON` | cash | leftover split 5.08 < 1 share @ 64.55 |
| 2026-08-26 | `AG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `BHP` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `HDSN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `IAG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `KGC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `NFGC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `DNA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `EXK` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `ORBS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `EMBC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `AMTX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `INSP` | cash | leftover split 4.55 < 1 share @ 60.07 |
| 2026-08-26 | `CRMD` | cash | leftover split 4.55 < 1 share @ 8.60 |
| 2026-08-26 | `SENS` | cash | leftover split 4.55 < 1 share @ 9.48 |
| 2026-08-26 | `BE` | cash | leftover split 4.55 < 1 share @ 213.94 |
| 2026-08-26 | `ACRS` | cash | leftover split 4.55 < 1 share @ 6.53 |
| 2026-08-26 | `TMCI` | cash | leftover split 4.55 < 1 share @ 4.78 |
| 2026-08-27 | `ORBS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `EMBC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `AMTX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `CRDL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-28 | `RZLT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `AMTX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `RRC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-28 | `MOS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-28 | `KURA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-28 | `AVBP` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-28 | `ABX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-28 | `BE` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-28 | `TTMI` | cash | leftover split 36.38 < 1 share @ 122.81 |
| 2026-08-28 | `BTSG` | cash | leftover split 36.38 < 1 share @ 60.54 |
| 2026-08-31 | `RZLT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `AMTX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `CRDL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-31 | `RRC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `CRK` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `MOS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `KURA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `AVBP` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `ABX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `BE` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `GRRR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `EQ` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `ZYME` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `ADBT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ACDC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FIG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PANW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SAIL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FROG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `CRDL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-01 | `RRC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `CRK` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `MOS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `KURA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `AVBP` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `ABX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `BE` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `GRRR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `EQ` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `ZYME` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `ADBT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `RES` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KOS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OIS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `FTI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KMI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OKE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `RRC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `CRK` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `MOS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `KURA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `AVBP` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `ABX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `BE` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `GRRR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `EQ` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `ZYME` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `ADBT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `PCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HRMY` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VSTM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TEL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `CYPH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ARCT` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `NVAX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | `GRRR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `EQ` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `ZYME` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `ADBT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `ARCT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `CRDL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `SDGR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `ATRC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `HRMY` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `CABA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `RVTY` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `ARCT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `CRDL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `SDGR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-08 | `ALEC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `BHC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `OABI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `VIR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `CRM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-08 | `GGB` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `TX` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `CHGG` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `SNOW` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `EVGO` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `BMNR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `PURR` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-08 | `HAFN` | hard_red | hard-red S=-11.47 sit; no new buys |
| 2026-09-09 | `ATRC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `HRMY` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `CABA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `RVTY` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `ARCT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `CRDL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `SDGR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-09 | `ALEC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `BHC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `OABI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `VIR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `CRM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-09 | `AR` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VET` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `GRNT` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `VENU` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `LFMD` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `KEP` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `ZIM` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-09 | `OBE` | hard_red | hard-red S=-13.95 sit; no new buys |
| 2026-09-10 | `ATRC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `HRMY` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `CABA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `RVTY` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `ARCT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `CRDL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `SDGR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-10 | `ALEC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `BHC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `OABI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `VIR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `CRM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-10 | `UGP` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `LBRT` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CLB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `DDOG` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `CHYM` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `SUNB` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `TNGX` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-10 | `TJGC` | hard_red | hard-red S=-13.28 sit; no new buys |
| 2026-09-11 | `ALEC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `BHC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `OABI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `VIR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-11 | `CRM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-14 | `NVT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `AMTX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `CLOV` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `BAK` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `FUBO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `VIST` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `BAND` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `PAGS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-14 | `CVE` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DK` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `DELL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `HTFL` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `FPS` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `NTAP` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `XRX` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-14 | `IMSR` | hard_red | hard-red S=-11.00 sit; no new buys |
| 2026-09-15 | `NVT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `AMTX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `CLOV` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `BAK` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `FUBO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `VIST` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `BAND` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `PAGS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-15 | `ATRC` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `RBRK` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `QLYS` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `CYPH` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `IVVD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `LFMD` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `OKTA` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-15 | `IOT` | hard_red | hard-red S=-3.84 sit; no new buys |
| 2026-09-16 | `NVT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `AMTX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `CLOV` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `BAK` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `FUBO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `VIST` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `BAND` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `PAGS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-16 | `IQV` | cash | leftover split 213.05 < 1 share @ 270.89 |
| 2026-09-17 | `NVT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `AMTX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `CLOV` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `BAK` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `FUBO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `VIST` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `BAND` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `PAGS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-17 | `AVAH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `BLFS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `TEM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `RIG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `VAL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `MRCY` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `QLYS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-17 | `SMTC` | cash | leftover split 45.17 < 1 share @ 170.85 |
| 2026-09-17 | `FTAI` | cash | leftover split 45.17 < 1 share @ 196.50 |
| 2026-09-18 | `AVAH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `BLFS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `TEM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `RIG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `VAL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `MRCY` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `QLYS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-18 | `PGEN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-18 | `AMN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-18 | `SABR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-18 | `AIB` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-18 | `BYND` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-18 | `QTRX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `AVAH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `BLFS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `TEM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `RIG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `VAL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `MRCY` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `QLYS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-21 | `PGEN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-21 | `AMN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-21 | `SABR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-21 | `AIB` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-21 | `BYND` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-21 | `QTRX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-21 | `BHVN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `DDD` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `RANI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `SHLS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `XE` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `AMD` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `SYM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `TH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-21 | `A` | cash | leftover split 50.59 < 1 share @ 157.87 |
| 2026-09-21 | `SNDK` | cash | leftover split 50.59 < 1 share @ 1826.00 |
| 2026-09-22 | `AVAH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `BLFS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `TEM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `RIG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `VAL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `MRCY` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `QLYS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-22 | `PGEN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-22 | `AMN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-22 | `SABR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-22 | `AIB` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-22 | `BYND` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-22 | `QTRX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-22 | `BHVN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `DDD` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `RANI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `SHLS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `XE` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `AMD` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `SYM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `TH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-22 | `BTDR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `ORBS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `SBET` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `BTBT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `SGML` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `GLXY` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-22 | `APPS` | no_price | no 09:30 open |
| 2026-09-22 | `ARHS` | no_price | no 09:30 open |
| 2026-09-22 | `RMBS` | no_price | no 09:30 open |
| 2026-09-22 | `EMAT` | no_price | no 09:30 open |
| 2026-09-22 | `NTSK` | no_price | no 09:30 open |
| 2026-09-22 | `ANAB` | no_price | no 09:30 open |
| 2026-09-23 | `PGEN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-23 | `AMN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-23 | `SABR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-23 | `AIB` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-23 | `BYND` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-23 | `QTRX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-23 | `BHVN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `DDD` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `RANI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `SHLS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `XE` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `AMD` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `SYM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `TH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-23 | `BTDR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-23 | `ORBS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-23 | `SBET` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-23 | `BTBT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-23 | `SGML` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-23 | `GLXY` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-23 | `ALOY` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `BHVN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `DDD` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `RANI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `SHLS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `XE` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `AMD` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `SYM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `TH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-24 | `BTDR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `ORBS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `SBET` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `BTBT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `SGML` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `GLXY` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-24 | `ALOY` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-24 | `DXCM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `HALO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `ADMA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `OMER` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `INDP` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `MNRO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-24 | `HIMS` | min_hold | dropped but min-hold 1/5 sess — no sell |
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
| `BHVN` | 66 | 2026-09-18 @ $14.07 | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; 🔵; ret5=+8.6; leftover $942.65 |
| `DDD` | 263 | 2026-09-18 @ $3.58 | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+8.3; leftover $942.65 |
| `RANI` | 1109 | 2026-09-18 @ $0.85 | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; ret5=+3.6; leftover $942.65 |
| `SHLS` | 123 | 2026-09-18 @ $7.64 | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ret5=+7.6; leftover $942.65 |
| `XE` | 57 | 2026-09-18 @ $16.28 | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; 🔵; ret5=+3.1; leftover $942.65 |
| `AMD` | 1 | 2026-09-18 @ $547.37 | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; ret5=+8.2; leftover $942.65 |
| `SYM` | 21 | 2026-09-18 @ $44.70 | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+8.5; leftover $942.65 |
| `TH` | 45 | 2026-09-18 @ $20.91 | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; 🔵; ⚪; ret5=+8.6; leftover $942.65 |
| `BTDR` | 3 | 2026-09-21 @ $13.47 | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover,ohlc_hot; ret5=+8.4; leftover $50.59 |
| `ORBS` | 45 | 2026-09-21 @ $1.11 | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover; ret5=+6.9; leftover $50.59 |
| `SBET` | 5 | 2026-09-21 @ $9.99 | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+5.3; leftover $50.59 |
| `BTBT` | 27 | 2026-09-21 @ $1.82 | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+8.8; leftover $50.59 |
| `SGML` | 4 | 2026-09-21 @ $10.13 | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; ret5=+4.9; leftover $50.59 |
| `GLXY` | 1 | 2026-09-21 @ $25.95 | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; 🔵; ret5=+1.2; leftover $50.59 |
| `ALOY` | 2 | 2026-09-22 @ $9.40 | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer; ret5=+9.5; leftover $20.83 |
| `DXCM` | 2 | 2026-09-23 @ $89.50 | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+5.3; leftover $179.12 |
| `HALO` | 1 | 2026-09-23 @ $116.85 | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+3.3; leftover $179.12 |
| `ADMA` | 18 | 2026-09-23 @ $9.81 | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten; 🔵; ⚪; ret5=+4.0; leftover $179.12 |
| `OMER` | 8 | 2026-09-23 @ $20.65 | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list flatten,probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+5.9; leftover $179.12 |
| `INDP` | 45 | 2026-09-23 @ $3.93 | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list probable,yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+9.6; leftover $179.12 |
| `MNRO` | 12 | 2026-09-23 @ $14.14 | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list yday_gainer; ret5=+0.3; leftover $179.12 |
| `NTSK` | 9 | 2026-09-23 @ $18.57 | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+8.2; leftover $179.12 |
| `HIMS` | 5 | 2026-09-23 @ $30.40 | union ∩ coil_off hold 5, no 🚨; gate ret_5_min=0.0,ret_5_max=10.0,rvol_min=0.7,rvol_max=2.2; list ohlc_hot; 🔵; ret5=+9.3; leftover $179.12 |
